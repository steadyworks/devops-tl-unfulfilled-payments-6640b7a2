# backend/lib/payments/fulfillment_service.py

import logging
import traceback
from typing import List, Optional, Tuple
from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from backend.db.dal import (
    DALGiftcards,
    DALPaymentEvents,
    DALPayments,
    DAOPaymentEventsCreate,
    DAOPaymentsUpdate,
    FilterOp,
    locked_row_by_id,
    safe_transaction,
)
from backend.db.data_models import (
    DAOGiftcards,
    DAOPayments,
    GiftcardStatus,
    PaymentEventSource,
    PaymentStatus,
)
from backend.db.data_models.types_ENSURE_BACKWARDS_COMPATIBILITY import (
    ShareCreateRequest,
)
from backend.lib.sharing.schemas import ShareCreateResponse
from backend.lib.sharing.service import initialize_shares_and_channels
from backend.lib.utils.common import utcnow


async def fulfill_payment_success_if_needed(
    session: AsyncSession,
    *,
    payment_id: UUID,
    audit_source: PaymentEventSource,
    audit_context: Optional[dict[str, str]] = None,
) -> Tuple[Optional[ShareCreateResponse], List[UUID], bool]:
    """
    Public entrypoint used by webhook and eager pollers.

    - Opens a transaction
    - Row-locks the payment to make this a single-runner
    - Skips if already fulfilled (giftcards exist for this payment)
    - Performs DB side-effects atomically:
        * initialize_shares_and_channels (idempotent)
        * annotate shares.created_by_payment_id if NULL
        * upsert 'GRANTED' giftcards per share (UNIQUE(share_id) UPSERT)
        * append audit event
    - Returns (share_resp, giftcard_ids, did_fulfill)

    Post-commit enqueue of outbox messages is left to the caller.
    """
    if session.in_transaction():
        raise RuntimeError(
            "[fulfill_payment_success_if_needed] Must be called with no active transaction on the session. "
            "Commit/close the caller transaction first."
        )

    if audit_context is None:
        audit_context = {}

    share_resp: Optional[ShareCreateResponse] = None
    giftcard_ids: List[UUID] = []
    did_fulfill = False

    async with safe_transaction(session, "payments.fulfill_success"):
        # Single-runner: lock the payment row
        async with locked_row_by_id(session, DAOPayments, payment_id) as payment_row:
            # Only fulfill successful payments
            if payment_row.status != PaymentStatus.SUCCEEDED:
                return None, [], False

            if payment_row.fulfilled_at is not None:
                return None, [], False

            # Preconditions
            if payment_row.photobook_id is None:
                logging.error(
                    "[fulfill_success] Missing photobook_id/user_id on payment %s",
                    payment_id,
                )
                return None, [], False

            if not payment_row.share_create_request:
                logging.error(
                    "[fulfill_success] No share_create_request snapshot on payment %s",
                    payment_id,
                )
                return None, [], False

            # Skip if any giftcard already created for this payment
            already = await DALGiftcards.exists(
                session,
                filters={"created_by_payment_id": (FilterOp.EQ, payment_id)},
            )
            if already:
                return None, [], False

            req: ShareCreateRequest = ShareCreateRequest.deserialize(
                payment_row.share_create_request
            )

            try:
                # 1) Upsert shares/channels (+ outbox)
                share_resp = await initialize_shares_and_channels(
                    session=session,
                    user_id=payment_row.created_by_user_id,
                    photobook_id=payment_row.photobook_id,
                    req=req,
                    created_by_payment_id=payment_id,
                )

                # 3) UPSERT a local 'GRANTED' giftcard per share (no provider issuance yet)
                if req.giftcard_request is not None:
                    amt = req.giftcard_request.amount_per_share
                    ccy = req.giftcard_request.currency.lower().strip()
                    brand = req.giftcard_request.brand_code
                    for r in share_resp.recipients:
                        gc_id = await _upsert_giftcard_for_share(
                            session=session,
                            share_id=r.share_id,
                            payment_id=payment_row.id,
                            user_id=payment_row.created_by_user_id,
                            amount=amt,
                            currency=ccy,
                            brand_code=brand,
                        )
                        if gc_id:
                            giftcard_ids.append(gc_id)

                # 4) Audit fulfillment (append-only)
                await DALPaymentEvents.create(
                    session,
                    DAOPaymentEventsCreate(
                        payment_id=payment_row.id,
                        stripe_event_id=None,
                        event_type="fulfill.success",
                        stripe_event_type=None,
                        source=audit_source,
                        payload={
                            "shares": [
                                {
                                    "share_id": str(r.share_id),
                                    "channels": [
                                        {
                                            "share_channel_id": str(
                                                ch.share_channel_id
                                            ),
                                            "channel_type": ch.channel_type.value,
                                            "destination": ch.destination,
                                        }
                                        for ch in r.share_channel_results
                                    ],
                                    "outbox": [
                                        str(ob.outbox_id) for ob in r.outbox_results
                                    ],
                                }
                                for r in (share_resp.recipients if share_resp else [])
                            ],
                            "giftcard_ids": [str(x) for x in giftcard_ids],
                            **audit_context,
                        },
                        signature_verified=None,
                        applied_status=None,
                    ),
                )

                await DALPayments.update_by_id(
                    session,
                    payment_id,
                    DAOPaymentsUpdate(
                        fulfilled_at=utcnow(),
                        fulfillment_last_error=None,
                    ),
                )

                did_fulfill = True
            except Exception as e:
                did_fulfill = False
                tb = traceback.format_exc()
                await DALPaymentEvents.create(
                    session,
                    DAOPaymentEventsCreate(
                        payment_id=payment_row.id,
                        stripe_event_id=None,
                        event_type="fulfill.failed",
                        stripe_event_type=None,
                        source=audit_source,
                        payload={
                            "error": str(e),
                            "traceback": tb,
                            "shares": [
                                {
                                    "share_id": str(r.share_id),
                                    "channels": [
                                        {
                                            "share_channel_id": str(
                                                ch.share_channel_id
                                            ),
                                            "channel_type": ch.channel_type.value,
                                            "destination": ch.destination,
                                        }
                                        for ch in r.share_channel_results
                                    ],
                                    "outbox": [
                                        str(ob.outbox_id) for ob in r.outbox_results
                                    ],
                                }
                                for r in (share_resp.recipients if share_resp else [])
                            ],
                            "giftcard_ids": [str(x) for x in giftcard_ids],
                            **audit_context,
                        },
                        signature_verified=None,
                        applied_status=None,
                    ),
                )

                await DALPayments.update_by_id(
                    session,
                    payment_id,
                    DAOPaymentsUpdate(
                        fulfilled_at=None,
                        fulfillment_last_error=str(e),
                    ),
                )

    return share_resp, giftcard_ids, did_fulfill


async def _upsert_giftcard_for_share(
    *,
    session: AsyncSession,
    share_id: UUID,
    payment_id: UUID,
    user_id: UUID,
    amount: int,
    currency: str,
    brand_code: Optional[str],
) -> Optional[UUID]:
    """
    Race-safe local grant using UNIQUE(share_id); no provider issuance. Sets linkage if NULL.
    """
    insert_stmt = pg_insert(DAOGiftcards).values(
        share_id=share_id,
        created_by_payment_id=payment_id,
        created_by_user_id=user_id,
        amount_total=amount,
        currency=currency,
        provider=None,
        brand_code=brand_code,
        provider_giftcard_id=None,
        giftcard_code_explicit_override=None,
        status=GiftcardStatus.GRANTED,
        description="Granted via Stripe PI success",
        metadata_json={},
    )
    upsert_stmt = insert_stmt.on_conflict_do_update(
        index_elements=[getattr(DAOGiftcards, "share_id")],
        set_={
            "created_by_payment_id": insert_stmt.excluded.created_by_payment_id,
            "updated_at": func.now(),
        },
        where=getattr(DAOGiftcards, "created_by_payment_id").is_(None),
    ).returning(getattr(DAOGiftcards, "id"))
    row = await session.execute(upsert_stmt)
    gc_id = row.scalar_one_or_none()
    if gc_id:
        return gc_id

    res = await session.execute(
        select(getattr(DAOGiftcards, "id"))
        .where(getattr(DAOGiftcards, "share_id") == share_id)
        .limit(1)
    )
    return res.scalar_one_or_none()
