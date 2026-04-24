# backend/route_handler/checkout.py

from __future__ import annotations

import hashlib
import json
import logging
from typing import Any
from uuid import UUID  # noqa: TC003

from fastapi import HTTPException, Request, status
from pydantic import BaseModel

# DAL imports
from backend.db.dal import (
    DALPaymentEvents,
    DALPayments,
    DAOPaymentEventsCreate,
    DAOPaymentsCreate,
    safe_transaction,
)
from backend.db.data_models import (
    PaymentEventSource,
    PaymentPurpose,
    PaymentStatus,
)
from backend.db.data_models.types_ENSURE_BACKWARDS_COMPATIBILITY import (
    ShareCreateRequest,  # noqa: TC001
)
from backend.route_handler.base import RouteHandler, enforce_response_model

from .utils import fingerprint_share_request

# -----------------------
# Response model
# -----------------------


class CheckoutPaymentBootstrapResponse(BaseModel):
    payment_id: UUID
    stripe_payment_intent_id: str
    client_secret: str
    status: PaymentStatus
    amount_total: int
    currency: str
    idempotency_key: str


# -----------------------
# Route handler
# -----------------------


class CheckoutAPIHandler(RouteHandler):
    def register_routes(self) -> None:
        self.route(
            "/api/checkout/{photobook_id}/initialize-payment",
            "checkout_initializing_payment",
            methods=["POST"],  # <— This should be POST because we accept a body
        )

    # ------------
    # Main handler
    # ------------
    @enforce_response_model
    async def checkout_initializing_payment(
        self,
        photobook_id: UUID,
        payload: ShareCreateRequest,
        request: Request,
    ) -> CheckoutPaymentBootstrapResponse:
        """
        Create a Stripe PI (server-derived idempotency key), persist 'payments' mirror,
        store request snapshot, and return a typed bootstrap for client to confirm().
        """
        async with self.app.new_db_session() as session:
            request_context = await self.get_request_context(request)
            current_user_id = request_context.user_id

            async with safe_transaction(session, context="auth photobook ownership"):
                await self.get_photobook_assert_owned_by(
                    session, photobook_id, current_user_id
                )

            stripe_client = self.get_stripe_client_for_request(request)

            # --- Validate request invariants ---
            if not payload.recipients:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="At least one recipient is required.",
                )

            if payload.giftcard_request is None:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Giftcard request is required for paid checkout.",
                )

            amount_per_share: int = payload.giftcard_request.amount_per_share
            currency: str = payload.giftcard_request.currency.lower().strip()
            n_recipients: int = len(payload.recipients)
            amount_total: int = amount_per_share * n_recipients

            if amount_total <= 0:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Computed payment amount must be > 0.",
                )

            # --- Derive a stable, server-side idempotency key ---
            recipients_fp: str = fingerprint_share_request(photobook_id, payload)
            idem_key: str = self._derive_idempotency_key(
                user_id=current_user_id,
                photobook_id=photobook_id,
                amount_total=amount_total,
                currency=currency,
                recipients_fingerprint=recipients_fp,
            )

            # --- Create Stripe PaymentIntent (idempotent) ---
            # We include a compact metadata set. We’ll add payment_id later if you want via update.
            metadata: dict[str, str] = {
                "user_id": str(current_user_id),
                "photobook_id": str(photobook_id),
                "recipients_fp": recipients_fp,
                "purpose": "giftcard_grant",
            }
            description: str = self._build_pi_description(
                n_recipients=n_recipients,
                amount_per_share=amount_per_share,
                currency=currency,
                photobook_id=photobook_id,
            )

            try:
                pi = await stripe_client.create_stripe_payment_intent_async(
                    amount=amount_total,
                    currency=currency,
                    description=description,
                    idempotency_key=idem_key,
                    metadata=metadata,
                )
            except Exception:
                # Bubble a clean error to the client; logs already captured inside Stripe helper.
                raise HTTPException(
                    status_code=status.HTTP_502_BAD_GATEWAY,
                    detail="Unable to initialize payment with Stripe. Please try again.",
                )

            serialized_payload = payload.serialize()

            # --- Mirror to DB (one transaction) ---
            # If the DB commit fails, we best-effort cancel the PI so the user doesn’t get stuck
            # with a chargeable-but-untracked intent.
            try:
                async with safe_transaction(
                    session, context="checkout initialize payment"
                ):
                    # Persist payments row
                    payment_row = await DALPayments.create(
                        session,
                        DAOPaymentsCreate(
                            created_by_user_id=current_user_id,
                            photobook_id=photobook_id,
                            purpose=PaymentPurpose.GIFTCARD,
                            amount_total=amount_total,
                            currency=currency,
                            stripe_payment_intent_id=pi.stripe_payment_intent_id,
                            stripe_latest_charge_id=pi.latest_charge_id,
                            status=pi.status,
                            description=description,
                            idempotency_key=idem_key,
                            # Store your original request (backward-compat JSON) for later idempotent replays
                            share_create_request=serialized_payload,
                            # Light snapshot for quick debugging/ops; keep immutable fields
                            metadata_json={
                                "n_recipients": n_recipients,
                                "amount_per_share": amount_per_share,
                                "brand_code": payload.giftcard_request.brand_code
                                if payload.giftcard_request
                                else None,
                                "recipients_fp": recipients_fp,
                            },
                            refunded_amount=0,
                        ),
                    )

                    # Audit "bootstrap" event (append-only)
                    await DALPaymentEvents.create(
                        session,
                        DAOPaymentEventsCreate(
                            payment_id=payment_row.id,
                            stripe_event_id=None,
                            event_type="bootstrap.initialize",
                            stripe_event_type=None,
                            source=PaymentEventSource.SYSTEM,
                            payload={
                                "stripe_payment_intent_id": pi.stripe_payment_intent_id,
                                "idempotency_key": idem_key,
                                "status": pi.status.value,
                                "amount_total": amount_total,
                                "currency": currency,
                                "request_snapshot": serialized_payload,
                            },
                            signature_verified=None,
                            applied_status=pi.status,
                        ),
                    )

                # Transaction succeeded → respond
                return CheckoutPaymentBootstrapResponse(
                    payment_id=payment_row.id,
                    stripe_payment_intent_id=pi.stripe_payment_intent_id,
                    client_secret=pi.client_secret,
                    status=pi.status,
                    amount_total=amount_total,
                    currency=currency,
                    idempotency_key=idem_key,
                )

            except Exception as db_exc:
                # Best-effort cancellation to avoid dangling/duplicate intents
                try:
                    await stripe_client.try_cancel_stripe_payment_intent_async(
                        stripe_payment_intent_id=pi.stripe_payment_intent_id
                    )
                except Exception:
                    # Already logged inside client helper
                    pass

                logging.exception("[checkout] DB failure during initialize-payment")
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail="Payment was initialized with Stripe but failed to persist. Please retry.",
                ) from db_exc

    # -----------------------
    # Helpers
    # -----------------------

    def _build_pi_description(
        self,
        *,
        n_recipients: int,
        amount_per_share: int,
        currency: str,
        photobook_id: UUID,
    ) -> str:
        # Keep it short and deterministic (useful in dashboards)
        return (
            f"Giftcard x{n_recipients} · {amount_per_share / 100.0:.2f} {currency.upper()} each "
            f"(photobook {str(photobook_id)[:8]})"
        )

    def _fingerprint_share_request(
        self, photobook_id: UUID, req: ShareCreateRequest
    ) -> str:
        """
        Generate a stable, collision-resistant fingerprint of the *semantic* share intent
        that impacts payment amount & recipients. This drives idempotency.
        """
        canonical: dict[str, Any] = {
            "photobook_id": str(photobook_id),
            "sender_display_name": req.sender_display_name,
            "scheduled_for": req.scheduled_for.isoformat()
            if req.scheduled_for
            else None,
            "giftcard": {
                "amount_per_share": (
                    req.giftcard_request.amount_per_share
                    if req.giftcard_request
                    else None
                ),
                "currency": (
                    (req.giftcard_request.currency or "").lower()
                    if req.giftcard_request
                    else None
                ),
                "brand_code": (
                    req.giftcard_request.brand_code if req.giftcard_request else None
                ),
            },
            # Normalize recipients deterministically
            "recipients": [
                {
                    "recipient_user_id": str(r.recipient_user_id)
                    if r.recipient_user_id
                    else None,
                    "recipient_display_name": r.recipient_display_name,
                    "notes": r.notes,
                    "channels": [
                        {
                            "channel_type": c.channel_type.value,
                            "destination": c.destination,
                        }
                        for c in sorted(
                            r.channels,
                            key=lambda x: (
                                str(getattr(x.channel_type, "value", x.channel_type)),
                                x.destination,
                            ),
                        )
                    ],
                }
                for r in sorted(
                    req.recipients,
                    key=lambda rr: (
                        str(rr.recipient_user_id) if rr.recipient_user_id else "",
                        rr.recipient_display_name or "",
                    ),
                )
            ],
        }
        # Compact, deterministic JSON → sha256
        raw = json.dumps(canonical, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    def _derive_idempotency_key(
        self,
        *,
        user_id: UUID,
        photobook_id: UUID,
        amount_total: int,
        currency: str,
        recipients_fingerprint: str,
    ) -> str:
        """
        Stripe idempotency key (<=255 chars). Compose inputs, sha256, prefix for readability.
        """
        src = f"{user_id}|{photobook_id}|{amount_total}|{currency.lower()}|{recipients_fingerprint}"
        digest = hashlib.sha256(src.encode("utf-8")).hexdigest()
        return f"checkout:init:{digest}"
