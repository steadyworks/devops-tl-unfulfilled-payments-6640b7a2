# backend/tests/test_fulfillment_service.py

from __future__ import annotations

import asyncio
from typing import Any, List, Optional, Tuple
from uuid import UUID, uuid4

import pytest
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from backend.db.data_models import (
    DAOGiftcards,
    DAONotificationOutbox,
    DAOPaymentEvents,
    DAOPayments,
    DAOShareChannels,
    DAOShares,
    PaymentEventSource,
    PaymentPurpose,
    PaymentStatus,
    ShareChannelStatus,
    ShareChannelType,
)
from backend.db.data_models.types_ENSURE_BACKWARDS_COMPATIBILITY import (
    GiftcardGrantRequest,
    ShareCreateRequest,
    ShareRecipientSpec,
)
from backend.lib.payments.fulfillment_service import fulfill_payment_success_if_needed
from backend.tests.conftest import email_recipient

# -------------------------
# Local fixtures / helpers
# -------------------------


@pytest.fixture(autouse=True)
async def ensure_giftcards_clean(db_session: AsyncSession) -> None:
    """
    Conftest truncates several tables but not giftcards; add a local cleanup and ensure
    the UNIQUE index on (share_id) exists for ON CONFLICT tests.
    """
    # ensure table exists & index on share_id for ON CONFLICT (harmless if already present)
    await db_session.execute(
        text("""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_indexes
                WHERE schemaname = 'public' AND indexname = 'uq_giftcards_share'
            ) THEN
                CREATE UNIQUE INDEX uq_giftcards_share ON giftcards(share_id);
            END IF;
        END$$;
        """)
    )
    await db_session.execute(text("TRUNCATE giftcards RESTART IDENTITY CASCADE"))
    await db_session.commit()


async def _seed_succeeded_payment_with_snapshot(
    session: AsyncSession,
    *,
    owner_id: UUID,
    photobook_id: UUID,
    recipients: list[ShareRecipientSpec] | None = None,
    amount: int = 500,
    currency: str = "usd",
    brand: str = "amazon",
) -> DAOPayments:
    """
    Create a SUCCEEDED payment with a ShareCreateRequest snapshot.
    """
    req: ShareCreateRequest = ShareCreateRequest(
        recipients=(
            [email_recipient("friend@example.com")]
            if recipients is None
            else [r for r in recipients]
        ),
        sender_display_name="Tester",
        giftcard_request=GiftcardGrantRequest(
            amount_per_share=amount,
            currency=currency,
            brand_code=brand,
        ),
    )

    p = DAOPayments(
        id=uuid4(),
        created_by_user_id=owner_id,
        photobook_id=photobook_id,
        purpose=PaymentPurpose.GIFTCARD,
        amount_total=amount * len(req.recipients),
        currency=currency,
        stripe_payment_intent_id="pi_succeeded_test",
        stripe_customer_id=None,
        stripe_payment_method_id=None,
        stripe_latest_charge_id="ch_123",
        status=PaymentStatus.SUCCEEDED,
        description="test payment",
        receipt_email=None,
        idempotency_key="idem-test",
        failure_code=None,
        failure_message=None,
        refunded_amount=0,
        metadata_json={},
        share_create_request=req.serialize(),
    )
    session.add(p)
    await session.commit()
    return p


# -------------------------
# Tests
# -------------------------


@pytest.mark.asyncio
async def test_FS1_fulfill_creates_shares_channels_outbox_giftcards_and_audit(
    db_session: AsyncSession,
    owner_user: Any,
    photobook: Any,
) -> None:
    payment: DAOPayments = await _seed_succeeded_payment_with_snapshot(
        db_session,
        owner_id=owner_user.id,
        photobook_id=photobook.id,
        amount=700,
        currency="usd",
        brand="amazon",
    )

    share_resp, giftcard_ids, did_fulfill = await fulfill_payment_success_if_needed(
        db_session,
        payment_id=payment.id,
        audit_source=PaymentEventSource.SYSTEM,
        audit_context={"test": "FS1"},
    )

    # service returns sane values
    assert did_fulfill is True
    assert share_resp is not None
    assert len(share_resp.recipients) == 1
    assert len(giftcard_ids) == 1

    # shares
    shares = (await db_session.execute(select(DAOShares))).scalars().all()
    assert len(shares) == 1
    share = shares[0]
    assert share.created_by_payment_id == payment.id

    # channels
    channels = (await db_session.execute(select(DAOShareChannels))).scalars().all()
    assert len(channels) == 1
    ch = channels[0]
    assert ch.photobook_share_id == share.id
    assert ch.channel_type == ShareChannelType.EMAIL
    assert ch.destination == "friend@example.com"

    # outbox
    outboxes = (await db_session.execute(select(DAONotificationOutbox))).scalars().all()
    assert len(outboxes) == 1
    ob = outboxes[0]
    assert ob.share_id == share.id
    assert ob.share_channel_id == ch.id
    # With no schedule -> send_now => PENDING (allow SCHEDULED if timing)
    assert ob.status in (ShareChannelStatus.PENDING, ShareChannelStatus.SCHEDULED)

    # giftcards
    gcs = (await db_session.execute(select(DAOGiftcards))).scalars().all()
    assert len(gcs) == 1
    gc = gcs[0]
    assert gc.share_id == share.id
    assert gc.created_by_payment_id == payment.id
    assert gc.amount_total == 700
    assert gc.currency == "usd"

    # audit event
    events = (await db_session.execute(select(DAOPaymentEvents))).scalars().all()
    assert len(events) == 1
    assert events[0].source == PaymentEventSource.SYSTEM
    assert events[0].event_type == "fulfill.success"
    assert events[0].applied_status is None


@pytest.mark.asyncio
async def test_FS2_idempotent_second_call_no_duplicate_giftcards_or_events(
    db_session: AsyncSession,
    owner_user: Any,
    photobook: Any,
) -> None:
    payment: DAOPayments = await _seed_succeeded_payment_with_snapshot(
        db_session,
        owner_id=owner_user.id,
        photobook_id=photobook.id,
        amount=500,
    )

    # First call fulfills
    _, _, did1 = await fulfill_payment_success_if_needed(
        db_session,
        payment_id=payment.id,
        audit_source=PaymentEventSource.SYSTEM,
        audit_context={"test": "FS2:first"},
    )
    assert did1 is True

    # Second call should be a no-op (giftcards already exist for this payment)
    _, _, did2 = await fulfill_payment_success_if_needed(
        db_session,
        payment_id=payment.id,
        audit_source=PaymentEventSource.SYSTEM,
        audit_context={"test": "FS2:second"},
    )
    assert did2 is False

    # Counts
    shares = (await db_session.execute(select(DAOShares))).scalars().all()
    channels = (await db_session.execute(select(DAOShareChannels))).scalars().all()
    outboxes = (await db_session.execute(select(DAONotificationOutbox))).scalars().all()
    gcs = (await db_session.execute(select(DAOGiftcards))).scalars().all()
    events = (await db_session.execute(select(DAOPaymentEvents))).scalars().all()
    payments = (await db_session.execute(select(DAOPayments))).scalars().all()

    assert len(payments) == 1
    # If you've added fulfilled_at / fulfillment_last_error to DAOPayments, these will assert:
    assert payments[0].fulfilled_at is not None
    assert payments[0].fulfillment_last_error is None

    assert len(shares) == 1
    assert len(channels) == 1
    assert len(outboxes) == 1
    assert len(gcs) == 1
    # only the first fulfill emitted the audit event
    assert len(events) == 1
    assert events[0].event_type == "fulfill.success"


@pytest.mark.asyncio
async def test_FS3_race_two_concurrent_calls_only_one_fulfills(
    pg_engine: AsyncEngine,
    owner_user: Any,
    photobook: Any,
) -> None:
    # Use two independent sessions to simulate concurrency
    Session = async_sessionmaker(pg_engine, class_=AsyncSession, expire_on_commit=False)
    async with Session() as s_seed:
        payment: DAOPayments = await _seed_succeeded_payment_with_snapshot(
            s_seed,
            owner_id=owner_user.id,
            photobook_id=photobook.id,
            amount=900,
        )

    async with Session() as s1, Session() as s2:

        async def _run(
            session: AsyncSession,
        ) -> Tuple[Optional[object], List[UUID], bool]:
            return await fulfill_payment_success_if_needed(
                session,
                payment_id=payment.id,
                audit_source=PaymentEventSource.SYSTEM,
                audit_context={"test": "FS3"},
            )

        (r1, r2) = await asyncio.gather(_run(s1), _run(s2))
        dids = (r1[2], r2[2])
        # exactly one True
        assert dids.count(True) == 1
        assert dids.count(False) == 1

    # Validate global state with a fresh session
    async with Session() as s_check:
        shares = (await s_check.execute(select(DAOShares))).scalars().all()
        channels = (await s_check.execute(select(DAOShareChannels))).scalars().all()
        outboxes = (
            (await s_check.execute(select(DAONotificationOutbox))).scalars().all()
        )
        gcs = (await s_check.execute(select(DAOGiftcards))).scalars().all()
        events = (await s_check.execute(select(DAOPaymentEvents))).scalars().all()
        payments = (await s_check.execute(select(DAOPayments))).scalars().all()

        assert len(shares) == 1
        assert len(channels) == 1
        assert len(outboxes) == 1
        assert len(gcs) == 1
        assert len(events) == 1
        assert events[0].event_type == "fulfill.success"
        assert len(payments) == 1
        assert payments[0].fulfilled_at is not None
        assert payments[0].fulfillment_last_error is None


@pytest.mark.asyncio
async def test_FS4_non_succeeded_payment_is_noop(
    db_session: AsyncSession,
    owner_user: Any,
    photobook: Any,
) -> None:
    # Seed a non-terminal payment
    p = DAOPayments(
        id=uuid4(),
        created_by_user_id=owner_user.id,
        photobook_id=photobook.id,
        purpose=PaymentPurpose.GIFTCARD,
        amount_total=500,
        currency="usd",
        stripe_payment_intent_id="pi_x",
        stripe_latest_charge_id=None,
        status=PaymentStatus.PROCESSING,
        description=None,
        receipt_email=None,
        idempotency_key="idem-x",
        refunded_amount=0,
        metadata_json={},
        share_create_request=None,
    )
    db_session.add(p)
    await db_session.commit()

    share_resp, giftcard_ids, did = await fulfill_payment_success_if_needed(
        db_session,
        payment_id=p.id,
        audit_source=PaymentEventSource.SYSTEM,
    )
    assert did is False
    assert share_resp is None
    assert giftcard_ids == []

    # No writes occurred
    assert len((await db_session.execute(select(DAOShares))).scalars().all()) == 0
    assert (
        len((await db_session.execute(select(DAOShareChannels))).scalars().all()) == 0
    )
    assert (
        len((await db_session.execute(select(DAONotificationOutbox))).scalars().all())
        == 0
    )
    assert len((await db_session.execute(select(DAOGiftcards))).scalars().all()) == 0
    assert (
        len((await db_session.execute(select(DAOPaymentEvents))).scalars().all()) == 0
    )


@pytest.mark.asyncio
async def test_FS5_missing_snapshot_is_noop_even_if_succeeded(
    db_session: AsyncSession,
    owner_user: Any,
    photobook: Any,
) -> None:
    # SUCCEEDED but no snapshot to fulfill from
    p = DAOPayments(
        id=uuid4(),
        created_by_user_id=owner_user.id,
        photobook_id=photobook.id,
        purpose=PaymentPurpose.GIFTCARD,
        amount_total=500,
        currency="usd",
        stripe_payment_intent_id="pi_y",
        stripe_latest_charge_id=None,
        status=PaymentStatus.SUCCEEDED,
        description=None,
        receipt_email=None,
        idempotency_key="idem-y",
        refunded_amount=0,
        metadata_json={},
        share_create_request=None,
    )
    db_session.add(p)
    await db_session.commit()

    share_resp, giftcard_ids, did = await fulfill_payment_success_if_needed(
        db_session,
        payment_id=p.id,
        audit_source=PaymentEventSource.SYSTEM,
    )
    assert did is False
    assert share_resp is None
    assert giftcard_ids == []

    # No side effects
    assert len((await db_session.execute(select(DAOShares))).scalars().all()) == 0
    assert len((await db_session.execute(select(DAOGiftcards))).scalars().all()) == 0
    assert (
        len((await db_session.execute(select(DAOPaymentEvents))).scalars().all()) == 0
    )


@pytest.mark.asyncio
async def test_FS6_money_fields_not_overwritten_on_reentry(
    db_session: AsyncSession,
    owner_user: Any,
    photobook: Any,
) -> None:
    # First snapshot: $5.00
    p: DAOPayments = await _seed_succeeded_payment_with_snapshot(
        db_session,
        owner_id=owner_user.id,
        photobook_id=photobook.id,
        amount=500,
        currency="usd",
    )

    # First fulfillment
    _, _, did1 = await fulfill_payment_success_if_needed(
        db_session,
        payment_id=p.id,
        audit_source=PaymentEventSource.SYSTEM,
        audit_context={"test": "FS6:1"},
    )
    assert did1 is True

    # Mutate the stored snapshot to a different amount (what-if reentry)
    req2: ShareCreateRequest = ShareCreateRequest(
        recipients=[email_recipient("friend@example.com")],
        giftcard_request=GiftcardGrantRequest(
            amount_per_share=9999, currency="usd", brand_code="amazon"
        ),
    )
    p.share_create_request = req2.serialize()
    await db_session.commit()

    # Second fulfillment attempt (should NO-OP due to giftcards already created for this payment)
    _, _, did2 = await fulfill_payment_success_if_needed(
        db_session,
        payment_id=p.id,
        audit_source=PaymentEventSource.SYSTEM,
        audit_context={"test": "FS6:2"},
    )
    assert did2 is False

    # Verify giftcard amount did not change
    gcs = (await db_session.execute(select(DAOGiftcards))).scalars().all()
    assert len(gcs) == 1
    assert gcs[0].amount_total == 500  # untouched


@pytest.mark.asyncio
async def test_FM1_no_giftcard_request_creates_no_giftcards(
    db_session: AsyncSession, owner_user: Any, photobook: Any
) -> None:
    # Build a SUCCEEDED payment but snapshot has NO giftcard_request
    req: ShareCreateRequest = ShareCreateRequest(
        recipients=[email_recipient("ngc@example.com")],
        sender_display_name="Tester",
        giftcard_request=None,
    )
    p = DAOPayments(
        id=uuid4(),
        created_by_user_id=owner_user.id,
        photobook_id=photobook.id,
        purpose=PaymentPurpose.GIFTCARD,
        amount_total=500,
        currency="usd",
        stripe_payment_intent_id="pi_ok",
        status=PaymentStatus.SUCCEEDED,
        metadata_json={},
        share_create_request=req.serialize(),
    )
    db_session.add(p)
    await db_session.commit()

    share_resp, giftcard_ids, did = await fulfill_payment_success_if_needed(
        db_session, payment_id=p.id, audit_source=PaymentEventSource.SYSTEM
    )
    assert did is True
    assert share_resp is not None
    assert giftcard_ids == []

    # DB: shares/channels/outbox present; no giftcards
    assert len((await db_session.execute(select(DAOShares))).scalars().all()) == 1
    assert (
        len((await db_session.execute(select(DAOShareChannels))).scalars().all()) == 1
    )
    assert (
        len((await db_session.execute(select(DAONotificationOutbox))).scalars().all())
        == 1
    )
    assert len((await db_session.execute(select(DAOGiftcards))).scalars().all()) == 0


@pytest.mark.asyncio
async def test_FM2_multiple_recipients_create_multiple_giftcards_and_idempotent(
    db_session: AsyncSession, owner_user: Any, photobook: Any
) -> None:
    req: ShareCreateRequest = ShareCreateRequest(
        recipients=[
            email_recipient("a@example.com"),
            email_recipient("b@example.com"),
            email_recipient("c@example.com"),
        ],
        sender_display_name="Tester",
        # giftcard_request lives inside your compat wrapper; seeded via helper in earlier tests,
        # but here we rely on fulfill() using the snapshot to grant per-share.
    )

    req.giftcard_request = GiftcardGrantRequest(
        amount_per_share=500, currency="usd", brand_code="amazon"
    )
    p = DAOPayments(
        id=uuid4(),
        created_by_user_id=owner_user.id,
        photobook_id=photobook.id,
        purpose=PaymentPurpose.GIFTCARD,
        amount_total=1500,
        currency="usd",
        stripe_payment_intent_id="pi_ok_multi",
        status=PaymentStatus.SUCCEEDED,
        metadata_json={},
        share_create_request=req.serialize(),
    )
    db_session.add(p)
    await db_session.commit()

    _, giftcards_1, did1 = await fulfill_payment_success_if_needed(
        db_session, payment_id=p.id, audit_source=PaymentEventSource.SYSTEM
    )
    assert did1 is True
    assert len(giftcards_1) == 3

    # Re-run → idempotent (no new giftcards)
    _, giftcards_2, did2 = await fulfill_payment_success_if_needed(
        db_session, payment_id=p.id, audit_source=PaymentEventSource.SYSTEM
    )
    assert did2 is False
    assert giftcards_2 == []

    gcs = (await db_session.execute(select(DAOGiftcards))).scalars().all()
    assert len(gcs) == 3
