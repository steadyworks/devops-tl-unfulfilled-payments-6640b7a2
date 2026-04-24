# tests/test_lease_reclaim.py

from datetime import timedelta
from uuid import UUID, uuid4

import pytest
from sqlalchemy import (
    select,
    update,
)
from sqlalchemy.ext.asyncio import (
    AsyncSession,
)

from backend.db.dal import safe_transaction
from backend.db.data_models import (
    DAONotificationOutbox,
    DAOPhotobooks,
    DAOShares,
    DAOUsers,
    ShareChannelStatus,
)
from backend.db.data_models.types_ENSURE_BACKWARDS_COMPATIBILITY import (
    ShareCreateRequest,
)
from backend.lib.notifs.dispatch_service import (
    reclaim_and_enqueue_expired_leases,
)
from backend.lib.sharing.service import initialize_shares_and_channels
from backend.lib.utils.common import utcnow

from .conftest import (
    FakeJobManagerBatch,
    email_recipient,
)

# -------------------------
# D. Lease reclaimer
# -------------------------


@pytest.mark.asyncio
async def test_D1_reclaim_expired_leases_enqueues_and_refreshes_token_and_times(
    db_session: AsyncSession, owner_user: DAOUsers, photobook: DAOPhotobooks
) -> None:
    # Seed due-now
    async with safe_transaction(db_session):
        resp = await initialize_shares_and_channels(
            session=db_session,
            user_id=owner_user.id,
            photobook_id=photobook.id,
            req=ShareCreateRequest(
                recipients=[email_recipient("expired1@example.com")],
                sender_display_name="Owner",
                scheduled_for=None,
            ),
        )
    outbox_id = resp.recipients[0].outbox_results[0].outbox_id

    # Pretend it was claimed long ago and expired
    old_token = uuid4()
    old_claim_time = utcnow() - timedelta(hours=2)
    old_expiry = utcnow() - timedelta(hours=1)
    await db_session.execute(
        update(DAONotificationOutbox)
        .where(getattr(DAONotificationOutbox, "id") == outbox_id)
        .values(
            status=ShareChannelStatus.SENDING,
            dispatch_token=old_token,
            dispatch_worker_id="stale-worker",
            dispatch_claimed_at=old_claim_time,
            dispatch_lease_expires_at=old_expiry,
        )
    )
    await db_session.commit()

    jm = FakeJobManagerBatch()
    job_ids = await reclaim_and_enqueue_expired_leases(
        session=db_session,
        job_manager=jm,
        user_id=owner_user.id,
        worker_id="reclaimer",
        limit=10,
        lease_seconds=600,
    )
    assert len(job_ids) == 1
    outbox = (
        await db_session.execute(
            select(DAONotificationOutbox).where(
                getattr(DAONotificationOutbox, "id") == outbox_id
            )
        )
    ).scalar_one()
    assert outbox.status == ShareChannelStatus.SENDING
    assert outbox.dispatch_worker_id == "reclaimer"
    assert outbox.dispatch_claimed_at is not None
    assert outbox.dispatch_lease_expires_at is not None
    assert outbox.dispatch_token is not None and outbox.dispatch_token != old_token


@pytest.mark.asyncio
async def test_D2_reclaimer_respects_limit_and_skips_revoked(
    db_session: AsyncSession, owner_user: DAOUsers, photobook: DAOPhotobooks
) -> None:
    # Seed 3 rows; all expired
    rows: list[UUID] = []
    async with safe_transaction(db_session):
        for email in ["exp_a@example.com", "exp_b@example.com", "exp_c@example.com"]:
            resp = await initialize_shares_and_channels(
                session=db_session,
                user_id=owner_user.id,
                photobook_id=photobook.id,
                req=ShareCreateRequest(
                    recipients=[email_recipient(email)],
                    sender_display_name="Owner",
                    scheduled_for=None,
                ),
            )
            rows.append(resp.recipients[0].outbox_results[0].outbox_id)

    # Mark expired + SENDING
    for oid in rows:
        await db_session.execute(
            update(DAONotificationOutbox)
            .where(getattr(DAONotificationOutbox, "id") == oid)
            .values(
                status=ShareChannelStatus.SENDING,
                dispatch_token=uuid4(),
                dispatch_worker_id="old",
                dispatch_claimed_at=utcnow() - timedelta(hours=2),
                dispatch_lease_expires_at=utcnow() - timedelta(hours=1),
            )
        )
    await db_session.commit()

    # Revoke the share of the middle one
    mid = rows[1]
    mid_row = (
        await db_session.execute(
            select(DAONotificationOutbox).where(
                getattr(DAONotificationOutbox, "id") == mid
            )
        )
    ).scalar_one()
    await db_session.execute(
        update(DAOShares)
        .where(getattr(DAOShares, "id") == mid_row.share_id)
        .values(access_policy="revoked")
    )
    await db_session.commit()

    jm = FakeJobManagerBatch()
    job_ids = await reclaim_and_enqueue_expired_leases(
        session=db_session,
        job_manager=jm,
        user_id=owner_user.id,
        worker_id="reclaimer",
        limit=2,  # should only reclaim 2 (but one is revoked -> result should be 2 because 3rd is available)
        lease_seconds=600,
    )
    assert len(job_ids) == 2  # 2 reclaimed, revoked one skipped


@pytest.mark.asyncio
async def test_D3_reclaimer_enqueue_failure_releases_claim(
    db_session: AsyncSession, owner_user: DAOUsers, photobook: DAOPhotobooks
) -> None:
    # Seed 1 expired
    async with safe_transaction(db_session):
        resp = await initialize_shares_and_channels(
            session=db_session,
            user_id=owner_user.id,
            photobook_id=photobook.id,
            req=ShareCreateRequest(
                recipients=[email_recipient("exp_fail@example.com")],
                sender_display_name="Owner",
                scheduled_for=None,
            ),
        )
    oid = resp.recipients[0].outbox_results[0].outbox_id
    await db_session.execute(
        update(DAONotificationOutbox)
        .where(getattr(DAONotificationOutbox, "id") == oid)
        .values(
            status=ShareChannelStatus.SENDING,
            dispatch_token=uuid4(),
            dispatch_worker_id="old",
            dispatch_claimed_at=utcnow() - timedelta(hours=2),
            dispatch_lease_expires_at=utcnow() - timedelta(hours=1),
        )
    )
    await db_session.commit()

    jm = FakeJobManagerBatch(fail_on_index={1})
    job_ids = await reclaim_and_enqueue_expired_leases(
        session=db_session,
        job_manager=jm,
        user_id=owner_user.id,
        worker_id="reclaimer",
        limit=10,
        lease_seconds=600,
    )
    assert job_ids == []  # single one failed

    # Claim should be released back to PENDING
    outbox = (
        await db_session.execute(
            select(DAONotificationOutbox).where(
                getattr(DAONotificationOutbox, "id") == oid
            )
        )
    ).scalar_one()
    assert outbox.status == ShareChannelStatus.PENDING
    assert outbox.dispatch_token is None
    assert outbox.dispatch_claimed_at is None
