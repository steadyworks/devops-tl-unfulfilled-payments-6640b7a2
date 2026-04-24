# backend/lib/notifs/scheduling_service.py

from datetime import datetime, timezone
from uuid import UUID

from sqlalchemy import and_, exists, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from backend.db.data_models import (
    DAONotificationOutbox,
    DAOShares,
    ShareAccessPolicy,
    ShareChannelStatus,
)
from backend.lib.notifs.scheduling_schemas import RescheduleResponse
from backend.lib.utils.common import utcnow


def _now() -> datetime:
    return utcnow()


async def reschedule_outbox(
    *,
    session: AsyncSession,
    outbox_id: UUID,
    user_id: UUID,
    new_scheduled_for: datetime,
) -> RescheduleResponse:
    """
    Atomically reschedule an outbox row.

    Rules:
      - Allowed only when status in (PENDING, SCHEDULED) AND not claimed (dispatch_claimed_at IS NULL).
      - Not allowed if SENT or CANCELED.
      - Not allowed if the share has been REVOKED.
      - If new_scheduled_for <= now, sets status=PENDING and clears schedule (or sets to now) to make it due.
      - Clears any prior claim/lease fields. Sets scheduled_by_user_id/last_scheduled_at audit fields.
    """
    if not session.in_transaction():
        raise RuntimeError(
            "[reschedule_outbox] Must be called within an active transaction on the session."
        )
    now_ts = _now()

    # Normalize tz: force aware UTC to be consistent
    if new_scheduled_for.tzinfo is None:
        new_scheduled_for = new_scheduled_for.replace(tzinfo=timezone.utc)
    new_dt = new_scheduled_for.astimezone(timezone.utc)

    send_asap = new_dt <= now_ts
    new_status = (
        ShareChannelStatus.PENDING if send_asap else ShareChannelStatus.SCHEDULED
    )
    new_sched_value = now_ts if send_asap else new_dt

    o = DAONotificationOutbox
    s = DAOShares

    o_id = getattr(o, "id")
    o_status = getattr(o, "status")
    o_scheduled_for = getattr(o, "scheduled_for")
    o_dispatch_claimed_at = getattr(o, "dispatch_claimed_at")
    o_dispatch_token = getattr(o, "dispatch_token")
    o_dispatch_lease_expires_at = getattr(o, "dispatch_lease_expires_at")
    o_dispatch_worker_id = getattr(o, "dispatch_worker_id")
    o_updated_at = getattr(o, "updated_at")
    o_share_id = getattr(o, "share_id")
    o_scheduled_by_user_id = getattr(o, "scheduled_by_user_id")
    o_last_scheduled_at = getattr(o, "last_scheduled_at")

    s_id = getattr(s, "id")
    s_access_policy = getattr(s, "access_policy")

    revoked_exists = exists().where(
        and_(s_id == o_share_id, s_access_policy == ShareAccessPolicy.REVOKED)
    )

    # Try atomic reschedule with concurrency guards
    res_stmt = (
        update(o)
        .where(
            and_(
                o_id == outbox_id,
                # Not terminal and not in-flight
                o_status.in_(
                    [ShareChannelStatus.PENDING, ShareChannelStatus.SCHEDULED]
                ),
                o_dispatch_claimed_at.is_(None),
                ~revoked_exists,
            )
        )
        .values(
            **{
                o_status.key: new_status,
                o_scheduled_for.key: new_sched_value,
                # clear any previous claims
                o_dispatch_token.key: None,
                o_dispatch_claimed_at.key: None,
                o_dispatch_lease_expires_at.key: None,
                o_dispatch_worker_id.key: None,
                # audit
                o_scheduled_by_user_id.key: user_id,
                o_last_scheduled_at.key: now_ts,
                o_updated_at.key: now_ts,
            }
        )
        .returning(o_id, o_status, o_scheduled_for)
    )
    result = await session.execute(res_stmt)
    row = result.first()

    if not row:
        # Determine why, to return an actionable error upstream (route can map to 409)
        # Fetch current state
        current = await session.execute(
            select(o_status, o_dispatch_claimed_at, o_scheduled_for).where(
                o_id == outbox_id
            )
        )
        cur = current.first()
        # If row missing entirely (deleted), cur might be None.
        status_now = cur[0] if cur else None
        claimed_at_now = cur[1] if cur else None
        if status_now in (ShareChannelStatus.SENT, ShareChannelStatus.CANCELED):
            raise RuntimeError(
                "Cannot reschedule: notification already terminal (sent or canceled)."
            )
        if status_now == ShareChannelStatus.SENDING or claimed_at_now is not None:
            raise RuntimeError(
                "Cannot reschedule: notification currently in-flight (claimed by a worker)."
            )
        # Possibly revoked or not found
        raise RuntimeError("Cannot reschedule: share revoked or outbox not found.")

    return RescheduleResponse(outbox_id=row[0], status=row[1], scheduled_for=row[2])
