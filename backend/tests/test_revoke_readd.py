from __future__ import annotations

from uuid import UUID, uuid4

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from backend.db.dal import safe_transaction
from backend.db.data_models import (
    BackgroundType,
    DAONotificationOutbox,
    DAOPhotobooks,
    DAOShareChannels,
    DAOShares,
    DAOUsers,
    ShareAccessPolicy,
    ShareChannelStatus,
    ShareChannelType,
)
from backend.db.data_models.types_ENSURE_BACKWARDS_COMPATIBILITY import (
    ShareChannelSpec,
    ShareCreateRequest,
    ShareRecipientSpec,
)
from backend.lib.sharing.service import (
    initialize_shares_and_channels,
    revoke_share,
)

from .conftest import async_fixture

# -------------------------
# Local helpers
# -------------------------


def _recipient_with(
    email: str | None = None,
    sms: str | None = None,
    *,
    recipient_display_name: str = "Friend",
    notes: str | None = None,
) -> ShareRecipientSpec:
    channels: list[ShareChannelSpec] = []
    if email:
        channels.append(
            ShareChannelSpec(
                channel_type=ShareChannelType.EMAIL,
                destination=email,
            )
        )
    if sms:
        channels.append(
            ShareChannelSpec(
                channel_type=ShareChannelType.SMS,
                destination=sms,
            )
        )
    return ShareRecipientSpec(
        channels=channels,
        recipient_display_name=recipient_display_name,
        notes=notes,
    )


async def _list_outbox_for_share(
    session: AsyncSession, share_id: UUID
) -> list[DAONotificationOutbox]:
    rows = (
        (
            await session.execute(
                select(DAONotificationOutbox)
                .where(getattr(DAONotificationOutbox, "share_id") == share_id)
                .order_by(getattr(DAONotificationOutbox, "created_at").asc())
            )
        )
        .scalars()
        .all()
    )
    return list(rows)


async def _get_share(session: AsyncSession, share_id: UUID) -> DAOShares:
    row = (
        await session.execute(
            select(DAOShares).where(getattr(DAOShares, "id") == share_id)
        )
    ).scalar_one()
    return row


async def _get_share_channel(
    session: AsyncSession,
    photobook_id: UUID,
    channel_type: ShareChannelType,
    destination: str,
) -> DAOShareChannels | None:
    row = (
        await session.execute(
            select(DAOShareChannels).where(
                getattr(DAOShareChannels, "photobook_id") == photobook_id,
                getattr(DAOShareChannels, "channel_type") == channel_type,
                getattr(DAOShareChannels, "destination") == destination,
            )
        )
    ).scalar_one_or_none()
    return row


# -------------------------
# Fixtures (uuid4 to avoid PK reuse)
# -------------------------


@async_fixture
async def owner_user(db_session: AsyncSession) -> DAOUsers:
    user = DAOUsers(id=uuid4(), email="owner@example.com", name="Owner")
    db_session.add(user)
    await db_session.commit()
    return user


@async_fixture
async def photobook(db_session: AsyncSession, owner_user: DAOUsers) -> DAOPhotobooks:
    pb = DAOPhotobooks(
        id=uuid4(),
        title="PB",
        user_id=owner_user.id,
        status=None,
        status_last_edited_by=None,
        background=BackgroundType.COLOR,
    )
    db_session.add(pb)
    await db_session.commit()
    return pb


# -------------------------
# R6. Revoke → add back ALL original channels
# -------------------------


@pytest.mark.asyncio
async def test_R6_revoke_then_add_back_all_channels_updates_metadata(
    db_session: AsyncSession, owner_user: DAOUsers, photobook: DAOPhotobooks
) -> None:
    email = "friend@example.com"
    phone = "+15555550101"

    # 1) Create initial share with EMAIL + SMS
    async with safe_transaction(db_session):
        resp1 = await initialize_shares_and_channels(
            session=db_session,
            user_id=owner_user.id,
            photobook_id=photobook.id,
            req=ShareCreateRequest(
                recipients=[
                    _recipient_with(
                        email=email, sms=phone, recipient_display_name="A", notes="n1"
                    )
                ],
                sender_display_name="Owner V1",
                scheduled_for=None,
            ),
        )
        share_id = resp1.recipients[0].share_id

    # Sanity: two outbox rows (pending)
    async with safe_transaction(db_session):
        all_ob1 = await _list_outbox_for_share(db_session, share_id)
        assert len(all_ob1) == 2
        assert {o.status for o in all_ob1} == {ShareChannelStatus.PENDING}

    async with safe_transaction(db_session):
        # 2) Revoke the share
        r = await revoke_share(
            session=db_session,
            actor_user_id=owner_user.id,
            share_id=share_id,
            photobook_id=photobook.id,
            reason="stop",
        )
        assert r.canceled_outbox_count == 2

    async with safe_transaction(db_session):
        # Confirm outboxes are canceled
        all_ob_after_revoke = await _list_outbox_for_share(db_session, share_id)
        assert {o.status for o in all_ob_after_revoke} == {ShareChannelStatus.CANCELED}

    # 3) Add back *all* original channels with updated metadata
    async with safe_transaction(db_session):
        resp2 = await initialize_shares_and_channels(
            session=db_session,
            user_id=owner_user.id,
            photobook_id=photobook.id,
            req=ShareCreateRequest(
                recipients=[
                    _recipient_with(
                        email=email, sms=phone, recipient_display_name="B", notes="n2"
                    )
                ],
                sender_display_name="Owner V2",
                scheduled_for=None,
            ),
        )
    # Should reuse same share_id (based on destination probe)
    assert resp2.recipients[0].share_id == share_id

    # Share metadata updated AND auto-unrevoked
    share_after = await _get_share(db_session, share_id)
    assert share_after.sender_display_name == "Owner V2"
    assert share_after.recipient_display_name == "B"
    assert share_after.notes == "n2"
    assert share_after.access_policy == ShareAccessPolicy.ANYONE_WITH_LINK
    assert getattr(share_after, "revoked_at") is None
    assert getattr(share_after, "revoked_by_user_id") is None
    assert getattr(share_after, "revoked_reason") is None

    # Outbox: two more rows created, these should be PENDING
    all_ob2 = await _list_outbox_for_share(db_session, share_id)
    assert len(all_ob2) == 4
    latest_statuses = {o.status for o in all_ob2[-2:]}
    assert latest_statuses == {ShareChannelStatus.PENDING}

    # Channels still 1 per (type,destination), same IDs (upsert on conflict)
    ch_email = await _get_share_channel(
        db_session, photobook.id, ShareChannelType.EMAIL, email
    )
    ch_sms = await _get_share_channel(
        db_session, photobook.id, ShareChannelType.SMS, phone
    )
    assert ch_email is not None and ch_sms is not None
    # And they still point to this share
    assert ch_email.photobook_share_id == share_id
    assert ch_sms.photobook_share_id == share_id


# -------------------------
# R7. Revoke → add back SUBSET of original channels
# -------------------------


@pytest.mark.asyncio
async def test_R7_revoke_then_add_back_subset_of_channels(
    db_session: AsyncSession, owner_user: DAOUsers, photobook: DAOPhotobooks
) -> None:
    email = "friend@example.com"
    phone = "+15555550101"

    # 1) Create initial share with EMAIL + SMS
    async with safe_transaction(db_session):
        resp1 = await initialize_shares_and_channels(
            session=db_session,
            user_id=owner_user.id,
            photobook_id=photobook.id,
            req=ShareCreateRequest(
                recipients=[
                    _recipient_with(
                        email=email, sms=phone, recipient_display_name="A", notes="n1"
                    )
                ],
                sender_display_name="Owner V1",
                scheduled_for=None,
            ),
        )
        share_id = resp1.recipients[0].share_id

    # 2) Revoke
    async with safe_transaction(db_session):
        await revoke_share(
            session=db_session,
            actor_user_id=owner_user.id,
            share_id=share_id,
            photobook_id=photobook.id,
            reason="stop",
        )

    # 3) Add back only EMAIL with updated metadata
    async with safe_transaction(db_session):
        resp2 = await initialize_shares_and_channels(
            session=db_session,
            user_id=owner_user.id,
            photobook_id=photobook.id,
            req=ShareCreateRequest(
                recipients=[
                    _recipient_with(
                        email=email, sms=None, recipient_display_name="C", notes="n3"
                    )
                ],
                sender_display_name="Owner V3",
                scheduled_for=None,
            ),
        )
        assert resp2.recipients[0].share_id == share_id

    # Share metadata updated AND auto-unrevoked
    share_after = await _get_share(db_session, share_id)
    assert share_after.sender_display_name == "Owner V3"
    assert share_after.recipient_display_name == "C"
    assert share_after.notes == "n3"
    assert share_after.access_policy == ShareAccessPolicy.ANYONE_WITH_LINK
    assert getattr(share_after, "revoked_at") is None
    assert getattr(share_after, "revoked_by_user_id") is None
    assert getattr(share_after, "revoked_reason") is None

    # Outbox: previously had 2 (canceled); now exactly 1 new (email) should be pending
    all_ob = await _list_outbox_for_share(db_session, share_id)
    pending = [o for o in all_ob if o.status == ShareChannelStatus.PENDING]
    canceled = [o for o in all_ob if o.status == ShareChannelStatus.CANCELED]
    assert len(canceled) == 2
    assert len(pending) == 1

    # Email channel still present and tied to this share; SMS channel also still exists
    ch_email = await _get_share_channel(
        db_session, photobook.id, ShareChannelType.EMAIL, email
    )
    ch_sms = await _get_share_channel(
        db_session, photobook.id, ShareChannelType.SMS, phone
    )
    assert ch_email is not None and ch_email.photobook_share_id == share_id
    assert ch_sms is not None and ch_sms.photobook_share_id == share_id


@pytest.mark.asyncio
async def test_R8_revoke_then_reinit_by_user_conflict_unrevokes_without_channels(
    db_session: AsyncSession, owner_user: DAOUsers, photobook: DAOPhotobooks
) -> None:
    target_user_id = uuid4()

    # 1) Create a share bound to recipient_user_id (no channels needed but include one to differ later)
    async with safe_transaction(db_session):
        resp1 = await initialize_shares_and_channels(
            session=db_session,
            user_id=owner_user.id,
            photobook_id=photobook.id,
            req=ShareCreateRequest(
                recipients=[
                    ShareRecipientSpec(
                        recipient_user_id=target_user_id,
                        recipient_display_name="First",
                        channels=[],  # no channels initially is fine too
                    )
                ],
                sender_display_name="Owner V1",
                scheduled_for=None,
            ),
        )
        share_id = resp1.recipients[0].share_id

    # Revoke that share
    async with safe_transaction(db_session):
        await revoke_share(
            session=db_session,
            actor_user_id=owner_user.id,
            share_id=share_id,
            photobook_id=photobook.id,
            reason="stop",
        )

    # 2) Re-init using the same recipient_user_id, *still with no channels* so we hit the conflict upsert path
    async with safe_transaction(db_session):
        resp2 = await initialize_shares_and_channels(
            session=db_session,
            user_id=owner_user.id,
            photobook_id=photobook.id,
            req=ShareCreateRequest(
                recipients=[
                    ShareRecipientSpec(
                        recipient_user_id=target_user_id,
                        recipient_display_name="Second",
                        channels=[],  # ensures we do not probe by channel
                    )
                ],
                sender_display_name="Owner V2",
                scheduled_for=None,
            ),
        )

    # Same share reused and un-revoked
    assert resp2.recipients[0].share_id == share_id
    share_after = await _get_share(db_session, share_id)
    assert share_after.access_policy == ShareAccessPolicy.ANYONE_WITH_LINK
    assert share_after.recipient_display_name == "Second"
    assert share_after.sender_display_name == "Owner V2"
    assert getattr(share_after, "revoked_at") is None
    assert getattr(share_after, "revoked_by_user_id") is None
    assert getattr(share_after, "revoked_reason") is None
