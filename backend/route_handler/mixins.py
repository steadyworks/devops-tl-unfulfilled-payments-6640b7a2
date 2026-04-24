from uuid import UUID

from fastapi import HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from backend.db.dal import (
    DALNotificationOutbox,
    DALPages,
    DALPayments,
    DALPhotobooks,
    DALShares,
)
from backend.db.data_models import (
    DAONotificationOutbox,
    DAOPages,
    DAOPayments,
    DAOPhotobooks,
    DAOShares,
)
from backend.lib.utils.common import none_throws


class DAORetrieveAssertingOwnershipMixin:
    async def get_page_assert_owned_by(
        self,
        db_session: AsyncSession,
        page_id: UUID,
        user_id: UUID,
    ) -> DAOPages:
        page = await DALPages.get_by_id(db_session, page_id)
        if page is None:
            raise HTTPException(status_code=404, detail="Page not found")

        photobook = await DALPhotobooks.get_by_id(
            db_session, none_throws(page.photobook_id)
        )
        if not photobook or photobook.user_id != user_id:
            raise HTTPException(
                status_code=403, detail="Not authorized to edit this page"
            )

        return page

    async def get_photobook_assert_owned_by(
        self,
        db_session: AsyncSession,
        photobook_id: UUID,
        user_id: UUID,
    ) -> DAOPhotobooks:
        photobook = await DALPhotobooks.get_by_id(db_session, photobook_id)
        if photobook is None:
            raise HTTPException(status_code=404, detail="Photobook not found")
        if photobook.user_id != user_id:
            raise HTTPException(
                status_code=403, detail="Not authorized to edit this page"
            )

        return photobook

    async def get_payment_assert_owned_by(
        self,
        db_session: AsyncSession,
        payment_id: UUID,
        user_id: UUID,
    ) -> DAOPayments:
        payment = await DALPayments.get_by_id(db_session, payment_id)
        if payment is None:
            raise HTTPException(status_code=404, detail="Payment not found")
        if payment.created_by_user_id != user_id:
            raise HTTPException(
                status_code=403, detail="Not authorized to retrieve this payment object"
            )

        return payment

    async def get_share_and_photobook_assert_owned_by(
        self,
        db_session: AsyncSession,
        share_id: UUID,
        user_id: UUID,
    ) -> tuple[DAOShares, DAOPhotobooks]:
        share_dao = await DALShares.get_by_id(db_session, share_id)
        if share_dao is None:
            raise HTTPException(status_code=404, detail="Share not found")

        photobook_dao = await self.get_photobook_assert_owned_by(
            db_session, share_dao.photobook_id, user_id
        )
        return (share_dao, photobook_dao)

    async def get_notification_outbox_row_and_photobook_assert_owned_by(
        self,
        db_session: AsyncSession,
        share_id: UUID,
        user_id: UUID,
    ) -> tuple[DAONotificationOutbox, DAOPhotobooks]:
        outbox_dao = await DALNotificationOutbox.get_by_id(db_session, share_id)
        if outbox_dao is None:
            raise HTTPException(status_code=404, detail="Share not found")

        photobook_dao = await self.get_photobook_assert_owned_by(
            db_session, outbox_dao.photobook_id, user_id
        )
        return (outbox_dao, photobook_dao)
