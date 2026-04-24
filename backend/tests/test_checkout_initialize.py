# backend/tests/test_checkout_initialize.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional
from uuid import UUID, uuid4

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from starlette.requests import Request

from backend.db.data_models import (
    DAOPaymentEvents,
    DAOPayments,
    PaymentEventSource,
    PaymentStatus,
    ShareChannelType,
)
from backend.db.data_models.types_ENSURE_BACKWARDS_COMPATIBILITY import (
    GiftcardGrantRequest,
    ShareChannelSpec,
    ShareCreateRequest,
    ShareRecipientSpec,
)
from backend.lib.payments.stripe.base import StripeCreatePaymentIntentResult
from backend.route_handler.checkout import (
    CheckoutAPIHandler,
    CheckoutPaymentBootstrapResponse,
)

# -------------------------
# Minimal Request builders
# -------------------------


def _make_post_request(
    path: str,
    *,
    headers: dict[str, str] | None = None,
) -> Request:
    """
    Construct a minimal Starlette Request for POST endpoints.
    The handler is invoked directly with the typed payload, so we do not
    need to provide a body here.
    """
    scope: dict[str, Any] = {
        "type": "http",
        "method": "POST",
        "path": path,
        "headers": [
            (k.encode("utf-8"), v.encode("utf-8"))
            for k, v in (headers or {"content-type": "application/json"}).items()
        ],
        "query_string": b"",
        "server": ("testserver", 80),
        "scheme": "http",
        "client": ("127.0.0.1", 12345),
    }

    async def _receive() -> dict[str, Any]:
        return {"type": "http.request", "body": b"", "more_body": False}

    return Request(scope, _receive)


# -------------------------
# App stub with new_db_session()
# -------------------------


class _AsyncSessionCtx:
    def __init__(self, s: AsyncSession) -> None:
        self._s = s

    async def __aenter__(self) -> AsyncSession:
        return self._s

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: Any | None,
    ) -> None:
        return None


class _AppStubCheckout:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    def new_db_session(self) -> _AsyncSessionCtx:
        return _AsyncSessionCtx(self._session)


# -------------------------
# Request context + Stripe fakes
# -------------------------


@dataclass
class _RequestContext:
    user_id: UUID


class _StripeClientFakeCheckout:
    """
    Async Stripe client stub capturing inputs and allowing custom behavior.
    """

    def __init__(self) -> None:
        self.create_calls: list[dict[str, Any]] = []
        self.cancel_calls: list[str] = []

        # Configurable response for create
        self._create_result: Optional[StripeCreatePaymentIntentResult] = (
            StripeCreatePaymentIntentResult(
                stripe_payment_intent_id="pi_test_123",
                client_secret="cs_test_abc",
                latest_charge_id="ch_test_001",
                status=PaymentStatus.REQUIRES_PAYMENT_METHOD,
            )
        )

        # Optional hook to raise during create
        self._should_raise_on_create: bool = False

    def set_create_result(self, res: StripeCreatePaymentIntentResult) -> None:
        self._create_result = res

    def set_raise_on_create(self, should_raise: bool) -> None:
        self._should_raise_on_create = should_raise

    async def create_stripe_payment_intent_async(
        self,
        *,
        amount: int,
        currency: str,
        description: Optional[str],
        idempotency_key: str,
        metadata: dict[str, str],
    ) -> StripeCreatePaymentIntentResult:
        self.create_calls.append(
            {
                "amount": amount,
                "currency": currency,
                "description": description,
                "idempotency_key": idempotency_key,
                "metadata": metadata,
            }
        )
        if self._should_raise_on_create:
            raise RuntimeError("stripe create failure (test)")
        assert self._create_result is not None
        return self._create_result

    async def try_cancel_stripe_payment_intent_async(
        self, *, stripe_payment_intent_id: str
    ) -> None:
        self.cancel_calls.append(stripe_payment_intent_id)


# -------------------------
# Handler fixture
# -------------------------


@pytest.fixture
def checkout_handler(db_session: AsyncSession) -> CheckoutAPIHandler:
    h = CheckoutAPIHandler(app=_AppStubCheckout(db_session))  # type: ignore[arg-type]
    try:
        h.register_routes()
    except Exception:
        # These handlers are called directly in tests; route table creation is not required.
        pass
    return h


# -------------------------
# Happy path
# -------------------------


@pytest.mark.asyncio
async def test_CI1_success_creates_pi_and_persists_payment_and_event(
    checkout_handler: CheckoutAPIHandler,
    db_session: AsyncSession,
    owner_user: Any,
    photobook: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stripe_fake = _StripeClientFakeCheckout()
    stripe_fake.set_create_result(
        StripeCreatePaymentIntentResult(
            stripe_payment_intent_id="pi_ok_1",
            client_secret="cs_ok_1",
            latest_charge_id="ch_ok_1",
            status=PaymentStatus.REQUIRES_PAYMENT_METHOD,
        )
    )

    # Stubs: auth, ownership check, stripe client resolver
    async def _get_rcx(_req: Request) -> _RequestContext:
        return _RequestContext(user_id=owner_user.id)

    async def _assert_owned(session: AsyncSession, pb_id: UUID, user_id: UUID) -> None:
        assert pb_id == photobook.id
        assert user_id == owner_user.id
        return None

    checkout_handler.get_request_context = _get_rcx  # type: ignore[assignment]
    checkout_handler.get_photobook_assert_owned_by = _assert_owned  # type: ignore[assignment]
    checkout_handler.get_stripe_client_for_request = lambda _req: stripe_fake  # type: ignore[method-assign, assignment, return-value]

    payload: ShareCreateRequest = ShareCreateRequest(
        recipients=[
            ShareRecipientSpec(
                recipient_display_name="Friend",
                channels=[
                    ShareChannelSpec(
                        channel_type=ShareChannelType.EMAIL,
                        destination="friend@example.com",
                    )
                ],
            )
        ],
        sender_display_name="Me",
        giftcard_request=GiftcardGrantRequest(
            amount_per_share=500, currency="USD", brand_code="amazon"
        ),
    )

    req: Request = _make_post_request(
        f"/api/checkout/{photobook.id}/initialize-payment",
        headers={"X-Debug-Use-Sandbox": "true"},
    )

    resp: CheckoutPaymentBootstrapResponse = (
        await checkout_handler.checkout_initializing_payment(
            photobook_id=photobook.id,
            payload=payload,
            request=req,
        )
    )
    # Response assertions
    assert resp.stripe_payment_intent_id == "pi_ok_1"
    assert resp.client_secret == "cs_ok_1"
    assert resp.status == PaymentStatus.REQUIRES_PAYMENT_METHOD
    assert resp.amount_total == 500  # 1 recipient * 500
    assert resp.currency == "usd"  # lowercased by handler
    assert len(resp.idempotency_key) > 16

    # Stripe was called once with the derived idempotency key
    assert len(stripe_fake.create_calls) == 1
    assert stripe_fake.create_calls[0]["amount"] == 500
    assert stripe_fake.create_calls[0]["currency"] == "usd"
    assert isinstance(stripe_fake.create_calls[0]["idempotency_key"], str)

    # DB persisted one payment and one bootstrap event
    pay_rows = (await db_session.execute(select(DAOPayments))).scalars().all()
    evt_rows = (await db_session.execute(select(DAOPaymentEvents))).scalars().all()

    assert len(pay_rows) == 1
    p = pay_rows[0]
    assert p.id == resp.payment_id
    assert p.stripe_payment_intent_id == "pi_ok_1"
    assert p.status == PaymentStatus.REQUIRES_PAYMENT_METHOD
    assert p.idempotency_key == resp.idempotency_key
    assert p.amount_total == 500
    assert p.currency == "usd"

    assert len(evt_rows) == 1
    e = evt_rows[0]
    assert e.payment_id == resp.payment_id
    assert e.source == PaymentEventSource.SYSTEM
    assert e.event_type == "bootstrap.initialize"
    assert e.applied_status == PaymentStatus.REQUIRES_PAYMENT_METHOD
    assert isinstance(e.payload, dict)


# -------------------------
# Validation failures
# -------------------------


@pytest.mark.asyncio
async def test_CI2_validation_error_empty_recipients_returns_400(
    checkout_handler: CheckoutAPIHandler,
    db_session: AsyncSession,
    owner_user: Any,
    photobook: Any,
) -> None:
    # Stripe shouldn't be called for validation errors; provide a fake anyway.
    stripe_fake = _StripeClientFakeCheckout()

    async def _get_rcx(_req: Request) -> _RequestContext:
        return _RequestContext(user_id=owner_user.id)

    async def _assert_owned(session: AsyncSession, pb_id: UUID, user_id: UUID) -> None:
        return None

    checkout_handler.get_request_context = _get_rcx  # type: ignore[assignment]
    checkout_handler.get_photobook_assert_owned_by = _assert_owned  # type: ignore[assignment]
    checkout_handler.get_stripe_client_for_request = lambda _req: stripe_fake  # type: ignore[method-assign, assignment, return-value]

    payload: ShareCreateRequest = ShareCreateRequest(
        recipients=[],
        giftcard_request=GiftcardGrantRequest(
            amount_per_share=1000, currency="usd", brand_code="amazon"
        ),
    )
    req: Request = _make_post_request(
        f"/api/checkout/{photobook.id}/initialize-payment"
    )

    with pytest.raises(Exception):
        await checkout_handler.checkout_initializing_payment(
            photobook_id=photobook.id,
            payload=payload,
            request=req,
        )

    # Ensure no DB writes occurred
    assert len((await db_session.execute(select(DAOPayments))).scalars().all()) == 0
    assert (
        len((await db_session.execute(select(DAOPaymentEvents))).scalars().all()) == 0
    )
    assert len(stripe_fake.create_calls) == 0


@pytest.mark.asyncio
async def test_CI3_validation_error_missing_giftcard_request_returns_400(
    checkout_handler: CheckoutAPIHandler,
    db_session: AsyncSession,
    owner_user: Any,
    photobook: Any,
) -> None:
    stripe_fake = _StripeClientFakeCheckout()

    async def _get_rcx(_req: Request) -> _RequestContext:
        return _RequestContext(user_id=owner_user.id)

    async def _assert_owned(session: AsyncSession, pb_id: UUID, user_id: UUID) -> None:
        return None

    checkout_handler.get_request_context = _get_rcx  # type: ignore[assignment]
    checkout_handler.get_photobook_assert_owned_by = _assert_owned  # type: ignore[assignment]
    checkout_handler.get_stripe_client_for_request = lambda _req: stripe_fake  # type: ignore[method-assign, assignment, return-value]

    payload: ShareCreateRequest = ShareCreateRequest(
        recipients=[
            ShareRecipientSpec(
                recipient_display_name="Friend",
                channels=[
                    ShareChannelSpec(
                        channel_type=ShareChannelType.EMAIL,
                        destination="friend@example.com",
                    )
                ],
            )
        ],
        giftcard_request=None,
    )
    req: Request = _make_post_request(
        f"/api/checkout/{photobook.id}/initialize-payment"
    )

    with pytest.raises(Exception):
        await checkout_handler.checkout_initializing_payment(
            photobook_id=photobook.id,
            payload=payload,
            request=req,
        )

    assert len((await db_session.execute(select(DAOPayments))).scalars().all()) == 0
    assert (
        len((await db_session.execute(select(DAOPaymentEvents))).scalars().all()) == 0
    )
    assert len(stripe_fake.create_calls) == 0


# -------------------------
# DB failure → cancel PI
# -------------------------


@pytest.mark.asyncio
async def test_CI4_db_failure_triggers_stripe_cancel_and_500(
    checkout_handler: CheckoutAPIHandler,
    db_session: AsyncSession,
    owner_user: Any,
    photobook: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stripe_fake = _StripeClientFakeCheckout()
    stripe_fake.set_create_result(
        StripeCreatePaymentIntentResult(
            stripe_payment_intent_id="pi_to_cancel",
            client_secret="cs_x",
            latest_charge_id="ch_x",
            status=PaymentStatus.REQUIRES_PAYMENT_METHOD,
        )
    )

    async def _get_rcx(_req: Request) -> _RequestContext:
        return _RequestContext(user_id=owner_user.id)

    async def _assert_owned(session: AsyncSession, pb_id: UUID, user_id: UUID) -> None:
        return None

    checkout_handler.get_request_context = _get_rcx  # type: ignore[assignment]
    checkout_handler.get_photobook_assert_owned_by = _assert_owned  # type: ignore[assignment]
    checkout_handler.get_stripe_client_for_request = lambda _req: stripe_fake  # type: ignore[method-assign, assignment, return-value]

    # Force DALPayments.create to raise inside the transaction
    from backend.db.dal import DALPayments as _DALPaymentsReal

    async def _raise_on_create(*args: Any, **kwargs: Any) -> Any:
        raise RuntimeError("db fail (test)")

    monkeypatch.setattr(_DALPaymentsReal, "create", _raise_on_create, raising=True)

    payload: ShareCreateRequest = ShareCreateRequest(
        recipients=[
            ShareRecipientSpec(
                recipient_display_name="Friend",
                channels=[
                    ShareChannelSpec(
                        channel_type=ShareChannelType.EMAIL,
                        destination="friend@example.com",
                    )
                ],
            )
        ],
        giftcard_request=GiftcardGrantRequest(
            amount_per_share=2500, currency="USD", brand_code="amazon"
        ),
    )

    req: Request = _make_post_request(
        f"/api/checkout/{photobook.id}/initialize-payment"
    )

    with pytest.raises(Exception):
        await checkout_handler.checkout_initializing_payment(
            photobook_id=photobook.id,
            payload=payload,
            request=req,
        )

    # PI should have been canceled best-effort
    assert stripe_fake.cancel_calls == ["pi_to_cancel"]

    # No DB rows should remain
    assert len((await db_session.execute(select(DAOPayments))).scalars().all()) == 0
    assert (
        len((await db_session.execute(select(DAOPaymentEvents))).scalars().all()) == 0
    )


# -------------------------
# Idempotency-key determinism across identical requests
# -------------------------


@pytest.mark.asyncio
async def test_CI5_idempotency_key_is_deterministic_for_same_payload(
    checkout_handler: CheckoutAPIHandler,
    db_session: AsyncSession,
    owner_user: Any,
    photobook: Any,
) -> None:
    stripe_fake = _StripeClientFakeCheckout()

    async def _get_rcx(_req: Request) -> _RequestContext:
        return _RequestContext(user_id=owner_user.id)

    async def _assert_owned(session: AsyncSession, pb_id: UUID, user_id: UUID) -> None:
        return None

    checkout_handler.get_request_context = _get_rcx  # type: ignore[assignment]
    checkout_handler.get_photobook_assert_owned_by = _assert_owned  # type: ignore[assignment]
    checkout_handler.get_stripe_client_for_request = lambda _req: stripe_fake  # type: ignore[method-assign, assignment, return-value]

    payload: ShareCreateRequest = ShareCreateRequest(
        recipients=[
            ShareRecipientSpec(
                recipient_display_name="A Friend",
                channels=[
                    ShareChannelSpec(
                        channel_type=ShareChannelType.EMAIL,
                        destination="friend@example.com",
                    )
                ],
            )
        ],
        sender_display_name="Me",
        giftcard_request=GiftcardGrantRequest(
            amount_per_share=1234, currency="USD", brand_code="amazon"
        ),
    )

    req: Request = _make_post_request(
        f"/api/checkout/{photobook.id}/initialize-payment"
    )

    r1: CheckoutPaymentBootstrapResponse = (
        await checkout_handler.checkout_initializing_payment(
            photobook_id=photobook.id,
            payload=payload,
            request=req,
        )
    )
    r2: CheckoutPaymentBootstrapResponse = (
        await checkout_handler.checkout_initializing_payment(
            photobook_id=photobook.id,
            payload=payload,
            request=req,
        )
    )

    # Same derived idempotency key each time
    assert r1.idempotency_key == r2.idempotency_key

    # Stripe saw the same key for both create calls
    assert len(stripe_fake.create_calls) == 2
    assert (
        stripe_fake.create_calls[0]["idempotency_key"]
        == stripe_fake.create_calls[1]["idempotency_key"]
    )

    # Two payments rows exist (current DB schema permits duplicates by idempotency_key)
    pay_rows = (await db_session.execute(select(DAOPayments))).scalars().all()
    assert len(pay_rows) == 2
    assert (
        pay_rows[0].idempotency_key == pay_rows[1].idempotency_key == r1.idempotency_key
    )


# -------------------------
# Ownership failure (404-like) prevents Stripe call
# -------------------------


@pytest.mark.asyncio
async def test_CI6_ownership_check_failure_short_circuits_and_never_calls_stripe(
    checkout_handler: CheckoutAPIHandler,
    db_session: AsyncSession,
    owner_user: Any,
    photobook: Any,
) -> None:
    stripe_fake = _StripeClientFakeCheckout()

    # Simulate a different caller
    other_user_id: UUID = uuid4()

    async def _get_rcx(_req: Request) -> _RequestContext:
        return _RequestContext(user_id=other_user_id)

    async def _assert_owned(session: AsyncSession, pb_id: UUID, user_id: UUID) -> None:
        # In the real handler this would raise HTTPException(404)
        raise RuntimeError("not owner (test)")

    checkout_handler.get_request_context = _get_rcx  # type: ignore[assignment]
    checkout_handler.get_photobook_assert_owned_by = _assert_owned  # type: ignore[assignment]
    checkout_handler.get_stripe_client_for_request = lambda _req: stripe_fake  # type: ignore[method-assign, assignment, return-value]

    payload: ShareCreateRequest = ShareCreateRequest(
        recipients=[
            ShareRecipientSpec(
                recipient_display_name="Friend",
                channels=[
                    ShareChannelSpec(
                        channel_type=ShareChannelType.EMAIL,
                        destination="friend@example.com",
                    )
                ],
            )
        ],
        giftcard_request=GiftcardGrantRequest(
            amount_per_share=500, currency="usd", brand_code="amazon"
        ),
    )

    req: Request = _make_post_request(
        f"/api/checkout/{photobook.id}/initialize-payment"
    )

    with pytest.raises(Exception):
        await checkout_handler.checkout_initializing_payment(
            photobook_id=photobook.id,
            payload=payload,
            request=req,
        )

    # No Stripe calls, no DB writes
    assert len(stripe_fake.create_calls) == 0
    assert len((await db_session.execute(select(DAOPayments))).scalars().all()) == 0
    assert (
        len((await db_session.execute(select(DAOPaymentEvents))).scalars().all()) == 0
    )
