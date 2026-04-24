import hashlib
import hmac
import json
import random
import time
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Dict, Final, Mapping, Optional, Tuple, TypeVar

import httpx
from pydantic import BaseModel, ConfigDict, Field, ValidationInfo, field_validator

from backend.lib.utils.common import utcnow

TModel = TypeVar("TModel", bound=BaseModel)

# =========================
# Public Types & Schemas
# =========================


class AGCODEndpoint(Enum):
    """Amazon-provided base endpoints (host only)."""

    # Sandbox
    NA_SANDBOX = "agcod-v2-gamma.amazon.com"  # us-east-1
    FE_SANDBOX = "agcod-v2-fe-gamma.amazon.com"  # us-west-2
    # EU_SANDBOX = "agcod-v2-eu-gamma.amazon.com"  # eu-west-1
    # Production
    NA_PROD = "agcod-v2.amazon.com"  # us-east-1
    FE_PROD = "agcod-v2-fe.amazon.com"  # us-west-2
    # EU_PROD = "agcod-v2-eu.amazon.com"  # eu-west-1


class AWSRegion(Enum):
    US_EAST_1 = "us-east-1"
    US_WEST_2 = "us-west-2"
    # EU_WEST_1 = "eu-west-1"


class ServiceName(str, Enum):
    AGCOD = "AGCODService"


class Operation(str, Enum):
    CREATE_GIFT_CARD = "CreateGiftCard"
    CANCEL_GIFT_CARD = "CancelGiftCard"
    GET_AVAILABLE_FUNDS = "GetAvailableFunds"


class StatusCode(str, Enum):
    SUCCESS = "SUCCESS"
    FAILURE = "FAILURE"
    RESEND = "RESEND"


class ErrorClass(str, Enum):
    F100 = "F100"  # Internal
    F200 = "F200"  # Invalid request
    F300 = "F300"  # Account / Access / Onboarding
    F400 = "F400"  # Retryable (temporary)
    F500 = "F500"  # Unknown
    THROTTLED = "Throttled"


class CurrencyCode(str, Enum):
    # Note: include only markets your account is enabled for, add more as needed
    USD = "USD"
    # EUR = "EUR"
    # GBP = "GBP"
    # JPY = "JPY"
    # CAD = "CAD"
    # AUD = "AUD"
    # TRY = "TRY"
    # AED = "AED"
    # MXN = "MXN"
    # PLN = "PLN"
    # SEK = "SEK"
    # SGD = "SGD"
    # ZAR = "ZAR"
    # EGP = "EGP"


class GiftCardValue(BaseModel):
    model_config = ConfigDict(extra="forbid")
    currencyCode: CurrencyCode
    amount: float  # Amazon docs say "Long"; they accept integral/decimal per currency. Keep float for JSON; validate below.

    @field_validator("amount")
    @classmethod
    def _amount_positive(cls, v: float) -> float:
        if v <= 0:
            raise ValueError("amount must be > 0")
        return v


class TransactionSource(BaseModel):
    """B&M only. Include only when authorized/required by Amazon."""

    model_config = ConfigDict(extra="forbid")
    sourceId: str = Field(min_length=1, max_length=40)
    institutionId: str = Field(min_length=1, max_length=40)
    sourceDetails: str = Field(
        min_length=1,
        max_length=200,
        description="JSON string containing institutionName and other metadata.",
    )


# ---------- Requests ----------


class CreateGiftCardRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    creationRequestId: str = Field(max_length=40)
    partnerId: str
    value: GiftCardValue

    # Optional program / product fields (only when authorized)
    programId: Optional[str] = Field(default=None, max_length=100)
    productType: Optional[str] = None  # constrained by Amazon if authorized

    # Optional B&M transaction source
    transactionSource: Optional[TransactionSource] = None
    sourceId: Optional[str] = Field(default=None, max_length=20)
    institutionId: Optional[str] = Field(default=None, max_length=20)
    sourceDetails: Optional[str] = Field(default=None, max_length=200)

    # Optional opaque external reference
    externalReference: Optional[str] = Field(default=None, max_length=100)

    @field_validator("creationRequestId")
    @classmethod
    def _validate_creation_id(cls, v: str, info: ValidationInfo) -> str:
        partner_id: Optional[str] = None
        # info.data is Mapping[str, object] during validation
        raw = info.data.get("partnerId")
        if isinstance(raw, str):
            partner_id = raw

        if partner_id and not v.startswith(partner_id):
            raise ValueError("creationRequestId must start with partnerId")
        if not v.isalnum():
            raise ValueError("creationRequestId must be alphanumeric")
        return v

    @field_validator("partnerId")
    @classmethod
    def _validate_partner_case(cls, v: str) -> str:
        # Docs: first letter capitalized and next four lower-case (partnerId is case sensitive).
        # We don't forcibly transform; we validate minimum pattern (len>=5).
        if len(v) < 5:
            raise ValueError(
                "partnerId must be at least 5 characters (case sensitive per Amazon)"
            )
        return v


class CancelGiftCardRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    creationRequestId: str = Field(max_length=40)
    partnerId: str

    @field_validator("creationRequestId")
    @classmethod
    def _validate_creation_id(cls, v: str, info: ValidationInfo) -> str:
        partner_id: Optional[str] = None
        # info.data is Mapping[str, object] during validation
        raw = info.data.get("partnerId")
        if isinstance(raw, str):
            partner_id = raw

        if partner_id and not v.startswith(partner_id):
            raise ValueError("creationRequestId must start with partnerId")
        if not v.isalnum():
            raise ValueError("creationRequestId must be alphanumeric")
        return v


class GetAvailableFundsRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    partnerId: str


# ---------- Responses ----------


class GiftCardInfo(BaseModel):
    model_config = ConfigDict(extra="forbid")
    cardStatus: str
    value: GiftCardValue


class CreateGiftCardResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")
    creationRequestId: str
    cardInfo: GiftCardInfo
    gcClaimCode: Optional[str] = (
        None  # may or may not be present depending on market/security
    )
    gcId: str
    gcExpirationDate: Optional[str] = None  # Not present NA/CA/AU
    status: StatusCode


class CancelGiftCardResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")
    creationRequestId: str
    status: StatusCode


class AvailableFunds(BaseModel):
    model_config = ConfigDict(extra="forbid")
    amount: float
    currencyCode: CurrencyCode


class GetAvailableFundsResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")
    availableFunds: AvailableFunds
    status: StatusCode
    timestamp: str


# =========================
# Exceptions
# =========================


class AGCODError(Exception):
    """Base exception for AGCOD client errors."""

    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.message: str = message


class AGCODHTTPError(AGCODError):
    def __init__(self, status_code: int, payload: str) -> None:
        super().__init__(f"HTTP {status_code}: {payload}")
        self.status_code: int = status_code
        self.payload: str = payload


class AGCODFailure(AGCODError):
    """AGCOD returned status=FAILURE with error metadata."""

    def __init__(
        self,
        status: StatusCode,
        error_code: Optional[str],
        message: Optional[str],
        raw: Mapping[str, object],
    ) -> None:
        msg: str = f"AGCOD failure ({status.value}) code={error_code or 'UNKNOWN'} message={message or ''}"
        super().__init__(msg)
        self.status: StatusCode = status
        self.error_code: Optional[str] = error_code
        self.raw: Mapping[str, object] = raw


class AGCODResend(AGCODError):
    """AGCOD returned status=RESEND (retryable). Raised to trigger retry policy."""

    def __init__(self, raw: Mapping[str, object]) -> None:
        super().__init__("AGCOD returned RESEND")
        self.raw: Mapping[str, object] = raw


class AGCODThrottled(AGCODError):
    """Throttling exception (HTTP 429 or known throttling response)."""

    pass


# =========================
# SigV4 Signing
# =========================

_HASH_ALGO: Final[str] = "AWS4-HMAC-SHA256"
_TERMINATION: Final[str] = "aws4_request"
_JSON_CT: Final[str] = "application/json"
_ACCEPT: Final[str] = "application/json"


@dataclass(frozen=True)
class Credentials:
    access_key_id: str
    secret_access_key: str


def _hmac(key: bytes, msg: str) -> bytes:
    return hmac.new(key, msg.encode("utf-8"), hashlib.sha256).digest()


def _signing_key(
    secret_access_key: str, datestamp: str, region: AWSRegion, service: ServiceName
) -> bytes:
    k_date: bytes = _hmac(("AWS4" + secret_access_key).encode("utf-8"), datestamp)
    k_region: bytes = _hmac(k_date, region.value)
    k_service: bytes = _hmac(k_region, service.value)
    k_signing: bytes = _hmac(k_service, _TERMINATION)
    return k_signing


def _amz_date(now: datetime) -> str:
    # Format: 20120325T120000Z
    return now.strftime("%Y%m%dT%H%M%SZ")


def _date_stamp(now: datetime) -> str:
    # Format: 20120325
    return now.strftime("%Y%m%d")


def _sha256_hexdigest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _canonical_headers(host: str, amz_date: str, amz_target: str) -> Tuple[str, str]:
    """
    Returns (canonical_headers, signed_headers)
    """
    # Required headers in canonical order and lowercase names
    headers: Dict[str, str] = {
        "accept": _ACCEPT,
        "content-type": _JSON_CT,
        "host": host,
        "x-amz-date": amz_date,
        "x-amz-target": amz_target,
    }
    # Canonical form: "<lowername>:<trimmed value>\n" sorted by header name
    items = sorted((k, v.strip()) for k, v in headers.items())
    canonical: str = "".join(f"{k}:{v}\n" for k, v in items)
    signed: str = ";".join(k for k, _ in items)
    return canonical, signed


def _canonical_request_json(
    method: str,
    path: str,
    canonical_headers: str,
    signed_headers: str,
    payload_hash: str,
) -> str:
    canonical_uri: str = path  # e.g., "/CreateGiftCard"
    canonical_query: str = ""  # POST with no query
    return "\n".join(
        [
            method,
            canonical_uri,
            canonical_query,
            canonical_headers,
            signed_headers,
            payload_hash,
        ]
    )


def _string_to_sign(amz_date: str, scope: str, hashed_canonical_request: str) -> str:
    return "\n".join(
        [
            _HASH_ALGO,
            amz_date,
            scope,
            hashed_canonical_request,
        ]
    )


def _authorization_header(
    access_key_id: str, scope: str, signed_headers: str, signature: str
) -> str:
    return f"{_HASH_ALGO} Credential={access_key_id}/{scope}, SignedHeaders={signed_headers}, Signature={signature}"


# =========================
# Client
# =========================


@dataclass(frozen=True)
class RetryPolicy:
    """Configurable retry policy for RESEND and throttling/network errors."""

    max_attempts: int = 5
    initial_delay_seconds: float = 0.5
    max_delay_seconds: float = 8.0
    jitter_fraction: float = 0.2  # +/- 20%

    def next_sleep(self, attempt_index: int) -> float:
        """
        attempt_index: 1-based index (1..max_attempts); returns seconds to sleep before next attempt.
        """
        base: float = min(
            self.max_delay_seconds,
            self.initial_delay_seconds * (2 ** (attempt_index - 1)),
        )
        jitter: float = base * self.jitter_fraction
        return max(0.0, base + random.uniform(-jitter, jitter))


class AGCODClient:
    """
    Strongly typed AGCOD client (CreateGiftCard, CancelGiftCard, GetAvailableFunds).
    """

    def __init__(
        self,
        *,
        partner_id: str,
        credentials: Credentials,
        endpoint: AGCODEndpoint,
        region: AWSRegion,
        http: Optional[httpx.Client] = None,
        retry_policy: Optional[RetryPolicy] = None,
        request_timeout_seconds: float = 15.0,
    ) -> None:
        self._partner_id: str = partner_id
        self._creds: Credentials = credentials
        self._endpoint: AGCODEndpoint = endpoint
        self._region: AWSRegion = region
        self._http: httpx.Client = http or httpx.Client(
            http2=True, timeout=request_timeout_seconds
        )
        self._retry: RetryPolicy = retry_policy or RetryPolicy()
        self._service: ServiceName = ServiceName.AGCOD

    # ---------- Public Operations ----------

    def create_gift_card(
        self,
        *,
        creation_request_id: str,
        currency: CurrencyCode,
        amount: float,
        program_id: Optional[str] = None,
        product_type: Optional[str] = None,
        external_reference: Optional[str] = None,
        transaction_source: Optional[TransactionSource] = None,
        source_id: Optional[str] = None,
        institution_id: Optional[str] = None,
        source_details: Optional[str] = None,
    ) -> CreateGiftCardResponse:
        payload = CreateGiftCardRequest(
            creationRequestId=creation_request_id,
            partnerId=self._partner_id,
            value=GiftCardValue(currencyCode=currency, amount=amount),
            programId=program_id,
            productType=product_type,
            externalReference=external_reference,
            transactionSource=transaction_source,
            sourceId=source_id,
            institutionId=institution_id,
            sourceDetails=source_details,
        )
        return self._send_and_parse(
            Operation.CREATE_GIFT_CARD, payload, CreateGiftCardResponse
        )

    def cancel_gift_card(self, *, creation_request_id: str) -> CancelGiftCardResponse:
        payload = CancelGiftCardRequest(
            creationRequestId=creation_request_id,
            partnerId=self._partner_id,
        )
        return self._send_and_parse(
            Operation.CANCEL_GIFT_CARD, payload, CancelGiftCardResponse
        )

    def get_available_funds(self) -> GetAvailableFundsResponse:
        payload = GetAvailableFundsRequest(partnerId=self._partner_id)
        return self._send_and_parse(
            Operation.GET_AVAILABLE_FUNDS, payload, GetAvailableFundsResponse
        )

    # ---------- Core Request Flow ----------

    def _send_and_parse(
        self,
        op: Operation,
        payload: BaseModel,
        model: type[TModel],
    ) -> TModel:
        body_bytes: bytes = payload.model_dump_json(by_alias=False).encode("utf-8")
        attempt: int = 1
        last_exc: Optional[Exception] = None

        while attempt <= self._retry.max_attempts:
            try:
                resp_json: Mapping[str, object] = self._signed_post(
                    op=op, body=body_bytes
                )

                status_raw: Optional[str] = _get_str(resp_json.get("status"))
                status: StatusCode = (
                    StatusCode(status_raw) if status_raw else StatusCode.FAILURE
                )

                if status is StatusCode.RESEND:
                    raise AGCODResend(resp_json)
                if status is StatusCode.FAILURE:
                    raise AGCODFailure(
                        status=status,
                        error_code=_get_str(resp_json.get("errorCode")),
                        message=_get_str(resp_json.get("message")),
                        raw=resp_json,
                    )

                validated: TModel = model.model_validate(resp_json)
                return validated

            except (
                httpx.ConnectError,
                httpx.ReadTimeout,
                httpx.WriteError,
                AGCODResend,
                AGCODThrottled,
            ) as e:
                last_exc = e
                if attempt == self._retry.max_attempts:
                    raise
                time.sleep(self._retry.next_sleep(attempt))
                attempt += 1
                continue
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 429:
                    last_exc = AGCODThrottled("Rate exceeded")
                    if attempt == self._retry.max_attempts:
                        raise last_exc
                    time.sleep(self._retry.next_sleep(attempt))
                    attempt += 1
                    continue
                payload_text: str = e.response.text
                raise AGCODHTTPError(e.response.status_code, payload_text) from e

        if last_exc:
            raise last_exc
        raise AGCODError("Unknown error without exception")

    def _signed_post(self, *, op: Operation, body: bytes) -> Mapping[str, object]:
        """
        Builds SigV4 headers and executes the POST. Returns parsed JSON mapping.
        """
        now_utc: datetime = utcnow().replace(tzinfo=None)
        amz_date: str = _amz_date(now_utc)  # 20160708T073147Z
        datestamp: str = _date_stamp(now_utc)  # 20160708

        path: str = f"/{op.value}"
        host: str = self._endpoint.value
        service: ServiceName = self._service
        region: AWSRegion = self._region

        payload_hash: str = _sha256_hexdigest(body)
        amz_target: str = f"com.amazonaws.agcod.AGCODService.{op.value}"

        canonical_headers, signed_headers = _canonical_headers(
            host, amz_date, amz_target
        )
        canonical_request: str = _canonical_request_json(
            method="POST",
            path=path,
            canonical_headers=canonical_headers,
            signed_headers=signed_headers,
            payload_hash=payload_hash,
        )

        hashed_canonical: str = _sha256_hexdigest(canonical_request.encode("utf-8"))
        scope: str = f"{datestamp}/{region.value}/{service.value}/{_TERMINATION}"
        string_to_sign: str = _string_to_sign(amz_date, scope, hashed_canonical)
        signing_key: bytes = _signing_key(
            self._creds.secret_access_key, datestamp, region, service
        )
        signature: str = hmac.new(
            signing_key, string_to_sign.encode("utf-8"), hashlib.sha256
        ).hexdigest()
        auth_header: str = _authorization_header(
            self._creds.access_key_id, scope, signed_headers, signature
        )

        headers: Dict[str, str] = {
            "accept": _ACCEPT,
            "content-type": _JSON_CT,
            "host": host,  # httpx will set host as well; include for canonical match
            "x-amz-date": amz_date,
            "x-amz-target": amz_target,
            "authorization": auth_header,
        }

        url: str = f"https://{host}{path}"
        r: httpx.Response = self._http.post(url, content=body, headers=headers)
        if r.status_code == 429:
            raise AGCODThrottled("Rate exceeded")
        # Raise for non-2xx to central handler:
        try:
            r.raise_for_status()
        except httpx.HTTPStatusError as e:
            # Surface actual body in AGCODHTTPError upstream
            raise e

        # JSON only (we request application/json)
        payload_text: str = r.text
        try:
            parsed: Mapping[str, object] = json.loads(payload_text)
        except json.JSONDecodeError as e:
            raise AGCODHTTPError(r.status_code, payload_text) from e
        return parsed


# =========================
# Small helpers (typed)
# =========================


def _get_str(value: object | None) -> Optional[str]:
    if isinstance(value, str):
        return value
    return None
