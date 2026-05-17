"""Direct tests for the at-most-once classifier (`_at_most_once`).

Before this module existed, every row of the eToro at-most-once decision
table had to be exercised through a mocked HTTP path inside
``test_at_most_once.py`` / ``test_bulk_concurrent.py``. Now the policy
lives in a single pure module, so each row is one parameterised test.

These tests are intentionally narrow — they cover **only** the
classifier's interface contract. The existing higher-level tests still
cover end-to-end POST → result flows.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any

import pytest

from etoro_bulk_trades._at_most_once import (
    Outcome,
    classify_close,
    classify_open,
)
from etoro_bulk_trades.errors import (
    AuthError,
    HttpStatusError,
    InvalidCredentialsError,
    RateLimitError,
    SessionExpiredError,
    TransportError,
)

# ── success branches ──────────────────────────────────────────────────────


def test_open_success_extracts_order_id_and_amount() -> None:
    """2xx with ``orderForOpen`` → ``ok`` + parsed order_id + amount."""
    body = {
        "orderForOpen": {"orderID": 13902598, "amount": 150, "statusID": 1},
        "token": "irrelevant",
    }
    out = classify_open(response=body, exception=None)
    assert out.status == "ok"
    assert out.order_id == 13902598
    assert out.filled_amount == Decimal("150")
    assert out.next_action == "none"
    assert out.error is None


def test_open_success_missing_order_id_still_ok() -> None:
    """Defensive: a 2xx with no order_id still maps to ``ok`` but with
    a ``None`` order_id so the verifier doesn't try to match on it."""
    out = classify_open(response={"orderForOpen": {}}, exception=None)
    assert out.status == "ok"
    assert out.order_id is None
    assert out.filled_amount is None


def test_close_success_extracts_order_id_from_nested_envelope() -> None:
    body = {"orderForCloseMultiple": {"orderID": 999, "statusID": 1}}
    out = classify_close(response=body, exception=None)
    assert out.status == "ok"
    assert out.order_id == 999


def test_close_success_accepts_flat_envelope_fallback() -> None:
    """Some undocumented response variants put ``orderID`` at the top
    level; the classifier accepts it rather than silently dropping the
    id."""
    out = classify_close(response={"orderID": 777}, exception=None)
    assert out.status == "ok"
    assert out.order_id == 777


# ── failure branches — the full decision table ───────────────────────────


@pytest.mark.parametrize(
    ("exc", "expected_status", "expected_next_action"),
    [
        # 4xx other than 429 → failed, terminal.
        (HttpStatusError(status_code=400, body={"err": "bad"}), "failed", "none"),
        (HttpStatusError(status_code=403, body={"err": "forbidden"}), "failed", "none"),
        (HttpStatusError(status_code=404, body=None), "failed", "none"),
        # 401 with no specific subclass → conservative: treat as creds-invalid.
        (AuthError("generic 401"), "failed", "none"),
        # Bearer session dead → reauth_required (the fix this whole refactor
        # exists for; previously mislabeled).
        (SessionExpiredError("invalid_grant"), "failed", "reauth_required"),
        # Credentials invalid → still failed, but distinguishable.
        (InvalidCredentialsError("401"), "failed", "none"),
        # 429 after backoff retries → rate_limited_giveup, terminal.
        (RateLimitError(), "rate_limited_giveup", "none"),
        # Network drop / timeout → ambiguous; verifier reconciles via /pnl.
        (
            TransportError(status_code=None, message="connection reset"),
            "ambiguous",
            "reconcile_via_pnl",
        ),
        (TransportError(status_code=503, message="upstream"), "ambiguous", "reconcile_via_pnl"),
    ],
)
def test_classify_open_failure_table(
    exc: Exception, expected_status: str, expected_next_action: str
) -> None:
    out = classify_open(response=None, exception=exc)
    assert out.status == expected_status
    assert out.next_action == expected_next_action
    assert out.error is not None
    assert out.order_id is None


@pytest.mark.parametrize(
    ("exc", "expected_status", "expected_next_action"),
    [
        (HttpStatusError(status_code=400, body={"err": "bad"}), "failed", "none"),
        (AuthError("generic 401"), "failed", "none"),
        (SessionExpiredError("invalid_grant"), "failed", "reauth_required"),
        (InvalidCredentialsError("401"), "failed", "none"),
        (RateLimitError(), "rate_limited_giveup", "none"),
        (
            TransportError(status_code=None, message="connection reset"),
            "ambiguous",
            "reconcile_via_pnl",
        ),
    ],
)
def test_classify_close_failure_table(
    exc: Exception, expected_status: str, expected_next_action: str
) -> None:
    out = classify_close(response=None, exception=exc)
    assert out.status == expected_status
    assert out.next_action == expected_next_action
    assert out.error is not None


def test_classify_open_session_expired_message_distinct_from_invalid_creds() -> None:
    """Regression: the bug this module was created for. The close path
    used to label SessionExpiredError as "auth rejected this POST",
    indistinguishable from a wrong API key. Now the error text is
    distinct AND the structured ``next_action`` carries the difference."""
    expired = classify_open(response=None, exception=SessionExpiredError("invalid_grant"))
    invalid = classify_open(response=None, exception=InvalidCredentialsError("401"))
    assert expired.next_action == "reauth_required"
    assert invalid.next_action == "none"
    assert "session expired" in (expired.error or "").lower()
    assert "credentials invalid" in (invalid.error or "").lower()


# ── interface contracts ───────────────────────────────────────────────────


def test_classify_open_requires_exactly_one_of_response_or_exception() -> None:
    with pytest.raises(ValueError):
        classify_open(response=None, exception=None)
    with pytest.raises(ValueError):
        classify_open(
            response={"orderForOpen": {"orderID": 1}},
            exception=RateLimitError(),
        )


def test_classify_close_requires_exactly_one_of_response_or_exception() -> None:
    with pytest.raises(ValueError):
        classify_close(response=None, exception=None)
    with pytest.raises(ValueError):
        classify_close(response={"orderID": 1}, exception=RateLimitError())


def test_classify_rejects_unknown_exception_type_loudly() -> None:
    """If a new error class is added without updating the classifier,
    the table is incomplete — fail loudly so it shows up in tests rather
    than silently miscategorising production trades."""

    class _UnregisteredEtoroError(Exception):
        pass

    with pytest.raises(TypeError) as exc_info:
        classify_open(response=None, exception=_UnregisteredEtoroError("x"))
    assert "_classify_exception" in str(exc_info.value)


def test_outcome_is_frozen_and_strict() -> None:
    """:class:`Outcome` inherits the project's :class:`StrictModel`
    discipline: ``extra='forbid'`` + ``frozen=True``. Stops downstream
    code from mutating an outcome between classifier and TradeResult."""
    out = Outcome(
        status="ok",
        error=None,
        next_action="none",
        order_id=None,
        filled_amount=None,
    )
    with pytest.raises(Exception):  # noqa: B017 - pydantic ValidationError
        out.status = "failed"


def test_outcome_field_order_preserved_under_construction_keywords() -> None:
    """Construction by keyword should match the class definition order
    so future field additions don't silently break call sites that build
    Outcomes positionally (we don't, but the test pins the surface)."""
    payload: dict[str, Any] = {
        "status": "ok",
        "error": None,
        "next_action": "none",
        "order_id": 42,
        "filled_amount": Decimal("100"),
    }
    out = Outcome(**payload)
    assert out.model_dump() == payload
