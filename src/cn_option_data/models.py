"""Normalized provider values and Asia/Shanghai timestamps."""

from __future__ import annotations

import math
import re
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any
from zoneinfo import ZoneInfo

from .registry import Product


SHANGHAI = ZoneInfo("Asia/Shanghai")


def now_shanghai() -> datetime:
    return datetime.now(SHANGHAI)


def timestamp_from_seconds(value: Any) -> datetime | None:
    try:
        seconds = float(value)
        if not math.isfinite(seconds) or seconds <= 0:
            return None
        return datetime.fromtimestamp(seconds, SHANGHAI)
    except (TypeError, ValueError, OverflowError):
        return None


def exchange_timestamp(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(value.strip())
    except ValueError:
        return None
    return parsed.replace(tzinfo=SHANGHAI) if parsed.tzinfo is None else parsed.astimezone(SHANGHAI)


def source_datetime(value: Any) -> datetime | None:
    """Parse only a source-supplied date; never date a clock-only string locally."""
    if value is None or str(value) in {"NaT", "nan", "None", ""}:
        return None
    if isinstance(value, datetime):
        return value.replace(tzinfo=SHANGHAI) if value.tzinfo is None else value.astimezone(SHANGHAI)
    if isinstance(value, date):
        return datetime(value.year, value.month, value.day, tzinfo=SHANGHAI)
    text = str(value).strip()
    if re.match(r"^\d{8}(?:\s|$)", text):
        text = f"{text[:4]}-{text[4:6]}-{text[6:]}"
    if not re.match(r"^\d{4}[-/]\d{1,2}[-/]\d{1,2}", text):
        return None
    return exchange_timestamp(text.replace("/", "-"))


def quote_clock(quote: dict[str, Any]) -> tuple[datetime | None, str, str | None]:
    """Full timestamp, known source day, quality flag (no Parquet schema additions)."""
    raw = quote.get("source_quote_time")
    if raw is None or not str(raw).strip():
        raw = quote.get("quote_timestamp")
    parsed = source_datetime(raw)
    if parsed is None:
        # A missing/malformed clock must not erase a separately supplied valid date.
        text = str(raw).strip() if raw is not None else ""
        match = re.match(r"^(\d{4})[-/](\d{1,2})[-/](\d{1,2})(?=\s|T|$)", text)
        if match:
            try:
                day = date(*(int(part) for part in match.groups())).isoformat()
            except ValueError:
                pass
            else:
                return None, day, "SOURCE_TIME_DATE_ONLY"
        return None, "unknown", "SOURCE_TIME_UNKNOWN"
    day = parsed.date().isoformat()
    if parsed.hour == parsed.minute == parsed.second == parsed.microsecond == 0:
        return None, day, "SOURCE_TIME_DATE_ONLY"
    return parsed, day, None


def finite_number(value: Any, *, positive: bool = False) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(result) or (positive and result <= 0):
        return None
    return result


def nonnegative_int(value: Any) -> int | None:
    number = finite_number(value)
    return int(number) if number is not None and number >= 0 and number.is_integer() else None


@dataclass(frozen=True)
class OptionContract:
    product: Product
    contract_code: str
    contract_name: str | None
    underlying_symbol: str
    option_class: str
    strike_price: float
    expire_datetime: datetime | None
    last_trading_date: datetime | None
    volume_multiple: float | None
    price_tick: float | None
    provider_symbol: str | None = None
    source_interface: str | None = None
    request_timestamp: datetime | None = None


@dataclass(frozen=True)
class FutureContract:
    symbol: str
    future_product: str
    expire_datetime: datetime


def quote_values(quote: dict[str, Any]) -> dict[str, Any]:
    """Normalize only values supplied by a live HTTP response."""
    result = {
        "bid_price1": finite_number(quote.get("bid_price1"), positive=True),
        "bid_volume1": nonnegative_int(quote.get("bid_volume1")),
        "ask_price1": finite_number(quote.get("ask_price1"), positive=True),
        "ask_volume1": nonnegative_int(quote.get("ask_volume1")),
        "last_price": finite_number(quote.get("last_price"), positive=True),
        "volume": nonnegative_int(quote.get("volume")),
        "amount": finite_number(quote.get("amount")),
        "open_interest": nonnegative_int(quote.get("open_interest")),
        "upper_limit": finite_number(quote.get("upper_limit"), positive=True),
        "lower_limit": finite_number(quote.get("lower_limit"), positive=True),
    }
    return result
