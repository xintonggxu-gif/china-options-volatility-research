"""Small, bounded batch reader for Sina's public quote endpoint.

The positional option fields follow the mapping used by AKShare's
``option_sse_spot_price_sina``. CFFEX index options (``P_OP_``) use the same
layout; CFFEX futures (``CFF_RE_``) positions were cross-checked field by field
against AKShare ``futures_zh_realtime`` on 2026-09-30. Only a verified contract
ID is accepted by the caller; a missing or malformed response never becomes a
synthetic quote.
"""

from __future__ import annotations

import logging
import re
import uuid
from collections import deque
from dataclasses import dataclass
from datetime import datetime
from typing import Callable

from ..models import finite_number, now_shanghai
from .bounded_calls import BoundedCalls, CallCancelled, NetworkCallError


LOG = logging.getLogger(__name__)
INTERFACE = "hq.sinajs.cn/list"
URL = "https://hq.sinajs.cn/list="
LINE = re.compile(r'^var hq_str_([A-Za-z0-9_]+)="([^"]*)";$')
HEADERS = {"Referer": "https://stock.finance.sina.com.cn/", "User-Agent": "Mozilla/5.0"}
SYMBOL = re.compile(r"[A-Za-z0-9_]+")
# Requests of 500 option symbols (~8 KB URL) succeeded on 2026-09-30 and 600 failed;
# 200 keeps a wide margin while fitting most single-product chains in one request.
BUNDLE_BATCH_SIZE = 200


@dataclass(frozen=True)
class SinaResult:
    fields: list[str]
    requested: datetime
    received: datetime
    batch_id: str


def cffex_option_symbol(contract_code: str) -> str:
    """IO2610C4600 -> P_OP_io2610C4600; Sina returns nothing if C/P is lowercased."""
    return "P_OP_" + contract_code[:2].lower() + contract_code[2:].upper()


class SinaQuotes:
    def __init__(self, should_stop: Callable[[], bool], errors: list[str] | deque[str], calls: BoundedCalls):
        self.should_stop = should_stop
        self.errors = errors
        self.calls = calls

    def _request(self, chunk: list[str]) -> dict[str, SinaResult] | None:
        """One HTTP request; every returned symbol shares the request's batch_id."""
        if not all(SYMBOL.fullmatch(symbol) for symbol in chunk):
            raise ValueError("Sina symbol 包含无效字符")
        requested = now_shanghai()
        LOG.info("Sina dispatch interface=%s dispatched_at=%s symbols=%s", INTERFACE, requested.isoformat(), len(chunk))
        try:
            content, requested, received = self.calls.call("http", URL + ",".join(chunk), headers=HEADERS)
            body = content.decode("gb18030")
        except CallCancelled:
            return None
        except (NetworkCallError, UnicodeError) as exc:
            LOG.warning("Sina interface=%s dispatched_at=%s failed: %s", INTERFACE, requested.isoformat(), exc)
            self.errors.append(f"{INTERFACE}: {exc}")
            return {}
        batch_id = uuid.uuid4().hex
        LOG.info("Sina result interface=%s requested_at=%s received_at=%s symbols=%s batch_id=%s", INTERFACE,
                 requested.isoformat(), received.isoformat(), len(chunk), batch_id)
        expected = set(chunk)
        found: dict[str, SinaResult] = {}
        for line in body.splitlines():
            match = LINE.fullmatch(line.strip())
            if match and match[1] in expected and match[2]:
                found[match[1]] = SinaResult(match[2].split(","), requested, received, batch_id)
        missing = expected - found.keys()
        if missing:
            LOG.warning("Sina 返回缺少 %s/%s 个请求的 symbol", len(missing), len(chunk))
        return found

    def fetch(self, symbols: list[str], batch_size: int = 60) -> dict[str, SinaResult]:
        found: dict[str, SinaResult] = {}
        for start in range(0, len(symbols), batch_size):
            if self.should_stop():
                break
            result = self._request(symbols[start : start + batch_size])
            if result is None:
                break
            found.update(result)
        return found

    def fetch_bundled(self, anchors: list[str], symbols: list[str], batch_size: int = BUNDLE_BATCH_SIZE,
                      ) -> tuple[dict[str, SinaResult], list[tuple[str, SinaResult]]]:
        """Request ``anchors`` (underlying, futures) inside every batch of ``symbols``.

        Each anchor observation therefore comes from the same HTTP response as the
        options it accompanies and carries the same batch_id. Returns the option
        results keyed by symbol and every anchor observation (one per batch).
        """
        options: dict[str, SinaResult] = {}
        anchor_rows: list[tuple[str, SinaResult]] = []
        anchor_set = set(anchors)
        for start in range(0, len(symbols), batch_size):
            if self.should_stop():
                break
            chunk = [s for s in symbols[start : start + batch_size] if s not in anchor_set]
            result = self._request(list(anchors) + chunk)
            if result is None:
                break
            for symbol, item in result.items():
                if symbol in anchor_set:
                    anchor_rows.append((symbol, item))
                else:
                    options[symbol] = item
        return options, anchor_rows

    @staticmethod
    def option(fields: list[str], provider_symbol: str, underlying_code: str, side: str, strike: float,
               requested: datetime, received: datetime, amount_multiplier: float = 1.0) -> dict | None:
        # Quote symbol and the payload's underlying/strike must agree with today's master table.
        # The response variable names the exact requested contract. Some
        # responses omit trailing metadata after the price/volume fields.
        if (len(fields) < 43 or fields[36] != underlying_code
                or finite_number(fields[7]) != strike
                or (len(fields) > 45 and fields[45] != side)):
            return None
        amount = finite_number(fields[42])
        return {
            "quote_timestamp": fields[32], "source_quote_time": fields[32],
            "request_timestamp": requested, "received_timestamp": received,
            "source_interface": INTERFACE, "source": "Sina",
            "bid_price1": fields[22], "bid_volume1": fields[23],
            "ask_price1": fields[20], "ask_volume1": fields[21],
            "last_price": fields[2], "volume": fields[41],
            "amount": None if amount is None else amount * amount_multiplier,
            "open_interest": fields[5], "upper_limit": fields[10], "lower_limit": fields[11],
            "provider_symbol": provider_symbol,
        }

    @staticmethod
    def underlying(fields: list[str], provider_symbol: str,
                   requested: datetime, received: datetime) -> dict | None:
        if len(fields) < 32 or not fields[0]:
            return None
        source_time = " ".join(part for part in fields[30:32] if part).strip() or None
        is_index = provider_symbol.startswith("sh000")
        return {
            "quote_timestamp": source_time,
            "source_quote_time": source_time,
            "request_timestamp": requested, "received_timestamp": received,
            "source_interface": INTERFACE, "source": "Sina",
            "bid_price1": None if is_index else fields[11],
            "bid_volume1": None if is_index else fields[10],
            "ask_price1": None if is_index else fields[21],
            "ask_volume1": None if is_index else fields[20],
            "last_price": fields[3], "volume": fields[8], "amount": fields[9],
            "provider_symbol": provider_symbol,
        }

    @staticmethod
    def future(fields: list[str], provider_symbol: str,
               requested: datetime, received: datetime) -> dict | None:
        """CFFEX futures ``CFF_RE_<symbol>``: positions match futures_zh_realtime names."""
        if len(fields) < 38 or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", fields[36]):
            return None
        return {
            "quote_timestamp": f"{fields[36]} {fields[37]}",
            "request_timestamp": requested, "received_timestamp": received,
            "source_interface": INTERFACE, "source": "Sina",
            "bid_price1": fields[16], "bid_volume1": fields[17],
            "ask_price1": fields[26], "ask_volume1": fields[27],
            "last_price": fields[3], "volume": fields[4], "open_interest": fields[6],
            "provider_symbol": provider_symbol,
        }
