"""Small, bounded batch reader for Sina's public quote endpoint.

The positional option fields follow the mapping used by AKShare's
``option_sse_spot_price_sina``. Only a verified contract ID is accepted by the
caller; a missing or malformed response never becomes a synthetic quote.
"""

from __future__ import annotations

import logging
import re
from datetime import datetime
from typing import Callable

import requests

from ..models import finite_number, now_shanghai


LOG = logging.getLogger(__name__)
INTERFACE = "hq.sinajs.cn/list"
URL = "https://hq.sinajs.cn/list="
LINE = re.compile(r'^var hq_str_([A-Za-z0-9_]+)="([^"]*)";$')
HEADERS = {"Referer": "https://stock.finance.sina.com.cn/", "User-Agent": "Mozilla/5.0"}


class SinaQuotes:
    def __init__(self, should_stop: Callable[[], bool], errors: list[str]):
        self.should_stop = should_stop
        self.errors = errors
        self.session = requests.Session()

    def fetch(self, symbols: list[str], batch_size: int = 60) -> dict[str, tuple[list[str], datetime, datetime]]:
        found: dict[str, tuple[list[str], datetime, datetime]] = {}
        for start in range(0, len(symbols), batch_size):
            if self.should_stop():
                break
            chunk = symbols[start : start + batch_size]
            if not all(re.fullmatch(r"[A-Za-z0-9_]+", symbol) for symbol in chunk):
                raise ValueError("Sina symbol 包含无效字符")
            requested = now_shanghai()
            LOG.info("Sina request interface=%s requested_at=%s symbols=%s", INTERFACE, requested.isoformat(), len(chunk))
            try:
                response = self.session.get(URL + ",".join(chunk), headers=HEADERS, timeout=15)
                response.raise_for_status()
                body = response.content.decode("gb18030")
            except (requests.RequestException, UnicodeError) as exc:
                LOG.warning("Sina interface=%s requested_at=%s failed: %s", INTERFACE, requested.isoformat(), exc)
                self.errors.append(f"{INTERFACE}: {exc}")
                continue
            received = now_shanghai()
            expected = set(chunk)
            for line in body.splitlines():
                match = LINE.fullmatch(line.strip())
                if match and match[1] in expected and match[2]:
                    found[match[1]] = (match[2].split(","), requested, received)
            missing = expected - found.keys()
            if missing:
                LOG.warning("Sina 返回缺少 %s/%s 个请求的 symbol", len(missing), len(chunk))
        return found

    @staticmethod
    def option(fields: list[str], contract_code: str, underlying_code: str, side: str, strike: float,
               requested: datetime, received: datetime) -> dict | None:
        # Quote symbol and the payload's underlying/strike must agree with today's master table.
        if (len(fields) < 46 or fields[36] != underlying_code or fields[45] != side
                or finite_number(fields[7]) != strike):
            return None
        return {
            "quote_timestamp": fields[32], "source_quote_time": fields[32],
            "request_timestamp": requested, "received_timestamp": received,
            "source_interface": INTERFACE, "source": "Sina",
            "bid_price1": fields[22], "bid_volume1": fields[23],
            "ask_price1": fields[20], "ask_volume1": fields[21],
            "last_price": fields[2], "volume": fields[41], "amount": fields[42],
            "open_interest": fields[5], "upper_limit": fields[10], "lower_limit": fields[11],
            "provider_symbol": "CON_OP_" + contract_code,
        }

    @staticmethod
    def underlying(fields: list[str], provider_symbol: str,
                   requested: datetime, received: datetime) -> dict | None:
        if len(fields) < 32 or not fields[0]:
            return None
        source_time = fields[30] + " " + fields[31] if fields[30] and fields[31] else None
        is_index = provider_symbol.startswith("sh000")
        return {
            "quote_timestamp": source_time,
            "request_timestamp": requested, "received_timestamp": received,
            "source_interface": INTERFACE, "source": "Sina",
            "bid_price1": None if is_index else fields[11],
            "bid_volume1": None if is_index else fields[10],
            "ask_price1": None if is_index else fields[21],
            "ask_volume1": None if is_index else fields[20],
            "last_price": fields[3], "volume": fields[8], "amount": fields[9],
            "provider_symbol": provider_symbol,
        }
