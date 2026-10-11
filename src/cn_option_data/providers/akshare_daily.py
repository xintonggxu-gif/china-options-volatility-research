"""Only the daily AKShare endpoints confirmed in the bounded source audit.

The Sina option function name says 'sse', but the same endpoint was separately
observed returning one contract from each of the four SZSE product chains.
That observation verifies prices for known codes, not historical discovery.
"""

from __future__ import annotations

import time
import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable

import pandas as pd

from .bounded_calls import BoundedCalls, CallCancelled, NetworkCallError


OPTION_INTERFACES = {
    "510050": "option_sse_daily_sina",
    "510300": "option_sse_daily_sina",
    "510500": "option_sse_daily_sina",
    "588000": "option_sse_daily_sina",
    "588080": "option_sse_daily_sina",
    "159919": "option_sse_daily_sina",
    "159922": "option_sse_daily_sina",
    "159915": "option_sse_daily_sina",
    "159901": "option_sse_daily_sina",
    "HO": "option_cffex_sz50_daily_sina",
    "IO": "option_cffex_hs300_daily_sina",
    "MO": "option_cffex_zz1000_daily_sina",
}
ETF_INTERFACES = {code: "stock_zh_a_hist_tx" for code in (
    "510050", "510300", "510500", "588000", "588080",
    "159919", "159922", "159915", "159901",
)}
INDEX_INTERFACE = "stock_zh_index_daily"


@dataclass
class DailySourceResult:
    status: str
    frame: Any
    interface: str
    provider_symbol: str
    requested_at: datetime | None = None
    received_at: datetime | None = None
    reason: str | None = None


class AkshareDailyProvider:
    def __init__(self, *, max_requests: int, should_stop: Callable[[], bool],
                 timeout_seconds: float = 15.0, min_interval_seconds: float = 0.5):
        self.calls = BoundedCalls(timeout=timeout_seconds, should_stop=should_stop,
                                  max_http_requests=max_requests)
        self.min_interval_seconds = min_interval_seconds
        self._last_call = 0.0
        self.should_stop = should_stop

    @property
    def http_requests(self) -> int:
        return self.calls.http_requests

    def close(self) -> None:
        self.calls.close()

    def _request(self, interface: str, provider_symbol: str, *, kind: str = "akshare",
                 target: str | None = None, **kwargs: Any) -> DailySourceResult:
        if self.http_requests >= self.calls.max_http_requests:
            return DailySourceResult("REQUEST_LIMIT", None, interface, provider_symbol, reason="HTTP request cap reached")
        for attempt in range(2):
            if self.should_stop():
                raise CallCancelled("Daily backfill interrupted")
            pause = self.min_interval_seconds - (time.monotonic() - self._last_call)
            if pause > 0:
                time.sleep(pause)
            self._last_call = time.monotonic()
            try:
                frame, requested, received = self.calls.call(kind, target or interface, **kwargs)
                if kind == "akshare" and not isinstance(frame, pd.DataFrame):
                    return DailySourceResult("SOURCE_UNAVAILABLE", None, interface, provider_symbol,
                                             requested, received, "interface did not return a DataFrame")
                if kind == "http" and not isinstance(frame, bytes):
                    return DailySourceResult("SOURCE_UNAVAILABLE", None, interface, provider_symbol,
                                             requested, received, "HTTP source did not return bytes")
                if (kind == "akshare" and frame.empty) or (kind == "http" and not frame):
                    return DailySourceResult("SOURCE_UNAVAILABLE", frame, interface, provider_symbol,
                                             requested, received, "empty source response")
                return DailySourceResult("OK", frame, interface, provider_symbol, requested, received)
            except NetworkCallError as exc:
                reason = str(exc)
                if "RequestBudgetExceeded" in reason or self.http_requests >= self.calls.max_http_requests:
                    return DailySourceResult("REQUEST_LIMIT", None, interface, provider_symbol, reason=reason)
                if attempt == 1:
                    return DailySourceResult("SOURCE_UNAVAILABLE", None, interface, provider_symbol, reason=reason)
        raise AssertionError("unreachable")

    def http_bytes(self, interface: str, url: str, provider_symbol: str,
                   **kwargs: Any) -> DailySourceResult:
        return self._request(interface, provider_symbol, kind="http", target=url, **kwargs)

    def trading_days(self) -> DailySourceResult:
        return self._request("tool_trade_date_hist_sina", "CN")

    def option_daily(self, product_id: str, contract_code: str) -> DailySourceResult:
        interface = OPTION_INTERFACES.get(product_id)
        if interface is None:
            return DailySourceResult("UNSUPPORTED", None, "", contract_code,
                                     reason="product's per-contract daily endpoint was not verified")
        if product_id not in {"HO", "IO", "MO"}:
            # AKShare's implementation of this function uses this same Sina
            # URL. Parse the raw response here so a historical `null` has a
            # precise cause instead of a generic DataFrame-column error.
            url = ("https://stock.finance.sina.com.cn/futures/api/jsonp_v2.php//"
                   "StockOptionDaylineService.getSymbolInfo")
            result = self.http_bytes(interface, url, contract_code,
                params={"symbol": f"CON_OP_{contract_code}"},
                headers={"User-Agent": "Mozilla/5.0",
                         "Referer": "https://stock.finance.sina.com.cn/option/quotes.html"})
            if result.status != "OK":
                return result
            try:
                body = result.frame.decode("utf-8", errors="replace")
                payload = json.loads(body[body.find("(") + 1:body.rfind(")")])
                if not isinstance(payload, list) or not payload:
                    result.status, result.reason, result.frame = (
                        "SOURCE_UNAVAILABLE", "SINA_NO_HISTORICAL_ROWS_FOR_CONTRACT", None)
                    return result
                frame = pd.DataFrame(payload).rename(columns={
                    "d": "日期", "o": "开盘", "h": "最高", "l": "最低",
                    "c": "收盘", "v": "成交量"})
                result.frame = frame
                return result
            except (ValueError, TypeError, KeyError) as exc:
                result.status, result.reason, result.frame = (
                    "SOURCE_UNAVAILABLE", f"SINA_FORMAT_ERROR:{type(exc).__name__}", None)
                return result
        symbol = product_id.lower() + contract_code[len(product_id):]
        return self._request(interface, symbol, symbol=symbol)

    def etf_daily(self, product_id: str, start_date: str, end_date: str) -> DailySourceResult:
        interface = ETF_INTERFACES.get(product_id)
        if interface is None:
            return DailySourceResult("UNSUPPORTED", None, "", product_id,
                                     reason="unadjusted ETF daily endpoint not verified for this product")
        symbol = ("sh" if product_id.startswith(("5", "6")) else "sz") + product_id
        # This AKShare function expects hyphenated dates. Compact YYYYMMDD
        # makes it request every year up to the current year instead.
        return self._request(interface, symbol, symbol=symbol,
                             start_date=start_date, end_date=end_date,
                             adjust="", timeout=10)

    def index_daily(self, underlying_symbol: str) -> DailySourceResult:
        if not underlying_symbol.startswith("SSE."):
            return DailySourceResult("UNSUPPORTED", None, INDEX_INTERFACE, underlying_symbol,
                                     reason="index symbol has no verified Sina mapping")
        symbol = "sh" + underlying_symbol.split(".", 1)[1]
        return self._request(INDEX_INTERFACE, symbol, symbol=symbol)
