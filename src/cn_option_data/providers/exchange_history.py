"""Bounded historical files published by SSE, SZSE, and CFFEX.

These are dated exchange sources. In particular, the SZSE current-contract
report is never used as a historical roster.
"""

from __future__ import annotations

from io import BytesIO
import json
import warnings

import pandas as pd

from .akshare_daily import AkshareDailyProvider, DailySourceResult


SSE_QUERY = "https://query.sse.com.cn/commonQuery.do"
SZSE_REPORT = "https://www.szse.cn/api/report/ShowReport"
SSE_HEADERS = {"User-Agent": "Mozilla/5.0", "Referer": "https://www.sse.com.cn/"}
SZSE_HEADERS = {"User-Agent": "Mozilla/5.0", "Referer": "https://www.szse.cn/option/"}


class ExchangeHistoryProvider:
    def __init__(self, calls: AkshareDailyProvider):
        # Share one worker, one HTTP budget, and one rate limiter with AKShare.
        self.calls = calls

    @staticmethod
    def _decode(result: DailySourceResult, kind: str, required: set[str]) -> DailySourceResult:
        if result.status != "OK":
            return result
        try:
            if kind == "json":
                payload = json.loads(result.frame)
                rows = payload["result"]
                if not isinstance(rows, list):
                    raise ValueError("result is not a list")
                frame = pd.DataFrame(rows)
            elif kind == "xlsx":
                with warnings.catch_warnings():
                    warnings.filterwarnings("ignore", message="Workbook contains no default style")
                    frame = pd.read_excel(BytesIO(result.frame))
            elif kind == "csv":
                frame = pd.read_csv(BytesIO(result.frame), encoding="gbk")
            else:
                raise ValueError("unknown source format")
            if not frame.empty and not required.issubset(frame.columns):
                raise ValueError(f"missing columns: {sorted(required - set(frame.columns))}")
            result.frame = frame
        except (ValueError, KeyError, TypeError, UnicodeError, ImportError) as exc:
            result.status = "SOURCE_UNAVAILABLE"
            result.reason = f"SOURCE_FORMAT_ERROR:{type(exc).__name__}:{exc}"
            result.frame = None
        return result

    def sse_risk(self, day: str) -> DailySourceResult:
        result = self.calls.http_bytes(
            "sse_option_risk_daily", SSE_QUERY, day,
            params={"isPagination": "false", "trade_date": day.replace("-", ""),
                    "sqlId": "SSE_ZQPZ_YSP_GGQQZSXT_YSHQ_QQFXZB_DATE_L", "contractSymbol": ""},
            headers=SSE_HEADERS,
        )
        result = self._decode(result, "json", {"TRADE_DATE", "SECURITY_ID", "CONTRACT_ID"})
        if result.status == "OK" and not result.frame.empty:
            dates = set(result.frame["TRADE_DATE"].astype(str))
            if dates != {day}:
                result.status, result.reason = "SOURCE_UNAVAILABLE", f"SOURCE_DATE_MISMATCH:{sorted(dates)}"
                result.frame = None
        return result

    def sse_changes(self, day: str, *, adjustment: bool) -> DailySourceResult:
        interface = "sse_contract_adjustment" if adjustment else "sse_contract_listing"
        sql = ("SSE_ZQPZ_YSP_OPTZSXT_ADJUST_INFO_HYTZ_SEARCH_L" if adjustment
               else "SSE_ZQPZ_YSP_OPTZSXT_ADJUST_INFO_HYXG_SEARCH_L")
        result = self.calls.http_bytes(
            interface, SSE_QUERY, day,
            params={"isPagination": "false", "sqlId": sql,
                    "adjustDate": day.replace("-", ""), "securityCode": ""},
            headers=SSE_HEADERS,
        )
        result = self._decode(result, "json", {"ADJUST_DATE", "SECURITY_ID", "CONTRACT_ID",
                                               "EXERCISE_PRICE", "CONTRACT_UNIT", "EXPIRE_DATE"})
        if result.status == "OK" and not result.frame.empty:
            dates = set(pd.to_datetime(result.frame["ADJUST_DATE"], errors="coerce").dt.date.astype(str))
            if dates != {day}:
                result.status, result.reason = "SOURCE_UNAVAILABLE", f"SOURCE_DATE_MISMATCH:{sorted(dates)}"
                result.frame = None
        return result

    def szse_risk(self, day: str) -> DailySourceResult:
        result = self.calls.http_bytes(
            "szse_option_risk_daily", SZSE_REPORT, day,
            params={"SHOWTYPE": "xlsx", "CATALOGID": "option_hyfxzb", "TABKEY": "tab1",
                    "txtSearchDate": day}, headers=SZSE_HEADERS,
        )
        return self._decode(result, "xlsx", {"合约编码", "合约代码"})

    def szse_changes(self, tab: str) -> DailySourceResult:
        if tab not in {"tab1", "tab2", "tab3"}:
            raise ValueError(f"unknown SZSE change tab: {tab}")
        result = self.calls.http_bytes(
            f"szse_contract_changes_{tab}", SZSE_REPORT, tab,
            params={"SHOWTYPE": "xlsx", "CATALOGID": "option_hybg", "TABKEY": tab},
            headers=SZSE_HEADERS,
        )
        columns = {"合约编码", "挂牌日期", "行权价", "合约单位", "到期日"} if tab == "tab1" else (
            {"合约编码", "摘牌日期"} if tab == "tab2" else
            {"合约编码", "调整日期", "新合约代码", "新行权价", "新合约单位", "到期日"})
        return self._decode(result, "xlsx", columns)

    def cffex_daily(self, day: str) -> DailySourceResult:
        digits = day.replace("-", "")
        # The exchange's historical CSV endpoint was verified on three launch
        # dates; HTTPS failed in that bounded audit, HTTP served the file.
        url = f"http://www.cffex.com.cn/sj/hqsj/rtj/{digits[:6]}/{digits[6:]}/{digits}_1.csv"
        result = self.calls.http_bytes("cffex_daily_csv", url, day,
                                       headers={"User-Agent": "Mozilla/5.0"})
        return self._decode(result, "csv", {"合约代码", "今开盘", "最高价", "最低价", "成交量",
                                            "成交金额", "持仓量", "今收盘", "今结算"})
