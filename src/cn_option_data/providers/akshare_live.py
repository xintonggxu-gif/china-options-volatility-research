"""AKShare HTTP polling for the products selected from the CSV registry."""

from __future__ import annotations

import logging
import re
import json
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any, Callable

from ..models import OptionContract, SHANGHAI, source_datetime, finite_number, now_shanghai
from ..registry import Product
from .sina_quotes import SinaQuotes, cffex_option_symbol
from .bounded_calls import BoundedCalls, CallCancelled, NetworkCallError
from ..storage.parquet_writer import StorageWriteError
from ..trading_calendar import CalendarError


LOG = logging.getLogger(__name__)
ETF_CODE = re.compile(r"^(?P<underlying>\d{6})(?P<side>[CP])", re.I)
INDEX_CODE = re.compile(r"^(?P<product>HO|IO|MO)(?P<month>\d{4})[-_ ]?(?P<side>[CP])[-_ ]?(?P<strike>\d+)$", re.I)
FUTURE_CODE = re.compile(r"^(IH|IF|IC|IM)\d{4}$", re.I)
MONTH_CODE = re.compile(r"^(HO|IO|MO)(\d{4})$", re.I)
CFFEX_REFERENCE_URL = "http://dict.openctp.cn/instruments?types=option"
CFFEX_OPTION_MULTIPLIER = 100.0
CFFEX_OPTION_PRICE_TICK = 0.2
ETF_RULES = {
    "SSE": "https://www.sse.com.cn/lawandrules/sselawsrules2025/option/c/c_20250610_10781448.shtml",
    "SZSE": "https://docs.static.szse.cn/www/option/rules/optrules/W020191207434561721119.pdf",
}
# Exact provider identifiers observed in futures_symbol_mark on 2026-09-27.
# This adapts provider identifiers, not the registry's option-to-future selection.
FUTURE_MARKS = {"qz_qh": "IF", "szgz_qh": "IH", "zzgz_qh": "IC", "im_qh": "IM"}
CFFEX_APIS = {
    "HO": ("option_cffex_sz50_list_sina", "option_cffex_sz50_spot_sina"),
    "IO": ("option_cffex_hs300_list_sina", "option_cffex_hs300_spot_sina"),
    "MO": ("option_cffex_zz1000_list_sina", "option_cffex_zz1000_spot_sina"),
}


def _code(value: Any) -> str:
    """AKShare sometimes returns numeric contract IDs as floats."""
    if value is None:
        return ""
    text = str(value).strip()
    return text[:-2] if text.endswith(".0") and text[:-2].isdigit() else text


def _source_datetime(value: Any) -> datetime | None:
    return source_datetime(value)


def _quote_datetime(value: Any) -> datetime | None:
    """A date-only or midnight value is not an intraday quote timestamp."""
    parsed = _source_datetime(value)
    if parsed is not None and parsed.hour == parsed.minute == parsed.second == parsed.microsecond == 0:
        return None
    return parsed


def _index_code(value: Any) -> str | None:
    match = INDEX_CODE.fullmatch(_code(value).upper())
    if match is None:
        return None
    return f"{match['product']}{match['month']}{match['side']}{int(match['strike'])}"


def _date(value: Any) -> date | None:
    result = _source_datetime(value)
    return result.date() if result else None


def _record(row: Any) -> dict[str, Any]:
    return {str(key).strip(): value for key, value in row.items()}


def sina_underlying_symbol(product: Product) -> str:
    """Registry SSE.510050 -> sh510050; SZSE.159919 -> sz159919; SSE.000300 -> sh000300."""
    exchange, code = product.underlying_symbol.split(".", 1)
    return ("sh" if exchange == "SSE" else "sz") + code


@dataclass
class ChainQuotes:
    """Option quotes plus the underlying/futures observed in the same Sina responses.

    ``underlyings`` and ``futures`` hold (product_id, symbol, quote) triples; the
    quote's ``batch_id`` equals the batch_id of the options fetched with it.
    """
    options: dict[str, dict[str, Any]] = field(default_factory=dict)
    underlyings: list[tuple[str, str, dict[str, Any]]] = field(default_factory=list)
    futures: list[tuple[str, str, dict[str, Any]]] = field(default_factory=list)


class AKShareLive:
    def __init__(self, products: list[Product], should_stop: Callable[[], bool] | None = None,
                 on_wait: Callable[[], None] = lambda: None, request_timeout: float = 15.0):
        try:
            import akshare as ak
        except ImportError as exc:
            raise ImportError("请先安装 AKShare：python -m pip install -e .") from exc
        self.ak = ak
        self.products = products
        self.should_stop = should_stop or (lambda: False)
        self.errors: deque[str] = deque(maxlen=1000)
        self.by_id = {product.product_id: product for product in products}
        self.months: dict[str, set[str]] = {}
        self.diagnostics: dict[str, str] = {}
        self._future_market_names: list[str] | None = None
        # IF/IH/IC/IM -> contract symbols returned by the latest futures_zh_realtime call.
        self.future_symbols: dict[str, list[str]] = {}
        self._cffex_reference_date: date | None = None
        self._cffex_reference: dict[str, date] = {}
        self.calls = BoundedCalls(timeout=request_timeout, should_stop=self.should_stop, on_wait=on_wait)
        self.sina = SinaQuotes(self.should_stop, self.errors, self.calls)
        self._em_failures = 0
        self._em_retry_at = 0.0

    def close(self) -> None:
        self.calls.close()

    def _load_cffex_reference(self) -> dict[str, date]:
        """Refresh contract dates with the master list; quote polling does not depend on it."""
        today = now_shanghai().date()
        if self.should_stop():
            return self._cffex_reference
        if self._cffex_reference_date != today:
            self._cffex_reference = {}
        requested = now_shanghai()
        LOG.info("OpenCTP dispatch interface=%s dispatched_at=%s", CFFEX_REFERENCE_URL, requested.isoformat())
        try:
            content, requested, received = self.calls.call("http", CFFEX_REFERENCE_URL)
            LOG.info("OpenCTP result requested_at=%s received_at=%s", requested.isoformat(), received.isoformat())
            rows = json.loads(content)["data"]
            if not isinstance(rows, list):
                raise ValueError("data is not a list")
        except CallCancelled:
            return self._cffex_reference
        except (NetworkCallError, ValueError, KeyError, TypeError) as exc:
            LOG.warning("OpenCTP 合约资料不可用；中金所最后交易日保持空值: %s", exc)
            self.errors.append(f"option_contract_info_ctp: {exc}")
            return self._cffex_reference

        reference: dict[str, date] = {}
        conflicts: set[str] = set()
        for row in rows:
            if not isinstance(row, dict) or str(row.get("ExchangeID", "")).upper() != "CFFEX":
                continue
            code = _index_code(row.get("InstrumentID"))
            if code is None or not code.startswith(("HO", "IO", "MO")):
                continue
            match = INDEX_CODE.fullmatch(code)
            expiry = _date(_code(row.get("ExpireDate")))
            strike = finite_number(row.get("StrikePrice"), positive=True)
            multiplier = finite_number(row.get("VolumeMultiple"), positive=True)
            tick = finite_number(row.get("PriceTick"), positive=True)
            option_type = finite_number(row.get("OptionsType"), positive=True)
            if (match is None or expiry is None or expiry < today
                    or (expiry.year, expiry.month) != (2000 + int(match['month'][:2]), int(match['month'][2:]))
                    or strike != float(match['strike'])
                    or (row.get("VolumeMultiple") not in (None, "") and multiplier != CFFEX_OPTION_MULTIPLIER)
                    or (row.get("PriceTick") not in (None, "")
                        and (tick is None or abs(tick - CFFEX_OPTION_PRICE_TICK) > 1e-9))
                    or (row.get("OptionsType") not in (None, "")
                        and option_type != (1.0 if match['side'] == "C" else 2.0))):
                continue
            if code in reference and reference[code] != expiry:
                conflicts.add(code)
            else:
                reference[code] = expiry
        for code in conflicts:
            reference.pop(code, None)
        if not reference:
            LOG.warning("OpenCTP 未返回可核对的当前中金所期权合约；最后交易日保持空值")
            self.errors.append("option_contract_info_ctp: no verified active CFFEX options")
            return self._cffex_reference
        self._cffex_reference = reference
        self._cffex_reference_date = today
        LOG.info("OpenCTP 合约资料验证通过 %s 张；冲突 %s 张", len(reference), len(conflicts))
        return reference

    def _request(self, interface: str, **kwargs: Any) -> tuple[Any, datetime, datetime] | None:
        if self.should_stop():
            return None
        requested = now_shanghai()
        LOG.info("AKShare dispatch interface=%s dispatched_at=%s args=%s", interface, requested.isoformat(), kwargs)
        try:
            data, requested, received = self.calls.call("akshare", interface, **kwargs)
        except CallCancelled:
            return None
        except NetworkCallError as exc:
            LOG.warning("AKShare interface=%s dispatched_at=%s failed: %s", interface, requested.isoformat(), exc)
            self.errors.append(f"{interface}: {exc}")
            return None
        LOG.info("AKShare result interface=%s requested_at=%s received_at=%s", interface,
                 requested.isoformat(), received.isoformat())
        return data, requested, received

    def _rows(self, frame: Any, interface: str) -> list[dict[str, Any]]:
        try:
            return [_record(row) for row in frame.to_dict("records")]
        except Exception as exc:
            LOG.warning("AKShare interface=%s 返回数据无法解析: %s", interface, exc)
            self.errors.append(f"{interface}: invalid response: {exc}")
            return []

    def _etf_contracts(self, exchange: str, interface: str) -> dict[str, OptionContract]:
        answer = self._request(interface)
        if answer is None:
            for product in self.products:
                if product.exchange == exchange:
                    self.diagnostics[product.product_id] = "INTERFACE_ERROR"
            return {}
        frame, requested, _ = answer
        found: dict[str, OptionContract] = {}
        today = now_shanghai().date()
        rows = self._rows(frame, interface)
        if not rows:
            self.diagnostics.update({p.product_id: "INTERFACE_ERROR" for p in self.products if p.exchange == exchange})
            self.errors.append(f"{interface}: EMPTY_CONTRACT_MASTER")
            return {}
        for row in rows:
            trading_code = _code(row.get("合约交易代码" if exchange == "SSE" else "合约代码")).upper()
            match = ETF_CODE.match(trading_code)
            if match is None:
                continue
            product = self.by_id.get(match["underlying"])
            if product is None or product.exchange != exchange:
                continue
            contract_code = _code(row.get("合约编码"))
            if not contract_code.isdigit():
                continue
            expiry = _date(row.get("到期日"))
            last_day = _date(row.get("最后交易日"))
            if exchange == "SSE" and expiry is not None:
                exercise_day = _date(row.get("期权行权日"))
                # SSE rule art.9: last trading/exercise day follows the actual expiry.
                # Use today's master dates, never a weekday formula or contract month.
                if last_day is None and exercise_day == expiry:
                    last_day = expiry
                elif exercise_day is not None and exercise_day != expiry:
                    LOG.warning("SSE 日期冲突 contract=%s expiry=%s exercise=%s", contract_code, expiry, exercise_day)
                    self.errors.append(f"CONTRACT_DATE_CONFLICT: {contract_code}")
                    last_day = None
            if expiry is None or expiry < today or (last_day is not None and last_day < today):
                continue
            if exchange == "SZSE" and str(row.get("停牌", "")).strip().upper() in {"是", "Y", "YES", "TRUE", "1"}:
                continue
            strike = finite_number(row.get("行权价"), positive=True)
            if strike is None:
                continue
            underlying_text = str(row.get("标的券名称及代码" if exchange == "SSE" else "标的证券简称(代码)", ""))
            # The trading code is the authoritative exact product key; reject conflicting master metadata.
            embedded_codes = re.findall(r"(?<!\d)\d{6}(?!\d)", underlying_text)
            if embedded_codes and product.product_id not in embedded_codes:
                LOG.warning("合约标的冲突，跳过 contract=%s trading_code=%s", contract_code, trading_code)
                continue
            found[contract_code] = OptionContract(
                product=product,
                contract_code=contract_code,
                contract_name=str(row.get("合约简称") or trading_code),
                underlying_symbol=product.underlying_symbol,
                option_class=match["side"].upper(),
                strike_price=strike,
                expire_datetime=datetime.combine(expiry, datetime.min.time(), SHANGHAI),
                last_trading_date=datetime.combine(last_day, datetime.min.time(), SHANGHAI) if last_day else None,
                volume_multiple=finite_number(row.get("合约单位"), positive=True),
                # SSE art.59 / SZSE 4.3.18 apply to ETF option quotes, including adjusted units.
                price_tick=0.0001,
                provider_symbol=contract_code,
                source_interface=f"{interface}+{ETF_RULES[exchange]}",
                request_timestamp=requested,
            )
        return found

    def _cffex_months(self, product: Product) -> set[str]:
        list_api, _ = CFFEX_APIS[product.product_id]
        answer = self._request(list_api)
        if answer is None:
            self.diagnostics[product.product_id] = "INTERFACE_ERROR"
            return self.months.get(product.product_id, set())
        data, _, _ = answer
        candidates = [item for values in data.values() for item in values] if isinstance(data, dict) else []
        if not candidates:
            self.diagnostics[product.product_id] = "INTERFACE_ERROR"
            self.errors.append(f"{list_api}: EMPTY_CONTRACT_LIST")
            return self.months.get(product.product_id, set())
        today = now_shanghai().date()
        months = set()
        for item in candidates:
            symbol = str(item).strip().upper()
            match = MONTH_CODE.fullmatch(symbol)
            if match is None or match[1] != product.product_id:
                continue
            year, month = 2000 + int(match[2][:2]), int(match[2][2:])
            if 1 <= month <= 12 and (year, month) >= (today.year, today.month):
                months.add(symbol.lower())
        self.months[product.product_id] = months
        return months

    def _cffex_contracts(self, product: Product, reference: dict[str, date]) -> dict[str, OptionContract]:
        _, spot_api = CFFEX_APIS[product.product_id]
        found: dict[str, OptionContract] = {}
        for month in sorted(self._cffex_months(product)):
            if self.should_stop():
                break
            answer = self._request(spot_api, symbol=month)
            if answer is None:
                self.diagnostics[product.product_id] = "INTERFACE_ERROR"
                continue
            frame, requested, _ = answer
            rows = self._rows(frame, spot_api)
            if not rows:
                self.diagnostics[product.product_id] = "INTERFACE_ERROR"
                self.errors.append(f"{spot_api}: EMPTY_CONTRACT_MONTH {month}")
            for row in rows:
                strike = finite_number(row.get("行权价"), positive=True)
                for label, side in (("看涨", "C"), ("看跌", "P")):
                    raw_symbol = _code(row.get(f"{label}合约-标识"))
                    symbol = raw_symbol.upper()
                    match = INDEX_CODE.fullmatch(symbol)
                    if (match is None or match["product"] != product.product_id
                            or match["month"] != month[2:].upper() or match["side"] != side or strike is None):
                        continue
                    if finite_number(match["strike"]) != strike:
                        continue
                    last_day = reference.get(_index_code(symbol))
                    if last_day is not None and last_day < now_shanghai().date():
                        continue
                    # The source supplies a date, not an exact time. As for ETF contracts,
                    # midnight in these fields is a date-only storage convention.
                    last_date_value = datetime.combine(last_day, datetime.min.time(), SHANGHAI) if last_day else None
                    found[symbol] = OptionContract(
                        product=product, contract_code=symbol, contract_name=symbol,
                        underlying_symbol=product.underlying_symbol,
                        option_class=side, strike_price=strike, expire_datetime=last_date_value,
                        last_trading_date=last_date_value,
                        volume_multiple=CFFEX_OPTION_MULTIPLIER,
                        price_tick=CFFEX_OPTION_PRICE_TICK,
                        provider_symbol=raw_symbol,
                        source_interface=(f"{spot_api}+dict.openctp.cn/instruments" if last_day else spot_api),
                        request_timestamp=requested,
                    )
        return found

    def get_active_contracts(self) -> dict[str, OptionContract]:
        self.diagnostics = {}
        found: dict[str, OptionContract] = {}
        if any(product.exchange == "SSE" for product in self.products):
            try:
                found.update(self._etf_contracts("SSE", "option_current_day_sse"))
            except (StorageWriteError, CalendarError):
                raise
            except Exception as exc:
                LOG.warning("SSE 合约主表解析失败: %s", exc)
                self.errors.append(f"option_current_day_sse: {exc}")
                self.diagnostics.update({p.product_id: "INTERFACE_ERROR" for p in self.products if p.exchange == "SSE"})
        if any(product.exchange == "SZSE" for product in self.products):
            try:
                found.update(self._etf_contracts("SZSE", "option_current_day_szse"))
            except (StorageWriteError, CalendarError):
                raise
            except Exception as exc:
                LOG.warning("SZSE 合约主表解析失败: %s", exc)
                self.errors.append(f"option_current_day_szse: {exc}")
                self.diagnostics.update({p.product_id: "INTERFACE_ERROR" for p in self.products if p.exchange == "SZSE"})
        reference = self._load_cffex_reference() if any(p.exchange == "CFFEX" for p in self.products) else {}
        for product in self.products:
            if product.exchange == "CFFEX":
                try:
                    found.update(self._cffex_contracts(product, reference))
                except (StorageWriteError, CalendarError):
                    raise
                except Exception as exc:
                    LOG.warning("CFFEX product=%s 合约发现失败: %s", product.product_id, exc)
                    self.errors.append(f"{product.product_id} discovery: {exc}")
                    self.diagnostics[product.product_id] = "INTERFACE_ERROR"
        for product in self.products:
            if product.product_id not in self.diagnostics and not any(c.product.product_id == product.product_id for c in found.values()):
                self.diagnostics[product.product_id] = "INTERFACE_ERROR" if self.should_stop() else "NO_ACTIVE_CONTRACTS"
            if product.exchange == "CFFEX":
                count = sum(c.product.product_id == product.product_id for c in found.values())
                matched = sum(c.product.product_id == product.product_id and c.last_trading_date is not None
                              for c in found.values())
                LOG.info("中金所合约资料 product=%s matched=%s discovered=%s", product.product_id, matched, count)
                if count and matched < count:
                    LOG.warning("中金所合约资料不全 product=%s missing=%s", product.product_id, count - matched)
            contracts = [c for c in found.values() if c.product.product_id == product.product_id]
            if contracts:
                missing_terms = {name: [c.contract_code for c in contracts if getattr(c, name) is None]
                                 for name in ("expire_datetime", "last_trading_date", "volume_multiple", "price_tick")}
                LOG.info("合约资料 product=%s applied_at=%s source=%s missing=%s", product.product_id,
                         now_shanghai().date(), sorted({c.source_interface for c in contracts}), missing_terms)
        return found

    @staticmethod
    def _base_quote(interface: str, requested: datetime, received: datetime, source_time: Any = None) -> dict[str, Any]:
        parsed = _quote_datetime(source_time)
        return {
            "quote_timestamp": parsed, "source_quote_time": str(source_time).strip() if source_time is not None else None,
            "request_timestamp": requested, "received_timestamp": received,
            "source_interface": interface,
        }

    def _em_quotes(self) -> dict[str, tuple[dict[str, Any], datetime, datetime]]:
        if time.monotonic() < self._em_retry_at:
            LOG.warning("option_current_em COOLDOWN remaining_seconds=%.0f", self._em_retry_at - time.monotonic())
            return {}
        answer = self._request("option_current_em")
        rows = self._rows(answer[0], "option_current_em") if answer is not None else []
        if answer is None or not rows:
            if not self.should_stop():
                if answer is not None:
                    self.errors.append("option_current_em: EMPTY_OR_INVALID_RESPONSE")
                self._em_failures += 1
                if self._em_failures >= 3:
                    self._em_retry_at = time.monotonic() + 300
                    LOG.warning("option_current_em 连续失败=%s，暂停300秒", self._em_failures)
            return {}
        frame, requested, received = answer
        self._em_failures = 0
        self._em_retry_at = 0.0
        return {_code(row.get("代码")).upper(): (row, requested, received) for row in rows}

    @staticmethod
    def _option_symbol(contract: OptionContract) -> str:
        if contract.product.exchange == "CFFEX":
            return cffex_option_symbol(contract.contract_code)
        return "CON_OP_" + contract.contract_code

    def _anchor_symbols(self, product: Product) -> list[str]:
        """The product's underlying plus every listed month of its hedge future."""
        futures = self.future_symbols.get(product.hedge_future, []) if product.hedge_future else []
        return [sina_underlying_symbol(product), *("CFF_RE_" + symbol for symbol in futures)]

    def get_option_quotes(self, contracts: dict[str, OptionContract]) -> ChainQuotes:
        chain = ChainQuotes()
        if not contracts:
            return chain
        for product_id, status in list(self.diagnostics.items()):
            if status == "INVALID_QUOTE":
                self.diagnostics.pop(product_id)
        # One product's chain is requested together with its underlying and hedge
        # futures, so each option has an underlying observed in the same response
        # (shared batch_id). AKShare's single-contract SSE function uses this same
        # Sina endpoint; CFFEX P_OP_ fields were checked against the AKShare tables.
        for product in self.products:
            if self.should_stop():
                break
            members = {code: c for code, c in contracts.items() if c.product.product_id == product.product_id}
            if not members:
                continue
            symbol_of = {code: self._option_symbol(contract) for code, contract in members.items()}
            underlying = sina_underlying_symbol(product)
            anchors = self._anchor_symbols(product)
            results, anchor_rows = self.sina.fetch_bundled(anchors, list(symbol_of.values()))
            missing = [symbol for symbol in symbol_of.values() if symbol not in results]
            if 0 < len(missing) <= 60 and not self.should_stop():
                more, more_anchors = self.sina.fetch_bundled(anchors, missing, batch_size=20)
                results.update(more)
                anchor_rows += more_anchors
            session_by_batch: dict[str, date] = {}
            for symbol, item in anchor_rows:
                if symbol == underlying:
                    raw = self.sina.underlying(item.fields, symbol, item.requested, item.received)
                    target, key = chain.underlyings, product.underlying_symbol
                    session = _date(item.fields[30]) if len(item.fields) > 30 else None
                    if session is not None:
                        session_by_batch[item.batch_id] = session
                else:
                    raw = self.sina.future(item.fields, symbol, item.requested, item.received)
                    target, key = chain.futures, symbol.removeprefix("CFF_RE_")
                if raw is None:
                    continue
                raw["quote_timestamp"] = _quote_datetime(raw["quote_timestamp"])
                raw["batch_id"] = item.batch_id
                target.append((product.product_id, key, raw))
            cffex = product.exchange == "CFFEX"
            for code, contract in members.items():
                item = results.get(symbol_of[code])
                if item is None:
                    continue
                raw = self.sina.option(item.fields, symbol_of[code], underlying if cffex else product.product_id,
                                       contract.option_class, contract.strike_price, item.requested, item.received,
                                       # CFFEX turnover is price x contracts in index points; x100 gives CNY.
                                       amount_multiplier=CFFEX_OPTION_MULTIPLIER if cffex else 1.0)
                if raw is None:
                    LOG.warning("Sina 合约字段与主表不一致 contract=%s product=%s", code, product.product_id)
                    self.diagnostics[product.product_id] = "INVALID_QUOTE"
                    continue
                raw["quote_timestamp"] = _quote_datetime(raw["quote_timestamp"])
                raw["batch_id"] = item.batch_id
                self._mark_untraded(raw, _date(item.fields[32]), session_by_batch.get(item.batch_id))
                chain.options[code] = raw
        self._fill_missing_sse(contracts, chain.options)
        self._fill_missing_cffex(contracts, chain.options)
        return chain

    @staticmethod
    def _mark_untraded(raw: dict[str, Any], last_trade: date | None, session: date | None) -> None:
        """A contract with no trade this session keeps Sina's record from its last trading day.

        On CFFEX payloads the time, volume, turnover and price limits then belong to
        that earlier day (checked against daily bars and limit widths on 2026-09-30
        and 2026-10-09); the BBO is current. The session date comes from the
        underlying in the same response. A last trade before it means no trade now.
        """
        if last_trade is None or session is None or last_trade >= session:
            return
        raw.update({
            "session_date": session.isoformat(), "quote_timestamp": None,
            "volume": 0, "amount": 0.0, "upper_limit": None, "lower_limit": None,
        })

    def _fill_missing_sse(self, contracts: dict[str, OptionContract], quotes: dict[str, dict[str, Any]]) -> None:
        """Eastmoney remains a last-price-only fallback for missing SSE quotes."""
        missing = [code for code, c in contracts.items() if c.product.exchange == "SSE" and code not in quotes]
        if not missing or self.should_stop():
            return
        em = self._em_quotes()
        fallback_missing = []
        for code in missing:
            fallback = em.get(code.upper())
            if fallback is None:
                fallback_missing.append(code)
                continue
            row, requested, received = fallback
            quotes[code] = {
                **self._base_quote("option_current_em", requested, received),
                "last_price": row.get("最新价"), "volume": row.get("成交量"),
                "amount": row.get("成交额"), "open_interest": row.get("持仓量"),
                "provider_symbol": code,
            }
        if fallback_missing:
            LOG.warning("SSE 补充未取得报价 contracts=%s missing_fields=bid/ask/last/volume/amount/open_interest/time",
                        fallback_missing)

    def _fill_missing_cffex(self, contracts: dict[str, OptionContract], quotes: dict[str, dict[str, Any]]) -> None:
        """The AKShare T-table (BBO, last, open interest; no volume/time) backs up P_OP_."""
        missing = {code for code, c in contracts.items() if c.product.exchange == "CFFEX" and code not in quotes}
        if not missing:
            return
        LOG.warning("CFFEX P_OP 未取得报价，改用 AKShare 表 contracts=%s", sorted(missing))
        for month in sorted({code[:6].lower() for code in missing}):
            if self.should_stop():
                break
            _, spot_api = CFFEX_APIS[month[:2].upper()]
            answer = self._request(spot_api, symbol=month)
            if answer is None:
                continue
            frame, requested, received = answer
            for row in self._rows(frame, spot_api):
                for label in ("看涨", "看跌"):
                    raw_symbol = _code(row.get(f"{label}合约-标识"))
                    code = raw_symbol.upper()
                    if code not in missing:
                        continue
                    quotes[code] = {
                        **self._base_quote(spot_api, requested, received),
                        "bid_price1": row.get(f"{label}合约-买价"),
                        "bid_volume1": row.get(f"{label}合约-买量"),
                        "ask_price1": row.get(f"{label}合约-卖价"),
                        "ask_volume1": row.get(f"{label}合约-卖量"),
                        "last_price": row.get(f"{label}合约-最新价"),
                        "open_interest": row.get(f"{label}合约-持仓量"),
                        "provider_symbol": raw_symbol,
                    }

    def get_underlying_quotes(self) -> dict[str, dict[str, Any]]:
        quotes: dict[str, dict[str, Any]] = {}
        by_symbol = {sina_underlying_symbol(product): product.underlying_symbol for product in self.products}
        for symbol, item in self.sina.fetch(list(by_symbol)).items():
            raw = self.sina.underlying(item.fields, symbol, item.requested, item.received)
            if raw is None:
                continue
            raw["quote_timestamp"] = _quote_datetime(raw["quote_timestamp"])
            raw["batch_id"] = item.batch_id
            quotes[by_symbol[symbol]] = raw
        missing = set(by_symbol.values()) - quotes.keys()
        if missing and not self.should_stop():
            LOG.warning("标的缺失 symbols=%s", sorted(missing))
            self.errors.append(f"UNDERLYING_MISSING: {sorted(missing)}")
        return quotes

    def get_futures_quotes(self) -> dict[str, dict[str, Any]]:
        wanted = {p.hedge_future for p in self.products if p.hedge_future}
        if not wanted:
            return {}
        market_names = self._future_market_names
        if market_names is None:
            answer = self._request("futures_symbol_mark")
            if answer is None:
                return {}
            frame, _, _ = answer
            names = {}
            for row in self._rows(frame, "futures_symbol_mark"):
                code = FUTURE_MARKS.get(str(row.get("mark", "")))
                if (code in wanted and row.get("exchange") == "中国金融期货交易所" and row.get("symbol")):
                    names[code] = str(row["symbol"])
            market_names = [names[code] for code in sorted(wanted) if code in names]
            if wanted - names.keys():
                self.errors.append(f"FUTURE_MAPPING_MISSING: {sorted(wanted - names.keys())}")
                LOG.warning("期货名称表缺少明确映射 products=%s", sorted(wanted - names.keys()))
                # Retry mapping later, while still requesting the successfully mapped products now.
                self._future_market_names = None
            else:
                self._future_market_names = market_names
            LOG.info("期货请求范围 products=%s names=%s", sorted(wanted), market_names)
        quotes: dict[str, dict[str, Any]] = {}
        for market_name in market_names:
            if self.should_stop():
                break
            answer = self._request("futures_zh_realtime", symbol=market_name)
            if answer is None:
                continue
            frame, requested, received = answer
            for row in self._rows(frame, "futures_zh_realtime"):
                symbol = _code(row.get("symbol")).upper()
                match = FUTURE_CODE.fullmatch(symbol)
                if match is None or match[1].upper() not in wanted or str(row.get("exchange", "")).lower() != "cffex":
                    continue
                timestamp = f"{row.get('tradedate', '')} {row.get('ticktime', '')}"
                quotes[symbol] = {
                    **self._base_quote("futures_zh_realtime", requested, received, timestamp),
                    "bid_price1": row.get("bidprice1"), "bid_volume1": row.get("bidvol1"),
                    "ask_price1": row.get("askprice1"), "ask_volume1": row.get("askvol1"),
                    "last_price": row.get("trade"), "volume": row.get("volume"),
                    "open_interest": row.get("position"), "provider_symbol": symbol,
                }
        returned: dict[str, list[str]] = {}
        for symbol in sorted(quotes):
            returned.setdefault(symbol[:2], []).append(symbol)
        # Keep the last known months for a product whose request failed this cycle.
        self.future_symbols.update(returned)
        missing_products = wanted - {symbol[:2] for symbol in quotes}
        if missing_products and not self.should_stop():
            LOG.warning("期货品种未返回 products=%s", sorted(missing_products))
            self.errors.append(f"FUTURES_MISSING: {sorted(missing_products)}")
        return quotes
