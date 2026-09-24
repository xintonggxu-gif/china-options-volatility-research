"""AKShare HTTP polling for the products selected from the CSV registry."""

from __future__ import annotations

import logging
import re
from datetime import date, datetime
from typing import Any, Callable

from ..models import OptionContract, SHANGHAI, exchange_timestamp, finite_number, now_shanghai
from ..registry import Product
from .sina_quotes import SinaQuotes


LOG = logging.getLogger(__name__)
ETF_CODE = re.compile(r"^(?P<underlying>\d{6})(?P<side>[CP])", re.I)
INDEX_CODE = re.compile(r"^(?P<product>HO|IO|MO)(?P<month>\d{4})[-_ ]?(?P<side>[CP])[-_ ]?(?P<strike>\d+)$", re.I)
FUTURE_CODE = re.compile(r"^(IH|IF|IC|IM)\d{4}$", re.I)
MONTH_CODE = re.compile(r"^(HO|IO|MO)(\d{4})$", re.I)
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
    if value is None or str(value) in {"NaT", "nan", "None"}:
        return None
    if isinstance(value, datetime):
        return value.replace(tzinfo=SHANGHAI) if value.tzinfo is None else value.astimezone(SHANGHAI)
    if isinstance(value, date):
        return datetime(value.year, value.month, value.day, tzinfo=SHANGHAI)
    text = str(value).strip()
    if re.fullmatch(r"\d{8}", text):
        text = f"{text[:4]}-{text[4:6]}-{text[6:]}"
    elif re.match(r"^\d{8}\s", text):
        text = f"{text[:4]}-{text[4:6]}-{text[6:]}"
    # A time without a source date is kept separately, never dated using the local clock.
    if not re.match(r"^\d{4}[-/]\d{1,2}[-/]\d{1,2}", text):
        return None
    return exchange_timestamp(text.replace("/", "-"))


def _date(value: Any) -> date | None:
    result = _source_datetime(value)
    return result.date() if result else None


def _record(row: Any) -> dict[str, Any]:
    return {str(key).strip(): value for key, value in row.items()}


class AKShareLive:
    def __init__(self, products: list[Product], should_stop: Callable[[], bool] | None = None):
        try:
            import akshare as ak
        except ImportError as exc:
            raise ImportError("请先安装 AKShare：python -m pip install -e .") from exc
        self.ak = ak
        self.products = products
        self.should_stop = should_stop or (lambda: False)
        self.errors: list[str] = []
        self.by_id = {product.product_id: product for product in products}
        self.months: dict[str, set[str]] = {}
        self.diagnostics: dict[str, str] = {}
        self._prefetched: dict[str, tuple[Any, datetime, datetime]] = {}
        self._future_market_names: list[str] | None = None
        self.sina = SinaQuotes(self.should_stop, self.errors)

    def _request(self, interface: str, **kwargs: Any) -> tuple[Any, datetime, datetime] | None:
        if self.should_stop():
            return None
        requested = now_shanghai()
        LOG.info("AKShare request interface=%s requested_at=%s args=%s", interface, requested.isoformat(), kwargs)
        try:
            data = getattr(self.ak, interface)(**kwargs)
        except Exception as exc:
            LOG.warning("AKShare interface=%s requested_at=%s failed: %s", interface, requested.isoformat(), exc)
            self.errors.append(f"{interface}: {exc}")
            return None
        received = now_shanghai()
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
        for row in self._rows(frame, interface):
            if self.should_stop():
                break
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
                price_tick=None,
                provider_symbol=contract_code,
                source_interface=interface,
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

    def _cffex_contracts(self, product: Product) -> dict[str, OptionContract]:
        _, spot_api = CFFEX_APIS[product.product_id]
        found: dict[str, OptionContract] = {}
        for month in sorted(self._cffex_months(product)):
            if self.should_stop():
                break
            answer = self._request(spot_api, symbol=month)
            if answer is None:
                self.diagnostics[product.product_id] = "INTERFACE_ERROR"
                continue
            self._prefetched[month] = answer
            frame, requested, _ = answer
            for row in self._rows(frame, spot_api):
                strike = finite_number(row.get("行权价"), positive=True)
                for label, side in (("看涨", "C"), ("看跌", "P")):
                    raw_symbol = _code(row.get(f"{label}合约-标识"))
                    symbol = raw_symbol.upper()
                    match = INDEX_CODE.fullmatch(symbol)
                    if match is None or match["product"] != product.product_id or match["side"] != side or strike is None:
                        continue
                    if finite_number(match["strike"]) != strike:
                        continue
                    # Month is known from the code, but an exact expiry day is not supplied.
                    found[symbol] = OptionContract(
                        product=product, contract_code=symbol, contract_name=symbol,
                        underlying_symbol=product.underlying_symbol,
                        option_class=side, strike_price=strike, expire_datetime=None,
                        last_trading_date=None, volume_multiple=None, price_tick=None,
                        provider_symbol=raw_symbol,
                        source_interface=spot_api, request_timestamp=requested,
                    )
        return found

    def get_active_contracts(self) -> dict[str, OptionContract]:
        self.diagnostics = {}
        found: dict[str, OptionContract] = {}
        if any(product.exchange == "SSE" for product in self.products):
            try:
                found.update(self._etf_contracts("SSE", "option_current_day_sse"))
            except Exception as exc:
                LOG.warning("SSE 合约主表解析失败: %s", exc)
                self.errors.append(f"option_current_day_sse: {exc}")
                self.diagnostics.update({p.product_id: "INTERFACE_ERROR" for p in self.products if p.exchange == "SSE"})
        if any(product.exchange == "SZSE" for product in self.products):
            try:
                found.update(self._etf_contracts("SZSE", "option_current_day_szse"))
            except Exception as exc:
                LOG.warning("SZSE 合约主表解析失败: %s", exc)
                self.errors.append(f"option_current_day_szse: {exc}")
                self.diagnostics.update({p.product_id: "INTERFACE_ERROR" for p in self.products if p.exchange == "SZSE"})
        for product in self.products:
            if product.exchange == "CFFEX":
                try:
                    found.update(self._cffex_contracts(product))
                except Exception as exc:
                    LOG.warning("CFFEX product=%s 合约发现失败: %s", product.product_id, exc)
                    self.errors.append(f"{product.product_id} discovery: {exc}")
                    self.diagnostics[product.product_id] = "INTERFACE_ERROR"
        for product in self.products:
            if product.product_id not in self.diagnostics and not any(c.product.product_id == product.product_id for c in found.values()):
                self.diagnostics[product.product_id] = "NO_ACTIVE_CONTRACTS"
        return found

    @staticmethod
    def _base_quote(interface: str, requested: datetime, received: datetime, source_time: Any = None) -> dict[str, Any]:
        parsed = _source_datetime(source_time)
        return {
            "quote_timestamp": parsed, "source_quote_time": str(source_time).strip() if source_time is not None else None,
            "request_timestamp": requested, "received_timestamp": received,
            "source_interface": interface,
        }

    def _em_quotes(self) -> dict[str, tuple[dict[str, Any], datetime, datetime]]:
        answer = self._request("option_current_em")
        if answer is None:
            for product in self.products:
                if product.exchange == "SZSE":
                    self.diagnostics[product.product_id] = "INTERFACE_ERROR"
            return {}
        frame, requested, received = answer
        return {_code(row.get("代码")).upper(): (row, requested, received) for row in self._rows(frame, "option_current_em")}

    def get_option_quotes(self, contracts: dict[str, OptionContract]) -> dict[str, dict[str, Any]]:
        quotes: dict[str, dict[str, Any]] = {}
        if not contracts:
            return quotes
        # Eastmoney is only a fallback for a failed SSE quote. It must not block
        # SZSE or CFFEX collection when its bulk endpoint is unavailable.
        em: dict[str, tuple[dict[str, Any], datetime, datetime]] | None = None
        szse = {code: c for code, c in contracts.items() if c.product.exchange == "SZSE"}
        sina = self.sina.fetch(["CON_OP_" + code for code in szse]) if szse else {}
        for code, contract in contracts.items():
            if self.should_stop():
                break
            if contract.product.exchange == "SZSE":
                item = sina.get("CON_OP_" + code)
                if item is not None:
                    fields, requested, received = item
                    raw = self.sina.option(fields, code, contract.product.product_id,
                                           contract.option_class, contract.strike_price, requested, received)
                    if raw is not None:
                        raw["quote_timestamp"] = _source_datetime(raw["quote_timestamp"])
                        quotes[code] = raw
                    else:
                        LOG.warning("Sina 合约字段与主表不一致 contract=%s product=%s", code, contract.product.product_id)
                        self.diagnostics[contract.product.product_id] = "INVALID_QUOTE"
            elif contract.product.exchange == "SSE":
                answer = self._request("option_sse_spot_price_sina", symbol=code)
                if answer is None:
                    if em is None:
                        em = self._em_quotes()
                    item = em.get(code.upper())
                    if item is None:
                        continue
                    row, requested, received = item
                    quotes[code] = {
                        **self._base_quote("option_current_em", requested, received),
                        "last_price": row.get("最新价"), "volume": row.get("成交量"),
                        "amount": row.get("成交额"), "open_interest": row.get("持仓量"),
                        "provider_symbol": code,
                    }
                    continue
                frame, requested, received = answer
                fields = {str(row.get("字段", "")).strip(): row.get("值") for row in self._rows(frame, "option_sse_spot_price_sina")}
                if not fields.get("最新价") and not fields.get("申买价一") and not fields.get("申卖价一"):
                    if em is None:
                        em = self._em_quotes()
                    item = em.get(code.upper())
                    if item is None:
                        continue
                    row, requested, received = item
                    quotes[code] = {
                        **self._base_quote("option_current_em", requested, received),
                        "last_price": row.get("最新价"), "volume": row.get("成交量"),
                        "amount": row.get("成交额"), "open_interest": row.get("持仓量"),
                        "provider_symbol": code,
                    }
                    continue
                quotes[code] = {
                    **self._base_quote("option_sse_spot_price_sina", requested, received, fields.get("行情时间")),
                    "bid_price1": fields.get("申买价一"), "bid_volume1": fields.get("申买量一"),
                    "ask_price1": fields.get("申卖价一"), "ask_volume1": fields.get("申卖量一"),
                    "last_price": fields.get("最新价"), "volume": fields.get("成交量"),
                    "amount": fields.get("成交额"), "open_interest": fields.get("持仓量"),
                    "upper_limit": fields.get("涨停价"), "lower_limit": fields.get("跌停价"),
                    "provider_symbol": code,
                }
        for product_id, months in self.months.items():
            if self.should_stop():
                break
            _, spot_api = CFFEX_APIS[product_id]
            for month in sorted(months):
                answer = self._prefetched.pop(month, None) or self._request(spot_api, symbol=month)
                if answer is None:
                    continue
                frame, requested, received = answer
                for row in self._rows(frame, spot_api):
                    for label in ("看涨", "看跌"):
                        raw_symbol = _code(row.get(f"{label}合约-标识"))
                        code = raw_symbol.upper()
                        if code not in contracts:
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
        return quotes

    def get_underlying_quotes(self) -> dict[str, dict[str, Any]]:
        quotes: dict[str, dict[str, Any]] = {}
        by_symbol = {}
        for product in self.products:
            exchange, code = product.underlying_symbol.split(".", 1)
            by_symbol[("sh" if exchange == "SSE" else "sz") + code] = product.underlying_symbol
        for symbol, (fields, requested, received) in self.sina.fetch(list(by_symbol)).items():
            raw = self.sina.underlying(fields, symbol, requested, received)
            if raw is None:
                continue
            raw["quote_timestamp"] = _source_datetime(raw["quote_timestamp"])
            quotes[by_symbol[symbol]] = raw
        return quotes

    def get_futures_quotes(self) -> dict[str, dict[str, Any]]:
        wanted = {p.hedge_future for p in self.products if p.hedge_future}
        if not wanted:
            return {}
        if self._future_market_names is None:
            answer = self._request("futures_symbol_mark")
            if answer is None:
                return {}
            frame, _, _ = answer
            self._future_market_names = [
                str(row["symbol"]) for row in self._rows(frame, "futures_symbol_mark")
                if row.get("symbol") is not None
                if any(token in str(row.get("exchange", "")).lower() for token in ("cffex", "中金", "金融期货"))
            ]
        quotes: dict[str, dict[str, Any]] = {}
        for market_name in self._future_market_names:
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
                timestamp = _source_datetime(f"{row.get('tradedate', '')} {row.get('ticktime', '')}")
                quotes[symbol] = {
                    **self._base_quote("futures_zh_realtime", requested, received, timestamp),
                    "bid_price1": row.get("bidprice1"), "bid_volume1": row.get("bidvol1"),
                    "ask_price1": row.get("askprice1"), "ask_volume1": row.get("askvol1"),
                    "last_price": row.get("trade"), "volume": row.get("volume"),
                    "open_interest": row.get("position"), "provider_symbol": symbol,
                }
        return quotes
