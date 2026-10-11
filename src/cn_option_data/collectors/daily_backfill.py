"""Exchange-roster-driven daily history; independent of locally collected live data."""

from __future__ import annotations

import csv
import json
import math
import re
import signal
import uuid
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd

from ..config import default_project_root
from ..models import now_shanghai
from ..providers.akshare_daily import AkshareDailyProvider, DailySourceResult
from ..providers.bounded_calls import CallCancelled
from ..providers.exchange_history import ExchangeHistoryProvider
from ..registry import Product, load_registry
from ..storage.historical_writer import HistoricalParquetWriter, write_report_atomic


SSE_TRADE_CODE = re.compile(r"^(\d{6})([CP])\d{4}[A-Z](\d{5})$")
CFFEX_OPTION = re.compile(r"^(HO|IO|MO)(\d{4})-([CP])-(\d+(?:\.\d+)?)$")
CFFEX_FUTURE = re.compile(r"^(IH|IF|IC|IM)\d{4}$")
FUTURE_LISTING = {"IF": "2010-04-16", "IH": "2015-04-16",
                  "IC": "2015-04-16", "IM": "2022-07-22"}


def _date(value: Any) -> str | None:
    if value is None or pd.isna(value):
        return None
    value = str(value).strip()
    if not value or value in {"-", "nan", "NaT"}:
        return None
    if re.fullmatch(r"\d{8}", value):
        value = f"{value[:4]}-{value[4:6]}-{value[6:]}"
    try:
        return pd.Timestamp(value).date().isoformat()
    except (ValueError, TypeError, OverflowError):
        return None


def _number(value: Any) -> float | None:
    try:
        number = float(value)
        return number if math.isfinite(number) else None
    except (ValueError, TypeError):
        return None


def _count(value: Any) -> int | None:
    number = _number(value)
    return int(number) if number is not None and number >= 0 and number.is_integer() else None


def _code(value: Any) -> str:
    if value is None or pd.isna(value):
        return ""
    return str(value).strip().removesuffix(".0")


def _daily_values(frame: pd.DataFrame, interface: str) -> dict[str, dict[str, Any]]:
    chinese = interface in {"option_sse_daily_sina", "fund_etf_hist_em"}
    columns = {"open": "开盘", "high": "最高", "low": "最低", "close": "收盘", "volume": "成交量"} if chinese else {
        "open": "open", "high": "high", "low": "low", "close": "close", "volume": "volume"}
    day_column = "日期" if chinese else "date"
    missing = {day_column, *columns.values()} - set(frame.columns)
    if missing:
        raise ValueError(f"SOURCE_COLUMNS_MISSING:{sorted(missing)}")
    values: dict[str, dict[str, Any]] = {}
    for raw in frame.to_dict("records"):
        day = _date(raw.get(day_column))
        if day is None:
            continue
        row = {name: (_count(raw.get(column)) if name == "volume" else _number(raw.get(column)))
               for name, column in columns.items()}
        row["amount"] = (_number(raw.get("成交额")) if interface == "fund_etf_hist_em" else
                         _number(raw.get("amount")) if interface == "stock_zh_a_hist_tx" else None)
        if day in values:
            if values[day] == row:
                continue
            raise ValueError(f"CONFLICTING_SOURCE_DATE:{day}")
        values[day] = row
    return values


def _price_missing(values: dict[str, Any], *, expiry: str | None = None) -> str | None:
    missing = [f"{name.upper()}_MISSING" for name in ("open", "high", "low", "close", "volume")
               if values.get(name) is None]
    if values.get("close") is not None and values["close"] <= 0:
        missing.append("NONPOSITIVE_SOURCE_CLOSE")
    missing.extend(_ohlc_issues(values))
    if expiry is None:
        missing.append("EXPIRY_DATE_MISSING")
    return "|".join(missing) or None


def _ohlc_issues(values: dict[str, Any]) -> list[str]:
    """Flag source prices without changing the raw OHLC values."""
    opening, high, low, close = (values.get(key) for key in ("open", "high", "low", "close"))
    issues: list[str] = []
    if high is not None and low is not None and high < low:
        issues.append("OHLC_HIGH_BELOW_LOW")
    if high is not None and any(value is not None and value > high for value in (opening, close)):
        issues.append("OHLC_OPEN_OR_CLOSE_ABOVE_HIGH")
    if low is not None and any(value is not None and value < low for value in (opening, close)):
        issues.append("OHLC_OPEN_OR_CLOSE_BELOW_LOW")
    return issues


def _roster_row(day: str, product: Product, code: str, trading_code: str,
                source: str, result: DailySourceResult) -> dict[str, Any]:
    return {
        "trade_date": day, "exchange": product.exchange, "product_id": product.product_id,
        "contract_code": code, "trading_code": trading_code,
        "source": source, "source_interface": result.interface, "provider_symbol": code,
        "request_timestamp": result.requested_at, "received_timestamp": result.received_at,
        "missing_reason": None,
    }


def _terms_row(day: str, product: Product, code: str, trading_code: str,
               name: Any, strike: Any, expiry: Any, last_day: Any, unit: Any,
               source: str, result: DailySourceResult) -> dict[str, Any]:
    values = {"strike_price": _number(strike), "expiry_date": _date(expiry),
              "last_trading_date": _date(last_day), "volume_multiple": _number(unit)}
    missing = [f"{key.upper()}_MISSING" for key, value in values.items() if value is None]
    return {
        "trade_date": day, "exchange": product.exchange, "product_id": product.product_id,
        "contract_code": code, "contract_name": str(name) if name is not None else None,
        "underlying_symbol": product.underlying_symbol,
        "option_class": trading_code[6] if len(trading_code) > 6 else None,
        **values, "price_tick": None, "is_trading": True, "snapshot_timestamp": None,
        "source": source, "source_interface": result.interface,
        "provider_symbol": trading_code, "request_timestamp": result.requested_at,
        "received_timestamp": result.received_at,
        "missing_reason": "|".join(missing) or "PRICE_TICK_NOT_PROVIDED",
    }


def _terms_complete(row: dict[str, Any]) -> bool:
    return all(row.get(key) is not None for key in (
        "option_class", "strike_price", "expiry_date", "last_trading_date", "volume_multiple"))


def _sse_term_usable(roster: dict[str, Any], term: dict[str, Any] | None, day: str) -> bool:
    if term is None or not _terms_complete(term):
        return False
    return bool(
        term.get("exchange") == "SSE"
        and term.get("product_id") == roster.get("product_id")
        and str(term.get("contract_code")) == str(roster.get("contract_code"))
        and term.get("provider_symbol") == roster.get("trading_code")
        and term["option_class"] in {"C", "P"}
        and term["option_class"] == str(roster.get("trading_code"))[6:7]
        and term["strike_price"] > 0 and term["volume_multiple"] > 0
        and term["expiry_date"] >= day and term["last_trading_date"] >= day
        and term["last_trading_date"] <= term["expiry_date"]
    )


def _ak_option_row(day: str, product: Product, contract: dict[str, Any],
                   values: dict[str, Any], result: DailySourceResult) -> dict[str, Any]:
    expiry = contract.get("expiry_date")
    return {
        "trade_date": day, "exchange": product.exchange, "product_id": product.product_id,
        "contract_code": contract["contract_code"], "option_class": contract.get("option_class"),
        "strike_price": contract.get("strike_price"), "expiry_date": expiry,
        **values, "settlement": None, "open_interest": None, "price_type": "TRADE_OHLC",
        "source": "Sina" if result.interface == "option_sse_daily_sina" else "AKShare",
        "source_interface": result.interface,
        "provider_symbol": result.provider_symbol, "request_timestamp": result.requested_at,
        "received_timestamp": result.received_at,
        "missing_reason": "|".join(filter(None, (
            _price_missing(values, expiry=expiry), "SETTLEMENT_NOT_PROVIDED", "OPEN_INTEREST_NOT_PROVIDED"))),
    }


def _underlying_row(day: str, product: Product, values: dict[str, Any],
                    result: DailySourceResult) -> dict[str, Any]:
    is_etf = product.option_category == "ETF"
    return {
        "trade_date": day, "exchange": product.exchange, "product_id": product.product_id,
        "underlying_symbol": product.underlying_symbol,
        **values, "settlement": None, "open_interest": None,
        "price_type": "TRADE_OHLC" if is_etf else "INDEX_OHLC",
        "adjustment": "NONE" if is_etf else "NOT_APPLICABLE",
        "source": "AKShare", "source_interface": result.interface,
        "provider_symbol": result.provider_symbol, "request_timestamp": result.requested_at,
        "received_timestamp": result.received_at,
        "missing_reason": _price_missing(values, expiry=day),
    }


def _entry(product: Product, day: str) -> dict[str, Any]:
    return {
        "product_id": product.product_id, "trade_date": day, "exchange": product.exchange,
        "roster_status": "PENDING", "roster_source": None, "roster_contracts": 0,
        "roster_complete_verified": False, "roster_invalid_rows": 0,
        "option_status": "PENDING", "option_rows_new": 0, "option_rows_existing": 0,
        "option_missing_count": 0, "option_missing_examples": [],
        "nonpositive_close_rows": 0, "missing_close_rows": 0,
        "ohlc_inconsistent_rows": 0,
        "terms_status": "PENDING", "terms_rows_new": 0, "terms_rows_existing": 0,
        "terms_missing_count": None,
        "underlying_status": "PENDING", "underlying_rows_new": 0,
        "futures_status": "PENDING" if product.hedge_future else "NOT_APPLICABLE",
        "futures_contracts": 0, "futures_rows_new": 0,
        "rates_status": "UNSUPPORTED", "dividends_status": "UNSUPPORTED" if product.option_category == "ETF" else "NOT_APPLICABLE",
        "adjustments_status": "UNSUPPORTED" if product.option_category == "ETF" else "NOT_APPLICABLE",
        "reasons": [],
    }


def _reason(entry: dict[str, Any], reason: str | None) -> None:
    if reason and reason not in entry["reasons"]:
        entry["reasons"].append(reason[:250])


def _listing_dates(path: Path) -> dict[str, str]:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        return {row["product_id"]: row["listing_date"] for row in csv.DictReader(handle)}


def _previous_sse_terms(writer: HistoricalParquetWriter, first_day: str,
                        product_ids: set[str]) -> dict[str, dict[str, Any]]:
    """Seed each selected chain from its latest saved day, not another chain's day."""
    base = writer.root / "contract_terms_daily"
    pending = set(product_ids)
    terms: dict[str, dict[str, Any]] = {}
    if not pending:
        return terms
    for folder in sorted(base.glob("trade_date=*"), reverse=True):
        day = folder.name.removeprefix("trade_date=")
        if day < first_day:
            rows = writer.read_partition("contract_terms_daily", day, "SSE")
            for product_id in list(pending):
                matching = [row for row in rows if row.get("source") == "SSE"
                            and row.get("product_id") == product_id]
                if matching:
                    terms.update({str(row["contract_code"]): row for row in matching})
                    pending.remove(product_id)
            if not pending:
                break
    return terms


def _contract_from_roster(row: dict[str, Any], term: dict[str, Any] | None) -> dict[str, Any]:
    trading = str(row.get("trading_code") or "")
    contract: dict[str, Any] = {"contract_code": str(row["contract_code"]),
                                "option_class": trading[6] if len(trading) > 6 and trading[6] in "CP" else None,
                                "strike_price": None, "expiry_date": None}
    if term is not None and term.get("provider_symbol") == trading:
        contract["strike_price"] = term.get("strike_price")
        contract["expiry_date"] = term.get("expiry_date")
    elif row.get("exchange") == "SSE":
        match = SSE_TRADE_CODE.fullmatch(trading)
        if match:
            contract["strike_price"] = int(match.group(3)) / 1000
    return contract


def _append_adjustment(writer: HistoricalParquetWriter, product: Product, day: str,
                       code: str, old_strike: Any, new_strike: Any, old_unit: Any,
                       new_unit: Any, reason: Any, source: str,
                       result: DailySourceResult) -> None:
    writer.append("contract_adjustments", {
        "trade_date": day, "exchange": product.exchange, "product_id": product.product_id,
        "contract_code": code, "effective_date": day, "old_strike": _number(old_strike),
        "new_strike": _number(new_strike), "old_multiplier": _number(old_unit),
        "new_multiplier": _number(new_unit), "reason": str(reason) if reason is not None else None,
        "source": source, "source_interface": result.interface, "provider_symbol": code,
        "request_timestamp": result.requested_at, "received_timestamp": result.received_at,
        "missing_reason": None,
    })


def _szse_master(exchange: ExchangeHistoryProvider) -> tuple[dict[str, list[dict[str, Any]]], str | None]:
    """One historical listing/adjustment download, shared by all four SZSE chains."""
    results = {tab: exchange.szse_changes(tab) for tab in ("tab1", "tab2", "tab3")}
    bad = [f"{tab}:{result.status}:{result.reason}" for tab, result in results.items()
           if result.status != "OK"]
    if bad:
        return {}, ";".join(bad)
    events: dict[str, list[dict[str, Any]]] = defaultdict(list)
    delisted = {_code(row["合约编码"]): _date(row["摘牌日期"])
                for row in results["tab2"].frame.to_dict("records")}
    listing_result = results["tab1"]
    for raw in listing_result.frame.to_dict("records"):
        code, day = _code(raw.get("合约编码")), _date(raw.get("挂牌日期"))
        trading = str(raw.get("合约代码") or "").strip()
        if not code or day is None or not re.match(r"^159\d{3}[CP]", trading):
            continue
        events[code].append({
            "effective_date": day, "trading_code": trading,
            "name": raw.get("合约简称"), "strike": raw.get("行权价"),
            "unit": raw.get("合约单位"), "expiry": raw.get("到期日"),
            "last_day": raw.get("最后交易日"), "source_result": listing_result,
            "delisted": delisted.get(code),
        })
    adjustment_result = results["tab3"]
    for raw in adjustment_result.frame.to_dict("records"):
        code, day = _code(raw.get("合约编码")), _date(raw.get("调整日期"))
        trading = str(raw.get("新合约代码") or "").strip()
        if not code or day is None or not re.match(r"^159\d{3}[CP]", trading):
            continue
        events[code].append({
            "effective_date": day, "trading_code": trading,
            "name": raw.get("新合约简称"), "strike": raw.get("新行权价"),
            "unit": raw.get("新合约单位"), "expiry": raw.get("到期日"),
            "last_day": raw.get("最后交易日期"), "source_result": adjustment_result,
            "delisted": delisted.get(code),
            "old_strike": raw.get("原行权价"), "old_unit": raw.get("原合约单位"),
        })
    for values in events.values():
        values.sort(key=lambda item: item["effective_date"])
    return events, None


def _active_szse_term(events: dict[str, list[dict[str, Any]]], code: str,
                      trading: str, day: str) -> dict[str, Any] | None:
    candidates = [item for item in events.get(code, []) if item["effective_date"] <= day]
    if not candidates:
        return None
    latest = candidates[-1]
    if latest["trading_code"] != trading:
        return None
    last_day = _date(latest.get("last_day"))
    if last_day and day > last_day:
        return None
    delisted = latest.get("delisted")
    if delisted and day >= delisted:
        return None
    return latest


def _cffex_number(raw: dict[str, Any], column: str) -> float | None:
    return _number(raw.get(column))


def _cffex_values(raw: dict[str, Any]) -> dict[str, Any]:
    # The official daily table labels turnover as ten-thousand CNY.
    turnover = _cffex_number(raw, "成交金额")
    return {
        "open": _cffex_number(raw, "今开盘"), "high": _cffex_number(raw, "最高价"),
        "low": _cffex_number(raw, "最低价"), "close": _cffex_number(raw, "今收盘"),
        "settlement": _cffex_number(raw, "今结算"), "volume": _count(raw.get("成交量")),
        "open_interest": _count(raw.get("持仓量")),
        "amount": turnover * 10_000 if turnover is not None else None,
    }


def _cffex_price_row(day: str, product: Product, code: str, match: re.Match[str],
                     values: dict[str, Any], result: DailySourceResult) -> dict[str, Any]:
    return {
        "trade_date": day, "exchange": "CFFEX", "product_id": product.product_id,
        "contract_code": code, "option_class": match.group(3),
        "strike_price": float(match.group(4)), "expiry_date": None,
        **values, "price_type": "TRADE_OHLC_WITH_SETTLEMENT",
        "source": "CFFEX", "source_interface": result.interface,
        "provider_symbol": code, "request_timestamp": result.requested_at,
        "received_timestamp": result.received_at,
        "missing_reason": "|".join(filter(None, (
            _price_missing(values), "LAST_TRADING_DATE_NOT_IN_DAILY_CSV", "MULTIPLIER_NOT_IN_DAILY_CSV"))),
    }


def _future_row(day: str, code: str, future_product: str, values: dict[str, Any],
                result: DailySourceResult) -> dict[str, Any]:
    return {
        "trade_date": day, "exchange": "CFFEX", "future_symbol": code,
        "future_product": future_product, **values,
        "price_type": "TRADE_OHLC_WITH_SETTLEMENT", "source": "CFFEX",
        "source_interface": result.interface, "provider_symbol": code,
        "request_timestamp": result.requested_at, "received_timestamp": result.received_at,
        "missing_reason": _price_missing(values, expiry=day),
    }


def _fetch_option_price(provider: AkshareDailyProvider, cache: dict[tuple[str, str],
                        tuple[dict[str, dict[str, Any]], DailySourceResult]],
                        product: Product, contract: dict[str, Any], day: str,
                        writer: HistoricalParquetWriter, entry: dict[str, Any],
                        absences: dict[str, dict[str, str]], retry_absent_prices: bool,
                        source_cache_root: Path) -> None:
    code = contract["contract_code"]
    key = (day, product.product_id, code, "option_sse_daily_sina")
    if writer.contains("option_daily", day, product.exchange, key):
        entry["option_rows_existing"] += 1
        return
    contract_key = f"{product.product_id}|{code}"
    date_key = f"{contract_key}|{day}"
    if not retry_absent_prices and (contract_key in absences["contracts"] or
                                    date_key in absences["dates"]):
        entry["option_missing_count"] += 1
        if len(entry["option_missing_examples"]) < 10:
            entry["option_missing_examples"].append({"contract_code": code,
                "status": "SOURCE_ABSENT_CACHED", "reason":
                absences["contracts"].get(contract_key) or absences["dates"].get(date_key)})
        return
    cache_key = (product.product_id, code)
    if cache_key not in cache:
        source_path = source_cache_root / product.product_id / f"{code}.json"
        if source_path.exists():
            try:
                saved = json.loads(source_path.read_text(encoding="utf-8"))
                if saved["contract_code"] != code or saved["product_id"] != product.product_id:
                    raise ValueError("source cache identity mismatch")
                values = saved["daily_values"]
                if not isinstance(values, dict):
                    raise ValueError("source cache values are not a mapping")
                result = DailySourceResult("OK", None, saved["interface"], saved["provider_symbol"],
                                           datetime.fromisoformat(saved["requested_at"]),
                                           datetime.fromisoformat(saved["received_at"]))
            except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
                raise ValueError(f"invalid source cache: {source_path}") from exc
        else:
            result = provider.option_daily(product.product_id, code)
            try:
                values = _daily_values(result.frame, result.interface) if result.status == "OK" else {}
            except (ValueError, TypeError) as exc:
                result.status, result.reason, values = "SOURCE_UNAVAILABLE", str(exc), {}
            if result.status == "OK" and values:
                write_report_atomic(source_path, {
                    "product_id": product.product_id, "contract_code": code,
                    "interface": result.interface, "provider_symbol": result.provider_symbol,
                    "requested_at": result.requested_at.isoformat(),
                    "received_at": result.received_at.isoformat(), "daily_values": values,
                })
        cache[cache_key] = (values, result)
    values, result = cache[cache_key]
    if result.reason == "SINA_NO_HISTORICAL_ROWS_FOR_CONTRACT":
        absences["contracts"][contract_key] = result.reason
    elif result.status == "OK" and day not in values:
        absences["dates"][date_key] = "SINA_NO_DAILY_ROW_FOR_DATE"
    if day not in values:
        entry["option_missing_count"] += 1
        if len(entry["option_missing_examples"]) < 10:
            entry["option_missing_examples"].append({"contract_code": code,
                "status": result.status if result.status != "OK" else "NO_DAILY_ROW",
                "reason": result.reason or "NO_SOURCE_ROW_FOR_DATE"})
        return
    absences["contracts"].pop(contract_key, None)
    absences["dates"].pop(date_key, None)
    if writer.append("option_daily", _ak_option_row(day, product, contract, values[day], result)):
        entry["option_rows_new"] += 1
        entry["nonpositive_close_rows"] += int(values[day].get("close") is not None
                                              and values[day]["close"] <= 0)


def _fetch_underlying(provider: AkshareDailyProvider,
                      cache: dict[str, tuple[dict[str, dict[str, Any]], DailySourceResult]],
                      product: Product, day: str, start: str, end: str,
                      writer: HistoricalParquetWriter, entry: dict[str, Any]) -> None:
    interface = "stock_zh_a_hist_tx" if product.option_category == "ETF" else "stock_zh_index_daily"
    key = (day, product.product_id, product.underlying_symbol, interface)
    if writer.contains("underlying_daily", day, product.exchange, key):
        entry["underlying_status"] = "ALREADY_PRESENT"
        return
    if product.product_id not in cache:
        result = (provider.etf_daily(product.product_id, start, end) if product.option_category == "ETF"
                  else provider.index_daily(product.underlying_symbol))
        try:
            values = _daily_values(result.frame, result.interface) if result.status == "OK" else {}
        except (ValueError, TypeError) as exc:
            result.status, result.reason, values = "SOURCE_UNAVAILABLE", str(exc), {}
        cache[product.product_id] = (values, result)
    values, result = cache[product.product_id]
    if day not in values:
        entry["underlying_status"] = result.status if result.status != "OK" else "SOURCE_UNAVAILABLE"
        _reason(entry, result.reason or "UNDERLYING_DAILY_ROW_ABSENT")
        return
    if writer.append("underlying_daily", _underlying_row(day, product, values[day], result)):
        entry["underlying_rows_new"] += 1
    entry["underlying_status"] = ("OK" if values[day].get("close") and values[day]["close"] > 0
                                  and not _ohlc_issues(values[day]) else "PRICE_QUALITY_WARNING")


def _process_sse_day(day: str, products: list[Product], entries: dict[str, dict[str, Any]],
                     exchange: ExchangeHistoryProvider, writer: HistoricalParquetWriter,
                     prior_terms: dict[str, dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    rosters: dict[str, list[dict[str, Any]]] = {p.product_id: [] for p in products}
    persisted = writer.read_partition("roster_daily", day, "SSE")
    saved = {p.product_id: [row for row in persisted if row["product_id"] == p.product_id]
             for p in products}
    term_rows = {(row["product_id"], str(row["contract_code"])): row for row in
                 writer.read_partition("contract_terms_daily", day, "SSE")}
    saved_complete = all(saved.values()) and all(
        _sse_term_usable(row, term_rows.get((product.product_id, str(row["contract_code"]))), day)
        for product in products for row in saved[product.product_id])
    if saved_complete:
        for product in products:
            entry = entries[product.product_id]
            entry["roster_status"], entry["roster_source"] = "OK", "sse_option_risk_daily"
            entry["adjustments_status"] = "NOT_RECHECKED"
            entry["roster_contracts"] = len(saved[product.product_id])
            for row in saved[product.product_id]:
                term = term_rows[(product.product_id, str(row["contract_code"]))]
                entry["terms_rows_existing"] += 1
                prior_terms[str(row["contract_code"])] = term
                rosters[product.product_id].append(_contract_from_roster(row, term))
        return rosters

    use_saved_roster = all(saved.values())
    risk = None
    if use_saved_roster:
        raw_rows = [{"CONTRACT_ID": row.get("trading_code"), "SECURITY_ID": row.get("contract_code")}
                    for product in products for row in saved[product.product_id]]
    else:
        risk = exchange.sse_risk(day)
        if risk.status != "OK" or risk.frame.empty:
            for product in products:
                entries[product.product_id]["roster_status"] = risk.status if risk.status != "OK" else "SOURCE_UNAVAILABLE"
                _reason(entries[product.product_id], risk.reason or "SSE_DAILY_ROSTER_EMPTY")
            return rosters
        raw_rows = risk.frame.to_dict("records")
    saved_by_code = {(row["product_id"], str(row["contract_code"])): row
                     for product in products for row in saved[product.product_id]}
    unresolved_saved = any(
        not _sse_term_usable(row, term_rows.get((product.product_id, str(row["contract_code"]))), day)
        and not _sse_term_usable(row, prior_terms.get(str(row["contract_code"])), day)
        for product in products for row in saved[product.product_id])
    need_events = not use_saved_roster or unresolved_saved
    listing = exchange.sse_changes(day, adjustment=False) if need_events else None
    adjustments = exchange.sse_changes(day, adjustment=True) if need_events else None
    for product in products:
        entries[product.product_id]["adjustments_status"] = (
            "NOT_RECHECKED" if adjustments is None else
            "EXCHANGE_SOURCE_AVAILABLE" if adjustments.status == "OK" else adjustments.status)
    events: dict[str, tuple[dict[str, Any], DailySourceResult]] = {}
    for result in (listing, adjustments):
        if result is None:
            continue
        if result.status == "OK":
            events.update({_code(row["SECURITY_ID"]): (row, result)
                           for row in result.frame.to_dict("records")})
        else:
            for product in products:
                _reason(entries[product.product_id], f"{result.interface}:{result.status}:{result.reason}")
    by_id = {product.product_id: product for product in products}
    seen: set[str] = set()
    for raw in raw_rows:
        trading = str(raw.get("CONTRACT_ID") or "").strip()
        product = by_id.get(trading[:6])
        if product is None:
            continue
        entry = entries[product.product_id]
        code = _code(raw.get("SECURITY_ID"))
        if not re.fullmatch(r"\d{8}", code) or not SSE_TRADE_CODE.fullmatch(trading) or code in seen:
            entry["roster_invalid_rows"] += 1
            _reason(entry, f"SSE_INVALID_OR_DUPLICATE_CONTRACT:{code}")
            continue
        seen.add(code)
        roster = saved_by_code.get((product.product_id, code))
        if roster is None:
            assert risk is not None
            roster = _roster_row(day, product, code, trading, "SSE", risk)
            writer.append("roster_daily", roster)
        existing = term_rows.get((product.product_id, code))
        term = existing if _sse_term_usable(roster, existing, day) else None
        if existing is not None and term is None:
            _reason(entry, f"SSE_SAVED_TERMS_CONFLICT:{code}")
        if term is not None:
            entry["terms_rows_existing"] += 1
        elif existing is None:
            event = events.get(code)
            event_conflict = False
            if event is not None:
                changed, source_result = event
                if str(changed.get("CONTRACT_ID") or "").strip() == trading:
                    term = _terms_row(day, product, code, trading, changed.get("CONTRACT_SYMBOL"),
                        changed.get("EXERCISE_PRICE"), changed.get("EXPIRE_DATE"),
                        changed.get("END_DATE"), changed.get("CONTRACT_UNIT"), "SSE", source_result)
                    if source_result is adjustments:
                        _append_adjustment(writer, product, day, code, changed.get("ORG_EXERCISE_PRICE"),
                            changed.get("EXERCISE_PRICE"), changed.get("ORG_CONTRACT_UNIT"),
                            changed.get("CONTRACT_UNIT"), changed.get("UPDATE_TYPE"), "SSE", source_result)
                else:
                    event_conflict = True
                    _reason(entry, f"SSE_TERMS_CODE_CONFLICT:{code}")
            if term is None and not event_conflict:
                previous = prior_terms.get(code)
                if _sse_term_usable(roster, previous, day):
                    term = {**previous, "trade_date": day}
            if _sse_term_usable(roster, term, day):
                if writer.append("contract_terms_daily", term):
                    entry["terms_rows_new"] += 1
                else:
                    entry["terms_rows_existing"] += 1
            else:
                term = None
        if term is not None:
            prior_terms[code] = term
        else:
            _reason(entry, f"SSE_EXACT_TERMS_UNAVAILABLE:{code}")
        rosters[product.product_id].append(_contract_from_roster(roster, term))
    for product in products:
        entry = entries[product.product_id]
        entry["roster_contracts"] = len(rosters[product.product_id])
        entry["roster_status"] = "OK" if rosters[product.product_id] and not entry["roster_invalid_rows"] else (
            "ROSTER_MISMATCH" if entry["roster_invalid_rows"] else "SOURCE_UNAVAILABLE")
        entry["roster_source"] = "sse_option_risk_daily" if risk is None else risk.interface
        if not rosters[product.product_id]:
            _reason(entry, "NO_SSE_CONTRACTS_ON_LISTED_TRADING_DAY")
    return rosters


def _process_szse_day(day: str, products: list[Product], entries: dict[str, dict[str, Any]],
                      exchange: ExchangeHistoryProvider, writer: HistoricalParquetWriter,
                      master: dict[str, list[dict[str, Any]]]) -> dict[str, list[dict[str, Any]]]:
    rosters: dict[str, list[dict[str, Any]]] = {p.product_id: [] for p in products}
    persisted = writer.read_partition("roster_daily", day, "SZSE")
    saved = {p.product_id: [row for row in persisted if row["product_id"] == p.product_id]
             for p in products}
    term_rows = {str(row["contract_code"]): row for row in
                 writer.read_partition("contract_terms_daily", day, "SZSE")}
    if all(saved.values()):
        for product in products:
            entry = entries[product.product_id]
            entry["roster_status"], entry["roster_source"] = "OK", "szse_option_risk_daily"
            entry["adjustments_status"] = "EXCHANGE_SOURCE_AVAILABLE"
            entry["roster_contracts"] = len(saved[product.product_id])
            for row in saved[product.product_id]:
                term = term_rows.get(str(row["contract_code"]))
                entry["terms_rows_existing"] += int(term is not None)
                rosters[product.product_id].append(_contract_from_roster(row, term))
        return rosters
    risk = exchange.szse_risk(day)
    if risk.status != "OK" or risk.frame.empty:
        for product in products:
            entries[product.product_id]["roster_status"] = risk.status if risk.status != "OK" else "SOURCE_UNAVAILABLE"
            _reason(entries[product.product_id], risk.reason or "SZSE_DAILY_ROSTER_EMPTY")
        return rosters
    by_id = {product.product_id: product for product in products}
    seen: set[str] = set()
    for raw in risk.frame.to_dict("records"):
        trading = str(raw.get("合约代码") or "").strip()
        product = by_id.get(trading[:6])
        if product is None:
            continue
        entry = entries[product.product_id]
        code = _code(raw.get("合约编码"))
        if not re.fullmatch(r"\d{8}", code) or code in seen:
            entry["roster_invalid_rows"] += 1
            _reason(entry, f"SZSE_INVALID_OR_DUPLICATE_CONTRACT:{code}")
            continue
        seen.add(code)
        term_event = _active_szse_term(master, code, trading, day)
        if term_event is None:
            entry["roster_invalid_rows"] += 1
            _reason(entry, f"SZSE_ROSTER_TERMS_OR_DATE_CONFLICT:{code}")
            continue
        result = term_event["source_result"]
        term = _terms_row(day, product, code, trading, term_event["name"],
                          term_event["strike"], term_event["expiry"], term_event["last_day"],
                          term_event["unit"], "SZSE", result)
        if _terms_complete(term):
            if writer.append("contract_terms_daily", term):
                entry["terms_rows_new"] += 1
            else:
                entry["terms_rows_existing"] += 1
        else:
            _reason(entry, f"SZSE_EXACT_TERMS_UNAVAILABLE:{code}")
        if "old_strike" in term_event and term_event["effective_date"] == day:
            _append_adjustment(writer, product, day, code, term_event["old_strike"],
                term_event["strike"], term_event["old_unit"], term_event["unit"],
                "ETF_OPTION_CONTRACT_ADJUSTMENT", "SZSE", result)
        roster = _roster_row(day, product, code, trading, "SZSE", risk)
        writer.append("roster_daily", roster)
        rosters[product.product_id].append(_contract_from_roster(roster, term))
    for product in products:
        entry = entries[product.product_id]
        entry["roster_contracts"] = len(rosters[product.product_id])
        entry["roster_status"] = "OK" if rosters[product.product_id] and not entry["roster_invalid_rows"] else (
            "ROSTER_MISMATCH" if entry["roster_invalid_rows"] else "SOURCE_UNAVAILABLE")
        entry["roster_source"] = risk.interface
        entry["adjustments_status"] = "EXCHANGE_SOURCE_AVAILABLE"
        if not rosters[product.product_id]:
            _reason(entry, "NO_SZSE_CONTRACTS_ON_LISTED_TRADING_DAY")
    return rosters


def _process_cffex_day(day: str, products: list[Product], entries: dict[str, dict[str, Any]],
                       exchange: ExchangeHistoryProvider,
                       writer: HistoricalParquetWriter) -> None:
    relevant = {product.hedge_future for product in products
                if product.hedge_future and day >= FUTURE_LISTING[product.hedge_future]}
    option_products = {product.product_id: product for product in products if product.exchange == "CFFEX"}
    if not relevant and not option_products:
        for product in products:
            if product.hedge_future:
                entries[product.product_id]["futures_status"] = "NOT_LISTED"
        return
    result = exchange.cffex_daily(day)
    if result.status != "OK" or result.frame.empty:
        for product in products:
            entry = entries[product.product_id]
            if product.exchange == "CFFEX":
                entry["roster_status"] = result.status if result.status != "OK" else "SOURCE_UNAVAILABLE"
            if product.hedge_future:
                entry["futures_status"] = ("NOT_LISTED" if day < FUTURE_LISTING[product.hedge_future]
                                            else result.status if result.status != "OK" else "SOURCE_UNAVAILABLE")
            _reason(entry, result.reason or "CFFEX_DAILY_CSV_EMPTY")
        return
    for raw in result.frame.to_dict("records"):
        code = _code(raw.get("合约代码"))
        if code in {"小计", "合计"}:
            continue
        match = CFFEX_OPTION.fullmatch(code)
        if match and match.group(1) in option_products:
            product = option_products[match.group(1)]
            entry = entries[product.product_id]
            writer.append("roster_daily", _roster_row(day, product, code, code, "CFFEX", result))
            entry["roster_contracts"] += 1
            values = _cffex_values(raw)
            if writer.append("option_daily", _cffex_price_row(day, product, code, match, values, result)):
                entry["option_rows_new"] += 1
                entry["nonpositive_close_rows"] += int(values.get("close") is not None and values["close"] <= 0)
            else:
                entry["option_rows_existing"] += 1
        future = CFFEX_FUTURE.fullmatch(code)
        if future and code[:2] in relevant:
            values = _cffex_values(raw)
            created = writer.append("futures_daily", _future_row(day, code, code[:2], values, result))
            for product in products:
                if product.hedge_future == code[:2]:
                    entry = entries[product.product_id]
                    entry["futures_contracts"] += 1
                    entry["futures_rows_new"] += int(created)
    for product in products:
        entry = entries[product.product_id]
        if product.exchange == "CFFEX":
            entry["roster_source"] = result.interface
            entry["roster_status"] = "OK" if entry["roster_contracts"] else "SOURCE_UNAVAILABLE"
            entry["terms_status"] = "UNSUPPORTED"
            _reason(entry, "CFFEX_EXACT_EXPIRY_AND_CONTRACT_TERMS_NOT_IN_DAILY_CSV")
        if product.hedge_future:
            entry["futures_status"] = ("NOT_LISTED" if day < FUTURE_LISTING[product.hedge_future]
                                       else "OK" if entry["futures_contracts"] else "SOURCE_UNAVAILABLE")
            if day >= FUTURE_LISTING[product.hedge_future] and not entry["futures_contracts"]:
                _reason(entry, f"NO_{product.hedge_future}_FUTURES_IN_CFFEX_CSV")


def _finalize_entry(entry: dict[str, Any], *, request_limit: bool) -> None:
    found = entry["roster_contracts"]
    saved = entry["option_rows_new"] + entry["option_rows_existing"]
    entry["option_coverage_of_exchange_roster"] = round(saved / found, 6) if found else None
    if entry["option_status"] == "PENDING":
        if entry["roster_status"] != "OK":
            entry["option_status"] = entry["roster_status"]
        elif saved == found:
            entry["option_status"] = ("PRICE_QUALITY_WARNING" if
                                      entry["nonpositive_close_rows"] or entry["missing_close_rows"]
                                      or entry["ohlc_inconsistent_rows"] else "OK")
        else:
            entry["option_status"] = "REQUEST_LIMIT" if request_limit else "SOURCE_UNAVAILABLE"
            _reason(entry, "REQUEST_LIMIT_BEFORE_ALL_PRICES" if request_limit else
                    "NOT_ALL_DISCOVERED_CONTRACTS_HAVE_DAILY_PRICE")
    _finalize_terms(entry, request_limit=request_limit)
    if entry["underlying_status"] == "PENDING":
        entry["underlying_status"] = "NOT_REQUESTED"
    if entry["futures_status"] == "PENDING":
        entry["futures_status"] = "NOT_REQUESTED"


def _finalize_terms(entry: dict[str, Any], *, request_limit: bool) -> None:
    if entry["terms_status"] != "PENDING":
        return
    found = entry["roster_contracts"]
    complete = entry["terms_rows_new"] + entry["terms_rows_existing"]
    entry["terms_missing_count"] = max(0, found - complete)
    entry["terms_status"] = "OK" if found and complete == found else (
        "REQUEST_LIMIT" if request_limit else
        "SOURCE_UNAVAILABLE" if found else entry["roster_status"])


def run_backfill_daily(*, products_selection: str, start_date: str, end_date: str,
                       max_requests: int, execute: bool, project_root: Path | None = None,
                       output_root: Path | None = None,
                       retry_absent_prices: bool = False,
                       repair_sse_terms: bool = False) -> dict[str, Any]:
    root = (project_root or default_project_root()).resolve()
    if max_requests <= 0:
        raise ValueError("--max-requests must be positive")
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", start_date) or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", end_date):
        raise ValueError("start-date/end-date must be YYYY-MM-DD")
    start, end = date.fromisoformat(start_date), date.fromisoformat(end_date)
    if start > end:
        raise ValueError("start-date must not exceed end-date")
    if end >= now_shanghai().date():
        raise ValueError("daily history requires completed dates before today (Asia/Shanghai)")
    registry_path = root / "config" / "product_registry.csv"
    products = load_registry(registry_path, products_selection)
    if repair_sse_terms and any(product.exchange != "SSE" for product in products):
        raise ValueError("--repair-sse-terms requires only SSE products")
    if repair_sse_terms and retry_absent_prices:
        raise ValueError("--retry-absent-prices cannot be used with --repair-sse-terms")
    listed = _listing_dates(registry_path)
    output = (output_root or root / "data").resolve()
    report: dict[str, Any] = {
        "run_id": uuid.uuid4().hex, "started_at": now_shanghai().isoformat(),
        "status": "PREVIEW" if not execute else "RUNNING",
        "mode": "SSE_TERMS_REPAIR" if repair_sse_terms else "DAILY_BACKFILL",
        "start_date": start_date,
        "end_date": end_date, "products": [p.product_id for p in products],
        "max_http_requests": max_requests, "http_requests_used": 0,
        "history_root": str(output / "historical"), "coverage": [],
        "source_limits": {
            "rates": "UNSUPPORTED: historical risk-free curve not source-verified",
            "etf_dividends": "UNSUPPORTED: historical cash distributions not source-verified",
            "cffex_terms": "UNSUPPORTED: exact historical expiry is absent from verified daily CSV",
            "szse_early_prices": "Some verified old SZSE contract codes return null or omit early dates in Sina daily history",
        },
    }
    weekdays = [start + timedelta(days=i) for i in range((end - start).days + 1)
                if (start + timedelta(days=i)).weekday() < 5]
    if not execute:
        report["request_estimate"] = {
            "weekday_upper_bound_by_product": {
                p.product_id: sum(day.isoformat() >= listed[p.product_id] for day in weekdays)
                for p in products},
            "notes": ([
                "SSE terms repair reads saved rosters, requests dated exchange listing/adjustment records where terms are missing, and does not request option or underlying prices.",
                "If a long earlier date gap exists, start from the last continuous covered date; a single later day cannot recover events that occurred in the gap.",
            ] if repair_sse_terms else [
                "Dry-run makes no HTTP requests; exact trading days and historical contract count are unknown until discovery.",
                "SSE: up to three exchange calls per date, plus one Sina call per distinct contract whose price is not saved.",
                "SZSE: three historical change files per run, one dated roster per date, plus missing distinct-contract prices.",
                "CFFEX: one official daily CSV per date covers selected options and matching futures.",
                "AKShare ETF/index daily histories are fetched once per product per run when needed.",
            ]),
        }
        report["finished_at"] = now_shanghai().isoformat()
        return report

    writer = HistoricalParquetWriter(output / "historical")
    source_cache_root = output / "metadata" / "backfill_price_cache"
    absence_path = output / "metadata" / "backfill_absences.json"
    if absence_path.exists():
        loaded = json.loads(absence_path.read_text(encoding="utf-8"))
        if not isinstance(loaded, dict) or not all(isinstance(loaded.get(k), dict)
                                                  for k in ("contracts", "dates")):
            raise ValueError(f"invalid absence ledger: {absence_path}")
        absences: dict[str, dict[str, str]] = loaded
    else:
        absences = {"contracts": {}, "dates": {}}
    stopping = False
    interrupted = False
    fatal: BaseException | None = None
    old_handlers: dict[int, Any] = {}

    def on_stop(_signum: int, _frame: Any) -> None:
        nonlocal stopping
        stopping = True

    for signum in (signal.SIGINT, signal.SIGTERM):
        old_handlers[signum] = signal.getsignal(signum)
        signal.signal(signum, on_stop)
    provider: AkshareDailyProvider | None = None
    try:
        provider = AkshareDailyProvider(max_requests=max_requests, should_stop=lambda: stopping)
        exchange = ExchangeHistoryProvider(provider)
        calendar = provider.trading_days()
        if calendar.status != "OK" or "trade_date" not in calendar.frame.columns:
            report["status"] = "SOURCE_UNAVAILABLE"
            report["calendar_error"] = calendar.reason or "TRADE_CALENDAR_COLUMNS_MISSING"
            return report
        market_days = sorted({_date(value) for value in calendar.frame["trade_date"]
                              if _date(value) is not None})
        if not market_days or market_days[0] > start_date or market_days[-1] < end_date:
            report["status"] = "SOURCE_UNAVAILABLE"
            report["calendar_error"] = "TRADE_CALENDAR_DOES_NOT_COVER_REQUESTED_RANGE"
            return report
        days = [day for day in market_days if start_date <= day <= end_date]
        report["trading_days"] = len(days)
        report["prelisting_days_skipped"] = {
            p.product_id: sum(day < listed[p.product_id] for day in days) for p in products}
        report["calendar_source"] = calendar.interface
        entries: dict[tuple[str, str], dict[str, Any]] = {}
        for day in days:
            for product in products:
                if day >= listed[product.product_id]:
                    entry = _entry(product, day)
                    entries[(product.product_id, day)] = entry
                    report["coverage"].append(entry)
        if not days:
            report["status"] = "NO_TRADING_DAYS"
            return report
        if not report["coverage"]:
            report["status"] = "NO_LISTED_PRODUCTS"
            return report
        first_day = days[0]
        prior_sse_terms = _previous_sse_terms(
            writer, first_day, {p.product_id for p in products
                                if p.exchange == "SSE" and listed[p.product_id] < first_day})
        szse_events: dict[str, list[dict[str, Any]]] | None = None
        szse_master_error: str | None = None
        option_cache: dict[tuple[str, str], tuple[dict[str, dict[str, Any]], DailySourceResult]] = {}
        underlying_cache: dict[str, tuple[dict[str, dict[str, Any]], DailySourceResult]] = {}
        for day_number, day in enumerate(days):
            if stopping:
                interrupted = True
                break
            if provider.http_requests >= max_requests:
                report["stop_reason"] = "REQUEST_LIMIT"
                break
            active = [p for p in products if (p.product_id, day) in entries]
            if not active:
                continue
            current = {p.product_id: entries[(p.product_id, day)] for p in active}
            sse = [p for p in active if p.exchange == "SSE"]
            szse = [p for p in active if p.exchange == "SZSE"]
            sse_rosters: dict[str, list[dict[str, Any]]] = {}
            szse_rosters: dict[str, list[dict[str, Any]]] = {}
            if sse:
                sse_rosters = _process_sse_day(day, sse, current, exchange, writer, prior_sse_terms)
            if repair_sse_terms:
                writer.flush()
                for entry in current.values():
                    entry["option_status"] = "NOT_REQUESTED_TERMS_REPAIR"
                    entry["underlying_status"] = "NOT_REQUESTED_TERMS_REPAIR"
                    _finalize_terms(entry, request_limit=provider.http_requests >= max_requests)
                if provider.http_requests >= max_requests:
                    if day_number < len(days) - 1 or any(
                        entry["terms_status"] != "OK" for entry in current.values()
                    ):
                        report["stop_reason"] = "REQUEST_LIMIT"
                    break
                continue
            if szse and provider.http_requests < max_requests:
                saved_rosters = {row["product_id"] for row in writer.read_partition("roster_daily", day, "SZSE")}
                saved_terms = {row["product_id"] for row in writer.read_partition("contract_terms_daily", day, "SZSE")}
                cached_szse = all(product.product_id in saved_rosters and
                                  product.product_id in saved_terms for product in szse)
                if not cached_szse and szse_events is None and szse_master_error is None:
                    szse_events, szse_master_error = _szse_master(exchange)
                if szse_master_error:
                    for product in szse:
                        current[product.product_id]["roster_status"] = (
                            "REQUEST_LIMIT" if "REQUEST_LIMIT" in szse_master_error else "SOURCE_UNAVAILABLE")
                        _reason(current[product.product_id], f"SZSE_HISTORICAL_CHANGES:{szse_master_error}")
                else:
                    szse_rosters = _process_szse_day(day, szse, current, exchange, writer, szse_events or {})
            for product in active:
                entry = current[product.product_id]
                if provider.http_requests >= max_requests:
                    break
                _fetch_underlying(provider, underlying_cache, product, day, start_date, end_date, writer, entry)
            if provider.http_requests < max_requests:
                _process_cffex_day(day, active, current, exchange, writer)
            for product in sse + szse:
                entry = current[product.product_id]
                roster = (sse_rosters if product.exchange == "SSE" else szse_rosters).get(product.product_id, [])
                for contract in roster:
                    if provider.http_requests >= max_requests:
                        break
                    _fetch_option_price(provider, option_cache, product, contract, day,
                                        writer, entry, absences, retry_absent_prices,
                                        source_cache_root)
            writer.flush()
            for exchange_name in {product.exchange for product in active}:
                stored_prices = writer.read_partition("option_daily", day, exchange_name)
                for entry in current.values():
                    if entry["exchange"] != exchange_name:
                        continue
                    matching = [row for row in stored_prices if row["product_id"] == entry["product_id"]]
                    entry["nonpositive_close_rows"] = sum(
                        row.get("close") is not None and row["close"] <= 0 for row in matching)
                    entry["missing_close_rows"] = sum(row.get("close") is None for row in matching)
                    entry["ohlc_inconsistent_rows"] = sum(bool(_ohlc_issues(row)) for row in matching)
            for entry in current.values():
                _finalize_entry(entry, request_limit=provider.http_requests >= max_requests)
            if provider.http_requests >= max_requests:
                if day_number < len(days) - 1 or any(
                    entry["option_status"] in {"REQUEST_LIMIT", "PENDING"} or
                    entry["underlying_status"] in {"NOT_REQUESTED", "REQUEST_LIMIT"} or
                    entry["futures_status"] in {"NOT_REQUESTED", "REQUEST_LIMIT"} or
                    entry["roster_status"] == "REQUEST_LIMIT"
                    for entry in current.values()
                ):
                    report["stop_reason"] = "REQUEST_LIMIT"
                break
    except CallCancelled:
        interrupted = True
    except BaseException as exc:
        fatal = exc
    finally:
        try:
            writer.flush()
            write_report_atomic(absence_path, absences)
            for entry in report["coverage"]:
                if entry["option_status"] == "PENDING":
                    if interrupted or stopping:
                        entry["option_status"] = "INTERRUPTED"
                    elif provider is not None and provider.http_requests >= max_requests:
                        entry["option_status"] = "REQUEST_LIMIT"
                    else:
                        entry["option_status"] = "NOT_ATTEMPTED"
                if entry["roster_status"] == "PENDING":
                    entry["roster_status"] = entry["option_status"]
                if entry["terms_status"] == "PENDING":
                    entry["terms_status"] = entry["option_status"]
                if entry["underlying_status"] == "PENDING":
                    entry["underlying_status"] = entry["option_status"]
                if entry["futures_status"] == "PENDING":
                    entry["futures_status"] = entry["option_status"]
            report["http_requests_used"] = provider.http_requests if provider is not None else 0
            report["rows_written"] = dict(writer.rows_written)
            if fatal:
                report["status"] = "FAILED"
                report["fatal_error"] = f"{type(fatal).__name__}:{str(fatal)[:200]}"
            elif interrupted or stopping:
                report["status"] = "INTERRUPTED"
            elif report["status"] == "RUNNING":
                if repair_sse_terms:
                    good = all(entry["roster_status"] == "OK" and entry["terms_status"] == "OK"
                               for entry in report["coverage"])
                else:
                    good = all(entry["option_status"] in {"OK", "PRICE_QUALITY_WARNING"}
                               and entry["underlying_status"] in {"OK", "ALREADY_PRESENT", "PRICE_QUALITY_WARNING"}
                               and entry["terms_status"] == "OK"
                               and entry["futures_status"] in {"OK", "NOT_LISTED", "NOT_APPLICABLE"}
                               and entry["rates_status"] == "OK"
                               and entry["dividends_status"] in {"OK", "NOT_APPLICABLE"}
                               for entry in report["coverage"])
                report["status"] = "OK" if good and not report.get("stop_reason") else "PARTIAL"
            report["finished_at"] = now_shanghai().isoformat()
            write_report_atomic(output / "metadata" / "backfill_runs" / f"run-{report['run_id']}.json", report)
        finally:
            if provider is not None:
                provider.close()
            for signum, handler in old_handlers.items():
                signal.signal(signum, handler)
    if fatal:
        raise fatal
    return report
