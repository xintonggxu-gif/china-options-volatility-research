"""Read-only quality audit for stored daily history; raw Parquet is never changed."""

from __future__ import annotations

import argparse
import json
from collections import Counter, defaultdict
from datetime import date
from pathlib import Path
from typing import Any

from ..config import default_project_root
from ..registry import load_registry
from ..storage.historical_writer import HistoricalParquetWriter, write_report_atomic
from .daily_backfill import _ohlc_issues, _sse_term_usable


def _stored_days(root: Path, start: str, end: str) -> list[str]:
    days: set[str] = set()
    for table in ("roster_daily", "option_daily", "contract_terms_daily", "underlying_daily"):
        for folder in (root / table).glob("trade_date=*"):
            day = folder.name.removeprefix("trade_date=")
            if start <= day <= end:
                days.add(day)
    return sorted(days)


def audit_historical_quality(*, products_selection: str, start_date: str,
                             end_date: str, project_root: Path | None = None,
                             output_root: Path | None = None,
                             report_path: Path | None = None) -> dict[str, Any]:
    start, end = date.fromisoformat(start_date), date.fromisoformat(end_date)
    if start > end:
        raise ValueError("start-date must not exceed end-date")
    root = (project_root or default_project_root()).resolve()
    products = load_registry(root / "config" / "product_registry.csv", products_selection)
    data_root = (output_root or root / "data").resolve()
    writer = HistoricalParquetWriter(data_root / "historical")
    days = _stored_days(writer.root, start_date, end_date)
    totals: Counter[str] = Counter()
    coverage: list[dict[str, Any]] = []
    by_exchange: dict[str, list[Any]] = defaultdict(list)
    for product in products:
        by_exchange[product.exchange].append(product)

    for day in days:
        for exchange, exchange_products in by_exchange.items():
            rosters = writer.read_partition("roster_daily", day, exchange)
            prices = writer.read_partition("option_daily", day, exchange)
            terms = writer.read_partition("contract_terms_daily", day, exchange)
            underlyings = writer.read_partition("underlying_daily", day, exchange)
            for product in exchange_products:
                pid = product.product_id
                roster_rows = [row for row in rosters if row["product_id"] == pid]
                price_rows = [row for row in prices if row["product_id"] == pid]
                term_rows = [row for row in terms if row["product_id"] == pid]
                underlying_rows = [row for row in underlyings if row["product_id"] == pid]
                if not any((roster_rows, price_rows, term_rows, underlying_rows)):
                    continue
                roster_by_code = {str(row["contract_code"]): row for row in roster_rows}
                term_by_code = {str(row["contract_code"]): row for row in term_rows}
                valid_underlying = any(row.get("close") is not None and row["close"] > 0
                                       and not _ohlc_issues(row) for row in underlying_rows)
                flags: Counter[str] = Counter()
                examples: list[dict[str, Any]] = []
                priced_codes = {str(row["contract_code"]) for row in price_rows}
                flags["ROSTER_CONTRACT_WITHOUT_PRICE"] = sum(
                    code not in priced_codes for code in roster_by_code)
                if not valid_underlying:
                    flags["VALID_UNDERLYING_DAY_MISSING"] = 1
                for code, roster in roster_by_code.items():
                    term = term_by_code.get(code)
                    if exchange == "SSE" and not _sse_term_usable(roster, term, day):
                        flags["SSE_TERMS_MISSING_OR_CONFLICT"] += 1
                    elif exchange == "SZSE" and (term is None or
                           term.get("provider_symbol") != roster.get("trading_code") or
                           not term.get("expiry_date") or not term.get("volume_multiple")):
                        flags["SZSE_TERMS_MISSING_OR_CONFLICT"] += 1
                if exchange == "CFFEX" and roster_rows and not term_rows:
                    flags["CFFEX_TERMS_NOT_VERIFIED"] = len(roster_rows)
                seen: set[tuple[str, str]] = set()
                for row in price_rows:
                    code = str(row["contract_code"])
                    key = (code, str(row.get("source_interface")))
                    row_flags: list[str] = []
                    roster = roster_by_code.get(code)
                    term = term_by_code.get(code)
                    verified_term = bool(
                        roster and term and (
                            _sse_term_usable(roster, term, day) if exchange == "SSE" else
                            exchange == "SZSE" and term.get("provider_symbol") == roster.get("trading_code")
                            and term.get("expiry_date") and term.get("expiry_date") >= day
                            and term.get("volume_multiple") and term["volume_multiple"] > 0
                        )
                    )
                    if key in seen:
                        row_flags.append("DUPLICATE_PRICE_KEY")
                    seen.add(key)
                    if code not in roster_by_code:
                        row_flags.append("PRICE_WITHOUT_ROSTER")
                    if row.get("close") is None:
                        row_flags.append("CLOSE_MISSING")
                    elif row["close"] <= 0:
                        row_flags.append("NONPOSITIVE_CLOSE")
                    row_flags.extend(_ohlc_issues(row))
                    if not valid_underlying:
                        row_flags.append("VALID_UNDERLYING_MISSING")
                    if exchange != "CFFEX" and code not in term_by_code:
                        row_flags.append("TERMS_ROW_MISSING")
                    if row.get("expiry_date") is None:
                        row_flags.append("RAW_EXPIRY_MISSING")
                        if not verified_term:
                            row_flags.append("EXPIRY_UNRESOLVED_AFTER_TERMS_JOIN")
                    for flag in row_flags:
                        flags[flag] += 1
                    if row_flags and len(examples) < 10:
                        examples.append({"contract_code": code, "flags": row_flags})
                item = {
                    "trade_date": day, "exchange": exchange, "product_id": pid,
                    "roster_rows": len(roster_rows), "option_price_rows": len(price_rows),
                    "term_rows": len(term_rows), "underlying_rows": len(underlying_rows),
                    "flags": {key: value for key, value in sorted(flags.items()) if value},
                    "examples": examples,
                }
                coverage.append(item)
                totals.update({key: value for key, value in flags.items() if value})
                totals["roster_rows"] += len(roster_rows)
                totals["option_price_rows"] += len(price_rows)
                totals["term_rows"] += len(term_rows)
                totals["underlying_rows"] += len(underlying_rows)

    report = {
        "start_date": start_date, "end_date": end_date,
        "products": [p.product_id for p in products],
        "scope": "STORED_DATES_ONLY; gaps with no saved files require the backfill coverage report",
        "raw_files_modified": False,
        "rates_status": "NOT_VERIFIED",
        "etf_dividends_status": "NOT_VERIFIED",
        "historical_bbo_status": "NOT_AVAILABLE_IN_DAILY_OHLC",
        "stored_dates": len(days), "totals": dict(sorted(totals.items())),
        "coverage": coverage,
    }
    if report_path is not None:
        write_report_atomic(report_path, report)
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description="Audit saved historical daily prices and terms")
    parser.add_argument("--products", default="all")
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--project-root", type=Path)
    parser.add_argument("--output-root", type=Path)
    parser.add_argument("--report-path", type=Path, help="Optional JSON output; omitted means stdout only")
    args = parser.parse_args()
    report = audit_historical_quality(
        products_selection=args.products, start_date=args.start_date,
        end_date=args.end_date, project_root=args.project_root,
        output_root=args.output_root, report_path=args.report_path,
    )
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
