"""Offline tests built from Sina payloads captured after the 2026-09-30 close."""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

import pyarrow.dataset as ds
import pytest

from cn_option_data.config import load_live_config
from cn_option_data.models import SHANGHAI, OptionContract
from cn_option_data.providers.sina_quotes import SinaQuotes, cffex_option_symbol
from cn_option_data.registry import load_registry
from cn_option_data.trading_calendar import TradingCalendar

ROOT = Path(__file__).resolve().parents[1]

PAYLOADS = {
    "sh510050": "上证50ETF华夏,2.925,2.922,2.938,2.940,2.920,2.937,2.938,442391602,1296541861.000,201200,2.937,272900,2.936,512000,2.935,246800,2.934,268400,2.933,1103300,2.938,1600100,2.939,700000,2.940,1117700,2.941,628900,2.942,2026-09-30,15:34:59,00,D|276200|811475.60",
    "sh000300": "沪深300,4356.7955,4345.2088,4357.6155,4368.6139,4341.8791,0,0,162949626,356491548621,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,2026-09-30,15:35:40,00,",
    "CON_OP_10012313": "22,0.0155,0.0160,0.0160,32,122798,-0.62,3.0000,0.0161,0.0188,0.3005,0.0001,0.0164,70,0.0163,139,0.0162,169,0.0161,45,0.0160,32,0.0155,22,0.0154,30,0.0153,1,0.0152,11,0.0151,80,2026-09-30 15:00:00,1,E 00,EBS,510050,50ETF购10月3000,28.57,0.0189,0.0143,64181,10371046.00,M,0.0161,C,2026-10-28,28,1,0,0.016",
    "P_OP_io2610C4600": "36,2.600,2.800,2.800,3,7837.000,-30.000,4600,4.000,4.200,438.400,0.200,0.000,0,0.000,0,0.000,0,0.000,0,2.800,3,2.600,36,0.000,0,0.000,0,0.000,0,0.000,0,2026-09-30 15:00:00,0,,cn,sh000300,沪深300购10月4600,60.000,5.000,2.600,2718,9789.400,,4.000,C,2026-07-20,16,,,,",
    "CFF_RE_IF2610": "4349.600,4364.200,4334.000,4353.000,19842,86279044.400,49879.000,4353.000,0.000,4775.200,3907.200,0.000,0.000,4334.800,4341.200,51603.000,4352.800,1,0.000,0,0.000,0,0.000,0,0.000,0,4354.000,1,0.000,0,0.000,0,0.000,0,0.000,0,2026-09-30,15:00:00,400,0,,,,,,,,,4348.304,沪深300指数期货2610",
}
T0 = datetime(2026, 9, 30, 15, 5, tzinfo=SHANGHAI)


def _untraded_payload() -> str:
    """IO2610C4650 whose last trade was on 2026-09-28: Sina then repeats that day's record."""
    fields = PAYLOADS["P_OP_io2610C4600"].split(",")
    fields[7] = "4650"
    fields[32] = "2026-09-28 15:00:00"
    return ",".join(fields)


PAYLOADS["P_OP_io2610C4650"] = _untraded_payload()


class FakeCalls:
    """Stands in for BoundedCalls; answers from PAYLOADS and records each URL."""

    def __init__(self, drop: set[str] | None = None):
        self.urls: list[str] = []
        self.drop = drop or set()

    def call(self, kind, name, **kwargs):
        assert kind == "http"
        self.urls.append(name)
        symbols = name.split("list=", 1)[1].split(",")
        lines = [f'var hq_str_{s}="{"" if s in self.drop else PAYLOADS.get(s, "")}";' for s in symbols]
        return "\n".join(lines).encode("gb18030"), T0, T0

    def close(self):
        pass


def fields(symbol: str) -> list[str]:
    return PAYLOADS[symbol].split(",")


def test_cffex_symbol_keeps_side_uppercase():
    assert cffex_option_symbol("IO2610C4600") == "P_OP_io2610C4600"
    assert cffex_option_symbol("MO2703P10000") == "P_OP_mo2703P10000"


def test_etf_option_fields_and_amount_unchanged():
    quote = SinaQuotes.option(fields("CON_OP_10012313"), "CON_OP_10012313", "510050", "C", 3.0, T0, T0)
    assert (quote["bid_price1"], quote["bid_volume1"], quote["ask_price1"], quote["ask_volume1"]) == ("0.0155", "22", "0.0160", "32")
    assert (quote["last_price"], quote["volume"], quote["open_interest"]) == ("0.0160", "64181", "122798")
    assert quote["amount"] == pytest.approx(10371046.0)  # already CNY: 64181 x ~0.0162 x 10000
    assert quote["source_quote_time"] == "2026-09-30 15:00:00"
    assert quote["provider_symbol"] == "CON_OP_10012313"


def test_cffex_option_fields_and_amount_converted_to_cny():
    quote = SinaQuotes.option(fields("P_OP_io2610C4600"), "P_OP_io2610C4600", "sh000300", "C", 4600.0, T0, T0,
                              amount_multiplier=100.0)
    assert (quote["bid_price1"], quote["bid_volume1"], quote["ask_price1"], quote["ask_volume1"]) == ("2.600", "36", "2.800", "3")
    assert (quote["last_price"], quote["volume"], quote["open_interest"]) == ("2.800", "2718", "7837.000")
    # 9789.4 / 2718 = 3.60 index points per contract, inside the day's 2.6-5.0 range.
    assert quote["amount"] == pytest.approx(978940.0)


@pytest.mark.parametrize("underlying,side,strike", [("sh000016", "C", 4600.0), ("sh000300", "P", 4600.0),
                                                    ("sh000300", "C", 4650.0)])
def test_option_rejects_payload_that_disagrees_with_master(underlying, side, strike):
    assert SinaQuotes.option(fields("P_OP_io2610C4600"), "P_OP_io2610C4600", underlying, side, strike, T0, T0) is None


def test_future_field_positions():
    quote = SinaQuotes.future(fields("CFF_RE_IF2610"), "CFF_RE_IF2610", T0, T0)
    assert quote["quote_timestamp"] == "2026-09-30 15:00:00"
    assert (quote["bid_price1"], quote["bid_volume1"], quote["ask_price1"], quote["ask_volume1"]) == ("4352.800", "1", "4354.000", "1")
    assert (quote["last_price"], quote["volume"], quote["open_interest"]) == ("4353.000", "19842", "49879.000")
    assert SinaQuotes.future(fields("CFF_RE_IF2610")[:30], "CFF_RE_IF2610", T0, T0) is None


def test_fetch_bundled_puts_anchors_in_every_batch():
    calls = FakeCalls()
    sina = SinaQuotes(lambda: False, [], calls)
    options = ["CON_OP_10012313", "P_OP_io2610C4600", "CON_OP_99999999"]
    found, anchors = sina.fetch_bundled(["sh510050", "CFF_RE_IF2610"], options, batch_size=2)
    assert len(calls.urls) == 2
    assert all(url.split("list=")[1].startswith("sh510050,CFF_RE_IF2610,") for url in calls.urls)
    assert set(found) == {"CON_OP_10012313", "P_OP_io2610C4600"}  # empty payload is not a quote
    assert sorted(symbol for symbol, _ in anchors) == ["CFF_RE_IF2610", "CFF_RE_IF2610", "sh510050", "sh510050"]
    first_batch = found["CON_OP_10012313"].batch_id
    assert found["P_OP_io2610C4600"].batch_id == first_batch
    anchor_batches = {item.batch_id for _, item in anchors}
    assert len(anchor_batches) == 2 and first_batch in anchor_batches


def _provider(calls: FakeCalls, product_ids: str):
    from cn_option_data.providers.akshare_live import AKShareLive

    products = load_registry(ROOT / "config" / "product_registry.csv", product_ids)
    provider = AKShareLive(products)
    provider.sina.calls = calls
    provider.future_symbols = {"IF": ["IF2610"], "IH": ["IH2610"]}
    return provider, {p.product_id: p for p in products}


def _contract(product, code, side, strike):
    return OptionContract(product=product, contract_code=code, contract_name=code,
                          underlying_symbol=product.underlying_symbol, option_class=side, strike_price=strike,
                          expire_datetime=None, last_trading_date=None, volume_multiple=None, price_tick=None)


def test_get_option_quotes_bundles_per_product():
    calls = FakeCalls()
    provider, by_id = _provider(calls, "510050,IO")
    try:
        contracts = {
            "10012313": _contract(by_id["510050"], "10012313", "C", 3.0),
            "IO2610C4600": _contract(by_id["IO"], "IO2610C4600", "C", 4600.0),
        }
        chain = provider.get_option_quotes(contracts)
    finally:
        provider.close()
    requested = [url.split("list=")[1].split(",") for url in calls.urls]
    assert ["sh510050", "CFF_RE_IH2610", "CON_OP_10012313"] in requested
    assert ["sh000300", "CFF_RE_IF2610", "P_OP_io2610C4600"] in requested
    assert set(chain.options) == {"10012313", "IO2610C4600"}
    assert chain.options["IO2610C4600"]["amount"] == pytest.approx(978940.0)
    by_product = {pid: quote["batch_id"] for pid, _, quote in chain.underlyings}
    assert by_product["510050"] == chain.options["10012313"]["batch_id"]
    assert by_product["IO"] == chain.options["IO2610C4600"]["batch_id"]
    # IH2610 was requested but the fixture returns an empty payload: no futures row is fabricated.
    assert [(pid, symbol) for pid, symbol, _ in chain.futures] == [("IO", "IF2610")]
    assert chain.futures[0][2]["batch_id"] == chain.options["IO2610C4600"]["batch_id"]
    assert chain.options["10012313"]["quote_timestamp"] == datetime(2026, 9, 30, 15, 0, tzinfo=SHANGHAI)


def test_collector_writes_joinable_batches(tmp_path):
    from cn_option_data.collectors.live_collector import LiveCollector

    calls = FakeCalls()
    provider, by_id = _provider(calls, "510050,IO")
    config = load_live_config(interval_seconds=60, project_root=tmp_path)
    collector = LiveCollector(config, list(by_id.values()))
    collector.active_options = {
        "10012313": _contract(by_id["510050"], "10012313", "C", 3.0),
        "IO2610C4600": _contract(by_id["IO"], "IO2610C4600", "C", 4600.0),
    }
    provider.get_futures_quotes = lambda: {}  # the AKShare futures phase is not under test here
    try:
        collector._collect(provider)
        collector.writer.flush()
    finally:
        provider.close()
    # Partition values are also stored as string columns inside each file.
    read = lambda table: ds.dataset(tmp_path / "data" / "raw" / table, format="parquet").to_table().to_pylist()
    options, underlyings = read("option_quotes"), read("underlying_quotes")
    assert {row["run_id"] for row in options + underlyings} == {collector.run_id}
    bundled = {row["batch_id"]: row for row in underlyings if row["collection_phase"] == "bundled"}
    assert sorted(row["collection_phase"] for row in underlyings) == ["after", "after", "before", "before",
                                                                      "bundled", "bundled"]
    for row in options:
        match = bundled[row["batch_id"]]
        assert match["product_id"] == row["product_id"]
    io = next(row for row in options if row["product_id"] == "IO")
    assert (io["volume"], io["amount"]) == (2718, pytest.approx(978940.0))
    assert bundled[io["batch_id"]]["last_price"] == pytest.approx(4357.6155)


def test_option_not_traded_this_session_is_dated_by_the_session(tmp_path):
    from cn_option_data.collectors.live_collector import LiveCollector

    calls = FakeCalls()
    provider, by_id = _provider(calls, "IO")
    contracts = {
        "IO2610C4600": _contract(by_id["IO"], "IO2610C4600", "C", 4600.0),
        "IO2610C4650": _contract(by_id["IO"], "IO2610C4650", "C", 4650.0),
    }
    try:
        chain = provider.get_option_quotes(contracts)
        stale, fresh = chain.options["IO2610C4650"], chain.options["IO2610C4600"]
        assert stale["session_date"] == "2026-09-30"  # from sh000300 in the same response
        assert (stale["volume"], stale["amount"], stale["upper_limit"], stale["lower_limit"]) == (0, 0.0, None, None)
        assert stale["quote_timestamp"] is None and stale["bid_price1"] == "2.600"
        assert "session_date" not in fresh and fresh["volume"] == "2718"

        collector = LiveCollector(load_live_config(interval_seconds=60, project_root=tmp_path), [by_id["IO"]])
        collector.active_options = contracts
        provider.get_futures_quotes = lambda: {}
        collector._collect(provider)
        collector.writer.flush()
    finally:
        provider.close()
    rows = {r["contract_code"]: r for r in
            ds.dataset(tmp_path / "data" / "raw" / "option_quotes", format="parquet").to_table().to_pylist()}
    assert rows["IO2610C4650"]["trade_date"] == "2026-09-30"
    assert rows["IO2610C4650"]["quote_status"] == "NO_TRADE_THIS_SESSION"
    assert rows["IO2610C4650"]["quote_timestamp"] is None
    assert rows["IO2610C4650"]["source_quote_time"] == "2026-09-28 15:00:00"
    assert rows["IO2610C4650"]["volume"] == 0
    assert rows["IO2610C4600"]["quote_status"] == "TIME_SEMANTICS_UNVERIFIED"


class FlakyDiscovery:
    """Contract master fails for HO on the first ``failures`` calls, then succeeds."""

    def __init__(self, failures: int):
        self.failures = failures
        self.calls = 0
        self.diagnostics: dict[str, str] = {}

    def get_active_contracts(self):
        self.calls += 1
        self.diagnostics = {"HO": "INTERFACE_ERROR"} if self.calls <= self.failures else {}
        return {}


def _collector(tmp_path, **overrides):
    from dataclasses import replace
    from cn_option_data.collectors.live_collector import LiveCollector

    config = replace(load_live_config(interval_seconds=0.1, project_root=tmp_path), **overrides)
    products = load_registry(ROOT / "config" / "product_registry.csv", "HO")
    return LiveCollector(config, products)


def test_failed_discovery_is_retried_before_the_30_minute_refresh(tmp_path):
    collector = _collector(tmp_path, discovery_retry_seconds=0.5)
    provider = FlakyDiscovery(failures=1)
    cycles = []

    def fake_collect(_):
        cycles.append(provider.calls)
        if len(cycles) == 3:
            collector.stop_requested = True
    collector._collect = fake_collect
    collector._session(provider)
    assert provider.calls == 2  # one retry, then the normal 1800 s schedule
    assert cycles == [1, 2, 2]


def test_single_cycle_retries_discovery_then_stops(tmp_path):
    collector = _collector(tmp_path, single_cycle=True)
    provider = FlakyDiscovery(failures=1)
    cycles = []
    collector._collect = lambda _: cycles.append(provider.calls)
    collector._session(provider)
    assert provider.calls == 2 and cycles == [2] and collector.stop_requested


def test_is_trading_day_uses_holidays_and_weekends():
    calendar = TradingCalendar(ROOT / "config" / "trading_calendar.json", {"SSE", "SZSE", "CFFEX"})
    assert calendar.is_trading_day(date(2026, 9, 30))
    assert not calendar.is_trading_day(date(2026, 10, 1))  # National Day
    assert not calendar.is_trading_day(date(2026, 10, 10))  # Saturday
    with pytest.raises(ValueError):
        calendar.is_trading_day(date(2027, 1, 4))  # beyond the verified calendar
