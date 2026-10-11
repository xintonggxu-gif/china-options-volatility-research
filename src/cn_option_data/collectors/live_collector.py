"""Scheduled HTTP polling with the existing buffered Parquet writer."""

from __future__ import annotations

import logging
import signal
import time
import uuid
from collections import Counter, defaultdict
from datetime import datetime
from typing import Any

from ..config import LiveConfig
from ..models import OptionContract, now_shanghai, quote_clock, quote_values
from ..providers.akshare_live import AKShareLive
from ..registry import Product
from ..storage.parquet_writer import ParquetWriter, StorageWriteError
from ..trading_calendar import TradingCalendar, CalendarError


LOG = logging.getLogger(__name__)
SOURCE = "AKShare"


class LiveCollector:
    def __init__(self, config: LiveConfig, products: list[Product]):
        self.config = config
        self.products = products
        self.by_product = {p.product_id: p for p in products}
        self.calendar = TradingCalendar(config.calendar_path, {p.exchange for p in products}) if config.trading_hours_only else None
        if self.calendar:
            self.calendar.validate_day(now_shanghai().date())
        self.writer = ParquetWriter(
            config.data_dir,
            max_buffer_rows=config.max_buffer_rows,
            flush_interval_seconds=config.flush_interval_seconds,
        )
        self.run_id = str(uuid.uuid4())
        self.started_at = now_shanghai()
        self.stop_requested = False
        self.active_options: dict[str, OptionContract] = {}
        self.underlyings: dict[str, list[Product]] = defaultdict(list)
        self.future_products_to_options: dict[str, list[str]] = defaultdict(list)
        for product in products:
            self.underlyings[product.underlying_symbol].append(product)
            if product.hedge_future:
                self.future_products_to_options[product.hedge_future].append(product.product_id)
        self.diagnostics: dict[str, str] = {}
        self.snapshot_seen: dict[tuple[str, str], tuple[Any, ...]] = {}
        self.contracts_seen: set[str] = set()
        self.products_seen: set[str] = set()
        self.product_rows: dict[str, int] = defaultdict(int)
        self.last_option_coverage: dict[str, tuple[int, int]] = {}
        self.last_error_type: str | None = None
        self.last_error_message: str | None = None
        self.last_registry_date = self.started_at.date()
        self.quality_issues: Counter[str] = Counter()
        self.field_gaps: Counter[str] = Counter()
        self.next_heartbeat = time.monotonic() + config.heartbeat_interval_seconds
        self.cycle = 0

    def request_stop(self, *_: Any) -> None:
        first = not self.stop_requested
        self.stop_requested = True
        if first:
            LOG.info("收到退出信号，停止新请求并保存已接收数据；重复信号不会中断写盘")

    def _requests_paused(self) -> bool:
        return self.stop_requested or (self.calendar is not None and not self.calendar.is_open(now_shanghai()))

    def _maintenance(self) -> None:
        self.writer.flush_if_due()
        if time.monotonic() >= self.next_heartbeat:
            self._heartbeat()
            self.next_heartbeat = time.monotonic() + self.config.heartbeat_interval_seconds

    def _record_error(self, code: str, message: str) -> None:
        self.last_error_type = code
        self.last_error_message = message[:1000]

    def _snapshot_contracts(self, discovered: dict[str, OptionContract]) -> None:
        stamp = now_shanghai()
        day = stamp.date().isoformat()
        for symbol, contract in discovered.items():
            key = (day, symbol)
            terms = (contract.underlying_symbol, contract.option_class, contract.strike_price,
                     contract.expire_datetime, contract.last_trading_date,
                     contract.volume_multiple, contract.price_tick)
            if self.snapshot_seen.get(key) == terms:
                continue
            self.writer.append("contract_snapshots", {
                "snapshot_timestamp": stamp,
                "trade_date": day,
                "exchange": contract.product.exchange,
                "hour": f"{stamp.hour:02d}",
                "product_id": contract.product.product_id,
                "contract_code": contract.contract_code,
                "contract_name": contract.contract_name,
                "underlying_symbol": contract.underlying_symbol,
                "option_class": contract.option_class,
                "strike_price": contract.strike_price,
                "expire_datetime": contract.expire_datetime,
                "last_trading_date": contract.last_trading_date,
                "volume_multiple": contract.volume_multiple,
                "price_tick": contract.price_tick,
                "is_trading": True,
                "source": SOURCE,
                "provider_symbol": contract.provider_symbol or symbol,
                "source_interface": contract.source_interface,
                "request_timestamp": contract.request_timestamp,
            })
            self.snapshot_seen[key] = terms
        self.writer.flush()

    def _refresh(self, provider: AKShareLive) -> None:
        discovered = provider.get_active_contracts()
        self.diagnostics = dict(provider.diagnostics)
        for product in self.products:
            status = self.diagnostics.get(product.product_id)
            if status:
                LOG.warning("%s product=%s", status, product.product_id)
        # Retain previously discovered contracts after a transient master-list failure.
        failed = {pid for pid, status in self.diagnostics.items() if status == "INTERFACE_ERROR"}
        for symbol, contract in self.active_options.items():
            today = now_shanghai().date()
            still_valid = all(value is None or value.date() >= today
                              for value in (contract.expire_datetime, contract.last_trading_date))
            if contract.product.product_id in failed and symbol not in discovered and still_valid:
                discovered[symbol] = contract
        self.contracts_seen.update(discovered)
        self.products_seen.update(c.product.product_id for c in discovered.values())
        new_symbols = set(discovered) - set(self.active_options)
        if new_symbols:
            LOG.info("新发现期权合约 %s 条", len(new_symbols))
        self.active_options = discovered
        active_products = {contract.product.product_id for contract in discovered.values()}
        for product_id in self.last_option_coverage.keys() - active_products:
            self.last_option_coverage.pop(product_id)
        self._snapshot_contracts(discovered)
        self.last_registry_date = now_shanghai().date()

    def _append_quote(self, table: str, symbol: str, quote: dict[str, Any], product_id: str, extra: dict[str, Any], exchange: str) -> str:
        values = quote_values(quote)
        if not any(values[name] is not None for name in ("bid_price1", "ask_price1", "last_price", "volume", "open_interest")):
            self.quality_issues[f"{table}:{product_id}:NO_VALID_VALUES"] += 1
            return "NO_VALID_VALUES"
        source_time, source_day, time_status = quote_clock(quote)
        if quote.get("session_date"):
            # The source time is the contract's last trade on an earlier day; the BBO is today's.
            source_time, source_day, time_status = None, quote["session_date"], "NO_TRADE_THIS_SESSION"
        received: datetime = quote["received_timestamp"]
        flags: list[str] = []
        index_quote = table == "underlying_quotes" and self.by_product[product_id].option_category == "INDEX"
        bid, ask = values["bid_price1"], values["ask_price1"]
        if bid is not None and ask is not None and bid > ask:
            LOG.warning("交叉盘口已置空 BBO symbol=%s bid=%s ask=%s", symbol, bid, ask)
            for field in ("bid_price1", "bid_volume1", "ask_price1", "ask_volume1"):
                values[field] = None
            flags.append("CROSSED_BBO")
        elif (bid is None or ask is None) and not index_quote:
            flags.append("BBO_NOT_AVAILABLE")
        if time_status:
            flags.append(time_status)
        if source_time is not None and not quote.get("bbo_time_verified", False):
            flags.append("TIME_SEMANTICS_UNVERIFIED")
        for flag in flags:
            self.quality_issues[f"{table}:{product_id}:{flag}"] += 1
        status = "|".join(flags) or "OK"
        fields = ["last_price", "volume"]
        if not index_quote:
            fields += ["bid_price1", "ask_price1", "bid_volume1", "ask_volume1"]
        if table in {"option_quotes", "underlying_quotes"}:
            fields.append("amount")
        if table in {"option_quotes", "futures_quotes"}:
            fields.append("open_interest")
        if table == "option_quotes":
            fields += ["upper_limit", "lower_limit"]
        for field in fields:
            if values[field] is None:
                self.field_gaps[f"{table}:{product_id}:{field}"] += 1
        row = {
            "exchange_timestamp": source_time,
            "quote_timestamp": source_time,
            "received_timestamp": received,
            "trade_date": source_day,
            "exchange": exchange,
            "hour": f"{received.hour:02d}",
            **extra,
            "bid_price1": values["bid_price1"],
            "bid_volume1": values["bid_volume1"],
            "ask_price1": values["ask_price1"],
            "ask_volume1": values["ask_volume1"],
            "last_price": values["last_price"],
            "volume": values["volume"],
            "source": quote.get("source", SOURCE),
            "source_interface": quote.get("source_interface"),
            "request_timestamp": quote.get("request_timestamp"),
            "provider_symbol": quote.get("provider_symbol", symbol),
            "batch_id": quote.get("batch_id"),
            "run_id": self.run_id,
        }
        if table in {"option_quotes", "underlying_quotes"}:
            row["amount"] = values["amount"]
        if table in {"option_quotes", "futures_quotes"}:
            row["open_interest"] = values["open_interest"]
        if table == "option_quotes":
            row.update({
                "upper_limit": values["upper_limit"],
                "lower_limit": values["lower_limit"],
                "source_quote_time": quote.get("source_quote_time"),
                "supplement_request_timestamp": quote.get("supplement_request_timestamp"),
                "quote_status": status,
            })
        self.writer.append(table, row)
        self.product_rows[product_id] += 1
        return "ACCEPTED"

    def _append_underlying(self, symbol: str, quote: dict[str, Any], product: Product, phase: str) -> None:
        self._append_quote("underlying_quotes", symbol, quote, product.product_id, {
            "underlying_symbol": symbol, "product_id": product.product_id, "collection_phase": phase,
        }, product.exchange)

    def _append_future(self, symbol: str, quote: dict[str, Any], product_id: str, phase: str) -> None:
        self._append_quote("futures_quotes", symbol, quote, product_id, {
            "future_symbol": symbol, "future_product": symbol[:2],
            "mapped_option_product": product_id, "collection_phase": phase,
        }, "CFFEX")

    def _collect(self, provider: AKShareLive) -> None:
        # Bracket a chain poll with the corresponding underlying and futures
        # observations. The options phase also returns each product's underlying
        # and futures from the same Sina responses (collection_phase="bundled",
        # joined to options by batch_id). Each family is isolated from other HTTP failures.
        families = (
            ("underlying_before", "underlying_quotes", provider.get_underlying_quotes, None),
            ("futures_before", "futures_quotes", provider.get_futures_quotes, None),
            ("options", "option_quotes", provider.get_option_quotes, self.active_options),
            ("underlying_after", "underlying_quotes", provider.get_underlying_quotes, None),
            ("futures_after", "futures_quotes", provider.get_futures_quotes, None),
        )
        for phase, table, get_quotes, argument in families:
            if self._requests_paused():
                break
            started = time.monotonic()
            try:
                result = get_quotes(argument) if argument is not None else get_quotes()
            except (StorageWriteError, CalendarError):
                raise
            except Exception as exc:
                self._record_error("INTERFACE_ERROR", f"{table}: {exc}")
                LOG.exception("%s 采集失败，继续其他行情", phase)
                if table == "option_quotes":
                    for contract in self.active_options.values():
                        self.diagnostics[contract.product.product_id] = "INTERFACE_ERROR"
                continue
            if table == "option_quotes":
                quotes = result.options
                for product_id, symbol, quote in result.underlyings:
                    self._append_underlying(symbol, quote, self.by_product[product_id], "bundled")
                for product_id, symbol, quote in result.futures:
                    self._append_future(symbol, quote, product_id, "bundled")
                LOG.info("采集阶段 cycle=%s phase=%s seconds=%.1f returned=%s bundled_underlying=%s bundled_futures=%s",
                         self.cycle, phase, time.monotonic() - started, len(quotes),
                         len(result.underlyings), len(result.futures))
            else:
                quotes = result
                LOG.info("采集阶段 cycle=%s phase=%s seconds=%.1f returned=%s", self.cycle, phase,
                         time.monotonic() - started, len(quotes))
            bracket = phase.rsplit("_", 1)[-1]
            outcomes: dict[str, str] = {}
            for symbol, quote in quotes.items():
                if table == "option_quotes":
                    contract = self.active_options.get(symbol)
                    if contract is None:
                        continue
                    outcomes[symbol] = self._append_quote(table, symbol, quote, contract.product.product_id, {
                        "product_id": contract.product.product_id,
                        "contract_code": contract.contract_code,
                        "underlying_symbol": contract.underlying_symbol,
                        "option_class": contract.option_class,
                        "strike_price": contract.strike_price,
                        "expire_datetime": contract.expire_datetime,
                    }, contract.product.exchange)
                elif table == "underlying_quotes":
                    for product in self.underlyings.get(symbol, []):
                        self._append_underlying(symbol, quote, product, bracket)
                else:
                    for product_id in self.future_products_to_options.get(symbol[:2], []):
                        self._append_future(symbol, quote, product_id, bracket)
            if table == "option_quotes":
                for product in self.products:
                    product_id = product.product_id
                    expected = {code for code, contract in self.active_options.items()
                                if contract.product.product_id == product_id}
                    if not expected:
                        continue
                    returned = expected & quotes.keys()
                    written = {code for code in returned if outcomes.get(code) == "ACCEPTED"}
                    missing = expected - returned
                    rejected = returned - written
                    date_only = sum(quote_clock(quotes[code])[2] == "SOURCE_TIME_DATE_ONLY" for code in written)
                    no_time = sum(quote_clock(quotes[code])[2] == "SOURCE_TIME_UNKNOWN" for code in written)
                    no_bbo = 0
                    for code in written:
                        values = quote_values(quotes[code])
                        bid, ask = values["bid_price1"], values["ask_price1"]
                        no_bbo += bid is None or ask is None or bid > ask
                    reasons = {reason: sum(outcomes.get(code) == reason for code in rejected)
                               for reason in ("NO_VALID_VALUES",)}
                    self.last_option_coverage[product_id] = (len(written), len(expected))
                    LOG.info("期权覆盖 product=%s discovered=%s returned=%s accepted=%s "
                             "missing_bbo=%s source_date_only=%s source_time_unknown=%s missing=%s rejected=%s "
                             "reasons=%s missing_codes=%s rejected_codes=%s persisted_total=%s",
                             product_id, len(expected), len(returned), len(written), no_bbo,
                             date_only, no_time, len(missing), len(rejected), reasons,
                             sorted(missing), sorted(rejected), self.writer.written_by_product[(table, product_id)])
                    if self.stop_requested:
                        continue
                    if not written:
                        self.diagnostics[product_id] = provider.diagnostics.get(product_id, "NO_QUOTE")
                    elif missing or rejected:
                        self.diagnostics[product_id] = provider.diagnostics.get(product_id, "PARTIAL_QUOTES")
                    elif product_id not in provider.diagnostics and self.diagnostics.get(product_id) in {
                        "NO_QUOTE", "PARTIAL_QUOTES", "INTERFACE_ERROR", "INVALID_QUOTE"
                    }:
                        self.diagnostics.pop(product_id, None)
            elif (not quotes and not self._requests_paused()
                  and (table != "futures_quotes" or self.future_products_to_options)):
                self._record_error("NO_QUOTE", f"{phase} 未返回记录")
            self._maintenance()

    def _heartbeat(self) -> None:
        parts = []
        for product in self.products:
            product_id = product.product_id
            count = sum(c.product.product_id == product_id for c in self.active_options.values())
            status = self.diagnostics.get(product_id, "OK")
            accepted, expected = self.last_option_coverage.get(product_id, (0, count))
            tables = {table: {"accepted": self.writer.accepted_by_product[(table, product_id)],
                              "persisted": self.writer.written_by_product[(table, product_id)]}
                      for table in ("option_quotes", "underlying_quotes", "futures_quotes")}
            parts.append(f"{product_id}:active={count},last_option_accepted={accepted}/{expected},tables={tables},status={status}")
        LOG.info("heartbeat run_id=%s %s buffered_rows=%s persisted_tables=%s quality=%s field_gaps=%s", self.run_id,
                 " | ".join(parts), self.writer.buffered_rows, dict(self.writer.rows_written), dict(self.quality_issues),
                 dict(self.field_gaps))

    def _session(self, provider: AKShareLive) -> None:
        next_refresh = 0.0
        waiting = False
        while not self.stop_requested:
            if self.calendar and not self.calendar.is_open(now_shanghai()):
                if not waiting:
                    LOG.info("交易时段控制：等待已核实日历中的采样窗口，不发起行情请求")
                    waiting = True
                self._maintenance()
                time.sleep(0.5)
                continue
            waiting = False
            if time.monotonic() >= next_refresh or self.last_registry_date != now_shanghai().date():
                self._refresh(provider)
                failed = self._discovery_failures()
                # A one-shot snapshot has no later refresh, so retry discovery briefly now.
                attempt = 1
                while failed and self.config.single_cycle and attempt < 3 and not self.stop_requested:
                    LOG.warning("合约发现失败 products=%s，5 秒后重试 attempt=%s", failed, attempt + 1)
                    self._wait(5.0)
                    self._refresh(provider)
                    failed = self._discovery_failures()
                    attempt += 1
                # A failed contract master would otherwise leave its products uncollected
                # until the next 30-minute refresh.
                delay = self.config.discovery_retry_seconds if failed else self.config.refresh_interval_seconds
                if failed:
                    LOG.warning("合约发现失败 products=%s，%.0f 秒后重试", failed, delay)
                next_refresh = time.monotonic() + delay
            if self._requests_paused():
                continue
            cycle_started = time.monotonic()
            self.cycle += 1
            self._collect(provider)
            self._maintenance()
            if self.config.single_cycle:
                LOG.info("单次快照完成 cycle=%s seconds=%.1f", self.cycle, time.monotonic() - cycle_started)
                self.stop_requested = True
                break
            elapsed = time.monotonic() - cycle_started
            if elapsed > self.config.interval_seconds:
                LOG.warning("轮询耗时 %.1f 秒，超过设置间隔 %.1f 秒", elapsed, self.config.interval_seconds)
            self._wait(max(1.0, self.config.interval_seconds - elapsed))

    def _discovery_failures(self) -> list[str]:
        return sorted(pid for pid, status in self.diagnostics.items() if status == "INTERFACE_ERROR")

    def _wait(self, seconds: float) -> None:
        wait_until = time.monotonic() + seconds
        while not self.stop_requested and time.monotonic() < wait_until:
            time.sleep(max(0.0, min(0.5, wait_until - time.monotonic())))
            self._maintenance()

    def run(self) -> None:
        previous_sigint = signal.getsignal(signal.SIGINT)
        previous_sigterm = signal.getsignal(signal.SIGTERM)
        signal.signal(signal.SIGINT, self.request_stop)
        signal.signal(signal.SIGTERM, self.request_stop)
        backoff = 1.0
        provider: AKShareLive | None = None
        fatal = False
        LOG.info("采集开始 run_id=%s mode=%s interval=%s trading_hours_only=%s", self.run_id, self.config.run_mode,
                 self.config.interval_seconds, self.config.trading_hours_only)
        if self.calendar:
            LOG.info("日历覆盖 %s 至 %s verified_on=%s sources=%s", self.calendar.start,
                     self.calendar.end, self.calendar.checked, self.calendar.sources)
        try:
            while not self.stop_requested:
                try:
                    provider = AKShareLive(self.products, should_stop=self._requests_paused,
                                           on_wait=self._maintenance,
                                           request_timeout=self.config.request_timeout_seconds)
                    self._session(provider)
                    backoff = 1.0
                except KeyboardInterrupt:
                    self.request_stop()
                except (ImportError, StorageWriteError, CalendarError) as exc:
                    fatal = True
                    self._record_error(type(exc).__name__, str(exc))
                    raise
                except Exception as exc:
                    self._record_error("INTERFACE_ERROR", str(exc))
                    try:
                        self.writer.flush()
                    except StorageWriteError as write_error:
                        fatal = True
                        self._record_error(type(write_error).__name__, str(write_error))
                        raise
                    if self.config.single_cycle:
                        LOG.exception("单次快照失败，不重试")
                        self.stop_requested = True
                        break
                    LOG.exception("采集会话中断，%.1f 秒后重试", backoff)
                    until = time.monotonic() + backoff
                    while not self.stop_requested and time.monotonic() < until:
                        time.sleep(min(0.5, until - time.monotonic()))
                        self._maintenance()
                    backoff = min(backoff * 2, 60.0)
                finally:
                    if provider is not None:
                        if provider.errors and not fatal:
                            self._record_error("INTERFACE_ERROR", provider.errors[-1])
                        try:
                            provider.close()
                        except Exception as exc:
                            fatal = True
                            self._record_error("REQUEST_CLEANUP_ERROR", type(exc).__name__)
                            raise
        finally:
            try:
                self.writer.flush()
                finished = now_shanghai()
                problems = [f"{product}: {status}" for product, status in self.diagnostics.items()]
                if self.quality_issues:
                    problems.append("QUALITY: " + str(dict(self.quality_issues)))
                if self.field_gaps:
                    problems.append("FIELD_GAPS: " + str(dict(self.field_gaps)))
                if fatal:
                    status = "FAILED"
                elif self.last_error_type or problems:
                    status = "PARTIAL"
                elif not self.contracts_seen:
                    status = "STOPPED" if self.calendar else "NO_ACTIVE_CONTRACTS"
                else:
                    status = "STOPPED"
                self.writer.append("ingestion_runs", {
                    "run_id": self.run_id, "started_at": self.started_at, "finished_at": finished,
                    "trade_date": self.started_at.date().isoformat(), "exchange": "ALL",
                    "hour": f"{self.started_at.hour:02d}", "provider": "AKShare+Sina", "status": status,
                    "contracts_discovered": len(self.contracts_seen), "products_discovered": len(self.products_seen),
                    "quote_rows_written": self.writer.quote_rows_written,
                    "error_type": self.last_error_type or ("PRODUCT_OR_DATA_QUALITY" if problems else None),
                    "error_message": ("; ".join(filter(None, [self.last_error_message, *problems])) or None),
                    "run_mode": self.config.run_mode,
                })
                self.writer.flush()
                self._heartbeat()
                LOG.info("采集结束 run_id=%s status=%s persisted_tables=%s buffered_rows=%s", self.run_id,
                         status, dict(self.writer.rows_written), self.writer.buffered_rows)
            except StorageWriteError:
                LOG.exception("最终保存失败 run_id=%s buffered_rows=%s，不能认定已保存", self.run_id, self.writer.buffered_rows)
                raise
            finally:
                signal.signal(signal.SIGINT, previous_sigint)
                signal.signal(signal.SIGTERM, previous_sigterm)
