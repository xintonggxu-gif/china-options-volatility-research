"""Scheduled HTTP polling with the existing buffered Parquet writer."""

from __future__ import annotations

import logging
import signal
import time
import uuid
from collections import defaultdict
from datetime import datetime
from typing import Any

from ..config import LiveConfig
from ..models import OptionContract, now_shanghai, quote_values
from ..providers.akshare_live import AKShareLive
from ..registry import Product
from ..storage.parquet_writer import ParquetWriter


LOG = logging.getLogger(__name__)
SOURCE = "AKShare"


class LiveCollector:
    def __init__(self, config: LiveConfig, products: list[Product]):
        self.config = config
        self.products = products
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
        self.snapshot_seen: set[tuple[str, str]] = set()
        self.contracts_seen: set[str] = set()
        self.products_seen: set[str] = set()
        self.product_rows: dict[str, int] = defaultdict(int)
        self.last_error_type: str | None = None
        self.last_error_message: str | None = None
        self.last_registry_date = self.started_at.date()

    def request_stop(self, *_: Any) -> None:
        self.stop_requested = True
        LOG.info("收到退出信号，正在保存缓冲数据")

    def _record_error(self, code: str, message: str) -> None:
        self.last_error_type = code
        self.last_error_message = message[:1000]

    def _snapshot_contracts(self, discovered: dict[str, OptionContract]) -> None:
        stamp = now_shanghai()
        day = stamp.date().isoformat()
        for symbol, contract in discovered.items():
            if (day, symbol) in self.snapshot_seen:
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
            self.snapshot_seen.add((day, symbol))
        self.writer.flush()

    def _refresh(self, provider: AKShareLive) -> None:
        discovered = provider.get_active_contracts()
        self.diagnostics = dict(provider.diagnostics)
        for product in self.products:
            status = self.diagnostics.get(product.product_id)
            if status:
                LOG.warning("%s product=%s", status, product.product_id)
        # Keep today's previous contracts after a transient master-list failure.
        if self.last_registry_date == now_shanghai().date():
            failed = {pid for pid, status in self.diagnostics.items() if status == "INTERFACE_ERROR"}
            for symbol, contract in self.active_options.items():
                if contract.product.product_id in failed and symbol not in discovered:
                    discovered[symbol] = contract
        self.contracts_seen.update(discovered)
        self.products_seen.update(c.product.product_id for c in discovered.values())
        new_symbols = set(discovered) - set(self.active_options)
        if new_symbols:
            LOG.info("新发现期权合约 %s 条", len(new_symbols))
        self.active_options = discovered
        self._snapshot_contracts(discovered)
        self.last_registry_date = now_shanghai().date()

    def _append_quote(self, table: str, symbol: str, quote: dict[str, Any], product_id: str, extra: dict[str, Any], exchange: str) -> None:
        values = quote_values(quote)
        if not any(values[name] is not None for name in ("bid_price1", "ask_price1", "last_price", "volume", "open_interest")):
            return
        source_time: datetime | None = quote.get("quote_timestamp")
        received: datetime = quote["received_timestamp"]
        if source_time is not None and source_time.date() != received.date():
            LOG.warning("跳过非当日行情 symbol=%s source_time=%s", symbol, source_time.isoformat())
            return
        status = "OK"
        bid, ask = values["bid_price1"], values["ask_price1"]
        if bid is not None and ask is not None and bid > ask:
            LOG.warning("交叉盘口已置空 BBO symbol=%s bid=%s ask=%s", symbol, bid, ask)
            for field in ("bid_price1", "bid_volume1", "ask_price1", "ask_volume1"):
                values[field] = None
            status = "CROSSED_BBO"
        elif bid is None or ask is None:
            status = "BBO_NOT_AVAILABLE"
        row = {
            "exchange_timestamp": source_time,
            "quote_timestamp": source_time,
            "received_timestamp": received,
            "trade_date": (source_time or received).date().isoformat(),
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

    def _collect(self, provider: AKShareLive) -> None:
        # Each family is isolated so one upstream HTTP failure does not stop the rest.
        option_rows_before = {p.product_id: self.product_rows[p.product_id] for p in self.products}
        option_rows_after = option_rows_before
        families = (
            ("option_quotes", provider.get_option_quotes, self.active_options),
            ("underlying_quotes", provider.get_underlying_quotes, None),
            ("futures_quotes", provider.get_futures_quotes, None),
        )
        for table, get_quotes, argument in families:
            if self.stop_requested:
                break
            try:
                quotes = get_quotes(argument) if argument is not None else get_quotes()
            except Exception as exc:
                self._record_error("INTERFACE_ERROR", f"{table}: {exc}")
                LOG.exception("%s 采集失败，继续其他行情", table)
                continue
            for symbol, quote in quotes.items():
                if table == "option_quotes":
                    contract = self.active_options.get(symbol)
                    if contract is None:
                        continue
                    self._append_quote(table, symbol, quote, contract.product.product_id, {
                        "product_id": contract.product.product_id,
                        "contract_code": contract.contract_code,
                        "underlying_symbol": contract.underlying_symbol,
                        "option_class": contract.option_class,
                        "strike_price": contract.strike_price,
                        "expire_datetime": contract.expire_datetime,
                    }, contract.product.exchange)
                elif table == "underlying_quotes":
                    for product in self.underlyings.get(symbol, []):
                        self._append_quote(table, symbol, quote, product.product_id, {
                            "underlying_symbol": symbol, "product_id": product.product_id,
                        }, product.exchange)
                else:
                    future_product = symbol[:2]
                    for product_id in self.future_products_to_options.get(future_product, []):
                        self._append_quote(table, symbol, quote, product_id, {
                            "future_symbol": symbol, "future_product": future_product,
                            "mapped_option_product": product_id,
                        }, "CFFEX")
            if table == "option_quotes":
                option_rows_after = {p.product_id: self.product_rows[p.product_id] for p in self.products}
        for product in self.products:
            product_id = product.product_id
            if any(c.product.product_id == product_id for c in self.active_options.values()):
                if option_rows_after[product_id] == option_rows_before[product_id]:
                    self.diagnostics[product_id] = provider.diagnostics.get(product_id, "NO_QUOTE")
                elif product_id not in provider.diagnostics and self.diagnostics.get(product_id) in {"NO_QUOTE", "INTERFACE_ERROR"}:
                    self.diagnostics.pop(product_id, None)

    def _heartbeat(self) -> None:
        parts = []
        for product in self.products:
            product_id = product.product_id
            count = sum(c.product.product_id == product_id for c in self.active_options.values())
            status = self.diagnostics.get(product_id, "OK")
            parts.append(f"{product_id}:active={count},rows={self.product_rows[product_id]},status={status}")
        LOG.info("heartbeat %s flushed_quote_rows=%s", " | ".join(parts), self.writer.quote_rows_written)

    def _session(self, provider: AKShareLive) -> None:
        self._refresh(provider)
        next_refresh = time.monotonic() + self.config.refresh_interval_seconds
        next_heartbeat = time.monotonic() + self.config.heartbeat_interval_seconds
        while not self.stop_requested:
            cycle_started = time.monotonic()
            self._collect(provider)
            self.writer.flush_if_due()
            if time.monotonic() >= next_heartbeat:
                self._heartbeat()
                next_heartbeat = time.monotonic() + self.config.heartbeat_interval_seconds
            if time.monotonic() >= next_refresh or self.last_registry_date != now_shanghai().date():
                self._refresh(provider)
                next_refresh = time.monotonic() + self.config.refresh_interval_seconds
            elapsed = time.monotonic() - cycle_started
            if elapsed > self.config.interval_seconds:
                LOG.warning("轮询耗时 %.1f 秒，超过设置间隔 %.1f 秒", elapsed, self.config.interval_seconds)
            wait_until = time.monotonic() + max(1.0, self.config.interval_seconds - elapsed)
            while not self.stop_requested and time.monotonic() < wait_until:
                time.sleep(min(0.5, wait_until - time.monotonic()))
                self.writer.flush_if_due()
                if time.monotonic() >= next_heartbeat:
                    self._heartbeat()
                    next_heartbeat = time.monotonic() + self.config.heartbeat_interval_seconds

    def run(self) -> None:
        previous_sigint = signal.getsignal(signal.SIGINT)
        previous_sigterm = signal.getsignal(signal.SIGTERM)
        signal.signal(signal.SIGINT, self.request_stop)
        signal.signal(signal.SIGTERM, self.request_stop)
        backoff = 1.0
        provider: AKShareLive | None = None
        try:
            while not self.stop_requested:
                try:
                    provider = AKShareLive(self.products, should_stop=lambda: self.stop_requested)
                    self._session(provider)
                    backoff = 1.0
                except KeyboardInterrupt:
                    self.request_stop()
                except ImportError:
                    raise
                except Exception as exc:
                    self._record_error("INTERFACE_ERROR", str(exc))
                    self.writer.flush()
                    LOG.exception("采集会话中断，%.1f 秒后重试", backoff)
                    until = time.monotonic() + backoff
                    while not self.stop_requested and time.monotonic() < until:
                        time.sleep(min(0.5, until - time.monotonic()))
                    backoff = min(backoff * 2, 60.0)
        finally:
            signal.signal(signal.SIGINT, previous_sigint)
            signal.signal(signal.SIGTERM, previous_sigterm)
            self.writer.flush()
            if provider and provider.errors:
                self._record_error("INTERFACE_ERROR", provider.errors[-1])
            finished = now_shanghai()
            problems = [f"{product}: {status}" for product, status in self.diagnostics.items()]
            if not self.contracts_seen and self.last_error_type is None:
                status = "NO_ACTIVE_CONTRACTS"
            elif self.last_error_type or problems:
                status = "PARTIAL"
            else:
                status = "STOPPED"
            self.writer.append("ingestion_runs", {
                "run_id": self.run_id,
                "started_at": self.started_at,
                "finished_at": finished,
                "trade_date": self.started_at.date().isoformat(),
                "exchange": "ALL",
                "hour": f"{self.started_at.hour:02d}",
                "provider": "AKShare+Sina",
                "status": status,
                "contracts_discovered": len(self.contracts_seen),
                "products_discovered": len(self.products_seen),
                "quote_rows_written": self.writer.quote_rows_written,
                "error_type": self.last_error_type or ("PRODUCT_STATUS" if problems else None),
                "error_message": self.last_error_message or ("; ".join(problems) if problems else None),
            })
            self.writer.flush()
            LOG.info("采集结束 run_id=%s status=%s quote_rows=%s", self.run_id, status, self.writer.quote_rows_written)
