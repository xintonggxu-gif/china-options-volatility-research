"""Buffered, append-only Parquet output with UUID part names."""

from __future__ import annotations

import logging
import os
import time
import uuid
from collections import defaultdict
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq


LOG = logging.getLogger(__name__)
TS = pa.timestamp("us", tz="Asia/Shanghai")
S = pa.string()
F = pa.float64()
I = pa.int64()
PARTITION = [("trade_date", S), ("exchange", S), ("hour", S)]
STAMP = [("exchange_timestamp", TS), ("received_timestamp", TS)]
BBO = [
    ("bid_price1", F), ("bid_volume1", I),
    ("ask_price1", F), ("ask_volume1", I),
    ("last_price", F), ("volume", I),
]

SCHEMAS = {
    "contract_snapshots": pa.schema([
        ("snapshot_timestamp", TS), *PARTITION,
        ("product_id", S), ("contract_code", S), ("contract_name", S),
        ("underlying_symbol", S), ("option_class", S), ("strike_price", F),
        ("expire_datetime", TS), ("last_trading_date", TS),
        ("volume_multiple", F), ("price_tick", F), ("is_trading", pa.bool_()),
        ("source", S), ("provider_symbol", S),
        ("source_interface", S), ("request_timestamp", TS),
    ]),
    "option_quotes": pa.schema([
        *STAMP, *PARTITION,
        ("product_id", S), ("contract_code", S), ("underlying_symbol", S),
        ("option_class", S), ("strike_price", F), ("expire_datetime", TS),
        *BBO, ("amount", F), ("open_interest", I),
        ("upper_limit", F), ("lower_limit", F),
        ("source", S), ("provider_symbol", S),
        ("quote_timestamp", TS), ("source_quote_time", S),
        ("source_interface", S), ("request_timestamp", TS),
        ("supplement_request_timestamp", TS), ("quote_status", S),
    ]),
    "underlying_quotes": pa.schema([
        *STAMP, *PARTITION,
        ("underlying_symbol", S), ("product_id", S),
        *BBO, ("amount", F), ("source", S), ("provider_symbol", S),
        ("quote_timestamp", TS), ("source_interface", S), ("request_timestamp", TS),
    ]),
    "futures_quotes": pa.schema([
        *STAMP, *PARTITION,
        ("future_symbol", S), ("future_product", S), ("mapped_option_product", S),
        *BBO, ("open_interest", I), ("source", S), ("provider_symbol", S),
        ("quote_timestamp", TS), ("source_interface", S), ("request_timestamp", TS),
    ]),
    "ingestion_runs": pa.schema([
        ("run_id", S), ("started_at", TS), ("finished_at", TS),
        *PARTITION, ("provider", S), ("status", S),
        ("contracts_discovered", I), ("products_discovered", I),
        ("quote_rows_written", I), ("error_type", S), ("error_message", S),
    ]),
}


class StorageWriteError(RuntimeError):
    """Fatal storage failure, never retry this as a provider/network failure."""


class ParquetWriter:
    def __init__(self, data_dir: Path, *, max_buffer_rows: int = 50_000, flush_interval_seconds: float = 30.0):
        if max_buffer_rows <= 0 or flush_interval_seconds <= 0:
            raise ValueError("缓冲阈值必须大于 0")
        self.data_dir = data_dir
        self.max_buffer_rows = max_buffer_rows
        self.flush_interval_seconds = flush_interval_seconds
        self.buffers: dict[str, list[dict[str, Any]]] = defaultdict(list)
        self.rows_written: dict[str, int] = defaultdict(int)
        self.rows_accepted: dict[str, int] = defaultdict(int)
        self.accepted_by_product: dict[tuple[str, str], int] = defaultdict(int)
        self.written_by_product: dict[tuple[str, str], int] = defaultdict(int)
        self.last_flush = time.monotonic()

    def append(self, table: str, row: dict[str, Any]) -> None:
        if table not in SCHEMAS:
            raise ValueError(f"未知表: {table}")
        self.buffers[table].append(row)
        self.rows_accepted[table] += 1
        self.accepted_by_product[(table, row.get("product_id", row.get("mapped_option_product", "ALL")))] += 1
        if sum(map(len, self.buffers.values())) >= self.max_buffer_rows:
            self.flush()

    def flush_if_due(self) -> None:
        if time.monotonic() - self.last_flush >= self.flush_interval_seconds:
            self.flush()

    def _table_root(self, table: str) -> Path:
        folder = "metadata" if table == "ingestion_runs" else "raw"
        return self.data_dir / folder / table

    @staticmethod
    def _partition(row: dict[str, Any]) -> tuple[str, str, str]:
        return row["trade_date"], row["exchange"], row["hour"]

    def flush(self) -> None:
        try:
            self._flush()
        except Exception as exc:
            raise StorageWriteError(f"Parquet 保存失败 ({type(exc).__name__})；未写入批次仍在缓冲") from exc

    def _flush(self) -> None:
        for table in SCHEMAS:
            while self.buffers[table]:
                key = self._partition(self.buffers[table][0])
                batch = [row for row in self.buffers[table] if self._partition(row) == key]
                partition_dir = self._table_root(table)
                for name, value in zip(("trade_date", "exchange", "hour"), key):
                    partition_dir /= f"{name}={value}"
                partition_dir.mkdir(parents=True, exist_ok=True)
                basename = f"part-{uuid.uuid4().hex}.parquet"
                target = partition_dir / basename
                temporary = partition_dir / f".{basename}.tmp"
                try:
                    arrow_table = pa.Table.from_pylist(batch, schema=SCHEMAS[table])
                    pq.write_table(arrow_table, temporary, compression="zstd")
                    os.replace(temporary, target)
                except Exception:
                    temporary.unlink(missing_ok=True)
                    raise
                self.rows_written[table] += len(batch)
                for row in batch:
                    product = row.get("product_id", row.get("mapped_option_product", "ALL"))
                    self.written_by_product[(table, product)] += 1
                self.buffers[table] = [row for row in self.buffers[table] if self._partition(row) != key]
                LOG.info("wrote %s rows=%s path=%s", table, len(batch), target)
        self.last_flush = time.monotonic()

    @property
    def buffered_rows(self) -> int:
        return sum(len(rows) for rows in self.buffers.values())

    @property
    def quote_rows_written(self) -> int:
        return sum(self.rows_written[name] for name in ("option_quotes", "underlying_quotes", "futures_quotes"))
