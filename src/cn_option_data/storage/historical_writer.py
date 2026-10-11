"""Separate, append-only daily history; never writes to the live quote tables."""

from __future__ import annotations

import json
import os
import uuid
from collections import defaultdict
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from .parquet_writer import StorageWriteError, TS


S = pa.string()
F = pa.float64()
I = pa.int64()
DAILY_PRICE = [("open", F), ("high", F), ("low", F), ("close", F),
               ("settlement", F), ("volume", I), ("open_interest", I), ("amount", F)]
PROVENANCE = [("source", S), ("source_interface", S), ("provider_symbol", S),
              ("request_timestamp", TS), ("received_timestamp", TS), ("missing_reason", S)]

HISTORICAL_SCHEMAS = {
    "roster_daily": pa.schema([
        ("trade_date", S), ("exchange", S), ("product_id", S),
        ("contract_code", S), ("trading_code", S), *PROVENANCE,
    ]),
    "option_daily": pa.schema([
        ("trade_date", S), ("exchange", S), ("product_id", S), ("contract_code", S),
        ("option_class", S), ("strike_price", F), ("expiry_date", S),
        *DAILY_PRICE, ("price_type", S), *PROVENANCE,
    ]),
    "underlying_daily": pa.schema([
        ("trade_date", S), ("exchange", S), ("product_id", S), ("underlying_symbol", S),
        *DAILY_PRICE, ("price_type", S), ("adjustment", S), *PROVENANCE,
    ]),
    "futures_daily": pa.schema([
        ("trade_date", S), ("exchange", S), ("future_symbol", S), ("future_product", S),
        *DAILY_PRICE, ("price_type", S), *PROVENANCE,
    ]),
    "contract_terms_daily": pa.schema([
        ("trade_date", S), ("exchange", S), ("product_id", S), ("contract_code", S),
        ("contract_name", S), ("underlying_symbol", S), ("option_class", S),
        ("strike_price", F), ("expiry_date", S), ("last_trading_date", S),
        ("volume_multiple", F), ("price_tick", F), ("is_trading", pa.bool_()),
        ("snapshot_timestamp", TS), *PROVENANCE,
    ]),
    "rates_daily": pa.schema([
        ("trade_date", S), ("exchange", S), ("curve", S), ("tenor_years", F),
        ("rate_percent", F), *PROVENANCE,
    ]),
    "etf_dividends": pa.schema([
        ("trade_date", S), ("exchange", S), ("product_id", S), ("underlying_symbol", S),
        ("ex_date", S), ("cash_dividend_per_unit", F), ("cumulative_dividend", F),
        *PROVENANCE,
    ]),
    "contract_adjustments": pa.schema([
        ("trade_date", S), ("exchange", S), ("product_id", S), ("contract_code", S),
        ("effective_date", S), ("old_strike", F), ("new_strike", F),
        ("old_multiplier", F), ("new_multiplier", F), ("reason", S), *PROVENANCE,
    ]),
}

KEY_COLUMNS = {
    "roster_daily": ("trade_date", "product_id", "contract_code", "source_interface"),
    "option_daily": ("trade_date", "product_id", "contract_code", "source_interface"),
    "underlying_daily": ("trade_date", "product_id", "underlying_symbol", "source_interface"),
    "futures_daily": ("trade_date", "future_symbol", "source_interface"),
    "contract_terms_daily": ("trade_date", "product_id", "contract_code"),
    "rates_daily": ("trade_date", "curve", "tenor_years", "source_interface"),
    "etf_dividends": ("product_id", "ex_date", "source_interface"),
    "contract_adjustments": ("product_id", "contract_code", "effective_date", "source_interface"),
}


class HistoricalParquetWriter:
    def __init__(self, root: Path, *, max_buffer_rows: int = 5_000):
        if max_buffer_rows <= 0:
            raise ValueError("max_buffer_rows must be positive")
        self.root = root
        self.max_buffer_rows = max_buffer_rows
        self.buffers: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
        self.keys: dict[tuple[str, str, str], set[tuple[Any, ...]]] = {}
        self.rows_written: dict[str, int] = defaultdict(int)

    def _partition_dir(self, table: str, day: str, exchange: str) -> Path:
        return self.root / table / f"trade_date={day}" / f"exchange={exchange}"

    def _keys(self, table: str, day: str, exchange: str) -> set[tuple[Any, ...]]:
        partition = (table, day, exchange)
        if partition not in self.keys:
            result: set[tuple[Any, ...]] = set()
            columns = KEY_COLUMNS[table]
            for path in self._partition_dir(*partition).glob("*.parquet"):
                try:
                    rows = pq.ParquetFile(path).read(columns=list(columns)).to_pylist()
                except Exception as exc:
                    raise StorageWriteError(f"Cannot read existing historical keys: {path}") from exc
                result.update(tuple(row[column] for column in columns) for row in rows)
            self.keys[partition] = result
        return self.keys[partition]

    def append(self, table: str, row: dict[str, Any]) -> bool:
        if table not in HISTORICAL_SCHEMAS:
            raise ValueError(f"unknown historical table: {table}")
        day, exchange = row["trade_date"], row["exchange"]
        key = tuple(row[column] for column in KEY_COLUMNS[table])
        known = self._keys(table, day, exchange)
        if key in known:
            return False
        known.add(key)
        self.buffers[(table, day, exchange)].append(row)
        if sum(map(len, self.buffers.values())) >= self.max_buffer_rows:
            self.flush()
        return True

    def contains(self, table: str, day: str, exchange: str, key: tuple[Any, ...]) -> bool:
        """Check a persisted or buffered business key before making a network call."""
        return key in self._keys(table, day, exchange)

    def read_partition(self, table: str, day: str, exchange: str) -> list[dict[str, Any]]:
        """Read completed parts, plus rows not yet flushed, for resumable discovery."""
        if table not in HISTORICAL_SCHEMAS:
            raise ValueError(f"unknown historical table: {table}")
        partition = (table, day, exchange)
        rows: list[dict[str, Any]] = []
        for path in sorted(self._partition_dir(*partition).glob("*.parquet")):
            try:
                rows.extend(pq.ParquetFile(path).read().to_pylist())
            except Exception as exc:
                raise StorageWriteError(f"Cannot read historical partition: {path}") from exc
        rows.extend(self.buffers.get(partition, []))
        return rows

    def flush(self) -> None:
        for partition, rows in list(self.buffers.items()):
            if not rows:
                continue
            table, day, exchange = partition
            folder = self._partition_dir(*partition)
            folder.mkdir(parents=True, exist_ok=True)
            name = f"part-{uuid.uuid4().hex}.parquet"
            target = folder / name
            temporary = folder / f".{name}.tmp"
            try:
                batch = pa.Table.from_pylist(rows, schema=HISTORICAL_SCHEMAS[table])
                pq.write_table(batch, temporary, compression="zstd")
                os.replace(temporary, target)
            except Exception as exc:
                temporary.unlink(missing_ok=True)
                raise StorageWriteError(f"Historical Parquet write failed: {target}") from exc
            self.rows_written[table] += len(rows)
            self.buffers[partition] = []


def write_report_atomic(path: Path, report: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    try:
        temporary.write_text(json.dumps(report, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")
        os.replace(temporary, path)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise
