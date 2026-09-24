"""Settings for the live collection process only."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class LiveConfig:
    project_root: Path
    interval_seconds: float = 60.0
    flush_interval_seconds: float = 30.0
    max_buffer_rows: int = 50_000
    refresh_interval_seconds: float = 1800.0
    heartbeat_interval_seconds: float = 60.0

    @property
    def registry_path(self) -> Path:
        return self.project_root / "config" / "product_registry.csv"

    @property
    def data_dir(self) -> Path:
        return self.project_root / "data"


def default_project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def load_live_config(
    *,
    interval_seconds: float,
    flush_interval_seconds: float = 30.0,
    max_buffer_rows: int = 50_000,
    project_root: Path | None = None,
) -> LiveConfig:
    if interval_seconds <= 0:
        raise ValueError("interval-seconds 必须大于 0")
    if flush_interval_seconds <= 0 or max_buffer_rows <= 0:
        raise ValueError("缓冲阈值必须大于 0")
    return LiveConfig(
        project_root=(project_root or default_project_root()).resolve(),
        interval_seconds=interval_seconds,
        flush_interval_seconds=flush_interval_seconds,
        max_buffer_rows=max_buffer_rows,
    )
