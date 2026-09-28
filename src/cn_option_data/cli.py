"""Command line entry point for this phase's live collector."""

from __future__ import annotations

import argparse
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime
import os
from pathlib import Path

from .config import load_live_config
from .registry import load_registry
from .models import SHANGHAI
from .storage.parquet_writer import StorageWriteError


class ShanghaiFormatter(logging.Formatter):
    def formatTime(self, record, datefmt=None):
        stamp = datetime.fromtimestamp(record.created, SHANGHAI)
        return stamp.strftime(datefmt) if datefmt else stamp.isoformat(timespec="seconds")


def configure_logging(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    formatter = ShanghaiFormatter("%(asctime)s %(levelname)s %(name)s %(message)s")
    file_handler = RotatingFileHandler(path, maxBytes=10 * 1024 * 1024, backupCount=3, encoding="utf-8")
    for handler in (logging.StreamHandler(), file_handler):
        handler.setFormatter(formatter)
        logging.getLogger().addHandler(handler)
    logging.getLogger().setLevel(logging.INFO)


def main() -> None:
    parser = argparse.ArgumentParser(prog="cn-option-data")
    commands = parser.add_subparsers(dest="command", required=True)
    live = commands.add_parser("collect-live", help="采集当前期权链实时行情")
    live.add_argument("--products", default="all", help="all、单个产品 ID 或逗号分隔的产品 ID")
    live.add_argument("--interval-seconds", type=float, default=os.environ.get("POLL_INTERVAL_SECONDS", "60"))
    live.add_argument("--flush-interval-seconds", type=float, default=30.0)
    live.add_argument("--max-buffer-rows", type=int, default=50_000)
    live.add_argument("--project-root", type=Path, help="项目根目录；默认使用安装包所在项目")
    live.add_argument("--trading-hours-only", action="store_true", help="只在已核实日历的采样窗口轮询；默认不启用")
    args = parser.parse_args()
    try:
        config = load_live_config(
            interval_seconds=args.interval_seconds,
            flush_interval_seconds=args.flush_interval_seconds,
            max_buffer_rows=args.max_buffer_rows,
            project_root=args.project_root,
            trading_hours_only=args.trading_hours_only,
        )
        products = load_registry(config.registry_path, args.products)
        configure_logging(config.log_path)
        from .collectors.live_collector import LiveCollector

        LiveCollector(config, products).run()
    except (FileNotFoundError, ValueError, ImportError) as exc:
        parser.exit(2, f"配置错误: {exc}\n")
    except (StorageWriteError, OSError) as exc:
        logging.getLogger(__name__).error("保存或本地文件操作失败: %s", exc)
        parser.exit(1, "程序失败退出；请检查日志和磁盘，不能认定缓冲已全部保存。\n")


if __name__ == "__main__":
    main()
