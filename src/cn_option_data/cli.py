"""Command line entry point for this phase's live collector."""

from __future__ import annotations

import argparse
import logging
import os
from pathlib import Path

from .config import load_live_config
from .registry import load_registry


def main() -> None:
    parser = argparse.ArgumentParser(prog="cn-option-data")
    commands = parser.add_subparsers(dest="command", required=True)
    live = commands.add_parser("collect-live", help="采集当前期权链实时行情")
    live.add_argument("--products", default="all", help="all、单个产品 ID 或逗号分隔的产品 ID")
    live.add_argument("--interval-seconds", type=float, default=os.environ.get("POLL_INTERVAL_SECONDS", "60"))
    live.add_argument("--flush-interval-seconds", type=float, default=30.0)
    live.add_argument("--max-buffer-rows", type=int, default=50_000)
    live.add_argument("--project-root", type=Path, help="项目根目录；默认使用安装包所在项目")
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    try:
        config = load_live_config(
            interval_seconds=args.interval_seconds,
            flush_interval_seconds=args.flush_interval_seconds,
            max_buffer_rows=args.max_buffer_rows,
            project_root=args.project_root,
        )
        products = load_registry(config.registry_path, args.products)
        from .collectors.live_collector import LiveCollector

        LiveCollector(config, products).run()
    except (FileNotFoundError, ValueError, ImportError) as exc:
        parser.exit(2, f"配置错误: {exc}\n")


if __name__ == "__main__":
    main()
