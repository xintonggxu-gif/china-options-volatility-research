"""Command line entry points for live collection and separate daily backfill."""

from __future__ import annotations

import argparse
import json
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime
import os
import re
from pathlib import Path

from .config import load_live_config
from .registry import load_registry
from .models import SHANGHAI, now_shanghai
from .storage.parquet_writer import StorageWriteError
from .trading_calendar import TradingCalendar


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
    snapshot = commands.add_parser("collect-snapshot", help="采集一轮完整期权链后退出，供定时任务在收盘前后调用")
    snapshot.add_argument("--products", default="all", help="all、单个产品 ID 或逗号分隔的产品 ID")
    snapshot.add_argument("--label", default="snapshot", help="写入 ingestion_runs.run_mode 的标签，例如 close 或 preclose")
    snapshot.add_argument("--project-root", type=Path, help="项目根目录；默认使用安装包所在项目")
    snapshot.add_argument("--force", action="store_true", help="跳过交易日检查（非交易日或日历未覆盖的日期）")
    history = commands.add_parser("backfill-daily", help="预览或执行有来源核实的逐日历史回填")
    history.add_argument("--products", default="all", help="all、单个产品 ID 或逗号分隔的产品 ID")
    history.add_argument("--start-date", required=True, help="首个日期 YYYY-MM-DD（必填）")
    history.add_argument("--end-date", required=True, help="最后日期 YYYY-MM-DD（必填）")
    history.add_argument("--max-requests", type=int, default=20, help="本次实际 HTTP 请求上限，默认 20")
    history.add_argument("--retry-absent-prices", action="store_true",
                         help="重新请求先前明确返回空的期权合约／日期；默认跳过这些已记录缺口")
    history.add_argument("--repair-sse-terms", action="store_true",
                         help="仅补上交所历史合约清单和条款，不请求期权或标的价格；只选 SSE 产品")
    action = history.add_mutually_exclusive_group()
    action.add_argument("--dry-run", action="store_true", help="只预览；不发请求、不写数据（默认）")
    action.add_argument("--execute", action="store_true", help="明确执行下载与回填")
    history.add_argument("--project-root", type=Path, help="项目根目录；默认使用安装包所在项目")
    history.add_argument("--output-root", type=Path, help="可选的独立输出目录；默认写项目 data/")
    args = parser.parse_args()
    if args.command == "backfill-daily":
        try:
            from .collectors.daily_backfill import run_backfill_daily

            report = run_backfill_daily(
                products_selection=args.products, start_date=args.start_date,
                end_date=args.end_date, max_requests=args.max_requests,
                execute=args.execute, project_root=args.project_root,
                output_root=args.output_root, retry_absent_prices=args.retry_absent_prices,
                repair_sse_terms=args.repair_sse_terms,
            )
            print(json.dumps(report, ensure_ascii=False, indent=2, default=str))
            return
        except (FileNotFoundError, ValueError, ImportError) as exc:
            parser.exit(2, f"回填配置错误: {exc}\n")
        except (StorageWriteError, OSError) as exc:
            parser.exit(1, f"历史数据保存失败: {exc}\n")
    try:
        if args.command == "collect-snapshot":
            if not re.fullmatch(r"[A-Za-z0-9_-]{1,32}", args.label):
                raise ValueError("--label 只能包含字母、数字、下划线和连字符")
            config = load_live_config(
                interval_seconds=60.0,
                project_root=args.project_root,
                single_cycle=True,
                run_mode=f"snapshot:{args.label}",
                log_name="collect-snapshot.log",
            )
        else:
            config = load_live_config(
                interval_seconds=args.interval_seconds,
                flush_interval_seconds=args.flush_interval_seconds,
                max_buffer_rows=args.max_buffer_rows,
                project_root=args.project_root,
                trading_hours_only=args.trading_hours_only,
            )
        products = load_registry(config.registry_path, args.products)
        configure_logging(config.log_path)
        if args.command == "collect-snapshot" and not args.force:
            today = now_shanghai().date()
            calendar = TradingCalendar(config.calendar_path, {p.exchange for p in products})
            if not calendar.is_trading_day(today):
                logging.getLogger(__name__).info("%s 不是交易日，快照未发起请求", today)
                return
        from .collectors.live_collector import LiveCollector

        LiveCollector(config, products).run()
    except (FileNotFoundError, ValueError, ImportError) as exc:
        parser.exit(2, f"配置错误: {exc}\n")
    except (StorageWriteError, OSError) as exc:
        logging.getLogger(__name__).error("保存或本地文件操作失败: %s", exc)
        parser.exit(1, "程序失败退出；请检查日志和磁盘，不能认定缓冲已全部保存。\n")


if __name__ == "__main__":
    main()
