"""Versioned, source-attributed local exchange calendar; no guessed future dates."""

from __future__ import annotations

from datetime import date, datetime, time, timedelta
import json
from pathlib import Path

from .models import SHANGHAI


class CalendarError(ValueError):
    pass


class TradingCalendar:
    def __init__(self, path: Path, exchanges: set[str]):
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
            if raw["timezone"] != "Asia/Shanghai":
                raise ValueError("timezone")
            self.start = date.fromisoformat(raw["valid_from"])
            self.end = date.fromisoformat(raw["valid_to"])
            if self.start > self.end:
                raise ValueError("coverage")
            self.checked = date.fromisoformat(raw["verified_on"])
            self.sources = {exchange: raw["sources"][exchange] for exchange in exchanges}
            if not all(isinstance(items, list) and items and all(
                    isinstance(url, str) and url.startswith("https://") for url in items)
                    for items in self.sources.values()):
                raise ValueError("missing source")
            if not raw.get("verification_note") or not raw.get("session_sources"):
                raise ValueError("missing verification/session sources")
            self.closed: set[date] = set()
            for first, last in raw["closed_ranges"]:
                day, finish = date.fromisoformat(first), date.fromisoformat(last)
                if not self.start <= day <= finish <= self.end:
                    raise ValueError("holiday outside coverage")
                while day <= finish:
                    self.closed.add(day)
                    day += timedelta(days=1)
            # Windows are an explicit common sampling window for the selected products.
            self.sessions = [(time.fromisoformat(a), time.fromisoformat(b)) for a, b in raw["sampling_windows"]]
            if not self.sessions or any(a >= b for a, b in self.sessions):
                raise ValueError("sampling windows")
        except (OSError, ValueError, KeyError, TypeError) as exc:
            raise CalendarError(f"交易日历不可用：{path} ({type(exc).__name__})；请核对来源及覆盖范围") from exc

    def validate_day(self, day: date) -> None:
        if not self.start <= day <= self.end:
            raise CalendarError(f"交易日历仅覆盖 {self.start} 至 {self.end}，无法确认 {day}；请更新日历")

    def is_open(self, stamp: datetime) -> bool:
        local = stamp.astimezone(SHANGHAI)
        self.validate_day(local.date())
        return (local.weekday() < 5 and local.date() not in self.closed
                and any(start <= local.time() <= end for start, end in self.sessions))
