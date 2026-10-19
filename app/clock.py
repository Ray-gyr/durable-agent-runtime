from __future__ import annotations

import threading
from datetime import datetime, timedelta, timezone
from typing import Protocol


class Clock(Protocol):
    def now(self) -> datetime:
        """Return the current time as an aware UTC datetime."""
        ...


class SystemClock:
    def now(self) -> datetime:
        return datetime.now(timezone.utc)


class ManualClock:
    """A clock that only moves when told to, so lease and backoff tests never sleep."""

    def __init__(self, start: datetime | None = None) -> None:
        self._now = start or datetime(2026, 1, 1, tzinfo=timezone.utc)
        self._lock = threading.Lock()

    def now(self) -> datetime:
        with self._lock:
            return self._now

    def advance(self, seconds: float) -> datetime:
        with self._lock:
            self._now += timedelta(seconds=seconds)
            return self._now


def to_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def format_timestamp(value: datetime) -> str:
    """Fixed-width ISO 8601 in UTC, so stored timestamps sort lexicographically."""
    return to_utc(value).isoformat(timespec="microseconds")
