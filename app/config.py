"""Environment settings and the single place where concrete adapters are chosen."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

from .adapters.faulty_store import FaultPlan, FaultyObjectStore
from .adapters.local_store import LocalObjectStore
from .adapters.sqlite import SqliteRepository
from .clock import Clock
from .ports import ObjectStore, Repository


PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(PROJECT_ROOT / ".env")


def int_setting(name: str, default: int) -> int:
    try:
        value = int(os.getenv(name, str(default)))
    except ValueError:
        return default
    return max(value, 1)


def float_setting(name: str, default: float) -> float:
    try:
        value = float(os.getenv(name, str(default)))
    except ValueError:
        return default
    return max(value, 0.0)


@dataclass(frozen=True)
class Settings:
    database_path: Path
    object_store_root: Path
    object_store_prefix: str
    buffer_flush_batch_size: int
    buffer_flush_threshold: int
    buffer_flush_lease_seconds: int
    buffer_worker_poll_seconds: int
    fault_plan: FaultPlan

    @classmethod
    def from_env(cls) -> Settings:
        data_dir = Path(os.getenv("DATA_DIR", str(PROJECT_ROOT / "data")))
        return cls(
            database_path=Path(os.getenv("DATABASE_PATH", str(data_dir / "runtime.db"))),
            object_store_root=Path(os.getenv("OBJECT_STORE_ROOT", str(data_dir / "objects"))),
            object_store_prefix=os.getenv("OBJECT_STORE_PREFIX", ""),
            buffer_flush_batch_size=int_setting("BUFFER_FLUSH_BATCH_SIZE", 100),
            buffer_flush_threshold=int_setting("BUFFER_FLUSH_THRESHOLD", 100),
            buffer_flush_lease_seconds=int_setting("BUFFER_FLUSH_LEASE_SECONDS", 300),
            buffer_worker_poll_seconds=int_setting("BUFFER_WORKER_POLL_SECONDS", 5),
            fault_plan=FaultPlan(
                put_failures=int(float_setting("FAULT_PUT_FAILURES", 0)),
                lost_acks=int(float_setting("FAULT_PUT_LOST_ACKS", 0)),
                failure_rate=min(float_setting("FAULT_PUT_FAILURE_RATE", 0.0), 1.0),
                latency_seconds=float_setting("FAULT_PUT_LATENCY_MS", 0.0) / 1000,
                seed=int(float_setting("FAULT_SEED", 0)),
            ),
        )


def build_repository(settings: Settings, clock: Clock | None = None) -> Repository:
    return SqliteRepository(settings.database_path, clock=clock)


def build_object_store(settings: Settings) -> ObjectStore:
    store: ObjectStore = LocalObjectStore(settings.object_store_root, settings.object_store_prefix)
    if settings.fault_plan.enabled:
        store = FaultyObjectStore(store, settings.fault_plan)
    return store
