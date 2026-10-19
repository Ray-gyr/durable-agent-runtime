from __future__ import annotations

from collections.abc import AsyncIterator, Iterator
from datetime import datetime, timezone
from pathlib import Path

import pytest
from httpx import ASGITransport, AsyncClient

from app.adapters.faulty_store import FaultPlan
from app.adapters.local_store import LocalObjectStore
from app.adapters.sqlite import SqliteRepository
from app.clock import ManualClock
from app.config import Settings
from app.main import app


MISSING = object()
START = datetime(2026, 8, 26, 0, 1, 2, tzinfo=timezone.utc)


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


@pytest.fixture
def clock() -> ManualClock:
    return ManualClock(START)


@pytest.fixture
def repo(tmp_path: Path, clock: ManualClock) -> SqliteRepository:
    return SqliteRepository(tmp_path / "runtime.db", clock=clock)


@pytest.fixture
def store(tmp_path: Path) -> LocalObjectStore:
    return LocalObjectStore(tmp_path / "objects", "test")


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    return Settings(
        database_path=tmp_path / "runtime.db",
        object_store_root=tmp_path / "objects",
        object_store_prefix="test",
        buffer_flush_batch_size=100,
        buffer_flush_threshold=100,
        buffer_flush_lease_seconds=300,
        buffer_worker_poll_seconds=1,
        fault_plan=FaultPlan(),
    )


@pytest.fixture
def app_state(repo: SqliteRepository, store: LocalObjectStore, settings: Settings) -> Iterator[None]:
    """Install real local adapters on the app; ASGITransport does not run lifespan."""
    previous = {name: getattr(app.state, name, MISSING) for name in ("repo", "store", "settings")}
    app.state.repo, app.state.store, app.state.settings = repo, store, settings
    yield
    for name, value in previous.items():
        if value is MISSING:
            delattr(app.state, name)
        else:
            setattr(app.state, name, value)


@pytest.fixture
async def api(app_state: None) -> AsyncIterator[AsyncClient]:
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        yield client
