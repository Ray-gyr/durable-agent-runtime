"""Real worker processes sharing one SQLite file and one object store directory.

These tests cross the process boundary, so locking, lease recovery and
conditional object creation are exercised by the OS, not by in-process fakes.
"""

from __future__ import annotations

import asyncio
import csv
import io
import os
import subprocess
import sys
import time
from collections.abc import Iterator
from pathlib import Path

import pytest

from app.adapters.sqlite import SqliteRepository
from test.integration_support import PROJECT_ROOT


pytestmark = [pytest.mark.integration, pytest.mark.anyio]


class Workers:
    def __init__(self, data_dir: Path) -> None:
        self.data_dir = data_dir
        self.processes: list[subprocess.Popen[bytes]] = []

    def env(self, **overrides: str) -> dict[str, str]:
        env = dict(os.environ)
        # Set every setting explicitly so a developer's .env cannot leak in.
        env.update(
            DATA_DIR=str(self.data_dir),
            DATABASE_PATH=str(self.data_dir / "runtime.db"),
            OBJECT_STORE_ROOT=str(self.data_dir / "objects"),
            OBJECT_STORE_PREFIX="it",
            BUFFER_FLUSH_BATCH_SIZE="25",
            BUFFER_FLUSH_THRESHOLD="1",
            BUFFER_FLUSH_LEASE_SECONDS="2",
            BUFFER_WORKER_POLL_SECONDS="1",
            FAULT_PUT_FAILURES="0",
            FAULT_PUT_LOST_ACKS="0",
            FAULT_PUT_FAILURE_RATE="0",
            FAULT_PUT_LATENCY_MS="0",
            PYTHONUNBUFFERED="1",
        )
        env.update(overrides)
        return env

    def start(self, **overrides: str) -> subprocess.Popen[bytes]:
        log = open(self.data_dir / f"worker-{len(self.processes)}.log", "wb")
        process = subprocess.Popen(
            [sys.executable, "-m", "app.buffer_worker"],
            cwd=PROJECT_ROOT,
            env=self.env(**overrides),
            stdout=log,
            stderr=subprocess.STDOUT,
        )
        log.close()
        self.processes.append(process)
        return process

    def stop_all(self) -> None:
        for process in self.processes:
            if process.poll() is None:
                process.kill()
            process.wait(timeout=10)


@pytest.fixture
def workers(tmp_path: Path) -> Iterator[Workers]:
    pool = Workers(tmp_path)
    yield pool
    pool.stop_all()


async def wait_for(repo: SqliteRepository, predicate, timeout: float = 60.0) -> dict[str, int]:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        status = await repo.get_buffer_status()
        if predicate(status):
            return status
        await asyncio.sleep(0.2)
    raise AssertionError(f"timed out; last buffer status: {await repo.get_buffer_status()}")


def flushed_files(data_dir: Path) -> dict[Path, list[int]]:
    files = sorted((data_dir / "objects" / "it" / "buffer").rglob("*.csv"))
    return {
        path: [int(row["seq"]) for row in csv.DictReader(io.StringIO(path.read_text(encoding="utf-8")))]
        for path in files
    }


async def test_competing_worker_processes_flush_every_event_exactly_once(tmp_path: Path, workers: Workers):
    repo = SqliteRepository(tmp_path / "runtime.db")
    for _ in range(3):
        workers.start()

    for index in range(300):
        await repo.enqueue_buffer_event("it.event", {"index": index}, None)
    await wait_for(repo, lambda status: status["flushed"] == 300)
    workers.stop_all()

    files = flushed_files(tmp_path)
    all_seqs = [seq for seqs in files.values() for seq in seqs]
    assert sorted(all_seqs) == list(range(1, 301)), "no event lost or written twice"
    assert all(seqs == sorted(seqs) for seqs in files.values())
    ranges = sorted((seqs[0], seqs[-1]) for seqs in files.values())
    assert all(previous[1] < current[0] for previous, current in zip(ranges, ranges[1:])), "batches never interleave"


async def test_worker_killed_mid_flush_is_recovered_by_another_process(tmp_path: Path, workers: Workers):
    repo = SqliteRepository(tmp_path / "runtime.db")
    for index in range(10):
        await repo.enqueue_buffer_event("it.event", {"index": index}, None)

    # Worker A claims the batch, then stalls inside the upload long enough to be killed.
    stalled = workers.start(FAULT_PUT_LATENCY_MS="60000")
    await wait_for(repo, lambda status: status["processing"] == 10, timeout=30)
    stalled.kill()
    stalled.wait(timeout=10)
    assert flushed_files(tmp_path) == {}

    workers.start()
    await wait_for(repo, lambda status: status["flushed"] == 10, timeout=30)

    files = flushed_files(tmp_path)
    assert list(files.values()) == [list(range(1, 11))], "the abandoned batch is flushed once, as one object"
