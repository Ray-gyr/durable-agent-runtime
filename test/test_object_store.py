"""ObjectStore contract: conditional create, atomic visibility, and key safety."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from pathlib import Path

import pytest

from app.adapters.faulty_store import FaultPlan, FaultyObjectStore, InjectedFault
from app.adapters.local_store import LocalObjectStore
from app.ports import ObjectNotFound


pytestmark = pytest.mark.anyio


def files_under(root: Path) -> list[Path]:
    return sorted(path for path in root.rglob("*") if path.is_file())


async def test_key_layout_uses_prefix_category_and_utc_day(store: LocalObjectStore):
    key = store.key("buffer", datetime(2026, 8, 26, 23, 59, tzinfo=timezone.utc), "flush.csv")
    assert key == "test/buffer/2026-08-26/flush.csv"
    assert LocalObjectStore(store.root, "").key("buffer", datetime(2026, 8, 26), "f") == "buffer/2026-08-26/f"


async def test_put_get_round_trip(store: LocalObjectStore):
    assert await store.put("test/a/b.txt", b"hello\n", "text/plain") is True
    assert await store.get("test/a/b.txt") == b"hello\n"


async def test_put_never_overwrites_an_existing_object(store: LocalObjectStore):
    assert await store.put("test/x.csv", b"first", "text/csv") is True
    assert await store.put("test/x.csv", b"second", "text/csv") is False
    assert await store.get("test/x.csv") == b"first"


async def test_concurrent_puts_to_one_key_have_exactly_one_winner(store: LocalObjectStore):
    results = await asyncio.gather(*(store.put("test/race.csv", f"{n}".encode(), "text/csv") for n in range(20)))

    assert results.count(True) == 1
    assert await store.get("test/race.csv") == str(results.index(True)).encode()
    assert files_under(store.root) == [store.path_for("test/race.csv")], "no temporary files are left behind"


async def test_missing_object_raises_object_not_found(store: LocalObjectStore):
    with pytest.raises(ObjectNotFound):
        await store.get("test/missing.json")


@pytest.mark.parametrize("key", ["", "/abs/path", "../escape", "a/../../escape", "a//b", "a/./b", "a\\b", "C:/x"])
async def test_keys_cannot_escape_the_store_root(store: LocalObjectStore, key: str):
    with pytest.raises(ValueError):
        await store.put(key, b"x", "text/plain")


async def test_injected_put_failure_writes_nothing(store: LocalObjectStore):
    faulty = FaultyObjectStore(store, FaultPlan(put_failures=1))

    with pytest.raises(InjectedFault):
        await faulty.put("test/f.csv", b"body", "text/csv")
    assert files_under(store.root) == []
    assert await faulty.put("test/f.csv", b"body", "text/csv") is True


async def test_injected_lost_ack_writes_the_object_but_reports_failure(store: LocalObjectStore):
    faulty = FaultyObjectStore(store, FaultPlan(lost_acks=1))

    with pytest.raises(InjectedFault):
        await faulty.put("test/f.csv", b"body", "text/csv")
    assert await store.get("test/f.csv") == b"body"
    assert await faulty.put("test/f.csv", b"body", "text/csv") is False
    assert faulty.injected == ["lost_ack:test/f.csv"]


async def test_random_failures_are_reproducible_from_the_seed(store: LocalObjectStore):
    async def outcomes(seed: int) -> list[bool]:
        faulty = FaultyObjectStore(store, FaultPlan(failure_rate=0.5, seed=seed))
        result = []
        for n in range(20):
            try:
                await faulty.put(f"test/{seed}/{n}", b"x", "text/plain")
                result.append(True)
            except InjectedFault:
                result.append(False)
        return result

    first = await outcomes(7)
    assert False in first and True in first
    assert [path.name for path in files_under(store.root / "test" / "7")] == sorted(
        str(n) for n, ok in enumerate(first) if ok
    )
    assert await outcomes(7) == first
    assert await outcomes(8) != first
