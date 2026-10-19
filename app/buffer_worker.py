from __future__ import annotations

import argparse
import asyncio
import json

from .buffer_service import flush_once
from .config import Settings, build_object_store, build_repository
from .ports import ObjectStore, Repository


async def run_worker(repo: Repository, store: ObjectStore, settings: Settings, once: bool = False) -> None:
    # No shutdown handshake is needed: a worker killed mid-flush leaves its batch
    # leased, and the next claim recovers it once the lease expires.
    while True:
        result = await flush_once(
            repo,
            store,
            batch_size=settings.buffer_flush_batch_size,
            threshold=settings.buffer_flush_threshold,
            force=False,
            lease_seconds=settings.buffer_flush_lease_seconds,
        )
        if result["status"] != "idle":
            print(json.dumps(result, ensure_ascii=False), flush=True)
        if once:
            return
        await asyncio.sleep(settings.buffer_worker_poll_seconds)


async def main_async(once: bool) -> None:
    settings = Settings.from_env()
    repo = build_repository(settings)
    try:
        await run_worker(repo, build_object_store(settings), settings, once=once)
    finally:
        await repo.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Flush the buffer to the object store")
    parser.add_argument("--once", action="store_true", help="Run one flush attempt and exit")
    args = parser.parse_args()
    asyncio.run(main_async(once=args.once))


if __name__ == "__main__":
    main()
