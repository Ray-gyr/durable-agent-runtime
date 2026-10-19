from __future__ import annotations

import argparse
import asyncio
import csv
import hashlib
import io
import json
from typing import Any

from .buffer_service import BACKUP_TABLES, validate_manifest
from .config import Settings, build_object_store, build_repository
from .ports import ObjectStore, Repository


JSON_COLUMNS = {
    "agent_registry": {"llm_config"},
    "tasks": {"input_payload", "output_payload"},
    "agent_runtime_log": {"message"},
}


def rows_from_csv(table: str, body: bytes) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for raw in csv.DictReader(io.StringIO(body.decode("utf-8"))):
        row: dict[str, Any] = {}
        for key, value in raw.items():
            if value == "":
                row[key] = None
            elif key in JSON_COLUMNS.get(table, set()):
                row[key] = json.loads(value)
            else:
                row[key] = value
        rows.append(row)
    return rows


def rebuild_runtime_current(runtime_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Derive agent_runtime_current from the latest log row per agent.

    Only a `running` state keeps its task_id. Completing a task logs `idle` with
    the finished task_id; copying that row verbatim violates the table's
    state/task_id check, which made restoring an agent whose last event was a
    task completion fail.
    """
    latest: dict[str, dict[str, Any]] = {}
    # Rows arrive in insertion order, so on equal timestamps the later row wins.
    for row in runtime_rows:
        aid = str(row["aid"])
        previous = latest.get(aid)
        if previous is None or str(row.get("timestamp") or "") >= str(previous.get("timestamp") or ""):
            latest[aid] = row
    return [
        {
            "aid": aid,
            "task_id": row.get("task_id") if row["state"] == "running" else None,
            "state": row["state"],
            "updated_at": row.get("timestamp"),
        }
        for aid, row in latest.items()
    ]


async def restore_from_manifest(
    manifest_key: str,
    apply: bool,
    *,
    repo: Repository,
    store: ObjectStore,
) -> dict[str, Any]:
    manifest = json.loads((await store.get(manifest_key)).decode("utf-8"))
    validation_errors = validate_manifest(manifest)
    if validation_errors:
        raise ValueError("Invalid backup manifest: " + "; ".join(validation_errors))

    # Read and verify every object before writing anything.
    restored: dict[str, int] = {}
    table_rows: dict[str, list[dict[str, Any]]] = {}
    for table in BACKUP_TABLES:
        item = next(item for item in manifest["objects"] if item["table"] == table)
        body = await store.get(item["key"])
        if hashlib.sha256(body).hexdigest() != item.get("sha256"):
            raise ValueError(f"Checksum mismatch for {table}")
        rows = rows_from_csv(table, body)
        if len(rows) != item.get("row_count"):
            raise ValueError(f"Row count mismatch for {table}")
        restored[table] = len(rows)
        table_rows[table] = rows

    if apply:
        await repo.restore_tables(table_rows, rebuild_runtime_current(table_rows["agent_runtime_log"]))
    return {"manifest_key": manifest_key, "status": "applied" if apply else "validated", "rows": restored}


async def main_async(manifest_key: str, apply: bool) -> dict[str, Any]:
    settings = Settings.from_env()
    repo = build_repository(settings)
    try:
        return await restore_from_manifest(manifest_key, apply, repo=repo, store=build_object_store(settings))
    finally:
        await repo.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate or restore a database backup from the object store")
    parser.add_argument("--manifest-key", required=True)
    parser.add_argument("--apply", action="store_true", help="Write rows to the database; otherwise validate only")
    args = parser.parse_args()
    print(json.dumps(asyncio.run(main_async(args.manifest_key, args.apply)), ensure_ascii=False))


if __name__ == "__main__":
    main()
