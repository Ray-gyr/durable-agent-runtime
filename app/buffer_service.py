from __future__ import annotations

import csv
import hashlib
import io
import json
from datetime import datetime, timezone
from typing import Any
from uuid import UUID, uuid4

from .ports import ObjectStore, Repository


BUFFER_CSV_FIELDS = [
    "seq",
    "event_id",
    "event_type",
    "event_timestamp",
    "payload_json",
    "created_at",
]

# Backed-up tables and the key columns that give each CSV a stable row order.
BACKUP_TABLES = {
    "agent_registry": "aid",
    "agent_skills": "aid, skill",
    "tasks": "task_id",
    # Insertion order, so restore can break equal-timestamp ties the same way
    # the live database does.
    "agent_runtime_log": "timestamp, rowid",
}


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def parse_timestamp(value: Any) -> datetime:
    if isinstance(value, datetime):
        timestamp = value
    else:
        timestamp = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=timezone.utc)
    return timestamp.astimezone(timezone.utc)


def serialize_cell(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (dict, list, bool, int, float)):
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    return str(value)


def events_to_csv(events: list[dict[str, Any]]) -> bytes:
    output = io.StringIO(newline="")
    writer = csv.DictWriter(output, fieldnames=BUFFER_CSV_FIELDS, extrasaction="ignore")
    writer.writeheader()
    for event in sorted(events, key=lambda item: int(item["seq"])):
        writer.writerow(
            {
                "seq": event["seq"],
                "event_id": event["event_id"],
                "event_type": event["event_type"],
                "event_timestamp": event.get("event_timestamp"),
                "payload_json": json.dumps(event["payload"], ensure_ascii=False, separators=(",", ":")),
                "created_at": event.get("created_at"),
            }
        )
    return output.getvalue().encode("utf-8")


def rows_to_csv(rows: list[dict[str, Any]]) -> bytes:
    fields: list[str] = []
    for row in rows:
        for field in row:
            if field not in fields:
                fields.append(field)
    output = io.StringIO(newline="")
    writer = csv.DictWriter(output, fieldnames=fields, extrasaction="ignore")
    writer.writeheader()
    for row in rows:
        writer.writerow({field: serialize_cell(row.get(field)) for field in fields})
    return output.getvalue().encode("utf-8")


async def flush_once(
    repo: Repository,
    store: ObjectStore,
    *,
    batch_size: int,
    threshold: int,
    force: bool,
    lease_seconds: int,
) -> dict[str, Any]:
    await repo.recover_expired_buffer_leases()
    events = await repo.claim_buffer_batch(batch_size, threshold, force, lease_seconds)
    if not events:
        return {"status": "idle", "event_count": 0, "object_key": None, "flush_id": None}

    flush_id = UUID(str(events[0]["flush_id"]))
    flush_timestamp = parse_timestamp(events[0]["flush_timestamp"])
    if any(str(event["flush_id"]) != str(flush_id) for event in events):
        raise RuntimeError("claim_buffer_batch returned multiple flush IDs")

    filename = f"flush-{flush_timestamp.strftime('%Y%m%dT%H%M%S%fZ')}-{flush_id}.csv"
    key = store.key("buffer", flush_timestamp, filename)
    try:
        body = events_to_csv(events)
        # A False result means an earlier attempt already uploaded this flush;
        # the object is final, so finishing the database update is enough.
        await store.put(key, body, "text/csv; charset=utf-8")
        flushed_count = await repo.mark_buffer_flushed(str(flush_id), key, None)
        return {
            "status": "flushed",
            "event_count": int(flushed_count or 0),
            "object_key": key,
            "flush_id": str(flush_id),
        }
    except Exception as exc:
        backoff = min(300, 2 ** min(max(int(events[0].get("attempt_count", 0)), 0), 8))
        retry_result = await repo.mark_buffer_retry(str(flush_id), str(exc), backoff)
        failed = int((retry_result or {}).get("failed", 0))
        return {
            "status": "failed" if failed else "retrying",
            "event_count": len(events),
            "object_key": key,
            "flush_id": str(flush_id),
            "retry": retry_result,
        }


async def create_backup(repo: Repository, store: ObjectStore) -> dict[str, Any]:
    backup_id = uuid4()
    created_at = utc_now()
    objects: list[dict[str, Any]] = []
    errors: list[str] = []
    try:
        snapshot = await repo.snapshot_tables(BACKUP_TABLES)
    except Exception as exc:
        snapshot = {}
        errors.append(f"snapshot: {exc}")
    for table in BACKUP_TABLES if not errors else ():
        try:
            rows = snapshot[table]
            body = rows_to_csv(rows)
            key = store.key("backups", created_at, f"{backup_id}/{table}.csv")
            await store.put(key, body, "text/csv; charset=utf-8")
            objects.append(
                {
                    "table": table,
                    "key": key,
                    "row_count": len(rows),
                    "byte_size": len(body),
                    "sha256": hashlib.sha256(body).hexdigest(),
                }
            )
        except Exception as exc:
            errors.append(f"{table}: {exc}")
            break

    manifest = {
        "backup_id": str(backup_id),
        "created_at": created_at.isoformat(),
        "date_partition": created_at.strftime("%Y-%m-%d"),
        "schema_version": "1",
        "tables": list(BACKUP_TABLES),
        "objects": objects,
        "status": "failed" if errors else "completed",
        "errors": errors,
    }
    # The manifest is written last: its presence with status=completed is the
    # signal that every data object above was fully written.
    manifest_key = store.key("backups", created_at, f"{backup_id}/manifest.json")
    await store.put(
        manifest_key,
        json.dumps(manifest, ensure_ascii=False, indent=2).encode("utf-8"),
        "application/manifest+json",
    )
    manifest["manifest_key"] = manifest_key
    return manifest


def validate_manifest(manifest: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    if manifest.get("status") != "completed":
        errors.append("manifest status is not completed")
    if set(manifest.get("tables", [])) != set(BACKUP_TABLES):
        errors.append("manifest table set does not match the backup contract")
    objects = {item.get("table"): item for item in manifest.get("objects", [])}
    for table in BACKUP_TABLES:
        if table not in objects:
            errors.append(f"missing backup object for {table}")
        elif not objects[table].get("key"):
            errors.append(f"missing object key for {table}")
    return errors
