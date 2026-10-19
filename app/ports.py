"""Boundaries between the application and its storage.

The API, worker, backup and restore code depend only on these protocols. Each
method of `Repository` is one transactional operation; every adapter must keep
the same atomicity and error codes.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Protocol


Row = dict[str, Any]


class ObjectNotFound(KeyError):
    pass


class ObjectStore(Protocol):
    def key(self, category: str, date: datetime, filename: str) -> str: ...

    async def put(self, key: str, body: bytes, content_type: str) -> bool:
        """Create the object only if the key is absent. Return False if it already exists."""
        ...

    async def get(self, key: str) -> bytes:
        """Return the object body or raise ObjectNotFound."""
        ...


class Repository(Protocol):
    async def close(self) -> None: ...

    # Agents
    async def create_agent(self, model_provider: str, skills: list[str], llm_config: dict[str, Any]) -> Row: ...

    async def get_agent(self, aid: str) -> Row: ...

    async def list_agents(self, status: str | None, skills: list[str]) -> list[Row]: ...

    async def modify_agent(self, aid: str, llm_config: dict[str, Any]) -> Row: ...

    async def disable_agent(self, aid: str) -> Row: ...

    # Tasks
    async def create_task(self, aid: str, input_payload: dict[str, Any]) -> Row: ...

    async def get_task(self, task_id: str) -> Row: ...

    async def list_tasks(self, aid: str | None, status: str | None, since: datetime | None) -> list[Row]: ...

    async def record_task_result(self, task_id: str, output_payload: Any, status: str) -> Row: ...

    # Runtime state
    async def get_agent_state(self, aid: str) -> Row: ...

    async def record_agent_state(
        self,
        aid: str,
        task_id: str | None,
        state: str,
        timestamp: datetime | None,
        message: Any,
    ) -> Row: ...

    # Buffer
    async def enqueue_buffer_event(self, event_type: str, payload: Any, event_timestamp: datetime | None) -> Row: ...

    async def get_buffer_status(self) -> dict[str, int]: ...

    async def recover_expired_buffer_leases(self) -> int: ...

    async def claim_buffer_batch(
        self,
        batch_size: int,
        threshold: int,
        force: bool,
        lease_seconds: int,
    ) -> list[Row]: ...

    async def mark_buffer_flushed(self, flush_id: str, object_key: str, flushed_at: datetime | None) -> int:
        """Finalize a flush; flushed_at defaults to the repository clock."""
        ...

    async def mark_buffer_retry(self, flush_id: str, error: str, backoff_seconds: int) -> dict[str, int]: ...

    # Backup and restore
    async def snapshot_tables(self, tables: dict[str, str]) -> dict[str, list[Row]]:
        """Read every table, ordered by its key columns, from one consistent snapshot."""
        ...

    async def restore_tables(self, table_rows: dict[str, list[Row]], runtime_current: list[Row]) -> None:
        """Upsert all rows in a single transaction: either the whole backup lands or nothing does."""
        ...
