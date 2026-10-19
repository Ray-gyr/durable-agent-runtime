from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from datetime import datetime, timezone
from pathlib import Path


MIGRATIONS_DIR = Path(__file__).resolve().parent / "migrations"


def split_statements(script: str) -> Iterator[str]:
    """Split a SQL script on statement boundaries without breaking quoted semicolons."""
    buffer = ""
    for line in script.splitlines(keepends=True):
        buffer += line
        if sqlite3.complete_statement(buffer):
            yield buffer.strip()
            buffer = ""
    leftover = "\n".join(
        line for line in buffer.splitlines() if line.strip() and not line.strip().startswith("--")
    )
    if leftover:
        raise ValueError(f"Incomplete SQL statement at end of migration: {leftover[:80]!r}")


def apply_migrations(conn: sqlite3.Connection, migrations_dir: Path = MIGRATIONS_DIR) -> list[str]:
    """Apply each pending migration in its own transaction and record it in schema_migrations.

    `executescript` is avoided on purpose: it commits implicitly, which would let a
    half-applied migration be recorded. Each statement runs inside BEGIN IMMEDIATE so
    two processes starting at once cannot both apply the same version.
    """
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute(
        "CREATE TABLE IF NOT EXISTS schema_migrations (version TEXT PRIMARY KEY, applied_at TEXT NOT NULL)"
    )
    applied: list[str] = []
    for path in sorted(migrations_dir.glob("*.sql")):
        version = path.stem
        conn.execute("BEGIN IMMEDIATE")
        try:
            if conn.execute("SELECT 1 FROM schema_migrations WHERE version = ?", (version,)).fetchone():
                conn.execute("COMMIT")
                continue
            for statement in split_statements(path.read_text(encoding="utf-8")):
                conn.execute(statement)
            conn.execute(
                "INSERT INTO schema_migrations (version, applied_at) VALUES (?, ?)",
                (version, datetime.now(timezone.utc).isoformat(timespec="microseconds")),
            )
            conn.execute("COMMIT")
            applied.append(version)
        except BaseException:
            conn.execute("ROLLBACK")
            raise
    return applied
