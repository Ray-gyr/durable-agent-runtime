# Multi-Agent Backend API

FastAPI backend for a multi-agent system: an agent registry, atomic task
claiming, an append-only runtime log, a durable JSON event buffer that a
separate worker flushes to an object store as CSV, and verified backup and
restore.

It runs entirely on one machine: state lives in SQLite and exported objects in
a local directory, each behind a port so the storage can be swapped without
touching the application. [DESIGN.md](DESIGN.md) explains the data model,
concurrency and failure model, and where each guarantee is tested.

## Architecture

```text
Client
  |
  v
FastAPI API  ───────────────┐
  | agent/task APIs          | POST /v1/buffer/events
  v                          v
Repository port ─────── SqliteRepository (data/runtime.db, WAL)
                             ^
                             | claim by global seq, lease, retry
                             |
                     buffer worker process(es)
                             |
                             v
ObjectStore port ────── LocalObjectStore (data/objects/<prefix>/...)
                        [FaultyObjectStore wrapper when fault injection is on]
```

| Module | Role |
| --- | --- |
| `app/ports.py` | `Repository` and `ObjectStore` protocols |
| `app/adapters/sqlite/` | Repository implementation, schema migrations |
| `app/adapters/local_store.py` | Conditional-create, atomically published files |
| `app/adapters/faulty_store.py` | Object store fault injection |
| `app/config.py` | Environment settings; the only place adapters are chosen |
| `app/main.py` | HTTP API |
| `app/buffer_service.py` | CSV encoding, flush, backup manifest |
| `app/buffer_worker.py` | Polling worker process |
| `app/restore_backup.py` | Restore CLI |

## Quick start

With Docker:

```powershell
Copy-Item .env.example .env      # then set BUFFER_DEBUG_TOKEN
docker compose up --build
```

The API listens on `http://localhost:8080`; data is written to `./data`.

Without Docker (Python 3.11+):

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements-dev.txt
.\.venv\Scripts\python.exe -m uvicorn app.main:app --port 8080
.\.venv\Scripts\python.exe -m app.buffer_worker          # second terminal
.\.venv\Scripts\python.exe -m app.buffer_worker --once   # one iteration, for debugging
```

The schema is created and migrated automatically on startup.

## Configuration

All settings have defaults except `BUFFER_DEBUG_TOKEN`; see `.env.example`.

| Variable | Default | Purpose |
| --- | --- | --- |
| `BUFFER_DEBUG_TOKEN` | — | Required for manual flush and backup endpoints |
| `DATA_DIR` | `./data` | Holds `runtime.db` and `objects/` |
| `DATABASE_PATH` | `$DATA_DIR/runtime.db` | SQLite file |
| `OBJECT_STORE_ROOT` | `$DATA_DIR/objects` | Object store directory |
| `OBJECT_STORE_PREFIX` | empty (`local` in compose) | Key prefix, like an S3 prefix |
| `BUFFER_FLUSH_BATCH_SIZE` | `100` | Events per flush object |
| `BUFFER_FLUSH_THRESHOLD` | `100` | Pending events that trigger an automatic flush |
| `BUFFER_FLUSH_LEASE_SECONDS` | `300` | How long a claimed batch is reserved |
| `BUFFER_WORKER_POLL_SECONDS` | `5` | Worker poll interval |
| `WORKER_HEARTBEAT_FILE` | unset (`/tmp/worker.heartbeat` in compose) | File the worker touches every loop; its age is the worker's health check |
| `FAULT_PUT_FAILURES` | `0` | Fail the first N object writes before writing |
| `FAULT_PUT_LOST_ACKS` | `0` | Write the next N objects, then report failure |
| `FAULT_PUT_FAILURE_RATE` | `0` | Seeded random write failures (0–1) |
| `FAULT_PUT_LATENCY_MS` | `0` | Added latency per write |
| `FAULT_SEED` | `0` | Seed for random faults |

For example, to watch retries and idempotent re-uploads in the running stack:

```powershell
$env:FAULT_PUT_FAILURE_RATE='0.3'; $env:FAULT_PUT_LOST_ACKS='3'; docker compose up --build
```

## REST API

Response shapes and error codes are in [DESIGN.md](DESIGN.md#10-api-contract).

| Method | Path | Purpose |
| --- | --- | --- |
| POST | `/v1/agents` | Create an agent and its skills |
| GET | `/v1/agents/{aid}` | Get an agent and all skills |
| GET | `/v1/agents` | List/filter agents; repeated `skill` filters use AND semantics |
| PATCH | `/v1/agents/{aid}` | Update `llm_config` |
| POST | `/v1/agents/{aid}/disable` | Disable an idle agent |
| POST | `/v1/agents/{aid}/tasks` | Create a task and atomically claim the agent |
| GET | `/v1/tasks/{task_id}` | Get one task |
| GET | `/v1/tasks` | List/filter tasks |
| POST | `/v1/tasks/{task_id}/result` | Record a completed or failed result once |
| GET | `/v1/agents/{aid}/state` | Get the latest runtime state |
| POST | `/v1/agents/{aid}/states` | Append a runtime state event |
| POST | `/v1/buffer/events` | Enqueue a JSON event; returns `event_id` and global `seq` |
| GET | `/v1/buffer/status` | Counts of `pending`, `processing`, `flushed`, `failed` |
| POST | `/v1/buffer/flush` | Force one flush (requires `X-Buffer-Debug-Token`) |
| POST | `/v1/backups` | Back up the database (requires `X-Buffer-Debug-Token`) |

## Buffer flush behavior

1. The API stores the event; SQLite assigns a global, never-reused `seq`.
2. The worker recovers expired leases and claims the oldest eligible events in
   one write transaction, so concurrent claims (several workers, or the manual
   endpoint) get disjoint batches in `seq` order.
3. Automatic flush starts when pending events reach the threshold; manual
   flush ignores it.
4. The batch is written as one CSV object:

   ```text
   <OBJECT_STORE_PREFIX>/buffer/YYYY-MM-DD/flush-<UTC-timestamp>-<flush-id>.csv
   ```

5. Only after the object exists are the events marked `flushed`.

The object key is derived from the batch's `flush_id` and claim timestamp,
which survive retries. Object creation is conditional, so a retry after the
object was written (the database update failed, or the write's acknowledgement
was lost) finds the existing object and only finishes the database update.

On failure the attempt count is incremented in the database, the batch becomes
eligible again after 1, 2, 4, 8… seconds (capped at 300), and the fifth failure
marks it `failed`, which removes it from automatic retries so newer events keep
flowing. A worker that dies mid-flush leaves its batch leased; the next claim
after the lease expires picks it up under the same `flush_id`.

CSV columns: `seq,event_id,event_type,event_timestamp,payload_json,created_at`.

## Backup and restore

```text
<OBJECT_STORE_PREFIX>/backups/YYYY-MM-DD/<backup-id>/{agent_registry,agent_skills,tasks,agent_runtime_log}.csv
<OBJECT_STORE_PREFIX>/backups/YYYY-MM-DD/<backup-id>/manifest.json
```

All four tables are read from one database snapshot. The manifest is written
last and records keys, row counts, byte sizes and SHA-256 checksums.

```powershell
# Validate only
.\.venv\Scripts\python.exe -m app.restore_backup --manifest-key 'local/backups/YYYY-MM-DD/<id>/manifest.json'
# Validate, then write everything in one transaction
.\.venv\Scripts\python.exe -m app.restore_backup --manifest-key '...' --apply
```

Restore reads and verifies every object before writing anything, then upserts
the four tables and rebuilds `agent_runtime_current` from the latest log row
per agent, all in a single transaction.

## Tests

```powershell
.\.venv\Scripts\python.exe -m pytest -q                  # unit, contract and multi-process tests
.\.venv\Scripts\python.exe -m pytest -m smoke -q         # against running services
```

| Layer | Marker | What runs |
| --- | --- | --- |
| API / contract / buffer | none | FastAPI, real SQLite and filesystem in a temp dir, manual clock, injected faults |
| Integration | `integration` | Real worker processes competing for one database; a worker killed mid-flush |
| Smoke | `smoke` | HTTP against the running compose stack |

The testing strategy is described in [DESIGN.md](DESIGN.md#11-testing-strategy).

## Dependencies

`requirements.in` lists runtime dependencies and `requirements-dev.in` adds
pytest, pytest-randomly, Ruff and mypy. `requirements.txt` and
`requirements-dev.txt` are lock files; Docker installs only the runtime lock.

```powershell
pip-compile requirements.in --output-file=requirements.txt
pip-compile requirements-dev.in --constraint=requirements.txt --output-file=requirements-dev.txt
```

Compile in a Linux Python 3.11 environment so the `uvloop` platform marker is
kept.
