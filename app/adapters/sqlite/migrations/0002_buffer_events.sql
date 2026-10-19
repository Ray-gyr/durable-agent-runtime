-- Durable generic JSON event buffer, flushed to the object store as CSV.
-- seq is an AUTOINCREMENT key: globally monotonic and never reused. Because
-- SQLite has a single writer, a lower seq can never commit after a higher one
-- has already been claimed.

CREATE TABLE buffer_events (
  seq             INTEGER PRIMARY KEY AUTOINCREMENT,
  event_id        TEXT NOT NULL UNIQUE,
  event_type      TEXT NOT NULL CHECK (length(trim(event_type)) > 0),
  payload         TEXT NOT NULL CHECK (json_valid(payload)),
  event_timestamp TEXT NOT NULL,
  status          TEXT NOT NULL DEFAULT 'pending'
    CHECK (status IN ('pending', 'processing', 'flushed', 'failed')),
  attempt_count   INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
  available_at    TEXT NOT NULL,
  lease_until     TEXT,
  flush_id        TEXT,
  flush_timestamp TEXT,
  object_key      TEXT,
  last_error      TEXT,
  created_at      TEXT NOT NULL,
  flushed_at      TEXT,
  CHECK ((status = 'flushed' AND object_key IS NOT NULL AND flushed_at IS NOT NULL)
      OR status <> 'flushed'),
  CHECK ((status = 'processing' AND flush_id IS NOT NULL AND flush_timestamp IS NOT NULL
          AND lease_until IS NOT NULL)
      OR status <> 'processing')
);

CREATE INDEX buffer_events_pending_idx ON buffer_events(status, available_at, seq)
  WHERE status = 'pending';
CREATE INDEX buffer_events_flush_idx ON buffer_events(flush_id, status, seq);
