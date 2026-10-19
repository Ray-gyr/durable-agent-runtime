-- Agent/task API schema.
-- UUIDs, JSON and timestamps are TEXT. Timestamps are fixed-width ISO 8601 UTC
-- strings written by the application clock, so they compare lexicographically.

CREATE TABLE agent_registry (
  aid            TEXT PRIMARY KEY,
  model_provider TEXT NOT NULL,
  llm_config     TEXT NOT NULL DEFAULT '{}' CHECK (json_valid(llm_config)),
  status         TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'disabled')),
  created_at     TEXT NOT NULL,
  updated_at     TEXT NOT NULL,
  disabled_at    TEXT,
  CHECK ((status = 'disabled' AND disabled_at IS NOT NULL)
      OR (status = 'active' AND disabled_at IS NULL))
);

CREATE TABLE agent_skills (
  aid   TEXT NOT NULL REFERENCES agent_registry(aid) ON DELETE CASCADE,
  skill TEXT NOT NULL CHECK (length(trim(skill)) > 0),
  PRIMARY KEY (aid, skill)
);

CREATE TABLE tasks (
  task_id        TEXT PRIMARY KEY,
  aid            TEXT NOT NULL REFERENCES agent_registry(aid),
  input_payload  TEXT NOT NULL CHECK (json_valid(input_payload)),
  output_payload TEXT CHECK (output_payload IS NULL OR json_valid(output_payload)),
  status         TEXT NOT NULL DEFAULT 'pending'
    CHECK (status IN ('pending', 'running', 'completed', 'failed')),
  created_at     TEXT NOT NULL,
  completed_at   TEXT,
  CHECK ((status IN ('completed', 'failed') AND completed_at IS NOT NULL)
      OR (status IN ('pending', 'running') AND completed_at IS NULL))
);

-- One row per agent that can be claimed atomically by create_task. History
-- belongs in the append-only agent_runtime_log.
CREATE TABLE agent_runtime_current (
  aid        TEXT PRIMARY KEY REFERENCES agent_registry(aid) ON DELETE CASCADE,
  task_id    TEXT REFERENCES tasks(task_id),
  state      TEXT NOT NULL DEFAULT 'idle'
    CHECK (state IN ('idle', 'running', 'offline', 'disabled')),
  updated_at TEXT NOT NULL,
  CHECK ((state = 'running' AND task_id IS NOT NULL)
      OR (state IN ('idle', 'offline', 'disabled') AND task_id IS NULL))
);

CREATE TABLE agent_runtime_log (
  state_id  TEXT PRIMARY KEY,
  aid       TEXT NOT NULL REFERENCES agent_registry(aid) ON DELETE CASCADE,
  task_id   TEXT REFERENCES tasks(task_id),
  state     TEXT NOT NULL CHECK (state IN ('idle', 'running', 'offline', 'disabled')),
  timestamp TEXT NOT NULL,
  message   TEXT CHECK (message IS NULL OR json_valid(message))
);

CREATE INDEX agent_registry_status_idx ON agent_registry(status);
CREATE INDEX agent_skills_skill_aid_idx ON agent_skills(skill, aid);
CREATE INDEX tasks_aid_status_created_idx ON tasks(aid, status, created_at DESC);
CREATE INDEX tasks_created_idx ON tasks(created_at DESC);
CREATE INDEX runtime_log_aid_timestamp_idx ON agent_runtime_log(aid, timestamp DESC, state_id DESC);
