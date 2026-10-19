from __future__ import annotations

import httpx
import pytest

from test.integration_support import assert_status, fastapi_base_url


pytestmark = pytest.mark.smoke


def test_deployed_fastapi_agent_task_lifecycle():
    with httpx.Client(base_url=fastapi_base_url(), timeout=30.0) as api:
        assert_status(api.get("/health"), 200, "health")

        created = api.post(
            "/v1/agents",
            json={"model_provider": "fastapi-smoke", "skills": ["smoke-a", "smoke-b"], "llm_config": {"temperature": 0}},
        )
        assert_status(created, 201, "create_agent")
        agent_id = str(created.json()["aid"])
        assert len(created.json()["skills"]) == 2

        task = api.post(f"/v1/agents/{agent_id}/tasks", json={"input_payload": {"prompt": "fastapi smoke"}})
        assert_status(task, 201, "create_task")
        task_id = str(task.json()["task_id"])

        busy = api.post(f"/v1/agents/{agent_id}/tasks", json={"input_payload": {"prompt": "busy"}})
        assert_status(busy, 409, "agent_busy")
        assert busy.json()["error_code"] == "AGENT_BUSY"

        result = api.post(f"/v1/tasks/{task_id}/result", json={"output_payload": {"ok": True}, "status": "completed"})
        assert_status(result, 200, "record_completed")
        assert result.json()["success"] is True
        assert result.json()["updated_task"]["completed_at"]

        duplicate = api.post(f"/v1/tasks/{task_id}/result", json={"output_payload": {}, "status": "failed"})
        assert_status(duplicate, 409, "terminal_task_rejected")
        assert duplicate.json()["error_code"] == "TASK_ALREADY_TERMINAL"

        disabled = api.post(f"/v1/agents/{agent_id}/disable")
        assert_status(disabled, 200, "disable_agent")
        assert disabled.json()["updated_agent"]["status"] == "disabled"

        rejected = api.post(f"/v1/agents/{agent_id}/tasks", json={"input_payload": {"prompt": "disabled"}})
        assert_status(rejected, 409, "disabled_agent_rejected")
        assert rejected.json()["error_code"] == "AGENT_DISABLED"
