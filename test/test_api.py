"""HTTP contract tests against the real SQLite repository.

No fake database: the API layer and the transactional rules run together.
"""

from __future__ import annotations

from uuid import UUID, uuid4

import pytest
from httpx import AsyncClient

from app.clock import ManualClock


pytestmark = pytest.mark.anyio


async def create_agent(api: AsyncClient, **overrides) -> dict:
    body = {"model_provider": "openai", "skills": ["planner"], "llm_config": {"temperature": 0.2}, **overrides}
    response = await api.post("/v1/agents", json=body)
    assert response.status_code == 201, response.text
    return response.json()


async def create_task(api: AsyncClient, aid: str, prompt: str = "hello"):
    return await api.post(f"/v1/agents/{aid}/tasks", json={"input_payload": {"prompt": prompt}})


async def test_health_returns_liveness_response(api: AsyncClient):
    result = await api.get("/health")
    assert result.status_code == 200
    assert result.json() == {"status": "ok"}


async def test_create_agent_returns_complete_agent_with_clean_skills(api: AsyncClient):
    agent = await create_agent(api, model_provider="  openai  ", skills=["planner", " planner ", "", "researcher"])
    assert UUID(agent["aid"])
    assert agent["model_provider"] == "openai"
    assert agent["llm_config"] == {"temperature": 0.2}
    assert agent["status"] == "active"
    assert agent["created_at"] and agent["updated_at"]
    assert agent["disabled_at"] is None
    assert agent["skills"] == ["planner", "researcher"]

    fetched = await api.get(f"/v1/agents/{agent['aid']}")
    assert fetched.json() == agent


async def test_blank_model_provider_is_rejected_by_the_repository(api: AsyncClient):
    result = await api.post("/v1/agents", json={"model_provider": "   "})
    assert result.status_code == 422
    assert result.json()["error_code"] == "INVALID_INPUT"


async def test_list_agents_uses_all_skill_semantics_and_status_filter(api: AsyncClient):
    both = await create_agent(api, skills=["planner", "researcher"])
    planner_only = await create_agent(api, skills=["planner"])
    await api.post(f"/v1/agents/{planner_only['aid']}/disable")

    matched = await api.get("/v1/agents", params=[("skill", "planner"), ("skill", "researcher")])
    assert [agent["aid"] for agent in matched.json()] == [both["aid"]]

    disabled = await api.get("/v1/agents", params={"status": "disabled"})
    assert [agent["aid"] for agent in disabled.json()] == [planner_only["aid"]]

    everyone = await api.get("/v1/agents")
    assert {agent["aid"] for agent in everyone.json()} == {both["aid"], planner_only["aid"]}


async def test_modify_and_disable_return_complete_updated_agent_envelopes(api: AsyncClient, clock: ManualClock):
    agent = await create_agent(api)
    clock.advance(1)
    modified = await api.patch(f"/v1/agents/{agent['aid']}", json={"llm_config": {"temperature": 0.7}})
    disabled = await api.post(f"/v1/agents/{agent['aid']}/disable")
    again = await api.post(f"/v1/agents/{agent['aid']}/disable")

    assert modified.status_code == 200
    assert modified.json()["success"] is True
    assert modified.json()["error_code"] is None
    assert modified.json()["updated_agent"]["llm_config"] == {"temperature": 0.7}
    assert modified.json()["updated_agent"]["updated_at"] > agent["updated_at"]
    assert modified.json()["updated_agent"]["skills"] == ["planner"]
    assert disabled.json()["updated_agent"]["status"] == "disabled"
    assert disabled.json()["updated_agent"]["disabled_at"] is not None
    assert again.status_code == 200, "disabling twice is idempotent"
    assert again.json()["updated_agent"] == disabled.json()["updated_agent"]

    rejected = await api.patch(f"/v1/agents/{agent['aid']}", json={"llm_config": {}})
    assert rejected.status_code == 409
    assert rejected.json()["error_code"] == "AGENT_DISABLED"


async def test_busy_agent_cannot_be_modified_or_disabled(api: AsyncClient):
    agent = await create_agent(api)
    assert (await create_task(api, agent["aid"])).status_code == 201

    modify = await api.patch(f"/v1/agents/{agent['aid']}", json={"llm_config": {}})
    disable = await api.post(f"/v1/agents/{agent['aid']}/disable")
    assert (modify.status_code, modify.json()["error_code"]) == (409, "AGENT_BUSY")
    assert (disable.status_code, disable.json()["error_code"]) == (409, "AGENT_BUSY")


async def test_task_lifecycle_claims_agent_and_writes_result_once(api: AsyncClient):
    agent = await create_agent(api)
    created = await create_task(api, agent["aid"])
    assert created.status_code == 201
    task_id = created.json()["task_id"]
    assert set(created.json()) == {"task_id"}

    task = (await api.get(f"/v1/tasks/{task_id}")).json()
    assert task["status"] == "pending"
    assert task["completed_at"] is None
    assert task["input_payload"] == {"prompt": "hello"}

    busy = await create_task(api, agent["aid"], "second")
    assert busy.status_code == 409
    assert busy.json() == {"success": False, "error_code": "AGENT_BUSY", "message": "AGENT_BUSY"}
    tasks = (await api.get("/v1/tasks", params={"aid": agent["aid"]})).json()
    assert [item["task_id"] for item in tasks] == [task_id], "a rejected claim must roll back its task insert"

    result = await api.post(f"/v1/tasks/{task_id}/result", json={"output_payload": {"ok": True}, "status": "completed"})
    assert result.status_code == 200
    assert result.json()["success"] is True
    assert result.json()["error_code"] is None
    assert result.json()["updated_task"]["status"] == "completed"
    assert result.json()["updated_task"]["output_payload"] == {"ok": True}
    assert result.json()["updated_task"]["completed_at"] is not None

    duplicate = await api.post(f"/v1/tasks/{task_id}/result", json={"output_payload": {}, "status": "failed"})
    assert (duplicate.status_code, duplicate.json()["error_code"]) == (409, "TASK_ALREADY_TERMINAL")

    assert (await create_task(api, agent["aid"], "after completion")).status_code == 201


async def test_disabled_and_missing_agents_reject_tasks(api: AsyncClient):
    agent = await create_agent(api)
    await api.post(f"/v1/agents/{agent['aid']}/disable")

    disabled = await create_task(api, agent["aid"])
    missing = await create_task(api, str(uuid4()))
    missing_task = await api.get(f"/v1/tasks/{uuid4()}")
    missing_result = await api.post(f"/v1/tasks/{uuid4()}/result", json={"status": "completed"})

    assert (disabled.status_code, disabled.json()["error_code"]) == (409, "AGENT_DISABLED")
    assert (missing.status_code, missing.json()["error_code"]) == (404, "NOT_FOUND")
    assert (missing_task.status_code, missing_task.json()["error_code"]) == (404, "TASK_NOT_FOUND")
    assert (missing_result.status_code, missing_result.json()["error_code"]) == (404, "TASK_NOT_FOUND")


async def test_list_tasks_filters_by_agent_status_and_since(api: AsyncClient, clock: ManualClock):
    first_agent = await create_agent(api)
    second_agent = await create_agent(api)
    first = (await create_task(api, first_agent["aid"])).json()["task_id"]
    await api.post(f"/v1/tasks/{first}/result", json={"status": "failed"})
    clock.advance(60)
    cutoff = clock.now().isoformat()
    second = (await create_task(api, second_agent["aid"])).json()["task_id"]

    newest_first = (await api.get("/v1/tasks")).json()
    by_status = (await api.get("/v1/tasks", params={"status": "failed"})).json()
    by_agent = (await api.get("/v1/tasks", params={"aid": second_agent["aid"]})).json()
    since = (await api.get("/v1/tasks", params={"since": cutoff})).json()

    assert [task["task_id"] for task in newest_first] == [second, first]
    assert [task["task_id"] for task in by_status] == [first]
    assert [task["task_id"] for task in by_agent] == [second]
    assert [task["task_id"] for task in since] == [second]


async def test_current_state_follows_the_runtime_log(api: AsyncClient):
    agent = await create_agent(api)
    assert (await api.get(f"/v1/agents/{agent['aid']}/state")).status_code == 404

    task_id = (await create_task(api, agent["aid"])).json()["task_id"]
    running = (await api.get(f"/v1/agents/{agent['aid']}/state")).json()
    assert (running["state"], running["task_id"]) == ("running", task_id)
    assert set(running) == {"state_id", "aid", "task_id", "state", "timestamp", "message"}

    await api.post(f"/v1/tasks/{task_id}/result", json={"status": "completed"})
    recorded = await api.post(f"/v1/agents/{agent['aid']}/states", json={"state": "offline", "message": {"why": "x"}})
    assert recorded.status_code == 201
    assert set(recorded.json()) == {"state_id"}

    latest = (await api.get(f"/v1/agents/{agent['aid']}/state")).json()
    assert latest["state_id"] == recorded.json()["state_id"]
    assert (latest["state"], latest["message"]) == ("offline", {"why": "x"})


@pytest.mark.parametrize(
    ("body", "status", "code"),
    [
        ({"state": "running"}, 422, "TASK_REQUIRED"),
        ({"state": "idle", "task_id": "TASK"}, 422, "TASK_NOT_ALLOWED"),
        ({"state": "disabled"}, 409, "AGENT_NOT_DISABLED"),
        ({"state": "running", "task_id": str(uuid4())}, 404, "TASK_NOT_FOUND"),
    ],
)
async def test_record_agent_state_validation(api: AsyncClient, body: dict, status: int, code: str):
    agent = await create_agent(api)
    task_id = (await create_task(api, agent["aid"])).json()["task_id"]
    body = {key: task_id if value == "TASK" else value for key, value in body.items()}

    result = await api.post(f"/v1/agents/{agent['aid']}/states", json=body)
    assert (result.status_code, result.json()["error_code"]) == (status, code)


async def test_agent_attached_to_a_task_cannot_report_a_detached_state(api: AsyncClient):
    agent = await create_agent(api)
    await create_task(api, agent["aid"])

    result = await api.post(f"/v1/agents/{agent['aid']}/states", json={"state": "idle"})
    assert (result.status_code, result.json()["error_code"]) == (409, "AGENT_BUSY")


async def test_invalid_uuid_uses_documented_input_error_shape(api: AsyncClient):
    result = await api.get("/v1/agents/not-a-uuid")
    assert result.status_code == 422
    assert result.json()["success"] is False
    assert result.json()["error_code"] == "INVALID_INPUT"
