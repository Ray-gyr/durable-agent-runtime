from __future__ import annotations

import hmac
import os
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any, Literal
from uuid import UUID

from fastapi import Depends, FastAPI, Query, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from .buffer_service import create_backup, flush_once
from .config import Settings, build_object_store, build_repository
from .errors import DomainError
from .ports import ObjectStore, Repository


class CreateAgentRequest(BaseModel):
    model_provider: str = Field(min_length=1)
    skills: list[str] = Field(default_factory=list)
    llm_config: dict[str, Any] = Field(default_factory=dict)


class ModifyAgentRequest(BaseModel):
    llm_config: dict[str, Any] = Field(default_factory=dict)


class CreateTaskRequest(BaseModel):
    input_payload: dict[str, Any] = Field(default_factory=dict)


class RecordTaskResultRequest(BaseModel):
    output_payload: dict[str, Any] | None = None
    status: Literal["completed", "failed"]


class RecordAgentStateRequest(BaseModel):
    task_id: UUID | None = None
    state: Literal["idle", "running", "offline", "disabled"]
    timestamp: datetime | None = None
    message: dict[str, Any] | None = None


class BufferEventRequest(BaseModel):
    event_type: str = Field(min_length=1)
    payload: Any
    event_timestamp: datetime | None = None


def repository(request: Request) -> Repository:
    return request.app.state.repo


def object_store(request: Request) -> ObjectStore:
    return request.app.state.store


def settings(request: Request) -> Settings:
    return request.app.state.settings


def require_buffer_debug(request: Request) -> None:
    expected = os.getenv("BUFFER_DEBUG_TOKEN", "").strip()
    if not expected:
        raise DomainError.of("BUFFER_DEBUG_NOT_CONFIGURED")
    provided = request.headers.get("X-Buffer-Debug-Token", "")
    if not hmac.compare_digest(provided, expected):
        raise DomainError.of("BUFFER_DEBUG_UNAUTHORIZED")


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.settings = Settings.from_env()
    app.state.repo = build_repository(app.state.settings)
    app.state.store = build_object_store(app.state.settings)
    try:
        yield
    finally:
        await app.state.repo.close()


app = FastAPI(title="Multi-Agent Backend API", version="1.0.0", lifespan=lifespan)


@app.exception_handler(DomainError)
async def domain_error_handler(_: Request, exc: DomainError):
    return JSONResponse(
        status_code=exc.status_code,
        content={"success": False, "error_code": exc.code, "message": exc.message},
    )


@app.exception_handler(RequestValidationError)
async def validation_error_handler(_: Request, exc: RequestValidationError):
    return JSONResponse(
        status_code=422,
        content={"success": False, "error_code": "INVALID_INPUT", "message": str(exc)},
    )


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/v1/agents", status_code=status.HTTP_201_CREATED)
async def create_agent(payload: CreateAgentRequest, repo: Repository = Depends(repository)):
    return await repo.create_agent(payload.model_provider.strip(), payload.skills, payload.llm_config)


@app.get("/v1/agents/{aid}")
async def get_agent(aid: UUID, repo: Repository = Depends(repository)):
    return await repo.get_agent(str(aid))


@app.get("/v1/agents")
async def list_agents(
    skill: list[str] = Query(default=[]),
    agent_status: str | None = Query(default=None, alias="status"),
    repo: Repository = Depends(repository),
):
    return await repo.list_agents(agent_status, skill)


@app.patch("/v1/agents/{aid}")
async def modify_agent(aid: UUID, payload: ModifyAgentRequest, repo: Repository = Depends(repository)):
    agent = await repo.modify_agent(str(aid), payload.llm_config)
    return {"success": True, "updated_agent": agent, "error_code": None, "message": None}


@app.post("/v1/agents/{aid}/disable")
async def disable_agent(aid: UUID, repo: Repository = Depends(repository)):
    agent = await repo.disable_agent(str(aid))
    return {"success": True, "updated_agent": agent, "error_code": None, "message": None}


@app.post("/v1/agents/{aid}/tasks", status_code=status.HTTP_201_CREATED)
async def create_task(aid: UUID, payload: CreateTaskRequest, repo: Repository = Depends(repository)):
    task = await repo.create_task(str(aid), payload.input_payload)
    return {"task_id": task["task_id"]}


@app.get("/v1/tasks/{task_id}")
async def get_task(task_id: UUID, repo: Repository = Depends(repository)):
    return await repo.get_task(str(task_id))


@app.get("/v1/tasks")
async def list_tasks(
    aid: UUID | None = None,
    task_status: str | None = Query(default=None, alias="status"),
    since: datetime | None = None,
    repo: Repository = Depends(repository),
):
    return await repo.list_tasks(str(aid) if aid else None, task_status, since)


@app.post("/v1/tasks/{task_id}/result")
async def record_task_result(task_id: UUID, payload: RecordTaskResultRequest, repo: Repository = Depends(repository)):
    task = await repo.record_task_result(str(task_id), payload.output_payload, payload.status)
    return {"success": True, "updated_task": task, "error_code": None, "message": None}


@app.get("/v1/agents/{aid}/state")
async def get_agent_current_state(aid: UUID, repo: Repository = Depends(repository)):
    return await repo.get_agent_state(str(aid))


@app.post("/v1/agents/{aid}/states", status_code=status.HTTP_201_CREATED)
async def record_agent_state(aid: UUID, payload: RecordAgentStateRequest, repo: Repository = Depends(repository)):
    state = await repo.record_agent_state(
        str(aid),
        str(payload.task_id) if payload.task_id else None,
        payload.state,
        payload.timestamp,
        payload.message,
    )
    return {"state_id": state["state_id"]}


@app.post("/v1/buffer/events", status_code=status.HTTP_201_CREATED)
async def enqueue_buffer_event(payload: BufferEventRequest, repo: Repository = Depends(repository)):
    event = await repo.enqueue_buffer_event(payload.event_type.strip(), payload.payload, payload.event_timestamp)
    return {"event_id": event["event_id"], "seq": event["seq"]}


@app.get("/v1/buffer/status")
async def get_buffer_status(repo: Repository = Depends(repository)):
    return await repo.get_buffer_status()


@app.post("/v1/buffer/flush")
async def manual_buffer_flush(
    _: None = Depends(require_buffer_debug),
    repo: Repository = Depends(repository),
    store: ObjectStore = Depends(object_store),
    config: Settings = Depends(settings),
):
    return await flush_once(
        repo,
        store,
        batch_size=config.buffer_flush_batch_size,
        threshold=config.buffer_flush_threshold,
        force=True,
        lease_seconds=config.buffer_flush_lease_seconds,
    )


@app.post("/v1/backups")
async def create_database_backup(
    _: None = Depends(require_buffer_debug),
    repo: Repository = Depends(repository),
    store: ObjectStore = Depends(object_store),
):
    return await create_backup(repo, store)
