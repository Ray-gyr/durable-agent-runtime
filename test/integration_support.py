from __future__ import annotations

import os
from pathlib import Path

import httpx
import pytest
from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def load_test_environment() -> None:
    load_dotenv(PROJECT_ROOT / ".env")


def fastapi_base_url() -> str:
    load_test_environment()
    return os.getenv("FASTAPI_BASE_URL", "http://127.0.0.1:8080").strip().rstrip("/")


def object_store_root() -> Path:
    """Host path of the object store the deployed services write to (the compose bind mount)."""
    load_test_environment()
    return Path(os.getenv("SMOKE_OBJECT_STORE_ROOT", str(PROJECT_ROOT / "data" / "objects")))


def buffer_debug_token() -> str:
    load_test_environment()
    token = os.getenv("BUFFER_DEBUG_TOKEN", "").strip()
    if not token:
        pytest.fail("BUFFER_DEBUG_TOKEN is required for buffer smoke tests")
    return token


def assert_status(response: httpx.Response, expected: int, name: str) -> None:
    assert response.status_code == expected, (
        f"{name} expected HTTP {expected}, got {response.status_code}: {response.text}"
    )
