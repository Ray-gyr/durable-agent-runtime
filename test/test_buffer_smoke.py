from __future__ import annotations

import time
from uuid import uuid4

import httpx
import pytest

from test.integration_support import assert_status, buffer_debug_token, fastapi_base_url, object_store_root


pytestmark = pytest.mark.smoke


def test_deployed_buffer_flush_and_backup_reach_the_object_store():
    headers = {"X-Buffer-Debug-Token": buffer_debug_token()}
    root = object_store_root()
    suffix = uuid4().hex

    with httpx.Client(base_url=fastapi_base_url(), timeout=30.0) as api:
        assert_status(api.get("/health"), 200, "health")
        flushed_before = int(api.get("/v1/buffer/status").json()["flushed"])

        manual_event = api.post(
            "/v1/buffer/events",
            json={"event_type": f"buffer-smoke-manual-{suffix}", "payload": {"comma": "a,b", "unicode": "café ✓"}},
        )
        assert_status(manual_event, 201, "enqueue_manual_event")

        manual_flush = api.post("/v1/buffer/flush", headers=headers)
        assert_status(manual_flush, 200, "manual_flush")
        manual_body = manual_flush.json()
        assert manual_body["status"] == "flushed"
        assert manual_body["event_count"] >= 1
        assert manual_body["object_key"].endswith(".csv")
        assert (root / manual_body["object_key"]).is_file()

        for index in range(100):
            event = api.post(
                "/v1/buffer/events",
                json={"event_type": f"buffer-smoke-auto-{suffix}", "payload": {"index": index}},
            )
            assert_status(event, 201, f"enqueue_auto_event_{index}")

        # The worker, not this test, must notice the threshold and flush.
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            if int(api.get("/v1/buffer/status").json()["flushed"]) >= flushed_before + 101:
                break
            time.sleep(1)
        else:
            pytest.fail("automatic threshold flush was not observed")

        backup = api.post("/v1/backups", headers=headers)
        assert_status(backup, 200, "create_backup")
        assert backup.json()["status"] == "completed"
        assert (root / backup.json()["manifest_key"]).is_file()
