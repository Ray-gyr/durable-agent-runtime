"""Filesystem implementation of the ObjectStore port.

It provides the two object-store properties the buffer relies on:

* Conditional create (`IfNoneMatch="*"`): a put on an existing key returns False
  and leaves the original object untouched, so a retried flush cannot create a
  duplicate or overwrite a finished one.
* Atomic visibility: readers never see a partially written object. The body is
  written and fsynced to a temporary file, then published with `os.link`, which
  fails if the target exists and is atomic on POSIX filesystems and NTFS.
"""

from __future__ import annotations

import asyncio
import os
import re
from datetime import datetime
from pathlib import Path
from uuid import uuid4

from ..ports import ObjectNotFound


KEY_PATTERN = re.compile(r"[A-Za-z0-9._-]+(/[A-Za-z0-9._-]+)*")


def daily_prefix(prefix: str, category: str, timestamp: datetime) -> str:
    clean_prefix = prefix.strip("/")
    root = f"{clean_prefix}/" if clean_prefix else ""
    return f"{root}{category}/{timestamp.strftime('%Y-%m-%d')}"


class LocalObjectStore:
    def __init__(self, root: str | Path, prefix: str = "") -> None:
        self.root = Path(root).resolve()
        self.prefix = prefix.strip("/")
        self.root.mkdir(parents=True, exist_ok=True)

    def key(self, category: str, date: datetime, filename: str) -> str:
        return f"{daily_prefix(self.prefix, category, date)}/{filename}"

    def path_for(self, key: str) -> Path:
        # Validate the raw key rather than a normalized path: normalization would
        # map "a//b", "a/./b" or "C:/b" onto another key's file, and two keys
        # sharing one file would break the conditional-create guarantee.
        parts = key.split("/")
        if not KEY_PATTERN.fullmatch(key) or any(part in (".", "..") for part in parts):
            raise ValueError(f"invalid object key: {key!r}")
        path = self.root.joinpath(*parts)
        if self.root not in path.parents:
            raise ValueError(f"object key escapes the store root: {key!r}")
        return path

    def _put(self, key: str, body: bytes) -> bool:
        target = self.path_for(key)
        if target.exists():
            return False
        target.parent.mkdir(parents=True, exist_ok=True)
        temporary = target.with_name(f".{target.name}.{uuid4().hex}.tmp")
        try:
            with open(temporary, "xb") as handle:
                handle.write(body)
                handle.flush()
                os.fsync(handle.fileno())
            try:
                os.link(temporary, target)
            except FileExistsError:
                return False
            return True
        finally:
            temporary.unlink(missing_ok=True)

    async def put(self, key: str, body: bytes, content_type: str) -> bool:
        # content_type is accepted for interface parity with S3; files carry no metadata.
        return await asyncio.to_thread(self._put, key, body)

    def _get(self, key: str) -> bytes:
        try:
            return self.path_for(key).read_bytes()
        except FileNotFoundError as exc:
            raise ObjectNotFound(key) from exc

    async def get(self, key: str) -> bytes:
        return await asyncio.to_thread(self._get, key)
