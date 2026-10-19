"""Fault injection for the ObjectStore port.

Real object stores fail in two different ways, and the buffer must survive both:

* `put_failures`: the request fails before anything is written.
* `lost_acks`: the object is written, but the caller sees an error (timeout,
  dropped connection). The retry then hits an existing key, which is exactly the
  path that the deterministic flush key and conditional create protect.

Counted faults fire first, in that order; `failure_rate` then adds seeded random
pre-write failures so a run is reproducible from its seed.
"""

from __future__ import annotations

import asyncio
import random
from dataclasses import dataclass, field
from datetime import datetime

from ..ports import ObjectStore


class InjectedFault(RuntimeError):
    pass


@dataclass
class FaultPlan:
    put_failures: int = 0
    lost_acks: int = 0
    failure_rate: float = 0.0
    latency_seconds: float = 0.0
    seed: int = 0

    @property
    def enabled(self) -> bool:
        return bool(self.put_failures or self.lost_acks or self.failure_rate or self.latency_seconds)


@dataclass
class FaultyObjectStore:
    inner: ObjectStore
    plan: FaultPlan = field(default_factory=FaultPlan)
    injected: list[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        self._random = random.Random(self.plan.seed)
        self._put_failures_left = self.plan.put_failures
        self._lost_acks_left = self.plan.lost_acks

    def key(self, category: str, date: datetime, filename: str) -> str:
        return self.inner.key(category, date, filename)

    async def put(self, key: str, body: bytes, content_type: str) -> bool:
        if self.plan.latency_seconds:
            await asyncio.sleep(self.plan.latency_seconds)
        if self._put_failures_left > 0:
            self._put_failures_left -= 1
            self.injected.append(f"put_failure:{key}")
            raise InjectedFault(f"injected put failure for {key}")
        if self._lost_acks_left > 0:
            self._lost_acks_left -= 1
            await self.inner.put(key, body, content_type)
            self.injected.append(f"lost_ack:{key}")
            raise InjectedFault(f"injected lost acknowledgement for {key}")
        if self.plan.failure_rate and self._random.random() < self.plan.failure_rate:
            self.injected.append(f"random_failure:{key}")
            raise InjectedFault(f"injected random put failure for {key}")
        return await self.inner.put(key, body, content_type)

    async def get(self, key: str) -> bytes:
        return await self.inner.get(key)
