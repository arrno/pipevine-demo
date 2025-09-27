import math
import random
import asyncio
from typing import AsyncIterator, Any

from config import DemoConfig


class Database:
    """Toy async database that keeps results in-memory."""

    def __init__(self, latency: float) -> None:
        self._latency = latency
        self._rows: list[dict[str, Any]] = []
        self._connected = False

    async def connect(self) -> "Database":
        if not self._connected:
            await asyncio.sleep(self._latency)
            self._connected = True
        return self

    async def write(self, row: dict[str, Any]) -> None:
        if not self._connected:
            raise RuntimeError("Database connection not initialized")
        await asyncio.sleep(self._latency)
        self._rows.append(row)

    @property
    def rows(self) -> list[dict[str, Any]]:
        return list(self._rows)


class FakeApiClient:
    """Simulated paginated API with deterministic payloads."""

    def __init__(self, config: DemoConfig, *, seed: int = 1) -> None:
        self._config = config
        self._seed = seed
        self._random = random.Random(seed)

    @property
    def total_pages(self) -> int:
        return math.ceil(self._config.total_items / self._config.page_size)

    def _index_range(self, page: int) -> range:
        start = page * self._config.page_size
        end = min(self._config.total_items, start + self._config.page_size)
        return range(start, end)

    def _build_sample(self, idx: int) -> dict[str, Any]:
        rng = random.Random(self._seed + idx)
        payload = [math.sin(rng.random() * math.pi) for _ in range(self._config.payload_size)]
        return {
            "id": idx,
            "page": idx // self._config.page_size,
            "payload": payload,
        }

    async def fetch_page(self, page: int) -> list[dict[str, Any]]:
        await asyncio.sleep(self._config.api_latency + self._config.api_jitter * self._random.random())
        return [self._build_sample(i) for i in self._index_range(page)]

    async def stream_items(self) -> AsyncIterator[dict[str, Any]]:
        for page in range(self.total_pages):
            for sample in await self.fetch_page(page):
                yield sample