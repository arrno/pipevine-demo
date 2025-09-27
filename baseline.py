from typing import Any
import time

from adapters import FakeApiClient, Database
from config import config
from logic import (
    merge_feature_batches,
    calculate_signal_energy, 
    calculate_spectral_entropy, 
    calculate_transition_complexity
)


def feature_energy(sample: dict[str, Any]) -> dict[str, Any]:
    return {
        "record": sample,
        "metrics": {"signal_energy": calculate_signal_energy(sample)},
    }


def feature_entropy(sample: dict[str, Any]) -> dict[str, Any]:
    return {"metrics": {"spectral_entropy": calculate_spectral_entropy(sample)}}


def feature_complexity(sample: dict[str, Any]) -> dict[str, Any]:
    return {"metrics": {"transition_complexity": calculate_transition_complexity(sample)}}


async def run_baseline() -> tuple[float, list[dict[str, Any]]]:
    client = FakeApiClient(config, seed=42)
    db = Database(config.db_latency)
    conn = await db.connect()

    start = time.perf_counter()
    async for sample in client.stream_items():
        parts = [
            feature_energy(sample),
            feature_entropy(sample),
            feature_complexity(sample),
        ]
        enriched = merge_feature_batches(parts)
        await conn.write(enriched)

    duration = time.perf_counter() - start
    return duration, db.rows