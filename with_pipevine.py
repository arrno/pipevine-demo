import time
from typing import Any, Callable

from pipevine import Pipeline, mix_pool, work_pool
from pipevine.util import is_ok
from pipevine.worker_state import WorkerState

from logic import (
    calculate_signal_energy, 
    calculate_spectral_entropy, 
    calculate_transition_complexity,
    merge_feature_batches
)
from config import config
from adapters import FakeApiClient, Database


client = FakeApiClient(config, seed=42)
db = Database(config.db_latency)


def feature_energy(sample: dict[str, Any], state: WorkerState) -> dict[str, Any]:
    return {
        "record": sample,
        "metrics": {"signal_energy": calculate_signal_energy(sample)},
    }


def feature_entropy(sample: dict[str, Any], state: WorkerState) -> dict[str, Any]:
    return {"metrics": {"spectral_entropy": calculate_spectral_entropy(sample)}}


def feature_complexity(sample: dict[str, Any], state: WorkerState) -> dict[str, Any]:
    return {"metrics": {"transition_complexity": calculate_transition_complexity(sample)}}

    
@mix_pool(buffer=32, multi_proc=True, fork_merge=merge_feature_batches)
def analysis_stage() -> list[Callable]:
    return [
        feature_energy, 
        feature_entropy, 
        feature_complexity
    ]


@work_pool(buffer=48, retries=3, num_workers=2)
async def store(enriched: dict[str, Any], state: WorkerState) -> dict[str, Any]:
    conn = state.get("connection")
    if conn is None:
        conn = await db.connect()
        state.update(connection=conn)
    await conn.write(enriched)
    return enriched


async def run_pipevine() -> tuple[float, list[dict[str, Any]]]:
    start = time.perf_counter()
    result = await (
        Pipeline(client.stream_items()) >>
        analysis_stage >> 
        store
    ).run()

    duration = time.perf_counter() - start
    assert is_ok(result)
    return duration, db.rows
