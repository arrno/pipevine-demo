import math
from typing import Any


def calculate_signal_energy(sample: dict[str, Any]) -> float:
    payload = sample["payload"]
    return math.fsum(val * val for val in payload)


def calculate_spectral_entropy(sample: dict[str, Any]) -> float:
    payload = sample["payload"]
    total = math.fsum(abs(val) for val in payload) + 1e-12
    entropy = 0.0
    for val in payload:
        p = abs(val) / total
        entropy -= p * math.log(p + 1e-12)
    return entropy


def merge_feature_batches(results: list[dict[str, Any]]) -> dict[str, Any]:
    record: dict[str, Any] | None = None
    metrics: dict[str, float] = {}
    for partial in results:
        if record is None and "record" in partial:
            record = partial["record"]
        metrics.update(partial.get("metrics", {}))
    if record is None:
        raise RuntimeError("Missing original record in analysis results")
    return {
        "id": record["id"],
        "page": record["page"],
        "payload": record["payload"],
        "metrics": metrics,
    }


def calculate_transition_complexity(sample: dict[str, Any]) -> float:
    payload = sample["payload"]
    if len(payload) < 2:
        return 0.0
    acc = 0.0
    for current, previous in zip(payload[1:], payload[:-1]):
        diff = current - previous
        acc += math.sqrt(diff * diff + 1e-9)
    return acc / (len(payload) - 1)

def compare_results(
    pipevine_rows: list[dict[str, Any]],
    baseline_rows: list[dict[str, Any]],
    *,
    tol: float = 1e-9,
) -> tuple[list[int], list[int], list[int]]:
    sync_index = {row["id"]: row for row in baseline_rows}
    async_index = {row["id"]: row for row in pipevine_rows}

    missing_in_async = [rid for rid in sync_index if rid not in async_index]
    missing_in_sync = [rid for rid in async_index if rid not in sync_index]

    mismatched: list[int] = []
    for rid, async_row in async_index.items():
        other = sync_index.get(rid)
        if other is None:
            continue
        metrics_a = async_row.get("metrics", {})
        metrics_b = other.get("metrics", {})
        keys = set(metrics_a) | set(metrics_b)
        for key in keys:
            aval = metrics_a.get(key)
            bval = metrics_b.get(key)
            if aval is None or bval is None:
                mismatched.append(rid)
                break
            if not math.isclose(aval, bval, rel_tol=tol, abs_tol=tol):
                mismatched.append(rid)
                break
    return missing_in_async, missing_in_sync, mismatched