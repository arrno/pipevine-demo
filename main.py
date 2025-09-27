import asyncio
import multiprocessing as mp

from logic import compare_results
from baseline import run_baseline
from with_pipevine import run_pipevine


def main() -> None:

    try:
        mp.set_start_method("fork")
    except RuntimeError:
        pass

    print("Running pipevine...")
    pipevine_duration, pipevine_rows = asyncio.run(run_pipevine())

    print("Running baseline...")
    baseline_duration, baseline_rows = asyncio.run(run_baseline())

    print(f"Pipevine processed {len(pipevine_rows)} records in {pipevine_duration:.3f}s")
    print(f"Baseline processed {len(baseline_rows)} records in {baseline_duration:.3f}s")

    missing_in_pipevine, missing_in_baseline, mismatched = compare_results(pipevine_rows, baseline_rows)

    if not missing_in_pipevine and not missing_in_baseline and not mismatched:
        print("Outputs match between implementations.")
    else:
        issues: list[str] = []
        if missing_in_baseline:
            issues.append(f"{len(missing_in_baseline)} missing from baseline output")
        if missing_in_pipevine:
            issues.append(f"{len(missing_in_pipevine)} missing from pipevine output")
        if mismatched:
            preview = ', '.join(str(rid) for rid in mismatched[:5])
            issues.append(f"{len(mismatched)} metric mismatches (e.g. ids: {preview})")
        print("WARNING: " + ", ".join(issues) + ".")

    if pipevine_duration > 0:
        print(f"Speedup: {baseline_duration / pipevine_duration:.2f}x over baseline")

    if pipevine_rows:
        sample = pipevine_rows[0]
        metrics = sample["metrics"]
        print(
            "Sample metrics -> "
            f"id: {sample['id']}, "
            f"energy: {metrics['signal_energy']:.4f}, "
            f"entropy: {metrics['spectral_entropy']:.4f}, "
            f"complexity: {metrics['transition_complexity']:.4f}"
        )


if __name__ == "__main__":
    main()