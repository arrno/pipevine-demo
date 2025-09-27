# Pipevine Demo

This workspace contains a runnable demonstration of [pipevine](https://pypi.org/project/pipevine/):

-   Pulls paginated results from a fake API using an async generator that feeds into the pipeline.
-   Fans each item out to three expensive analyses via a `mix_pool` running in `multi_proc` mode, with a merge function to stitch the per-process results back together.
-   Persists the enriched records with a buffered, retry-aware async `work_pool` stage that maintains a database connection in `WorkerState`.
-   Compares Pipevine’s dataflow to a vanilla implementation and records simple wall-clock benchmarks.

### Baseline

```python
async for sample in client.stream_items():
    parts = [
        feature_energy(sample),
        feature_entropy(sample),
        feature_complexity(sample),
    ]
    enriched = merge_feature_batches(parts)
    await conn.write(enriched)
```

### Pipevine

```python
result = await (
    Pipeline(client.stream_items()) >>
    analysis_stage >>
    store
).run()
```

## Main Files

-   `main.py` – main demo script
-   `baseline.py` – vanilla implementation (no concurrent stages/workers)
-   `with_pipevine.py` – implementation using Pipevine

## Running the Demo

Set up the project:

```bash
python3 -m venv venv
source ./venv/bin/activate
pip install -r requirements.txt
```

Run:

```bash
python ./main.py
```

Example output:

```
Running pipevine...
Running baseline...
Pipevine processed 600 records in 0.416s
Baseline processed 600 records in 1.108s
Outputs match between implementations.
Speedup: 2.67x over baseline
Sample metrics -> id: 0, energy: 62.6652, entropy: 4.6899, complexity: 0.4050
```

You can adjust the `DemoConfig` values in `config.py` to experiment with different workloads (more pages, heavier payloads, etc.).
