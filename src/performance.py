"""Measure local throughput using a reproducible generated batch."""

import argparse
import tempfile
import time
from pathlib import Path

from .pipeline import run_pipeline
from .sample_data import generate


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=10_000)
    parser.add_argument("--seed", type=int, default=17)
    args = parser.parse_args()

    with tempfile.TemporaryDirectory(prefix="identity-pipeline-") as temporary_dir:
        root = Path(temporary_dir)
        generate(root / "input", args.rows, args.seed, include_edge_cases=False)

        started = time.perf_counter()
        result = run_pipeline(
            root / "input",
            root / "catalog.sqlite",
            root / "reports",
            run_id="benchmark",
        )
        elapsed = time.perf_counter() - started

    throughput = result["input_rows"] / elapsed
    print(
        f"source_rows={result['input_rows']} "
        f"mapped={result['mapped_rows']} "
        f"elapsed_seconds={elapsed:.3f} "
        f"rows_per_second={throughput:.0f}"
    )


if __name__ == "__main__":
    main()
