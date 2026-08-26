#!/usr/bin/env python3
"""Reads a results directory the benchmark wrote and prints one row per run.

The raw series is the artifact; quantiles are recomputed here from the timings file rather than
trusted from the manifest, so a question asked later is answered from the data. Standard library
only.

Usage:
  summarize.py --results DIR
"""

import argparse
import json
import struct
from pathlib import Path


def quantile(sorted_values, q):
    if not sorted_values:
        return 0
    return sorted_values[int(q * (len(sorted_values) - 1))]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--results", type=Path, required=True)
    arguments = parser.parse_args()

    rows = []
    for manifest_path in sorted(arguments.results.glob("*-manifest.json")):
        manifest = json.loads(manifest_path.read_text())
        label = manifest["label"]
        raw = (arguments.results / f"{label}-timings.bin").read_bytes()
        timings = sorted(struct.unpack(f"<{len(raw) // 8}Q", raw))
        rows.append(
            (
                label,
                len(timings),
                quantile(timings, 0.50),
                quantile(timings, 0.99),
                quantile(timings, 0.999),
                timings[-1] if timings else 0,
                manifest.get("isolation", ""),
            )
        )

    print(f"{'label':<24} {'commands':>10} {'p50':>8} {'p99':>8} {'p99.9':>8} {'max':>10}  isolation")
    for label, count, p50, p99, p999, top, isolation in rows:
        print(f"{label:<24} {count:>10} {p50:>8} {p99:>8} {p999:>8} {top:>10}  {isolation}")


if __name__ == "__main__":
    main()
