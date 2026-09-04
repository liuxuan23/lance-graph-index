#!/usr/bin/env python3
"""Summarize one or more LDBC SNB graph-index benchmark result directories."""

from __future__ import annotations

import argparse
import csv
from collections import defaultdict
from pathlib import Path
from statistics import mean, median


def percentile(values: list[float], quantile: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = round((len(ordered) - 1) * quantile)
    return ordered[index]


def load_rows(path: Path) -> list[dict]:
    with path.open("r", encoding="utf-8", newline="") as source:
        rows = list(csv.DictReader(source))
    for row in rows:
        row["latency_ms"] = float(row["latency_ms"])
        row["result_rows"] = int(row["result_rows"])
        row["success"] = row["success"].lower() == "true"
    return rows


def summarize(rows: list[dict]) -> list[dict]:
    grouped = defaultdict(list)
    for row in rows:
        if row["success"]:
            key = (row["query_id"], row["degree_bucket"], row["mode"])
            grouped[key].append(row)

    summaries = []
    for (query_id, bucket, mode), items in sorted(grouped.items()):
        times = [item["latency_ms"] for item in items]
        result_rows = [item["result_rows"] for item in items]
        summaries.append(
            {
                "query_id": query_id,
                "degree_bucket": bucket,
                "mode": mode,
                "runs": len(items),
                "mean_ms": mean(times),
                "p50_ms": median(times),
                "p95_ms": percentile(times, 0.95),
                "p99_ms": percentile(times, 0.99),
                "mean_result_rows": mean(result_rows),
            }
        )
    join_p50 = {
        (item["query_id"], item["degree_bucket"]): item["p50_ms"]
        for item in summaries
        if item["mode"] == "join"
    }
    for item in summaries:
        baseline = join_p50.get((item["query_id"], item["degree_bucket"]))
        item["join_speedup"] = baseline / item["p50_ms"] if baseline and item["p50_ms"] else 0.0
    return summaries


def render(summaries: list[dict], raw_names: list[str]) -> str:
    lines = [
        "# LDBC SNB Graph Index Benchmark Summary",
        "",
        "Raw results:",
        "",
        *(f"- `{name}`" for name in raw_names),
        "",
        "| query | degree bucket | mode | runs | mean ms | p50 ms | p95 ms | p99 ms | mean rows | join / mode |",
        "|---|---|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for item in summaries:
        lines.append(
            "| {query_id} | {degree_bucket} | {mode} | {runs} | {mean_ms:.3f} | "
            "{p50_ms:.3f} | {p95_ms:.3f} | {p99_ms:.3f} | {mean_result_rows:.1f} | "
            "{join_speedup:.2f}x |".format(**item)
        )
    return "\n".join(lines) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("result_dirs", nargs="+", type=Path)
    parser.add_argument(
        "--output",
        type=Path,
        help="summary Markdown path; required when combining multiple result directories",
    )
    args = parser.parse_args()
    raw_paths = [result_dir / "raw_results.csv" for result_dir in args.result_dirs]
    for raw_path in raw_paths:
        if not raw_path.is_file():
            raise FileNotFoundError(raw_path)
    if args.output is None and len(args.result_dirs) != 1:
        parser.error("--output is required when combining multiple result directories")
    summary_path = args.output or args.result_dirs[0] / "summary.md"
    rows = [row for raw_path in raw_paths for row in load_rows(raw_path)]
    summaries = summarize(rows)
    raw_names = [str(raw_path) for raw_path in raw_paths]
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(render(summaries, raw_names), encoding="utf-8")
    print(summary_path)


if __name__ == "__main__":
    main()
