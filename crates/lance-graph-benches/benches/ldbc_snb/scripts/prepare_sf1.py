#!/usr/bin/env python3
"""Prepare LDBC SNB Person/KNOWS CSV files for the Rust graph-index benchmark."""

from __future__ import annotations

import argparse
import csv
import glob
import json
from pathlib import Path
from typing import Iterable

import lance
import pyarrow as pa


def normalize_column(name: str) -> str:
    return "".join(ch.lower() for ch in name if ch.isalnum())


def expand_inputs(patterns: Iterable[str]) -> list[Path]:
    paths: list[Path] = []
    for pattern in patterns:
        matches = sorted(Path(value) for value in glob.glob(pattern))
        if matches:
            paths.extend(matches)
        else:
            path = Path(pattern)
            if path.is_file():
                paths.append(path)
            else:
                raise FileNotFoundError(f"input pattern matched no files: {pattern}")
    unique = list(dict.fromkeys(path.resolve() for path in paths))
    if not unique:
        raise ValueError("at least one input file is required")
    return unique


def detect_delimiter(header: str) -> str:
    if "|" in header:
        return "|"
    if "," in header:
        return ","
    raise ValueError("unable to detect CSV delimiter; expected '|' or ','")


def read_rows(path: Path):
    with path.open("r", encoding="utf-8", newline="") as source:
        header_line = source.readline()
        if not header_line:
            return
        delimiter = detect_delimiter(header_line)
        source.seek(0)
        reader = csv.reader(source, delimiter=delimiter)
        header = next(reader)
        while header and header[-1] == "":
            header.pop()
        for row in reader:
            while len(row) > len(header) and row[-1] == "":
                row.pop()
            if not row or all(not value for value in row):
                continue
            if len(row) < len(header):
                row.extend([""] * (len(header) - len(row)))
            yield header, row[: len(header)]


def find_person_columns(header: list[str]) -> tuple[int, int | None, int | None]:
    normalized = [normalize_column(name) for name in header]
    id_idx = next(
        (
            idx
            for idx, name in enumerate(normalized)
            if name in {"id", "personid"} or name.endswith("personid")
        ),
        None,
    )
    if id_idx is None:
        raise ValueError(f"unable to find Person ID column in header: {header}")
    first_idx = next((idx for idx, name in enumerate(normalized) if name == "firstname"), None)
    last_idx = next((idx for idx, name in enumerate(normalized) if name == "lastname"), None)
    return id_idx, first_idx, last_idx


def find_knows_columns(header: list[str]) -> tuple[int, int]:
    normalized = [normalize_column(name) for name in header]
    candidates = [
        idx
        for idx, name in enumerate(normalized)
        if name in {"id", "personid", "person1id", "person2id"}
        or name.endswith("personid")
    ]
    if len(candidates) < 2:
        if len(header) < 2:
            raise ValueError(f"KNOWS header has fewer than two columns: {header}")
        return 0, 1
    return candidates[0], candidates[1]


def read_people(paths: list[Path]) -> list[tuple[int, str, str]]:
    people: dict[int, tuple[str, str]] = {}
    for path in paths:
        columns = None
        for header, row in read_rows(path):
            if columns is None:
                columns = find_person_columns(header)
            id_idx, first_idx, last_idx = columns
            original_id = int(row[id_idx])
            first_name = row[first_idx] if first_idx is not None else ""
            last_name = row[last_idx] if last_idx is not None else ""
            previous = people.setdefault(original_id, (first_name, last_name))
            if previous != (first_name, last_name):
                raise ValueError(f"conflicting Person rows for ID {original_id}")
    if not people:
        raise ValueError("Person inputs contained no rows")
    return [(person_id, *people[person_id]) for person_id in sorted(people)]


def read_knows(
    paths: list[Path], dense_by_original: dict[int, int]
) -> tuple[list[tuple[int, int]], int]:
    directed: list[tuple[int, int]] = []
    logical_edges = 0
    for path in paths:
        columns = None
        for header, row in read_rows(path):
            if columns is None:
                columns = find_knows_columns(header)
            src_idx, dst_idx = columns
            original_src = int(row[src_idx])
            original_dst = int(row[dst_idx])
            try:
                src = dense_by_original[original_src]
                dst = dense_by_original[original_dst]
            except KeyError as error:
                raise ValueError(
                    f"KNOWS references missing Person ID {error.args[0]} in {path}"
                ) from error
            logical_edges += 1
            directed.append((src, dst))
            if src != dst:
                directed.append((dst, src))
    directed.sort()
    return directed, logical_edges


def evenly_spaced(values: list[int], count: int) -> list[int]:
    if count <= 0 or not values:
        return []
    if len(values) <= count:
        return values
    if count == 1:
        return [values[len(values) // 2]]
    return [values[round(idx * (len(values) - 1) / (count - 1))] for idx in range(count)]


def path_count(adjacency: list[list[int]], seed: int, depth: int) -> int:
    if depth == 0:
        return 1
    return sum(path_count(adjacency, target, depth - 1) for target in adjacency[seed])


def choose_seeds(
    adjacency: list[list[int]], original_by_dense: list[int], per_bucket: int
) -> dict:
    by_degree = sorted(range(len(adjacency)), key=lambda value: (len(adjacency[value]), value))
    nonzero = [value for value in by_degree if adjacency[value]]
    zero = [value for value in by_degree if not adjacency[value]]

    def rank_slice(start: float, end: float) -> list[int]:
        if not nonzero:
            return []
        left = min(len(nonzero), int(len(nonzero) * start))
        right = min(len(nonzero), max(left + 1, int(len(nonzero) * end)))
        return nonzero[left:right]

    candidates = {
        "zero": zero,
        "low": rank_slice(0.10, 0.30),
        "medium": rank_slice(0.45, 0.55),
        "high": rank_slice(0.90, 0.99),
        "hub": list(reversed(nonzero[-per_bucket:])),
    }

    groups = {}
    for bucket, values in candidates.items():
        selected = values[:per_bucket] if bucket == "hub" else evenly_spaced(values, per_bucket)
        groups[bucket] = [
            {
                "person_id": seed,
                "original_person_id": original_by_dense[seed],
                "degree": len(adjacency[seed]),
                "two_hop_path_count": path_count(adjacency, seed, 2),
                "three_hop_path_count": path_count(adjacency, seed, 3),
            }
            for seed in selected
        ]

    mixed = []
    for bucket in ("low", "medium", "high", "hub"):
        mixed.extend(seed["person_id"] for seed in groups[bucket])
    return {
        "relationship": "KNOWS",
        "groups": groups,
        "batches": {
            "mixed_10": mixed[:10],
            "mixed_100": mixed[:100],
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--person", action="append", required=True, help="Person CSV path/glob")
    parser.add_argument("--knows", action="append", required=True, help="KNOWS CSV path/glob")
    parser.add_argument("--output", required=True, type=Path, help="Prepared dataset root")
    parser.add_argument("--scale-factor", default="1")
    parser.add_argument("--seeds-per-bucket", type=int, default=10)
    parser.add_argument("--overwrite", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    person_files = expand_inputs(args.person)
    knows_files = expand_inputs(args.knows)
    root = args.output.resolve()
    person_uri = root / "datasets" / "Person.lance"
    knows_uri = root / "datasets" / "KNOWS.lance"
    if not args.overwrite and (person_uri.exists() or knows_uri.exists()):
        raise FileExistsError(
            f"prepared datasets already exist under {root}; pass --overwrite to replace them"
        )
    root.mkdir(parents=True, exist_ok=True)
    person_uri.parent.mkdir(parents=True, exist_ok=True)

    people = read_people(person_files)
    original_by_dense = [person[0] for person in people]
    dense_by_original = {original: dense for dense, original in enumerate(original_by_dense)}
    directed_edges, logical_edge_count = read_knows(knows_files, dense_by_original)

    person_table = pa.table(
        {
            "person_id": pa.array(range(len(people)), type=pa.int64()),
            "original_person_id": pa.array(original_by_dense, type=pa.int64()),
            "first_name": pa.array([person[1] for person in people], type=pa.string()),
            "last_name": pa.array([person[2] for person in people], type=pa.string()),
        }
    )
    knows_table = pa.table(
        {
            "src_id": pa.array([edge[0] for edge in directed_edges], type=pa.int64()),
            "dst_id": pa.array([edge[1] for edge in directed_edges], type=pa.int64()),
        }
    )
    mode = "overwrite" if args.overwrite else "create"
    person_dataset = lance.write_dataset(person_table, person_uri, mode=mode)
    person_dataset.create_scalar_index(
        "person_id", "BTREE", name="person_id_btree", replace=False
    )
    person_dataset = lance.dataset(person_uri)
    knows_dataset = lance.write_dataset(knows_table, knows_uri, mode=mode)

    adjacency: list[list[int]] = [[] for _ in people]
    for src, dst in directed_edges:
        adjacency[src].append(dst)
    seeds = choose_seeds(adjacency, original_by_dense, args.seeds_per_bucket)
    seeds.update({"dataset": "ldbc-snb-interactive", "scale_factor": args.scale_factor})

    manifest = {
        "dataset": "ldbc-snb-interactive",
        "scale_factor": args.scale_factor,
        "person_count": len(people),
        "logical_knows_edges": logical_edge_count,
        "physical_knows_edges": len(directed_edges),
        "id_mapping": "person_dense_0_based",
        "knows_direction": "bidirectional_materialized_as_outgoing",
        "person_dataset_uri": str(person_uri),
        "person_dataset_version": person_dataset.version,
        "knows_dataset_uri": str(knows_uri),
        "knows_dataset_version": knows_dataset.version,
        "person_inputs": [str(path) for path in person_files],
        "knows_inputs": [str(path) for path in knows_files],
    }
    (root / "manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    (root / "seeds.json").write_text(
        json.dumps(seeds, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(json.dumps(manifest, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
