#!/usr/bin/env python3
"""Compose a broad local-adapter screen with predeclared confirmation runs."""

from __future__ import annotations

import argparse
import csv
import pathlib
import sys
from collections import defaultdict


Group = tuple[str, int, str, str]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--screen", required=True, type=pathlib.Path)
    parser.add_argument(
        "--confirmation", required=True, action="append", type=pathlib.Path
    )
    parser.add_argument("--output", required=True, type=pathlib.Path)
    parser.add_argument("--sources-output", required=True, type=pathlib.Path)
    return parser.parse_args()


def read_rows(path: pathlib.Path) -> tuple[list[str], list[dict[str, str]]]:
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None:
            raise ValueError(f"missing CSV header: {path}")
        rows = list(reader)
    if not rows:
        raise ValueError(f"no data rows: {path}")
    return reader.fieldnames, rows


def group(row: dict[str, str]) -> Group:
    if row.get("suite") != "local-adapters":
        raise ValueError(f"confirmation composer only accepts local-adapters rows: {row}")
    return (row["adapter"], int(row["records"]), row["api"], row["pattern"])


def measurement_shape(rows: list[dict[str, str]]) -> tuple[int, int]:
    pairs: set[int] = set()
    samples: dict[tuple[int, str], set[int]] = defaultdict(set)
    seen: set[tuple[int, str, int]] = set()
    for row in rows:
        pair = int(row["pair"])
        role = row["revision_role"]
        sample = int(row.get("run") or row.get("repetition") or 1)
        if pair <= 0 or sample <= 0:
            raise ValueError("pair and sample identifiers must be positive")
        if role not in {"baseline", "candidate"}:
            raise ValueError(f"unknown revision role: {role!r}")
        identity = (pair, role, sample)
        if identity in seen:
            raise ValueError(f"duplicate confirmation measurement: {identity}")
        seen.add(identity)
        pairs.add(pair)
        samples[(pair, role)].add(sample)
    expected_pairs = set(range(1, max(pairs) + 1))
    if pairs != expected_pairs:
        raise ValueError(
            f"confirmation pair identifiers are not contiguous: {sorted(pairs)}"
        )
    for pair in sorted(pairs):
        baseline = samples.get((pair, "baseline"))
        candidate = samples.get((pair, "candidate"))
        if baseline is None or candidate is None:
            raise ValueError(f"incomplete confirmation pair: {pair}")
        if baseline != candidate:
            raise ValueError(
                f"confirmation sample identifiers differ for pair {pair}: "
                f"{sorted(baseline)} != {sorted(candidate)}"
            )
    counts = {len(samples[(pair, "baseline")]) for pair in pairs}
    if len(counts) != 1:
        raise ValueError(f"inconsistent samples per revision pair: {sorted(counts)}")
    return len(pairs), next(iter(counts))


def write_rows(path: pathlib.Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    if path.exists():
        raise ValueError(f"refusing existing output: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    args = parse_args()
    try:
        header, screen_rows = read_rows(args.screen)
        screen_groups = {group(row) for row in screen_rows}
        replacements: dict[Group, tuple[pathlib.Path, list[dict[str, str]]]] = {}
        for path in args.confirmation:
            candidate_header, rows = read_rows(path)
            if candidate_header != header:
                raise ValueError(f"CSV schema differs from screen: {path}")
            grouped: dict[Group, list[dict[str, str]]] = defaultdict(list)
            for row in rows:
                grouped[group(row)].append(row)
            for key, group_rows in grouped.items():
                if key not in screen_groups:
                    raise ValueError(f"confirmation group is absent from screen: {key}")
                if key in replacements:
                    raise ValueError(f"confirmation group overlaps another input: {key}")
                replacements[key] = (path, group_rows)

        composed = [row for row in screen_rows if group(row) not in replacements]
        source_rows: list[dict[str, object]] = []
        for key in sorted(replacements):
            path, rows = replacements[key]
            composed.extend(rows)
            pair_count, sample_count = measurement_shape(rows)
            adapter, records, api, pattern = key
            source_rows.append(
                {
                    "adapter": adapter,
                    "records": records,
                    "api": api,
                    "pattern": pattern,
                    "source": str(path),
                    "pairs": pair_count,
                    "samples_per_revision_pair": sample_count,
                }
            )

        write_rows(args.output, header, composed)
        write_rows(
            args.sources_output,
            [
                "adapter",
                "records",
                "api",
                "pattern",
                "source",
                "pairs",
                "samples_per_revision_pair",
            ],
            source_rows,
        )
    except (KeyError, OSError, TypeError, ValueError) as error:
        print(f"failed to compose confirmation gate: {error}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
