#!/usr/bin/env python3
"""Fail closed unless an aggregate DynamoDB client matrix is exact and complete."""

import argparse
import csv
import re
from pathlib import Path


FULL_SCHEMA = "dynamodb-client-matrix-v2"
HISTORY_SCHEMA = "dynamodb-client-history-matrix-v2"
FULL_FIELDS = {
    "schema", "matrix_profile", "case", "records", "value_bytes",
    "read_batch_items", "history_depth", "transaction_shapes",
    "concurrency_writers", "concurrency_operations_per_writer",
    "concurrency_retry_limit", "node_cache_max_bytes", "samples", "revision",
    "result_dir", "status",
}
HISTORY_FIELDS = {
    "schema", "matrix_profile", "history_depth", "samples", "records",
    "value_bytes", "read_batch_items", "transaction_shapes",
    "concurrency_writers", "concurrency_operations_per_writer",
    "concurrency_retry_limit", "node_cache_max_bytes", "revision", "result_dir",
    "status",
}


def read_manifest(path):
    values = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        if "=" not in line:
            raise ValueError(f"malformed matrix manifest line: {line!r}")
        key, value = line.split("=", 1)
        if not key or key in values:
            raise ValueError(f"empty or duplicate matrix manifest key: {key!r}")
        values[key] = value
    return values


def increasing_csv_integers(value, label):
    if not re.fullmatch(r"[1-9][0-9]*(,[1-9][0-9]*)*", value):
        raise ValueError(f"{label} must be unique positive comma-separated integers")
    numbers = [int(item) for item in value.split(",")]
    if numbers != sorted(set(numbers)):
        raise ValueError(f"{label} must be strictly increasing positive integers")
    return numbers


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--kind", choices=("full", "history"), required=True)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--profile", choices=("smoke", "qualification"), required=True)
    parser.add_argument("--revision", required=True)
    parser.add_argument("--samples", type=int, required=True)
    parser.add_argument("--node-cache-max-bytes", type=int, required=True)
    parser.add_argument("--expected-cases", required=True)
    parser.add_argument("--expected-history-shape")
    args = parser.parse_args()
    if args.samples < 1 or args.node_cache_max_bytes < 0:
        raise SystemExit("matrix samples must be positive and cache bytes non-negative")

    try:
        manifest = read_manifest(args.manifest)
        with args.input.open(newline="", encoding="utf-8") as source:
            reader = csv.DictReader(source)
            rows = list(reader)
            fields = set(reader.fieldnames or [])
    except (OSError, ValueError) as error:
        raise SystemExit(str(error)) from error

    schema = FULL_SCHEMA if args.kind == "full" else HISTORY_SCHEMA
    expected_fields = FULL_FIELDS if args.kind == "full" else HISTORY_FIELDS
    if fields != expected_fields or not rows or any(None in row for row in rows):
        raise SystemExit("aggregate matrix has an invalid CSV schema")

    if args.kind == "full":
        full_shapes = []
        for specification in args.expected_cases.split(";"):
            fields = specification.split(":")
            if len(fields) != 5 or not re.fullmatch(
                r"[a-z0-9][a-z0-9-]*", fields[0]
            ):
                raise SystemExit("full matrix case specifications are malformed")
            try:
                records, value_bytes, read_batch_items = map(int, fields[1:4])
                transaction_shapes = increasing_csv_integers(
                    fields[4], "transaction shapes"
                )
            except ValueError as error:
                raise SystemExit(str(error)) from error
            if records < 100 or value_bytes < 1 or not 10 <= read_batch_items <= 100:
                raise SystemExit("full matrix case specifications are out of range")
            full_shapes.append(
                (fields[0], records, value_bytes, read_batch_items, transaction_shapes)
            )
        cases = [shape[0] for shape in full_shapes]
        if len(cases) != len(set(cases)):
            raise SystemExit("full matrix case names must be unique")
        identity_field = "case"
        expected_identities = cases
        expected_dirs = cases
        manifest_identity = ("case_names", ",".join(cases))
    else:
        try:
            depths = increasing_csv_integers(args.expected_cases, "history matrix depths")
        except ValueError as error:
            raise SystemExit(str(error)) from error
        expected_identities = [str(depth) for depth in depths]
        expected_dirs = [f"history-{depth}" for depth in depths]
        identity_field = "history_depth"
        manifest_identity = ("history_depths", args.expected_cases)
        if args.expected_history_shape is None:
            raise SystemExit("history matrix requires its exact common shape")
        history_fields = args.expected_history_shape.split(":")
        if len(history_fields) != 7:
            raise SystemExit("history matrix common shape is malformed")
        try:
            history_shape = (
                int(history_fields[0]), int(history_fields[1]), int(history_fields[2]),
                increasing_csv_integers(history_fields[3], "transaction shapes"),
                increasing_csv_integers(history_fields[4], "concurrency writers"),
                int(history_fields[5]), int(history_fields[6]),
            )
        except ValueError as error:
            raise SystemExit(str(error)) from error
        if (
            history_shape[0] < 100
            or history_shape[1] < 1
            or not 10 <= history_shape[2] <= 100
            or history_shape[5] < 1
            or history_shape[6] < 0
        ):
            raise SystemExit("history matrix common shape is out of range")

    required_manifest = {
        "schema": schema,
        "profile": args.profile,
        "revision": args.revision,
        manifest_identity[0]: manifest_identity[1],
        "cases": str(len(expected_identities)),
        "samples_per_case": str(args.samples),
        "node_cache_max_bytes": str(args.node_cache_max_bytes),
    }
    if set(manifest) != set(required_manifest) | {"completed_utc"}:
        raise SystemExit("aggregate matrix manifest has an invalid schema")
    mismatches = {
        key: (manifest.get(key), expected)
        for key, expected in required_manifest.items()
        if manifest.get(key) != expected
    }
    if mismatches:
        raise SystemExit(f"aggregate matrix manifest mismatch: {mismatches}")
    if not re.fullmatch(
        r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z",
        manifest["completed_utc"],
    ):
        raise SystemExit("aggregate matrix manifest has an invalid completion timestamp")

    if len(rows) != len(expected_identities):
        raise SystemExit("aggregate matrix has an invalid row count")
    observed = []
    for row_index, row in enumerate(rows):
        if (
            row["schema"] != schema
            or row["matrix_profile"] != args.profile
            or row["revision"] != args.revision
            or row["status"] != "validated"
            or row["samples"] != str(args.samples)
            or row["node_cache_max_bytes"] != str(args.node_cache_max_bytes)
        ):
            raise SystemExit("aggregate matrix row provenance mismatch")
        for field in (
            "records", "value_bytes", "read_batch_items", "history_depth",
            "concurrency_operations_per_writer",
        ):
            try:
                if int(row[field]) <= 0:
                    raise ValueError
            except ValueError as error:
                raise SystemExit(f"aggregate matrix field {field} is not positive") from error
        try:
            if int(row["concurrency_retry_limit"]) < 0:
                raise ValueError
        except ValueError as error:
            raise SystemExit(
                "aggregate matrix field concurrency_retry_limit is not non-negative"
            ) from error
        try:
            increasing_csv_integers(row["transaction_shapes"], "transaction shapes")
            increasing_csv_integers(row["concurrency_writers"], "concurrency writers")
        except ValueError as error:
            raise SystemExit(str(error)) from error
        if args.kind == "full":
            expected = full_shapes[row_index]
            actual = (
                row["case"], int(row["records"]), int(row["value_bytes"]),
                int(row["read_batch_items"]),
                [int(value) for value in row["transaction_shapes"].split(",")],
            )
            if actual != expected:
                raise SystemExit("aggregate full matrix case shape mismatch")
        else:
            actual = (
                int(row["records"]), int(row["value_bytes"]),
                int(row["read_batch_items"]),
                [int(value) for value in row["transaction_shapes"].split(",")],
                [int(value) for value in row["concurrency_writers"].split(",")],
                int(row["concurrency_operations_per_writer"]),
                int(row["concurrency_retry_limit"]),
            )
            if actual != history_shape:
                raise SystemExit("aggregate history matrix common shape mismatch")
        observed.append(row[identity_field])

    if observed != expected_identities:
        raise SystemExit("aggregate matrix cases are missing, duplicated, or out of order")
    if [row["result_dir"] for row in rows] != expected_dirs:
        raise SystemExit("aggregate matrix result directories do not match their cases")
    print(f"validated {args.kind} aggregate matrix rows={len(rows)}")


if __name__ == "__main__":
    main()
