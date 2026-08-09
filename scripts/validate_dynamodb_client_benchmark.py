#!/usr/bin/env python3
"""Fail closed unless a client benchmark contains its exact declared workload."""

import argparse
import csv
import re
from collections import Counter
from pathlib import Path

SCHEMA = "versioned-dynamodb-client-samples-v2"
GC_SCHEMA = "versioned-dynamodb-client-gc-reachability-v2"
CACHE_SCHEMA = "versioned-dynamodb-client-cache-usage-v1"
FIXED = [
    ("GetItem", "warm"),
    ("PutItemIndexed", "warm"),
    ("QueryGsi", "warm"),
    ("PutItemBlob128KiB", "warm"),
    ("GetItemBlob128KiB", "warm"),
    ("Restore", "warm"),
    ("IndexPlan", "warm"),
    ("IndexApply", "warm"),
    ("IndexReplacePlan", "warm"),
    ("IndexReplaceApply", "warm"),
    ("IndexRemovePlan", "warm"),
    ("IndexRemoveApply", "warm"),
    ("HistoryVersionsAll", "warm"),
    ("HistoryGetOldest", "warm"),
    ("HistoryDiffOldestHead", "warm"),
    ("RetentionPlan", "warm"),
    ("RetentionApply", "warm"),
    ("GcPlan", "warm"),
    ("GcApply", "warm"),
    ("GetItemAt", "warm"),
    ("Query", "warm"),
    ("Scan", "warm"),
    ("BatchGetItem10", "warm"),
    ("TransactGetItems10", "warm"),
    ("GetItem", "cold"),
    ("BatchWriteItem10", "warm"),
    ("BatchWriteItem25", "warm"),
    ("PutItem", "warm"),
    ("Diff", "warm"),
    ("UpdateItem", "warm"),
    ("DeleteItem", "warm"),
    ("Versions", "warm"),
]
HISTORY = [
    ("HistoryAppendAll", "warm"),
    ("HistoryVersionsAll", "warm"),
    ("HistoryGetOldest", "warm"),
    ("HistoryDiffOldestHead", "warm"),
    ("RetentionPlan", "warm"),
    ("RetentionApply", "warm"),
]


def parse_shapes(value):
    shapes = sorted({int(shape) for shape in value.split(",")})
    if not shapes or any(shape < 1 or shape > 100 for shape in shapes):
        raise ValueError("transaction shapes must be unique integers in 1..=100")
    return shapes


def read_manifest(path):
    manifest = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        if "=" not in line:
            raise ValueError(f"malformed manifest line: {line!r}")
        key, value = line.split("=", 1)
        if not key or key in manifest:
            raise ValueError(f"empty or duplicate manifest key: {key!r}")
        manifest[key] = value
    return manifest


def validate_gc_reachability(path, revision, samples):
    try:
        with path.open(newline="", encoding="utf-8") as source:
            rows = list(csv.DictReader(source))
    except OSError as error:
        raise ValueError(f"GC reachability evidence is unavailable: {error}") from error
    expected_fields = {
        "schema", "revision", "sample", "max_protected_trees",
        "retained_roots", "protected_trees",
        "live_nodes", "live_node_bytes", "scanned_blob_nodes", "scanned_values",
        "live_blobs", "live_blob_bytes", "examined_node_candidates",
        "examined_blob_candidates",
    }
    if not rows or set(rows[0]) != expected_fields:
        raise ValueError("GC reachability evidence has an invalid schema")
    if len(rows) != samples:
        raise ValueError("GC reachability evidence has an invalid sample count")
    observed_samples = set()
    for row in rows:
        if row["schema"] != GC_SCHEMA or row["revision"] != revision:
            raise ValueError("GC reachability evidence identity mismatch")
        try:
            values = {field: int(row[field]) for field in expected_fields - {"schema", "revision"}}
        except (TypeError, ValueError) as error:
            raise ValueError("GC reachability evidence contains a non-integer counter") from error
        if any(value < 0 for value in values.values()):
            raise ValueError("GC reachability evidence contains a negative counter")
        observed_samples.add(values["sample"])
        if values["retained_roots"] == 0 or values["protected_trees"] == 0:
            raise ValueError("GC reachability evidence contains an empty protected graph")
        if values["protected_trees"] > values["max_protected_trees"]:
            raise ValueError("GC reachability evidence exceeds its declared tree bound")
    if observed_samples != set(range(samples)):
        raise ValueError("GC reachability evidence has missing or duplicate samples")


def validate_cache_usage(path, revision, samples, configured_max_bytes):
    try:
        with path.open(newline="", encoding="utf-8") as source:
            rows = list(csv.DictReader(source))
    except OSError as error:
        raise ValueError(f"cache usage evidence is unavailable: {error}") from error
    expected_fields = {
        "schema", "revision", "sample", "client_role", "configured_max_bytes",
        "entries", "serialized_bytes", "pinned_entries", "pinned_serialized_bytes",
    }
    if not rows or set(rows[0]) != expected_fields:
        raise ValueError("cache usage evidence has an invalid schema")
    if len(rows) != samples:
        raise ValueError("cache usage evidence has an invalid sample count")
    observed_samples = set()
    for row in rows:
        if (
            row["schema"] != CACHE_SCHEMA
            or row["revision"] != revision
            or row["client_role"] != "primary"
        ):
            raise ValueError("cache usage evidence identity mismatch")
        try:
            values = {
                field: int(row[field])
                for field in expected_fields
                - {"schema", "revision", "client_role"}
            }
        except (TypeError, ValueError) as error:
            raise ValueError("cache usage evidence contains a non-integer counter") from error
        if any(value < 0 for value in values.values()):
            raise ValueError("cache usage evidence contains a negative counter")
        if values["configured_max_bytes"] != configured_max_bytes:
            raise ValueError("cache usage evidence configuration mismatch")
        if (
            values["pinned_entries"] > values["entries"]
            or values["pinned_serialized_bytes"] > values["serialized_bytes"]
        ):
            raise ValueError("cache usage evidence has impossible pinned occupancy")
        if (values["entries"] == 0) != (values["serialized_bytes"] == 0):
            raise ValueError("cache usage evidence has inconsistent empty occupancy")
        if (values["pinned_entries"] == 0) != (
            values["pinned_serialized_bytes"] == 0
        ):
            raise ValueError("cache usage evidence has inconsistent pinned occupancy")
        unpinned_bytes = (
            values["serialized_bytes"] - values["pinned_serialized_bytes"]
        )
        if unpinned_bytes > configured_max_bytes:
            raise ValueError("cache usage evidence exceeds its configured byte bound")
        observed_samples.add(values["sample"])
    if observed_samples != set(range(samples)):
        raise ValueError("cache usage evidence has missing or duplicate samples")


def expected_versions_created(operation, history_depth):
    if operation == "HistoryAppendAll":
        return history_depth
    if operation.startswith("BatchWriteItem"):
        return int(operation.removeprefix("BatchWriteItem"))
    if operation.startswith("TransactWriteItems"):
        return 1
    concurrent = re.fullmatch(r"ConcurrentPutItemW([1-9][0-9]*)O([1-9][0-9]*)", operation)
    if concurrent:
        return int(concurrent.group(1)) * int(concurrent.group(2))
    if operation in {
        "PutItem",
        "UpdateItem",
        "DeleteItem",
        "PutItemIndexed",
        "PutItemBlob128KiB",
        "IndexApply",
        "IndexReplaceApply",
        "IndexRemoveApply",
    }:
        return 1
    return 0


def validate_row_semantics(row, read_batch_items, history_depth):
    operation = row["operation"]
    observed = int(row["observed_items"])
    executions = int(row["sdk_executions"])
    attempts = int(row["http_attempts"])
    if observed <= 0:
        raise ValueError(f"{operation} observed no logical result")
    if executions <= 0 or attempts < executions:
        raise ValueError(f"{operation} has invalid SDK execution/attempt counts")
    if int(row["sdk_retries"]) != attempts - executions:
        raise ValueError(f"{operation} has inconsistent SDK retry derivation")
    if row["physical_response_bytes_complete"] != "true":
        raise ValueError(f"{operation} has incomplete physical response bytes")
    if int(row["versions_created"]) != expected_versions_created(
        operation, history_depth
    ):
        raise ValueError(f"{operation} has an invalid version-transition count")
    if operation.startswith("BatchGetItem") or operation.startswith("TransactGetItems"):
        prefix = "BatchGetItem" if operation.startswith("BatchGetItem") else "TransactGetItems"
        expected_items = int(operation.removeprefix(prefix))
        if observed != expected_items:
            raise ValueError(f"{operation} returned an invalid logical item count")
    if operation.startswith("BatchWriteItem"):
        expected_items = int(operation.removeprefix("BatchWriteItem"))
        if observed != expected_items:
            raise ValueError(f"{operation} applied an invalid logical item count")
    concurrent = re.fullmatch(r"ConcurrentPutItemW([1-9][0-9]*)O([1-9][0-9]*)", operation)
    if concurrent:
        expected_items = int(concurrent.group(1)) * int(concurrent.group(2))
        if observed != expected_items:
            raise ValueError(f"{operation} completed an invalid logical write count")
    if operation in {"IndexPlan", "IndexApply", "IndexReplacePlan", "IndexReplaceApply", "IndexRemovePlan", "IndexRemoveApply"} and observed != 2:
        raise ValueError(f"{operation} did not cover exactly two indexes")
    if operation == "RetentionPlan" and observed != history_depth + 1:
        raise ValueError("RetentionPlan examined an invalid history depth")
    if operation == "HistoryVersionsAll" and observed != history_depth + 1:
        raise ValueError("HistoryVersionsAll enumerated an invalid history depth")
    if operation == "HistoryAppendAll" and observed != history_depth:
        raise ValueError("HistoryAppendAll created an invalid history depth")
    if operation in {"HistoryGetOldest", "HistoryDiffOldestHead"} and observed != 1:
        raise ValueError(f"{operation} returned an invalid logical result count")
    if operation == "RetentionApply" and observed != min(history_depth, 80):
        raise ValueError("RetentionApply did not apply the exact bounded removal set")

    logical_input = int(row["logical_input_item_bytes"])
    logical_output = int(row["logical_output_item_bytes"])
    logical_complete = row["logical_item_bytes_complete"]
    if operation == "PutItemBlob128KiB":
        if logical_input <= 128 * 1024 or logical_output != 0 or logical_complete != "true":
            raise ValueError("blob put logical-byte evidence is invalid")
    elif operation == "GetItemBlob128KiB":
        if logical_output <= 128 * 1024 or logical_complete != "true":
            raise ValueError("blob get logical-byte evidence is invalid")
    elif operation == "Restore":
        if logical_input != 64 or logical_output != 0 or logical_complete != "true":
            raise ValueError("restore logical-byte evidence is invalid")
    elif operation == "HistoryAppendAll":
        if logical_input <= 0 or logical_output != 0 or logical_complete != "true":
            raise ValueError("history append logical-byte evidence is invalid")
    elif operation.startswith("ConcurrentPutItem"):
        if logical_input <= 0 or logical_output != 0 or logical_complete != "true":
            raise ValueError("concurrent write logical-byte evidence is invalid")
    elif operation in {
        "IndexPlan",
        "IndexApply",
        "IndexReplacePlan",
        "IndexReplaceApply",
        "IndexRemovePlan",
        "IndexRemoveApply",
        "RetentionPlan",
        "RetentionApply",
        "GcPlan",
        "GcApply",
    }:
        if logical_input != 0 or logical_output != 0 or logical_complete != "false":
            raise ValueError(f"{operation} logical-byte evidence is invalid")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument("--manifest", required=True, type=Path)
    parser.add_argument("--samples", required=True, type=int)
    parser.add_argument("--records", required=True, type=int)
    parser.add_argument("--value-bytes", required=True, type=int)
    parser.add_argument("--read-batch-items", required=True, type=int)
    parser.add_argument("--history-depth", required=True, type=int)
    parser.add_argument("--workload", choices=("full", "history"), default="full")
    parser.add_argument("--revision", required=True)
    parser.add_argument("--transaction-shapes", required=True)
    parser.add_argument("--concurrency-writers", required=True)
    parser.add_argument("--concurrency-operations-per-writer", required=True, type=int)
    parser.add_argument("--concurrency-retry-limit", required=True, type=int)
    parser.add_argument("--node-cache-max-bytes", required=True, type=int)
    args = parser.parse_args()
    if args.samples < 1 or args.records < 100:
        raise SystemExit("samples must be positive and records must be at least 100")
    if not 10 <= args.read_batch_items <= 100:
        raise SystemExit("read batch items must be in 10..=100")
    if (args.value_bytes + 37) * args.read_batch_items > 4 * 1024 * 1024:
        raise SystemExit("read batch shape exceeds the transaction-read response envelope")
    if args.history_depth < 10:
        raise SystemExit("history depth must be at least 10")
    try:
        shapes = parse_shapes(args.transaction_shapes)
        concurrency_writers = parse_shapes(args.concurrency_writers)
    except (ValueError, TypeError) as error:
        raise SystemExit(str(error)) from error
    if any(writer > 64 for writer in concurrency_writers):
        raise SystemExit("concurrency writers must be in 1..=64")
    if concurrency_writers[0] != 1:
        raise SystemExit("concurrency writers must include the one-writer baseline")
    if not 1 <= args.concurrency_operations_per_writer <= 1000:
        raise SystemExit("concurrency operations per writer must be in 1..=1000")
    if not 0 <= args.concurrency_retry_limit <= 63:
        raise SystemExit("concurrency retry limit must be in 0..=63")
    if args.node_cache_max_bytes < 0:
        raise SystemExit("node cache byte limit must be non-negative")

    try:
        manifest = read_manifest(args.manifest)
    except (OSError, ValueError) as error:
        raise SystemExit(str(error)) from error
    required_manifest = {
        "schema": SCHEMA,
        "runner_version": "15",
        "revision": args.revision,
        "samples": str(args.samples),
        "records": str(args.records),
        "value_bytes": str(args.value_bytes),
        "read_batch_items": str(args.read_batch_items),
        "history_depth": str(args.history_depth),
        "workload": args.workload,
        "transaction_shapes": ",".join(str(shape) for shape in shapes),
        "concurrency_writers": ",".join(str(writer) for writer in concurrency_writers),
        "concurrency_operations_per_writer": str(args.concurrency_operations_per_writer),
        "concurrency_retry_limit": str(args.concurrency_retry_limit),
        "node_cache_max_bytes": str(args.node_cache_max_bytes),
    }
    mismatches = {
        key: (manifest.get(key), expected)
        for key, expected in required_manifest.items()
        if manifest.get(key) != expected
    }
    if mismatches:
        raise SystemExit(f"run manifest mismatch: {mismatches}")
    if manifest.get("teardown") not in {"namespace", "docker-volume"}:
        raise SystemExit("run manifest has an invalid teardown mode")
    try:
        validate_cache_usage(
            args.input.parent / "cache-usage.csv",
            args.revision,
            args.samples,
            args.node_cache_max_bytes,
        )
    except ValueError as error:
        raise SystemExit(str(error)) from error
    if args.workload == "full":
        try:
            validate_gc_reachability(
                args.input.parent / "gc-reachability.csv", args.revision, args.samples
            )
        except ValueError as error:
            raise SystemExit(str(error)) from error
    with args.input.open(newline="", encoding="utf-8") as source:
        rows = list(csv.DictReader(source))
    expected_pairs = HISTORY.copy() if args.workload == "history" else FIXED.copy()
    if args.workload == "full" and args.read_batch_items > 10:
        expected_pairs.extend(
            [
                (f"BatchGetItem{args.read_batch_items}", "warm"),
                (f"TransactGetItems{args.read_batch_items}", "warm"),
            ]
        )
    if args.workload == "full":
        expected_pairs += [
            (f"TransactWriteItems{shape}", "warm") for shape in shapes
        ]
        expected_pairs += [
            (
                f"ConcurrentPutItemW{writers}O{args.concurrency_operations_per_writer}",
                "warm",
            )
            for writers in concurrency_writers
        ]
    expected_rows = args.samples * len(expected_pairs)
    if len(rows) != expected_rows:
        raise SystemExit(f"expected {expected_rows} rows, found {len(rows)}")
    expected_samples = set(range(args.samples))
    if {int(row["sample"]) for row in rows} != expected_samples:
        raise SystemExit("sample indexes are incomplete or out of range")
    if any(row["schema"] != SCHEMA or row["validated"] != "true" for row in rows):
        raise SystemExit("sample schema or validation marker is invalid")
    if any(
        row["revision"] != args.revision
        or int(row["records"]) != args.records
        or int(row["configured_value_bytes"]) != args.value_bytes
        for row in rows
    ):
        raise SystemExit("raw sample provenance differs from the declared run")
    try:
        for row in rows:
            validate_row_semantics(row, args.read_batch_items, args.history_depth)
    except (KeyError, TypeError, ValueError) as error:
        raise SystemExit(f"raw sample semantic validation failed: {error}") from error

    concurrency_samples = range(args.samples) if args.workload == "full" else ()
    for sample in concurrency_samples:
        concurrency_rows = {
            int(match.group(1)): row
            for row in rows
            if int(row["sample"]) == sample
            and (
                match := re.fullmatch(
                    r"ConcurrentPutItemW([1-9][0-9]*)O([1-9][0-9]*)",
                    row["operation"],
                )
            )
        }
        baseline = concurrency_rows.get(1)
        if baseline is None:
            raise SystemExit(f"sample {sample} lacks the one-writer admission baseline")
        baseline_per_write = int(baseline["sdk_executions"]) / int(
            baseline["observed_items"]
        )
        for writers, row in concurrency_rows.items():
            executions_per_write = int(row["sdk_executions"]) / int(
                row["observed_items"]
            )
            if executions_per_write > baseline_per_write * 1.25 + 5:
                raise SystemExit(
                    "concurrent admission amplification exceeded the fail-closed "
                    f"envelope: sample={sample} writers={writers} "
                    f"baseline={baseline_per_write:.3f} actual={executions_per_write:.3f}"
                )

    actual = Counter(
        (int(row["sample"]), row["operation"], row["cache_mode"]) for row in rows
    )
    expected = Counter(
        (sample, operation, cache)
        for sample in range(args.samples)
        for operation, cache in expected_pairs
    )
    if actual != expected:
        missing = list((expected - actual).elements())
        unexpected = list((actual - expected).elements())
        raise SystemExit(
            f"workload manifest mismatch; missing={missing[:5]} unexpected={unexpected[:5]}"
        )
    print(f"validated exact workload rows={expected_rows} samples={args.samples}")


if __name__ == "__main__":
    main()
