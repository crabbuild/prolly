#!/usr/bin/env python3
"""Validate and summarize the PostgreSQL-backed Prolly scale benchmark."""

import argparse
import csv
import itertools
import math
import pathlib
import statistics


SCHEMA = "postgres-scale-v1"
KEY_FIELDS = ("records", "repetition", "operation", "pattern", "cache_state")


def validate_rows(rows):
    seen = set()
    for row in rows:
        key = tuple(row[name] for name in KEY_FIELDS)
        if key in seen:
            raise ValueError(f"duplicate benchmark cell: {key}")
        seen.add(key)
        if row.get("schema") != SCHEMA:
            raise ValueError(f"unsupported schema: {row.get('schema')}")
        if row.get("validated", "").lower() != "true" or row.get("error"):
            raise ValueError(f"failed cell: {key}: {row.get('error', '')}")
        logical = int(row["logical_operations"])
        observed = int(row["observed_items"])
        total_ns = int(row["total_ns"])
        ns_per_op = float(row["ns_per_op"])
        throughput = float(row["ops_per_sec"])
        if logical <= 0 or observed <= 0 or total_ns <= 0:
            raise ValueError(f"non-positive operation metric: {key}")
        expected_ns = total_ns / logical
        expected_rate = logical * 1_000_000_000 / total_ns
        if not math.isfinite(ns_per_op) or not math.isclose(
            ns_per_op, expected_ns, rel_tol=1e-9
        ):
            raise ValueError(f"per-operation latency mismatch: {key}")
        if not math.isfinite(throughput) or not math.isclose(
            throughput, expected_rate, rel_tol=1e-9
        ):
            raise ValueError(f"throughput mismatch: {key}")


def aggregate(rows):
    groups = {}
    for row in rows:
        key = (
            int(row["records"]),
            row["operation"],
            row["pattern"],
            row["cache_state"],
        )
        groups.setdefault(key, []).append(row)
    summaries = []
    for (records, operation, pattern, cache_state), group in sorted(groups.items()):
        latencies = [int(row["total_ns"]) for row in group]
        summary = {
            "records": records,
            "operation": operation,
            "pattern": pattern,
            "cache_state": cache_state,
            "repetitions": len(group),
            "latency_median_ns": statistics.median(latencies),
            "latency_min_ns": min(latencies),
            "latency_max_ns": max(latencies),
            "ns_per_op_median": statistics.median(float(row["ns_per_op"]) for row in group),
            "ops_per_sec_median": statistics.median(float(row["ops_per_sec"]) for row in group),
        }
        for source, target in (
            ("nodes_read", "nodes_read_median"),
            ("nodes_written", "nodes_written_median"),
            ("bytes_read", "bytes_read_median"),
            ("bytes_written", "bytes_written_median"),
            ("node_cache_hits", "cache_hits_median"),
            ("node_cache_misses", "cache_misses_median"),
            ("pg_statement_calls", "pg_calls_median"),
            ("pg_execution_ms", "pg_execution_ms_median"),
            ("pg_shared_blks_hit", "pg_shared_blks_hit_median"),
            ("pg_shared_blks_read", "pg_shared_blks_read_median"),
            ("pg_wal_bytes", "pg_wal_bytes_median"),
            ("tree_records", "tree_records_median"),
            ("tree_nodes", "tree_nodes_median"),
            ("tree_height", "tree_height_median"),
            ("tree_bytes", "tree_bytes_median"),
            ("prolly_table_bytes_after", "table_bytes_after_median"),
            ("prolly_index_bytes_after", "index_bytes_after_median"),
        ):
            values = [float(row[source]) for row in group if source in row and row[source] != ""]
            summary[target] = statistics.median(values) if values else ""
        summaries.append(summary)
    return summaries


def render_report(summaries, rows, manifest=None):
    manifest = manifest or {}
    revision = rows[0].get("revision", "unknown") if rows else "unknown"
    dirty = rows[0].get("dirty", "unknown") if rows else "unknown"
    changes = int(manifest.get("changes", "0")) if manifest.get("changes", "").isdigit() else 0
    read_samples = int(manifest.get("read_samples", "0")) if manifest.get("read_samples", "").isdigit() else 0
    lines = [
        "# PostgreSQL-backed Prolly performance",
        "",
        f"Revision `{revision}` (dirty={dirty}); {len(rows)} validated raw rows.",
        "",
        "This is an end-to-end single-client measurement of the public async Prolly API over SQLx and PostgreSQL 16 in Docker Desktop. Latency is wall-clock time; PostgreSQL execution time is separately observed by `pg_stat_statements`.",
        "",
    ]
    if changes and read_samples:
        lines.extend(
            [
                "## Workload cardinality",
                "",
                f"Batch and diff mutate {changes:,} keys. Point get, multi-get, and bounded scan sample {read_samples:,} keys or entries.",
                f"Merge treats {changes:,} as the total change count: {changes // 2:,} changes per branch across two disjoint branches.",
                "Random merge keys are interleaved across both branches so each branch spans the full base keyspace.",
                "",
            ]
        )
    for records in sorted({item["records"] for item in summaries}):
        lines.extend(
            [
                f"## {records:,} records",
                "",
                "| Operation | Pattern | Cache | n | Median ms | Min–max ms | ns/op | ops/s | Nodes R/W | MiB R/W | PG calls / ms |",
                "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
            ]
        )
        for item in (entry for entry in summaries if entry["records"] == records):
            mib_read = _number(item.get("bytes_read_median")) / (1024 * 1024)
            mib_written = _number(item.get("bytes_written_median")) / (1024 * 1024)
            lines.append(
                "| {operation} | {pattern} | {cache} | n={n} | {median:.3f} | {minimum:.3f}–{maximum:.3f} | {ns_per_op:.0f} | {rate:.1f} | {nodes_read:.0f}/{nodes_written:.0f} | {mib_read:.2f}/{mib_written:.2f} | {pg_calls:.0f}/{pg_ms:.3f} |".format(
                    operation=item["operation"],
                    pattern=item["pattern"],
                    cache=item["cache_state"],
                    n=item["repetitions"],
                    median=item["latency_median_ns"] / 1_000_000,
                    minimum=item["latency_min_ns"] / 1_000_000,
                    maximum=item["latency_max_ns"] / 1_000_000,
                    ns_per_op=item["ns_per_op_median"],
                    rate=item["ops_per_sec_median"],
                    nodes_read=_number(item.get("nodes_read_median")),
                    nodes_written=_number(item.get("nodes_written_median")),
                    mib_read=mib_read,
                    mib_written=mib_written,
                    pg_calls=_number(item.get("pg_calls_median")),
                    pg_ms=_number(item.get("pg_execution_ms_median")),
                )
            )
        lines.append("")
    lines.extend(
        [
            "## Interpretation limits",
            "",
            "- Results describe the recorded machine, Docker Desktop allocation, code revision, PostgreSQL defaults, and fixed 24-byte keys/27-byte values.",
            "- `cold-manager` clears or recreates the decoded Prolly node cache; PostgreSQL and host OS caches are not forcibly dropped.",
            "- The workload is serial and single-client. It does not measure connection-pool or concurrent transaction scaling.",
            "- `query` means the public Prolly `get_many` API. Random-key range scans are intentionally not defined.",
            "- Build and full scan have n=1 per size; other full-profile cells normally have n=3.",
            "- Database-side statement time excludes client/runtime/tree processing and must not be compared as if it were end-to-end latency.",
            "",
        ]
    )
    return "\n".join(lines)


def read_manifest(path):
    values = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key] = value
    return values


def validate_matrix(rows, manifest, allow_partial=False):
    if not manifest:
        return
    sizes = [int(value) for value in manifest["sizes"].split(",")]
    runs = int(manifest["runs"])
    operations = manifest["operations"].split(",")
    patterns = manifest["patterns"].split(",")
    expected = set()
    for records in sizes:
        expected.add((str(records), "1", "build", "base", "cold-manager"))
        for repetition in range(1, runs + 1):
            for operation in operations:
                if operation == "full_scan":
                    if repetition == 1:
                        expected.add((str(records), "1", operation, "append", "cold-manager"))
                    continue
                for pattern in patterns:
                    if operation == "scan" and pattern == "random":
                        continue
                    cache = "warm-manager" if operation == "get_warm" else "cold-manager"
                    expected.add((str(records), str(repetition), operation, pattern, cache))
    observed = {tuple(row[name] for name in KEY_FIELDS) for row in rows}
    if not allow_partial and observed != expected:
        raise ValueError(
            f"incomplete benchmark matrix: expected {len(expected)}, observed {len(observed)}, missing={sorted(expected-observed)[:5]}, extra={sorted(observed-expected)[:5]}"
        )


def summarize(input_path, manifest_path, output_dir, allow_partial=False):
    with input_path.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    validate_rows(rows)
    manifest = read_manifest(manifest_path) if manifest_path and manifest_path.exists() else {}
    validate_matrix(rows, manifest, allow_partial=allow_partial)
    summaries = aggregate(rows)
    output_dir.mkdir(parents=True, exist_ok=True)
    summary_path = output_dir / "summary.csv"
    with summary_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=list(summaries[0]) if summaries else [],
            lineterminator="\n",
        )
        if summaries:
            writer.writeheader()
            writer.writerows(summaries)
    (output_dir / "report.md").write_text(
        render_report(summaries, rows, manifest), encoding="utf-8"
    )
    return summaries


def _number(value):
    return float(value) if value not in (None, "") else 0.0


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=pathlib.Path, required=True)
    parser.add_argument("--manifest", type=pathlib.Path)
    parser.add_argument("--output-dir", type=pathlib.Path, required=True)
    parser.add_argument("--allow-partial", action="store_true")
    args = parser.parse_args()
    summaries = summarize(
        args.input,
        args.manifest,
        args.output_dir,
        allow_partial=args.allow_partial,
    )
    print(f"validated and summarized {len(summaries)} workload groups")


if __name__ == "__main__":
    main()
