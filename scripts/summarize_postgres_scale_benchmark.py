#!/usr/bin/env python3
"""Validate and summarize the PostgreSQL-backed Prolly scale benchmark."""

import argparse
import csv
import itertools
import math
import pathlib
import shutil
import statistics
import tomllib


SCHEMA = "postgres-scale-v1"
KEY_FIELDS = ("records", "repetition", "operation", "pattern", "cache_state")
SERVICE_SCHEMA = "postgres-service-v1"
SERVICE_KEY_FIELDS = (
    "config_hash",
    "records",
    "value_bytes",
    "clients",
    "pool_size",
    "operation",
    "tenant_class",
)


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


def validate_service_rows(rows):
    seen = set()
    for row in rows:
        key = tuple(row[name] for name in SERVICE_KEY_FIELDS)
        if key in seen:
            raise ValueError(f"duplicate service cell row: {key}")
        seen.add(key)
        if row.get("schema") != SERVICE_SCHEMA:
            raise ValueError(f"unsupported service schema: {row.get('schema')}")
        if row.get("validated", "").lower() != "true" or row.get("error"):
            raise ValueError(f"failed service row: {key}: {row.get('error', '')}")
        attempted = int(row["attempted"])
        completed = int(row["completed"])
        cell_attempted = int(row["cell_attempted"])
        cell_completed = int(row["cell_completed"])
        duration_ns = int(row["duration_ns"])
        percentiles = [
            int(row[name])
            for name in ("p50_ns", "p95_ns", "p99_ns", "p999_ns", "max_ns")
        ]
        if attempted <= 0 or duration_ns <= 0:
            raise ValueError(f"non-positive service metric: {key}")
        if not (0 <= completed <= attempted <= cell_attempted):
            raise ValueError(f"inconsistent service counts: {key}")
        if not (completed <= cell_completed <= cell_attempted):
            raise ValueError(f"inconsistent service cell counts: {key}")
        if percentiles != sorted(percentiles):
            raise ValueError(f"unordered service percentiles: {key}")
        seconds = duration_ns / 1_000_000_000
        expected_attempted = attempted / seconds
        expected_successful = completed / seconds
        for field, expected in (
            ("attempted_ops_per_sec", expected_attempted),
            ("successful_ops_per_sec", expected_successful),
        ):
            actual = float(row[field])
            if not math.isfinite(actual) or not math.isclose(
                actual, expected, rel_tol=1e-9, abs_tol=1e-9
            ):
                raise ValueError(f"service throughput mismatch: {key}/{field}")


def aggregate_service(rows):
    validate_service_rows(rows)
    summaries = []
    for row in sorted(
        rows,
        key=lambda item: (
            int(item["clients"]),
            int(item["pool_size"]),
            item["operation"],
            item["tenant_class"],
        ),
    ):
        attempted = int(row["attempted"])
        completed = int(row["completed"])
        cas_attempts = int(row["cas_attempts"])
        cell_completed = int(row["cell_completed"])
        unexpected_errors = sum(
            int(row[name])
            for name in (
                "timeouts",
                "sql_errors",
                "validation_errors",
                "worker_panics",
            )
        )
        summaries.append(
            {
                "config_hash": row["config_hash"],
                "records": int(row["records"]),
                "value_bytes": int(row["value_bytes"]),
                "clients": int(row["clients"]),
                "pool_size": int(row["pool_size"]),
                "operation": row["operation"],
                "tenant_class": row["tenant_class"],
                "attempted": attempted,
                "completed": completed,
                "successful_ops_per_sec": float(row["successful_ops_per_sec"]),
                "p50_ns": int(row["p50_ns"]),
                "p95_ns": int(row["p95_ns"]),
                "p99_ns": int(row["p99_ns"]),
                "p999_ns": int(row["p999_ns"]),
                "max_ns": int(row["max_ns"]),
                "conflict_rate": (
                    int(row["conflicts"]) / cas_attempts if cas_attempts else 0.0
                ),
                "retry_rate": int(row["retries"]) / attempted,
                "error_rate": unexpected_errors / attempted,
                "pg_statements_per_cell_operation": (
                    int(row["pg_statement_calls"]) / cell_completed
                    if cell_completed
                    else math.inf
                ),
                "pg_wal_bytes_per_cell_operation": (
                    int(row["pg_wal_bytes"]) / cell_completed
                    if cell_completed
                    else math.inf
                ),
                "prolly_nodes_read_per_operation": (
                    int(row["prolly_nodes_read"]) / completed
                    if completed
                    else math.inf
                ),
                "prolly_nodes_written_per_operation": (
                    int(row["prolly_nodes_written"]) / completed
                    if completed
                    else math.inf
                ),
                "prolly_batch_reads_per_operation": (
                    int(row["prolly_store_batch_get_calls"]) / completed
                    if completed
                    else math.inf
                ),
            }
        )
    return summaries


def compare_service(
    current,
    baseline,
    budgets,
    current_environment=None,
    baseline_environment=None,
    allow_environment_mismatch=False,
):
    validate_service_rows(current)
    validate_service_rows(baseline)
    current_environment = current_environment or {}
    baseline_environment = baseline_environment or {}
    current_hashes = {row["config_hash"] for row in current}
    baseline_hashes = {row["config_hash"] for row in baseline}
    if len(current_hashes) != 1 or current_hashes != baseline_hashes:
        raise ValueError("service configuration hash mismatch")
    if current_environment != baseline_environment:
        if allow_environment_mismatch:
            return ["exploratory: environment mismatch; regression gates skipped"]
        raise ValueError("service environment mismatch")
    baseline_by_key = {
        tuple(row[name] for name in SERVICE_KEY_FIELDS[1:]): row for row in baseline
    }
    failures = []
    minimum_samples = int(budgets.get("minimum_percentile_samples", 1000))
    for row in current:
        key = tuple(row[name] for name in SERVICE_KEY_FIELDS[1:])
        reference = baseline_by_key.get(key)
        if reference is None:
            failures.append(f"missing baseline cell {key}")
            continue
        current_rate = float(row["successful_ops_per_sec"])
        baseline_rate = float(reference["successful_ops_per_sec"])
        throughput_loss = (
            (baseline_rate - current_rate) * 100 / baseline_rate
            if baseline_rate
            else 0.0
        )
        if throughput_loss > float(
            budgets.get("max_throughput_loss_percent", math.inf)
        ):
            failures.append(f"throughput regression {key}: {throughput_loss:.2f}%")
        if int(row["attempted"]) >= minimum_samples:
            current_p99 = int(row["p99_ns"])
            baseline_p99 = int(reference["p99_ns"])
            p99_increase = (
                (current_p99 - baseline_p99) * 100 / baseline_p99
                if baseline_p99
                else 0.0
            )
            if p99_increase > float(
                budgets.get("max_p99_increase_percent", math.inf)
            ):
                failures.append(f"p99 regression {key}: {p99_increase:.2f}%")
        cas_attempts = int(row["cas_attempts"])
        conflict_rate = int(row["conflicts"]) / cas_attempts if cas_attempts else 0
        if conflict_rate > float(budgets.get("max_conflict_rate", math.inf)):
            failures.append(f"conflict-rate regression {key}: {conflict_rate:.6f}")
        unexpected_errors = sum(
            int(row[name])
            for name in (
                "timeouts",
                "sql_errors",
                "validation_errors",
                "worker_panics",
            )
        )
        error_rate = unexpected_errors / int(row["attempted"])
        if error_rate > float(budgets.get("max_error_rate", 0.0)):
            failures.append(f"error-rate regression {key}: {error_rate:.6f}")
        statements_per_operation = int(row["pg_statement_calls"]) / max(
            int(row["cell_completed"]), 1
        )
        if statements_per_operation > float(
            budgets.get("max_pg_statements_per_operation", math.inf)
        ):
            failures.append(
                f"statement regression {key}: {statements_per_operation:.3f}"
            )
    if failures:
        raise ValueError("; ".join(failures))
    return []


def write_service_summary(path, summaries):
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=list(summaries[0]) if summaries else [],
            lineterminator="\n",
        )
        if summaries:
            writer.writeheader()
            writer.writerows(summaries)


def render_service_report(summaries, rows, regression_messages=None):
    regression_messages = regression_messages or []
    revision = rows[0].get("revision", "unknown") if rows else "unknown"
    dirty = rows[0].get("dirty", "unknown") if rows else "unknown"
    lines = [
        "# PostgreSQL-backed Prolly service performance",
        "",
        f"Revision `{revision}` (dirty={dirty}); {len(rows)} validated service rows.",
        "",
        "This closed-loop workload measures end-to-end public Prolly operations. Latency includes PostgreSQL pool wait.",
        "",
        "## Service saturation",
        "",
        "| Clients | Pool | Attempted ops/s | Successful ops/s | Conflicts | Unexpected errors | PG statements/op | Prolly node reads/op |",
        "|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    grouped = {}
    for row in rows:
        key = (int(row["clients"]), int(row["pool_size"]))
        cell = grouped.setdefault(
            key,
            {
                "attempted_rate": 0.0,
                "successful_rate": 0.0,
                "conflicts": 0,
                "errors": 0,
                "pg_calls": int(row["pg_statement_calls"]),
                "completed": int(row["cell_completed"]),
                "nodes_read": 0,
            },
        )
        cell["attempted_rate"] += float(row["attempted_ops_per_sec"])
        cell["successful_rate"] += float(row["successful_ops_per_sec"])
        cell["conflicts"] += int(row["conflicts"])
        cell["errors"] += sum(
            int(row[name])
            for name in (
                "timeouts",
                "sql_errors",
                "validation_errors",
                "worker_panics",
            )
        )
        cell["nodes_read"] += int(row["prolly_nodes_read"])
    for (clients, pool_size), cell in sorted(grouped.items()):
        calls_per_operation = cell["pg_calls"] / max(cell["completed"], 1)
        lines.append(
            f"| {clients} | {pool_size} | {cell['attempted_rate']:.1f} | "
            f"{cell['successful_rate']:.1f} | {cell['conflicts']} | "
            f"{cell['errors']} | {calls_per_operation:.3f} | "
            f"{cell['nodes_read'] / max(cell['completed'], 1):.3f} |"
        )
    lines.extend(
        [
            "",
            "## Operation latency",
            "",
            "| Clients | Pool | Operation | Tenant class | Samples | Successful ops/s | p50 ms | p95 ms | p99 ms | p99.9 ms | Max ms | Conflict rate |",
            "|---:|---:|---|---|---:|---:|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for item in summaries:
        lines.append(
            "| {clients} | {pool} | {operation} | {tenant} | {attempted} | "
            "{rate:.1f} | {p50:.3f} | {p95:.3f} | {p99:.3f} | "
            "{p999:.3f} | {maximum:.3f} | {conflicts:.4f} |".format(
                clients=item["clients"],
                pool=item["pool_size"],
                operation=item["operation"],
                tenant=item["tenant_class"],
                attempted=item["attempted"],
                rate=item["successful_ops_per_sec"],
                p50=item["p50_ns"] / 1_000_000,
                p95=item["p95_ns"] / 1_000_000,
                p99=item["p99_ns"] / 1_000_000,
                p999=item["p999_ns"] / 1_000_000,
                maximum=item["max_ns"] / 1_000_000,
                conflicts=item["conflict_rate"],
            )
        )
    lines.extend(["", "## Regression verdict", ""])
    if regression_messages:
        lines.extend(f"- {message}" for message in regression_messages)
    else:
        lines.append("- All configured service gates passed or no baseline was supplied.")
    lines.extend(
        [
            "",
            "## Interpretation limits",
            "",
            "- Results apply to the recorded machine, PostgreSQL settings, pool sizes, workload, and revision.",
            "- The generator is closed-loop; it measures saturation by concurrency rather than an external arrival-rate distribution.",
            "- Each logical service operation uses a fresh Prolly manager, so decoded node-cache entries are not shared between operations; PostgreSQL and host caches remain warm.",
            "- Scheduler and transaction interleaving are nondeterministic even though operation traces and data are seeded.",
            "- PostgreSQL statement and WAL counters are cell-wide and are repeated on operation rows; the report divides them by total cell completions.",
            "",
        ]
    )
    return "\n".join(lines)


def read_environment(directory):
    environment = {}
    for name in ("machine.txt", "postgres.txt"):
        path = directory / name
        contents = path.read_text(encoding="utf-8") if path.exists() else ""
        if name == "machine.txt":
            material_lines = []
            for line in contents.splitlines():
                if line.startswith("Filesystem"):
                    break
                if not line.startswith("captured_utc="):
                    material_lines.append(line)
            contents = "\n".join(material_lines)
        environment[name] = contents.strip()
    return environment


def read_service_budgets(path):
    if not path or not path.exists():
        return {}
    with path.open("rb") as handle:
        return tomllib.load(handle).get("regression", {})


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
    parser.add_argument("--input", type=pathlib.Path)
    parser.add_argument("--scale-input", type=pathlib.Path)
    parser.add_argument("--service-input", type=pathlib.Path)
    parser.add_argument("--manifest", type=pathlib.Path)
    parser.add_argument("--resolved-config", type=pathlib.Path)
    parser.add_argument("--baseline-dir", type=pathlib.Path)
    parser.add_argument("--allow-environment-mismatch", action="store_true")
    parser.add_argument("--output-dir", type=pathlib.Path, required=True)
    parser.add_argument("--allow-partial", action="store_true")
    args = parser.parse_args()
    scale_input = args.scale_input or args.input
    scale_report = ""
    scale_summaries = []
    if scale_input:
        scale_summaries = summarize(
            scale_input,
            args.manifest,
            args.output_dir,
            allow_partial=args.allow_partial,
        )
        generated_report = args.output_dir / "report.md"
        if generated_report.exists():
            scale_report = generated_report.read_text(encoding="utf-8")
        generated_summary = args.output_dir / "summary.csv"
        if generated_summary.exists() and args.scale_input:
            shutil.copyfile(
                generated_summary, args.output_dir / "scale-summary.csv"
            )

    service_summaries = []
    if args.service_input:
        with args.service_input.open(newline="", encoding="utf-8") as handle:
            service_rows = list(csv.DictReader(handle))
        service_summaries = aggregate_service(service_rows)
        write_service_summary(
            args.output_dir / "service-summary.csv", service_summaries
        )
        regression_messages = []
        if args.baseline_dir:
            baseline_path = args.baseline_dir / "service-raw.csv"
            if not baseline_path.exists():
                raise ValueError(f"missing service baseline: {baseline_path}")
            with baseline_path.open(newline="", encoding="utf-8") as handle:
                baseline_rows = list(csv.DictReader(handle))
            regression_messages = compare_service(
                service_rows,
                baseline_rows,
                read_service_budgets(args.resolved_config),
                read_environment(args.output_dir),
                read_environment(args.baseline_dir),
                allow_environment_mismatch=args.allow_environment_mismatch,
            )
        service_report = render_service_report(
            service_summaries, service_rows, regression_messages
        )
        combined = service_report
        if scale_report:
            combined += "\n\n" + scale_report.replace(
                "# PostgreSQL-backed Prolly performance",
                "## Serial large-tree performance",
                1,
            )
        (args.output_dir / "report.md").write_text(combined, encoding="utf-8")

    if not scale_input and not args.service_input:
        parser.error("one of --input, --scale-input, or --service-input is required")
    print(
        "validated and summarized "
        f"{len(service_summaries)} service rows and {len(scale_summaries)} scale groups"
    )


if __name__ == "__main__":
    main()
