#!/usr/bin/env python3
"""Strictly validate and summarize the local SQLite/Turso prolly benchmark."""

import argparse
import csv
import itertools
import math
import pathlib
import statistics


SCHEMA = "sqlite-turso-local-v1"
ADAPTER_COLUMNS = {
    "sqlite-sync": "sqlite",
    "turso-async": "turso",
}
RAW_FIELDS = {
    "schema", "revision", "dirty", "adapter", "records", "repetition", "api",
    "pattern", "configured_changes", "observed_changes", "total_ns",
    "operations_per_sec", "p50_ns", "p95_ns", "p99_ns", "max_ns",
    "db_bytes_before", "db_bytes_after", "expected_records", "observed_records",
    "validated", "error",
}
FIXTURE_FIELDS = {
    "schema", "revision", "dirty", "adapter", "records", "repetition", "build_ns",
    "records_per_sec", "database_bytes", "observed_records", "validated", "error",
}


def read_manifest(path):
    values = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line or "=" not in line:
            raise ValueError(f"invalid manifest line: {line!r}")
        key, value = line.split("=", 1)
        if key in values:
            raise ValueError(f"duplicate manifest key: {key}")
        values[key] = value
    if values.get("schema") != SCHEMA:
        raise ValueError(f"unsupported benchmark schema: {values.get('schema')}")
    return values


def read_raw(path):
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if set(reader.fieldnames or ()) != RAW_FIELDS:
            raise ValueError("raw-results.csv fields do not match the frozen schema")
        rows = list(reader)
    for row in rows:
        if row["schema"] != SCHEMA:
            raise ValueError(f"unsupported raw schema: {row['schema']}")
        if row["validated"].lower() != "true" or row["error"]:
            raise ValueError(
                "benchmark contains a failed cell: "
                f"{row['adapter']}/{row['records']}/{row['repetition']}/"
                f"{row['api']}/{row['pattern']}: {row['error']}"
            )
    return rows


def read_fixtures(path):
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if set(reader.fieldnames or ()) != FIXTURE_FIELDS:
            raise ValueError("fixture-results.csv fields do not match the frozen schema")
        rows = list(reader)
    for row in rows:
        if row["schema"] != SCHEMA:
            raise ValueError(f"unsupported fixture schema: {row['schema']}")
        if row["validated"].lower() != "true" or row["error"]:
            raise ValueError(
                "benchmark contains a failed fixture: "
                f"{row['adapter']}/{row['records']}/{row['repetition']}: {row['error']}"
            )
    return rows


def change_count(records):
    return min(records, max(100, min(10_000, records // 100)))


def median(values):
    return float(statistics.median(values))


def aggregate_adapter(rows, prefix):
    latencies = [int(row["total_ns"]) for row in rows]
    throughput = [float(row["operations_per_sec"]) for row in rows]
    result = {
        f"{prefix}_latency_median_ns": median(latencies),
        f"{prefix}_latency_min_ns": min(latencies),
        f"{prefix}_latency_max_ns": max(latencies),
        f"{prefix}_throughput_median_ops_sec": median(throughput),
        f"{prefix}_throughput_min_ops_sec": min(throughput),
        f"{prefix}_throughput_max_ops_sec": max(throughput),
    }
    for percentile in ("p50_ns", "p95_ns", "p99_ns", "max_ns"):
        values = [int(row[percentile]) for row in rows if row[percentile]]
        result[f"{prefix}_{percentile}_median"] = median(values) if values else ""
    return result


def validate_matrix(rows, fixtures, manifest, allow_partial=False):
    adapters = manifest["adapters"].split(",")
    sizes = [int(value) for value in manifest["sizes"].split(",")]
    repetitions = range(1, int(manifest["runs"]) + 1)
    apis = manifest["apis"].split(",")
    patterns = manifest["patterns"].split(",")
    expected = set(itertools.product(adapters, sizes, repetitions, apis, patterns))
    observed = set()
    for row in rows:
        key = (
            row["adapter"],
            int(row["records"]),
            int(row["repetition"]),
            row["api"],
            row["pattern"],
        )
        if key in observed:
            raise ValueError(f"duplicate benchmark cell: {key}")
        observed.add(key)
        if row["revision"] != manifest["revision"]:
            raise ValueError(f"raw revision does not match manifest: {key}")
        if row["dirty"].lower() != manifest["dirty"].lower():
            raise ValueError(f"raw dirty state does not match manifest: {key}")
        if int(row["configured_changes"]) <= 0:
            raise ValueError(f"cell has no configured changes: {key}")
        changes = int(row["configured_changes"])
        expected_changes = (
            int(manifest["changes"])
            if manifest["changes"] != "automatic"
            else change_count(int(row["records"]))
        )
        operations = changes * 2 if row["api"] == "merge" else changes
        expected_records = int(row["records"])
        if row["pattern"] == "append":
            expected_records += operations if row["api"] == "merge" else changes
        total_ns = int(row["total_ns"])
        throughput = float(row["operations_per_sec"])
        if changes != expected_changes or int(row["observed_changes"]) != operations:
            raise ValueError(f"cell operation count is invalid: {key}")
        if int(row["expected_records"]) != expected_records or int(row["observed_records"]) != expected_records:
            raise ValueError(f"cell record count is invalid: {key}")
        if total_ns <= 0 or not math.isfinite(throughput) or throughput <= 0:
            raise ValueError(f"cell timing is invalid: {key}")
        calculated = operations * 1_000_000_000.0 / total_ns
        if not math.isclose(throughput, calculated, rel_tol=1e-9):
            raise ValueError(f"cell throughput is inconsistent: {key}")
        percentiles = [row[name] for name in ("p50_ns", "p95_ns", "p99_ns", "max_ns")]
        if row["api"] == "put":
            if any(not value for value in percentiles):
                raise ValueError(f"put percentiles are missing: {key}")
            values = [int(value) for value in percentiles]
            if any(value <= 0 for value in values) or values != sorted(values):
                raise ValueError(f"put percentiles are invalid: {key}")
        elif any(percentiles):
            raise ValueError(f"non-put cell contains percentiles: {key}")
    if not allow_partial and expected != observed:
        missing = sorted(expected - observed)
        extra = sorted(observed - expected)
        raise ValueError(
            "incomplete benchmark matrix: "
            f"expected {len(expected)}, observed {len(observed)}, "
            f"missing={missing[:5]}, extra={extra[:5]}"
        )

    expected_fixtures = set(itertools.product(adapters, sizes, repetitions))
    observed_fixtures = set()
    for row in fixtures:
        key = (row["adapter"], int(row["records"]), int(row["repetition"]))
        if key in observed_fixtures:
            raise ValueError(f"duplicate fixture row: {key}")
        observed_fixtures.add(key)
        if row["revision"] != manifest["revision"] or row["dirty"].lower() != manifest["dirty"].lower():
            raise ValueError(f"fixture provenance does not match manifest: {key}")
        records = int(row["records"])
        build_ns = int(row["build_ns"])
        rate = float(row["records_per_sec"])
        if int(row["observed_records"]) != records or build_ns <= 0 or int(row["database_bytes"]) <= 0:
            raise ValueError(f"fixture contract is invalid: {key}")
        if not math.isfinite(rate) or not math.isclose(rate, records * 1_000_000_000.0 / build_ns, rel_tol=1e-9):
            raise ValueError(f"fixture throughput is inconsistent: {key}")
    if not allow_partial and expected_fixtures != observed_fixtures:
        raise ValueError(
            "incomplete fixture matrix: "
            f"expected {len(expected_fixtures)}, observed {len(observed_fixtures)}"
        )


def build_summary(rows, manifest, allow_partial=False):
    grouped = {}
    for row in rows:
        key = (int(row["records"]), row["api"], row["pattern"])
        grouped.setdefault(key, {}).setdefault(row["adapter"], []).append(row)
    summary = []
    for (records, api, pattern), adapters in sorted(grouped.items()):
        if set(adapters) != set(ADAPTER_COLUMNS):
            if allow_partial:
                continue
            raise ValueError(f"adapter pair is incomplete: {(records, api, pattern)}")
        repetitions = {
            adapter: {int(item["repetition"]) for item in adapter_rows}
            for adapter, adapter_rows in adapters.items()
        }
        if repetitions["sqlite-sync"] != repetitions["turso-async"]:
            if allow_partial:
                continue
            raise ValueError(f"adapter repetitions do not match: {(records, api, pattern)}")
        row = {
            "schema": SCHEMA,
            "records": records,
            "api": api,
            "pattern": pattern,
            "repetitions": len(repetitions["sqlite-sync"]),
            "configured_changes": int(adapters["sqlite-sync"][0]["configured_changes"]),
        }
        for adapter, prefix in ADAPTER_COLUMNS.items():
            row.update(aggregate_adapter(adapters[adapter], prefix))
        sqlite_latency = row["sqlite_latency_median_ns"]
        turso_latency = row["turso_latency_median_ns"]
        sqlite_throughput = row["sqlite_throughput_median_ops_sec"]
        turso_throughput = row["turso_throughput_median_ops_sec"]
        row["turso_over_sqlite_latency_ratio"] = turso_latency / sqlite_latency
        row["turso_over_sqlite_throughput_ratio"] = turso_throughput / sqlite_throughput
        summary.append(row)
    return summary


def write_summary(path, rows):
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def write_report(path, rows, fixtures, raw_rows, manifest, allow_partial=False):
    lines = [
        "# SQLite Sync vs Turso Async Local Prolly Comparison"
        + (" (Partial)" if allow_partial else ""),
        "",
        f"Schema: `{SCHEMA}`. Revision: `{manifest['revision']}` "
        f"(dirty: `{manifest['dirty']}`). Planned repetitions: {manifest['runs']}.",
        "",
        "Lower Turso/SQLite latency ratios favor Turso; higher throughput ratios favor Turso.",
        f"Validation: {len(raw_rows)} measured cells and {len(fixtures)} base fixtures passed the frozen row contract.",
        "",
        "| Records | API | Pattern | Turso/SQLite latency | Turso/SQLite throughput |",
        "|---:|---|---|---:|---:|",
    ]
    if allow_partial:
        lines[4:4] = [
            "**Partial evidence:** the run was interrupted; tables include only adapter-paired cells with matching completed repetitions and must not be treated as the final matrix.",
            "",
        ]
    for row in rows:
        lines.append(
            f"| {row['records']:,} | {row['api']} | {row['pattern']} | "
            f"{row['turso_over_sqlite_latency_ratio']:.3f}× | "
            f"{row['turso_over_sqlite_throughput_ratio']:.3f}× |"
        )
    fixture_groups = {}
    for fixture in fixtures:
        fixture_groups.setdefault((fixture["adapter"], int(fixture["records"])), []).append(fixture)
    lines.extend([
        "",
        "## Fixture build context",
        "",
        "| Adapter | Records | Median build ms | Median records/s | Median bytes |",
        "|---|---:|---:|---:|---:|",
    ])
    for (adapter, records), group in sorted(fixture_groups.items()):
        lines.append(
            f"| {adapter} | {records:,} | {median([int(item['build_ns']) for item in group]) / 1_000_000:.3f} | "
            f"{median([float(item['records_per_sec']) for item in group]):.1f} | "
            f"{median([int(item['database_bytes']) for item in group]):.0f} |"
        )
    latency_extreme = max(rows, key=lambda row: abs(math.log(row["turso_over_sqlite_latency_ratio"])))
    throughput_extreme = max(rows, key=lambda row: abs(math.log(row["turso_over_sqlite_throughput_ratio"])))
    lines.extend([
        "",
        "## Largest observed differences",
        "",
        f"- Largest latency-ratio departure from parity: {latency_extreme['records']:,} records, "
        f"{latency_extreme['api']}/{latency_extreme['pattern']} at "
        f"{latency_extreme['turso_over_sqlite_latency_ratio']:.3f}× Turso/SQLite.",
        f"- Largest throughput-ratio departure from parity: {throughput_extreme['records']:,} records, "
        f"{throughput_extreme['api']}/{throughput_extreme['pattern']} at "
        f"{throughput_extreme['turso_over_sqlite_throughput_ratio']:.3f}× Turso/SQLite.",
        "- These are observed ratios on this run; no statistical-significance claim is made.",
        "",
        "## Observed scaling",
        "",
        "The tables report each requested size independently. Compare rows within the same API and pattern; changes scale at 1% until the 10K cap, so operation counts are not proportional above 1M records.",
    ])
    lines.extend([
        "",
        "## Method and limitations",
        "",
        "- This compares preferred end-to-end prolly paths, not raw SQL engines: synchronous `Prolly<SqliteStore>` versus Tokio-driven asynchronous `AsyncProlly<TursoStore>`.",
        "- All databases are local files. Turso Cloud sync, credentials, `push()`, and `pull()` are not used.",
        "- Adapter durability defaults are recorded but are not asserted to provide identical fsync or journaling semantics.",
        "- Each measured cell starts with a cold prolly manager; the operating-system filesystem cache is uncontrolled.",
        "- Results describe the recorded machine, filesystem, code revision, and Turso beta version and do not predict Turso Cloud performance.",
        "- Individual-put percentiles are available in `summary.csv`; batch, diff, and merge latency ranges are across independent repetitions.",
        "",
    ])
    path.write_text("\n".join(lines), encoding="utf-8")


def summarize(output_dir, input_path=None, fixture_path=None, sizes=None, runs=None, allow_partial=False):
    output = pathlib.Path(output_dir)
    manifest = read_manifest(output / "run-manifest.txt")
    if sizes is not None and [int(value) for value in manifest["sizes"].split(",")] != list(sizes):
        raise ValueError("requested sizes do not match run manifest")
    if runs is not None and int(manifest["runs"]) != runs:
        raise ValueError("requested repetitions do not match run manifest")
    rows = read_raw(pathlib.Path(input_path) if input_path else output / "raw-results.csv")
    fixtures = read_fixtures(pathlib.Path(fixture_path) if fixture_path else output / "fixture-results.csv")
    validate_matrix(rows, fixtures, manifest, allow_partial=allow_partial)
    summary = build_summary(rows, manifest, allow_partial=allow_partial)
    if not summary:
        raise ValueError("benchmark matrix is empty")
    write_summary(output / "summary.csv", summary)
    write_report(
        output / "report.md",
        summary,
        fixtures,
        rows,
        manifest,
        allow_partial=allow_partial,
    )
    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, type=pathlib.Path)
    parser.add_argument("--fixtures", required=True, type=pathlib.Path)
    parser.add_argument("--output-dir", required=True, type=pathlib.Path)
    parser.add_argument("--sizes", required=True)
    parser.add_argument("--runs", required=True, type=int)
    parser.add_argument("--allow-partial", action="store_true")
    args = parser.parse_args()
    sizes = [int(value) for value in args.sizes.split(",")]
    rows = summarize(
        args.output_dir,
        input_path=args.input,
        fixture_path=args.fixtures,
        sizes=sizes,
        runs=args.runs,
        allow_partial=args.allow_partial,
    )
    print(f"wrote {len(rows)} comparison rows to {args.output_dir}")


if __name__ == "__main__":
    main()

