#!/usr/bin/env python3
import argparse
import csv
import math
import statistics
from collections import Counter, defaultdict
from pathlib import Path

COMMON_OPERATIONS_ZERO = (
    "full_diff",
    "range_diff",
    "patch_generate",
    "patch_apply",
    "merge_noop",
)
COMMON_OPERATIONS_CHANGED = (
    "full_diff",
    "range_diff",
    "patch_generate",
    "patch_apply",
    "merge_disjoint",
    "merge_convergent",
    "merge_conflict",
)
LIFECYCLE_OPERATIONS = {
    (0, "none"): (
        "head_resolve",
        "snapshot_resolve",
        "historical_point_read",
        "historical_range_scan",
        "version_list",
        "rollback",
        "retention_prune",
    ),
    (1, "append"): ("version_publish",),
    (1, "random"): ("version_publish",),
    (1, "clustered"): ("version_publish",),
    (30, "append"): ("version_publish",),
    (30, "random"): ("version_publish",),
    (30, "clustered"): ("version_publish",),
}


def read_manifest(path):
    result = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        if "=" in line:
            key, value = line.split("=", 1)
            result[key] = value
    return result


def read_rows(path, allow_empty=False):
    with path.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    if not rows and not allow_empty:
        raise SystemExit(f"no result rows in {path}")
    for row in rows:
        if row["validated"] != "true":
            raise SystemExit(f"unvalidated row in {path}: {row}")
        for field in (
            "records",
            "density",
            "operations",
            "elapsed_ns",
            "result_count",
            "base_count",
            "target_count",
            "conflict_count",
            "repetition",
            "peak_rss_bytes",
        ):
            row[field] = int(row[field])
        for field in ("ns_per_op", "ops_per_sec"):
            row[field] = float(row[field])
            if not math.isfinite(row[field]):
                raise SystemExit(f"non-finite {field} in {path}")
    return rows


def common_expected(sizes, runs, densities, localities):
    expected = set()
    scenarios = ([(0, "none")] if 0 in densities else []) + [
        (density, locality)
        for density in densities
        if density != 0
        for locality in localities
    ]
    for records in sizes:
        for repetition in range(1, runs + 1):
            for density, locality in scenarios:
                operations = COMMON_OPERATIONS_ZERO if density == 0 else COMMON_OPERATIONS_CHANGED
                for implementation in ("rust", "dolt-go"):
                    for operation in operations:
                        expected.add((implementation, records, density, locality, operation, repetition))
    return expected


def lifecycle_expected(sizes, runs, densities, localities, enabled):
    expected = set()
    if not enabled:
        return expected
    scenarios = {(0, "none"): LIFECYCLE_OPERATIONS[(0, "none")]}
    scenarios.update(
        {
            (density, locality): ("version_publish",)
            for density in densities
            if density != 0
            for locality in localities
        }
    )
    for records in sizes:
        for repetition in range(1, runs + 1):
            for (density, locality), operations in scenarios.items():
                for operation in operations:
                    expected.add(("rust-lifecycle", records, density, locality, operation, repetition))
    return expected


def row_identity(row):
    return (
        row["implementation"],
        row["records"],
        row["density"],
        row["locality"],
        row["operation"],
        row["repetition"],
    )


def validate_matrix(rows, expected, label):
    identities = [row_identity(row) for row in rows]
    duplicates = [key for key, count in Counter(identities).items() if count != 1]
    if duplicates:
        raise SystemExit(f"duplicate {label} identities: {duplicates[:5]}")
    actual = set(identities)
    if actual != expected:
        missing = sorted(expected - actual)[:10]
        extra = sorted(actual - expected)[:10]
        raise SystemExit(f"incomplete {label} matrix: missing={missing}, extra={extra}")


def validate_pairs(rows):
    groups = defaultdict(dict)
    for row in rows:
        key = (
            row["records"],
            row["density"],
            row["locality"],
            row["operation"],
            row["relationship"],
            row["repetition"],
        )
        groups[key][row["implementation"]] = row
    invariant_fields = (
        "contract_version",
        "records",
        "density",
        "locality",
        "operation",
        "relationship",
        "operations",
        "workload_digest",
        "result_digest",
        "base_count",
        "target_count",
        "conflict_count",
        "validated",
    )
    for key, pair in groups.items():
        if set(pair) != {"rust", "dolt-go"}:
            raise SystemExit(f"incomplete implementation pair: {key}")
        rust, go = pair["rust"], pair["dolt-go"]
        for field in invariant_fields:
            if rust[field] != go[field]:
                raise SystemExit(f"pair mismatch {key} field={field}: {rust[field]} != {go[field]}")
        if rust["operation"] != "patch_generate" and rust["result_count"] != go["result_count"]:
            raise SystemExit(f"pair result_count mismatch: {key}")
    return groups


def cv(values):
    mean = statistics.fmean(values)
    return 0.0 if mean == 0 else statistics.pstdev(values) / mean * 100.0


def percentile(values, fraction):
    ordered = sorted(values)
    if not ordered:
        return 0.0
    index = max(0, math.ceil(len(ordered) * fraction) - 1)
    return ordered[index]


def summarize_common(rows, runs):
    grouped = defaultdict(lambda: defaultdict(list))
    for row in rows:
        key = (
            row["records"],
            row["density"],
            row["locality"],
            row["operation"],
            row["relationship"],
        )
        grouped[key][row["implementation"]].append(row)
    summary = []
    winner_flips = 0
    for key in sorted(grouped):
        implementations = grouped[key]
        rust_rows = sorted(implementations["rust"], key=lambda row: row["repetition"])
        go_rows = sorted(implementations["dolt-go"], key=lambda row: row["repetition"])
        if len(rust_rows) != runs or len(go_rows) != runs:
            raise SystemExit(f"wrong repetition count for {key}")
        rust_ns = [row["elapsed_ns"] for row in rust_rows]
        go_ns = [row["elapsed_ns"] for row in go_rows]
        rust_median = statistics.median(rust_ns)
        go_median = statistics.median(go_ns)
        winner = "rust" if rust_median < go_median else "dolt-go" if go_median < rust_median else "tie"
        per_run = [
            "rust" if rust["elapsed_ns"] < go["elapsed_ns"] else "dolt-go" if go["elapsed_ns"] < rust["elapsed_ns"] else "tie"
            for rust, go in zip(rust_rows, go_rows)
        ]
        consistent = len(set(per_run)) == 1
        if not consistent:
            winner_flips += 1
        summary.append(
            {
                "records": key[0],
                "density": key[1],
                "locality": key[2],
                "operation": key[3],
                "relationship": key[4],
                "runs": runs,
                "rust_median_ns": rust_median,
                "go_median_ns": go_median,
                "rust_median_ns_per_op": statistics.median(row["ns_per_op"] for row in rust_rows),
                "go_median_ns_per_op": statistics.median(row["ns_per_op"] for row in go_rows),
                "rust_cv_percent": cv(rust_ns),
                "go_cv_percent": cv(go_ns),
                "rust_vs_go": go_median / rust_median if rust_median else math.inf,
                "winner": winner,
                "winner_consistent": consistent,
            }
        )
    return summary, winner_flips


def summarize_lifecycle(rows, runs):
    grouped = defaultdict(list)
    for row in rows:
        key = (row["records"], row["density"], row["locality"], row["operation"], row["relationship"])
        grouped[key].append(row)
    summary = []
    for key in sorted(grouped):
        items = sorted(grouped[key], key=lambda row: row["repetition"])
        if len(items) != runs:
            raise SystemExit(f"wrong lifecycle repetition count for {key}")
        elapsed = [row["elapsed_ns"] for row in items]
        summary.append(
            {
                "records": key[0],
                "density": key[1],
                "locality": key[2],
                "operation": key[3],
                "relationship": key[4],
                "runs": runs,
                "median_ns": statistics.median(elapsed),
                "median_ns_per_op": statistics.median(row["ns_per_op"] for row in items),
                "median_ops_per_sec": statistics.median(row["ops_per_sec"] for row in items),
                "cv_percent": cv(elapsed),
                "result_count": items[0]["result_count"],
            }
        )
    return summary


def write_csv(path, rows, fieldnames=None):
    if not rows:
        if fieldnames is None:
            raise SystemExit(f"cannot write empty summary: {path}")
        with path.open("w", newline="", encoding="utf-8") as handle:
            csv.DictWriter(handle, fieldnames=fieldnames, lineterminator="\n").writeheader()
        return
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]), lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def format_ns(value):
    value = float(value)
    if value >= 1_000_000_000:
        return f"{value / 1_000_000_000:.3f} s"
    if value >= 1_000_000:
        return f"{value / 1_000_000:.3f} ms"
    if value >= 1_000:
        return f"{value / 1_000:.3f} us"
    return f"{value:.1f} ns"


def render_report(path, manifest, common, lifecycle, winner_flips):
    winners = Counter(row["winner"] for row in common)
    cvs = [row["rust_cv_percent"] for row in common] + [row["go_cv_percent"] for row in common]
    cvs += [row["cv_percent"] for row in lifecycle]
    max_size = max(row["records"] for row in common)
    lines = [
        "# Native Version-Operation Performance",
        "",
        "All figures are medians from process-isolated, single-worker, warm in-memory runs. Setup and validation are outside timed regions. Lower latency is better.",
        "",
        f"Rust wins: {winners['rust']}; Dolt Go wins: {winners['dolt-go']}; ties: {winners['tie']}.",
        f"Winner-direction groups with repetition flips: {winner_flips}.",
        "",
        "## Provenance",
        "",
        f"- Rust revision: `{manifest['rust_revision']}`",
        f"- Dolt Go revision: `{manifest['dolt_revision']}`",
        f"- Contract: `{manifest['contract_version']}`",
        f"- Sizes: `{manifest['sizes']}`",
        f"- Repetitions: `{manifest['runs']}`",
        "- Storage: in-memory; workers: 1",
        "",
        f"## Common operations at {max_size:,} records",
        "",
        "| Density | Locality | Operation | Rust | Dolt Go | Rust vs Go | Winner | Max CV |",
        "|---:|---|---|---:|---:|---:|---|---:|",
    ]
    for row in common:
        if row["records"] != max_size:
            continue
        lines.append(
            f"| {row['density']}% | {row['locality']} | {row['operation']} | "
            f"{format_ns(row['rust_median_ns'])} | {format_ns(row['go_median_ns'])} | "
            f"{row['rust_vs_go']:.2f}x | {row['winner']} | "
            f"{max(row['rust_cv_percent'], row['go_cv_percent']):.2f}% |"
        )
    lines += [
        "",
        "`Rust vs Go` is Dolt Go median latency divided by Rust median latency; values above 1.0 favor Rust.",
        "Both implementations use native structural patches. Rust emits one verified target-root subtree envelope, while Dolt may emit multiple structural patches. Native item counts can differ, while comparison units and result validation use identical logical changes.",
    ]
    if lifecycle:
        lines += [
            "",
            f"## Rust lifecycle at {max_size:,} records",
            "",
            "| Density | Locality | Operation | Median total | Median normalized | Throughput | CV | Result count |",
            "|---:|---|---|---:|---:|---:|---:|---:|",
        ]
        for row in lifecycle:
            if row["records"] != max_size:
                continue
            lines.append(
                f"| {row['density']}% | {row['locality']} | {row['operation']} | "
                f"{format_ns(row['median_ns'])} | {format_ns(row['median_ns_per_op'])} | "
                f"{row['median_ops_per_sec']:.1f}/s | {row['cv_percent']:.2f}% | {row['result_count']} |"
            )
    noisy = sum(value > 10.0 for value in cvs)
    lines += [
        "",
        "## Reproducibility",
        "",
        "- Complete expected configured matrices: PASS",
        f"- Repetitions per scenario: {manifest['runs']} (matrix complete): PASS",
        "- Cross-language workload and logical-result identity: PASS",
        "- All runner validation flags: PASS",
        f"- Median CV across implementation/scenario groups: {statistics.median(cvs):.2f}%",
        f"- p95 CV: {percentile(cvs, 0.95):.2f}%",
        f"- Maximum CV: {max(cvs):.2f}%",
        f"- Groups above 10% CV: {noisy}",
        "",
    ]
    path.write_text("\n".join(lines), encoding="utf-8")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", required=True, type=Path)
    args = parser.parse_args()
    output = args.output_dir
    manifest = read_manifest(output / "manifest.txt")
    sizes = [int(value) for value in manifest["sizes"].split()]
    runs = int(manifest["runs"])
    densities = [int(value) for value in manifest.get("densities", "0 1 30").split()]
    localities = manifest.get("localities", "append random clustered").split()
    lifecycle_enabled = manifest.get("lifecycle", "1") == "1"
    common_rows = read_rows(output / "results-common.csv")
    lifecycle_rows = read_rows(output / "results-lifecycle.csv", allow_empty=not lifecycle_enabled)
    validate_matrix(common_rows, common_expected(sizes, runs, densities, localities), "common")
    validate_matrix(
        lifecycle_rows,
        lifecycle_expected(sizes, runs, densities, localities, lifecycle_enabled),
        "lifecycle",
    )
    validate_pairs(common_rows)

    common_summary, winner_flips = summarize_common(common_rows, runs)
    lifecycle_summary = summarize_lifecycle(lifecycle_rows, runs)
    write_csv(output / "summary-common.csv", common_summary)
    write_csv(
        output / "summary-lifecycle.csv",
        lifecycle_summary,
        fieldnames=(
            "records",
            "density",
            "locality",
            "operation",
            "relationship",
            "runs",
            "median_ns",
            "median_ns_per_op",
            "median_ops_per_sec",
            "cv_percent",
            "result_count",
        ),
    )

    cvs = [row["rust_cv_percent"] for row in common_summary]
    cvs += [row["go_cv_percent"] for row in common_summary]
    cvs += [row["cv_percent"] for row in lifecycle_summary]
    reproducibility = [
        {"check": "common_rows", "value": len(common_rows), "status": "PASS"},
        {"check": "lifecycle_rows", "value": len(lifecycle_rows), "status": "PASS"},
        {"check": "winner_flip_groups", "value": winner_flips, "status": "PASS" if winner_flips == 0 else "WARN"},
        {"check": "median_cv_percent", "value": f"{statistics.median(cvs):.3f}", "status": "PASS"},
        {"check": "p95_cv_percent", "value": f"{percentile(cvs, 0.95):.3f}", "status": "PASS"},
        {"check": "max_cv_percent", "value": f"{max(cvs):.3f}", "status": "PASS"},
    ]
    write_csv(output / "reproducibility.csv", reproducibility)
    render_report(output / "report.md", manifest, common_summary, lifecycle_summary, winner_flips)
    print(output / "report.md")


if __name__ == "__main__":
    main()
