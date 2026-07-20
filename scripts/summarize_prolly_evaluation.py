#!/usr/bin/env python3
"""Validate and summarize a bounded/unbounded Rust versus Dolt-Go evaluation."""

from __future__ import annotations

import argparse
import csv
import math
import statistics
from collections import Counter, defaultdict
from itertools import product
from pathlib import Path
from typing import Iterable, Sequence


TREE_OPERATIONS = ("write", "point_read", "range_scan")
PHASES = ("fresh", "mutation")
WORKLOADS = ("append", "random", "clustered")
VERSION_ZERO_OPERATIONS = (
    "full_diff",
    "range_diff",
    "patch_generate",
    "patch_apply",
    "merge_noop",
)
VERSION_CHANGED_OPERATIONS = (
    "full_diff",
    "range_diff",
    "patch_generate",
    "patch_apply",
    "merge_disjoint",
    "merge_convergent",
    "merge_conflict",
)
LIFECYCLE_OPERATIONS = {
    "publish": ("version_publish",),
    "read": (
        "head_resolve",
        "snapshot_resolve",
        "historical_point_read",
        "historical_range_scan",
        "version_list",
    ),
    "rollback": ("rollback",),
    "prune": ("retention_prune",),
}
TREE_INVARIANTS = (
    "contract_version",
    "records",
    "phase",
    "workload",
    "operation",
    "operations",
    "workload_digest",
    "result_count",
    "validated",
)
VERSION_INVARIANTS = (
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
LIFECYCLE_INVARIANTS = (
    "contract_version",
    "records",
    "density",
    "locality",
    "operation",
    "relationship",
    "operations",
    "workload_digest",
    "result_count",
    "base_count",
    "target_count",
    "conflict_count",
    "validated",
)


class SummaryError(RuntimeError):
    pass


def read_manifest(path: Path) -> dict[str, str]:
    if not path.is_file():
        raise SummaryError(f"manifest is missing: {path}")
    manifest = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        if "=" in line:
            key, value = line.split("=", 1)
            manifest[key] = value
    required = {
        "fingerprint",
        "rust_revision",
        "dolt_revision",
        "sizes",
        "runs",
        "densities",
        "localities",
        "rust_cache_profiles",
        "lifecycle",
    }
    missing = required - set(manifest)
    if missing:
        raise SummaryError(f"manifest fields are missing: {sorted(missing)}")
    return manifest


def read_rows(path: Path, allow_empty: bool = False) -> list[dict[str, str]]:
    if not path.is_file():
        raise SummaryError(f"results are missing: {path}")
    with path.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    if not rows and not allow_empty:
        raise SummaryError(f"results are empty: {path}")
    required = {
        "implementation",
        "records",
        "operation",
        "elapsed_ns",
        "ns_per_op",
        "ops_per_sec",
        "validated",
        "repetition",
        "peak_rss_bytes",
        "cache_profile",
    }
    for row in rows:
        missing = required - set(row)
        if missing:
            raise SummaryError(f"row fields are missing in {path}: {sorted(missing)}")
        if row["validated"] != "true":
            raise SummaryError(f"unvalidated row in {path}")
        for field in ("records", "elapsed_ns", "repetition", "peak_rss_bytes"):
            try:
                row[field] = int(row[field])
            except ValueError as error:
                raise SummaryError(f"invalid {field} in {path}") from error
        for field in ("ns_per_op", "ops_per_sec"):
            try:
                row[field] = float(row[field])
            except ValueError as error:
                raise SummaryError(f"invalid {field} in {path}") from error
            if not math.isfinite(row[field]):
                raise SummaryError(f"non-finite {field} in {path}")
    return rows


def integers(raw: str) -> tuple[int, ...]:
    return tuple(int(value) for value in raw.replace(",", " ").split())


def words(raw: str) -> tuple[str, ...]:
    return tuple(raw.replace(",", " ").split())


def validate_tree_matrix(
    rows: Sequence[dict[str, object]], sizes: Sequence[int], runs: int, profiles: Sequence[str]
) -> None:
    identities = Counter(
        (
            row["implementation"],
            row["cache_profile"],
            row["records"],
            row["phase"],
            row["workload"],
            row["operation"],
            row["repetition"],
        )
        for row in rows
    )
    duplicates = [key for key, count in identities.items() if count != 1]
    if duplicates:
        raise SummaryError(f"duplicate tree rows: {duplicates[:3]}")
    participants = (("dolt-go", "native"), *(('rust', profile) for profile in profiles))
    expected = {
        (implementation, profile, size, phase, workload, operation, repetition)
        for (implementation, profile), size, phase, workload, operation, repetition in product(
            participants, sizes, PHASES, WORKLOADS, TREE_OPERATIONS, range(1, runs + 1)
        )
    }
    actual = set(identities)
    if actual != expected:
        raise SummaryError(
            f"incomplete tree matrix: missing={list(expected - actual)[:3]}, "
            f"extra={list(actual - expected)[:3]}"
        )


def version_scenarios(
    densities: Sequence[int], localities: Sequence[str]
) -> Iterable[tuple[int, str, tuple[str, ...]]]:
    if 0 in densities:
        yield 0, "none", VERSION_ZERO_OPERATIONS
    for density in densities:
        if density == 0:
            continue
        for locality in localities:
            yield density, locality, VERSION_CHANGED_OPERATIONS


def validate_version_matrix(
    rows: Sequence[dict[str, object]],
    sizes: Sequence[int],
    runs: int,
    profiles: Sequence[str],
    densities: Sequence[int],
    localities: Sequence[str],
) -> None:
    identities = Counter(
        (
            row["implementation"],
            row["cache_profile"],
            row["records"],
            int(row["density"]),
            row["locality"],
            row["operation"],
            row["repetition"],
        )
        for row in rows
    )
    duplicates = [key for key, count in identities.items() if count != 1]
    if duplicates:
        raise SummaryError(f"duplicate version rows: {duplicates[:3]}")
    participants = (("dolt-go", "native"), *(('rust', profile) for profile in profiles))
    expected = set()
    for size, repetition, (density, locality, operations), (implementation, profile) in product(
        sizes,
        range(1, runs + 1),
        tuple(version_scenarios(densities, localities)),
        participants,
    ):
        for operation in operations:
            expected.add(
                (implementation, profile, size, density, locality, operation, repetition)
            )
    actual = set(identities)
    if actual != expected:
        raise SummaryError(
            f"incomplete version matrix: missing={list(expected - actual)[:3]}, "
            f"extra={list(actual - expected)[:3]}"
        )


def validate_lifecycle_matrix(
    rows: Sequence[dict[str, object]],
    sizes: Sequence[int],
    runs: int,
    profiles: Sequence[str],
    densities: Sequence[int],
    localities: Sequence[str],
    enabled: bool,
) -> None:
    identities = Counter(
        (
            row["implementation"],
            row["cache_profile"],
            row["records"],
            int(row["density"]),
            row["locality"],
            row["operation"],
            row["relationship"],
            row["repetition"],
        )
        for row in rows
    )
    duplicates = [key for key, count in identities.items() if count != 1]
    if duplicates:
        raise SummaryError(f"duplicate lifecycle rows: {duplicates[:3]}")
    expected = set()
    if enabled:
        for size, repetition, profile in product(
            sizes, range(1, runs + 1), profiles
        ):
            for density in densities:
                if density == 0:
                    continue
                for locality in localities:
                    expected.add(
                        (
                            "rust-lifecycle",
                            profile,
                            size,
                            density,
                            locality,
                            "version_publish",
                            "publish",
                            repetition,
                        )
                    )
            for scenario in ("read", "rollback", "prune"):
                for operation in LIFECYCLE_OPERATIONS[scenario]:
                    expected.add(
                        (
                            "rust-lifecycle",
                            profile,
                            size,
                            0,
                            "none",
                            operation,
                            scenario,
                            repetition,
                        )
                    )
    actual = set(identities)
    if actual != expected:
        raise SummaryError(
            f"incomplete lifecycle matrix: missing={list(expected - actual)[:3]}, "
            f"extra={list(actual - expected)[:3]}"
        )


def validate_comparison_identity(
    rows: Sequence[dict[str, object]],
    profiles: Sequence[str],
    domain: str,
) -> None:
    key_fields = (
        ("records", "phase", "workload", "operation", "repetition")
        if domain == "tree"
        else (
            "records",
            "density",
            "locality",
            "operation",
            "relationship",
            "repetition",
        )
    )
    invariants = TREE_INVARIANTS if domain == "tree" else VERSION_INVARIANTS
    grouped = defaultdict(dict)
    for row in rows:
        key = tuple(row[field] for field in key_fields)
        grouped[key][row["cache_profile"]] = row
    expected = {"native", *profiles}
    for key, group in grouped.items():
        if set(group) != expected:
            raise SummaryError(f"incomplete {domain} profile identity: {key}")
        go = group["native"]
        for profile in profiles:
            rust = group[profile]
            for field in invariants:
                if rust[field] != go[field]:
                    raise SummaryError(
                        f"{domain} identity mismatch {key} profile={profile} "
                        f"field={field}: {rust[field]!r} != {go[field]!r}"
                    )
            if domain == "version" and rust["operation"] != "patch_generate":
                if rust["result_count"] != go["result_count"]:
                    raise SummaryError(
                        f"version result_count mismatch {key} profile={profile}"
                    )


def validate_lifecycle_identity(
    rows: Sequence[dict[str, object]], profiles: Sequence[str]
) -> None:
    grouped = defaultdict(dict)
    for row in rows:
        key = (
            row["records"],
            row["density"],
            row["locality"],
            row["operation"],
            row["relationship"],
            row["repetition"],
        )
        grouped[key][row["cache_profile"]] = row
    expected = set(profiles)
    for key, group in grouped.items():
        if set(group) != expected:
            raise SummaryError(f"incomplete lifecycle profile identity: {key}")
        baseline = group[profiles[0]]
        for profile in profiles[1:]:
            candidate = group[profile]
            for field in LIFECYCLE_INVARIANTS:
                if candidate[field] != baseline[field]:
                    raise SummaryError(
                        f"lifecycle identity mismatch {key} profile={profile} "
                        f"field={field}: {candidate[field]!r} != {baseline[field]!r}"
                    )


def cv(values: Sequence[float]) -> float:
    mean = statistics.fmean(values)
    return statistics.pstdev(values) / mean * 100.0 if mean else 0.0


def quality_label(
    repetitions: int, consistent: bool, ratio: float, maximum_cv: float
) -> str:
    if repetitions < 3:
        return "insufficient_repetitions"
    if not consistent:
        return "winner_flip"
    if 0.95 <= ratio <= 1.05:
        return "narrow"
    if maximum_cv > 25.0:
        return "high_variance"
    return "stable"


def safe_ratio(numerator: float, denominator: float) -> float:
    if denominator:
        return numerator / denominator
    return 1.0 if numerator == 0 else math.inf


def comparison_summary(
    rows: Sequence[dict[str, object]], profiles: Sequence[str], domain: str
) -> list[dict[str, object]]:
    scenario_fields = (
        ("records", "phase", "workload", "operation")
        if domain == "tree"
        else ("records", "density", "locality", "operation", "relationship")
    )
    grouped: dict[tuple[object, ...], dict[str, list[dict[str, object]]]] = defaultdict(
        lambda: defaultdict(list)
    )
    for row in rows:
        key = tuple(row[field] for field in scenario_fields)
        grouped[key][str(row["cache_profile"])].append(row)
    summary = []
    for key in sorted(grouped):
        group = grouped[key]
        go_rows = sorted(group["native"], key=lambda row: int(row["repetition"]))
        for profile in profiles:
            rust_rows = sorted(group[profile], key=lambda row: int(row["repetition"]))
            if len(rust_rows) != len(go_rows):
                raise SummaryError(f"profile repetition mismatch: {key} {profile}")
            rust_elapsed = [int(row["elapsed_ns"]) for row in rust_rows]
            go_elapsed = [int(row["elapsed_ns"]) for row in go_rows]
            rust_median = statistics.median(rust_elapsed)
            go_median = statistics.median(go_elapsed)
            ratio = safe_ratio(go_median, rust_median)
            winner = "rust" if ratio > 1 else "dolt-go" if ratio < 1 else "tie"
            per_run = [
                "rust" if int(rust["elapsed_ns"]) < int(go["elapsed_ns"]) else "dolt-go"
                if int(go["elapsed_ns"]) < int(rust["elapsed_ns"]) else "tie"
                for rust, go in zip(rust_rows, go_rows)
            ]
            rust_cv = cv(rust_elapsed)
            go_cv = cv(go_elapsed)
            result = {field: value for field, value in zip(scenario_fields, key)}
            result.update(
                {
                    "cache_profile": profile,
                    "runs": len(rust_rows),
                    "rust_median_ns": rust_median,
                    "go_median_ns": go_median,
                    "rust_median_ns_per_op": statistics.median(
                        float(row["ns_per_op"]) for row in rust_rows
                    ),
                    "go_median_ns_per_op": statistics.median(
                        float(row["ns_per_op"]) for row in go_rows
                    ),
                    "rust_cv_percent": rust_cv,
                    "go_cv_percent": go_cv,
                    "rust_median_peak_rss_bytes": statistics.median(
                        int(row["peak_rss_bytes"]) for row in rust_rows
                    ),
                    "go_median_peak_rss_bytes": statistics.median(
                        int(row["peak_rss_bytes"]) for row in go_rows
                    ),
                    "rust_vs_go": ratio,
                    "winner": winner,
                    "winner_consistent": len(set(per_run)) == 1,
                    "quality": quality_label(
                        len(rust_rows),
                        len(set(per_run)) == 1,
                        ratio,
                        max(rust_cv, go_cv),
                    ),
                }
            )
            summary.append(result)
    return summary


def lifecycle_summary(
    rows: Sequence[dict[str, object]], profiles: Sequence[str], runs: int
) -> list[dict[str, object]]:
    grouped = defaultdict(list)
    digest_groups = defaultdict(set)
    for row in rows:
        scenario = (
            row["records"],
            row["density"],
            row["locality"],
            row["operation"],
            row["relationship"],
        )
        key = (
            *scenario,
            row["cache_profile"],
        )
        grouped[key].append(row)
        digest_groups[(*scenario, row["repetition"])].add(row["result_digest"])
    summary = []
    for key in sorted(grouped):
        items = grouped[key]
        if key[-1] not in profiles or len(items) != runs:
            raise SummaryError(f"invalid lifecycle group: {key}")
        elapsed = [int(row["elapsed_ns"]) for row in items]
        summary.append(
            {
                "records": key[0],
                "density": key[1],
                "locality": key[2],
                "operation": key[3],
                "relationship": key[4],
                "cache_profile": key[5],
                "runs": runs,
                "median_ns": statistics.median(elapsed),
                "median_ns_per_op": statistics.median(float(row["ns_per_op"]) for row in items),
                "cv_percent": cv(elapsed),
                "median_peak_rss_bytes": statistics.median(
                    int(row["peak_rss_bytes"]) for row in items
                ),
                "result_digest_consistent": all(
                    len(digest_groups[(*key[:-1], repetition)]) == 1
                    for repetition in range(1, runs + 1)
                ),
            }
        )
    return summary


def cache_effect(
    summary: Sequence[dict[str, object]], domain: str
) -> list[dict[str, object]]:
    if {str(row["cache_profile"]) for row in summary} != {"bounded", "unbounded"}:
        return []
    fields = (
        ("records", "phase", "workload", "operation")
        if domain == "tree"
        else ("records", "density", "locality", "operation", "relationship")
    )
    grouped = defaultdict(dict)
    for row in summary:
        key = tuple(row[field] for field in fields)
        grouped[key][row["cache_profile"]] = row
    effects = []
    for key in sorted(grouped):
        bounded = grouped[key]["bounded"]
        unbounded = grouped[key]["unbounded"]
        result = {field: value for field, value in zip(fields, key)}
        result.update(
            {
                "bounded_rust_median_ns": bounded["rust_median_ns"],
                "unbounded_rust_median_ns": unbounded["rust_median_ns"],
                "unbounded_speedup": safe_ratio(
                    float(bounded["rust_median_ns"]),
                    float(unbounded["rust_median_ns"]),
                ),
                "bounded_rust_median_peak_rss_bytes": bounded[
                    "rust_median_peak_rss_bytes"
                ],
                "unbounded_rust_median_peak_rss_bytes": unbounded[
                    "rust_median_peak_rss_bytes"
                ],
                "rss_ratio": safe_ratio(
                    float(unbounded["rust_median_peak_rss_bytes"]),
                    float(bounded["rust_median_peak_rss_bytes"]),
                ),
            }
        )
        effects.append(result)
    return effects


def write_csv(path: Path, rows: Sequence[dict[str, object]], fields: Sequence[str] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if rows:
        fields = tuple(rows[0])
    elif fields is None:
        raise SummaryError(f"cannot write empty CSV without fields: {path}")
    temporary = path.with_suffix(path.suffix + ".tmp")
    with temporary.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)
    temporary.replace(path)


def format_duration(value: object) -> str:
    ns = float(value)
    if ns >= 1_000_000_000:
        return f"{ns / 1_000_000_000:.3f} s"
    if ns >= 1_000_000:
        return f"{ns / 1_000_000:.3f} ms"
    if ns >= 1_000:
        return f"{ns / 1_000:.3f} us"
    return f"{ns:.1f} ns"


def render_report(
    path: Path,
    manifest: dict[str, str],
    tree: Sequence[dict[str, object]],
    version: Sequence[dict[str, object]],
    lifecycle: Sequence[dict[str, object]],
) -> None:
    profiles = words(manifest["rust_cache_profiles"])
    maximum_size = max(int(row["records"]) for row in tree)
    lines = [
        "# Repeatable Dolt Go vs Rust Prolly Evaluation",
        "",
        "One pinned Dolt Go baseline is compared with bounded and unbounded Rust cache profiles. All figures are medians from process-isolated, single-worker, in-memory runs.",
        "",
        "## Provenance",
        "",
        f"- Fingerprint: `{manifest['fingerprint']}`",
        f"- Rust: `{manifest['rust_revision']}`",
        f"- Dolt Go: `{manifest['dolt_revision']}`",
        f"- Sizes: `{manifest['sizes']}`; repetitions: {manifest['runs']}",
        f"- Rust cache profiles: `{manifest['rust_cache_profiles']}`",
        "",
        "## Winner summary",
        "",
        "| Domain | Rust profile | Rust wins | Dolt Go wins | Ties | Unstable/narrow groups |",
        "|---|---|---:|---:|---:|---:|",
    ]
    for domain, rows in (("tree", tree), ("version", version)):
        for profile in profiles:
            selected = [row for row in rows if row["cache_profile"] == profile]
            winners = Counter(str(row["winner"]) for row in selected)
            flagged = sum(row["quality"] != "stable" for row in selected)
            lines.append(
                f"| {domain} | {profile} | {winners['rust']} | {winners['dolt-go']} | "
                f"{winners['tie']} | {flagged} |"
            )
    lines.extend(
        [
            "",
            f"## Tree operations at {maximum_size:,} records",
            "",
            "| Profile | Phase | Workload | Operation | Rust | Dolt Go | Rust vs Go | Winner | Quality | Rust RSS |",
            "|---|---|---|---|---:|---:|---:|---|---|---:|",
        ]
    )
    for row in tree:
        if int(row["records"]) != maximum_size:
            continue
        lines.append(
            f"| {row['cache_profile']} | {row['phase']} | {row['workload']} | {row['operation']} | "
            f"{float(row['rust_median_ns_per_op']):.3f} ns/op | {float(row['go_median_ns_per_op']):.3f} ns/op | "
            f"{float(row['rust_vs_go']):.2f}x | {row['winner']} | {row['quality']} | "
            f"{float(row['rust_median_peak_rss_bytes']) / 2**30:.2f} GiB |"
        )
    lines.extend(
        [
            "",
            f"## Version operations at {maximum_size:,} records",
            "",
            "| Profile | Density | Locality | Operation | Rust | Dolt Go | Rust vs Go | Winner | Quality | Rust RSS |",
            "|---|---:|---|---|---:|---:|---:|---|---|---:|",
        ]
    )
    for row in version:
        if int(row["records"]) != maximum_size:
            continue
        lines.append(
            f"| {row['cache_profile']} | {row['density']}% | {row['locality']} | {row['operation']} | "
            f"{format_duration(row['rust_median_ns'])} | {format_duration(row['go_median_ns'])} | "
            f"{float(row['rust_vs_go']):.2f}x | {row['winner']} | {row['quality']} | "
            f"{float(row['rust_median_peak_rss_bytes']) / 2**30:.2f} GiB |"
        )
    if lifecycle:
        lines.extend(
            [
                "",
                f"## Rust lifecycle at {maximum_size:,} records",
                "",
                "| Profile | Density | Locality | Operation | Median | CV | RSS | Result identity |",
                "|---|---:|---|---|---:|---:|---:|---|",
            ]
        )
        for row in lifecycle:
            if int(row["records"]) != maximum_size:
                continue
            lines.append(
                f"| {row['cache_profile']} | {row['density']}% | {row['locality']} | "
                f"{row['operation']} | {format_duration(row['median_ns'])} | "
                f"{float(row['cv_percent']):.2f}% | "
                f"{float(row['median_peak_rss_bytes']) / 2**30:.2f} GiB | "
                f"{'match' if row['result_digest_consistent'] else 'DIVERGED'} |"
            )
    all_rows = [*tree, *version]
    lifecycle_divergences = {
        (
            row["records"],
            row["density"],
            row["locality"],
            row["operation"],
            row["relationship"],
        )
        for row in lifecycle
        if not bool(row["result_digest_consistent"])
    }
    lines.extend(
        [
            "",
            "## Reproducibility audit",
            "",
            "- Complete configured tree matrix: PASS",
            "- Complete configured version matrix: PASS",
            "- Cross-profile and cross-language logical identity: PASS",
            f"- Lifecycle result-digest divergence groups: {len(lifecycle_divergences)} "
            f"({'WARN' if lifecycle_divergences else 'PASS'})",
            "- All runner validation flags: PASS",
            f"- Publication repetition gate: {'PASS' if int(manifest['runs']) >= 3 else 'SMOKE ONLY'}",
            f"- Winner-direction flip groups: {sum(not bool(row['winner_consistent']) for row in all_rows)}",
            f"- Groups with CV above 25%: {sum(max(float(row['rust_cv_percent']), float(row['go_cv_percent'])) > 25 for row in all_rows)}",
            "",
            "`Rust vs Go` is Go median latency divided by Rust median latency. Values above 1.0 favor Rust. `insufficient_repetitions`, `winner_flip`, `narrow`, and `high_variance` rows should not be used as strong winner claims.",
            "",
        ]
    )
    path.write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    manifest = read_manifest(args.output / "manifest.txt")
    sizes = integers(manifest["sizes"])
    runs = int(manifest["runs"])
    profiles = words(manifest["rust_cache_profiles"])
    densities = integers(manifest["densities"])
    localities = words(manifest["localities"])
    tree_rows = read_rows(args.output / "tree" / "results.csv")
    version_rows = read_rows(args.output / "version" / "results-common.csv")
    lifecycle_enabled = manifest["lifecycle"] == "1"
    lifecycle_rows = read_rows(
        args.output / "lifecycle" / "results.csv", allow_empty=not lifecycle_enabled
    )
    validate_tree_matrix(tree_rows, sizes, runs, profiles)
    validate_comparison_identity(tree_rows, profiles, "tree")
    validate_version_matrix(
        version_rows, sizes, runs, profiles, densities, localities
    )
    validate_comparison_identity(version_rows, profiles, "version")
    validate_lifecycle_matrix(
        lifecycle_rows,
        sizes,
        runs,
        profiles,
        densities,
        localities,
        lifecycle_enabled,
    )
    validate_lifecycle_identity(lifecycle_rows, profiles)
    tree_summary = comparison_summary(tree_rows, profiles, "tree")
    version_summary = comparison_summary(version_rows, profiles, "version")
    life_summary = lifecycle_summary(lifecycle_rows, profiles, runs)
    write_csv(args.output / "tree" / "summary.csv", tree_summary)
    write_csv(args.output / "version" / "summary.csv", version_summary)
    write_csv(
        args.output / "lifecycle" / "summary.csv",
        life_summary,
        fields=(
            "records",
            "density",
            "locality",
            "operation",
            "relationship",
            "cache_profile",
            "runs",
            "median_ns",
            "median_ns_per_op",
            "cv_percent",
            "median_peak_rss_bytes",
            "result_digest_consistent",
        ),
    )
    write_csv(
        args.output / "tree" / "cache-effect.csv",
        cache_effect(tree_summary, "tree"),
        fields=(
            "records",
            "phase",
            "workload",
            "operation",
            "bounded_rust_median_ns",
            "unbounded_rust_median_ns",
            "unbounded_speedup",
            "bounded_rust_median_peak_rss_bytes",
            "unbounded_rust_median_peak_rss_bytes",
            "rss_ratio",
        ),
    )
    write_csv(
        args.output / "version" / "cache-effect.csv",
        cache_effect(version_summary, "version"),
        fields=(
            "records",
            "density",
            "locality",
            "operation",
            "relationship",
            "bounded_rust_median_ns",
            "unbounded_rust_median_ns",
            "unbounded_speedup",
            "bounded_rust_median_peak_rss_bytes",
            "unbounded_rust_median_peak_rss_bytes",
            "rss_ratio",
        ),
    )
    render_report(
        args.output / "report.md",
        manifest,
        tree_summary,
        version_summary,
        life_summary,
    )
    warning_path = args.output / "WARNINGS"
    lifecycle_divergences = sorted(
        {
            (
                row["records"],
                row["density"],
                row["locality"],
                row["operation"],
                row["relationship"],
            )
            for row in life_summary
            if not bool(row["result_digest_consistent"])
        }
    )
    if lifecycle_divergences:
        warning_path.write_text(
            "Lifecycle result digests differ across Rust cache profiles for:\n"
            + "\n".join(",".join(map(str, item)) for item in lifecycle_divergences)
            + "\n",
            encoding="utf-8",
        )
    else:
        warning_path.unlink(missing_ok=True)
    (args.output / "COMPLETE").write_text(manifest["fingerprint"] + "\n", encoding="utf-8")
    print(args.output / "report.md")


if __name__ == "__main__":
    try:
        main()
    except SummaryError as error:
        raise SystemExit(f"invalid evaluation results: {error}") from error
