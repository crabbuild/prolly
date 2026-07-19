#!/usr/bin/env python3
"""Summarize paired local node-publication benchmark revisions."""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import pathlib
import statistics
import sys
from collections import defaultdict
from dataclasses import dataclass


LOCAL_ADAPTER_MEDIAN_NOISE_FLOOR_NS = 5_000.0
LOCAL_ADAPTER_P95_NOISE_FLOOR_NS = 10_000.0


@dataclass(frozen=True)
class Observation:
    suite: str
    role: str
    pair: int
    sample: int
    target: str
    records: int
    api: str
    pattern: str
    latency: float
    throughput: float
    p50: float
    p95: float
    root: str
    valid: bool
    revision: str

    @property
    def group(self) -> tuple[str, str, int, str, str]:
        return (self.suite, self.target, self.records, self.api, self.pattern)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, type=pathlib.Path)
    parser.add_argument("--output-dir", required=True, type=pathlib.Path)
    parser.add_argument("--environment-limitations", type=pathlib.Path)
    parser.add_argument("--minimum-pairs", type=int, default=5)
    return parser.parse_args()


def as_bool(value: str | None, *, default: bool = True) -> bool:
    if value is None or value == "":
        return default
    return value.strip().lower() in {"true", "1", "yes"}


def number(row: dict[str, str], name: str, fallback: str | None = None) -> float:
    value = row.get(name, "")
    if value == "" and fallback is not None:
        value = row.get(fallback, "")
    if value == "":
        raise ValueError(f"missing numeric field {name}")
    return float(value)


def normalize(row: dict[str, str]) -> Observation:
    suite = row.get("suite", "")
    role = row.get("revision_role", "")
    if suite not in {"foundation", "sqlite-turso", "local-adapters"}:
        raise ValueError(f"unknown suite {suite!r}")
    if role not in {"baseline", "candidate"}:
        raise ValueError(f"unknown revision role {role!r}")
    pair = int(row["pair"])
    sample = int(row.get("run") or row.get("repetition") or 1)
    records = int(row["records"])
    api = row["api"]
    revision = row.get("revision", "unknown")
    if suite == "foundation":
        target = row["facade"]
        pattern = row.get("pattern", "")
        latency = number(row, "median_ns")
        throughput = number(row, "throughput_items_per_sec")
        p50 = latency
        p95 = number(row, "p95_ns")
        root = row.get("root", "")
        valid = bool(root)
    else:
        target = row["adapter"]
        pattern = row["pattern"]
        latency = number(row, "total_ns")
        throughput = number(row, "operations_per_sec")
        p50 = number(row, "p50_ns", "total_ns")
        p95 = number(row, "p95_ns", "total_ns")
        root = row.get("root", "")
        if suite == "sqlite-turso":
            valid = as_bool(row.get("validated"))
        else:
            valid = all(
                as_bool(row.get(field), default=False)
                for field in (
                    "value_valid",
                    "count_valid",
                    "root_valid",
                    "reopen_valid",
                )
            )
    if (
        pair <= 0
        or sample <= 0
        or records <= 0
        or latency <= 0
        or throughput <= 0
        or p95 <= 0
    ):
        raise ValueError("pair, sample, records, and measurements must be positive")
    return Observation(
        suite=suite,
        role=role,
        pair=pair,
        sample=sample,
        target=target,
        records=records,
        api=api,
        pattern=pattern,
        latency=latency,
        throughput=throughput,
        p50=p50,
        p95=p95,
        root=root,
        valid=valid,
        revision=revision,
    )


def percent_change(baseline: float, candidate: float) -> float:
    return (candidate - baseline) / baseline * 100.0


def load_limitations(path: pathlib.Path | None) -> list[dict[str, str]]:
    if path is None or not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def aggregate_samples(samples: list[Observation]) -> Observation:
    first = samples[0]
    roots = {sample.root for sample in samples if sample.root}
    revisions = {sample.revision for sample in samples}
    if len(roots) > 1:
        raise ValueError(
            f"sample roots differ for {first.group}:{first.role}:{first.pair}"
        )
    if len(revisions) != 1:
        raise ValueError(
            f"sample revisions differ for {first.group}:{first.role}:{first.pair}"
        )
    return Observation(
        suite=first.suite,
        role=first.role,
        pair=first.pair,
        sample=0,
        target=first.target,
        records=first.records,
        api=first.api,
        pattern=first.pattern,
        latency=statistics.median(sample.latency for sample in samples),
        throughput=statistics.median(sample.throughput for sample in samples),
        p50=statistics.median(sample.p50 for sample in samples),
        p95=statistics.median(sample.p95 for sample in samples),
        root=next(iter(roots), ""),
        valid=all(sample.valid for sample in samples),
        revision=first.revision,
    )


def summarize(
    observations: list[Observation], minimum_pairs: int
) -> tuple[list[dict[str, object]], list[dict[str, object]], list[str]]:
    grouped: dict[tuple[str, str, int, str, str], list[Observation]] = defaultdict(list)
    failures: list[str] = []
    seen: set[tuple[tuple[str, str, int, str, str], str, int, int]] = set()
    for observation in observations:
        identity = (
            observation.group,
            observation.role,
            observation.pair,
            observation.sample,
        )
        if identity in seen:
            failures.append(
                f"duplicate_row:{observation.group}:{observation.role}:"
                f"{observation.pair}:{observation.sample}"
            )
        seen.add(identity)
        if not observation.valid:
            failures.append(
                f"fixture_validation_failure:{observation.group}:{observation.role}:{observation.pair}"
            )
        grouped[observation.group].append(observation)

    summary_rows: list[dict[str, object]] = []
    gate_rows: list[dict[str, object]] = []
    for group in sorted(grouped):
        rows = grouped[group]
        pairs: dict[int, dict[str, list[Observation]]] = defaultdict(
            lambda: defaultdict(list)
        )
        for row in rows:
            pairs[row.pair][row.role].append(row)
        complete_pairs: list[dict[str, Observation]] = []
        sample_counts: set[int] = set()
        for pair, roles in sorted(pairs.items()):
            if set(roles) != {"baseline", "candidate"}:
                failures.append(f"missing_pair:{group}:{pair}")
                continue
            baseline_samples = roles["baseline"]
            candidate_samples = roles["candidate"]
            if len(baseline_samples) != len(candidate_samples):
                failures.append(
                    f"sample_count_mismatch:{group}:{pair}:"
                    f"{len(baseline_samples)}:{len(candidate_samples)}"
                )
                continue
            baseline_sample_ids = {sample.sample for sample in baseline_samples}
            candidate_sample_ids = {sample.sample for sample in candidate_samples}
            if baseline_sample_ids != candidate_sample_ids:
                failures.append(
                    f"sample_id_mismatch:{group}:{pair}:"
                    f"{sorted(baseline_sample_ids)}:{sorted(candidate_sample_ids)}"
                )
                continue
            sample_counts.add(len(baseline_sample_ids))
            baseline = aggregate_samples(baseline_samples)
            candidate = aggregate_samples(candidate_samples)
            baseline_root = baseline.root
            candidate_root = candidate.root
            if baseline_root and candidate_root and baseline_root != candidate_root:
                failures.append(f"fixture_validation_failure:root_mismatch:{group}:{pair}")
            complete_pairs.append({"baseline": baseline, "candidate": candidate})

        if len(sample_counts) > 1:
            failures.append(f"inconsistent_sample_count:{group}:{sorted(sample_counts)}")
        baseline = [roles["baseline"] for roles in complete_pairs]
        candidate = [roles["candidate"] for roles in complete_pairs]
        if not baseline or not candidate:
            continue
        baseline_latency = statistics.median(row.latency for row in baseline)
        candidate_latency = statistics.median(row.latency for row in candidate)
        baseline_throughput = statistics.median(row.throughput for row in baseline)
        candidate_throughput = statistics.median(row.throughput for row in candidate)
        baseline_p50 = statistics.median(row.p50 for row in baseline)
        candidate_p50 = statistics.median(row.p50 for row in candidate)
        baseline_p95 = statistics.median(row.p95 for row in baseline)
        candidate_p95 = statistics.median(row.p95 for row in candidate)
        latency_change = percent_change(baseline_latency, candidate_latency)
        throughput_change = percent_change(baseline_throughput, candidate_throughput)
        p50_change = percent_change(baseline_p50, candidate_p50)
        p95_change = percent_change(baseline_p95, candidate_p95)
        paired_latency_change = statistics.median(
            percent_change(roles["baseline"].latency, roles["candidate"].latency)
            for roles in complete_pairs
        )
        paired_throughput_change = statistics.median(
            percent_change(roles["baseline"].throughput, roles["candidate"].throughput)
            for roles in complete_pairs
        )
        paired_p50_change = statistics.median(
            percent_change(roles["baseline"].p50, roles["candidate"].p50)
            for roles in complete_pairs
        )
        paired_p95_change = statistics.median(
            percent_change(roles["baseline"].p95, roles["candidate"].p95)
            for roles in complete_pairs
        )
        paired_latency_change_ns = statistics.median(
            roles["candidate"].latency - roles["baseline"].latency
            for roles in complete_pairs
        )
        paired_p50_change_ns = statistics.median(
            roles["candidate"].p50 - roles["baseline"].p50
            for roles in complete_pairs
        )
        paired_p95_change_ns = statistics.median(
            roles["candidate"].p95 - roles["baseline"].p95
            for roles in complete_pairs
        )
        pair_count = len(complete_pairs)
        suite, target, records, api, pattern = group
        reasons: list[str] = []
        if pair_count < minimum_pairs:
            reasons.append("statistically_insufficient")
        else:
            latency_above_resolution = (
                suite != "local-adapters"
                or paired_latency_change_ns > LOCAL_ADAPTER_MEDIAN_NOISE_FLOOR_NS
            )
            p95_above_resolution = (
                suite != "local-adapters"
                or paired_p95_change_ns > LOCAL_ADAPTER_P95_NOISE_FLOOR_NS
            )
            if paired_latency_change > 5.0 and latency_above_resolution:
                reasons.append("median_latency_regression")
            if paired_throughput_change < -5.0 and latency_above_resolution:
                reasons.append("median_throughput_regression")
            if paired_p95_change > 10.0 and p95_above_resolution:
                reasons.append("p95_latency_regression")
            if (
                suite == "sqlite-turso"
                and target == "turso-async"
                and api == "put"
                and -paired_latency_change < 40.0
            ):
                reasons.append("turso_point_target_miss")
            if (
                suite == "sqlite-turso"
                and target == "turso-async"
                and api == "put"
                and paired_p50_change > 0.0
            ):
                reasons.append("turso_point_p50_regression")
            if (
                suite == "sqlite-turso"
                and target == "turso-async"
                and api == "put"
                and paired_p95_change > 0.0
            ):
                reasons.append("turso_point_p95_regression")
        for reason in reasons:
            if reason != "statistically_insufficient":
                failures.append(f"{reason}:{group}")
        base = {
            "suite": suite,
            "target": target,
            "records": records,
            "api": api,
            "pattern": pattern,
            "pairs": pair_count,
            "samples_per_revision_pair": min(sample_counts, default=0),
        }
        summary_rows.append(
            {
                **base,
                "baseline_median_ns": baseline_latency,
                "candidate_median_ns": candidate_latency,
                "median_latency_change_pct": latency_change,
                "baseline_throughput": baseline_throughput,
                "candidate_throughput": candidate_throughput,
                "throughput_change_pct": throughput_change,
                "baseline_p50_ns": baseline_p50,
                "candidate_p50_ns": candidate_p50,
                "p50_change_pct": p50_change,
                "baseline_p95_ns": baseline_p95,
                "candidate_p95_ns": candidate_p95,
                "p95_change_pct": p95_change,
                "paired_median_latency_change_pct": paired_latency_change,
                "paired_median_latency_change_ns": paired_latency_change_ns,
                "paired_median_throughput_change_pct": paired_throughput_change,
                "paired_median_p50_change_pct": paired_p50_change,
                "paired_median_p50_change_ns": paired_p50_change_ns,
                "paired_median_p95_change_pct": paired_p95_change,
                "paired_median_p95_change_ns": paired_p95_change_ns,
            }
        )
        gate_rows.append(
            {
                **base,
                "paired_median_latency_change_pct": paired_latency_change,
                "paired_median_latency_change_ns": paired_latency_change_ns,
                "paired_median_throughput_change_pct": paired_throughput_change,
                "paired_median_p50_change_pct": paired_p50_change,
                "paired_median_p50_change_ns": paired_p50_change_ns,
                "paired_median_p95_change_pct": paired_p95_change,
                "paired_median_p95_change_ns": paired_p95_change_ns,
                "status": "insufficient"
                if reasons == ["statistically_insufficient"]
                else ("fail" if reasons else "pass"),
                "reasons": ";".join(reasons),
            }
        )
    return summary_rows, gate_rows, failures


def write_csv(path: pathlib.Path, rows: list[dict[str, object]]) -> None:
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def write_report(
    path: pathlib.Path,
    source: pathlib.Path,
    summary_rows: list[dict[str, object]],
    gate_rows: list[dict[str, object]],
    limitations: list[dict[str, str]],
    failures: list[str],
) -> None:
    revisions = sorted(
        {
            row.get("revision", "unknown")
            for row in read_rows(source)
            if row.get("revision")
        }
    )
    lines = [
        "# Node publication revision gate",
        "",
        f"Generated UTC: {dt.datetime.now(dt.timezone.utc).isoformat()}",
        f"Input: `{source}`",
        f"Revisions: {', '.join(revisions) if revisions else 'unknown'}",
        "",
        "All measurements are local-only; Turso Cloud synchronization is disabled.",
        "Repeated samples within a revision pair are collapsed by median before paired changes are evaluated.",
        "The broad local-adapter screen applies 5 us median and 10 us p95 absolute noise floors in addition to percentage limits; focused and foundation suites do not.",
        "",
        f"Evaluated groups: {len(summary_rows)}",
        f"Gate failures: {len(failures)}",
        "",
    ]
    insufficient = sum(row["status"] == "insufficient" for row in gate_rows)
    if insufficient:
        lines.extend(
            [
                f"Statistically insufficient smoke groups: {insufficient}",
                "These rows validate tooling and correctness but do not support performance claims.",
                "",
            ]
        )
    if limitations:
        lines.extend(["## Environment limitations", ""])
        for limitation in limitations:
            lines.append(
                f"- {limitation.get('adapter', 'unknown')}: {limitation.get('reason', 'unspecified')}"
            )
        lines.append("")
    if failures:
        lines.extend(["## Failures", ""])
        lines.extend(f"- {failure}" for failure in failures)
        lines.append("")
    path.write_text("\n".join(lines), encoding="utf-8")


def read_rows(path: pathlib.Path) -> list[dict[str, str]]:
    if not path.exists() or path.stat().st_size == 0:
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def main() -> int:
    args = parse_args()
    if args.minimum_pairs <= 0:
        print("minimum pairs must be positive", file=sys.stderr)
        return 2
    raw_rows = read_rows(args.input)
    limitations = load_limitations(args.environment_limitations)
    if not raw_rows and not limitations:
        print("input contains no benchmark rows", file=sys.stderr)
        return 2
    try:
        observations = [normalize(row) for row in raw_rows]
        summary_rows, gate_rows, failures = summarize(observations, args.minimum_pairs)
    except (KeyError, TypeError, ValueError) as error:
        print(f"invalid benchmark input: {error}", file=sys.stderr)
        return 2
    args.output_dir.mkdir(parents=True, exist_ok=True)
    write_csv(args.output_dir / "summary.csv", summary_rows)
    write_csv(args.output_dir / "gate.csv", gate_rows)
    write_report(
        args.output_dir / "report.md",
        args.input,
        summary_rows,
        gate_rows,
        limitations,
        failures,
    )
    if failures:
        print("\n".join(failures), file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
