#!/usr/bin/env python3
"""Run and validate the repeatable Dolt-Go/Rust prolly evaluation matrix."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Sequence


TREE_HEADER = (
    "implementation,revision,contract_version,records,phase,workload,operation,"
    "operations,elapsed_ns,ns_per_op,ops_per_sec,workload_digest,result_count,validated"
)
VERSION_HEADER = (
    "implementation,revision,contract_version,records,density,locality,operation,"
    "relationship,operations,elapsed_ns,ns_per_op,ops_per_sec,workload_digest,"
    "result_digest,result_count,base_count,target_count,conflict_count,validated"
)
TREE_OPERATIONS = ("write", "point_read", "range_scan")
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


class EvaluationError(RuntimeError):
    pass


@dataclass(frozen=True)
class Participant:
    implementation: str
    profile: str
    binary: Path
    revision: str

    @property
    def runner_implementation(self) -> str:
        return "rust" if self.implementation == "rust" else "dolt-go"


@dataclass(frozen=True)
class Sample:
    domain: str
    participant: Participant
    repetition: int
    arguments: tuple[str, ...]
    identity: tuple[str, ...]
    expected_operations: tuple[str, ...]

    @property
    def slug(self) -> str:
        return "-".join((self.domain, self.participant.implementation, self.participant.profile, *self.identity, f"run{self.repetition}"))


def parse_positive_list(raw: str, label: str) -> tuple[int, ...]:
    try:
        values = tuple(int(value) for value in raw.replace(",", " ").split())
    except ValueError as error:
        raise argparse.ArgumentTypeError(f"{label} must contain integers") from error
    if not values or any(value <= 0 for value in values):
        raise argparse.ArgumentTypeError(f"{label} must contain positive integers")
    if len(set(values)) != len(values):
        raise argparse.ArgumentTypeError(f"{label} contains duplicates")
    return values


def parse_profiles(raw: str) -> tuple[str, ...]:
    profiles = tuple(raw.replace(",", " ").split())
    if not profiles:
        raise argparse.ArgumentTypeError("at least one Rust cache profile is required")
    invalid = set(profiles) - {"bounded", "unbounded"}
    if invalid:
        raise argparse.ArgumentTypeError(f"unknown Rust cache profiles: {sorted(invalid)}")
    if len(set(profiles)) != len(profiles):
        raise argparse.ArgumentTypeError("Rust cache profiles contain duplicates")
    return profiles


def read_csv(path: Path, expected_header: str) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        header = handle.readline().rstrip("\r\n")
        handle.seek(0)
        rows = list(csv.DictReader(handle))
    if header != expected_header:
        raise EvaluationError(f"header mismatch in {path}: {header!r}")
    if not rows:
        raise EvaluationError(f"no rows in {path}")
    return rows


def validate_rows(sample: Sample, rows: Sequence[dict[str, str]]) -> None:
    operations = tuple(row.get("operation", "") for row in rows)
    if operations != sample.expected_operations:
        raise EvaluationError(
            f"operation sequence mismatch for {sample.slug}: {operations!r}"
        )
    expected_implementation = (
        "rust-lifecycle" if sample.domain == "lifecycle" else sample.participant.runner_implementation
    )
    for row in rows:
        if row.get("implementation") != expected_implementation:
            raise EvaluationError(f"implementation mismatch for {sample.slug}")
        if row.get("validated") != "true":
            raise EvaluationError(f"unvalidated row for {sample.slug}")
        if row.get("revision") != sample.participant.revision:
            raise EvaluationError(f"revision mismatch for {sample.slug}")
        for field in ("operations", "elapsed_ns", "result_count"):
            try:
                value = int(row[field])
            except (KeyError, ValueError) as error:
                raise EvaluationError(f"invalid {field} for {sample.slug}") from error
            if value < 0:
                raise EvaluationError(f"negative {field} for {sample.slug}")
        for field in ("ns_per_op", "ops_per_sec"):
            try:
                value = float(row[field])
            except (KeyError, ValueError) as error:
                raise EvaluationError(f"invalid {field} for {sample.slug}") from error
            if value != value or value in (float("inf"), float("-inf")):
                raise EvaluationError(f"non-finite {field} for {sample.slug}")


def parse_peak_rss(path: Path) -> int:
    text = path.read_text(encoding="utf-8")
    match = re.search(r"^\s*(\d+)\s+maximum resident set size\s*$", text, re.MULTILINE)
    if match:
        return int(match.group(1))
    match = re.search(r"Maximum resident set size \(kbytes\):\s*(\d+)", text)
    if match:
        return int(match.group(1)) * 1024
    raise EvaluationError(f"peak RSS is missing from {path}")


def atomic_json(path: Path, value: dict[str, object]) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    temporary.replace(path)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def sample_command(sample: Sample) -> list[str]:
    return [str(sample.participant.binary), *sample.arguments]


def load_completed_sample(
    sample: Sample,
    state_path: Path,
    csv_path: Path,
    time_path: Path,
    fingerprint: str,
) -> tuple[list[dict[str, str]], int] | None:
    if not state_path.is_file():
        return None
    try:
        state = json.loads(state_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as error:
        raise EvaluationError(f"invalid resume state {state_path}: {error}") from error
    expected = {
        "fingerprint": fingerprint,
        "command": sample_command(sample),
        "profile": sample.participant.profile,
        "revision": sample.participant.revision,
        "exit_status": 0,
    }
    for field, value in expected.items():
        if state.get(field) != value:
            raise EvaluationError(
                f"resume state mismatch for {sample.slug} field={field}: "
                f"{state.get(field)!r} != {value!r}"
            )
    if not csv_path.is_file() or not time_path.is_file():
        raise EvaluationError(f"resume state exists but artifacts are missing for {sample.slug}")
    artifact_paths = {
        "stdout_sha256": csv_path,
        "stderr_sha256": csv_path.with_suffix(".stderr"),
        "time_sha256": time_path,
    }
    for field, artifact_path in artifact_paths.items():
        if not artifact_path.is_file():
            raise EvaluationError(
                f"resume state exists but artifact is missing for {sample.slug}: {artifact_path}"
            )
        actual_digest = sha256_file(artifact_path)
        if state.get(field) != actual_digest:
            raise EvaluationError(
                f"resume artifact mismatch for {sample.slug} field={field}"
            )
    header = TREE_HEADER if sample.domain == "tree" else VERSION_HEADER
    rows = read_csv(csv_path, header)
    validate_rows(sample, rows)
    peak_rss = parse_peak_rss(time_path)
    if state.get("peak_rss_bytes") != peak_rss:
        raise EvaluationError(f"resume RSS mismatch for {sample.slug}")
    return rows, peak_rss


def run_sample(
    sample: Sample,
    raw_dir: Path,
    state_dir: Path,
    fingerprint: str,
    time_bin: Path,
    time_mode: str,
    resume: bool,
) -> tuple[list[dict[str, str]], int]:
    prefix = raw_dir / sample.slug
    csv_path = prefix.with_suffix(".csv")
    stderr_path = prefix.with_suffix(".stderr")
    time_path = prefix.with_suffix(".time")
    state_path = state_dir / f"{sample.slug}.json"
    if resume:
        completed = load_completed_sample(
            sample, state_path, csv_path, time_path, fingerprint
        )
        if completed is not None:
            print(f"reusing {sample.slug}", file=sys.stderr, flush=True)
            return completed
    elif any(path.exists() for path in (csv_path, stderr_path, time_path, state_path)):
        raise EvaluationError(f"sample artifacts already exist: {sample.slug}")

    command = sample_command(sample)
    env = os.environ.copy()
    env.update(
        {
            "RAYON_NUM_THREADS": "1",
            "GOMAXPROCS": "1",
            "BENCH_REVISION": sample.participant.revision,
        }
    )
    if sample.participant.implementation == "rust":
        env["PROLLY_BENCH_CACHE_PROFILE"] = sample.participant.profile
        env.pop("PROLLY_COMPARE_CACHE_NODES", None)
        env.pop("PROLLY_COMPARE_CACHE_BYTES", None)
    else:
        env.pop("PROLLY_BENCH_CACHE_PROFILE", None)
    print(f"running {sample.slug}", file=sys.stderr, flush=True)
    with csv_path.open("w", encoding="utf-8") as stdout, stderr_path.open(
        "w", encoding="utf-8"
    ) as stderr:
        completed = subprocess.run(
            [str(time_bin), time_mode, "-o", str(time_path), *command],
            env=env,
            stdout=stdout,
            stderr=stderr,
            check=False,
        )
    if completed.returncode != 0:
        raise EvaluationError(
            f"sample failed exit={completed.returncode}: {sample.slug}; see {stderr_path}"
        )
    header = TREE_HEADER if sample.domain == "tree" else VERSION_HEADER
    rows = read_csv(csv_path, header)
    validate_rows(sample, rows)
    peak_rss = parse_peak_rss(time_path)
    atomic_json(
        state_path,
        {
            "fingerprint": fingerprint,
            "command": command,
            "profile": sample.participant.profile,
            "revision": sample.participant.revision,
            "exit_status": 0,
            "peak_rss_bytes": peak_rss,
            "stdout": str(csv_path),
            "stderr": str(stderr_path),
            "time": str(time_path),
            "stdout_sha256": sha256_file(csv_path),
            "stderr_sha256": sha256_file(stderr_path),
            "time_sha256": sha256_file(time_path),
        },
    )
    return rows, peak_rss


def rotate(values: Sequence[Participant], offset: int) -> tuple[Participant, ...]:
    split = offset % len(values)
    return tuple((*values[split:], *values[:split]))


def tree_samples(
    participants: Sequence[Participant], sizes: Sequence[int], runs: int
) -> Iterable[Sample]:
    for records in sizes:
        for repetition in range(1, runs + 1):
            for phase in ("fresh", "mutation"):
                for workload in ("append", "random", "clustered"):
                    order = rotate(participants, records + repetition + len(phase) + len(workload))
                    for participant in order:
                        yield Sample(
                            "tree",
                            participant,
                            repetition,
                            ("--records", str(records), "--phase", phase, "--workload", workload),
                            (str(records), phase, workload),
                            TREE_OPERATIONS,
                        )


def version_samples(
    participants: Sequence[Participant],
    sizes: Sequence[int],
    runs: int,
    densities: Sequence[int],
    localities: Sequence[str],
) -> Iterable[Sample]:
    scenarios = []
    if 0 in densities:
        scenarios.append((0, "none"))
    scenarios.extend(
        (density, locality)
        for density in densities
        if density != 0
        for locality in localities
    )
    for records in sizes:
        for repetition in range(1, runs + 1):
            for density, locality in scenarios:
                operations = VERSION_ZERO_OPERATIONS if density == 0 else VERSION_CHANGED_OPERATIONS
                order = rotate(participants, records + repetition + density + len(locality))
                for participant in order:
                    yield Sample(
                        "version",
                        participant,
                        repetition,
                        (
                            "--records",
                            str(records),
                            "--density",
                            str(density),
                            "--locality",
                            locality,
                        ),
                        (str(records), str(density), locality),
                        operations,
                    )


def lifecycle_samples(
    participants: Sequence[Participant],
    sizes: Sequence[int],
    runs: int,
    densities: Sequence[int],
    localities: Sequence[str],
) -> Iterable[Sample]:
    for records in sizes:
        for repetition in range(1, runs + 1):
            publish_scenarios = [
                ("publish", density, locality)
                for density in densities
                if density != 0
                for locality in localities
            ]
            other_scenarios = [
                (scenario, 0, "none") for scenario in ("read", "rollback", "prune")
            ]
            for scenario, density, locality in (*publish_scenarios, *other_scenarios):
                order = rotate(participants, records + repetition + density + len(scenario))
                for participant in order:
                    yield Sample(
                        "lifecycle",
                        participant,
                        repetition,
                        (
                            "--records",
                            str(records),
                            "--scenario",
                            scenario,
                            "--density",
                            str(density),
                            "--locality",
                            locality,
                        ),
                        (str(records), scenario, str(density), locality),
                        LIFECYCLE_OPERATIONS[scenario],
                    )


def validate_profile_pairs(
    rows: Sequence[dict[str, str]], profiles: Sequence[str], invariants: Sequence[str]
) -> None:
    groups: dict[tuple[str, ...], dict[str, dict[str, str]]] = {}
    domain_fields = (
        ("records", "phase", "workload", "operation", "repetition")
        if "phase" in rows[0]
        else ("records", "density", "locality", "operation", "relationship", "repetition")
    )
    for row in rows:
        key = tuple(row[field] for field in domain_fields)
        groups.setdefault(key, {})[row["cache_profile"]] = row
    expected = {"native", *profiles}
    for key, group in groups.items():
        if set(group) != expected:
            raise EvaluationError(f"incomplete profile group {key}: {sorted(group)}")
        go = group["native"]
        for profile in profiles:
            rust = group[profile]
            for field in invariants:
                if rust[field] != go[field]:
                    raise EvaluationError(
                        f"profile mismatch {key} profile={profile} field={field}: "
                        f"{rust[field]!r} != {go[field]!r}"
                    )
            if rust["operation"] != "patch_generate" and rust["result_count"] != go["result_count"]:
                raise EvaluationError(f"result_count mismatch {key} profile={profile}")


def validate_rust_profiles(
    rows: Sequence[dict[str, str]], profiles: Sequence[str], invariants: Sequence[str]
) -> None:
    groups: dict[tuple[str, ...], dict[str, dict[str, str]]] = {}
    for row in rows:
        key = (
            row["records"],
            row["density"],
            row["locality"],
            row["operation"],
            row["relationship"],
            row["repetition"],
        )
        groups.setdefault(key, {})[row["cache_profile"]] = row
    expected = set(profiles)
    for key, group in groups.items():
        if set(group) != expected:
            raise EvaluationError(f"incomplete Rust profile group {key}: {sorted(group)}")
        baseline = group[profiles[0]]
        for profile in profiles[1:]:
            candidate = group[profile]
            for field in invariants:
                if candidate[field] != baseline[field]:
                    raise EvaluationError(
                        f"Rust profile mismatch {key} profile={profile} field={field}: "
                        f"{candidate[field]!r} != {baseline[field]!r}"
                    )


def write_rows(path: Path, rows: Sequence[dict[str, str]], fieldnames: Sequence[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    with temporary.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)
    temporary.replace(path)


def execute_samples(
    samples: Iterable[Sample],
    output: Path,
    fingerprint: str,
    time_bin: Path,
    time_mode: str,
    resume: bool,
) -> list[dict[str, str]]:
    raw_dir = output / "raw"
    state_dir = output / "state"
    raw_dir.mkdir(parents=True, exist_ok=True)
    state_dir.mkdir(parents=True, exist_ok=True)
    normalized: list[dict[str, str]] = []
    for sample in samples:
        rows, peak_rss = run_sample(
            sample, raw_dir, state_dir, fingerprint, time_bin, time_mode, resume
        )
        for row in rows:
            normalized.append(
                {
                    **row,
                    "repetition": str(sample.repetition),
                    "peak_rss_bytes": str(peak_rss),
                    "cache_profile": sample.participant.profile,
                }
            )
    return normalized


def smoke_samples(
    tree_participants: Sequence[Participant],
    version_participants: Sequence[Participant],
) -> Iterable[Sample]:
    for participant in tree_participants:
        yield Sample(
            "tree",
            participant,
            1,
            ("--records", "10000", "--phase", "fresh", "--workload", "random"),
            ("smoke", "10000", "fresh", "random"),
            TREE_OPERATIONS,
        )
    for participant in version_participants:
        yield Sample(
            "version",
            participant,
            1,
            ("--records", "10000", "--density", "1", "--locality", "random"),
            ("smoke", "10000", "1", "random"),
            VERSION_CHANGED_OPERATIONS,
        )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--fingerprint", required=True)
    parser.add_argument("--rust-revision", required=True)
    parser.add_argument("--go-revision", required=True)
    parser.add_argument("--rust-tree", required=True, type=Path)
    parser.add_argument("--go-tree", required=True, type=Path)
    parser.add_argument("--rust-version", required=True, type=Path)
    parser.add_argument("--go-version", required=True, type=Path)
    parser.add_argument("--rust-lifecycle", required=True, type=Path)
    parser.add_argument("--sizes", default="10000 50000 1000000 5000000 10000000")
    parser.add_argument("--runs", type=int, default=3)
    parser.add_argument("--densities", default="0 1 30")
    parser.add_argument("--localities", default="append random clustered")
    parser.add_argument("--rust-cache-profiles", default="bounded unbounded")
    parser.add_argument("--lifecycle", choices=("0", "1"), default="1")
    parser.add_argument("--time-bin", type=Path, default=Path("/usr/bin/time"))
    parser.add_argument("--time-mode", choices=("-l", "-v"), default="-l")
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--skip-smoke", action="store_true")
    args = parser.parse_args()
    if args.runs <= 0:
        parser.error("--runs must be positive")
    sizes = parse_positive_list(args.sizes, "sizes")
    densities = tuple(int(value) for value in args.densities.replace(",", " ").split())
    if not densities or any(value not in (0, 1, 30) for value in densities):
        parser.error("--densities must contain 0, 1, and/or 30")
    if len(set(densities)) != len(densities):
        parser.error("--densities contains duplicates")
    localities = tuple(args.localities.replace(",", " ").split())
    if not localities or set(localities) - {"append", "random", "clustered"}:
        parser.error("--localities contains an unsupported value")
    if len(set(localities)) != len(localities):
        parser.error("--localities contains duplicates")
    profiles = parse_profiles(args.rust_cache_profiles)
    for binary in (
        args.rust_tree,
        args.go_tree,
        args.rust_version,
        args.go_version,
        args.rust_lifecycle,
        args.time_bin,
    ):
        if not binary.is_file():
            parser.error(f"executable does not exist: {binary}")

    rust_tree = tuple(
        Participant("rust", profile, args.rust_tree, args.rust_revision)
        for profile in profiles
    )
    rust_version = tuple(
        Participant("rust", profile, args.rust_version, args.rust_revision)
        for profile in profiles
    )
    rust_lifecycle = tuple(
        Participant("rust", profile, args.rust_lifecycle, args.rust_revision)
        for profile in profiles
    )
    tree_participants = (
        Participant("dolt-go", "native", args.go_tree, args.go_revision),
        *rust_tree,
    )
    version_participants = (
        Participant("dolt-go", "native", args.go_version, args.go_revision),
        *rust_version,
    )
    args.output.mkdir(parents=True, exist_ok=True)

    if not args.skip_smoke:
        smoke_output = args.output / "smoke"
        smoke_rows = execute_samples(
            smoke_samples(tree_participants, version_participants),
            smoke_output,
            args.fingerprint,
            args.time_bin,
            args.time_mode,
            args.resume,
        )
        tree_smoke = [row for row in smoke_rows if row["contract_version"] == "prolly-compare-v1"]
        version_smoke = [row for row in smoke_rows if row["contract_version"] == "prolly-version-compare-v3"]
        validate_profile_pairs(tree_smoke, profiles, TREE_INVARIANTS)
        validate_profile_pairs(version_smoke, profiles, VERSION_INVARIANTS)

    tree_output = args.output / "tree"
    tree_rows = execute_samples(
        tree_samples(tree_participants, sizes, args.runs),
        tree_output,
        args.fingerprint,
        args.time_bin,
        args.time_mode,
        args.resume,
    )
    validate_profile_pairs(tree_rows, profiles, TREE_INVARIANTS)
    tree_fields = [*TREE_HEADER.split(","), "repetition", "peak_rss_bytes", "cache_profile"]
    write_rows(tree_output / "results.csv", tree_rows, tree_fields)

    version_output = args.output / "version"
    version_rows = execute_samples(
        version_samples(version_participants, sizes, args.runs, densities, localities),
        version_output,
        args.fingerprint,
        args.time_bin,
        args.time_mode,
        args.resume,
    )
    validate_profile_pairs(version_rows, profiles, VERSION_INVARIANTS)
    version_fields = [*VERSION_HEADER.split(","), "repetition", "peak_rss_bytes", "cache_profile"]
    write_rows(version_output / "results-common.csv", version_rows, version_fields)

    lifecycle_rows: list[dict[str, str]] = []
    if args.lifecycle == "1":
        lifecycle_rows = execute_samples(
            lifecycle_samples(rust_lifecycle, sizes, args.runs, densities, localities),
            args.output / "lifecycle",
            args.fingerprint,
            args.time_bin,
            args.time_mode,
            args.resume,
        )
        validate_rust_profiles(lifecycle_rows, profiles, LIFECYCLE_INVARIANTS)
    write_rows(
        args.output / "lifecycle" / "results.csv",
        lifecycle_rows,
        version_fields,
    )
    print(args.output)


if __name__ == "__main__":
    try:
        main()
    except EvaluationError as error:
        raise SystemExit(f"evaluation failed: {error}") from error
