#!/usr/bin/env python3
"""Run the versioned, resumable TurboQuant qualification matrix."""

from __future__ import annotations

import argparse
import csv
import hashlib
import itertools
import json
import math
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Iterable, Sequence


CONTRACT_SCHEMA = "prolly-turboquant-qualification-v2"
BENCH_SCHEMA_VERSION = 2
FULL_RECORDS = (1_000, 10_000, 100_000, 1_000_000)
FULL_DIMENSIONS = (128, 200, 768, 1_536, 3_072)
FULL_METRICS = ("l2", "cosine", "inner_product")
FULL_K = (1, 10, 100)
FULL_ELIGIBILITY_PPM = (1_000_000, 100_000, 10_000, 1_000)
FULL_BITS = (2, 3, 4)
FULL_RERANK = (4, 8, 16)
DEFAULT_WORKERS = (1, 2, 4)


class QualificationError(RuntimeError):
    """A qualification contract or evidence check failed."""


@dataclass(frozen=True)
class Environment:
    name: str
    store: str
    cold: bool
    async_quantizers: bool


ENVIRONMENTS = (
    Environment("memory-warm-sync", "memory", False, False),
    Environment("memory-cold-sync", "memory", True, False),
    Environment("file-warm-sync", "file", False, False),
    Environment("file-cold-sync", "file", True, False),
    Environment("memory-warm-async", "memory", False, True),
    Environment("memory-cold-async", "memory", True, True),
)


@dataclass(frozen=True)
class Cell:
    records: int
    dimensions: int
    metric: str
    k: int
    eligibility_ppm: int
    bits: int
    rerank: str
    rerank_multiplier: int
    environment: Environment

    @property
    def identifier(self) -> str:
        eligibility = {
            1_000_000: "all",
            100_000: "10pct",
            10_000: "1pct",
            1_000: "0p1pct",
        }.get(self.eligibility_ppm, f"ppm{self.eligibility_ppm}")
        return (
            f"n{self.records}-d{self.dimensions}-{self.metric}-k{self.k}-"
            f"e{eligibility}-b{self.bits}-r{self.rerank_multiplier}-{self.rerank}-"
            f"{self.environment.name}"
        )


@dataclass(frozen=True)
class ParsedOutput:
    preamble: dict[str, str]
    rows: dict[str, list[list[str]]]


def _rerank_cases(records: int, k: int) -> Iterable[tuple[str, int]]:
    yield from (("fixed", multiplier) for multiplier in FULL_RERANK)
    if records <= 10_000:
        exhaustive = math.ceil(records / k)
        if exhaustive not in FULL_RERANK:
            yield "exhaustive", exhaustive


def enumerate_cells(profile: str) -> list[Cell]:
    if profile == "smoke":
        return [
            Cell(32, 24, "l2", 10, 1_000_000, 4, "fixed", 8, environment)
            for environment in ENVIRONMENTS
        ]
    if profile != "full":
        raise QualificationError(f"unsupported profile: {profile}")
    cells = []
    for records, dimensions, metric, k, eligibility, bits in itertools.product(
        FULL_RECORDS,
        FULL_DIMENSIONS,
        FULL_METRICS,
        FULL_K,
        FULL_ELIGIBILITY_PPM,
        FULL_BITS,
    ):
        for rerank, multiplier in _rerank_cases(records, k):
            for environment in ENVIRONMENTS:
                cells.append(
                    Cell(
                        records,
                        dimensions,
                        metric,
                        k,
                        eligibility,
                        bits,
                        rerank,
                        multiplier,
                        environment,
                    )
                )
    return cells


def shard_cells(cells: Sequence[Cell], index: int, count: int) -> list[Cell]:
    if count <= 0:
        raise QualificationError("shard count must be positive")
    if not 0 <= index < count:
        raise QualificationError("shard index must be in [0, shard count)")
    selected = []
    for cell in cells:
        digest = hashlib.sha256(cell.identifier.encode()).digest()
        if int.from_bytes(digest[:8], "big") % count == index:
            selected.append(cell)
    return selected


def matrix_digest(cells: Sequence[Cell]) -> str:
    payload = "\n".join(cell.identifier for cell in cells).encode()
    return hashlib.sha256(payload).hexdigest()


def _run_text(arguments: Sequence[str], cwd: Path) -> str:
    result = subprocess.run(arguments, cwd=cwd, check=True, text=True, capture_output=True)
    return result.stdout.strip()


def current_revision(repo: Path) -> str:
    return _run_text(("git", "rev-parse", "HEAD"), repo)


def tracked_worktree_is_dirty(repo: Path) -> bool:
    unstaged = subprocess.run(("git", "diff", "--quiet"), cwd=repo, check=False)
    staged = subprocess.run(("git", "diff", "--cached", "--quiet"), cwd=repo, check=False)
    return unstaged.returncode != 0 or staged.returncode != 0


def make_contract(
    profile: str,
    revision: str,
    workers: Sequence[int],
    repeats: int,
    shard_index: int,
    shard_count: int,
    all_cells: Sequence[Cell],
    selected: Sequence[Cell],
    max_cell_records: int | None = None,
) -> dict[str, object]:
    return {
        "schema": CONTRACT_SCHEMA,
        "benchmark_schema_version": BENCH_SCHEMA_VERSION,
        "profile": profile,
        "revision": revision,
        "workers": list(workers),
        "search_repeats": repeats,
        "shard_index": shard_index,
        "shard_count": shard_count,
        "full_cell_count": len(all_cells),
        "full_matrix_digest": matrix_digest(all_cells),
        "shard_cell_count": len(selected),
        "shard_matrix_digest": matrix_digest(selected),
        "environments": [asdict(environment) for environment in ENVIRONMENTS],
        "exhaustive_max_records": 10_000,
        "wasm_smoke": shard_index == 0,
        "max_cell_records": max_cell_records,
    }


def expected_scalability_failure(
    cell: Cell, max_cell_records: int | None
) -> dict[str, object] | None:
    if max_cell_records is None or cell.records <= max_cell_records:
        return None
    return {
        "kind": "ProximityResourceLimitExceeded",
        "resource": "TurboQuant records",
        "limit": max_cell_records,
        "actual": cell.records,
        "phase": "qualification preflight",
    }


def atomic_write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", dir=path.parent, delete=False
    ) as temporary:
        temporary.write(content)
        temporary.flush()
        os.fsync(temporary.fileno())
        temporary_path = Path(temporary.name)
    os.replace(temporary_path, path)


def write_json(path: Path, value: object) -> None:
    atomic_write(path, json.dumps(value, indent=2, sort_keys=True) + "\n")


def prepare_output(output: Path, contract: dict[str, object], resume: bool) -> None:
    manifest_path = output / "manifest.json"
    if resume:
        if not manifest_path.is_file():
            raise QualificationError("resume requires an existing manifest.json")
        try:
            existing = json.loads(manifest_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as error:
            raise QualificationError(f"cannot read resume manifest: {error}") from error
        if existing.get("contract") != contract:
            raise QualificationError("resume manifest does not match this exact matrix contract")
        return
    if output.exists() and any(output.iterdir()):
        raise QualificationError(f"refusing to overwrite non-empty output: {output}")
    output.mkdir(parents=True, exist_ok=True)
    write_json(
        manifest_path,
        {
            "contract": contract,
            "created_at": datetime.now(timezone.utc).isoformat(),
        },
    )


def _required_operation_counts(async_quantizers: bool, workers: Sequence[int]) -> dict[str, int]:
    counts = {
        "source_build": 1,
        "source_closure_bytes": 1,
        "turboquant_build": len(workers),
        "turboquant_build_resources": len(workers),
        "turboquant_sidecar_bytes": 1,
        "turboquant_manifest_code_bytes": 1,
        "turboquant_recall": 1,
        "pq_build": len(workers),
        "pq_sidecar_bytes": 1,
        "pq_manifest_code_bytes": 1,
        "pq_recall": 1,
    }
    for name in ("turboquant_search_scalar", "turboquant_search_simd", "turboquant_search_auto"):
        for suffix in ("", "_p95", "_p99", "_work", "_io", "_rerank"):
            counts[name + suffix] = 1
    for suffix in ("", "_p95", "_p99", "_work", "_io", "_rerank"):
        counts["pq_search" + suffix] = 1
    if async_quantizers:
        counts.update(
            {
                "turboquant_build_async": len(workers),
                "turboquant_build_async_resources": len(workers),
                "turboquant_build_async_publication": len(workers),
                "turboquant_recall_async": 1,
                "pq_build_async": len(workers),
                "pq_build_async_publication": len(workers),
                "pq_recall_async": 1,
            }
        )
        for name in ("turboquant_search_async", "pq_search_async"):
            for suffix in ("", "_p95", "_p99", "_work", "_io", "_rerank"):
                counts[name + suffix] = 1
    return counts


def _expected_preamble(cell: Cell, revision: str, repeats: int) -> dict[str, str]:
    metric = {"l2": "L2Squared", "cosine": "Cosine", "inner_product": "InnerProduct"}[
        cell.metric
    ]
    eligible = max(1, math.ceil(cell.records * cell.eligibility_ppm / 1_000_000))
    return {
        "schema_version": str(BENCH_SCHEMA_VERSION),
        "revision": revision,
        "store": cell.environment.store,
        "records": str(cell.records),
        "profile": "quantizers",
        "search_repeats": str(repeats),
        "metric": metric,
        "k": str(cell.k),
        "eligibility_ppm": str(cell.eligibility_ppm),
        "effective_k": str(min(cell.k, eligible)),
        "turboquant_bits": str(cell.bits),
        "rerank_multiplier": str(cell.rerank_multiplier),
        "search_cache": "reset" if cell.environment.cold else "warm",
        "async_quantizers": str(cell.environment.async_quantizers).lower(),
    }


def validate_output(
    output: str, cell: Cell, revision: str, workers: Sequence[int], repeats: int
) -> ParsedOutput:
    lines = output.splitlines()
    try:
        header_index = lines.index("operation,dimensions,threads,micros,metric_a,metric_b")
    except ValueError as error:
        raise QualificationError(f"{cell.identifier}: missing CSV header") from error
    preamble: dict[str, str] = {}
    for line in lines[:header_index]:
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        if key in preamble:
            raise QualificationError(f"{cell.identifier}: duplicate preamble key {key}")
        preamble[key] = value
    for key, expected in _expected_preamble(cell, revision, repeats).items():
        if preamble.get(key) != expected:
            raise QualificationError(
                f"{cell.identifier}: preamble {key}={preamble.get(key)!r}, expected {expected!r}"
            )
    for key in ("compiler", "target_arch", "target_os", "machine", "seed"):
        if not preamble.get(key):
            raise QualificationError(f"{cell.identifier}: missing preamble key {key}")

    rows: dict[str, list[list[str]]] = {}
    reader = csv.reader(lines[header_index + 1 :])
    for row in reader:
        if len(row) != 6:
            raise QualificationError(f"{cell.identifier}: malformed CSV row {row!r}")
        operation, dimensions, threads, micros, metric_a, metric_b = row
        try:
            parsed = [int(dimensions), int(threads), float(micros), float(metric_a), float(metric_b)]
        except ValueError as error:
            raise QualificationError(f"{cell.identifier}: non-numeric CSV row {row!r}") from error
        if (
            parsed[0] != cell.dimensions
            or not all(math.isfinite(value) for value in parsed[2:])
            or any(value < 0 for value in parsed[2:])
        ):
            raise QualificationError(f"{cell.identifier}: invalid CSV row {row!r}")
        rows.setdefault(operation, []).append(row)

    required = _required_operation_counts(cell.environment.async_quantizers, workers)
    if set(rows) != set(required):
        missing = sorted(set(required) - set(rows))
        unexpected = sorted(set(rows) - set(required))
        raise QualificationError(
            f"{cell.identifier}: operation set mismatch missing={missing} unexpected={unexpected}"
        )
    for operation, expected_count in required.items():
        if len(rows[operation]) != expected_count:
            raise QualificationError(
                f"{cell.identifier}: {operation} count={len(rows[operation])}, expected={expected_count}"
            )
    for operation in (
        "turboquant_build",
        "turboquant_build_resources",
        "pq_build",
        "turboquant_build_async",
        "turboquant_build_async_resources",
        "turboquant_build_async_publication",
        "pq_build_async",
        "pq_build_async_publication",
    ):
        if operation in rows:
            observed = sorted(int(row[2]) for row in rows[operation])
            if observed != sorted(workers):
                raise QualificationError(
                    f"{cell.identifier}: {operation} workers={observed}, expected={sorted(workers)}"
                )
    for operation in ("turboquant_recall", "pq_recall", "turboquant_recall_async", "pq_recall_async"):
        if operation in rows and not 0.0 <= float(rows[operation][0][4]) <= 1.0:
            raise QualificationError(f"{cell.identifier}: invalid recall in {operation}")

    physical_rows = [
        "turboquant_search_scalar_p95",
        "pq_search_p95",
        "turboquant_search_scalar_io",
        "pq_search_io",
    ]
    if cell.environment.async_quantizers:
        physical_rows.extend(
            [
                "turboquant_search_async_p95",
                "pq_search_async_p95",
                "turboquant_search_async_io",
                "pq_search_async_io",
            ]
        )
    for operation in physical_rows:
        physical = float(rows[operation][0][5])
        if cell.environment.cold and physical <= 0:
            raise QualificationError(f"{cell.identifier}: cold {operation} has no physical I/O")
        # A warm cell proves that every measured sample shares the runtime
        # primed by its untimed warmup. It does not promise that the complete
        # working set fits the bounded cache. Sequential scans larger than a
        # cache partition can legitimately perform physical reads on every
        # sample; requiring zero here made the 100K/1M matrix impossible to
        # record under the production cache limits.

    if cell.environment.async_quantizers:
        for sync_name, async_name in (
            ("turboquant_search_scalar", "turboquant_search_async"),
            ("pq_search", "pq_search_async"),
        ):
            for suffix in ("", "_p99", "_work", "_rerank"):
                if rows[sync_name + suffix][0][4:] != rows[async_name + suffix][0][4:]:
                    raise QualificationError(
                        f"{cell.identifier}: sync/async logical mismatch for {sync_name + suffix}"
                    )
            for suffix in ("_p95", "_io"):
                if rows[sync_name + suffix][0][4] != rows[async_name + suffix][0][4]:
                    raise QualificationError(
                        f"{cell.identifier}: sync/async logical mismatch for {sync_name + suffix}"
                    )
    return ParsedOutput(preamble, rows)


def cell_environment(cell: Cell, output: Path, workers: Sequence[int], repeats: int) -> dict[str, str]:
    environment = {
        key: value for key, value in os.environ.items() if not key.startswith("PROLLY_PROXIMITY_BENCH_")
    }
    environment.update(
        {
            "CARGO_INCREMENTAL": "0",
            "PROLLY_PROXIMITY_BENCH_RECORDS": str(cell.records),
            "PROLLY_PROXIMITY_BENCH_DIMENSIONS": str(cell.dimensions),
            "PROLLY_PROXIMITY_BENCH_THREADS": ",".join(str(worker) for worker in workers),
            "PROLLY_PROXIMITY_BENCH_SEARCH_REPEATS": str(repeats),
            "PROLLY_PROXIMITY_BENCH_QUANTIZERS_ONLY": "1",
            "PROLLY_PROXIMITY_BENCH_METRIC": cell.metric,
            "PROLLY_PROXIMITY_BENCH_K": str(cell.k),
            "PROLLY_PROXIMITY_BENCH_ELIGIBILITY_PPM": str(cell.eligibility_ppm),
            "PROLLY_PROXIMITY_BENCH_TURBOQUANT_BITS": str(cell.bits),
            "PROLLY_PROXIMITY_BENCH_RERANK_MULTIPLIER": str(cell.rerank_multiplier),
            "PROLLY_PROXIMITY_BENCH_STORE": cell.environment.store,
        }
    )
    if cell.environment.cold:
        environment["PROLLY_PROXIMITY_BENCH_RESET_SEARCH_CACHE"] = "1"
    if cell.environment.async_quantizers:
        environment["PROLLY_PROXIMITY_BENCH_ASYNC_QUANTIZERS"] = "1"
    if cell.environment.store == "file":
        store_path = output / "stores"
        store_path.mkdir(parents=True, exist_ok=True)
        environment["PROLLY_PROXIMITY_BENCH_STORE_PATH"] = str(store_path.resolve())
    return environment


def completed_cell_is_valid(
    output: Path,
    cell: Cell,
    revision: str,
    workers: Sequence[int],
    repeats: int,
    max_cell_records: int | None = None,
) -> bool:
    state_path = output / "state" / f"{cell.identifier}.json"
    raw_path = output / "raw" / f"{cell.identifier}.csv"
    if not state_path.exists():
        return False
    try:
        state = json.loads(state_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise QualificationError(f"cannot validate resume state for {cell.identifier}: {error}") from error
    if state.get("cell") != asdict(cell):
        raise QualificationError(f"resume cell contract mismatch for {cell.identifier}")
    expected_failure = expected_scalability_failure(cell, max_cell_records)
    if expected_failure is not None:
        if state.get("disposition") != "typed_scalability_failure":
            raise QualificationError(
                f"resume disposition mismatch for limited cell {cell.identifier}"
            )
        if state.get("failure") != expected_failure:
            raise QualificationError(
                f"resume scalability failure mismatch for {cell.identifier}"
            )
        if raw_path.exists():
            raise QualificationError(
                f"limited cell has unexpected raw benchmark output for {cell.identifier}"
            )
        return True
    if not raw_path.is_file():
        raise QualificationError(f"resume state exists without raw output for {cell.identifier}")
    try:
        raw = raw_path.read_text(encoding="utf-8")
    except OSError as error:
        raise QualificationError(f"cannot read resume output for {cell.identifier}: {error}") from error
    if state.get("disposition") != "completed":
        raise QualificationError(f"resume disposition mismatch for {cell.identifier}")
    if state.get("sha256") != hashlib.sha256(raw.encode()).hexdigest():
        raise QualificationError(f"resume output digest mismatch for {cell.identifier}")
    validate_output(raw, cell, revision, workers, repeats)
    return True


def run_cell(
    repo: Path,
    output: Path,
    cell: Cell,
    revision: str,
    workers: Sequence[int],
    repeats: int,
) -> None:
    command = ("cargo", "bench", "--quiet", "--all-features", "--bench", "prolly_proximity_bench")
    environment = cell_environment(cell, output, workers, repeats)
    result = subprocess.run(command, cwd=repo, env=environment, text=True, capture_output=True)
    raw_path = output / "raw" / f"{cell.identifier}.csv"
    stderr_path = output / "stderr" / f"{cell.identifier}.log"
    atomic_write(raw_path, result.stdout)
    atomic_write(stderr_path, result.stderr)
    if result.returncode != 0:
        raise QualificationError(
            f"{cell.identifier}: benchmark exited {result.returncode}; see {stderr_path}"
        )
    validate_output(result.stdout, cell, revision, workers, repeats)
    write_json(
        output / "state" / f"{cell.identifier}.json",
        {
            "cell": asdict(cell),
            "command": list(command),
            "completed_at": datetime.now(timezone.utc).isoformat(),
            "disposition": "completed",
            "sha256": hashlib.sha256(result.stdout.encode()).hexdigest(),
        },
    )


def record_scalability_failure(
    output: Path, cell: Cell, max_cell_records: int
) -> None:
    failure = expected_scalability_failure(cell, max_cell_records)
    if failure is None:
        raise QualificationError(
            f"{cell.identifier}: scalability failure requested for an in-limit cell"
        )
    write_json(
        output / "state" / f"{cell.identifier}.json",
        {
            "cell": asdict(cell),
            "completed_at": datetime.now(timezone.utc).isoformat(),
            "disposition": "typed_scalability_failure",
            "failure": failure,
        },
    )


def wasm_smoke_is_valid(output: Path, revision: str) -> bool:
    state_path = output / "wasm" / "status.json"
    build_path = output / "wasm" / "build.log"
    test_path = output / "wasm" / "test.log"
    if not state_path.exists():
        return False
    if not build_path.is_file() or not test_path.is_file():
        raise QualificationError("WASM resume state exists without build/test logs")
    try:
        state = json.loads(state_path.read_text(encoding="utf-8"))
        build_log = build_path.read_text(encoding="utf-8")
        test_log = test_path.read_text(encoding="utf-8")
    except (OSError, json.JSONDecodeError) as error:
        raise QualificationError(f"cannot validate WASM resume state: {error}") from error
    if state.get("revision") != revision:
        raise QualificationError("WASM resume revision mismatch")
    if state.get("commands") != [["npm", "run", "build"], ["npm", "test"]]:
        raise QualificationError("WASM resume command contract mismatch")
    if state.get("build_sha256") != hashlib.sha256(build_log.encode()).hexdigest():
        raise QualificationError("WASM resume build digest mismatch")
    if state.get("test_sha256") != hashlib.sha256(test_log.encode()).hexdigest():
        raise QualificationError("WASM resume test digest mismatch")
    if not _tap_summary_is_zero(test_log, "fail") or not _tap_summary_is_zero(
        test_log, "skipped"
    ):
        raise QualificationError("WASM resume test log is not an unskipped passing suite")
    return True


def _tap_summary_is_zero(output: str, field: str) -> bool:
    return re.search(rf"^(?:#|ℹ)\s*{re.escape(field)}\s+0\s*$", output, re.MULTILINE) is not None


def run_wasm_smoke(repo: Path, output: Path, revision: str) -> None:
    wasm_dir = repo / "bindings" / "wasm"
    logs = output / "wasm"
    commands = (
        ("npm", "run", "build"),
        ("npm", "test"),
    )
    names = ("build.log", "test.log")
    results = []
    for command, name in zip(commands, names, strict=True):
        result = subprocess.run(command, cwd=wasm_dir, text=True, capture_output=True)
        combined = result.stdout + result.stderr
        atomic_write(logs / name, combined)
        results.append((result, combined))
        if result.returncode != 0:
            raise QualificationError(f"WASM smoke failed in {' '.join(command)}; see {logs / name}")
    test_log = results[1][1]
    if not _tap_summary_is_zero(test_log, "fail") or not _tap_summary_is_zero(
        test_log, "skipped"
    ):
        raise QualificationError("WASM smoke did not report an unskipped passing suite")
    for expected in (
        "WASM TurboQuant lifecycle is portable, verified, and cancellable",
        "WASM TurboQuant build matches the checked-in Rust wire fixture",
    ):
        if expected not in test_log:
            raise QualificationError(f"WASM smoke is missing required test: {expected}")
    write_json(
        logs / "status.json",
        {
            "build_sha256": hashlib.sha256(results[0][1].encode()).hexdigest(),
            "commands": [list(command) for command in commands],
            "completed_at": datetime.now(timezone.utc).isoformat(),
            "revision": revision,
            "test_sha256": hashlib.sha256(test_log.encode()).hexdigest(),
        },
    )


def parse_workers(value: str) -> tuple[int, ...]:
    try:
        workers = tuple(int(item) for item in value.split(","))
    except ValueError as error:
        raise argparse.ArgumentTypeError("workers must be comma-separated integers") from error
    if not workers or any(worker <= 0 for worker in workers) or len(set(workers)) != len(workers):
        raise argparse.ArgumentTypeError("workers must be unique positive integers")
    return workers


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", choices=("smoke", "full"), required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--workers", type=parse_workers, default=DEFAULT_WORKERS)
    parser.add_argument("--search-repeats", type=int)
    parser.add_argument("--shard-index", type=int, default=0)
    parser.add_argument("--shard-count", type=int, default=1)
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--keep-going", action="store_true")
    parser.add_argument("--allow-dirty", action="store_true", help="smoke profile only")
    parser.add_argument(
        "--max-cell-records",
        type=int,
        help=(
            "full profile only: record a typed scalability disposition instead of "
            "running 1M cells above this explicit host limit (minimum 100000)"
        ),
    )
    args = parser.parse_args(argv)

    if args.search_repeats is None:
        args.search_repeats = 2 if args.profile == "smoke" else 30
    if args.search_repeats <= 0:
        parser.error("search repeats must be positive")
    if args.allow_dirty and args.profile != "smoke":
        parser.error("--allow-dirty is only valid for smoke runs")
    if args.max_cell_records is not None:
        if args.profile != "full":
            parser.error("--max-cell-records is only valid for full runs")
        if not 100_000 <= args.max_cell_records < 1_000_000:
            parser.error("--max-cell-records must be in 100000..1000000")

    repo = Path(__file__).resolve().parents[1]
    revision = current_revision(repo)
    if tracked_worktree_is_dirty(repo) and not args.allow_dirty:
        raise QualificationError("tracked worktree is dirty; qualification evidence requires a commit")
    all_cells = enumerate_cells(args.profile)
    selected = shard_cells(all_cells, args.shard_index, args.shard_count)
    contract = make_contract(
        args.profile,
        revision,
        args.workers,
        args.search_repeats,
        args.shard_index,
        args.shard_count,
        all_cells,
        selected,
        args.max_cell_records,
    )
    if args.dry_run:
        print(json.dumps(contract, indent=2, sort_keys=True))
        for cell in selected:
            print(cell.identifier)
        return 0

    output = args.output.resolve()
    prepare_output(output, contract, args.resume)
    compile_result = subprocess.run(
        ("cargo", "bench", "--all-features", "--bench", "prolly_proximity_bench", "--no-run"),
        cwd=repo,
        env={**os.environ, "CARGO_INCREMENTAL": "0"},
        text=True,
        capture_output=True,
    )
    atomic_write(output / "compile.log", compile_result.stdout + compile_result.stderr)
    if compile_result.returncode != 0:
        raise QualificationError(f"benchmark compilation failed; see {output / 'compile.log'}")

    wasm_smoke = args.shard_index == 0
    if wasm_smoke:
        if args.resume and wasm_smoke_is_valid(output, revision):
            print("resume WASM TurboQuant smoke", flush=True)
        else:
            print("run WASM TurboQuant smoke", flush=True)
            run_wasm_smoke(repo, output, revision)

    completed = 0
    scalability_failures = 0
    failed: list[str] = []
    for ordinal, cell in enumerate(selected, start=1):
        if args.resume and completed_cell_is_valid(
            output,
            cell,
            revision,
            args.workers,
            args.search_repeats,
            args.max_cell_records,
        ):
            completed += 1
            if expected_scalability_failure(cell, args.max_cell_records) is not None:
                scalability_failures += 1
                disposition = "typed scalability failure"
            else:
                disposition = "benchmark"
            print(
                f"[{ordinal}/{len(selected)}] resume {disposition} {cell.identifier}",
                flush=True,
            )
            continue
        expected_failure = expected_scalability_failure(cell, args.max_cell_records)
        if expected_failure is not None:
            record_scalability_failure(output, cell, args.max_cell_records)
            completed += 1
            scalability_failures += 1
            print(
                f"[{ordinal}/{len(selected)}] typed scalability failure "
                f"{cell.identifier}: {expected_failure['resource']} "
                f"actual={expected_failure['actual']} limit={expected_failure['limit']}",
                flush=True,
            )
            continue
        print(f"[{ordinal}/{len(selected)}] run {cell.identifier}", flush=True)
        try:
            run_cell(repo, output, cell, revision, args.workers, args.search_repeats)
            completed += 1
        except QualificationError as error:
            failed.append(str(error))
            print(error, file=sys.stderr, flush=True)
            if not args.keep_going:
                break

    status = {
        "complete": completed == len(selected) and not failed,
        "completed_cells": completed,
        "expected_cells": len(selected),
        "failed": failed,
        "finished_at": datetime.now(timezone.utc).isoformat(),
        "revision": revision,
        "schema": CONTRACT_SCHEMA,
        "typed_scalability_failures": scalability_failures,
        "wasm_smoke": wasm_smoke,
    }
    write_json(output / "status.json", status)
    if not status["complete"]:
        return 1
    print(f"qualification shard complete: {output}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except QualificationError as error:
        print(f"error: {error}", file=sys.stderr)
        raise SystemExit(2) from error
