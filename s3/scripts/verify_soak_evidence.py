#!/usr/bin/env python3
"""Independently verify the structured records from a RustFS soak run."""

from __future__ import annotations

import argparse
import pathlib
import re
import shlex
from collections import defaultdict


SCHEMA = "prolly-s3-soak/v2"
EXPECTED_TESTS = {"ref-contention", "multipart-recovery"}


def fail(message: str) -> "NoReturn":
    raise SystemExit(f"invalid soak evidence: {message}")


def records(path: pathlib.Path, kind: str) -> list[dict[str, str]]:
    prefix = f"{kind} "
    found: list[dict[str, str]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.startswith(prefix):
            continue
        parsed: dict[str, str] = {}
        for item in shlex.split(line[len(prefix) :]):
            key, separator, value = item.partition("=")
            if not separator or not key or not value:
                fail(f"malformed {kind} field: {item!r}")
            if key in parsed:
                fail(f"duplicate {kind} field: {key}")
            parsed[key] = value
        found.append(parsed)
    return found


def one(items: list[dict[str, str]], kind: str) -> dict[str, str]:
    if len(items) != 1:
        fail(f"expected exactly one {kind} record, found {len(items)}")
    return items[0]


def integer(record: dict[str, str], key: str, *, minimum: int = 0) -> int:
    try:
        value = int(record[key])
    except KeyError:
        fail(f"missing field {key}")
    except ValueError:
        fail(f"field {key} is not an integer")
    if value < minimum:
        fail(f"field {key} must be at least {minimum}, found {value}")
    return value


def signed_integer(record: dict[str, str], key: str) -> int:
    try:
        return int(record[key])
    except KeyError:
        fail(f"missing field {key}")
    except ValueError:
        fail(f"field {key} is not an integer")


def require(record: dict[str, str], key: str, expected: str) -> None:
    actual = record.get(key)
    if actual != expected:
        fail(f"field {key} expected {expected!r}, found {actual!r}")


def verify(path: pathlib.Path, minimum_seconds: int) -> str:
    if not path.is_file() or path.is_symlink():
        fail(f"test log is not a regular file: {path}")
    start = one(records(path, "SOAK_START"), "SOAK_START")
    complete = one(records(path, "SOAK_COMPLETE"), "SOAK_COMPLETE")
    iterations = records(path, "SOAK_ITERATION")
    tests = records(path, "SOAK_TEST")
    workflows = records(path, "SOAK_WORKFLOW")
    cleanups = records(path, "SOAK_CLEANUP")
    if records(path, "SOAK_INVARIANT_FAILED"):
        fail("log contains SOAK_INVARIANT_FAILED")

    require(start, "schema", SCHEMA)
    require(complete, "schema", SCHEMA)
    run_id = start.get("run_id")
    if not run_id:
        fail("SOAK_START has no run_id")
    require(complete, "run_id", run_id)
    require(start, "health", "healthy")
    require(complete, "health", "healthy")
    for key in (
        "container",
        "image",
        "mount",
        "source_revision",
        "source_state",
        "cargo_version",
        "rustc_version",
    ):
        if not start.get(key):
            fail(f"SOAK_START has no {key}")
    if not re.fullmatch(r"[0-9a-f]{64}", start.get("test_binary_sha256", "")):
        fail("SOAK_START has an invalid test_binary_sha256")

    declared_duration = integer(start, "duration_seconds", minimum=1)
    integer(start, "iteration_interval_seconds", minimum=1)
    elapsed_seconds = integer(complete, "elapsed_seconds", minimum=1)
    if declared_duration < minimum_seconds:
        fail(
            f"declared duration {declared_duration} is below required {minimum_seconds}"
        )
    if elapsed_seconds < declared_duration or elapsed_seconds < minimum_seconds:
        fail(
            f"elapsed duration {elapsed_seconds} does not satisfy declared/required duration"
        )
    start_epoch = integer(start, "epoch", minimum=1)
    complete_epoch = integer(complete, "epoch", minimum=start_epoch)
    if complete_epoch - start_epoch != elapsed_seconds:
        fail("SOAK_COMPLETE elapsed_seconds does not match its epoch interval")

    expected_count = integer(complete, "iterations", minimum=1)
    if len(iterations) != expected_count:
        fail(f"expected {expected_count} iteration records, found {len(iterations)}")
    if integer(complete, "test_runs") != expected_count * len(EXPECTED_TESTS):
        fail("SOAK_COMPLETE test_runs does not equal two tests per iteration")

    initial_data = integer(start, "initial_data_kib")
    initial_build = integer(start, "initial_build_kib")
    restart_count = integer(start, "restart_count")
    memory_limit = integer(start, "max_rustfs_memory_bytes", minimum=1)
    data_limit = integer(start, "max_data_growth_kib_per_iteration", minimum=1)
    total_data_limit = integer(start, "max_total_data_growth_kib", minimum=1)
    repository_limit = integer(start, "max_repository_bytes_per_workflow", minimum=1)
    build_limit = integer(start, "max_build_growth_kib")
    max_memory = 0
    max_iteration_millis = 0
    max_workflow_storage = 0

    tests_by_iteration: dict[int, set[str]] = defaultdict(set)
    for test in tests:
        require(test, "run_id", run_id)
        require(test, "status", "passed")
        ordinal = integer(test, "iteration", minimum=1)
        name = test.get("name")
        if name not in EXPECTED_TESTS:
            fail(f"unexpected soak test name: {name!r}")
        if name in tests_by_iteration[ordinal]:
            fail(f"duplicate test {name} in iteration {ordinal}")
        tests_by_iteration[ordinal].add(name)
        integer(test, "elapsed_millis", minimum=1)

    workflows_by_iteration: dict[int, set[str]] = defaultdict(set)
    for workflow in workflows:
        require(workflow, "run_id", run_id)
        require(workflow, "final_fsck", "ok")
        ordinal = integer(workflow, "iteration", minimum=1)
        name = workflow.get("name")
        if name not in EXPECTED_TESTS:
            fail(f"unexpected soak workflow name: {name!r}")
        if name in workflows_by_iteration[ordinal]:
            fail(f"duplicate workflow {name} in iteration {ordinal}")
        workflows_by_iteration[ordinal].add(name)
        storage = integer(workflow, "physical_storage_bytes", minimum=1)
        if storage > repository_limit:
            fail(f"workflow {name} in iteration {ordinal} exceeded its storage limit")
        max_workflow_storage = max(max_workflow_storage, storage)

    cleanups_by_iteration: dict[int, set[str]] = defaultdict(set)
    for cleanup in cleanups:
        require(cleanup, "run_id", run_id)
        require(cleanup, "remaining_versions", "0")
        ordinal = integer(cleanup, "iteration", minimum=1)
        name = cleanup.get("name")
        if name not in EXPECTED_TESTS:
            fail(f"unexpected soak cleanup name: {name!r}")
        if name in cleanups_by_iteration[ordinal]:
            fail(f"duplicate cleanup {name} in iteration {ordinal}")
        cleanups_by_iteration[ordinal].add(name)
        integer(cleanup, "deleted_versions", minimum=1)

    for expected_ordinal, record in enumerate(iterations, start=1):
        require(record, "run_id", run_id)
        ordinal = integer(record, "iteration", minimum=1)
        if ordinal != expected_ordinal:
            fail(f"iteration sequence expected {expected_ordinal}, found {ordinal}")
        if tests_by_iteration[ordinal] != EXPECTED_TESTS:
            fail(f"iteration {ordinal} does not contain both required tests")
        if workflows_by_iteration[ordinal] != EXPECTED_TESTS:
            fail(f"iteration {ordinal} does not contain both workflow records")
        if cleanups_by_iteration[ordinal] != EXPECTED_TESTS:
            fail(f"iteration {ordinal} does not contain both cleanup records")
        require(record, "health", "healthy")
        if integer(record, "restart_count") != restart_count:
            fail(f"RustFS restart count changed in iteration {ordinal}")
        integer(record, "epoch", minimum=start_epoch)
        elapsed_millis = integer(record, "elapsed_millis", minimum=1)
        memory = integer(record, "rustfs_memory_bytes", minimum=1)
        if memory > memory_limit:
            fail(f"iteration {ordinal} exceeded the RustFS memory limit")
        max_memory = max(max_memory, memory)
        max_iteration_millis = max(max_iteration_millis, elapsed_millis)

    if set(tests_by_iteration) != set(range(1, expected_count + 1)):
        fail("test iteration set does not match iteration records")
    if set(workflows_by_iteration) != set(range(1, expected_count + 1)):
        fail("workflow iteration set does not match iteration records")
    if set(cleanups_by_iteration) != set(range(1, expected_count + 1)):
        fail("cleanup iteration set does not match iteration records")
    if integer(complete, "restart_count") != restart_count:
        fail("RustFS restart count changed by SOAK_COMPLETE")
    final_data = integer(complete, "final_data_kib")
    data_growth = signed_integer(complete, "data_growth_kib")
    if data_growth != final_data - initial_data:
        fail("SOAK_COMPLETE data_growth_kib is inconsistent")
    if data_growth > expected_count * data_limit:
        fail("SOAK_COMPLETE exceeded the total provider-data growth limit")
    if data_growth > total_data_limit:
        fail("SOAK_COMPLETE exceeded the absolute provider-data growth limit")
    final_build = integer(complete, "final_build_kib")
    build_growth = signed_integer(complete, "build_growth_kib")
    if build_growth != final_build - initial_build:
        fail("SOAK_COMPLETE build_growth_kib is inconsistent")
    if final_build - initial_build > build_limit:
        fail("SOAK_COMPLETE exceeded the build-growth limit")
    if integer(complete, "max_rustfs_memory_bytes_observed") != max_memory:
        fail("SOAK_COMPLETE maximum RustFS memory is inconsistent")
    if integer(complete, "max_iteration_millis") != max_iteration_millis:
        fail("SOAK_COMPLETE maximum iteration latency is inconsistent")

    return (
        "SOAK_EVIDENCE_VERIFIED "
        f"run_id={run_id} elapsed_seconds={elapsed_seconds} "
        f"iterations={expected_count} test_runs={len(tests)} "
        f"data_growth_kib={data_growth} "
        f"build_growth_kib={build_growth} "
        f"max_rustfs_memory_bytes={max_memory} "
        f"max_workflow_storage_bytes={max_workflow_storage} "
        f"max_iteration_millis={max_iteration_millis}"
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--test-log", required=True)
    parser.add_argument("--minimum-seconds", type=int, required=True)
    args = parser.parse_args()
    if args.minimum_seconds < 1:
        fail("--minimum-seconds must be positive")
    print(verify(pathlib.Path(args.test_log), args.minimum_seconds))


if __name__ == "__main__":
    main()
