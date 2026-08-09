#!/usr/bin/env python3
"""Run two independently built Versioned DynamoDB client probes together."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time
from typing import Any


PROTOCOL = "prolly-dynamodb-rolling-probe-v1"
DURABLE_CAPABILITY_FIELDS = (
    "database_format_version",
    "database_format_record_hex",
    "logical_protocol_major",
    "logical_protocol_minor",
    "item_codec_digest",
    "key_codec_digest",
    "catalog_codec_digest",
    "commit_codec_digest",
    "tree_format_digest",
    "large_value_inline_threshold",
    "transaction_publication_mode",
    "maximum_root_actions",
    "staged_node_deletes",
)


class QualificationError(RuntimeError):
    """A fail-closed rolling-compatibility qualification result."""


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def parse_probe_output(stdout: str, expected_command: str) -> dict[str, Any]:
    lines = [line for line in stdout.splitlines() if line.strip()]
    if len(lines) != 1:
        raise QualificationError(
            f"{expected_command} emitted {len(lines)} non-empty stdout lines; expected one"
        )
    try:
        value = json.loads(lines[0])
    except json.JSONDecodeError as error:
        raise QualificationError(
            f"{expected_command} emitted invalid JSON: {error}"
        ) from error
    if not isinstance(value, dict):
        raise QualificationError(f"{expected_command} output must be a JSON object")
    if value.get("protocol") != PROTOCOL:
        raise QualificationError(f"{expected_command} protocol mismatch")
    if value.get("command") != expected_command:
        raise QualificationError(f"{expected_command} command echo mismatch")
    if not isinstance(value.get("package_version"), str):
        raise QualificationError(f"{expected_command} omitted package_version")
    return value


def validate_identity_pair(old: dict[str, Any], new: dict[str, Any]) -> None:
    old_capabilities = old.get("capabilities")
    new_capabilities = new.get("capabilities")
    if not isinstance(old_capabilities, dict) or not isinstance(new_capabilities, dict):
        raise QualificationError("identity output omitted capabilities")
    for field in DURABLE_CAPABILITY_FIELDS:
        if field not in old_capabilities or field not in new_capabilities:
            raise QualificationError(f"identity omitted durable capability {field!r}")
        if old_capabilities[field] != new_capabilities[field]:
            raise QualificationError(
                f"durable capability {field!r} differs: "
                f"old={old_capabilities[field]!r}, new={new_capabilities[field]!r}"
            )


def validate_write(value: dict[str, Any], writer: str, count: int) -> None:
    if value.get("writer") != writer or value.get("start") != 0 or value.get("count") != count:
        raise QualificationError(f"{writer} write manifest differs from requested range")
    for boundary, counter in (("first", 0), ("last", count - 1)):
        item = value.get(boundary)
        expected_id = f"{writer}-{counter:010d}"
        if not isinstance(item, dict) or item.get("id") != expected_id:
            raise QualificationError(f"{writer} {boundary} item mismatch")
        version = item.get("version")
        if not isinstance(version, str) or len(version) != 64:
            raise QualificationError(f"{writer} {boundary} version is not a 32-byte hex ID")
        if any(character not in "0123456789abcdef" for character in version):
            raise QualificationError(f"{writer} {boundary} version is not lowercase hex")


def validate_verify(value: dict[str, Any], writes: int) -> None:
    expected = writes + 1
    if value.get("items") != writes:
        raise QualificationError("verification item count mismatch")
    if value.get("versions") != expected:
        raise QualificationError("verification version count mismatch")
    if value.get("commits") != expected:
        raise QualificationError("verification commit count mismatch")
    head = value.get("head")
    if not isinstance(head, str) or len(head) != 64:
        raise QualificationError("verification head is not a 32-byte hex ID")


class Runner:
    def __init__(
        self,
        old_binary: Path,
        new_binary: Path,
        environment: dict[str, str],
        timeout_seconds: float,
    ) -> None:
        self.old_binary = old_binary
        self.new_binary = new_binary
        self.environment = environment
        self.timeout_seconds = timeout_seconds
        self.commands: list[dict[str, Any]] = []

    def run(self, label: str, binary: Path, *arguments: str) -> dict[str, Any]:
        started = time.monotonic()
        try:
            result = subprocess.run(
                [str(binary), *arguments],
                env=self.environment,
                capture_output=True,
                text=True,
                timeout=self.timeout_seconds,
                check=False,
            )
        except subprocess.TimeoutExpired as error:
            raise QualificationError(f"{label} timed out") from error
        duration = time.monotonic() - started
        record = {
            "label": label,
            "binary": str(binary),
            "arguments": list(arguments),
            "returncode": result.returncode,
            "duration_seconds": duration,
            "stdout": result.stdout,
            "stderr": result.stderr,
        }
        self.commands.append(record)
        if result.returncode != 0:
            raise QualificationError(
                f"{label} failed with status {result.returncode}; see report command log"
            )
        return parse_probe_output(result.stdout, arguments[0])

    def run_writers(self, count: int) -> tuple[dict[str, Any], dict[str, Any]]:
        processes: list[tuple[str, Path, subprocess.Popen[str], float]] = []
        for label, binary, writer in (
            ("old-write", self.old_binary, "old"),
            ("new-write", self.new_binary, "new"),
        ):
            started = time.monotonic()
            process = subprocess.Popen(
                [str(binary), "write", writer, "0", str(count)],
                env=self.environment,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            processes.append((label, binary, process, started))

        outputs: dict[str, dict[str, Any]] = {}
        deadline = time.monotonic() + self.timeout_seconds
        try:
            for label, binary, process, started in processes:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise subprocess.TimeoutExpired(process.args, self.timeout_seconds)
                stdout, stderr = process.communicate(timeout=remaining)
                record = {
                    "label": label,
                    "binary": str(binary),
                    "arguments": ["write", label.removesuffix("-write"), "0", str(count)],
                    "returncode": process.returncode,
                    "duration_seconds": time.monotonic() - started,
                    "stdout": stdout,
                    "stderr": stderr,
                }
                self.commands.append(record)
                if process.returncode != 0:
                    raise QualificationError(
                        f"{label} failed with status {process.returncode}; see report command log"
                    )
                writer = label.removesuffix("-write")
                value = parse_probe_output(stdout, "write")
                validate_write(value, writer, count)
                outputs[writer] = value
        except BaseException:
            for _, _, process, _ in processes:
                if process.poll() is None:
                    process.kill()
            recorded = {record["label"] for record in self.commands}
            for label, binary, process, started in processes:
                if label in recorded:
                    continue
                try:
                    stdout, stderr = process.communicate(timeout=5)
                except subprocess.TimeoutExpired:
                    stdout, stderr = "", "process did not terminate after kill"
                self.commands.append(
                    {
                        "label": label,
                        "binary": str(binary),
                        "arguments": [
                            "write",
                            label.removesuffix("-write"),
                            "0",
                            str(count),
                        ],
                        "returncode": process.returncode,
                        "duration_seconds": time.monotonic() - started,
                        "stdout": stdout,
                        "stderr": stderr,
                        "terminated_by_coordinator": True,
                    }
                )
            raise
        return outputs["old"], outputs["new"]


def atomic_json(path: Path, value: dict[str, Any]) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    temporary.replace(path)


def executable_path(value: str, label: str) -> Path:
    path = Path(value).expanduser().resolve(strict=True)
    if not path.is_file() or not os.access(path, os.X_OK):
        raise QualificationError(f"{label} is not an executable regular file: {path}")
    return path


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Qualify two independently built Versioned DynamoDB client probes"
    )
    parser.add_argument("--old-binary", required=True)
    parser.add_argument("--new-binary", required=True)
    parser.add_argument("--physical-table", required=True)
    parser.add_argument("--root-table")
    parser.add_argument("--endpoint")
    parser.add_argument("--prefix")
    parser.add_argument("--iterations", type=int, default=50)
    parser.add_argument("--timeout-seconds", type=float, default=300.0)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument(
        "--allow-identical-binaries",
        action="store_true",
        help="diagnostic smoke only; never counts as mixed-version evidence",
    )
    parser.add_argument("--keep-namespace", action="store_true")
    parser.add_argument("--cleanup-on-failure", action="store_true")
    return parser.parse_args()


def main() -> int:
    arguments = parse_arguments()
    output_dir = Path(arguments.output_dir).expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=False)
    started_utc = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    report: dict[str, Any] = {
        "schema": "prolly-dynamodb-rolling-compatibility-v1",
        "started_utc": started_utc,
        "status": "running",
        "commands": [],
    }
    initialized = False
    runner: Runner | None = None

    try:
        if not 1 <= arguments.iterations <= 4_999:
            raise QualificationError("iterations must be in 1..=4999")
        if arguments.timeout_seconds <= 0:
            raise QualificationError("timeout-seconds must be positive")
        old_binary = executable_path(arguments.old_binary, "old binary")
        new_binary = executable_path(arguments.new_binary, "new binary")
        old_hash = sha256_file(old_binary)
        new_hash = sha256_file(new_binary)
        if old_hash == new_hash and not arguments.allow_identical_binaries:
            raise QualificationError(
                "old and new binary SHA-256 values are identical; use "
                "--allow-identical-binaries only for diagnostic smoke"
            )

        prefix = arguments.prefix or (
            f"rolling-compat-{int(time.time())}-{os.getpid()}:"
        )
        if not prefix or len(prefix.encode("utf-8")) > 256:
            raise QualificationError("prefix must contain 1..=256 UTF-8 bytes")
        root_table = arguments.root_table or f"{arguments.physical_table}-rolling-roots"
        environment = os.environ.copy()
        environment.update(
            {
                "PROLLY_DYNAMODB_COMPAT_TABLE": arguments.physical_table,
                "PROLLY_DYNAMODB_COMPAT_ROOT_TABLE": root_table,
                "PROLLY_DYNAMODB_COMPAT_PREFIX": prefix,
            }
        )
        if arguments.endpoint:
            environment["PROLLY_DYNAMODB_COMPAT_ENDPOINT"] = arguments.endpoint
        else:
            environment.pop("PROLLY_DYNAMODB_COMPAT_ENDPOINT", None)

        report.update(
            {
                "old_binary": {"path": str(old_binary), "sha256": old_hash},
                "new_binary": {"path": str(new_binary), "sha256": new_hash},
                "identical_binary_diagnostic": old_hash == new_hash,
                "physical_table": arguments.physical_table,
                "root_table": root_table,
                "prefix": prefix,
                "iterations_per_binary": arguments.iterations,
            }
        )
        runner = Runner(
            old_binary,
            new_binary,
            environment,
            arguments.timeout_seconds,
        )
        report["commands"] = runner.commands

        runner.run("old-init", old_binary, "init")
        initialized = True
        old_identity = runner.run("old-identity", old_binary, "identity")
        new_identity = runner.run("new-identity", new_binary, "identity")
        validate_identity_pair(old_identity, new_identity)

        old_write, new_write = runner.run_writers(arguments.iterations)
        total_writes = 2 * arguments.iterations
        old_verify = runner.run(
            "old-verify",
            old_binary,
            "verify",
            str(arguments.iterations),
            str(arguments.iterations),
        )
        new_verify = runner.run(
            "new-verify",
            new_binary,
            "verify",
            str(arguments.iterations),
            str(arguments.iterations),
        )
        validate_verify(old_verify, total_writes)
        validate_verify(new_verify, total_writes)
        if old_verify.get("head") != new_verify.get("head"):
            raise QualificationError("old and new readers observed different current heads")

        old_first = old_write["first"]
        new_first = new_write["first"]
        runner.run(
            "new-read-old-version",
            new_binary,
            "verify-at",
            old_first["version"],
            old_first["id"],
            "old",
            "0",
        )
        runner.run(
            "old-read-new-version",
            old_binary,
            "verify-at",
            new_first["version"],
            new_first["id"],
            "new",
            "0",
        )

        report.update(
            {
                "old_identity": old_identity,
                "new_identity": new_identity,
                "old_verify": old_verify,
                "new_verify": new_verify,
                "status": "passed",
            }
        )
        if not arguments.keep_namespace:
            runner.run("new-cleanup", new_binary, "cleanup")
            report["namespace_cleaned"] = True
        else:
            report["namespace_cleaned"] = False
        return_code = 0
    except BaseException as error:  # fail closed while preserving a forensic report
        report["status"] = "failed"
        report["error_type"] = type(error).__name__
        report["error"] = str(error)
        return_code = 1
        if (
            initialized
            and runner is not None
            and arguments.cleanup_on_failure
            and not arguments.keep_namespace
        ):
            try:
                runner.run("failure-cleanup", runner.new_binary, "cleanup")
                report["namespace_cleaned"] = True
            except BaseException as cleanup_error:
                report["cleanup_error"] = str(cleanup_error)
        else:
            report["namespace_cleaned"] = False
    finally:
        report["ended_utc"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        atomic_json(output_dir / "report.json", report)

    if return_code:
        print(report.get("error", "qualification failed"), file=sys.stderr)
    else:
        print(output_dir / "report.json")
    return return_code


if __name__ == "__main__":
    raise SystemExit(main())
