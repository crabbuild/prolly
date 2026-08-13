#!/usr/bin/env python3
"""Dependency-free structural verifier for the sole Prolly S3 format."""

from pathlib import Path
import sys

HERE = Path(__file__).resolve().parent
SPEC = HERE.parent
S3 = SPEC.parents[1]
CORE = S3 / "core" / "src"
CLIENT = S3 / "client" / "src"


def verify() -> None:
    paths = (SPEC / "paths.md").read_text()
    states = (SPEC / "state-machines.md").read_text()
    source = "\n".join(path.read_text() for path in [*CORE.glob("*.rs"), *CLIENT.glob("*.rs")])

    for marker in (
        "P/format/repository.cbor",
        "P/refs/heads/N",
        "P/commits/sha256/H0/H1/H",
        "P/publications/sha256/H0/H1/H",
        "P/payloads/R/sha256/H0/H1/H",
        "P/administration/merge/E/plan/",
    ):
        assert marker in paths, f"missing path marker: {marker}"

    for marker in (
        "## Branch publication",
        "## Authority renewal and takeover",
        "## Durable commit session",
        "## Structural merge",
    ):
        assert marker in states, f"missing state machine: {marker}"

    for marker in (
        'repository_prefix: ".prolly".to_string()',
        '"{prefix}/format/repository.cbor"',
        '"{}/refs/heads/{}"',
        '"{}/commits/sha256/{}/{}/{}"',
        '"{}/publications/sha256/{}/{}/{}"',
        '"{}/payloads/{}/sha256/{}/{}/{}"',
    ):
        assert marker in source, f"missing source marker: {marker}"

    forbidden = (
        "ClientV2",
        "RepositoryV2",
        "RepositoryFormatV1",
        "V1ToV2",
        ".prolly/v1",
        ".prolly/v2",
        "/v1/",
        "/v2/",
    )
    for marker in forbidden:
        assert marker not in source, f"legacy marker remains in source: {marker}"

    print("prolly-s3 conformance structure: ok")


if __name__ == "__main__":
    try:
        verify()
    except (AssertionError, OSError) as error:
        print(f"prolly-s3 conformance structure: FAILED: {error}", file=sys.stderr)
        raise SystemExit(1)
