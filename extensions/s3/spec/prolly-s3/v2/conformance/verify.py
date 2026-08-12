#!/usr/bin/env python3
"""Dependency-free structural verifier for native Prolly S3 Protocol v2."""

from __future__ import annotations

import pathlib
import re
import sys

HERE = pathlib.Path(__file__).resolve().parent
V2 = HERE.parent
S3 = V2.parents[2]


def check_source_surface() -> None:
    model = (S3 / "core/src/model.rs").read_text()
    repository = (S3 / "core/src/repository_v2.rs").read_text()
    client = (S3 / "client/src/client_v2.rs").read_text()
    combined = model + repository + client
    required = [
        'hash_id!(CommitIdV2, "pbc2_")',
        'hash_id!(ObjectVersionIdV2, "pov2_")',
        "pub struct RepositoryFormatV2",
        "pub struct BucketCommitV2",
        "pub struct RefValueV2",
        'repository_prefix: ".prolly/v2".to_string()',
        'b"prolly-s3/object-version/v2"',
        'b"prolly-s3/commit/v2"',
        "pub async fn start_merge",
        "pub async fn advance_merge",
        "pub async fn publish_merge",
    ]
    for marker in required:
        assert marker in combined, f"missing v2 source marker: {marker}"

    format_impl = re.search(
        r"impl RepositoryFormatV2 \{(?P<body>.*?)\n\}", model, re.DOTALL
    )
    assert format_impl is not None, "missing RepositoryFormatV2 implementation"
    constants = re.findall(
        r"(?:VERSION|CAPABILITY_PROFILE|PROTOCOL_VERSION|CURRENT_READER_VERSION|CURRENT_WRITER_VERSION):\s*u(?:16|32)\s*=\s*(\d+)",
        format_impl.group("body"),
    )
    assert constants and set(constants) == {"2"}, (
        f"non-v2 protocol constants: {constants}"
    )


def check_spec_surface() -> None:
    paths = (V2 / "paths.md").read_text()
    states = (V2 / "state-machines.md").read_text()
    required_paths = [
        "P/format/v2.cbor",
        "P/refs/v2/heads/N",
        "P/commits/v2/sha256/H0/H1/H",
        "P/publications/v2/sha256/H0/H1/H",
        "P/payloads/v2/R/sha256/H0/H1/H",
        "P/administration/v2/merge/E/plan/nodes/sha256/H0/H1/H",
    ]
    for marker in required_paths:
        assert marker in paths, f"missing v2 path marker: {marker}"
    required_states = [
        "## Branch-shard lease",
        "## Durable commit session",
        "## Ref lifecycle and sharded catalog",
        "## Explicit v1-to-v2 branch migration",
        "## Resumable structural merge",
    ]
    for marker in required_states:
        assert marker in states, f"missing v2 state machine: {marker}"


def main() -> int:
    check_source_surface()
    check_spec_surface()
    print("prolly-s3 v2 conformance structure: ok")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (AssertionError, KeyError, ValueError) as error:
        print(f"prolly-s3 v2 conformance structure: FAILED: {error}", file=sys.stderr)
        raise SystemExit(1)
