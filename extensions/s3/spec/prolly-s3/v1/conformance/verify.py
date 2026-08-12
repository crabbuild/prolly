#!/usr/bin/env python3
"""Dependency-free structural verifier for Prolly S3 Protocol v1."""

from __future__ import annotations

import hashlib
import json
import pathlib
import re
import sys

HERE = pathlib.Path(__file__).resolve().parent
V1 = HERE.parent
S3 = V1.parents[2]


def domain_hash(domain: bytes, parts: list[bytes]) -> str:
    framed = len(domain).to_bytes(4, "big") + domain
    for part in parts:
        framed += len(part).to_bytes(8, "big") + part
    return hashlib.sha256(framed).hexdigest()


def derive_path(case: dict[str, object]) -> str:
    prefix = str(case["prefix"])
    kind = case["kind"]
    if kind == "format":
        return f"{prefix}/format/v1.cbor"
    if kind in {"branch", "tag"}:
        segment = "heads" if kind == "branch" else "tags"
        encoded = str(case["name_utf8"]).encode().hex()
        return f"{prefix}/refs/{segment}/{encoded}"
    if kind == "checkpoint_head":
        return f"{prefix}/node-index/latest.cbor"
    digest = str(case["digest_hex"])
    if kind == "commit":
        return f"{prefix}/commits/sha256/{digest[:2]}/{digest[2:4]}/{digest}"
    if kind == "checkpoint":
        generation = int(str(case["generation"]))
        return f"{prefix}/node-index/checkpoints/{generation:020d}-{digest}.cbor"
    raise AssertionError(f"unknown path kind: {kind}")


def check_registry() -> None:
    protocol = json.loads((V1 / "protocol.json").read_text())
    assert protocol["id"] == "prolly-s3/v1"
    assert protocol["status"] == "frozen"
    for section in ("versions",):
        for name, value in protocol[section].items():
            assert value == 1, f"{section}.{name} must be 1, got {value!r}"
    for name, value in protocol["defaults"].items():
        if name not in {"repository_prefix", "default_branch"}:
            assert value == 1, f"defaults.{name} must be 1, got {value!r}"
    assert protocol["defaults"]["repository_prefix"] == ".prolly/v1"
    assert protocol["format_path"] == "{prefix}/format/v1.cbor"


def check_cases() -> None:
    corpus = json.loads((HERE / "cases.json").read_text())
    assert corpus["schema"] == "prolly-s3-conformance/v1"
    assert corpus["version"] == 1
    for case in corpus["hashes"]:
        if case["algorithm"] == "sha256":
            actual = hashlib.sha256(bytes.fromhex(case["input_hex"])).hexdigest()
        else:
            actual = domain_hash(
                case["domain_utf8"].encode(),
                [bytes.fromhex(value) for value in case["parts_hex"]],
            )
        assert actual == case["expected_hex"], f"hash vector {case['name']}"
    for case in corpus["paths"]:
        actual = derive_path(case)
        assert actual == case["expected"], f"path vector {case['name']}: {actual}"
    assert len(corpus["invalid_cbor"]) >= 8
    assert {case["retry"] for case in corpus["publication_scenarios"] if "retry" in case} == {
        "ReloadHead", "ReconcileOperation"
    }


def check_source_defaults() -> None:
    model = (S3 / "core/src/model.rs").read_text()
    repository = (S3 / "core/src/repository.rs").read_text()
    client = (S3 / "client/src/client.rs").read_text()
    combined = model + repository + client
    default_surface = repository + client
    required = [
        'hash_id!(CommitId, "pbc1_")',
        'hash_id!(ObjectVersionId, "pov1_")',
        "pub struct RepositoryFormatV1",
        "pub struct BucketCommitV1",
        "pub struct RefValueV1",
        'repository_prefix: ".prolly/v1".to_string()',
        'format!("{prefix}/format/v1.cbor")',
        'b"prolly-s3/object-version/v1"',
        'b"prolly-s3/commit/v1"',
    ]
    for marker in required:
        assert marker in combined, f"missing v1 source marker: {marker}"
    # Protocol-v2 records intentionally coexist in model.rs. The v1 verifier
    # protects the default high-level repository/client surface from silently
    # switching wire formats; it must not reject side-by-side protocol types.
    forbidden_defaults = [
        "RepositoryFormatV2", "BucketCommitV2", "RefValueV2",
        '"pbc2_"', '"pov2_"', "/format/v2.cbor", '".prolly/v2"',
        "prolly-s3/object-version/v2", "prolly-s3/commit/v2",
    ]
    for marker in forbidden_defaults:
        assert marker not in default_surface, f"forbidden v2 default marker: {marker}"
    format_impl = re.search(
        r"impl RepositoryFormatV1 \{(?P<body>.*?)\n\}", model, re.DOTALL
    )
    assert format_impl is not None, "missing RepositoryFormatV1 implementation"
    constants = re.findall(
        r"(?:VERSION|CAPABILITY_PROFILE|PROTOCOL_VERSION|CURRENT_READER_VERSION|CURRENT_WRITER_VERSION):\s*u(?:16|32)\s*=\s*(\d+)",
        format_impl.group("body"),
    )
    assert constants and set(constants) == {"1"}, f"non-v1 protocol constants: {constants}"


def main() -> int:
    check_registry()
    check_cases()
    check_source_defaults()
    print("prolly-s3 v1 conformance structure: ok")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (AssertionError, KeyError, ValueError) as error:
        print(f"prolly-s3 v1 conformance structure: FAILED: {error}", file=sys.stderr)
        raise SystemExit(1)
