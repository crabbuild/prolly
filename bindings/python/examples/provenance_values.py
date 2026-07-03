from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import prolly


def b(value: str) -> bytes:
    return value.encode("utf-8")


def text(value: bytes | None) -> str | None:
    return None if value is None else value.decode("utf-8")


def assert_bytes(expected: bytes, actual: bytes | None, label: str) -> None:
    if actual != expected:
        raise AssertionError(f"{label}: expected {expected!r}, got {actual!r}")


def upsert(key: str, value: str | bytes) -> prolly.MutationRecord:
    payload = value if isinstance(value, bytes) else b(value)
    return prolly.MutationRecord(kind=prolly.MutationKind.UPSERT, key=b(key), value=payload)


def delete(key: str) -> prolly.MutationRecord:
    return prolly.MutationRecord(kind=prolly.MutationKind.DELETE, key=b(key), value=None)

def provenance_values() -> None:
    engine = prolly.ProllyEngine.memory(prolly.default_config())
    source = b"CrabDB language bindings design"
    source_cid = prolly.cid_from_bytes(source).hex()
    chunk_cid = prolly.cid_from_bytes(source[:16]).hex()

    tree = engine.batch(
        engine.create(),
        [
            upsert("provenance/chunk/file-1/chunk-1", f"source={source_cid}|chunk={chunk_cid}|parser=v1"),
            upsert("provenance/claim/file-1/claim-1", "CrabDB uses Rust-backed bindings|chunk=file-1/chunk-1"),
        ],
    )

    claims = engine.range(tree, b"provenance/claim/file-1/", b"provenance/claim/file-10")
    assert len(claims) == 1
    assert b"Rust-backed" in claims[0].value

    print("provenance_values: claim links back to source and chunk CIDs")


if __name__ == "__main__":
    provenance_values()
