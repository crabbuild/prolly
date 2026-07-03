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

def vector_sidecar() -> None:
    engine = prolly.ProllyEngine.memory(prolly.default_config())
    sidecar = {
        "vec-1": [0.9, 0.1],
        "vec-2": [0.8, 0.2],
        "vec-stale": [1.0, 0.0],
    }
    tree = engine.batch(
        engine.create(),
        [
            upsert("vector-sidecar/corpus/docs/chunk/doc-1/0001", "vec-1|doc-1|parser-v1"),
            upsert("vector-sidecar/corpus/docs/chunk/doc-2/0001", "vec-2|doc-2|parser-v1"),
        ],
    )

    allowed = {
        text(entry.value).split("|", 1)[0]
        for entry in engine.range(tree, b"vector-sidecar/corpus/docs/chunk/", b"vector-sidecar/corpus/docs/chunk0")
    }
    hits = [vector_id for vector_id, _score in sorted(sidecar.items()) if vector_id in allowed]

    assert hits == ["vec-1", "vec-2"]

    print(f"vector_sidecar: filtered sidecar hits to {len(hits)} snapshot vectors")


if __name__ == "__main__":
    vector_sidecar()
