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

def batch_build() -> None:
    engine = prolly.ProllyEngine.memory(prolly.default_config())
    entries = [
        prolly.EntryRecord(key=b(f"event/{idx:04d}"), value=b(f"payload-{idx}"))
        for idx in range(64, 0, -1)
    ]
    tree = engine.build_from_entries(entries)
    rows = engine.range(tree, b"event/", b"event0")
    stats = engine.collect_stats_json(tree).json

    assert len(rows) == 64
    assert rows[0].key == b"event/0001"
    assert "num_nodes" in stats

    print(f"batch_build: imported {len(rows)} events")


if __name__ == "__main__":
    batch_build()
