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

def crdt_merge() -> None:
    engine = prolly.ProllyEngine.memory(prolly.default_config())
    empty = engine.create()
    base_value = prolly.timestamped_value_to_bytes(
        prolly.TimestampedValueRecord(value=b"base", timestamp=1)
    )
    left_value = prolly.timestamped_value_to_bytes(
        prolly.TimestampedValueRecord(value=b"left", timestamp=2)
    )
    right_value = prolly.timestamped_value_to_bytes(
        prolly.TimestampedValueRecord(value=b"right", timestamp=3)
    )

    base = engine.put(empty, b"counter/global", base_value)
    left = engine.put(base, b"counter/global", left_value)
    right = engine.put(base, b"counter/global", right_value)

    merged = engine.crdt_merge(base, left, right, prolly.crdt_config_lww(prolly.CrdtDeletePolicyKind.UPDATE_WINS))
    decoded = prolly.timestamped_value_from_bytes(engine.get(merged, b"counter/global"))
    merged_set = prolly.multi_value_set_merge([b"candidate-b"], [b"candidate-a", b"candidate-b"])

    assert decoded.value == b"right"
    assert decoded.timestamp == 3
    assert merged_set == [b"candidate-a", b"candidate-b"]

    print("crdt_merge: last-writer-wins and multi-value helpers passed")


if __name__ == "__main__":
    crdt_merge()
