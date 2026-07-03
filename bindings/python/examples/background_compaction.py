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

def background_compaction() -> None:
    engine = prolly.ProllyEngine.memory(prolly.default_config())
    current_name = b"compaction/run/r7/root/events/current"
    checkpoint_name = b"compaction/run/r7/root/events/0001"

    events = engine.batch(
        engine.create(),
        [upsert(f"event/{idx:04d}", f"raw-event-{idx}") for idx in range(1, 7)],
    )
    engine.publish_named_root(checkpoint_name, events)

    mutations = [delete(f"event/{idx:04d}") for idx in range(1, 5)]
    mutations.append(upsert("event/0004-summary", "summary of events 1..4"))
    compacted = engine.batch(events, mutations)
    engine.publish_named_root(current_name, compacted)

    plan = engine.plan_store_gc([events, compacted])
    remaining = engine.range(compacted, b"event/", b"event0")

    assert len(remaining) == 3
    assert int(plan.reclaimable_nodes) >= 0

    print(f"background_compaction: compacted log to {len(remaining)} records")


if __name__ == "__main__":
    background_compaction()
