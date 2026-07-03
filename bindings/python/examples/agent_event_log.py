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

def agent_event_log() -> None:
    engine = prolly.ProllyEngine.memory(prolly.default_config())
    root = b"agent-log/run-7/root/events/current"
    tree = engine.batch(
        engine.create(),
        [
            upsert("agent-log/run-7/event/1783036805000/0001", "user|Summarize the plan"),
            upsert("agent-log/run-7/event/1783036805000/0002", "tool-call|search-docs"),
            upsert("agent-log/run-7/event/1783036806000/0003", "assistant|Plan ready"),
        ],
    )
    engine.publish_named_root(root, tree)

    page = engine.range_page(engine.load_named_root(root), None, None, 2)
    assert len(page.entries) == 2
    assert page.next_cursor is not None

    print(f"agent_event_log: first page has {len(page.entries)} events")


if __name__ == "__main__":
    agent_event_log()
