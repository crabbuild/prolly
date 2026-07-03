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

def conversation_memory() -> None:
    engine = prolly.ProllyEngine.memory(prolly.default_config())
    main = b"conversation/c42/root/main"
    attempt_name = b"conversation/c42/attempt/extractor/a1"

    base = engine.put(engine.create(), b"conversation/c42/memory/m001", b"user|likes terse summaries|0.91")
    engine.publish_named_root(main, base)

    attempt = engine.put(base, b"conversation/c42/memory/m002", b"user|uses Python|0.87")
    engine.publish_named_root(attempt_name, attempt)

    canonical = engine.put(base, b"conversation/c42/memory/m003", b"user|prefers local-first apps|0.82")
    engine.publish_named_root(main, canonical)

    merged = engine.merge(base, engine.load_named_root(main), engine.load_named_root(attempt_name), "prefer_right")
    update = engine.compare_and_swap_named_root(main, canonical, merged)

    assert update.applied
    assert len(engine.range(merged, b"conversation/c42/memory/", b"conversation/c42/memory0")) == 3

    print("conversation_memory: accepted extractor attempt into canonical memory")


if __name__ == "__main__":
    conversation_memory()
