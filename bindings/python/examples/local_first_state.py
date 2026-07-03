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

def local_first_state() -> None:
    engine = prolly.ProllyEngine.memory(prolly.default_config())
    main = b"app/demo/root/main"

    base = engine.batch(
        engine.create(),
        [
            upsert("entity/user/001", "Ada"),
            upsert("index/user/name/Ada/001", b""),
        ],
    )
    engine.publish_named_root(main, base)

    device = engine.batch(
        base,
        [
            upsert("entity/task/900", "offline draft"),
            upsert("index/task/status/open/900", b""),
        ],
    )
    canonical = engine.put(base, b"entity/user/002", b"Grace")
    engine.publish_named_root(main, canonical)

    current = engine.load_named_root(main)
    merged = engine.merge(base, current, device, "prefer_right")
    update = engine.compare_and_swap_named_root(main, current, merged)

    assert update.applied
    assert_bytes(b"Grace", engine.get(merged, b"entity/user/002"), "canonical user")
    assert_bytes(b"offline draft", engine.get(merged, b"entity/task/900"), "device task")

    print("local_first_state: merged offline branch into main")


if __name__ == "__main__":
    local_first_state()
