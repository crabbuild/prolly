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

def resolver() -> None:
    engine = prolly.ProllyEngine.memory(prolly.default_config())
    base = engine.put(engine.create(), b"settings/theme", b"light")
    left_delete = engine.delete(base, b"settings/theme")
    right_update = engine.put(base, b"settings/theme", b"dark")

    update_wins = engine.merge(base, left_delete, right_update, "update_wins")
    delete_wins = engine.merge(base, left_delete, right_update, "delete_wins")

    assert_bytes(b"dark", engine.get(update_wins, b"settings/theme"), "update-wins setting")
    assert engine.get(delete_wins, b"settings/theme") is None

    print("resolver: demonstrated update-wins and delete-wins policies")


if __name__ == "__main__":
    resolver()
