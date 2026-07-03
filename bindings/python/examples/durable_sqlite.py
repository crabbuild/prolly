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

def durable_sqlite() -> None:
    with TemporaryDirectory() as tmp:
        engine = prolly.ProllyEngine.sqlite(f"{tmp}/app.prolly.sqlite", prolly.default_config())
        tree = engine.batch(engine.create(), [upsert("user/1", "Ada"), upsert("user/2", "Grace")])
        engine.publish_named_root(b"users/main", tree)
        loaded = engine.load_named_root(b"users/main")

        assert loaded == tree
        assert_bytes(b"Ada", engine.get(loaded, b"user/1"), "sqlite user")

    print("durable_sqlite: named root survived through SQLite store API")


if __name__ == "__main__":
    durable_sqlite()
