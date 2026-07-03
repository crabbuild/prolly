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

def filesystem_snapshot() -> None:
    engine = prolly.ProllyEngine.memory(prolly.default_config())
    blob_store = prolly.ProllyBlobStore.memory()
    branch = b"refs/heads/main"

    tree = engine.create()
    for path, contents in {
        "README.md": b"# Demo\n",
        "src/lib.rs": b"pub fn answer() -> u8 { 42 }\n",
    }.items():
        tree = engine.put_large_value(
            blob_store,
            tree,
            b(f"path/{path}"),
            contents,
            prolly.LargeValueConfigRecord(inline_threshold=4),
        )

    engine.publish_named_root(branch, tree)
    loaded = engine.load_named_root(branch)
    readme = engine.get_large_value(blob_store, loaded, b"path/README.md")

    assert_bytes(b"# Demo\n", readme, "README.md")

    print("filesystem_snapshot: published branch with blob-backed file contents")


if __name__ == "__main__":
    filesystem_snapshot()
