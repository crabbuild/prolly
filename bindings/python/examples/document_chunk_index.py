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

def document_chunk_index() -> None:
    engine = prolly.ProllyEngine.memory(prolly.default_config())
    blob_store = prolly.ProllyBlobStore.memory()
    tree = engine.create()
    text_key = b"doc-index/corpus/text/parser-v1/doc-1/chunk-0001"
    metadata_key = b"doc-index/corpus/parser/parser-v1/document/doc-1/chunk/000000"

    tree = engine.put_large_value(
        blob_store,
        tree,
        text_key,
        b"CrabDB stores large chunk text outside prolly leaves." * 8,
        prolly.LargeValueConfigRecord(inline_threshold=32),
    )
    tree = engine.put(tree, metadata_key, b"doc-1|chunk-0001|0|384|vector-0001")

    metadata = engine.range(tree, b"doc-index/corpus/parser/", b"doc-index/corpus/parser0")
    loaded_text = engine.get_large_value(blob_store, tree, text_key)

    assert len(metadata) == 1
    assert loaded_text.startswith(b"CrabDB stores")

    print("document_chunk_index: metadata and blob-backed chunk text are linked")


if __name__ == "__main__":
    document_chunk_index()
