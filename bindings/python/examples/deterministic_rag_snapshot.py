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

def deterministic_rag_snapshot() -> None:
    engine = prolly.ProllyEngine.memory(prolly.default_config())
    index_root = b"rag/corpus/docs/root/index/current"
    answers_root = b"rag/corpus/docs/root/answers"

    index_v1 = engine.batch(
        engine.create(),
        [
            upsert("rag/corpus/docs/chunk/doc-1/0001", "vector:v1|CrabDB stores deterministic roots"),
            upsert("rag/corpus/docs/chunk/doc-2/0001", "vector:v2|Prolly trees diff by key"),
        ],
    )
    engine.publish_named_root(index_root, index_v1)

    answer = b"query:q1|snapshot:" + (index_v1.root or b"") + b"|citation:doc-1/0001"
    answers = engine.put(engine.create(), b"rag/answer/q1", answer)
    engine.publish_named_root(answers_root, answers)

    index_v2 = engine.put(index_v1, b"rag/corpus/docs/chunk/doc-3/0001", b"vector:v3|New content")
    engine.publish_named_root(index_root, index_v2)

    replay_rows = engine.range(index_v1, b"rag/corpus/docs/chunk/", b"rag/corpus/docs/chunk0")
    current_rows = engine.range(engine.load_named_root(index_root), b"rag/corpus/docs/chunk/", b"rag/corpus/docs/chunk0")

    assert len(replay_rows) == 2
    assert len(current_rows) == 3

    print("deterministic_rag_snapshot: replay kept original index root")


if __name__ == "__main__":
    deterministic_rag_snapshot()
