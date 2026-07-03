from __future__ import annotations

from dataclasses import dataclass
from tempfile import TemporaryDirectory

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


def batch_build() -> None:
    engine = prolly.ProllyEngine.memory(prolly.default_config())
    entries = [
        prolly.EntryRecord(key=b(f"event/{idx:04d}"), value=b(f"payload-{idx}"))
        for idx in range(64, 0, -1)
    ]
    tree = engine.build_from_entries(entries)
    rows = engine.range(tree, b"event/", b"event0")
    stats = engine.collect_stats_json(tree).json

    assert len(rows) == 64
    assert rows[0].key == b"event/0001"
    assert "num_nodes" in stats

    print(f"batch_build: imported {len(rows)} events")


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


def vector_sidecar() -> None:
    engine = prolly.ProllyEngine.memory(prolly.default_config())
    sidecar = {
        "vec-1": [0.9, 0.1],
        "vec-2": [0.8, 0.2],
        "vec-stale": [1.0, 0.0],
    }
    tree = engine.batch(
        engine.create(),
        [
            upsert("vector-sidecar/corpus/docs/chunk/doc-1/0001", "vec-1|doc-1|parser-v1"),
            upsert("vector-sidecar/corpus/docs/chunk/doc-2/0001", "vec-2|doc-2|parser-v1"),
        ],
    )

    allowed = {
        text(entry.value).split("|", 1)[0]
        for entry in engine.range(tree, b"vector-sidecar/corpus/docs/chunk/", b"vector-sidecar/corpus/docs/chunk0")
    }
    hits = [vector_id for vector_id, _score in sorted(sidecar.items()) if vector_id in allowed]

    assert hits == ["vec-1", "vec-2"]

    print(f"vector_sidecar: filtered sidecar hits to {len(hits)} snapshot vectors")


def provenance_values() -> None:
    engine = prolly.ProllyEngine.memory(prolly.default_config())
    source = b"CrabDB language bindings design"
    source_cid = prolly.cid_from_bytes(source).hex()
    chunk_cid = prolly.cid_from_bytes(source[:16]).hex()

    tree = engine.batch(
        engine.create(),
        [
            upsert("provenance/chunk/file-1/chunk-1", f"source={source_cid}|chunk={chunk_cid}|parser=v1"),
            upsert("provenance/claim/file-1/claim-1", "CrabDB uses Rust-backed bindings|chunk=file-1/chunk-1"),
        ],
    )

    claims = engine.range(tree, b"provenance/claim/file-1/", b"provenance/claim/file-10")
    assert len(claims) == 1
    assert b"Rust-backed" in claims[0].value

    print("provenance_values: claim links back to source and chunk CIDs")


@dataclass(frozen=True)
class Order:
    tenant: str
    order_id: str
    status: str
    cents: int


def encode_order(order: Order) -> bytes:
    return b(f"{order.tenant}|{order.order_id}|{order.status}|{order.cents}")


def decode_order(value: bytes) -> Order:
    tenant, order_id, status, cents = text(value).split("|")
    return Order(tenant, order_id, status, int(cents))


def order_key(order: Order) -> bytes:
    return b(f"orders/source/tenant/{order.tenant}/order/{order.order_id}")


def view_key(tenant: str, status: str) -> bytes:
    return b(f"orders/view/by-status/tenant/{tenant}/status/{status}")


def build_revenue_view(engine: prolly.ProllyEngine, source: prolly.TreeRecord) -> prolly.TreeRecord:
    totals: dict[tuple[str, str], int] = {}
    for entry in engine.range(source, b"orders/source/", b"orders/source0"):
        order = decode_order(entry.value)
        totals[(order.tenant, order.status)] = totals.get((order.tenant, order.status), 0) + order.cents

    mutations = [
        prolly.MutationRecord(kind=prolly.MutationKind.UPSERT, key=view_key(tenant, status), value=str(cents).encode())
        for (tenant, status), cents in sorted(totals.items())
    ]
    return engine.batch(engine.create(), mutations)


def materialized_view() -> None:
    engine = prolly.ProllyEngine.memory(prolly.default_config())
    source_v1 = engine.batch(
        engine.create(),
        [
            prolly.MutationRecord(kind=prolly.MutationKind.UPSERT, key=order_key(Order("acme", "o1", "paid", 1200)), value=encode_order(Order("acme", "o1", "paid", 1200))),
            prolly.MutationRecord(kind=prolly.MutationKind.UPSERT, key=order_key(Order("acme", "o2", "open", 500)), value=encode_order(Order("acme", "o2", "open", 500))),
        ],
    )
    source_v2 = engine.put(source_v1, order_key(Order("acme", "o2", "paid", 500)), encode_order(Order("acme", "o2", "paid", 500)))
    view_v2 = build_revenue_view(engine, source_v2)

    assert_bytes(b"1700", engine.get(view_v2, view_key("acme", "paid")), "paid revenue")
    assert engine.get(view_v2, view_key("acme", "open")) is None

    print(f"materialized_view: folded {len(engine.diff(source_v1, source_v2))} source diff")


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


def durable_sqlite() -> None:
    with TemporaryDirectory() as tmp:
        engine = prolly.ProllyEngine.sqlite(f"{tmp}/app.prolly.sqlite", prolly.default_config())
        tree = engine.batch(engine.create(), [upsert("user/1", "Ada"), upsert("user/2", "Grace")])
        engine.publish_named_root(b"users/main", tree)
        loaded = engine.load_named_root(b"users/main")

        assert loaded == tree
        assert_bytes(b"Ada", engine.get(loaded, b"user/1"), "sqlite user")

    print("durable_sqlite: named root survived through SQLite store API")


SCENARIOS = {
    "batch_build": batch_build,
    "local_first_state": local_first_state,
    "resolver": resolver,
    "crdt_merge": crdt_merge,
    "conversation_memory": conversation_memory,
    "agent_event_log": agent_event_log,
    "background_compaction": background_compaction,
    "deterministic_rag_snapshot": deterministic_rag_snapshot,
    "document_chunk_index": document_chunk_index,
    "vector_sidecar": vector_sidecar,
    "provenance_values": provenance_values,
    "materialized_view": materialized_view,
    "filesystem_snapshot": filesystem_snapshot,
    "durable_sqlite": durable_sqlite,
}


def run_all() -> None:
    for scenario in SCENARIOS.values():
        scenario()
