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


if __name__ == "__main__":
    materialized_view()
