from __future__ import annotations

import prolly


def user_key(tenant: str, user_id: str) -> bytes:
    return f"source/tenant/{tenant}/user/{user_id}".encode()


def encode_user(tenant: str, user_id: str, status: str, display_name: str) -> bytes:
    return "|".join([tenant, user_id, status, display_name]).encode()


def decode_user(value: bytes) -> tuple[str, str, str, str]:
    tenant, user_id, status, display_name = value.decode().split("|", 3)
    return tenant, user_id, status, display_name


def status_index_prefix(tenant: str, status: str) -> bytes:
    return f"index/user-by-status/tenant/{tenant}/status/{status}/".encode()


def status_index_key(user: tuple[str, str, str, str]) -> bytes:
    tenant, user_id, status, _ = user
    return status_index_prefix(tenant, status) + user_id.encode()


def put_user(tree, tenant: str, user_id: str, status: str, display_name: str):
    return engine.put(tree, user_key(tenant, user_id), encode_user(tenant, user_id, status, display_name))


def build_status_index(source):
    index = engine.create()
    for entry in engine.range(source, b"source/", b"source0"):
        user = decode_user(entry.value)
        index = engine.put(index, status_index_key(user), b"1")
    return index


def apply_source_diff(index, changes):
    for change in changes:
        if change.kind == prolly.DiffKind.ADDED:
            index = engine.put(index, status_index_key(decode_user(change.value)), b"1")
        elif change.kind == prolly.DiffKind.REMOVED:
            index = engine.delete(index, status_index_key(decode_user(change.value)))
        elif change.kind == prolly.DiffKind.CHANGED:
            old_key = status_index_key(decode_user(change.old_value))
            new_key = status_index_key(decode_user(change.new_value))
            if old_key != new_key:
                index = engine.delete(index, old_key)
                index = engine.put(index, new_key, b"1")
    return index


def users_by_status(index, tenant: str, status: str):
    start = status_index_prefix(tenant, status)
    end = prolly.prefix_end(start)
    return engine.range(index, start, end)


engine = prolly.ProllyEngine.memory(prolly.default_config())
empty = engine.create()

source_v1 = put_user(empty, "acme", "u001", "active", "Ada")
source_v1 = put_user(source_v1, "acme", "u002", "invited", "Grace")
index_v1 = build_status_index(source_v1)

source_v2 = put_user(source_v1, "acme", "u002", "active", "Grace")
source_v2 = put_user(source_v2, "globex", "u003", "active", "Linus")

source_changes = engine.diff(source_v1, source_v2)
assert len(source_changes) == 2

index_v2 = apply_source_diff(index_v1, source_changes)
rebuilt_index_v2 = build_status_index(source_v2)
assert index_v2 == rebuilt_index_v2

assert len(users_by_status(index_v2, "acme", "active")) == 2
assert len(users_by_status(index_v2, "acme", "invited")) == 0
assert len(users_by_status(index_v2, "globex", "active")) == 1

print(f"secondary_index: applied {len(source_changes)} source diffs")
