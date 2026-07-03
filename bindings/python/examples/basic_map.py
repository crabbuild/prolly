from __future__ import annotations

import prolly


engine = prolly.ProllyEngine.memory(prolly.default_config())

tree = engine.create()
tree = engine.put(tree, b"user:001", b"Ada")
tree = engine.put(tree, b"user:002", b"Grace")
tree = engine.put(tree, b"user:003", b"Linus")

assert engine.get(tree, b"user:001") == b"Ada"

tree = engine.delete(tree, b"user:003")
assert engine.get(tree, b"user:003") is None

users = engine.range(tree, b"user:", b"user;")
assert [(entry.key, entry.value) for entry in users] == [
    (b"user:001", b"Ada"),
    (b"user:002", b"Grace"),
]

print(f"basic_map: {len(users)} users in range")
