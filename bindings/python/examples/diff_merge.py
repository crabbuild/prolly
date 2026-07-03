from __future__ import annotations

import prolly


engine = prolly.ProllyEngine.memory(prolly.default_config())

base = engine.create()
base = engine.put(base, b"doc:title", b"Draft")

left = engine.put(base, b"doc:body", b"Hello")
right = engine.put(base, b"doc:tags", b"example")

left_changes = engine.diff(base, left)
assert len(left_changes) == 1
assert left_changes[0].key == b"doc:body"

merged = engine.merge(base, left, right, "prefer_right")
assert engine.get(merged, b"doc:body") == b"Hello"
assert engine.get(merged, b"doc:tags") == b"example"

print(f"diff_merge: merged {len(left_changes)} left-side change")
