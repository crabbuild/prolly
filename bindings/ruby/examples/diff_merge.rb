# frozen_string_literal: true

require 'prolly'

engine = Prolly::ProllyEngine.memory(Prolly.default_config)
base = engine.create
base = engine.put(base, 'doc:title'.b, 'Draft'.b)

left = engine.put(base, 'doc:body'.b, 'Hello'.b)
right = engine.put(base, 'doc:tags'.b, 'example'.b)

left_changes = engine.diff(base, left)
raise 'expected one left-side change' unless left_changes.length == 1
raise 'expected doc:body diff' unless left_changes.first.key == 'doc:body'.b

merged = engine.merge(base, left, right, 'prefer_right')
raise 'missing body' unless engine.get(merged, 'doc:body'.b) == 'Hello'.b
raise 'missing tags' unless engine.get(merged, 'doc:tags'.b) == 'example'.b

puts "diff_merge: merged #{left_changes.length} left-side change"
