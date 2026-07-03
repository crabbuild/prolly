# Prolly Ruby Cookbook

The Ruby gem loads the Rust UniFFI library and exposes binary `String` keys and
values. Point `PROLLY_BINDINGS_LIBRARY` at the local Cargo dylib when running
from the source tree.

```sh
cargo build -p prolly-bindings
PROLLY_BINDINGS_LIBRARY="$PWD/target/debug/libprolly_bindings.dylib" \
  ruby -Icrates/prolly/bindings/ruby/lib app.rb
```

Runnable scenarios live as separate files under `examples/`, matching the Rust
example style:

```sh
cargo build -p prolly-bindings
BUNDLE_GEMFILE=crates/prolly/bindings/ruby/Gemfile \
  BUNDLE_PATH=/tmp/prolly-ruby-bundle \
  bundle install
PROLLY_BINDINGS_LIBRARY="$PWD/target/debug/libprolly_bindings.dylib" \
  BUNDLE_GEMFILE=crates/prolly/bindings/ruby/Gemfile \
  BUNDLE_PATH=/tmp/prolly-ruby-bundle \
  bundle exec ruby -Icrates/prolly/bindings/ruby/lib \
  crates/prolly/bindings/ruby/examples/cookbook_scenarios.rb
PROLLY_BINDINGS_LIBRARY="$PWD/target/debug/libprolly_bindings.dylib" \
  BUNDLE_GEMFILE=crates/prolly/bindings/ruby/Gemfile \
  BUNDLE_PATH=/tmp/prolly-ruby-bundle \
  bundle exec ruby -Icrates/prolly/bindings/ruby/lib \
  crates/prolly/bindings/ruby/examples/basic_map.rb
PROLLY_BINDINGS_LIBRARY="$PWD/target/debug/libprolly_bindings.dylib" \
  BUNDLE_GEMFILE=crates/prolly/bindings/ruby/Gemfile \
  BUNDLE_PATH=/tmp/prolly-ruby-bundle \
  bundle exec ruby -Icrates/prolly/bindings/ruby/lib \
  crates/prolly/bindings/ruby/examples/secondary_index.rb
```

Application-style files include `batch_build.rb`, `local_first_state.rb`,
`resolver.rb`, `crdt_merge.rb`, `conversation_memory.rb`,
`agent_event_log.rb`, `background_compaction.rb`,
`deterministic_rag_snapshot.rb`, `document_chunk_index.rb`,
`vector_sidecar.rb`, `provenance_values.rb`, `materialized_view.rb`,
`filesystem_snapshot.rb`, and `durable_sqlite.rb`.

## Create A Durable Index

```ruby
require 'prolly'

engine = Prolly::ProllyEngine.sqlite('app.prolly.db', Prolly.default_config)
tree = engine.create

tree = engine.batch(
  tree,
  [
    Prolly::MutationRecord.new(kind: Prolly::MutationKind::UPSERT, key: 'user/1'.b, value: 'Ada'.b),
    Prolly::MutationRecord.new(kind: Prolly::MutationKind::UPSERT, key: 'user/2'.b, value: 'Linus'.b)
  ]
)

engine.publish_named_root('users/main'.b, tree)
```

## Prefix Queries And Pages

```ruby
prefix = 'user/'.b
finish = Prolly.prefix_end(prefix)

engine.range(tree, prefix, finish).each do |entry|
  puts "#{entry.key}=#{entry.value}"
end

cursor = nil
loop do
  page = engine.range_page(tree, cursor, nil, 100)
  page.entries.each { |entry| handle(entry.key, entry.value) }
  break unless page.next_cursor

  cursor = page.next_cursor
end

reverse_cursor = nil
loop do
  page = engine.reverse_page(tree, reverse_cursor, ''.b, 100)
  page.entries.each { |entry| handle_newest_first(entry.key, entry.value) }
  break unless page.next_cursor

  reverse_cursor = page.next_cursor
end

diffs = engine.diff_from_cursor(
  old_tree,
  new_tree,
  Prolly::RangeCursorRecord.new(after_key: 'user/42'.b),
  nil
)
```

## Use Future Wrappers

```ruby
async = Prolly::AsyncEngine.memory(Prolly.default_config)
tree = async.create.value
tree = async.put(tree, 'k'.b, 'v'.b).value
value = async.get(tree, 'k'.b).value
```

## Merge Writers

```ruby
base = tree
left = engine.put(base, 'user/1'.b, 'Ada Lovelace'.b)
right = engine.put(base, 'user/1'.b, 'Countess Ada'.b)

merged = engine.merge(base, left, right, 'prefer_right')

class JoinResolver < Prolly::MergeResolverCallback
  def resolve(conflict)
    if conflict.left && conflict.right
      return Prolly::ResolutionRecord.new(
        kind: Prolly::ResolutionKind::VALUE,
        value: conflict.left + ' | '.b + conflict.right
      )
    end

    Prolly::ResolutionRecord.new(kind: Prolly::ResolutionKind::UNRESOLVED, value: nil)
  end
end

merged = engine.merge_with_resolver(base, left, right, JoinResolver.new)
```

## Large Values And Blob GC

```ruby
blob_store = Prolly::ProllyBlobStore.file('app.blobs')
large = 'x'.b * 1_000_000

tree = engine.put_large_value(
  blob_store,
  tree,
  'doc/1'.b,
  large,
  Prolly::LargeValueConfigRecord.new(inline_threshold: 4096)
)

loaded = engine.get_large_value(blob_store, tree, 'doc/1'.b)
plan = engine.plan_blob_store_gc(blob_store, [tree])
engine.sweep_blob_store_gc(blob_store, [tree]) if plan.reclaimable_blob_count.positive?
```

## Custom Stores

Subclass `Prolly::HostStoreCallback` when Ruby owns persistence.

```ruby
class MemoryHostStore < Prolly::HostStoreCallback
  def initialize
    @nodes = {}
    @roots = {}
  end

  def get(key)
    Prolly::HostStoreBytesResultRecord.new(value: @nodes[key]&.dup, error: nil)
  end

  def put(key, value)
    @nodes[key.dup.b.freeze] = value.dup.b
    Prolly::HostStoreUnitResultRecord.new(error: nil)
  end

  def delete(key)
    @nodes.delete(key)
    Prolly::HostStoreUnitResultRecord.new(error: nil)
  end

  def batch(ops)
    ops.each do |op|
      if op.kind == Prolly::MutationKind::UPSERT
        @nodes[op.key.dup.b.freeze] = op.value.dup.b
      else
        @nodes.delete(op.key)
      end
    end
    Prolly::HostStoreUnitResultRecord.new(error: nil)
  end

  def batch_get_ordered(keys)
    Prolly::HostStoreBatchGetResultRecord.new(values: keys.map { |key| @nodes[key]&.dup }, error: nil)
  end

  def prefers_batch_reads
    Prolly::HostStoreBoolResultRecord.new(value: true, error: nil)
  end

  def supports_hints
    Prolly::HostStoreBoolResultRecord.new(value: false, error: nil)
  end

  def get_hint(namespace, key)
    Prolly::HostStoreBytesResultRecord.new(value: nil, error: nil)
  end

  def put_hint(namespace, key, value)
    Prolly::HostStoreUnitResultRecord.new(error: nil)
  end

  def list_node_cids
    Prolly::HostStoreListBytesResultRecord.new(values: @nodes.keys.map(&:dup), error: nil)
  end

  def get_root(name)
    Prolly::HostStoreRootResultRecord.new(value: @roots[name], error: nil)
  end

  def put_root(name, manifest)
    @roots[name.dup.b.freeze] = manifest
    Prolly::HostStoreUnitResultRecord.new(error: nil)
  end

  def delete_root(name)
    @roots.delete(name)
    Prolly::HostStoreUnitResultRecord.new(error: nil)
  end

  def compare_and_swap_root(name, expected, replacement)
    current = @roots[name]
    return Prolly::HostStoreRootCasResultRecord.new(applied: false, current: current, error: nil) unless current == expected

    replacement ? @roots[name.dup.b.freeze] = replacement : @roots.delete(name)
    Prolly::HostStoreRootCasResultRecord.new(applied: true, current: nil, error: nil)
  end

  def list_roots
    Prolly::HostStoreListRootsResultRecord.new(
      values: @roots.map { |name, manifest| Prolly::HostStoreNamedRootManifestRecord.new(name: name, manifest: manifest) },
      error: nil
    )
  end
end

engine = Prolly::ProllyEngine.custom_store(MemoryHostStore.new, Prolly.default_config)
```
