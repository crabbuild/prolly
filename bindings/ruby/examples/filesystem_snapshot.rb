# frozen_string_literal: true

require 'tmpdir'

$LOAD_PATH.unshift(File.expand_path('../lib', __dir__))
require 'prolly'

def upsert(key, value)
  Prolly::MutationRecord.new(kind: Prolly::MutationKind::UPSERT, key: key.b, value: value.b)
end

def delete(key)
  Prolly::MutationRecord.new(kind: Prolly::MutationKind::DELETE, key: key.b, value: nil)
end

def assert_equal(expected, actual, label = 'value')
  return if expected == actual

  raise "#{label}: expected #{expected.inspect}, got #{actual.inspect}"
end

def filesystem_snapshot
  engine = Prolly::ProllyEngine.memory(Prolly.default_config)
  blob_store = Prolly::ProllyBlobStore.memory
  tree = engine.create
  { 'README.md' => "# Demo\n", 'src/lib.rs' => "pub fn answer() -> u8 { 42 }\n" }.each do |path, contents|
    tree = engine.put_large_value(
      blob_store,
      tree,
      "path/#{path}".b,
      contents.b,
      Prolly::LargeValueConfigRecord.new(inline_threshold: 4)
    )
  end
  engine.publish_named_root('refs/heads/main'.b, tree)
  loaded = engine.load_named_root('refs/heads/main'.b)
  assert_equal "# Demo\n".b, engine.get_large_value(blob_store, loaded, 'path/README.md'.b), 'README.md'

  puts 'filesystem_snapshot: published branch with blob-backed file contents'
end

filesystem_snapshot
