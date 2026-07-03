# frozen_string_literal: true

require 'json'
require 'prolly'
require 'tmpdir'

def assert_equal(expected, actual)
  return if expected == actual

  raise "expected #{expected.inspect}, got #{actual.inspect}"
end

def assert(condition, message)
  raise message unless condition
end

def assert_raises(expected_error)
  yield
rescue expected_error
  nil
else
  raise "expected #{expected_error}"
end

def fixture_path
  [
    File.expand_path('../../../conformance/prolly-fixtures.v1.json', __dir__),
    File.expand_path('../conformance/prolly-fixtures.v1.json', Dir.pwd)
  ].find { |path| File.exist?(path) } || raise('could not locate prolly-fixtures.v1.json')
end

def fixtures
  @fixtures ||= JSON.parse(File.read(fixture_path))
end

def hex(value)
  [value].pack('H*').b
end

def to_hex(value)
  value.nil? ? nil : value.unpack1('H*')
end

def config_from_fixture(fixture)
  Prolly::ConfigRecord.new(
    min_chunk_size: fixture.fetch('min_chunk_size'),
    max_chunk_size: fixture.fetch('max_chunk_size'),
    chunking_factor: fixture.fetch('chunking_factor'),
    hash_seed: fixture.fetch('hash_seed'),
    encoding: Prolly::EncodingRecord.new(
      kind: {
        'raw' => Prolly::EncodingKind::RAW,
        'cbor' => Prolly::EncodingKind::CBOR,
        'json' => Prolly::EncodingKind::JSON,
        'custom' => Prolly::EncodingKind::CUSTOM
      }.fetch(fixture.fetch('encoding').fetch('kind')),
      custom_name: fixture.fetch('encoding')['custom_name']
    ),
    node_cache_max_nodes: fixture['node_cache_max_nodes'],
    node_cache_max_bytes: fixture['node_cache_max_bytes']
  )
end

def build_tree(engine, entries)
  tree = engine.create
  entries.each do |entry|
    tree = engine.put(tree, hex(entry.fetch('key')), hex(entry.fetch('value')))
  end
  tree
end

def assert_entries(expected, actual)
  assert_equal expected.length, actual.length
  expected.each_with_index do |entry, index|
    assert_equal entry.fetch('key'), to_hex(actual[index].key)
    assert_equal entry.fetch('value'), to_hex(actual[index].value)
  end
end

class MemoryHostStore < Prolly::HostStoreCallback
  def initialize
    @nodes = {}
    @hints = {}
    @roots = {}
  end

  def get(key)
    Prolly::HostStoreBytesResultRecord.new(value: @nodes[key_for(key)]&.dup, error: nil)
  end

  def put(key, value)
    @nodes[key_for(key)] = value.dup.b
    unit
  end

  def delete(key)
    @nodes.delete(key_for(key))
    unit
  end

  def batch(ops)
    ops.each do |op|
      if op.kind == Prolly::MutationKind::UPSERT
        @nodes[key_for(op.key)] = op.value.dup.b
      else
        @nodes.delete(key_for(op.key))
      end
    end
    unit
  end

  def batch_get_ordered(keys)
    Prolly::HostStoreBatchGetResultRecord.new(
      values: keys.map { |key| @nodes[key_for(key)]&.dup },
      error: nil
    )
  end

  def prefers_batch_reads
    Prolly::HostStoreBoolResultRecord.new(value: true, error: nil)
  end

  def supports_hints
    Prolly::HostStoreBoolResultRecord.new(value: true, error: nil)
  end

  def get_hint(namespace, key)
    Prolly::HostStoreBytesResultRecord.new(
      value: @hints[[key_for(namespace), key_for(key)]]&.dup,
      error: nil
    )
  end

  def put_hint(namespace, key, value)
    @hints[[key_for(namespace), key_for(key)]] = value.dup.b
    unit
  end

  def list_node_cids
    Prolly::HostStoreListBytesResultRecord.new(values: @nodes.keys.map(&:dup), error: nil)
  end

  def get_root(name)
    Prolly::HostStoreRootResultRecord.new(value: @roots[key_for(name)], error: nil)
  end

  def put_root(name, manifest)
    @roots[key_for(name)] = manifest
    unit
  end

  def delete_root(name)
    @roots.delete(key_for(name))
    unit
  end

  def compare_and_swap_root(name, expected, replacement)
    root_name = key_for(name)
    current = @roots[root_name]
    if same_manifest?(current, expected)
      if replacement
        @roots[root_name] = replacement
      else
        @roots.delete(root_name)
      end
      return Prolly::HostStoreRootCasResultRecord.new(applied: true, current: nil, error: nil)
    end

    Prolly::HostStoreRootCasResultRecord.new(applied: false, current: current, error: nil)
  end

  def list_roots
    Prolly::HostStoreListRootsResultRecord.new(
      values: @roots.map do |name, manifest|
        Prolly::HostStoreNamedRootManifestRecord.new(name: name.dup, manifest: manifest)
      end,
      error: nil
    )
  end

  private

  def key_for(bytes)
    bytes.dup.b.freeze
  end

  def unit
    Prolly::HostStoreUnitResultRecord.new(error: nil)
  end

  def same_manifest?(left, right)
    return left.equal?(right) if left.nil? || right.nil?

    Prolly.root_manifest_to_bytes(left) == Prolly.root_manifest_to_bytes(right)
  end
end

engine = Prolly::ProllyEngine.memory(Prolly.default_config)
tree = engine.create
tree = engine.put(tree, 'a'.b, '1'.b)

assert_equal '1'.b, engine.get(tree, 'a'.b)

entries = engine.range(tree, ''.b, nil)
assert_equal 1, entries.length
assert_equal 'a'.b, entries.first.key
assert_equal '1'.b, entries.first.value

proof = engine.prove_key(tree, 'a'.b)
verified_proof = Prolly.verify_key_proof(proof)
assert_equal true, verified_proof.valid
assert_equal true, verified_proof.exists
assert_equal false, verified_proof.absence
assert_equal '1'.b, verified_proof.value

decoded_proof = Prolly.key_proof_from_node_bytes(
  proof.root,
  proof.key,
  Prolly.key_proof_path_node_bytes(proof)
)
assert_equal '1'.b, Prolly.verify_key_proof(decoded_proof).value
key_bundle = Prolly.key_proof_to_bytes(proof)
key_summary = Prolly.inspect_proof_bundle(key_bundle)
assert_equal 'key', key_summary.kind
assert_equal proof.root, key_summary.root
assert_equal 1, key_summary.key_count
assert_equal proof.path.length, key_summary.path_node_count
key_bundle_verified = Prolly.verify_proof_bundle(key_bundle)
assert_equal true, key_bundle_verified.valid
assert_equal 'key', key_bundle_verified.summary.kind
assert_equal 1, key_bundle_verified.exists_count
assert_equal 0, key_bundle_verified.absence_count
decoded_proof_from_bytes = Prolly.key_proof_from_bytes(key_bundle)
assert_equal '1'.b, Prolly.verify_key_proof(decoded_proof_from_bytes).value

missing_proof = Prolly.verify_key_proof(engine.prove_key(tree, 'missing'.b))
assert_equal true, missing_proof.valid
assert_equal false, missing_proof.exists
assert_equal true, missing_proof.absence

tampered_root = proof.root.dup
tampered_root.setbyte(0, tampered_root.getbyte(0) ^ 0x01)
tampered_proof = Prolly::KeyProofRecord.new(root: tampered_root, key: proof.key, path: proof.path)
assert_equal false, Prolly.verify_proof_bundle(Prolly.key_proof_to_bytes(tampered_proof)).valid
assert_equal false, Prolly.verify_key_proof(tampered_proof).valid

multi_proof = engine.prove_keys(tree, ['a'.b, 'missing'.b])
multi_verified = Prolly.verify_multi_key_proof(multi_proof)
assert_equal true, multi_verified.valid
assert_equal 2, multi_verified.results.length
assert_equal '1'.b, multi_verified.results[0].value
assert_equal true, multi_verified.results[1].absence

decoded_multi_proof = Prolly.multi_key_proof_from_node_bytes(
  multi_proof.root,
  multi_proof.keys,
  Prolly.multi_key_proof_path_node_bytes(multi_proof)
)
assert_equal '1'.b, Prolly.verify_multi_key_proof(decoded_multi_proof).results[0].value
decoded_multi_proof_from_bytes = Prolly.multi_key_proof_from_bytes(
  Prolly.multi_key_proof_to_bytes(multi_proof)
)
assert_equal '1'.b, Prolly.verify_multi_key_proof(decoded_multi_proof_from_bytes).results[0].value

range_proof = engine.prove_range(tree, 'a'.b, 'b'.b)
range_verified = Prolly.verify_range_proof(range_proof)
assert_equal true, range_verified.valid
assert_equal 1, range_verified.entries.length
assert_equal '1'.b, range_verified.entries[0].value
decoded_range_proof = Prolly.range_proof_from_node_bytes(
  range_proof.root,
  range_proof.start,
  range_proof._end,
  Prolly.range_proof_path_node_bytes(range_proof)
)
assert_equal '1'.b, Prolly.verify_range_proof(decoded_range_proof).entries[0].value
decoded_range_proof_from_bytes = Prolly.range_proof_from_bytes(
  Prolly.range_proof_to_bytes(range_proof)
)
assert_equal '1'.b, Prolly.verify_range_proof(decoded_range_proof_from_bytes).entries[0].value
prefix_proof = engine.prove_prefix(tree, 'a'.b)
prefix_verified = Prolly.verify_range_proof(prefix_proof)
assert_equal true, prefix_verified.valid
assert_equal 1, prefix_verified.entries.length
assert_equal '1'.b, prefix_verified.entries[0].value
proved_page = engine.prove_range_page(
  tree,
  nil,
  nil,
  1
)
page_verified = Prolly.verify_range_page_proof(proved_page.proof)
assert_equal true, page_verified.valid
assert_equal 1, page_verified.entries.length
assert_equal 'a'.b, page_verified.entries[0].key
decoded_page_proof = Prolly.range_page_proof_from_node_bytes(
  proved_page.proof.root,
  proved_page.proof.after,
  proved_page.proof._end,
  Prolly.range_page_proof_path_node_bytes(proved_page.proof)
)
assert_equal 'a'.b, Prolly.verify_range_page_proof(decoded_page_proof).entries[0].key
decoded_page_proof_from_bytes = Prolly.range_page_proof_from_bytes(
  Prolly.range_page_proof_to_bytes(proved_page.proof)
)
assert_equal 'a'.b, Prolly.verify_range_page_proof(decoded_page_proof_from_bytes).entries[0].key

other = engine.delete(tree, 'a'.b)
other = engine.put(other, 'b'.b, '22'.b)
other = engine.put(other, 'd'.b, '4'.b)
proved_diff_page = engine.prove_diff_page(tree, other, nil, nil, 1)
assert_equal 1, proved_diff_page.page.diffs.length
assert_equal Prolly::DiffKind::REMOVED, proved_diff_page.page.diffs[0].kind
assert_equal 'a'.b, proved_diff_page.page.diffs[0].key
assert_equal 'b'.b, proved_diff_page.proof.base._end
assert_equal 'b'.b, proved_diff_page.proof.lookahead_base.key
assert_equal 'a'.b, proved_diff_page.page.next_cursor.after_key

diff_page_verified = Prolly.verify_diff_page_proof(proved_diff_page.proof)
assert_equal true, diff_page_verified.valid
assert_equal true, diff_page_verified.lookahead_valid
assert_equal proved_diff_page.page.diffs, diff_page_verified.diffs
assert_equal proved_diff_page.page.next_cursor, diff_page_verified.next_cursor

diff_page_bundle = Prolly.diff_page_proof_to_bytes(proved_diff_page.proof)
assert_equal diff_page_bundle, Prolly.diff_page_proof_to_bytes(proved_diff_page.proof)
diff_page_summary = Prolly.inspect_proof_bundle(diff_page_bundle)
assert_equal 'diff_page', diff_page_summary.kind
assert_equal tree.root, diff_page_summary.root
assert_equal other.root, diff_page_summary.other_root
assert_equal 1, diff_page_summary.limit
assert_equal true, diff_page_summary.has_lookahead
diff_page_bundle_verified = Prolly.verify_proof_bundle(diff_page_bundle)
assert_equal true, diff_page_bundle_verified.valid
assert_equal 'diff_page', diff_page_bundle_verified.summary.kind
assert_equal 1, diff_page_bundle_verified.diff_count
assert_equal proved_diff_page.page.next_cursor, diff_page_bundle_verified.next_cursor
decoded_diff_page = Prolly.diff_page_proof_from_bytes(diff_page_bundle)
assert_equal proved_diff_page.page.diffs, Prolly.verify_diff_page_proof(decoded_diff_page).diffs

signed_envelope = Prolly.sign_proof_bundle_hmac_sha256(
  Prolly.key_proof_to_bytes(proof),
  'ruby-key'.b,
  'shared secret'.b,
  'tenant=t1'.b,
  1_700_000_000_000,
  1_700_000_100_000,
  'nonce-1'.b
)
signed_envelope_bytes = Prolly.authenticated_proof_envelope_to_bytes(signed_envelope)
assert_equal signed_envelope_bytes, Prolly.authenticated_proof_envelope_to_bytes(signed_envelope)
decoded_envelope = Prolly.authenticated_proof_envelope_from_bytes(signed_envelope_bytes)
envelope_verified = Prolly.verify_authenticated_proof_envelope(
  decoded_envelope,
  'shared secret'.b,
  1_700_000_050_000
)
assert_equal true, envelope_verified.valid
assert_equal true, envelope_verified.signature_valid
assert_equal 'ruby-key'.b, envelope_verified.key_id
assert_equal 'tenant=t1'.b, envelope_verified.context
assert_equal '1'.b, Prolly.verify_key_proof(
  Prolly.key_proof_from_bytes(envelope_verified.proof_bundle)
).value
authenticated_bundle = Prolly.verify_authenticated_proof_bundle(
  signed_envelope_bytes,
  'shared secret'.b,
  1_700_000_050_000
)
assert_equal true, authenticated_bundle.valid
assert_equal true, authenticated_bundle.envelope.valid
assert_equal nil, authenticated_bundle.proof_error
assert_equal 1, authenticated_bundle.proof.exists_count
wrong_envelope_secret = Prolly.verify_authenticated_proof_envelope(
  decoded_envelope,
  'wrong secret'.b,
  1_700_000_050_000
)
assert_equal false, wrong_envelope_secret.valid
wrong_authenticated_bundle = Prolly.verify_authenticated_proof_bundle(
  signed_envelope_bytes,
  'wrong secret'.b,
  1_700_000_050_000
)
assert_equal false, wrong_authenticated_bundle.valid
assert_equal false, wrong_authenticated_bundle.envelope.valid
assert_equal nil, wrong_authenticated_bundle.proof

async_engine = Prolly::AsyncEngine.memory(Prolly.default_config)
async_empty = async_engine.create.value
async_tree = async_engine.batch(
  async_empty,
  [
    Prolly::MutationRecord.new(kind: Prolly::MutationKind::UPSERT, key: 'a'.b, value: '1'.b),
    Prolly::MutationRecord.new(kind: Prolly::MutationKind::UPSERT, key: 'b'.b, value: '2'.b)
  ]
).value
assert_equal '1'.b, async_engine.get(async_tree, 'a'.b).value
assert_equal ['b'.b], async_engine.range_after(async_tree, 'a'.b, nil).value.map(&:key)
async_changed = async_engine.put(async_tree, 'b'.b, '22'.b).value
assert_equal 1, async_engine.diff(async_tree, async_changed).value.length
assert_equal ['b'.b, 'a'.b], async_engine.reverse_page(async_changed, nil, ''.b, 2).value.entries.map(&:key)
assert_equal ['a'.b], async_engine.prefix_reverse_page(async_changed, 'a'.b, nil, 2).value.entries.map(&:key)
async_blob_store = Prolly::AsyncBlobStore.memory
direct_ref = async_blob_store.put_blob('direct'.b).value
assert_equal 'direct'.b, async_blob_store.get_blob(direct_ref).value
assert_equal 1, async_blob_store.blob_count.value
async_blob_store.delete_blob(direct_ref).value
large_value = Array.new(32, 7).pack('C*').b
async_large_tree = async_engine.put_large_value(
  async_blob_store,
  async_empty,
  'big'.b,
  large_value,
  Prolly::LargeValueConfigRecord.new(inline_threshold: 8)
).value
assert_equal Prolly::ValueRefKind::BLOB, async_engine.get_value_ref(async_large_tree, 'big'.b).value.kind
assert_equal large_value, async_engine.get_large_value(async_blob_store, async_large_tree, 'big'.b).value
assert_equal 1, async_engine.plan_blob_store_gc(async_blob_store, [async_large_tree]).value.reachability.live_blob_count
async_engine.publish_named_root('async-main'.b, async_large_tree).value
assert async_engine.load_named_root('async-main'.b).value, 'expected async named root'
assert async_engine.collect_stats_json(async_large_tree).value.json.include?('"num_nodes"'), 'expected async stats JSON'
assert async_engine.mark_reachable([async_large_tree]).value.live_nodes.positive?, 'expected async reachability'

host_engine = Prolly::ProllyEngine.custom_store(MemoryHostStore.new, Prolly.default_config)
host_empty = host_engine.create
host_tree = host_engine.batch(
  host_empty,
  [
    Prolly::MutationRecord.new(kind: Prolly::MutationKind::UPSERT, key: 'a'.b, value: '1'.b),
    Prolly::MutationRecord.new(kind: Prolly::MutationKind::UPSERT, key: 'b'.b, value: '2'.b)
  ]
)
assert_equal '1'.b, host_engine.get(host_tree, 'a'.b)
assert_equal ['1'.b, nil, '2'.b], host_engine.get_many(host_tree, ['a'.b, 'missing'.b, 'b'.b])
assert_equal true, host_engine.publish_prefix_path_hint(host_tree, 'a'.b)
assert_equal true, host_engine.hydrate_prefix_path_hint(host_tree, 'a'.b)
host_engine.publish_named_root_at_millis('main'.b, host_tree, 7)
assert_equal host_tree, host_engine.load_named_root('main'.b)
assert_equal 1, host_engine.list_named_roots.length
assert host_engine.list_node_cids.any?, 'expected custom store node CIDs'
assert_equal 0, host_engine.plan_store_gc([host_tree]).reclaimable_nodes

host_destination = Prolly::ProllyEngine.custom_store(MemoryHostStore.new, Prolly.default_config)
missing_plan = host_engine.plan_missing_nodes(host_tree, host_destination)
assert missing_plan.missing_nodes.positive?, 'expected custom store missing nodes before sync'
copy_result = host_engine.copy_missing_nodes(host_tree, host_destination)
assert_equal missing_plan.missing_nodes, copy_result.copied_nodes
assert_equal '2'.b, host_destination.get(host_tree, 'b'.b)

host_update = host_engine.compare_and_swap_named_root('main'.b, host_tree, nil)
assert_equal true, host_update.applied
assert_equal false, host_update.conflict
assert_equal nil, host_engine.load_named_root('main'.b)

Dir.mktmpdir('prolly-ruby-') do |dir|
  file_path = File.join(dir, 'nodes')
  first_file = Prolly::ProllyEngine.file(file_path, Prolly.default_config)
  file_tree = first_file.put(first_file.create, 'k'.b, 'v'.b)
  first_file = nil
  GC.start
  reopened_file = Prolly::ProllyEngine.file(file_path, Prolly.default_config)
  assert_equal 'v'.b, reopened_file.get(file_tree, 'k'.b)

  sqlite_path = File.join(dir, 'prolly.db')
  first_sqlite = Prolly::ProllyEngine.sqlite(sqlite_path, Prolly.default_config)
  sqlite_tree = first_sqlite.put(first_sqlite.create, 'k'.b, 'v'.b)
  first_sqlite = nil
  GC.start
  reopened_sqlite = Prolly::ProllyEngine.sqlite(sqlite_path, Prolly.default_config)
  assert_equal 'v'.b, reopened_sqlite.get(sqlite_tree, 'k'.b)

  sqlite_memory = Prolly::ProllyEngine.sqlite_in_memory(Prolly.default_config)
  sqlite_memory_tree = sqlite_memory.put(sqlite_memory.create, 'transient'.b, 'ok'.b)
  assert_equal 'ok'.b, sqlite_memory.get(sqlite_memory_tree, 'transient'.b)
end

fixtures.fetch('node_fixtures').each do |fixture|
  bytes = hex(fixture.fetch('bytes'))
  node = Prolly.node_from_bytes(bytes)
  assert_equal fixture.fetch('bytes'), to_hex(Prolly.node_to_bytes(node))
  assert_equal fixture.fetch('cid'), to_hex(Prolly.node_cid(node))
  assert_equal fixture.fetch('cid'), to_hex(Prolly.cid_from_bytes(bytes))
end

fixtures.fetch('boundary_fixtures').each do |fixture|
  actual = Prolly.is_boundary_config(
    config_from_fixture(fixture.fetch('config')),
    fixture.fetch('count'),
    hex(fixture.fetch('key')),
    hex(fixture.fetch('value'))
  )
  assert_equal fixture.fetch('is_boundary'), actual
end

fixtures.fetch('key_fixtures').fetch('prefix_end').each do |fixture|
  prefix = hex(fixture.fetch('prefix'))
  assert_equal fixture['end'], to_hex(Prolly.prefix_end(prefix))
  bounds = Prolly.prefix_range(prefix)
  assert_equal fixture.fetch('prefix'), to_hex(bounds.start)
  assert_equal fixture['end'], to_hex(bounds._end)
end

fixtures.fetch('key_fixtures').fetch('numeric').each do |fixture|
  actual = case fixture.fetch('kind')
           when 'u64'
             Prolly.u64_key(fixture.fetch('value').to_i)
           when 'u128'
             Prolly.u128_key(fixture.fetch('value'))
           when 'i64'
             Prolly.i64_key(fixture.fetch('value').to_i)
           when 'i128'
             Prolly.i128_key(fixture.fetch('value'))
           when 'timestamp_millis'
             Prolly.timestamp_millis_key(fixture.fetch('value').to_i)
           end
  assert_equal fixture.fetch('encoded'), to_hex(actual) if actual
end

fixtures.fetch('key_fixtures').fetch('segments').each do |fixture|
  segment_bytes = fixture.fetch('segments').map { |segment| hex(segment) }
  encoded = segment_bytes.map { |segment| Prolly.encode_segment(segment) }.join
  assert_equal fixture.fetch('encoded'), to_hex(encoded)
  assert_equal fixture.fetch('encoded'), to_hex(Prolly.key_from_segments(segment_bytes))
  prefix = Prolly.key_from_segments(segment_bytes.first(1))
  assert_equal fixture.fetch('encoded'), to_hex(Prolly.key_from_prefixed_segments(prefix, segment_bytes.drop(1)))
  assert_equal fixture.fetch('decoded'), Prolly.decode_segments(hex(fixture.fetch('encoded'))).map { |segment| to_hex(segment) }
end

fixtures.fetch('key_fixtures').fetch('debug').each do |fixture|
  assert_equal fixture.fetch('debug'), Prolly.debug_key(hex(fixture.fetch('key')))
end

fixtures.fetch('tree_fixtures').each do |fixture|
  tree_engine = Prolly::ProllyEngine.memory(config_from_fixture(fixture.fetch('config')))
  tree = build_tree(tree_engine, fixture.fetch('entries'))
  assert_equal fixture.fetch('root'), to_hex(tree.root)

  fixture.fetch('lookups').each do |lookup|
    assert_equal lookup['value'], to_hex(tree_engine.get(tree, hex(lookup.fetch('key'))))
  end

  fixture.fetch('ranges').each do |range_fixture|
    actual = tree_engine.range(
      tree,
      hex(range_fixture.fetch('start')),
      range_fixture['end'].nil? ? nil : hex(range_fixture.fetch('end'))
    )
    assert_entries range_fixture.fetch('entries'), actual
  end
end

diff_fixture = fixtures.fetch('diff_fixtures').first
diff_engine = Prolly::ProllyEngine.memory(config_from_fixture(diff_fixture.fetch('config')))
base = build_tree(
  diff_engine,
  [
    { 'key' => '61', 'value' => '31' },
    { 'key' => '62', 'value' => '32' },
    { 'key' => '63', 'value' => '33' }
  ]
)
other = build_tree(
  diff_engine,
  [
    { 'key' => '61', 'value' => '31' },
    { 'key' => '62', 'value' => '3232' },
    { 'key' => '64', 'value' => '34' }
  ]
)
assert_equal diff_fixture.fetch('base_root'), to_hex(base.root)
assert_equal diff_fixture.fetch('other_root'), to_hex(other.root)

diff_kind = {
  Prolly::DiffKind::ADDED => 'added',
  Prolly::DiffKind::REMOVED => 'removed',
  Prolly::DiffKind::CHANGED => 'changed'
}
actual_diffs = diff_engine.diff(base, other)
assert_equal diff_fixture.fetch('diffs').length, actual_diffs.length
diff_fixture.fetch('diffs').each_with_index do |expected, index|
  actual = actual_diffs[index]
  assert_equal expected.fetch('kind'), diff_kind.fetch(actual.kind)
  assert_equal expected.fetch('key'), to_hex(actual.key)
  assert_equal expected['value'], to_hex(actual.value)
  assert_equal expected['old'], to_hex(actual.old_value)
  assert_equal expected['new'], to_hex(actual.new_value)
end

assert_equal Prolly::EncodingKind::RAW, Prolly.encoding_raw.kind
assert_equal Prolly::EncodingKind::CBOR, Prolly.encoding_cbor.kind
assert_equal Prolly::EncodingKind::JSON, Prolly.encoding_json.kind
custom_encoding = Prolly.encoding_custom('postcard')
assert_equal Prolly::EncodingKind::CUSTOM, custom_encoding.kind
assert_equal 'postcard', custom_encoding.custom_name
constructed_config = Prolly.tree_config(2, 64, 32, 7, custom_encoding, 16, 4096)
assert_equal custom_encoding, constructed_config.encoding
assert_equal 16, constructed_config.node_cache_max_nodes
assert_raises(Prolly::ProllyBindingError::InvalidArgument) do
  Prolly.tree_config(
    2,
    64,
    32,
    7,
    Prolly::EncodingRecord.new(kind: Prolly::EncodingKind::CUSTOM, custom_name: nil),
    nil,
    nil
  )
end
assert_equal 8, Prolly.large_value_config(8).inline_threshold
assert_equal 2, Prolly.parallel_config(2, 24).max_threads
assert_equal 1, Prolly.parallel_config_sequential.max_threads

fixtures.fetch('value_fixtures').each do |fixture|
  bytes = hex(fixture.fetch('bytes'))
  record = Prolly.versioned_value_from_bytes(bytes)
  schema = fixture.fetch('schema_name')
  version = fixture.fetch('version')
  assert_equal fixture.fetch('bytes'), to_hex(Prolly.versioned_value_to_bytes(record))
  assert_equal true, Prolly.versioned_value_matches_schema(record, schema, version)
  assert_equal false, Prolly.versioned_value_matches_schema(record, schema, version + 1)
  Prolly.versioned_value_require_schema(record, schema, version)
  assert_raises(Prolly::ProllyBindingError::Serialization) do
    Prolly.versioned_value_require_schema(record, schema, version + 1)
  end
  assert_equal true, Prolly.versioned_value_bytes_matches_schema(bytes, schema, version)
  assert_equal false, Prolly.versioned_value_bytes_matches_schema(bytes, schema, version + 1)
  Prolly.versioned_value_bytes_require_schema(bytes, schema, version)
  assert_raises(Prolly::ProllyBindingError::Serialization) do
    Prolly.versioned_value_bytes_require_schema(bytes, schema, version + 1)
  end
end

fixtures.fetch('blob_fixtures').each do |fixture|
  bytes = hex(fixture.fetch('bytes'))
  assert_equal fixture.fetch('bytes'), to_hex(Prolly.value_ref_to_bytes(Prolly.value_ref_from_bytes(bytes)))
  expected_kind = fixture.fetch('kind') == 'inline' ? Prolly::ValueRefKind::INLINE : Prolly::ValueRefKind::BLOB
  assert_equal expected_kind, Prolly.value_ref_from_stored_bytes(bytes).kind
  assert_equal true, Prolly.value_ref_inline_requires_escape(bytes)
end
raw_value_ref = Prolly.value_ref_from_stored_bytes('plain'.b)
assert_equal Prolly::ValueRefKind::INLINE, raw_value_ref.kind
assert_equal 'plain'.b, raw_value_ref.value
assert_equal false, Prolly.value_ref_inline_requires_escape('plain'.b)

fixtures.fetch('manifest_fixtures').each do |fixture|
  bytes = hex(fixture.fetch('bytes'))
  assert_equal fixture.fetch('bytes'), to_hex(Prolly.root_manifest_to_bytes(Prolly.root_manifest_from_bytes(bytes)))
end

parity_engine = Prolly::ProllyEngine.memory(Prolly.default_config)
empty = parity_engine.create
batched = parity_engine.batch(
  empty,
  [
    Prolly::MutationRecord.new(kind: Prolly::MutationKind::UPSERT, key: 'a'.b, value: '1'.b),
    Prolly::MutationRecord.new(kind: Prolly::MutationKind::UPSERT, key: 'b'.b, value: '2'.b),
    Prolly::MutationRecord.new(kind: Prolly::MutationKind::UPSERT, key: 'a'.b, value: '11'.b),
    Prolly::MutationRecord.new(kind: Prolly::MutationKind::DELETE, key: 'missing'.b, value: nil)
  ]
)
assert_equal ['11'.b, nil, '2'.b], parity_engine.get_many(batched, ['a'.b, 'missing'.b, 'b'.b])
assert_equal 'a'.b, parity_engine.first_entry(batched).key
assert_equal '11'.b, parity_engine.first_entry(batched).value
assert_equal 'b'.b, parity_engine.last_entry(batched).key
assert_equal 'b'.b, parity_engine.lower_bound(batched, 'aa'.b).key
assert_equal nil, parity_engine.upper_bound(batched, 'b'.b)
assert_equal ['11'.b], parity_engine.prefix(batched, 'a'.b).map(&:value)
prefix_page = parity_engine.prefix_page(batched, 'a'.b, nil, 1)
assert_equal ['11'.b], prefix_page.entries.map(&:value)
assert prefix_page.next_cursor

built = parity_engine.build_from_entries(
  [
    Prolly::EntryRecord.new(key: 'c'.b, value: '3'.b),
    Prolly::EntryRecord.new(key: 'a'.b, value: '1'.b),
    Prolly::EntryRecord.new(key: 'b'.b, value: '2'.b)
  ]
)
sorted_built = parity_engine.build_from_sorted_entries(
  [
    Prolly::EntryRecord.new(key: 'a'.b, value: '1'.b),
    Prolly::EntryRecord.new(key: 'b'.b, value: '2'.b),
    Prolly::EntryRecord.new(key: 'c'.b, value: '3'.b)
  ]
)
assert_equal built.root, sorted_built.root
begin
  parity_engine.build_from_sorted_entries(
    [
      Prolly::EntryRecord.new(key: 'b'.b, value: '2'.b),
      Prolly::EntryRecord.new(key: 'a'.b, value: '1'.b)
    ]
  )
  raise 'expected sorted build to reject out-of-order keys'
rescue Prolly::ProllyBindingError::InvalidArgument
  # expected
end
assert_equal Prolly::MutationKind::UPSERT, Prolly.upsert_mutation('probe'.b, 'value'.b).kind
assert_equal Prolly::MutationKind::DELETE, Prolly.delete_mutation('probe'.b).kind
batch_stats = parity_engine.batch_with_stats(
  empty,
  [
    Prolly.upsert_mutation('b'.b, '2'.b),
    Prolly.upsert_mutation('a'.b, '1'.b),
    Prolly.upsert_mutation('a'.b, '11'.b)
  ]
)
assert_equal '11'.b, parity_engine.get(batch_stats.tree, 'a'.b)
assert_equal 3, batch_stats.stats.input_mutations
assert_equal 2, batch_stats.stats.effective_mutations
assert_equal false, batch_stats.stats.preprocess_input_sorted

default_parallel_config = Prolly.default_parallel_config
assert_equal 100, default_parallel_config.parallelism_threshold
parallel_config = Prolly::ParallelConfigRecord.new(max_threads: 1, parallelism_threshold: 1)
parallel_tree = parity_engine.parallel_batch(
  empty,
  [
    Prolly::MutationRecord.new(kind: Prolly::MutationKind::UPSERT, key: 'p'.b, value: 'parallel'.b),
    Prolly::MutationRecord.new(kind: Prolly::MutationKind::UPSERT, key: 'q'.b, value: 'ruby'.b)
  ],
  parallel_config
)
assert_equal 'ruby'.b, parity_engine.get(parallel_tree, 'q'.b)
parallel_stats = parity_engine.parallel_batch_with_stats(
  empty,
  [
    Prolly::MutationRecord.new(kind: Prolly::MutationKind::UPSERT, key: 'r'.b, value: 'route'.b),
    Prolly::MutationRecord.new(kind: Prolly::MutationKind::UPSERT, key: 's'.b, value: 'stats'.b)
  ],
  parallel_config
)
assert_equal 'stats'.b, parity_engine.get(parallel_stats.tree, 's'.b)
assert_equal 2, parallel_stats.stats.input_mutations
assert_equal 2, parallel_stats.stats.effective_mutations
assert parallel_stats.stats.written_nodes.positive?

appended = parity_engine.append_batch(
  built,
  [
    Prolly::MutationRecord.new(kind: Prolly::MutationKind::UPSERT, key: 'd'.b, value: '4'.b),
    Prolly::MutationRecord.new(kind: Prolly::MutationKind::UPSERT, key: 'e'.b, value: '5'.b),
    Prolly::MutationRecord.new(kind: Prolly::MutationKind::UPSERT, key: 'd'.b, value: '44'.b)
  ]
)
assert_equal '44'.b, parity_engine.get(appended, 'd'.b)
appended_stats = parity_engine.append_batch_with_stats(
  built,
  [
    Prolly::MutationRecord.new(kind: Prolly::MutationKind::UPSERT, key: 'd'.b, value: '4'.b),
    Prolly::MutationRecord.new(kind: Prolly::MutationKind::UPSERT, key: 'e'.b, value: '5'.b),
    Prolly::MutationRecord.new(kind: Prolly::MutationKind::UPSERT, key: 'd'.b, value: '44'.b)
  ]
)
assert_equal '44'.b, parity_engine.get(appended_stats.tree, 'd'.b)
assert_equal 3, appended_stats.stats.input_mutations
assert_equal 2, appended_stats.stats.effective_mutations
assert_equal false, appended_stats.stats.preprocess_input_sorted
assert_equal true, appended_stats.stats.used_append_fast_path
assert appended_stats.stats.written_nodes.positive?, 'expected append stats to include written nodes'

first_page = parity_engine.range_page(batched, nil, nil, 1)
assert_equal 1, first_page.entries.length
assert_equal 'a'.b, first_page.entries.first.key
assert first_page.next_cursor, 'expected a next range cursor'

after_a = parity_engine.range_after(batched, 'a'.b, nil)
assert_equal ['b'.b], after_a.map(&:key)
assert_equal nil, Prolly.range_cursor_start.after_key
after_a_cursor = Prolly.range_cursor_after_key('a'.b)
assert_equal 'a'.b, after_a_cursor.after_key
from_cursor = parity_engine.range_from_cursor(
  batched,
  after_a_cursor,
  nil
)
assert_equal after_a.map(&:key), from_cursor.map(&:key)

window = parity_engine.cursor_window(batched, 'aa'.b, nil, 1)
assert_equal 'a'.b, window.position_key
assert_equal '11'.b, window.position_value
assert_equal false, window.found
assert_equal ['b'.b], window.entries.map(&:key)
assert_equal 'b'.b, window.next_cursor.after_key

exact_probe = parity_engine.cursor_window(batched, 'a'.b, nil, 0)
assert_equal true, exact_probe.found
assert_equal 'a'.b, exact_probe.position_key
assert_equal [], exact_probe.entries
assert_equal nil, exact_probe.next_cursor

second_page = parity_engine.range_page(batched, first_page.next_cursor, nil, 1)
assert_equal 1, second_page.entries.length
assert_equal 'b'.b, second_page.entries.first.key
unless second_page.next_cursor.nil?
  third_page = parity_engine.range_page(batched, second_page.next_cursor, nil, 1)
  assert_equal 0, third_page.entries.length
  assert_equal nil, third_page.next_cursor
end

assert_equal nil, Prolly.reverse_cursor_end.before_key
before_c_cursor = Prolly.reverse_cursor_before_key('c'.b)
assert_equal 'c'.b, before_c_cursor.before_key
reverse_first = parity_engine.reverse_page(built, nil, ''.b, 2)
assert_equal ['c'.b, 'b'.b], reverse_first.entries.map(&:key)
assert_equal 'b'.b, reverse_first.next_cursor.before_key
reverse_second = parity_engine.reverse_page(built, reverse_first.next_cursor, ''.b, 2)
assert_equal ['a'.b], reverse_second.entries.map(&:key)
assert_equal nil, reverse_second.next_cursor
prefix_reverse = parity_engine.prefix_reverse_page(built, 'b'.b, nil, 8)
assert_equal ['b'.b], prefix_reverse.entries.map(&:key)
assert_equal nil, prefix_reverse.next_cursor

changed = parity_engine.put(batched, 'b'.b, '22'.b)
diff_page = parity_engine.diff_page(batched, changed, nil, nil, 1)
assert_equal 1, diff_page.diffs.length
assert_equal Prolly::DiffKind::CHANGED, diff_page.diffs.first.kind
unless diff_page.next_cursor.nil?
  second_diff_page = parity_engine.diff_page(batched, changed, diff_page.next_cursor, nil, 1)
  assert_equal 0, second_diff_page.diffs.length
  assert_equal nil, second_diff_page.next_cursor
end

changed_for_cursor = parity_engine.batch(
  built,
  [
    Prolly::MutationRecord.new(kind: Prolly::MutationKind::UPSERT, key: 'b'.b, value: '22'.b),
    Prolly::MutationRecord.new(kind: Prolly::MutationKind::UPSERT, key: 'c'.b, value: '33'.b)
  ]
)
resumed_diffs = parity_engine.diff_from_cursor(
  built,
  changed_for_cursor,
  Prolly::RangeCursorRecord.new(after_key: 'a'.b),
  'c'.b
)
assert_equal [[Prolly::DiffKind::CHANGED, 'b'.b]], resumed_diffs.map { |diff| [diff.kind, diff.key] }

conflict_base = parity_engine.batch(
  empty,
  [
    Prolly::MutationRecord.new(kind: Prolly::MutationKind::UPSERT, key: 'a'.b, value: 'base-a'.b),
    Prolly::MutationRecord.new(kind: Prolly::MutationKind::UPSERT, key: 'b'.b, value: 'base-b'.b)
  ]
)
conflict_left = parity_engine.batch(
  conflict_base,
  [
    Prolly::MutationRecord.new(kind: Prolly::MutationKind::UPSERT, key: 'a'.b, value: 'left-a'.b),
    Prolly::MutationRecord.new(kind: Prolly::MutationKind::UPSERT, key: 'b'.b, value: 'left-b'.b)
  ]
)
conflict_right = parity_engine.batch(
  conflict_base,
  [
    Prolly::MutationRecord.new(kind: Prolly::MutationKind::UPSERT, key: 'a'.b, value: 'right-a'.b),
    Prolly::MutationRecord.new(kind: Prolly::MutationKind::UPSERT, key: 'b'.b, value: 'right-b'.b)
  ]
)
conflict_page = parity_engine.conflict_page(conflict_base, conflict_left, conflict_right, nil, 1)
assert_equal 1, conflict_page.conflicts.length
assert_equal 'a'.b, conflict_page.conflicts.first.key
assert_equal 'base-a'.b, conflict_page.conflicts.first.base
assert_equal 'left-a'.b, conflict_page.conflicts.first.left
assert_equal 'right-a'.b, conflict_page.conflicts.first.right
assert conflict_page.next_cursor, 'expected a next conflict cursor'

second_conflict_page = parity_engine.conflict_page(
  conflict_base,
  conflict_left,
  conflict_right,
  conflict_page.next_cursor,
  1
)
assert_equal 1, second_conflict_page.conflicts.length
assert_equal 'b'.b, second_conflict_page.conflicts.first.key
assert_equal nil, second_conflict_page.next_cursor

base = parity_engine.put(empty, 'k'.b, 'base'.b)
left = parity_engine.put(base, 'k'.b, 'left'.b)
right = parity_engine.put(base, 'k'.b, 'right'.b)
explanation = parity_engine.merge_explain(base, left, right, 'prefer_right')
assert explanation.result, 'expected merge explanation to include a result'
assert_equal nil, explanation.error
assert explanation.trace_json.include?('events'), 'expected merge trace JSON to include events'
assert explanation.trace.events.any? { |event|
  event.kind == Prolly::MergeTraceEventKind::RESOLVER_CALLED &&
    event.resolution == Prolly::MergeTraceResolutionKind::VALUE
}, 'expected typed merge trace to include a resolver-called value event'

merged = parity_engine.merge(base, left, right, 'prefer_right')
assert_equal 'right'.b, parity_engine.get(merged, 'k'.b)
merged_range = parity_engine.merge_range(base, left, right, 'k'.b, nil, 'prefer_right')
assert_equal 'right'.b, parity_engine.get(merged_range, 'k'.b)
merged_prefix = parity_engine.merge_prefix(base, left, right, 'k'.b, 'prefer_right')
assert_equal 'right'.b, parity_engine.get(merged_prefix, 'k'.b)

class JoinResolver < Prolly::MergeResolverCallback
  def resolve(conflict)
    if conflict.left && conflict.right
      return Prolly.resolution_value(conflict.left + '|'.b + conflict.right)
    end
    if conflict.left
      return Prolly.resolution_value(conflict.left)
    end
    if conflict.right
      return Prolly.resolution_value(conflict.right)
    end

    Prolly.resolution_delete
  end
end
assert_equal Prolly::ResolutionKind::UNRESOLVED, Prolly.resolution_unresolved.kind

update_conflict = Prolly::ConflictRecord.new(key: 'k'.b, base: 'base'.b, left: 'left'.b, right: 'right'.b)
assert_equal 'left'.b, Prolly.resolve_prefer_left(update_conflict).value
assert_equal Prolly::ResolutionKind::UNRESOLVED, Prolly.resolve_delete_wins(update_conflict).kind
delete_conflict = Prolly::ConflictRecord.new(key: 'k'.b, base: 'base'.b, left: nil, right: 'right'.b)
assert_equal Prolly::ResolutionKind::DELETE, Prolly.resolve_delete_wins(delete_conflict).kind
assert_equal 'right'.b, Prolly.resolve_update_wins(delete_conflict).value

class PreferRightResolver < Prolly::MergeResolverCallback
  def resolve(conflict)
    Prolly.resolve_prefer_right(conflict)
  end
end

prefer_right_merged = parity_engine.merge_with_resolver(base, left, right, PreferRightResolver.new)
assert_equal 'right'.b, parity_engine.get(prefer_right_merged, 'k'.b)
join_resolver = JoinResolver.new
callback_merged = parity_engine.merge_with_resolver(base, left, right, join_resolver)
assert_equal 'left|right'.b, parity_engine.get(callback_merged, 'k'.b)
callback_explanation = parity_engine.merge_explain_with_resolver(base, left, right, join_resolver)
assert callback_explanation.result, 'expected callback merge explanation to include a result'
assert_equal nil, callback_explanation.error
callback_range = parity_engine.merge_range_with_resolver(base, left, right, 'k'.b, nil, join_resolver)
assert_equal 'left|right'.b, parity_engine.get(callback_range, 'k'.b)
callback_prefix = parity_engine.merge_prefix_with_resolver(base, left, right, 'k'.b, join_resolver)
assert_equal 'left|right'.b, parity_engine.get(callback_prefix, 'k'.b)

policy_base = parity_engine.batch(
  empty,
  [
    Prolly::MutationRecord.new(kind: Prolly::MutationKind::UPSERT, key: 'doc/title'.b, value: 'base-title'.b),
    Prolly::MutationRecord.new(kind: Prolly::MutationKind::UPSERT, key: 'k'.b, value: 'base-k'.b)
  ]
)
policy_left = parity_engine.batch(
  policy_base,
  [
    Prolly::MutationRecord.new(kind: Prolly::MutationKind::UPSERT, key: 'doc/title'.b, value: 'left-title'.b),
    Prolly::MutationRecord.new(kind: Prolly::MutationKind::UPSERT, key: 'k'.b, value: 'left-k'.b)
  ]
)
policy_right = parity_engine.batch(
  policy_base,
  [
    Prolly::MutationRecord.new(kind: Prolly::MutationKind::UPSERT, key: 'doc/title'.b, value: 'right-title'.b),
    Prolly::MutationRecord.new(kind: Prolly::MutationKind::UPSERT, key: 'k'.b, value: 'right-k'.b)
  ]
)
policy = Prolly::MergePolicyRegistry.new
assert_equal true, policy.is_empty
assert_equal false, policy.has_default
policy.set_default_resolver_name('prefer_left')
policy.push_prefix_resolver('doc/'.b, join_resolver)
policy.push_exact_resolver_name('k'.b, 'prefer_right')
assert_equal 2, policy.len
assert_equal true, policy.has_default
policy_merged = parity_engine.merge_with_policy(policy_base, policy_left, policy_right, policy)
assert_equal 'left-title|right-title'.b, parity_engine.get(policy_merged, 'doc/title'.b)
assert_equal 'right-k'.b, parity_engine.get(policy_merged, 'k'.b)
policy_explanation = parity_engine.merge_explain_with_policy(policy_base, policy_left, policy_right, policy)
assert policy_explanation.result, 'expected policy merge explanation to include a result'
assert_equal nil, policy_explanation.error
policy_range = parity_engine.merge_range_with_policy(policy_base, policy_left, policy_right, 'doc/'.b, 'doc0'.b, policy)
assert_equal 'left-title|right-title'.b, parity_engine.get(policy_range, 'doc/title'.b)
policy_prefix = parity_engine.merge_prefix_with_policy(policy_base, policy_left, policy_right, 'doc/'.b, policy)
assert_equal 'left-title|right-title'.b, parity_engine.get(policy_prefix, 'doc/title'.b)

parity_engine.publish_named_root_at_millis('main'.b, merged, 42)
assert parity_engine.load_named_root('main'.b), 'expected named root to load'
assert_equal 1, parity_engine.list_named_roots.length
manifests = parity_engine.list_named_root_manifests
assert_equal 1, manifests.length
assert_equal 'main'.b, manifests[0].name
assert_equal merged.root, manifests[0].manifest.tree.root
assert_equal 42, manifests[0].manifest.created_at_millis
assert_equal 42, manifests[0].manifest.updated_at_millis

selection = parity_engine.load_named_roots(['main'.b, 'missing'.b])
assert_equal 1, selection.roots.length
assert_equal 1, selection.missing_names.length

retention_all = Prolly.retain_all_named_roots
assert_equal Prolly::NamedRootRetentionKind::ALL, retention_all.kind
retention_exact = Prolly.retain_exact_named_roots(['main'.b, 'missing'.b])
assert_equal Prolly::NamedRootRetentionKind::EXACT, retention_exact.kind
assert_equal 2, retention_exact.names.length
retention_prefix = Prolly.retain_named_root_prefix('ma'.b)
assert_equal Prolly::NamedRootRetentionKind::PREFIX, retention_prefix.kind
assert_equal 'ma'.b, retention_prefix.prefix
retention_newest = Prolly.retain_newest_named_roots('checkpoint/'.b, 2)
assert_equal Prolly::NamedRootRetentionKind::NEWEST_BY_NAME, retention_newest.kind
assert_equal 'checkpoint/'.b, retention_newest.prefix
assert_equal 2, retention_newest.count
retention_updated = Prolly.retain_named_roots_updated_since('checkpoint/'.b, 42)
assert_equal Prolly::NamedRootRetentionKind::UPDATED_SINCE, retention_updated.kind
assert_equal 'checkpoint/'.b, retention_updated.prefix
assert_equal 42, retention_updated.min_updated_at_millis

retained = parity_engine.load_retained_named_roots(retention_all)
assert_equal 1, retained.roots.length
assert_equal 1, parity_engine.plan_store_gc_for_retention(retention_all).reachability.live_nodes
assert_equal 1, parity_engine.sweep_store_gc_for_retention(retention_all).plan.reachability.live_nodes

update = parity_engine.compare_and_swap_named_root('main'.b, merged, nil)
assert_equal true, update.applied
assert_equal false, update.conflict
assert_equal nil, parity_engine.load_named_root('main'.b)

crdt_engine = Prolly::ProllyEngine.memory(Prolly.default_config)
crdt_empty = crdt_engine.create
base_value = Prolly.timestamped_value_to_bytes(
  Prolly::TimestampedValueRecord.new(value: 'base'.b, timestamp: 1)
)
left_value = Prolly.timestamped_value_to_bytes(
  Prolly::TimestampedValueRecord.new(value: 'left'.b, timestamp: 2)
)
right_value = Prolly.timestamped_value_to_bytes(
  Prolly::TimestampedValueRecord.new(value: 'right'.b, timestamp: 3)
)
crdt_base = crdt_engine.put(crdt_empty, 'k'.b, base_value)
crdt_left = crdt_engine.put(crdt_base, 'k'.b, left_value)
crdt_right = crdt_engine.put(crdt_base, 'k'.b, right_value)

lww_config = Prolly.crdt_config_lww(Prolly::CrdtDeletePolicyKind::UPDATE_WINS)
assert_equal Prolly::CrdtMergeStrategyKind::LAST_WRITER_WINS, lww_config.strategy
assert_equal Prolly::CrdtDeletePolicyKind::UPDATE_WINS, lww_config.delete_policy
crdt_merged = crdt_engine.crdt_merge(crdt_base, crdt_left, crdt_right, lww_config)
merged_value = Prolly.timestamped_value_from_bytes(crdt_engine.get(crdt_merged, 'k'.b))
assert_equal 'right'.b, merged_value.value
assert_equal 3, merged_value.timestamp

class CrdtJoinResolver < Prolly::CrdtResolverCallback
  def resolve(conflict)
    if conflict.left && conflict.right
      return Prolly.crdt_resolution_value(conflict.left + '|'.b + conflict.right)
    end
    if conflict.left
      return Prolly.crdt_resolution_value(conflict.left)
    end
    if conflict.right
      return Prolly.crdt_resolution_value(conflict.right)
    end

    Prolly.crdt_resolution_delete
  end
end

crdt_callback_merged = crdt_engine.crdt_merge_with_resolver(
  crdt_base,
  crdt_left,
  crdt_right,
  Prolly::CrdtDeletePolicyKind::UPDATE_WINS,
  CrdtJoinResolver.new
)
assert_equal left_value + '|'.b + right_value, crdt_engine.get(crdt_callback_merged, 'k'.b)

now_value = Prolly.timestamped_value_now('now'.b)
assert_equal 'now'.b, now_value.value
assert now_value.timestamp.positive?, 'expected current timestamp'

multi_config = Prolly.crdt_config_multi_value(Prolly::CrdtDeletePolicyKind::DELETE_WINS)
assert_equal Prolly::CrdtMergeStrategyKind::MULTI_VALUE, multi_config.strategy
assert_equal Prolly::CrdtDeletePolicyKind::DELETE_WINS, multi_config.delete_policy
decoded_set = Prolly.multi_value_set_from_bytes(
  Prolly.multi_value_set_to_bytes(['b'.b, 'a'.b, 'a'.b])
)
assert_equal ['a'.b, 'b'.b], decoded_set
merged_set = Prolly.multi_value_set_merge(['b'.b], ['a'.b, 'b'.b])
assert_equal ['a'.b, 'b'.b], merged_set

tombstone = Prolly::TombstoneRecord.new(
  actor: 'actor'.b,
  timestamp_millis: 7,
  causal_metadata: [
    Prolly::TombstoneMetadataRecord.new(key: 'clock', value: '7'.b)
  ]
)
tombstone_bytes = Prolly.tombstone_to_bytes(tombstone)
assert_equal true, Prolly.is_tombstone_value(tombstone_bytes)
assert_equal 7, Prolly.tombstone_from_bytes(tombstone_bytes).timestamp_millis
assert_equal 'clock', Prolly.tombstone_from_stored_bytes(tombstone_bytes).causal_metadata.first.key

upsert = Prolly.tombstone_upsert_mutation('deleted'.b, tombstone)
assert_equal Prolly::MutationKind::UPSERT, upsert.kind
assert_equal 'deleted'.b, upsert.key
assert upsert.value, 'expected tombstone upsert value'

compaction = Prolly.tombstone_compaction_mutation('deleted'.b, tombstone_bytes)
assert_equal Prolly::MutationKind::DELETE, compaction.kind
assert_equal 'deleted'.b, compaction.key
assert_equal nil, compaction.value

ops_engine = Prolly::ProllyEngine.memory(Prolly.default_config)
ops_empty = ops_engine.create
ops_tree = ops_engine.put(ops_empty, 'k'.b, 'v'.b)

assert ops_engine.collect_stats_json(ops_tree).json.include?('"num_nodes"'), 'expected stats JSON'
ops_typed_stats = ops_engine.collect_stats(ops_tree)
assert_equal 1, ops_typed_stats.total_key_value_pairs
assert ops_typed_stats.nodes_per_level.any? { |level| level.level == 0 && level.value.positive? }, 'expected level stats'
assert ops_engine.stats_diff_json(ops_empty, ops_tree).json.include?('"absolute"'), 'expected stats diff JSON'
ops_typed_diff = ops_engine.stats_diff(ops_empty, ops_tree)
assert_equal 0, ops_typed_diff.before.total_key_value_pairs
assert_equal 1, ops_typed_diff.after.total_key_value_pairs
assert_equal 1, ops_typed_diff.absolute.total_key_value_pairs_diff
assert ops_engine.debug_tree_json(ops_tree).json.include?('"levels"'), 'expected debug tree JSON'
ops_debug_tree = ops_engine.debug_tree(ops_tree)
assert ops_debug_tree.levels.any? { |level| !level.nodes.empty? }, 'expected typed debug tree nodes'
assert ops_engine.debug_tree_text(ops_tree).include?('level'), 'expected debug tree text'
assert ops_engine.debug_compare_trees_json(ops_empty, ops_tree).json.include?('"right_only_nodes"'), 'expected compare JSON'
ops_debug_compare = ops_engine.debug_compare_trees(ops_empty, ops_tree)
assert_equal 0, ops_debug_compare.left_only_nodes
assert ops_debug_compare.right_only_nodes.positive?, 'expected right-only debug nodes'
assert ops_debug_compare.levels.any? { |level| level.nodes.any? { |node| node.status == Prolly::TreeDebugNodeStatusKind::RIGHT_ONLY } }, 'expected typed right-only debug node'
assert ops_engine.debug_compare_trees_text(ops_empty, ops_tree).include?('right'), 'expected compare text'

assert ops_engine.pin_tree_path(ops_tree, 'k'.b).positive?, 'expected pinned path count'
assert ops_engine.unpin_all_cache_nodes >= 0, 'expected unpin count'
assert ops_engine.pin_tree_root(ops_tree).positive?, 'expected pinned root count'
assert ops_engine.cache_stats.cached_nodes.positive?, 'expected cached nodes'
assert ops_engine.unpin_all_cache_nodes >= 0, 'expected unpin count'
ops_engine.clear_cache

assert ops_engine.metrics.nodes_written.positive?, 'expected write metrics'
ops_engine.reset_metrics
assert_equal 0, ops_engine.metrics.nodes_written

assert_equal false, ops_engine.publish_prefix_path_hint(ops_tree, 'k'.b)
assert_equal false, ops_engine.hydrate_prefix_path_hint(ops_tree, 'k'.b)
assert_equal 'k'.b + "\0".b, Prolly.changed_span_from_key('k'.b)._end
assert_equal 'l'.b, Prolly.changed_span_for_prefix('k'.b)._end
assert_equal false, ops_engine.publish_changed_spans_hint(
  ops_empty,
  ops_tree,
  [Prolly.changed_span('k'.b, 'l'.b)]
)
assert_equal nil, ops_engine.load_changed_spans_hint(ops_empty, ops_tree)

structural_page = ops_engine.structural_diff_page(ops_empty, ops_tree, nil, 1)
assert structural_page.diffs.any?, 'expected structural diff page diffs'
assert structural_page.stats.emitted_diffs.positive?, 'expected structural diff stats'
structural_cursor_page = ops_engine.structural_diff_page(ops_empty, ops_tree, nil, 0)
assert structural_cursor_page.next_cursor, 'expected typed structural diff cursor'
assert structural_cursor_page.next_cursor_json, 'expected structural diff cursor JSON'
resumed_structural_page = ops_engine.structural_diff_page_with_cursor(
  ops_empty,
  ops_tree,
  structural_cursor_page.next_cursor,
  1
)
assert resumed_structural_page.diffs.any?, 'expected resumed structural diff page diffs'

reachability = ops_engine.mark_reachable([ops_tree])
assert reachability.live_nodes.positive?, 'expected live nodes'
assert reachability.live_cids.any?, 'expected live CIDs'
node_cids = ops_engine.list_node_cids
assert node_cids.any?, 'expected listed node CIDs'
gc_plan = ops_engine.plan_gc([ops_tree], node_cids)
assert_equal node_cids.length, gc_plan.candidate_nodes
assert_equal 0, gc_plan.reclaimable_nodes
assert_equal 0, ops_engine.sweep_gc([ops_tree], node_cids).deleted_nodes
assert_equal 0, ops_engine.plan_store_gc([ops_tree]).reclaimable_nodes
assert_equal 0, ops_engine.sweep_store_gc([ops_tree]).deleted_nodes

destination_engine = Prolly::ProllyEngine.memory(Prolly.default_config)
missing_plan = ops_engine.plan_missing_nodes(ops_tree, destination_engine)
assert missing_plan.missing_nodes.positive?, 'expected missing nodes before sync'
copy_result = ops_engine.copy_missing_nodes(ops_tree, destination_engine)
assert_equal missing_plan.missing_nodes, copy_result.copied_nodes
assert_equal 0, ops_engine.plan_missing_nodes(ops_tree, destination_engine).missing_nodes
assert_equal 'v'.b, destination_engine.get(ops_tree, 'k'.b)

snapshot_bundle = ops_engine.export_snapshot(ops_tree)
assert_equal 1, snapshot_bundle.format_version
assert snapshot_bundle.nodes.any?, 'expected snapshot bundle nodes'
snapshot_bundle_bytes = Prolly.snapshot_bundle_to_bytes(snapshot_bundle)
snapshot_bundle_digest = Prolly.snapshot_bundle_digest(snapshot_bundle)
assert_equal Prolly.cid_from_bytes(snapshot_bundle_bytes), snapshot_bundle_digest
assert_equal snapshot_bundle_digest, Prolly.snapshot_bundle_digest_bytes(snapshot_bundle_bytes)
snapshot_summary = Prolly.snapshot_bundle_summary(snapshot_bundle)
assert_equal 1, snapshot_summary.format_version
assert_equal snapshot_bundle.nodes.length, snapshot_summary.node_count
assert snapshot_summary.byte_count.positive?, 'expected snapshot bundle byte count'
assert_equal snapshot_summary, Prolly.snapshot_bundle_summary_from_bytes(snapshot_bundle_bytes)
snapshot_verification = Prolly.verify_snapshot_bundle(snapshot_bundle)
assert_equal true, snapshot_verification.valid
assert_equal snapshot_summary, snapshot_verification.summary
assert_equal [], snapshot_verification.missing_cids
assert_equal [], snapshot_verification.extra_cids
assert_equal snapshot_verification, Prolly.verify_snapshot_bundle_bytes(snapshot_bundle_bytes)
incomplete_snapshot_bundle = Prolly::SnapshotBundleRecord.new(
  format_version: snapshot_bundle.format_version,
  tree: snapshot_bundle.tree,
  nodes: snapshot_bundle.nodes[0...-1]
)
incomplete_snapshot_verification = Prolly.verify_snapshot_bundle(incomplete_snapshot_bundle)
assert_equal false, incomplete_snapshot_verification.valid
assert incomplete_snapshot_verification.missing_cids.any?, 'expected incomplete bundle missing CIDs'
snapshot_bundle = Prolly.snapshot_bundle_from_bytes(snapshot_bundle_bytes)
snapshot_destination = Prolly::ProllyEngine.memory(Prolly.default_config)
imported_tree = snapshot_destination.import_snapshot(snapshot_bundle)
assert_equal 'v'.b, snapshot_destination.get(imported_tree, 'k'.b)

blob_engine = Prolly::ProllyEngine.memory(Prolly.default_config)
blob_store = Prolly::ProllyBlobStore.memory
assert_equal 0, blob_store.blob_count

direct_ref = blob_store.put_blob('direct'.b)
assert_equal 'direct'.b, blob_store.get_blob(direct_ref)
Prolly.blob_ref_validate_bytes(direct_ref, 'direct'.b)
assert_raises(Prolly::ProllyBindingError::Serialization) do
  Prolly.blob_ref_validate_bytes(direct_ref, 'wrong'.b)
end
blob_store.delete_blob(direct_ref)
assert_equal 0, blob_store.blob_count

blob_empty = blob_engine.create
large_value = Array.new(64, 42).pack('C*').b
blob_tree = blob_engine.put_large_value(
  blob_store,
  blob_empty,
  'big'.b,
  large_value,
  Prolly::LargeValueConfigRecord.new(inline_threshold: 8)
)
value_ref = blob_engine.get_value_ref(blob_tree, 'big'.b)
assert_equal Prolly::ValueRefKind::BLOB, value_ref.kind
assert value_ref.blob, 'expected blob value ref'
assert_equal large_value, blob_engine.get_large_value(blob_store, blob_tree, 'big'.b)

reachable_blobs = blob_engine.mark_reachable_blobs([blob_tree])
assert_equal 1, reachable_blobs.live_blob_count
assert_equal 1, reachable_blobs.live_blobs.length
assert_equal 0, blob_engine.plan_blob_gc(blob_store, [blob_tree], reachable_blobs.live_blobs).reclaimable_blob_count

blob_store.put_blob('orphan'.b)
assert_equal 2, blob_store.list_blob_refs.length
assert_equal 1, blob_engine.plan_blob_store_gc(blob_store, [blob_tree]).reclaimable_blob_count
assert_equal 1, blob_engine.sweep_blob_store_gc(blob_store, [blob_tree]).deleted_blobs
assert_equal 1, blob_store.blob_count

blob_without_big = blob_engine.delete(blob_tree, 'big'.b)
assert_equal 1, blob_engine.plan_blob_store_gc(blob_store, [blob_without_big]).reclaimable_blob_count
assert_equal 1, blob_engine.sweep_blob_store_gc(blob_store, [blob_without_big]).deleted_blobs
assert_equal 0, blob_store.blob_count
