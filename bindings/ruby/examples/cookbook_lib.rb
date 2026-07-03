# frozen_string_literal: true

require 'tmpdir'
require 'prolly'

module Cookbook
  module_function

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

  def batch_build
    engine = Prolly::ProllyEngine.memory(Prolly.default_config)
    entries = (1..64).to_a.reverse.map do |idx|
      Prolly::EntryRecord.new(key: format('event/%04d', idx).b, value: "payload-#{idx}".b)
    end
    tree = engine.build_from_entries(entries)
    rows = engine.range(tree, 'event/'.b, 'event0'.b)
    stats = engine.collect_stats_json(tree).json

    assert_equal 64, rows.length, 'row count'
    assert_equal 'event/0001'.b, rows.first.key, 'first key'
    raise 'stats missing num_nodes' unless stats.include?('num_nodes')

    puts "batch_build: imported #{rows.length} events"
  end

  def local_first_state
    engine = Prolly::ProllyEngine.memory(Prolly.default_config)
    main = 'app/demo/root/main'.b
    base = engine.batch(
      engine.create,
      [
        upsert('entity/user/001', 'Ada'),
        Prolly::MutationRecord.new(kind: Prolly::MutationKind::UPSERT, key: 'index/user/name/Ada/001'.b, value: ''.b)
      ]
    )
    engine.publish_named_root(main, base)

    device = engine.batch(
      base,
      [
        upsert('entity/task/900', 'offline draft'),
        Prolly::MutationRecord.new(kind: Prolly::MutationKind::UPSERT, key: 'index/task/status/open/900'.b, value: ''.b)
      ]
    )
    canonical = engine.put(base, 'entity/user/002'.b, 'Grace'.b)
    engine.publish_named_root(main, canonical)

    current = engine.load_named_root(main)
    merged = engine.merge(base, current, device, 'prefer_right')
    update = engine.compare_and_swap_named_root(main, current, merged)

    assert_equal true, update.applied, 'CAS applied'
    assert_equal 'Grace'.b, engine.get(merged, 'entity/user/002'.b), 'canonical user'
    assert_equal 'offline draft'.b, engine.get(merged, 'entity/task/900'.b), 'device task'

    puts 'local_first_state: merged offline branch into main'
  end

  def resolver
    engine = Prolly::ProllyEngine.memory(Prolly.default_config)
    base = engine.put(engine.create, 'settings/theme'.b, 'light'.b)
    left_delete = engine.delete(base, 'settings/theme'.b)
    right_update = engine.put(base, 'settings/theme'.b, 'dark'.b)

    update_wins = engine.merge(base, left_delete, right_update, 'update_wins')
    delete_wins = engine.merge(base, left_delete, right_update, 'delete_wins')

    assert_equal 'dark'.b, engine.get(update_wins, 'settings/theme'.b), 'update-wins setting'
    assert_equal nil, engine.get(delete_wins, 'settings/theme'.b), 'delete-wins setting'

    puts 'resolver: demonstrated update-wins and delete-wins policies'
  end

  def crdt_merge
    engine = Prolly::ProllyEngine.memory(Prolly.default_config)
    base_value = Prolly.timestamped_value_to_bytes(Prolly::TimestampedValueRecord.new(value: 'base'.b, timestamp: 1))
    left_value = Prolly.timestamped_value_to_bytes(Prolly::TimestampedValueRecord.new(value: 'left'.b, timestamp: 2))
    right_value = Prolly.timestamped_value_to_bytes(Prolly::TimestampedValueRecord.new(value: 'right'.b, timestamp: 3))

    base = engine.put(engine.create, 'counter/global'.b, base_value)
    left = engine.put(base, 'counter/global'.b, left_value)
    right = engine.put(base, 'counter/global'.b, right_value)
    merged = engine.crdt_merge(base, left, right, Prolly.crdt_config_lww(Prolly::CrdtDeletePolicyKind::UPDATE_WINS))
    decoded = Prolly.timestamped_value_from_bytes(engine.get(merged, 'counter/global'.b))
    merged_set = Prolly.multi_value_set_merge(['candidate-b'.b], ['candidate-a'.b, 'candidate-b'.b])

    assert_equal 'right'.b, decoded.value, 'CRDT value'
    assert_equal 3, decoded.timestamp, 'CRDT timestamp'
    assert_equal ['candidate-a'.b, 'candidate-b'.b], merged_set, 'multi-value set'

    puts 'crdt_merge: last-writer-wins and multi-value helpers passed'
  end

  def conversation_memory
    engine = Prolly::ProllyEngine.memory(Prolly.default_config)
    main = 'conversation/c42/root/main'.b
    attempt_name = 'conversation/c42/attempt/extractor/a1'.b

    base = engine.put(engine.create, 'conversation/c42/memory/m001'.b, 'user|likes terse summaries|0.91'.b)
    engine.publish_named_root(main, base)
    attempt = engine.put(base, 'conversation/c42/memory/m002'.b, 'user|uses Ruby|0.87'.b)
    engine.publish_named_root(attempt_name, attempt)
    canonical = engine.put(base, 'conversation/c42/memory/m003'.b, 'user|prefers local-first apps|0.82'.b)
    engine.publish_named_root(main, canonical)

    merged = engine.merge(base, engine.load_named_root(main), engine.load_named_root(attempt_name), 'prefer_right')
    update = engine.compare_and_swap_named_root(main, canonical, merged)
    rows = engine.range(merged, 'conversation/c42/memory/'.b, 'conversation/c42/memory0'.b)

    assert_equal true, update.applied, 'CAS applied'
    assert_equal 3, rows.length, 'memory count'

    puts 'conversation_memory: accepted extractor attempt into canonical memory'
  end

  def agent_event_log
    engine = Prolly::ProllyEngine.memory(Prolly.default_config)
    root = 'agent-log/run-7/root/events/current'.b
    tree = engine.batch(
      engine.create,
      [
        upsert('agent-log/run-7/event/1783036805000/0001', 'user|Summarize the plan'),
        upsert('agent-log/run-7/event/1783036805000/0002', 'tool-call|search-docs'),
        upsert('agent-log/run-7/event/1783036806000/0003', 'assistant|Plan ready')
      ]
    )
    engine.publish_named_root(root, tree)

    page = engine.range_page(engine.load_named_root(root), nil, nil, 2)
    assert_equal 2, page.entries.length, 'page length'
    raise 'missing next cursor' unless page.next_cursor

    puts "agent_event_log: first page has #{page.entries.length} events"
  end

  def background_compaction
    engine = Prolly::ProllyEngine.memory(Prolly.default_config)
    events = engine.batch(
      engine.create,
      (1..6).map { |idx| upsert(format('event/%04d', idx), "raw-event-#{idx}") }
    )
    engine.publish_named_root('compaction/run/r7/root/events/0001'.b, events)
    compacted = engine.batch(
      events,
      [
        delete('event/0001'),
        delete('event/0002'),
        delete('event/0003'),
        delete('event/0004'),
        upsert('event/0004-summary', 'summary of events 1..4')
      ]
    )
    engine.publish_named_root('compaction/run/r7/root/events/current'.b, compacted)

    plan = engine.plan_store_gc([events, compacted])
    remaining = engine.range(compacted, 'event/'.b, 'event0'.b)

    assert_equal 3, remaining.length, 'remaining records'
    raise 'invalid GC plan' if plan.reclaimable_nodes.negative?

    puts "background_compaction: compacted log to #{remaining.length} records"
  end

  def deterministic_rag_snapshot
    engine = Prolly::ProllyEngine.memory(Prolly.default_config)
    index_root = 'rag/corpus/docs/root/index/current'.b
    index_v1 = engine.batch(
      engine.create,
      [
        upsert('rag/corpus/docs/chunk/doc-1/0001', 'vector:v1|CrabDB stores deterministic roots'),
        upsert('rag/corpus/docs/chunk/doc-2/0001', 'vector:v2|Prolly trees diff by key')
      ]
    )
    engine.publish_named_root(index_root, index_v1)
    answers = engine.put(engine.create, 'rag/answer/q1'.b, "query:q1|snapshot:#{index_v1.root.unpack1('H*')}|citation:doc-1/0001".b)
    engine.publish_named_root('rag/corpus/docs/root/answers'.b, answers)

    index_v2 = engine.put(index_v1, 'rag/corpus/docs/chunk/doc-3/0001'.b, 'vector:v3|New content'.b)
    engine.publish_named_root(index_root, index_v2)

    assert_equal 2, engine.range(index_v1, 'rag/corpus/docs/chunk/'.b, 'rag/corpus/docs/chunk0'.b).length, 'replay rows'
    assert_equal 3, engine.range(engine.load_named_root(index_root), 'rag/corpus/docs/chunk/'.b, 'rag/corpus/docs/chunk0'.b).length, 'current rows'

    puts 'deterministic_rag_snapshot: replay kept original index root'
  end

  def document_chunk_index
    engine = Prolly::ProllyEngine.memory(Prolly.default_config)
    blob_store = Prolly::ProllyBlobStore.memory
    text_key = 'doc-index/corpus/text/parser-v1/doc-1/chunk-0001'.b
    metadata_key = 'doc-index/corpus/parser/parser-v1/document/doc-1/chunk/000000'.b

    tree = engine.put_large_value(
      blob_store,
      engine.create,
      text_key,
      ('CrabDB stores large chunk text outside prolly leaves.' * 8).b,
      Prolly::LargeValueConfigRecord.new(inline_threshold: 32)
    )
    tree = engine.put(tree, metadata_key, 'doc-1|chunk-0001|0|384|vector-0001'.b)

    metadata = engine.range(tree, 'doc-index/corpus/parser/'.b, 'doc-index/corpus/parser0'.b)
    loaded_text = engine.get_large_value(blob_store, tree, text_key)

    assert_equal 1, metadata.length, 'metadata count'
    raise 'missing chunk text' unless loaded_text.start_with?('CrabDB stores'.b)

    puts 'document_chunk_index: metadata and blob-backed chunk text are linked'
  end

  def vector_sidecar
    engine = Prolly::ProllyEngine.memory(Prolly.default_config)
    sidecar = { 'vec-1' => [0.9, 0.1], 'vec-2' => [0.8, 0.2], 'vec-stale' => [1.0, 0.0] }
    tree = engine.batch(
      engine.create,
      [
        upsert('vector-sidecar/corpus/docs/chunk/doc-1/0001', 'vec-1|doc-1|parser-v1'),
        upsert('vector-sidecar/corpus/docs/chunk/doc-2/0001', 'vec-2|doc-2|parser-v1')
      ]
    )
    allowed = engine
              .range(tree, 'vector-sidecar/corpus/docs/chunk/'.b, 'vector-sidecar/corpus/docs/chunk0'.b)
              .map { |entry| entry.value.split('|'.b, 2).first }
    hits = sidecar.keys.sort.select { |vector_id| allowed.include?(vector_id.b) }

    assert_equal %w[vec-1 vec-2], hits, 'sidecar hits'

    puts "vector_sidecar: filtered sidecar hits to #{hits.length} snapshot vectors"
  end

  def provenance_values
    engine = Prolly::ProllyEngine.memory(Prolly.default_config)
    source = 'CrabDB language bindings design'.b
    source_cid = Prolly.cid_from_bytes(source).unpack1('H*')
    chunk_cid = Prolly.cid_from_bytes(source[0, 16]).unpack1('H*')
    tree = engine.batch(
      engine.create,
      [
        upsert('provenance/chunk/file-1/chunk-1', "source=#{source_cid}|chunk=#{chunk_cid}|parser=v1"),
        upsert('provenance/claim/file-1/claim-1', 'CrabDB uses Rust-backed bindings|chunk=file-1/chunk-1')
      ]
    )

    claims = engine.range(tree, 'provenance/claim/file-1/'.b, 'provenance/claim/file-10'.b)
    assert_equal 1, claims.length, 'claim count'
    raise 'missing claim text' unless claims.first.value.include?('Rust-backed'.b)

    puts 'provenance_values: claim links back to source and chunk CIDs'
  end

  Order = Struct.new(:tenant, :id, :status, :cents)

  def order_key(order)
    "orders/source/tenant/#{order.tenant}/order/#{order.id}".b
  end

  def encode_order(order)
    "#{order.tenant}|#{order.id}|#{order.status}|#{order.cents}".b
  end

  def decode_order(value)
    tenant, id, status, cents = value.split('|'.b)
    Order.new(tenant, id, status, cents.to_i)
  end

  def view_key(tenant, status)
    "orders/view/by-status/tenant/#{tenant}/status/#{status}".b
  end

  def build_revenue_view(engine, source)
    totals = Hash.new(0)
    engine.range(source, 'orders/source/'.b, 'orders/source0'.b).each do |entry|
      order = decode_order(entry.value)
      totals[[order.tenant, order.status]] += order.cents
    end
    mutations = totals.sort.map do |(tenant, status), cents|
      Prolly::MutationRecord.new(kind: Prolly::MutationKind::UPSERT, key: view_key(tenant, status), value: cents.to_s.b)
    end
    engine.batch(engine.create, mutations)
  end

  def materialized_view
    engine = Prolly::ProllyEngine.memory(Prolly.default_config)
    o1 = Order.new('acme', 'o1', 'paid', 1200)
    o2 = Order.new('acme', 'o2', 'open', 500)
    source_v1 = engine.batch(
      engine.create,
      [
        Prolly::MutationRecord.new(kind: Prolly::MutationKind::UPSERT, key: order_key(o1), value: encode_order(o1)),
        Prolly::MutationRecord.new(kind: Prolly::MutationKind::UPSERT, key: order_key(o2), value: encode_order(o2))
      ]
    )
    paid_o2 = Order.new('acme', 'o2', 'paid', 500)
    source_v2 = engine.put(source_v1, order_key(paid_o2), encode_order(paid_o2))
    view_v2 = build_revenue_view(engine, source_v2)

    assert_equal '1700'.b, engine.get(view_v2, view_key('acme', 'paid')), 'paid revenue'
    assert_equal nil, engine.get(view_v2, view_key('acme', 'open')), 'open revenue'

    puts "materialized_view: folded #{engine.diff(source_v1, source_v2).length} source diff"
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

  def durable_sqlite
    Dir.mktmpdir('prolly-ruby-') do |dir|
      engine = Prolly::ProllyEngine.sqlite(File.join(dir, 'app.prolly.sqlite'), Prolly.default_config)
      tree = engine.batch(engine.create, [upsert('user/1', 'Ada'), upsert('user/2', 'Grace')])
      engine.publish_named_root('users/main'.b, tree)
      loaded = engine.load_named_root('users/main'.b)

      assert_equal tree, loaded, 'loaded SQLite root'
      assert_equal 'Ada'.b, engine.get(loaded, 'user/1'.b), 'sqlite user'
    end
    puts 'durable_sqlite: named root survived through SQLite store API'
  end

  SCENARIOS = [
    method(:batch_build),
    method(:local_first_state),
    method(:resolver),
    method(:crdt_merge),
    method(:conversation_memory),
    method(:agent_event_log),
    method(:background_compaction),
    method(:deterministic_rag_snapshot),
    method(:document_chunk_index),
    method(:vector_sidecar),
    method(:provenance_values),
    method(:materialized_view),
    method(:filesystem_snapshot),
    method(:durable_sqlite)
  ].freeze

  def run_all
    SCENARIOS.each(&:call)
  end
end
