# frozen_string_literal: true

require 'minitest/autorun'
require 'prolly'

class PortableParityTest < Minitest::Test
  def test_versioned_indexed_and_proximity_maps_use_portable_api
    Prolly::Engine.memory.use do |engine|
      versioned = engine.versioned_map('users'.b)
      versioned.initialize_map
      versioned.put('u1'.b, 'Ada'.b)
      assert_equal 'Ada'.b, versioned.get('u1'.b)

      registry = engine.index_registry
      registry.register(
        'by_team'.b, 1, 'team-v1', Prolly::IndexProjectionRecord::ALL,
        ->(_primary_key, source_value) { [[source_value, nil]] }
      )
      indexed = engine.indexed_map('members'.b, registry)
      indexed.put('u1'.b, 'red'.b)
      indexed.ensure_index('by_team'.b)
      rows = indexed.snapshot.index('by_team'.b).records('red'.b)
      assert_equal ['u1'.b], rows.map(&:primary_key)
      assert_equal 'members'.b, indexed.id
      applied = indexed.apply([
        Prolly::MutationRecord.new(kind: Prolly::MutationKind::UPSERT, key: 'u2'.b, value: 'red'.b)
      ])
      conditional = indexed.apply_if(applied.source_version, [
        Prolly::MutationRecord.new(kind: Prolly::MutationKind::UPSERT, key: 'u3'.b, value: 'blue'.b)
      ])
      refute_nil conditional.current
      assert_equal 2, indexed.snapshot_at(applied.source_version).index('by_team'.b).exact_page('red'.b).matches.size
      current_indexed = indexed.snapshot
      assert_equal current_indexed.id, indexed.snapshot_by_id(current_indexed.id).id
      assert_equal 'by_team'.b, current_indexed.index('by_team'.b).name

      proximity = engine.build_proximity(
        dimensions: 2,
        records: [Prolly::ProximityRecord.new(key: 'a'.b, vector: [0.0, 0.0], value: 'alpha'.b)]
      )
      assert_equal 'a'.b, proximity.search_exact([0.1, 0.1], 1).neighbors.first.key
      viewed = proximity.search_view([0.1, 0.1], 1) { |neighbors| neighbors.first.key.bytes }
      assert_equal 'a'.b, viewed
    end
  end

  def test_future_copies_inputs_before_dispatch
    Prolly::Engine.memory.use do |engine|
      versioned = engine.versioned_map('async'.b)
      versioned.initialize_map
      key = 'k'.b
      future = versioned.put_async(key, 'v'.b)
      key.replace('x'.b)
      future.value
      assert_equal 'v'.b, versioned.get('k'.b)
    end
  end

  def test_versioned_snapshot_lifecycle
    Prolly::Engine.memory.use do |engine|
      versioned = engine.versioned_map('versioned-lifecycle'.b)
      assert_equal 'versioned-lifecycle'.b, versioned.id
      refute versioned.initialized?
      initial = versioned.initialize_map
      assert versioned.initialized?
      assert_equal initial.id, versioned.head_id
      first = versioned.put('k'.b, 'v1'.b)
      versioned.put('k'.b, 'v2'.b)
      assert_equal versioned.head.id, versioned.head_id
      assert_equal first.id, versioned.version(first.id).id
      assert_operator versioned.versions.size, :>=, 3
      historical = versioned.snapshot_at(first.id)
      assert_equal first.id, historical.id
      assert_equal first.id, historical.version.id
      assert_equal 'v1'.b, historical.get('k'.b)
    end
  end

  def test_versioned_snapshots_expose_ordered_navigation_and_bounded_pages
    Prolly::Engine.memory.use do |engine|
      versioned = engine.versioned_map('versioned-ordered'.b)
      versioned.initialize_map
      versioned.apply([
        Prolly::MutationRecord.new(kind: Prolly::MutationKind::UPSERT, key: 'a'.b, value: 'one'.b),
        Prolly::MutationRecord.new(kind: Prolly::MutationKind::UPSERT, key: 'ab'.b, value: 'two'.b),
        Prolly::MutationRecord.new(kind: Prolly::MutationKind::UPSERT, key: 'b'.b, value: 'three'.b),
        Prolly::MutationRecord.new(kind: Prolly::MutationKind::UPSERT, key: 'c'.b, value: 'four'.b)
      ])
      snapshot = versioned.snapshot
      assert snapshot.contains?('ab'.b)
      assert_equal ['one'.b, nil], snapshot.get_many(['a'.b, 'missing'.b])
      assert_equal 'a'.b, snapshot.first_entry.key
      assert_equal 'c'.b, snapshot.last_entry.key
      assert_equal 'ab'.b, snapshot.lower_bound('aa'.b).key
      assert_equal 'b'.b, snapshot.upper_bound('ab'.b).key
      assert_equal ['a'.b, 'ab'.b], snapshot.prefix('a'.b).map(&:key)
      assert_equal ['ab'.b, 'b'.b], snapshot.range('ab'.b, 'c'.b).map(&:key)
      prefix_page = snapshot.prefix_page('a'.b, nil, 1)
      assert_equal ['a'.b], prefix_page.entries.map(&:key)
      refute_nil prefix_page.next_cursor
      first = snapshot.range_page(nil, 'c'.b, 2)
      assert_equal ['a'.b, 'ab'.b], first.entries.map(&:key)
      second = snapshot.range_page(first.next_cursor, 'c'.b, 2)
      assert_equal ['b'.b], second.entries.map(&:key)
      assert_equal ['c'.b, 'b'.b], snapshot.reverse_page(nil, 'a'.b, 2).entries.map(&:key)
      assert_equal ['ab'.b, 'a'.b], snapshot.prefix_reverse_page('a'.b, nil, 2).entries.map(&:key)
    end
  end

  def test_versioned_batch_cas_and_pinned_point_reads
    Prolly::Engine.memory.use do |engine|
      versioned = engine.versioned_map('versioned-cas'.b)
      versioned.initialize_map
      first = versioned.apply([
        Prolly::MutationRecord.new(kind: Prolly::MutationKind::UPSERT, key: 'a'.b, value: 'one'.b),
        Prolly::MutationRecord.new(kind: Prolly::MutationKind::UPSERT, key: 'b'.b, value: 'two'.b)
      ])
      assert versioned.contains?('a'.b)
      assert_equal ['one'.b, nil], versioned.get_many(['a'.b, 'missing'.b])
      applied = versioned.put_if(first.id, 'a'.b, 'updated'.b)
      assert_equal Prolly::MapUpdateKind::APPLIED, applied.kind
      assert_equal Prolly::MapUpdateKind::CONFLICT, versioned.delete_if(first.id, 'b'.b).kind
      assert_equal ['one'.b, 'two'.b], versioned.get_many_at(first.id, ['a'.b, 'b'.b])
      assert_equal 'one'.b, versioned.get_at(first.id, 'a'.b)
      result = versioned.apply_if(applied.current.id, [
        Prolly::MutationRecord.new(kind: Prolly::MutationKind::DELETE, key: 'b'.b, value: nil)
      ])
      assert_equal Prolly::MapUpdateKind::APPLIED, result.kind
    end
  end

  def test_versioned_backup_restore_and_retention
    Prolly::Engine.memory.use do |source_engine|
      Prolly::Engine.memory.use do |target_engine|
        source = source_engine.versioned_map('versioned-backup'.b)
        source.initialize_map
        source.put('k'.b, 'v1'.b)
        source.put('k'.b, 'v2'.b)
        target = target_engine.versioned_map('versioned-backup'.b)
        restored = target.restore_backup(source.backup)
        assert_equal source.head_id, restored.id
        assert_equal 'v2'.b, target.get('k'.b)
        pruned = source.keep_last(1)
        refute_empty pruned.retained
        refute_empty pruned.removed
      end
    end
  end

  def test_proofs_sessions_and_maintenance_are_application_facing
    Prolly::Engine.memory.use do |engine|
      versioned = engine.versioned_map('proofs'.b)
      versioned.initialize_map
      versioned.put('k'.b, 'v'.b)
      versioned.put('ka'.b, 'v2'.b)
      snapshot = versioned.snapshot
      verified = Prolly.verify_key_proof(snapshot.prove_key('k'.b))
      assert verified.valid
      assert_equal 'v'.b, verified.value
      multi = Prolly.verify_multi_key_proof(snapshot.prove_keys(['k'.b, 'missing'.b]))
      assert_equal [true, false], multi.results.map(&:exists)
      ranged = Prolly.verify_range_proof(snapshot.prove_range('k'.b, 'l'.b))
      assert_equal ['k'.b, 'ka'.b], ranged.entries.map(&:key)
      prefixed = Prolly.verify_range_proof(snapshot.prove_prefix('k'.b))
      assert_equal ['k'.b, 'ka'.b], prefixed.entries.map(&:key)
      proved_page = snapshot.prove_range_page(nil, 'l'.b, 1)
      assert Prolly.verify_range_page_proof(proved_page.proof).valid
      assert_equal ['k'.b], proved_page.page.entries.map(&:key)
      assert_equal 2, snapshot.stats.total_key_value_pairs
      refute_empty snapshot.export.nodes
      snapshot.read.use do |session|
        assert_equal 'v'.b, session.get('k'.b)
        escaped = nil
        seen = []
        outcome = session.scan_range_view('k'.b, 'l'.b) do |entry|
          escaped ||= entry.key
          seen << "#{entry.key.bytes}=#{entry.value.bytes}"
          true
        end
        assert_equal 2, outcome.visited
        refute outcome.stopped
        assert_equal ['k=v', 'ka=v2'], seen
        assert_raises(RuntimeError) { escaped.bytes }
        stopped = session.scan_range_view('k'.b, 'l'.b) { false }
        assert_equal 1, stopped.visited
        assert stopped.stopped
      end
      assert_operator versioned.verify_catalog.version_count, :>=, 2
      refute_empty versioned.backup
      refute_empty versioned.plan_gc.reachability.live_cids

      registry = engine.index_registry
      registry.register(
        'by_value'.b, 1, 'value-v1', Prolly::IndexProjectionRecord::ALL,
        ->(_key, value) { [[value, nil]] }
      )
      indexed = engine.indexed_map('indexed-maintenance'.b, registry)
      version = indexed.put('k'.b, 'term'.b)
      indexed.ensure_index('by_value'.b)
      assert indexed.verify_index('by_value'.b, version.source_version).valid
      assert_operator indexed.metrics.build_attempts, :>=, 1
      refute_empty indexed.export_current
      refute_empty indexed.keep_last(1).retained_source_versions

      proximity = engine.build_proximity(
        dimensions: 2,
        records: [Prolly::ProximityRecord.new(key: 'p'.b, vector: [0.0, 0.0], value: 'payload'.b)]
      )
      membership = Prolly.verify_proximity_membership_proof(
        proximity.prove_membership('p'.b), proximity.descriptor
      )
      assert_equal 'payload'.b, membership.record.value
      assert_equal 1, proximity.verify.record_count
      assert_equal 1, proximity.count
      assert proximity.contains?('p'.b)
      assert_equal 2, proximity.config.dimensions
      structure = Prolly.verify_proximity_structure_proof(
        proximity.prove_structure, proximity.descriptor, Prolly.default_content_graph_limits
      )
      assert_equal 1, structure.summary.record_count
      mutated, stats = proximity.mutate([
        Prolly::ProximityMutationRecord.new(key: 'q'.b, vector: [1.0, 1.0], value: 'second'.b)
      ])
      assert_equal 2, mutated.count
      assert_operator stats.records_rebuilt, :>=, 1
      proximity.read.use do |retained|
        assert_equal 'p'.b, retained.search_exact([0.0, 0.0], 1).neighbors.first.key
        assert_equal 'p'.b, retained.search_view([0.0, 0.0], 1) { |rows| rows.first.key.to_s }
      end
      proximity.prove_search_exact([0.0, 0.0], 1).use do |proof|
        verified_search = proof.verify(proximity.descriptor)
        assert_equal 'p'.b, verified_search.result.neighbors.first.key
        assert_operator verified_search.replayed_events, :>, 0
      end
    end
  end

  def test_versioned_comparison_pins_versions_and_pages_diffs
    Prolly::Engine.memory.use do |engine|
      map = engine.versioned_map('comparison'.b)
      base = map.initialize_map
      target = map.put('k'.b, 'v'.b)
      comparison = map.compare(base.id, target.id)
      assert_equal base.id, comparison.base.id
      assert_equal target.id, comparison.target.id
      assert_equal ['k'.b], comparison.diff.map(&:key)
      assert_equal ['k'.b], comparison.diff_page(nil, nil, 1).diffs.map(&:key)
      comparison.close
    end
  end


  def test_versioned_subscription_resumes_and_polls_owned_diffs
    Prolly::Engine.memory.use do |engine|
      map = engine.versioned_map('subscription'.b)
      initial = map.initialize_map
      subscription = map.subscribe
      assert_equal initial.id, subscription.last_seen
      assert_nil subscription.poll
      current = map.put('k'.b, 'v'.b)
      event = subscription.poll
      assert_equal initial.id, event.previous
      assert_equal current.id, event.current.id
      assert_equal ['k'.b], event.diffs.map(&:key)
      assert_equal current.id, subscription.last_seen
      subscription.close
    end
  end
end
