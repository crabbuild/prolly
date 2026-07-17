# frozen_string_literal: true

require 'minitest/autorun'
require 'prolly'

class PortableParityTest < Minitest::Test
  def test_versioned_bulk_publication_uses_native_performance_paths
    Prolly::Engine.memory.use do |engine|
      map = engine.versioned_map('bulk-publication'.b)
      initialized = map.initialize_sorted([
        Prolly::EntryRecord.new(key: 'a'.b, value: 'one'.b),
        Prolly::EntryRecord.new(key: 'b'.b, value: 'two'.b)
      ])
      assert_equal Prolly::MapUpdateKind::APPLIED, initialized.kind
      map.append([Prolly::MutationRecord.new(kind: Prolly::MutationKind::UPSERT, key: 'c'.b, value: 'three'.b)])
      parallel = map.parallel_apply([
        Prolly::MutationRecord.new(kind: Prolly::MutationKind::UPSERT, key: 'b'.b, value: 'updated'.b),
        Prolly::MutationRecord.new(kind: Prolly::MutationKind::UPSERT, key: 'd'.b, value: 'four'.b)
      ], Prolly::ParallelConfigRecord.new(max_threads: 1, parallelism_threshold: 1))
      assert_equal 2, parallel.stats.input_mutations
      rebuilt = map.rebuild_sorted_if(parallel.version.id, [
        Prolly::EntryRecord.new(key: 'x'.b, value: 'nine'.b),
        Prolly::EntryRecord.new(key: 'y'.b, value: 'ten'.b)
      ])
      assert_equal Prolly::MapUpdateKind::APPLIED, rebuilt.kind
      iter_rebuilt = map.rebuild_from_entries_if(rebuilt.current.id, [
        Prolly::EntryRecord.new(key: 'q'.b, value: 'queue'.b),
        Prolly::EntryRecord.new(key: 'p'.b, value: 'priority'.b)
      ])
      assert_equal Prolly::MapUpdateKind::APPLIED, iter_rebuilt.kind
      assert_equal 'priority'.b, map.get('p'.b)
    end
  end

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

  def test_versioned_history_navigation_diff_and_rollback_stay_native
    Prolly::Engine.memory.use do |engine|
      map = engine.versioned_map('history-navigation'.b)
      map.initialize_map
      map.put('a'.b, 'one'.b)
      map.put('ab'.b, 'two'.b)
      base = map.put('b'.b, 'three'.b)
      target = map.put('a'.b, 'updated'.b)

      assert_equal %w[a ab b].map(&:b), map.range('a'.b, 'c'.b).map(&:key)
      assert_equal %w[a ab].map(&:b), map.prefix('a'.b).map(&:key)
      assert_equal 'one'.b, map.range_at(base.id, 'a'.b, 'b'.b).first.value
      assert_equal %w[a ab].map(&:b), map.prefix_at(base.id, 'a'.b).map(&:key)
      assert_equal %w[a ab].map(&:b), map.range_page(nil, nil, 2).entries.map(&:key)
      assert_equal ['a'.b], map.prefix_page('a'.b, nil, 1).entries.map(&:key)
      historical_page = map.prefix_page_at(base.id, 'a'.b, nil, 1)
      assert_equal ['a'.b], historical_page.entries.map(&:key)
      refute_nil historical_page.next_cursor
      assert_equal ['a'.b], map.diff(base.id, target.id).map(&:key)
      assert_equal ['a'.b], map.changes_since(base.id).map(&:key)

      rolled_back = map.rollback_to(base.id)
      assert_equal rolled_back.id, map.head_id
      assert_equal 'one'.b, map.get('a'.b)
      assert_empty map.changes_since(base.id)
    end
  end

  def test_versioned_timestamped_writes_expose_complete_maintenance_records
    Prolly::Engine.memory.use do |engine|
      map = engine.versioned_map('maintenance-complete'.b)
      first = map.apply_at_millis([
        Prolly::MutationRecord.new(kind: Prolly::MutationKind::UPSERT, key: 'k'.b, value: 'one'.b)
      ], 1_000)
      second = map.apply_if_at_millis(first.id, [
        Prolly::MutationRecord.new(kind: Prolly::MutationKind::UPSERT, key: 'k'.b, value: 'two'.b)
      ], 2_000).current
      third = map.apply_at_millis([
        Prolly::MutationRecord.new(kind: Prolly::MutationKind::UPSERT, key: 'k'.b, value: 'three'.b)
      ], 3_000)

      assert_equal 1_000, first.created_at_millis
      assert_equal 2_000, second.created_at_millis
      assert_equal Prolly::NamedRootRetentionKind::PREFIX, map.retention_policy.kind
      verification = map.verify_catalog
      assert_equal third.id, verification.head
      assert_equal 3, verification.version_count
      plan = map.plan_gc
      assert_operator plan.reachability.live_nodes, :>, 0
      assert_operator plan.candidate_nodes, :>=, plan.reclaimable_nodes

      aged = map.keep_for_at(3_000, 1_500)
      assert_includes aged.retained, second.id
      assert_includes aged.removed, first.id
      assert_includes map.keep_versions([second.id]).retained, third.id
      pruned = map.prune_versions(0)
      assert_equal [third.id], pruned.retained
      assert_includes pruned.removed, second.id
      refute_empty map.keep_for(10_000).retained
      assert_operator map.sweep_gc.deleted_nodes, :>=, 0
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


  def test_multi_map_transactions_are_atomic_and_read_staged_values
    Prolly::Engine.memory.use do |engine|
      tx = engine.begin_versioned_transaction
      tx.put('a'.b, 'k'.b, 'one'.b)
      tx.put('b'.b, 'k'.b, 'two'.b)
      assert_equal 'one'.b, tx.get('a'.b, 'k'.b)
      committed = tx.commit
      assert committed.applied
      assert_equal 2, committed.versions.size
      assert_equal 'one'.b, engine.versioned_map('a'.b).get('k'.b)
      assert_equal 'two'.b, engine.versioned_map('b'.b).get('k'.b)
      rolled_back = engine.begin_versioned_transaction
      rolled_back.put('a'.b, 'discard'.b, 'x'.b)
      rolled_back.rollback
      assert_nil engine.versioned_map('a'.b).get('discard'.b)
    end
  end


  def test_pinned_merges_page_conflicts_and_cas_publish
    Prolly::Engine.memory.use do |engine|
      map = engine.versioned_map('merge'.b)
      base = map.initialize_map
      candidate = map.put('k'.b, 'candidate'.b)
      map.put('k'.b, 'head'.b)
      merge = map.prepare_merge(base.id, candidate.id)
      assert_equal base.id, merge.base.id
      assert_equal candidate.id, merge.candidate.id
      assert_equal ['k'.b], merge.conflict_page(nil, 1).conflicts.map(&:key)
      assert_equal candidate.id, merge.publish('prefer_right').current.id
      assert_equal 'candidate'.b, map.get('k'.b)
      merge.close
    end
  end
end
