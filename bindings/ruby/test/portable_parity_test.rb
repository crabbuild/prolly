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

  def test_proofs_sessions_and_maintenance_are_application_facing
    Prolly::Engine.memory.use do |engine|
      versioned = engine.versioned_map('proofs'.b)
      versioned.initialize_map
      versioned.put('k'.b, 'v'.b)
      snapshot = versioned.snapshot
      verified = Prolly.verify_key_proof(snapshot.prove_key('k'.b))
      assert verified.valid
      assert_equal 'v'.b, verified.value
      assert_equal 1, snapshot.stats.total_key_value_pairs
      refute_empty snapshot.export.nodes
      snapshot.read.use { |session| assert_equal 'v'.b, session.get('k'.b) }
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
end
