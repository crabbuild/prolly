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
    end
  end
end
