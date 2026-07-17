# frozen_string_literal: true

require 'minitest/autorun'
require 'prolly'

class PortableParityTest < Minitest::Test
  def test_typed_versioned_map_is_application_facing
    Prolly::Engine.memory.use do |engine|
      raw = engine.versioned_map('typed-users'.b)
      raw.initialize_map
      typed = raw.typed(Prolly::StringKeyCodec.new, Prolly::JsonValueCodec.new)
      first = typed.put('alice', { 'score' => 1 })
      assert_equal({ 'score' => 1 }, typed.get('alice'))
      assert_equal({ 'score' => 1 }, typed.get_at(first.id, 'alice'))
      assert_equal [Prolly::TypedEntry.new(key: 'alice', value: { 'score' => 1 })], typed.entries
      updated = typed.put_if(first.id, 'alice', { 'score' => 2 })
      assert_equal Prolly::MapUpdateKind::APPLIED, updated.kind
      migrated = typed.migrate_from(updated.current.id, Prolly::JsonValueCodec.new) do |value|
        value.merge('active' => true)
      end
      assert_equal Prolly::MapUpdateKind::APPLIED, migrated.update.kind
      assert_equal [1, 1], [migrated.scanned_values, migrated.rewritten_values]
      assert_equal({ 'score' => 2, 'active' => true }, typed.get('alice'))
      assert_same raw, typed.raw
      typed.delete('alice')
      assert_nil typed.get('alice')
    end
  end

  def test_versioned_large_values_and_blob_gc_are_application_facing
    Prolly::Engine.memory.use do |engine|
      Prolly::BlobStore.memory.use do |blobs|
        versioned = engine.versioned_map('large-values'.b)
        versioned.initialize_map
        refute_empty versioned.head_name
        refute_empty versioned.versions_prefix
        config = Prolly::LargeValueConfigRecord.new(inline_threshold: 1)
        first = versioned.put_large_value(blobs, 'document'.b, 'large-value'.b, config)
        assert_equal 'large-value'.b, versioned.get_large_value(blobs, 'document'.b)
        updated = versioned.put_large_value_if(
          blobs, first.id, 'document'.b, 'new-large-value'.b, config
        )
        assert_equal Prolly::MapUpdateKind::APPLIED, updated.kind
        snapshot = versioned.snapshot
        begin
          snapshot.read.use do |session|
            blob = nil
            assert session.get_value_ref_view('document'.b) { |value| blob = value if value.kind == :blob }
            assert_equal 32, blob.cid.bytesize
            assert_equal 'new-large-value'.b.bytesize, blob.length
          end
        ensure
          snapshot.close
        end
        assert_operator versioned.plan_blob_gc(blobs).reachability.live_blob_count, :>=, 1
        sweep = versioned.sweep_blob_gc_async(blobs)
        blobs.close
        assert_operator sweep.value.plan.reachability.live_blob_count, :>=, 1
      end
    end
  end

  def test_retained_search_runtime_reuses_validated_content
    Prolly::Engine.memory.use do |engine|
      proximity = engine.build_proximity(
        dimensions: 2,
        records: 16.times.map do |index|
          Prolly::ProximityRecord.new(
            key: format('vector-%02d', index).b,
            vector: [index.to_f, 0.0],
            value: format('value-%02d', index).b
          )
        end
      )
      request = Prolly.exact_proximity_search_request([0.0, 0.0], 3)

      engine.proximity_search_runtime.use do |runtime|
        cold = proximity.search_with_runtime(request, runtime)
        warm = proximity.search_with_runtime(request, runtime)
        assert_operator cold.stats.physical_bytes_read, :>, 0
        assert_equal 0, warm.stats.physical_bytes_read
        assert_operator runtime.stats.physical_reads, :>, 0
        assert_equal Prolly.default_proximity_search_runtime_policy, runtime.policy

        runtime.clear
        assert_operator proximity.search_with_runtime(request, runtime).stats.physical_bytes_read, :>, 0
      end
      proximity.close
    end
  end

  def test_proximity_future_uses_native_cooperative_cancellation
    Prolly::Engine.memory.use do |engine|
      proximity = engine.build_proximity(
        dimensions: 2,
        records: 256.times.map do |index|
          Prolly::ProximityRecord.new(
            key: format('vector-%04d', index).b,
            vector: [index.to_f, (index % 7).to_f],
            value: [index].pack('Q<')
          )
        end
      )
      request = Prolly.exact_proximity_search_request([0.0, 0.0], 10)
      engine.proximity_search_runtime.use do |runtime|
        Prolly::ProximityCancellationToken.new.use do |cancellation|
          cancellation.cancel
          result = proximity.search_async(
            request, runtime: runtime, cancellation: cancellation
          ).value
          session_result = proximity.read.use do |session|
            session.search_async(
              request, runtime: runtime, cancellation: cancellation
            ).value
          end
          assert_equal Prolly::SearchCompletionRecord::CANCELLED, result.completion
          assert_empty result.neighbors
          assert_equal Prolly::SearchCompletionRecord::CANCELLED, session_result.completion
          assert_empty session_result.neighbors
        end
      end
      proximity.close
    end
  end

  def test_composite_and_catalog_lifecycle_is_portable_and_bounded
    Prolly::Engine.memory.use do |engine|
      base = engine.build_proximity(
        dimensions: 2,
        records: 16.times.map do |index|
          Prolly::ProximityRecord.new(
            key: format('vector-%02d', index).b,
            vector: [index.to_f, 0.0],
            value: format('value-%02d', index).b
          )
        end
      )
      hnsw = base.build_hnsw.index
      current, = base.mutate([
        Prolly::ProximityMutationRecord.new(
          key: 'vector-00'.b, vector: [0.25, 0.0], value: 'updated'.b
        )
      ])
      built = current.build_composite_hnsw(base, hnsw)
      assert_equal 1, built.stats.vector_updated_records
      assert_empty built.reasons
      exact = Prolly.exact_proximity_search_request([0.0, 0.0], 3)
      request = Prolly::ProximitySearchRequestRecord.new(
        query: exact.query, k: exact.k,
        policy: Prolly::SearchPolicyKind::FIXED_BUDGET,
        adaptive_quality: exact.adaptive_quality, budget: exact.budget,
        filter: exact.filter, kernel: exact.kernel,
        backend: Prolly::SearchBackendRecord::COMPOSITE,
        hnsw_ef_search: exact.hnsw_ef_search,
        pq_rerank_multiplier: exact.pq_rerank_multiplier
      )
      built.accelerator.use do |composite|
        assert_equal Prolly::CompositeBaseKindRecord::HNSW, composite.base_kind
        assert_equal current.descriptor, composite.current_source_descriptor
        assert_equal base.descriptor, composite.base_source_descriptor
        assert_equal 1, composite.delta_count
        assert_equal 1, composite.shadow_count
        assert_equal Prolly::SearchBackendRecord::COMPOSITE,
                     composite.search(current, request).backend
        composite.prove_search(current, request).use do |proof|
          assert_equal Prolly::SearchBackendRecord::COMPOSITE,
                       proof.verify(current.descriptor).result.backend
        end
        manifest = composite.manifest
        current.build_accelerator_catalog(composite: composite).use do |catalog|
          assert_equal current.descriptor, catalog.source_descriptor
          assert_equal Prolly::CatalogAcceleratorKindRecord::COMPOSITE,
                       catalog.entries.first.kind
          assert_equal Prolly::SearchBackendRecord::COMPOSITE,
                       catalog.search(current, request).backend
          current.load_accelerator_catalog(catalog.manifest).use do |loaded|
            assert_equal catalog.manifest, loaded.manifest
          end
        end
        current.load_composite(manifest).use do |loaded|
          assert_equal manifest, loaded.manifest
        end
      end
      defaults = Prolly.default_composite_accelerator_config
      forced = Prolly::CompositeAcceleratorConfigRecord.new(
        max_delta_records: 0,
        max_shadow_records: defaults.max_shadow_records,
        max_delta_ratio_ppm: defaults.max_delta_ratio_ppm,
        max_shadow_ratio_ppm: defaults.max_shadow_ratio_ppm,
        base_overfetch_multiplier: defaults.base_overfetch_multiplier
      )
      rebuilt = current.build_or_rebuild_composite_hnsw(base, hnsw, config: forced)
      assert_equal Prolly::CompositeBuildOrRebuildKindRecord::HNSW_REBUILT, rebuilt.kind
      rebuilt.hnsw.close
      hnsw.close
      current.close
      base.close
    end
  end

  def test_product_quantizer_lifecycle_is_portable_and_bounded
    Prolly::Engine.memory.use do |engine|
      proximity = engine.build_proximity(
        dimensions: 4,
        records: 16.times.map do |index|
          Prolly::ProximityRecord.new(
            key: format('vector-%02d', index).b,
            vector: [index.to_f, (index % 3).to_f, 0.0, 1.0],
            value: format('value-%02d', index).b
          )
        end
      )
      config = Prolly::ProductQuantizationConfigRecord.new(
        subquantizers: 2,
        centroids_per_subquantizer: 4,
        training_iterations: 2,
        rerank_multiplier: 4,
        seed: (1 << 64) - 1,
        max_training_vectors: 16
      )
      built = proximity.build_pq(config: config, worker_threads: 2)
      assert_equal 16, built.stats.encoded_vectors
      exact = Prolly.exact_proximity_search_request([0.0, 0.0, 0.0, 1.0], 3)
      request = Prolly::ProximitySearchRequestRecord.new(
        query: exact.query,
        k: exact.k,
        policy: Prolly::SearchPolicyKind::FIXED_BUDGET,
        adaptive_quality: exact.adaptive_quality,
        budget: exact.budget,
        filter: exact.filter,
        kernel: exact.kernel,
        backend: Prolly::SearchBackendRecord::PRODUCT_QUANTIZED,
        hnsw_ef_search: exact.hnsw_ef_search,
        pq_rerank_multiplier: exact.pq_rerank_multiplier
      )
      built.index.use do |index|
        assert_equal config, index.config
        assert_equal proximity.descriptor, index.source_descriptor
        assert_operator index.quality.mean_squared_error, :>=, 0.0
        result = index.search(proximity, request)
        assert_equal Prolly::SearchBackendRecord::PRODUCT_QUANTIZED, result.backend
        assert_equal 'vector-00'.b, result.neighbors.first.key
        Prolly::ProximityCancellationToken.new.use do |cancellation|
          cancellation.cancel
          cancelled = index.search_cancellable(
            proximity, request, cancellation: cancellation
          )
          assert_equal Prolly::SearchCompletionRecord::CANCELLED, cancelled.completion
          assert_empty cancelled.neighbors
        end
        manifest = index.manifest
        index.prove_search(proximity, request).use do |proof|
          assert_equal Prolly::SearchBackendRecord::PRODUCT_QUANTIZED,
                       proof.verify(proximity.descriptor).result.backend
        end
        proximity.load_pq(manifest).use do |loaded|
          assert_equal manifest, loaded.manifest
        end
      end
    end
  end

  def test_hnsw_accelerator_lifecycle_is_portable
    Prolly::Engine.memory.use do |engine|
      proximity = engine.build_proximity(
        dimensions: 2,
        records: 16.times.map do |index|
          Prolly::ProximityRecord.new(
            key: format('vector-%02d', index).b,
            vector: [index.to_f, 0.0],
            value: format('value-%02d', index).b
          )
        end
      )
      built = proximity.build_hnsw
      assert_equal 16, built.stats.records
      exact = Prolly.exact_proximity_search_request([0.0, 0.0], 3)
      request = Prolly::ProximitySearchRequestRecord.new(
        query: exact.query,
        k: exact.k,
        policy: Prolly::SearchPolicyKind::FIXED_BUDGET,
        adaptive_quality: exact.adaptive_quality,
        budget: exact.budget,
        filter: exact.filter,
        kernel: exact.kernel,
        backend: Prolly::SearchBackendRecord::HNSW,
        hnsw_ef_search: exact.hnsw_ef_search,
        pq_rerank_multiplier: exact.pq_rerank_multiplier
      )
      built.index.use do |index|
        assert index.canonical?
        assert_equal proximity.descriptor, index.source_descriptor
        result = index.search(proximity, request)
        assert_equal Prolly::SearchBackendRecord::HNSW, result.backend
        assert_equal 'vector-00'.b, result.neighbors.first.key
        manifest = index.manifest
        index.prove_search(proximity, request).use do |proof|
          assert_equal Prolly::SearchBackendRecord::HNSW,
                       proof.verify(proximity.descriptor).result.backend
        end
        proximity.load_hnsw(manifest).use do |loaded|
          assert_equal manifest, loaded.manifest
        end
      end
    end
  end

  def test_proximity_rich_search_request_is_shared_by_map_session_and_proof
    Prolly::Engine.memory.use do |engine|
      proximity = engine.build_proximity(
        dimensions: 2,
        records: [
          Prolly::ProximityRecord.new(key: 'a'.b, vector: [0.0, 0.0], value: 'alpha'.b),
          Prolly::ProximityRecord.new(key: 'ab'.b, vector: [1.0, 0.0], value: 'alphabet'.b),
          Prolly::ProximityRecord.new(key: 'b'.b, vector: [0.1, 0.0], value: 'beta'.b)
        ]
      )
      request = Prolly::ProximitySearchRequestRecord.new(
        query: [0.0, 0.0],
        k: 3,
        policy: Prolly::SearchPolicyKind::FIXED_BUDGET,
        adaptive_quality: nil,
        budget: Prolly::SearchBudgetRecord.new(
          max_nodes: 1_000,
          max_committed_bytes: 1_000_000,
          max_distance_evaluations: 1_000,
          max_frontier_entries: 1_000
        ),
        filter: Prolly::ProximityFilterRecord.new(
          kind: Prolly::ProximityFilterKind::PREFIX,
          start: nil,
          range_end: nil,
          prefix: 'a'.b,
          eligible_keys: []
        ),
        kernel: Prolly::QueryKernelRecord::SCALAR_DETERMINISTIC,
        backend: Prolly::SearchBackendRecord::AUTO,
        hnsw_ef_search: nil,
        pq_rerank_multiplier: nil
      )

      result = proximity.search(request)
      assert_equal ['a'.b, 'ab'.b], result.neighbors.map(&:key)
      assert_operator result.stats.distance_evaluations, :>, 0
      assert_operator result.plan_format_version, :>, 0
      scanned = []
      assert_equal 2, proximity.scan_records { |record| scanned << record.key; scanned.length < 2 }
      assert_equal ['a'.b, 'ab'.b], scanned
      proximity.read.use do |session|
        assert_equal ['a'.b, 'ab'.b], session.search(request).neighbors.map(&:key)
        retained = []
        assert_equal 3, session.scan_records { |record| retained << record.key; true }
        assert_equal ['a'.b, 'ab'.b, 'b'.b], retained
      end
      proximity.prove_search(request).use do |proof|
        assert_equal ['a'.b, 'ab'.b], proof.verify(proximity.descriptor).result.neighbors.map(&:key)
      end
    end
  end

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
      subscription = versioned.subscribe_async.value
      key = 'k'.b
      future = versioned.put_async(key, 'v'.b)
      key.replace('x'.b)
      updated = future.value
      assert_equal 'v'.b, versioned.get('k'.b)
      assert_equal updated.id, versioned.head_async.value.id
      snapshot = versioned.snapshot_at_async(updated.id).value
      assert_equal 'v'.b, snapshot.get_async('k'.b).value
      bundle = snapshot.export_async.value
      imported = engine.versioned_map('async-import'.b)
      pending_import = imported.import_as_head_async(bundle)
      bundle.nodes.first.bytes.replace('mutated-after-handoff'.b)
      pending_import.value
      assert_equal 'v'.b, imported.get('k'.b)
      snapshot.read.use { |session| assert_equal 'v'.b, session.get_async('k'.b).value }
      refute_nil subscription.poll_async.value
      snapshot.close
      subscription.close
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
        bundle = source.snapshot.export
        imported_map = target_engine.versioned_map('versioned-import'.b)
        assert imported_map.import_as_head(bundle).is_head
        assert_equal 'v2'.b, imported_map.get('k'.b)
        timestamped_map = target_engine.versioned_map('versioned-import-at'.b)
        timestamped = timestamped_map.import_as_head_at_millis(bundle, 12_345)
        assert_equal 12_345, timestamped.created_at_millis
        assert_equal 'v2'.b, timestamped_map.get('k'.b)
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
        found, copied = session.get_view('k'.b, &:bytes)
        assert found
        assert_equal 'v'.b, copied
        escaped_value = nil
        session.get_view('k'.b) { |value| escaped_value = value }
        assert_raises(RuntimeError) { escaped_value.bytes }
        assert_equal [false, nil], session.get_view('missing'.b, &:bytes)
        value_ref = nil
        session.get_value_ref_view('k'.b) { |value| value_ref = value.inline.bytes }
        assert_equal 'v'.b, value_ref
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
      old_snapshot_id = indexed.snapshot.id
      defaults = Prolly.default_secondary_index_limits
      too_small = Prolly::SecondaryIndexLimitsRecord.new(
        max_term_bytes: 3,
        max_projection_bytes: defaults.max_projection_bytes,
        max_all_value_bytes: defaults.max_all_value_bytes,
        max_terms_per_record: defaults.max_terms_per_record,
        max_projected_bytes_per_record: defaults.max_projected_bytes_per_record,
        max_derived_mutations_per_transaction: defaults.max_derived_mutations_per_transaction,
        max_projected_bytes_per_transaction: defaults.max_projected_bytes_per_transaction,
        max_indexes: defaults.max_indexes,
        build_page_size: defaults.build_page_size,
        max_temporary_sort_bytes: defaults.max_temporary_sort_bytes,
        max_bundle_nodes: defaults.max_bundle_nodes,
        max_bundle_bytes: defaults.max_bundle_bytes,
        max_verification_entries: defaults.max_verification_entries,
        max_write_retries: defaults.max_write_retries,
        max_build_retries: defaults.max_build_retries
      )
      assert_raises(Prolly::ProllyBindingError::Internal) do
        indexed.replace_index(
          'by_value'.b, 2, 'value-too-small-v2', Prolly::IndexProjectionRecord::ALL,
          ->(_key, value) { [[value, nil]] }, limits: too_small
        )
      end
      assert_equal 1, indexed.health.active_indexes.first.generation
      replacement = indexed.replace_index(
        'by_value'.b, 2, 'value-v2', Prolly::IndexProjectionRecord::ALL,
        ->(_key, value) { [[value, nil]] }
      )
      assert_equal 2, replacement.generation
      assert_equal 2, indexed.health.active_indexes.first.generation
      assert_equal 1, indexed.snapshot_by_id(old_snapshot_id).index('by_value'.b).exact('term'.b).size
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
