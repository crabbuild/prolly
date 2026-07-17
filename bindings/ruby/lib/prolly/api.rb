# frozen_string_literal: true

module Prolly
  ProximityRecord = Data.define(:key, :vector, :value)
  HnswBuildResult = Data.define(:index, :stats)
  ProductQuantizationBuildResult = Data.define(:index, :stats)
  CompositeBuildOutcome = Data.define(:accelerator, :reasons, :stats)
  CompositeBuildOrRebuildOutcome = Data.define(
    :kind, :composite, :hnsw, :pq, :reasons, :composite_stats, :hnsw_stats, :pq_stats
  )

  def self.owned_proximity_search_request(request)
    budget = request.budget
    filter = request.filter
    ProximitySearchRequestRecord.new(
      query: request.query.map(&:to_f),
      k: request.k,
      policy: request.policy,
      adaptive_quality: request.adaptive_quality,
      budget: SearchBudgetRecord.new(
        max_nodes: budget.max_nodes,
        max_committed_bytes: budget.max_committed_bytes,
        max_distance_evaluations: budget.max_distance_evaluations,
        max_frontier_entries: budget.max_frontier_entries
      ),
      filter: ProximityFilterRecord.new(
        kind: filter.kind,
        start: filter.start&.dup,
        range_end: filter.range_end&.dup,
        prefix: filter.prefix&.dup,
        eligible_keys: filter.eligible_keys.map(&:dup)
      ),
      kernel: request.kernel,
      backend: request.backend,
      hnsw_ef_search: request.hnsw_ef_search,
      pq_rerank_multiplier: request.pq_rerank_multiplier
    )
  end

  module RubySecondaryIndexExtractorCallbacks
    class VTable < FFI::Struct
      layout :uniffi_free, :pointer,
             :uniffi_clone, :pointer,
             :extract, :pointer
    end

    class << self
      def insert(extractor)
        @mutex.synchronize do
          handle = @next_handle
          @next_handle += 2
          @extractors[handle] = extractor
          handle
        end
      end

      def clone(handle)
        @mutex.synchronize do
          extractor = @extractors[handle]
          return 0 unless extractor

          clone_handle = @next_handle
          @next_handle += 2
          @extractors[clone_handle] = extractor
          clone_handle
        end
      end

      def remove(handle) = @mutex.synchronize { @extractors.delete(handle) }
      def fetch(handle) = @mutex.synchronize { @extractors[handle] }

      def reset_status(pointer)
        return if pointer.null?

        status = RustCallStatus.new(pointer)
        status[:code] = CALL_SUCCESS
        status[:error_buf][:capacity] = 0
        status[:error_buf][:len] = 0
        status[:error_buf][:data] = FFI::Pointer::NULL
      end

      def write_buffer(buffer, pointer)
        out = RustBuffer.new(pointer)
        out[:capacity] = buffer.capacity
        out[:len] = buffer.len
        out[:data] = buffer.data
      end

      def write_panic(error, pointer)
        status = RustCallStatus.new(pointer)
        status[:code] = CALL_PANIC
        status[:error_buf] = RustBuffer.allocFromString(error.message)
      end
    end

    @mutex = Mutex.new
    @next_handle = 1
    @extractors = {}

    FREE = FFI::Function.new(:void, [:uint64]) { |handle| remove(handle) }
    CLONE = FFI::Function.new(:uint64, [:uint64]) { |handle| clone(handle) }
    EXTRACT = FFI::Function.new(
      :void,
      [:uint64, RustBuffer.by_value, RustBuffer.by_value, :pointer, :pointer]
    ) do |handle, key_buffer, value_buffer, out_return, out_status|
      reset_status(out_status)
      extractor = fetch(handle) || raise('secondary index extractor was released')
      key = key_buffer.consumeIntoBytes
      value = value_buffer.consumeIntoBytes
      entries = extractor.extract(key, value)
      write_buffer(RustBuffer.alloc_from_SequenceTypeIndexEntryRecord(entries), out_return)
    rescue StandardError => e
      write_panic(e, out_status) unless out_status.null?
    end

    VTABLE = VTable.new
    VTABLE[:uniffi_free] = FREE
    VTABLE[:uniffi_clone] = CLONE
    VTABLE[:extract] = EXTRACT
    Prolly.rust_call(
      :uniffi_prolly_bindings_fn_init_callback_vtable_secondaryindexextractorcallback,
      VTABLE
    )
  end

  class SecondaryIndexExtractorCallback
    class << self
      alias uniffi_lower_rust_handle uniffi_lower unless method_defined?(:uniffi_lower_rust_handle)

      def uniffi_lower(instance)
        return uniffi_lower_rust_handle(instance) if instance.instance_variable_defined?(:@handle)

        RubySecondaryIndexExtractorCallbacks.insert(instance)
      end
    end
  end

  class ProcIndexExtractor < SecondaryIndexExtractorCallback
    def initialize(callable)
      @callable = callable
    end

    def extract(primary_key, source_value)
      @callable.call(primary_key, source_value).map do |term, projection|
        IndexEntryRecord.new(term: term.b, projection: projection&.b)
      end
    end
  end

  class Engine
    def self.memory(config = Prolly.default_config)
      new(ProllyEngine.memory(config))
    end

    def initialize(native)
      @native = native
      @closed = false
    end

    def close
      @closed = true
    end

    def use
      raise 'engine is closed' if @closed
      return self unless block_given?

      begin
        yield self
      ensure
        close
      end
    end

    def versioned_map(id)
      ensure_open
      VersionedMap.new(@native.versioned_map(id.b))
    end
    def begin_versioned_transaction
      ensure_open
      VersionedTransaction.new(@native.begin_versioned_transaction)
    end

    def index_registry
      ensure_open
      IndexRegistry.new(BindingIndexRegistry.new)
    end

    def indexed_map(id, registry)
      ensure_open
      IndexedMap.new(@native.indexed_map(id.b, registry.native))
    end

    def build_proximity(dimensions:, records:, threads: nil)
      ensure_open
      native_records = records.map do |record|
        ProximityRecordRecord.new(key: record.key.b, vector: record.vector, value: record.value.b)
      end
      ProximityMap.new(
        @native.build_proximity_map(
          Prolly.default_proximity_config(dimensions), native_records, threads
        )
      )
    end

    def load_proximity(descriptor)
      ensure_open
      ProximityMap.new(@native.load_proximity_map(descriptor.b))
    end

    private

    def ensure_open
      raise 'engine is closed' if @closed
    end
  end

  class IndexRegistry
    attr_reader :native

    def initialize(native)
      @native = native
      @extractors = []
    end

    def register(name, generation, extractor_id, projection, extractor, limits: nil)
      adapter = extractor.is_a?(SecondaryIndexExtractorCallback) ? extractor : ProcIndexExtractor.new(extractor)
      @extractors << adapter
      @native.register(name.b, generation, extractor_id, projection, limits, adapter)
      self
    end
  end

  class IndexedMap
    def initialize(native)
      @native = native
      @closed = false
    end

    def id = open! { @native.id }
    def ensure_index(name) = open! { @native.ensure_index(name.b) }
    def get(key) = open! { @native.get(key.b) }
    def contains?(key) = open! { @native.contains_key(key.b) }
    def get_many(keys) = open! { @native.get_many(keys.map(&:b)) }
    def get_at(id, key) = open! { @native.get_at(id.b, key.b) }
    def get_many_at(id, keys) = open! { @native.get_many_at(id.b, keys.map(&:b)) }
    def put(key, value) = open! { @native.put(key.b, value.b) }
    def apply(mutations) = open! { @native.apply(owned_mutations(mutations)) }
    def apply_if(expected, mutations) = open! { @native.apply_if(expected&.b, owned_mutations(mutations)) }
    def put_if(expected, key, value) = open! { @native.put_if(expected&.b, key.b, value.b) }
    def delete_if(expected, key) = open! { @native.delete_if(expected&.b, key.b) }
    def apply(mutations) = open! { @native.apply(mutations) }
    def apply_if(expected_source, mutations) = open! { @native.apply_if(expected_source&.b, mutations) }
    def delete(key) = open! { @native.delete(key.b) }
    def health = open! { @native.health }
    def metrics = open! { @native.metrics }
    def verify_index(name, source_version) = open! { @native.verify_index(name.b, source_version.b) }
    def verify_all(source_version) = open! { @native.verify_all(source_version.b) }
    def repair_index(name, source_version) = open! { @native.repair_index(name.b, source_version.b) }
    def deactivate_index(name) = open! { @native.deactivate_index(name.b) }
    def export_current = open! { @native.export_current }
    def import_current(bundle, expected_source = nil) = open! { @native.import_current(bundle.b, expected_source&.b) }
    def keep_last(count) = open! { @native.keep_last(count) }
    def snapshot = open! { IndexedSnapshot.new(@native.snapshot) }
    def snapshot_at(source_version) = open! { IndexedSnapshot.new(@native.snapshot_at(source_version.b)) }
    def snapshot_by_id(id) = open! { IndexedSnapshot.new(@native.snapshot_by_id(id)) }
    def close = @closed = true

    private

    def owned_mutations(mutations)
      mutations.map do |mutation|
        MutationRecord.new(
          kind: mutation.kind, key: mutation.key.b.dup, value: mutation.value&.b&.dup
        )
      end
    end

    def open!
      raise 'indexed map is closed' if @closed
      yield
    end
  end

  class IndexedSnapshot
    def initialize(native) = @native = native
    def id = @native.id
    def index(name) = SecondaryIndex.new(@native.index(name.b))
  end

  class SecondaryIndex
    def initialize(native) = @native = native
    def name = @native.name
    def exact(term) = @native.exact(term.b)
    def prefix(prefix) = @native.prefix(prefix.b)
    def range(start, range_end = nil) = @native.range(start.b, range_end&.b)
    def records(term) = @native.records(term.b)
    def exact_page(term, cursor = nil, limit = 256) = @native.exact_page(term.b, cursor, limit)
    def exact_reverse_page(term, cursor = nil, limit = 256) = @native.exact_reverse_page(term.b, cursor, limit)
    def prefix_page(prefix, cursor = nil, limit = 256) = @native.prefix_page(prefix.b, cursor, limit)
    def prefix_reverse_page(prefix, cursor = nil, limit = 256) = @native.prefix_reverse_page(prefix.b, cursor, limit)
    def range_page(start, range_end = nil, cursor = nil, limit = 256) = @native.range_page(start.b, range_end&.b, cursor, limit)
    def range_reverse_page(start, range_end = nil, cursor = nil, limit = 256) = @native.range_reverse_page(start.b, range_end&.b, cursor, limit)
  end

  class VersionedMap
    def initialize(native)
      @native = native
      @closed = false
    end

    # Ruby makes `initialize` private even when generated as an FFI method.
    def initialize_map = open! { @native.__send__(:initialize) }
    def initialize_sorted(entries) = open! { @native.initialize_sorted(owned_entries(entries)) }
    def id = open! { @native.id }
    def initialized? = open! { @native.is_initialized }
    def head = open! { @native.head }
    def head_id = open! { @native.head_id }
    def version(id) = open! { @native.version(id.b) }
    def versions = open! { @native.versions }
    def get(key) = open! { @native.get(key.b) }
    def contains?(key) = open! { @native.contains_key(key.b) }
    def get_many(keys) = open! { @native.get_many(keys.map { |key| key.b.dup }) }
    def get_at(id, key) = open! { @native.get_at(id.b, key.b) }
    def get_many_at(id, keys) = open! { @native.get_many_at(id.b, keys.map { |key| key.b.dup }) }
    def range(start = ''.b, range_end = nil) = open! { @native.range(start.b, range_end&.b) }
    def prefix(prefix) = open! { @native.prefix(prefix.b) }
    def range_at(id, start = ''.b, range_end = nil) = open! { @native.range_at(id.b, start.b, range_end&.b) }
    def prefix_at(id, prefix) = open! { @native.prefix_at(id.b, prefix.b) }
    def range_page(cursor = nil, range_end = nil, limit = 256) = open! { @native.range_page(cursor, range_end&.b, limit) }
    def prefix_page(prefix, cursor = nil, limit = 256) = open! { @native.prefix_page(prefix.b, cursor, limit) }
    def range_page_at(id, cursor = nil, range_end = nil, limit = 256) = open! { @native.range_page_at(id.b, cursor, range_end&.b, limit) }
    def prefix_page_at(id, prefix, cursor = nil, limit = 256) = open! { @native.prefix_page_at(id.b, prefix.b, cursor, limit) }
    def diff(base, target) = open! { @native.diff(base.b, target.b) }
    def changes_since(base) = open! { @native.changes_since(base.b) }
    def rollback_to(id) = open! { @native.rollback_to(id.b) }
    def put(key, value) = open! { @native.put(key.b, value.b) }
    def apply(mutations) = open! { @native.apply(owned_mutations(mutations)) }
    def append(mutations) = open! { @native.append(owned_mutations(mutations)) }
    def parallel_apply(mutations, config)
      open! do
        @native.parallel_apply(
          owned_mutations(mutations),
          ParallelConfigRecord.new(
            max_threads: config.max_threads,
            parallelism_threshold: config.parallelism_threshold
          )
        )
      end
    end
    def rebuild_sorted_if(expected, entries) = open! { @native.rebuild_sorted_if(expected&.b, owned_entries(entries)) }
    def rebuild_from_entries_if(expected, entries) = open! { @native.rebuild_from_entries_if(expected&.b, owned_entries(entries)) }
    def rebuild_from_iter_if(expected, entries) = rebuild_from_entries_if(expected, entries)
    def apply_if(expected, mutations) = open! { @native.apply_if(expected&.b, owned_mutations(mutations)) }
    def put_if(expected, key, value) = open! { @native.put_if(expected&.b, key.b, value.b) }
    def delete_if(expected, key) = open! { @native.delete_if(expected&.b, key.b) }
    def apply_at_millis(mutations, timestamp_millis) = open! { @native.apply_at_millis(owned_mutations(mutations), timestamp_millis) }
    def apply_if_at_millis(expected, mutations, timestamp_millis) = open! { @native.apply_if_at_millis(expected&.b, owned_mutations(mutations), timestamp_millis) }
    def delete(key) = open! { @native.delete(key.b) }
    def snapshot = open! { @native.snapshot&.then { |value| MapSnapshot.new(value) } }
    def snapshot_at(id) = open! { @native.snapshot_at(id.b)&.then { |value| MapSnapshot.new(value) } }
    def compare(base, target) = open! { MapComparison.new(@native.compare(base.b, target.b)) }
    def compare_to_head(base) = open! { MapComparison.new(@native.compare_to_head(base.b)) }
    def subscribe = open! { MapSubscription.new(@native.subscribe) }
    def subscribe_from(last_seen = nil) = open! { MapSubscription.new(@native.subscribe_from(last_seen&.b)) }
    def prepare_merge(base, candidate) = open! { MapMerge.new(@native.prepare_merge(base.b, candidate.b)) }
    def backup = open! { @native.backup }
    def restore_backup(bundle) = open! { @native.restore_backup(bundle.b) }
    def keep_last(count) = open! { @native.keep_last(count) }
    def prune_versions(keep_latest) = open! { @native.prune_versions(keep_latest) }
    def keep_for_at(now_millis, max_age_millis) = open! { @native.keep_for_at(now_millis, max_age_millis) }
    def keep_for(max_age_millis) = open! { @native.keep_for(max_age_millis) }
    def keep_versions(ids) = open! { @native.keep_versions(ids.map { |id| id.b.dup }) }
    def retention_policy = open! { @native.retention_policy }
    def verify_catalog = open! { @native.verify_catalog }
    def plan_gc = open! { @native.plan_gc }
    def sweep_gc = open! { @native.sweep_gc }

    def put_async(key, value)
      copied_key = key.b.dup
      copied_value = value.b.dup
      Future.new { put(copied_key, copied_value) }
    end

    def close = @closed = true

    private

    def owned_mutations(mutations)
      mutations.map do |mutation|
        MutationRecord.new(
          kind: mutation.kind, key: mutation.key.b.dup, value: mutation.value&.b&.dup
        )
      end
    end

    def owned_entries(entries)
      entries.map { |entry| EntryRecord.new(key: entry.key.b.dup, value: entry.value.b.dup) }
    end

    def open!
      raise 'versioned map is closed' if @closed
      yield
    end
  end

  class VersionedTransaction
    def initialize(native) = @native = native
    def head(map_id) = open! { @native.head(map_id.b) }
    def get(map_id, key) = open! { @native.get(map_id.b, key.b) }
    def apply(map_id, mutations) = open! { @native.apply(map_id.b, mutations) }
    def apply_if(map_id, expected, mutations) = open! { @native.apply_if(map_id.b, expected&.b, mutations) }
    def put(map_id, key, value) = open! { @native.put(map_id.b, key.b, value.b) }
    def delete(map_id, key) = open! { @native.delete(map_id.b, key.b) }
    def commit = open! { @native.commit }.tap { close }
    def rollback = open! { @native.rollback }.tap { close }
    def close
      @native = nil
    end
    private
    def open!
      raise IOError, 'versioned transaction is completed' unless @native
      yield
    end
  end

  class MapComparison
    def initialize(native)
      @native = native
      @closed = false
    end

    def base = open! { @native.base }
    def target = open! { @native.target }
    def diff = open! { @native.diff }
    def diff_page(cursor = nil, range_end = nil, limit = 256) = open! { @native.diff_page(cursor, range_end&.b, limit) }
    def close = @closed = true

    private

    def open!
      raise IOError, 'map comparison is closed' if @closed
      yield
    end
  end

  class MapSubscription
    def initialize(native)
      @native = native
      @closed = false
    end

    def last_seen = open! { @native.last_seen }
    def poll = open! { @native.poll }
    def close = @closed = true

    private

    def open!
      raise IOError, 'map subscription is closed' if @closed
      yield
    end
  end

  class MapMerge
    def initialize(native)
      @native = native
      @closed = false
    end
    def base = open! { @native.base }
    def head = open! { @native.head }
    def candidate = open! { @native.candidate }
    def merge(resolver = nil) = open! { @native.merge(resolver) }
    def conflict_page(cursor = nil, limit = 256) = open! { @native.conflict_page(cursor, limit) }
    def publish(resolver = nil) = open! { @native.publish(resolver) }
    def close = @closed = true
    private
    def open!
      raise IOError, 'map merge is closed' if @closed
      yield
    end
  end

  class MapSnapshot
    def initialize(native)
      @native = native
      @closed = false
    end

    def id = open! { @native.id }
    def version = open! { @native.version }
    def get(key) = open! { @native.get(key.b) }
    def get_many(keys) = open! { @native.get_many(keys.map(&:b)) }
    def contains?(key) = open! { @native.contains_key(key.b) }
    def first_entry = open! { @native.first_entry }
    def last_entry = open! { @native.last_entry }
    def lower_bound(key) = open! { @native.lower_bound(key.b) }
    def upper_bound(key) = open! { @native.upper_bound(key.b) }
    def range(start = ''.b, range_end = nil) = open! { @native.range(start.b, range_end&.b) }
    def prefix(prefix) = open! { @native.prefix(prefix.b) }
    def range_page(cursor = nil, range_end = nil, limit = 256) = open! { @native.range_page(cursor, range_end&.b, limit) }
    def prefix_page(prefix, cursor = nil, limit = 256) = open! { @native.prefix_page(prefix.b, cursor, limit) }
    def reverse_page(cursor = nil, start = ''.b, limit = 256) = open! { @native.reverse_page(cursor, start.b, limit) }
    def prefix_reverse_page(prefix, cursor = nil, limit = 256) = open! { @native.prefix_reverse_page(prefix.b, cursor, limit) }
    def prove_key(key) = open! { @native.prove_key(key.b) }
    def prove_keys(keys) = open! { @native.prove_keys(keys.map(&:b)) }
    def prove_range(start = ''.b, range_end = nil) = open! { @native.prove_range(start.b, range_end&.b) }
    def prove_prefix(prefix) = open! { @native.prove_prefix(prefix.b) }
    def prove_range_page(cursor = nil, range_end = nil, limit = 256) = open! { @native.prove_range_page(cursor, range_end&.b, limit) }
    def stats = open! { @native.stats }
    def export = open! { @native.export }
    def read = open! { ReadSession.new(@native.read_session) }
    def close = @closed = true

    private

    def open!
      raise 'map snapshot is closed' if @closed
      yield
    end
  end

  class ReadSession
    def initialize(native)
      @native = native
      @closed = false
    end

    def get(key) = open! { @native.get(key.b) }
    def get_many(keys) = open! { @native.get_many(keys.map(&:b)) }
    def scan_range_view(start = ''.b, range_end = nil, &block)
      open! do
        PackedPage.scan_range_view(
          @native.fast_handle, start.b, range_end&.b, &block
        )
      end
    end
    def close = @closed = true

    def use
      raise 'read session is closed' if @closed
      return self unless block_given?

      begin
        yield self
      ensure
        close
      end
    end

    private

    def open!
      raise 'read session is closed' if @closed
      yield
    end
  end

  class ProximityMap
    def initialize(native)
      @native = native
      @closed = false
    end

    def get(key) = open! { @native.get(key.b) }
    def contains?(key) = open! { @native.contains_key(key.b) }
    def count = open! { @native.count }
    def config = open! { @native.config }
    def descriptor = open! { @native.descriptor }
    def build_hnsw(config = Prolly.default_hnsw_config, limits = Prolly.default_hnsw_build_limits)
      open! do
        result = @native.build_hnsw(config, limits)
        HnswBuildResult.new(index: HnswIndex.new(result.index), stats: result.stats)
      end
    end
    def load_hnsw(manifest) = open! { HnswIndex.new(@native.load_hnsw(manifest.b)) }
    def build_pq(config: Prolly.default_pq_config, worker_threads: 1,
                 limits: Prolly.default_pq_build_limits)
      open! do
        result = @native.build_pq(config, worker_threads, limits)
        ProductQuantizationBuildResult.new(
          index: ProductQuantizer.new(result.index),
          stats: result.stats
        )
      end
    end
    def load_pq(manifest) = open! { ProductQuantizer.new(@native.load_pq(manifest.b)) }
    def build_composite_hnsw(base_map, base, config: Prolly.default_composite_accelerator_config,
                             limits: Prolly.default_composite_build_limits)
      open! do
        result = @native.build_composite_hnsw(
          base_map.send(:native_for_accelerator), base.send(:native_for_composite), config, limits
        )
        CompositeBuildOutcome.new(
          accelerator: result.accelerator && CompositeAccelerator.new(result.accelerator),
          reasons: result.reasons,
          stats: result.stats
        )
      end
    end
    def build_composite_pq(base_map, base, config: Prolly.default_composite_accelerator_config,
                           limits: Prolly.default_composite_build_limits)
      open! do
        result = @native.build_composite_pq(
          base_map.send(:native_for_accelerator), base.send(:native_for_composite), config, limits
        )
        CompositeBuildOutcome.new(
          accelerator: result.accelerator && CompositeAccelerator.new(result.accelerator),
          reasons: result.reasons,
          stats: result.stats
        )
      end
    end
    def portable_rebuild_outcome(result)
      CompositeBuildOrRebuildOutcome.new(
        kind: result.kind,
        composite: result.composite && CompositeAccelerator.new(result.composite),
        hnsw: result.hnsw && HnswIndex.new(result.hnsw),
        pq: result.pq && ProductQuantizer.new(result.pq),
        reasons: result.reasons,
        composite_stats: result.composite_stats,
        hnsw_stats: result.hnsw_stats,
        pq_stats: result.pq_stats
      )
    end
    private :portable_rebuild_outcome
    def build_or_rebuild_composite_hnsw(
      base_map, base, config: Prolly.default_composite_accelerator_config,
      limits: Prolly.default_composite_build_limits,
      rebuild: Prolly.default_composite_rebuild_options
    )
      open! do
        portable_rebuild_outcome(
          @native.build_or_rebuild_composite_hnsw(
            base_map.send(:native_for_accelerator), base.send(:native_for_composite),
            config, limits, rebuild
          )
        )
      end
    end
    def build_or_rebuild_composite_pq(
      base_map, base, config: Prolly.default_composite_accelerator_config,
      limits: Prolly.default_composite_build_limits,
      rebuild: Prolly.default_composite_rebuild_options
    )
      open! do
        portable_rebuild_outcome(
          @native.build_or_rebuild_composite_pq(
            base_map.send(:native_for_accelerator), base.send(:native_for_composite),
            config, limits, rebuild
          )
        )
      end
    end
    def load_composite(manifest) = open! { CompositeAccelerator.new(@native.load_composite(manifest.b)) }
    def build_accelerator_catalog(hnsw: nil, pq: nil, composite: nil)
      open! do
        AcceleratorCatalog.new(
          @native.build_accelerator_catalog(
            hnsw&.send(:native_for_composite),
            pq&.send(:native_for_composite),
            composite&.send(:native_for_composite)
          )
        )
      end
    end
    def load_accelerator_catalog(manifest)
      open! { AcceleratorCatalog.new(@native.load_accelerator_catalog(manifest.b)) }
    end
    def verify = open! { @native.verify }
    def prove_membership(key) = open! { @native.prove_membership(key.b) }
    def prove_search(request, limits = Prolly.default_content_graph_limits)
      owned = Prolly.owned_proximity_search_request(request)
      open! { ProximitySearchProof.new(@native.prove_search(owned, limits)) }
    end
    def prove_search_exact(query, k, limits = Prolly.default_content_graph_limits)
      prove_search(Prolly.exact_proximity_search_request(query, k), limits)
    end
    def prove_structure(limits = Prolly.default_content_graph_limits) = open! { @native.prove_structure(limits) }
    def clear_cache = open! { @native.clear_content_cache }
    def mutate(mutations)
      open! do
        result = @native.mutate(mutations)
        [ProximityMap.new(result.map), result.stats]
      end
    end
    def rebuild(mutations) = open! { ProximityMap.new(@native.rebuild(mutations)) }
    def read = open! { ProximityReadSession.new(@native.read_session) }

    def search(request)
      read.use { |session| session.search(request) }
    end

    def search_exact(query, k)
      read.use { |session| session.search_exact(query, k) }
    end

    def scan_records(&block)
      raise ArgumentError, 'scan_records requires a block' unless block
      scan_record_views do |record|
        block.call(ProximityRecordRecord.new(
          key: record.key.bytes, vector: record.vector.to_a, value: record.value.bytes
        ))
      end.visited
    end
    def scan_record_views(&block)
      raise ArgumentError, 'scan_record_views requires a block' unless block
      open! { PackedPage.proximity_scan_view(@native.fast_handle, &block) }
    end

    def search_view(query, k, &block)
      read.use { |session| session.search_view(query, k, &block) }
    end

    def close = @closed = true

    private

    def native_for_accelerator = open! { @native }

    def open!
      raise 'proximity map is closed' if @closed
      yield
    end
  end

  class HnswIndex
    def initialize(native)
      @native = native
      @closed = false
    end

    def manifest = open! { @native.manifest }
    def source_descriptor = open! { @native.source_descriptor }
    def config = open! { @native.config }
    def canonical? = open! { @native.is_canonical }
    def search(map, request)
      open! { @native.search(map.send(:native_for_accelerator), Prolly.owned_proximity_search_request(request)) }
    end
    def prove_search(map, request, limits = Prolly.default_content_graph_limits)
      open! do
        ProximitySearchProof.new(
          @native.prove_search(
            map.send(:native_for_accelerator),
            Prolly.owned_proximity_search_request(request),
            limits
          )
        )
      end
    end
    def close = @closed = true

    def use
      raise 'HNSW index is closed' if @closed
      return self unless block_given?

      begin
        yield self
      ensure
        close
      end
    end

    private

    def native_for_composite = open! { @native }

    def open!
      raise 'HNSW index is closed' if @closed
      yield
    end
  end

  class ProductQuantizer
    def initialize(native)
      @native = native
      @closed = false
    end

    def manifest = open! { @native.manifest }
    def source_descriptor = open! { @native.source_descriptor }
    def config = open! { @native.config }
    def quality = open! { @native.quality }
    def search(map, request)
      open! do
        @native.search(
          map.send(:native_for_accelerator),
          Prolly.owned_proximity_search_request(request)
        )
      end
    end
    def prove_search(map, request, limits = Prolly.default_content_graph_limits)
      open! do
        ProximitySearchProof.new(
          @native.prove_search(
            map.send(:native_for_accelerator),
            Prolly.owned_proximity_search_request(request),
            limits
          )
        )
      end
    end
    def close = @closed = true

    def use
      raise 'product quantizer is closed' if @closed
      return self unless block_given?

      begin
        yield self
      ensure
        close
      end
    end

    private

    def native_for_composite = open! { @native }

    def open!
      raise 'product quantizer is closed' if @closed
      yield
    end
  end

  class CompositeAccelerator
    def initialize(native)
      @native = native
      @closed = false
    end

    def manifest = open! { @native.manifest }
    def current_source_descriptor = open! { @native.current_source_descriptor }
    def base_source_descriptor = open! { @native.base_source_descriptor }
    def base_kind = open! { @native.base_kind }
    def delta_count = open! { @native.delta_count }
    def shadow_count = open! { @native.shadow_count }
    def config = open! { @native.config }
    def build_stats = open! { @native.build_stats }
    def search(map, request)
      open! do
        @native.search(
          map.send(:native_for_accelerator), Prolly.owned_proximity_search_request(request)
        )
      end
    end
    def prove_search(map, request, limits = Prolly.default_content_graph_limits)
      open! do
        ProximitySearchProof.new(
          @native.prove_search(
            map.send(:native_for_accelerator),
            Prolly.owned_proximity_search_request(request), limits
          )
        )
      end
    end
    def close = @closed = true
    def use
      raise 'composite accelerator is closed' if @closed
      return self unless block_given?
      begin
        yield self
      ensure
        close
      end
    end

    private

    def native_for_composite = open! { @native }
    def open!
      raise 'composite accelerator is closed' if @closed
      yield
    end
  end

  class AcceleratorCatalog
    def initialize(native)
      @native = native
      @closed = false
    end

    def manifest = open! { @native.manifest }
    def source_descriptor = open! { @native.source_descriptor }
    def entries = open! { @native.entries }
    def search(map, request)
      open! do
        @native.search(
          map.send(:native_for_accelerator), Prolly.owned_proximity_search_request(request)
        )
      end
    end
    def prove_search(map, request, limits = Prolly.default_content_graph_limits)
      open! do
        ProximitySearchProof.new(
          @native.prove_search(
            map.send(:native_for_accelerator),
            Prolly.owned_proximity_search_request(request), limits
          )
        )
      end
    end
    def close = @closed = true
    def use
      raise 'accelerator catalog is closed' if @closed
      return self unless block_given?
      begin
        yield self
      ensure
        close
      end
    end

    private

    def open!
      raise 'accelerator catalog is closed' if @closed
      yield
    end
  end

  class ProximityReadSession
    def initialize(native)
      @native = native
      @closed = false
    end

    def get(key) = open! { @native.get(key.b) }
    def contains?(key) = open! { @native.contains_key(key.b) }
    def search(request) = open! { @native.search(Prolly.owned_proximity_search_request(request)) }
    def search_exact(query, k) = search(Prolly.exact_proximity_search_request(query, k))
    def scan_records(&block)
      raise ArgumentError, 'scan_records requires a block' unless block
      scan_record_views do |record|
        block.call(ProximityRecordRecord.new(
          key: record.key.bytes, vector: record.vector.to_a, value: record.value.bytes
        ))
      end.visited
    end
    def scan_record_views(&block)
      raise ArgumentError, 'scan_record_views requires a block' unless block
      open! { PackedPage.proximity_scan_view(@native.fast_handle, &block) }
    end
    def search_view(query, k, &block)
      open! { PackedPage.proximity_search_view(@native.fast_handle, query, k, &block) }
    end
    def close = @closed = true

    def use
      raise 'proximity read session is closed' if @closed
      return self unless block_given?

      begin
        yield self
      ensure
        close
      end
    end

    private

    def open!
      raise 'proximity read session is closed' if @closed
      yield
    end
  end

  class ProximitySearchProof
    def initialize(native)
      @native = native
      @closed = false
    end

    def source_descriptor = open! { @native.source_descriptor }
    def verify(expected_descriptor = nil, limits = Prolly.default_content_graph_limits)
      open! { @native.verify(expected_descriptor, limits) }
    end
    def close = @closed = true

    def use
      raise 'proximity search proof is closed' if @closed
      return self unless block_given?

      begin
        yield self
      ensure
        close
      end
    end

    private

    def open!
      raise 'proximity search proof is closed' if @closed
      yield
    end
  end
end
