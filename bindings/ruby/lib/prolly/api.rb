# frozen_string_literal: true

module Prolly
  ProximityRecord = Data.define(:key, :vector, :value)

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
    def id = open! { @native.id }
    def initialized? = open! { @native.is_initialized }
    def head = open! { @native.head }
    def head_id = open! { @native.head_id }
    def version(id) = open! { @native.version(id.b) }
    def versions = open! { @native.versions }
    def get(key) = open! { @native.get(key.b) }
    def put(key, value) = open! { @native.put(key.b, value.b) }
    def delete(key) = open! { @native.delete(key.b) }
    def snapshot = open! { @native.snapshot&.then { |value| MapSnapshot.new(value) } }
    def snapshot_at(id) = open! { @native.snapshot_at(id.b)&.then { |value| MapSnapshot.new(value) } }
    def backup = open! { @native.backup }
    def restore_backup(bundle) = open! { @native.restore_backup(bundle.b) }
    def keep_last(count) = open! { @native.keep_last(count) }
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

    def open!
      raise 'versioned map is closed' if @closed
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
    def verify = open! { @native.verify }
    def prove_membership(key) = open! { @native.prove_membership(key.b) }
    def prove_search(request, limits = Prolly.default_content_graph_limits)
      open! { ProximitySearchProof.new(@native.prove_search(request, limits)) }
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

    def search_exact(query, k)
      read.use { |session| session.search_exact(query, k) }
    end

    def search_view(query, k, &block)
      read.use { |session| session.search_view(query, k, &block) }
    end

    def close = @closed = true

    private

    def open!
      raise 'proximity map is closed' if @closed
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
    def search_exact(query, k) = open! { @native.search(Prolly.exact_proximity_search_request(query, k)) }
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
