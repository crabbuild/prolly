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

    def ensure_index(name) = open! { @native.ensure_index(name.b) }
    def get(key) = open! { @native.get(key.b) }
    def put(key, value) = open! { @native.put(key.b, value.b) }
    def delete(key) = open! { @native.delete(key.b) }
    def health = open! { @native.health }
    def snapshot = open! { IndexedSnapshot.new(@native.snapshot) }
    def close = @closed = true

    private

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
    def exact(term) = @native.exact(term.b)
    def prefix(prefix) = @native.prefix(prefix.b)
    def range(start, range_end = nil) = @native.range(start.b, range_end&.b)
    def records(term) = @native.records(term.b)
  end

  class VersionedMap
    def initialize(native)
      @native = native
      @closed = false
    end

    # Ruby makes `initialize` private even when generated as an FFI method.
    def initialize_map = open! { @native.__send__(:initialize) }
    def get(key) = open! { @native.get(key.b) }
    def put(key, value) = open! { @native.put(key.b, value.b) }
    def delete(key) = open! { @native.delete(key.b) }
    def snapshot = open! { @native.snapshot }

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

  class ProximityMap
    def initialize(native)
      @native = native
      @closed = false
    end

    def get(key) = open! { @native.get(key.b) }
    def verify = open! { @native.verify }

    def search_exact(query, k)
      open! { @native.search(Prolly.exact_proximity_search_request(query, k)) }
    end

    def search_view(query, k, &block)
      open! do
        PackedPage.proximity_search_view(@native.fast_handle, query, k, &block)
      end
    end

    def close = @closed = true

    private

    def open!
      raise 'proximity map is closed' if @closed
      yield
    end
  end
end
