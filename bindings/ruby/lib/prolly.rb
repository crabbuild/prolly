# frozen_string_literal: true

require_relative 'prolly/generated/prolly'

module Prolly
  module RubyMergeResolverCallbacks
    class VTable < FFI::Struct
      layout :uniffi_free, :pointer,
             :uniffi_clone, :pointer,
             :resolve, :pointer
    end

    class << self
      def insert(resolver)
        @mutex.synchronize do
          handle = @next_handle
          @next_handle += 2
          @resolvers[handle] = resolver
          handle
        end
      end

      def clone(handle)
        @mutex.synchronize do
          resolver = @resolvers[handle]
          return 0 if resolver.nil?

          clone_handle = @next_handle
          @next_handle += 2
          @resolvers[clone_handle] = resolver
          clone_handle
        end
      end

      def remove(handle)
        @mutex.synchronize { @resolvers.delete(handle) }
      end

      def fetch(handle)
        @mutex.synchronize { @resolvers[handle] }
      end

      def reset_status(status_pointer)
        return if status_pointer.null?

        status = RustCallStatus.new(status_pointer)
        status[:code] = 0
        status[:error_buf][:capacity] = 0
        status[:error_buf][:len] = 0
        status[:error_buf][:data] = FFI::Pointer::NULL
      end

      def copy_buffer_to_pointer(buffer, pointer)
        out = RustBuffer.new(pointer)
        out[:capacity] = buffer.capacity
        out[:len] = buffer.len
        out[:data] = buffer.data
      end

      def unresolved_buffer
        RustBuffer.alloc_from_TypeResolutionRecord(
          ResolutionRecord.new(kind: ResolutionKind::UNRESOLVED, value: nil)
        )
      end
    end

    @mutex = Mutex.new
    @next_handle = 1
    @resolvers = {}

    FREE = FFI::Function.new(:void, [:uint64]) do |handle|
      RubyMergeResolverCallbacks.remove(handle)
    end

    CLONE = FFI::Function.new(:uint64, [:uint64]) do |handle|
      RubyMergeResolverCallbacks.clone(handle)
    end

    RESOLVE = FFI::Function.new(
      :void,
      [:uint64, RustBuffer.by_value, :pointer, :pointer]
    ) do |handle, conflict_buffer, out_return, out_status|
      RubyMergeResolverCallbacks.reset_status(out_status)
      conflict = conflict_buffer.consumeIntoTypeConflictRecord
      resolver = RubyMergeResolverCallbacks.fetch(handle)
      result_buffer =
        if resolver.nil?
          RubyMergeResolverCallbacks.unresolved_buffer
        else
          resolution = resolver.resolve(conflict)
          RustBuffer.alloc_from_TypeResolutionRecord(resolution)
        end
      RubyMergeResolverCallbacks.copy_buffer_to_pointer(result_buffer, out_return)
    rescue StandardError
      RubyMergeResolverCallbacks.copy_buffer_to_pointer(
        RubyMergeResolverCallbacks.unresolved_buffer,
        out_return
      )
    end

    VTABLE = VTable.new
    VTABLE[:uniffi_free] = FREE
    VTABLE[:uniffi_clone] = CLONE
    VTABLE[:resolve] = RESOLVE

    Prolly.rust_call(
      :uniffi_prolly_bindings_fn_init_callback_vtable_mergeresolvercallback,
      VTABLE
    )
  end

  class MergeResolverCallback
    class << self
      alias uniffi_lower_rust_handle uniffi_lower

      def uniffi_lower(instance)
        if instance.instance_variable_defined?(:@handle)
          uniffi_lower_rust_handle(instance)
        else
          RubyMergeResolverCallbacks.insert(instance)
        end
      end
    end
  end

  module RubyCrdtResolverCallbacks
    class VTable < FFI::Struct
      layout :uniffi_free, :pointer,
             :uniffi_clone, :pointer,
             :resolve, :pointer
    end

    class << self
      def insert(resolver)
        @mutex.synchronize do
          handle = @next_handle
          @next_handle += 2
          @resolvers[handle] = resolver
          handle
        end
      end

      def clone(handle)
        @mutex.synchronize do
          resolver = @resolvers[handle]
          return 0 if resolver.nil?

          clone_handle = @next_handle
          @next_handle += 2
          @resolvers[clone_handle] = resolver
          clone_handle
        end
      end

      def remove(handle)
        @mutex.synchronize { @resolvers.delete(handle) }
      end

      def fetch(handle)
        @mutex.synchronize { @resolvers[handle] }
      end

      def reset_status(status_pointer)
        return if status_pointer.null?

        status = RustCallStatus.new(status_pointer)
        status[:code] = 0
        status[:error_buf][:capacity] = 0
        status[:error_buf][:len] = 0
        status[:error_buf][:data] = FFI::Pointer::NULL
      end

      def copy_buffer_to_pointer(buffer, pointer)
        out = RustBuffer.new(pointer)
        out[:capacity] = buffer.capacity
        out[:len] = buffer.len
        out[:data] = buffer.data
      end

      def delete_buffer
        RustBuffer.alloc_from_TypeCrdtResolutionRecord(
          CrdtResolutionRecord.new(kind: CrdtResolutionKind::DELETE, value: nil)
        )
      end
    end

    @mutex = Mutex.new
    @next_handle = 1
    @resolvers = {}

    FREE = FFI::Function.new(:void, [:uint64]) do |handle|
      RubyCrdtResolverCallbacks.remove(handle)
    end

    CLONE = FFI::Function.new(:uint64, [:uint64]) do |handle|
      RubyCrdtResolverCallbacks.clone(handle)
    end

    RESOLVE = FFI::Function.new(
      :void,
      [:uint64, RustBuffer.by_value, :pointer, :pointer]
    ) do |handle, conflict_buffer, out_return, out_status|
      RubyCrdtResolverCallbacks.reset_status(out_status)
      conflict = conflict_buffer.consumeIntoTypeConflictRecord
      resolver = RubyCrdtResolverCallbacks.fetch(handle)
      result_buffer =
        if resolver.nil?
          RubyCrdtResolverCallbacks.delete_buffer
        else
          resolution = resolver.resolve(conflict)
          RustBuffer.alloc_from_TypeCrdtResolutionRecord(resolution)
        end
      RubyCrdtResolverCallbacks.copy_buffer_to_pointer(result_buffer, out_return)
    rescue StandardError
      RubyCrdtResolverCallbacks.copy_buffer_to_pointer(
        RubyCrdtResolverCallbacks.delete_buffer,
        out_return
      )
    end

    VTABLE = VTable.new
    VTABLE[:uniffi_free] = FREE
    VTABLE[:uniffi_clone] = CLONE
    VTABLE[:resolve] = RESOLVE

    Prolly.rust_call(
      :uniffi_prolly_bindings_fn_init_callback_vtable_crdtresolvercallback,
      VTABLE
    )
  end

  class CrdtResolverCallback
    class << self
      alias uniffi_lower_rust_handle uniffi_lower

      def uniffi_lower(instance)
        if instance.instance_variable_defined?(:@handle)
          uniffi_lower_rust_handle(instance)
        else
          RubyCrdtResolverCallbacks.insert(instance)
        end
      end
    end
  end

  module RubyHostStoreCallbacks
    class VTable < FFI::Struct
      layout :uniffi_free, :pointer,
             :uniffi_clone, :pointer,
             :get, :pointer,
             :put, :pointer,
             :delete, :pointer,
             :batch, :pointer,
             :batch_get_ordered, :pointer,
             :prefers_batch_reads, :pointer,
             :supports_hints, :pointer,
             :get_hint, :pointer,
             :put_hint, :pointer,
             :list_node_cids, :pointer,
             :get_root, :pointer,
             :put_root, :pointer,
             :delete_root, :pointer,
             :compare_and_swap_root, :pointer,
             :list_roots, :pointer
    end

    class << self
      def insert(store)
        @mutex.synchronize do
          handle = @next_handle
          @next_handle += 2
          @stores[handle] = store
          handle
        end
      end

      def clone(handle)
        @mutex.synchronize do
          store = @stores[handle]
          return 0 if store.nil?

          clone_handle = @next_handle
          @next_handle += 2
          @stores[clone_handle] = store
          clone_handle
        end
      end

      def remove(handle)
        @mutex.synchronize { @stores.delete(handle) }
      end

      def fetch(handle)
        @mutex.synchronize { @stores[handle] }
      end

      def callback(handle)
        fetch(handle) || raise('host store callback is no longer available')
      end

      def reset_status(status_pointer)
        return if status_pointer.null?

        status = RustCallStatus.new(status_pointer)
        status[:code] = 0
        status[:error_buf][:capacity] = 0
        status[:error_buf][:len] = 0
        status[:error_buf][:data] = FFI::Pointer::NULL
      end

      def copy_buffer_to_pointer(buffer, pointer)
        out = RustBuffer.new(pointer)
        out[:capacity] = buffer.capacity
        out[:len] = buffer.len
        out[:data] = buffer.data
      end

      def write_record(record, allocator, pointer)
        copy_buffer_to_pointer(RustBuffer.public_send(allocator, record), pointer)
      end

      def call_with_record(out_return, out_status, allocator, error_factory)
        reset_status(out_status)
        write_record(yield, allocator, out_return)
      rescue StandardError => error
        write_record(error_factory.call(error), allocator, out_return)
      end

      def message(error)
        error.message.nil? || error.message.empty? ? error.to_s : error.message
      end

      def bytes_error(error)
        HostStoreBytesResultRecord.new(value: nil, error: message(error))
      end

      def unit_error(error)
        HostStoreUnitResultRecord.new(error: message(error))
      end

      def bool_error(error)
        HostStoreBoolResultRecord.new(value: false, error: message(error))
      end

      def batch_get_error(error)
        HostStoreBatchGetResultRecord.new(values: [], error: message(error))
      end

      def list_bytes_error(error)
        HostStoreListBytesResultRecord.new(values: [], error: message(error))
      end

      def root_error(error)
        HostStoreRootResultRecord.new(value: nil, error: message(error))
      end

      def cas_error(error)
        HostStoreRootCasResultRecord.new(applied: false, current: nil, error: message(error))
      end

      def list_roots_error(error)
        HostStoreListRootsResultRecord.new(values: [], error: message(error))
      end
    end

    @mutex = Mutex.new
    @next_handle = 1
    @stores = {}

    FREE = FFI::Function.new(:void, [:uint64]) do |handle|
      RubyHostStoreCallbacks.remove(handle)
    end

    CLONE = FFI::Function.new(:uint64, [:uint64]) do |handle|
      RubyHostStoreCallbacks.clone(handle)
    end

    GET = FFI::Function.new(
      :void,
      [:uint64, RustBuffer.by_value, :pointer, :pointer]
    ) do |handle, key_buffer, out_return, out_status|
      RubyHostStoreCallbacks.call_with_record(
        out_return,
        out_status,
        :alloc_from_TypeHostStoreBytesResultRecord,
        RubyHostStoreCallbacks.method(:bytes_error)
      ) do
        RubyHostStoreCallbacks.callback(handle).get(key_buffer.consumeIntoBytes)
      end
    end

    PUT = FFI::Function.new(
      :void,
      [:uint64, RustBuffer.by_value, RustBuffer.by_value, :pointer, :pointer]
    ) do |handle, key_buffer, value_buffer, out_return, out_status|
      RubyHostStoreCallbacks.call_with_record(
        out_return,
        out_status,
        :alloc_from_TypeHostStoreUnitResultRecord,
        RubyHostStoreCallbacks.method(:unit_error)
      ) do
        RubyHostStoreCallbacks.callback(handle).put(
          key_buffer.consumeIntoBytes,
          value_buffer.consumeIntoBytes
        )
      end
    end

    DELETE = FFI::Function.new(
      :void,
      [:uint64, RustBuffer.by_value, :pointer, :pointer]
    ) do |handle, key_buffer, out_return, out_status|
      RubyHostStoreCallbacks.call_with_record(
        out_return,
        out_status,
        :alloc_from_TypeHostStoreUnitResultRecord,
        RubyHostStoreCallbacks.method(:unit_error)
      ) do
        RubyHostStoreCallbacks.callback(handle).delete(key_buffer.consumeIntoBytes)
      end
    end

    BATCH = FFI::Function.new(
      :void,
      [:uint64, RustBuffer.by_value, :pointer, :pointer]
    ) do |handle, ops_buffer, out_return, out_status|
      RubyHostStoreCallbacks.call_with_record(
        out_return,
        out_status,
        :alloc_from_TypeHostStoreUnitResultRecord,
        RubyHostStoreCallbacks.method(:unit_error)
      ) do
        RubyHostStoreCallbacks.callback(handle).batch(
          ops_buffer.consumeIntoSequenceTypeMutationRecord
        )
      end
    end

    BATCH_GET_ORDERED = FFI::Function.new(
      :void,
      [:uint64, RustBuffer.by_value, :pointer, :pointer]
    ) do |handle, keys_buffer, out_return, out_status|
      RubyHostStoreCallbacks.call_with_record(
        out_return,
        out_status,
        :alloc_from_TypeHostStoreBatchGetResultRecord,
        RubyHostStoreCallbacks.method(:batch_get_error)
      ) do
        RubyHostStoreCallbacks.callback(handle).batch_get_ordered(
          keys_buffer.consumeIntoSequencebytes
        )
      end
    end

    PREFERS_BATCH_READS = FFI::Function.new(
      :void,
      [:uint64, :pointer, :pointer]
    ) do |handle, out_return, out_status|
      RubyHostStoreCallbacks.call_with_record(
        out_return,
        out_status,
        :alloc_from_TypeHostStoreBoolResultRecord,
        RubyHostStoreCallbacks.method(:bool_error)
      ) do
        RubyHostStoreCallbacks.callback(handle).prefers_batch_reads
      end
    end

    SUPPORTS_HINTS = FFI::Function.new(
      :void,
      [:uint64, :pointer, :pointer]
    ) do |handle, out_return, out_status|
      RubyHostStoreCallbacks.call_with_record(
        out_return,
        out_status,
        :alloc_from_TypeHostStoreBoolResultRecord,
        RubyHostStoreCallbacks.method(:bool_error)
      ) do
        RubyHostStoreCallbacks.callback(handle).supports_hints
      end
    end

    GET_HINT = FFI::Function.new(
      :void,
      [:uint64, RustBuffer.by_value, RustBuffer.by_value, :pointer, :pointer]
    ) do |handle, namespace_buffer, key_buffer, out_return, out_status|
      RubyHostStoreCallbacks.call_with_record(
        out_return,
        out_status,
        :alloc_from_TypeHostStoreBytesResultRecord,
        RubyHostStoreCallbacks.method(:bytes_error)
      ) do
        RubyHostStoreCallbacks.callback(handle).get_hint(
          namespace_buffer.consumeIntoBytes,
          key_buffer.consumeIntoBytes
        )
      end
    end

    PUT_HINT = FFI::Function.new(
      :void,
      [:uint64, RustBuffer.by_value, RustBuffer.by_value, RustBuffer.by_value, :pointer, :pointer]
    ) do |handle, namespace_buffer, key_buffer, value_buffer, out_return, out_status|
      RubyHostStoreCallbacks.call_with_record(
        out_return,
        out_status,
        :alloc_from_TypeHostStoreUnitResultRecord,
        RubyHostStoreCallbacks.method(:unit_error)
      ) do
        RubyHostStoreCallbacks.callback(handle).put_hint(
          namespace_buffer.consumeIntoBytes,
          key_buffer.consumeIntoBytes,
          value_buffer.consumeIntoBytes
        )
      end
    end

    LIST_NODE_CIDS = FFI::Function.new(
      :void,
      [:uint64, :pointer, :pointer]
    ) do |handle, out_return, out_status|
      RubyHostStoreCallbacks.call_with_record(
        out_return,
        out_status,
        :alloc_from_TypeHostStoreListBytesResultRecord,
        RubyHostStoreCallbacks.method(:list_bytes_error)
      ) do
        RubyHostStoreCallbacks.callback(handle).list_node_cids
      end
    end

    GET_ROOT = FFI::Function.new(
      :void,
      [:uint64, RustBuffer.by_value, :pointer, :pointer]
    ) do |handle, name_buffer, out_return, out_status|
      RubyHostStoreCallbacks.call_with_record(
        out_return,
        out_status,
        :alloc_from_TypeHostStoreRootResultRecord,
        RubyHostStoreCallbacks.method(:root_error)
      ) do
        RubyHostStoreCallbacks.callback(handle).get_root(name_buffer.consumeIntoBytes)
      end
    end

    PUT_ROOT = FFI::Function.new(
      :void,
      [:uint64, RustBuffer.by_value, RustBuffer.by_value, :pointer, :pointer]
    ) do |handle, name_buffer, manifest_buffer, out_return, out_status|
      RubyHostStoreCallbacks.call_with_record(
        out_return,
        out_status,
        :alloc_from_TypeHostStoreUnitResultRecord,
        RubyHostStoreCallbacks.method(:unit_error)
      ) do
        RubyHostStoreCallbacks.callback(handle).put_root(
          name_buffer.consumeIntoBytes,
          manifest_buffer.consumeIntoTypeRootManifestRecord
        )
      end
    end

    DELETE_ROOT = FFI::Function.new(
      :void,
      [:uint64, RustBuffer.by_value, :pointer, :pointer]
    ) do |handle, name_buffer, out_return, out_status|
      RubyHostStoreCallbacks.call_with_record(
        out_return,
        out_status,
        :alloc_from_TypeHostStoreUnitResultRecord,
        RubyHostStoreCallbacks.method(:unit_error)
      ) do
        RubyHostStoreCallbacks.callback(handle).delete_root(name_buffer.consumeIntoBytes)
      end
    end

    COMPARE_AND_SWAP_ROOT = FFI::Function.new(
      :void,
      [:uint64, RustBuffer.by_value, RustBuffer.by_value, RustBuffer.by_value, :pointer, :pointer]
    ) do |handle, name_buffer, expected_buffer, replacement_buffer, out_return, out_status|
      RubyHostStoreCallbacks.call_with_record(
        out_return,
        out_status,
        :alloc_from_TypeHostStoreRootCasResultRecord,
        RubyHostStoreCallbacks.method(:cas_error)
      ) do
        RubyHostStoreCallbacks.callback(handle).compare_and_swap_root(
          name_buffer.consumeIntoBytes,
          expected_buffer.consumeIntoOptionalTypeRootManifestRecord,
          replacement_buffer.consumeIntoOptionalTypeRootManifestRecord
        )
      end
    end

    LIST_ROOTS = FFI::Function.new(
      :void,
      [:uint64, :pointer, :pointer]
    ) do |handle, out_return, out_status|
      RubyHostStoreCallbacks.call_with_record(
        out_return,
        out_status,
        :alloc_from_TypeHostStoreListRootsResultRecord,
        RubyHostStoreCallbacks.method(:list_roots_error)
      ) do
        RubyHostStoreCallbacks.callback(handle).list_roots
      end
    end

    VTABLE = VTable.new
    VTABLE[:uniffi_free] = FREE
    VTABLE[:uniffi_clone] = CLONE
    VTABLE[:get] = GET
    VTABLE[:put] = PUT
    VTABLE[:delete] = DELETE
    VTABLE[:batch] = BATCH
    VTABLE[:batch_get_ordered] = BATCH_GET_ORDERED
    VTABLE[:prefers_batch_reads] = PREFERS_BATCH_READS
    VTABLE[:supports_hints] = SUPPORTS_HINTS
    VTABLE[:get_hint] = GET_HINT
    VTABLE[:put_hint] = PUT_HINT
    VTABLE[:list_node_cids] = LIST_NODE_CIDS
    VTABLE[:get_root] = GET_ROOT
    VTABLE[:put_root] = PUT_ROOT
    VTABLE[:delete_root] = DELETE_ROOT
    VTABLE[:compare_and_swap_root] = COMPARE_AND_SWAP_ROOT
    VTABLE[:list_roots] = LIST_ROOTS

    Prolly.rust_call(
      :uniffi_prolly_bindings_fn_init_callback_vtable_hoststorecallback,
      VTABLE
    )
  end

  class HostStoreCallback
    class << self
      alias uniffi_lower_rust_handle uniffi_lower

      def uniffi_lower(instance)
        if instance.instance_variable_defined?(:@handle)
          uniffi_lower_rust_handle(instance)
        else
          RubyHostStoreCallbacks.insert(instance)
        end
      end
    end
  end

  class Future
    def initialize(&block)
      @thread = Thread.new { block.call }
    end

    def value
      @thread.value
    end

    def wait
      @thread.join
      self
    end

    def complete?
      !@thread.alive?
    end
  end

  class AsyncBlobStore
    def self.memory
      new(ProllyBlobStore.memory)
    end

    def self.file(path)
      new(ProllyBlobStore.file(path))
    end

    def initialize(store)
      @store = store
    end

    attr_reader :store

    def method_missing(method_name, *args, &block)
      return super unless @store.respond_to?(method_name)

      future { @store.public_send(method_name, *unwrap_args(args), &block) }
    end

    def respond_to_missing?(method_name, include_private = false)
      @store.respond_to?(method_name, include_private) || super
    end

    private

    def future(&block)
      Future.new(&block)
    end

    def unwrap_args(args)
      args.map do |arg|
        case arg
        when AsyncBlobStore
          arg.store
        when AsyncEngine
          arg.engine
        else
          arg
        end
      end
    end
  end

  class AsyncEngine
    def self.memory(config = Prolly.default_config)
      new(ProllyEngine.memory(config))
    end

    def self.file(path, config = Prolly.default_config)
      new(ProllyEngine.file(path, config))
    end

    def self.sqlite(path, config = Prolly.default_config)
      new(ProllyEngine.sqlite(path, config))
    end

    def self.sqlite_in_memory(config = Prolly.default_config)
      new(ProllyEngine.sqlite_in_memory(config))
    end

    def initialize(engine)
      @engine = engine
    end

    attr_reader :engine

    def create
      future { @engine.create }
    end

    def get(tree, key)
      future { @engine.get(tree, key) }
    end

    def get_many(tree, keys)
      future { @engine.get_many(tree, keys) }
    end

    def put(tree, key, value)
      future { @engine.put(tree, key, value) }
    end

    def delete(tree, key)
      future { @engine.delete(tree, key) }
    end

    def batch(tree, mutations)
      future { @engine.batch(tree, mutations) }
    end

    def build_from_entries(entries)
      future { @engine.build_from_entries(entries) }
    end

    def build_from_sorted_entries(entries)
      future { @engine.build_from_sorted_entries(entries) }
    end

    def append_batch(tree, mutations)
      future { @engine.append_batch(tree, mutations) }
    end

    def range(tree, start, finish)
      future { @engine.range(tree, start, finish) }
    end

    def range_after(tree, after_key, finish)
      future { @engine.range_after(tree, after_key, finish) }
    end

    def range_from_cursor(tree, cursor, finish)
      future { @engine.range_from_cursor(tree, cursor, finish) }
    end

    def range_page(tree, cursor, finish, limit)
      future { @engine.range_page(tree, cursor, finish, limit) }
    end

    def diff(base, other)
      future { @engine.diff(base, other) }
    end

    def range_diff(base, other, start, finish)
      future { @engine.range_diff(base, other, start, finish) }
    end

    def diff_from_cursor(base, other, cursor, finish)
      future { @engine.diff_from_cursor(base, other, cursor, finish) }
    end

    def diff_page(base, other, cursor, finish, limit)
      future { @engine.diff_page(base, other, cursor, finish, limit) }
    end

    def method_missing(method_name, *args, &block)
      return super unless @engine.respond_to?(method_name)

      future { @engine.public_send(method_name, *unwrap_args(args), &block) }
    end

    def respond_to_missing?(method_name, include_private = false)
      @engine.respond_to?(method_name, include_private) || super
    end

    private

    def future(&block)
      Future.new(&block)
    end

    def unwrap_args(args)
      args.map do |arg|
        case arg
        when AsyncBlobStore
          arg.store
        when AsyncEngine
          arg.engine
        else
          arg
        end
      end
    end
  end
end
