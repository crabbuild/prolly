# frozen_string_literal: true

module Prolly
  module UniFFILib
    attach_function :prolly_init_foreign_remote_vtable,
                    :uniffi_prolly_bindings_fn_init_callback_vtable_foreignremotestore,
                    [:pointer], :void
    attach_function :prolly_async_open_remote_start,
                    :uniffi_prolly_bindings_fn_func_open_remote_prolly_engine,
                    [:uint64, RustBuffer.by_value], :uint64
    attach_function :prolly_async_engine_put_start,
                    :uniffi_prolly_bindings_fn_method_asyncprollyengine_put,
                    [:uint64, RustBuffer.by_value, RustBuffer.by_value, RustBuffer.by_value], :uint64
    attach_function :prolly_async_engine_get_start,
                    :uniffi_prolly_bindings_fn_method_asyncprollyengine_get,
                    [:uint64, RustBuffer.by_value, RustBuffer.by_value], :uint64
    attach_function :prolly_future_poll_u64,
                    :ffi_prolly_bindings_rust_future_poll_u64,
                    [:uint64, :pointer, :uint64], :void
    attach_function :prolly_future_complete_u64,
                    :ffi_prolly_bindings_rust_future_complete_u64,
                    [:uint64, RustCallStatus.by_ref], :uint64
    attach_function :prolly_future_free_u64,
                    :ffi_prolly_bindings_rust_future_free_u64,
                    [:uint64], :void
    attach_function :prolly_future_poll_buffer,
                    :ffi_prolly_bindings_rust_future_poll_rust_buffer,
                    [:uint64, :pointer, :uint64], :void
    attach_function :prolly_future_complete_buffer,
                    :ffi_prolly_bindings_rust_future_complete_rust_buffer,
                    [:uint64, RustCallStatus.by_ref], RustBuffer.by_value
    attach_function :prolly_future_free_buffer,
                    :ffi_prolly_bindings_rust_future_free_rust_buffer,
                    [:uint64], :void
  end

  module RubyRustFutures
    class << self
      def await_u64(handle, error_module = nil)
        await_ready(handle, :prolly_future_poll_u64)
        Prolly.rust_call_with_error(error_module, :prolly_future_complete_u64, handle)
      ensure
        UniFFILib.prolly_future_free_u64(handle) if handle
      end

      def await_buffer(handle, error_module = nil)
        await_ready(handle, :prolly_future_poll_buffer)
        Prolly.rust_call_with_error(error_module, :prolly_future_complete_buffer, handle)
      ensure
        UniFFILib.prolly_future_free_buffer(handle) if handle
      end

      private

      def await_ready(handle, poll_method)
        loop do
          mutex = Mutex.new
          condition = ConditionVariable.new
          poll_code = nil
          continuation = FFI::Function.new(:void, %i[uint64 int8]) do |_data, code|
            mutex.synchronize do
              poll_code = code
              condition.signal
            end
          end
          UniFFILib.public_send(poll_method, handle, continuation, 0)
          mutex.synchronize do
            condition.wait(mutex) while poll_code.nil?
          end
          return if poll_code.zero?
        end
      end
    end
  end

  # UniFFI 0.31's Ruby generator emits Rust-side async objects but omits the
  # foreign async callback vtable. This small runtime shim supplies that vtable
  # so Ruby provider stores can implement ForeignRemoteStore.
  module RubyForeignRemoteStores
    class FutureResult < FFI::Struct
      layout :return_value, RustBuffer,
             :call_status, RustCallStatus
    end

    class DroppedCallback < FFI::Struct
      layout :handle, :uint64,
             :free, :pointer
    end

    class VTable < FFI::Struct
      layout :uniffi_free, :pointer,
             :uniffi_clone, :pointer,
             :descriptor, :pointer,
             :get_node, :pointer,
             :put_node, :pointer,
             :delete_node, :pointer,
             :batch_nodes, :pointer,
             :batch_get_nodes_ordered, :pointer,
             :list_node_cids, :pointer,
             :get_hint, :pointer,
             :put_hint, :pointer,
             :batch_put_nodes_with_hint, :pointer,
             :get_root_manifest, :pointer,
             :put_root_manifest, :pointer,
             :delete_root_manifest, :pointer,
             :compare_and_swap_root_manifest, :pointer,
             :list_root_manifests, :pointer,
             :commit_transaction, :pointer
    end

    SPECS = [
      [:descriptor, [], :alloc_from_TypeStoreDescriptorResultRecord],
      [:get_node, [:consumeIntoBytes], :alloc_from_TypeOptionalBytesResultRecord],
      [:put_node, %i[consumeIntoBytes consumeIntoBytes], :alloc_from_TypeUnitResultRecord],
      [:delete_node, [:consumeIntoBytes], :alloc_from_TypeUnitResultRecord],
      [:batch_nodes, [:consumeIntoSequenceTypeNodeMutationRecord], :alloc_from_TypeUnitResultRecord],
      [:batch_get_nodes_ordered, [:consumeIntoSequencebytes], :alloc_from_TypeOptionalBytesListResultRecord],
      [:list_node_cids, [], :alloc_from_TypeBytesListResultRecord],
      [:get_hint, %i[consumeIntoBytes consumeIntoBytes], :alloc_from_TypeOptionalBytesResultRecord],
      [:put_hint, %i[consumeIntoBytes consumeIntoBytes consumeIntoBytes], :alloc_from_TypeUnitResultRecord],
      [:batch_put_nodes_with_hint, %i[consumeIntoSequenceTypeNodeEntryRecord consumeIntoBytes consumeIntoBytes consumeIntoBytes], :alloc_from_TypeUnitResultRecord],
      [:get_root_manifest, [:consumeIntoBytes], :alloc_from_TypeOptionalBytesResultRecord],
      [:put_root_manifest, %i[consumeIntoBytes consumeIntoBytes], :alloc_from_TypeUnitResultRecord],
      [:delete_root_manifest, [:consumeIntoBytes], :alloc_from_TypeUnitResultRecord],
      [:compare_and_swap_root_manifest, %i[consumeIntoBytes consumeIntoTypeOptionalBytesRecord consumeIntoTypeOptionalBytesRecord], :alloc_from_TypeRootCasResultRecord],
      [:list_root_manifests, [], :alloc_from_TypeNamedBytesListResultRecord],
      [:commit_transaction, %i[consumeIntoSequenceTypeNodeMutationRecord consumeIntoSequenceTypeRootConditionRecord consumeIntoSequenceTypeRootWriteRecord], :alloc_from_TypeTransactionResultRecord]
    ].freeze

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
          return 0 unless store

          cloned = @next_handle
          @next_handle += 2
          @stores[cloned] = store
          cloned
        end
      end

      def remove(handle) = @mutex.synchronize { @stores.delete(handle) }
      def fetch(handle) = @mutex.synchronize { @stores.fetch(handle) }

      def callback(method_name, readers, writer)
        arguments = [:uint64] + Array.new(readers.length, RustBuffer.by_value) + [:pointer, :uint64, :pointer]
        FFI::Function.new(:void, arguments) do |handle, *raw|
          dropped_pointer = raw.pop
          callback_data = raw.pop
          completion_pointer = raw.pop
          install_noop_drop(dropped_pointer)
          values = raw.zip(readers).map { |buffer, reader| buffer.public_send(reader) }
          result = fetch(handle).public_send(method_name, *values)
          complete(completion_pointer, callback_data, RustBuffer.public_send(writer, result), nil)
        rescue StandardError => error
          complete(completion_pointer, callback_data, nil, error)
        end
      end

      def install_noop_drop(pointer)
        return if pointer.null?

        dropped = DroppedCallback.new(pointer)
        dropped[:handle] = 0
        dropped[:free] = NOOP_DROP
      end

      def complete(pointer, callback_data, buffer, error)
        completion = FFI::Function.new(:void, [:uint64, FutureResult.by_value], pointer)
        result = FutureResult.new
        status = result[:call_status]
        if error
          status[:code] = CALL_PANIC
          status[:error_buf] = RustBuffer.allocFromString(error.message)
          result[:return_value] = RustBuffer.new
        else
          status[:code] = CALL_SUCCESS
          status[:error_buf] = RustBuffer.new
          result[:return_value] = buffer
        end
        completion.call(callback_data, result)
      end
    end

    @mutex = Mutex.new
    @next_handle = 1
    @stores = {}
    NOOP_DROP = FFI::Function.new(:void, [:uint64]) { |_handle| }
    FREE = FFI::Function.new(:void, [:uint64]) { |handle| remove(handle) }
    CLONE = FFI::Function.new(:uint64, [:uint64]) { |handle| clone(handle) }
    CALLBACKS = SPECS.map { |method_name, readers, writer| callback(method_name, readers, writer) }.freeze

    VTABLE = VTable.new
    VTABLE[:uniffi_free] = FREE
    VTABLE[:uniffi_clone] = CLONE
    SPECS.each_with_index { |(method_name, _readers, _writer), index| VTABLE[method_name] = CALLBACKS[index] }
    UniFFILib.prolly_init_foreign_remote_vtable(VTABLE)
  end

  class ForeignRemoteStore
    class << self
      alias uniffi_lower_rust_handle uniffi_lower unless method_defined?(:uniffi_lower_rust_handle)

      def uniffi_lower(instance)
        return uniffi_lower_rust_handle(instance) if instance.instance_variable_defined?(:@handle)

        RubyForeignRemoteStores.insert(instance)
      end
    end
  end

  def self.open_remote_prolly_engine(store, config)
    ForeignRemoteStore.uniffi_check_lower(store)
    RustBuffer.check_lower_TypeConfigRecord(config)
    future = UniFFILib.prolly_async_open_remote_start(
      ForeignRemoteStore.uniffi_lower(store), RustBuffer.alloc_from_TypeConfigRecord(config)
    )
    AsyncProllyEngine.uniffi_allocate(RubyRustFutures.await_u64(future, ProllyBindingError))
  end

  class AsyncProllyEngine
    def put(tree, key, value)
      future = UniFFILib.prolly_async_engine_put_start(
        uniffi_clone_handle,
        RustBuffer.alloc_from_TypeTreeRecord(tree),
        RustBuffer.allocFromBytes(Prolly.uniffi_bytes(key)),
        RustBuffer.allocFromBytes(Prolly.uniffi_bytes(value))
      )
      RubyRustFutures.await_buffer(future, ProllyBindingError).consumeIntoTypeTreeRecord
    end

    def get(tree, key)
      future = UniFFILib.prolly_async_engine_get_start(
        uniffi_clone_handle,
        RustBuffer.alloc_from_TypeTreeRecord(tree),
        RustBuffer.allocFromBytes(Prolly.uniffi_bytes(key))
      )
      RubyRustFutures.await_buffer(future, ProllyBindingError).consumeIntoOptionalbytes
    end
  end
end
