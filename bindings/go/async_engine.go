package prolly

/*
#include <stdint.h>

typedef struct RustBuffer {
	uint64_t capacity;
	uint64_t len;
	uint8_t *data;
} RustBuffer;

typedef struct RustCallStatus {
	int8_t code;
	RustBuffer error_buf;
} RustCallStatus;

typedef void (*UniffiRustFutureContinuationCallback)(uint64_t, int8_t);
extern void prolly_go_rust_future_continuation(uint64_t, int8_t);

extern uint64_t uniffi_prolly_bindings_fn_constructor_asyncprollyengine_new(uint64_t, RustBuffer);
extern uint64_t uniffi_prolly_bindings_fn_clone_asyncprollyengine(uint64_t, RustCallStatus *);
extern void uniffi_prolly_bindings_fn_free_asyncprollyengine(uint64_t, RustCallStatus *);
extern RustBuffer uniffi_prolly_bindings_fn_method_asyncprollyengine_create(uint64_t, RustCallStatus *);
extern uint64_t uniffi_prolly_bindings_fn_method_asyncprollyengine_get(uint64_t, RustBuffer, RustBuffer);
extern uint64_t uniffi_prolly_bindings_fn_method_asyncprollyengine_get_many(uint64_t, RustBuffer, RustBuffer);
extern uint64_t uniffi_prolly_bindings_fn_method_asyncprollyengine_put(uint64_t, RustBuffer, RustBuffer, RustBuffer);
extern uint64_t uniffi_prolly_bindings_fn_method_asyncprollyengine_delete(uint64_t, RustBuffer, RustBuffer);
extern uint64_t uniffi_prolly_bindings_fn_method_asyncprollyengine_batch(uint64_t, RustBuffer, RustBuffer);
extern uint64_t uniffi_prolly_bindings_fn_method_asyncprollyengine_range(uint64_t, RustBuffer, RustBuffer, RustBuffer);
extern uint64_t uniffi_prolly_bindings_fn_method_asyncprollyengine_prefix(uint64_t, RustBuffer, RustBuffer);
extern uint64_t uniffi_prolly_bindings_fn_method_asyncprollyengine_range_page(uint64_t, RustBuffer, RustBuffer, RustBuffer, uint64_t);
extern uint64_t uniffi_prolly_bindings_fn_method_asyncprollyengine_diff(uint64_t, RustBuffer, RustBuffer);
extern uint64_t uniffi_prolly_bindings_fn_method_asyncprollyengine_merge(uint64_t, RustBuffer, RustBuffer, RustBuffer, RustBuffer);
extern uint64_t uniffi_prolly_bindings_fn_method_asyncprollyengine_collect_stats(uint64_t, RustBuffer);
extern uint64_t uniffi_prolly_bindings_fn_method_asyncprollyengine_publish_named_root(uint64_t, RustBuffer, RustBuffer);
extern uint64_t uniffi_prolly_bindings_fn_method_asyncprollyengine_publish_named_root_at_millis(uint64_t, RustBuffer, RustBuffer, uint64_t);
extern uint64_t uniffi_prolly_bindings_fn_method_asyncprollyengine_load_named_root(uint64_t, RustBuffer);
extern uint64_t uniffi_prolly_bindings_fn_method_asyncprollyengine_list_named_roots(uint64_t);
extern uint64_t uniffi_prolly_bindings_fn_method_asyncprollyengine_compare_and_swap_named_root(uint64_t, RustBuffer, RustBuffer, RustBuffer);
extern uint64_t uniffi_prolly_bindings_fn_method_asyncprollyengine_delete_named_root(uint64_t, RustBuffer);
extern uint64_t uniffi_prolly_bindings_fn_method_asyncprollyengine_begin_transaction(uint64_t);

extern uint64_t uniffi_prolly_bindings_fn_clone_asyncprollytransaction(uint64_t, RustCallStatus *);
extern void uniffi_prolly_bindings_fn_free_asyncprollytransaction(uint64_t, RustCallStatus *);
extern uint64_t uniffi_prolly_bindings_fn_method_asyncprollytransaction_create(uint64_t);
extern uint64_t uniffi_prolly_bindings_fn_method_asyncprollytransaction_get(uint64_t, RustBuffer, RustBuffer);
extern uint64_t uniffi_prolly_bindings_fn_method_asyncprollytransaction_put(uint64_t, RustBuffer, RustBuffer, RustBuffer);
extern uint64_t uniffi_prolly_bindings_fn_method_asyncprollytransaction_delete(uint64_t, RustBuffer, RustBuffer);
extern uint64_t uniffi_prolly_bindings_fn_method_asyncprollytransaction_batch(uint64_t, RustBuffer, RustBuffer);
extern uint64_t uniffi_prolly_bindings_fn_method_asyncprollytransaction_publish_named_root(uint64_t, RustBuffer, RustBuffer);
extern uint64_t uniffi_prolly_bindings_fn_method_asyncprollytransaction_publish_named_root_at_millis(uint64_t, RustBuffer, RustBuffer, uint64_t);
extern uint64_t uniffi_prolly_bindings_fn_method_asyncprollytransaction_load_named_root(uint64_t, RustBuffer);
extern uint64_t uniffi_prolly_bindings_fn_method_asyncprollytransaction_compare_and_swap_named_root(uint64_t, RustBuffer, RustBuffer, RustBuffer);
extern uint64_t uniffi_prolly_bindings_fn_method_asyncprollytransaction_delete_named_root(uint64_t, RustBuffer);
extern uint64_t uniffi_prolly_bindings_fn_method_asyncprollytransaction_commit(uint64_t);
extern uint64_t uniffi_prolly_bindings_fn_method_asyncprollytransaction_rollback(uint64_t);

extern void ffi_prolly_bindings_rust_future_poll_u64(uint64_t, UniffiRustFutureContinuationCallback, uint64_t);
extern void ffi_prolly_bindings_rust_future_cancel_u64(uint64_t);
extern void ffi_prolly_bindings_rust_future_free_u64(uint64_t);
extern uint64_t ffi_prolly_bindings_rust_future_complete_u64(uint64_t, RustCallStatus *);
extern void ffi_prolly_bindings_rust_future_poll_rust_buffer(uint64_t, UniffiRustFutureContinuationCallback, uint64_t);
extern void ffi_prolly_bindings_rust_future_cancel_rust_buffer(uint64_t);
extern void ffi_prolly_bindings_rust_future_free_rust_buffer(uint64_t);
extern RustBuffer ffi_prolly_bindings_rust_future_complete_rust_buffer(uint64_t, RustCallStatus *);
extern void ffi_prolly_bindings_rust_future_poll_void(uint64_t, UniffiRustFutureContinuationCallback, uint64_t);
extern void ffi_prolly_bindings_rust_future_cancel_void(uint64_t);
extern void ffi_prolly_bindings_rust_future_free_void(uint64_t);
extern void ffi_prolly_bindings_rust_future_complete_void(uint64_t, RustCallStatus *);

static void prolly_poll_future_u64(uint64_t future, uint64_t callback_data) {
	ffi_prolly_bindings_rust_future_poll_u64(future, prolly_go_rust_future_continuation, callback_data);
}
static void prolly_poll_future_rust_buffer(uint64_t future, uint64_t callback_data) {
	ffi_prolly_bindings_rust_future_poll_rust_buffer(future, prolly_go_rust_future_continuation, callback_data);
}
static void prolly_poll_future_void(uint64_t future, uint64_t callback_data) {
	ffi_prolly_bindings_rust_future_poll_void(future, prolly_go_rust_future_continuation, callback_data);
}
*/
import "C"

import (
	"context"
	"errors"
	"math"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
)

var (
	rustFuturePollNext atomic.Uint64
	rustFuturePollMu   sync.Mutex
	rustFuturePolls    = map[uint64]chan int8{}
)

var ErrAsyncEngineClosed = errors.New("prolly async engine is closed")

type AsyncEngine struct {
	handle      uint64
	storeHandle uint64
	closed      atomic.Bool
	mu          sync.RWMutex
}

type AsyncTransaction struct {
	handle    uint64
	closed    atomic.Bool
	completed atomic.Bool
	mu        sync.RWMutex
}

func NewAsyncEngine(ctx context.Context, store RemoteStore, config *Config) (*AsyncEngine, error) {
	if store == nil {
		return nil, errors.New("remote store must not be nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	registerRemoteVTable()
	storeHandle := registerRemoteStore(store)

	resolvedConfig := config
	if resolvedConfig == nil {
		value, err := DefaultConfig()
		if err != nil {
			removeRemoteStoreHandle(storeHandle)
			return nil, err
		}
		resolvedConfig = &value
	}
	configBuffer, err := rustBufferFromBytes(resolvedConfig.raw)
	if err != nil {
		removeRemoteStoreHandle(storeHandle)
		return nil, err
	}
	future := C.uniffi_prolly_bindings_fn_constructor_asyncprollyengine_new(C.uint64_t(storeHandle), configBuffer)
	handle, err := awaitRustFutureU64(ctx, uint64(future))
	if err != nil {
		removeRemoteStoreHandle(storeHandle)
		return nil, err
	}
	engine := &AsyncEngine{handle: handle, storeHandle: storeHandle}
	runtime.SetFinalizer(engine, func(value *AsyncEngine) { _ = value.Close() })
	return engine, nil
}

func awaitRustFutureU64(ctx context.Context, future uint64) (uint64, error) {
	defer C.ffi_prolly_bindings_rust_future_free_u64(C.uint64_t(future))
	if err := pollRustFuture(ctx, future, func(data uint64) {
		C.prolly_poll_future_u64(C.uint64_t(future), C.uint64_t(data))
	}, func() {
		C.ffi_prolly_bindings_rust_future_cancel_u64(C.uint64_t(future))
	}); err != nil {
		return 0, err
	}
	var status C.RustCallStatus
	handle := C.ffi_prolly_bindings_rust_future_complete_u64(C.uint64_t(future), &status)
	if err := statusError(&status); err != nil {
		return 0, err
	}
	return uint64(handle), nil
}

func awaitRustFutureBuffer(ctx context.Context, future uint64) ([]byte, error) {
	defer C.ffi_prolly_bindings_rust_future_free_rust_buffer(C.uint64_t(future))
	if err := pollRustFuture(ctx, future, func(data uint64) {
		C.prolly_poll_future_rust_buffer(C.uint64_t(future), C.uint64_t(data))
	}, func() {
		C.ffi_prolly_bindings_rust_future_cancel_rust_buffer(C.uint64_t(future))
	}); err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	buffer := C.ffi_prolly_bindings_rust_future_complete_rust_buffer(C.uint64_t(future), &status)
	if err := statusError(&status); err != nil {
		return nil, err
	}
	defer freeRustBuffer(buffer)
	return copyRustBuffer(buffer), nil
}

func awaitRustFutureVoid(ctx context.Context, future uint64) error {
	defer C.ffi_prolly_bindings_rust_future_free_void(C.uint64_t(future))
	if err := pollRustFuture(ctx, future, func(data uint64) {
		C.prolly_poll_future_void(C.uint64_t(future), C.uint64_t(data))
	}, func() {
		C.ffi_prolly_bindings_rust_future_cancel_void(C.uint64_t(future))
	}); err != nil {
		return err
	}
	var status C.RustCallStatus
	C.ffi_prolly_bindings_rust_future_complete_void(C.uint64_t(future), &status)
	return statusError(&status)
}

func pollRustFuture(ctx context.Context, future uint64, poll func(uint64), cancel func()) error {
	for {
		callbackData := rustFuturePollNext.Add(1)
		result := make(chan int8, 1)
		rustFuturePollMu.Lock()
		rustFuturePolls[callbackData] = result
		rustFuturePollMu.Unlock()
		poll(callbackData)

		select {
		case code := <-result:
			if code == 0 {
				return nil
			}
			if code != 1 {
				cancel()
				return errors.New("invalid UniFFI Rust future poll code")
			}
		case <-ctx.Done():
			rustFuturePollMu.Lock()
			delete(rustFuturePolls, callbackData)
			rustFuturePollMu.Unlock()
			cancel()
			return ctx.Err()
		}
	}
}

func (e *AsyncEngine) Close() error {
	if e == nil || e.closed.Swap(true) {
		return nil
	}
	runtime.SetFinalizer(e, nil)
	e.mu.Lock()
	defer e.mu.Unlock()
	var status C.RustCallStatus
	if e.handle != 0 {
		C.uniffi_prolly_bindings_fn_free_asyncprollyengine(C.uint64_t(e.handle), &status)
		e.handle = 0
	}
	if e.storeHandle != 0 {
		removeRemoteStoreHandle(e.storeHandle)
		e.storeHandle = 0
	}
	return statusError(&status)
}

func (e *AsyncEngine) cloneHandle() (uint64, error) {
	if e == nil || e.closed.Load() {
		return 0, ErrAsyncEngineClosed
	}
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.handle == 0 || e.closed.Load() {
		return 0, ErrAsyncEngineClosed
	}
	var status C.RustCallStatus
	handle := C.uniffi_prolly_bindings_fn_clone_asyncprollyengine(C.uint64_t(e.handle), &status)
	if err := statusError(&status); err != nil {
		return 0, err
	}
	return uint64(handle), nil
}

func (e *AsyncEngine) Create() (Tree, error) {
	handle, err := e.cloneHandle()
	if err != nil {
		return Tree{}, err
	}
	var status C.RustCallStatus
	buffer := C.uniffi_prolly_bindings_fn_method_asyncprollyengine_create(C.uint64_t(handle), &status)
	if err := statusError(&status); err != nil {
		return Tree{}, err
	}
	defer freeRustBuffer(buffer)
	return decodeTree(copyRustBuffer(buffer))
}

func (e *AsyncEngine) Get(ctx context.Context, tree Tree, key []byte) ([]byte, bool, error) {
	result, err := e.callTreeBytes(ctx, tree, key, func(handle C.uint64_t, tree, key C.RustBuffer) C.uint64_t {
		return C.uniffi_prolly_bindings_fn_method_asyncprollyengine_get(handle, tree, key)
	})
	if err != nil {
		return nil, false, err
	}
	return decodeOptionalByteArray(result)
}

func (e *AsyncEngine) GetMany(ctx context.Context, tree Tree, keys [][]byte) ([][]byte, []bool, error) {
	handle, buffers, err := e.lowerCall(tree.raw, encodeByteArraySequence(keys))
	if err != nil {
		return nil, nil, err
	}
	future := C.uniffi_prolly_bindings_fn_method_asyncprollyengine_get_many(C.uint64_t(handle), buffers[0], buffers[1])
	result, err := awaitRustFutureBuffer(contextOrBackground(ctx), uint64(future))
	if err != nil {
		return nil, nil, err
	}
	return decodeOptionalByteArraySequence(result)
}

func (e *AsyncEngine) Put(ctx context.Context, tree Tree, key, value []byte) (Tree, error) {
	handle, buffers, err := e.lowerCall(tree.raw, encodeByteArray(key), encodeByteArray(value))
	if err != nil {
		return Tree{}, err
	}
	future := C.uniffi_prolly_bindings_fn_method_asyncprollyengine_put(C.uint64_t(handle), buffers[0], buffers[1], buffers[2])
	return awaitTree(ctx, uint64(future))
}

func (e *AsyncEngine) Delete(ctx context.Context, tree Tree, key []byte) (Tree, error) {
	result, err := e.callTreeBytes(ctx, tree, key, func(handle C.uint64_t, tree, key C.RustBuffer) C.uint64_t {
		return C.uniffi_prolly_bindings_fn_method_asyncprollyengine_delete(handle, tree, key)
	})
	if err != nil {
		return Tree{}, err
	}
	return decodeTree(result)
}

func (e *AsyncEngine) Batch(ctx context.Context, tree Tree, mutations []Mutation) (Tree, error) {
	encoded, err := encodeMutations(mutations)
	if err != nil {
		return Tree{}, err
	}
	handle, buffers, err := e.lowerCall(tree.raw, encoded)
	if err != nil {
		return Tree{}, err
	}
	future := C.uniffi_prolly_bindings_fn_method_asyncprollyengine_batch(C.uint64_t(handle), buffers[0], buffers[1])
	return awaitTree(ctx, uint64(future))
}

func (e *AsyncEngine) Range(ctx context.Context, tree Tree, start, end []byte) ([]Entry, error) {
	handle, buffers, err := e.lowerCall(tree.raw, encodeByteArray(start), encodeOptionalByteArray(end))
	if err != nil {
		return nil, err
	}
	future := C.uniffi_prolly_bindings_fn_method_asyncprollyengine_range(C.uint64_t(handle), buffers[0], buffers[1], buffers[2])
	result, err := awaitRustFutureBuffer(contextOrBackground(ctx), uint64(future))
	if err != nil {
		return nil, err
	}
	return decodeEntries(result)
}

func (e *AsyncEngine) Prefix(ctx context.Context, tree Tree, prefix []byte) ([]Entry, error) {
	result, err := e.callTreeBytes(ctx, tree, prefix, func(handle C.uint64_t, tree, prefix C.RustBuffer) C.uint64_t {
		return C.uniffi_prolly_bindings_fn_method_asyncprollyengine_prefix(handle, tree, prefix)
	})
	if err != nil {
		return nil, err
	}
	return decodeEntries(result)
}

func (e *AsyncEngine) RangePage(ctx context.Context, tree Tree, cursor *RangeCursor, end []byte, limit uint64) (RangePage, error) {
	handle, buffers, err := e.lowerCall(tree.raw, encodeOptionalRangeCursor(cursor), encodeOptionalByteArray(end))
	if err != nil {
		return RangePage{}, err
	}
	future := C.uniffi_prolly_bindings_fn_method_asyncprollyengine_range_page(C.uint64_t(handle), buffers[0], buffers[1], buffers[2], C.uint64_t(limit))
	result, err := awaitRustFutureBuffer(contextOrBackground(ctx), uint64(future))
	if err != nil {
		return RangePage{}, err
	}
	return decodeRangePage(result)
}

func (e *AsyncEngine) Diff(ctx context.Context, base, other Tree) ([]Diff, error) {
	handle, buffers, err := e.lowerCall(base.raw, other.raw)
	if err != nil {
		return nil, err
	}
	future := C.uniffi_prolly_bindings_fn_method_asyncprollyengine_diff(C.uint64_t(handle), buffers[0], buffers[1])
	result, err := awaitRustFutureBuffer(contextOrBackground(ctx), uint64(future))
	if err != nil {
		return nil, err
	}
	return decodeDiffs(result)
}

func (e *AsyncEngine) Merge(ctx context.Context, base, left, right Tree, resolver string) (Tree, error) {
	handle, buffers, err := e.lowerCall(base.raw, left.raw, right.raw, encodeOptionalStringValue(resolver))
	if err != nil {
		return Tree{}, err
	}
	future := C.uniffi_prolly_bindings_fn_method_asyncprollyengine_merge(C.uint64_t(handle), buffers[0], buffers[1], buffers[2], buffers[3])
	return awaitTree(ctx, uint64(future))
}

func (e *AsyncEngine) CollectStats(ctx context.Context, tree Tree) (TreeStats, error) {
	handle, buffers, err := e.lowerCall(tree.raw)
	if err != nil {
		return TreeStats{}, err
	}
	future := C.uniffi_prolly_bindings_fn_method_asyncprollyengine_collect_stats(C.uint64_t(handle), buffers[0])
	result, err := awaitRustFutureBuffer(contextOrBackground(ctx), uint64(future))
	if err != nil {
		return TreeStats{}, err
	}
	return decodeAsyncTreeStats(result)
}

func (e *AsyncEngine) PublishNamedRoot(ctx context.Context, name []byte, tree Tree) error {
	handle, buffers, err := e.lowerCall(encodeByteArray(name), tree.raw)
	if err != nil {
		return err
	}
	future := C.uniffi_prolly_bindings_fn_method_asyncprollyengine_publish_named_root(C.uint64_t(handle), buffers[0], buffers[1])
	return awaitRustFutureVoid(contextOrBackground(ctx), uint64(future))
}

func (e *AsyncEngine) PublishNamedRootAtMillis(ctx context.Context, name []byte, tree Tree, timestampMillis uint64) error {
	handle, buffers, err := e.lowerCall(encodeByteArray(name), tree.raw)
	if err != nil {
		return err
	}
	future := C.uniffi_prolly_bindings_fn_method_asyncprollyengine_publish_named_root_at_millis(C.uint64_t(handle), buffers[0], buffers[1], C.uint64_t(timestampMillis))
	return awaitRustFutureVoid(contextOrBackground(ctx), uint64(future))
}

func (e *AsyncEngine) LoadNamedRoot(ctx context.Context, name []byte) (*Tree, error) {
	handle, buffers, err := e.lowerCall(encodeByteArray(name))
	if err != nil {
		return nil, err
	}
	future := C.uniffi_prolly_bindings_fn_method_asyncprollyengine_load_named_root(C.uint64_t(handle), buffers[0])
	result, err := awaitRustFutureBuffer(contextOrBackground(ctx), uint64(future))
	if err != nil {
		return nil, err
	}
	tree, found, err := decodeOptionalTree(result)
	if err != nil || !found {
		return nil, err
	}
	return &tree, nil
}

func (e *AsyncEngine) ListNamedRoots(ctx context.Context) ([]NamedRoot, error) {
	handle, err := e.cloneHandle()
	if err != nil {
		return nil, err
	}
	future := C.uniffi_prolly_bindings_fn_method_asyncprollyengine_list_named_roots(C.uint64_t(handle))
	result, err := awaitRustFutureBuffer(contextOrBackground(ctx), uint64(future))
	if err != nil {
		return nil, err
	}
	return decodeNamedRoots(result)
}

func (e *AsyncEngine) CompareAndSwapNamedRoot(ctx context.Context, name []byte, expected, replacement *Tree) (NamedRootUpdate, error) {
	handle, buffers, err := e.lowerCall(encodeByteArray(name), encodeOptionalTree(expected), encodeOptionalTree(replacement))
	if err != nil {
		return NamedRootUpdate{}, err
	}
	future := C.uniffi_prolly_bindings_fn_method_asyncprollyengine_compare_and_swap_named_root(C.uint64_t(handle), buffers[0], buffers[1], buffers[2])
	result, err := awaitRustFutureBuffer(contextOrBackground(ctx), uint64(future))
	if err != nil {
		return NamedRootUpdate{}, err
	}
	return decodeNamedRootUpdate(result)
}

func (e *AsyncEngine) DeleteNamedRoot(ctx context.Context, name []byte) error {
	handle, buffers, err := e.lowerCall(encodeByteArray(name))
	if err != nil {
		return err
	}
	future := C.uniffi_prolly_bindings_fn_method_asyncprollyengine_delete_named_root(C.uint64_t(handle), buffers[0])
	return awaitRustFutureVoid(contextOrBackground(ctx), uint64(future))
}

func (e *AsyncEngine) BeginTransaction(ctx context.Context) (*AsyncTransaction, error) {
	handle, err := e.cloneHandle()
	if err != nil {
		return nil, err
	}
	future := C.uniffi_prolly_bindings_fn_method_asyncprollyengine_begin_transaction(C.uint64_t(handle))
	transactionHandle, err := awaitRustFutureU64(contextOrBackground(ctx), uint64(future))
	if err != nil {
		return nil, err
	}
	transaction := &AsyncTransaction{handle: transactionHandle}
	runtime.SetFinalizer(transaction, func(value *AsyncTransaction) { _ = value.Close() })
	return transaction, nil
}

func (e *AsyncEngine) lowerCall(values ...[]byte) (uint64, []C.RustBuffer, error) {
	handle, err := e.cloneHandle()
	if err != nil {
		return 0, nil, err
	}
	buffers := make([]C.RustBuffer, 0, len(values))
	for _, value := range values {
		buffer, err := rustBufferFromBytes(value)
		if err != nil {
			for _, existing := range buffers {
				freeRustBuffer(existing)
			}
			var status C.RustCallStatus
			C.uniffi_prolly_bindings_fn_free_asyncprollyengine(C.uint64_t(handle), &status)
			return 0, nil, err
		}
		buffers = append(buffers, buffer)
	}
	return handle, buffers, nil
}

func (e *AsyncEngine) callTreeBytes(ctx context.Context, tree Tree, value []byte, call func(C.uint64_t, C.RustBuffer, C.RustBuffer) C.uint64_t) ([]byte, error) {
	handle, buffers, err := e.lowerCall(tree.raw, encodeByteArray(value))
	if err != nil {
		return nil, err
	}
	return awaitRustFutureBuffer(contextOrBackground(ctx), uint64(call(C.uint64_t(handle), buffers[0], buffers[1])))
}

func awaitTree(ctx context.Context, future uint64) (Tree, error) {
	result, err := awaitRustFutureBuffer(contextOrBackground(ctx), future)
	if err != nil {
		return Tree{}, err
	}
	return decodeTree(result)
}

func contextOrBackground(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}

func encodeOptionalStringValue(value string) []byte {
	var result []byte
	if value == "" {
		result = []byte{0}
	} else {
		result = append(result, 1)
		result = append(result, encodeByteArray([]byte(value))...)
	}
	return result
}

func decodeAsyncTreeStats(data []byte) (TreeStats, error) {
	decoder := byteDecoder{data: data}
	var result TreeStats
	var err error
	if result.NumNodes, err = decoder.readUint64(); err != nil {
		return TreeStats{}, err
	}
	if result.NumLeaves, err = decoder.readUint64(); err != nil {
		return TreeStats{}, err
	}
	if result.NumInternalNodes, err = decoder.readUint64(); err != nil {
		return TreeStats{}, err
	}
	if result.TreeHeight, err = decoder.readByte(); err != nil {
		return TreeStats{}, err
	}
	if result.TotalKeyValuePairs, err = decoder.readUint64(); err != nil {
		return TreeStats{}, err
	}
	if result.TotalTreeSizeBytes, err = decoder.readUint64(); err != nil {
		return TreeStats{}, err
	}
	if result.AvgNodeSizeBytes, err = readAsyncFloat64(&decoder); err != nil {
		return TreeStats{}, err
	}
	if result.MinNodeSizeBytes, err = decoder.readUint64(); err != nil {
		return TreeStats{}, err
	}
	if result.MaxNodeSizeBytes, err = decoder.readUint64(); err != nil {
		return TreeStats{}, err
	}
	if result.AvgEntriesPerNode, err = readAsyncFloat64(&decoder); err != nil {
		return TreeStats{}, err
	}
	if result.NodesPerLevel, err = readAsyncLevelU64(&decoder); err != nil {
		return TreeStats{}, err
	}
	if result.AvgNodeSizePerLevel, err = readAsyncLevelF64(&decoder); err != nil {
		return TreeStats{}, err
	}
	if result.AvgEntriesPerLevel, err = readAsyncLevelF64(&decoder); err != nil {
		return TreeStats{}, err
	}
	if result.MinEntriesPerLevel, err = readAsyncLevelU64(&decoder); err != nil {
		return TreeStats{}, err
	}
	if result.MaxEntriesPerLevel, err = readAsyncLevelU64(&decoder); err != nil {
		return TreeStats{}, err
	}
	floatTargets := []*float64{
		&result.AvgFanout,
		&result.AvgFillFactor,
		&result.AvgLeafFillFactor,
		&result.AvgInternalFillFactor,
		&result.AvgKeySizeBytes,
		&result.AvgValueSizeBytes,
	}
	if result.AvgFanout, err = readAsyncFloat64(&decoder); err != nil {
		return TreeStats{}, err
	}
	if result.MinFanout, err = decoder.readUint64(); err != nil {
		return TreeStats{}, err
	}
	if result.MaxFanout, err = decoder.readUint64(); err != nil {
		return TreeStats{}, err
	}
	for _, target := range floatTargets[1:] {
		if *target, err = readAsyncFloat64(&decoder); err != nil {
			return TreeStats{}, err
		}
	}
	uintTargets := []*uint64{
		&result.MinKeySizeBytes,
		&result.MaxKeySizeBytes,
		&result.MinValueSizeBytes,
		&result.MaxValueSizeBytes,
		&result.TotalKeysSizeBytes,
		&result.TotalValuesSizeBytes,
	}
	for _, target := range uintTargets {
		if *target, err = decoder.readUint64(); err != nil {
			return TreeStats{}, err
		}
	}
	return result, decoder.done()
}

func readAsyncFloat64(decoder *byteDecoder) (float64, error) {
	bits, err := decoder.readUint64()
	if err != nil {
		return 0, err
	}
	return math.Float64frombits(bits), nil
}

func readAsyncLevelU64(decoder *byteDecoder) (map[string]uint64, error) {
	count, err := decoder.readInt32()
	if err != nil || count < 0 {
		return nil, errors.New("invalid tree stats level count")
	}
	result := make(map[string]uint64, count)
	for range count {
		level, err := decoder.readByte()
		if err != nil {
			return nil, err
		}
		value, err := decoder.readUint64()
		if err != nil {
			return nil, err
		}
		result[strconv.Itoa(int(level))] = value
	}
	return result, nil
}

func readAsyncLevelF64(decoder *byteDecoder) (map[string]float64, error) {
	count, err := decoder.readInt32()
	if err != nil || count < 0 {
		return nil, errors.New("invalid tree stats level count")
	}
	result := make(map[string]float64, count)
	for range count {
		level, err := decoder.readByte()
		if err != nil {
			return nil, err
		}
		value, err := readAsyncFloat64(decoder)
		if err != nil {
			return nil, err
		}
		result[strconv.Itoa(int(level))] = value
	}
	return result, nil
}

func (t *AsyncTransaction) cloneHandle() (uint64, error) {
	if t == nil || t.closed.Load() || t.completed.Load() {
		return 0, ErrAsyncEngineClosed
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	if t.handle == 0 || t.closed.Load() || t.completed.Load() {
		return 0, ErrAsyncEngineClosed
	}
	var status C.RustCallStatus
	handle := C.uniffi_prolly_bindings_fn_clone_asyncprollytransaction(C.uint64_t(t.handle), &status)
	if err := statusError(&status); err != nil {
		return 0, err
	}
	return uint64(handle), nil
}

func (t *AsyncTransaction) Close() error {
	if t == nil || t.closed.Swap(true) {
		return nil
	}
	runtime.SetFinalizer(t, nil)
	t.mu.Lock()
	defer t.mu.Unlock()
	var status C.RustCallStatus
	if t.handle != 0 {
		C.uniffi_prolly_bindings_fn_free_asyncprollytransaction(C.uint64_t(t.handle), &status)
		t.handle = 0
	}
	return statusError(&status)
}

func (t *AsyncTransaction) Create(ctx context.Context) (Tree, error) {
	handle, err := t.cloneHandle()
	if err != nil {
		return Tree{}, err
	}
	future := C.uniffi_prolly_bindings_fn_method_asyncprollytransaction_create(C.uint64_t(handle))
	return awaitTree(ctx, uint64(future))
}

func (t *AsyncTransaction) Get(ctx context.Context, tree Tree, key []byte) ([]byte, bool, error) {
	handle, buffers, err := t.lowerCall(tree.raw, encodeByteArray(key))
	if err != nil {
		return nil, false, err
	}
	future := C.uniffi_prolly_bindings_fn_method_asyncprollytransaction_get(C.uint64_t(handle), buffers[0], buffers[1])
	result, err := awaitRustFutureBuffer(contextOrBackground(ctx), uint64(future))
	if err != nil {
		return nil, false, err
	}
	return decodeOptionalByteArray(result)
}

func (t *AsyncTransaction) Put(ctx context.Context, tree Tree, key, value []byte) (Tree, error) {
	handle, buffers, err := t.lowerCall(tree.raw, encodeByteArray(key), encodeByteArray(value))
	if err != nil {
		return Tree{}, err
	}
	future := C.uniffi_prolly_bindings_fn_method_asyncprollytransaction_put(C.uint64_t(handle), buffers[0], buffers[1], buffers[2])
	return awaitTree(ctx, uint64(future))
}

func (t *AsyncTransaction) Delete(ctx context.Context, tree Tree, key []byte) (Tree, error) {
	handle, buffers, err := t.lowerCall(tree.raw, encodeByteArray(key))
	if err != nil {
		return Tree{}, err
	}
	future := C.uniffi_prolly_bindings_fn_method_asyncprollytransaction_delete(C.uint64_t(handle), buffers[0], buffers[1])
	return awaitTree(ctx, uint64(future))
}

func (t *AsyncTransaction) Batch(ctx context.Context, tree Tree, mutations []Mutation) (Tree, error) {
	encoded, err := encodeMutations(mutations)
	if err != nil {
		return Tree{}, err
	}
	handle, buffers, err := t.lowerCall(tree.raw, encoded)
	if err != nil {
		return Tree{}, err
	}
	future := C.uniffi_prolly_bindings_fn_method_asyncprollytransaction_batch(C.uint64_t(handle), buffers[0], buffers[1])
	return awaitTree(ctx, uint64(future))
}

func (t *AsyncTransaction) LoadNamedRoot(ctx context.Context, name []byte) (*Tree, error) {
	handle, buffers, err := t.lowerCall(encodeByteArray(name))
	if err != nil {
		return nil, err
	}
	future := C.uniffi_prolly_bindings_fn_method_asyncprollytransaction_load_named_root(C.uint64_t(handle), buffers[0])
	result, err := awaitRustFutureBuffer(contextOrBackground(ctx), uint64(future))
	if err != nil {
		return nil, err
	}
	tree, found, err := decodeOptionalTree(result)
	if err != nil || !found {
		return nil, err
	}
	return &tree, nil
}

func (t *AsyncTransaction) PublishNamedRoot(ctx context.Context, name []byte, tree Tree) error {
	handle, buffers, err := t.lowerCall(encodeByteArray(name), tree.raw)
	if err != nil {
		return err
	}
	future := C.uniffi_prolly_bindings_fn_method_asyncprollytransaction_publish_named_root(C.uint64_t(handle), buffers[0], buffers[1])
	return awaitRustFutureVoid(contextOrBackground(ctx), uint64(future))
}

func (t *AsyncTransaction) PublishNamedRootAtMillis(ctx context.Context, name []byte, tree Tree, timestampMillis uint64) error {
	handle, buffers, err := t.lowerCall(encodeByteArray(name), tree.raw)
	if err != nil {
		return err
	}
	future := C.uniffi_prolly_bindings_fn_method_asyncprollytransaction_publish_named_root_at_millis(C.uint64_t(handle), buffers[0], buffers[1], C.uint64_t(timestampMillis))
	return awaitRustFutureVoid(contextOrBackground(ctx), uint64(future))
}

func (t *AsyncTransaction) CompareAndSwapNamedRoot(ctx context.Context, name []byte, expected, replacement *Tree) (NamedRootUpdate, error) {
	handle, buffers, err := t.lowerCall(encodeByteArray(name), encodeOptionalTree(expected), encodeOptionalTree(replacement))
	if err != nil {
		return NamedRootUpdate{}, err
	}
	future := C.uniffi_prolly_bindings_fn_method_asyncprollytransaction_compare_and_swap_named_root(C.uint64_t(handle), buffers[0], buffers[1], buffers[2])
	result, err := awaitRustFutureBuffer(contextOrBackground(ctx), uint64(future))
	if err != nil {
		return NamedRootUpdate{}, err
	}
	return decodeNamedRootUpdate(result)
}

func (t *AsyncTransaction) DeleteNamedRoot(ctx context.Context, name []byte) error {
	handle, buffers, err := t.lowerCall(encodeByteArray(name))
	if err != nil {
		return err
	}
	future := C.uniffi_prolly_bindings_fn_method_asyncprollytransaction_delete_named_root(C.uint64_t(handle), buffers[0])
	return awaitRustFutureVoid(contextOrBackground(ctx), uint64(future))
}

func (t *AsyncTransaction) Commit(ctx context.Context) (TransactionUpdate, error) {
	handle, err := t.cloneHandle()
	if err != nil {
		return TransactionUpdate{}, err
	}
	future := C.uniffi_prolly_bindings_fn_method_asyncprollytransaction_commit(C.uint64_t(handle))
	result, err := awaitRustFutureBuffer(contextOrBackground(ctx), uint64(future))
	if err != nil {
		return TransactionUpdate{}, err
	}
	update, err := decodeTransactionUpdate(result)
	if err == nil {
		t.completed.Store(true)
	}
	return update, err
}

func (t *AsyncTransaction) Rollback(ctx context.Context) error {
	handle, err := t.cloneHandle()
	if err != nil {
		return err
	}
	future := C.uniffi_prolly_bindings_fn_method_asyncprollytransaction_rollback(C.uint64_t(handle))
	err = awaitRustFutureVoid(contextOrBackground(ctx), uint64(future))
	if err == nil {
		t.completed.Store(true)
	}
	return err
}

func (t *AsyncTransaction) lowerCall(values ...[]byte) (uint64, []C.RustBuffer, error) {
	handle, err := t.cloneHandle()
	if err != nil {
		return 0, nil, err
	}
	buffers := make([]C.RustBuffer, 0, len(values))
	for _, value := range values {
		buffer, err := rustBufferFromBytes(value)
		if err != nil {
			for _, existing := range buffers {
				freeRustBuffer(existing)
			}
			var status C.RustCallStatus
			C.uniffi_prolly_bindings_fn_free_asyncprollytransaction(C.uint64_t(handle), &status)
			return 0, nil, err
		}
		buffers = append(buffers, buffer)
	}
	return handle, buffers, nil
}
