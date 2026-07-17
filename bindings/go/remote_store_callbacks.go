package prolly

/*
#include <stdint.h>
#include <stdlib.h>

#ifndef PROLLY_GO_CALLBACK_TYPES
#define PROLLY_GO_CALLBACK_TYPES
typedef struct RustBuffer {
	uint64_t capacity;
	uint64_t len;
	uint8_t *data;
} RustBuffer;

typedef struct RustCallStatus {
	int8_t code;
	RustBuffer error_buf;
} RustCallStatus;
#endif

extern void ffi_prolly_bindings_rustbuffer_free(RustBuffer, RustCallStatus *);
*/
import "C"

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"unsafe"
)

type remoteStoreHandleEntry struct {
	store RemoteStore
}

var (
	remoteStoreNext atomic.Uint64
	remoteStoreMu   sync.RWMutex
	remoteStores    = map[uint64]remoteStoreHandleEntry{}

	remoteCallNext atomic.Uint64
	remoteCallMu   sync.Mutex
	remoteCalls    = map[uint64]*remoteCall{}
)

type remoteCall struct {
	cancel    context.CancelFunc
	cancelled atomic.Bool
}

func registerRemoteStore(store RemoteStore) uint64 {
	handle := remoteStoreNext.Add(2) - 1
	remoteStoreMu.Lock()
	remoteStores[handle] = remoteStoreHandleEntry{store: store}
	remoteStoreMu.Unlock()
	return handle
}

func cloneRemoteStoreHandle(handle uint64) uint64 {
	remoteStoreMu.RLock()
	entry, ok := remoteStores[handle]
	remoteStoreMu.RUnlock()
	if !ok {
		return 0
	}
	return registerRemoteStore(entry.store)
}

func removeRemoteStoreHandle(handle uint64) {
	remoteStoreMu.Lock()
	delete(remoteStores, handle)
	remoteStoreMu.Unlock()
}

func getRemoteStore(handle uint64) RemoteStore {
	remoteStoreMu.RLock()
	entry := remoteStores[handle]
	remoteStoreMu.RUnlock()
	return entry.store
}

//export prolly_go_remote_store_free
func prolly_go_remote_store_free(handle C.uint64_t) {
	removeRemoteStoreHandle(uint64(handle))
}

//export prolly_go_remote_store_clone
func prolly_go_remote_store_clone(handle C.uint64_t) C.uint64_t {
	return C.uint64_t(cloneRemoteStoreHandle(uint64(handle)))
}

//export prolly_go_remote_future_dropped
func prolly_go_remote_future_dropped(handle C.uint64_t) {
	remoteCallMu.Lock()
	call := remoteCalls[uint64(handle)]
	delete(remoteCalls, uint64(handle))
	remoteCallMu.Unlock()
	if call != nil {
		call.cancelled.Store(true)
		call.cancel()
	}
}

//export prolly_go_rust_future_continuation
func prolly_go_rust_future_continuation(handle C.uint64_t, pollCode C.int8_t) {
	rustFuturePollMu.Lock()
	result := rustFuturePolls[uint64(handle)]
	delete(rustFuturePolls, uint64(handle))
	rustFuturePollMu.Unlock()
	if result != nil {
		result <- int8(pollCode)
	}
}

//export prolly_go_remote_dispatch0
func prolly_go_remote_dispatch0(method C.uint32_t, store C.uint64_t, callback C.uintptr_t, callbackData C.uint64_t) C.uint64_t {
	return C.uint64_t(startRemoteCall(uint32(method), uint64(store), uint64(callback), uint64(callbackData), nil))
}

//export prolly_go_remote_dispatch1
func prolly_go_remote_dispatch1(method C.uint32_t, store C.uint64_t, a C.RustBuffer, callback C.uintptr_t, callbackData C.uint64_t) C.uint64_t {
	return C.uint64_t(startRemoteCall(uint32(method), uint64(store), uint64(callback), uint64(callbackData), [][]byte{takeRemoteBuffer(a)}))
}

//export prolly_go_remote_dispatch2
func prolly_go_remote_dispatch2(method C.uint32_t, store C.uint64_t, a, b C.RustBuffer, callback C.uintptr_t, callbackData C.uint64_t) C.uint64_t {
	return C.uint64_t(startRemoteCall(uint32(method), uint64(store), uint64(callback), uint64(callbackData), [][]byte{takeRemoteBuffer(a), takeRemoteBuffer(b)}))
}

//export prolly_go_remote_dispatch3
func prolly_go_remote_dispatch3(method C.uint32_t, store C.uint64_t, a, b, c C.RustBuffer, callback C.uintptr_t, callbackData C.uint64_t) C.uint64_t {
	return C.uint64_t(startRemoteCall(uint32(method), uint64(store), uint64(callback), uint64(callbackData), [][]byte{takeRemoteBuffer(a), takeRemoteBuffer(b), takeRemoteBuffer(c)}))
}

//export prolly_go_remote_dispatch4
func prolly_go_remote_dispatch4(method C.uint32_t, store C.uint64_t, a, b, c, d C.RustBuffer, callback C.uintptr_t, callbackData C.uint64_t) C.uint64_t {
	return C.uint64_t(startRemoteCall(uint32(method), uint64(store), uint64(callback), uint64(callbackData), [][]byte{takeRemoteBuffer(a), takeRemoteBuffer(b), takeRemoteBuffer(c), takeRemoteBuffer(d)}))
}

func takeRemoteBuffer(buffer C.RustBuffer) []byte {
	var value []byte
	if buffer.data != nil && buffer.len != 0 {
		value = C.GoBytes(unsafe.Pointer(buffer.data), C.int(buffer.len))
	}
	var status C.RustCallStatus
	C.ffi_prolly_bindings_rustbuffer_free(buffer, &status)
	return value
}

func startRemoteCall(method uint32, storeHandle, callback, callbackData uint64, args [][]byte) uint64 {
	ctx, cancel := context.WithCancel(context.Background())
	call := &remoteCall{cancel: cancel}
	handle := remoteCallNext.Add(1)
	remoteCallMu.Lock()
	remoteCalls[handle] = call
	remoteCallMu.Unlock()

	go func() {
		var payload []byte
		var unexpected error
		func() {
			defer func() {
				if recovered := recover(); recovered != nil {
					payload = encodeRemoteErrorResult(method, &StoreError{Code: "panic", Message: fmt.Sprint(recovered)})
				}
			}()
			payload, unexpected = invokeRemoteStore(ctx, getRemoteStore(storeHandle), method, args)
		}()
		cancel()

		remoteCallMu.Lock()
		current := remoteCalls[handle]
		delete(remoteCalls, handle)
		remoteCallMu.Unlock()
		if current == nil || current.cancelled.Load() {
			return
		}
		completeRemoteFuture(callback, callbackData, payload, unexpected)
	}()
	return handle
}

type remoteOutcome struct {
	descriptor  StoreDescriptor
	optional    OptionalBytes
	optionals   []OptionalBytes
	bytesList   [][]byte
	roots       []NamedStoreRoot
	cas         RootCASResult
	transaction StoreTransactionResult
	err         error
}

func invokeRemoteStore(ctx context.Context, store RemoteStore, method uint32, args [][]byte) ([]byte, error) {
	if store == nil {
		return encodeRemoteErrorResult(method, &StoreError{Code: "invalid_handle", Message: "remote store handle is not registered"}), nil
	}
	var outcome remoteOutcome
	var decodeErr error
	switch method {
	case 0:
		outcome.descriptor, outcome.err = store.Descriptor(ctx)
	case 1:
		var key []byte
		key, decodeErr = decodeRequiredByteArray(args[0])
		if decodeErr == nil {
			outcome.optional, outcome.err = store.GetNode(ctx, key)
		}
	case 2:
		var key, value []byte
		key, decodeErr = decodeRequiredByteArray(args[0])
		if decodeErr == nil {
			value, decodeErr = decodeRequiredByteArray(args[1])
		}
		if decodeErr == nil {
			outcome.err = store.PutNode(ctx, key, value)
		}
	case 3:
		var key []byte
		key, decodeErr = decodeRequiredByteArray(args[0])
		if decodeErr == nil {
			outcome.err = store.DeleteNode(ctx, key)
		}
	case 4:
		var mutations []NodeMutation
		mutations, decodeErr = decodeNodeMutations(args[0])
		if decodeErr == nil {
			outcome.err = store.BatchNodes(ctx, mutations)
		}
	case 5:
		var keys [][]byte
		keys, decodeErr = decodeRemoteByteSequence(args[0])
		if decodeErr == nil {
			outcome.optionals, outcome.err = store.BatchGetNodesOrdered(ctx, keys)
		}
	case 6:
		outcome.bytesList, outcome.err = store.ListNodeCIDs(ctx)
	case 7:
		var namespace, key []byte
		namespace, decodeErr = decodeRequiredByteArray(args[0])
		if decodeErr == nil {
			key, decodeErr = decodeRequiredByteArray(args[1])
		}
		if decodeErr == nil {
			outcome.optional, outcome.err = store.GetHint(ctx, namespace, key)
		}
	case 8:
		var namespace, key, value []byte
		namespace, decodeErr = decodeRequiredByteArray(args[0])
		if decodeErr == nil {
			key, decodeErr = decodeRequiredByteArray(args[1])
		}
		if decodeErr == nil {
			value, decodeErr = decodeRequiredByteArray(args[2])
		}
		if decodeErr == nil {
			outcome.err = store.PutHint(ctx, namespace, key, value)
		}
	case 9:
		var nodes []NodeEntry
		var namespace, key, value []byte
		nodes, decodeErr = decodeNodeEntries(args[0])
		if decodeErr == nil {
			namespace, decodeErr = decodeRequiredByteArray(args[1])
		}
		if decodeErr == nil {
			key, decodeErr = decodeRequiredByteArray(args[2])
		}
		if decodeErr == nil {
			value, decodeErr = decodeRequiredByteArray(args[3])
		}
		if decodeErr == nil {
			outcome.err = store.BatchPutNodesWithHint(ctx, nodes, namespace, key, value)
		}
	case 10:
		var name []byte
		name, decodeErr = decodeRequiredByteArray(args[0])
		if decodeErr == nil {
			outcome.optional, outcome.err = store.GetRootManifest(ctx, name)
		}
	case 11:
		var name, manifest []byte
		name, decodeErr = decodeRequiredByteArray(args[0])
		if decodeErr == nil {
			manifest, decodeErr = decodeRequiredByteArray(args[1])
		}
		if decodeErr == nil {
			outcome.err = store.PutRootManifest(ctx, name, manifest)
		}
	case 12:
		var name []byte
		name, decodeErr = decodeRequiredByteArray(args[0])
		if decodeErr == nil {
			outcome.err = store.DeleteRootManifest(ctx, name)
		}
	case 13:
		var name []byte
		var expected, replacement OptionalBytes
		name, decodeErr = decodeRequiredByteArray(args[0])
		if decodeErr == nil {
			expected, decodeErr = decodeRemoteOptionalBytes(args[1])
		}
		if decodeErr == nil {
			replacement, decodeErr = decodeRemoteOptionalBytes(args[2])
		}
		if decodeErr == nil {
			outcome.cas, outcome.err = store.CompareAndSwapRootManifest(ctx, name, expected, replacement)
		}
	case 14:
		outcome.roots, outcome.err = store.ListRootManifests(ctx)
	case 15:
		var nodes []NodeMutation
		var conditions []RootCondition
		var roots []RootWrite
		nodes, decodeErr = decodeNodeMutations(args[0])
		if decodeErr == nil {
			conditions, decodeErr = decodeRootConditions(args[1])
		}
		if decodeErr == nil {
			roots, decodeErr = decodeRootWrites(args[2])
		}
		if decodeErr == nil {
			outcome.transaction, outcome.err = store.CommitTransaction(ctx, nodes, conditions, roots)
		}
	default:
		return nil, fmt.Errorf("unknown remote store callback method %d", method)
	}
	if decodeErr != nil {
		outcome.err = &StoreError{Code: "invalid_argument", Message: decodeErr.Error(), Cause: decodeErr}
	}
	return encodeRemoteOutcome(method, outcome), nil
}

func decodeRemoteOptionalBytes(data []byte) (OptionalBytes, error) {
	decoder := byteDecoder{data: data}
	present, err := decoder.readBool()
	if err != nil {
		return OptionalBytes{}, err
	}
	value, err := decoder.readByteArray()
	if err != nil {
		return OptionalBytes{}, err
	}
	return OptionalBytes{Present: present, Value: value}, decoder.done()
}

func decodeRemoteByteSequence(data []byte) ([][]byte, error) {
	decoder := byteDecoder{data: data}
	values, err := decoder.readByteArraySequence()
	if err != nil {
		return nil, err
	}
	return values, decoder.done()
}

func decodeNodeMutations(data []byte) ([]NodeMutation, error) {
	decoder := byteDecoder{data: data}
	count, err := decoder.readInt32()
	if err != nil || count < 0 {
		return nil, fmt.Errorf("invalid node mutation count %d: %w", count, err)
	}
	result := make([]NodeMutation, 0, count)
	for range count {
		key, err := decoder.readByteArray()
		if err != nil {
			return nil, err
		}
		present, err := decoder.readBool()
		if err != nil {
			return nil, err
		}
		value, err := decoder.readByteArray()
		if err != nil {
			return nil, err
		}
		result = append(result, NodeMutation{Key: key, Value: OptionalBytes{Present: present, Value: value}})
	}
	return result, decoder.done()
}

func decodeNodeEntries(data []byte) ([]NodeEntry, error) {
	decoder := byteDecoder{data: data}
	count, err := decoder.readInt32()
	if err != nil || count < 0 {
		return nil, fmt.Errorf("invalid node entry count %d: %w", count, err)
	}
	result := make([]NodeEntry, 0, count)
	for range count {
		key, err := decoder.readByteArray()
		if err != nil {
			return nil, err
		}
		value, err := decoder.readByteArray()
		if err != nil {
			return nil, err
		}
		result = append(result, NodeEntry{Key: key, Value: value})
	}
	return result, decoder.done()
}

func decodeRootConditions(data []byte) ([]RootCondition, error) {
	decoder := byteDecoder{data: data}
	count, err := decoder.readInt32()
	if err != nil || count < 0 {
		return nil, fmt.Errorf("invalid root condition count %d: %w", count, err)
	}
	result := make([]RootCondition, 0, count)
	for range count {
		name, err := decoder.readByteArray()
		if err != nil {
			return nil, err
		}
		present, err := decoder.readBool()
		if err != nil {
			return nil, err
		}
		value, err := decoder.readByteArray()
		if err != nil {
			return nil, err
		}
		result = append(result, RootCondition{Name: name, Expected: OptionalBytes{Present: present, Value: value}})
	}
	return result, decoder.done()
}

func decodeRootWrites(data []byte) ([]RootWrite, error) {
	decoder := byteDecoder{data: data}
	count, err := decoder.readInt32()
	if err != nil || count < 0 {
		return nil, fmt.Errorf("invalid root write count %d: %w", count, err)
	}
	result := make([]RootWrite, 0, count)
	for range count {
		name, err := decoder.readByteArray()
		if err != nil {
			return nil, err
		}
		present, err := decoder.readBool()
		if err != nil {
			return nil, err
		}
		value, err := decoder.readByteArray()
		if err != nil {
			return nil, err
		}
		result = append(result, RootWrite{Name: name, Replacement: OptionalBytes{Present: present, Value: value}})
	}
	return result, decoder.done()
}

func encodeRemoteErrorResult(method uint32, err error) []byte {
	return encodeRemoteOutcome(method, remoteOutcome{err: err})
}

func encodeRemoteOutcome(method uint32, result remoteOutcome) []byte {
	var out bytes.Buffer
	switch method {
	case 0:
		if result.err == nil {
			out.WriteByte(1)
			encodeStoreDescriptor(&out, result.descriptor)
		} else {
			out.WriteByte(0)
		}
	case 1, 7, 10:
		encodeRemoteOptionalBytesInto(&out, result.optional)
	case 5:
		writeI32(&out, int32(len(result.optionals)))
		for _, value := range result.optionals {
			encodeRemoteOptionalBytesInto(&out, value)
		}
	case 6:
		writeI32(&out, int32(len(result.bytesList)))
		for _, value := range result.bytesList {
			encodeByteArrayInto(&out, value)
		}
	case 13:
		encodeBoolInto(&out, result.cas.Applied)
		encodeRemoteOptionalBytesInto(&out, result.cas.Current)
	case 14:
		writeI32(&out, int32(len(result.roots)))
		for _, root := range result.roots {
			encodeByteArrayInto(&out, root.Name)
			encodeByteArrayInto(&out, root.Manifest)
		}
	case 15:
		encodeBoolInto(&out, result.transaction.Applied)
		if result.transaction.Conflict == nil {
			out.WriteByte(0)
		} else {
			out.WriteByte(1)
			encodeByteArrayInto(&out, result.transaction.Conflict.Name)
			encodeRemoteOptionalBytesInto(&out, result.transaction.Conflict.Expected)
			encodeRemoteOptionalBytesInto(&out, result.transaction.Conflict.Current)
		}
	}
	encodeOptionalStoreError(&out, result.err)
	return out.Bytes()
}

func encodeStoreDescriptor(out *bytes.Buffer, descriptor StoreDescriptor) {
	writeU32(out, descriptor.ProtocolMajor)
	encodeStringInto(out, descriptor.AdapterName)
	encodeStringInto(out, descriptor.Provider)
	writeU32(out, descriptor.SchemaVersion)
	capabilities := descriptor.Capabilities
	encodeBoolInto(out, capabilities.NativeBatchReads)
	encodeBoolInto(out, capabilities.AtomicBatchWrites)
	encodeBoolInto(out, capabilities.NodeScan)
	encodeBoolInto(out, capabilities.Hints)
	encodeBoolInto(out, capabilities.AtomicNodesAndHint)
	encodeBoolInto(out, capabilities.RootScan)
	encodeBoolInto(out, capabilities.RootCompareAndSwap)
	encodeBoolInto(out, capabilities.Transactions)
	writeU32(out, capabilities.ReadParallelism)
	encodeOptionalU32(out, descriptor.Limits.MaxBatchReadItems)
	encodeOptionalU32(out, descriptor.Limits.MaxBatchWriteItems)
	encodeOptionalU32(out, descriptor.Limits.MaxTransactionOperations)
	encodeOptionalU64(out, descriptor.Limits.MaxNodeBytes)
}

func encodeRemoteOptionalBytesInto(out *bytes.Buffer, value OptionalBytes) {
	encodeBoolInto(out, value.Present)
	encodeByteArrayInto(out, value.Value)
}

func encodeOptionalStoreError(out *bytes.Buffer, err error) {
	if err == nil {
		out.WriteByte(0)
		return
	}
	out.WriteByte(1)
	storeErr := normalizeStoreError(err)
	encodeStringInto(out, storeErr.Code)
	encodeStringInto(out, storeErr.Message)
	encodeBoolInto(out, storeErr.Retryable)
	if storeErr.ProviderCode == "" {
		out.WriteByte(0)
	} else {
		out.WriteByte(1)
		encodeStringInto(out, storeErr.ProviderCode)
	}
}

func normalizeStoreError(err error) *StoreError {
	var storeErr *StoreError
	if errors.As(err, &storeErr) {
		return storeErr
	}
	code := "provider"
	if errors.Is(err, context.Canceled) {
		code = "cancelled"
	} else if errors.Is(err, context.DeadlineExceeded) {
		code = "deadline_exceeded"
	}
	return &StoreError{Code: code, Message: err.Error(), Cause: err}
}

func encodeBoolInto(out *bytes.Buffer, value bool) {
	if value {
		out.WriteByte(1)
	} else {
		out.WriteByte(0)
	}
}

func encodeOptionalU32(out *bytes.Buffer, value *uint32) {
	if value == nil {
		out.WriteByte(0)
		return
	}
	out.WriteByte(1)
	writeU32(out, *value)
}
