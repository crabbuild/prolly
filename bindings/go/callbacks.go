package prolly

/*
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

typedef struct RustBuffer {
	uint64_t capacity;
	uint64_t len;
	uint8_t *data;
} RustBuffer;

typedef struct RustCallStatus {
	int8_t code;
	RustBuffer error_buf;
} RustCallStatus;

extern RustBuffer ffi_prolly_bindings_rustbuffer_alloc(uint64_t size, RustCallStatus *out_err);
extern void ffi_prolly_bindings_rustbuffer_free(RustBuffer buf, RustCallStatus *out_err);
*/
import "C"

import (
	"errors"
	"sync"
	"sync/atomic"
	"unsafe"
)

var errHostStoreMissing = errors.New("host store callback handle is not registered")

var (
	goResolverNext atomic.Uint64
	goResolverMu   sync.Mutex
	goResolvers    = map[uint64]Resolver{}

	goCrdtResolverNext atomic.Uint64
	goCrdtResolverMu   sync.Mutex
	goCrdtResolvers    = map[uint64]CrdtResolver{}

	goHostStoreNext atomic.Uint64
	goHostStoreMu   sync.Mutex
	goHostStores    = map[uint64]HostStore{}
)

func registerGoResolver(resolver Resolver) uint64 {
	handle := goResolverNext.Add(2) - 1
	goResolverMu.Lock()
	goResolvers[handle] = resolver
	goResolverMu.Unlock()
	return handle
}

func cloneGoResolver(handle uint64) uint64 {
	goResolverMu.Lock()
	defer goResolverMu.Unlock()
	resolver := goResolvers[handle]
	if resolver == nil {
		return 0
	}
	clone := goResolverNext.Add(2) - 1
	goResolvers[clone] = resolver
	return clone
}

func removeGoResolver(handle uint64) {
	goResolverMu.Lock()
	delete(goResolvers, handle)
	goResolverMu.Unlock()
}

func getGoResolver(handle uint64) Resolver {
	goResolverMu.Lock()
	defer goResolverMu.Unlock()
	return goResolvers[handle]
}

func registerGoCrdtResolver(resolver CrdtResolver) uint64 {
	handle := goCrdtResolverNext.Add(2) - 1
	goCrdtResolverMu.Lock()
	goCrdtResolvers[handle] = resolver
	goCrdtResolverMu.Unlock()
	return handle
}

func cloneGoCrdtResolver(handle uint64) uint64 {
	goCrdtResolverMu.Lock()
	defer goCrdtResolverMu.Unlock()
	resolver := goCrdtResolvers[handle]
	if resolver == nil {
		return 0
	}
	clone := goCrdtResolverNext.Add(2) - 1
	goCrdtResolvers[clone] = resolver
	return clone
}

func removeGoCrdtResolver(handle uint64) {
	goCrdtResolverMu.Lock()
	delete(goCrdtResolvers, handle)
	goCrdtResolverMu.Unlock()
}

func getGoCrdtResolver(handle uint64) CrdtResolver {
	goCrdtResolverMu.Lock()
	defer goCrdtResolverMu.Unlock()
	return goCrdtResolvers[handle]
}

func registerGoHostStore(store HostStore) uint64 {
	handle := goHostStoreNext.Add(2) - 1
	goHostStoreMu.Lock()
	goHostStores[handle] = store
	goHostStoreMu.Unlock()
	return handle
}

func cloneGoHostStore(handle uint64) uint64 {
	goHostStoreMu.Lock()
	defer goHostStoreMu.Unlock()
	store := goHostStores[handle]
	if store == nil {
		return 0
	}
	clone := goHostStoreNext.Add(2) - 1
	goHostStores[clone] = store
	return clone
}

func removeGoHostStore(handle uint64) {
	goHostStoreMu.Lock()
	delete(goHostStores, handle)
	goHostStoreMu.Unlock()
}

func getGoHostStore(handle uint64) HostStore {
	goHostStoreMu.Lock()
	defer goHostStoreMu.Unlock()
	return goHostStores[handle]
}

//export prolly_go_resolver_free
func prolly_go_resolver_free(handle C.uint64_t) {
	removeGoResolver(uint64(handle))
}

//export prolly_go_resolver_clone
func prolly_go_resolver_clone(handle C.uint64_t) C.uint64_t {
	return C.uint64_t(cloneGoResolver(uint64(handle)))
}

//export prolly_go_resolver_resolve
func prolly_go_resolver_resolve(handle C.uint64_t, conflict C.RustBuffer, outReturn *C.RustBuffer, outStatus *C.RustCallStatus) {
	if outStatus != nil {
		outStatus.code = 0
		outStatus.error_buf = C.RustBuffer{}
	}
	if outReturn == nil {
		return
	}

	conflictBytes := copyCallbackRustBuffer(conflict)
	freeCallbackRustBuffer(conflict)
	decoded, err := decodeConflict(conflictBytes)
	if err != nil {
		*outReturn = callbackRustBufferFromBytes(encodeCallbackUnresolved())
		return
	}

	resolver := getGoResolver(uint64(handle))
	if resolver == nil {
		*outReturn = callbackRustBufferFromBytes(encodeCallbackUnresolved())
		return
	}
	encoded, err := encodeResolution(resolver(decoded))
	if err != nil {
		encoded = encodeCallbackUnresolved()
	}
	*outReturn = callbackRustBufferFromBytes(encoded)
}

//export prolly_go_crdt_resolver_free
func prolly_go_crdt_resolver_free(handle C.uint64_t) {
	removeGoCrdtResolver(uint64(handle))
}

//export prolly_go_crdt_resolver_clone
func prolly_go_crdt_resolver_clone(handle C.uint64_t) C.uint64_t {
	return C.uint64_t(cloneGoCrdtResolver(uint64(handle)))
}

//export prolly_go_crdt_resolver_resolve
func prolly_go_crdt_resolver_resolve(handle C.uint64_t, conflict C.RustBuffer, outReturn *C.RustBuffer, outStatus *C.RustCallStatus) {
	if outStatus != nil {
		outStatus.code = 0
		outStatus.error_buf = C.RustBuffer{}
	}
	if outReturn == nil {
		return
	}

	conflictBytes := copyCallbackRustBuffer(conflict)
	freeCallbackRustBuffer(conflict)
	decoded, err := decodeConflict(conflictBytes)
	if err != nil {
		*outReturn = callbackRustBufferFromBytes(encodeCallbackCrdtDelete())
		return
	}

	resolver := getGoCrdtResolver(uint64(handle))
	if resolver == nil {
		*outReturn = callbackRustBufferFromBytes(encodeCallbackCrdtDelete())
		return
	}
	encoded, err := encodeCrdtResolution(resolver(decoded))
	if err != nil {
		encoded = encodeCallbackCrdtDelete()
	}
	*outReturn = callbackRustBufferFromBytes(encoded)
}

//export prolly_go_host_store_free
func prolly_go_host_store_free(handle C.uint64_t) {
	removeGoHostStore(uint64(handle))
}

//export prolly_go_host_store_clone
func prolly_go_host_store_clone(handle C.uint64_t) C.uint64_t {
	return C.uint64_t(cloneGoHostStore(uint64(handle)))
}

//export prolly_go_host_store_get
func prolly_go_host_store_get(handle C.uint64_t, key C.RustBuffer, outReturn *C.RustBuffer, outStatus *C.RustCallStatus) {
	resetCallbackStatus(outStatus)
	store := getGoHostStore(uint64(handle))
	keyBytes, err := callbackByteArray(key)
	result := HostStoreResult{Err: err}
	if err == nil && store == nil {
		result.Err = errMissingHostStore()
	}
	if err == nil && store != nil {
		result = store.Get(keyBytes)
	}
	writeCallbackReturn(outReturn, encodeHostStoreBytesResult(result))
}

//export prolly_go_host_store_put
func prolly_go_host_store_put(handle C.uint64_t, key C.RustBuffer, value C.RustBuffer, outReturn *C.RustBuffer, outStatus *C.RustCallStatus) {
	resetCallbackStatus(outStatus)
	store := getGoHostStore(uint64(handle))
	keyBytes, err := callbackByteArray(key)
	valueBytes, valueErr := callbackByteArray(value)
	if err == nil {
		err = valueErr
	}
	if err == nil && store == nil {
		err = errMissingHostStore()
	}
	if err == nil {
		err = store.Put(keyBytes, valueBytes)
	}
	writeCallbackReturn(outReturn, encodeHostStoreUnitResult(err))
}

//export prolly_go_host_store_delete
func prolly_go_host_store_delete(handle C.uint64_t, key C.RustBuffer, outReturn *C.RustBuffer, outStatus *C.RustCallStatus) {
	resetCallbackStatus(outStatus)
	store := getGoHostStore(uint64(handle))
	keyBytes, err := callbackByteArray(key)
	if err == nil && store == nil {
		err = errMissingHostStore()
	}
	if err == nil {
		err = store.Delete(keyBytes)
	}
	writeCallbackReturn(outReturn, encodeHostStoreUnitResult(err))
}

//export prolly_go_host_store_batch
func prolly_go_host_store_batch(handle C.uint64_t, ops C.RustBuffer, outReturn *C.RustBuffer, outStatus *C.RustCallStatus) {
	resetCallbackStatus(outStatus)
	store := getGoHostStore(uint64(handle))
	mutations, err := callbackMutations(ops)
	if err == nil && store == nil {
		err = errMissingHostStore()
	}
	if err == nil {
		err = store.Batch(mutations)
	}
	writeCallbackReturn(outReturn, encodeHostStoreUnitResult(err))
}

//export prolly_go_host_store_batch_get_ordered
func prolly_go_host_store_batch_get_ordered(handle C.uint64_t, keys C.RustBuffer, outReturn *C.RustBuffer, outStatus *C.RustCallStatus) {
	resetCallbackStatus(outStatus)
	store := getGoHostStore(uint64(handle))
	keyValues, err := callbackByteArraySequence(keys)
	var values []HostStoreResult
	if err == nil && store == nil {
		err = errMissingHostStore()
	}
	if err == nil {
		values, err = store.BatchGetOrdered(keyValues)
	}
	writeCallbackReturn(outReturn, encodeHostStoreBatchGetResult(values, err))
}

//export prolly_go_host_store_prefers_batch_reads
func prolly_go_host_store_prefers_batch_reads(handle C.uint64_t, outReturn *C.RustBuffer, outStatus *C.RustCallStatus) {
	resetCallbackStatus(outStatus)
	store := getGoHostStore(uint64(handle))
	if store == nil {
		writeCallbackReturn(outReturn, encodeHostStoreBoolResult(false, errMissingHostStore()))
		return
	}
	writeCallbackReturn(outReturn, encodeHostStoreBoolResult(store.PrefersBatchReads(), nil))
}

//export prolly_go_host_store_supports_hints
func prolly_go_host_store_supports_hints(handle C.uint64_t, outReturn *C.RustBuffer, outStatus *C.RustCallStatus) {
	resetCallbackStatus(outStatus)
	store := getGoHostStore(uint64(handle))
	if store == nil {
		writeCallbackReturn(outReturn, encodeHostStoreBoolResult(false, errMissingHostStore()))
		return
	}
	writeCallbackReturn(outReturn, encodeHostStoreBoolResult(store.SupportsHints(), nil))
}

//export prolly_go_host_store_get_hint
func prolly_go_host_store_get_hint(handle C.uint64_t, namespace C.RustBuffer, key C.RustBuffer, outReturn *C.RustBuffer, outStatus *C.RustCallStatus) {
	resetCallbackStatus(outStatus)
	store := getGoHostStore(uint64(handle))
	namespaceBytes, err := callbackByteArray(namespace)
	keyBytes, keyErr := callbackByteArray(key)
	if err == nil {
		err = keyErr
	}
	result := HostStoreResult{Err: err}
	if err == nil && store == nil {
		result.Err = errMissingHostStore()
	}
	if err == nil && store != nil {
		result = store.GetHint(namespaceBytes, keyBytes)
	}
	writeCallbackReturn(outReturn, encodeHostStoreBytesResult(result))
}

//export prolly_go_host_store_put_hint
func prolly_go_host_store_put_hint(handle C.uint64_t, namespace C.RustBuffer, key C.RustBuffer, value C.RustBuffer, outReturn *C.RustBuffer, outStatus *C.RustCallStatus) {
	resetCallbackStatus(outStatus)
	store := getGoHostStore(uint64(handle))
	namespaceBytes, err := callbackByteArray(namespace)
	keyBytes, keyErr := callbackByteArray(key)
	valueBytes, valueErr := callbackByteArray(value)
	if err == nil {
		err = keyErr
	}
	if err == nil {
		err = valueErr
	}
	if err == nil && store == nil {
		err = errMissingHostStore()
	}
	if err == nil {
		err = store.PutHint(namespaceBytes, keyBytes, valueBytes)
	}
	writeCallbackReturn(outReturn, encodeHostStoreUnitResult(err))
}

//export prolly_go_host_store_list_node_cids
func prolly_go_host_store_list_node_cids(handle C.uint64_t, outReturn *C.RustBuffer, outStatus *C.RustCallStatus) {
	resetCallbackStatus(outStatus)
	store := getGoHostStore(uint64(handle))
	var values [][]byte
	var err error
	if store == nil {
		err = errMissingHostStore()
	} else {
		values, err = store.ListNodeCids()
	}
	writeCallbackReturn(outReturn, encodeHostStoreListBytesResult(values, err))
}

//export prolly_go_host_store_get_root
func prolly_go_host_store_get_root(handle C.uint64_t, name C.RustBuffer, outReturn *C.RustBuffer, outStatus *C.RustCallStatus) {
	resetCallbackStatus(outStatus)
	store := getGoHostStore(uint64(handle))
	nameBytes, err := callbackByteArray(name)
	var manifest *RootManifest
	if err == nil && store == nil {
		err = errMissingHostStore()
	}
	if err == nil {
		manifest, err = store.GetRoot(nameBytes)
	}
	writeCallbackReturn(outReturn, encodeHostStoreRootResult(manifest, err))
}

//export prolly_go_host_store_put_root
func prolly_go_host_store_put_root(handle C.uint64_t, name C.RustBuffer, manifest C.RustBuffer, outReturn *C.RustBuffer, outStatus *C.RustCallStatus) {
	resetCallbackStatus(outStatus)
	store := getGoHostStore(uint64(handle))
	nameBytes, err := callbackByteArray(name)
	manifestBytes := copyCallbackRustBuffer(manifest)
	freeCallbackRustBuffer(manifest)
	if err == nil && store == nil {
		err = errMissingHostStore()
	}
	if err == nil {
		err = store.PutRoot(nameBytes, RootManifest{raw: manifestBytes})
	}
	writeCallbackReturn(outReturn, encodeHostStoreUnitResult(err))
}

//export prolly_go_host_store_delete_root
func prolly_go_host_store_delete_root(handle C.uint64_t, name C.RustBuffer, outReturn *C.RustBuffer, outStatus *C.RustCallStatus) {
	resetCallbackStatus(outStatus)
	store := getGoHostStore(uint64(handle))
	nameBytes, err := callbackByteArray(name)
	if err == nil && store == nil {
		err = errMissingHostStore()
	}
	if err == nil {
		err = store.DeleteRoot(nameBytes)
	}
	writeCallbackReturn(outReturn, encodeHostStoreUnitResult(err))
}

//export prolly_go_host_store_compare_and_swap_root
func prolly_go_host_store_compare_and_swap_root(handle C.uint64_t, name C.RustBuffer, expected C.RustBuffer, replacement C.RustBuffer, outReturn *C.RustBuffer, outStatus *C.RustCallStatus) {
	resetCallbackStatus(outStatus)
	store := getGoHostStore(uint64(handle))
	nameBytes, err := callbackByteArray(name)
	expectedRoot, expectedErr := callbackOptionalRootManifest(expected)
	replacementRoot, replacementErr := callbackOptionalRootManifest(replacement)
	if err == nil {
		err = expectedErr
	}
	if err == nil {
		err = replacementErr
	}
	result := HostStoreCasResult{Err: err}
	if err == nil && store == nil {
		result.Err = errMissingHostStore()
	}
	if err == nil && store != nil {
		result = store.CompareAndSwapRoot(nameBytes, expectedRoot, replacementRoot)
	}
	writeCallbackReturn(outReturn, encodeHostStoreCasResult(result))
}

//export prolly_go_host_store_list_roots
func prolly_go_host_store_list_roots(handle C.uint64_t, outReturn *C.RustBuffer, outStatus *C.RustCallStatus) {
	resetCallbackStatus(outStatus)
	store := getGoHostStore(uint64(handle))
	var roots []NamedRootManifest
	var err error
	if store == nil {
		err = errMissingHostStore()
	} else {
		roots, err = store.ListRoots()
	}
	writeCallbackReturn(outReturn, encodeHostStoreListRootsResult(roots, err))
}

func copyCallbackRustBuffer(buf C.RustBuffer) []byte {
	if buf.len == 0 || buf.data == nil {
		return nil
	}
	return C.GoBytes(unsafe.Pointer(buf.data), C.int(buf.len))
}

func resetCallbackStatus(outStatus *C.RustCallStatus) {
	if outStatus != nil {
		outStatus.code = 0
		outStatus.error_buf = C.RustBuffer{}
	}
}

func writeCallbackReturn(outReturn *C.RustBuffer, data []byte) {
	if outReturn != nil {
		*outReturn = callbackRustBufferFromBytes(data)
	}
}

func callbackByteArray(buf C.RustBuffer) ([]byte, error) {
	data := copyCallbackRustBuffer(buf)
	freeCallbackRustBuffer(buf)
	return decodeRequiredByteArray(data)
}

func callbackByteArraySequence(buf C.RustBuffer) ([][]byte, error) {
	data := copyCallbackRustBuffer(buf)
	freeCallbackRustBuffer(buf)
	return decodeByteArraySequence(data)
}

func callbackMutations(buf C.RustBuffer) ([]Mutation, error) {
	data := copyCallbackRustBuffer(buf)
	freeCallbackRustBuffer(buf)
	return decodeMutations(data)
}

func callbackOptionalRootManifest(buf C.RustBuffer) (*RootManifest, error) {
	data := copyCallbackRustBuffer(buf)
	freeCallbackRustBuffer(buf)
	return decodeOptionalRootManifest(data)
}

func errMissingHostStore() error {
	return errHostStoreMissing
}

func callbackRustBufferFromBytes(data []byte) C.RustBuffer {
	var status C.RustCallStatus
	buf := C.ffi_prolly_bindings_rustbuffer_alloc(C.uint64_t(len(data)), &status)
	if status.code != 0 {
		return C.RustBuffer{}
	}
	if len(data) > 0 {
		C.memcpy(unsafe.Pointer(buf.data), unsafe.Pointer(&data[0]), C.size_t(len(data)))
	}
	buf.len = C.uint64_t(len(data))
	return buf
}

func freeCallbackRustBuffer(buf C.RustBuffer) {
	var status C.RustCallStatus
	C.ffi_prolly_bindings_rustbuffer_free(buf, &status)
}

func encodeCallbackUnresolved() []byte {
	encoded, _ := encodeResolution(ResolveUnresolved())
	return encoded
}

func encodeCallbackCrdtDelete() []byte {
	encoded, _ := encodeCrdtResolution(CrdtResolveDelete())
	return encoded
}
