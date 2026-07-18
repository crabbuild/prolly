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

typedef void (*UniffiForeignFutureDroppedCallback)(uint64_t);

typedef struct UniffiForeignFutureDroppedCallbackStruct {
	uint64_t handle;
	UniffiForeignFutureDroppedCallback free;
} UniffiForeignFutureDroppedCallbackStruct;

typedef struct UniffiForeignFutureResultRustBuffer {
	RustBuffer return_value;
	RustCallStatus call_status;
} UniffiForeignFutureResultRustBuffer;

typedef void (*UniffiForeignFutureCompleteRustBuffer)(uint64_t, UniffiForeignFutureResultRustBuffer);

typedef void (*RemoteMethod0)(uint64_t, UniffiForeignFutureCompleteRustBuffer, uint64_t, UniffiForeignFutureDroppedCallbackStruct *);
typedef void (*RemoteMethod1)(uint64_t, RustBuffer, UniffiForeignFutureCompleteRustBuffer, uint64_t, UniffiForeignFutureDroppedCallbackStruct *);
typedef void (*RemoteMethod2)(uint64_t, RustBuffer, RustBuffer, UniffiForeignFutureCompleteRustBuffer, uint64_t, UniffiForeignFutureDroppedCallbackStruct *);
typedef void (*RemoteMethod3)(uint64_t, RustBuffer, RustBuffer, RustBuffer, UniffiForeignFutureCompleteRustBuffer, uint64_t, UniffiForeignFutureDroppedCallbackStruct *);
typedef void (*RemoteMethod4)(uint64_t, RustBuffer, RustBuffer, RustBuffer, RustBuffer, UniffiForeignFutureCompleteRustBuffer, uint64_t, UniffiForeignFutureDroppedCallbackStruct *);

typedef struct ForeignRemoteStoreVTable {
	void (*uniffi_free)(uint64_t);
	uint64_t (*uniffi_clone)(uint64_t);
	RemoteMethod0 descriptor;
	RemoteMethod1 get_node;
	RemoteMethod2 put_node;
	RemoteMethod1 delete_node;
	RemoteMethod1 batch_nodes;
	RemoteMethod1 batch_get_nodes_ordered;
	RemoteMethod0 list_node_cids;
	RemoteMethod2 get_hint;
	RemoteMethod3 put_hint;
	RemoteMethod4 batch_put_nodes_with_hint;
	RemoteMethod1 get_root_manifest;
	RemoteMethod2 put_root_manifest;
	RemoteMethod1 delete_root_manifest;
	RemoteMethod3 compare_and_swap_root_manifest;
	RemoteMethod0 list_root_manifests;
	RemoteMethod3 commit_transaction;
} ForeignRemoteStoreVTable;

extern void prolly_go_remote_store_free(uint64_t);
extern uint64_t prolly_go_remote_store_clone(uint64_t);
extern void prolly_go_remote_future_dropped(uint64_t);
extern uint64_t prolly_go_remote_dispatch0(uint32_t, uint64_t, uintptr_t, uint64_t);
extern uint64_t prolly_go_remote_dispatch1(uint32_t, uint64_t, RustBuffer, uintptr_t, uint64_t);
extern uint64_t prolly_go_remote_dispatch2(uint32_t, uint64_t, RustBuffer, RustBuffer, uintptr_t, uint64_t);
extern uint64_t prolly_go_remote_dispatch3(uint32_t, uint64_t, RustBuffer, RustBuffer, RustBuffer, uintptr_t, uint64_t);
extern uint64_t prolly_go_remote_dispatch4(uint32_t, uint64_t, RustBuffer, RustBuffer, RustBuffer, RustBuffer, uintptr_t, uint64_t);

static void remote_set_dropped(UniffiForeignFutureDroppedCallbackStruct *out, uint64_t handle) {
	out->handle = handle;
	out->free = prolly_go_remote_future_dropped;
}

static void remote_descriptor(uint64_t h, UniffiForeignFutureCompleteRustBuffer cb, uint64_t d, UniffiForeignFutureDroppedCallbackStruct *out) { remote_set_dropped(out, prolly_go_remote_dispatch0(0, h, (uintptr_t)cb, d)); }
static void remote_get_node(uint64_t h, RustBuffer a, UniffiForeignFutureCompleteRustBuffer cb, uint64_t d, UniffiForeignFutureDroppedCallbackStruct *out) { remote_set_dropped(out, prolly_go_remote_dispatch1(1, h, a, (uintptr_t)cb, d)); }
static void remote_put_node(uint64_t h, RustBuffer a, RustBuffer b, UniffiForeignFutureCompleteRustBuffer cb, uint64_t d, UniffiForeignFutureDroppedCallbackStruct *out) { remote_set_dropped(out, prolly_go_remote_dispatch2(2, h, a, b, (uintptr_t)cb, d)); }
static void remote_delete_node(uint64_t h, RustBuffer a, UniffiForeignFutureCompleteRustBuffer cb, uint64_t d, UniffiForeignFutureDroppedCallbackStruct *out) { remote_set_dropped(out, prolly_go_remote_dispatch1(3, h, a, (uintptr_t)cb, d)); }
static void remote_batch_nodes(uint64_t h, RustBuffer a, UniffiForeignFutureCompleteRustBuffer cb, uint64_t d, UniffiForeignFutureDroppedCallbackStruct *out) { remote_set_dropped(out, prolly_go_remote_dispatch1(4, h, a, (uintptr_t)cb, d)); }
static void remote_batch_get(uint64_t h, RustBuffer a, UniffiForeignFutureCompleteRustBuffer cb, uint64_t d, UniffiForeignFutureDroppedCallbackStruct *out) { remote_set_dropped(out, prolly_go_remote_dispatch1(5, h, a, (uintptr_t)cb, d)); }
static void remote_list_nodes(uint64_t h, UniffiForeignFutureCompleteRustBuffer cb, uint64_t d, UniffiForeignFutureDroppedCallbackStruct *out) { remote_set_dropped(out, prolly_go_remote_dispatch0(6, h, (uintptr_t)cb, d)); }
static void remote_get_hint(uint64_t h, RustBuffer a, RustBuffer b, UniffiForeignFutureCompleteRustBuffer cb, uint64_t d, UniffiForeignFutureDroppedCallbackStruct *out) { remote_set_dropped(out, prolly_go_remote_dispatch2(7, h, a, b, (uintptr_t)cb, d)); }
static void remote_put_hint(uint64_t h, RustBuffer a, RustBuffer b, RustBuffer c, UniffiForeignFutureCompleteRustBuffer cb, uint64_t d, UniffiForeignFutureDroppedCallbackStruct *out) { remote_set_dropped(out, prolly_go_remote_dispatch3(8, h, a, b, c, (uintptr_t)cb, d)); }
static void remote_batch_hint(uint64_t h, RustBuffer a, RustBuffer b, RustBuffer c, RustBuffer e, UniffiForeignFutureCompleteRustBuffer cb, uint64_t d, UniffiForeignFutureDroppedCallbackStruct *out) { remote_set_dropped(out, prolly_go_remote_dispatch4(9, h, a, b, c, e, (uintptr_t)cb, d)); }
static void remote_get_root(uint64_t h, RustBuffer a, UniffiForeignFutureCompleteRustBuffer cb, uint64_t d, UniffiForeignFutureDroppedCallbackStruct *out) { remote_set_dropped(out, prolly_go_remote_dispatch1(10, h, a, (uintptr_t)cb, d)); }
static void remote_put_root(uint64_t h, RustBuffer a, RustBuffer b, UniffiForeignFutureCompleteRustBuffer cb, uint64_t d, UniffiForeignFutureDroppedCallbackStruct *out) { remote_set_dropped(out, prolly_go_remote_dispatch2(11, h, a, b, (uintptr_t)cb, d)); }
static void remote_delete_root(uint64_t h, RustBuffer a, UniffiForeignFutureCompleteRustBuffer cb, uint64_t d, UniffiForeignFutureDroppedCallbackStruct *out) { remote_set_dropped(out, prolly_go_remote_dispatch1(12, h, a, (uintptr_t)cb, d)); }
static void remote_cas_root(uint64_t h, RustBuffer a, RustBuffer b, RustBuffer c, UniffiForeignFutureCompleteRustBuffer cb, uint64_t d, UniffiForeignFutureDroppedCallbackStruct *out) { remote_set_dropped(out, prolly_go_remote_dispatch3(13, h, a, b, c, (uintptr_t)cb, d)); }
static void remote_list_roots(uint64_t h, UniffiForeignFutureCompleteRustBuffer cb, uint64_t d, UniffiForeignFutureDroppedCallbackStruct *out) { remote_set_dropped(out, prolly_go_remote_dispatch0(14, h, (uintptr_t)cb, d)); }
static void remote_transaction(uint64_t h, RustBuffer a, RustBuffer b, RustBuffer c, UniffiForeignFutureCompleteRustBuffer cb, uint64_t d, UniffiForeignFutureDroppedCallbackStruct *out) { remote_set_dropped(out, prolly_go_remote_dispatch3(15, h, a, b, c, (uintptr_t)cb, d)); }

static ForeignRemoteStoreVTable remote_vtable = {
	prolly_go_remote_store_free, prolly_go_remote_store_clone,
	remote_descriptor, remote_get_node, remote_put_node, remote_delete_node,
	remote_batch_nodes, remote_batch_get, remote_list_nodes, remote_get_hint,
	remote_put_hint, remote_batch_hint, remote_get_root, remote_put_root,
	remote_delete_root, remote_cas_root, remote_list_roots, remote_transaction
};

extern void uniffi_prolly_bindings_fn_init_callback_vtable_foreignremotestore(const ForeignRemoteStoreVTable *);
static void prolly_register_go_remote_store_vtable(void) {
	uniffi_prolly_bindings_fn_init_callback_vtable_foreignremotestore(&remote_vtable);
}

static void prolly_remote_complete(uintptr_t cb, uint64_t data, RustBuffer value, RustCallStatus status) {
	((UniffiForeignFutureCompleteRustBuffer)cb)(data, (UniffiForeignFutureResultRustBuffer){value, status});
}
*/
import "C"

import "sync"

var registerRemoteVTableOnce sync.Once

func registerRemoteVTable() {
	registerRemoteVTableOnce.Do(func() { C.prolly_register_go_remote_store_vtable() })
}

func completeRemoteFuture(callback, callbackData uint64, payload []byte, unexpected error) {
	var status C.RustCallStatus
	var value C.RustBuffer
	if unexpected != nil {
		status.code = 2
		errorPayload, err := rustBufferFromBytes(encodeByteArray([]byte(unexpected.Error())))
		if err == nil {
			status.error_buf = errorPayload
		}
	} else {
		var err error
		value, err = rustBufferFromBytes(payload)
		if err != nil {
			status.code = 2
		}
	}
	C.prolly_remote_complete(C.uintptr_t(callback), C.uint64_t(callbackData), value, status)
}
