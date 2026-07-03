package prolly

/*
#cgo darwin LDFLAGS: -L${SRCDIR}/../../../../target/debug -Wl,-rpath,${SRCDIR}/../../../../target/debug -lprolly_bindings
#cgo linux LDFLAGS: -L${SRCDIR}/../../../../target/debug -Wl,-rpath,${SRCDIR}/../../../../target/debug -lprolly_bindings
#cgo windows LDFLAGS: -L${SRCDIR}/../../../../target/debug -lprolly_bindings
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

typedef void (*MergeResolverFreeCallback)(uint64_t handle);
typedef uint64_t (*MergeResolverCloneCallback)(uint64_t handle);
typedef void (*MergeResolverResolveCallback)(uint64_t handle, RustBuffer conflict, RustBuffer *out_return, RustCallStatus *out_status);
typedef void (*CrdtResolverFreeCallback)(uint64_t handle);
typedef uint64_t (*CrdtResolverCloneCallback)(uint64_t handle);
typedef void (*CrdtResolverResolveCallback)(uint64_t handle, RustBuffer conflict, RustBuffer *out_return, RustCallStatus *out_status);
typedef void (*HostStoreFreeCallback)(uint64_t handle);
typedef uint64_t (*HostStoreCloneCallback)(uint64_t handle);
typedef void (*HostStoreGetCallback)(uint64_t handle, RustBuffer key, RustBuffer *out_return, RustCallStatus *out_status);
typedef void (*HostStorePutCallback)(uint64_t handle, RustBuffer key, RustBuffer value, RustBuffer *out_return, RustCallStatus *out_status);
typedef void (*HostStoreDeleteCallback)(uint64_t handle, RustBuffer key, RustBuffer *out_return, RustCallStatus *out_status);
typedef void (*HostStoreBatchCallback)(uint64_t handle, RustBuffer ops, RustBuffer *out_return, RustCallStatus *out_status);
typedef void (*HostStoreBatchGetOrderedCallback)(uint64_t handle, RustBuffer keys, RustBuffer *out_return, RustCallStatus *out_status);
typedef void (*HostStorePrefersBatchReadsCallback)(uint64_t handle, RustBuffer *out_return, RustCallStatus *out_status);
typedef void (*HostStoreSupportsHintsCallback)(uint64_t handle, RustBuffer *out_return, RustCallStatus *out_status);
typedef void (*HostStoreGetHintCallback)(uint64_t handle, RustBuffer namespace, RustBuffer key, RustBuffer *out_return, RustCallStatus *out_status);
typedef void (*HostStorePutHintCallback)(uint64_t handle, RustBuffer namespace, RustBuffer key, RustBuffer value, RustBuffer *out_return, RustCallStatus *out_status);
typedef void (*HostStoreListNodeCidsCallback)(uint64_t handle, RustBuffer *out_return, RustCallStatus *out_status);
typedef void (*HostStoreGetRootCallback)(uint64_t handle, RustBuffer name, RustBuffer *out_return, RustCallStatus *out_status);
typedef void (*HostStorePutRootCallback)(uint64_t handle, RustBuffer name, RustBuffer manifest, RustBuffer *out_return, RustCallStatus *out_status);
typedef void (*HostStoreDeleteRootCallback)(uint64_t handle, RustBuffer name, RustBuffer *out_return, RustCallStatus *out_status);
typedef void (*HostStoreCompareAndSwapRootCallback)(uint64_t handle, RustBuffer name, RustBuffer expected, RustBuffer replacement, RustBuffer *out_return, RustCallStatus *out_status);
typedef void (*HostStoreListRootsCallback)(uint64_t handle, RustBuffer *out_return, RustCallStatus *out_status);

typedef struct UniFfiTraitVtableMergeResolverCallback {
	MergeResolverFreeCallback uniffi_free;
	MergeResolverCloneCallback uniffi_clone;
	MergeResolverResolveCallback resolve;
} UniFfiTraitVtableMergeResolverCallback;

typedef struct UniFfiTraitVtableCrdtResolverCallback {
	CrdtResolverFreeCallback uniffi_free;
	CrdtResolverCloneCallback uniffi_clone;
	CrdtResolverResolveCallback resolve;
} UniFfiTraitVtableCrdtResolverCallback;

typedef struct UniFfiTraitVtableHostStoreCallback {
	HostStoreFreeCallback uniffi_free;
	HostStoreCloneCallback uniffi_clone;
	HostStoreGetCallback get;
	HostStorePutCallback put;
	HostStoreDeleteCallback delete_;
	HostStoreBatchCallback batch;
	HostStoreBatchGetOrderedCallback batch_get_ordered;
	HostStorePrefersBatchReadsCallback prefers_batch_reads;
	HostStoreSupportsHintsCallback supports_hints;
	HostStoreGetHintCallback get_hint;
	HostStorePutHintCallback put_hint;
	HostStoreListNodeCidsCallback list_node_cids;
	HostStoreGetRootCallback get_root;
	HostStorePutRootCallback put_root;
	HostStoreDeleteRootCallback delete_root;
	HostStoreCompareAndSwapRootCallback compare_and_swap_root;
	HostStoreListRootsCallback list_roots;
} UniFfiTraitVtableHostStoreCallback;

extern void prolly_go_resolver_free(uint64_t handle);
extern uint64_t prolly_go_resolver_clone(uint64_t handle);
extern void prolly_go_resolver_resolve(uint64_t handle, RustBuffer conflict, RustBuffer *out_return, RustCallStatus *out_status);
extern void prolly_go_crdt_resolver_free(uint64_t handle);
extern uint64_t prolly_go_crdt_resolver_clone(uint64_t handle);
extern void prolly_go_crdt_resolver_resolve(uint64_t handle, RustBuffer conflict, RustBuffer *out_return, RustCallStatus *out_status);
extern void prolly_go_host_store_free(uint64_t handle);
extern uint64_t prolly_go_host_store_clone(uint64_t handle);
extern void prolly_go_host_store_get(uint64_t handle, RustBuffer key, RustBuffer *out_return, RustCallStatus *out_status);
extern void prolly_go_host_store_put(uint64_t handle, RustBuffer key, RustBuffer value, RustBuffer *out_return, RustCallStatus *out_status);
extern void prolly_go_host_store_delete(uint64_t handle, RustBuffer key, RustBuffer *out_return, RustCallStatus *out_status);
extern void prolly_go_host_store_batch(uint64_t handle, RustBuffer ops, RustBuffer *out_return, RustCallStatus *out_status);
extern void prolly_go_host_store_batch_get_ordered(uint64_t handle, RustBuffer keys, RustBuffer *out_return, RustCallStatus *out_status);
extern void prolly_go_host_store_prefers_batch_reads(uint64_t handle, RustBuffer *out_return, RustCallStatus *out_status);
extern void prolly_go_host_store_supports_hints(uint64_t handle, RustBuffer *out_return, RustCallStatus *out_status);
extern void prolly_go_host_store_get_hint(uint64_t handle, RustBuffer namespace, RustBuffer key, RustBuffer *out_return, RustCallStatus *out_status);
extern void prolly_go_host_store_put_hint(uint64_t handle, RustBuffer namespace, RustBuffer key, RustBuffer value, RustBuffer *out_return, RustCallStatus *out_status);
extern void prolly_go_host_store_list_node_cids(uint64_t handle, RustBuffer *out_return, RustCallStatus *out_status);
extern void prolly_go_host_store_get_root(uint64_t handle, RustBuffer name, RustBuffer *out_return, RustCallStatus *out_status);
extern void prolly_go_host_store_put_root(uint64_t handle, RustBuffer name, RustBuffer manifest, RustBuffer *out_return, RustCallStatus *out_status);
extern void prolly_go_host_store_delete_root(uint64_t handle, RustBuffer name, RustBuffer *out_return, RustCallStatus *out_status);
extern void prolly_go_host_store_compare_and_swap_root(uint64_t handle, RustBuffer name, RustBuffer expected, RustBuffer replacement, RustBuffer *out_return, RustCallStatus *out_status);
extern void prolly_go_host_store_list_roots(uint64_t handle, RustBuffer *out_return, RustCallStatus *out_status);
extern void uniffi_prolly_bindings_fn_init_callback_vtable_mergeresolvercallback(UniFfiTraitVtableMergeResolverCallback *vtable);
extern void uniffi_prolly_bindings_fn_init_callback_vtable_crdtresolvercallback(UniFfiTraitVtableCrdtResolverCallback *vtable);
extern void uniffi_prolly_bindings_fn_init_callback_vtable_hoststorecallback(UniFfiTraitVtableHostStoreCallback *vtable);

static UniFfiTraitVtableMergeResolverCallback prolly_go_resolver_vtable = {
	prolly_go_resolver_free,
	prolly_go_resolver_clone,
	prolly_go_resolver_resolve,
};

static UniFfiTraitVtableCrdtResolverCallback prolly_go_crdt_resolver_vtable = {
	prolly_go_crdt_resolver_free,
	prolly_go_crdt_resolver_clone,
	prolly_go_crdt_resolver_resolve,
};

static UniFfiTraitVtableHostStoreCallback prolly_go_host_store_vtable = {
	prolly_go_host_store_free,
	prolly_go_host_store_clone,
	prolly_go_host_store_get,
	prolly_go_host_store_put,
	prolly_go_host_store_delete,
	prolly_go_host_store_batch,
	prolly_go_host_store_batch_get_ordered,
	prolly_go_host_store_prefers_batch_reads,
	prolly_go_host_store_supports_hints,
	prolly_go_host_store_get_hint,
	prolly_go_host_store_put_hint,
	prolly_go_host_store_list_node_cids,
	prolly_go_host_store_get_root,
	prolly_go_host_store_put_root,
	prolly_go_host_store_delete_root,
	prolly_go_host_store_compare_and_swap_root,
	prolly_go_host_store_list_roots,
};

static void prolly_register_go_resolver_vtable(void) {
	uniffi_prolly_bindings_fn_init_callback_vtable_mergeresolvercallback(&prolly_go_resolver_vtable);
}

static void prolly_register_go_crdt_resolver_vtable(void) {
	uniffi_prolly_bindings_fn_init_callback_vtable_crdtresolvercallback(&prolly_go_crdt_resolver_vtable);
}

static void prolly_register_go_host_store_vtable(void) {
	uniffi_prolly_bindings_fn_init_callback_vtable_hoststorecallback(&prolly_go_host_store_vtable);
}

extern RustBuffer ffi_prolly_bindings_rustbuffer_alloc(uint64_t size, RustCallStatus *out_err);
extern void ffi_prolly_bindings_rustbuffer_free(RustBuffer buf, RustCallStatus *out_err);

extern uint64_t uniffi_prolly_bindings_fn_clone_prollyblobstore(uint64_t ptr, RustCallStatus *out_err);
extern void uniffi_prolly_bindings_fn_free_prollyblobstore(uint64_t ptr, RustCallStatus *out_err);
extern uint64_t uniffi_prolly_bindings_fn_constructor_prollyblobstore_memory(RustCallStatus *out_err);
extern uint64_t uniffi_prolly_bindings_fn_constructor_prollyblobstore_file(RustBuffer path, RustCallStatus *out_err);
extern uint64_t uniffi_prolly_bindings_fn_method_prollyblobstore_blob_count(uint64_t ptr, RustCallStatus *out_err);
extern void uniffi_prolly_bindings_fn_method_prollyblobstore_delete_blob(uint64_t ptr, RustBuffer reference, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_prollyblobstore_get_blob(uint64_t ptr, RustBuffer reference, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_prollyblobstore_list_blob_refs(uint64_t ptr, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_prollyblobstore_put_blob(uint64_t ptr, RustBuffer bytes, RustCallStatus *out_err);
extern uint64_t uniffi_prolly_bindings_fn_clone_mergepolicyregistry(uint64_t ptr, RustCallStatus *out_err);
extern void uniffi_prolly_bindings_fn_free_mergepolicyregistry(uint64_t ptr, RustCallStatus *out_err);
extern uint64_t uniffi_prolly_bindings_fn_constructor_mergepolicyregistry_new(RustCallStatus *out_err);
extern int8_t uniffi_prolly_bindings_fn_method_mergepolicyregistry_has_default(uint64_t ptr, RustCallStatus *out_err);
extern int8_t uniffi_prolly_bindings_fn_method_mergepolicyregistry_is_empty(uint64_t ptr, RustCallStatus *out_err);
extern uint64_t uniffi_prolly_bindings_fn_method_mergepolicyregistry_len(uint64_t ptr, RustCallStatus *out_err);
extern void uniffi_prolly_bindings_fn_method_mergepolicyregistry_push_exact_resolver(uint64_t ptr, RustBuffer key, uint64_t resolver, RustCallStatus *out_err);
extern void uniffi_prolly_bindings_fn_method_mergepolicyregistry_push_exact_resolver_name(uint64_t ptr, RustBuffer key, RustBuffer name, RustCallStatus *out_err);
extern void uniffi_prolly_bindings_fn_method_mergepolicyregistry_push_prefix_resolver(uint64_t ptr, RustBuffer prefix, uint64_t resolver, RustCallStatus *out_err);
extern void uniffi_prolly_bindings_fn_method_mergepolicyregistry_push_prefix_resolver_name(uint64_t ptr, RustBuffer prefix, RustBuffer name, RustCallStatus *out_err);
extern void uniffi_prolly_bindings_fn_method_mergepolicyregistry_set_default_resolver(uint64_t ptr, uint64_t resolver, RustCallStatus *out_err);
extern void uniffi_prolly_bindings_fn_method_mergepolicyregistry_set_default_resolver_name(uint64_t ptr, RustBuffer name, RustCallStatus *out_err);
extern uint64_t uniffi_prolly_bindings_fn_clone_prollyengine(uint64_t ptr, RustCallStatus *out_err);
extern void uniffi_prolly_bindings_fn_free_prollyengine(uint64_t ptr, RustCallStatus *out_err);
extern uint64_t uniffi_prolly_bindings_fn_constructor_prollyengine_custom_store(uint64_t callback, RustBuffer config, RustCallStatus *out_err);
extern uint64_t uniffi_prolly_bindings_fn_constructor_prollyengine_file(RustBuffer path, RustBuffer config, RustCallStatus *out_err);
extern uint64_t uniffi_prolly_bindings_fn_constructor_prollyengine_memory(RustBuffer config, RustCallStatus *out_err);
extern uint64_t uniffi_prolly_bindings_fn_constructor_prollyengine_sqlite(RustBuffer path, RustBuffer config, RustCallStatus *out_err);
extern uint64_t uniffi_prolly_bindings_fn_constructor_prollyengine_sqlite_in_memory(RustBuffer config, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_func_changed_span(RustBuffer start, RustBuffer end, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_func_changed_span_for_prefix(RustBuffer prefix, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_func_changed_span_from_key(RustBuffer key, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_func_cid_from_bytes(RustBuffer bytes, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_func_crdt_config_lww(RustBuffer delete_policy, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_func_crdt_config_multi_value(RustBuffer delete_policy, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_func_debug_key(RustBuffer key, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_func_decode_segments(RustBuffer key, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_func_default_config(RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_func_default_large_value_config(RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_func_default_parallel_config(RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_func_encode_segment(RustBuffer segment, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_func_i64_key(int64_t value, RustCallStatus *out_err);
extern uint8_t uniffi_prolly_bindings_fn_func_is_boundary_config(RustBuffer config, uint64_t count, RustBuffer key, RustBuffer value, RustCallStatus *out_err);
extern uint8_t uniffi_prolly_bindings_fn_func_is_tombstone_value(RustBuffer bytes, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_func_key_from_prefixed_segments(RustBuffer prefix, RustBuffer segments, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_func_key_from_segments(RustBuffer segments, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_func_retain_all_named_roots(RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_func_retain_exact_named_roots(RustBuffer names, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_func_retain_named_root_prefix(RustBuffer prefix, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_func_retain_named_roots_updated_since(RustBuffer prefix, uint64_t min_updated_at_millis, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_func_retain_newest_named_roots(RustBuffer prefix, uint64_t count, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_func_key_proof_from_node_bytes(RustBuffer root, RustBuffer key, RustBuffer path_node_bytes, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_func_key_proof_path_node_bytes(RustBuffer proof, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_func_key_proof_from_bytes(RustBuffer bytes, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_func_key_proof_to_bytes(RustBuffer proof, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_func_multi_key_proof_from_node_bytes(RustBuffer root, RustBuffer keys, RustBuffer path_node_bytes, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_func_multi_key_proof_path_node_bytes(RustBuffer proof, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_func_multi_key_proof_from_bytes(RustBuffer bytes, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_func_multi_key_proof_to_bytes(RustBuffer proof, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_func_range_proof_from_node_bytes(RustBuffer root, RustBuffer start, RustBuffer end, RustBuffer path_node_bytes, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_func_range_proof_path_node_bytes(RustBuffer proof, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_func_range_proof_from_bytes(RustBuffer bytes, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_func_range_proof_to_bytes(RustBuffer proof, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_func_range_page_proof_from_node_bytes(RustBuffer root, RustBuffer after, RustBuffer end, RustBuffer path_node_bytes, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_func_range_page_proof_path_node_bytes(RustBuffer proof, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_func_range_page_proof_from_bytes(RustBuffer bytes, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_func_range_page_proof_to_bytes(RustBuffer proof, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_func_diff_page_proof_from_bytes(RustBuffer bytes, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_func_diff_page_proof_to_bytes(RustBuffer proof, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_func_inspect_proof_bundle(RustBuffer bytes, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_func_verify_proof_bundle(RustBuffer bytes, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_func_authenticated_proof_envelope_from_bytes(RustBuffer bytes, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_func_authenticated_proof_envelope_to_bytes(RustBuffer envelope, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_func_sign_proof_bundle_hmac_sha256(RustBuffer proof_bundle, RustBuffer key_id, RustBuffer secret, RustBuffer context, RustBuffer issued_at_millis, RustBuffer expires_at_millis, RustBuffer nonce, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_func_verify_authenticated_proof_envelope(RustBuffer envelope, RustBuffer secret, RustBuffer now_millis, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_func_verify_authenticated_proof_bundle(RustBuffer envelope_bytes, RustBuffer secret, RustBuffer now_millis, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_func_multi_value_set_from_bytes(RustBuffer bytes, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_func_multi_value_set_merge(RustBuffer left, RustBuffer right, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_func_multi_value_set_to_bytes(RustBuffer values, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_func_node_cid(RustBuffer node, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_func_node_from_bytes(RustBuffer bytes, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_func_node_to_bytes(RustBuffer node, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_func_prefix_end(RustBuffer prefix, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_func_prefix_range(RustBuffer prefix, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_func_range_cursor_after_key(RustBuffer key, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_func_range_cursor_start(RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_func_reverse_cursor_before_key(RustBuffer key, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_func_reverse_cursor_end(RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_func_root_manifest_from_bytes(RustBuffer bytes, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_func_root_manifest_to_bytes(RustBuffer record, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_func_snapshot_bundle_digest(RustBuffer record, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_func_snapshot_bundle_digest_bytes(RustBuffer bytes, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_func_snapshot_bundle_from_bytes(RustBuffer bytes, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_func_snapshot_bundle_summary(RustBuffer record, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_func_snapshot_bundle_summary_from_bytes(RustBuffer bytes, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_func_snapshot_bundle_to_bytes(RustBuffer record, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_func_snapshot_id_from_name(RustBuffer namespace, RustBuffer name, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_func_snapshot_namespace_branch(RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_func_snapshot_namespace_checkpoint(RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_func_snapshot_namespace_custom(RustBuffer prefix, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_func_snapshot_namespace_tag(RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_func_snapshot_root_name(RustBuffer namespace, RustBuffer id, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_func_timestamp_millis_key(uint64_t value, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_func_timestamped_value_from_bytes(RustBuffer bytes, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_func_timestamped_value_now(RustBuffer value, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_func_timestamped_value_to_bytes(RustBuffer record, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_func_verify_snapshot_bundle(RustBuffer record, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_func_verify_snapshot_bundle_bytes(RustBuffer bytes, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_func_tombstone_compaction_mutation(RustBuffer key, RustBuffer stored_value, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_func_tombstone_from_bytes(RustBuffer bytes, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_func_tombstone_from_stored_bytes(RustBuffer bytes, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_func_tombstone_to_bytes(RustBuffer record, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_func_tombstone_upsert_mutation(RustBuffer key, RustBuffer tombstone, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_func_u64_key(uint64_t value, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_func_u128_key(RustBuffer value, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_func_i128_key(RustBuffer value, RustCallStatus *out_err);
extern void uniffi_prolly_bindings_fn_func_blob_ref_validate_bytes(RustBuffer reference, RustBuffer bytes, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_func_value_ref_from_bytes(RustBuffer bytes, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_func_value_ref_from_stored_bytes(RustBuffer bytes, RustCallStatus *out_err);
extern uint8_t uniffi_prolly_bindings_fn_func_value_ref_inline_requires_escape(RustBuffer value, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_func_value_ref_to_bytes(RustBuffer record, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_func_verify_key_proof(RustBuffer proof, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_func_verify_multi_key_proof(RustBuffer proof, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_func_verify_range_proof(RustBuffer proof, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_func_verify_range_page_proof(RustBuffer proof, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_func_verify_diff_page_proof(RustBuffer proof, RustCallStatus *out_err);
extern uint8_t uniffi_prolly_bindings_fn_func_versioned_value_bytes_matches_schema(RustBuffer bytes, RustBuffer schema, uint64_t version, RustCallStatus *out_err);
extern void uniffi_prolly_bindings_fn_func_versioned_value_bytes_require_schema(RustBuffer bytes, RustBuffer schema, uint64_t version, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_func_versioned_value_from_bytes(RustBuffer bytes, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_func_versioned_value_to_bytes(RustBuffer record, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_prollyengine_create(uint64_t ptr, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_prollyengine_batch(uint64_t ptr, RustBuffer tree, RustBuffer mutations, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_prollyengine_batch_with_stats(uint64_t ptr, RustBuffer tree, RustBuffer mutations, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_prollyengine_append_batch(uint64_t ptr, RustBuffer tree, RustBuffer mutations, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_prollyengine_append_batch_with_stats(uint64_t ptr, RustBuffer tree, RustBuffer mutations, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_prollyengine_build_from_entries(uint64_t ptr, RustBuffer entries, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_prollyengine_build_from_sorted_entries(uint64_t ptr, RustBuffer entries, RustCallStatus *out_err);
	extern RustBuffer uniffi_prolly_bindings_fn_method_prollyengine_cache_stats(uint64_t ptr, RustCallStatus *out_err);
	extern void uniffi_prolly_bindings_fn_method_prollyengine_clear_cache(uint64_t ptr, RustCallStatus *out_err);
	extern RustBuffer uniffi_prolly_bindings_fn_method_prollyengine_collect_stats_json(uint64_t ptr, RustBuffer tree, RustCallStatus *out_err);
	extern RustBuffer uniffi_prolly_bindings_fn_method_prollyengine_compare_and_swap_named_root(uint64_t ptr, RustBuffer name, RustBuffer expected, RustBuffer replacement, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_prollyengine_compare_and_swap_snapshot(uint64_t ptr, RustBuffer namespace, RustBuffer id, RustBuffer expected, RustBuffer replacement, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_prollyengine_compare_and_swap_snapshot_at_millis(uint64_t ptr, RustBuffer namespace, RustBuffer id, RustBuffer expected, RustBuffer replacement, uint64_t timestamp_millis, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_prollyengine_copy_missing_nodes(uint64_t ptr, RustBuffer tree, uint64_t destination, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_prollyengine_cursor_window(uint64_t ptr, RustBuffer tree, RustBuffer key, RustBuffer end, uint64_t limit, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_prollyengine_crdt_merge(uint64_t ptr, RustBuffer base, RustBuffer left, RustBuffer right, RustBuffer config, RustCallStatus *out_err);
	extern RustBuffer uniffi_prolly_bindings_fn_method_prollyengine_crdt_merge_with_resolver(uint64_t ptr, RustBuffer base, RustBuffer left, RustBuffer right, RustBuffer delete_policy, uint64_t resolver, RustCallStatus *out_err);
	extern RustBuffer uniffi_prolly_bindings_fn_method_prollyengine_debug_compare_trees_json(uint64_t ptr, RustBuffer left, RustBuffer right, RustCallStatus *out_err);
	extern RustBuffer uniffi_prolly_bindings_fn_method_prollyengine_debug_compare_trees_text(uint64_t ptr, RustBuffer left, RustBuffer right, RustCallStatus *out_err);
	extern RustBuffer uniffi_prolly_bindings_fn_method_prollyengine_debug_tree_json(uint64_t ptr, RustBuffer tree, RustCallStatus *out_err);
	extern RustBuffer uniffi_prolly_bindings_fn_method_prollyengine_debug_tree_text(uint64_t ptr, RustBuffer tree, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_prollyengine_delete(uint64_t ptr, RustBuffer tree, RustBuffer key, RustCallStatus *out_err);
extern void uniffi_prolly_bindings_fn_method_prollyengine_delete_named_root(uint64_t ptr, RustBuffer name, RustCallStatus *out_err);
extern void uniffi_prolly_bindings_fn_method_prollyengine_delete_snapshot(uint64_t ptr, RustBuffer namespace, RustBuffer id, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_prollyengine_diff(uint64_t ptr, RustBuffer base, RustBuffer other, RustCallStatus *out_err);
	extern RustBuffer uniffi_prolly_bindings_fn_method_prollyengine_range_diff(uint64_t ptr, RustBuffer base, RustBuffer other, RustBuffer start, RustBuffer end, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_prollyengine_diff_from_cursor(uint64_t ptr, RustBuffer base, RustBuffer other, RustBuffer cursor, RustBuffer end, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_prollyengine_diff_page(uint64_t ptr, RustBuffer base, RustBuffer other, RustBuffer cursor, RustBuffer end, uint64_t limit, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_prollyengine_conflict_page(uint64_t ptr, RustBuffer base, RustBuffer left, RustBuffer right, RustBuffer cursor, uint64_t limit, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_prollyengine_export_snapshot(uint64_t ptr, RustBuffer tree, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_prollyengine_first_entry(uint64_t ptr, RustBuffer tree, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_prollyengine_get(uint64_t ptr, RustBuffer tree, RustBuffer key, RustCallStatus *out_err);
	extern RustBuffer uniffi_prolly_bindings_fn_method_prollyengine_get_large_value(uint64_t ptr, uint64_t blob_store, RustBuffer tree, RustBuffer key, RustCallStatus *out_err);
	extern RustBuffer uniffi_prolly_bindings_fn_method_prollyengine_get_many(uint64_t ptr, RustBuffer tree, RustBuffer keys, RustCallStatus *out_err);
	extern RustBuffer uniffi_prolly_bindings_fn_method_prollyengine_get_value_ref(uint64_t ptr, RustBuffer tree, RustBuffer key, RustCallStatus *out_err);
	extern uint8_t uniffi_prolly_bindings_fn_method_prollyengine_hydrate_prefix_path_hint(uint64_t ptr, RustBuffer tree, RustBuffer prefix, RustCallStatus *out_err);
	extern RustBuffer uniffi_prolly_bindings_fn_method_prollyengine_import_snapshot(uint64_t ptr, RustBuffer bundle, RustCallStatus *out_err);
	extern RustBuffer uniffi_prolly_bindings_fn_method_prollyengine_last_entry(uint64_t ptr, RustBuffer tree, RustCallStatus *out_err);
	extern RustBuffer uniffi_prolly_bindings_fn_method_prollyengine_list_named_root_manifests(uint64_t ptr, RustCallStatus *out_err);
	extern RustBuffer uniffi_prolly_bindings_fn_method_prollyengine_list_named_roots(uint64_t ptr, RustCallStatus *out_err);
	extern RustBuffer uniffi_prolly_bindings_fn_method_prollyengine_list_node_cids(uint64_t ptr, RustCallStatus *out_err);
	extern RustBuffer uniffi_prolly_bindings_fn_method_prollyengine_list_snapshots(uint64_t ptr, RustBuffer namespace, RustCallStatus *out_err);
	extern RustBuffer uniffi_prolly_bindings_fn_method_prollyengine_load_changed_spans_hint(uint64_t ptr, RustBuffer base, RustBuffer changed, RustCallStatus *out_err);
	extern RustBuffer uniffi_prolly_bindings_fn_method_prollyengine_load_named_root(uint64_t ptr, RustBuffer name, RustCallStatus *out_err);
	extern RustBuffer uniffi_prolly_bindings_fn_method_prollyengine_load_named_roots(uint64_t ptr, RustBuffer names, RustCallStatus *out_err);
	extern RustBuffer uniffi_prolly_bindings_fn_method_prollyengine_load_retained_named_roots(uint64_t ptr, RustBuffer retention, RustCallStatus *out_err);
	extern RustBuffer uniffi_prolly_bindings_fn_method_prollyengine_load_snapshot(uint64_t ptr, RustBuffer namespace, RustBuffer id, RustCallStatus *out_err);
	extern RustBuffer uniffi_prolly_bindings_fn_method_prollyengine_load_snapshots(uint64_t ptr, RustBuffer namespace, RustBuffer ids, RustCallStatus *out_err);
	extern RustBuffer uniffi_prolly_bindings_fn_method_prollyengine_lower_bound(uint64_t ptr, RustBuffer tree, RustBuffer key, RustCallStatus *out_err);
	extern RustBuffer uniffi_prolly_bindings_fn_method_prollyengine_mark_reachable(uint64_t ptr, RustBuffer roots, RustCallStatus *out_err);
	extern RustBuffer uniffi_prolly_bindings_fn_method_prollyengine_mark_reachable_blobs(uint64_t ptr, RustBuffer roots, RustCallStatus *out_err);
	extern RustBuffer uniffi_prolly_bindings_fn_method_prollyengine_merge(uint64_t ptr, RustBuffer base, RustBuffer left, RustBuffer right, RustBuffer resolver, RustCallStatus *out_err);
	extern RustBuffer uniffi_prolly_bindings_fn_method_prollyengine_merge_explain(uint64_t ptr, RustBuffer base, RustBuffer left, RustBuffer right, RustBuffer resolver, RustCallStatus *out_err);
	extern RustBuffer uniffi_prolly_bindings_fn_method_prollyengine_merge_explain_with_policy(uint64_t ptr, RustBuffer base, RustBuffer left, RustBuffer right, uint64_t policy, RustCallStatus *out_err);
	extern RustBuffer uniffi_prolly_bindings_fn_method_prollyengine_merge_explain_with_resolver(uint64_t ptr, RustBuffer base, RustBuffer left, RustBuffer right, uint64_t resolver, RustCallStatus *out_err);
	extern RustBuffer uniffi_prolly_bindings_fn_method_prollyengine_merge_prefix(uint64_t ptr, RustBuffer base, RustBuffer left, RustBuffer right, RustBuffer prefix, RustBuffer resolver, RustCallStatus *out_err);
	extern RustBuffer uniffi_prolly_bindings_fn_method_prollyengine_merge_prefix_with_policy(uint64_t ptr, RustBuffer base, RustBuffer left, RustBuffer right, RustBuffer prefix, uint64_t policy, RustCallStatus *out_err);
	extern RustBuffer uniffi_prolly_bindings_fn_method_prollyengine_merge_prefix_with_resolver(uint64_t ptr, RustBuffer base, RustBuffer left, RustBuffer right, RustBuffer prefix, uint64_t resolver, RustCallStatus *out_err);
	extern RustBuffer uniffi_prolly_bindings_fn_method_prollyengine_merge_range(uint64_t ptr, RustBuffer base, RustBuffer left, RustBuffer right, RustBuffer start, RustBuffer end, RustBuffer resolver, RustCallStatus *out_err);
	extern RustBuffer uniffi_prolly_bindings_fn_method_prollyengine_merge_range_with_policy(uint64_t ptr, RustBuffer base, RustBuffer left, RustBuffer right, RustBuffer start, RustBuffer end, uint64_t policy, RustCallStatus *out_err);
	extern RustBuffer uniffi_prolly_bindings_fn_method_prollyengine_merge_range_with_resolver(uint64_t ptr, RustBuffer base, RustBuffer left, RustBuffer right, RustBuffer start, RustBuffer end, uint64_t resolver, RustCallStatus *out_err);
	extern RustBuffer uniffi_prolly_bindings_fn_method_prollyengine_merge_with_policy(uint64_t ptr, RustBuffer base, RustBuffer left, RustBuffer right, uint64_t policy, RustCallStatus *out_err);
	extern RustBuffer uniffi_prolly_bindings_fn_method_prollyengine_merge_with_resolver(uint64_t ptr, RustBuffer base, RustBuffer left, RustBuffer right, uint64_t resolver, RustCallStatus *out_err);
	extern RustBuffer uniffi_prolly_bindings_fn_method_prollyengine_metrics(uint64_t ptr, RustCallStatus *out_err);
	extern RustBuffer uniffi_prolly_bindings_fn_method_prollyengine_parallel_batch(uint64_t ptr, RustBuffer tree, RustBuffer mutations, RustBuffer config, RustCallStatus *out_err);
	extern RustBuffer uniffi_prolly_bindings_fn_method_prollyengine_parallel_batch_with_stats(uint64_t ptr, RustBuffer tree, RustBuffer mutations, RustBuffer config, RustCallStatus *out_err);
	extern uint64_t uniffi_prolly_bindings_fn_method_prollyengine_pin_tree_path(uint64_t ptr, RustBuffer tree, RustBuffer key, RustCallStatus *out_err);
	extern uint64_t uniffi_prolly_bindings_fn_method_prollyengine_pin_tree_root(uint64_t ptr, RustBuffer tree, RustCallStatus *out_err);
	extern RustBuffer uniffi_prolly_bindings_fn_method_prollyengine_plan_blob_gc(uint64_t ptr, uint64_t blob_store, RustBuffer roots, RustBuffer candidate_blobs, RustCallStatus *out_err);
	extern RustBuffer uniffi_prolly_bindings_fn_method_prollyengine_plan_blob_store_gc(uint64_t ptr, uint64_t blob_store, RustBuffer roots, RustCallStatus *out_err);
	extern RustBuffer uniffi_prolly_bindings_fn_method_prollyengine_plan_gc(uint64_t ptr, RustBuffer roots, RustBuffer candidate_cids, RustCallStatus *out_err);
	extern RustBuffer uniffi_prolly_bindings_fn_method_prollyengine_plan_missing_nodes(uint64_t ptr, RustBuffer tree, uint64_t destination, RustCallStatus *out_err);
	extern RustBuffer uniffi_prolly_bindings_fn_method_prollyengine_plan_store_gc(uint64_t ptr, RustBuffer roots, RustCallStatus *out_err);
	extern RustBuffer uniffi_prolly_bindings_fn_method_prollyengine_plan_store_gc_for_retention(uint64_t ptr, RustBuffer retention, RustCallStatus *out_err);
	extern uint8_t uniffi_prolly_bindings_fn_method_prollyengine_publish_changed_spans_hint(uint64_t ptr, RustBuffer base, RustBuffer changed, RustBuffer spans, RustCallStatus *out_err);
	extern RustBuffer uniffi_prolly_bindings_fn_method_prollyengine_compare_and_swap_named_root_at_millis(uint64_t ptr, RustBuffer name, RustBuffer expected, RustBuffer replacement, uint64_t timestamp_millis, RustCallStatus *out_err);
	extern void uniffi_prolly_bindings_fn_method_prollyengine_publish_named_root(uint64_t ptr, RustBuffer name, RustBuffer tree, RustCallStatus *out_err);
	extern void uniffi_prolly_bindings_fn_method_prollyengine_publish_named_root_at_millis(uint64_t ptr, RustBuffer name, RustBuffer tree, uint64_t timestamp_millis, RustCallStatus *out_err);
	extern uint8_t uniffi_prolly_bindings_fn_method_prollyengine_publish_prefix_path_hint(uint64_t ptr, RustBuffer tree, RustBuffer prefix, RustCallStatus *out_err);
	extern void uniffi_prolly_bindings_fn_method_prollyengine_publish_snapshot(uint64_t ptr, RustBuffer namespace, RustBuffer id, RustBuffer tree, RustCallStatus *out_err);
	extern void uniffi_prolly_bindings_fn_method_prollyengine_publish_snapshot_at_millis(uint64_t ptr, RustBuffer namespace, RustBuffer id, RustBuffer tree, uint64_t timestamp_millis, RustCallStatus *out_err);
	extern RustBuffer uniffi_prolly_bindings_fn_method_prollyengine_prove_key(uint64_t ptr, RustBuffer tree, RustBuffer key, RustCallStatus *out_err);
	extern RustBuffer uniffi_prolly_bindings_fn_method_prollyengine_prove_keys(uint64_t ptr, RustBuffer tree, RustBuffer keys, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_prollyengine_prove_prefix(uint64_t ptr, RustBuffer tree, RustBuffer prefix, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_prollyengine_prove_range(uint64_t ptr, RustBuffer tree, RustBuffer start, RustBuffer end, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_prollyengine_prove_range_page(uint64_t ptr, RustBuffer tree, RustBuffer cursor, RustBuffer end, uint64_t limit, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_prollyengine_prove_diff_page(uint64_t ptr, RustBuffer base, RustBuffer other, RustBuffer cursor, RustBuffer end, uint64_t limit, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_prollyengine_put(uint64_t ptr, RustBuffer tree, RustBuffer key, RustBuffer value, RustCallStatus *out_err);
	extern RustBuffer uniffi_prolly_bindings_fn_method_prollyengine_put_large_value(uint64_t ptr, uint64_t blob_store, RustBuffer tree, RustBuffer key, RustBuffer value, RustBuffer config, RustCallStatus *out_err);
	extern RustBuffer uniffi_prolly_bindings_fn_method_prollyengine_prefix(uint64_t ptr, RustBuffer tree, RustBuffer prefix, RustCallStatus *out_err);
	extern RustBuffer uniffi_prolly_bindings_fn_method_prollyengine_prefix_page(uint64_t ptr, RustBuffer tree, RustBuffer prefix, RustBuffer cursor, uint64_t limit, RustCallStatus *out_err);
	extern RustBuffer uniffi_prolly_bindings_fn_method_prollyengine_prefix_reverse_page(uint64_t ptr, RustBuffer tree, RustBuffer prefix, RustBuffer cursor, uint64_t limit, RustCallStatus *out_err);
	extern RustBuffer uniffi_prolly_bindings_fn_method_prollyengine_range(uint64_t ptr, RustBuffer tree, RustBuffer start, RustBuffer end, RustCallStatus *out_err);
	extern RustBuffer uniffi_prolly_bindings_fn_method_prollyengine_range_after(uint64_t ptr, RustBuffer tree, RustBuffer after_key, RustBuffer end, RustCallStatus *out_err);
	extern RustBuffer uniffi_prolly_bindings_fn_method_prollyengine_range_from_cursor(uint64_t ptr, RustBuffer tree, RustBuffer cursor, RustBuffer end, RustCallStatus *out_err);
	extern RustBuffer uniffi_prolly_bindings_fn_method_prollyengine_range_page(uint64_t ptr, RustBuffer tree, RustBuffer cursor, RustBuffer end, uint64_t limit, RustCallStatus *out_err);
	extern RustBuffer uniffi_prolly_bindings_fn_method_prollyengine_reverse_page(uint64_t ptr, RustBuffer tree, RustBuffer cursor, RustBuffer start, uint64_t limit, RustCallStatus *out_err);
	extern void uniffi_prolly_bindings_fn_method_prollyengine_reset_metrics(uint64_t ptr, RustCallStatus *out_err);
	extern RustBuffer uniffi_prolly_bindings_fn_method_prollyengine_stats_diff_json(uint64_t ptr, RustBuffer before, RustBuffer after, RustCallStatus *out_err);
	extern RustBuffer uniffi_prolly_bindings_fn_method_prollyengine_structural_diff_page(uint64_t ptr, RustBuffer base, RustBuffer other, RustBuffer cursor_json, uint64_t limit, RustCallStatus *out_err);
	extern RustBuffer uniffi_prolly_bindings_fn_method_prollyengine_structural_diff_page_with_cursor(uint64_t ptr, RustBuffer base, RustBuffer other, RustBuffer cursor, uint64_t limit, RustCallStatus *out_err);
	extern RustBuffer uniffi_prolly_bindings_fn_method_prollyengine_sweep_blob_gc(uint64_t ptr, uint64_t blob_store, RustBuffer roots, RustBuffer candidate_blobs, RustCallStatus *out_err);
	extern RustBuffer uniffi_prolly_bindings_fn_method_prollyengine_sweep_blob_store_gc(uint64_t ptr, uint64_t blob_store, RustBuffer roots, RustCallStatus *out_err);
	extern RustBuffer uniffi_prolly_bindings_fn_method_prollyengine_sweep_gc(uint64_t ptr, RustBuffer roots, RustBuffer candidate_cids, RustCallStatus *out_err);
	extern RustBuffer uniffi_prolly_bindings_fn_method_prollyengine_sweep_store_gc(uint64_t ptr, RustBuffer roots, RustCallStatus *out_err);
	extern RustBuffer uniffi_prolly_bindings_fn_method_prollyengine_sweep_store_gc_for_retention(uint64_t ptr, RustBuffer retention, RustCallStatus *out_err);
	extern uint64_t uniffi_prolly_bindings_fn_method_prollyengine_unpin_all_cache_nodes(uint64_t ptr, RustCallStatus *out_err);
	extern RustBuffer uniffi_prolly_bindings_fn_method_prollyengine_upper_bound(uint64_t ptr, RustBuffer tree, RustBuffer key, RustCallStatus *out_err);
*/
import "C"

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"unsafe"
)

type Config struct {
	raw []byte
}

type ConfigOptions struct {
	MinChunkSize      uint64
	MaxChunkSize      uint64
	ChunkingFactor    uint32
	HashSeed          uint64
	EncodingKind      string
	CustomEncoding    string
	NodeCacheMaxNodes *uint64
	NodeCacheMaxBytes *uint64
}

type Encoding struct {
	Kind       string
	CustomName *string
}

type Tree struct {
	raw []byte
}

type SnapshotBundleNode struct {
	CID   []byte
	Bytes []byte
}

type SnapshotBundle struct {
	FormatVersion uint32
	Tree          Tree
	Nodes         []SnapshotBundleNode
}

type SnapshotBundleSummary struct {
	FormatVersion uint32
	Root          []byte
	HasRoot       bool
	NodeCount     uint64
	ByteCount     uint64
	MinNodeBytes  uint64
	MaxNodeBytes  uint64
}

type SnapshotBundleVerification struct {
	Valid          bool
	Summary        SnapshotBundleSummary
	ReachableNodes uint64
	ReachableBytes uint64
	MissingCids    [][]byte
	ExtraCids      [][]byte
}

type Entry struct {
	Key   []byte
	Value []byte
}

type Diff struct {
	Kind     string
	Key      []byte
	Value    []byte
	OldValue []byte
	NewValue []byte
}

type Mutation struct {
	Kind  string
	Key   []byte
	Value []byte
}

func UpsertMutation(key []byte, value []byte) Mutation {
	return Mutation{Kind: "upsert", Key: key, Value: value}
}

func DeleteMutation(key []byte) Mutation {
	return Mutation{Kind: "delete", Key: key}
}

type BatchApplyStats struct {
	InputMutations          uint64
	EffectiveMutations      uint64
	PreprocessInputSorted   bool
	AffectedLeaves          uint64
	ChangedLeaves           uint64
	SparseLeafApplies       uint64
	WrittenNodes            uint64
	WrittenBytes            uint64
	UsedAppendFastPath      bool
	UsedBatchedRoute        bool
	UsedCoalescedRebuild    bool
	UsedDeferredRebalancing bool
	UsedBottomUpRebuild     bool
	CacheWrittenNodes       bool
}

type BatchApplyResult struct {
	Tree  Tree
	Stats BatchApplyStats
}

type RangeCursor struct {
	AfterKey []byte
}

type ReverseCursor struct {
	BeforeKey []byte
}

func RangeCursorStart() (RangeCursor, error) {
	var status C.RustCallStatus
	out := C.uniffi_prolly_bindings_fn_func_range_cursor_start(&status)
	if err := statusError(&status); err != nil {
		return RangeCursor{}, err
	}
	defer freeRustBuffer(out)
	return decodeRangeCursor(copyRustBuffer(out))
}

func RangeCursorAfterKey(key []byte) (RangeCursor, error) {
	in, err := rustBufferFromBytes(encodeByteArray(key))
	if err != nil {
		return RangeCursor{}, err
	}
	var status C.RustCallStatus
	out := C.uniffi_prolly_bindings_fn_func_range_cursor_after_key(in, &status)
	if err := statusError(&status); err != nil {
		return RangeCursor{}, err
	}
	defer freeRustBuffer(out)
	return decodeRangeCursor(copyRustBuffer(out))
}

func ReverseCursorEnd() (ReverseCursor, error) {
	var status C.RustCallStatus
	out := C.uniffi_prolly_bindings_fn_func_reverse_cursor_end(&status)
	if err := statusError(&status); err != nil {
		return ReverseCursor{}, err
	}
	defer freeRustBuffer(out)
	return decodeReverseCursor(copyRustBuffer(out))
}

func ReverseCursorBeforeKey(key []byte) (ReverseCursor, error) {
	in, err := rustBufferFromBytes(encodeByteArray(key))
	if err != nil {
		return ReverseCursor{}, err
	}
	var status C.RustCallStatus
	out := C.uniffi_prolly_bindings_fn_func_reverse_cursor_before_key(in, &status)
	if err := statusError(&status); err != nil {
		return ReverseCursor{}, err
	}
	defer freeRustBuffer(out)
	return decodeReverseCursor(copyRustBuffer(out))
}

type RangeBounds struct {
	Start []byte
	End   []byte
}

type RangePage struct {
	Entries    []Entry
	NextCursor *RangeCursor
}

type ReversePage struct {
	Entries    []Entry
	NextCursor *ReverseCursor
}

type CursorWindow struct {
	PositionKey      []byte
	HasPositionKey   bool
	PositionValue    []byte
	HasPositionValue bool
	Found            bool
	Entries          []Entry
	NextCursor       *RangeCursor
}

type DiffPage struct {
	Diffs      []Diff
	NextCursor *RangeCursor
}

type Conflict struct {
	Key          []byte
	Base         []byte
	BasePresent  bool
	Left         []byte
	LeftPresent  bool
	Right        []byte
	RightPresent bool
}

type Resolution struct {
	Kind  string
	Value []byte
}

type Resolver func(Conflict) Resolution

func ResolveValue(value []byte) Resolution {
	return Resolution{Kind: "value", Value: value}
}

func ResolutionValue(value []byte) Resolution {
	return ResolveValue(value)
}

func ResolveDelete() Resolution {
	return Resolution{Kind: "delete"}
}

func ResolutionDelete() Resolution {
	return ResolveDelete()
}

func ResolveUnresolved() Resolution {
	return Resolution{Kind: "unresolved"}
}

func ResolutionUnresolved() Resolution {
	return ResolveUnresolved()
}

func ResolvePreferLeft(conflict Conflict) Resolution {
	if conflict.LeftPresent {
		return ResolutionValue(conflict.Left)
	}
	return ResolutionDelete()
}

func ResolvePreferRight(conflict Conflict) Resolution {
	if conflict.RightPresent {
		return ResolutionValue(conflict.Right)
	}
	return ResolutionDelete()
}

func ResolveDeleteWins(conflict Conflict) Resolution {
	if !conflict.LeftPresent || !conflict.RightPresent {
		return ResolutionDelete()
	}
	return ResolutionUnresolved()
}

func ResolveUpdateWins(conflict Conflict) Resolution {
	if conflict.LeftPresent && !conflict.RightPresent {
		return ResolutionValue(conflict.Left)
	}
	if !conflict.LeftPresent && conflict.RightPresent {
		return ResolutionValue(conflict.Right)
	}
	return ResolutionUnresolved()
}

type CrdtResolution struct {
	Kind  string
	Value []byte
}

type CrdtResolver func(Conflict) CrdtResolution

func CrdtResolveValue(value []byte) CrdtResolution {
	return CrdtResolution{Kind: "value", Value: value}
}

func CrdtResolutionValue(value []byte) CrdtResolution {
	return CrdtResolveValue(value)
}

func CrdtResolveDelete() CrdtResolution {
	return CrdtResolution{Kind: "delete"}
}

func CrdtResolutionDelete() CrdtResolution {
	return CrdtResolveDelete()
}

type ConflictPage struct {
	Conflicts  []Conflict
	NextCursor *RangeCursor
}

type DiffTraversalStats struct {
	ComparedNodes      uint64
	ReusedSubtrees     uint64
	AddedSubtrees      uint64
	RemovedSubtrees    uint64
	CollectedFallbacks uint64
	EmittedDiffs       uint64
}

type StructuralDiffPage struct {
	Diffs          []Diff
	NextCursorJSON string
	HasNextCursor  bool
	Stats          DiffTraversalStats
	NextCursor     *StructuralDiffCursor
}

type StructuralDiffCursor struct {
	BaseRoot  []byte
	OtherRoot []byte
	Markers   []StructuralDiffMarker
	Pending   []Diff
}

type StructuralDiffMarker struct {
	Kind       string
	BaseCID    []byte
	OtherCID   []byte
	SpanEnd    []byte
	HasSpanEnd bool
	CID        []byte
}

type MergeTrace struct {
	Events []MergeTraceEvent
}

type MergeTraceEvent struct {
	Kind           string
	FastPath       string
	CID            []byte
	HasCID         bool
	ReuseReason    string
	Level          *uint64
	Entries        *uint64
	FirstKey       []byte
	HasFirstKey    bool
	LastKey        []byte
	HasLastKey     bool
	Stage          string
	Key            []byte
	HasKey         bool
	Resolution     string
	FallbackReason string
	DiffStats      *DiffTraversalStats
	RightChanges   *uint64
	Mutations      *uint64
	AppendOnly     *bool
}

type TreeStats struct {
	NumNodes              uint64             `json:"num_nodes"`
	NumLeaves             uint64             `json:"num_leaves"`
	NumInternalNodes      uint64             `json:"num_internal_nodes"`
	TreeHeight            uint8              `json:"tree_height"`
	TotalKeyValuePairs    uint64             `json:"total_key_value_pairs"`
	TotalTreeSizeBytes    uint64             `json:"total_tree_size_bytes"`
	AvgNodeSizeBytes      float64            `json:"avg_node_size_bytes"`
	MinNodeSizeBytes      uint64             `json:"min_node_size_bytes"`
	MaxNodeSizeBytes      uint64             `json:"max_node_size_bytes"`
	AvgEntriesPerNode     float64            `json:"avg_entries_per_node"`
	NodesPerLevel         map[string]uint64  `json:"nodes_per_level"`
	AvgNodeSizePerLevel   map[string]float64 `json:"avg_node_size_per_level"`
	AvgEntriesPerLevel    map[string]float64 `json:"avg_entries_per_level"`
	MinEntriesPerLevel    map[string]uint64  `json:"min_entries_per_level"`
	MaxEntriesPerLevel    map[string]uint64  `json:"max_entries_per_level"`
	AvgFanout             float64            `json:"avg_fanout"`
	MinFanout             uint64             `json:"min_fanout"`
	MaxFanout             uint64             `json:"max_fanout"`
	AvgFillFactor         float64            `json:"avg_fill_factor"`
	AvgLeafFillFactor     float64            `json:"avg_leaf_fill_factor"`
	AvgInternalFillFactor float64            `json:"avg_internal_fill_factor"`
	AvgKeySizeBytes       float64            `json:"avg_key_size_bytes"`
	AvgValueSizeBytes     float64            `json:"avg_value_size_bytes"`
	MinKeySizeBytes       uint64             `json:"min_key_size_bytes"`
	MaxKeySizeBytes       uint64             `json:"max_key_size_bytes"`
	MinValueSizeBytes     uint64             `json:"min_value_size_bytes"`
	MaxValueSizeBytes     uint64             `json:"max_value_size_bytes"`
	TotalKeysSizeBytes    uint64             `json:"total_keys_size_bytes"`
	TotalValuesSizeBytes  uint64             `json:"total_values_size_bytes"`
}

type StatsDiff struct {
	NumNodesDiff              int64   `json:"num_nodes_diff"`
	NumLeavesDiff             int64   `json:"num_leaves_diff"`
	NumInternalNodesDiff      int64   `json:"num_internal_nodes_diff"`
	TreeHeightDiff            int8    `json:"tree_height_diff"`
	TotalKeyValuePairsDiff    int64   `json:"total_key_value_pairs_diff"`
	TotalTreeSizeBytesDiff    int64   `json:"total_tree_size_bytes_diff"`
	AvgNodeSizeBytesDiff      float64 `json:"avg_node_size_bytes_diff"`
	MinNodeSizeBytesDiff      int64   `json:"min_node_size_bytes_diff"`
	MaxNodeSizeBytesDiff      int64   `json:"max_node_size_bytes_diff"`
	AvgEntriesPerNodeDiff     float64 `json:"avg_entries_per_node_diff"`
	AvgFanoutDiff             float64 `json:"avg_fanout_diff"`
	MinFanoutDiff             int64   `json:"min_fanout_diff"`
	MaxFanoutDiff             int64   `json:"max_fanout_diff"`
	AvgFillFactorDiff         float64 `json:"avg_fill_factor_diff"`
	AvgLeafFillFactorDiff     float64 `json:"avg_leaf_fill_factor_diff"`
	AvgInternalFillFactorDiff float64 `json:"avg_internal_fill_factor_diff"`
	AvgKeySizeBytesDiff       float64 `json:"avg_key_size_bytes_diff"`
	AvgValueSizeBytesDiff     float64 `json:"avg_value_size_bytes_diff"`
	MinKeySizeBytesDiff       int64   `json:"min_key_size_bytes_diff"`
	MaxKeySizeBytesDiff       int64   `json:"max_key_size_bytes_diff"`
	MinValueSizeBytesDiff     int64   `json:"min_value_size_bytes_diff"`
	MaxValueSizeBytesDiff     int64   `json:"max_value_size_bytes_diff"`
	TotalKeysSizeBytesDiff    int64   `json:"total_keys_size_bytes_diff"`
	TotalValuesSizeBytesDiff  int64   `json:"total_values_size_bytes_diff"`
}

type StatsPercentageChange struct {
	NumNodesPct              float64 `json:"num_nodes_pct"`
	NumLeavesPct             float64 `json:"num_leaves_pct"`
	NumInternalNodesPct      float64 `json:"num_internal_nodes_pct"`
	TreeHeightPct            float64 `json:"tree_height_pct"`
	TotalKeyValuePairsPct    float64 `json:"total_key_value_pairs_pct"`
	TotalTreeSizeBytesPct    float64 `json:"total_tree_size_bytes_pct"`
	AvgNodeSizeBytesPct      float64 `json:"avg_node_size_bytes_pct"`
	MinNodeSizeBytesPct      float64 `json:"min_node_size_bytes_pct"`
	MaxNodeSizeBytesPct      float64 `json:"max_node_size_bytes_pct"`
	AvgEntriesPerNodePct     float64 `json:"avg_entries_per_node_pct"`
	AvgFanoutPct             float64 `json:"avg_fanout_pct"`
	MinFanoutPct             float64 `json:"min_fanout_pct"`
	MaxFanoutPct             float64 `json:"max_fanout_pct"`
	AvgFillFactorPct         float64 `json:"avg_fill_factor_pct"`
	AvgLeafFillFactorPct     float64 `json:"avg_leaf_fill_factor_pct"`
	AvgInternalFillFactorPct float64 `json:"avg_internal_fill_factor_pct"`
	AvgKeySizeBytesPct       float64 `json:"avg_key_size_bytes_pct"`
	AvgValueSizeBytesPct     float64 `json:"avg_value_size_bytes_pct"`
	MinKeySizeBytesPct       float64 `json:"min_key_size_bytes_pct"`
	MaxKeySizeBytesPct       float64 `json:"max_key_size_bytes_pct"`
	MinValueSizeBytesPct     float64 `json:"min_value_size_bytes_pct"`
	MaxValueSizeBytesPct     float64 `json:"max_value_size_bytes_pct"`
	TotalKeysSizeBytesPct    float64 `json:"total_keys_size_bytes_pct"`
	TotalValuesSizeBytesPct  float64 `json:"total_values_size_bytes_pct"`
}

type StatsComparison struct {
	Before     TreeStats             `json:"before"`
	After      TreeStats             `json:"after"`
	Absolute   StatsDiff             `json:"absolute"`
	Percentage StatsPercentageChange `json:"percentage"`
}

type TreeDebugNode struct {
	Cid          []byte  `json:"cid"`
	Leaf         bool    `json:"leaf"`
	Level        uint8   `json:"level"`
	EntryCount   uint64  `json:"entry_count"`
	MaxEntries   uint64  `json:"max_entries"`
	FillFactor   float64 `json:"fill_factor"`
	EncodedBytes uint64  `json:"encoded_bytes"`
	FirstKey     []byte  `json:"first_key"`
	LastKey      []byte  `json:"last_key"`
}

type TreeDebugLevel struct {
	Level uint8           `json:"level"`
	Nodes []TreeDebugNode `json:"nodes"`
}

type TreeDebugView struct {
	Levels []TreeDebugLevel `json:"levels"`
}

type TreeDebugComparedNode struct {
	Status string        `json:"status"`
	Node   TreeDebugNode `json:"node"`
}

type TreeDebugComparisonLevel struct {
	Level          uint8                   `json:"level"`
	SharedNodes    uint64                  `json:"shared_nodes"`
	LeftOnlyNodes  uint64                  `json:"left_only_nodes"`
	RightOnlyNodes uint64                  `json:"right_only_nodes"`
	SharedBytes    uint64                  `json:"shared_bytes"`
	LeftOnlyBytes  uint64                  `json:"left_only_bytes"`
	RightOnlyBytes uint64                  `json:"right_only_bytes"`
	Nodes          []TreeDebugComparedNode `json:"nodes"`
}

type TreeDebugComparison struct {
	SharedNodes    uint64                     `json:"shared_nodes"`
	LeftOnlyNodes  uint64                     `json:"left_only_nodes"`
	RightOnlyNodes uint64                     `json:"right_only_nodes"`
	SharedBytes    uint64                     `json:"shared_bytes"`
	LeftOnlyBytes  uint64                     `json:"left_only_bytes"`
	RightOnlyBytes uint64                     `json:"right_only_bytes"`
	Levels         []TreeDebugComparisonLevel `json:"levels"`
}

type MergeExplanation struct {
	Result    *Tree
	Error     string
	HasError  bool
	TraceJSON string
	Trace     MergeTrace
}

type NamedRoot struct {
	Name []byte
	Tree Tree
}

type RootManifestRecord struct {
	Tree            Tree
	CreatedAtMillis *uint64
	UpdatedAtMillis *uint64
}

type NamedRootManifestRecord struct {
	Name     []byte
	Manifest RootManifestRecord
}

type NamedRootSelection struct {
	Roots        []NamedRoot
	MissingNames [][]byte
}

type NamedRootUpdate struct {
	Applied  bool
	Conflict bool
	Current  *Tree
}

type SnapshotNamespace struct {
	Kind         string
	CustomPrefix []byte
}

type SnapshotRoot struct {
	ID              []byte
	Name            []byte
	Tree            Tree
	CreatedAtMillis *uint64
	UpdatedAtMillis *uint64
}

type SnapshotSelection struct {
	Snapshots  []SnapshotRoot
	MissingIDs [][]byte
}

type KeyProof struct {
	Root    []byte
	HasRoot bool
	Key     []byte
	Path    [][]byte
}

type KeyProofVerification struct {
	Valid    bool
	Exists   bool
	Absence  bool
	Root     []byte
	HasRoot  bool
	Key      []byte
	Value    []byte
	HasValue bool
}

type MultiKeyProof struct {
	Root    []byte
	HasRoot bool
	Keys    [][]byte
	Path    [][]byte
}

type MultiKeyProofVerification struct {
	Valid   bool
	Root    []byte
	HasRoot bool
	Results []KeyProofVerification
}

type RangeProof struct {
	Root    []byte
	HasRoot bool
	Start   []byte
	End     []byte
	HasEnd  bool
	Path    [][]byte
}

type RangeProofVerification struct {
	Valid   bool
	Root    []byte
	HasRoot bool
	Start   []byte
	End     []byte
	HasEnd  bool
	Entries []Entry
}

type RangePageProof struct {
	Root     []byte
	HasRoot  bool
	After    []byte
	HasAfter bool
	End      []byte
	HasEnd   bool
	Path     [][]byte
}

type RangePageProofVerification struct {
	Valid    bool
	Root     []byte
	HasRoot  bool
	After    []byte
	HasAfter bool
	End      []byte
	HasEnd   bool
	Entries  []Entry
}

type ProvedRangePage struct {
	Page  RangePage
	Proof RangePageProof
}

type DiffPageProof struct {
	Base              RangePageProof
	Other             RangePageProof
	LookaheadBase     KeyProof
	HasLookaheadBase  bool
	LookaheadOther    KeyProof
	HasLookaheadOther bool
	RequestedEnd      []byte
	HasRequestedEnd   bool
	Limit             uint64
}

type DiffPageProofVerification struct {
	Valid           bool
	BaseValid       bool
	OtherValid      bool
	LookaheadValid  bool
	BaseRoot        []byte
	HasBaseRoot     bool
	OtherRoot       []byte
	HasOtherRoot    bool
	After           []byte
	HasAfter        bool
	RequestedEnd    []byte
	HasRequestedEnd bool
	ProofEnd        []byte
	HasProofEnd     bool
	Limit           uint64
	Diffs           []Diff
	NextCursor      RangeCursor
	HasNextCursor   bool
}

type ProvedDiffPage struct {
	Page  DiffPage
	Proof DiffPageProof
}

type ProofBundleSummary struct {
	Version         uint64
	Kind            string
	Root            []byte
	HasRoot         bool
	OtherRoot       []byte
	HasOtherRoot    bool
	KeyCount        uint64
	PathNodeCount   uint64
	Start           []byte
	HasStart        bool
	End             []byte
	HasEnd          bool
	After           []byte
	HasAfter        bool
	RequestedEnd    []byte
	HasRequestedEnd bool
	Limit           uint64
	HasLimit        bool
	HasLookahead    bool
}

type ProofBundleVerification struct {
	Summary       ProofBundleSummary
	Valid         bool
	ExistsCount   uint64
	AbsenceCount  uint64
	EntryCount    uint64
	DiffCount     uint64
	NextCursor    RangeCursor
	HasNextCursor bool
}

type AuthenticatedProofEnvelope struct {
	Algorithm          string
	KeyID              []byte
	ProofBundle        []byte
	Context            []byte
	IssuedAtMillis     uint64
	HasIssuedAtMillis  bool
	ExpiresAtMillis    uint64
	HasExpiresAtMillis bool
	Nonce              []byte
	Signature          []byte
}

type AuthenticatedProofEnvelopeVerification struct {
	Valid              bool
	SignatureValid     bool
	TimeValid          bool
	NotYetValid        bool
	Expired            bool
	Algorithm          string
	KeyID              []byte
	ProofBundle        []byte
	Context            []byte
	IssuedAtMillis     uint64
	HasIssuedAtMillis  bool
	ExpiresAtMillis    uint64
	HasExpiresAtMillis bool
	Nonce              []byte
}

type AuthenticatedProofBundleVerification struct {
	Valid         bool
	Envelope      AuthenticatedProofEnvelopeVerification
	Proof         ProofBundleVerification
	HasProof      bool
	ProofError    string
	HasProofError bool
}

type RootManifest struct {
	raw []byte
}

type NamedRootManifest struct {
	Name     []byte
	Manifest RootManifest
}

type HostStoreResult struct {
	Value []byte
	Ok    bool
	Err   error
}

type HostStoreCasResult struct {
	Applied bool
	Current *RootManifest
	Err     error
}

type HostStore interface {
	Get(key []byte) HostStoreResult
	Put(key []byte, value []byte) error
	Delete(key []byte) error
	Batch(ops []Mutation) error
	BatchGetOrdered(keys [][]byte) ([]HostStoreResult, error)
	PrefersBatchReads() bool
	SupportsHints() bool
	GetHint(namespace []byte, key []byte) HostStoreResult
	PutHint(namespace []byte, key []byte, value []byte) error
	ListNodeCids() ([][]byte, error)
	GetRoot(name []byte) (*RootManifest, error)
	PutRoot(name []byte, manifest RootManifest) error
	DeleteRoot(name []byte) error
	CompareAndSwapRoot(name []byte, expected *RootManifest, replacement *RootManifest) HostStoreCasResult
	ListRoots() ([]NamedRootManifest, error)
}

type NamedRootRetention struct {
	Kind               string
	Names              [][]byte
	Prefix             []byte
	Count              *uint64
	MinUpdatedAtMillis *uint64
}

func RetainAllNamedRoots() (NamedRootRetention, error) {
	var status C.RustCallStatus
	out := C.uniffi_prolly_bindings_fn_func_retain_all_named_roots(&status)
	if err := statusError(&status); err != nil {
		return NamedRootRetention{}, err
	}
	defer freeRustBuffer(out)
	return decodeNamedRootRetention(copyRustBuffer(out))
}

func RetainExactNamedRoots(names [][]byte) (NamedRootRetention, error) {
	in, err := rustBufferFromBytes(encodeByteArraySequence(names))
	if err != nil {
		return NamedRootRetention{}, err
	}
	var status C.RustCallStatus
	out := C.uniffi_prolly_bindings_fn_func_retain_exact_named_roots(in, &status)
	if err := statusError(&status); err != nil {
		return NamedRootRetention{}, err
	}
	defer freeRustBuffer(out)
	return decodeNamedRootRetention(copyRustBuffer(out))
}

func RetainNamedRootPrefix(prefix []byte) (NamedRootRetention, error) {
	in, err := rustBufferFromBytes(encodeByteArray(prefix))
	if err != nil {
		return NamedRootRetention{}, err
	}
	var status C.RustCallStatus
	out := C.uniffi_prolly_bindings_fn_func_retain_named_root_prefix(in, &status)
	if err := statusError(&status); err != nil {
		return NamedRootRetention{}, err
	}
	defer freeRustBuffer(out)
	return decodeNamedRootRetention(copyRustBuffer(out))
}

func RetainNewestNamedRoots(prefix []byte, count uint64) (NamedRootRetention, error) {
	in, err := rustBufferFromBytes(encodeByteArray(prefix))
	if err != nil {
		return NamedRootRetention{}, err
	}
	var status C.RustCallStatus
	out := C.uniffi_prolly_bindings_fn_func_retain_newest_named_roots(in, C.uint64_t(count), &status)
	if err := statusError(&status); err != nil {
		return NamedRootRetention{}, err
	}
	defer freeRustBuffer(out)
	return decodeNamedRootRetention(copyRustBuffer(out))
}

func RetainNamedRootsUpdatedSince(prefix []byte, minUpdatedAtMillis uint64) (NamedRootRetention, error) {
	in, err := rustBufferFromBytes(encodeByteArray(prefix))
	if err != nil {
		return NamedRootRetention{}, err
	}
	var status C.RustCallStatus
	out := C.uniffi_prolly_bindings_fn_func_retain_named_roots_updated_since(
		in,
		C.uint64_t(minUpdatedAtMillis),
		&status,
	)
	if err := statusError(&status); err != nil {
		return NamedRootRetention{}, err
	}
	defer freeRustBuffer(out)
	return decodeNamedRootRetention(copyRustBuffer(out))
}

type CacheStats struct {
	CachedNodes uint64
	CachedBytes uint64
	PinnedNodes uint64
	PinnedBytes uint64
}

type Metrics struct {
	NodeCacheHits      uint64
	NodeCacheMisses    uint64
	NodeCacheEvictions uint64
	NodesRead          uint64
	BytesRead          uint64
	NodesWritten       uint64
	BytesWritten       uint64
	StoreGetCalls      uint64
	StoreBatchGetCalls uint64
	StoreBatchGetKeys  uint64
	StorePutCalls      uint64
	StoreBatchPutCalls uint64
	StoreBatchPutNodes uint64
}

type ChangedSpan struct {
	Start []byte
	End   []byte
}

type ChangedSpanHint struct {
	BaseRoot           []byte
	BaseRootPresent    bool
	ChangedRoot        []byte
	ChangedRootPresent bool
	Spans              []ChangedSpan
}

func ChangedSpanRange(start []byte, end []byte) (ChangedSpan, error) {
	startBuf, err := rustBufferFromBytes(encodeByteArray(start))
	if err != nil {
		return ChangedSpan{}, err
	}
	endBuf, err := rustBufferFromBytes(encodeOptionalByteArray(end))
	if err != nil {
		return ChangedSpan{}, err
	}
	var status C.RustCallStatus
	out := C.uniffi_prolly_bindings_fn_func_changed_span(startBuf, endBuf, &status)
	if err := statusError(&status); err != nil {
		return ChangedSpan{}, err
	}
	defer freeRustBuffer(out)
	return decodeChangedSpan(copyRustBuffer(out))
}

func ChangedSpanFromKey(key []byte) (ChangedSpan, error) {
	in, err := rustBufferFromBytes(encodeByteArray(key))
	if err != nil {
		return ChangedSpan{}, err
	}
	var status C.RustCallStatus
	out := C.uniffi_prolly_bindings_fn_func_changed_span_from_key(in, &status)
	if err := statusError(&status); err != nil {
		return ChangedSpan{}, err
	}
	defer freeRustBuffer(out)
	return decodeChangedSpan(copyRustBuffer(out))
}

func ChangedSpanForPrefix(prefix []byte) (ChangedSpan, error) {
	in, err := rustBufferFromBytes(encodeByteArray(prefix))
	if err != nil {
		return ChangedSpan{}, err
	}
	var status C.RustCallStatus
	out := C.uniffi_prolly_bindings_fn_func_changed_span_for_prefix(in, &status)
	if err := statusError(&status); err != nil {
		return ChangedSpan{}, err
	}
	defer freeRustBuffer(out)
	return decodeChangedSpan(copyRustBuffer(out))
}

type GcReachability struct {
	LiveCids      [][]byte
	LiveNodes     uint64
	LiveBytes     uint64
	LeafNodes     uint64
	InternalNodes uint64
}

type GcPlan struct {
	Reachability      GcReachability
	CandidateNodes    uint64
	ReclaimableCids   [][]byte
	ReclaimableNodes  uint64
	ReclaimableBytes  uint64
	MissingCandidates uint64
}

type GcSweep struct {
	Plan         GcPlan
	DeletedNodes uint64
	DeletedBytes uint64
}

type MissingNodePlan struct {
	RequiredCids  [][]byte
	RequiredNodes uint64
	RequiredBytes uint64
	MissingCids   [][]byte
	MissingNodes  uint64
	MissingBytes  uint64
}

type MissingNodeCopy struct {
	Plan        MissingNodePlan
	CopiedNodes uint64
	CopiedBytes uint64
}

type BlobRef struct {
	Cid []byte
	Len uint64
}

type LargeValueConfig struct {
	InlineThreshold uint64
}

type ParallelConfig struct {
	MaxThreads           uint64
	ParallelismThreshold uint64
}

type ValueRef struct {
	Kind  string
	Value []byte
	Blob  *BlobRef
}

type BlobGcReachability struct {
	LiveBlobs     []BlobRef
	LiveBlobCount uint64
	LiveBlobBytes uint64
	ScannedNodes  uint64
	ScannedValues uint64
}

type BlobGcPlan struct {
	Reachability         BlobGcReachability
	CandidateBlobs       uint64
	ReclaimableBlobs     []BlobRef
	ReclaimableBlobCount uint64
	ReclaimableBlobBytes uint64
	MissingCandidates    uint64
}

type BlobGcSweep struct {
	Plan             BlobGcPlan
	DeletedBlobs     uint64
	DeletedBlobBytes uint64
}

type CrdtConfig struct {
	Strategy     string
	DeletePolicy string
	raw          []byte
}

type TimestampedValue struct {
	Value     []byte
	Timestamp uint64
}

type TombstoneMetadata struct {
	Key   string
	Value []byte
}

type Tombstone struct {
	Actor           []byte
	TimestampMillis uint64
	CausalMetadata  []TombstoneMetadata
}

type Engine struct {
	handle          C.uint64_t
	closed          atomic.Bool
	hostStoreHandle uint64
}

type BlobStore struct {
	handle C.uint64_t
	closed atomic.Bool
}

type MergePolicyRegistry struct {
	handle C.uint64_t
	closed atomic.Bool

	mu              sync.Mutex
	resolverHandles []uint64
}

func NewConfig(options ConfigOptions) (Config, error) {
	return encodeConfigRecord(
		options.MinChunkSize,
		options.MaxChunkSize,
		options.ChunkingFactor,
		options.HashSeed,
		options.EncodingKind,
		optionalString(options.CustomEncoding),
		options.NodeCacheMaxNodes,
		options.NodeCacheMaxBytes,
	)
}

func EncodingRaw() Encoding {
	return Encoding{Kind: "raw"}
}

func EncodingCBOR() Encoding {
	return Encoding{Kind: "cbor"}
}

func EncodingJSON() Encoding {
	return Encoding{Kind: "json"}
}

func EncodingCustom(name string) Encoding {
	return Encoding{Kind: "custom", CustomName: &name}
}

func TreeConfig(
	minChunkSize uint64,
	maxChunkSize uint64,
	chunkingFactor uint32,
	hashSeed uint64,
	encoding Encoding,
	nodeCacheMaxNodes *uint64,
	nodeCacheMaxBytes *uint64,
) (Config, error) {
	if encoding.Kind == "custom" && encoding.CustomName == nil {
		return Config{}, errors.New("custom encoding requires custom name")
	}
	return encodeConfigRecord(
		minChunkSize,
		maxChunkSize,
		chunkingFactor,
		hashSeed,
		encoding.Kind,
		encoding.CustomName,
		nodeCacheMaxNodes,
		nodeCacheMaxBytes,
	)
}

func NewLargeValueConfig(inlineThreshold uint64) LargeValueConfig {
	return LargeValueConfig{InlineThreshold: inlineThreshold}
}

func NewParallelConfig(maxThreads uint64, parallelismThreshold uint64) ParallelConfig {
	return ParallelConfig{MaxThreads: maxThreads, ParallelismThreshold: parallelismThreshold}
}

func SequentialParallelConfig() ParallelConfig {
	return ParallelConfig{MaxThreads: 1, ParallelismThreshold: ^uint64(0)}
}

func encodeConfigRecord(
	minChunkSize uint64,
	maxChunkSize uint64,
	chunkingFactor uint32,
	hashSeed uint64,
	encodingKindName string,
	customEncodingName *string,
	nodeCacheMaxNodes *uint64,
	nodeCacheMaxBytes *uint64,
) (Config, error) {
	if encodingKindName == "" {
		encodingKindName = "raw"
	}
	encodingKind, err := encodeEncodingKind(encodingKindName)
	if err != nil {
		return Config{}, err
	}

	var out bytes.Buffer
	writeU64(&out, minChunkSize)
	writeU64(&out, maxChunkSize)
	writeU32(&out, chunkingFactor)
	writeU64(&out, hashSeed)
	writeI32(&out, encodingKind)
	encodeOptionalString(&out, customEncodingName)
	encodeOptionalU64(&out, nodeCacheMaxNodes)
	encodeOptionalU64(&out, nodeCacheMaxBytes)
	return Config{raw: out.Bytes()}, nil
}

func DefaultConfig() (Config, error) {
	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_func_default_config(&status)
	if err := statusError(&status); err != nil {
		return Config{}, err
	}
	defer freeRustBuffer(buf)
	return Config{raw: copyRustBuffer(buf)}, nil
}

func DefaultLargeValueConfig() (LargeValueConfig, error) {
	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_func_default_large_value_config(&status)
	if err := statusError(&status); err != nil {
		return LargeValueConfig{}, err
	}
	defer freeRustBuffer(buf)
	return decodeLargeValueConfig(copyRustBuffer(buf))
}

func DefaultParallelConfig() (ParallelConfig, error) {
	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_func_default_parallel_config(&status)
	if err := statusError(&status); err != nil {
		return ParallelConfig{}, err
	}
	defer freeRustBuffer(buf)
	return decodeParallelConfig(copyRustBuffer(buf))
}

func Memory(config Config) (*Engine, error) {
	buf, err := rustBufferFromBytes(config.raw)
	if err != nil {
		return nil, err
	}

	var status C.RustCallStatus
	handle := C.uniffi_prolly_bindings_fn_constructor_prollyengine_memory(buf, &status)
	if err := statusError(&status); err != nil {
		return nil, err
	}
	engine := &Engine{handle: handle}
	runtime.SetFinalizer(engine, (*Engine).Close)
	return engine, nil
}

var goHostStoreVtableOnce sync.Once

func registerGoHostStoreVtable() {
	goHostStoreVtableOnce.Do(func() {
		C.prolly_register_go_host_store_vtable()
	})
}

func CustomStore(store HostStore, config Config) (*Engine, error) {
	if store == nil {
		return nil, errors.New("nil host store")
	}
	registerGoHostStoreVtable()
	configBuf, err := rustBufferFromBytes(config.raw)
	if err != nil {
		return nil, err
	}
	storeHandle := registerGoHostStore(store)

	var status C.RustCallStatus
	handle := C.uniffi_prolly_bindings_fn_constructor_prollyengine_custom_store(C.uint64_t(storeHandle), configBuf, &status)
	if err := statusError(&status); err != nil {
		removeGoHostStore(storeHandle)
		return nil, err
	}
	engine := &Engine{handle: handle, hostStoreHandle: storeHandle}
	runtime.SetFinalizer(engine, (*Engine).Close)
	return engine, nil
}

func File(path string, config Config) (*Engine, error) {
	return engineFromPath(path, config, "file")
}

func OpenFile(path string) (*Engine, error) {
	config, err := DefaultConfig()
	if err != nil {
		return nil, err
	}
	return File(path, config)
}

func SQLite(path string, config Config) (*Engine, error) {
	return engineFromPath(path, config, "sqlite")
}

func OpenSQLite(path string) (*Engine, error) {
	config, err := DefaultConfig()
	if err != nil {
		return nil, err
	}
	return SQLite(path, config)
}

func SQLiteInMemory(config Config) (*Engine, error) {
	configBuf, err := rustBufferFromBytes(config.raw)
	if err != nil {
		return nil, err
	}

	var status C.RustCallStatus
	handle := C.uniffi_prolly_bindings_fn_constructor_prollyengine_sqlite_in_memory(configBuf, &status)
	if err := statusError(&status); err != nil {
		return nil, err
	}
	engine := &Engine{handle: handle}
	runtime.SetFinalizer(engine, (*Engine).Close)
	return engine, nil
}

func OpenSQLiteInMemory() (*Engine, error) {
	config, err := DefaultConfig()
	if err != nil {
		return nil, err
	}
	return SQLiteInMemory(config)
}

func engineFromPath(path string, config Config, kind string) (*Engine, error) {
	pathBuf, err := rustBufferFromBytes([]byte(path))
	if err != nil {
		return nil, err
	}
	configBuf, err := rustBufferFromBytes(config.raw)
	if err != nil {
		return nil, err
	}

	var status C.RustCallStatus
	var handle C.uint64_t
	switch kind {
	case "file":
		handle = C.uniffi_prolly_bindings_fn_constructor_prollyengine_file(pathBuf, configBuf, &status)
	case "sqlite":
		handle = C.uniffi_prolly_bindings_fn_constructor_prollyengine_sqlite(pathBuf, configBuf, &status)
	default:
		return nil, fmt.Errorf("unknown engine store kind %q", kind)
	}
	if err := statusError(&status); err != nil {
		return nil, err
	}
	engine := &Engine{handle: handle}
	runtime.SetFinalizer(engine, (*Engine).Close)
	return engine, nil
}

func OpenMemory() (*Engine, error) {
	config, err := DefaultConfig()
	if err != nil {
		return nil, err
	}
	return Memory(config)
}

func MemoryBlobStore() (*BlobStore, error) {
	var status C.RustCallStatus
	handle := C.uniffi_prolly_bindings_fn_constructor_prollyblobstore_memory(&status)
	if err := statusError(&status); err != nil {
		return nil, err
	}
	store := &BlobStore{handle: handle}
	runtime.SetFinalizer(store, (*BlobStore).Close)
	return store, nil
}

func OpenMemoryBlobStore() (*BlobStore, error) {
	return MemoryBlobStore()
}

func FileBlobStore(path string) (*BlobStore, error) {
	pathBuf, err := rustBufferFromBytes([]byte(path))
	if err != nil {
		return nil, err
	}

	var status C.RustCallStatus
	handle := C.uniffi_prolly_bindings_fn_constructor_prollyblobstore_file(pathBuf, &status)
	if err := statusError(&status); err != nil {
		return nil, err
	}
	store := &BlobStore{handle: handle}
	runtime.SetFinalizer(store, (*BlobStore).Close)
	return store, nil
}

func NewMergePolicyRegistry() (*MergePolicyRegistry, error) {
	var status C.RustCallStatus
	handle := C.uniffi_prolly_bindings_fn_constructor_mergepolicyregistry_new(&status)
	if err := statusError(&status); err != nil {
		return nil, err
	}
	policy := &MergePolicyRegistry{handle: handle}
	runtime.SetFinalizer(policy, (*MergePolicyRegistry).Close)
	return policy, nil
}

func (p *MergePolicyRegistry) Close() {
	if p == nil || p.closed.Swap(true) || p.handle == 0 {
		return
	}
	var status C.RustCallStatus
	C.uniffi_prolly_bindings_fn_free_mergepolicyregistry(p.handle, &status)
	_ = statusError(&status)
	p.handle = 0

	p.mu.Lock()
	for _, handle := range p.resolverHandles {
		removeGoResolver(handle)
	}
	p.resolverHandles = nil
	p.mu.Unlock()
}

func (p *MergePolicyRegistry) Len() (uint64, error) {
	handle, err := p.cloneHandle()
	if err != nil {
		return 0, err
	}
	var status C.RustCallStatus
	count := C.uniffi_prolly_bindings_fn_method_mergepolicyregistry_len(handle, &status)
	return uint64(count), statusError(&status)
}

func (p *MergePolicyRegistry) IsEmpty() (bool, error) {
	handle, err := p.cloneHandle()
	if err != nil {
		return false, err
	}
	var status C.RustCallStatus
	value := C.uniffi_prolly_bindings_fn_method_mergepolicyregistry_is_empty(handle, &status)
	return value != 0, statusError(&status)
}

func (p *MergePolicyRegistry) HasDefault() (bool, error) {
	handle, err := p.cloneHandle()
	if err != nil {
		return false, err
	}
	var status C.RustCallStatus
	value := C.uniffi_prolly_bindings_fn_method_mergepolicyregistry_has_default(handle, &status)
	return value != 0, statusError(&status)
}

func (p *MergePolicyRegistry) SetDefaultResolverName(name string) error {
	handle, err := p.cloneHandle()
	if err != nil {
		return err
	}
	nameBuf, err := rustBufferFromBytes([]byte(name))
	if err != nil {
		return err
	}
	var status C.RustCallStatus
	C.uniffi_prolly_bindings_fn_method_mergepolicyregistry_set_default_resolver_name(handle, nameBuf, &status)
	return statusError(&status)
}

func (p *MergePolicyRegistry) PushPrefixResolverName(prefix []byte, name string) error {
	handle, err := p.cloneHandle()
	if err != nil {
		return err
	}
	prefixBuf, err := rustBufferFromBytes(encodeByteArray(prefix))
	if err != nil {
		return err
	}
	nameBuf, err := rustBufferFromBytes([]byte(name))
	if err != nil {
		return err
	}
	var status C.RustCallStatus
	C.uniffi_prolly_bindings_fn_method_mergepolicyregistry_push_prefix_resolver_name(handle, prefixBuf, nameBuf, &status)
	return statusError(&status)
}

func (p *MergePolicyRegistry) PushExactResolverName(key []byte, name string) error {
	handle, err := p.cloneHandle()
	if err != nil {
		return err
	}
	keyBuf, err := rustBufferFromBytes(encodeByteArray(key))
	if err != nil {
		return err
	}
	nameBuf, err := rustBufferFromBytes([]byte(name))
	if err != nil {
		return err
	}
	var status C.RustCallStatus
	C.uniffi_prolly_bindings_fn_method_mergepolicyregistry_push_exact_resolver_name(handle, keyBuf, nameBuf, &status)
	return statusError(&status)
}

func (p *MergePolicyRegistry) SetDefaultResolver(resolver Resolver) error {
	if resolver == nil {
		return errors.New("nil resolver")
	}
	registerGoResolverVtable()
	resolverHandle := registerGoResolver(resolver)
	if err := p.setDefaultResolverHandle(resolverHandle); err != nil {
		removeGoResolver(resolverHandle)
		return err
	}
	p.retainResolverHandle(resolverHandle)
	return nil
}

func (p *MergePolicyRegistry) PushPrefixResolver(prefix []byte, resolver Resolver) error {
	if resolver == nil {
		return errors.New("nil resolver")
	}
	registerGoResolverVtable()
	resolverHandle := registerGoResolver(resolver)
	if err := p.pushPrefixResolverHandle(prefix, resolverHandle); err != nil {
		removeGoResolver(resolverHandle)
		return err
	}
	p.retainResolverHandle(resolverHandle)
	return nil
}

func (p *MergePolicyRegistry) PushExactResolver(key []byte, resolver Resolver) error {
	if resolver == nil {
		return errors.New("nil resolver")
	}
	registerGoResolverVtable()
	resolverHandle := registerGoResolver(resolver)
	if err := p.pushExactResolverHandle(key, resolverHandle); err != nil {
		removeGoResolver(resolverHandle)
		return err
	}
	p.retainResolverHandle(resolverHandle)
	return nil
}

func TreeFromRoot(root []byte, config Config) Tree {
	var out bytes.Buffer
	encodeOptionalByteArrayInto(&out, root)
	out.Write(config.raw)
	return Tree{raw: out.Bytes()}
}

func (t Tree) Root() ([]byte, bool, error) {
	decoder := byteDecoder{data: t.raw}
	root, ok, err := decoder.readOptionalByteArray()
	if err != nil {
		return nil, false, err
	}
	return root, ok, nil
}

func (e *Engine) Close() {
	if e == nil || e.closed.Swap(true) || e.handle == 0 {
		return
	}
	var status C.RustCallStatus
	C.uniffi_prolly_bindings_fn_free_prollyengine(e.handle, &status)
	_ = statusError(&status)
	e.handle = 0
	if e.hostStoreHandle != 0 {
		removeGoHostStore(e.hostStoreHandle)
		e.hostStoreHandle = 0
	}
}

func (s *BlobStore) Close() {
	if s == nil || s.closed.Swap(true) || s.handle == 0 {
		return
	}
	var status C.RustCallStatus
	C.uniffi_prolly_bindings_fn_free_prollyblobstore(s.handle, &status)
	_ = statusError(&status)
	s.handle = 0
}

func (s *BlobStore) PutBlob(data []byte) (BlobRef, error) {
	handle, err := s.cloneHandle()
	if err != nil {
		return BlobRef{}, err
	}
	dataBuf, err := rustBufferFromBytes(encodeByteArray(data))
	if err != nil {
		return BlobRef{}, err
	}

	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_prollyblobstore_put_blob(handle, dataBuf, &status)
	if err := statusError(&status); err != nil {
		return BlobRef{}, err
	}
	defer freeRustBuffer(buf)
	return decodeBlobRef(copyRustBuffer(buf))
}

func BlobRefValidateBytes(ref BlobRef, data []byte) error {
	refBuf, err := rustBufferFromBytes(encodeBlobRef(ref))
	if err != nil {
		return err
	}
	dataBuf, err := rustBufferFromBytes(encodeByteArray(data))
	if err != nil {
		return err
	}
	var status C.RustCallStatus
	C.uniffi_prolly_bindings_fn_func_blob_ref_validate_bytes(refBuf, dataBuf, &status)
	return statusError(&status)
}

func (s *BlobStore) GetBlob(ref BlobRef) ([]byte, bool, error) {
	handle, err := s.cloneHandle()
	if err != nil {
		return nil, false, err
	}
	refBuf, err := rustBufferFromBytes(encodeBlobRef(ref))
	if err != nil {
		return nil, false, err
	}

	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_prollyblobstore_get_blob(handle, refBuf, &status)
	if err := statusError(&status); err != nil {
		return nil, false, err
	}
	defer freeRustBuffer(buf)
	return decodeOptionalByteArray(copyRustBuffer(buf))
}

func (s *BlobStore) DeleteBlob(ref BlobRef) error {
	handle, err := s.cloneHandle()
	if err != nil {
		return err
	}
	refBuf, err := rustBufferFromBytes(encodeBlobRef(ref))
	if err != nil {
		return err
	}

	var status C.RustCallStatus
	C.uniffi_prolly_bindings_fn_method_prollyblobstore_delete_blob(handle, refBuf, &status)
	return statusError(&status)
}

func (s *BlobStore) ListBlobRefs() ([]BlobRef, error) {
	handle, err := s.cloneHandle()
	if err != nil {
		return nil, err
	}

	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_prollyblobstore_list_blob_refs(handle, &status)
	if err := statusError(&status); err != nil {
		return nil, err
	}
	defer freeRustBuffer(buf)
	return decodeBlobRefs(copyRustBuffer(buf))
}

func (s *BlobStore) BlobCount() (uint64, error) {
	handle, err := s.cloneHandle()
	if err != nil {
		return 0, err
	}
	var status C.RustCallStatus
	count := C.uniffi_prolly_bindings_fn_method_prollyblobstore_blob_count(handle, &status)
	return uint64(count), statusError(&status)
}

func (e *Engine) Create() (Tree, error) {
	handle, err := e.cloneHandle()
	if err != nil {
		return Tree{}, err
	}
	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_prollyengine_create(handle, &status)
	if err := statusError(&status); err != nil {
		return Tree{}, err
	}
	defer freeRustBuffer(buf)
	return Tree{raw: copyRustBuffer(buf)}, nil
}

func (e *Engine) Put(tree Tree, key []byte, value []byte) (Tree, error) {
	handle, err := e.cloneHandle()
	if err != nil {
		return Tree{}, err
	}
	treeBuf, err := rustBufferFromBytes(tree.raw)
	if err != nil {
		return Tree{}, err
	}
	keyBuf, err := rustBufferFromBytes(encodeByteArray(key))
	if err != nil {
		return Tree{}, err
	}
	valueBuf, err := rustBufferFromBytes(encodeByteArray(value))
	if err != nil {
		return Tree{}, err
	}

	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_prollyengine_put(handle, treeBuf, keyBuf, valueBuf, &status)
	if err := statusError(&status); err != nil {
		return Tree{}, err
	}
	defer freeRustBuffer(buf)
	return Tree{raw: copyRustBuffer(buf)}, nil
}

func (e *Engine) Delete(tree Tree, key []byte) (Tree, error) {
	handle, err := e.cloneHandle()
	if err != nil {
		return Tree{}, err
	}
	treeBuf, err := rustBufferFromBytes(tree.raw)
	if err != nil {
		return Tree{}, err
	}
	keyBuf, err := rustBufferFromBytes(encodeByteArray(key))
	if err != nil {
		return Tree{}, err
	}

	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_prollyengine_delete(handle, treeBuf, keyBuf, &status)
	if err := statusError(&status); err != nil {
		return Tree{}, err
	}
	defer freeRustBuffer(buf)
	return Tree{raw: copyRustBuffer(buf)}, nil
}

func (e *Engine) Get(tree Tree, key []byte) ([]byte, bool, error) {
	handle, err := e.cloneHandle()
	if err != nil {
		return nil, false, err
	}
	treeBuf, err := rustBufferFromBytes(tree.raw)
	if err != nil {
		return nil, false, err
	}
	keyBuf, err := rustBufferFromBytes(encodeByteArray(key))
	if err != nil {
		return nil, false, err
	}

	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_prollyengine_get(handle, treeBuf, keyBuf, &status)
	if err := statusError(&status); err != nil {
		return nil, false, err
	}
	defer freeRustBuffer(buf)
	return decodeOptionalByteArray(copyRustBuffer(buf))
}

func (e *Engine) FirstEntry(tree Tree) (*Entry, error) {
	handle, err := e.cloneHandle()
	if err != nil {
		return nil, err
	}
	treeBuf, err := rustBufferFromBytes(tree.raw)
	if err != nil {
		return nil, err
	}

	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_prollyengine_first_entry(handle, treeBuf, &status)
	if err := statusError(&status); err != nil {
		return nil, err
	}
	defer freeRustBuffer(buf)
	return decodeOptionalEntry(copyRustBuffer(buf))
}

func (e *Engine) LastEntry(tree Tree) (*Entry, error) {
	handle, err := e.cloneHandle()
	if err != nil {
		return nil, err
	}
	treeBuf, err := rustBufferFromBytes(tree.raw)
	if err != nil {
		return nil, err
	}

	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_prollyengine_last_entry(handle, treeBuf, &status)
	if err := statusError(&status); err != nil {
		return nil, err
	}
	defer freeRustBuffer(buf)
	return decodeOptionalEntry(copyRustBuffer(buf))
}

func (e *Engine) LowerBound(tree Tree, key []byte) (*Entry, error) {
	handle, err := e.cloneHandle()
	if err != nil {
		return nil, err
	}
	treeBuf, err := rustBufferFromBytes(tree.raw)
	if err != nil {
		return nil, err
	}
	keyBuf, err := rustBufferFromBytes(encodeByteArray(key))
	if err != nil {
		return nil, err
	}

	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_prollyengine_lower_bound(handle, treeBuf, keyBuf, &status)
	if err := statusError(&status); err != nil {
		return nil, err
	}
	defer freeRustBuffer(buf)
	return decodeOptionalEntry(copyRustBuffer(buf))
}

func (e *Engine) UpperBound(tree Tree, key []byte) (*Entry, error) {
	handle, err := e.cloneHandle()
	if err != nil {
		return nil, err
	}
	treeBuf, err := rustBufferFromBytes(tree.raw)
	if err != nil {
		return nil, err
	}
	keyBuf, err := rustBufferFromBytes(encodeByteArray(key))
	if err != nil {
		return nil, err
	}

	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_prollyengine_upper_bound(handle, treeBuf, keyBuf, &status)
	if err := statusError(&status); err != nil {
		return nil, err
	}
	defer freeRustBuffer(buf)
	return decodeOptionalEntry(copyRustBuffer(buf))
}

func (e *Engine) GetValueRef(tree Tree, key []byte) (*ValueRef, error) {
	handle, err := e.cloneHandle()
	if err != nil {
		return nil, err
	}
	treeBuf, err := rustBufferFromBytes(tree.raw)
	if err != nil {
		return nil, err
	}
	keyBuf, err := rustBufferFromBytes(encodeByteArray(key))
	if err != nil {
		return nil, err
	}

	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_prollyengine_get_value_ref(handle, treeBuf, keyBuf, &status)
	if err := statusError(&status); err != nil {
		return nil, err
	}
	defer freeRustBuffer(buf)
	return decodeOptionalValueRef(copyRustBuffer(buf))
}

func (e *Engine) GetLargeValue(blobStore *BlobStore, tree Tree, key []byte) ([]byte, bool, error) {
	handle, blobHandle, treeBuf, keyBuf, err := e.largeValueReadArgs(blobStore, tree, key)
	if err != nil {
		return nil, false, err
	}

	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_prollyengine_get_large_value(handle, blobHandle, treeBuf, keyBuf, &status)
	if err := statusError(&status); err != nil {
		return nil, false, err
	}
	defer freeRustBuffer(buf)
	return decodeOptionalByteArray(copyRustBuffer(buf))
}

func (e *Engine) PutLargeValue(blobStore *BlobStore, tree Tree, key []byte, value []byte, config LargeValueConfig) (Tree, error) {
	handle, blobHandle, treeBuf, keyBuf, err := e.largeValueReadArgs(blobStore, tree, key)
	if err != nil {
		return Tree{}, err
	}
	valueBuf, err := rustBufferFromBytes(encodeByteArray(value))
	if err != nil {
		return Tree{}, err
	}
	configBuf, err := rustBufferFromBytes(encodeLargeValueConfig(config))
	if err != nil {
		return Tree{}, err
	}

	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_prollyengine_put_large_value(handle, blobHandle, treeBuf, keyBuf, valueBuf, configBuf, &status)
	if err := statusError(&status); err != nil {
		return Tree{}, err
	}
	defer freeRustBuffer(buf)
	return Tree{raw: copyRustBuffer(buf)}, nil
}

func (e *Engine) GetMany(tree Tree, keys [][]byte) ([][]byte, []bool, error) {
	handle, err := e.cloneHandle()
	if err != nil {
		return nil, nil, err
	}
	treeBuf, err := rustBufferFromBytes(tree.raw)
	if err != nil {
		return nil, nil, err
	}
	keysBuf, err := rustBufferFromBytes(encodeByteArraySequence(keys))
	if err != nil {
		return nil, nil, err
	}

	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_prollyengine_get_many(handle, treeBuf, keysBuf, &status)
	if err := statusError(&status); err != nil {
		return nil, nil, err
	}
	defer freeRustBuffer(buf)
	return decodeOptionalByteArraySequence(copyRustBuffer(buf))
}

func (e *Engine) ProveKey(tree Tree, key []byte) (KeyProof, error) {
	handle, err := e.cloneHandle()
	if err != nil {
		return KeyProof{}, err
	}
	treeBuf, err := rustBufferFromBytes(tree.raw)
	if err != nil {
		return KeyProof{}, err
	}
	keyBuf, err := rustBufferFromBytes(encodeByteArray(key))
	if err != nil {
		return KeyProof{}, err
	}

	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_prollyengine_prove_key(handle, treeBuf, keyBuf, &status)
	if err := statusError(&status); err != nil {
		return KeyProof{}, err
	}
	defer freeRustBuffer(buf)
	return decodeKeyProof(copyRustBuffer(buf))
}

func (e *Engine) ProveKeys(tree Tree, keys [][]byte) (MultiKeyProof, error) {
	handle, err := e.cloneHandle()
	if err != nil {
		return MultiKeyProof{}, err
	}
	treeBuf, err := rustBufferFromBytes(tree.raw)
	if err != nil {
		return MultiKeyProof{}, err
	}
	keysBuf, err := rustBufferFromBytes(encodeByteArraySequence(keys))
	if err != nil {
		return MultiKeyProof{}, err
	}

	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_prollyengine_prove_keys(handle, treeBuf, keysBuf, &status)
	if err := statusError(&status); err != nil {
		return MultiKeyProof{}, err
	}
	defer freeRustBuffer(buf)
	return decodeMultiKeyProof(copyRustBuffer(buf))
}

func (e *Engine) ProveRange(tree Tree, start []byte, end []byte) (RangeProof, error) {
	handle, err := e.cloneHandle()
	if err != nil {
		return RangeProof{}, err
	}
	treeBuf, err := rustBufferFromBytes(tree.raw)
	if err != nil {
		return RangeProof{}, err
	}
	startBuf, err := rustBufferFromBytes(encodeByteArray(start))
	if err != nil {
		return RangeProof{}, err
	}
	endBuf, err := rustBufferFromBytes(encodeOptionalByteArray(end))
	if err != nil {
		return RangeProof{}, err
	}

	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_prollyengine_prove_range(handle, treeBuf, startBuf, endBuf, &status)
	if err := statusError(&status); err != nil {
		return RangeProof{}, err
	}
	defer freeRustBuffer(buf)
	return decodeRangeProof(copyRustBuffer(buf))
}

func (e *Engine) ProvePrefix(tree Tree, prefix []byte) (RangeProof, error) {
	handle, err := e.cloneHandle()
	if err != nil {
		return RangeProof{}, err
	}
	treeBuf, err := rustBufferFromBytes(tree.raw)
	if err != nil {
		return RangeProof{}, err
	}
	prefixBuf, err := rustBufferFromBytes(encodeByteArray(prefix))
	if err != nil {
		return RangeProof{}, err
	}

	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_prollyengine_prove_prefix(handle, treeBuf, prefixBuf, &status)
	if err := statusError(&status); err != nil {
		return RangeProof{}, err
	}
	defer freeRustBuffer(buf)
	return decodeRangeProof(copyRustBuffer(buf))
}

func (e *Engine) ProveRangePage(tree Tree, cursor *RangeCursor, end []byte, limit uint64) (ProvedRangePage, error) {
	handle, err := e.cloneHandle()
	if err != nil {
		return ProvedRangePage{}, err
	}
	treeBuf, err := rustBufferFromBytes(tree.raw)
	if err != nil {
		return ProvedRangePage{}, err
	}
	cursorBuf, err := rustBufferFromBytes(encodeOptionalRangeCursor(cursor))
	if err != nil {
		return ProvedRangePage{}, err
	}
	endBuf, err := rustBufferFromBytes(encodeOptionalByteArray(end))
	if err != nil {
		return ProvedRangePage{}, err
	}

	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_prollyengine_prove_range_page(handle, treeBuf, cursorBuf, endBuf, C.uint64_t(limit), &status)
	if err := statusError(&status); err != nil {
		return ProvedRangePage{}, err
	}
	defer freeRustBuffer(buf)
	return decodeProvedRangePage(copyRustBuffer(buf))
}

func (e *Engine) ProveDiffPage(base Tree, other Tree, cursor *RangeCursor, end []byte, limit uint64) (ProvedDiffPage, error) {
	handle, err := e.cloneHandle()
	if err != nil {
		return ProvedDiffPage{}, err
	}
	baseBuf, err := rustBufferFromBytes(base.raw)
	if err != nil {
		return ProvedDiffPage{}, err
	}
	otherBuf, err := rustBufferFromBytes(other.raw)
	if err != nil {
		return ProvedDiffPage{}, err
	}
	cursorBuf, err := rustBufferFromBytes(encodeOptionalRangeCursor(cursor))
	if err != nil {
		return ProvedDiffPage{}, err
	}
	endBuf, err := rustBufferFromBytes(encodeOptionalByteArray(end))
	if err != nil {
		return ProvedDiffPage{}, err
	}

	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_prollyengine_prove_diff_page(handle, baseBuf, otherBuf, cursorBuf, endBuf, C.uint64_t(limit), &status)
	if err := statusError(&status); err != nil {
		return ProvedDiffPage{}, err
	}
	defer freeRustBuffer(buf)
	return decodeProvedDiffPage(copyRustBuffer(buf))
}

func (e *Engine) Batch(tree Tree, mutations []Mutation) (Tree, error) {
	handle, err := e.cloneHandle()
	if err != nil {
		return Tree{}, err
	}
	treeBuf, err := rustBufferFromBytes(tree.raw)
	if err != nil {
		return Tree{}, err
	}
	mutationsBytes, err := encodeMutations(mutations)
	if err != nil {
		return Tree{}, err
	}
	mutationsBuf, err := rustBufferFromBytes(mutationsBytes)
	if err != nil {
		return Tree{}, err
	}

	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_prollyengine_batch(handle, treeBuf, mutationsBuf, &status)
	if err := statusError(&status); err != nil {
		return Tree{}, err
	}
	defer freeRustBuffer(buf)
	return Tree{raw: copyRustBuffer(buf)}, nil
}

func (e *Engine) BatchWithStats(tree Tree, mutations []Mutation) (BatchApplyResult, error) {
	handle, err := e.cloneHandle()
	if err != nil {
		return BatchApplyResult{}, err
	}
	treeBuf, err := rustBufferFromBytes(tree.raw)
	if err != nil {
		return BatchApplyResult{}, err
	}
	mutationsBytes, err := encodeMutations(mutations)
	if err != nil {
		return BatchApplyResult{}, err
	}
	mutationsBuf, err := rustBufferFromBytes(mutationsBytes)
	if err != nil {
		return BatchApplyResult{}, err
	}

	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_prollyengine_batch_with_stats(handle, treeBuf, mutationsBuf, &status)
	if err := statusError(&status); err != nil {
		return BatchApplyResult{}, err
	}
	defer freeRustBuffer(buf)
	return decodeBatchApplyResult(copyRustBuffer(buf))
}

func (e *Engine) BuildFromEntries(entries []Entry) (Tree, error) {
	handle, err := e.cloneHandle()
	if err != nil {
		return Tree{}, err
	}
	entriesBuf, err := rustBufferFromBytes(encodeEntries(entries))
	if err != nil {
		return Tree{}, err
	}

	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_prollyengine_build_from_entries(handle, entriesBuf, &status)
	if err := statusError(&status); err != nil {
		return Tree{}, err
	}
	defer freeRustBuffer(buf)
	return Tree{raw: copyRustBuffer(buf)}, nil
}

func (e *Engine) BuildFromSortedEntries(entries []Entry) (Tree, error) {
	handle, err := e.cloneHandle()
	if err != nil {
		return Tree{}, err
	}
	entriesBuf, err := rustBufferFromBytes(encodeEntries(entries))
	if err != nil {
		return Tree{}, err
	}

	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_prollyengine_build_from_sorted_entries(handle, entriesBuf, &status)
	if err := statusError(&status); err != nil {
		return Tree{}, err
	}
	defer freeRustBuffer(buf)
	return Tree{raw: copyRustBuffer(buf)}, nil
}

func (e *Engine) AppendBatch(tree Tree, mutations []Mutation) (Tree, error) {
	handle, err := e.cloneHandle()
	if err != nil {
		return Tree{}, err
	}
	treeBuf, err := rustBufferFromBytes(tree.raw)
	if err != nil {
		return Tree{}, err
	}
	mutationBytes, err := encodeMutations(mutations)
	if err != nil {
		return Tree{}, err
	}
	mutationsBuf, err := rustBufferFromBytes(mutationBytes)
	if err != nil {
		return Tree{}, err
	}

	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_prollyengine_append_batch(handle, treeBuf, mutationsBuf, &status)
	if err := statusError(&status); err != nil {
		return Tree{}, err
	}
	defer freeRustBuffer(buf)
	return Tree{raw: copyRustBuffer(buf)}, nil
}

func (e *Engine) AppendBatchWithStats(tree Tree, mutations []Mutation) (BatchApplyResult, error) {
	handle, err := e.cloneHandle()
	if err != nil {
		return BatchApplyResult{}, err
	}
	treeBuf, err := rustBufferFromBytes(tree.raw)
	if err != nil {
		return BatchApplyResult{}, err
	}
	mutationBytes, err := encodeMutations(mutations)
	if err != nil {
		return BatchApplyResult{}, err
	}
	mutationsBuf, err := rustBufferFromBytes(mutationBytes)
	if err != nil {
		return BatchApplyResult{}, err
	}

	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_prollyengine_append_batch_with_stats(handle, treeBuf, mutationsBuf, &status)
	if err := statusError(&status); err != nil {
		return BatchApplyResult{}, err
	}
	defer freeRustBuffer(buf)
	return decodeBatchApplyResult(copyRustBuffer(buf))
}

func (e *Engine) ParallelBatch(tree Tree, mutations []Mutation, config ParallelConfig) (Tree, error) {
	handle, err := e.cloneHandle()
	if err != nil {
		return Tree{}, err
	}
	treeBuf, err := rustBufferFromBytes(tree.raw)
	if err != nil {
		return Tree{}, err
	}
	mutationBytes, err := encodeMutations(mutations)
	if err != nil {
		return Tree{}, err
	}
	mutationsBuf, err := rustBufferFromBytes(mutationBytes)
	if err != nil {
		return Tree{}, err
	}
	configBuf, err := rustBufferFromBytes(encodeParallelConfig(config))
	if err != nil {
		return Tree{}, err
	}

	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_prollyengine_parallel_batch(handle, treeBuf, mutationsBuf, configBuf, &status)
	if err := statusError(&status); err != nil {
		return Tree{}, err
	}
	defer freeRustBuffer(buf)
	return Tree{raw: copyRustBuffer(buf)}, nil
}

func (e *Engine) ParallelBatchWithStats(tree Tree, mutations []Mutation, config ParallelConfig) (BatchApplyResult, error) {
	handle, err := e.cloneHandle()
	if err != nil {
		return BatchApplyResult{}, err
	}
	treeBuf, err := rustBufferFromBytes(tree.raw)
	if err != nil {
		return BatchApplyResult{}, err
	}
	mutationBytes, err := encodeMutations(mutations)
	if err != nil {
		return BatchApplyResult{}, err
	}
	mutationsBuf, err := rustBufferFromBytes(mutationBytes)
	if err != nil {
		return BatchApplyResult{}, err
	}
	configBuf, err := rustBufferFromBytes(encodeParallelConfig(config))
	if err != nil {
		return BatchApplyResult{}, err
	}

	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_prollyengine_parallel_batch_with_stats(handle, treeBuf, mutationsBuf, configBuf, &status)
	if err := statusError(&status); err != nil {
		return BatchApplyResult{}, err
	}
	defer freeRustBuffer(buf)
	return decodeBatchApplyResult(copyRustBuffer(buf))
}

func (e *Engine) Range(tree Tree, start []byte, end []byte) ([]Entry, error) {
	handle, err := e.cloneHandle()
	if err != nil {
		return nil, err
	}
	treeBuf, err := rustBufferFromBytes(tree.raw)
	if err != nil {
		return nil, err
	}
	startBuf, err := rustBufferFromBytes(encodeByteArray(start))
	if err != nil {
		return nil, err
	}
	endBuf, err := rustBufferFromBytes(encodeOptionalByteArray(end))
	if err != nil {
		return nil, err
	}

	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_prollyengine_range(handle, treeBuf, startBuf, endBuf, &status)
	if err := statusError(&status); err != nil {
		return nil, err
	}
	defer freeRustBuffer(buf)
	return decodeEntries(copyRustBuffer(buf))
}

func (e *Engine) Prefix(tree Tree, prefix []byte) ([]Entry, error) {
	handle, err := e.cloneHandle()
	if err != nil {
		return nil, err
	}
	treeBuf, err := rustBufferFromBytes(tree.raw)
	if err != nil {
		return nil, err
	}
	prefixBuf, err := rustBufferFromBytes(encodeByteArray(prefix))
	if err != nil {
		return nil, err
	}

	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_prollyengine_prefix(handle, treeBuf, prefixBuf, &status)
	if err := statusError(&status); err != nil {
		return nil, err
	}
	defer freeRustBuffer(buf)
	return decodeEntries(copyRustBuffer(buf))
}

func (e *Engine) PrefixPage(tree Tree, prefix []byte, cursor *RangeCursor, limit uint64) (RangePage, error) {
	handle, err := e.cloneHandle()
	if err != nil {
		return RangePage{}, err
	}
	treeBuf, err := rustBufferFromBytes(tree.raw)
	if err != nil {
		return RangePage{}, err
	}
	prefixBuf, err := rustBufferFromBytes(encodeByteArray(prefix))
	if err != nil {
		return RangePage{}, err
	}
	cursorBuf, err := rustBufferFromBytes(encodeOptionalRangeCursor(cursor))
	if err != nil {
		return RangePage{}, err
	}

	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_prollyengine_prefix_page(handle, treeBuf, prefixBuf, cursorBuf, C.uint64_t(limit), &status)
	if err := statusError(&status); err != nil {
		return RangePage{}, err
	}
	defer freeRustBuffer(buf)
	return decodeRangePage(copyRustBuffer(buf))
}

func (e *Engine) PrefixReversePage(tree Tree, prefix []byte, cursor *ReverseCursor, limit uint64) (ReversePage, error) {
	handle, err := e.cloneHandle()
	if err != nil {
		return ReversePage{}, err
	}
	treeBuf, err := rustBufferFromBytes(tree.raw)
	if err != nil {
		return ReversePage{}, err
	}
	prefixBuf, err := rustBufferFromBytes(encodeByteArray(prefix))
	if err != nil {
		return ReversePage{}, err
	}
	cursorBuf, err := rustBufferFromBytes(encodeOptionalReverseCursor(cursor))
	if err != nil {
		return ReversePage{}, err
	}

	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_prollyengine_prefix_reverse_page(handle, treeBuf, prefixBuf, cursorBuf, C.uint64_t(limit), &status)
	if err := statusError(&status); err != nil {
		return ReversePage{}, err
	}
	defer freeRustBuffer(buf)
	return decodeReversePage(copyRustBuffer(buf))
}

func (e *Engine) RangeAfter(tree Tree, afterKey []byte, end []byte) ([]Entry, error) {
	handle, err := e.cloneHandle()
	if err != nil {
		return nil, err
	}
	treeBuf, err := rustBufferFromBytes(tree.raw)
	if err != nil {
		return nil, err
	}
	afterKeyBuf, err := rustBufferFromBytes(encodeByteArray(afterKey))
	if err != nil {
		return nil, err
	}
	endBuf, err := rustBufferFromBytes(encodeOptionalByteArray(end))
	if err != nil {
		return nil, err
	}

	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_prollyengine_range_after(handle, treeBuf, afterKeyBuf, endBuf, &status)
	if err := statusError(&status); err != nil {
		return nil, err
	}
	defer freeRustBuffer(buf)
	return decodeEntries(copyRustBuffer(buf))
}

func (e *Engine) RangeFromCursor(tree Tree, cursor *RangeCursor, end []byte) ([]Entry, error) {
	handle, err := e.cloneHandle()
	if err != nil {
		return nil, err
	}
	treeBuf, err := rustBufferFromBytes(tree.raw)
	if err != nil {
		return nil, err
	}
	cursorBuf, err := rustBufferFromBytes(encodeOptionalRangeCursor(cursor))
	if err != nil {
		return nil, err
	}
	endBuf, err := rustBufferFromBytes(encodeOptionalByteArray(end))
	if err != nil {
		return nil, err
	}

	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_prollyengine_range_from_cursor(handle, treeBuf, cursorBuf, endBuf, &status)
	if err := statusError(&status); err != nil {
		return nil, err
	}
	defer freeRustBuffer(buf)
	return decodeEntries(copyRustBuffer(buf))
}

func (e *Engine) RangePage(tree Tree, cursor *RangeCursor, end []byte, limit uint64) (RangePage, error) {
	handle, err := e.cloneHandle()
	if err != nil {
		return RangePage{}, err
	}
	treeBuf, err := rustBufferFromBytes(tree.raw)
	if err != nil {
		return RangePage{}, err
	}
	cursorBuf, err := rustBufferFromBytes(encodeOptionalRangeCursor(cursor))
	if err != nil {
		return RangePage{}, err
	}
	endBuf, err := rustBufferFromBytes(encodeOptionalByteArray(end))
	if err != nil {
		return RangePage{}, err
	}

	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_prollyengine_range_page(handle, treeBuf, cursorBuf, endBuf, C.uint64_t(limit), &status)
	if err := statusError(&status); err != nil {
		return RangePage{}, err
	}
	defer freeRustBuffer(buf)
	return decodeRangePage(copyRustBuffer(buf))
}

func (e *Engine) ReversePage(tree Tree, cursor *ReverseCursor, start []byte, limit uint64) (ReversePage, error) {
	handle, err := e.cloneHandle()
	if err != nil {
		return ReversePage{}, err
	}
	treeBuf, err := rustBufferFromBytes(tree.raw)
	if err != nil {
		return ReversePage{}, err
	}
	cursorBuf, err := rustBufferFromBytes(encodeOptionalReverseCursor(cursor))
	if err != nil {
		return ReversePage{}, err
	}
	startBuf, err := rustBufferFromBytes(encodeByteArray(start))
	if err != nil {
		return ReversePage{}, err
	}

	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_prollyengine_reverse_page(handle, treeBuf, cursorBuf, startBuf, C.uint64_t(limit), &status)
	if err := statusError(&status); err != nil {
		return ReversePage{}, err
	}
	defer freeRustBuffer(buf)
	return decodeReversePage(copyRustBuffer(buf))
}

func (e *Engine) CursorWindow(tree Tree, key []byte, end []byte, limit uint64) (CursorWindow, error) {
	handle, err := e.cloneHandle()
	if err != nil {
		return CursorWindow{}, err
	}
	treeBuf, err := rustBufferFromBytes(tree.raw)
	if err != nil {
		return CursorWindow{}, err
	}
	keyBuf, err := rustBufferFromBytes(encodeByteArray(key))
	if err != nil {
		return CursorWindow{}, err
	}
	endBuf, err := rustBufferFromBytes(encodeOptionalByteArray(end))
	if err != nil {
		return CursorWindow{}, err
	}

	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_prollyengine_cursor_window(handle, treeBuf, keyBuf, endBuf, C.uint64_t(limit), &status)
	if err := statusError(&status); err != nil {
		return CursorWindow{}, err
	}
	defer freeRustBuffer(buf)
	return decodeCursorWindow(copyRustBuffer(buf))
}

func (e *Engine) Diff(base Tree, other Tree) ([]Diff, error) {
	handle, err := e.cloneHandle()
	if err != nil {
		return nil, err
	}
	baseBuf, err := rustBufferFromBytes(base.raw)
	if err != nil {
		return nil, err
	}
	otherBuf, err := rustBufferFromBytes(other.raw)
	if err != nil {
		return nil, err
	}

	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_prollyengine_diff(handle, baseBuf, otherBuf, &status)
	if err := statusError(&status); err != nil {
		return nil, err
	}
	defer freeRustBuffer(buf)
	return decodeDiffs(copyRustBuffer(buf))
}

func (e *Engine) RangeDiff(base Tree, other Tree, start []byte, end []byte) ([]Diff, error) {
	handle, err := e.cloneHandle()
	if err != nil {
		return nil, err
	}
	baseBuf, err := rustBufferFromBytes(base.raw)
	if err != nil {
		return nil, err
	}
	otherBuf, err := rustBufferFromBytes(other.raw)
	if err != nil {
		return nil, err
	}
	startBuf, err := rustBufferFromBytes(encodeByteArray(start))
	if err != nil {
		return nil, err
	}
	endBuf, err := rustBufferFromBytes(encodeOptionalByteArray(end))
	if err != nil {
		return nil, err
	}

	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_prollyengine_range_diff(handle, baseBuf, otherBuf, startBuf, endBuf, &status)
	if err := statusError(&status); err != nil {
		return nil, err
	}
	defer freeRustBuffer(buf)
	return decodeDiffs(copyRustBuffer(buf))
}

func (e *Engine) DiffFromCursor(base Tree, other Tree, cursor *RangeCursor, end []byte) ([]Diff, error) {
	handle, err := e.cloneHandle()
	if err != nil {
		return nil, err
	}
	baseBuf, err := rustBufferFromBytes(base.raw)
	if err != nil {
		return nil, err
	}
	otherBuf, err := rustBufferFromBytes(other.raw)
	if err != nil {
		return nil, err
	}
	cursorBuf, err := rustBufferFromBytes(encodeOptionalRangeCursor(cursor))
	if err != nil {
		return nil, err
	}
	endBuf, err := rustBufferFromBytes(encodeOptionalByteArray(end))
	if err != nil {
		return nil, err
	}

	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_prollyengine_diff_from_cursor(handle, baseBuf, otherBuf, cursorBuf, endBuf, &status)
	if err := statusError(&status); err != nil {
		return nil, err
	}
	defer freeRustBuffer(buf)
	return decodeDiffs(copyRustBuffer(buf))
}

func (e *Engine) DiffPage(base Tree, other Tree, cursor *RangeCursor, end []byte, limit uint64) (DiffPage, error) {
	handle, err := e.cloneHandle()
	if err != nil {
		return DiffPage{}, err
	}
	baseBuf, err := rustBufferFromBytes(base.raw)
	if err != nil {
		return DiffPage{}, err
	}
	otherBuf, err := rustBufferFromBytes(other.raw)
	if err != nil {
		return DiffPage{}, err
	}
	cursorBuf, err := rustBufferFromBytes(encodeOptionalRangeCursor(cursor))
	if err != nil {
		return DiffPage{}, err
	}
	endBuf, err := rustBufferFromBytes(encodeOptionalByteArray(end))
	if err != nil {
		return DiffPage{}, err
	}

	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_prollyengine_diff_page(handle, baseBuf, otherBuf, cursorBuf, endBuf, C.uint64_t(limit), &status)
	if err := statusError(&status); err != nil {
		return DiffPage{}, err
	}
	defer freeRustBuffer(buf)
	return decodeDiffPage(copyRustBuffer(buf))
}

func (e *Engine) ConflictPage(base Tree, left Tree, right Tree, cursor *RangeCursor, limit uint64) (ConflictPage, error) {
	handle, err := e.cloneHandle()
	if err != nil {
		return ConflictPage{}, err
	}
	baseBuf, err := rustBufferFromBytes(base.raw)
	if err != nil {
		return ConflictPage{}, err
	}
	leftBuf, err := rustBufferFromBytes(left.raw)
	if err != nil {
		return ConflictPage{}, err
	}
	rightBuf, err := rustBufferFromBytes(right.raw)
	if err != nil {
		return ConflictPage{}, err
	}
	cursorBuf, err := rustBufferFromBytes(encodeOptionalRangeCursor(cursor))
	if err != nil {
		return ConflictPage{}, err
	}

	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_prollyengine_conflict_page(handle, baseBuf, leftBuf, rightBuf, cursorBuf, C.uint64_t(limit), &status)
	if err := statusError(&status); err != nil {
		return ConflictPage{}, err
	}
	defer freeRustBuffer(buf)
	return decodeConflictPage(copyRustBuffer(buf))
}

func (e *Engine) Merge(base Tree, left Tree, right Tree, resolver string) (Tree, error) {
	handle, err := e.cloneHandle()
	if err != nil {
		return Tree{}, err
	}
	baseBuf, leftBuf, rightBuf, resolverBuf, err := mergeBuffers(base, left, right, resolver)
	if err != nil {
		return Tree{}, err
	}

	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_prollyengine_merge(handle, baseBuf, leftBuf, rightBuf, resolverBuf, &status)
	if err := statusError(&status); err != nil {
		return Tree{}, err
	}
	defer freeRustBuffer(buf)
	return Tree{raw: copyRustBuffer(buf)}, nil
}

var goResolverVtableOnce sync.Once

func registerGoResolverVtable() {
	goResolverVtableOnce.Do(func() {
		C.prolly_register_go_resolver_vtable()
	})
}

var goCrdtResolverVtableOnce sync.Once

func registerGoCrdtResolverVtable() {
	goCrdtResolverVtableOnce.Do(func() {
		C.prolly_register_go_crdt_resolver_vtable()
	})
}

func (e *Engine) MergeWithResolver(base Tree, left Tree, right Tree, resolver Resolver) (Tree, error) {
	if resolver == nil {
		return Tree{}, errors.New("nil resolver")
	}
	registerGoResolverVtable()
	resolverHandle := registerGoResolver(resolver)
	defer removeGoResolver(resolverHandle)
	handle, err := e.cloneHandle()
	if err != nil {
		return Tree{}, err
	}
	baseBuf, leftBuf, rightBuf, err := mergeTreeBuffers(base, left, right)
	if err != nil {
		return Tree{}, err
	}

	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_prollyengine_merge_with_resolver(handle, baseBuf, leftBuf, rightBuf, C.uint64_t(resolverHandle), &status)
	if err := statusError(&status); err != nil {
		return Tree{}, err
	}
	defer freeRustBuffer(buf)
	return Tree{raw: copyRustBuffer(buf)}, nil
}

func (e *Engine) MergeWithPolicy(base Tree, left Tree, right Tree, policy *MergePolicyRegistry) (Tree, error) {
	handle, err := e.cloneHandle()
	if err != nil {
		return Tree{}, err
	}
	policyHandle, err := policy.cloneHandle()
	if err != nil {
		return Tree{}, err
	}
	baseBuf, leftBuf, rightBuf, err := mergeTreeBuffers(base, left, right)
	if err != nil {
		return Tree{}, err
	}

	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_prollyengine_merge_with_policy(handle, baseBuf, leftBuf, rightBuf, policyHandle, &status)
	if err := statusError(&status); err != nil {
		return Tree{}, err
	}
	defer freeRustBuffer(buf)
	return Tree{raw: copyRustBuffer(buf)}, nil
}

func (e *Engine) MergeRange(base Tree, left Tree, right Tree, start []byte, end []byte, resolver string) (Tree, error) {
	handle, err := e.cloneHandle()
	if err != nil {
		return Tree{}, err
	}
	baseBuf, leftBuf, rightBuf, startBuf, endBuf, resolverBuf, err := mergeRangeBuffers(base, left, right, start, end, resolver)
	if err != nil {
		return Tree{}, err
	}

	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_prollyengine_merge_range(handle, baseBuf, leftBuf, rightBuf, startBuf, endBuf, resolverBuf, &status)
	if err := statusError(&status); err != nil {
		return Tree{}, err
	}
	defer freeRustBuffer(buf)
	return Tree{raw: copyRustBuffer(buf)}, nil
}

func (e *Engine) MergeRangeWithResolver(base Tree, left Tree, right Tree, start []byte, end []byte, resolver Resolver) (Tree, error) {
	if resolver == nil {
		return Tree{}, errors.New("nil resolver")
	}
	registerGoResolverVtable()
	resolverHandle := registerGoResolver(resolver)
	defer removeGoResolver(resolverHandle)
	handle, err := e.cloneHandle()
	if err != nil {
		return Tree{}, err
	}
	baseBuf, leftBuf, rightBuf, startBuf, endBuf, err := mergeRangeTreeBuffers(base, left, right, start, end)
	if err != nil {
		return Tree{}, err
	}

	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_prollyengine_merge_range_with_resolver(handle, baseBuf, leftBuf, rightBuf, startBuf, endBuf, C.uint64_t(resolverHandle), &status)
	if err := statusError(&status); err != nil {
		return Tree{}, err
	}
	defer freeRustBuffer(buf)
	return Tree{raw: copyRustBuffer(buf)}, nil
}

func (e *Engine) MergeRangeWithPolicy(base Tree, left Tree, right Tree, start []byte, end []byte, policy *MergePolicyRegistry) (Tree, error) {
	handle, err := e.cloneHandle()
	if err != nil {
		return Tree{}, err
	}
	policyHandle, err := policy.cloneHandle()
	if err != nil {
		return Tree{}, err
	}
	baseBuf, leftBuf, rightBuf, startBuf, endBuf, err := mergeRangeTreeBuffers(base, left, right, start, end)
	if err != nil {
		return Tree{}, err
	}

	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_prollyengine_merge_range_with_policy(handle, baseBuf, leftBuf, rightBuf, startBuf, endBuf, policyHandle, &status)
	if err := statusError(&status); err != nil {
		return Tree{}, err
	}
	defer freeRustBuffer(buf)
	return Tree{raw: copyRustBuffer(buf)}, nil
}

func (e *Engine) MergePrefix(base Tree, left Tree, right Tree, prefix []byte, resolver string) (Tree, error) {
	handle, err := e.cloneHandle()
	if err != nil {
		return Tree{}, err
	}
	baseBuf, leftBuf, rightBuf, prefixBuf, resolverBuf, err := mergePrefixBuffers(base, left, right, prefix, resolver)
	if err != nil {
		return Tree{}, err
	}

	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_prollyengine_merge_prefix(handle, baseBuf, leftBuf, rightBuf, prefixBuf, resolverBuf, &status)
	if err := statusError(&status); err != nil {
		return Tree{}, err
	}
	defer freeRustBuffer(buf)
	return Tree{raw: copyRustBuffer(buf)}, nil
}

func (e *Engine) MergePrefixWithResolver(base Tree, left Tree, right Tree, prefix []byte, resolver Resolver) (Tree, error) {
	if resolver == nil {
		return Tree{}, errors.New("nil resolver")
	}
	registerGoResolverVtable()
	resolverHandle := registerGoResolver(resolver)
	defer removeGoResolver(resolverHandle)
	handle, err := e.cloneHandle()
	if err != nil {
		return Tree{}, err
	}
	baseBuf, leftBuf, rightBuf, prefixBuf, err := mergePrefixTreeBuffers(base, left, right, prefix)
	if err != nil {
		return Tree{}, err
	}

	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_prollyengine_merge_prefix_with_resolver(handle, baseBuf, leftBuf, rightBuf, prefixBuf, C.uint64_t(resolverHandle), &status)
	if err := statusError(&status); err != nil {
		return Tree{}, err
	}
	defer freeRustBuffer(buf)
	return Tree{raw: copyRustBuffer(buf)}, nil
}

func (e *Engine) MergePrefixWithPolicy(base Tree, left Tree, right Tree, prefix []byte, policy *MergePolicyRegistry) (Tree, error) {
	handle, err := e.cloneHandle()
	if err != nil {
		return Tree{}, err
	}
	policyHandle, err := policy.cloneHandle()
	if err != nil {
		return Tree{}, err
	}
	baseBuf, leftBuf, rightBuf, prefixBuf, err := mergePrefixTreeBuffers(base, left, right, prefix)
	if err != nil {
		return Tree{}, err
	}

	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_prollyengine_merge_prefix_with_policy(handle, baseBuf, leftBuf, rightBuf, prefixBuf, policyHandle, &status)
	if err := statusError(&status); err != nil {
		return Tree{}, err
	}
	defer freeRustBuffer(buf)
	return Tree{raw: copyRustBuffer(buf)}, nil
}

func (e *Engine) MergeExplain(base Tree, left Tree, right Tree, resolver string) (MergeExplanation, error) {
	handle, err := e.cloneHandle()
	if err != nil {
		return MergeExplanation{}, err
	}
	baseBuf, leftBuf, rightBuf, resolverBuf, err := mergeBuffers(base, left, right, resolver)
	if err != nil {
		return MergeExplanation{}, err
	}

	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_prollyengine_merge_explain(handle, baseBuf, leftBuf, rightBuf, resolverBuf, &status)
	if err := statusError(&status); err != nil {
		return MergeExplanation{}, err
	}
	defer freeRustBuffer(buf)
	return decodeMergeExplanation(copyRustBuffer(buf))
}

func (e *Engine) MergeExplainWithResolver(base Tree, left Tree, right Tree, resolver Resolver) (MergeExplanation, error) {
	if resolver == nil {
		return MergeExplanation{}, errors.New("nil resolver")
	}
	registerGoResolverVtable()
	resolverHandle := registerGoResolver(resolver)
	defer removeGoResolver(resolverHandle)
	handle, err := e.cloneHandle()
	if err != nil {
		return MergeExplanation{}, err
	}
	baseBuf, leftBuf, rightBuf, err := mergeTreeBuffers(base, left, right)
	if err != nil {
		return MergeExplanation{}, err
	}

	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_prollyengine_merge_explain_with_resolver(handle, baseBuf, leftBuf, rightBuf, C.uint64_t(resolverHandle), &status)
	if err := statusError(&status); err != nil {
		return MergeExplanation{}, err
	}
	defer freeRustBuffer(buf)
	return decodeMergeExplanation(copyRustBuffer(buf))
}

func (e *Engine) MergeExplainWithPolicy(base Tree, left Tree, right Tree, policy *MergePolicyRegistry) (MergeExplanation, error) {
	handle, err := e.cloneHandle()
	if err != nil {
		return MergeExplanation{}, err
	}
	policyHandle, err := policy.cloneHandle()
	if err != nil {
		return MergeExplanation{}, err
	}
	baseBuf, leftBuf, rightBuf, err := mergeTreeBuffers(base, left, right)
	if err != nil {
		return MergeExplanation{}, err
	}

	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_prollyengine_merge_explain_with_policy(handle, baseBuf, leftBuf, rightBuf, policyHandle, &status)
	if err := statusError(&status); err != nil {
		return MergeExplanation{}, err
	}
	defer freeRustBuffer(buf)
	return decodeMergeExplanation(copyRustBuffer(buf))
}

func (e *Engine) CrdtMerge(base Tree, left Tree, right Tree, config CrdtConfig) (Tree, error) {
	handle, err := e.cloneHandle()
	if err != nil {
		return Tree{}, err
	}
	baseBuf, err := rustBufferFromBytes(base.raw)
	if err != nil {
		return Tree{}, err
	}
	leftBuf, err := rustBufferFromBytes(left.raw)
	if err != nil {
		return Tree{}, err
	}
	rightBuf, err := rustBufferFromBytes(right.raw)
	if err != nil {
		return Tree{}, err
	}
	configBuf, err := rustBufferFromBytes(config.raw)
	if err != nil {
		return Tree{}, err
	}

	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_prollyengine_crdt_merge(handle, baseBuf, leftBuf, rightBuf, configBuf, &status)
	if err := statusError(&status); err != nil {
		return Tree{}, err
	}
	defer freeRustBuffer(buf)
	return Tree{raw: copyRustBuffer(buf)}, nil
}

func (e *Engine) CrdtMergeWithResolver(base Tree, left Tree, right Tree, deletePolicy string, resolver CrdtResolver) (Tree, error) {
	if resolver == nil {
		return Tree{}, errors.New("nil CRDT resolver")
	}
	registerGoCrdtResolverVtable()
	resolverHandle := registerGoCrdtResolver(resolver)
	defer removeGoCrdtResolver(resolverHandle)
	handle, err := e.cloneHandle()
	if err != nil {
		return Tree{}, err
	}
	baseBuf, leftBuf, rightBuf, err := mergeTreeBuffers(base, left, right)
	if err != nil {
		return Tree{}, err
	}
	deletePolicyBytes, err := encodeCrdtDeletePolicy(deletePolicy)
	if err != nil {
		return Tree{}, err
	}
	deletePolicyBuf, err := rustBufferFromBytes(deletePolicyBytes)
	if err != nil {
		return Tree{}, err
	}

	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_prollyengine_crdt_merge_with_resolver(handle, baseBuf, leftBuf, rightBuf, deletePolicyBuf, C.uint64_t(resolverHandle), &status)
	if err := statusError(&status); err != nil {
		return Tree{}, err
	}
	defer freeRustBuffer(buf)
	return Tree{raw: copyRustBuffer(buf)}, nil
}

func (e *Engine) PublishNamedRoot(name []byte, tree Tree) error {
	handle, err := e.cloneHandle()
	if err != nil {
		return err
	}
	nameBuf, err := rustBufferFromBytes(encodeByteArray(name))
	if err != nil {
		return err
	}
	treeBuf, err := rustBufferFromBytes(tree.raw)
	if err != nil {
		return err
	}

	var status C.RustCallStatus
	C.uniffi_prolly_bindings_fn_method_prollyengine_publish_named_root(handle, nameBuf, treeBuf, &status)
	return statusError(&status)
}

func (e *Engine) PublishNamedRootAtMillis(name []byte, tree Tree, timestampMillis uint64) error {
	handle, err := e.cloneHandle()
	if err != nil {
		return err
	}
	nameBuf, err := rustBufferFromBytes(encodeByteArray(name))
	if err != nil {
		return err
	}
	treeBuf, err := rustBufferFromBytes(tree.raw)
	if err != nil {
		return err
	}

	var status C.RustCallStatus
	C.uniffi_prolly_bindings_fn_method_prollyengine_publish_named_root_at_millis(handle, nameBuf, treeBuf, C.uint64_t(timestampMillis), &status)
	return statusError(&status)
}

func (e *Engine) LoadNamedRoot(name []byte) (*Tree, error) {
	handle, err := e.cloneHandle()
	if err != nil {
		return nil, err
	}
	nameBuf, err := rustBufferFromBytes(encodeByteArray(name))
	if err != nil {
		return nil, err
	}

	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_prollyengine_load_named_root(handle, nameBuf, &status)
	if err := statusError(&status); err != nil {
		return nil, err
	}
	defer freeRustBuffer(buf)
	tree, ok, err := decodeOptionalTree(copyRustBuffer(buf))
	if err != nil || !ok {
		return nil, err
	}
	return &tree, nil
}

func (e *Engine) LoadNamedRoots(names [][]byte) (NamedRootSelection, error) {
	handle, err := e.cloneHandle()
	if err != nil {
		return NamedRootSelection{}, err
	}
	namesBuf, err := rustBufferFromBytes(encodeByteArraySequence(names))
	if err != nil {
		return NamedRootSelection{}, err
	}

	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_prollyengine_load_named_roots(handle, namesBuf, &status)
	if err := statusError(&status); err != nil {
		return NamedRootSelection{}, err
	}
	defer freeRustBuffer(buf)
	return decodeNamedRootSelection(copyRustBuffer(buf))
}

func (e *Engine) LoadRetainedNamedRoots(retention NamedRootRetention) (NamedRootSelection, error) {
	handle, err := e.cloneHandle()
	if err != nil {
		return NamedRootSelection{}, err
	}
	retentionBytes, err := encodeNamedRootRetention(retention)
	if err != nil {
		return NamedRootSelection{}, err
	}
	retentionBuf, err := rustBufferFromBytes(retentionBytes)
	if err != nil {
		return NamedRootSelection{}, err
	}

	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_prollyengine_load_retained_named_roots(handle, retentionBuf, &status)
	if err := statusError(&status); err != nil {
		return NamedRootSelection{}, err
	}
	defer freeRustBuffer(buf)
	return decodeNamedRootSelection(copyRustBuffer(buf))
}

func (e *Engine) ListNamedRoots() ([]NamedRoot, error) {
	handle, err := e.cloneHandle()
	if err != nil {
		return nil, err
	}

	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_prollyengine_list_named_roots(handle, &status)
	if err := statusError(&status); err != nil {
		return nil, err
	}
	defer freeRustBuffer(buf)
	return decodeNamedRoots(copyRustBuffer(buf))
}

func (e *Engine) ListNamedRootManifests() ([]NamedRootManifestRecord, error) {
	handle, err := e.cloneHandle()
	if err != nil {
		return nil, err
	}

	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_prollyengine_list_named_root_manifests(handle, &status)
	if err := statusError(&status); err != nil {
		return nil, err
	}
	defer freeRustBuffer(buf)
	return decodeNamedRootManifests(copyRustBuffer(buf))
}

func (e *Engine) CompareAndSwapNamedRoot(name []byte, expected *Tree, replacement *Tree) (NamedRootUpdate, error) {
	handle, err := e.cloneHandle()
	if err != nil {
		return NamedRootUpdate{}, err
	}
	nameBuf, err := rustBufferFromBytes(encodeByteArray(name))
	if err != nil {
		return NamedRootUpdate{}, err
	}
	expectedBuf, err := rustBufferFromBytes(encodeOptionalTree(expected))
	if err != nil {
		return NamedRootUpdate{}, err
	}
	replacementBuf, err := rustBufferFromBytes(encodeOptionalTree(replacement))
	if err != nil {
		return NamedRootUpdate{}, err
	}

	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_prollyengine_compare_and_swap_named_root(handle, nameBuf, expectedBuf, replacementBuf, &status)
	if err := statusError(&status); err != nil {
		return NamedRootUpdate{}, err
	}
	defer freeRustBuffer(buf)
	return decodeNamedRootUpdate(copyRustBuffer(buf))
}

func (e *Engine) CompareAndSwapNamedRootAtMillis(name []byte, expected *Tree, replacement *Tree, timestampMillis uint64) (NamedRootUpdate, error) {
	handle, err := e.cloneHandle()
	if err != nil {
		return NamedRootUpdate{}, err
	}
	nameBuf, err := rustBufferFromBytes(encodeByteArray(name))
	if err != nil {
		return NamedRootUpdate{}, err
	}
	expectedBuf, err := rustBufferFromBytes(encodeOptionalTree(expected))
	if err != nil {
		return NamedRootUpdate{}, err
	}
	replacementBuf, err := rustBufferFromBytes(encodeOptionalTree(replacement))
	if err != nil {
		return NamedRootUpdate{}, err
	}

	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_prollyengine_compare_and_swap_named_root_at_millis(
		handle,
		nameBuf,
		expectedBuf,
		replacementBuf,
		C.uint64_t(timestampMillis),
		&status,
	)
	if err := statusError(&status); err != nil {
		return NamedRootUpdate{}, err
	}
	defer freeRustBuffer(buf)
	return decodeNamedRootUpdate(copyRustBuffer(buf))
}

func (e *Engine) DeleteNamedRoot(name []byte) error {
	handle, err := e.cloneHandle()
	if err != nil {
		return err
	}
	nameBuf, err := rustBufferFromBytes(encodeByteArray(name))
	if err != nil {
		return err
	}

	var status C.RustCallStatus
	C.uniffi_prolly_bindings_fn_method_prollyengine_delete_named_root(handle, nameBuf, &status)
	return statusError(&status)
}

func (e *Engine) PublishSnapshot(namespace SnapshotNamespace, id []byte, tree Tree) error {
	handle, err := e.cloneHandle()
	if err != nil {
		return err
	}
	namespaceBuf, err := rustBufferFromBytesMustEncodeSnapshotNamespace(namespace)
	if err != nil {
		return err
	}
	idBuf, err := rustBufferFromBytes(encodeByteArray(id))
	if err != nil {
		return err
	}
	treeBuf, err := rustBufferFromBytes(tree.raw)
	if err != nil {
		return err
	}

	var status C.RustCallStatus
	C.uniffi_prolly_bindings_fn_method_prollyengine_publish_snapshot(handle, namespaceBuf, idBuf, treeBuf, &status)
	return statusError(&status)
}

func (e *Engine) PublishSnapshotAtMillis(namespace SnapshotNamespace, id []byte, tree Tree, timestampMillis uint64) error {
	handle, err := e.cloneHandle()
	if err != nil {
		return err
	}
	namespaceBuf, err := rustBufferFromBytesMustEncodeSnapshotNamespace(namespace)
	if err != nil {
		return err
	}
	idBuf, err := rustBufferFromBytes(encodeByteArray(id))
	if err != nil {
		return err
	}
	treeBuf, err := rustBufferFromBytes(tree.raw)
	if err != nil {
		return err
	}

	var status C.RustCallStatus
	C.uniffi_prolly_bindings_fn_method_prollyengine_publish_snapshot_at_millis(
		handle,
		namespaceBuf,
		idBuf,
		treeBuf,
		C.uint64_t(timestampMillis),
		&status,
	)
	return statusError(&status)
}

func (e *Engine) LoadSnapshot(namespace SnapshotNamespace, id []byte) (*Tree, error) {
	handle, err := e.cloneHandle()
	if err != nil {
		return nil, err
	}
	namespaceBuf, err := rustBufferFromBytesMustEncodeSnapshotNamespace(namespace)
	if err != nil {
		return nil, err
	}
	idBuf, err := rustBufferFromBytes(encodeByteArray(id))
	if err != nil {
		return nil, err
	}

	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_prollyengine_load_snapshot(handle, namespaceBuf, idBuf, &status)
	if err := statusError(&status); err != nil {
		return nil, err
	}
	defer freeRustBuffer(buf)
	tree, ok, err := decodeOptionalTree(copyRustBuffer(buf))
	if err != nil || !ok {
		return nil, err
	}
	return &tree, nil
}

func (e *Engine) LoadSnapshots(namespace SnapshotNamespace, ids [][]byte) (SnapshotSelection, error) {
	handle, err := e.cloneHandle()
	if err != nil {
		return SnapshotSelection{}, err
	}
	namespaceBuf, err := rustBufferFromBytesMustEncodeSnapshotNamespace(namespace)
	if err != nil {
		return SnapshotSelection{}, err
	}
	idsBuf, err := rustBufferFromBytes(encodeByteArraySequence(ids))
	if err != nil {
		return SnapshotSelection{}, err
	}

	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_prollyengine_load_snapshots(handle, namespaceBuf, idsBuf, &status)
	if err := statusError(&status); err != nil {
		return SnapshotSelection{}, err
	}
	defer freeRustBuffer(buf)
	return decodeSnapshotSelection(copyRustBuffer(buf))
}

func (e *Engine) ListSnapshots(namespace SnapshotNamespace) ([]SnapshotRoot, error) {
	handle, err := e.cloneHandle()
	if err != nil {
		return nil, err
	}
	namespaceBuf, err := rustBufferFromBytesMustEncodeSnapshotNamespace(namespace)
	if err != nil {
		return nil, err
	}

	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_prollyengine_list_snapshots(handle, namespaceBuf, &status)
	if err := statusError(&status); err != nil {
		return nil, err
	}
	defer freeRustBuffer(buf)
	return decodeSnapshotRoots(copyRustBuffer(buf))
}

func (e *Engine) DeleteSnapshot(namespace SnapshotNamespace, id []byte) error {
	handle, err := e.cloneHandle()
	if err != nil {
		return err
	}
	namespaceBuf, err := rustBufferFromBytesMustEncodeSnapshotNamespace(namespace)
	if err != nil {
		return err
	}
	idBuf, err := rustBufferFromBytes(encodeByteArray(id))
	if err != nil {
		return err
	}

	var status C.RustCallStatus
	C.uniffi_prolly_bindings_fn_method_prollyengine_delete_snapshot(handle, namespaceBuf, idBuf, &status)
	return statusError(&status)
}

func (e *Engine) CompareAndSwapSnapshot(namespace SnapshotNamespace, id []byte, expected *Tree, replacement *Tree) (NamedRootUpdate, error) {
	handle, err := e.cloneHandle()
	if err != nil {
		return NamedRootUpdate{}, err
	}
	namespaceBuf, err := rustBufferFromBytesMustEncodeSnapshotNamespace(namespace)
	if err != nil {
		return NamedRootUpdate{}, err
	}
	idBuf, err := rustBufferFromBytes(encodeByteArray(id))
	if err != nil {
		return NamedRootUpdate{}, err
	}
	expectedBuf, err := rustBufferFromBytes(encodeOptionalTree(expected))
	if err != nil {
		return NamedRootUpdate{}, err
	}
	replacementBuf, err := rustBufferFromBytes(encodeOptionalTree(replacement))
	if err != nil {
		return NamedRootUpdate{}, err
	}

	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_prollyengine_compare_and_swap_snapshot(handle, namespaceBuf, idBuf, expectedBuf, replacementBuf, &status)
	if err := statusError(&status); err != nil {
		return NamedRootUpdate{}, err
	}
	defer freeRustBuffer(buf)
	return decodeNamedRootUpdate(copyRustBuffer(buf))
}

func (e *Engine) CompareAndSwapSnapshotAtMillis(namespace SnapshotNamespace, id []byte, expected *Tree, replacement *Tree, timestampMillis uint64) (NamedRootUpdate, error) {
	handle, err := e.cloneHandle()
	if err != nil {
		return NamedRootUpdate{}, err
	}
	namespaceBuf, err := rustBufferFromBytesMustEncodeSnapshotNamespace(namespace)
	if err != nil {
		return NamedRootUpdate{}, err
	}
	idBuf, err := rustBufferFromBytes(encodeByteArray(id))
	if err != nil {
		return NamedRootUpdate{}, err
	}
	expectedBuf, err := rustBufferFromBytes(encodeOptionalTree(expected))
	if err != nil {
		return NamedRootUpdate{}, err
	}
	replacementBuf, err := rustBufferFromBytes(encodeOptionalTree(replacement))
	if err != nil {
		return NamedRootUpdate{}, err
	}

	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_prollyengine_compare_and_swap_snapshot_at_millis(
		handle,
		namespaceBuf,
		idBuf,
		expectedBuf,
		replacementBuf,
		C.uint64_t(timestampMillis),
		&status,
	)
	if err := statusError(&status); err != nil {
		return NamedRootUpdate{}, err
	}
	defer freeRustBuffer(buf)
	return decodeNamedRootUpdate(copyRustBuffer(buf))
}

func (e *Engine) CollectStatsJSON(tree Tree) (string, error) {
	return e.callTreeJSON(tree, "collect_stats_json")
}

func (e *Engine) CollectStats(tree Tree) (TreeStats, error) {
	raw, err := e.CollectStatsJSON(tree)
	if err != nil {
		return TreeStats{}, err
	}
	var stats TreeStats
	if err := json.Unmarshal([]byte(raw), &stats); err != nil {
		return TreeStats{}, err
	}
	return stats, nil
}

func (e *Engine) StatsDiffJSON(before Tree, after Tree) (string, error) {
	return e.callTreePairJSON(before, after, "stats_diff_json")
}

func (e *Engine) StatsDiff(before Tree, after Tree) (StatsComparison, error) {
	raw, err := e.StatsDiffJSON(before, after)
	if err != nil {
		return StatsComparison{}, err
	}
	var stats StatsComparison
	if err := json.Unmarshal([]byte(raw), &stats); err != nil {
		return StatsComparison{}, err
	}
	return stats, nil
}

func (e *Engine) DebugTreeJSON(tree Tree) (string, error) {
	return e.callTreeJSON(tree, "debug_tree_json")
}

func (e *Engine) DebugTree(tree Tree) (TreeDebugView, error) {
	raw, err := e.DebugTreeJSON(tree)
	if err != nil {
		return TreeDebugView{}, err
	}
	var view TreeDebugView
	if err := json.Unmarshal([]byte(raw), &view); err != nil {
		return TreeDebugView{}, err
	}
	return view, nil
}

func (e *Engine) DebugTreeText(tree Tree) (string, error) {
	return e.callTreeString(tree, "debug_tree_text")
}

func (e *Engine) DebugCompareTreesJSON(left Tree, right Tree) (string, error) {
	return e.callTreePairJSON(left, right, "debug_compare_trees_json")
}

func (e *Engine) DebugCompareTrees(left Tree, right Tree) (TreeDebugComparison, error) {
	raw, err := e.DebugCompareTreesJSON(left, right)
	if err != nil {
		return TreeDebugComparison{}, err
	}
	var comparison TreeDebugComparison
	if err := json.Unmarshal([]byte(raw), &comparison); err != nil {
		return TreeDebugComparison{}, err
	}
	return comparison, nil
}

func (e *Engine) DebugCompareTreesText(left Tree, right Tree) (string, error) {
	return e.callTreePairString(left, right, "debug_compare_trees_text")
}

func (e *Engine) CacheStats() (CacheStats, error) {
	handle, err := e.cloneHandle()
	if err != nil {
		return CacheStats{}, err
	}
	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_prollyengine_cache_stats(handle, &status)
	if err := statusError(&status); err != nil {
		return CacheStats{}, err
	}
	defer freeRustBuffer(buf)
	return decodeCacheStats(copyRustBuffer(buf))
}

func (e *Engine) ClearCache() error {
	handle, err := e.cloneHandle()
	if err != nil {
		return err
	}
	var status C.RustCallStatus
	C.uniffi_prolly_bindings_fn_method_prollyengine_clear_cache(handle, &status)
	return statusError(&status)
}

func (e *Engine) PinTreeRoot(tree Tree) (uint64, error) {
	handle, err := e.cloneHandle()
	if err != nil {
		return 0, err
	}
	treeBuf, err := rustBufferFromBytes(tree.raw)
	if err != nil {
		return 0, err
	}
	var status C.RustCallStatus
	count := C.uniffi_prolly_bindings_fn_method_prollyengine_pin_tree_root(handle, treeBuf, &status)
	return uint64(count), statusError(&status)
}

func (e *Engine) PinTreePath(tree Tree, key []byte) (uint64, error) {
	handle, err := e.cloneHandle()
	if err != nil {
		return 0, err
	}
	treeBuf, err := rustBufferFromBytes(tree.raw)
	if err != nil {
		return 0, err
	}
	keyBuf, err := rustBufferFromBytes(encodeByteArray(key))
	if err != nil {
		return 0, err
	}
	var status C.RustCallStatus
	count := C.uniffi_prolly_bindings_fn_method_prollyengine_pin_tree_path(handle, treeBuf, keyBuf, &status)
	return uint64(count), statusError(&status)
}

func (e *Engine) UnpinAllCacheNodes() (uint64, error) {
	handle, err := e.cloneHandle()
	if err != nil {
		return 0, err
	}
	var status C.RustCallStatus
	count := C.uniffi_prolly_bindings_fn_method_prollyengine_unpin_all_cache_nodes(handle, &status)
	return uint64(count), statusError(&status)
}

func (e *Engine) Metrics() (Metrics, error) {
	handle, err := e.cloneHandle()
	if err != nil {
		return Metrics{}, err
	}
	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_prollyengine_metrics(handle, &status)
	if err := statusError(&status); err != nil {
		return Metrics{}, err
	}
	defer freeRustBuffer(buf)
	return decodeMetrics(copyRustBuffer(buf))
}

func (e *Engine) ResetMetrics() error {
	handle, err := e.cloneHandle()
	if err != nil {
		return err
	}
	var status C.RustCallStatus
	C.uniffi_prolly_bindings_fn_method_prollyengine_reset_metrics(handle, &status)
	return statusError(&status)
}

func (e *Engine) PublishPrefixPathHint(tree Tree, prefix []byte) (bool, error) {
	return e.callTreeBytesBool(tree, prefix, "publish_prefix_path_hint")
}

func (e *Engine) HydratePrefixPathHint(tree Tree, prefix []byte) (bool, error) {
	return e.callTreeBytesBool(tree, prefix, "hydrate_prefix_path_hint")
}

func (e *Engine) PublishChangedSpansHint(base Tree, changed Tree, spans []ChangedSpan) (bool, error) {
	handle, err := e.cloneHandle()
	if err != nil {
		return false, err
	}
	baseBuf, err := rustBufferFromBytes(base.raw)
	if err != nil {
		return false, err
	}
	changedBuf, err := rustBufferFromBytes(changed.raw)
	if err != nil {
		return false, err
	}
	spansBuf, err := rustBufferFromBytes(encodeChangedSpans(spans))
	if err != nil {
		return false, err
	}
	var status C.RustCallStatus
	ok := C.uniffi_prolly_bindings_fn_method_prollyengine_publish_changed_spans_hint(handle, baseBuf, changedBuf, spansBuf, &status)
	if err := statusError(&status); err != nil {
		return false, err
	}
	return ok != 0, nil
}

func (e *Engine) LoadChangedSpansHint(base Tree, changed Tree) (*ChangedSpanHint, error) {
	handle, err := e.cloneHandle()
	if err != nil {
		return nil, err
	}
	baseBuf, err := rustBufferFromBytes(base.raw)
	if err != nil {
		return nil, err
	}
	changedBuf, err := rustBufferFromBytes(changed.raw)
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_prollyengine_load_changed_spans_hint(handle, baseBuf, changedBuf, &status)
	if err := statusError(&status); err != nil {
		return nil, err
	}
	defer freeRustBuffer(buf)
	return decodeChangedSpanHint(copyRustBuffer(buf))
}

func (e *Engine) StructuralDiffPage(base Tree, other Tree, cursorJSON *string, limit uint64) (StructuralDiffPage, error) {
	handle, err := e.cloneHandle()
	if err != nil {
		return StructuralDiffPage{}, err
	}
	baseBuf, err := rustBufferFromBytes(base.raw)
	if err != nil {
		return StructuralDiffPage{}, err
	}
	otherBuf, err := rustBufferFromBytes(other.raw)
	if err != nil {
		return StructuralDiffPage{}, err
	}
	var cursorBytes bytes.Buffer
	encodeOptionalString(&cursorBytes, cursorJSON)
	cursorBuf, err := rustBufferFromBytes(cursorBytes.Bytes())
	if err != nil {
		return StructuralDiffPage{}, err
	}
	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_prollyengine_structural_diff_page(handle, baseBuf, otherBuf, cursorBuf, C.uint64_t(limit), &status)
	if err := statusError(&status); err != nil {
		return StructuralDiffPage{}, err
	}
	defer freeRustBuffer(buf)
	return decodeStructuralDiffPage(copyRustBuffer(buf))
}

func (e *Engine) StructuralDiffPageWithCursor(base Tree, other Tree, cursor *StructuralDiffCursor, limit uint64) (StructuralDiffPage, error) {
	handle, err := e.cloneHandle()
	if err != nil {
		return StructuralDiffPage{}, err
	}
	baseBuf, err := rustBufferFromBytes(base.raw)
	if err != nil {
		return StructuralDiffPage{}, err
	}
	otherBuf, err := rustBufferFromBytes(other.raw)
	if err != nil {
		return StructuralDiffPage{}, err
	}
	cursorBuf, err := rustBufferFromBytes(encodeOptionalStructuralDiffCursor(cursor))
	if err != nil {
		return StructuralDiffPage{}, err
	}
	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_prollyengine_structural_diff_page_with_cursor(handle, baseBuf, otherBuf, cursorBuf, C.uint64_t(limit), &status)
	if err := statusError(&status); err != nil {
		return StructuralDiffPage{}, err
	}
	defer freeRustBuffer(buf)
	return decodeStructuralDiffPage(copyRustBuffer(buf))
}

func (e *Engine) MarkReachable(roots []Tree) (GcReachability, error) {
	payload, err := e.callRootsBuffer(roots, nil, "mark_reachable")
	if err != nil {
		return GcReachability{}, err
	}
	return decodeGcReachability(payload)
}

func (e *Engine) PlanGC(roots []Tree, candidateCids [][]byte) (GcPlan, error) {
	payload, err := e.callRootsBuffer(roots, candidateCids, "plan_gc")
	if err != nil {
		return GcPlan{}, err
	}
	return decodeGcPlan(payload)
}

func (e *Engine) SweepGC(roots []Tree, candidateCids [][]byte) (GcSweep, error) {
	payload, err := e.callRootsBuffer(roots, candidateCids, "sweep_gc")
	if err != nil {
		return GcSweep{}, err
	}
	return decodeGcSweep(payload)
}

func (e *Engine) ListNodeCids() ([][]byte, error) {
	handle, err := e.cloneHandle()
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_prollyengine_list_node_cids(handle, &status)
	if err := statusError(&status); err != nil {
		return nil, err
	}
	defer freeRustBuffer(buf)
	return decodeByteArraySequence(copyRustBuffer(buf))
}

func (e *Engine) PlanStoreGC(roots []Tree) (GcPlan, error) {
	payload, err := e.callRootsBuffer(roots, nil, "plan_store_gc")
	if err != nil {
		return GcPlan{}, err
	}
	return decodeGcPlan(payload)
}

func (e *Engine) SweepStoreGC(roots []Tree) (GcSweep, error) {
	payload, err := e.callRootsBuffer(roots, nil, "sweep_store_gc")
	if err != nil {
		return GcSweep{}, err
	}
	return decodeGcSweep(payload)
}

func (e *Engine) PlanStoreGCForRetention(retention NamedRootRetention) (GcPlan, error) {
	payload, err := e.callRetentionGcBuffer(retention, "plan_store_gc_for_retention")
	if err != nil {
		return GcPlan{}, err
	}
	return decodeGcPlan(payload)
}

func (e *Engine) SweepStoreGCForRetention(retention NamedRootRetention) (GcSweep, error) {
	payload, err := e.callRetentionGcBuffer(retention, "sweep_store_gc_for_retention")
	if err != nil {
		return GcSweep{}, err
	}
	return decodeGcSweep(payload)
}

func (e *Engine) MarkReachableBlobs(roots []Tree) (BlobGcReachability, error) {
	payload, err := e.callBlobGcBuffer(nil, roots, nil, "mark_reachable_blobs")
	if err != nil {
		return BlobGcReachability{}, err
	}
	return decodeBlobGcReachability(payload)
}

func (e *Engine) PlanBlobGC(blobStore *BlobStore, roots []Tree, candidates []BlobRef) (BlobGcPlan, error) {
	payload, err := e.callBlobGcBuffer(blobStore, roots, candidates, "plan_blob_gc")
	if err != nil {
		return BlobGcPlan{}, err
	}
	return decodeBlobGcPlan(payload)
}

func (e *Engine) SweepBlobGC(blobStore *BlobStore, roots []Tree, candidates []BlobRef) (BlobGcSweep, error) {
	payload, err := e.callBlobGcBuffer(blobStore, roots, candidates, "sweep_blob_gc")
	if err != nil {
		return BlobGcSweep{}, err
	}
	return decodeBlobGcSweep(payload)
}

func (e *Engine) PlanBlobStoreGC(blobStore *BlobStore, roots []Tree) (BlobGcPlan, error) {
	payload, err := e.callBlobGcBuffer(blobStore, roots, nil, "plan_blob_store_gc")
	if err != nil {
		return BlobGcPlan{}, err
	}
	return decodeBlobGcPlan(payload)
}

func (e *Engine) SweepBlobStoreGC(blobStore *BlobStore, roots []Tree) (BlobGcSweep, error) {
	payload, err := e.callBlobGcBuffer(blobStore, roots, nil, "sweep_blob_store_gc")
	if err != nil {
		return BlobGcSweep{}, err
	}
	return decodeBlobGcSweep(payload)
}

func (e *Engine) PlanMissingNodes(tree Tree, destination *Engine) (MissingNodePlan, error) {
	payload, err := e.callTreeDestinationBuffer(tree, destination, "plan_missing_nodes")
	if err != nil {
		return MissingNodePlan{}, err
	}
	return decodeMissingNodePlan(payload)
}

func (e *Engine) CopyMissingNodes(tree Tree, destination *Engine) (MissingNodeCopy, error) {
	payload, err := e.callTreeDestinationBuffer(tree, destination, "copy_missing_nodes")
	if err != nil {
		return MissingNodeCopy{}, err
	}
	return decodeMissingNodeCopy(payload)
}

func (e *Engine) ExportSnapshot(tree Tree) (SnapshotBundle, error) {
	handle, err := e.cloneHandle()
	if err != nil {
		return SnapshotBundle{}, err
	}
	treeBuf, err := rustBufferFromBytes(tree.raw)
	if err != nil {
		return SnapshotBundle{}, err
	}
	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_prollyengine_export_snapshot(handle, treeBuf, &status)
	if err := statusError(&status); err != nil {
		return SnapshotBundle{}, err
	}
	defer freeRustBuffer(buf)
	return decodeSnapshotBundle(copyRustBuffer(buf))
}

func (e *Engine) ImportSnapshot(bundle SnapshotBundle) (Tree, error) {
	handle, err := e.cloneHandle()
	if err != nil {
		return Tree{}, err
	}
	bundleBuf, err := rustBufferFromBytes(encodeSnapshotBundle(bundle))
	if err != nil {
		return Tree{}, err
	}
	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_prollyengine_import_snapshot(handle, bundleBuf, &status)
	if err := statusError(&status); err != nil {
		return Tree{}, err
	}
	defer freeRustBuffer(buf)
	return decodeTree(copyRustBuffer(buf))
}

func (e *Engine) callRootsBuffer(roots []Tree, candidateCids [][]byte, op string) ([]byte, error) {
	handle, err := e.cloneHandle()
	if err != nil {
		return nil, err
	}
	rootsBuf, err := rustBufferFromBytes(encodeTrees(roots))
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	var buf C.RustBuffer
	switch op {
	case "mark_reachable":
		buf = C.uniffi_prolly_bindings_fn_method_prollyengine_mark_reachable(handle, rootsBuf, &status)
	case "plan_gc":
		candidatesBuf, err := rustBufferFromBytes(encodeByteArraySequence(candidateCids))
		if err != nil {
			return nil, err
		}
		buf = C.uniffi_prolly_bindings_fn_method_prollyengine_plan_gc(handle, rootsBuf, candidatesBuf, &status)
	case "sweep_gc":
		candidatesBuf, err := rustBufferFromBytes(encodeByteArraySequence(candidateCids))
		if err != nil {
			return nil, err
		}
		buf = C.uniffi_prolly_bindings_fn_method_prollyengine_sweep_gc(handle, rootsBuf, candidatesBuf, &status)
	case "plan_store_gc":
		buf = C.uniffi_prolly_bindings_fn_method_prollyengine_plan_store_gc(handle, rootsBuf, &status)
	case "sweep_store_gc":
		buf = C.uniffi_prolly_bindings_fn_method_prollyengine_sweep_store_gc(handle, rootsBuf, &status)
	default:
		return nil, fmt.Errorf("unknown roots operation %q", op)
	}
	if err := statusError(&status); err != nil {
		return nil, err
	}
	defer freeRustBuffer(buf)
	return copyRustBuffer(buf), nil
}

func (e *Engine) callRetentionGcBuffer(retention NamedRootRetention, op string) ([]byte, error) {
	handle, err := e.cloneHandle()
	if err != nil {
		return nil, err
	}
	retentionBytes, err := encodeNamedRootRetention(retention)
	if err != nil {
		return nil, err
	}
	retentionBuf, err := rustBufferFromBytes(retentionBytes)
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	var buf C.RustBuffer
	switch op {
	case "plan_store_gc_for_retention":
		buf = C.uniffi_prolly_bindings_fn_method_prollyengine_plan_store_gc_for_retention(handle, retentionBuf, &status)
	case "sweep_store_gc_for_retention":
		buf = C.uniffi_prolly_bindings_fn_method_prollyengine_sweep_store_gc_for_retention(handle, retentionBuf, &status)
	default:
		return nil, fmt.Errorf("unknown retained roots operation %q", op)
	}
	if err := statusError(&status); err != nil {
		return nil, err
	}
	defer freeRustBuffer(buf)
	return copyRustBuffer(buf), nil
}

func (e *Engine) callBlobGcBuffer(blobStore *BlobStore, roots []Tree, candidateBlobs []BlobRef, op string) ([]byte, error) {
	handle, err := e.cloneHandle()
	if err != nil {
		return nil, err
	}
	rootsBuf, err := rustBufferFromBytes(encodeTrees(roots))
	if err != nil {
		return nil, err
	}

	var blobHandle C.uint64_t
	if op != "mark_reachable_blobs" {
		blobHandle, err = blobStore.cloneHandle()
		if err != nil {
			return nil, err
		}
	}

	var status C.RustCallStatus
	var buf C.RustBuffer
	switch op {
	case "mark_reachable_blobs":
		buf = C.uniffi_prolly_bindings_fn_method_prollyengine_mark_reachable_blobs(handle, rootsBuf, &status)
	case "plan_blob_gc":
		candidatesBuf, err := rustBufferFromBytes(encodeBlobRefs(candidateBlobs))
		if err != nil {
			return nil, err
		}
		buf = C.uniffi_prolly_bindings_fn_method_prollyengine_plan_blob_gc(handle, blobHandle, rootsBuf, candidatesBuf, &status)
	case "sweep_blob_gc":
		candidatesBuf, err := rustBufferFromBytes(encodeBlobRefs(candidateBlobs))
		if err != nil {
			return nil, err
		}
		buf = C.uniffi_prolly_bindings_fn_method_prollyengine_sweep_blob_gc(handle, blobHandle, rootsBuf, candidatesBuf, &status)
	case "plan_blob_store_gc":
		buf = C.uniffi_prolly_bindings_fn_method_prollyengine_plan_blob_store_gc(handle, blobHandle, rootsBuf, &status)
	case "sweep_blob_store_gc":
		buf = C.uniffi_prolly_bindings_fn_method_prollyengine_sweep_blob_store_gc(handle, blobHandle, rootsBuf, &status)
	default:
		return nil, fmt.Errorf("unknown blob GC operation %q", op)
	}
	if err := statusError(&status); err != nil {
		return nil, err
	}
	defer freeRustBuffer(buf)
	return copyRustBuffer(buf), nil
}

func (e *Engine) callTreeDestinationBuffer(tree Tree, destination *Engine, op string) ([]byte, error) {
	handle, err := e.cloneHandle()
	if err != nil {
		return nil, err
	}
	destinationHandle, err := destination.cloneHandle()
	if err != nil {
		return nil, err
	}
	treeBuf, err := rustBufferFromBytes(tree.raw)
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	var buf C.RustBuffer
	switch op {
	case "plan_missing_nodes":
		buf = C.uniffi_prolly_bindings_fn_method_prollyengine_plan_missing_nodes(handle, treeBuf, destinationHandle, &status)
	case "copy_missing_nodes":
		buf = C.uniffi_prolly_bindings_fn_method_prollyengine_copy_missing_nodes(handle, treeBuf, destinationHandle, &status)
	default:
		return nil, fmt.Errorf("unknown tree destination operation %q", op)
	}
	if err := statusError(&status); err != nil {
		return nil, err
	}
	defer freeRustBuffer(buf)
	return copyRustBuffer(buf), nil
}

func (e *Engine) callTreeJSON(tree Tree, op string) (string, error) {
	payload, err := e.callTreeBuffer(tree, op)
	if err != nil {
		return "", err
	}
	return decodeJsonDocument(payload)
}

func (e *Engine) callTreeString(tree Tree, op string) (string, error) {
	payload, err := e.callTreeBuffer(tree, op)
	if err != nil {
		return "", err
	}
	return decodeStringRecord(payload)
}

func (e *Engine) callTreeBuffer(tree Tree, op string) ([]byte, error) {
	handle, err := e.cloneHandle()
	if err != nil {
		return nil, err
	}
	treeBuf, err := rustBufferFromBytes(tree.raw)
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	var buf C.RustBuffer
	switch op {
	case "collect_stats_json":
		buf = C.uniffi_prolly_bindings_fn_method_prollyengine_collect_stats_json(handle, treeBuf, &status)
	case "debug_tree_json":
		buf = C.uniffi_prolly_bindings_fn_method_prollyengine_debug_tree_json(handle, treeBuf, &status)
	case "debug_tree_text":
		buf = C.uniffi_prolly_bindings_fn_method_prollyengine_debug_tree_text(handle, treeBuf, &status)
	default:
		return nil, fmt.Errorf("unknown tree operation %q", op)
	}
	if err := statusError(&status); err != nil {
		return nil, err
	}
	defer freeRustBuffer(buf)
	return copyRustBuffer(buf), nil
}

func (e *Engine) callTreePairJSON(left Tree, right Tree, op string) (string, error) {
	payload, err := e.callTreePairBuffer(left, right, op)
	if err != nil {
		return "", err
	}
	return decodeJsonDocument(payload)
}

func (e *Engine) callTreePairString(left Tree, right Tree, op string) (string, error) {
	payload, err := e.callTreePairBuffer(left, right, op)
	if err != nil {
		return "", err
	}
	return decodeStringRecord(payload)
}

func (e *Engine) callTreePairBuffer(left Tree, right Tree, op string) ([]byte, error) {
	handle, err := e.cloneHandle()
	if err != nil {
		return nil, err
	}
	leftBuf, err := rustBufferFromBytes(left.raw)
	if err != nil {
		return nil, err
	}
	rightBuf, err := rustBufferFromBytes(right.raw)
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	var buf C.RustBuffer
	switch op {
	case "stats_diff_json":
		buf = C.uniffi_prolly_bindings_fn_method_prollyengine_stats_diff_json(handle, leftBuf, rightBuf, &status)
	case "debug_compare_trees_json":
		buf = C.uniffi_prolly_bindings_fn_method_prollyengine_debug_compare_trees_json(handle, leftBuf, rightBuf, &status)
	case "debug_compare_trees_text":
		buf = C.uniffi_prolly_bindings_fn_method_prollyengine_debug_compare_trees_text(handle, leftBuf, rightBuf, &status)
	default:
		return nil, fmt.Errorf("unknown tree pair operation %q", op)
	}
	if err := statusError(&status); err != nil {
		return nil, err
	}
	defer freeRustBuffer(buf)
	return copyRustBuffer(buf), nil
}

func (e *Engine) callTreeBytesBool(tree Tree, value []byte, op string) (bool, error) {
	handle, err := e.cloneHandle()
	if err != nil {
		return false, err
	}
	treeBuf, err := rustBufferFromBytes(tree.raw)
	if err != nil {
		return false, err
	}
	valueBuf, err := rustBufferFromBytes(encodeByteArray(value))
	if err != nil {
		return false, err
	}
	var status C.RustCallStatus
	var ok C.uint8_t
	switch op {
	case "publish_prefix_path_hint":
		ok = C.uniffi_prolly_bindings_fn_method_prollyengine_publish_prefix_path_hint(handle, treeBuf, valueBuf, &status)
	case "hydrate_prefix_path_hint":
		ok = C.uniffi_prolly_bindings_fn_method_prollyengine_hydrate_prefix_path_hint(handle, treeBuf, valueBuf, &status)
	default:
		return false, fmt.Errorf("unknown tree bytes operation %q", op)
	}
	if err := statusError(&status); err != nil {
		return false, err
	}
	return ok != 0, nil
}

func CidFromBytes(data []byte) ([]byte, error) {
	in, err := rustBufferFromBytes(encodeByteArray(data))
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	out := C.uniffi_prolly_bindings_fn_func_cid_from_bytes(in, &status)
	if err := statusError(&status); err != nil {
		return nil, err
	}
	defer freeRustBuffer(out)
	return decodeRequiredByteArray(copyRustBuffer(out))
}

func NodeBytesRoundTrip(data []byte) ([]byte, error) {
	node, err := NodeFromBytes(data)
	if err != nil {
		return nil, err
	}
	return NodeToBytes(node)
}

func NodeCidFromBytes(data []byte) ([]byte, error) {
	node, err := NodeFromBytes(data)
	if err != nil {
		return nil, err
	}
	in, err := rustBufferFromBytes(node)
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	out := C.uniffi_prolly_bindings_fn_func_node_cid(in, &status)
	if err := statusError(&status); err != nil {
		return nil, err
	}
	defer freeRustBuffer(out)
	return decodeRequiredByteArray(copyRustBuffer(out))
}

func NodeFromBytes(data []byte) ([]byte, error) {
	in, err := rustBufferFromBytes(encodeByteArray(data))
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	out := C.uniffi_prolly_bindings_fn_func_node_from_bytes(in, &status)
	if err := statusError(&status); err != nil {
		return nil, err
	}
	defer freeRustBuffer(out)
	return copyRustBuffer(out), nil
}

func NodeToBytes(node []byte) ([]byte, error) {
	in, err := rustBufferFromBytes(node)
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	out := C.uniffi_prolly_bindings_fn_func_node_to_bytes(in, &status)
	if err := statusError(&status); err != nil {
		return nil, err
	}
	defer freeRustBuffer(out)
	return decodeRequiredByteArray(copyRustBuffer(out))
}

func VerifyKeyProof(proof KeyProof) (KeyProofVerification, error) {
	encoded, err := encodeKeyProof(proof)
	if err != nil {
		return KeyProofVerification{}, err
	}
	in, err := rustBufferFromBytes(encoded)
	if err != nil {
		return KeyProofVerification{}, err
	}
	var status C.RustCallStatus
	out := C.uniffi_prolly_bindings_fn_func_verify_key_proof(in, &status)
	if err := statusError(&status); err != nil {
		return KeyProofVerification{}, err
	}
	defer freeRustBuffer(out)
	return decodeKeyProofVerification(copyRustBuffer(out))
}

func VerifyMultiKeyProof(proof MultiKeyProof) (MultiKeyProofVerification, error) {
	encoded, err := encodeMultiKeyProof(proof)
	if err != nil {
		return MultiKeyProofVerification{}, err
	}
	in, err := rustBufferFromBytes(encoded)
	if err != nil {
		return MultiKeyProofVerification{}, err
	}
	var status C.RustCallStatus
	out := C.uniffi_prolly_bindings_fn_func_verify_multi_key_proof(in, &status)
	if err := statusError(&status); err != nil {
		return MultiKeyProofVerification{}, err
	}
	defer freeRustBuffer(out)
	return decodeMultiKeyProofVerification(copyRustBuffer(out))
}

func KeyProofPathNodeBytes(proof KeyProof) ([][]byte, error) {
	encoded, err := encodeKeyProof(proof)
	if err != nil {
		return nil, err
	}
	in, err := rustBufferFromBytes(encoded)
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	out := C.uniffi_prolly_bindings_fn_func_key_proof_path_node_bytes(in, &status)
	if err := statusError(&status); err != nil {
		return nil, err
	}
	defer freeRustBuffer(out)
	return decodeByteArraySequence(copyRustBuffer(out))
}

func KeyProofToBytes(proof KeyProof) ([]byte, error) {
	encoded, err := encodeKeyProof(proof)
	if err != nil {
		return nil, err
	}
	in, err := rustBufferFromBytes(encoded)
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	out := C.uniffi_prolly_bindings_fn_func_key_proof_to_bytes(in, &status)
	if err := statusError(&status); err != nil {
		return nil, err
	}
	defer freeRustBuffer(out)
	return decodeRequiredByteArray(copyRustBuffer(out))
}

func KeyProofFromBytes(bytes []byte) (KeyProof, error) {
	in, err := rustBufferFromBytes(encodeByteArray(bytes))
	if err != nil {
		return KeyProof{}, err
	}
	var status C.RustCallStatus
	out := C.uniffi_prolly_bindings_fn_func_key_proof_from_bytes(in, &status)
	if err := statusError(&status); err != nil {
		return KeyProof{}, err
	}
	defer freeRustBuffer(out)
	return decodeKeyProof(copyRustBuffer(out))
}

func MultiKeyProofPathNodeBytes(proof MultiKeyProof) ([][]byte, error) {
	encoded, err := encodeMultiKeyProof(proof)
	if err != nil {
		return nil, err
	}
	in, err := rustBufferFromBytes(encoded)
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	out := C.uniffi_prolly_bindings_fn_func_multi_key_proof_path_node_bytes(in, &status)
	if err := statusError(&status); err != nil {
		return nil, err
	}
	defer freeRustBuffer(out)
	return decodeByteArraySequence(copyRustBuffer(out))
}

func MultiKeyProofToBytes(proof MultiKeyProof) ([]byte, error) {
	encoded, err := encodeMultiKeyProof(proof)
	if err != nil {
		return nil, err
	}
	in, err := rustBufferFromBytes(encoded)
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	out := C.uniffi_prolly_bindings_fn_func_multi_key_proof_to_bytes(in, &status)
	if err := statusError(&status); err != nil {
		return nil, err
	}
	defer freeRustBuffer(out)
	return decodeRequiredByteArray(copyRustBuffer(out))
}

func MultiKeyProofFromBytes(bytes []byte) (MultiKeyProof, error) {
	in, err := rustBufferFromBytes(encodeByteArray(bytes))
	if err != nil {
		return MultiKeyProof{}, err
	}
	var status C.RustCallStatus
	out := C.uniffi_prolly_bindings_fn_func_multi_key_proof_from_bytes(in, &status)
	if err := statusError(&status); err != nil {
		return MultiKeyProof{}, err
	}
	defer freeRustBuffer(out)
	return decodeMultiKeyProof(copyRustBuffer(out))
}

func VerifyRangeProof(proof RangeProof) (RangeProofVerification, error) {
	encoded, err := encodeRangeProof(proof)
	if err != nil {
		return RangeProofVerification{}, err
	}
	in, err := rustBufferFromBytes(encoded)
	if err != nil {
		return RangeProofVerification{}, err
	}
	var status C.RustCallStatus
	out := C.uniffi_prolly_bindings_fn_func_verify_range_proof(in, &status)
	if err := statusError(&status); err != nil {
		return RangeProofVerification{}, err
	}
	defer freeRustBuffer(out)
	return decodeRangeProofVerification(copyRustBuffer(out))
}

func RangeProofPathNodeBytes(proof RangeProof) ([][]byte, error) {
	encoded, err := encodeRangeProof(proof)
	if err != nil {
		return nil, err
	}
	in, err := rustBufferFromBytes(encoded)
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	out := C.uniffi_prolly_bindings_fn_func_range_proof_path_node_bytes(in, &status)
	if err := statusError(&status); err != nil {
		return nil, err
	}
	defer freeRustBuffer(out)
	return decodeByteArraySequence(copyRustBuffer(out))
}

func RangeProofToBytes(proof RangeProof) ([]byte, error) {
	encoded, err := encodeRangeProof(proof)
	if err != nil {
		return nil, err
	}
	in, err := rustBufferFromBytes(encoded)
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	out := C.uniffi_prolly_bindings_fn_func_range_proof_to_bytes(in, &status)
	if err := statusError(&status); err != nil {
		return nil, err
	}
	defer freeRustBuffer(out)
	return decodeRequiredByteArray(copyRustBuffer(out))
}

func RangeProofFromBytes(bytes []byte) (RangeProof, error) {
	in, err := rustBufferFromBytes(encodeByteArray(bytes))
	if err != nil {
		return RangeProof{}, err
	}
	var status C.RustCallStatus
	out := C.uniffi_prolly_bindings_fn_func_range_proof_from_bytes(in, &status)
	if err := statusError(&status); err != nil {
		return RangeProof{}, err
	}
	defer freeRustBuffer(out)
	return decodeRangeProof(copyRustBuffer(out))
}

func RangeProofFromNodeBytes(root []byte, hasRoot bool, start []byte, end []byte, pathNodeBytes [][]byte) (RangeProof, error) {
	var rootValue []byte
	if hasRoot {
		rootValue = root
	}
	rootBuf, err := rustBufferFromBytes(encodeOptionalByteArray(rootValue))
	if err != nil {
		return RangeProof{}, err
	}
	startBuf, err := rustBufferFromBytes(encodeByteArray(start))
	if err != nil {
		return RangeProof{}, err
	}
	endBuf, err := rustBufferFromBytes(encodeOptionalByteArray(end))
	if err != nil {
		return RangeProof{}, err
	}
	pathBuf, err := rustBufferFromBytes(encodeByteArraySequence(pathNodeBytes))
	if err != nil {
		return RangeProof{}, err
	}
	var status C.RustCallStatus
	out := C.uniffi_prolly_bindings_fn_func_range_proof_from_node_bytes(rootBuf, startBuf, endBuf, pathBuf, &status)
	if err := statusError(&status); err != nil {
		return RangeProof{}, err
	}
	defer freeRustBuffer(out)
	return decodeRangeProof(copyRustBuffer(out))
}

func VerifyRangePageProof(proof RangePageProof) (RangePageProofVerification, error) {
	encoded, err := encodeRangePageProof(proof)
	if err != nil {
		return RangePageProofVerification{}, err
	}
	in, err := rustBufferFromBytes(encoded)
	if err != nil {
		return RangePageProofVerification{}, err
	}
	var status C.RustCallStatus
	out := C.uniffi_prolly_bindings_fn_func_verify_range_page_proof(in, &status)
	if err := statusError(&status); err != nil {
		return RangePageProofVerification{}, err
	}
	defer freeRustBuffer(out)
	return decodeRangePageProofVerification(copyRustBuffer(out))
}

func RangePageProofPathNodeBytes(proof RangePageProof) ([][]byte, error) {
	encoded, err := encodeRangePageProof(proof)
	if err != nil {
		return nil, err
	}
	in, err := rustBufferFromBytes(encoded)
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	out := C.uniffi_prolly_bindings_fn_func_range_page_proof_path_node_bytes(in, &status)
	if err := statusError(&status); err != nil {
		return nil, err
	}
	defer freeRustBuffer(out)
	return decodeByteArraySequence(copyRustBuffer(out))
}

func RangePageProofToBytes(proof RangePageProof) ([]byte, error) {
	encoded, err := encodeRangePageProof(proof)
	if err != nil {
		return nil, err
	}
	in, err := rustBufferFromBytes(encoded)
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	out := C.uniffi_prolly_bindings_fn_func_range_page_proof_to_bytes(in, &status)
	if err := statusError(&status); err != nil {
		return nil, err
	}
	defer freeRustBuffer(out)
	return decodeRequiredByteArray(copyRustBuffer(out))
}

func RangePageProofFromBytes(bytes []byte) (RangePageProof, error) {
	in, err := rustBufferFromBytes(encodeByteArray(bytes))
	if err != nil {
		return RangePageProof{}, err
	}
	var status C.RustCallStatus
	out := C.uniffi_prolly_bindings_fn_func_range_page_proof_from_bytes(in, &status)
	if err := statusError(&status); err != nil {
		return RangePageProof{}, err
	}
	defer freeRustBuffer(out)
	return decodeRangePageProof(copyRustBuffer(out))
}

func RangePageProofFromNodeBytes(root []byte, hasRoot bool, after []byte, hasAfter bool, end []byte, hasEnd bool, pathNodeBytes [][]byte) (RangePageProof, error) {
	var rootValue []byte
	if hasRoot {
		rootValue = root
	}
	rootBuf, err := rustBufferFromBytes(encodeOptionalByteArray(rootValue))
	if err != nil {
		return RangePageProof{}, err
	}
	var afterValue []byte
	if hasAfter {
		afterValue = after
	}
	afterBuf, err := rustBufferFromBytes(encodeOptionalByteArray(afterValue))
	if err != nil {
		return RangePageProof{}, err
	}
	var endValue []byte
	if hasEnd {
		endValue = end
	}
	endBuf, err := rustBufferFromBytes(encodeOptionalByteArray(endValue))
	if err != nil {
		return RangePageProof{}, err
	}
	pathBuf, err := rustBufferFromBytes(encodeByteArraySequence(pathNodeBytes))
	if err != nil {
		return RangePageProof{}, err
	}
	var status C.RustCallStatus
	out := C.uniffi_prolly_bindings_fn_func_range_page_proof_from_node_bytes(rootBuf, afterBuf, endBuf, pathBuf, &status)
	if err := statusError(&status); err != nil {
		return RangePageProof{}, err
	}
	defer freeRustBuffer(out)
	return decodeRangePageProof(copyRustBuffer(out))
}

func VerifyDiffPageProof(proof DiffPageProof) (DiffPageProofVerification, error) {
	encoded, err := encodeDiffPageProof(proof)
	if err != nil {
		return DiffPageProofVerification{}, err
	}
	in, err := rustBufferFromBytes(encoded)
	if err != nil {
		return DiffPageProofVerification{}, err
	}
	var status C.RustCallStatus
	out := C.uniffi_prolly_bindings_fn_func_verify_diff_page_proof(in, &status)
	if err := statusError(&status); err != nil {
		return DiffPageProofVerification{}, err
	}
	defer freeRustBuffer(out)
	return decodeDiffPageProofVerification(copyRustBuffer(out))
}

func DiffPageProofToBytes(proof DiffPageProof) ([]byte, error) {
	encoded, err := encodeDiffPageProof(proof)
	if err != nil {
		return nil, err
	}
	in, err := rustBufferFromBytes(encoded)
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	out := C.uniffi_prolly_bindings_fn_func_diff_page_proof_to_bytes(in, &status)
	if err := statusError(&status); err != nil {
		return nil, err
	}
	defer freeRustBuffer(out)
	return decodeRequiredByteArray(copyRustBuffer(out))
}

func DiffPageProofFromBytes(bytes []byte) (DiffPageProof, error) {
	in, err := rustBufferFromBytes(encodeByteArray(bytes))
	if err != nil {
		return DiffPageProof{}, err
	}
	var status C.RustCallStatus
	out := C.uniffi_prolly_bindings_fn_func_diff_page_proof_from_bytes(in, &status)
	if err := statusError(&status); err != nil {
		return DiffPageProof{}, err
	}
	defer freeRustBuffer(out)
	return decodeDiffPageProof(copyRustBuffer(out))
}

func InspectProofBundle(bytes []byte) (ProofBundleSummary, error) {
	in, err := rustBufferFromBytes(encodeByteArray(bytes))
	if err != nil {
		return ProofBundleSummary{}, err
	}
	var status C.RustCallStatus
	out := C.uniffi_prolly_bindings_fn_func_inspect_proof_bundle(in, &status)
	if err := statusError(&status); err != nil {
		return ProofBundleSummary{}, err
	}
	defer freeRustBuffer(out)
	return decodeProofBundleSummary(copyRustBuffer(out))
}

func VerifyProofBundle(bytes []byte) (ProofBundleVerification, error) {
	in, err := rustBufferFromBytes(encodeByteArray(bytes))
	if err != nil {
		return ProofBundleVerification{}, err
	}
	var status C.RustCallStatus
	out := C.uniffi_prolly_bindings_fn_func_verify_proof_bundle(in, &status)
	if err := statusError(&status); err != nil {
		return ProofBundleVerification{}, err
	}
	defer freeRustBuffer(out)
	return decodeProofBundleVerification(copyRustBuffer(out))
}

func SignProofBundleHmacSha256(proofBundle, keyID, secret, context []byte, issuedAtMillis, expiresAtMillis *uint64, nonce []byte) (AuthenticatedProofEnvelope, error) {
	proofBuf, err := rustBufferFromBytes(encodeByteArray(proofBundle))
	if err != nil {
		return AuthenticatedProofEnvelope{}, err
	}
	keyIDBuf, err := rustBufferFromBytes(encodeByteArray(keyID))
	if err != nil {
		return AuthenticatedProofEnvelope{}, err
	}
	secretBuf, err := rustBufferFromBytes(encodeByteArray(secret))
	if err != nil {
		return AuthenticatedProofEnvelope{}, err
	}
	contextBuf, err := rustBufferFromBytes(encodeByteArray(context))
	if err != nil {
		return AuthenticatedProofEnvelope{}, err
	}
	issuedBuf, err := rustBufferFromBytes(encodeOptionalU64Bytes(issuedAtMillis))
	if err != nil {
		return AuthenticatedProofEnvelope{}, err
	}
	expiresBuf, err := rustBufferFromBytes(encodeOptionalU64Bytes(expiresAtMillis))
	if err != nil {
		return AuthenticatedProofEnvelope{}, err
	}
	nonceBuf, err := rustBufferFromBytes(encodeByteArray(nonce))
	if err != nil {
		return AuthenticatedProofEnvelope{}, err
	}
	var status C.RustCallStatus
	out := C.uniffi_prolly_bindings_fn_func_sign_proof_bundle_hmac_sha256(proofBuf, keyIDBuf, secretBuf, contextBuf, issuedBuf, expiresBuf, nonceBuf, &status)
	if err := statusError(&status); err != nil {
		return AuthenticatedProofEnvelope{}, err
	}
	defer freeRustBuffer(out)
	return decodeAuthenticatedProofEnvelope(copyRustBuffer(out))
}

func VerifyAuthenticatedProofEnvelope(envelope AuthenticatedProofEnvelope, secret []byte, nowMillis *uint64) (AuthenticatedProofEnvelopeVerification, error) {
	encoded, err := encodeAuthenticatedProofEnvelope(envelope)
	if err != nil {
		return AuthenticatedProofEnvelopeVerification{}, err
	}
	envelopeBuf, err := rustBufferFromBytes(encoded)
	if err != nil {
		return AuthenticatedProofEnvelopeVerification{}, err
	}
	secretBuf, err := rustBufferFromBytes(encodeByteArray(secret))
	if err != nil {
		return AuthenticatedProofEnvelopeVerification{}, err
	}
	nowBuf, err := rustBufferFromBytes(encodeOptionalU64Bytes(nowMillis))
	if err != nil {
		return AuthenticatedProofEnvelopeVerification{}, err
	}
	var status C.RustCallStatus
	out := C.uniffi_prolly_bindings_fn_func_verify_authenticated_proof_envelope(envelopeBuf, secretBuf, nowBuf, &status)
	if err := statusError(&status); err != nil {
		return AuthenticatedProofEnvelopeVerification{}, err
	}
	defer freeRustBuffer(out)
	return decodeAuthenticatedProofEnvelopeVerification(copyRustBuffer(out))
}

func VerifyAuthenticatedProofBundle(envelopeBytes, secret []byte, nowMillis *uint64) (AuthenticatedProofBundleVerification, error) {
	envelopeBuf, err := rustBufferFromBytes(encodeByteArray(envelopeBytes))
	if err != nil {
		return AuthenticatedProofBundleVerification{}, err
	}
	secretBuf, err := rustBufferFromBytes(encodeByteArray(secret))
	if err != nil {
		return AuthenticatedProofBundleVerification{}, err
	}
	nowBuf, err := rustBufferFromBytes(encodeOptionalU64Bytes(nowMillis))
	if err != nil {
		return AuthenticatedProofBundleVerification{}, err
	}
	var status C.RustCallStatus
	out := C.uniffi_prolly_bindings_fn_func_verify_authenticated_proof_bundle(envelopeBuf, secretBuf, nowBuf, &status)
	if err := statusError(&status); err != nil {
		return AuthenticatedProofBundleVerification{}, err
	}
	defer freeRustBuffer(out)
	return decodeAuthenticatedProofBundleVerification(copyRustBuffer(out))
}

func AuthenticatedProofEnvelopeToBytes(envelope AuthenticatedProofEnvelope) ([]byte, error) {
	encoded, err := encodeAuthenticatedProofEnvelope(envelope)
	if err != nil {
		return nil, err
	}
	in, err := rustBufferFromBytes(encoded)
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	out := C.uniffi_prolly_bindings_fn_func_authenticated_proof_envelope_to_bytes(in, &status)
	if err := statusError(&status); err != nil {
		return nil, err
	}
	defer freeRustBuffer(out)
	return decodeRequiredByteArray(copyRustBuffer(out))
}

func AuthenticatedProofEnvelopeFromBytes(bytes []byte) (AuthenticatedProofEnvelope, error) {
	in, err := rustBufferFromBytes(encodeByteArray(bytes))
	if err != nil {
		return AuthenticatedProofEnvelope{}, err
	}
	var status C.RustCallStatus
	out := C.uniffi_prolly_bindings_fn_func_authenticated_proof_envelope_from_bytes(in, &status)
	if err := statusError(&status); err != nil {
		return AuthenticatedProofEnvelope{}, err
	}
	defer freeRustBuffer(out)
	return decodeAuthenticatedProofEnvelope(copyRustBuffer(out))
}

func KeyProofFromNodeBytes(root []byte, hasRoot bool, key []byte, pathNodeBytes [][]byte) (KeyProof, error) {
	var rootValue []byte
	if hasRoot {
		rootValue = root
	}
	rootBuf, err := rustBufferFromBytes(encodeOptionalByteArray(rootValue))
	if err != nil {
		return KeyProof{}, err
	}
	keyBuf, err := rustBufferFromBytes(encodeByteArray(key))
	if err != nil {
		return KeyProof{}, err
	}
	pathBuf, err := rustBufferFromBytes(encodeByteArraySequence(pathNodeBytes))
	if err != nil {
		return KeyProof{}, err
	}
	var status C.RustCallStatus
	out := C.uniffi_prolly_bindings_fn_func_key_proof_from_node_bytes(rootBuf, keyBuf, pathBuf, &status)
	if err := statusError(&status); err != nil {
		return KeyProof{}, err
	}
	defer freeRustBuffer(out)
	return decodeKeyProof(copyRustBuffer(out))
}

func MultiKeyProofFromNodeBytes(root []byte, hasRoot bool, keys [][]byte, pathNodeBytes [][]byte) (MultiKeyProof, error) {
	var rootValue []byte
	if hasRoot {
		rootValue = root
	}
	rootBuf, err := rustBufferFromBytes(encodeOptionalByteArray(rootValue))
	if err != nil {
		return MultiKeyProof{}, err
	}
	keysBuf, err := rustBufferFromBytes(encodeByteArraySequence(keys))
	if err != nil {
		return MultiKeyProof{}, err
	}
	pathBuf, err := rustBufferFromBytes(encodeByteArraySequence(pathNodeBytes))
	if err != nil {
		return MultiKeyProof{}, err
	}
	var status C.RustCallStatus
	out := C.uniffi_prolly_bindings_fn_func_multi_key_proof_from_node_bytes(rootBuf, keysBuf, pathBuf, &status)
	if err := statusError(&status); err != nil {
		return MultiKeyProof{}, err
	}
	defer freeRustBuffer(out)
	return decodeMultiKeyProof(copyRustBuffer(out))
}

func IsBoundary(config Config, count uint64, key []byte, value []byte) (bool, error) {
	configBuf, err := rustBufferFromBytes(config.raw)
	if err != nil {
		return false, err
	}
	keyBuf, err := rustBufferFromBytes(encodeByteArray(key))
	if err != nil {
		return false, err
	}
	valueBuf, err := rustBufferFromBytes(encodeByteArray(value))
	if err != nil {
		return false, err
	}
	var status C.RustCallStatus
	out := C.uniffi_prolly_bindings_fn_func_is_boundary_config(configBuf, C.uint64_t(count), keyBuf, valueBuf, &status)
	if err := statusError(&status); err != nil {
		return false, err
	}
	return out != 0, nil
}

func PrefixEnd(prefix []byte) ([]byte, bool, error) {
	in, err := rustBufferFromBytes(encodeByteArray(prefix))
	if err != nil {
		return nil, false, err
	}
	var status C.RustCallStatus
	out := C.uniffi_prolly_bindings_fn_func_prefix_end(in, &status)
	if err := statusError(&status); err != nil {
		return nil, false, err
	}
	defer freeRustBuffer(out)
	return decodeOptionalByteArray(copyRustBuffer(out))
}

func PrefixRange(prefix []byte) (RangeBounds, error) {
	in, err := rustBufferFromBytes(encodeByteArray(prefix))
	if err != nil {
		return RangeBounds{}, err
	}
	var status C.RustCallStatus
	out := C.uniffi_prolly_bindings_fn_func_prefix_range(in, &status)
	if err := statusError(&status); err != nil {
		return RangeBounds{}, err
	}
	defer freeRustBuffer(out)
	return decodeRangeBounds(copyRustBuffer(out))
}

func U64Key(value uint64) ([]byte, error) {
	var status C.RustCallStatus
	out := C.uniffi_prolly_bindings_fn_func_u64_key(C.uint64_t(value), &status)
	if err := statusError(&status); err != nil {
		return nil, err
	}
	defer freeRustBuffer(out)
	return decodeRequiredByteArray(copyRustBuffer(out))
}

func U128Key(value string) ([]byte, error) {
	in, err := rustBufferFromBytes([]byte(value))
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	out := C.uniffi_prolly_bindings_fn_func_u128_key(in, &status)
	if err := statusError(&status); err != nil {
		return nil, err
	}
	defer freeRustBuffer(out)
	return decodeRequiredByteArray(copyRustBuffer(out))
}

func I64Key(value int64) ([]byte, error) {
	var status C.RustCallStatus
	out := C.uniffi_prolly_bindings_fn_func_i64_key(C.int64_t(value), &status)
	if err := statusError(&status); err != nil {
		return nil, err
	}
	defer freeRustBuffer(out)
	return decodeRequiredByteArray(copyRustBuffer(out))
}

func I128Key(value string) ([]byte, error) {
	in, err := rustBufferFromBytes([]byte(value))
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	out := C.uniffi_prolly_bindings_fn_func_i128_key(in, &status)
	if err := statusError(&status); err != nil {
		return nil, err
	}
	defer freeRustBuffer(out)
	return decodeRequiredByteArray(copyRustBuffer(out))
}

func TimestampMillisKey(value uint64) ([]byte, error) {
	var status C.RustCallStatus
	out := C.uniffi_prolly_bindings_fn_func_timestamp_millis_key(C.uint64_t(value), &status)
	if err := statusError(&status); err != nil {
		return nil, err
	}
	defer freeRustBuffer(out)
	return decodeRequiredByteArray(copyRustBuffer(out))
}

func EncodeSegment(segment []byte) ([]byte, error) {
	in, err := rustBufferFromBytes(encodeByteArray(segment))
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	out := C.uniffi_prolly_bindings_fn_func_encode_segment(in, &status)
	if err := statusError(&status); err != nil {
		return nil, err
	}
	defer freeRustBuffer(out)
	return decodeRequiredByteArray(copyRustBuffer(out))
}

func KeyFromSegments(segments [][]byte) ([]byte, error) {
	in, err := rustBufferFromBytes(encodeByteArraySequence(segments))
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	out := C.uniffi_prolly_bindings_fn_func_key_from_segments(in, &status)
	if err := statusError(&status); err != nil {
		return nil, err
	}
	defer freeRustBuffer(out)
	return decodeRequiredByteArray(copyRustBuffer(out))
}

func KeyFromPrefixedSegments(prefix []byte, segments [][]byte) ([]byte, error) {
	prefixBuf, err := rustBufferFromBytes(encodeByteArray(prefix))
	if err != nil {
		return nil, err
	}
	segmentsBuf, err := rustBufferFromBytes(encodeByteArraySequence(segments))
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	out := C.uniffi_prolly_bindings_fn_func_key_from_prefixed_segments(prefixBuf, segmentsBuf, &status)
	if err := statusError(&status); err != nil {
		return nil, err
	}
	defer freeRustBuffer(out)
	return decodeRequiredByteArray(copyRustBuffer(out))
}

func DecodeSegments(key []byte) ([][]byte, error) {
	in, err := rustBufferFromBytes(encodeByteArray(key))
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	out := C.uniffi_prolly_bindings_fn_func_decode_segments(in, &status)
	if err := statusError(&status); err != nil {
		return nil, err
	}
	defer freeRustBuffer(out)
	return decodeByteArraySequence(copyRustBuffer(out))
}

func DebugKey(key []byte) (string, error) {
	in, err := rustBufferFromBytes(encodeByteArray(key))
	if err != nil {
		return "", err
	}
	var status C.RustCallStatus
	out := C.uniffi_prolly_bindings_fn_func_debug_key(in, &status)
	if err := statusError(&status); err != nil {
		return "", err
	}
	defer freeRustBuffer(out)
	return string(copyRustBuffer(out)), nil
}

func SnapshotNamespaceBranch() (SnapshotNamespace, error) {
	var status C.RustCallStatus
	out := C.uniffi_prolly_bindings_fn_func_snapshot_namespace_branch(&status)
	if err := statusError(&status); err != nil {
		return SnapshotNamespace{}, err
	}
	defer freeRustBuffer(out)
	return decodeSnapshotNamespace(copyRustBuffer(out))
}

func SnapshotNamespaceTag() (SnapshotNamespace, error) {
	var status C.RustCallStatus
	out := C.uniffi_prolly_bindings_fn_func_snapshot_namespace_tag(&status)
	if err := statusError(&status); err != nil {
		return SnapshotNamespace{}, err
	}
	defer freeRustBuffer(out)
	return decodeSnapshotNamespace(copyRustBuffer(out))
}

func SnapshotNamespaceCheckpoint() (SnapshotNamespace, error) {
	var status C.RustCallStatus
	out := C.uniffi_prolly_bindings_fn_func_snapshot_namespace_checkpoint(&status)
	if err := statusError(&status); err != nil {
		return SnapshotNamespace{}, err
	}
	defer freeRustBuffer(out)
	return decodeSnapshotNamespace(copyRustBuffer(out))
}

func SnapshotNamespaceCustom(prefix []byte) (SnapshotNamespace, error) {
	prefixBuf, err := rustBufferFromBytes(encodeByteArray(prefix))
	if err != nil {
		return SnapshotNamespace{}, err
	}
	var status C.RustCallStatus
	out := C.uniffi_prolly_bindings_fn_func_snapshot_namespace_custom(prefixBuf, &status)
	if err := statusError(&status); err != nil {
		return SnapshotNamespace{}, err
	}
	defer freeRustBuffer(out)
	return decodeSnapshotNamespace(copyRustBuffer(out))
}

func SnapshotRootName(namespace SnapshotNamespace, id []byte) ([]byte, error) {
	namespaceBytes, err := encodeSnapshotNamespace(namespace)
	if err != nil {
		return nil, err
	}
	namespaceBuf, err := rustBufferFromBytes(namespaceBytes)
	if err != nil {
		return nil, err
	}
	idBuf, err := rustBufferFromBytes(encodeByteArray(id))
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	out := C.uniffi_prolly_bindings_fn_func_snapshot_root_name(namespaceBuf, idBuf, &status)
	if err := statusError(&status); err != nil {
		return nil, err
	}
	defer freeRustBuffer(out)
	return decodeRequiredByteArray(copyRustBuffer(out))
}

func SnapshotIDFromName(namespace SnapshotNamespace, name []byte) ([]byte, bool, error) {
	namespaceBytes, err := encodeSnapshotNamespace(namespace)
	if err != nil {
		return nil, false, err
	}
	namespaceBuf, err := rustBufferFromBytes(namespaceBytes)
	if err != nil {
		return nil, false, err
	}
	nameBuf, err := rustBufferFromBytes(encodeByteArray(name))
	if err != nil {
		return nil, false, err
	}
	var status C.RustCallStatus
	out := C.uniffi_prolly_bindings_fn_func_snapshot_id_from_name(namespaceBuf, nameBuf, &status)
	if err := statusError(&status); err != nil {
		return nil, false, err
	}
	defer freeRustBuffer(out)
	return decodeOptionalByteArray(copyRustBuffer(out))
}

func CrdtConfigLWW(deletePolicy string) (CrdtConfig, error) {
	return crdtConfig(deletePolicy, "lww")
}

func CrdtConfigMultiValue(deletePolicy string) (CrdtConfig, error) {
	return crdtConfig(deletePolicy, "multi_value")
}

func crdtConfig(deletePolicy string, strategy string) (CrdtConfig, error) {
	policyBytes, err := encodeCrdtDeletePolicy(deletePolicy)
	if err != nil {
		return CrdtConfig{}, err
	}
	policyBuf, err := rustBufferFromBytes(policyBytes)
	if err != nil {
		return CrdtConfig{}, err
	}
	var status C.RustCallStatus
	var out C.RustBuffer
	switch strategy {
	case "lww":
		out = C.uniffi_prolly_bindings_fn_func_crdt_config_lww(policyBuf, &status)
	case "multi_value":
		out = C.uniffi_prolly_bindings_fn_func_crdt_config_multi_value(policyBuf, &status)
	default:
		return CrdtConfig{}, fmt.Errorf("unknown CRDT strategy %q", strategy)
	}
	if err := statusError(&status); err != nil {
		return CrdtConfig{}, err
	}
	defer freeRustBuffer(out)
	return decodeCrdtConfig(copyRustBuffer(out))
}

func TimestampedValueToBytes(record TimestampedValue) ([]byte, error) {
	recordBuf, err := rustBufferFromBytes(encodeTimestampedValue(record))
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	out := C.uniffi_prolly_bindings_fn_func_timestamped_value_to_bytes(recordBuf, &status)
	if err := statusError(&status); err != nil {
		return nil, err
	}
	defer freeRustBuffer(out)
	return decodeRequiredByteArray(copyRustBuffer(out))
}

func TimestampedValueFromBytes(data []byte) (TimestampedValue, error) {
	in, err := rustBufferFromBytes(encodeByteArray(data))
	if err != nil {
		return TimestampedValue{}, err
	}
	var status C.RustCallStatus
	out := C.uniffi_prolly_bindings_fn_func_timestamped_value_from_bytes(in, &status)
	if err := statusError(&status); err != nil {
		return TimestampedValue{}, err
	}
	defer freeRustBuffer(out)
	return decodeTimestampedValue(copyRustBuffer(out))
}

func TimestampedValueNow(value []byte) (TimestampedValue, error) {
	in, err := rustBufferFromBytes(encodeByteArray(value))
	if err != nil {
		return TimestampedValue{}, err
	}
	var status C.RustCallStatus
	out := C.uniffi_prolly_bindings_fn_func_timestamped_value_now(in, &status)
	if err := statusError(&status); err != nil {
		return TimestampedValue{}, err
	}
	defer freeRustBuffer(out)
	return decodeTimestampedValue(copyRustBuffer(out))
}

func MultiValueSetToBytes(values [][]byte) ([]byte, error) {
	in, err := rustBufferFromBytes(encodeByteArraySequence(values))
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	out := C.uniffi_prolly_bindings_fn_func_multi_value_set_to_bytes(in, &status)
	if err := statusError(&status); err != nil {
		return nil, err
	}
	defer freeRustBuffer(out)
	return decodeRequiredByteArray(copyRustBuffer(out))
}

func MultiValueSetFromBytes(data []byte) ([][]byte, error) {
	in, err := rustBufferFromBytes(encodeByteArray(data))
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	out := C.uniffi_prolly_bindings_fn_func_multi_value_set_from_bytes(in, &status)
	if err := statusError(&status); err != nil {
		return nil, err
	}
	defer freeRustBuffer(out)
	return decodeByteArraySequence(copyRustBuffer(out))
}

func MultiValueSetMerge(left [][]byte, right [][]byte) ([][]byte, error) {
	leftBuf, err := rustBufferFromBytes(encodeByteArraySequence(left))
	if err != nil {
		return nil, err
	}
	rightBuf, err := rustBufferFromBytes(encodeByteArraySequence(right))
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	out := C.uniffi_prolly_bindings_fn_func_multi_value_set_merge(leftBuf, rightBuf, &status)
	if err := statusError(&status); err != nil {
		return nil, err
	}
	defer freeRustBuffer(out)
	return decodeByteArraySequence(copyRustBuffer(out))
}

func TombstoneToBytes(record Tombstone) ([]byte, error) {
	recordBuf, err := rustBufferFromBytes(encodeTombstone(record))
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	out := C.uniffi_prolly_bindings_fn_func_tombstone_to_bytes(recordBuf, &status)
	if err := statusError(&status); err != nil {
		return nil, err
	}
	defer freeRustBuffer(out)
	return decodeRequiredByteArray(copyRustBuffer(out))
}

func TombstoneFromBytes(data []byte) (Tombstone, error) {
	in, err := rustBufferFromBytes(encodeByteArray(data))
	if err != nil {
		return Tombstone{}, err
	}
	var status C.RustCallStatus
	out := C.uniffi_prolly_bindings_fn_func_tombstone_from_bytes(in, &status)
	if err := statusError(&status); err != nil {
		return Tombstone{}, err
	}
	defer freeRustBuffer(out)
	return decodeTombstone(copyRustBuffer(out))
}

func TombstoneFromStoredBytes(data []byte) (*Tombstone, error) {
	in, err := rustBufferFromBytes(encodeByteArray(data))
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	out := C.uniffi_prolly_bindings_fn_func_tombstone_from_stored_bytes(in, &status)
	if err := statusError(&status); err != nil {
		return nil, err
	}
	defer freeRustBuffer(out)
	return decodeOptionalTombstone(copyRustBuffer(out))
}

func IsTombstoneValue(data []byte) (bool, error) {
	in, err := rustBufferFromBytes(encodeByteArray(data))
	if err != nil {
		return false, err
	}
	var status C.RustCallStatus
	out := C.uniffi_prolly_bindings_fn_func_is_tombstone_value(in, &status)
	if err := statusError(&status); err != nil {
		return false, err
	}
	return out != 0, nil
}

func TombstoneUpsertMutation(key []byte, tombstone Tombstone) (Mutation, error) {
	keyBuf, err := rustBufferFromBytes(encodeByteArray(key))
	if err != nil {
		return Mutation{}, err
	}
	tombstoneBuf, err := rustBufferFromBytes(encodeTombstone(tombstone))
	if err != nil {
		return Mutation{}, err
	}
	var status C.RustCallStatus
	out := C.uniffi_prolly_bindings_fn_func_tombstone_upsert_mutation(keyBuf, tombstoneBuf, &status)
	if err := statusError(&status); err != nil {
		return Mutation{}, err
	}
	defer freeRustBuffer(out)
	return decodeMutation(copyRustBuffer(out))
}

func TombstoneCompactionMutation(key []byte, storedValue []byte) (*Mutation, error) {
	keyBuf, err := rustBufferFromBytes(encodeByteArray(key))
	if err != nil {
		return nil, err
	}
	valueBuf, err := rustBufferFromBytes(encodeByteArray(storedValue))
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	out := C.uniffi_prolly_bindings_fn_func_tombstone_compaction_mutation(keyBuf, valueBuf, &status)
	if err := statusError(&status); err != nil {
		return nil, err
	}
	defer freeRustBuffer(out)
	return decodeOptionalMutation(copyRustBuffer(out))
}

func VersionedValueBytesRoundTrip(data []byte) ([]byte, error) {
	record, err := versionedValueFromBytes(data)
	if err != nil {
		return nil, err
	}
	return versionedValueToBytes(record)
}

func VersionedValueBytesMatchesSchema(data []byte, schema string, version uint64) (bool, error) {
	dataBuf, schemaBuf, err := versionedValueSchemaBuffers(data, schema)
	if err != nil {
		return false, err
	}
	var status C.RustCallStatus
	ok := C.uniffi_prolly_bindings_fn_func_versioned_value_bytes_matches_schema(dataBuf, schemaBuf, C.uint64_t(version), &status)
	if err := statusError(&status); err != nil {
		return false, err
	}
	return ok != 0, nil
}

func VersionedValueBytesRequireSchema(data []byte, schema string, version uint64) error {
	dataBuf, schemaBuf, err := versionedValueSchemaBuffers(data, schema)
	if err != nil {
		return err
	}
	var status C.RustCallStatus
	C.uniffi_prolly_bindings_fn_func_versioned_value_bytes_require_schema(dataBuf, schemaBuf, C.uint64_t(version), &status)
	return statusError(&status)
}

func ValueRefBytesRoundTrip(data []byte) ([]byte, error) {
	record, err := valueRefFromBytes(data)
	if err != nil {
		return nil, err
	}
	return valueRefToBytes(record)
}

func ValueRefFromStoredBytes(data []byte) (ValueRef, error) {
	record, err := valueRefFromStoredBytes(data)
	if err != nil {
		return ValueRef{}, err
	}
	return decodeValueRef(record)
}

func ValueRefInlineRequiresEscape(value []byte) (bool, error) {
	in, err := rustBufferFromBytes(encodeByteArray(value))
	if err != nil {
		return false, err
	}
	var status C.RustCallStatus
	out := C.uniffi_prolly_bindings_fn_func_value_ref_inline_requires_escape(in, &status)
	if err := statusError(&status); err != nil {
		return false, err
	}
	return out != 0, nil
}

func RootManifestBytesRoundTrip(data []byte) ([]byte, error) {
	record, err := rootManifestFromBytes(data)
	if err != nil {
		return nil, err
	}
	return rootManifestToBytes(record)
}

func SnapshotBundleToBytes(bundle SnapshotBundle) ([]byte, error) {
	recordBuf, err := rustBufferFromBytes(encodeSnapshotBundle(bundle))
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	out := C.uniffi_prolly_bindings_fn_func_snapshot_bundle_to_bytes(recordBuf, &status)
	if err := statusError(&status); err != nil {
		return nil, err
	}
	defer freeRustBuffer(out)
	return decodeRequiredByteArray(copyRustBuffer(out))
}

func SnapshotBundleFromBytes(data []byte) (SnapshotBundle, error) {
	in, err := rustBufferFromBytes(encodeByteArray(data))
	if err != nil {
		return SnapshotBundle{}, err
	}
	var status C.RustCallStatus
	record := C.uniffi_prolly_bindings_fn_func_snapshot_bundle_from_bytes(in, &status)
	if err := statusError(&status); err != nil {
		return SnapshotBundle{}, err
	}
	defer freeRustBuffer(record)
	return decodeSnapshotBundle(copyRustBuffer(record))
}

func SnapshotBundleDigest(bundle SnapshotBundle) ([]byte, error) {
	recordBuf, err := rustBufferFromBytes(encodeSnapshotBundle(bundle))
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	out := C.uniffi_prolly_bindings_fn_func_snapshot_bundle_digest(recordBuf, &status)
	if err := statusError(&status); err != nil {
		return nil, err
	}
	defer freeRustBuffer(out)
	return decodeRequiredByteArray(copyRustBuffer(out))
}

func SnapshotBundleDigestBytes(data []byte) ([]byte, error) {
	in, err := rustBufferFromBytes(encodeByteArray(data))
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	out := C.uniffi_prolly_bindings_fn_func_snapshot_bundle_digest_bytes(in, &status)
	if err := statusError(&status); err != nil {
		return nil, err
	}
	defer freeRustBuffer(out)
	return decodeRequiredByteArray(copyRustBuffer(out))
}

func SummarizeSnapshotBundle(bundle SnapshotBundle) (SnapshotBundleSummary, error) {
	recordBuf, err := rustBufferFromBytes(encodeSnapshotBundle(bundle))
	if err != nil {
		return SnapshotBundleSummary{}, err
	}
	var status C.RustCallStatus
	out := C.uniffi_prolly_bindings_fn_func_snapshot_bundle_summary(recordBuf, &status)
	if err := statusError(&status); err != nil {
		return SnapshotBundleSummary{}, err
	}
	defer freeRustBuffer(out)
	return decodeSnapshotBundleSummary(copyRustBuffer(out))
}

func SummarizeSnapshotBundleBytes(data []byte) (SnapshotBundleSummary, error) {
	in, err := rustBufferFromBytes(encodeByteArray(data))
	if err != nil {
		return SnapshotBundleSummary{}, err
	}
	var status C.RustCallStatus
	out := C.uniffi_prolly_bindings_fn_func_snapshot_bundle_summary_from_bytes(in, &status)
	if err := statusError(&status); err != nil {
		return SnapshotBundleSummary{}, err
	}
	defer freeRustBuffer(out)
	return decodeSnapshotBundleSummary(copyRustBuffer(out))
}

func VerifySnapshotBundle(bundle SnapshotBundle) (SnapshotBundleVerification, error) {
	recordBuf, err := rustBufferFromBytes(encodeSnapshotBundle(bundle))
	if err != nil {
		return SnapshotBundleVerification{}, err
	}
	var status C.RustCallStatus
	out := C.uniffi_prolly_bindings_fn_func_verify_snapshot_bundle(recordBuf, &status)
	if err := statusError(&status); err != nil {
		return SnapshotBundleVerification{}, err
	}
	defer freeRustBuffer(out)
	return decodeSnapshotBundleVerification(copyRustBuffer(out))
}

func VerifySnapshotBundleBytes(data []byte) (SnapshotBundleVerification, error) {
	in, err := rustBufferFromBytes(encodeByteArray(data))
	if err != nil {
		return SnapshotBundleVerification{}, err
	}
	var status C.RustCallStatus
	out := C.uniffi_prolly_bindings_fn_func_verify_snapshot_bundle_bytes(in, &status)
	if err := statusError(&status); err != nil {
		return SnapshotBundleVerification{}, err
	}
	defer freeRustBuffer(out)
	return decodeSnapshotBundleVerification(copyRustBuffer(out))
}

func versionedValueFromBytes(data []byte) ([]byte, error) {
	in, err := rustBufferFromBytes(encodeByteArray(data))
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	record := C.uniffi_prolly_bindings_fn_func_versioned_value_from_bytes(in, &status)
	if err := statusError(&status); err != nil {
		return nil, err
	}
	defer freeRustBuffer(record)
	return copyRustBuffer(record), nil
}

func versionedValueToBytes(record []byte) ([]byte, error) {
	recordBuf, err := rustBufferFromBytes(record)
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	out := C.uniffi_prolly_bindings_fn_func_versioned_value_to_bytes(recordBuf, &status)
	if err := statusError(&status); err != nil {
		return nil, err
	}
	defer freeRustBuffer(out)
	return decodeRequiredByteArray(copyRustBuffer(out))
}

func versionedValueSchemaBuffers(data []byte, schema string) (C.RustBuffer, C.RustBuffer, error) {
	dataBuf, err := rustBufferFromBytes(encodeByteArray(data))
	if err != nil {
		return C.RustBuffer{}, C.RustBuffer{}, err
	}
	schemaBuf, err := rustBufferFromBytes([]byte(schema))
	if err != nil {
		return C.RustBuffer{}, C.RustBuffer{}, err
	}
	return dataBuf, schemaBuf, nil
}

func valueRefFromBytes(data []byte) ([]byte, error) {
	in, err := rustBufferFromBytes(encodeByteArray(data))
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	record := C.uniffi_prolly_bindings_fn_func_value_ref_from_bytes(in, &status)
	if err := statusError(&status); err != nil {
		return nil, err
	}
	defer freeRustBuffer(record)
	return copyRustBuffer(record), nil
}

func valueRefFromStoredBytes(data []byte) ([]byte, error) {
	in, err := rustBufferFromBytes(encodeByteArray(data))
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	record := C.uniffi_prolly_bindings_fn_func_value_ref_from_stored_bytes(in, &status)
	if err := statusError(&status); err != nil {
		return nil, err
	}
	defer freeRustBuffer(record)
	return copyRustBuffer(record), nil
}

func valueRefToBytes(record []byte) ([]byte, error) {
	recordBuf, err := rustBufferFromBytes(record)
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	out := C.uniffi_prolly_bindings_fn_func_value_ref_to_bytes(recordBuf, &status)
	if err := statusError(&status); err != nil {
		return nil, err
	}
	defer freeRustBuffer(out)
	return decodeRequiredByteArray(copyRustBuffer(out))
}

func rootManifestFromBytes(data []byte) ([]byte, error) {
	in, err := rustBufferFromBytes(encodeByteArray(data))
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	record := C.uniffi_prolly_bindings_fn_func_root_manifest_from_bytes(in, &status)
	if err := statusError(&status); err != nil {
		return nil, err
	}
	defer freeRustBuffer(record)
	return copyRustBuffer(record), nil
}

func rootManifestToBytes(record []byte) ([]byte, error) {
	recordBuf, err := rustBufferFromBytes(record)
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	out := C.uniffi_prolly_bindings_fn_func_root_manifest_to_bytes(recordBuf, &status)
	if err := statusError(&status); err != nil {
		return nil, err
	}
	defer freeRustBuffer(out)
	return decodeRequiredByteArray(copyRustBuffer(out))
}

func mergeTreeBuffers(base Tree, left Tree, right Tree) (C.RustBuffer, C.RustBuffer, C.RustBuffer, error) {
	baseBuf, err := rustBufferFromBytes(base.raw)
	if err != nil {
		return C.RustBuffer{}, C.RustBuffer{}, C.RustBuffer{}, err
	}
	leftBuf, err := rustBufferFromBytes(left.raw)
	if err != nil {
		return C.RustBuffer{}, C.RustBuffer{}, C.RustBuffer{}, err
	}
	rightBuf, err := rustBufferFromBytes(right.raw)
	if err != nil {
		return C.RustBuffer{}, C.RustBuffer{}, C.RustBuffer{}, err
	}
	return baseBuf, leftBuf, rightBuf, nil
}

func mergeRangeTreeBuffers(base Tree, left Tree, right Tree, start []byte, end []byte) (C.RustBuffer, C.RustBuffer, C.RustBuffer, C.RustBuffer, C.RustBuffer, error) {
	baseBuf, leftBuf, rightBuf, err := mergeTreeBuffers(base, left, right)
	if err != nil {
		return C.RustBuffer{}, C.RustBuffer{}, C.RustBuffer{}, C.RustBuffer{}, C.RustBuffer{}, err
	}
	startBuf, err := rustBufferFromBytes(encodeByteArray(start))
	if err != nil {
		return C.RustBuffer{}, C.RustBuffer{}, C.RustBuffer{}, C.RustBuffer{}, C.RustBuffer{}, err
	}
	endBuf, err := rustBufferFromBytes(encodeOptionalByteArray(end))
	if err != nil {
		return C.RustBuffer{}, C.RustBuffer{}, C.RustBuffer{}, C.RustBuffer{}, C.RustBuffer{}, err
	}
	return baseBuf, leftBuf, rightBuf, startBuf, endBuf, nil
}

func mergePrefixTreeBuffers(base Tree, left Tree, right Tree, prefix []byte) (C.RustBuffer, C.RustBuffer, C.RustBuffer, C.RustBuffer, error) {
	baseBuf, leftBuf, rightBuf, err := mergeTreeBuffers(base, left, right)
	if err != nil {
		return C.RustBuffer{}, C.RustBuffer{}, C.RustBuffer{}, C.RustBuffer{}, err
	}
	prefixBuf, err := rustBufferFromBytes(encodeByteArray(prefix))
	if err != nil {
		return C.RustBuffer{}, C.RustBuffer{}, C.RustBuffer{}, C.RustBuffer{}, err
	}
	return baseBuf, leftBuf, rightBuf, prefixBuf, nil
}

func mergeBuffers(base Tree, left Tree, right Tree, resolver string) (C.RustBuffer, C.RustBuffer, C.RustBuffer, C.RustBuffer, error) {
	baseBuf, leftBuf, rightBuf, err := mergeTreeBuffers(base, left, right)
	if err != nil {
		return C.RustBuffer{}, C.RustBuffer{}, C.RustBuffer{}, C.RustBuffer{}, err
	}
	var resolverBytes bytes.Buffer
	encodeOptionalString(&resolverBytes, optionalString(resolver))
	resolverBuf, err := rustBufferFromBytes(resolverBytes.Bytes())
	if err != nil {
		return C.RustBuffer{}, C.RustBuffer{}, C.RustBuffer{}, C.RustBuffer{}, err
	}
	return baseBuf, leftBuf, rightBuf, resolverBuf, nil
}

func mergeRangeBuffers(base Tree, left Tree, right Tree, start []byte, end []byte, resolver string) (C.RustBuffer, C.RustBuffer, C.RustBuffer, C.RustBuffer, C.RustBuffer, C.RustBuffer, error) {
	baseBuf, leftBuf, rightBuf, startBuf, endBuf, err := mergeRangeTreeBuffers(base, left, right, start, end)
	if err != nil {
		return C.RustBuffer{}, C.RustBuffer{}, C.RustBuffer{}, C.RustBuffer{}, C.RustBuffer{}, C.RustBuffer{}, err
	}
	var resolverBytes bytes.Buffer
	encodeOptionalString(&resolverBytes, optionalString(resolver))
	resolverBuf, err := rustBufferFromBytes(resolverBytes.Bytes())
	if err != nil {
		return C.RustBuffer{}, C.RustBuffer{}, C.RustBuffer{}, C.RustBuffer{}, C.RustBuffer{}, C.RustBuffer{}, err
	}
	return baseBuf, leftBuf, rightBuf, startBuf, endBuf, resolverBuf, nil
}

func mergePrefixBuffers(base Tree, left Tree, right Tree, prefix []byte, resolver string) (C.RustBuffer, C.RustBuffer, C.RustBuffer, C.RustBuffer, C.RustBuffer, error) {
	baseBuf, leftBuf, rightBuf, prefixBuf, err := mergePrefixTreeBuffers(base, left, right, prefix)
	if err != nil {
		return C.RustBuffer{}, C.RustBuffer{}, C.RustBuffer{}, C.RustBuffer{}, C.RustBuffer{}, err
	}
	var resolverBytes bytes.Buffer
	encodeOptionalString(&resolverBytes, optionalString(resolver))
	resolverBuf, err := rustBufferFromBytes(resolverBytes.Bytes())
	if err != nil {
		return C.RustBuffer{}, C.RustBuffer{}, C.RustBuffer{}, C.RustBuffer{}, C.RustBuffer{}, err
	}
	return baseBuf, leftBuf, rightBuf, prefixBuf, resolverBuf, nil
}

func (e *Engine) largeValueReadArgs(blobStore *BlobStore, tree Tree, key []byte) (C.uint64_t, C.uint64_t, C.RustBuffer, C.RustBuffer, error) {
	handle, err := e.cloneHandle()
	if err != nil {
		return 0, 0, C.RustBuffer{}, C.RustBuffer{}, err
	}
	blobHandle, err := blobStore.cloneHandle()
	if err != nil {
		return 0, 0, C.RustBuffer{}, C.RustBuffer{}, err
	}
	treeBuf, err := rustBufferFromBytes(tree.raw)
	if err != nil {
		return 0, 0, C.RustBuffer{}, C.RustBuffer{}, err
	}
	keyBuf, err := rustBufferFromBytes(encodeByteArray(key))
	if err != nil {
		return 0, 0, C.RustBuffer{}, C.RustBuffer{}, err
	}
	return handle, blobHandle, treeBuf, keyBuf, nil
}

func (e *Engine) cloneHandle() (C.uint64_t, error) {
	if e == nil || e.closed.Load() || e.handle == 0 {
		return 0, errors.New("prolly engine is closed")
	}
	var status C.RustCallStatus
	handle := C.uniffi_prolly_bindings_fn_clone_prollyengine(e.handle, &status)
	if err := statusError(&status); err != nil {
		return 0, err
	}
	return handle, nil
}

func (s *BlobStore) cloneHandle() (C.uint64_t, error) {
	if s == nil || s.closed.Load() || s.handle == 0 {
		return 0, errors.New("prolly blob store is closed")
	}
	var status C.RustCallStatus
	handle := C.uniffi_prolly_bindings_fn_clone_prollyblobstore(s.handle, &status)
	if err := statusError(&status); err != nil {
		return 0, err
	}
	return handle, nil
}

func (p *MergePolicyRegistry) cloneHandle() (C.uint64_t, error) {
	if p == nil || p.closed.Load() || p.handle == 0 {
		return 0, errors.New("prolly merge policy registry is closed")
	}
	var status C.RustCallStatus
	handle := C.uniffi_prolly_bindings_fn_clone_mergepolicyregistry(p.handle, &status)
	if err := statusError(&status); err != nil {
		return 0, err
	}
	return handle, nil
}

func (p *MergePolicyRegistry) retainResolverHandle(handle uint64) {
	p.mu.Lock()
	p.resolverHandles = append(p.resolverHandles, handle)
	p.mu.Unlock()
}

func (p *MergePolicyRegistry) setDefaultResolverHandle(resolverHandle uint64) error {
	handle, err := p.cloneHandle()
	if err != nil {
		return err
	}
	var status C.RustCallStatus
	C.uniffi_prolly_bindings_fn_method_mergepolicyregistry_set_default_resolver(handle, C.uint64_t(resolverHandle), &status)
	return statusError(&status)
}

func (p *MergePolicyRegistry) pushPrefixResolverHandle(prefix []byte, resolverHandle uint64) error {
	handle, err := p.cloneHandle()
	if err != nil {
		return err
	}
	prefixBuf, err := rustBufferFromBytes(encodeByteArray(prefix))
	if err != nil {
		return err
	}
	var status C.RustCallStatus
	C.uniffi_prolly_bindings_fn_method_mergepolicyregistry_push_prefix_resolver(handle, prefixBuf, C.uint64_t(resolverHandle), &status)
	return statusError(&status)
}

func (p *MergePolicyRegistry) pushExactResolverHandle(key []byte, resolverHandle uint64) error {
	handle, err := p.cloneHandle()
	if err != nil {
		return err
	}
	keyBuf, err := rustBufferFromBytes(encodeByteArray(key))
	if err != nil {
		return err
	}
	var status C.RustCallStatus
	C.uniffi_prolly_bindings_fn_method_mergepolicyregistry_push_exact_resolver(handle, keyBuf, C.uint64_t(resolverHandle), &status)
	return statusError(&status)
}

func rustBufferFromBytes(data []byte) (C.RustBuffer, error) {
	var status C.RustCallStatus
	buf := C.ffi_prolly_bindings_rustbuffer_alloc(C.uint64_t(len(data)), &status)
	if err := statusError(&status); err != nil {
		return C.RustBuffer{}, err
	}
	if len(data) > 0 {
		C.memcpy(unsafe.Pointer(buf.data), unsafe.Pointer(&data[0]), C.size_t(len(data)))
	}
	buf.len = C.uint64_t(len(data))
	return buf, nil
}

func copyRustBuffer(buf C.RustBuffer) []byte {
	if buf.len == 0 || buf.data == nil {
		return nil
	}
	return C.GoBytes(unsafe.Pointer(buf.data), C.int(buf.len))
}

func freeRustBuffer(buf C.RustBuffer) {
	var status C.RustCallStatus
	C.ffi_prolly_bindings_rustbuffer_free(buf, &status)
	_ = statusError(&status)
}

func statusError(status *C.RustCallStatus) error {
	if status.code == 0 {
		return nil
	}
	message := fmt.Sprintf("prolly rust call failed with status code %d", int(status.code))
	if status.error_buf.data != nil {
		payload := copyRustBuffer(status.error_buf)
		freeRustBuffer(status.error_buf)
		if len(payload) > 0 {
			message = fmt.Sprintf("%s: %x", message, payload)
		}
	}
	return errors.New(message)
}

func encodeByteArray(value []byte) []byte {
	var out bytes.Buffer
	encodeByteArrayInto(&out, value)
	return out.Bytes()
}

func encodeByteArrayInto(out *bytes.Buffer, value []byte) {
	_ = binary.Write(out, binary.BigEndian, int32(len(value)))
	out.Write(value)
}

func encodeByteArraySequence(values [][]byte) []byte {
	var out bytes.Buffer
	writeI32(&out, int32(len(values)))
	for _, value := range values {
		encodeByteArrayInto(&out, value)
	}
	return out.Bytes()
}

func encodeOptionalByteArray(value []byte) []byte {
	var out bytes.Buffer
	encodeOptionalByteArrayInto(&out, value)
	return out.Bytes()
}

func encodeOptionalByteArrayInto(out *bytes.Buffer, value []byte) {
	if value == nil {
		out.WriteByte(0)
		return
	}
	out.WriteByte(1)
	encodeByteArrayInto(out, value)
}

func encodeOptionalRangeCursor(cursor *RangeCursor) []byte {
	var out bytes.Buffer
	if cursor == nil {
		out.WriteByte(0)
		return out.Bytes()
	}
	out.WriteByte(1)
	encodeOptionalByteArrayInto(&out, cursor.AfterKey)
	return out.Bytes()
}

func encodeOptionalReverseCursor(cursor *ReverseCursor) []byte {
	var out bytes.Buffer
	if cursor == nil {
		out.WriteByte(0)
		return out.Bytes()
	}
	out.WriteByte(1)
	encodeOptionalByteArrayInto(&out, cursor.BeforeKey)
	return out.Bytes()
}

func encodeOptionalStructuralDiffCursor(cursor *StructuralDiffCursor) []byte {
	var out bytes.Buffer
	if cursor == nil {
		out.WriteByte(0)
		return out.Bytes()
	}
	out.WriteByte(1)
	encodeOptionalByteArrayInto(&out, cursor.BaseRoot)
	encodeOptionalByteArrayInto(&out, cursor.OtherRoot)
	encodeStructuralDiffMarkersInto(&out, cursor.Markers)
	encodeDiffsInto(&out, cursor.Pending)
	return out.Bytes()
}

func encodeStructuralDiffMarkersInto(out *bytes.Buffer, markers []StructuralDiffMarker) {
	writeI32(out, int32(len(markers)))
	for _, marker := range markers {
		writeI32(out, encodeStructuralDiffMarkerKind(marker.Kind))
		encodeOptionalByteArrayInto(out, marker.BaseCID)
		encodeOptionalByteArrayInto(out, marker.OtherCID)
		if marker.HasSpanEnd {
			encodeOptionalByteArrayInto(out, marker.SpanEnd)
		} else {
			encodeOptionalByteArrayInto(out, nil)
		}
		encodeOptionalByteArrayInto(out, marker.CID)
	}
}

func encodeDiffsInto(out *bytes.Buffer, diffs []Diff) {
	writeI32(out, int32(len(diffs)))
	for _, diff := range diffs {
		writeI32(out, encodeDiffKind(diff.Kind))
		encodeByteArrayInto(out, diff.Key)
		encodeOptionalByteArrayInto(out, diff.Value)
		encodeOptionalByteArrayInto(out, diff.OldValue)
		encodeOptionalByteArrayInto(out, diff.NewValue)
	}
}

func encodeOptionalTree(tree *Tree) []byte {
	if tree == nil {
		return []byte{0}
	}
	out := make([]byte, 0, len(tree.raw)+1)
	out = append(out, 1)
	out = append(out, tree.raw...)
	return out
}

func encodeTrees(trees []Tree) []byte {
	var out bytes.Buffer
	writeI32(&out, int32(len(trees)))
	for _, tree := range trees {
		out.Write(tree.raw)
	}
	return out.Bytes()
}

func encodeSnapshotBundle(bundle SnapshotBundle) []byte {
	var out bytes.Buffer
	writeU32(&out, bundle.FormatVersion)
	out.Write(bundle.Tree.raw)
	writeI32(&out, int32(len(bundle.Nodes)))
	for _, node := range bundle.Nodes {
		encodeByteArrayInto(&out, node.CID)
		encodeByteArrayInto(&out, node.Bytes)
	}
	return out.Bytes()
}

func encodeEntries(entries []Entry) []byte {
	var out bytes.Buffer
	writeI32(&out, int32(len(entries)))
	for _, entry := range entries {
		encodeByteArrayInto(&out, entry.Key)
		encodeByteArrayInto(&out, entry.Value)
	}
	return out.Bytes()
}

func encodeMutations(mutations []Mutation) ([]byte, error) {
	var out bytes.Buffer
	writeI32(&out, int32(len(mutations)))
	for _, mutation := range mutations {
		switch mutation.Kind {
		case "upsert":
			writeI32(&out, 1)
		case "delete":
			writeI32(&out, 2)
		default:
			return nil, fmt.Errorf("unknown mutation kind %q", mutation.Kind)
		}
		encodeByteArrayInto(&out, mutation.Key)
		encodeOptionalByteArrayInto(&out, mutation.Value)
	}
	return out.Bytes(), nil
}

func encodeKeyProof(proof KeyProof) ([]byte, error) {
	var out bytes.Buffer
	if proof.HasRoot {
		encodeOptionalByteArrayInto(&out, proof.Root)
	} else {
		encodeOptionalByteArrayInto(&out, nil)
	}
	encodeByteArrayInto(&out, proof.Key)
	writeI32(&out, int32(len(proof.Path)))
	for _, node := range proof.Path {
		out.Write(node)
	}
	return out.Bytes(), nil
}

func encodeMultiKeyProof(proof MultiKeyProof) ([]byte, error) {
	var out bytes.Buffer
	if proof.HasRoot {
		encodeOptionalByteArrayInto(&out, proof.Root)
	} else {
		encodeOptionalByteArrayInto(&out, nil)
	}
	out.Write(encodeByteArraySequence(proof.Keys))
	writeI32(&out, int32(len(proof.Path)))
	for _, node := range proof.Path {
		out.Write(node)
	}
	return out.Bytes(), nil
}

func encodeRangeProof(proof RangeProof) ([]byte, error) {
	var out bytes.Buffer
	if proof.HasRoot {
		encodeOptionalByteArrayInto(&out, proof.Root)
	} else {
		encodeOptionalByteArrayInto(&out, nil)
	}
	encodeByteArrayInto(&out, proof.Start)
	if proof.HasEnd {
		encodeOptionalByteArrayInto(&out, proof.End)
	} else {
		encodeOptionalByteArrayInto(&out, nil)
	}
	writeI32(&out, int32(len(proof.Path)))
	for _, node := range proof.Path {
		out.Write(node)
	}
	return out.Bytes(), nil
}

func encodeRangePageProof(proof RangePageProof) ([]byte, error) {
	var out bytes.Buffer
	if proof.HasRoot {
		encodeOptionalByteArrayInto(&out, proof.Root)
	} else {
		encodeOptionalByteArrayInto(&out, nil)
	}
	if proof.HasAfter {
		encodeOptionalByteArrayInto(&out, proof.After)
	} else {
		encodeOptionalByteArrayInto(&out, nil)
	}
	if proof.HasEnd {
		encodeOptionalByteArrayInto(&out, proof.End)
	} else {
		encodeOptionalByteArrayInto(&out, nil)
	}
	writeI32(&out, int32(len(proof.Path)))
	for _, node := range proof.Path {
		out.Write(node)
	}
	return out.Bytes(), nil
}

func encodeDiffPageProof(proof DiffPageProof) ([]byte, error) {
	var out bytes.Buffer
	base, err := encodeRangePageProof(proof.Base)
	if err != nil {
		return nil, err
	}
	out.Write(base)
	other, err := encodeRangePageProof(proof.Other)
	if err != nil {
		return nil, err
	}
	out.Write(other)
	if proof.HasLookaheadBase {
		out.WriteByte(1)
		lookaheadBase, err := encodeKeyProof(proof.LookaheadBase)
		if err != nil {
			return nil, err
		}
		out.Write(lookaheadBase)
	} else {
		out.WriteByte(0)
	}
	if proof.HasLookaheadOther {
		out.WriteByte(1)
		lookaheadOther, err := encodeKeyProof(proof.LookaheadOther)
		if err != nil {
			return nil, err
		}
		out.Write(lookaheadOther)
	} else {
		out.WriteByte(0)
	}
	if proof.HasRequestedEnd {
		encodeOptionalByteArrayInto(&out, proof.RequestedEnd)
	} else {
		encodeOptionalByteArrayInto(&out, nil)
	}
	writeU64(&out, proof.Limit)
	return out.Bytes(), nil
}

func encodeAuthenticatedProofEnvelope(envelope AuthenticatedProofEnvelope) ([]byte, error) {
	var out bytes.Buffer
	encodeStringInto(&out, envelope.Algorithm)
	encodeByteArrayInto(&out, envelope.KeyID)
	encodeByteArrayInto(&out, envelope.ProofBundle)
	encodeByteArrayInto(&out, envelope.Context)
	if envelope.HasIssuedAtMillis {
		issuedAt := envelope.IssuedAtMillis
		encodeOptionalU64(&out, &issuedAt)
	} else {
		encodeOptionalU64(&out, nil)
	}
	if envelope.HasExpiresAtMillis {
		expiresAt := envelope.ExpiresAtMillis
		encodeOptionalU64(&out, &expiresAt)
	} else {
		encodeOptionalU64(&out, nil)
	}
	encodeByteArrayInto(&out, envelope.Nonce)
	encodeByteArrayInto(&out, envelope.Signature)
	return out.Bytes(), nil
}

func encodeResolution(resolution Resolution) ([]byte, error) {
	var out bytes.Buffer
	switch resolution.Kind {
	case "value":
		writeI32(&out, 1)
	case "delete":
		writeI32(&out, 2)
	case "unresolved":
		writeI32(&out, 3)
	default:
		return nil, fmt.Errorf("unknown resolution kind %q", resolution.Kind)
	}
	encodeOptionalByteArrayInto(&out, resolution.Value)
	return out.Bytes(), nil
}

func encodeCrdtResolution(resolution CrdtResolution) ([]byte, error) {
	var out bytes.Buffer
	switch resolution.Kind {
	case "value":
		writeI32(&out, 1)
	case "delete":
		writeI32(&out, 2)
	default:
		return nil, fmt.Errorf("unknown CRDT resolution kind %q", resolution.Kind)
	}
	encodeOptionalByteArrayInto(&out, resolution.Value)
	return out.Bytes(), nil
}

func encodeHostStoreBytesResult(result HostStoreResult) []byte {
	var out bytes.Buffer
	if result.Err != nil {
		encodeOptionalByteArrayInto(&out, nil)
		encodeOptionalErrorInto(&out, result.Err)
		return out.Bytes()
	}
	if result.Ok {
		encodeOptionalByteArrayInto(&out, result.Value)
	} else {
		encodeOptionalByteArrayInto(&out, nil)
	}
	encodeOptionalErrorInto(&out, nil)
	return out.Bytes()
}

func encodeHostStoreUnitResult(err error) []byte {
	var out bytes.Buffer
	encodeOptionalErrorInto(&out, err)
	return out.Bytes()
}

func encodeHostStoreBoolResult(value bool, err error) []byte {
	var out bytes.Buffer
	if value {
		out.WriteByte(1)
	} else {
		out.WriteByte(0)
	}
	encodeOptionalErrorInto(&out, err)
	return out.Bytes()
}

func encodeHostStoreBatchGetResult(values []HostStoreResult, err error) []byte {
	var out bytes.Buffer
	if err != nil {
		writeI32(&out, 0)
		encodeOptionalErrorInto(&out, err)
		return out.Bytes()
	}
	writeI32(&out, int32(len(values)))
	for _, value := range values {
		if value.Err != nil || !value.Ok {
			encodeOptionalByteArrayInto(&out, nil)
			continue
		}
		encodeOptionalByteArrayInto(&out, value.Value)
	}
	encodeOptionalErrorInto(&out, nil)
	return out.Bytes()
}

func encodeHostStoreListBytesResult(values [][]byte, err error) []byte {
	var out bytes.Buffer
	if err != nil {
		writeI32(&out, 0)
		encodeOptionalErrorInto(&out, err)
		return out.Bytes()
	}
	writeI32(&out, int32(len(values)))
	for _, value := range values {
		encodeByteArrayInto(&out, value)
	}
	encodeOptionalErrorInto(&out, nil)
	return out.Bytes()
}

func encodeHostStoreRootResult(manifest *RootManifest, err error) []byte {
	var out bytes.Buffer
	encodeOptionalRootManifestInto(&out, manifest)
	encodeOptionalErrorInto(&out, err)
	return out.Bytes()
}

func encodeHostStoreCasResult(result HostStoreCasResult) []byte {
	var out bytes.Buffer
	if result.Applied {
		out.WriteByte(1)
	} else {
		out.WriteByte(0)
	}
	encodeOptionalRootManifestInto(&out, result.Current)
	encodeOptionalErrorInto(&out, result.Err)
	return out.Bytes()
}

func encodeHostStoreListRootsResult(roots []NamedRootManifest, err error) []byte {
	var out bytes.Buffer
	if err != nil {
		writeI32(&out, 0)
		encodeOptionalErrorInto(&out, err)
		return out.Bytes()
	}
	writeI32(&out, int32(len(roots)))
	for _, root := range roots {
		encodeByteArrayInto(&out, root.Name)
		out.Write(root.Manifest.raw)
	}
	encodeOptionalErrorInto(&out, nil)
	return out.Bytes()
}

func encodeOptionalRootManifestInto(out *bytes.Buffer, manifest *RootManifest) {
	if manifest == nil {
		out.WriteByte(0)
		return
	}
	out.WriteByte(1)
	out.Write(manifest.raw)
}

func encodeOptionalErrorInto(out *bytes.Buffer, err error) {
	if err == nil {
		encodeOptionalString(out, nil)
		return
	}
	message := err.Error()
	encodeOptionalString(out, &message)
}

func encodeNamedRootRetention(retention NamedRootRetention) ([]byte, error) {
	var out bytes.Buffer
	switch retention.Kind {
	case "all":
		writeI32(&out, 1)
	case "exact":
		writeI32(&out, 2)
	case "prefix":
		writeI32(&out, 3)
	case "newest_by_name":
		writeI32(&out, 4)
	case "updated_since":
		writeI32(&out, 5)
	default:
		return nil, fmt.Errorf("unknown named root retention kind %q", retention.Kind)
	}
	out.Write(encodeByteArraySequence(retention.Names))
	encodeByteArrayInto(&out, retention.Prefix)
	encodeOptionalU64(&out, retention.Count)
	encodeOptionalU64(&out, retention.MinUpdatedAtMillis)
	return out.Bytes(), nil
}

func rustBufferFromBytesMustEncodeSnapshotNamespace(namespace SnapshotNamespace) (C.RustBuffer, error) {
	encoded, err := encodeSnapshotNamespace(namespace)
	if err != nil {
		return C.RustBuffer{}, err
	}
	return rustBufferFromBytes(encoded)
}

func encodeSnapshotNamespace(namespace SnapshotNamespace) ([]byte, error) {
	var out bytes.Buffer
	switch namespace.Kind {
	case "branch":
		writeI32(&out, 1)
	case "tag":
		writeI32(&out, 2)
	case "checkpoint":
		writeI32(&out, 3)
	case "custom":
		writeI32(&out, 4)
	default:
		return nil, fmt.Errorf("unknown snapshot namespace kind %q", namespace.Kind)
	}
	encodeOptionalByteArrayInto(&out, namespace.CustomPrefix)
	return out.Bytes(), nil
}

func encodeChangedSpans(spans []ChangedSpan) []byte {
	var out bytes.Buffer
	writeI32(&out, int32(len(spans)))
	for _, span := range spans {
		encodeByteArrayInto(&out, span.Start)
		encodeOptionalByteArrayInto(&out, span.End)
	}
	return out.Bytes()
}

func encodeBlobRef(ref BlobRef) []byte {
	var out bytes.Buffer
	encodeByteArrayInto(&out, ref.Cid)
	writeU64(&out, ref.Len)
	return out.Bytes()
}

func encodeBlobRefs(refs []BlobRef) []byte {
	var out bytes.Buffer
	writeI32(&out, int32(len(refs)))
	for _, ref := range refs {
		out.Write(encodeBlobRef(ref))
	}
	return out.Bytes()
}

func encodeLargeValueConfig(config LargeValueConfig) []byte {
	var out bytes.Buffer
	writeU64(&out, config.InlineThreshold)
	return out.Bytes()
}

func encodeParallelConfig(config ParallelConfig) []byte {
	var out bytes.Buffer
	writeU64(&out, config.MaxThreads)
	writeU64(&out, config.ParallelismThreshold)
	return out.Bytes()
}

func encodeCrdtDeletePolicy(policy string) ([]byte, error) {
	var out bytes.Buffer
	switch policy {
	case "delete_wins":
		writeI32(&out, 1)
	case "update_wins":
		writeI32(&out, 2)
	default:
		return nil, fmt.Errorf("unknown CRDT delete policy %q", policy)
	}
	return out.Bytes(), nil
}

func encodeTimestampedValue(record TimestampedValue) []byte {
	var out bytes.Buffer
	encodeByteArrayInto(&out, record.Value)
	writeU64(&out, record.Timestamp)
	return out.Bytes()
}

func encodeTombstone(record Tombstone) []byte {
	var out bytes.Buffer
	encodeByteArrayInto(&out, record.Actor)
	writeU64(&out, record.TimestampMillis)
	writeI32(&out, int32(len(record.CausalMetadata)))
	for _, metadata := range record.CausalMetadata {
		encodeStringInto(&out, metadata.Key)
		encodeByteArrayInto(&out, metadata.Value)
	}
	return out.Bytes()
}

func encodeStringInto(out *bytes.Buffer, value string) {
	encodeByteArrayInto(out, []byte(value))
}

func decodeOptionalByteArray(data []byte) ([]byte, bool, error) {
	decoder := byteDecoder{data: data}
	value, ok, err := decoder.readOptionalByteArray()
	if err != nil {
		return nil, false, err
	}
	return value, ok, decoder.done()
}

func decodeRequiredByteArray(data []byte) ([]byte, error) {
	decoder := byteDecoder{data: data}
	value, err := decoder.readByteArray()
	if err != nil {
		return nil, err
	}
	return value, decoder.done()
}

func decodeEntries(data []byte) ([]Entry, error) {
	decoder := byteDecoder{data: data}
	entries, err := decoder.readEntries()
	if err != nil {
		return nil, err
	}
	return entries, decoder.done()
}

func decodeOptionalEntry(data []byte) (*Entry, error) {
	decoder := byteDecoder{data: data}
	entry, err := decoder.readOptionalEntry()
	if err != nil {
		return nil, err
	}
	return entry, decoder.done()
}

func decodeBatchApplyResult(data []byte) (BatchApplyResult, error) {
	decoder := byteDecoder{data: data}
	tree, err := decoder.readTree()
	if err != nil {
		return BatchApplyResult{}, err
	}
	stats, err := decoder.readBatchApplyStats()
	if err != nil {
		return BatchApplyResult{}, err
	}
	return BatchApplyResult{Tree: tree, Stats: stats}, decoder.done()
}

func decodeRangePage(data []byte) (RangePage, error) {
	decoder := byteDecoder{data: data}
	page, err := decoder.readRangePage()
	if err != nil {
		return RangePage{}, err
	}
	return page, decoder.done()
}

func decodeReversePage(data []byte) (ReversePage, error) {
	decoder := byteDecoder{data: data}
	page, err := decoder.readReversePage()
	if err != nil {
		return ReversePage{}, err
	}
	return page, decoder.done()
}

func decodeCursorWindow(data []byte) (CursorWindow, error) {
	decoder := byteDecoder{data: data}
	window, err := decoder.readCursorWindow()
	if err != nil {
		return CursorWindow{}, err
	}
	return window, decoder.done()
}

func decodeProvedRangePage(data []byte) (ProvedRangePage, error) {
	decoder := byteDecoder{data: data}
	page, err := decoder.readRangePage()
	if err != nil {
		return ProvedRangePage{}, err
	}
	proof, err := decoder.readRangePageProof()
	if err != nil {
		return ProvedRangePage{}, err
	}
	return ProvedRangePage{Page: page, Proof: proof}, decoder.done()
}

func decodeProvedDiffPage(data []byte) (ProvedDiffPage, error) {
	decoder := byteDecoder{data: data}
	page, err := decoder.readDiffPage()
	if err != nil {
		return ProvedDiffPage{}, err
	}
	proof, err := decoder.readDiffPageProof()
	if err != nil {
		return ProvedDiffPage{}, err
	}
	return ProvedDiffPage{Page: page, Proof: proof}, decoder.done()
}

func decodeDiffs(data []byte) ([]Diff, error) {
	decoder := byteDecoder{data: data}
	diffs, err := decoder.readDiffs()
	if err != nil {
		return nil, err
	}
	return diffs, decoder.done()
}

func decodeDiffPage(data []byte) (DiffPage, error) {
	decoder := byteDecoder{data: data}
	page, err := decoder.readDiffPage()
	if err != nil {
		return DiffPage{}, err
	}
	return page, decoder.done()
}

func decodeConflictPage(data []byte) (ConflictPage, error) {
	decoder := byteDecoder{data: data}
	conflicts, err := decoder.readConflicts()
	if err != nil {
		return ConflictPage{}, err
	}
	cursor, err := decoder.readOptionalRangeCursor()
	if err != nil {
		return ConflictPage{}, err
	}
	return ConflictPage{Conflicts: conflicts, NextCursor: cursor}, decoder.done()
}

func decodeStructuralDiffPage(data []byte) (StructuralDiffPage, error) {
	decoder := byteDecoder{data: data}
	diffs, err := decoder.readDiffs()
	if err != nil {
		return StructuralDiffPage{}, err
	}
	nextCursorJSON, hasNextCursor, err := decoder.readOptionalString()
	if err != nil {
		return StructuralDiffPage{}, err
	}
	stats, err := decoder.readDiffTraversalStats()
	if err != nil {
		return StructuralDiffPage{}, err
	}
	nextCursor, err := decoder.readOptionalStructuralDiffCursor()
	if err != nil {
		return StructuralDiffPage{}, err
	}
	return StructuralDiffPage{
		Diffs:          diffs,
		NextCursorJSON: nextCursorJSON,
		HasNextCursor:  hasNextCursor,
		Stats:          stats,
		NextCursor:     nextCursor,
	}, decoder.done()
}

func decodeMergeExplanation(data []byte) (MergeExplanation, error) {
	decoder := byteDecoder{data: data}
	result, ok, err := decoder.readOptionalTree()
	if err != nil {
		return MergeExplanation{}, err
	}
	errorValue, hasError, err := decoder.readOptionalString()
	if err != nil {
		return MergeExplanation{}, err
	}
	trace, err := decoder.readString()
	if err != nil {
		return MergeExplanation{}, err
	}
	typedTrace, err := decoder.readMergeTrace()
	if err != nil {
		return MergeExplanation{}, err
	}
	explanation := MergeExplanation{
		Error:     errorValue,
		HasError:  hasError,
		TraceJSON: trace,
		Trace:     typedTrace,
	}
	if ok {
		explanation.Result = &result
	}
	return explanation, decoder.done()
}

func decodeNamedRootSelection(data []byte) (NamedRootSelection, error) {
	decoder := byteDecoder{data: data}
	roots, err := decoder.readNamedRoots()
	if err != nil {
		return NamedRootSelection{}, err
	}
	missing, err := decoder.readByteArraySequence()
	if err != nil {
		return NamedRootSelection{}, err
	}
	return NamedRootSelection{Roots: roots, MissingNames: missing}, decoder.done()
}

func decodeSnapshotNamespace(data []byte) (SnapshotNamespace, error) {
	decoder := byteDecoder{data: data}
	kind, err := decoder.readInt32()
	if err != nil {
		return SnapshotNamespace{}, err
	}
	kindName, err := decodeSnapshotNamespaceKind(kind)
	if err != nil {
		return SnapshotNamespace{}, err
	}
	customPrefix, _, err := decoder.readOptionalByteArray()
	if err != nil {
		return SnapshotNamespace{}, err
	}
	return SnapshotNamespace{Kind: kindName, CustomPrefix: customPrefix}, decoder.done()
}

func decodeSnapshotSelection(data []byte) (SnapshotSelection, error) {
	decoder := byteDecoder{data: data}
	snapshots, err := decoder.readSnapshotRoots()
	if err != nil {
		return SnapshotSelection{}, err
	}
	missing, err := decoder.readByteArraySequence()
	if err != nil {
		return SnapshotSelection{}, err
	}
	return SnapshotSelection{Snapshots: snapshots, MissingIDs: missing}, decoder.done()
}

func decodeSnapshotRoots(data []byte) ([]SnapshotRoot, error) {
	decoder := byteDecoder{data: data}
	snapshots, err := decoder.readSnapshotRoots()
	if err != nil {
		return nil, err
	}
	return snapshots, decoder.done()
}

func decodeKeyProof(data []byte) (KeyProof, error) {
	decoder := byteDecoder{data: data}
	proof, err := decoder.readKeyProof()
	if err != nil {
		return KeyProof{}, err
	}
	return proof, decoder.done()
}

func decodeMultiKeyProof(data []byte) (MultiKeyProof, error) {
	decoder := byteDecoder{data: data}
	proof, err := decoder.readMultiKeyProof()
	if err != nil {
		return MultiKeyProof{}, err
	}
	return proof, decoder.done()
}

func decodeRangeProof(data []byte) (RangeProof, error) {
	decoder := byteDecoder{data: data}
	proof, err := decoder.readRangeProof()
	if err != nil {
		return RangeProof{}, err
	}
	return proof, decoder.done()
}

func decodeRangePageProof(data []byte) (RangePageProof, error) {
	decoder := byteDecoder{data: data}
	proof, err := decoder.readRangePageProof()
	if err != nil {
		return RangePageProof{}, err
	}
	return proof, decoder.done()
}

func decodeDiffPageProof(data []byte) (DiffPageProof, error) {
	decoder := byteDecoder{data: data}
	proof, err := decoder.readDiffPageProof()
	if err != nil {
		return DiffPageProof{}, err
	}
	return proof, decoder.done()
}

func decodeProofBundleSummary(data []byte) (ProofBundleSummary, error) {
	decoder := byteDecoder{data: data}
	summary, err := decoder.readProofBundleSummary()
	if err != nil {
		return ProofBundleSummary{}, err
	}
	return summary, decoder.done()
}

func decodeProofBundleVerification(data []byte) (ProofBundleVerification, error) {
	decoder := byteDecoder{data: data}
	summary, err := decoder.readProofBundleSummary()
	if err != nil {
		return ProofBundleVerification{}, err
	}
	valid, err := decoder.readBool()
	if err != nil {
		return ProofBundleVerification{}, err
	}
	existsCount, err := decoder.readUint64()
	if err != nil {
		return ProofBundleVerification{}, err
	}
	absenceCount, err := decoder.readUint64()
	if err != nil {
		return ProofBundleVerification{}, err
	}
	entryCount, err := decoder.readUint64()
	if err != nil {
		return ProofBundleVerification{}, err
	}
	diffCount, err := decoder.readUint64()
	if err != nil {
		return ProofBundleVerification{}, err
	}
	nextCursorValue, err := decoder.readOptionalRangeCursor()
	if err != nil {
		return ProofBundleVerification{}, err
	}
	nextCursor := RangeCursor{}
	hasNextCursor := false
	if nextCursorValue != nil {
		nextCursor = *nextCursorValue
		hasNextCursor = true
	}
	if err := decoder.done(); err != nil {
		return ProofBundleVerification{}, err
	}
	return ProofBundleVerification{
		Summary:       summary,
		Valid:         valid,
		ExistsCount:   existsCount,
		AbsenceCount:  absenceCount,
		EntryCount:    entryCount,
		DiffCount:     diffCount,
		NextCursor:    nextCursor,
		HasNextCursor: hasNextCursor,
	}, nil
}

func (d *byteDecoder) readProofBundleSummary() (ProofBundleSummary, error) {
	version, err := d.readUint64()
	if err != nil {
		return ProofBundleSummary{}, err
	}
	kind, err := d.readString()
	if err != nil {
		return ProofBundleSummary{}, err
	}
	root, hasRoot, err := d.readOptionalByteArray()
	if err != nil {
		return ProofBundleSummary{}, err
	}
	otherRoot, hasOtherRoot, err := d.readOptionalByteArray()
	if err != nil {
		return ProofBundleSummary{}, err
	}
	keyCount, err := d.readUint64()
	if err != nil {
		return ProofBundleSummary{}, err
	}
	pathNodeCount, err := d.readUint64()
	if err != nil {
		return ProofBundleSummary{}, err
	}
	start, hasStart, err := d.readOptionalByteArray()
	if err != nil {
		return ProofBundleSummary{}, err
	}
	end, hasEnd, err := d.readOptionalByteArray()
	if err != nil {
		return ProofBundleSummary{}, err
	}
	after, hasAfter, err := d.readOptionalByteArray()
	if err != nil {
		return ProofBundleSummary{}, err
	}
	requestedEnd, hasRequestedEnd, err := d.readOptionalByteArray()
	if err != nil {
		return ProofBundleSummary{}, err
	}
	limitValue, err := d.readOptionalUint64()
	if err != nil {
		return ProofBundleSummary{}, err
	}
	limit := uint64(0)
	hasLimit := false
	if limitValue != nil {
		limit = *limitValue
		hasLimit = true
	}
	hasLookahead, err := d.readBool()
	if err != nil {
		return ProofBundleSummary{}, err
	}
	return ProofBundleSummary{
		Version:         version,
		Kind:            kind,
		Root:            root,
		HasRoot:         hasRoot,
		OtherRoot:       otherRoot,
		HasOtherRoot:    hasOtherRoot,
		KeyCount:        keyCount,
		PathNodeCount:   pathNodeCount,
		Start:           start,
		HasStart:        hasStart,
		End:             end,
		HasEnd:          hasEnd,
		After:           after,
		HasAfter:        hasAfter,
		RequestedEnd:    requestedEnd,
		HasRequestedEnd: hasRequestedEnd,
		Limit:           limit,
		HasLimit:        hasLimit,
		HasLookahead:    hasLookahead,
	}, nil
}

func decodeAuthenticatedProofEnvelope(data []byte) (AuthenticatedProofEnvelope, error) {
	decoder := byteDecoder{data: data}
	algorithm, err := decoder.readString()
	if err != nil {
		return AuthenticatedProofEnvelope{}, err
	}
	keyID, err := decoder.readByteArray()
	if err != nil {
		return AuthenticatedProofEnvelope{}, err
	}
	proofBundle, err := decoder.readByteArray()
	if err != nil {
		return AuthenticatedProofEnvelope{}, err
	}
	context, err := decoder.readByteArray()
	if err != nil {
		return AuthenticatedProofEnvelope{}, err
	}
	issuedAt, err := decoder.readOptionalUint64()
	if err != nil {
		return AuthenticatedProofEnvelope{}, err
	}
	expiresAt, err := decoder.readOptionalUint64()
	if err != nil {
		return AuthenticatedProofEnvelope{}, err
	}
	nonce, err := decoder.readByteArray()
	if err != nil {
		return AuthenticatedProofEnvelope{}, err
	}
	signature, err := decoder.readByteArray()
	if err != nil {
		return AuthenticatedProofEnvelope{}, err
	}
	if err := decoder.done(); err != nil {
		return AuthenticatedProofEnvelope{}, err
	}
	envelope := AuthenticatedProofEnvelope{
		Algorithm:   algorithm,
		KeyID:       keyID,
		ProofBundle: proofBundle,
		Context:     context,
		Nonce:       nonce,
		Signature:   signature,
	}
	if issuedAt != nil {
		envelope.IssuedAtMillis = *issuedAt
		envelope.HasIssuedAtMillis = true
	}
	if expiresAt != nil {
		envelope.ExpiresAtMillis = *expiresAt
		envelope.HasExpiresAtMillis = true
	}
	return envelope, nil
}

func decodeKeyProofVerification(data []byte) (KeyProofVerification, error) {
	decoder := byteDecoder{data: data}
	verification, err := decoder.readKeyProofVerification()
	if err != nil {
		return KeyProofVerification{}, err
	}
	return verification, decoder.done()
}

func decodeMultiKeyProofVerification(data []byte) (MultiKeyProofVerification, error) {
	decoder := byteDecoder{data: data}
	valid, err := decoder.readBool()
	if err != nil {
		return MultiKeyProofVerification{}, err
	}
	root, hasRoot, err := decoder.readOptionalByteArray()
	if err != nil {
		return MultiKeyProofVerification{}, err
	}
	count, err := decoder.readInt32()
	if err != nil {
		return MultiKeyProofVerification{}, err
	}
	if count < 0 {
		return MultiKeyProofVerification{}, fmt.Errorf("negative multi-key proof result count %d", count)
	}
	results := make([]KeyProofVerification, 0, count)
	for i := int32(0); i < count; i++ {
		result, err := decoder.readKeyProofVerification()
		if err != nil {
			return MultiKeyProofVerification{}, err
		}
		results = append(results, result)
	}
	if err := decoder.done(); err != nil {
		return MultiKeyProofVerification{}, err
	}
	return MultiKeyProofVerification{
		Valid:   valid,
		Root:    root,
		HasRoot: hasRoot,
		Results: results,
	}, nil
}

func decodeRangeProofVerification(data []byte) (RangeProofVerification, error) {
	decoder := byteDecoder{data: data}
	valid, err := decoder.readBool()
	if err != nil {
		return RangeProofVerification{}, err
	}
	root, hasRoot, err := decoder.readOptionalByteArray()
	if err != nil {
		return RangeProofVerification{}, err
	}
	start, err := decoder.readByteArray()
	if err != nil {
		return RangeProofVerification{}, err
	}
	end, hasEnd, err := decoder.readOptionalByteArray()
	if err != nil {
		return RangeProofVerification{}, err
	}
	entries, err := decoder.readEntries()
	if err != nil {
		return RangeProofVerification{}, err
	}
	if err := decoder.done(); err != nil {
		return RangeProofVerification{}, err
	}
	return RangeProofVerification{
		Valid:   valid,
		Root:    root,
		HasRoot: hasRoot,
		Start:   start,
		End:     end,
		HasEnd:  hasEnd,
		Entries: entries,
	}, nil
}

func decodeRangePageProofVerification(data []byte) (RangePageProofVerification, error) {
	decoder := byteDecoder{data: data}
	valid, err := decoder.readBool()
	if err != nil {
		return RangePageProofVerification{}, err
	}
	root, hasRoot, err := decoder.readOptionalByteArray()
	if err != nil {
		return RangePageProofVerification{}, err
	}
	after, hasAfter, err := decoder.readOptionalByteArray()
	if err != nil {
		return RangePageProofVerification{}, err
	}
	end, hasEnd, err := decoder.readOptionalByteArray()
	if err != nil {
		return RangePageProofVerification{}, err
	}
	entries, err := decoder.readEntries()
	if err != nil {
		return RangePageProofVerification{}, err
	}
	if err := decoder.done(); err != nil {
		return RangePageProofVerification{}, err
	}
	return RangePageProofVerification{
		Valid:    valid,
		Root:     root,
		HasRoot:  hasRoot,
		After:    after,
		HasAfter: hasAfter,
		End:      end,
		HasEnd:   hasEnd,
		Entries:  entries,
	}, nil
}

func decodeDiffPageProofVerification(data []byte) (DiffPageProofVerification, error) {
	decoder := byteDecoder{data: data}
	valid, err := decoder.readBool()
	if err != nil {
		return DiffPageProofVerification{}, err
	}
	baseValid, err := decoder.readBool()
	if err != nil {
		return DiffPageProofVerification{}, err
	}
	otherValid, err := decoder.readBool()
	if err != nil {
		return DiffPageProofVerification{}, err
	}
	lookaheadValid, err := decoder.readBool()
	if err != nil {
		return DiffPageProofVerification{}, err
	}
	baseRoot, hasBaseRoot, err := decoder.readOptionalByteArray()
	if err != nil {
		return DiffPageProofVerification{}, err
	}
	otherRoot, hasOtherRoot, err := decoder.readOptionalByteArray()
	if err != nil {
		return DiffPageProofVerification{}, err
	}
	after, hasAfter, err := decoder.readOptionalByteArray()
	if err != nil {
		return DiffPageProofVerification{}, err
	}
	requestedEnd, hasRequestedEnd, err := decoder.readOptionalByteArray()
	if err != nil {
		return DiffPageProofVerification{}, err
	}
	proofEnd, hasProofEnd, err := decoder.readOptionalByteArray()
	if err != nil {
		return DiffPageProofVerification{}, err
	}
	limit, err := decoder.readUint64()
	if err != nil {
		return DiffPageProofVerification{}, err
	}
	diffs, err := decoder.readDiffs()
	if err != nil {
		return DiffPageProofVerification{}, err
	}
	nextCursor, err := decoder.readOptionalRangeCursor()
	if err != nil {
		return DiffPageProofVerification{}, err
	}
	if err := decoder.done(); err != nil {
		return DiffPageProofVerification{}, err
	}
	verification := DiffPageProofVerification{
		Valid:           valid,
		BaseValid:       baseValid,
		OtherValid:      otherValid,
		LookaheadValid:  lookaheadValid,
		BaseRoot:        baseRoot,
		HasBaseRoot:     hasBaseRoot,
		OtherRoot:       otherRoot,
		HasOtherRoot:    hasOtherRoot,
		After:           after,
		HasAfter:        hasAfter,
		RequestedEnd:    requestedEnd,
		HasRequestedEnd: hasRequestedEnd,
		ProofEnd:        proofEnd,
		HasProofEnd:     hasProofEnd,
		Limit:           limit,
		Diffs:           diffs,
	}
	if nextCursor != nil {
		verification.NextCursor = *nextCursor
		verification.HasNextCursor = true
	}
	return verification, nil
}

func decodeAuthenticatedProofEnvelopeVerification(data []byte) (AuthenticatedProofEnvelopeVerification, error) {
	decoder := byteDecoder{data: data}
	verification, err := decoder.readAuthenticatedProofEnvelopeVerification()
	if err != nil {
		return AuthenticatedProofEnvelopeVerification{}, err
	}
	if err := decoder.done(); err != nil {
		return AuthenticatedProofEnvelopeVerification{}, err
	}
	return verification, nil
}

func decodeAuthenticatedProofBundleVerification(data []byte) (AuthenticatedProofBundleVerification, error) {
	decoder := byteDecoder{data: data}
	valid, err := decoder.readBool()
	if err != nil {
		return AuthenticatedProofBundleVerification{}, err
	}
	envelope, err := decoder.readAuthenticatedProofEnvelopeVerification()
	if err != nil {
		return AuthenticatedProofBundleVerification{}, err
	}
	proof, hasProof, err := decoder.readOptionalProofBundleVerification()
	if err != nil {
		return AuthenticatedProofBundleVerification{}, err
	}
	proofError, hasProofError, err := decoder.readOptionalString()
	if err != nil {
		return AuthenticatedProofBundleVerification{}, err
	}
	if err := decoder.done(); err != nil {
		return AuthenticatedProofBundleVerification{}, err
	}
	return AuthenticatedProofBundleVerification{
		Valid:         valid,
		Envelope:      envelope,
		Proof:         proof,
		HasProof:      hasProof,
		ProofError:    proofError,
		HasProofError: hasProofError,
	}, nil
}

func (d *byteDecoder) readAuthenticatedProofEnvelopeVerification() (AuthenticatedProofEnvelopeVerification, error) {
	valid, err := d.readBool()
	if err != nil {
		return AuthenticatedProofEnvelopeVerification{}, err
	}
	signatureValid, err := d.readBool()
	if err != nil {
		return AuthenticatedProofEnvelopeVerification{}, err
	}
	timeValid, err := d.readBool()
	if err != nil {
		return AuthenticatedProofEnvelopeVerification{}, err
	}
	notYetValid, err := d.readBool()
	if err != nil {
		return AuthenticatedProofEnvelopeVerification{}, err
	}
	expired, err := d.readBool()
	if err != nil {
		return AuthenticatedProofEnvelopeVerification{}, err
	}
	algorithm, err := d.readString()
	if err != nil {
		return AuthenticatedProofEnvelopeVerification{}, err
	}
	keyID, err := d.readByteArray()
	if err != nil {
		return AuthenticatedProofEnvelopeVerification{}, err
	}
	proofBundle, err := d.readByteArray()
	if err != nil {
		return AuthenticatedProofEnvelopeVerification{}, err
	}
	context, err := d.readByteArray()
	if err != nil {
		return AuthenticatedProofEnvelopeVerification{}, err
	}
	issuedAt, err := d.readOptionalUint64()
	if err != nil {
		return AuthenticatedProofEnvelopeVerification{}, err
	}
	expiresAt, err := d.readOptionalUint64()
	if err != nil {
		return AuthenticatedProofEnvelopeVerification{}, err
	}
	nonce, err := d.readByteArray()
	if err != nil {
		return AuthenticatedProofEnvelopeVerification{}, err
	}
	verification := AuthenticatedProofEnvelopeVerification{
		Valid:          valid,
		SignatureValid: signatureValid,
		TimeValid:      timeValid,
		NotYetValid:    notYetValid,
		Expired:        expired,
		Algorithm:      algorithm,
		KeyID:          keyID,
		ProofBundle:    proofBundle,
		Context:        context,
		Nonce:          nonce,
	}
	if issuedAt != nil {
		verification.IssuedAtMillis = *issuedAt
		verification.HasIssuedAtMillis = true
	}
	if expiresAt != nil {
		verification.ExpiresAtMillis = *expiresAt
		verification.HasExpiresAtMillis = true
	}
	return verification, nil
}

func (d *byteDecoder) readOptionalProofBundleVerification() (ProofBundleVerification, bool, error) {
	present, err := d.readByte()
	if err != nil {
		return ProofBundleVerification{}, false, err
	}
	if present == 0 {
		return ProofBundleVerification{}, false, nil
	}
	summary, err := d.readProofBundleSummary()
	if err != nil {
		return ProofBundleVerification{}, false, err
	}
	valid, err := d.readBool()
	if err != nil {
		return ProofBundleVerification{}, false, err
	}
	existsCount, err := d.readUint64()
	if err != nil {
		return ProofBundleVerification{}, false, err
	}
	absenceCount, err := d.readUint64()
	if err != nil {
		return ProofBundleVerification{}, false, err
	}
	entryCount, err := d.readUint64()
	if err != nil {
		return ProofBundleVerification{}, false, err
	}
	diffCount, err := d.readUint64()
	if err != nil {
		return ProofBundleVerification{}, false, err
	}
	nextCursorValue, err := d.readOptionalRangeCursor()
	if err != nil {
		return ProofBundleVerification{}, false, err
	}
	nextCursor := RangeCursor{}
	hasNextCursor := false
	if nextCursorValue != nil {
		nextCursor = *nextCursorValue
		hasNextCursor = true
	}
	return ProofBundleVerification{
		Summary:       summary,
		Valid:         valid,
		ExistsCount:   existsCount,
		AbsenceCount:  absenceCount,
		EntryCount:    entryCount,
		DiffCount:     diffCount,
		NextCursor:    nextCursor,
		HasNextCursor: hasNextCursor,
	}, true, nil
}

func (d *byteDecoder) readKeyProofVerification() (KeyProofVerification, error) {
	valid, err := d.readBool()
	if err != nil {
		return KeyProofVerification{}, err
	}
	exists, err := d.readBool()
	if err != nil {
		return KeyProofVerification{}, err
	}
	absence, err := d.readBool()
	if err != nil {
		return KeyProofVerification{}, err
	}
	root, hasRoot, err := d.readOptionalByteArray()
	if err != nil {
		return KeyProofVerification{}, err
	}
	key, err := d.readByteArray()
	if err != nil {
		return KeyProofVerification{}, err
	}
	value, hasValue, err := d.readOptionalByteArray()
	if err != nil {
		return KeyProofVerification{}, err
	}
	return KeyProofVerification{
		Valid:    valid,
		Exists:   exists,
		Absence:  absence,
		Root:     root,
		HasRoot:  hasRoot,
		Key:      key,
		Value:    value,
		HasValue: hasValue,
	}, nil
}

func decodeNamedRoots(data []byte) ([]NamedRoot, error) {
	decoder := byteDecoder{data: data}
	roots, err := decoder.readNamedRoots()
	if err != nil {
		return nil, err
	}
	return roots, decoder.done()
}

func decodeNamedRootManifests(data []byte) ([]NamedRootManifestRecord, error) {
	decoder := byteDecoder{data: data}
	roots, err := decoder.readNamedRootManifests()
	if err != nil {
		return nil, err
	}
	return roots, decoder.done()
}

func decodeNamedRootUpdate(data []byte) (NamedRootUpdate, error) {
	decoder := byteDecoder{data: data}
	applied, err := decoder.readBool()
	if err != nil {
		return NamedRootUpdate{}, err
	}
	conflict, err := decoder.readBool()
	if err != nil {
		return NamedRootUpdate{}, err
	}
	current, ok, err := decoder.readOptionalTree()
	if err != nil {
		return NamedRootUpdate{}, err
	}
	update := NamedRootUpdate{Applied: applied, Conflict: conflict}
	if ok {
		update.Current = &current
	}
	return update, decoder.done()
}

func decodeJsonDocument(data []byte) (string, error) {
	decoder := byteDecoder{data: data}
	json, err := decoder.readString()
	if err != nil {
		return "", err
	}
	return json, decoder.done()
}

func decodeStringRecord(data []byte) (string, error) {
	return string(data), nil
}

func decodeCacheStats(data []byte) (CacheStats, error) {
	decoder := byteDecoder{data: data}
	cachedNodes, err := decoder.readUint64()
	if err != nil {
		return CacheStats{}, err
	}
	cachedBytes, err := decoder.readUint64()
	if err != nil {
		return CacheStats{}, err
	}
	pinnedNodes, err := decoder.readUint64()
	if err != nil {
		return CacheStats{}, err
	}
	pinnedBytes, err := decoder.readUint64()
	if err != nil {
		return CacheStats{}, err
	}
	stats := CacheStats{
		CachedNodes: cachedNodes,
		CachedBytes: cachedBytes,
		PinnedNodes: pinnedNodes,
		PinnedBytes: pinnedBytes,
	}
	return stats, decoder.done()
}

func decodeMetrics(data []byte) (Metrics, error) {
	decoder := byteDecoder{data: data}
	var values [13]uint64
	for i := range values {
		value, err := decoder.readUint64()
		if err != nil {
			return Metrics{}, err
		}
		values[i] = value
	}
	metrics := Metrics{
		NodeCacheHits:      values[0],
		NodeCacheMisses:    values[1],
		NodeCacheEvictions: values[2],
		NodesRead:          values[3],
		BytesRead:          values[4],
		NodesWritten:       values[5],
		BytesWritten:       values[6],
		StoreGetCalls:      values[7],
		StoreBatchGetCalls: values[8],
		StoreBatchGetKeys:  values[9],
		StorePutCalls:      values[10],
		StoreBatchPutCalls: values[11],
		StoreBatchPutNodes: values[12],
	}
	return metrics, decoder.done()
}

func decodeChangedSpanHint(data []byte) (*ChangedSpanHint, error) {
	decoder := byteDecoder{data: data}
	present, err := decoder.readByte()
	if err != nil {
		return nil, err
	}
	if present == 0 {
		return nil, decoder.done()
	}
	if present != 1 {
		return nil, fmt.Errorf("unexpected optional changed span hint flag %d", present)
	}
	baseRoot, baseRootPresent, err := decoder.readOptionalByteArray()
	if err != nil {
		return nil, err
	}
	changedRoot, changedRootPresent, err := decoder.readOptionalByteArray()
	if err != nil {
		return nil, err
	}
	spans, err := decoder.readChangedSpans()
	if err != nil {
		return nil, err
	}
	hint := &ChangedSpanHint{
		BaseRoot:           baseRoot,
		BaseRootPresent:    baseRootPresent,
		ChangedRoot:        changedRoot,
		ChangedRootPresent: changedRootPresent,
		Spans:              spans,
	}
	return hint, decoder.done()
}

func decodeChangedSpan(data []byte) (ChangedSpan, error) {
	decoder := byteDecoder{data: data}
	start, err := decoder.readByteArray()
	if err != nil {
		return ChangedSpan{}, err
	}
	end, _, err := decoder.readOptionalByteArray()
	if err != nil {
		return ChangedSpan{}, err
	}
	return ChangedSpan{Start: start, End: end}, decoder.done()
}

func decodeRangeCursor(data []byte) (RangeCursor, error) {
	decoder := byteDecoder{data: data}
	afterKey, _, err := decoder.readOptionalByteArray()
	if err != nil {
		return RangeCursor{}, err
	}
	return RangeCursor{AfterKey: afterKey}, decoder.done()
}

func decodeReverseCursor(data []byte) (ReverseCursor, error) {
	decoder := byteDecoder{data: data}
	beforeKey, _, err := decoder.readOptionalByteArray()
	if err != nil {
		return ReverseCursor{}, err
	}
	return ReverseCursor{BeforeKey: beforeKey}, decoder.done()
}

func decodeNamedRootRetention(data []byte) (NamedRootRetention, error) {
	decoder := byteDecoder{data: data}
	rawKind, err := decoder.readInt32()
	if err != nil {
		return NamedRootRetention{}, err
	}
	kind, err := decodeNamedRootRetentionKind(rawKind)
	if err != nil {
		return NamedRootRetention{}, err
	}
	names, err := decoder.readByteArraySequence()
	if err != nil {
		return NamedRootRetention{}, err
	}
	prefix, err := decoder.readByteArray()
	if err != nil {
		return NamedRootRetention{}, err
	}
	count, err := decoder.readOptionalUint64()
	if err != nil {
		return NamedRootRetention{}, err
	}
	minUpdatedAtMillis, err := decoder.readOptionalUint64()
	if err != nil {
		return NamedRootRetention{}, err
	}
	return NamedRootRetention{
		Kind:               kind,
		Names:              names,
		Prefix:             prefix,
		Count:              count,
		MinUpdatedAtMillis: minUpdatedAtMillis,
	}, decoder.done()
}

func decodeGcReachability(data []byte) (GcReachability, error) {
	decoder := byteDecoder{data: data}
	reachability, err := decoder.readGcReachability()
	if err != nil {
		return GcReachability{}, err
	}
	return reachability, decoder.done()
}

func decodeGcPlan(data []byte) (GcPlan, error) {
	decoder := byteDecoder{data: data}
	plan, err := decoder.readGcPlan()
	if err != nil {
		return GcPlan{}, err
	}
	return plan, decoder.done()
}

func decodeGcSweep(data []byte) (GcSweep, error) {
	decoder := byteDecoder{data: data}
	plan, err := decoder.readGcPlan()
	if err != nil {
		return GcSweep{}, err
	}
	deletedNodes, err := decoder.readUint64()
	if err != nil {
		return GcSweep{}, err
	}
	deletedBytes, err := decoder.readUint64()
	if err != nil {
		return GcSweep{}, err
	}
	return GcSweep{
		Plan:         plan,
		DeletedNodes: deletedNodes,
		DeletedBytes: deletedBytes,
	}, decoder.done()
}

func decodeMissingNodePlan(data []byte) (MissingNodePlan, error) {
	decoder := byteDecoder{data: data}
	plan, err := decoder.readMissingNodePlan()
	if err != nil {
		return MissingNodePlan{}, err
	}
	return plan, decoder.done()
}

func decodeMissingNodeCopy(data []byte) (MissingNodeCopy, error) {
	decoder := byteDecoder{data: data}
	plan, err := decoder.readMissingNodePlan()
	if err != nil {
		return MissingNodeCopy{}, err
	}
	copiedNodes, err := decoder.readUint64()
	if err != nil {
		return MissingNodeCopy{}, err
	}
	copiedBytes, err := decoder.readUint64()
	if err != nil {
		return MissingNodeCopy{}, err
	}
	return MissingNodeCopy{
		Plan:        plan,
		CopiedNodes: copiedNodes,
		CopiedBytes: copiedBytes,
	}, decoder.done()
}

func decodeSnapshotBundle(data []byte) (SnapshotBundle, error) {
	decoder := byteDecoder{data: data}
	bundle, err := decoder.readSnapshotBundle()
	if err != nil {
		return SnapshotBundle{}, err
	}
	return bundle, decoder.done()
}

func decodeSnapshotBundleSummary(data []byte) (SnapshotBundleSummary, error) {
	decoder := byteDecoder{data: data}
	summary, err := decoder.readSnapshotBundleSummary()
	if err != nil {
		return SnapshotBundleSummary{}, err
	}
	return summary, decoder.done()
}

func decodeSnapshotBundleVerification(data []byte) (SnapshotBundleVerification, error) {
	decoder := byteDecoder{data: data}
	verification, err := decoder.readSnapshotBundleVerification()
	if err != nil {
		return SnapshotBundleVerification{}, err
	}
	return verification, decoder.done()
}

func decodeBlobRef(data []byte) (BlobRef, error) {
	decoder := byteDecoder{data: data}
	ref, err := decoder.readBlobRef()
	if err != nil {
		return BlobRef{}, err
	}
	return ref, decoder.done()
}

func decodeBlobRefs(data []byte) ([]BlobRef, error) {
	decoder := byteDecoder{data: data}
	refs, err := decoder.readBlobRefs()
	if err != nil {
		return nil, err
	}
	return refs, decoder.done()
}

func decodeLargeValueConfig(data []byte) (LargeValueConfig, error) {
	decoder := byteDecoder{data: data}
	inlineThreshold, err := decoder.readUint64()
	if err != nil {
		return LargeValueConfig{}, err
	}
	return LargeValueConfig{InlineThreshold: inlineThreshold}, decoder.done()
}

func decodeParallelConfig(data []byte) (ParallelConfig, error) {
	decoder := byteDecoder{data: data}
	maxThreads, err := decoder.readUint64()
	if err != nil {
		return ParallelConfig{}, err
	}
	parallelismThreshold, err := decoder.readUint64()
	if err != nil {
		return ParallelConfig{}, err
	}
	return ParallelConfig{MaxThreads: maxThreads, ParallelismThreshold: parallelismThreshold}, decoder.done()
}

func decodeRangeBounds(data []byte) (RangeBounds, error) {
	decoder := byteDecoder{data: data}
	start, err := decoder.readByteArray()
	if err != nil {
		return RangeBounds{}, err
	}
	end, _, err := decoder.readOptionalByteArray()
	if err != nil {
		return RangeBounds{}, err
	}
	return RangeBounds{Start: start, End: end}, decoder.done()
}

func decodeCrdtConfig(data []byte) (CrdtConfig, error) {
	decoder := byteDecoder{data: data}
	strategy, err := decoder.readInt32()
	if err != nil {
		return CrdtConfig{}, err
	}
	deletePolicy, err := decoder.readInt32()
	if err != nil {
		return CrdtConfig{}, err
	}
	strategyName, err := decodeCrdtMergeStrategy(strategy)
	if err != nil {
		return CrdtConfig{}, err
	}
	deletePolicyName, err := decodeCrdtDeletePolicy(deletePolicy)
	if err != nil {
		return CrdtConfig{}, err
	}
	if err := decoder.done(); err != nil {
		return CrdtConfig{}, err
	}
	return CrdtConfig{
		Strategy:     strategyName,
		DeletePolicy: deletePolicyName,
		raw:          append([]byte(nil), data...),
	}, nil
}

func decodeTimestampedValue(data []byte) (TimestampedValue, error) {
	decoder := byteDecoder{data: data}
	value, err := decoder.readByteArray()
	if err != nil {
		return TimestampedValue{}, err
	}
	timestamp, err := decoder.readUint64()
	if err != nil {
		return TimestampedValue{}, err
	}
	return TimestampedValue{Value: value, Timestamp: timestamp}, decoder.done()
}

func decodeTombstone(data []byte) (Tombstone, error) {
	decoder := byteDecoder{data: data}
	tombstone, err := decoder.readTombstone()
	if err != nil {
		return Tombstone{}, err
	}
	return tombstone, decoder.done()
}

func decodeOptionalTombstone(data []byte) (*Tombstone, error) {
	decoder := byteDecoder{data: data}
	present, err := decoder.readByte()
	if err != nil {
		return nil, err
	}
	if present == 0 {
		return nil, decoder.done()
	}
	if present != 1 {
		return nil, fmt.Errorf("unexpected optional tombstone flag %d", present)
	}
	tombstone, err := decoder.readTombstone()
	if err != nil {
		return nil, err
	}
	return &tombstone, decoder.done()
}

func decodeMutation(data []byte) (Mutation, error) {
	decoder := byteDecoder{data: data}
	mutation, err := decoder.readMutation()
	if err != nil {
		return Mutation{}, err
	}
	return mutation, decoder.done()
}

func decodeMutations(data []byte) ([]Mutation, error) {
	decoder := byteDecoder{data: data}
	count, err := decoder.readInt32()
	if err != nil {
		return nil, err
	}
	if count < 0 {
		return nil, fmt.Errorf("negative mutation count %d", count)
	}
	mutations := make([]Mutation, 0, count)
	for i := int32(0); i < count; i++ {
		mutation, err := decoder.readMutation()
		if err != nil {
			return nil, err
		}
		mutations = append(mutations, mutation)
	}
	return mutations, decoder.done()
}

func decodeOptionalRootManifest(data []byte) (*RootManifest, error) {
	if len(data) == 0 {
		return nil, errors.New("empty optional root manifest buffer")
	}
	switch data[0] {
	case 0:
		if len(data) != 1 {
			return nil, fmt.Errorf("unexpected trailing optional root manifest bytes: %d", len(data)-1)
		}
		return nil, nil
	case 1:
		return &RootManifest{raw: append([]byte(nil), data[1:]...)}, nil
	default:
		return nil, fmt.Errorf("unexpected optional root manifest flag %d", data[0])
	}
}

func decodeOptionalMutation(data []byte) (*Mutation, error) {
	decoder := byteDecoder{data: data}
	present, err := decoder.readByte()
	if err != nil {
		return nil, err
	}
	if present == 0 {
		return nil, decoder.done()
	}
	if present != 1 {
		return nil, fmt.Errorf("unexpected optional mutation flag %d", present)
	}
	mutation, err := decoder.readMutation()
	if err != nil {
		return nil, err
	}
	return &mutation, decoder.done()
}

func decodeOptionalValueRef(data []byte) (*ValueRef, error) {
	decoder := byteDecoder{data: data}
	present, err := decoder.readByte()
	if err != nil {
		return nil, err
	}
	if present == 0 {
		return nil, decoder.done()
	}
	if present != 1 {
		return nil, fmt.Errorf("unexpected optional value ref flag %d", present)
	}
	valueRef, err := decoder.readValueRef()
	if err != nil {
		return nil, err
	}
	return &valueRef, decoder.done()
}

func decodeValueRef(data []byte) (ValueRef, error) {
	decoder := byteDecoder{data: data}
	valueRef, err := decoder.readValueRef()
	if err != nil {
		return ValueRef{}, err
	}
	return valueRef, decoder.done()
}

func decodeBlobGcReachability(data []byte) (BlobGcReachability, error) {
	decoder := byteDecoder{data: data}
	reachability, err := decoder.readBlobGcReachability()
	if err != nil {
		return BlobGcReachability{}, err
	}
	return reachability, decoder.done()
}

func decodeBlobGcPlan(data []byte) (BlobGcPlan, error) {
	decoder := byteDecoder{data: data}
	plan, err := decoder.readBlobGcPlan()
	if err != nil {
		return BlobGcPlan{}, err
	}
	return plan, decoder.done()
}

func decodeBlobGcSweep(data []byte) (BlobGcSweep, error) {
	decoder := byteDecoder{data: data}
	plan, err := decoder.readBlobGcPlan()
	if err != nil {
		return BlobGcSweep{}, err
	}
	deletedBlobs, err := decoder.readUint64()
	if err != nil {
		return BlobGcSweep{}, err
	}
	deletedBlobBytes, err := decoder.readUint64()
	if err != nil {
		return BlobGcSweep{}, err
	}
	return BlobGcSweep{
		Plan:             plan,
		DeletedBlobs:     deletedBlobs,
		DeletedBlobBytes: deletedBlobBytes,
	}, decoder.done()
}

func decodeOptionalTree(data []byte) (Tree, bool, error) {
	decoder := byteDecoder{data: data}
	tree, ok, err := decoder.readOptionalTree()
	if err != nil {
		return Tree{}, false, err
	}
	return tree, ok, decoder.done()
}

func decodeTree(data []byte) (Tree, error) {
	decoder := byteDecoder{data: data}
	tree, err := decoder.readTree()
	if err != nil {
		return Tree{}, err
	}
	return tree, decoder.done()
}

func (d *byteDecoder) readEntries() ([]Entry, error) {
	count, err := d.readInt32()
	if err != nil {
		return nil, err
	}
	if count < 0 {
		return nil, fmt.Errorf("negative entry count %d", count)
	}
	entries := make([]Entry, 0, count)
	for i := int32(0); i < count; i++ {
		key, err := d.readByteArray()
		if err != nil {
			return nil, err
		}
		value, err := d.readByteArray()
		if err != nil {
			return nil, err
		}
		entries = append(entries, Entry{Key: key, Value: value})
	}
	return entries, nil
}

func (d *byteDecoder) readOptionalEntry() (*Entry, error) {
	present, err := d.readByte()
	if err != nil {
		return nil, err
	}
	if present == 0 {
		return nil, nil
	}
	key, err := d.readByteArray()
	if err != nil {
		return nil, err
	}
	value, err := d.readByteArray()
	if err != nil {
		return nil, err
	}
	return &Entry{Key: key, Value: value}, nil
}

func (d *byteDecoder) readDiffs() ([]Diff, error) {
	count, err := d.readInt32()
	if err != nil {
		return nil, err
	}
	if count < 0 {
		return nil, fmt.Errorf("negative diff count %d", count)
	}
	diffs := make([]Diff, 0, count)
	for i := int32(0); i < count; i++ {
		kind, err := d.readInt32()
		if err != nil {
			return nil, err
		}
		key, err := d.readByteArray()
		if err != nil {
			return nil, err
		}
		value, _, err := d.readOptionalByteArray()
		if err != nil {
			return nil, err
		}
		oldValue, _, err := d.readOptionalByteArray()
		if err != nil {
			return nil, err
		}
		newValue, _, err := d.readOptionalByteArray()
		if err != nil {
			return nil, err
		}
		kindName, err := decodeDiffKind(kind)
		if err != nil {
			return nil, err
		}
		diffs = append(diffs, Diff{
			Kind:     kindName,
			Key:      key,
			Value:    value,
			OldValue: oldValue,
			NewValue: newValue,
		})
	}
	return diffs, nil
}

func (d *byteDecoder) readConflicts() ([]Conflict, error) {
	count, err := d.readInt32()
	if err != nil {
		return nil, err
	}
	if count < 0 {
		return nil, fmt.Errorf("negative conflict count %d", count)
	}
	conflicts := make([]Conflict, 0, count)
	for i := int32(0); i < count; i++ {
		key, err := d.readByteArray()
		if err != nil {
			return nil, err
		}
		base, basePresent, err := d.readOptionalByteArray()
		if err != nil {
			return nil, err
		}
		left, leftPresent, err := d.readOptionalByteArray()
		if err != nil {
			return nil, err
		}
		right, rightPresent, err := d.readOptionalByteArray()
		if err != nil {
			return nil, err
		}
		conflicts = append(conflicts, Conflict{
			Key:          key,
			Base:         base,
			BasePresent:  basePresent,
			Left:         left,
			LeftPresent:  leftPresent,
			Right:        right,
			RightPresent: rightPresent,
		})
	}
	return conflicts, nil
}

func decodeConflict(data []byte) (Conflict, error) {
	decoder := byteDecoder{data: data}
	key, err := decoder.readByteArray()
	if err != nil {
		return Conflict{}, err
	}
	base, basePresent, err := decoder.readOptionalByteArray()
	if err != nil {
		return Conflict{}, err
	}
	left, leftPresent, err := decoder.readOptionalByteArray()
	if err != nil {
		return Conflict{}, err
	}
	right, rightPresent, err := decoder.readOptionalByteArray()
	if err != nil {
		return Conflict{}, err
	}
	if err := decoder.done(); err != nil {
		return Conflict{}, err
	}
	return Conflict{
		Key:          key,
		Base:         base,
		BasePresent:  basePresent,
		Left:         left,
		LeftPresent:  leftPresent,
		Right:        right,
		RightPresent: rightPresent,
	}, nil
}

func (d *byteDecoder) readDiffTraversalStats() (DiffTraversalStats, error) {
	var values [6]uint64
	for i := range values {
		value, err := d.readUint64()
		if err != nil {
			return DiffTraversalStats{}, err
		}
		values[i] = value
	}
	return DiffTraversalStats{
		ComparedNodes:      values[0],
		ReusedSubtrees:     values[1],
		AddedSubtrees:      values[2],
		RemovedSubtrees:    values[3],
		CollectedFallbacks: values[4],
		EmittedDiffs:       values[5],
	}, nil
}

func (d *byteDecoder) readOptionalStructuralDiffCursor() (*StructuralDiffCursor, error) {
	present, err := d.readByte()
	if err != nil {
		return nil, err
	}
	if present == 0 {
		return nil, nil
	}
	cursor, err := d.readStructuralDiffCursor()
	if err != nil {
		return nil, err
	}
	return &cursor, nil
}

func (d *byteDecoder) readStructuralDiffCursor() (StructuralDiffCursor, error) {
	baseRoot, _, err := d.readOptionalByteArray()
	if err != nil {
		return StructuralDiffCursor{}, err
	}
	otherRoot, _, err := d.readOptionalByteArray()
	if err != nil {
		return StructuralDiffCursor{}, err
	}
	markers, err := d.readStructuralDiffMarkers()
	if err != nil {
		return StructuralDiffCursor{}, err
	}
	pending, err := d.readDiffs()
	if err != nil {
		return StructuralDiffCursor{}, err
	}
	return StructuralDiffCursor{
		BaseRoot:  baseRoot,
		OtherRoot: otherRoot,
		Markers:   markers,
		Pending:   pending,
	}, nil
}

func (d *byteDecoder) readStructuralDiffMarkers() ([]StructuralDiffMarker, error) {
	count, err := d.readInt32()
	if err != nil {
		return nil, err
	}
	if count < 0 {
		return nil, fmt.Errorf("negative structural diff marker count %d", count)
	}
	markers := make([]StructuralDiffMarker, 0, count)
	for i := int32(0); i < count; i++ {
		kindValue, err := d.readInt32()
		if err != nil {
			return nil, err
		}
		kind, err := decodeStructuralDiffMarkerKind(kindValue)
		if err != nil {
			return nil, err
		}
		baseCID, _, err := d.readOptionalByteArray()
		if err != nil {
			return nil, err
		}
		otherCID, _, err := d.readOptionalByteArray()
		if err != nil {
			return nil, err
		}
		spanEnd, hasSpanEnd, err := d.readOptionalByteArray()
		if err != nil {
			return nil, err
		}
		cid, _, err := d.readOptionalByteArray()
		if err != nil {
			return nil, err
		}
		markers = append(markers, StructuralDiffMarker{
			Kind:       kind,
			BaseCID:    baseCID,
			OtherCID:   otherCID,
			SpanEnd:    spanEnd,
			HasSpanEnd: hasSpanEnd,
			CID:        cid,
		})
	}
	return markers, nil
}

func (d *byteDecoder) readOptionalDiffTraversalStats() (*DiffTraversalStats, error) {
	present, err := d.readByte()
	if err != nil {
		return nil, err
	}
	if present == 0 {
		return nil, nil
	}
	stats, err := d.readDiffTraversalStats()
	if err != nil {
		return nil, err
	}
	return &stats, nil
}

func (d *byteDecoder) readMergeTrace() (MergeTrace, error) {
	count, err := d.readInt32()
	if err != nil {
		return MergeTrace{}, err
	}
	if count < 0 {
		return MergeTrace{}, fmt.Errorf("negative merge trace event count %d", count)
	}
	events := make([]MergeTraceEvent, 0, count)
	for i := int32(0); i < count; i++ {
		event, err := d.readMergeTraceEvent()
		if err != nil {
			return MergeTrace{}, err
		}
		events = append(events, event)
	}
	return MergeTrace{Events: events}, nil
}

func (d *byteDecoder) readMergeTraceEvent() (MergeTraceEvent, error) {
	kindValue, err := d.readInt32()
	if err != nil {
		return MergeTraceEvent{}, err
	}
	kind, err := decodeMergeTraceEventKind(kindValue)
	if err != nil {
		return MergeTraceEvent{}, err
	}
	fastPath, err := d.readOptionalMergeFastPathKind()
	if err != nil {
		return MergeTraceEvent{}, err
	}
	cid, hasCID, err := d.readOptionalByteArray()
	if err != nil {
		return MergeTraceEvent{}, err
	}
	reuseReason, err := d.readOptionalMergeReuseReasonKind()
	if err != nil {
		return MergeTraceEvent{}, err
	}
	level, err := d.readOptionalUint64()
	if err != nil {
		return MergeTraceEvent{}, err
	}
	entries, err := d.readOptionalUint64()
	if err != nil {
		return MergeTraceEvent{}, err
	}
	firstKey, hasFirstKey, err := d.readOptionalByteArray()
	if err != nil {
		return MergeTraceEvent{}, err
	}
	lastKey, hasLastKey, err := d.readOptionalByteArray()
	if err != nil {
		return MergeTraceEvent{}, err
	}
	stage, err := d.readOptionalMergeTraceStageKind()
	if err != nil {
		return MergeTraceEvent{}, err
	}
	key, hasKey, err := d.readOptionalByteArray()
	if err != nil {
		return MergeTraceEvent{}, err
	}
	resolution, err := d.readOptionalMergeTraceResolutionKind()
	if err != nil {
		return MergeTraceEvent{}, err
	}
	fallbackReason, err := d.readOptionalMergeFallbackReasonKind()
	if err != nil {
		return MergeTraceEvent{}, err
	}
	diffStats, err := d.readOptionalDiffTraversalStats()
	if err != nil {
		return MergeTraceEvent{}, err
	}
	rightChanges, err := d.readOptionalUint64()
	if err != nil {
		return MergeTraceEvent{}, err
	}
	mutations, err := d.readOptionalUint64()
	if err != nil {
		return MergeTraceEvent{}, err
	}
	appendOnly, err := d.readOptionalBool()
	if err != nil {
		return MergeTraceEvent{}, err
	}

	return MergeTraceEvent{
		Kind:           kind,
		FastPath:       fastPath,
		CID:            cid,
		HasCID:         hasCID,
		ReuseReason:    reuseReason,
		Level:          level,
		Entries:        entries,
		FirstKey:       firstKey,
		HasFirstKey:    hasFirstKey,
		LastKey:        lastKey,
		HasLastKey:     hasLastKey,
		Stage:          stage,
		Key:            key,
		HasKey:         hasKey,
		Resolution:     resolution,
		FallbackReason: fallbackReason,
		DiffStats:      diffStats,
		RightChanges:   rightChanges,
		Mutations:      mutations,
		AppendOnly:     appendOnly,
	}, nil
}

func (d *byteDecoder) readBatchApplyStats() (BatchApplyStats, error) {
	inputMutations, err := d.readUint64()
	if err != nil {
		return BatchApplyStats{}, err
	}
	effectiveMutations, err := d.readUint64()
	if err != nil {
		return BatchApplyStats{}, err
	}
	preprocessInputSorted, err := d.readBool()
	if err != nil {
		return BatchApplyStats{}, err
	}
	affectedLeaves, err := d.readUint64()
	if err != nil {
		return BatchApplyStats{}, err
	}
	changedLeaves, err := d.readUint64()
	if err != nil {
		return BatchApplyStats{}, err
	}
	sparseLeafApplies, err := d.readUint64()
	if err != nil {
		return BatchApplyStats{}, err
	}
	writtenNodes, err := d.readUint64()
	if err != nil {
		return BatchApplyStats{}, err
	}
	writtenBytes, err := d.readUint64()
	if err != nil {
		return BatchApplyStats{}, err
	}
	usedAppendFastPath, err := d.readBool()
	if err != nil {
		return BatchApplyStats{}, err
	}
	usedBatchedRoute, err := d.readBool()
	if err != nil {
		return BatchApplyStats{}, err
	}
	usedCoalescedRebuild, err := d.readBool()
	if err != nil {
		return BatchApplyStats{}, err
	}
	usedDeferredRebalancing, err := d.readBool()
	if err != nil {
		return BatchApplyStats{}, err
	}
	usedBottomUpRebuild, err := d.readBool()
	if err != nil {
		return BatchApplyStats{}, err
	}
	cacheWrittenNodes, err := d.readBool()
	if err != nil {
		return BatchApplyStats{}, err
	}
	return BatchApplyStats{
		InputMutations:          inputMutations,
		EffectiveMutations:      effectiveMutations,
		PreprocessInputSorted:   preprocessInputSorted,
		AffectedLeaves:          affectedLeaves,
		ChangedLeaves:           changedLeaves,
		SparseLeafApplies:       sparseLeafApplies,
		WrittenNodes:            writtenNodes,
		WrittenBytes:            writtenBytes,
		UsedAppendFastPath:      usedAppendFastPath,
		UsedBatchedRoute:        usedBatchedRoute,
		UsedCoalescedRebuild:    usedCoalescedRebuild,
		UsedDeferredRebalancing: usedDeferredRebalancing,
		UsedBottomUpRebuild:     usedBottomUpRebuild,
		CacheWrittenNodes:       cacheWrittenNodes,
	}, nil
}

func (d *byteDecoder) readGcReachability() (GcReachability, error) {
	liveCids, err := d.readByteArraySequence()
	if err != nil {
		return GcReachability{}, err
	}
	liveNodes, err := d.readUint64()
	if err != nil {
		return GcReachability{}, err
	}
	liveBytes, err := d.readUint64()
	if err != nil {
		return GcReachability{}, err
	}
	leafNodes, err := d.readUint64()
	if err != nil {
		return GcReachability{}, err
	}
	internalNodes, err := d.readUint64()
	if err != nil {
		return GcReachability{}, err
	}
	return GcReachability{
		LiveCids:      liveCids,
		LiveNodes:     liveNodes,
		LiveBytes:     liveBytes,
		LeafNodes:     leafNodes,
		InternalNodes: internalNodes,
	}, nil
}

func (d *byteDecoder) readGcPlan() (GcPlan, error) {
	reachability, err := d.readGcReachability()
	if err != nil {
		return GcPlan{}, err
	}
	candidateNodes, err := d.readUint64()
	if err != nil {
		return GcPlan{}, err
	}
	reclaimableCids, err := d.readByteArraySequence()
	if err != nil {
		return GcPlan{}, err
	}
	reclaimableNodes, err := d.readUint64()
	if err != nil {
		return GcPlan{}, err
	}
	reclaimableBytes, err := d.readUint64()
	if err != nil {
		return GcPlan{}, err
	}
	missingCandidates, err := d.readUint64()
	if err != nil {
		return GcPlan{}, err
	}
	return GcPlan{
		Reachability:      reachability,
		CandidateNodes:    candidateNodes,
		ReclaimableCids:   reclaimableCids,
		ReclaimableNodes:  reclaimableNodes,
		ReclaimableBytes:  reclaimableBytes,
		MissingCandidates: missingCandidates,
	}, nil
}

func (d *byteDecoder) readMissingNodePlan() (MissingNodePlan, error) {
	requiredCids, err := d.readByteArraySequence()
	if err != nil {
		return MissingNodePlan{}, err
	}
	requiredNodes, err := d.readUint64()
	if err != nil {
		return MissingNodePlan{}, err
	}
	requiredBytes, err := d.readUint64()
	if err != nil {
		return MissingNodePlan{}, err
	}
	missingCids, err := d.readByteArraySequence()
	if err != nil {
		return MissingNodePlan{}, err
	}
	missingNodes, err := d.readUint64()
	if err != nil {
		return MissingNodePlan{}, err
	}
	missingBytes, err := d.readUint64()
	if err != nil {
		return MissingNodePlan{}, err
	}
	return MissingNodePlan{
		RequiredCids:  requiredCids,
		RequiredNodes: requiredNodes,
		RequiredBytes: requiredBytes,
		MissingCids:   missingCids,
		MissingNodes:  missingNodes,
		MissingBytes:  missingBytes,
	}, nil
}

func (d *byteDecoder) readMutation() (Mutation, error) {
	kind, err := d.readInt32()
	if err != nil {
		return Mutation{}, err
	}
	key, err := d.readByteArray()
	if err != nil {
		return Mutation{}, err
	}
	value, _, err := d.readOptionalByteArray()
	if err != nil {
		return Mutation{}, err
	}
	kindName, err := decodeMutationKind(kind)
	if err != nil {
		return Mutation{}, err
	}
	return Mutation{Kind: kindName, Key: key, Value: value}, nil
}

func (d *byteDecoder) readTombstone() (Tombstone, error) {
	actor, err := d.readByteArray()
	if err != nil {
		return Tombstone{}, err
	}
	timestampMillis, err := d.readUint64()
	if err != nil {
		return Tombstone{}, err
	}
	count, err := d.readInt32()
	if err != nil {
		return Tombstone{}, err
	}
	if count < 0 {
		return Tombstone{}, fmt.Errorf("negative tombstone metadata count %d", count)
	}
	metadata := make([]TombstoneMetadata, 0, count)
	for i := int32(0); i < count; i++ {
		key, err := d.readString()
		if err != nil {
			return Tombstone{}, err
		}
		value, err := d.readByteArray()
		if err != nil {
			return Tombstone{}, err
		}
		metadata = append(metadata, TombstoneMetadata{Key: key, Value: value})
	}
	return Tombstone{
		Actor:           actor,
		TimestampMillis: timestampMillis,
		CausalMetadata:  metadata,
	}, nil
}

func (d *byteDecoder) readBlobRef() (BlobRef, error) {
	cid, err := d.readByteArray()
	if err != nil {
		return BlobRef{}, err
	}
	length, err := d.readUint64()
	if err != nil {
		return BlobRef{}, err
	}
	return BlobRef{Cid: cid, Len: length}, nil
}

func (d *byteDecoder) readBlobRefs() ([]BlobRef, error) {
	count, err := d.readInt32()
	if err != nil {
		return nil, err
	}
	if count < 0 {
		return nil, fmt.Errorf("negative blob ref count %d", count)
	}
	refs := make([]BlobRef, 0, count)
	for i := int32(0); i < count; i++ {
		ref, err := d.readBlobRef()
		if err != nil {
			return nil, err
		}
		refs = append(refs, ref)
	}
	return refs, nil
}

func (d *byteDecoder) readOptionalBlobRef() (*BlobRef, error) {
	present, err := d.readByte()
	if err != nil {
		return nil, err
	}
	if present == 0 {
		return nil, nil
	}
	if present != 1 {
		return nil, fmt.Errorf("unexpected optional blob ref flag %d", present)
	}
	ref, err := d.readBlobRef()
	if err != nil {
		return nil, err
	}
	return &ref, nil
}

func (d *byteDecoder) readValueRef() (ValueRef, error) {
	kind, err := d.readInt32()
	if err != nil {
		return ValueRef{}, err
	}
	value, _, err := d.readOptionalByteArray()
	if err != nil {
		return ValueRef{}, err
	}
	blob, err := d.readOptionalBlobRef()
	if err != nil {
		return ValueRef{}, err
	}
	kindName, err := decodeValueRefKind(kind)
	if err != nil {
		return ValueRef{}, err
	}
	return ValueRef{Kind: kindName, Value: value, Blob: blob}, nil
}

func (d *byteDecoder) readBlobGcReachability() (BlobGcReachability, error) {
	liveBlobs, err := d.readBlobRefs()
	if err != nil {
		return BlobGcReachability{}, err
	}
	liveBlobCount, err := d.readUint64()
	if err != nil {
		return BlobGcReachability{}, err
	}
	liveBlobBytes, err := d.readUint64()
	if err != nil {
		return BlobGcReachability{}, err
	}
	scannedNodes, err := d.readUint64()
	if err != nil {
		return BlobGcReachability{}, err
	}
	scannedValues, err := d.readUint64()
	if err != nil {
		return BlobGcReachability{}, err
	}
	return BlobGcReachability{
		LiveBlobs:     liveBlobs,
		LiveBlobCount: liveBlobCount,
		LiveBlobBytes: liveBlobBytes,
		ScannedNodes:  scannedNodes,
		ScannedValues: scannedValues,
	}, nil
}

func (d *byteDecoder) readBlobGcPlan() (BlobGcPlan, error) {
	reachability, err := d.readBlobGcReachability()
	if err != nil {
		return BlobGcPlan{}, err
	}
	candidateBlobs, err := d.readUint64()
	if err != nil {
		return BlobGcPlan{}, err
	}
	reclaimableBlobs, err := d.readBlobRefs()
	if err != nil {
		return BlobGcPlan{}, err
	}
	reclaimableBlobCount, err := d.readUint64()
	if err != nil {
		return BlobGcPlan{}, err
	}
	reclaimableBlobBytes, err := d.readUint64()
	if err != nil {
		return BlobGcPlan{}, err
	}
	missingCandidates, err := d.readUint64()
	if err != nil {
		return BlobGcPlan{}, err
	}
	return BlobGcPlan{
		Reachability:         reachability,
		CandidateBlobs:       candidateBlobs,
		ReclaimableBlobs:     reclaimableBlobs,
		ReclaimableBlobCount: reclaimableBlobCount,
		ReclaimableBlobBytes: reclaimableBlobBytes,
		MissingCandidates:    missingCandidates,
	}, nil
}

func decodeByteArraySequence(data []byte) ([][]byte, error) {
	decoder := byteDecoder{data: data}
	values, err := decoder.readByteArraySequence()
	if err != nil {
		return nil, err
	}
	return values, decoder.done()
}

func decodeOptionalByteArraySequence(data []byte) ([][]byte, []bool, error) {
	decoder := byteDecoder{data: data}
	count, err := decoder.readInt32()
	if err != nil {
		return nil, nil, err
	}
	if count < 0 {
		return nil, nil, fmt.Errorf("negative sequence count %d", count)
	}
	values := make([][]byte, 0, count)
	present := make([]bool, 0, count)
	for i := int32(0); i < count; i++ {
		value, ok, err := decoder.readOptionalByteArray()
		if err != nil {
			return nil, nil, err
		}
		values = append(values, value)
		present = append(present, ok)
	}
	return values, present, decoder.done()
}

func decodeDiffKind(kind int32) (string, error) {
	switch kind {
	case 1:
		return "added", nil
	case 2:
		return "removed", nil
	case 3:
		return "changed", nil
	default:
		return "", fmt.Errorf("unknown diff kind %d", kind)
	}
}

func encodeDiffKind(kind string) int32 {
	switch kind {
	case "added":
		return 1
	case "removed":
		return 2
	case "changed":
		return 3
	default:
		return 3
	}
}

func encodeStructuralDiffMarkerKind(kind string) int32 {
	switch kind {
	case "compare":
		return 1
	case "added":
		return 2
	case "removed":
		return 3
	default:
		return 1
	}
}

func decodeStructuralDiffMarkerKind(kind int32) (string, error) {
	switch kind {
	case 1:
		return "compare", nil
	case 2:
		return "added", nil
	case 3:
		return "removed", nil
	default:
		return "", fmt.Errorf("unknown structural diff marker kind %d", kind)
	}
}

func (d *byteDecoder) readOptionalEnumString(decode func(int32) (string, error)) (string, error) {
	present, err := d.readByte()
	if err != nil {
		return "", err
	}
	if present == 0 {
		return "", nil
	}
	kind, err := d.readInt32()
	if err != nil {
		return "", err
	}
	return decode(kind)
}

func (d *byteDecoder) readOptionalMergeFastPathKind() (string, error) {
	return d.readOptionalEnumString(decodeMergeFastPathKind)
}

func (d *byteDecoder) readOptionalMergeReuseReasonKind() (string, error) {
	return d.readOptionalEnumString(decodeMergeReuseReasonKind)
}

func (d *byteDecoder) readOptionalMergeTraceStageKind() (string, error) {
	return d.readOptionalEnumString(decodeMergeTraceStageKind)
}

func (d *byteDecoder) readOptionalMergeTraceResolutionKind() (string, error) {
	return d.readOptionalEnumString(decodeMergeTraceResolutionKind)
}

func (d *byteDecoder) readOptionalMergeFallbackReasonKind() (string, error) {
	return d.readOptionalEnumString(decodeMergeFallbackReasonKind)
}

func decodeMergeTraceEventKind(kind int32) (string, error) {
	switch kind {
	case 1:
		return "fast_path", nil
	case 2:
		return "structural_merge_started", nil
	case 3:
		return "reused_subtree", nil
	case 4:
		return "rewritten_node", nil
	case 5:
		return "resolver_called", nil
	case 6:
		return "fallback", nil
	case 7:
		return "diff_traversal", nil
	case 8:
		return "batch_merge", nil
	default:
		return "", fmt.Errorf("unknown merge trace event kind %d", kind)
	}
}

func decodeMergeFastPathKind(kind int32) (string, error) {
	switch kind {
	case 1:
		return "branches_equal", nil
	case 2:
		return "left_unchanged", nil
	case 3:
		return "right_unchanged", nil
	default:
		return "", fmt.Errorf("unknown merge fast path kind %d", kind)
	}
}

func decodeMergeReuseReasonKind(kind int32) (string, error) {
	switch kind {
	case 1:
		return "branches_equal", nil
	case 2:
		return "left_unchanged", nil
	case 3:
		return "right_unchanged", nil
	case 4:
		return "unchanged_after_merge", nil
	case 5:
		return "matches_left", nil
	case 6:
		return "matches_right", nil
	default:
		return "", fmt.Errorf("unknown merge reuse reason kind %d", kind)
	}
}

func decodeMergeTraceStageKind(kind int32) (string, error) {
	switch kind {
	case 1:
		return "structural", nil
	case 2:
		return "batch", nil
	default:
		return "", fmt.Errorf("unknown merge trace stage kind %d", kind)
	}
}

func decodeMergeTraceResolutionKind(kind int32) (string, error) {
	switch kind {
	case 1:
		return "value", nil
	case 2:
		return "delete", nil
	case 3:
		return "unresolved", nil
	default:
		return "", fmt.Errorf("unknown merge trace resolution kind %d", kind)
	}
}

func decodeMergeFallbackReasonKind(kind int32) (string, error) {
	switch kind {
	case 1:
		return "missing_root", nil
	case 2:
		return "shape_mismatch", nil
	case 3:
		return "node_length_mismatch", nil
	case 4:
		return "child_fallback", nil
	case 5:
		return "delete_resolution", nil
	case 6:
		return "diff_batch", nil
	default:
		return "", fmt.Errorf("unknown merge fallback reason kind %d", kind)
	}
}

func decodeMutationKind(kind int32) (string, error) {
	switch kind {
	case 1:
		return "upsert", nil
	case 2:
		return "delete", nil
	default:
		return "", fmt.Errorf("unknown mutation kind %d", kind)
	}
}

func decodeSnapshotNamespaceKind(kind int32) (string, error) {
	switch kind {
	case 1:
		return "branch", nil
	case 2:
		return "tag", nil
	case 3:
		return "checkpoint", nil
	case 4:
		return "custom", nil
	default:
		return "", fmt.Errorf("unknown snapshot namespace kind %d", kind)
	}
}

func decodeNamedRootRetentionKind(kind int32) (string, error) {
	switch kind {
	case 1:
		return "all", nil
	case 2:
		return "exact", nil
	case 3:
		return "prefix", nil
	case 4:
		return "newest_by_name", nil
	case 5:
		return "updated_since", nil
	default:
		return "", fmt.Errorf("unknown named root retention kind %d", kind)
	}
}

func decodeCrdtMergeStrategy(kind int32) (string, error) {
	switch kind {
	case 1:
		return "last_writer_wins", nil
	case 2:
		return "multi_value", nil
	default:
		return "", fmt.Errorf("unknown CRDT merge strategy %d", kind)
	}
}

func decodeCrdtDeletePolicy(kind int32) (string, error) {
	switch kind {
	case 1:
		return "delete_wins", nil
	case 2:
		return "update_wins", nil
	default:
		return "", fmt.Errorf("unknown CRDT delete policy %d", kind)
	}
}

func decodeValueRefKind(kind int32) (string, error) {
	switch kind {
	case 1:
		return "inline", nil
	case 2:
		return "blob", nil
	default:
		return "", fmt.Errorf("unknown value ref kind %d", kind)
	}
}

type byteDecoder struct {
	data []byte
	pos  int
}

func (d *byteDecoder) readByte() (byte, error) {
	if d.pos >= len(d.data) {
		return 0, errors.New("unexpected end of UniFFI buffer")
	}
	value := d.data[d.pos]
	d.pos++
	return value, nil
}

func (d *byteDecoder) readInt32() (int32, error) {
	if d.pos+4 > len(d.data) {
		return 0, errors.New("unexpected end of UniFFI buffer")
	}
	value := int32(binary.BigEndian.Uint32(d.data[d.pos : d.pos+4]))
	d.pos += 4
	return value, nil
}

func (d *byteDecoder) readByteArray() ([]byte, error) {
	length, err := d.readInt32()
	if err != nil {
		return nil, err
	}
	if length < 0 || d.pos+int(length) > len(d.data) {
		return nil, fmt.Errorf("invalid UniFFI byte array length %d", length)
	}
	value := append([]byte(nil), d.data[d.pos:d.pos+int(length)]...)
	d.pos += int(length)
	return value, nil
}

func (d *byteDecoder) readOptionalByteArray() ([]byte, bool, error) {
	present, err := d.readByte()
	if err != nil {
		return nil, false, err
	}
	if present == 0 {
		return nil, false, nil
	}
	value, err := d.readByteArray()
	if err != nil {
		return nil, false, err
	}
	return value, true, nil
}

func (d *byteDecoder) readBool() (bool, error) {
	value, err := d.readByte()
	if err != nil {
		return false, err
	}
	return value != 0, nil
}

func (d *byteDecoder) readOptionalBool() (*bool, error) {
	present, err := d.readByte()
	if err != nil {
		return nil, err
	}
	if present == 0 {
		return nil, nil
	}
	value, err := d.readBool()
	if err != nil {
		return nil, err
	}
	return &value, nil
}

func (d *byteDecoder) readString() (string, error) {
	bytes, err := d.readByteArray()
	if err != nil {
		return "", err
	}
	return string(bytes), nil
}

func (d *byteDecoder) readOptionalString() (string, bool, error) {
	present, err := d.readByte()
	if err != nil {
		return "", false, err
	}
	if present == 0 {
		return "", false, nil
	}
	value, err := d.readString()
	if err != nil {
		return "", false, err
	}
	return value, true, nil
}

func (d *byteDecoder) readByteArraySequence() ([][]byte, error) {
	count, err := d.readInt32()
	if err != nil {
		return nil, err
	}
	if count < 0 {
		return nil, fmt.Errorf("negative sequence count %d", count)
	}
	values := make([][]byte, 0, count)
	for i := int32(0); i < count; i++ {
		value, err := d.readByteArray()
		if err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	return values, nil
}

func (d *byteDecoder) readChangedSpans() ([]ChangedSpan, error) {
	count, err := d.readInt32()
	if err != nil {
		return nil, err
	}
	if count < 0 {
		return nil, fmt.Errorf("negative changed span count %d", count)
	}
	spans := make([]ChangedSpan, 0, count)
	for i := int32(0); i < count; i++ {
		start, err := d.readByteArray()
		if err != nil {
			return nil, err
		}
		end, _, err := d.readOptionalByteArray()
		if err != nil {
			return nil, err
		}
		spans = append(spans, ChangedSpan{Start: start, End: end})
	}
	return spans, nil
}

func (d *byteDecoder) readOptionalRangeCursor() (*RangeCursor, error) {
	present, err := d.readByte()
	if err != nil {
		return nil, err
	}
	if present == 0 {
		return nil, nil
	}
	afterKey, _, err := d.readOptionalByteArray()
	if err != nil {
		return nil, err
	}
	return &RangeCursor{AfterKey: afterKey}, nil
}

func (d *byteDecoder) readOptionalReverseCursor() (*ReverseCursor, error) {
	present, err := d.readByte()
	if err != nil {
		return nil, err
	}
	if present == 0 {
		return nil, nil
	}
	beforeKey, _, err := d.readOptionalByteArray()
	if err != nil {
		return nil, err
	}
	return &ReverseCursor{BeforeKey: beforeKey}, nil
}

func (d *byteDecoder) readOptionalTree() (Tree, bool, error) {
	present, err := d.readByte()
	if err != nil {
		return Tree{}, false, err
	}
	if present == 0 {
		return Tree{}, false, nil
	}
	tree, err := d.readTree()
	if err != nil {
		return Tree{}, false, err
	}
	return tree, true, nil
}

func (d *byteDecoder) readTree() (Tree, error) {
	start := d.pos
	if _, _, err := d.readOptionalByteArray(); err != nil {
		return Tree{}, err
	}
	if err := d.readConfigRecordRaw(); err != nil {
		return Tree{}, err
	}
	return Tree{raw: append([]byte(nil), d.data[start:d.pos]...)}, nil
}

func (d *byteDecoder) readSnapshotBundle() (SnapshotBundle, error) {
	formatVersion, err := d.readUint32()
	if err != nil {
		return SnapshotBundle{}, err
	}
	tree, err := d.readTree()
	if err != nil {
		return SnapshotBundle{}, err
	}
	count, err := d.readInt32()
	if err != nil {
		return SnapshotBundle{}, err
	}
	if count < 0 {
		return SnapshotBundle{}, fmt.Errorf("negative snapshot bundle node count %d", count)
	}
	nodes := make([]SnapshotBundleNode, 0, count)
	for i := int32(0); i < count; i++ {
		cid, err := d.readByteArray()
		if err != nil {
			return SnapshotBundle{}, err
		}
		bytes, err := d.readByteArray()
		if err != nil {
			return SnapshotBundle{}, err
		}
		nodes = append(nodes, SnapshotBundleNode{
			CID:   cid,
			Bytes: bytes,
		})
	}
	return SnapshotBundle{
		FormatVersion: formatVersion,
		Tree:          tree,
		Nodes:         nodes,
	}, nil
}

func (d *byteDecoder) readSnapshotBundleSummary() (SnapshotBundleSummary, error) {
	formatVersion, err := d.readUint32()
	if err != nil {
		return SnapshotBundleSummary{}, err
	}
	root, hasRoot, err := d.readOptionalByteArray()
	if err != nil {
		return SnapshotBundleSummary{}, err
	}
	nodeCount, err := d.readUint64()
	if err != nil {
		return SnapshotBundleSummary{}, err
	}
	byteCount, err := d.readUint64()
	if err != nil {
		return SnapshotBundleSummary{}, err
	}
	minNodeBytes, err := d.readUint64()
	if err != nil {
		return SnapshotBundleSummary{}, err
	}
	maxNodeBytes, err := d.readUint64()
	if err != nil {
		return SnapshotBundleSummary{}, err
	}
	return SnapshotBundleSummary{
		FormatVersion: formatVersion,
		Root:          root,
		HasRoot:       hasRoot,
		NodeCount:     nodeCount,
		ByteCount:     byteCount,
		MinNodeBytes:  minNodeBytes,
		MaxNodeBytes:  maxNodeBytes,
	}, nil
}

func (d *byteDecoder) readSnapshotBundleVerification() (SnapshotBundleVerification, error) {
	valid, err := d.readBool()
	if err != nil {
		return SnapshotBundleVerification{}, err
	}
	summary, err := d.readSnapshotBundleSummary()
	if err != nil {
		return SnapshotBundleVerification{}, err
	}
	reachableNodes, err := d.readUint64()
	if err != nil {
		return SnapshotBundleVerification{}, err
	}
	reachableBytes, err := d.readUint64()
	if err != nil {
		return SnapshotBundleVerification{}, err
	}
	missingCids, err := d.readByteArraySequence()
	if err != nil {
		return SnapshotBundleVerification{}, err
	}
	extraCids, err := d.readByteArraySequence()
	if err != nil {
		return SnapshotBundleVerification{}, err
	}
	return SnapshotBundleVerification{
		Valid:          valid,
		Summary:        summary,
		ReachableNodes: reachableNodes,
		ReachableBytes: reachableBytes,
		MissingCids:    missingCids,
		ExtraCids:      extraCids,
	}, nil
}

func (d *byteDecoder) readKeyProof() (KeyProof, error) {
	root, hasRoot, err := d.readOptionalByteArray()
	if err != nil {
		return KeyProof{}, err
	}
	key, err := d.readByteArray()
	if err != nil {
		return KeyProof{}, err
	}
	count, err := d.readInt32()
	if err != nil {
		return KeyProof{}, err
	}
	if count < 0 {
		return KeyProof{}, fmt.Errorf("negative key proof path count %d", count)
	}
	path := make([][]byte, 0, count)
	for i := int32(0); i < count; i++ {
		node, err := d.readNodeRecordRaw()
		if err != nil {
			return KeyProof{}, err
		}
		path = append(path, node)
	}
	return KeyProof{
		Root:    root,
		HasRoot: hasRoot,
		Key:     key,
		Path:    path,
	}, nil
}

func (d *byteDecoder) readMultiKeyProof() (MultiKeyProof, error) {
	root, hasRoot, err := d.readOptionalByteArray()
	if err != nil {
		return MultiKeyProof{}, err
	}
	keys, err := d.readByteArraySequence()
	if err != nil {
		return MultiKeyProof{}, err
	}
	count, err := d.readInt32()
	if err != nil {
		return MultiKeyProof{}, err
	}
	if count < 0 {
		return MultiKeyProof{}, fmt.Errorf("negative multi-key proof path count %d", count)
	}
	path := make([][]byte, 0, count)
	for i := int32(0); i < count; i++ {
		node, err := d.readNodeRecordRaw()
		if err != nil {
			return MultiKeyProof{}, err
		}
		path = append(path, node)
	}
	return MultiKeyProof{
		Root:    root,
		HasRoot: hasRoot,
		Keys:    keys,
		Path:    path,
	}, nil
}

func (d *byteDecoder) readRangePage() (RangePage, error) {
	entries, err := d.readEntries()
	if err != nil {
		return RangePage{}, err
	}
	cursor, err := d.readOptionalRangeCursor()
	if err != nil {
		return RangePage{}, err
	}
	return RangePage{Entries: entries, NextCursor: cursor}, nil
}

func (d *byteDecoder) readReversePage() (ReversePage, error) {
	entries, err := d.readEntries()
	if err != nil {
		return ReversePage{}, err
	}
	cursor, err := d.readOptionalReverseCursor()
	if err != nil {
		return ReversePage{}, err
	}
	return ReversePage{Entries: entries, NextCursor: cursor}, nil
}

func (d *byteDecoder) readCursorWindow() (CursorWindow, error) {
	positionKey, hasPositionKey, err := d.readOptionalByteArray()
	if err != nil {
		return CursorWindow{}, err
	}
	positionValue, hasPositionValue, err := d.readOptionalByteArray()
	if err != nil {
		return CursorWindow{}, err
	}
	found, err := d.readBool()
	if err != nil {
		return CursorWindow{}, err
	}
	entries, err := d.readEntries()
	if err != nil {
		return CursorWindow{}, err
	}
	cursor, err := d.readOptionalRangeCursor()
	if err != nil {
		return CursorWindow{}, err
	}
	return CursorWindow{
		PositionKey:      positionKey,
		HasPositionKey:   hasPositionKey,
		PositionValue:    positionValue,
		HasPositionValue: hasPositionValue,
		Found:            found,
		Entries:          entries,
		NextCursor:       cursor,
	}, nil
}

func (d *byteDecoder) readDiffPage() (DiffPage, error) {
	diffs, err := d.readDiffs()
	if err != nil {
		return DiffPage{}, err
	}
	cursor, err := d.readOptionalRangeCursor()
	if err != nil {
		return DiffPage{}, err
	}
	return DiffPage{Diffs: diffs, NextCursor: cursor}, nil
}

func (d *byteDecoder) readRangeProof() (RangeProof, error) {
	root, hasRoot, err := d.readOptionalByteArray()
	if err != nil {
		return RangeProof{}, err
	}
	start, err := d.readByteArray()
	if err != nil {
		return RangeProof{}, err
	}
	end, hasEnd, err := d.readOptionalByteArray()
	if err != nil {
		return RangeProof{}, err
	}
	count, err := d.readInt32()
	if err != nil {
		return RangeProof{}, err
	}
	if count < 0 {
		return RangeProof{}, fmt.Errorf("negative range proof path count %d", count)
	}
	path := make([][]byte, 0, count)
	for i := int32(0); i < count; i++ {
		node, err := d.readNodeRecordRaw()
		if err != nil {
			return RangeProof{}, err
		}
		path = append(path, node)
	}
	return RangeProof{
		Root:    root,
		HasRoot: hasRoot,
		Start:   start,
		End:     end,
		HasEnd:  hasEnd,
		Path:    path,
	}, nil
}

func (d *byteDecoder) readRangePageProof() (RangePageProof, error) {
	root, hasRoot, err := d.readOptionalByteArray()
	if err != nil {
		return RangePageProof{}, err
	}
	after, hasAfter, err := d.readOptionalByteArray()
	if err != nil {
		return RangePageProof{}, err
	}
	end, hasEnd, err := d.readOptionalByteArray()
	if err != nil {
		return RangePageProof{}, err
	}
	count, err := d.readInt32()
	if err != nil {
		return RangePageProof{}, err
	}
	if count < 0 {
		return RangePageProof{}, fmt.Errorf("negative range page proof path count %d", count)
	}
	path := make([][]byte, 0, count)
	for i := int32(0); i < count; i++ {
		node, err := d.readNodeRecordRaw()
		if err != nil {
			return RangePageProof{}, err
		}
		path = append(path, node)
	}
	return RangePageProof{
		Root:     root,
		HasRoot:  hasRoot,
		After:    after,
		HasAfter: hasAfter,
		End:      end,
		HasEnd:   hasEnd,
		Path:     path,
	}, nil
}

func (d *byteDecoder) readOptionalKeyProof() (KeyProof, bool, error) {
	present, err := d.readByte()
	if err != nil {
		return KeyProof{}, false, err
	}
	if present == 0 {
		return KeyProof{}, false, nil
	}
	proof, err := d.readKeyProof()
	if err != nil {
		return KeyProof{}, false, err
	}
	return proof, true, nil
}

func (d *byteDecoder) readDiffPageProof() (DiffPageProof, error) {
	base, err := d.readRangePageProof()
	if err != nil {
		return DiffPageProof{}, err
	}
	other, err := d.readRangePageProof()
	if err != nil {
		return DiffPageProof{}, err
	}
	lookaheadBase, hasLookaheadBase, err := d.readOptionalKeyProof()
	if err != nil {
		return DiffPageProof{}, err
	}
	lookaheadOther, hasLookaheadOther, err := d.readOptionalKeyProof()
	if err != nil {
		return DiffPageProof{}, err
	}
	requestedEnd, hasRequestedEnd, err := d.readOptionalByteArray()
	if err != nil {
		return DiffPageProof{}, err
	}
	limit, err := d.readUint64()
	if err != nil {
		return DiffPageProof{}, err
	}
	return DiffPageProof{
		Base:              base,
		Other:             other,
		LookaheadBase:     lookaheadBase,
		HasLookaheadBase:  hasLookaheadBase,
		LookaheadOther:    lookaheadOther,
		HasLookaheadOther: hasLookaheadOther,
		RequestedEnd:      requestedEnd,
		HasRequestedEnd:   hasRequestedEnd,
		Limit:             limit,
	}, nil
}

func (d *byteDecoder) readNodeRecordRaw() ([]byte, error) {
	start := d.pos
	if err := d.readByteArraySequenceRaw(); err != nil {
		return nil, err
	}
	if err := d.readByteArraySequenceRaw(); err != nil {
		return nil, err
	}
	if _, err := d.readBool(); err != nil {
		return nil, err
	}
	if _, err := d.readByte(); err != nil {
		return nil, err
	}
	if _, err := d.readUint64(); err != nil {
		return nil, err
	}
	if _, err := d.readUint64(); err != nil {
		return nil, err
	}
	if _, err := d.readUint32(); err != nil {
		return nil, err
	}
	if _, err := d.readUint64(); err != nil {
		return nil, err
	}
	if err := d.readEncodingRecordRaw(); err != nil {
		return nil, err
	}
	return append([]byte(nil), d.data[start:d.pos]...), nil
}

func (d *byteDecoder) readByteArraySequenceRaw() error {
	count, err := d.readInt32()
	if err != nil {
		return err
	}
	if count < 0 {
		return fmt.Errorf("negative byte array sequence count %d", count)
	}
	for i := int32(0); i < count; i++ {
		if _, err := d.readByteArray(); err != nil {
			return err
		}
	}
	return nil
}

func (d *byteDecoder) readEncodingRecordRaw() error {
	if _, err := d.readInt32(); err != nil {
		return err
	}
	_, _, err := d.readOptionalString()
	return err
}

func (d *byteDecoder) readConfigRecordRaw() error {
	if _, err := d.readUint64(); err != nil {
		return err
	}
	if _, err := d.readUint64(); err != nil {
		return err
	}
	if _, err := d.readUint32(); err != nil {
		return err
	}
	if _, err := d.readUint64(); err != nil {
		return err
	}
	if _, err := d.readInt32(); err != nil {
		return err
	}
	if _, _, err := d.readOptionalString(); err != nil {
		return err
	}
	if err := d.readOptionalUint64Raw(); err != nil {
		return err
	}
	return d.readOptionalUint64Raw()
}

func (d *byteDecoder) readNamedRoots() ([]NamedRoot, error) {
	count, err := d.readInt32()
	if err != nil {
		return nil, err
	}
	if count < 0 {
		return nil, fmt.Errorf("negative named root count %d", count)
	}
	roots := make([]NamedRoot, 0, count)
	for i := int32(0); i < count; i++ {
		name, err := d.readByteArray()
		if err != nil {
			return nil, err
		}
		tree, err := d.readTree()
		if err != nil {
			return nil, err
		}
		roots = append(roots, NamedRoot{Name: name, Tree: tree})
	}
	return roots, nil
}

func (d *byteDecoder) readRootManifestRecord() (RootManifestRecord, error) {
	tree, err := d.readTree()
	if err != nil {
		return RootManifestRecord{}, err
	}
	createdAtMillis, err := d.readOptionalUint64()
	if err != nil {
		return RootManifestRecord{}, err
	}
	updatedAtMillis, err := d.readOptionalUint64()
	if err != nil {
		return RootManifestRecord{}, err
	}
	return RootManifestRecord{
		Tree:            tree,
		CreatedAtMillis: createdAtMillis,
		UpdatedAtMillis: updatedAtMillis,
	}, nil
}

func (d *byteDecoder) readNamedRootManifests() ([]NamedRootManifestRecord, error) {
	count, err := d.readInt32()
	if err != nil {
		return nil, err
	}
	if count < 0 {
		return nil, fmt.Errorf("negative named root manifest count %d", count)
	}
	roots := make([]NamedRootManifestRecord, 0, count)
	for i := int32(0); i < count; i++ {
		name, err := d.readByteArray()
		if err != nil {
			return nil, err
		}
		manifest, err := d.readRootManifestRecord()
		if err != nil {
			return nil, err
		}
		roots = append(roots, NamedRootManifestRecord{Name: name, Manifest: manifest})
	}
	return roots, nil
}

func (d *byteDecoder) readSnapshotRoots() ([]SnapshotRoot, error) {
	count, err := d.readInt32()
	if err != nil {
		return nil, err
	}
	if count < 0 {
		return nil, fmt.Errorf("negative snapshot root count %d", count)
	}
	snapshots := make([]SnapshotRoot, 0, count)
	for i := int32(0); i < count; i++ {
		id, err := d.readByteArray()
		if err != nil {
			return nil, err
		}
		name, err := d.readByteArray()
		if err != nil {
			return nil, err
		}
		tree, err := d.readTree()
		if err != nil {
			return nil, err
		}
		createdAtMillis, err := d.readOptionalUint64()
		if err != nil {
			return nil, err
		}
		updatedAtMillis, err := d.readOptionalUint64()
		if err != nil {
			return nil, err
		}
		snapshots = append(snapshots, SnapshotRoot{
			ID:              id,
			Name:            name,
			Tree:            tree,
			CreatedAtMillis: createdAtMillis,
			UpdatedAtMillis: updatedAtMillis,
		})
	}
	return snapshots, nil
}

func (d *byteDecoder) done() error {
	if d.pos != len(d.data) {
		return fmt.Errorf("trailing bytes in UniFFI buffer: %d", len(d.data)-d.pos)
	}
	return nil
}

func (d *byteDecoder) readUint32() (uint32, error) {
	if d.pos+4 > len(d.data) {
		return 0, errors.New("unexpected end of UniFFI buffer")
	}
	value := binary.BigEndian.Uint32(d.data[d.pos : d.pos+4])
	d.pos += 4
	return value, nil
}

func (d *byteDecoder) readUint64() (uint64, error) {
	if d.pos+8 > len(d.data) {
		return 0, errors.New("unexpected end of UniFFI buffer")
	}
	value := binary.BigEndian.Uint64(d.data[d.pos : d.pos+8])
	d.pos += 8
	return value, nil
}

func (d *byteDecoder) readOptionalUint64() (*uint64, error) {
	present, err := d.readByte()
	if err != nil {
		return nil, err
	}
	if present == 0 {
		return nil, nil
	}
	value, err := d.readUint64()
	if err != nil {
		return nil, err
	}
	return &value, nil
}

func (d *byteDecoder) readOptionalUint64Raw() error {
	present, err := d.readByte()
	if err != nil {
		return err
	}
	if present == 0 {
		return nil
	}
	_, err = d.readUint64()
	return err
}

func writeU64(out *bytes.Buffer, value uint64) {
	var scratch [8]byte
	binary.BigEndian.PutUint64(scratch[:], value)
	out.Write(scratch[:])
}

func writeU32(out *bytes.Buffer, value uint32) {
	var scratch [4]byte
	binary.BigEndian.PutUint32(scratch[:], value)
	out.Write(scratch[:])
}

func writeI32(out *bytes.Buffer, value int32) {
	writeU32(out, uint32(value))
}

func encodeOptionalU64(out *bytes.Buffer, value *uint64) {
	if value == nil {
		out.WriteByte(0)
		return
	}
	out.WriteByte(1)
	writeU64(out, *value)
}

func encodeOptionalU64Bytes(value *uint64) []byte {
	var out bytes.Buffer
	encodeOptionalU64(&out, value)
	return out.Bytes()
}

func encodeOptionalString(out *bytes.Buffer, value *string) {
	if value == nil {
		out.WriteByte(0)
		return
	}
	out.WriteByte(1)
	encoded := []byte(*value)
	writeI32(out, int32(len(encoded)))
	out.Write(encoded)
}

func optionalString(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}

func encodeEncodingKind(kind string) (int32, error) {
	switch kind {
	case "raw":
		return 1, nil
	case "cbor":
		return 2, nil
	case "json":
		return 3, nil
	case "custom":
		return 4, nil
	default:
		return 0, fmt.Errorf("unknown encoding kind %q", kind)
	}
}
