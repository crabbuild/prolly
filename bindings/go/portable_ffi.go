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

typedef struct ProllyFastPageResult {
	int32_t status;
	uint8_t terminal;
	uint8_t reserved[3];
	uint32_t record_count;
	uint64_t lease_handle;
	const uint8_t *data_ptr;
	uint64_t data_len;
} ProllyFastPageResult;

typedef struct ProllyFastScanOpenResult {
	int32_t status;
	uint32_t reserved;
	uint64_t scan_handle;
} ProllyFastScanOpenResult;

typedef void (*IndexExtractorFreeCallback)(uint64_t handle);
typedef uint64_t (*IndexExtractorCloneCallback)(uint64_t handle);
typedef void (*IndexExtractorExtractCallback)(uint64_t handle, RustBuffer key, RustBuffer value, RustBuffer *out_return, RustCallStatus *out_status);

typedef struct UniFfiTraitVtableSecondaryIndexExtractorCallback {
	IndexExtractorFreeCallback uniffi_free;
	IndexExtractorCloneCallback uniffi_clone;
	IndexExtractorExtractCallback extract;
} UniFfiTraitVtableSecondaryIndexExtractorCallback;

extern void prolly_go_index_extractor_free(uint64_t handle);
extern uint64_t prolly_go_index_extractor_clone(uint64_t handle);
extern void prolly_go_index_extractor_extract(uint64_t handle, RustBuffer key, RustBuffer value, RustBuffer *out_return, RustCallStatus *out_status);

extern void uniffi_prolly_bindings_fn_init_callback_vtable_secondaryindexextractorcallback(UniFfiTraitVtableSecondaryIndexExtractorCallback *vtable);

static UniFfiTraitVtableSecondaryIndexExtractorCallback prolly_go_index_extractor_vtable = {
	prolly_go_index_extractor_free,
	prolly_go_index_extractor_clone,
	prolly_go_index_extractor_extract,
};

static void prolly_register_go_index_extractor_vtable(void) {
	uniffi_prolly_bindings_fn_init_callback_vtable_secondaryindexextractorcallback(&prolly_go_index_extractor_vtable);
}

extern RustBuffer ffi_prolly_bindings_rustbuffer_alloc(uint64_t size, RustCallStatus *out_err);
extern void ffi_prolly_bindings_rustbuffer_free(RustBuffer buf, RustCallStatus *out_err);

extern uint64_t uniffi_prolly_bindings_fn_clone_prollyengine(uint64_t ptr, RustCallStatus *out_err);
extern uint64_t uniffi_prolly_bindings_fn_method_prollyengine_versioned_map(uint64_t ptr, RustBuffer id, RustCallStatus *out_err);
extern uint64_t uniffi_prolly_bindings_fn_method_prollyengine_begin_versioned_transaction(uint64_t ptr, RustCallStatus *out_err);
extern uint64_t uniffi_prolly_bindings_fn_method_prollyengine_indexed_map(uint64_t ptr, RustBuffer id, uint64_t registry, RustCallStatus *out_err);
extern uint64_t uniffi_prolly_bindings_fn_method_prollyengine_build_proximity_map(uint64_t ptr, RustBuffer config, RustBuffer records, RustBuffer threads, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_func_default_proximity_config(uint32_t dimensions, RustCallStatus *out_err);

extern uint64_t uniffi_prolly_bindings_fn_clone_bindingversionedmap(uint64_t ptr, RustCallStatus *out_err);
extern void uniffi_prolly_bindings_fn_free_bindingversionedmap(uint64_t ptr, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_bindingversionedmap_id(uint64_t ptr, RustCallStatus *out_err);
extern int8_t uniffi_prolly_bindings_fn_method_bindingversionedmap_is_initialized(uint64_t ptr, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_bindingversionedmap_initialize(uint64_t ptr, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_bindingversionedmap_initialize_sorted(uint64_t ptr, RustBuffer entries, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_bindingversionedmap_head(uint64_t ptr, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_bindingversionedmap_head_id(uint64_t ptr, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_bindingversionedmap_version(uint64_t ptr, RustBuffer id, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_bindingversionedmap_versions(uint64_t ptr, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_bindingversionedmap_get(uint64_t ptr, RustBuffer key, RustCallStatus *out_err);
extern int8_t uniffi_prolly_bindings_fn_method_bindingversionedmap_contains_key(uint64_t ptr, RustBuffer key, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_bindingversionedmap_get_many(uint64_t ptr, RustBuffer keys, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_bindingversionedmap_get_at(uint64_t ptr, RustBuffer id, RustBuffer key, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_bindingversionedmap_get_many_at(uint64_t ptr, RustBuffer id, RustBuffer keys, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_bindingversionedmap_put(uint64_t ptr, RustBuffer key, RustBuffer value, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_bindingversionedmap_apply(uint64_t ptr, RustBuffer mutations, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_bindingversionedmap_append(uint64_t ptr, RustBuffer mutations, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_bindingversionedmap_parallel_apply(uint64_t ptr, RustBuffer mutations, RustBuffer config, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_bindingversionedmap_rebuild_sorted_if(uint64_t ptr, RustBuffer expected, RustBuffer entries, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_bindingversionedmap_rebuild_from_entries_if(uint64_t ptr, RustBuffer expected, RustBuffer entries, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_bindingversionedmap_apply_at_millis(uint64_t ptr, RustBuffer mutations, uint64_t timestamp_millis, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_bindingversionedmap_apply_if(uint64_t ptr, RustBuffer expected, RustBuffer mutations, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_bindingversionedmap_apply_if_at_millis(uint64_t ptr, RustBuffer expected, RustBuffer mutations, uint64_t timestamp_millis, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_bindingversionedmap_put_if(uint64_t ptr, RustBuffer expected, RustBuffer key, RustBuffer value, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_bindingversionedmap_delete_if(uint64_t ptr, RustBuffer expected, RustBuffer key, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_bindingversionedmap_delete(uint64_t ptr, RustBuffer key, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_bindingversionedmap_snapshot(uint64_t ptr, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_bindingversionedmap_snapshot_at(uint64_t ptr, RustBuffer id, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_bindingversionedmap_backup(uint64_t ptr, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_bindingversionedmap_restore_backup(uint64_t ptr, RustBuffer bytes, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_bindingversionedmap_rollback_to(uint64_t ptr, RustBuffer id, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_bindingversionedmap_keep_last(uint64_t ptr, uint64_t count, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_bindingversionedmap_prune_versions(uint64_t ptr, uint64_t keep_latest, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_bindingversionedmap_keep_for_at(uint64_t ptr, uint64_t now_millis, uint64_t max_age_millis, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_bindingversionedmap_keep_for(uint64_t ptr, uint64_t max_age_millis, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_bindingversionedmap_keep_versions(uint64_t ptr, RustBuffer ids, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_bindingversionedmap_verify_catalog(uint64_t ptr, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_bindingversionedmap_retention_policy(uint64_t ptr, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_bindingversionedmap_plan_gc(uint64_t ptr, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_bindingversionedmap_sweep_gc(uint64_t ptr, RustCallStatus *out_err);
extern uint64_t uniffi_prolly_bindings_fn_method_bindingversionedmap_prepare_merge(uint64_t ptr, RustBuffer base, RustBuffer candidate, RustCallStatus *out_err);
extern uint64_t uniffi_prolly_bindings_fn_clone_bindingmapmerge(uint64_t ptr, RustCallStatus *out_err);
extern void uniffi_prolly_bindings_fn_free_bindingmapmerge(uint64_t ptr, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_bindingmapmerge_base(uint64_t ptr, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_bindingmapmerge_head(uint64_t ptr, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_bindingmapmerge_candidate(uint64_t ptr, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_bindingmapmerge_merge(uint64_t ptr, RustBuffer resolver, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_bindingmapmerge_conflict_page(uint64_t ptr, RustBuffer cursor, uint64_t limit, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_bindingmapmerge_publish(uint64_t ptr, RustBuffer resolver, RustCallStatus *out_err);
extern uint64_t uniffi_prolly_bindings_fn_clone_bindingversionedtransaction(uint64_t ptr, RustCallStatus *out_err);
extern void uniffi_prolly_bindings_fn_free_bindingversionedtransaction(uint64_t ptr, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_bindingversionedtransaction_head(uint64_t ptr, RustBuffer map_id, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_bindingversionedtransaction_get(uint64_t ptr, RustBuffer map_id, RustBuffer key, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_bindingversionedtransaction_apply(uint64_t ptr, RustBuffer map_id, RustBuffer mutations, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_bindingversionedtransaction_apply_if(uint64_t ptr, RustBuffer map_id, RustBuffer expected, RustBuffer mutations, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_bindingversionedtransaction_put(uint64_t ptr, RustBuffer map_id, RustBuffer key, RustBuffer value, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_bindingversionedtransaction_delete(uint64_t ptr, RustBuffer map_id, RustBuffer key, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_bindingversionedtransaction_commit(uint64_t ptr, RustCallStatus *out_err);
extern void uniffi_prolly_bindings_fn_method_bindingversionedtransaction_rollback(uint64_t ptr, RustCallStatus *out_err);
extern uint64_t uniffi_prolly_bindings_fn_clone_bindingmapsnapshot(uint64_t ptr, RustCallStatus *out_err);
extern void uniffi_prolly_bindings_fn_free_bindingmapsnapshot(uint64_t ptr, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_bindingmapsnapshot_id(uint64_t ptr, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_bindingmapsnapshot_version(uint64_t ptr, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_bindingmapsnapshot_get(uint64_t ptr, RustBuffer key, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_bindingmapsnapshot_get_many(uint64_t ptr, RustBuffer keys, RustCallStatus *out_err);
extern int8_t uniffi_prolly_bindings_fn_method_bindingmapsnapshot_contains_key(uint64_t ptr, RustBuffer key, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_bindingmapsnapshot_first_entry(uint64_t ptr, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_bindingmapsnapshot_last_entry(uint64_t ptr, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_bindingmapsnapshot_lower_bound(uint64_t ptr, RustBuffer key, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_bindingmapsnapshot_upper_bound(uint64_t ptr, RustBuffer key, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_bindingmapsnapshot_range(uint64_t ptr, RustBuffer start, RustBuffer range_end, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_bindingmapsnapshot_prefix(uint64_t ptr, RustBuffer prefix, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_bindingmapsnapshot_range_page(uint64_t ptr, RustBuffer cursor, RustBuffer range_end, uint64_t limit, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_bindingmapsnapshot_prefix_page(uint64_t ptr, RustBuffer prefix, RustBuffer cursor, uint64_t limit, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_bindingmapsnapshot_reverse_page(uint64_t ptr, RustBuffer cursor, RustBuffer start, uint64_t limit, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_bindingmapsnapshot_prefix_reverse_page(uint64_t ptr, RustBuffer prefix, RustBuffer cursor, uint64_t limit, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_bindingmapsnapshot_prove_key(uint64_t ptr, RustBuffer key, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_bindingmapsnapshot_prove_keys(uint64_t ptr, RustBuffer keys, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_bindingmapsnapshot_prove_range(uint64_t ptr, RustBuffer start, RustBuffer range_end, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_bindingmapsnapshot_prove_prefix(uint64_t ptr, RustBuffer prefix, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_bindingmapsnapshot_prove_range_page(uint64_t ptr, RustBuffer cursor, RustBuffer range_end, uint64_t limit, RustCallStatus *out_err);
extern uint64_t uniffi_prolly_bindings_fn_method_bindingmapsnapshot_read_session(uint64_t ptr, RustCallStatus *out_err);
extern uint64_t uniffi_prolly_bindings_fn_clone_prollyreadsession(uint64_t ptr, RustCallStatus *out_err);
extern void uniffi_prolly_bindings_fn_free_prollyreadsession(uint64_t ptr, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_prollyreadsession_get(uint64_t ptr, RustBuffer key, RustCallStatus *out_err);
extern uint64_t uniffi_prolly_bindings_fn_method_prollyreadsession_fast_handle(uint64_t ptr, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_func_verify_key_proof(RustBuffer proof, RustCallStatus *out_err);

extern uint64_t uniffi_prolly_bindings_fn_constructor_bindingindexregistry_new(RustCallStatus *out_err);
extern uint64_t uniffi_prolly_bindings_fn_clone_bindingindexregistry(uint64_t ptr, RustCallStatus *out_err);
extern void uniffi_prolly_bindings_fn_free_bindingindexregistry(uint64_t ptr, RustCallStatus *out_err);
extern void uniffi_prolly_bindings_fn_method_bindingindexregistry_register(uint64_t ptr, RustBuffer name, uint64_t generation, RustBuffer extractor_id, RustBuffer projection, RustBuffer limits, uint64_t extractor, RustCallStatus *out_err);

extern uint64_t uniffi_prolly_bindings_fn_clone_bindingindexedmap(uint64_t ptr, RustCallStatus *out_err);
extern void uniffi_prolly_bindings_fn_free_bindingindexedmap(uint64_t ptr, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_bindingindexedmap_id(uint64_t ptr, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_bindingindexedmap_apply(uint64_t ptr, RustBuffer mutations, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_bindingindexedmap_apply_if(uint64_t ptr, RustBuffer expected_source, RustBuffer mutations, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_bindingindexedmap_put(uint64_t ptr, RustBuffer key, RustBuffer value, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_bindingindexedmap_delete(uint64_t ptr, RustBuffer key, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_bindingindexedmap_get(uint64_t ptr, RustBuffer key, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_bindingindexedmap_ensure_index(uint64_t ptr, RustBuffer name, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_bindingindexedmap_health(uint64_t ptr, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_bindingindexedmap_repair_index(uint64_t ptr, RustBuffer name, RustBuffer source_version, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_bindingindexedmap_deactivate_index(uint64_t ptr, RustBuffer name, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_bindingindexedmap_export_current(uint64_t ptr, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_bindingindexedmap_import_current(uint64_t ptr, RustBuffer bundle, RustBuffer expected_source, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_bindingindexedmap_keep_last(uint64_t ptr, uint64_t count, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_bindingindexedmap_metrics(uint64_t ptr, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_bindingindexedmap_verify_all(uint64_t ptr, RustBuffer source_version, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_bindingindexedmap_verify_index(uint64_t ptr, RustBuffer name, RustBuffer source_version, RustCallStatus *out_err);
extern uint64_t uniffi_prolly_bindings_fn_method_bindingindexedmap_snapshot(uint64_t ptr, RustCallStatus *out_err);
extern uint64_t uniffi_prolly_bindings_fn_method_bindingindexedmap_snapshot_at(uint64_t ptr, RustBuffer source_version, RustCallStatus *out_err);
extern uint64_t uniffi_prolly_bindings_fn_method_bindingindexedmap_snapshot_by_id(uint64_t ptr, RustBuffer snapshot_id, RustCallStatus *out_err);

extern uint64_t uniffi_prolly_bindings_fn_clone_bindingindexedsnapshot(uint64_t ptr, RustCallStatus *out_err);
extern void uniffi_prolly_bindings_fn_free_bindingindexedsnapshot(uint64_t ptr, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_bindingindexedsnapshot_id(uint64_t ptr, RustCallStatus *out_err);
extern uint64_t uniffi_prolly_bindings_fn_method_bindingindexedsnapshot_index(uint64_t ptr, RustBuffer name, RustCallStatus *out_err);

extern uint64_t uniffi_prolly_bindings_fn_clone_bindingsecondaryindexsnapshot(uint64_t ptr, RustCallStatus *out_err);
extern void uniffi_prolly_bindings_fn_free_bindingsecondaryindexsnapshot(uint64_t ptr, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_bindingsecondaryindexsnapshot_name(uint64_t ptr, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_bindingsecondaryindexsnapshot_exact(uint64_t ptr, RustBuffer term, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_bindingsecondaryindexsnapshot_prefix(uint64_t ptr, RustBuffer prefix, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_bindingsecondaryindexsnapshot_range(uint64_t ptr, RustBuffer start, RustBuffer range_end, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_bindingsecondaryindexsnapshot_exact_page(uint64_t ptr, RustBuffer term, RustBuffer cursor, uint64_t limit, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_bindingsecondaryindexsnapshot_exact_reverse_page(uint64_t ptr, RustBuffer term, RustBuffer cursor, uint64_t limit, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_bindingsecondaryindexsnapshot_prefix_page(uint64_t ptr, RustBuffer prefix, RustBuffer cursor, uint64_t limit, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_bindingsecondaryindexsnapshot_prefix_reverse_page(uint64_t ptr, RustBuffer prefix, RustBuffer cursor, uint64_t limit, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_bindingsecondaryindexsnapshot_range_page(uint64_t ptr, RustBuffer start, RustBuffer range_end, RustBuffer cursor, uint64_t limit, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_bindingsecondaryindexsnapshot_range_reverse_page(uint64_t ptr, RustBuffer start, RustBuffer range_end, RustBuffer cursor, uint64_t limit, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_bindingsecondaryindexsnapshot_records(uint64_t ptr, RustBuffer term, RustCallStatus *out_err);
extern uint64_t uniffi_prolly_bindings_fn_method_bindingsecondaryindexsnapshot_fast_handle(uint64_t ptr, RustCallStatus *out_err);

extern uint64_t uniffi_prolly_bindings_fn_clone_bindingproximitymap(uint64_t ptr, RustCallStatus *out_err);
extern void uniffi_prolly_bindings_fn_free_bindingproximitymap(uint64_t ptr, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_bindingproximitymap_build_hnsw(uint64_t ptr, RustBuffer config, RustBuffer limits, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_bindingproximitymap_build_composite_hnsw(uint64_t ptr, uint64_t base_map, uint64_t base, RustBuffer config, RustBuffer limits, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_bindingproximitymap_build_composite_pq(uint64_t ptr, uint64_t base_map, uint64_t base, RustBuffer config, RustBuffer limits, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_bindingproximitymap_build_or_rebuild_composite_hnsw(uint64_t ptr, uint64_t base_map, uint64_t base, RustBuffer config, RustBuffer limits, RustBuffer rebuild, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_bindingproximitymap_build_or_rebuild_composite_pq(uint64_t ptr, uint64_t base_map, uint64_t base, RustBuffer config, RustBuffer limits, RustBuffer rebuild, RustCallStatus *out_err);
extern uint64_t uniffi_prolly_bindings_fn_method_bindingproximitymap_load_composite(uint64_t ptr, RustBuffer manifest, RustCallStatus *out_err);
extern uint64_t uniffi_prolly_bindings_fn_method_bindingproximitymap_build_accelerator_catalog(uint64_t ptr, RustBuffer hnsw, RustBuffer pq, RustBuffer composite, RustCallStatus *out_err);
extern uint64_t uniffi_prolly_bindings_fn_method_bindingproximitymap_load_accelerator_catalog(uint64_t ptr, RustBuffer manifest, RustCallStatus *out_err);
extern void uniffi_prolly_bindings_fn_method_bindingproximitymap_clear_content_cache(uint64_t ptr, RustCallStatus *out_err);
extern int8_t uniffi_prolly_bindings_fn_method_bindingproximitymap_contains_key(uint64_t ptr, RustBuffer key, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_bindingproximitymap_config(uint64_t ptr, RustCallStatus *out_err);
extern uint64_t uniffi_prolly_bindings_fn_method_bindingproximitymap_count(uint64_t ptr, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_bindingproximitymap_descriptor(uint64_t ptr, RustCallStatus *out_err);
extern uint64_t uniffi_prolly_bindings_fn_method_bindingproximitymap_fast_handle(uint64_t ptr, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_bindingproximitymap_get(uint64_t ptr, RustBuffer key, RustCallStatus *out_err);
extern uint64_t uniffi_prolly_bindings_fn_method_bindingproximitymap_load_hnsw(uint64_t ptr, RustBuffer manifest, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_bindingproximitymap_mutate(uint64_t ptr, RustBuffer mutations, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_bindingproximitymap_prove_membership(uint64_t ptr, RustBuffer key, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_bindingproximitymap_prove_structure(uint64_t ptr, RustBuffer limits, RustCallStatus *out_err);
extern uint64_t uniffi_prolly_bindings_fn_method_bindingproximitymap_prove_search(uint64_t ptr, RustBuffer request, RustBuffer limits, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_bindingproximitymap_search(uint64_t ptr, RustBuffer request, RustCallStatus *out_err);
extern uint64_t uniffi_prolly_bindings_fn_method_bindingproximitymap_read_session(uint64_t ptr, RustCallStatus *out_err);
extern uint64_t uniffi_prolly_bindings_fn_method_bindingproximitymap_rebuild(uint64_t ptr, RustBuffer mutations, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_bindingproximitymap_verify(uint64_t ptr, RustCallStatus *out_err);
extern uint64_t uniffi_prolly_bindings_fn_clone_bindingproximityreadsession(uint64_t ptr, RustCallStatus *out_err);
extern void uniffi_prolly_bindings_fn_free_bindingproximityreadsession(uint64_t ptr, RustCallStatus *out_err);
extern int8_t uniffi_prolly_bindings_fn_method_bindingproximityreadsession_contains_key(uint64_t ptr, RustBuffer key, RustCallStatus *out_err);
extern uint64_t uniffi_prolly_bindings_fn_method_bindingproximityreadsession_fast_handle(uint64_t ptr, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_bindingproximityreadsession_get(uint64_t ptr, RustBuffer key, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_bindingproximityreadsession_search(uint64_t ptr, RustBuffer request, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_func_verify_proximity_membership_proof(RustBuffer proof, RustBuffer expected_descriptor, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_func_verify_proximity_structure_proof(RustBuffer proof, RustBuffer expected_descriptor, RustBuffer limits, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_func_default_content_graph_limits(RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_func_default_hnsw_build_limits(RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_func_default_hnsw_config(RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_func_default_composite_accelerator_config(RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_func_default_composite_build_limits(RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_func_default_composite_rebuild_options(RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_func_default_pq_build_limits(RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_func_default_pq_config(RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_func_exact_proximity_search_request(RustBuffer query, uint64_t k, RustCallStatus *out_err);
extern uint64_t uniffi_prolly_bindings_fn_clone_bindingproximitysearchproof(uint64_t ptr, RustCallStatus *out_err);
extern void uniffi_prolly_bindings_fn_free_bindingproximitysearchproof(uint64_t ptr, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_bindingproximitysearchproof_verify(uint64_t ptr, RustBuffer expected_descriptor, RustBuffer limits, RustCallStatus *out_err);
extern uint64_t uniffi_prolly_bindings_fn_clone_bindinghnswindex(uint64_t ptr, RustCallStatus *out_err);
extern void uniffi_prolly_bindings_fn_free_bindinghnswindex(uint64_t ptr, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_bindinghnswindex_config(uint64_t ptr, RustCallStatus *out_err);
extern int8_t uniffi_prolly_bindings_fn_method_bindinghnswindex_is_canonical(uint64_t ptr, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_bindinghnswindex_manifest(uint64_t ptr, RustCallStatus *out_err);
extern uint64_t uniffi_prolly_bindings_fn_method_bindinghnswindex_prove_search(uint64_t ptr, uint64_t map, RustBuffer request, RustBuffer limits, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_bindinghnswindex_search(uint64_t ptr, uint64_t map, RustBuffer request, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_bindinghnswindex_source_descriptor(uint64_t ptr, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_bindingproximitymap_build_pq(uint64_t ptr, RustBuffer config, uint64_t worker_threads, RustBuffer limits, RustCallStatus *out_err);
extern uint64_t uniffi_prolly_bindings_fn_method_bindingproximitymap_load_pq(uint64_t ptr, RustBuffer manifest, RustCallStatus *out_err);
extern uint64_t uniffi_prolly_bindings_fn_clone_bindingproductquantizer(uint64_t ptr, RustCallStatus *out_err);
extern void uniffi_prolly_bindings_fn_free_bindingproductquantizer(uint64_t ptr, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_bindingproductquantizer_config(uint64_t ptr, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_bindingproductquantizer_manifest(uint64_t ptr, RustCallStatus *out_err);
extern uint64_t uniffi_prolly_bindings_fn_method_bindingproductquantizer_prove_search(uint64_t ptr, uint64_t map, RustBuffer request, RustBuffer limits, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_bindingproductquantizer_quality(uint64_t ptr, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_bindingproductquantizer_search(uint64_t ptr, uint64_t map, RustBuffer request, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_bindingproductquantizer_source_descriptor(uint64_t ptr, RustCallStatus *out_err);

extern uint64_t uniffi_prolly_bindings_fn_clone_bindingcompositeaccelerator(uint64_t ptr, RustCallStatus *out_err);
extern void uniffi_prolly_bindings_fn_free_bindingcompositeaccelerator(uint64_t ptr, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_bindingcompositeaccelerator_manifest(uint64_t ptr, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_bindingcompositeaccelerator_current_source_descriptor(uint64_t ptr, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_bindingcompositeaccelerator_base_source_descriptor(uint64_t ptr, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_bindingcompositeaccelerator_base_kind(uint64_t ptr, RustCallStatus *out_err);
extern uint64_t uniffi_prolly_bindings_fn_method_bindingcompositeaccelerator_delta_count(uint64_t ptr, RustCallStatus *out_err);
extern uint64_t uniffi_prolly_bindings_fn_method_bindingcompositeaccelerator_shadow_count(uint64_t ptr, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_bindingcompositeaccelerator_config(uint64_t ptr, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_bindingcompositeaccelerator_build_stats(uint64_t ptr, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_bindingcompositeaccelerator_search(uint64_t ptr, uint64_t map, RustBuffer request, RustCallStatus *out_err);
extern uint64_t uniffi_prolly_bindings_fn_method_bindingcompositeaccelerator_prove_search(uint64_t ptr, uint64_t map, RustBuffer request, RustBuffer limits, RustCallStatus *out_err);

extern uint64_t uniffi_prolly_bindings_fn_clone_bindingacceleratorcatalog(uint64_t ptr, RustCallStatus *out_err);
extern void uniffi_prolly_bindings_fn_free_bindingacceleratorcatalog(uint64_t ptr, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_bindingacceleratorcatalog_manifest(uint64_t ptr, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_bindingacceleratorcatalog_source_descriptor(uint64_t ptr, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_bindingacceleratorcatalog_entries(uint64_t ptr, RustCallStatus *out_err);
extern RustBuffer uniffi_prolly_bindings_fn_method_bindingacceleratorcatalog_search(uint64_t ptr, uint64_t map, RustBuffer request, RustCallStatus *out_err);
extern uint64_t uniffi_prolly_bindings_fn_method_bindingacceleratorcatalog_prove_search(uint64_t ptr, uint64_t map, RustBuffer request, RustBuffer limits, RustCallStatus *out_err);

extern ProllyFastScanOpenResult prolly_fast_index_cursor_open(uint64_t snapshot_handle, uint32_t query_kind, const uint8_t *start_ptr, size_t start_len, const uint8_t *end_ptr, size_t end_len, uint8_t has_end, uint8_t reverse);
extern ProllyFastPageResult prolly_fast_index_cursor_next(uint64_t snapshot_handle, uint64_t cursor_handle, uint32_t max_records, uint64_t max_arena_bytes);
extern void prolly_fast_index_cursor_close(uint64_t cursor_handle);
extern ProllyFastPageResult prolly_fast_proximity_search(uint64_t map_handle, const float *query_ptr, size_t dimensions, uint32_t k, uint64_t max_arena_bytes);
extern void prolly_fast_page_release(uint64_t lease_handle);
*/
import "C"

import (
	"bytes"
	"errors"
	"fmt"
	"runtime"
	"sync/atomic"
	"unsafe"
)

const portableMaxArenaBytes = 64 * 1024 * 1024

func portableInput(data []byte) (C.RustBuffer, error) {
	var status C.RustCallStatus
	buf := C.ffi_prolly_bindings_rustbuffer_alloc(C.uint64_t(len(data)), &status)
	if err := portableStatusError(&status); err != nil {
		return C.RustBuffer{}, err
	}
	if len(data) != 0 {
		C.memcpy(unsafe.Pointer(buf.data), unsafe.Pointer(&data[0]), C.size_t(len(data)))
	}
	buf.len = C.uint64_t(len(data))
	return buf, nil
}

func portableFreeBuffer(buf C.RustBuffer) {
	var status C.RustCallStatus
	C.ffi_prolly_bindings_rustbuffer_free(buf, &status)
}

func portableTakeBuffer(buf C.RustBuffer) []byte {
	defer portableFreeBuffer(buf)
	if buf.data == nil || buf.len == 0 {
		return nil
	}
	return C.GoBytes(unsafe.Pointer(buf.data), C.int(buf.len))
}

func portableStatusError(status *C.RustCallStatus) error {
	if status == nil || status.code == 0 {
		return nil
	}
	message := fmt.Sprintf("prolly portable call failed with status %d", int(status.code))
	if status.error_buf.data != nil {
		payload := portableTakeBuffer(status.error_buf)
		if len(payload) != 0 {
			message += fmt.Sprintf(": %x", payload)
		}
	}
	return errors.New(message)
}

func portableEngineHandle(engine *Engine) (C.uint64_t, func(), error) {
	if engine == nil || engine.closed.Load() {
		return 0, nil, errors.New("prolly engine is closed")
	}
	engine.mu.RLock()
	if engine.closed.Load() || engine.handle == 0 {
		engine.mu.RUnlock()
		return 0, nil, errors.New("prolly engine is closed")
	}
	return C.uint64_t(engine.handle), engine.mu.RUnlock, nil
}

func portableCloneEngine(handle C.uint64_t) (C.uint64_t, error) {
	var status C.RustCallStatus
	clone := C.uniffi_prolly_bindings_fn_clone_prollyengine(handle, &status)
	return clone, portableStatusError(&status)
}

func portableCloneVersioned(handle uint64) (C.uint64_t, error) {
	var status C.RustCallStatus
	clone := C.uniffi_prolly_bindings_fn_clone_bindingversionedmap(C.uint64_t(handle), &status)
	return clone, portableStatusError(&status)
}

func portableCloneVersionedTransaction(handle uint64) (C.uint64_t, error) {
	var status C.RustCallStatus
	clone := C.uniffi_prolly_bindings_fn_clone_bindingversionedtransaction(C.uint64_t(handle), &status)
	return clone, portableStatusError(&status)
}

func portableCloneMapMerge(handle uint64) (C.uint64_t, error) {
	var status C.RustCallStatus
	clone := C.uniffi_prolly_bindings_fn_clone_bindingmapmerge(C.uint64_t(handle), &status)
	return clone, portableStatusError(&status)
}

func portableCloneMapSnapshot(handle uint64) (C.uint64_t, error) {
	var status C.RustCallStatus
	clone := C.uniffi_prolly_bindings_fn_clone_bindingmapsnapshot(C.uint64_t(handle), &status)
	return clone, portableStatusError(&status)
}

func portableCloneReadSession(handle uint64) (C.uint64_t, error) {
	var status C.RustCallStatus
	clone := C.uniffi_prolly_bindings_fn_clone_prollyreadsession(C.uint64_t(handle), &status)
	return clone, portableStatusError(&status)
}

func portableCloneRegistry(handle uint64) (C.uint64_t, error) {
	var status C.RustCallStatus
	clone := C.uniffi_prolly_bindings_fn_clone_bindingindexregistry(C.uint64_t(handle), &status)
	return clone, portableStatusError(&status)
}

func portableCloneIndexedMap(handle uint64) (C.uint64_t, error) {
	var status C.RustCallStatus
	clone := C.uniffi_prolly_bindings_fn_clone_bindingindexedmap(C.uint64_t(handle), &status)
	return clone, portableStatusError(&status)
}

func portableCloneIndexedSnapshot(handle uint64) (C.uint64_t, error) {
	var status C.RustCallStatus
	clone := C.uniffi_prolly_bindings_fn_clone_bindingindexedsnapshot(C.uint64_t(handle), &status)
	return clone, portableStatusError(&status)
}

func portableCloneSecondaryIndex(handle uint64) (C.uint64_t, error) {
	var status C.RustCallStatus
	clone := C.uniffi_prolly_bindings_fn_clone_bindingsecondaryindexsnapshot(C.uint64_t(handle), &status)
	return clone, portableStatusError(&status)
}

func ffiEngineVersionedMap(engine *Engine, id []byte) (uint64, error) {
	handle, unlock, err := portableEngineHandle(engine)
	if err != nil {
		return 0, err
	}
	defer unlock()
	handle, err = portableCloneEngine(handle)
	if err != nil {
		return 0, err
	}
	idBuf, err := portableInput(encodeByteArray(id))
	if err != nil {
		return 0, err
	}
	var status C.RustCallStatus
	result := C.uniffi_prolly_bindings_fn_method_prollyengine_versioned_map(handle, idBuf, &status)
	return uint64(result), portableStatusError(&status)
}

func ffiEngineBeginVersionedTransaction(engine *Engine) (uint64, error) {
	handle, unlock, err := portableEngineHandle(engine)
	if err != nil {
		return 0, err
	}
	defer unlock()
	clone, err := portableCloneEngine(handle)
	if err != nil {
		return 0, err
	}
	var status C.RustCallStatus
	result := C.uniffi_prolly_bindings_fn_method_prollyengine_begin_versioned_transaction(clone, &status)
	return uint64(result), portableStatusError(&status)
}

func ffiVersionedPrepareMerge(handle uint64, base, candidate []byte) (uint64, error) {
	clone, err := portableCloneVersioned(handle)
	if err != nil {
		return 0, err
	}
	baseBuf, err := portableInput(encodeByteArray(base))
	if err != nil {
		return 0, err
	}
	candidateBuf, err := portableInput(encodeByteArray(candidate))
	if err != nil {
		return 0, err
	}
	var status C.RustCallStatus
	result := C.uniffi_prolly_bindings_fn_method_bindingversionedmap_prepare_merge(clone, baseBuf, candidateBuf, &status)
	return uint64(result), portableStatusError(&status)
}

func ffiEngineIndexedMap(engine *Engine, id []byte, registry uint64) (uint64, error) {
	handle, unlock, err := portableEngineHandle(engine)
	if err != nil {
		return 0, err
	}
	defer unlock()
	handle, err = portableCloneEngine(handle)
	if err != nil {
		return 0, err
	}
	registryClone, err := portableCloneRegistry(registry)
	if err != nil {
		return 0, err
	}
	idBuf, err := portableInput(encodeByteArray(id))
	if err != nil {
		return 0, err
	}
	var status C.RustCallStatus
	result := C.uniffi_prolly_bindings_fn_method_prollyengine_indexed_map(handle, idBuf, registryClone, &status)
	return uint64(result), portableStatusError(&status)
}

func ffiDefaultProximityConfig(dimensions uint32) ([]byte, error) {
	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_func_default_proximity_config(C.uint32_t(dimensions), &status)
	if err := portableStatusError(&status); err != nil {
		return nil, err
	}
	return portableTakeBuffer(buf), nil
}

func ffiEngineBuildProximity(engine *Engine, config, records []byte) (uint64, error) {
	handle, unlock, err := portableEngineHandle(engine)
	if err != nil {
		return 0, err
	}
	defer unlock()
	handle, err = portableCloneEngine(handle)
	if err != nil {
		return 0, err
	}
	configBuf, err := portableInput(config)
	if err != nil {
		return 0, err
	}
	recordsBuf, err := portableInput(records)
	if err != nil {
		return 0, err
	}
	threadsBuf, err := portableInput([]byte{0})
	if err != nil {
		return 0, err
	}
	var status C.RustCallStatus
	result := C.uniffi_prolly_bindings_fn_method_prollyengine_build_proximity_map(handle, configBuf, recordsBuf, threadsBuf, &status)
	return uint64(result), portableStatusError(&status)
}

func ffiVersionedNoArg(handle uint64, call func(C.uint64_t, *C.RustCallStatus) C.RustBuffer) ([]byte, error) {
	clone, err := portableCloneVersioned(handle)
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	buf := call(clone, &status)
	if err := portableStatusError(&status); err != nil {
		return nil, err
	}
	return portableTakeBuffer(buf), nil
}

func ffiVersionedID(handle uint64) ([]byte, error) {
	return ffiVersionedNoArg(handle, func(clone C.uint64_t, status *C.RustCallStatus) C.RustBuffer {
		return C.uniffi_prolly_bindings_fn_method_bindingversionedmap_id(clone, status)
	})
}

func ffiVersionedIsInitialized(handle uint64) (bool, error) {
	clone, err := portableCloneVersioned(handle)
	if err != nil {
		return false, err
	}
	var status C.RustCallStatus
	value := C.uniffi_prolly_bindings_fn_method_bindingversionedmap_is_initialized(clone, &status)
	return value != 0, portableStatusError(&status)
}

func ffiVersionedInitialize(handle uint64) ([]byte, error) {
	return ffiVersionedNoArg(handle, func(clone C.uint64_t, status *C.RustCallStatus) C.RustBuffer {
		return C.uniffi_prolly_bindings_fn_method_bindingversionedmap_initialize(clone, status)
	})
}

func ffiVersionedHead(handle uint64) ([]byte, error) {
	return ffiVersionedNoArg(handle, func(clone C.uint64_t, status *C.RustCallStatus) C.RustBuffer {
		return C.uniffi_prolly_bindings_fn_method_bindingversionedmap_head(clone, status)
	})
}

func ffiVersionedHeadID(handle uint64) ([]byte, error) {
	return ffiVersionedNoArg(handle, func(clone C.uint64_t, status *C.RustCallStatus) C.RustBuffer {
		return C.uniffi_prolly_bindings_fn_method_bindingversionedmap_head_id(clone, status)
	})
}

func ffiVersionedVersion(handle uint64, id []byte) ([]byte, error) {
	clone, err := portableCloneVersioned(handle)
	if err != nil {
		return nil, err
	}
	idBuf, err := portableInput(encodeByteArray(id))
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_bindingversionedmap_version(clone, idBuf, &status)
	if err := portableStatusError(&status); err != nil {
		return nil, err
	}
	return portableTakeBuffer(buf), nil
}

func ffiVersionedVersions(handle uint64) ([]byte, error) {
	return ffiVersionedNoArg(handle, func(clone C.uint64_t, status *C.RustCallStatus) C.RustBuffer {
		return C.uniffi_prolly_bindings_fn_method_bindingversionedmap_versions(clone, status)
	})
}

func ffiVersionedGet(handle uint64, key []byte) ([]byte, error) {
	clone, err := portableCloneVersioned(handle)
	if err != nil {
		return nil, err
	}
	keyBuf, err := portableInput(encodeByteArray(key))
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_bindingversionedmap_get(clone, keyBuf, &status)
	if err := portableStatusError(&status); err != nil {
		return nil, err
	}
	return portableTakeBuffer(buf), nil
}

func ffiVersionedContainsKey(handle uint64, key []byte) (bool, error) {
	clone, err := portableCloneVersioned(handle)
	if err != nil {
		return false, err
	}
	keyBuf, err := portableInput(encodeByteArray(key))
	if err != nil {
		return false, err
	}
	var status C.RustCallStatus
	value := C.uniffi_prolly_bindings_fn_method_bindingversionedmap_contains_key(clone, keyBuf, &status)
	return value != 0, portableStatusError(&status)
}

func ffiVersionedGetMany(handle uint64, keys [][]byte) ([]byte, error) {
	clone, err := portableCloneVersioned(handle)
	if err != nil {
		return nil, err
	}
	keysBuf, err := portableInput(encodeByteArraySequence(keys))
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_bindingversionedmap_get_many(clone, keysBuf, &status)
	if err := portableStatusError(&status); err != nil {
		return nil, err
	}
	return portableTakeBuffer(buf), nil
}

func ffiVersionedGetAt(handle uint64, id, key []byte) ([]byte, error) {
	clone, err := portableCloneVersioned(handle)
	if err != nil {
		return nil, err
	}
	idBuf, err := portableInput(encodeByteArray(id))
	if err != nil {
		return nil, err
	}
	keyBuf, err := portableInput(encodeByteArray(key))
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_bindingversionedmap_get_at(clone, idBuf, keyBuf, &status)
	if err := portableStatusError(&status); err != nil {
		return nil, err
	}
	return portableTakeBuffer(buf), nil
}

func ffiVersionedGetManyAt(handle uint64, id []byte, keys [][]byte) ([]byte, error) {
	clone, err := portableCloneVersioned(handle)
	if err != nil {
		return nil, err
	}
	idBuf, err := portableInput(encodeByteArray(id))
	if err != nil {
		return nil, err
	}
	keysBuf, err := portableInput(encodeByteArraySequence(keys))
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_bindingversionedmap_get_many_at(clone, idBuf, keysBuf, &status)
	if err := portableStatusError(&status); err != nil {
		return nil, err
	}
	return portableTakeBuffer(buf), nil
}

func ffiVersionedPut(handle uint64, key, value []byte) ([]byte, error) {
	clone, err := portableCloneVersioned(handle)
	if err != nil {
		return nil, err
	}
	keyBuf, err := portableInput(encodeByteArray(key))
	if err != nil {
		return nil, err
	}
	valueBuf, err := portableInput(encodeByteArray(value))
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_bindingversionedmap_put(clone, keyBuf, valueBuf, &status)
	if err := portableStatusError(&status); err != nil {
		return nil, err
	}
	return portableTakeBuffer(buf), nil
}

func ffiVersionedApply(handle uint64, mutations []byte) ([]byte, error) {
	clone, err := portableCloneVersioned(handle)
	if err != nil {
		return nil, err
	}
	mutationsBuf, err := portableInput(mutations)
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_bindingversionedmap_apply(clone, mutationsBuf, &status)
	if err := portableStatusError(&status); err != nil {
		return nil, err
	}
	return portableTakeBuffer(buf), nil
}

func ffiVersionedInitializeSorted(handle uint64, entries []byte) ([]byte, error) {
	clone, err := portableCloneVersioned(handle)
	if err != nil {
		return nil, err
	}
	entriesBuf, err := portableInput(entries)
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_bindingversionedmap_initialize_sorted(clone, entriesBuf, &status)
	if err := portableStatusError(&status); err != nil {
		return nil, err
	}
	return portableTakeBuffer(buf), nil
}

func ffiVersionedAppend(handle uint64, mutations []byte) ([]byte, error) {
	clone, err := portableCloneVersioned(handle)
	if err != nil {
		return nil, err
	}
	mutationsBuf, err := portableInput(mutations)
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_bindingversionedmap_append(clone, mutationsBuf, &status)
	if err := portableStatusError(&status); err != nil {
		return nil, err
	}
	return portableTakeBuffer(buf), nil
}

func ffiVersionedParallelApply(handle uint64, mutations, config []byte) ([]byte, error) {
	clone, err := portableCloneVersioned(handle)
	if err != nil {
		return nil, err
	}
	mutationsBuf, err := portableInput(mutations)
	if err != nil {
		return nil, err
	}
	configBuf, err := portableInput(config)
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_bindingversionedmap_parallel_apply(clone, mutationsBuf, configBuf, &status)
	if err := portableStatusError(&status); err != nil {
		return nil, err
	}
	return portableTakeBuffer(buf), nil
}

func ffiVersionedRebuildSortedIf(handle uint64, expected, entries []byte) ([]byte, error) {
	return ffiVersionedRebuildEntries(handle, expected, entries, true)
}

func ffiVersionedRebuildFromEntriesIf(handle uint64, expected, entries []byte) ([]byte, error) {
	return ffiVersionedRebuildEntries(handle, expected, entries, false)
}

func ffiVersionedRebuildEntries(handle uint64, expected, entries []byte, sorted bool) ([]byte, error) {
	clone, err := portableCloneVersioned(handle)
	if err != nil {
		return nil, err
	}
	expectedBuf, err := portableInput(encodeOptionalByteArray(expected))
	if err != nil {
		return nil, err
	}
	entriesBuf, err := portableInput(entries)
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	var buf C.RustBuffer
	if sorted {
		buf = C.uniffi_prolly_bindings_fn_method_bindingversionedmap_rebuild_sorted_if(clone, expectedBuf, entriesBuf, &status)
	} else {
		buf = C.uniffi_prolly_bindings_fn_method_bindingversionedmap_rebuild_from_entries_if(clone, expectedBuf, entriesBuf, &status)
	}
	if err := portableStatusError(&status); err != nil {
		return nil, err
	}
	return portableTakeBuffer(buf), nil
}

func ffiVersionedApplyAtMillis(handle uint64, mutations []byte, timestampMillis uint64) ([]byte, error) {
	clone, err := portableCloneVersioned(handle)
	if err != nil {
		return nil, err
	}
	mutationsBuf, err := portableInput(mutations)
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_bindingversionedmap_apply_at_millis(
		clone, mutationsBuf, C.uint64_t(timestampMillis), &status,
	)
	if err := portableStatusError(&status); err != nil {
		return nil, err
	}
	return portableTakeBuffer(buf), nil
}

func ffiVersionedApplyIf(handle uint64, expected, mutations []byte) ([]byte, error) {
	clone, err := portableCloneVersioned(handle)
	if err != nil {
		return nil, err
	}
	expectedBuf, err := portableInput(encodeOptionalByteArray(expected))
	if err != nil {
		return nil, err
	}
	mutationsBuf, err := portableInput(mutations)
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_bindingversionedmap_apply_if(clone, expectedBuf, mutationsBuf, &status)
	if err := portableStatusError(&status); err != nil {
		return nil, err
	}
	return portableTakeBuffer(buf), nil
}

func ffiVersionedApplyIfAtMillis(handle uint64, expected, mutations []byte, timestampMillis uint64) ([]byte, error) {
	clone, err := portableCloneVersioned(handle)
	if err != nil {
		return nil, err
	}
	expectedBuf, err := portableInput(encodeOptionalByteArray(expected))
	if err != nil {
		return nil, err
	}
	mutationsBuf, err := portableInput(mutations)
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_bindingversionedmap_apply_if_at_millis(
		clone, expectedBuf, mutationsBuf, C.uint64_t(timestampMillis), &status,
	)
	if err := portableStatusError(&status); err != nil {
		return nil, err
	}
	return portableTakeBuffer(buf), nil
}

func ffiVersionedPutIf(handle uint64, expected, key, value []byte) ([]byte, error) {
	clone, err := portableCloneVersioned(handle)
	if err != nil {
		return nil, err
	}
	expectedBuf, err := portableInput(encodeOptionalByteArray(expected))
	if err != nil {
		return nil, err
	}
	keyBuf, err := portableInput(encodeByteArray(key))
	if err != nil {
		return nil, err
	}
	valueBuf, err := portableInput(encodeByteArray(value))
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_bindingversionedmap_put_if(clone, expectedBuf, keyBuf, valueBuf, &status)
	if err := portableStatusError(&status); err != nil {
		return nil, err
	}
	return portableTakeBuffer(buf), nil
}

func ffiVersionedDeleteIf(handle uint64, expected, key []byte) ([]byte, error) {
	clone, err := portableCloneVersioned(handle)
	if err != nil {
		return nil, err
	}
	expectedBuf, err := portableInput(encodeOptionalByteArray(expected))
	if err != nil {
		return nil, err
	}
	keyBuf, err := portableInput(encodeByteArray(key))
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_bindingversionedmap_delete_if(clone, expectedBuf, keyBuf, &status)
	if err := portableStatusError(&status); err != nil {
		return nil, err
	}
	return portableTakeBuffer(buf), nil
}

func ffiVersionedDelete(handle uint64, key []byte) ([]byte, error) {
	clone, err := portableCloneVersioned(handle)
	if err != nil {
		return nil, err
	}
	keyBuf, err := portableInput(encodeByteArray(key))
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_bindingversionedmap_delete(clone, keyBuf, &status)
	if err := portableStatusError(&status); err != nil {
		return nil, err
	}
	return portableTakeBuffer(buf), nil
}

func ffiVersionedSnapshot(handle uint64) (uint64, error) {
	clone, err := portableCloneVersioned(handle)
	if err != nil {
		return 0, err
	}
	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_bindingversionedmap_snapshot(clone, &status)
	if err := portableStatusError(&status); err != nil {
		return 0, err
	}
	d := byteDecoder{data: portableTakeBuffer(buf)}
	value, err := d.readOptionalUint64()
	if err != nil {
		return 0, err
	}
	if value == nil {
		return 0, d.done()
	}
	return *value, d.done()
}

func ffiVersionedSnapshotAt(handle uint64, id []byte) (uint64, error) {
	clone, err := portableCloneVersioned(handle)
	if err != nil {
		return 0, err
	}
	idBuf, err := portableInput(encodeByteArray(id))
	if err != nil {
		return 0, err
	}
	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_bindingversionedmap_snapshot_at(clone, idBuf, &status)
	if err := portableStatusError(&status); err != nil {
		return 0, err
	}
	d := byteDecoder{data: portableTakeBuffer(buf)}
	value, err := d.readOptionalUint64()
	if err != nil {
		return 0, err
	}
	if value == nil {
		return 0, d.done()
	}
	return *value, d.done()
}

func ffiVersionedBackup(handle uint64) ([]byte, error) {
	clone, err := portableCloneVersioned(handle)
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_bindingversionedmap_backup(clone, &status)
	if err := portableStatusError(&status); err != nil {
		return nil, err
	}
	return portableTakeBuffer(buf), nil
}

func ffiVersionedRestoreBackup(handle uint64, bytes []byte) ([]byte, error) {
	clone, err := portableCloneVersioned(handle)
	if err != nil {
		return nil, err
	}
	bytesBuf, err := portableInput(encodeByteArray(bytes))
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_bindingversionedmap_restore_backup(clone, bytesBuf, &status)
	if err := portableStatusError(&status); err != nil {
		return nil, err
	}
	return portableTakeBuffer(buf), nil
}

func ffiVersionedRollbackTo(handle uint64, id []byte) ([]byte, error) {
	clone, err := portableCloneVersioned(handle)
	if err != nil {
		return nil, err
	}
	idBuf, err := portableInput(encodeByteArray(id))
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_bindingversionedmap_rollback_to(clone, idBuf, &status)
	if err := portableStatusError(&status); err != nil {
		return nil, err
	}
	return portableTakeBuffer(buf), nil
}

func ffiVersionedKeepLast(handle, count uint64) ([]byte, error) {
	clone, err := portableCloneVersioned(handle)
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_bindingversionedmap_keep_last(clone, C.uint64_t(count), &status)
	if err := portableStatusError(&status); err != nil {
		return nil, err
	}
	return portableTakeBuffer(buf), nil
}

func ffiVersionedCountArg(
	handle, count uint64,
	call func(C.uint64_t, C.uint64_t, *C.RustCallStatus) C.RustBuffer,
) ([]byte, error) {
	clone, err := portableCloneVersioned(handle)
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	buf := call(clone, C.uint64_t(count), &status)
	if err := portableStatusError(&status); err != nil {
		return nil, err
	}
	return portableTakeBuffer(buf), nil
}

func ffiVersionedPruneVersions(handle, keepLatest uint64) ([]byte, error) {
	return ffiVersionedCountArg(handle, keepLatest, func(clone, count C.uint64_t, status *C.RustCallStatus) C.RustBuffer {
		return C.uniffi_prolly_bindings_fn_method_bindingversionedmap_prune_versions(clone, count, status)
	})
}

func ffiVersionedKeepFor(handle, maxAgeMillis uint64) ([]byte, error) {
	return ffiVersionedCountArg(handle, maxAgeMillis, func(clone, age C.uint64_t, status *C.RustCallStatus) C.RustBuffer {
		return C.uniffi_prolly_bindings_fn_method_bindingversionedmap_keep_for(clone, age, status)
	})
}

func ffiVersionedKeepForAt(handle, nowMillis, maxAgeMillis uint64) ([]byte, error) {
	clone, err := portableCloneVersioned(handle)
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_bindingversionedmap_keep_for_at(
		clone, C.uint64_t(nowMillis), C.uint64_t(maxAgeMillis), &status,
	)
	if err := portableStatusError(&status); err != nil {
		return nil, err
	}
	return portableTakeBuffer(buf), nil
}

func ffiVersionedKeepVersions(handle uint64, ids [][]byte) ([]byte, error) {
	clone, err := portableCloneVersioned(handle)
	if err != nil {
		return nil, err
	}
	idsBuf, err := portableInput(encodeByteArraySequence(ids))
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_bindingversionedmap_keep_versions(clone, idsBuf, &status)
	if err := portableStatusError(&status); err != nil {
		return nil, err
	}
	return portableTakeBuffer(buf), nil
}

func ffiVersionedVerifyCatalog(handle uint64) ([]byte, error) {
	clone, err := portableCloneVersioned(handle)
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_bindingversionedmap_verify_catalog(clone, &status)
	if err := portableStatusError(&status); err != nil {
		return nil, err
	}
	return portableTakeBuffer(buf), nil
}

func ffiVersionedRetentionPolicy(handle uint64) ([]byte, error) {
	return ffiVersionedNoArg(handle, func(clone C.uint64_t, status *C.RustCallStatus) C.RustBuffer {
		return C.uniffi_prolly_bindings_fn_method_bindingversionedmap_retention_policy(clone, status)
	})
}

func ffiVersionedPlanGC(handle uint64) ([]byte, error) {
	return ffiVersionedNoArg(handle, func(clone C.uint64_t, status *C.RustCallStatus) C.RustBuffer {
		return C.uniffi_prolly_bindings_fn_method_bindingversionedmap_plan_gc(clone, status)
	})
}

func ffiVersionedSweepGC(handle uint64) ([]byte, error) {
	return ffiVersionedNoArg(handle, func(clone C.uint64_t, status *C.RustCallStatus) C.RustBuffer {
		return C.uniffi_prolly_bindings_fn_method_bindingversionedmap_sweep_gc(clone, status)
	})
}

func ffiMapSnapshotGet(handle uint64, key []byte) ([]byte, error) {
	clone, err := portableCloneMapSnapshot(handle)
	if err != nil {
		return nil, err
	}
	keyBuf, err := portableInput(encodeByteArray(key))
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_bindingmapsnapshot_get(clone, keyBuf, &status)
	if err := portableStatusError(&status); err != nil {
		return nil, err
	}
	return portableTakeBuffer(buf), nil
}

func ffiMapSnapshotGetMany(handle uint64, keys [][]byte) ([]byte, error) {
	clone, err := portableCloneMapSnapshot(handle)
	if err != nil {
		return nil, err
	}
	keysBuf, err := portableInput(encodeByteArraySequence(keys))
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_bindingmapsnapshot_get_many(clone, keysBuf, &status)
	if err := portableStatusError(&status); err != nil {
		return nil, err
	}
	return portableTakeBuffer(buf), nil
}

func ffiMapSnapshotContainsKey(handle uint64, key []byte) (bool, error) {
	clone, err := portableCloneMapSnapshot(handle)
	if err != nil {
		return false, err
	}
	keyBuf, err := portableInput(encodeByteArray(key))
	if err != nil {
		return false, err
	}
	var status C.RustCallStatus
	value := C.uniffi_prolly_bindings_fn_method_bindingmapsnapshot_contains_key(clone, keyBuf, &status)
	return value != 0, portableStatusError(&status)
}

func ffiMapSnapshotFirstEntry(handle uint64) ([]byte, error) {
	clone, err := portableCloneMapSnapshot(handle)
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_bindingmapsnapshot_first_entry(clone, &status)
	if err := portableStatusError(&status); err != nil {
		return nil, err
	}
	return portableTakeBuffer(buf), nil
}

func ffiMapSnapshotLastEntry(handle uint64) ([]byte, error) {
	clone, err := portableCloneMapSnapshot(handle)
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_bindingmapsnapshot_last_entry(clone, &status)
	if err := portableStatusError(&status); err != nil {
		return nil, err
	}
	return portableTakeBuffer(buf), nil
}

func ffiMapSnapshotLowerBound(handle uint64, key []byte) ([]byte, error) {
	clone, err := portableCloneMapSnapshot(handle)
	if err != nil {
		return nil, err
	}
	keyBuf, err := portableInput(encodeByteArray(key))
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_bindingmapsnapshot_lower_bound(clone, keyBuf, &status)
	if err := portableStatusError(&status); err != nil {
		return nil, err
	}
	return portableTakeBuffer(buf), nil
}

func ffiMapSnapshotUpperBound(handle uint64, key []byte) ([]byte, error) {
	clone, err := portableCloneMapSnapshot(handle)
	if err != nil {
		return nil, err
	}
	keyBuf, err := portableInput(encodeByteArray(key))
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_bindingmapsnapshot_upper_bound(clone, keyBuf, &status)
	if err := portableStatusError(&status); err != nil {
		return nil, err
	}
	return portableTakeBuffer(buf), nil
}

func ffiMapSnapshotRange(handle uint64, start, end []byte) ([]byte, error) {
	clone, err := portableCloneMapSnapshot(handle)
	if err != nil {
		return nil, err
	}
	startBuf, err := portableInput(encodeByteArray(start))
	if err != nil {
		return nil, err
	}
	endBuf, err := portableInput(encodeOptionalByteArray(end))
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_bindingmapsnapshot_range(clone, startBuf, endBuf, &status)
	if err := portableStatusError(&status); err != nil {
		return nil, err
	}
	return portableTakeBuffer(buf), nil
}

func ffiMapSnapshotPrefix(handle uint64, prefix []byte) ([]byte, error) {
	clone, err := portableCloneMapSnapshot(handle)
	if err != nil {
		return nil, err
	}
	prefixBuf, err := portableInput(encodeByteArray(prefix))
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_bindingmapsnapshot_prefix(clone, prefixBuf, &status)
	if err := portableStatusError(&status); err != nil {
		return nil, err
	}
	return portableTakeBuffer(buf), nil
}

func ffiMapSnapshotRangePage(handle uint64, cursor *RangeCursor, end []byte, limit uint64) ([]byte, error) {
	clone, err := portableCloneMapSnapshot(handle)
	if err != nil {
		return nil, err
	}
	cursorBuf, err := portableInput(encodeOptionalRangeCursor(cursor))
	if err != nil {
		return nil, err
	}
	endBuf, err := portableInput(encodeOptionalByteArray(end))
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_bindingmapsnapshot_range_page(clone, cursorBuf, endBuf, C.uint64_t(limit), &status)
	if err := portableStatusError(&status); err != nil {
		return nil, err
	}
	return portableTakeBuffer(buf), nil
}

func ffiMapSnapshotPrefixPage(handle uint64, prefix []byte, cursor *RangeCursor, limit uint64) ([]byte, error) {
	clone, err := portableCloneMapSnapshot(handle)
	if err != nil {
		return nil, err
	}
	prefixBuf, err := portableInput(encodeByteArray(prefix))
	if err != nil {
		return nil, err
	}
	cursorBuf, err := portableInput(encodeOptionalRangeCursor(cursor))
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_bindingmapsnapshot_prefix_page(clone, prefixBuf, cursorBuf, C.uint64_t(limit), &status)
	if err := portableStatusError(&status); err != nil {
		return nil, err
	}
	return portableTakeBuffer(buf), nil
}

func ffiMapSnapshotReversePage(handle uint64, cursor *ReverseCursor, start []byte, limit uint64) ([]byte, error) {
	clone, err := portableCloneMapSnapshot(handle)
	if err != nil {
		return nil, err
	}
	cursorBuf, err := portableInput(encodeOptionalReverseCursor(cursor))
	if err != nil {
		return nil, err
	}
	startBuf, err := portableInput(encodeByteArray(start))
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_bindingmapsnapshot_reverse_page(clone, cursorBuf, startBuf, C.uint64_t(limit), &status)
	if err := portableStatusError(&status); err != nil {
		return nil, err
	}
	return portableTakeBuffer(buf), nil
}

func ffiMapSnapshotPrefixReversePage(handle uint64, prefix []byte, cursor *ReverseCursor, limit uint64) ([]byte, error) {
	clone, err := portableCloneMapSnapshot(handle)
	if err != nil {
		return nil, err
	}
	prefixBuf, err := portableInput(encodeByteArray(prefix))
	if err != nil {
		return nil, err
	}
	cursorBuf, err := portableInput(encodeOptionalReverseCursor(cursor))
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_bindingmapsnapshot_prefix_reverse_page(clone, prefixBuf, cursorBuf, C.uint64_t(limit), &status)
	if err := portableStatusError(&status); err != nil {
		return nil, err
	}
	return portableTakeBuffer(buf), nil
}

func ffiMapSnapshotID(handle uint64) ([]byte, error) {
	clone, err := portableCloneMapSnapshot(handle)
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_bindingmapsnapshot_id(clone, &status)
	if err := portableStatusError(&status); err != nil {
		return nil, err
	}
	return portableTakeBuffer(buf), nil
}

func ffiMapSnapshotVersion(handle uint64) ([]byte, error) {
	clone, err := portableCloneMapSnapshot(handle)
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_bindingmapsnapshot_version(clone, &status)
	if err := portableStatusError(&status); err != nil {
		return nil, err
	}
	return portableTakeBuffer(buf), nil
}

func ffiMapSnapshotProveKey(handle uint64, key []byte) ([]byte, error) {
	clone, err := portableCloneMapSnapshot(handle)
	if err != nil {
		return nil, err
	}
	keyBuf, err := portableInput(encodeByteArray(key))
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_bindingmapsnapshot_prove_key(clone, keyBuf, &status)
	if err := portableStatusError(&status); err != nil {
		return nil, err
	}
	return portableTakeBuffer(buf), nil
}

func ffiMapSnapshotProveKeys(handle uint64, keys [][]byte) ([]byte, error) {
	clone, err := portableCloneMapSnapshot(handle)
	if err != nil {
		return nil, err
	}
	keysBuf, err := portableInput(encodeByteArraySequence(keys))
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_bindingmapsnapshot_prove_keys(clone, keysBuf, &status)
	if err := portableStatusError(&status); err != nil {
		return nil, err
	}
	return portableTakeBuffer(buf), nil
}

func ffiMapSnapshotProveRange(handle uint64, start, end []byte) ([]byte, error) {
	clone, err := portableCloneMapSnapshot(handle)
	if err != nil {
		return nil, err
	}
	startBuf, err := portableInput(encodeByteArray(start))
	if err != nil {
		return nil, err
	}
	endBuf, err := portableInput(encodeOptionalByteArray(end))
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_bindingmapsnapshot_prove_range(clone, startBuf, endBuf, &status)
	if err := portableStatusError(&status); err != nil {
		return nil, err
	}
	return portableTakeBuffer(buf), nil
}

func ffiMapSnapshotProvePrefix(handle uint64, prefix []byte) ([]byte, error) {
	clone, err := portableCloneMapSnapshot(handle)
	if err != nil {
		return nil, err
	}
	prefixBuf, err := portableInput(encodeByteArray(prefix))
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_bindingmapsnapshot_prove_prefix(clone, prefixBuf, &status)
	if err := portableStatusError(&status); err != nil {
		return nil, err
	}
	return portableTakeBuffer(buf), nil
}

func ffiMapSnapshotProveRangePage(handle uint64, cursor *RangeCursor, end []byte, limit uint64) ([]byte, error) {
	clone, err := portableCloneMapSnapshot(handle)
	if err != nil {
		return nil, err
	}
	cursorBuf, err := portableInput(encodeOptionalRangeCursor(cursor))
	if err != nil {
		return nil, err
	}
	endBuf, err := portableInput(encodeOptionalByteArray(end))
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_bindingmapsnapshot_prove_range_page(clone, cursorBuf, endBuf, C.uint64_t(limit), &status)
	if err := portableStatusError(&status); err != nil {
		return nil, err
	}
	return portableTakeBuffer(buf), nil
}

func ffiVerifyKeyProof(proof []byte) ([]byte, error) {
	proofBuf, err := portableInput(proof)
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_func_verify_key_proof(proofBuf, &status)
	if err := portableStatusError(&status); err != nil {
		return nil, err
	}
	return portableTakeBuffer(buf), nil
}

func ffiMapSnapshotRead(handle uint64) (uint64, error) {
	clone, err := portableCloneMapSnapshot(handle)
	if err != nil {
		return 0, err
	}
	var status C.RustCallStatus
	result := C.uniffi_prolly_bindings_fn_method_bindingmapsnapshot_read_session(clone, &status)
	return uint64(result), portableStatusError(&status)
}

func ffiReadSessionGet(handle uint64, key []byte) ([]byte, error) {
	clone, err := portableCloneReadSession(handle)
	if err != nil {
		return nil, err
	}
	keyBuf, err := portableInput(encodeByteArray(key))
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_prollyreadsession_get(clone, keyBuf, &status)
	if err := portableStatusError(&status); err != nil {
		return nil, err
	}
	return portableTakeBuffer(buf), nil
}

func ffiReadSessionFastHandle(handle uint64) (uint64, error) {
	clone, err := portableCloneReadSession(handle)
	if err != nil {
		return 0, err
	}
	var status C.RustCallStatus
	fast := C.uniffi_prolly_bindings_fn_method_prollyreadsession_fast_handle(clone, &status)
	return uint64(fast), portableStatusError(&status)
}
func ffiAdoptReadSession(handle uint64) (*ReadSession, error) {
	fast, err := ffiReadSessionFastHandle(handle)
	if err != nil {
		ffiFreeReadSession(handle)
		return nil, err
	}
	result := &ReadSession{handle: C.uint64_t(handle), fast: C.uint64_t(fast)}
	runtime.SetFinalizer(result, (*ReadSession).Close)
	return result, nil
}

func ffiFreeMapSnapshot(handle uint64) {
	var status C.RustCallStatus
	C.uniffi_prolly_bindings_fn_free_bindingmapsnapshot(C.uint64_t(handle), &status)
}
func ffiFreeReadSession(handle uint64) {
	var status C.RustCallStatus
	C.uniffi_prolly_bindings_fn_free_prollyreadsession(C.uint64_t(handle), &status)
}

func ffiFreeVersioned(handle uint64) {
	var status C.RustCallStatus
	C.uniffi_prolly_bindings_fn_free_bindingversionedmap(C.uint64_t(handle), &status)
}

func ffiFreeVersionedTransaction(handle uint64) {
	var status C.RustCallStatus
	C.uniffi_prolly_bindings_fn_free_bindingversionedtransaction(C.uint64_t(handle), &status)
}

func ffiFreeMapMerge(handle uint64) {
	var status C.RustCallStatus
	C.uniffi_prolly_bindings_fn_free_bindingmapmerge(C.uint64_t(handle), &status)
}

func ffiMapMergeVersion(handle uint64, which string) ([]byte, error) {
	clone, err := portableCloneMapMerge(handle)
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	var buf C.RustBuffer
	switch which {
	case "base":
		buf = C.uniffi_prolly_bindings_fn_method_bindingmapmerge_base(clone, &status)
	case "head":
		buf = C.uniffi_prolly_bindings_fn_method_bindingmapmerge_head(clone, &status)
	case "candidate":
		buf = C.uniffi_prolly_bindings_fn_method_bindingmapmerge_candidate(clone, &status)
	default:
		return nil, errors.New("unknown merge version selector")
	}
	if err := portableStatusError(&status); err != nil {
		return nil, err
	}
	return portableTakeBuffer(buf), nil
}

func portableResolverInput(resolver string) (C.RustBuffer, error) {
	var encoded bytes.Buffer
	encodeOptionalString(&encoded, optionalString(resolver))
	return portableInput(encoded.Bytes())
}

func ffiMapMergeMerge(handle uint64, resolver string) ([]byte, error) {
	clone, err := portableCloneMapMerge(handle)
	if err != nil {
		return nil, err
	}
	resolverBuf, err := portableResolverInput(resolver)
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_bindingmapmerge_merge(clone, resolverBuf, &status)
	if err := portableStatusError(&status); err != nil {
		return nil, err
	}
	return portableTakeBuffer(buf), nil
}

func ffiMapMergeConflictPage(handle uint64, cursor *RangeCursor, limit uint64) ([]byte, error) {
	clone, err := portableCloneMapMerge(handle)
	if err != nil {
		return nil, err
	}
	cursorBuf, err := portableInput(encodeOptionalRangeCursor(cursor))
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_bindingmapmerge_conflict_page(clone, cursorBuf, C.uint64_t(limit), &status)
	if err := portableStatusError(&status); err != nil {
		return nil, err
	}
	return portableTakeBuffer(buf), nil
}

func ffiMapMergePublish(handle uint64, resolver string) ([]byte, error) {
	clone, err := portableCloneMapMerge(handle)
	if err != nil {
		return nil, err
	}
	resolverBuf, err := portableResolverInput(resolver)
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_bindingmapmerge_publish(clone, resolverBuf, &status)
	if err := portableStatusError(&status); err != nil {
		return nil, err
	}
	return portableTakeBuffer(buf), nil
}

func ffiVersionedTransactionHead(handle uint64, mapID []byte) ([]byte, error) {
	clone, err := portableCloneVersionedTransaction(handle)
	if err != nil {
		return nil, err
	}
	mapBuf, err := portableInput(encodeByteArray(mapID))
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_bindingversionedtransaction_head(clone, mapBuf, &status)
	if err := portableStatusError(&status); err != nil {
		return nil, err
	}
	return portableTakeBuffer(buf), nil
}

func ffiVersionedTransactionGet(handle uint64, mapID, key []byte) ([]byte, error) {
	clone, err := portableCloneVersionedTransaction(handle)
	if err != nil {
		return nil, err
	}
	mapBuf, err := portableInput(encodeByteArray(mapID))
	if err != nil {
		return nil, err
	}
	keyBuf, err := portableInput(encodeByteArray(key))
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_bindingversionedtransaction_get(clone, mapBuf, keyBuf, &status)
	if err := portableStatusError(&status); err != nil {
		return nil, err
	}
	return portableTakeBuffer(buf), nil
}

func ffiVersionedTransactionApply(handle uint64, mapID, mutations []byte) ([]byte, error) {
	clone, err := portableCloneVersionedTransaction(handle)
	if err != nil {
		return nil, err
	}
	mapBuf, err := portableInput(encodeByteArray(mapID))
	if err != nil {
		return nil, err
	}
	mutationsBuf, err := portableInput(mutations)
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_bindingversionedtransaction_apply(clone, mapBuf, mutationsBuf, &status)
	if err := portableStatusError(&status); err != nil {
		return nil, err
	}
	return portableTakeBuffer(buf), nil
}

func ffiVersionedTransactionApplyIf(handle uint64, mapID, expected, mutations []byte) ([]byte, error) {
	clone, err := portableCloneVersionedTransaction(handle)
	if err != nil {
		return nil, err
	}
	mapBuf, err := portableInput(encodeByteArray(mapID))
	if err != nil {
		return nil, err
	}
	expectedBuf, err := portableInput(encodeOptionalByteArray(expected))
	if err != nil {
		return nil, err
	}
	mutationsBuf, err := portableInput(mutations)
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_bindingversionedtransaction_apply_if(clone, mapBuf, expectedBuf, mutationsBuf, &status)
	if err := portableStatusError(&status); err != nil {
		return nil, err
	}
	return portableTakeBuffer(buf), nil
}

func ffiVersionedTransactionPut(handle uint64, mapID, key, value []byte) ([]byte, error) {
	clone, err := portableCloneVersionedTransaction(handle)
	if err != nil {
		return nil, err
	}
	mapBuf, err := portableInput(encodeByteArray(mapID))
	if err != nil {
		return nil, err
	}
	keyBuf, err := portableInput(encodeByteArray(key))
	if err != nil {
		return nil, err
	}
	valueBuf, err := portableInput(encodeByteArray(value))
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_bindingversionedtransaction_put(clone, mapBuf, keyBuf, valueBuf, &status)
	if err := portableStatusError(&status); err != nil {
		return nil, err
	}
	return portableTakeBuffer(buf), nil
}

func ffiVersionedTransactionDelete(handle uint64, mapID, key []byte) ([]byte, error) {
	clone, err := portableCloneVersionedTransaction(handle)
	if err != nil {
		return nil, err
	}
	mapBuf, err := portableInput(encodeByteArray(mapID))
	if err != nil {
		return nil, err
	}
	keyBuf, err := portableInput(encodeByteArray(key))
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_bindingversionedtransaction_delete(clone, mapBuf, keyBuf, &status)
	if err := portableStatusError(&status); err != nil {
		return nil, err
	}
	return portableTakeBuffer(buf), nil
}

func ffiVersionedTransactionCommit(handle uint64) ([]byte, error) {
	clone, err := portableCloneVersionedTransaction(handle)
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_bindingversionedtransaction_commit(clone, &status)
	if err := portableStatusError(&status); err != nil {
		return nil, err
	}
	return portableTakeBuffer(buf), nil
}

func ffiVersionedTransactionRollback(handle uint64) error {
	clone, err := portableCloneVersionedTransaction(handle)
	if err != nil {
		return err
	}
	var status C.RustCallStatus
	C.uniffi_prolly_bindings_fn_method_bindingversionedtransaction_rollback(clone, &status)
	return portableStatusError(&status)
}

func ffiNewIndexRegistry() (uint64, error) {
	var status C.RustCallStatus
	handle := C.uniffi_prolly_bindings_fn_constructor_bindingindexregistry_new(&status)
	return uint64(handle), portableStatusError(&status)
}

func ffiRegisterIndexExtractorVtable() { C.prolly_register_go_index_extractor_vtable() }

func ffiIndexRegistryRegister(handle uint64, name []byte, generation uint64, extractorID string, projection int32, limits []byte, extractor uint64) error {
	clone, err := portableCloneRegistry(handle)
	if err != nil {
		return err
	}
	nameBuf, err := portableInput(encodeByteArray(name))
	if err != nil {
		return err
	}
	// UniFFI strings are raw UTF-8 RustBuffers, unlike Vec<u8> values which
	// carry an inner length prefix.
	idBuf, err := portableInput([]byte(extractorID))
	if err != nil {
		return err
	}
	projectionBuf, err := portableInput(encodeEnum(projection))
	if err != nil {
		return err
	}
	limitsBuf, err := portableInput(limits)
	if err != nil {
		return err
	}
	var status C.RustCallStatus
	C.uniffi_prolly_bindings_fn_method_bindingindexregistry_register(clone, nameBuf, C.uint64_t(generation), idBuf, projectionBuf, limitsBuf, C.uint64_t(extractor), &status)
	return portableStatusError(&status)
}

func ffiFreeIndexRegistry(handle uint64) {
	var status C.RustCallStatus
	C.uniffi_prolly_bindings_fn_free_bindingindexregistry(C.uint64_t(handle), &status)
}

func ffiIndexedMapID(handle uint64) ([]byte, error) {
	clone, err := portableCloneIndexedMap(handle)
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_bindingindexedmap_id(clone, &status)
	if err := portableStatusError(&status); err != nil {
		return nil, err
	}
	return portableTakeBuffer(buf), nil
}

func ffiIndexedMapApply(handle uint64, mutations []byte) ([]byte, error) {
	clone, err := portableCloneIndexedMap(handle)
	if err != nil {
		return nil, err
	}
	mutationsBuf, err := portableInput(mutations)
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_bindingindexedmap_apply(clone, mutationsBuf, &status)
	if err := portableStatusError(&status); err != nil {
		return nil, err
	}
	return portableTakeBuffer(buf), nil
}

func ffiIndexedMapApplyIf(handle uint64, expectedSource, mutations []byte) ([]byte, error) {
	clone, err := portableCloneIndexedMap(handle)
	if err != nil {
		return nil, err
	}
	expectedBuf, err := portableInput(encodeOptionalByteArray(expectedSource))
	if err != nil {
		return nil, err
	}
	mutationsBuf, err := portableInput(mutations)
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_bindingindexedmap_apply_if(clone, expectedBuf, mutationsBuf, &status)
	if err := portableStatusError(&status); err != nil {
		return nil, err
	}
	return portableTakeBuffer(buf), nil
}

func ffiIndexedMapPut(handle uint64, key, value []byte) ([]byte, error) {
	clone, err := portableCloneIndexedMap(handle)
	if err != nil {
		return nil, err
	}
	keyBuf, err := portableInput(encodeByteArray(key))
	if err != nil {
		return nil, err
	}
	valueBuf, err := portableInput(encodeByteArray(value))
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_bindingindexedmap_put(clone, keyBuf, valueBuf, &status)
	if err := portableStatusError(&status); err != nil {
		return nil, err
	}
	return portableTakeBuffer(buf), nil
}

func ffiIndexedMapGet(handle uint64, key []byte) ([]byte, error) {
	clone, err := portableCloneIndexedMap(handle)
	if err != nil {
		return nil, err
	}
	keyBuf, err := portableInput(encodeByteArray(key))
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_bindingindexedmap_get(clone, keyBuf, &status)
	if err := portableStatusError(&status); err != nil {
		return nil, err
	}
	return portableTakeBuffer(buf), nil
}

func ffiIndexedMapDelete(handle uint64, key []byte) ([]byte, error) {
	clone, err := portableCloneIndexedMap(handle)
	if err != nil {
		return nil, err
	}
	keyBuf, err := portableInput(encodeByteArray(key))
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_bindingindexedmap_delete(clone, keyBuf, &status)
	if err := portableStatusError(&status); err != nil {
		return nil, err
	}
	return portableTakeBuffer(buf), nil
}

func ffiIndexedMapEnsureIndex(handle uint64, name []byte) ([]byte, error) {
	clone, err := portableCloneIndexedMap(handle)
	if err != nil {
		return nil, err
	}
	nameBuf, err := portableInput(encodeByteArray(name))
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_bindingindexedmap_ensure_index(clone, nameBuf, &status)
	if err := portableStatusError(&status); err != nil {
		return nil, err
	}
	return portableTakeBuffer(buf), nil
}

func ffiIndexedMapHealth(handle uint64) ([]byte, error) {
	clone, err := portableCloneIndexedMap(handle)
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_bindingindexedmap_health(clone, &status)
	if err := portableStatusError(&status); err != nil {
		return nil, err
	}
	return portableTakeBuffer(buf), nil
}

func ffiIndexedMapRepairIndex(handle uint64, name, sourceVersion []byte) ([]byte, error) {
	clone, err := portableCloneIndexedMap(handle)
	if err != nil {
		return nil, err
	}
	nameBuf, err := portableInput(encodeByteArray(name))
	if err != nil {
		return nil, err
	}
	versionBuf, err := portableInput(encodeByteArray(sourceVersion))
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_bindingindexedmap_repair_index(clone, nameBuf, versionBuf, &status)
	if err := portableStatusError(&status); err != nil {
		return nil, err
	}
	return portableTakeBuffer(buf), nil
}

func ffiIndexedMapDeactivateIndex(handle uint64, name []byte) ([]byte, error) {
	clone, err := portableCloneIndexedMap(handle)
	if err != nil {
		return nil, err
	}
	nameBuf, err := portableInput(encodeByteArray(name))
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_bindingindexedmap_deactivate_index(clone, nameBuf, &status)
	if err := portableStatusError(&status); err != nil {
		return nil, err
	}
	return portableTakeBuffer(buf), nil
}

func ffiIndexedMapSnapshot(handle uint64) (uint64, error) {
	clone, err := portableCloneIndexedMap(handle)
	if err != nil {
		return 0, err
	}
	var status C.RustCallStatus
	result := C.uniffi_prolly_bindings_fn_method_bindingindexedmap_snapshot(clone, &status)
	return uint64(result), portableStatusError(&status)
}

func ffiIndexedMapSnapshotAt(handle uint64, sourceVersion []byte) (uint64, error) {
	clone, err := portableCloneIndexedMap(handle)
	if err != nil {
		return 0, err
	}
	versionBuf, err := portableInput(encodeByteArray(sourceVersion))
	if err != nil {
		return 0, err
	}
	var status C.RustCallStatus
	result := C.uniffi_prolly_bindings_fn_method_bindingindexedmap_snapshot_at(clone, versionBuf, &status)
	return uint64(result), portableStatusError(&status)
}

func ffiIndexedMapSnapshotByID(handle uint64, snapshotID []byte) (uint64, error) {
	clone, err := portableCloneIndexedMap(handle)
	if err != nil {
		return 0, err
	}
	idBuf, err := portableInput(snapshotID)
	if err != nil {
		return 0, err
	}
	var status C.RustCallStatus
	result := C.uniffi_prolly_bindings_fn_method_bindingindexedmap_snapshot_by_id(clone, idBuf, &status)
	return uint64(result), portableStatusError(&status)
}

func ffiIndexedMapExportCurrent(handle uint64) ([]byte, error) {
	clone, err := portableCloneIndexedMap(handle)
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_bindingindexedmap_export_current(clone, &status)
	if err := portableStatusError(&status); err != nil {
		return nil, err
	}
	return portableTakeBuffer(buf), nil
}

func ffiIndexedMapImportCurrent(handle uint64, bundle, expectedSource []byte) ([]byte, error) {
	clone, err := portableCloneIndexedMap(handle)
	if err != nil {
		return nil, err
	}
	bundleBuf, err := portableInput(encodeByteArray(bundle))
	if err != nil {
		return nil, err
	}
	expectedBuf, err := portableInput(encodeOptionalByteArray(expectedSource))
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_bindingindexedmap_import_current(clone, bundleBuf, expectedBuf, &status)
	if err := portableStatusError(&status); err != nil {
		return nil, err
	}
	return portableTakeBuffer(buf), nil
}

func ffiIndexedMapKeepLast(handle, count uint64) ([]byte, error) {
	clone, err := portableCloneIndexedMap(handle)
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_bindingindexedmap_keep_last(clone, C.uint64_t(count), &status)
	if err := portableStatusError(&status); err != nil {
		return nil, err
	}
	return portableTakeBuffer(buf), nil
}

func ffiIndexedMapMetrics(handle uint64) ([]byte, error) {
	clone, err := portableCloneIndexedMap(handle)
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_bindingindexedmap_metrics(clone, &status)
	if err := portableStatusError(&status); err != nil {
		return nil, err
	}
	return portableTakeBuffer(buf), nil
}

func ffiIndexedMapVerifyIndex(handle uint64, name, sourceVersion []byte) ([]byte, error) {
	clone, err := portableCloneIndexedMap(handle)
	if err != nil {
		return nil, err
	}
	nameBuf, err := portableInput(encodeByteArray(name))
	if err != nil {
		return nil, err
	}
	versionBuf, err := portableInput(encodeByteArray(sourceVersion))
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_bindingindexedmap_verify_index(clone, nameBuf, versionBuf, &status)
	if err := portableStatusError(&status); err != nil {
		return nil, err
	}
	return portableTakeBuffer(buf), nil
}

func ffiIndexedMapVerifyAll(handle uint64, sourceVersion []byte) ([]byte, error) {
	clone, err := portableCloneIndexedMap(handle)
	if err != nil {
		return nil, err
	}
	versionBuf, err := portableInput(encodeByteArray(sourceVersion))
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_bindingindexedmap_verify_all(clone, versionBuf, &status)
	if err := portableStatusError(&status); err != nil {
		return nil, err
	}
	return portableTakeBuffer(buf), nil
}

func ffiFreeIndexedMap(handle uint64) {
	var status C.RustCallStatus
	C.uniffi_prolly_bindings_fn_free_bindingindexedmap(C.uint64_t(handle), &status)
}
func ffiFreeIndexedSnapshot(handle uint64) {
	var status C.RustCallStatus
	C.uniffi_prolly_bindings_fn_free_bindingindexedsnapshot(C.uint64_t(handle), &status)
}
func ffiFreeSecondaryIndex(handle uint64) {
	var status C.RustCallStatus
	C.uniffi_prolly_bindings_fn_free_bindingsecondaryindexsnapshot(C.uint64_t(handle), &status)
}

func ffiIndexedSnapshotID(handle uint64) ([]byte, error) {
	clone, err := portableCloneIndexedSnapshot(handle)
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_bindingindexedsnapshot_id(clone, &status)
	if err := portableStatusError(&status); err != nil {
		return nil, err
	}
	return portableTakeBuffer(buf), nil
}

func ffiIndexedSnapshotIndex(handle uint64, name []byte) (uint64, error) {
	clone, err := portableCloneIndexedSnapshot(handle)
	if err != nil {
		return 0, err
	}
	nameBuf, err := portableInput(encodeByteArray(name))
	if err != nil {
		return 0, err
	}
	var status C.RustCallStatus
	result := C.uniffi_prolly_bindings_fn_method_bindingindexedsnapshot_index(clone, nameBuf, &status)
	return uint64(result), portableStatusError(&status)
}

func ffiSecondaryIndexName(handle uint64) ([]byte, error) {
	clone, err := portableCloneSecondaryIndex(handle)
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_bindingsecondaryindexsnapshot_name(clone, &status)
	if err := portableStatusError(&status); err != nil {
		return nil, err
	}
	return portableTakeBuffer(buf), nil
}

func ffiSecondaryIndexUnary(handle uint64, value []byte, call func(C.uint64_t, C.RustBuffer, *C.RustCallStatus) C.RustBuffer) ([]byte, error) {
	clone, err := portableCloneSecondaryIndex(handle)
	if err != nil {
		return nil, err
	}
	valueBuf, err := portableInput(encodeByteArray(value))
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	buf := call(clone, valueBuf, &status)
	if err := portableStatusError(&status); err != nil {
		return nil, err
	}
	return portableTakeBuffer(buf), nil
}

func ffiSecondaryIndexExact(handle uint64, term []byte) ([]byte, error) {
	return ffiSecondaryIndexUnary(handle, term, func(clone C.uint64_t, value C.RustBuffer, status *C.RustCallStatus) C.RustBuffer {
		return C.uniffi_prolly_bindings_fn_method_bindingsecondaryindexsnapshot_exact(clone, value, status)
	})
}

func ffiSecondaryIndexPrefix(handle uint64, prefix []byte) ([]byte, error) {
	return ffiSecondaryIndexUnary(handle, prefix, func(clone C.uint64_t, value C.RustBuffer, status *C.RustCallStatus) C.RustBuffer {
		return C.uniffi_prolly_bindings_fn_method_bindingsecondaryindexsnapshot_prefix(clone, value, status)
	})
}

func ffiSecondaryIndexRange(handle uint64, start, end []byte) ([]byte, error) {
	clone, err := portableCloneSecondaryIndex(handle)
	if err != nil {
		return nil, err
	}
	startBuf, err := portableInput(encodeByteArray(start))
	if err != nil {
		return nil, err
	}
	endBuf, err := portableInput(encodeOptionalByteArray(end))
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_bindingsecondaryindexsnapshot_range(clone, startBuf, endBuf, &status)
	if err := portableStatusError(&status); err != nil {
		return nil, err
	}
	return portableTakeBuffer(buf), nil
}

type secondaryIndexPageCall func(C.uint64_t, C.RustBuffer, C.RustBuffer, C.uint64_t, *C.RustCallStatus) C.RustBuffer

func ffiSecondaryIndexPage(handle uint64, key, cursor []byte, limit uint64, call secondaryIndexPageCall) ([]byte, error) {
	clone, err := portableCloneSecondaryIndex(handle)
	if err != nil {
		return nil, err
	}
	keyBuf, err := portableInput(encodeByteArray(key))
	if err != nil {
		return nil, err
	}
	cursorBuf, err := portableInput(encodeOptionalByteArray(cursor))
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	buf := call(clone, keyBuf, cursorBuf, C.uint64_t(limit), &status)
	if err := portableStatusError(&status); err != nil {
		return nil, err
	}
	return portableTakeBuffer(buf), nil
}

func ffiSecondaryIndexExactPage(handle uint64, term, cursor []byte, limit uint64, reverse bool) ([]byte, error) {
	if reverse {
		return ffiSecondaryIndexPage(handle, term, cursor, limit, func(clone C.uint64_t, key, cursor C.RustBuffer, limit C.uint64_t, status *C.RustCallStatus) C.RustBuffer {
			return C.uniffi_prolly_bindings_fn_method_bindingsecondaryindexsnapshot_exact_reverse_page(clone, key, cursor, limit, status)
		})
	}
	return ffiSecondaryIndexPage(handle, term, cursor, limit, func(clone C.uint64_t, key, cursor C.RustBuffer, limit C.uint64_t, status *C.RustCallStatus) C.RustBuffer {
		return C.uniffi_prolly_bindings_fn_method_bindingsecondaryindexsnapshot_exact_page(clone, key, cursor, limit, status)
	})
}

func ffiSecondaryIndexPrefixPage(handle uint64, prefix, cursor []byte, limit uint64, reverse bool) ([]byte, error) {
	if reverse {
		return ffiSecondaryIndexPage(handle, prefix, cursor, limit, func(clone C.uint64_t, key, cursor C.RustBuffer, limit C.uint64_t, status *C.RustCallStatus) C.RustBuffer {
			return C.uniffi_prolly_bindings_fn_method_bindingsecondaryindexsnapshot_prefix_reverse_page(clone, key, cursor, limit, status)
		})
	}
	return ffiSecondaryIndexPage(handle, prefix, cursor, limit, func(clone C.uint64_t, key, cursor C.RustBuffer, limit C.uint64_t, status *C.RustCallStatus) C.RustBuffer {
		return C.uniffi_prolly_bindings_fn_method_bindingsecondaryindexsnapshot_prefix_page(clone, key, cursor, limit, status)
	})
}

type secondaryIndexRangePageCall func(C.uint64_t, C.RustBuffer, C.RustBuffer, C.RustBuffer, C.uint64_t, *C.RustCallStatus) C.RustBuffer

func ffiSecondaryIndexRangePage(handle uint64, start, end, cursor []byte, limit uint64, reverse bool) ([]byte, error) {
	clone, err := portableCloneSecondaryIndex(handle)
	if err != nil {
		return nil, err
	}
	startBuf, err := portableInput(encodeByteArray(start))
	if err != nil {
		return nil, err
	}
	endBuf, err := portableInput(encodeOptionalByteArray(end))
	if err != nil {
		return nil, err
	}
	cursorBuf, err := portableInput(encodeOptionalByteArray(cursor))
	if err != nil {
		return nil, err
	}
	var call secondaryIndexRangePageCall
	if reverse {
		call = func(clone C.uint64_t, start, end, cursor C.RustBuffer, limit C.uint64_t, status *C.RustCallStatus) C.RustBuffer {
			return C.uniffi_prolly_bindings_fn_method_bindingsecondaryindexsnapshot_range_reverse_page(clone, start, end, cursor, limit, status)
		}
	} else {
		call = func(clone C.uint64_t, start, end, cursor C.RustBuffer, limit C.uint64_t, status *C.RustCallStatus) C.RustBuffer {
			return C.uniffi_prolly_bindings_fn_method_bindingsecondaryindexsnapshot_range_page(clone, start, end, cursor, limit, status)
		}
	}
	var status C.RustCallStatus
	buf := call(clone, startBuf, endBuf, cursorBuf, C.uint64_t(limit), &status)
	if err := portableStatusError(&status); err != nil {
		return nil, err
	}
	return portableTakeBuffer(buf), nil
}

func ffiSecondaryIndexRecords(handle uint64, term []byte) ([]byte, error) {
	clone, err := portableCloneSecondaryIndex(handle)
	if err != nil {
		return nil, err
	}
	termBuf, err := portableInput(encodeByteArray(term))
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_bindingsecondaryindexsnapshot_records(clone, termBuf, &status)
	if err := portableStatusError(&status); err != nil {
		return nil, err
	}
	return portableTakeBuffer(buf), nil
}

func ffiSecondaryIndexFastHandle(handle uint64) (uint64, error) {
	clone, err := portableCloneSecondaryIndex(handle)
	if err != nil {
		return 0, err
	}
	var status C.RustCallStatus
	fast := C.uniffi_prolly_bindings_fn_method_bindingsecondaryindexsnapshot_fast_handle(clone, &status)
	return uint64(fast), portableStatusError(&status)
}

func ffiProximityFastHandle(handle uint64) (uint64, error) {
	clone, err := ffiCloneProximity(handle)
	if err != nil {
		return 0, err
	}
	var status C.RustCallStatus
	fast := C.uniffi_prolly_bindings_fn_method_bindingproximitymap_fast_handle(C.uint64_t(clone), &status)
	return uint64(fast), portableStatusError(&status)
}

func ffiDefaultHNSWConfig() ([]byte, error) {
	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_func_default_hnsw_config(&status)
	if err := portableStatusError(&status); err != nil {
		return nil, err
	}
	return portableTakeBuffer(buf), nil
}

func ffiDefaultHNSWBuildLimits() ([]byte, error) {
	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_func_default_hnsw_build_limits(&status)
	if err := portableStatusError(&status); err != nil {
		return nil, err
	}
	return portableTakeBuffer(buf), nil
}

func ffiProximityBuildHNSW(handle uint64, config, limits []byte) ([]byte, error) {
	configBuf, err := portableInput(config)
	if err != nil {
		return nil, err
	}
	limitsBuf, err := portableInput(limits)
	if err != nil {
		portableFreeBuffer(configBuf)
		return nil, err
	}
	clone, err := ffiCloneProximity(handle)
	if err != nil {
		portableFreeBuffer(configBuf)
		portableFreeBuffer(limitsBuf)
		return nil, err
	}
	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_bindingproximitymap_build_hnsw(
		C.uint64_t(clone), configBuf, limitsBuf, &status,
	)
	if err := portableStatusError(&status); err != nil {
		return nil, err
	}
	return portableTakeBuffer(buf), nil
}

func ffiProximityLoadHNSW(handle uint64, manifest []byte) (uint64, error) {
	manifestBuf, err := portableInput(encodeByteArray(manifest))
	if err != nil {
		return 0, err
	}
	clone, err := ffiCloneProximity(handle)
	if err != nil {
		portableFreeBuffer(manifestBuf)
		return 0, err
	}
	var status C.RustCallStatus
	index := C.uniffi_prolly_bindings_fn_method_bindingproximitymap_load_hnsw(
		C.uint64_t(clone), manifestBuf, &status,
	)
	return uint64(index), portableStatusError(&status)
}

func ffiCloneHNSWIndex(handle uint64) (uint64, error) {
	var status C.RustCallStatus
	clone := C.uniffi_prolly_bindings_fn_clone_bindinghnswindex(C.uint64_t(handle), &status)
	return uint64(clone), portableStatusError(&status)
}

func ffiFreeHNSWIndex(handle uint64) {
	var status C.RustCallStatus
	C.uniffi_prolly_bindings_fn_free_bindinghnswindex(C.uint64_t(handle), &status)
}

func ffiHNSWIndexBufferCall(
	handle uint64,
	call func(C.uint64_t, *C.RustCallStatus) C.RustBuffer,
) ([]byte, error) {
	clone, err := ffiCloneHNSWIndex(handle)
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	buf := call(C.uint64_t(clone), &status)
	if err := portableStatusError(&status); err != nil {
		return nil, err
	}
	return portableTakeBuffer(buf), nil
}

func ffiHNSWIndexManifest(handle uint64) ([]byte, error) {
	return ffiHNSWIndexBufferCall(handle, func(clone C.uint64_t, status *C.RustCallStatus) C.RustBuffer {
		return C.uniffi_prolly_bindings_fn_method_bindinghnswindex_manifest(clone, status)
	})
}

func ffiHNSWIndexSourceDescriptor(handle uint64) ([]byte, error) {
	return ffiHNSWIndexBufferCall(handle, func(clone C.uint64_t, status *C.RustCallStatus) C.RustBuffer {
		return C.uniffi_prolly_bindings_fn_method_bindinghnswindex_source_descriptor(clone, status)
	})
}

func ffiHNSWIndexConfig(handle uint64) ([]byte, error) {
	return ffiHNSWIndexBufferCall(handle, func(clone C.uint64_t, status *C.RustCallStatus) C.RustBuffer {
		return C.uniffi_prolly_bindings_fn_method_bindinghnswindex_config(clone, status)
	})
}

func ffiHNSWIndexIsCanonical(handle uint64) (bool, error) {
	clone, err := ffiCloneHNSWIndex(handle)
	if err != nil {
		return false, err
	}
	var status C.RustCallStatus
	value := C.uniffi_prolly_bindings_fn_method_bindinghnswindex_is_canonical(C.uint64_t(clone), &status)
	return value != 0, portableStatusError(&status)
}

func ffiHNSWIndexSearch(index, proximity uint64, request []byte) ([]byte, error) {
	requestBuf, err := portableInput(request)
	if err != nil {
		return nil, err
	}
	indexClone, err := ffiCloneHNSWIndex(index)
	if err != nil {
		portableFreeBuffer(requestBuf)
		return nil, err
	}
	mapClone, err := ffiCloneProximity(proximity)
	if err != nil {
		portableFreeBuffer(requestBuf)
		ffiFreeHNSWIndex(indexClone)
		return nil, err
	}
	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_bindinghnswindex_search(
		C.uint64_t(indexClone), C.uint64_t(mapClone), requestBuf, &status,
	)
	if err := portableStatusError(&status); err != nil {
		return nil, err
	}
	return portableTakeBuffer(buf), nil
}

func ffiHNSWIndexProveSearch(index, proximity uint64, request, limits []byte) (uint64, error) {
	requestBuf, err := portableInput(request)
	if err != nil {
		return 0, err
	}
	limitsBuf, err := portableInput(limits)
	if err != nil {
		portableFreeBuffer(requestBuf)
		return 0, err
	}
	indexClone, err := ffiCloneHNSWIndex(index)
	if err != nil {
		portableFreeBuffer(requestBuf)
		portableFreeBuffer(limitsBuf)
		return 0, err
	}
	mapClone, err := ffiCloneProximity(proximity)
	if err != nil {
		portableFreeBuffer(requestBuf)
		portableFreeBuffer(limitsBuf)
		ffiFreeHNSWIndex(indexClone)
		return 0, err
	}
	var status C.RustCallStatus
	proof := C.uniffi_prolly_bindings_fn_method_bindinghnswindex_prove_search(
		C.uint64_t(indexClone), C.uint64_t(mapClone), requestBuf, limitsBuf, &status,
	)
	return uint64(proof), portableStatusError(&status)
}

func ffiDefaultPQConfig() ([]byte, error) {
	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_func_default_pq_config(&status)
	if err := portableStatusError(&status); err != nil {
		return nil, err
	}
	return portableTakeBuffer(buf), nil
}

func ffiDefaultPQBuildLimits() ([]byte, error) {
	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_func_default_pq_build_limits(&status)
	if err := portableStatusError(&status); err != nil {
		return nil, err
	}
	return portableTakeBuffer(buf), nil
}

func ffiProximityBuildPQ(handle uint64, config []byte, workerThreads uint64, limits []byte) ([]byte, error) {
	configBuf, err := portableInput(config)
	if err != nil {
		return nil, err
	}
	limitsBuf, err := portableInput(limits)
	if err != nil {
		portableFreeBuffer(configBuf)
		return nil, err
	}
	clone, err := ffiCloneProximity(handle)
	if err != nil {
		portableFreeBuffer(configBuf)
		portableFreeBuffer(limitsBuf)
		return nil, err
	}
	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_bindingproximitymap_build_pq(
		C.uint64_t(clone), configBuf, C.uint64_t(workerThreads), limitsBuf, &status,
	)
	if err := portableStatusError(&status); err != nil {
		return nil, err
	}
	return portableTakeBuffer(buf), nil
}

func ffiProximityLoadPQ(handle uint64, manifest []byte) (uint64, error) {
	manifestBuf, err := portableInput(encodeByteArray(manifest))
	if err != nil {
		return 0, err
	}
	clone, err := ffiCloneProximity(handle)
	if err != nil {
		portableFreeBuffer(manifestBuf)
		return 0, err
	}
	var status C.RustCallStatus
	index := C.uniffi_prolly_bindings_fn_method_bindingproximitymap_load_pq(
		C.uint64_t(clone), manifestBuf, &status,
	)
	return uint64(index), portableStatusError(&status)
}

func ffiCloneProductQuantizer(handle uint64) (uint64, error) {
	var status C.RustCallStatus
	clone := C.uniffi_prolly_bindings_fn_clone_bindingproductquantizer(C.uint64_t(handle), &status)
	return uint64(clone), portableStatusError(&status)
}

func ffiFreeProductQuantizer(handle uint64) {
	var status C.RustCallStatus
	C.uniffi_prolly_bindings_fn_free_bindingproductquantizer(C.uint64_t(handle), &status)
}

func ffiProductQuantizerBufferCall(
	handle uint64,
	call func(C.uint64_t, *C.RustCallStatus) C.RustBuffer,
) ([]byte, error) {
	clone, err := ffiCloneProductQuantizer(handle)
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	buf := call(C.uint64_t(clone), &status)
	if err := portableStatusError(&status); err != nil {
		return nil, err
	}
	return portableTakeBuffer(buf), nil
}

func ffiProductQuantizerManifest(handle uint64) ([]byte, error) {
	return ffiProductQuantizerBufferCall(handle, func(clone C.uint64_t, status *C.RustCallStatus) C.RustBuffer {
		return C.uniffi_prolly_bindings_fn_method_bindingproductquantizer_manifest(clone, status)
	})
}

func ffiProductQuantizerSourceDescriptor(handle uint64) ([]byte, error) {
	return ffiProductQuantizerBufferCall(handle, func(clone C.uint64_t, status *C.RustCallStatus) C.RustBuffer {
		return C.uniffi_prolly_bindings_fn_method_bindingproductquantizer_source_descriptor(clone, status)
	})
}

func ffiProductQuantizerConfig(handle uint64) ([]byte, error) {
	return ffiProductQuantizerBufferCall(handle, func(clone C.uint64_t, status *C.RustCallStatus) C.RustBuffer {
		return C.uniffi_prolly_bindings_fn_method_bindingproductquantizer_config(clone, status)
	})
}

func ffiProductQuantizerQuality(handle uint64) ([]byte, error) {
	return ffiProductQuantizerBufferCall(handle, func(clone C.uint64_t, status *C.RustCallStatus) C.RustBuffer {
		return C.uniffi_prolly_bindings_fn_method_bindingproductquantizer_quality(clone, status)
	})
}

func ffiProductQuantizerSearch(index, proximity uint64, request []byte) ([]byte, error) {
	requestBuf, err := portableInput(request)
	if err != nil {
		return nil, err
	}
	indexClone, err := ffiCloneProductQuantizer(index)
	if err != nil {
		portableFreeBuffer(requestBuf)
		return nil, err
	}
	mapClone, err := ffiCloneProximity(proximity)
	if err != nil {
		portableFreeBuffer(requestBuf)
		ffiFreeProductQuantizer(indexClone)
		return nil, err
	}
	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_bindingproductquantizer_search(
		C.uint64_t(indexClone), C.uint64_t(mapClone), requestBuf, &status,
	)
	if err := portableStatusError(&status); err != nil {
		return nil, err
	}
	return portableTakeBuffer(buf), nil
}

func ffiProductQuantizerProveSearch(index, proximity uint64, request, limits []byte) (uint64, error) {
	requestBuf, err := portableInput(request)
	if err != nil {
		return 0, err
	}
	limitsBuf, err := portableInput(limits)
	if err != nil {
		portableFreeBuffer(requestBuf)
		return 0, err
	}
	indexClone, err := ffiCloneProductQuantizer(index)
	if err != nil {
		portableFreeBuffer(requestBuf)
		portableFreeBuffer(limitsBuf)
		return 0, err
	}
	mapClone, err := ffiCloneProximity(proximity)
	if err != nil {
		portableFreeBuffer(requestBuf)
		portableFreeBuffer(limitsBuf)
		ffiFreeProductQuantizer(indexClone)
		return 0, err
	}
	var status C.RustCallStatus
	proof := C.uniffi_prolly_bindings_fn_method_bindingproductquantizer_prove_search(
		C.uint64_t(indexClone), C.uint64_t(mapClone), requestBuf, limitsBuf, &status,
	)
	return uint64(proof), portableStatusError(&status)
}

func ffiDefaultCompositeConfig() ([]byte, error) {
	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_func_default_composite_accelerator_config(&status)
	if err := portableStatusError(&status); err != nil {
		return nil, err
	}
	return portableTakeBuffer(buf), nil
}

func ffiDefaultCompositeBuildLimits() ([]byte, error) {
	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_func_default_composite_build_limits(&status)
	if err := portableStatusError(&status); err != nil {
		return nil, err
	}
	return portableTakeBuffer(buf), nil
}

func ffiDefaultCompositeRebuildOptions() ([]byte, error) {
	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_func_default_composite_rebuild_options(&status)
	if err := portableStatusError(&status); err != nil {
		return nil, err
	}
	return portableTakeBuffer(buf), nil
}

func ffiCompositeInputs(current, baseMap, base uint64, cloneBase func(uint64) (uint64, error), config, limits []byte) (uint64, uint64, uint64, C.RustBuffer, C.RustBuffer, error) {
	configBuf, err := portableInput(config)
	if err != nil {
		return 0, 0, 0, C.RustBuffer{}, C.RustBuffer{}, err
	}
	limitsBuf, err := portableInput(limits)
	if err != nil {
		portableFreeBuffer(configBuf)
		return 0, 0, 0, C.RustBuffer{}, C.RustBuffer{}, err
	}
	currentClone, err := ffiCloneProximity(current)
	if err != nil {
		portableFreeBuffer(configBuf)
		portableFreeBuffer(limitsBuf)
		return 0, 0, 0, C.RustBuffer{}, C.RustBuffer{}, err
	}
	baseMapClone, err := ffiCloneProximity(baseMap)
	if err != nil {
		portableFreeBuffer(configBuf)
		portableFreeBuffer(limitsBuf)
		ffiFreeProximity(currentClone)
		return 0, 0, 0, C.RustBuffer{}, C.RustBuffer{}, err
	}
	baseClone, err := cloneBase(base)
	if err != nil {
		portableFreeBuffer(configBuf)
		portableFreeBuffer(limitsBuf)
		ffiFreeProximity(currentClone)
		ffiFreeProximity(baseMapClone)
		return 0, 0, 0, C.RustBuffer{}, C.RustBuffer{}, err
	}
	return currentClone, baseMapClone, baseClone, configBuf, limitsBuf, nil
}

func ffiProximityBuildCompositeHNSW(current, baseMap, base uint64, config, limits []byte) ([]byte, error) {
	currentClone, baseMapClone, baseClone, configBuf, limitsBuf, err := ffiCompositeInputs(current, baseMap, base, ffiCloneHNSWIndex, config, limits)
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_bindingproximitymap_build_composite_hnsw(C.uint64_t(currentClone), C.uint64_t(baseMapClone), C.uint64_t(baseClone), configBuf, limitsBuf, &status)
	if err := portableStatusError(&status); err != nil {
		return nil, err
	}
	return portableTakeBuffer(buf), nil
}

func ffiProximityBuildCompositePQ(current, baseMap, base uint64, config, limits []byte) ([]byte, error) {
	currentClone, baseMapClone, baseClone, configBuf, limitsBuf, err := ffiCompositeInputs(current, baseMap, base, ffiCloneProductQuantizer, config, limits)
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_bindingproximitymap_build_composite_pq(C.uint64_t(currentClone), C.uint64_t(baseMapClone), C.uint64_t(baseClone), configBuf, limitsBuf, &status)
	if err := portableStatusError(&status); err != nil {
		return nil, err
	}
	return portableTakeBuffer(buf), nil
}

func ffiProximityRebuildCompositeHNSW(current, baseMap, base uint64, config, limits, rebuild []byte) ([]byte, error) {
	currentClone, baseMapClone, baseClone, configBuf, limitsBuf, err := ffiCompositeInputs(current, baseMap, base, ffiCloneHNSWIndex, config, limits)
	if err != nil {
		return nil, err
	}
	rebuildBuf, err := portableInput(rebuild)
	if err != nil {
		portableFreeBuffer(configBuf)
		portableFreeBuffer(limitsBuf)
		ffiFreeProximity(currentClone)
		ffiFreeProximity(baseMapClone)
		ffiFreeHNSWIndex(baseClone)
		return nil, err
	}
	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_bindingproximitymap_build_or_rebuild_composite_hnsw(C.uint64_t(currentClone), C.uint64_t(baseMapClone), C.uint64_t(baseClone), configBuf, limitsBuf, rebuildBuf, &status)
	if err := portableStatusError(&status); err != nil {
		return nil, err
	}
	return portableTakeBuffer(buf), nil
}

func ffiProximityRebuildCompositePQ(current, baseMap, base uint64, config, limits, rebuild []byte) ([]byte, error) {
	currentClone, baseMapClone, baseClone, configBuf, limitsBuf, err := ffiCompositeInputs(current, baseMap, base, ffiCloneProductQuantizer, config, limits)
	if err != nil {
		return nil, err
	}
	rebuildBuf, err := portableInput(rebuild)
	if err != nil {
		portableFreeBuffer(configBuf)
		portableFreeBuffer(limitsBuf)
		ffiFreeProximity(currentClone)
		ffiFreeProximity(baseMapClone)
		ffiFreeProductQuantizer(baseClone)
		return nil, err
	}
	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_bindingproximitymap_build_or_rebuild_composite_pq(C.uint64_t(currentClone), C.uint64_t(baseMapClone), C.uint64_t(baseClone), configBuf, limitsBuf, rebuildBuf, &status)
	if err := portableStatusError(&status); err != nil {
		return nil, err
	}
	return portableTakeBuffer(buf), nil
}

func ffiProximityLoadComposite(handle uint64, manifest []byte) (uint64, error) {
	manifestBuf, err := portableInput(encodeByteArray(manifest))
	if err != nil {
		return 0, err
	}
	clone, err := ffiCloneProximity(handle)
	if err != nil {
		portableFreeBuffer(manifestBuf)
		return 0, err
	}
	var status C.RustCallStatus
	value := C.uniffi_prolly_bindings_fn_method_bindingproximitymap_load_composite(C.uint64_t(clone), manifestBuf, &status)
	return uint64(value), portableStatusError(&status)
}

func ffiCloneComposite(handle uint64) (uint64, error) {
	var status C.RustCallStatus
	value := C.uniffi_prolly_bindings_fn_clone_bindingcompositeaccelerator(C.uint64_t(handle), &status)
	return uint64(value), portableStatusError(&status)
}
func ffiFreeComposite(handle uint64) {
	var status C.RustCallStatus
	C.uniffi_prolly_bindings_fn_free_bindingcompositeaccelerator(C.uint64_t(handle), &status)
}

func ffiCompositeBufferCall(handle uint64, call func(C.uint64_t, *C.RustCallStatus) C.RustBuffer) ([]byte, error) {
	clone, err := ffiCloneComposite(handle)
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	buf := call(C.uint64_t(clone), &status)
	if err := portableStatusError(&status); err != nil {
		return nil, err
	}
	return portableTakeBuffer(buf), nil
}
func ffiCompositeManifest(handle uint64) ([]byte, error) {
	return ffiCompositeBufferCall(handle, func(clone C.uint64_t, status *C.RustCallStatus) C.RustBuffer {
		return C.uniffi_prolly_bindings_fn_method_bindingcompositeaccelerator_manifest(clone, status)
	})
}
func ffiCompositeCurrentSource(handle uint64) ([]byte, error) {
	return ffiCompositeBufferCall(handle, func(clone C.uint64_t, status *C.RustCallStatus) C.RustBuffer {
		return C.uniffi_prolly_bindings_fn_method_bindingcompositeaccelerator_current_source_descriptor(clone, status)
	})
}
func ffiCompositeBaseSource(handle uint64) ([]byte, error) {
	return ffiCompositeBufferCall(handle, func(clone C.uint64_t, status *C.RustCallStatus) C.RustBuffer {
		return C.uniffi_prolly_bindings_fn_method_bindingcompositeaccelerator_base_source_descriptor(clone, status)
	})
}
func ffiCompositeBaseKind(handle uint64) ([]byte, error) {
	return ffiCompositeBufferCall(handle, func(clone C.uint64_t, status *C.RustCallStatus) C.RustBuffer {
		return C.uniffi_prolly_bindings_fn_method_bindingcompositeaccelerator_base_kind(clone, status)
	})
}
func ffiCompositeConfig(handle uint64) ([]byte, error) {
	return ffiCompositeBufferCall(handle, func(clone C.uint64_t, status *C.RustCallStatus) C.RustBuffer {
		return C.uniffi_prolly_bindings_fn_method_bindingcompositeaccelerator_config(clone, status)
	})
}
func ffiCompositeBuildStats(handle uint64) ([]byte, error) {
	return ffiCompositeBufferCall(handle, func(clone C.uint64_t, status *C.RustCallStatus) C.RustBuffer {
		return C.uniffi_prolly_bindings_fn_method_bindingcompositeaccelerator_build_stats(clone, status)
	})
}
func ffiCompositeCount(handle uint64, shadow bool) (uint64, error) {
	clone, err := ffiCloneComposite(handle)
	if err != nil {
		return 0, err
	}
	var status C.RustCallStatus
	var value C.uint64_t
	if shadow {
		value = C.uniffi_prolly_bindings_fn_method_bindingcompositeaccelerator_shadow_count(C.uint64_t(clone), &status)
	} else {
		value = C.uniffi_prolly_bindings_fn_method_bindingcompositeaccelerator_delta_count(C.uint64_t(clone), &status)
	}
	return uint64(value), portableStatusError(&status)
}

func ffiCompositeSearch(index, proximity uint64, request []byte) ([]byte, error) {
	requestBuf, err := portableInput(request)
	if err != nil {
		return nil, err
	}
	indexClone, err := ffiCloneComposite(index)
	if err != nil {
		portableFreeBuffer(requestBuf)
		return nil, err
	}
	mapClone, err := ffiCloneProximity(proximity)
	if err != nil {
		portableFreeBuffer(requestBuf)
		ffiFreeComposite(indexClone)
		return nil, err
	}
	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_bindingcompositeaccelerator_search(C.uint64_t(indexClone), C.uint64_t(mapClone), requestBuf, &status)
	if err := portableStatusError(&status); err != nil {
		return nil, err
	}
	return portableTakeBuffer(buf), nil
}
func ffiCompositeProveSearch(index, proximity uint64, request, limits []byte) (uint64, error) {
	requestBuf, err := portableInput(request)
	if err != nil {
		return 0, err
	}
	limitsBuf, err := portableInput(limits)
	if err != nil {
		portableFreeBuffer(requestBuf)
		return 0, err
	}
	indexClone, err := ffiCloneComposite(index)
	if err != nil {
		portableFreeBuffer(requestBuf)
		portableFreeBuffer(limitsBuf)
		return 0, err
	}
	mapClone, err := ffiCloneProximity(proximity)
	if err != nil {
		portableFreeBuffer(requestBuf)
		portableFreeBuffer(limitsBuf)
		ffiFreeComposite(indexClone)
		return 0, err
	}
	var status C.RustCallStatus
	proof := C.uniffi_prolly_bindings_fn_method_bindingcompositeaccelerator_prove_search(C.uint64_t(indexClone), C.uint64_t(mapClone), requestBuf, limitsBuf, &status)
	return uint64(proof), portableStatusError(&status)
}

func ffiProximityBuildCatalog(proximity uint64, hnsw, pq, composite *uint64) (uint64, error) {
	cloneOptional := func(value *uint64, clone func(uint64) (uint64, error)) (*uint64, error) {
		if value == nil {
			return nil, nil
		}
		result, err := clone(*value)
		return &result, err
	}
	hnswClone, err := cloneOptional(hnsw, ffiCloneHNSWIndex)
	if err != nil {
		return 0, err
	}
	pqClone, err := cloneOptional(pq, ffiCloneProductQuantizer)
	if err != nil {
		if hnswClone != nil {
			ffiFreeHNSWIndex(*hnswClone)
		}
		return 0, err
	}
	compositeClone, err := cloneOptional(composite, ffiCloneComposite)
	if err != nil {
		if hnswClone != nil {
			ffiFreeHNSWIndex(*hnswClone)
		}
		if pqClone != nil {
			ffiFreeProductQuantizer(*pqClone)
		}
		return 0, err
	}
	freeClones := func() {
		if hnswClone != nil {
			ffiFreeHNSWIndex(*hnswClone)
		}
		if pqClone != nil {
			ffiFreeProductQuantizer(*pqClone)
		}
		if compositeClone != nil {
			ffiFreeComposite(*compositeClone)
		}
	}
	hnswBuf, err := portableInput(encodeOptionalU64Bytes(hnswClone))
	if err != nil {
		freeClones()
		return 0, err
	}
	pqBuf, err := portableInput(encodeOptionalU64Bytes(pqClone))
	if err != nil {
		portableFreeBuffer(hnswBuf)
		freeClones()
		return 0, err
	}
	compositeBuf, err := portableInput(encodeOptionalU64Bytes(compositeClone))
	if err != nil {
		portableFreeBuffer(hnswBuf)
		portableFreeBuffer(pqBuf)
		freeClones()
		return 0, err
	}
	mapClone, err := ffiCloneProximity(proximity)
	if err != nil {
		portableFreeBuffer(hnswBuf)
		portableFreeBuffer(pqBuf)
		portableFreeBuffer(compositeBuf)
		freeClones()
		return 0, err
	}
	var status C.RustCallStatus
	value := C.uniffi_prolly_bindings_fn_method_bindingproximitymap_build_accelerator_catalog(C.uint64_t(mapClone), hnswBuf, pqBuf, compositeBuf, &status)
	return uint64(value), portableStatusError(&status)
}
func ffiProximityLoadCatalog(handle uint64, manifest []byte) (uint64, error) {
	manifestBuf, err := portableInput(encodeByteArray(manifest))
	if err != nil {
		return 0, err
	}
	clone, err := ffiCloneProximity(handle)
	if err != nil {
		portableFreeBuffer(manifestBuf)
		return 0, err
	}
	var status C.RustCallStatus
	value := C.uniffi_prolly_bindings_fn_method_bindingproximitymap_load_accelerator_catalog(C.uint64_t(clone), manifestBuf, &status)
	return uint64(value), portableStatusError(&status)
}
func ffiCloneCatalog(handle uint64) (uint64, error) {
	var status C.RustCallStatus
	value := C.uniffi_prolly_bindings_fn_clone_bindingacceleratorcatalog(C.uint64_t(handle), &status)
	return uint64(value), portableStatusError(&status)
}
func ffiFreeCatalog(handle uint64) {
	var status C.RustCallStatus
	C.uniffi_prolly_bindings_fn_free_bindingacceleratorcatalog(C.uint64_t(handle), &status)
}
func ffiCatalogBufferCall(handle uint64, call func(C.uint64_t, *C.RustCallStatus) C.RustBuffer) ([]byte, error) {
	clone, err := ffiCloneCatalog(handle)
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	buf := call(C.uint64_t(clone), &status)
	if err := portableStatusError(&status); err != nil {
		return nil, err
	}
	return portableTakeBuffer(buf), nil
}
func ffiCatalogManifest(handle uint64) ([]byte, error) {
	return ffiCatalogBufferCall(handle, func(clone C.uint64_t, status *C.RustCallStatus) C.RustBuffer {
		return C.uniffi_prolly_bindings_fn_method_bindingacceleratorcatalog_manifest(clone, status)
	})
}
func ffiCatalogSource(handle uint64) ([]byte, error) {
	return ffiCatalogBufferCall(handle, func(clone C.uint64_t, status *C.RustCallStatus) C.RustBuffer {
		return C.uniffi_prolly_bindings_fn_method_bindingacceleratorcatalog_source_descriptor(clone, status)
	})
}
func ffiCatalogEntries(handle uint64) ([]byte, error) {
	return ffiCatalogBufferCall(handle, func(clone C.uint64_t, status *C.RustCallStatus) C.RustBuffer {
		return C.uniffi_prolly_bindings_fn_method_bindingacceleratorcatalog_entries(clone, status)
	})
}
func ffiCatalogSearch(index, proximity uint64, request []byte) ([]byte, error) {
	requestBuf, err := portableInput(request)
	if err != nil {
		return nil, err
	}
	indexClone, err := ffiCloneCatalog(index)
	if err != nil {
		portableFreeBuffer(requestBuf)
		return nil, err
	}
	mapClone, err := ffiCloneProximity(proximity)
	if err != nil {
		portableFreeBuffer(requestBuf)
		ffiFreeCatalog(indexClone)
		return nil, err
	}
	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_bindingacceleratorcatalog_search(C.uint64_t(indexClone), C.uint64_t(mapClone), requestBuf, &status)
	if err := portableStatusError(&status); err != nil {
		return nil, err
	}
	return portableTakeBuffer(buf), nil
}
func ffiCatalogProveSearch(index, proximity uint64, request, limits []byte) (uint64, error) {
	requestBuf, err := portableInput(request)
	if err != nil {
		return 0, err
	}
	limitsBuf, err := portableInput(limits)
	if err != nil {
		portableFreeBuffer(requestBuf)
		return 0, err
	}
	indexClone, err := ffiCloneCatalog(index)
	if err != nil {
		portableFreeBuffer(requestBuf)
		portableFreeBuffer(limitsBuf)
		return 0, err
	}
	mapClone, err := ffiCloneProximity(proximity)
	if err != nil {
		portableFreeBuffer(requestBuf)
		portableFreeBuffer(limitsBuf)
		ffiFreeCatalog(indexClone)
		return 0, err
	}
	var status C.RustCallStatus
	proof := C.uniffi_prolly_bindings_fn_method_bindingacceleratorcatalog_prove_search(C.uint64_t(indexClone), C.uint64_t(mapClone), requestBuf, limitsBuf, &status)
	return uint64(proof), portableStatusError(&status)
}

func ffiProximityReadSession(handle uint64) (uint64, error) {
	clone, err := ffiCloneProximity(handle)
	if err != nil {
		return 0, err
	}
	var status C.RustCallStatus
	session := C.uniffi_prolly_bindings_fn_method_bindingproximitymap_read_session(C.uint64_t(clone), &status)
	return uint64(session), portableStatusError(&status)
}

func ffiCloneProximityReadSession(handle uint64) (uint64, error) {
	var status C.RustCallStatus
	clone := C.uniffi_prolly_bindings_fn_clone_bindingproximityreadsession(C.uint64_t(handle), &status)
	return uint64(clone), portableStatusError(&status)
}

func ffiFreeProximityReadSession(handle uint64) {
	var status C.RustCallStatus
	C.uniffi_prolly_bindings_fn_free_bindingproximityreadsession(C.uint64_t(handle), &status)
}

func ffiProximityReadSessionFastHandle(handle uint64) (uint64, error) {
	clone, err := ffiCloneProximityReadSession(handle)
	if err != nil {
		return 0, err
	}
	var status C.RustCallStatus
	fast := C.uniffi_prolly_bindings_fn_method_bindingproximityreadsession_fast_handle(C.uint64_t(clone), &status)
	return uint64(fast), portableStatusError(&status)
}

func ffiProximityReadSessionContains(handle uint64, key []byte) (bool, error) {
	clone, err := ffiCloneProximityReadSession(handle)
	if err != nil {
		return false, err
	}
	keyBuf, err := portableInput(encodeByteArray(key))
	if err != nil {
		return false, err
	}
	var status C.RustCallStatus
	found := C.uniffi_prolly_bindings_fn_method_bindingproximityreadsession_contains_key(C.uint64_t(clone), keyBuf, &status)
	return found != 0, portableStatusError(&status)
}

func ffiProximityReadSessionGet(handle uint64, key []byte) ([]byte, error) {
	clone, err := ffiCloneProximityReadSession(handle)
	if err != nil {
		return nil, err
	}
	keyBuf, err := portableInput(encodeByteArray(key))
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_bindingproximityreadsession_get(C.uint64_t(clone), keyBuf, &status)
	if err := portableStatusError(&status); err != nil {
		return nil, err
	}
	return portableTakeBuffer(buf), nil
}

func ffiProximityReadSessionSearch(handle uint64, request []byte) ([]byte, error) {
	clone, err := ffiCloneProximityReadSession(handle)
	if err != nil {
		return nil, err
	}
	requestBuf, err := portableInput(request)
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_bindingproximityreadsession_search(C.uint64_t(clone), requestBuf, &status)
	if err := portableStatusError(&status); err != nil {
		return nil, err
	}
	return portableTakeBuffer(buf), nil
}

func ffiProximityDescriptor(handle uint64) ([]byte, error) {
	clone, err := ffiCloneProximity(handle)
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_bindingproximitymap_descriptor(C.uint64_t(clone), &status)
	if err := portableStatusError(&status); err != nil {
		return nil, err
	}
	return portableTakeBuffer(buf), nil
}

func ffiProximityConfig(handle uint64) ([]byte, error) {
	clone, err := ffiCloneProximity(handle)
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_bindingproximitymap_config(C.uint64_t(clone), &status)
	if err := portableStatusError(&status); err != nil {
		return nil, err
	}
	return portableTakeBuffer(buf), nil
}

func ffiProximityCount(handle uint64) (uint64, error) {
	clone, err := ffiCloneProximity(handle)
	if err != nil {
		return 0, err
	}
	var status C.RustCallStatus
	count := C.uniffi_prolly_bindings_fn_method_bindingproximitymap_count(C.uint64_t(clone), &status)
	return uint64(count), portableStatusError(&status)
}

func ffiProximityContains(handle uint64, key []byte) (bool, error) {
	clone, err := ffiCloneProximity(handle)
	if err != nil {
		return false, err
	}
	keyBuf, err := portableInput(encodeByteArray(key))
	if err != nil {
		return false, err
	}
	var status C.RustCallStatus
	found := C.uniffi_prolly_bindings_fn_method_bindingproximitymap_contains_key(C.uint64_t(clone), keyBuf, &status)
	return found != 0, portableStatusError(&status)
}

func ffiProximityBufferCall(handle uint64, key []byte, call func(C.uint64_t, C.RustBuffer, *C.RustCallStatus) C.RustBuffer) ([]byte, error) {
	clone, err := ffiCloneProximity(handle)
	if err != nil {
		return nil, err
	}
	keyBuf, err := portableInput(encodeByteArray(key))
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	buf := call(C.uint64_t(clone), keyBuf, &status)
	if err := portableStatusError(&status); err != nil {
		return nil, err
	}
	return portableTakeBuffer(buf), nil
}

func ffiProximityGet(handle uint64, key []byte) ([]byte, error) {
	return ffiProximityBufferCall(handle, key, func(clone C.uint64_t, key C.RustBuffer, status *C.RustCallStatus) C.RustBuffer {
		return C.uniffi_prolly_bindings_fn_method_bindingproximitymap_get(clone, key, status)
	})
}

func ffiProximityProveMembership(handle uint64, key []byte) ([]byte, error) {
	return ffiProximityBufferCall(handle, key, func(clone C.uint64_t, key C.RustBuffer, status *C.RustCallStatus) C.RustBuffer {
		return C.uniffi_prolly_bindings_fn_method_bindingproximitymap_prove_membership(clone, key, status)
	})
}

func ffiProximityMutate(handle uint64, mutations []byte) ([]byte, error) {
	clone, err := ffiCloneProximity(handle)
	if err != nil {
		return nil, err
	}
	mutationsBuf, err := portableInput(mutations)
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_bindingproximitymap_mutate(C.uint64_t(clone), mutationsBuf, &status)
	if err := portableStatusError(&status); err != nil {
		return nil, err
	}
	return portableTakeBuffer(buf), nil
}

func ffiProximityRebuild(handle uint64, mutations []byte) (uint64, error) {
	clone, err := ffiCloneProximity(handle)
	if err != nil {
		return 0, err
	}
	mutationsBuf, err := portableInput(mutations)
	if err != nil {
		return 0, err
	}
	var status C.RustCallStatus
	updated := C.uniffi_prolly_bindings_fn_method_bindingproximitymap_rebuild(C.uint64_t(clone), mutationsBuf, &status)
	return uint64(updated), portableStatusError(&status)
}

func ffiProximityProveStructure(handle uint64, limits []byte) ([]byte, error) {
	clone, err := ffiCloneProximity(handle)
	if err != nil {
		return nil, err
	}
	limitsBuf, err := portableInput(limits)
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_bindingproximitymap_prove_structure(C.uint64_t(clone), limitsBuf, &status)
	if err := portableStatusError(&status); err != nil {
		return nil, err
	}
	return portableTakeBuffer(buf), nil
}

func ffiProximitySearchRecord(handle uint64, request []byte) ([]byte, error) {
	clone, err := ffiCloneProximity(handle)
	if err != nil {
		return nil, err
	}
	requestBuf, err := portableInput(request)
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_bindingproximitymap_search(C.uint64_t(clone), requestBuf, &status)
	if err := portableStatusError(&status); err != nil {
		return nil, err
	}
	return portableTakeBuffer(buf), nil
}

func ffiExactProximitySearchRequest(query []float32, k uint64) ([]byte, error) {
	queryBuf, err := portableInput(encodeFloat32Sequence(query))
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_func_exact_proximity_search_request(queryBuf, C.uint64_t(k), &status)
	if err := portableStatusError(&status); err != nil {
		return nil, err
	}
	return portableTakeBuffer(buf), nil
}

func ffiDefaultContentGraphLimits() ([]byte, error) {
	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_func_default_content_graph_limits(&status)
	if err := portableStatusError(&status); err != nil {
		return nil, err
	}
	return portableTakeBuffer(buf), nil
}

func ffiProximityProveSearch(handle uint64, request, limits []byte) (uint64, error) {
	clone, err := ffiCloneProximity(handle)
	if err != nil {
		return 0, err
	}
	requestBuf, err := portableInput(request)
	if err != nil {
		return 0, err
	}
	limitsBuf, err := portableInput(limits)
	if err != nil {
		return 0, err
	}
	var status C.RustCallStatus
	proof := C.uniffi_prolly_bindings_fn_method_bindingproximitymap_prove_search(C.uint64_t(clone), requestBuf, limitsBuf, &status)
	return uint64(proof), portableStatusError(&status)
}

func ffiCloneProximitySearchProof(handle uint64) (uint64, error) {
	var status C.RustCallStatus
	clone := C.uniffi_prolly_bindings_fn_clone_bindingproximitysearchproof(C.uint64_t(handle), &status)
	return uint64(clone), portableStatusError(&status)
}

func ffiFreeProximitySearchProof(handle uint64) {
	var status C.RustCallStatus
	C.uniffi_prolly_bindings_fn_free_bindingproximitysearchproof(C.uint64_t(handle), &status)
}

func ffiProximitySearchProofVerify(handle uint64, expectedDescriptor, limits []byte) ([]byte, error) {
	clone, err := ffiCloneProximitySearchProof(handle)
	if err != nil {
		return nil, err
	}
	expectedBuf, err := portableInput(encodeOptionalByteArray(expectedDescriptor))
	if err != nil {
		return nil, err
	}
	limitsBuf, err := portableInput(limits)
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_bindingproximitysearchproof_verify(C.uint64_t(clone), expectedBuf, limitsBuf, &status)
	if err := portableStatusError(&status); err != nil {
		return nil, err
	}
	return portableTakeBuffer(buf), nil
}

func ffiProximityVerify(handle uint64) ([]byte, error) {
	clone, err := ffiCloneProximity(handle)
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_method_bindingproximitymap_verify(C.uint64_t(clone), &status)
	if err := portableStatusError(&status); err != nil {
		return nil, err
	}
	return portableTakeBuffer(buf), nil
}

func ffiProximityClearContentCache(handle uint64) error {
	clone, err := ffiCloneProximity(handle)
	if err != nil {
		return err
	}
	var status C.RustCallStatus
	C.uniffi_prolly_bindings_fn_method_bindingproximitymap_clear_content_cache(C.uint64_t(clone), &status)
	return portableStatusError(&status)
}

func ffiVerifyProximityMembershipProof(proof, expectedDescriptor []byte) ([]byte, error) {
	proofBuf, err := portableInput(proof)
	if err != nil {
		return nil, err
	}
	expectedBuf, err := portableInput(encodeOptionalByteArray(expectedDescriptor))
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_func_verify_proximity_membership_proof(proofBuf, expectedBuf, &status)
	if err := portableStatusError(&status); err != nil {
		return nil, err
	}
	return portableTakeBuffer(buf), nil
}

func ffiVerifyProximityStructureProof(proof, expectedDescriptor, limits []byte) ([]byte, error) {
	proofBuf, err := portableInput(proof)
	if err != nil {
		return nil, err
	}
	expectedBuf, err := portableInput(encodeOptionalByteArray(expectedDescriptor))
	if err != nil {
		return nil, err
	}
	limitsBuf, err := portableInput(limits)
	if err != nil {
		return nil, err
	}
	var status C.RustCallStatus
	buf := C.uniffi_prolly_bindings_fn_func_verify_proximity_structure_proof(proofBuf, expectedBuf, limitsBuf, &status)
	if err := portableStatusError(&status); err != nil {
		return nil, err
	}
	return portableTakeBuffer(buf), nil
}

func ffiCloneProximity(handle uint64) (uint64, error) {
	var status C.RustCallStatus
	clone := C.uniffi_prolly_bindings_fn_clone_bindingproximitymap(C.uint64_t(handle), &status)
	return uint64(clone), portableStatusError(&status)
}

func ffiFreeProximity(handle uint64) {
	var status C.RustCallStatus
	C.uniffi_prolly_bindings_fn_free_bindingproximitymap(C.uint64_t(handle), &status)
}

type nativePageLease struct {
	handle   uint64
	data     []byte
	terminal bool
	closed   atomic.Bool
}

func nativePageFromResult(result C.ProllyFastPageResult) (*nativePageLease, error) {
	if result.status != 0 {
		return nil, fmt.Errorf("native packed page failed with status %d", int(result.status))
	}
	if result.data_len > portableMaxArenaBytes+256*1024*1024 {
		C.prolly_fast_page_release(result.lease_handle)
		return nil, errors.New("native packed page exceeds safety limit")
	}
	var data []byte
	if result.data_ptr != nil && result.data_len != 0 {
		data = unsafe.Slice((*byte)(unsafe.Pointer(result.data_ptr)), int(result.data_len))
	}
	return &nativePageLease{handle: uint64(result.lease_handle), data: data, terminal: result.terminal != 0}, nil
}

func (p *nativePageLease) Close() {
	if p == nil || p.closed.Swap(true) {
		return
	}
	C.prolly_fast_page_release(C.uint64_t(p.handle))
	p.data = nil
}

type nativeIndexCursor struct {
	snapshot, handle uint64
	closed           atomic.Bool
}

func ffiOpenIndexCursor(snapshot uint64, query IndexQuery) (*nativeIndexCursor, error) {
	start := query.Start
	var startPtr *C.uint8_t
	if len(start) != 0 {
		startPtr = (*C.uint8_t)(unsafe.Pointer(&start[0]))
	}
	var endPtr *C.uint8_t
	if len(query.End) != 0 {
		endPtr = (*C.uint8_t)(unsafe.Pointer(&query.End[0]))
	}
	hasEnd := C.uint8_t(0)
	if query.End != nil {
		hasEnd = 1
	}
	result := C.prolly_fast_index_cursor_open(C.uint64_t(snapshot), C.uint32_t(query.Kind), startPtr, C.size_t(len(start)), endPtr, C.size_t(len(query.End)), hasEnd, C.uint8_t(boolByte(query.Reverse)))
	runtime.KeepAlive(start)
	runtime.KeepAlive(query.End)
	if result.status != 0 {
		return nil, fmt.Errorf("native index cursor open failed with status %d", int(result.status))
	}
	return &nativeIndexCursor{snapshot: snapshot, handle: uint64(result.scan_handle)}, nil
}

func (c *nativeIndexCursor) Next(limit uint32) (*nativePageLease, error) {
	if c == nil || c.closed.Load() {
		return nil, errors.New("native index cursor is closed")
	}
	return nativePageFromResult(C.prolly_fast_index_cursor_next(C.uint64_t(c.snapshot), C.uint64_t(c.handle), C.uint32_t(limit), portableMaxArenaBytes))
}

func (c *nativeIndexCursor) Close() {
	if c == nil || c.closed.Swap(true) {
		return
	}
	C.prolly_fast_index_cursor_close(C.uint64_t(c.handle))
}

func ffiProximitySearch(fast uint64, query []float32, k uint32) (*nativePageLease, error) {
	var ptr *C.float
	if len(query) != 0 {
		ptr = (*C.float)(unsafe.Pointer(&query[0]))
	}
	result := C.prolly_fast_proximity_search(C.uint64_t(fast), ptr, C.size_t(len(query)), C.uint32_t(k), portableMaxArenaBytes)
	runtime.KeepAlive(query)
	return nativePageFromResult(result)
}

func boolByte(value bool) byte {
	if value {
		return 1
	}
	return 0
}

func encodeEnum(value int32) []byte {
	return []byte{byte(uint32(value) >> 24), byte(uint32(value) >> 16), byte(uint32(value) >> 8), byte(value)}
}
