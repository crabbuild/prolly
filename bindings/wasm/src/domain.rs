use super::{
    borrowed_entry_view_object, call_scan_visitor, conflict_to_object, diffs_to_array,
    entries_to_array, entry_object, js_error, multi_key_proof_verification_to_object,
    optional_bytes, parallel_config_from_js, range_cursor_value,
    range_page_proof_verification_to_object, range_page_to_object,
    range_proof_verification_to_object, resolver_from_name, reverse_page_to_object,
    scan_callback_flow, scan_outcome_to_object, WasmProllyEngine, WasmRangeCursor,
    WasmReverseCursor, WasmSnapshotBundle,
};
use crate::page::set_bytes;
use js_sys::{Array, Function, Object, Reflect, Uint8Array};
use prolly::{
    KeyProof, LargeValueConfig, MapVersionId, MemBlobStore, MultiKeyProof, OwnedReadSession,
    ProvedRangePage, RangeProof, SnapshotBundle, VersionedMapBackup, VersionedMapUpdate,
};
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::sync::Arc;
use wasm_bindgen::prelude::*;

#[wasm_bindgen(js_name = WasmBlobStore)]
pub struct WasmBlobStore {
    inner: Arc<MemBlobStore>,
}

#[wasm_bindgen(js_class = WasmBlobStore)]
impl WasmBlobStore {
    #[wasm_bindgen(constructor)]
    pub fn new() -> Self {
        Self {
            inner: Arc::new(MemBlobStore::new()),
        }
    }

    #[wasm_bindgen(js_name = cloneHandle)]
    pub fn clone_handle(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

#[wasm_bindgen(js_name = WasmVersionedMap)]
pub struct WasmVersionedMap {
    engine: Arc<super::WasmEngine>,
    id: Vec<u8>,
}

#[wasm_bindgen(js_class = WasmVersionedMap)]
impl WasmVersionedMap {
    pub fn id(&self) -> Vec<u8> {
        self.id.clone()
    }

    #[wasm_bindgen(js_name = headName)]
    pub fn head_name(&self) -> Vec<u8> {
        self.engine.versioned_map(&self.id).head_name().to_vec()
    }

    #[wasm_bindgen(js_name = versionsPrefix)]
    pub fn versions_prefix(&self) -> Vec<u8> {
        self.engine
            .versioned_map(&self.id)
            .versions_prefix()
            .to_vec()
    }

    #[wasm_bindgen(js_name = isInitialized)]
    pub fn is_initialized(&self) -> Result<bool, JsValue> {
        self.engine
            .versioned_map(&self.id)
            .is_initialized()
            .map_err(js_error)
    }

    pub fn initialize(&self) -> Result<Object, JsValue> {
        self.engine
            .versioned_map(&self.id)
            .initialize()
            .map_err(js_error)
            .and_then(map_version_object)
    }

    #[wasm_bindgen(js_name = initializeSorted)]
    pub fn initialize_sorted(&self, entries: Array) -> Result<Object, JsValue> {
        self.engine
            .versioned_map(&self.id)
            .initialize_sorted(super::entries_array(entries)?)
            .map_err(js_error)
            .and_then(map_update_object)
    }

    pub fn head(&self) -> Result<Option<Object>, JsValue> {
        self.engine
            .versioned_map(&self.id)
            .head()
            .map_err(js_error)?
            .map(map_version_object)
            .transpose()
    }

    #[wasm_bindgen(js_name = headId)]
    pub fn head_id(&self) -> Result<JsValue, JsValue> {
        self.engine
            .versioned_map(&self.id)
            .head_id()
            .map(|value| optional_bytes(value.map(|id| id.into_cid().0.to_vec())))
            .map_err(js_error)
    }

    pub fn version(&self, id: Uint8Array) -> Result<Option<Object>, JsValue> {
        let id = MapVersionId::from_bytes(&id.to_vec()).map_err(js_error)?;
        self.engine
            .versioned_map(&self.id)
            .version(&id)
            .map_err(js_error)?
            .map(map_version_object)
            .transpose()
    }

    pub fn versions(&self) -> Result<Vec<Object>, JsValue> {
        self.engine
            .versioned_map(&self.id)
            .versions()
            .map_err(js_error)?
            .into_iter()
            .map(map_version_object)
            .collect()
    }

    pub fn get(&self, key: Uint8Array) -> Result<JsValue, JsValue> {
        self.engine
            .versioned_map(&self.id)
            .get(&key.to_vec())
            .map(optional_bytes)
            .map_err(js_error)
    }

    #[wasm_bindgen(js_name = getLargeValue)]
    pub fn get_large_value(
        &self,
        blob_store: &WasmBlobStore,
        key: Uint8Array,
    ) -> Result<JsValue, JsValue> {
        self.engine
            .versioned_map(&self.id)
            .get_large_value(blob_store.inner.as_ref(), &key.to_vec())
            .map(optional_bytes)
            .map_err(js_error)
    }

    #[wasm_bindgen(js_name = containsKey)]
    pub fn contains_key(&self, key: Uint8Array) -> Result<bool, JsValue> {
        self.engine
            .versioned_map(&self.id)
            .get(&key.to_vec())
            .map(|value| value.is_some())
            .map_err(js_error)
    }

    #[wasm_bindgen(js_name = getMany)]
    pub fn get_many(&self, keys: Array) -> Result<Array, JsValue> {
        let keys = keys
            .iter()
            .map(|value| Uint8Array::new(&value).to_vec())
            .collect::<Vec<_>>();
        let values = self
            .engine
            .versioned_map(&self.id)
            .get_many(&keys)
            .map_err(js_error)?;
        let result = Array::new();
        for value in values {
            result.push(&optional_bytes(value));
        }
        Ok(result)
    }

    #[wasm_bindgen(js_name = getAt)]
    pub fn get_at(&self, id: Uint8Array, key: Uint8Array) -> Result<JsValue, JsValue> {
        let id = MapVersionId::from_bytes(&id.to_vec()).map_err(js_error)?;
        self.engine
            .versioned_map(&self.id)
            .get_at(&id, &key.to_vec())
            .map(optional_bytes)
            .map_err(js_error)
    }

    #[wasm_bindgen(js_name = getManyAt)]
    pub fn get_many_at(&self, id: Uint8Array, keys: Array) -> Result<Array, JsValue> {
        let id = MapVersionId::from_bytes(&id.to_vec()).map_err(js_error)?;
        let keys = keys
            .iter()
            .map(|value| Uint8Array::new(&value).to_vec())
            .collect::<Vec<_>>();
        let values = self
            .engine
            .versioned_map(&self.id)
            .get_many_at(&id, &keys)
            .map_err(js_error)?;
        let result = Array::new();
        for value in values {
            result.push(&optional_bytes(value));
        }
        Ok(result)
    }

    pub fn range(&self, start: Uint8Array, end: Option<Uint8Array>) -> Result<Array, JsValue> {
        let entries = self
            .engine
            .versioned_map(&self.id)
            .range(
                &start.to_vec(),
                end.as_ref().map(Uint8Array::to_vec).as_deref(),
            )
            .map_err(js_error)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(js_error)?;
        entries_to_array(entries)
    }

    pub fn prefix(&self, prefix: Uint8Array) -> Result<Array, JsValue> {
        let entries = self
            .engine
            .versioned_map(&self.id)
            .prefix(&prefix.to_vec())
            .map_err(js_error)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(js_error)?;
        entries_to_array(entries)
    }

    #[wasm_bindgen(js_name = rangeAt)]
    pub fn range_at(
        &self,
        id: Uint8Array,
        start: Uint8Array,
        end: Option<Uint8Array>,
    ) -> Result<Array, JsValue> {
        let id = MapVersionId::from_bytes(&id.to_vec()).map_err(js_error)?;
        let entries = self
            .engine
            .versioned_map(&self.id)
            .range_at(
                &id,
                &start.to_vec(),
                end.as_ref().map(Uint8Array::to_vec).as_deref(),
            )
            .map_err(js_error)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(js_error)?;
        entries_to_array(entries)
    }

    #[wasm_bindgen(js_name = prefixAt)]
    pub fn prefix_at(&self, id: Uint8Array, prefix: Uint8Array) -> Result<Array, JsValue> {
        let id = MapVersionId::from_bytes(&id.to_vec()).map_err(js_error)?;
        let entries = self
            .engine
            .versioned_map(&self.id)
            .prefix_at(&id, &prefix.to_vec())
            .map_err(js_error)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(js_error)?;
        entries_to_array(entries)
    }

    #[wasm_bindgen(js_name = rangePage)]
    pub fn range_page(
        &self,
        cursor: Option<WasmRangeCursor>,
        end: Option<Uint8Array>,
        limit: u32,
    ) -> Result<Object, JsValue> {
        self.engine
            .versioned_map(&self.id)
            .range_page(
                &cursor
                    .map(|value| value.inner)
                    .unwrap_or_else(prolly::RangeCursor::start),
                end.as_ref().map(Uint8Array::to_vec).as_deref(),
                limit as usize,
            )
            .map_err(js_error)
            .and_then(range_page_to_object)
    }

    #[wasm_bindgen(js_name = prefixPage)]
    pub fn prefix_page(
        &self,
        prefix: Uint8Array,
        cursor: Option<WasmRangeCursor>,
        limit: u32,
    ) -> Result<Object, JsValue> {
        self.engine
            .versioned_map(&self.id)
            .prefix_page(
                &prefix.to_vec(),
                &cursor
                    .map(|value| value.inner)
                    .unwrap_or_else(prolly::RangeCursor::start),
                limit as usize,
            )
            .map_err(js_error)
            .and_then(range_page_to_object)
    }

    #[wasm_bindgen(js_name = rangePageAt)]
    pub fn range_page_at(
        &self,
        id: Uint8Array,
        cursor: Option<WasmRangeCursor>,
        end: Option<Uint8Array>,
        limit: u32,
    ) -> Result<Object, JsValue> {
        let id = MapVersionId::from_bytes(&id.to_vec()).map_err(js_error)?;
        self.engine
            .versioned_map(&self.id)
            .range_page_at(
                &id,
                &cursor
                    .map(|value| value.inner)
                    .unwrap_or_else(prolly::RangeCursor::start),
                end.as_ref().map(Uint8Array::to_vec).as_deref(),
                limit as usize,
            )
            .map_err(js_error)
            .and_then(range_page_to_object)
    }

    #[wasm_bindgen(js_name = prefixPageAt)]
    pub fn prefix_page_at(
        &self,
        id: Uint8Array,
        prefix: Uint8Array,
        cursor: Option<WasmRangeCursor>,
        limit: u32,
    ) -> Result<Object, JsValue> {
        let id = MapVersionId::from_bytes(&id.to_vec()).map_err(js_error)?;
        self.engine
            .versioned_map(&self.id)
            .prefix_page_at(
                &id,
                &prefix.to_vec(),
                &cursor
                    .map(|value| value.inner)
                    .unwrap_or_else(prolly::RangeCursor::start),
                limit as usize,
            )
            .map_err(js_error)
            .and_then(range_page_to_object)
    }

    pub fn diff(&self, base: Uint8Array, target: Uint8Array) -> Result<Array, JsValue> {
        let base = MapVersionId::from_bytes(&base.to_vec()).map_err(js_error)?;
        let target = MapVersionId::from_bytes(&target.to_vec()).map_err(js_error)?;
        self.engine
            .versioned_map(&self.id)
            .diff(&base, &target)
            .map_err(js_error)
            .and_then(diffs_to_array)
    }

    #[wasm_bindgen(js_name = changesSince)]
    pub fn changes_since(&self, base: Uint8Array) -> Result<Array, JsValue> {
        let base = MapVersionId::from_bytes(&base.to_vec()).map_err(js_error)?;
        self.engine
            .versioned_map(&self.id)
            .changes_since(&base)
            .map_err(js_error)
            .and_then(diffs_to_array)
    }

    #[wasm_bindgen(js_name = rollbackTo)]
    pub fn rollback_to(&self, id: Uint8Array) -> Result<Object, JsValue> {
        let id = MapVersionId::from_bytes(&id.to_vec()).map_err(js_error)?;
        self.engine
            .versioned_map(&self.id)
            .rollback_to(&id)
            .map_err(js_error)
            .and_then(map_version_object)
    }

    pub fn put(&self, key: Uint8Array, value: Uint8Array) -> Result<Object, JsValue> {
        self.engine
            .versioned_map(&self.id)
            .put(key.to_vec(), value.to_vec())
            .map_err(js_error)
            .and_then(map_version_object)
    }

    #[wasm_bindgen(js_name = putLargeValue)]
    pub fn put_large_value(
        &self,
        blob_store: &WasmBlobStore,
        key: Uint8Array,
        value: Uint8Array,
        inline_threshold: u64,
    ) -> Result<Object, JsValue> {
        let inline_threshold = usize::try_from(inline_threshold)
            .map_err(|_| JsValue::from_str("inline threshold does not fit this platform"))?;
        self.engine
            .versioned_map(&self.id)
            .put_large_value(
                blob_store.inner.as_ref(),
                key.to_vec(),
                value.to_vec(),
                LargeValueConfig::new(inline_threshold),
            )
            .map_err(js_error)
            .and_then(map_version_object)
    }

    pub fn apply(&self, mutations: Array) -> Result<Object, JsValue> {
        self.engine
            .versioned_map(&self.id)
            .apply(crate::indexed::mutations_from_array(&mutations)?)
            .map_err(js_error)
            .and_then(map_version_object)
    }

    pub fn append(&self, mutations: Array) -> Result<Object, JsValue> {
        self.engine
            .versioned_map(&self.id)
            .append(crate::indexed::mutations_from_array(&mutations)?)
            .map_err(js_error)
            .and_then(map_version_object)
    }

    #[wasm_bindgen(js_name = parallelApply)]
    pub fn parallel_apply(&self, mutations: Array, config: JsValue) -> Result<Object, JsValue> {
        let result = self
            .engine
            .versioned_map(&self.id)
            .parallel_apply(
                crate::indexed::mutations_from_array(&mutations)?,
                &parallel_config_from_js(&config)?,
            )
            .map_err(js_error)?;
        let object = Object::new();
        Reflect::set(
            &object,
            &"version".into(),
            &map_version_object(result.version)?.into(),
        )?;
        Reflect::set(
            &object,
            &"stats".into(),
            &versioned_batch_stats_object(result.stats)?.into(),
        )?;
        Ok(object)
    }

    #[wasm_bindgen(js_name = rebuildSortedIf)]
    pub fn rebuild_sorted_if(
        &self,
        expected: Option<Uint8Array>,
        entries: Array,
    ) -> Result<Object, JsValue> {
        let expected = expected
            .map(|value| MapVersionId::from_bytes(&value.to_vec()))
            .transpose()
            .map_err(js_error)?;
        self.engine
            .versioned_map(&self.id)
            .rebuild_sorted_if(expected.as_ref(), super::entries_array(entries)?)
            .map_err(js_error)
            .and_then(map_update_object)
    }

    #[wasm_bindgen(js_name = rebuildFromEntriesIf)]
    pub fn rebuild_from_entries_if(
        &self,
        expected: Option<Uint8Array>,
        entries: Array,
    ) -> Result<Object, JsValue> {
        let expected = expected
            .map(|value| MapVersionId::from_bytes(&value.to_vec()))
            .transpose()
            .map_err(js_error)?;
        self.engine
            .versioned_map(&self.id)
            .rebuild_from_iter_if(expected.as_ref(), super::entries_array(entries)?)
            .map_err(js_error)
            .and_then(map_update_object)
    }

    #[wasm_bindgen(js_name = applyAtMillis)]
    pub fn apply_at_millis(
        &self,
        mutations: Array,
        timestamp_millis: u64,
    ) -> Result<Object, JsValue> {
        self.engine
            .versioned_map(&self.id)
            .apply_at_millis(
                crate::indexed::mutations_from_array(&mutations)?,
                timestamp_millis,
            )
            .map_err(js_error)
            .and_then(map_version_object)
    }

    #[wasm_bindgen(js_name = applyIf)]
    pub fn apply_if(
        &self,
        expected: Option<Uint8Array>,
        mutations: Array,
    ) -> Result<Object, JsValue> {
        let expected = expected
            .map(|value| MapVersionId::from_bytes(&value.to_vec()))
            .transpose()
            .map_err(js_error)?;
        self.engine
            .versioned_map(&self.id)
            .apply_if(
                expected.as_ref(),
                crate::indexed::mutations_from_array(&mutations)?,
            )
            .map_err(js_error)
            .and_then(map_update_object)
    }

    #[wasm_bindgen(js_name = applyIfAtMillis)]
    pub fn apply_if_at_millis(
        &self,
        expected: Option<Uint8Array>,
        mutations: Array,
        timestamp_millis: u64,
    ) -> Result<Object, JsValue> {
        let expected = expected
            .map(|value| MapVersionId::from_bytes(&value.to_vec()))
            .transpose()
            .map_err(js_error)?;
        self.engine
            .versioned_map(&self.id)
            .apply_if_at_millis(
                expected.as_ref(),
                crate::indexed::mutations_from_array(&mutations)?,
                timestamp_millis,
            )
            .map_err(js_error)
            .and_then(map_update_object)
    }

    #[wasm_bindgen(js_name = putIf)]
    pub fn put_if(
        &self,
        expected: Option<Uint8Array>,
        key: Uint8Array,
        value: Uint8Array,
    ) -> Result<Object, JsValue> {
        let expected = expected
            .map(|value| MapVersionId::from_bytes(&value.to_vec()))
            .transpose()
            .map_err(js_error)?;
        self.engine
            .versioned_map(&self.id)
            .put_if(expected.as_ref(), key.to_vec(), value.to_vec())
            .map_err(js_error)
            .and_then(map_update_object)
    }

    #[wasm_bindgen(js_name = putLargeValueIf)]
    pub fn put_large_value_if(
        &self,
        blob_store: &WasmBlobStore,
        expected: Option<Uint8Array>,
        key: Uint8Array,
        value: Uint8Array,
        inline_threshold: u64,
    ) -> Result<Object, JsValue> {
        let expected = expected
            .map(|value| MapVersionId::from_bytes(&value.to_vec()))
            .transpose()
            .map_err(js_error)?;
        let inline_threshold = usize::try_from(inline_threshold)
            .map_err(|_| JsValue::from_str("inline threshold does not fit this platform"))?;
        self.engine
            .versioned_map(&self.id)
            .put_large_value_if(
                blob_store.inner.as_ref(),
                expected.as_ref(),
                key.to_vec(),
                value.to_vec(),
                LargeValueConfig::new(inline_threshold),
            )
            .map_err(js_error)
            .and_then(map_update_object)
    }

    #[wasm_bindgen(js_name = deleteIf)]
    pub fn delete_if(
        &self,
        expected: Option<Uint8Array>,
        key: Uint8Array,
    ) -> Result<Object, JsValue> {
        let expected = expected
            .map(|value| MapVersionId::from_bytes(&value.to_vec()))
            .transpose()
            .map_err(js_error)?;
        self.engine
            .versioned_map(&self.id)
            .delete_if(expected.as_ref(), key.to_vec())
            .map_err(js_error)
            .and_then(map_update_object)
    }

    pub fn delete(&self, key: Uint8Array) -> Result<Object, JsValue> {
        self.engine
            .versioned_map(&self.id)
            .delete(key.to_vec())
            .map_err(js_error)
            .and_then(map_version_object)
    }

    pub fn snapshot(&self) -> Result<Option<WasmMapSnapshot>, JsValue> {
        let map = self.engine.versioned_map(&self.id);
        Ok(map
            .snapshot()
            .map_err(js_error)?
            .map(|snapshot| WasmMapSnapshot {
                engine: Arc::clone(&self.engine),
                id: self.id.clone(),
                version: snapshot.id().clone(),
            }))
    }

    #[wasm_bindgen(js_name = snapshotAt)]
    pub fn snapshot_at(&self, id: Uint8Array) -> Result<Option<WasmMapSnapshot>, JsValue> {
        let version = MapVersionId::from_bytes(&id.to_vec()).map_err(js_error)?;
        Ok(self
            .engine
            .versioned_map(&self.id)
            .snapshot_at(&version)
            .map_err(js_error)?
            .map(|_| WasmMapSnapshot {
                engine: Arc::clone(&self.engine),
                id: self.id.clone(),
                version,
            }))
    }

    pub fn compare(
        &self,
        base: Uint8Array,
        target: Uint8Array,
    ) -> Result<WasmMapComparison, JsValue> {
        let base = MapVersionId::from_bytes(&base.to_vec()).map_err(js_error)?;
        let target = MapVersionId::from_bytes(&target.to_vec()).map_err(js_error)?;
        let map = self.engine.versioned_map(&self.id);
        let base = map
            .version(&base)
            .map_err(js_error)?
            .ok_or_else(|| JsValue::from_str("base map version is not cataloged"))?;
        let target = map
            .version(&target)
            .map_err(js_error)?
            .ok_or_else(|| JsValue::from_str("target map version is not cataloged"))?;
        Ok(WasmMapComparison {
            engine: Arc::clone(&self.engine),
            base,
            target,
        })
    }

    #[wasm_bindgen(js_name = compareToHead)]
    pub fn compare_to_head(&self, base: Uint8Array) -> Result<WasmMapComparison, JsValue> {
        let target = self
            .engine
            .versioned_map(&self.id)
            .head()
            .map_err(js_error)?
            .ok_or_else(|| JsValue::from_str("versioned map has not been initialized"))?;
        let base = MapVersionId::from_bytes(&base.to_vec()).map_err(js_error)?;
        let base = self
            .engine
            .versioned_map(&self.id)
            .version(&base)
            .map_err(js_error)?
            .ok_or_else(|| JsValue::from_str("base map version is not cataloged"))?;
        Ok(WasmMapComparison {
            engine: Arc::clone(&self.engine),
            base,
            target,
        })
    }

    pub fn subscribe(&self) -> Result<WasmMapSubscription, JsValue> {
        let last_seen = self
            .engine
            .versioned_map(&self.id)
            .head_id()
            .map_err(js_error)?;
        Ok(WasmMapSubscription {
            engine: Arc::clone(&self.engine),
            id: self.id.clone(),
            last_seen: RefCell::new(last_seen),
        })
    }

    #[wasm_bindgen(js_name = subscribeFrom)]
    pub fn subscribe_from(
        &self,
        last_seen: Option<Uint8Array>,
    ) -> Result<WasmMapSubscription, JsValue> {
        let last_seen = last_seen
            .map(|value| MapVersionId::from_bytes(&value.to_vec()))
            .transpose()
            .map_err(js_error)?;
        Ok(WasmMapSubscription {
            engine: Arc::clone(&self.engine),
            id: self.id.clone(),
            last_seen: RefCell::new(last_seen),
        })
    }

    #[wasm_bindgen(js_name = prepareMerge)]
    pub fn prepare_merge(
        &self,
        base: Uint8Array,
        candidate: Uint8Array,
    ) -> Result<WasmMapMerge, JsValue> {
        let base_id = MapVersionId::from_bytes(&base.to_vec()).map_err(js_error)?;
        let candidate_id = MapVersionId::from_bytes(&candidate.to_vec()).map_err(js_error)?;
        let map = self.engine.versioned_map(&self.id);
        let merge = map
            .prepare_merge(&base_id, &candidate_id)
            .map_err(js_error)?;
        Ok(WasmMapMerge {
            engine: Arc::clone(&self.engine),
            id: self.id.clone(),
            base: merge.base().clone(),
            head: merge.head().clone(),
            candidate: merge.candidate().clone(),
        })
    }

    pub fn backup(&self) -> Result<Vec<u8>, JsValue> {
        self.engine
            .versioned_map(&self.id)
            .backup()
            .and_then(|value| value.to_bytes())
            .map_err(js_error)
    }

    #[wasm_bindgen(js_name = restoreBackup)]
    pub fn restore_backup(&self, bytes: Uint8Array) -> Result<Object, JsValue> {
        let backup = VersionedMapBackup::from_bytes(&bytes.to_vec()).map_err(js_error)?;
        self.engine
            .versioned_map(&self.id)
            .restore_backup(&backup)
            .map_err(js_error)
            .and_then(map_version_object)
    }

    #[wasm_bindgen(js_name = importAsHead)]
    pub fn import_as_head(&self, bytes: Uint8Array) -> Result<Object, JsValue> {
        let bundle = SnapshotBundle::from_bytes(&bytes.to_vec()).map_err(js_error)?;
        self.engine
            .versioned_map(&self.id)
            .import_as_head(&bundle)
            .map_err(js_error)
            .and_then(map_version_object)
    }

    #[wasm_bindgen(js_name = importAsHeadAtMillis)]
    pub fn import_as_head_at_millis(
        &self,
        bytes: Uint8Array,
        timestamp_millis: u64,
    ) -> Result<Object, JsValue> {
        let bundle = SnapshotBundle::from_bytes(&bytes.to_vec()).map_err(js_error)?;
        self.engine
            .versioned_map(&self.id)
            .import_as_head_at_millis(&bundle, timestamp_millis)
            .map_err(js_error)
            .and_then(map_version_object)
    }

    #[wasm_bindgen(js_name = keepLast)]
    pub fn keep_last(&self, count: u32) -> Result<Object, JsValue> {
        let result = self
            .engine
            .versioned_map(&self.id)
            .keep_last(count as usize)
            .map_err(js_error)?;
        version_prune_object(result)
    }

    #[wasm_bindgen(js_name = pruneVersions)]
    pub fn prune_versions(&self, keep_latest: u64) -> Result<Object, JsValue> {
        let keep_latest = usize::try_from(keep_latest)
            .map_err(|_| JsValue::from_str("version count does not fit this platform"))?;
        self.engine
            .versioned_map(&self.id)
            .prune_versions(keep_latest)
            .map_err(js_error)
            .and_then(version_prune_object)
    }

    #[wasm_bindgen(js_name = keepForAt)]
    pub fn keep_for_at(&self, now_millis: u64, max_age_millis: u64) -> Result<Object, JsValue> {
        self.engine
            .versioned_map(&self.id)
            .keep_for_at(now_millis, std::time::Duration::from_millis(max_age_millis))
            .map_err(js_error)
            .and_then(version_prune_object)
    }

    #[wasm_bindgen(js_name = keepFor)]
    pub fn keep_for(&self, max_age_millis: u64) -> Result<Object, JsValue> {
        self.engine
            .versioned_map(&self.id)
            .keep_for(std::time::Duration::from_millis(max_age_millis))
            .map_err(js_error)
            .and_then(version_prune_object)
    }

    #[wasm_bindgen(js_name = keepVersions)]
    pub fn keep_versions(&self, ids: Array) -> Result<Object, JsValue> {
        let ids = ids
            .iter()
            .map(|value| MapVersionId::from_bytes(&Uint8Array::new(&value).to_vec()))
            .collect::<Result<Vec<_>, _>>()
            .map_err(js_error)?;
        self.engine
            .versioned_map(&self.id)
            .keep_versions(&ids)
            .map_err(js_error)
            .and_then(version_prune_object)
    }

    #[wasm_bindgen(js_name = retentionPolicy)]
    pub fn retention_policy(&self) -> Result<Object, JsValue> {
        named_root_retention_object(self.engine.versioned_map(&self.id).retention_policy())
    }

    #[wasm_bindgen(js_name = verifyCatalog)]
    pub fn verify_catalog(&self) -> Result<Object, JsValue> {
        let value = self
            .engine
            .versioned_map(&self.id)
            .verify_catalog()
            .map_err(js_error)?;
        catalog_verification_object(value)
    }

    #[wasm_bindgen(js_name = planGc)]
    pub fn plan_gc(&self) -> Result<Object, JsValue> {
        let value = self
            .engine
            .versioned_map(&self.id)
            .plan_gc()
            .map_err(js_error)?;
        gc_plan_object(value)
    }

    #[wasm_bindgen(js_name = sweepGc)]
    pub fn sweep_gc(&self) -> Result<Object, JsValue> {
        self.engine
            .versioned_map(&self.id)
            .sweep_gc()
            .map_err(js_error)
            .and_then(gc_sweep_object)
    }

    #[wasm_bindgen(js_name = planBlobGc)]
    pub fn plan_blob_gc(&self, blob_store: &WasmBlobStore) -> Result<Object, JsValue> {
        self.engine
            .versioned_map(&self.id)
            .plan_blob_gc(blob_store.inner.as_ref())
            .map_err(js_error)
            .and_then(blob_gc_plan_object)
    }

    #[wasm_bindgen(js_name = sweepBlobGc)]
    pub fn sweep_blob_gc(&self, blob_store: &WasmBlobStore) -> Result<Object, JsValue> {
        self.engine
            .versioned_map(&self.id)
            .sweep_blob_gc(blob_store.inner.as_ref())
            .map_err(js_error)
            .and_then(blob_gc_sweep_object)
    }
}

#[wasm_bindgen(js_name = WasmMapComparison)]
pub struct WasmMapComparison {
    engine: Arc<super::WasmEngine>,
    base: prolly::MapVersion,
    target: prolly::MapVersion,
}

#[wasm_bindgen(js_class = WasmMapComparison)]
impl WasmMapComparison {
    pub fn base(&self) -> Result<Object, JsValue> {
        map_version_object(self.base.clone())
    }
    pub fn target(&self) -> Result<Object, JsValue> {
        map_version_object(self.target.clone())
    }
    pub fn diff(&self) -> Result<Array, JsValue> {
        self.engine
            .diff(&self.base.tree, &self.target.tree)
            .map_err(js_error)
            .and_then(diffs_to_array)
    }
    #[wasm_bindgen(js_name = diffPage)]
    pub fn diff_page(
        &self,
        cursor: Option<WasmRangeCursor>,
        end: Option<Uint8Array>,
        limit: u32,
    ) -> Result<Object, JsValue> {
        let cursor = cursor
            .map(|value| value.inner)
            .unwrap_or_else(prolly::RangeCursor::start);
        let end = end.map(|value| value.to_vec());
        let page = self
            .engine
            .diff_page(
                &self.base.tree,
                &self.target.tree,
                &cursor,
                end.as_deref(),
                limit as usize,
            )
            .map_err(js_error)?;
        let object = Object::new();
        Reflect::set(
            &object,
            &"diffs".into(),
            &diffs_to_array(page.diffs)?.into(),
        )?;
        Reflect::set(
            &object,
            &"nextCursor".into(),
            &range_cursor_value(page.next_cursor),
        )?;
        Ok(object)
    }
}

#[wasm_bindgen(js_name = WasmMapSubscription)]
pub struct WasmMapSubscription {
    engine: Arc<super::WasmEngine>,
    id: Vec<u8>,
    last_seen: RefCell<Option<MapVersionId>>,
}

#[wasm_bindgen(js_class = WasmMapSubscription)]
impl WasmMapSubscription {
    #[wasm_bindgen(js_name = lastSeen)]
    pub fn last_seen(&self) -> JsValue {
        optional_bytes(
            self.last_seen
                .borrow()
                .as_ref()
                .map(|value| value.as_cid().as_bytes().to_vec()),
        )
    }

    pub fn poll(&self) -> Result<JsValue, JsValue> {
        let map = self.engine.versioned_map(&self.id);
        let Some(current) = map.head().map_err(js_error)? else {
            return Ok(JsValue::NULL);
        };
        let mut last_seen = self.last_seen.borrow_mut();
        if last_seen.as_ref() == Some(&current.id) {
            return Ok(JsValue::NULL);
        }
        let previous_tree = match last_seen.as_ref() {
            Some(id) => {
                map.version(id)
                    .map_err(js_error)?
                    .ok_or_else(|| JsValue::from_str("subscription resume version was pruned"))?
                    .tree
            }
            None => self.engine.create(),
        };
        let diffs = self
            .engine
            .diff(&previous_tree, &current.tree)
            .map_err(js_error)?;
        let previous = last_seen.replace(current.id.clone());
        let object = Object::new();
        Reflect::set(
            &object,
            &"previous".into(),
            &optional_bytes(previous.map(|value| value.as_cid().as_bytes().to_vec())),
        )?;
        Reflect::set(
            &object,
            &"current".into(),
            &map_version_object(current)?.into(),
        )?;
        Reflect::set(&object, &"diffs".into(), &diffs_to_array(diffs)?.into())?;
        Ok(object.into())
    }
}

#[wasm_bindgen(js_name = WasmMapMerge)]
pub struct WasmMapMerge {
    engine: Arc<super::WasmEngine>,
    id: Vec<u8>,
    base: prolly::MapVersion,
    head: prolly::MapVersion,
    candidate: prolly::MapVersion,
}

#[wasm_bindgen(js_class = WasmMapMerge)]
impl WasmMapMerge {
    pub fn base(&self) -> Result<Object, JsValue> {
        map_version_object(self.base.clone())
    }

    pub fn head(&self) -> Result<Object, JsValue> {
        map_version_object(self.head.clone())
    }

    pub fn candidate(&self) -> Result<Object, JsValue> {
        map_version_object(self.candidate.clone())
    }

    pub fn merge(&self, resolver: Option<String>) -> Result<super::WasmTree, JsValue> {
        self.engine
            .merge(
                &self.base.tree,
                &self.head.tree,
                &self.candidate.tree,
                resolver_from_name(resolver)?,
            )
            .map(|inner| super::WasmTree { inner })
            .map_err(js_error)
    }

    #[wasm_bindgen(js_name = conflictPage)]
    pub fn conflict_page(
        &self,
        cursor: Option<WasmRangeCursor>,
        limit: u32,
    ) -> Result<Object, JsValue> {
        let after = cursor.and_then(|value| value.inner.after().map(Vec::from));
        let conflicts = Array::new();
        let mut last_key = None;
        let mut has_more = false;
        if limit > 0 {
            for conflict in self
                .engine
                .stream_conflicts(&self.base.tree, &self.head.tree, &self.candidate.tree)
                .map_err(js_error)?
            {
                let conflict = conflict.map_err(js_error)?;
                if after
                    .as_ref()
                    .is_some_and(|value| conflict.key.as_slice() <= value.as_slice())
                {
                    continue;
                }
                if conflicts.length() == limit {
                    has_more = true;
                    break;
                }
                last_key = Some(conflict.key.clone());
                conflicts.push(&conflict_to_object(conflict)?.into());
            }
        }
        let object = Object::new();
        Reflect::set(&object, &"conflicts".into(), &conflicts.into())?;
        let next = if has_more {
            last_key
                .map(|key| {
                    WasmRangeCursor {
                        inner: prolly::RangeCursor::after_key(key),
                    }
                    .into()
                })
                .unwrap_or(JsValue::NULL)
        } else {
            JsValue::NULL
        };
        Reflect::set(&object, &"nextCursor".into(), &next)?;
        Ok(object)
    }

    pub fn publish(&self, resolver: Option<String>) -> Result<Object, JsValue> {
        let map = self.engine.versioned_map(&self.id);
        let merge = map
            .prepare_merge(&self.base.id, &self.candidate.id)
            .map_err(js_error)?;
        merge
            .publish(resolver_from_name(resolver)?)
            .map_err(js_error)
            .and_then(map_update_object)
    }
}

#[derive(Clone)]
struct WasmStagedMapEdit {
    expected: Option<prolly::MapVersion>,
    mutations: Vec<prolly::Mutation>,
}

#[wasm_bindgen(js_name = WasmVersionedTransaction)]
pub struct WasmVersionedTransaction {
    engine: Arc<super::WasmEngine>,
    edits: RefCell<Option<BTreeMap<Vec<u8>, WasmStagedMapEdit>>>,
}

impl WasmVersionedTransaction {
    fn staged_version(&self, edit: &WasmStagedMapEdit) -> Result<prolly::MapVersion, JsValue> {
        let base = edit
            .expected
            .as_ref()
            .map(|value| value.tree.clone())
            .unwrap_or_else(|| self.engine.create());
        let tree = self
            .engine
            .batch(&base, edit.mutations.clone())
            .map_err(js_error)?;
        let id = MapVersionId::for_tree(&tree).map_err(js_error)?;
        Ok(prolly::MapVersion {
            id,
            tree,
            created_at_millis: None,
            is_head: true,
        })
    }

    fn head_value(&self, map_id: &[u8]) -> Result<Option<prolly::MapVersion>, JsValue> {
        let edits = self.edits.borrow();
        let edits = edits
            .as_ref()
            .ok_or_else(|| JsValue::from_str("versioned transaction is completed"))?;
        match edits.get(map_id) {
            Some(edit) => self.staged_version(edit).map(Some),
            None => self.engine.versioned_map(map_id).head().map_err(js_error),
        }
    }

    fn stage(
        &self,
        map_id: Vec<u8>,
        mutations: Vec<prolly::Mutation>,
    ) -> Result<prolly::MapVersion, JsValue> {
        let current = self
            .engine
            .versioned_map(&map_id)
            .head()
            .map_err(js_error)?;
        let mut edits = self.edits.borrow_mut();
        let edits = edits
            .as_mut()
            .ok_or_else(|| JsValue::from_str("versioned transaction is completed"))?;
        let edit = edits.entry(map_id).or_insert_with(|| WasmStagedMapEdit {
            expected: current,
            mutations: Vec::new(),
        });
        edit.mutations.extend(mutations);
        self.staged_version(edit)
    }
}

#[wasm_bindgen(js_class = WasmVersionedTransaction)]
impl WasmVersionedTransaction {
    pub fn head(&self, map_id: Uint8Array) -> Result<Option<Object>, JsValue> {
        self.head_value(&map_id.to_vec())?
            .map(map_version_object)
            .transpose()
    }

    pub fn get(&self, map_id: Uint8Array, key: Uint8Array) -> Result<JsValue, JsValue> {
        match self.head_value(&map_id.to_vec())? {
            Some(version) => self
                .engine
                .get(&version.tree, &key.to_vec())
                .map(optional_bytes)
                .map_err(js_error),
            None => Ok(JsValue::UNDEFINED),
        }
    }

    pub fn apply(&self, map_id: Uint8Array, mutations: Array) -> Result<Object, JsValue> {
        self.stage(
            map_id.to_vec(),
            crate::indexed::mutations_from_array(&mutations)?,
        )
        .and_then(map_version_object)
    }

    #[wasm_bindgen(js_name = applyIf)]
    pub fn apply_if(
        &self,
        map_id: Uint8Array,
        expected: Option<Uint8Array>,
        mutations: Array,
    ) -> Result<Object, JsValue> {
        let expected = expected
            .map(|value| MapVersionId::from_bytes(&value.to_vec()))
            .transpose()
            .map_err(js_error)?;
        let current = self.head_value(&map_id.to_vec())?;
        if current.as_ref().map(|value| &value.id) != expected.as_ref() {
            return map_update_object(VersionedMapUpdate::Conflict { current });
        }
        let previous = current.map(|value| value.id);
        let current = self.stage(
            map_id.to_vec(),
            crate::indexed::mutations_from_array(&mutations)?,
        )?;
        map_update_object(if previous.as_ref() == Some(&current.id) {
            VersionedMapUpdate::Unchanged {
                current: Some(current),
            }
        } else {
            VersionedMapUpdate::Applied { previous, current }
        })
    }

    pub fn put(
        &self,
        map_id: Uint8Array,
        key: Uint8Array,
        value: Uint8Array,
    ) -> Result<Object, JsValue> {
        self.stage(
            map_id.to_vec(),
            vec![prolly::Mutation::Upsert {
                key: key.to_vec(),
                val: value.to_vec(),
            }],
        )
        .and_then(map_version_object)
    }

    pub fn delete(&self, map_id: Uint8Array, key: Uint8Array) -> Result<Object, JsValue> {
        self.stage(
            map_id.to_vec(),
            vec![prolly::Mutation::Delete { key: key.to_vec() }],
        )
        .and_then(map_version_object)
    }

    pub fn commit(&self) -> Result<Object, JsValue> {
        let edits = self
            .edits
            .borrow_mut()
            .take()
            .ok_or_else(|| JsValue::from_str("versioned transaction is completed"))?;
        let mut logical_conflict = None;
        let result = self.engine.versioned_maps_transaction(|maps| {
            let mut versions = Vec::with_capacity(edits.len());
            for (map_id, edit) in edits {
                let expected = edit.expected.as_ref().map(|value| &value.id);
                match maps.apply_if(&map_id, expected, edit.mutations)? {
                    VersionedMapUpdate::Applied { current, .. } => versions.push(current),
                    VersionedMapUpdate::Unchanged {
                        current: Some(current),
                    } => versions.push(current),
                    VersionedMapUpdate::Conflict { current } => {
                        logical_conflict = Some((map_id, current));
                        return Err(prolly::Error::InvalidVersionedMap(
                            "portable multi-map transaction conflict".into(),
                        ));
                    }
                    VersionedMapUpdate::Unchanged { current: None } => {
                        return Err(prolly::Error::InvalidVersionedMap(
                            "multi-map transaction produced no current version".into(),
                        ));
                    }
                }
            }
            Ok(versions)
        });
        let object = Object::new();
        if let Some((map_id, current)) = logical_conflict {
            Reflect::set(&object, &"applied".into(), &false.into())?;
            Reflect::set(&object, &"versions".into(), &Array::new().into())?;
            set_bytes(&object, "conflictMapId", &map_id)?;
            Reflect::set(
                &object,
                &"conflictCurrent".into(),
                &current
                    .map(map_version_object)
                    .transpose()?
                    .map(Into::into)
                    .unwrap_or(JsValue::UNDEFINED),
            )?;
            return Ok(object);
        }
        let versions = result.map_err(js_error)?;
        let array = Array::new();
        for version in versions {
            array.push(&map_version_object(version)?.into());
        }
        Reflect::set(&object, &"applied".into(), &true.into())?;
        Reflect::set(&object, &"versions".into(), &array.into())?;
        Reflect::set(&object, &"conflictMapId".into(), &JsValue::UNDEFINED)?;
        Reflect::set(&object, &"conflictCurrent".into(), &JsValue::UNDEFINED)?;
        Ok(object)
    }

    pub fn rollback(&self) -> Result<(), JsValue> {
        self.edits
            .borrow_mut()
            .take()
            .ok_or_else(|| JsValue::from_str("versioned transaction is completed"))?;
        Ok(())
    }
}

#[wasm_bindgen(js_name = WasmMapSnapshot)]
pub struct WasmMapSnapshot {
    engine: Arc<super::WasmEngine>,
    id: Vec<u8>,
    version: MapVersionId,
}

impl WasmMapSnapshot {
    fn load(&self) -> Result<prolly::MapSnapshot<'_, Arc<prolly::MemStore>>, JsValue> {
        self.engine
            .versioned_map(&self.id)
            .snapshot_at(&self.version)
            .map_err(js_error)?
            .ok_or_else(|| JsValue::from_str("versioned snapshot no longer exists"))
    }
}

#[wasm_bindgen(js_class = WasmMapSnapshot)]
impl WasmMapSnapshot {
    pub fn id(&self) -> Vec<u8> {
        self.version.clone().into_cid().0.to_vec()
    }

    pub fn version(&self) -> Result<Object, JsValue> {
        map_version_object(self.load()?.version().clone())
    }

    pub fn get(&self, key: Uint8Array) -> Result<JsValue, JsValue> {
        self.load()?
            .get(&key.to_vec())
            .map(optional_bytes)
            .map_err(js_error)
    }

    #[wasm_bindgen(js_name = getMany)]
    pub fn get_many(&self, keys: Array) -> Result<Array, JsValue> {
        let keys = keys
            .iter()
            .map(|value| Uint8Array::new(&value).to_vec())
            .collect::<Vec<_>>();
        let result = Array::new();
        for value in self.load()?.get_many(&keys).map_err(js_error)? {
            result.push(&optional_bytes(value));
        }
        Ok(result)
    }

    #[wasm_bindgen(js_name = containsKey)]
    pub fn contains_key(&self, key: Uint8Array) -> Result<bool, JsValue> {
        self.load()?.contains_key(&key.to_vec()).map_err(js_error)
    }

    #[wasm_bindgen(js_name = firstEntry)]
    pub fn first_entry(&self) -> Result<JsValue, JsValue> {
        optional_entry(self.load()?.first_entry().map_err(js_error)?)
    }

    #[wasm_bindgen(js_name = lastEntry)]
    pub fn last_entry(&self) -> Result<JsValue, JsValue> {
        optional_entry(self.load()?.last_entry().map_err(js_error)?)
    }

    #[wasm_bindgen(js_name = lowerBound)]
    pub fn lower_bound(&self, key: Uint8Array) -> Result<JsValue, JsValue> {
        optional_entry(self.load()?.lower_bound(&key.to_vec()).map_err(js_error)?)
    }

    #[wasm_bindgen(js_name = upperBound)]
    pub fn upper_bound(&self, key: Uint8Array) -> Result<JsValue, JsValue> {
        optional_entry(self.load()?.upper_bound(&key.to_vec()).map_err(js_error)?)
    }

    pub fn range(&self, start: Uint8Array, end: Option<Uint8Array>) -> Result<Array, JsValue> {
        let entries = self
            .load()?
            .range(
                &start.to_vec(),
                end.as_ref().map(Uint8Array::to_vec).as_deref(),
            )
            .map_err(js_error)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(js_error)?;
        entries_to_array(entries)
    }

    pub fn prefix(&self, prefix: Uint8Array) -> Result<Array, JsValue> {
        let entries = self
            .load()?
            .prefix(&prefix.to_vec())
            .map_err(js_error)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(js_error)?;
        entries_to_array(entries)
    }

    #[wasm_bindgen(js_name = rangePage)]
    pub fn range_page(
        &self,
        cursor: Option<WasmRangeCursor>,
        end: Option<Uint8Array>,
        limit: u32,
    ) -> Result<Object, JsValue> {
        self.load()?
            .range_page(
                &cursor
                    .map(|value| value.inner)
                    .unwrap_or_else(prolly::RangeCursor::start),
                end.as_ref().map(Uint8Array::to_vec).as_deref(),
                limit as usize,
            )
            .map_err(js_error)
            .and_then(range_page_to_object)
    }

    #[wasm_bindgen(js_name = prefixPage)]
    pub fn prefix_page(
        &self,
        prefix: Uint8Array,
        cursor: Option<WasmRangeCursor>,
        limit: u32,
    ) -> Result<Object, JsValue> {
        self.load()?
            .prefix_page(
                &prefix.to_vec(),
                &cursor
                    .map(|value| value.inner)
                    .unwrap_or_else(prolly::RangeCursor::start),
                limit as usize,
            )
            .map_err(js_error)
            .and_then(range_page_to_object)
    }

    #[wasm_bindgen(js_name = reversePage)]
    pub fn reverse_page(
        &self,
        cursor: Option<WasmReverseCursor>,
        start: Uint8Array,
        limit: u32,
    ) -> Result<Object, JsValue> {
        self.load()?
            .reverse_page(
                &cursor
                    .map(|value| value.inner)
                    .unwrap_or_else(prolly::ReverseCursor::end),
                &start.to_vec(),
                limit as usize,
            )
            .map_err(js_error)
            .and_then(reverse_page_to_object)
    }

    #[wasm_bindgen(js_name = prefixReversePage)]
    pub fn prefix_reverse_page(
        &self,
        prefix: Uint8Array,
        cursor: Option<WasmReverseCursor>,
        limit: u32,
    ) -> Result<Object, JsValue> {
        self.load()?
            .prefix_reverse_page(
                &prefix.to_vec(),
                &cursor
                    .map(|value| value.inner)
                    .unwrap_or_else(prolly::ReverseCursor::end),
                limit as usize,
            )
            .map_err(js_error)
            .and_then(reverse_page_to_object)
    }
    #[wasm_bindgen(js_name = proveKey)]
    pub fn prove_key(&self, key: Uint8Array) -> Result<WasmKeyProof, JsValue> {
        self.load()?
            .prove_key(&key.to_vec())
            .map(|inner| WasmKeyProof { inner })
            .map_err(js_error)
    }

    #[wasm_bindgen(js_name = proveKeys)]
    pub fn prove_keys(&self, keys: Array) -> Result<WasmMultiKeyProof, JsValue> {
        let keys = keys
            .iter()
            .map(|value| Uint8Array::new(&value).to_vec())
            .collect::<Vec<_>>();
        self.load()?
            .prove_keys(&keys)
            .map(|inner| WasmMultiKeyProof { inner })
            .map_err(js_error)
    }

    #[wasm_bindgen(js_name = proveRange)]
    pub fn prove_range(
        &self,
        start: Uint8Array,
        end: Option<Uint8Array>,
    ) -> Result<WasmRangeProof, JsValue> {
        let end = end.map(|value| value.to_vec());
        self.load()?
            .prove_range(&start.to_vec(), end.as_deref())
            .map(|inner| WasmRangeProof { inner })
            .map_err(js_error)
    }

    #[wasm_bindgen(js_name = provePrefix)]
    pub fn prove_prefix(&self, prefix: Uint8Array) -> Result<WasmRangeProof, JsValue> {
        self.load()?
            .prove_prefix(&prefix.to_vec())
            .map(|inner| WasmRangeProof { inner })
            .map_err(js_error)
    }

    #[wasm_bindgen(js_name = proveRangePage)]
    pub fn prove_range_page(
        &self,
        cursor: Option<WasmRangeCursor>,
        end: Option<Uint8Array>,
        limit: u32,
    ) -> Result<WasmProvedRangePage, JsValue> {
        let end = end.map(|value| value.to_vec());
        self.load()?
            .prove_range_page(
                &cursor
                    .map(|value| value.inner)
                    .unwrap_or_else(prolly::RangeCursor::start),
                end.as_deref(),
                limit as usize,
            )
            .map(|inner| WasmProvedRangePage { inner })
            .map_err(js_error)
    }
    pub fn stats(&self) -> Result<Object, JsValue> {
        let value = self.load()?.stats().map_err(js_error)?;
        maintenance_summary(
            value.total_key_value_pairs as u64,
            value.total_tree_size_bytes as u64,
        )
    }
    pub fn export(&self) -> Result<WasmSnapshotBundle, JsValue> {
        self.load()
            .and_then(|snapshot| snapshot.export().map_err(js_error))
            .map(|inner| WasmSnapshotBundle { inner })
    }
    pub fn read(&self) -> Result<WasmReadSession, JsValue> {
        let tree = self.load()?.tree().clone();
        self.engine
            .read_owned(tree)
            .map(|inner| WasmReadSession { inner })
            .map_err(js_error)
    }
}

fn optional_entry(value: Option<(Vec<u8>, Vec<u8>)>) -> Result<JsValue, JsValue> {
    value
        .map(|(key, value)| entry_object(key, value).map(JsValue::from))
        .transpose()
        .map(|value| value.unwrap_or(JsValue::NULL))
}

#[wasm_bindgen(js_name = WasmReadSession)]
pub struct WasmReadSession {
    inner: OwnedReadSession<Arc<prolly::MemStore>>,
}

#[wasm_bindgen(js_class = WasmReadSession)]
impl WasmReadSession {
    pub fn get(&self, key: Uint8Array) -> Result<JsValue, JsValue> {
        self.inner
            .get_with(&key.to_vec(), <[u8]>::to_vec)
            .map(optional_bytes)
            .map_err(js_error)
    }

    #[wasm_bindgen(js_name = withValueView)]
    pub fn with_value_view(&self, key: Uint8Array, visitor: &Function) -> Result<bool, JsValue> {
        let key = key.to_vec();
        let visited = self
            .inner
            .get_with(&key, |value| {
                // SAFETY: JavaScript runs synchronously while the immutable
                // leaf is borrowed. The TypeScript facade expires the view
                // before this call returns.
                let view = unsafe { Uint8Array::view(value) };
                visitor.call1(&JsValue::UNDEFINED, &view.into()).map(|_| ())
            })
            .map_err(js_error)?;
        match visited {
            Some(result) => result.map(|()| true),
            None => Ok(false),
        }
    }

    #[wasm_bindgen(js_name = scanRangeView)]
    pub fn scan_range_view(
        &self,
        start: Uint8Array,
        end: Option<Uint8Array>,
        visitor: &Function,
    ) -> Result<Object, JsValue> {
        let start = start.to_vec();
        let end = end.map(|value| value.to_vec());
        let mut callback_error = None;
        let outcome = self
            .inner
            .scan_range_until(&start, end.as_deref(), |entry| {
                let result = borrowed_entry_view_object(entry.key(), entry.value())
                    .and_then(|value| call_scan_visitor(visitor, value));
                scan_callback_flow(result, &mut callback_error)
            })
            .map_err(js_error)?;
        callback_error.map_or_else(|| scan_outcome_to_object(outcome), Err)
    }
}

#[wasm_bindgen(js_name = WasmKeyProof)]
pub struct WasmKeyProof {
    inner: KeyProof,
}

#[wasm_bindgen(js_class = WasmKeyProof)]
impl WasmKeyProof {
    pub fn verify(&self) -> Result<Object, JsValue> {
        let value = prolly::verify_key_proof(&self.inner);
        let object = Object::new();
        Reflect::set(&object, &"valid".into(), &value.valid.into())?;
        Reflect::set(&object, &"exists".into(), &value.exists().into())?;
        match value.value {
            Some(value) => set_bytes(&object, "value", &value)?,
            None => {
                Reflect::set(&object, &"value".into(), &JsValue::UNDEFINED)?;
            }
        }
        Ok(object)
    }
}

#[wasm_bindgen(js_name = WasmMultiKeyProof)]
pub struct WasmMultiKeyProof {
    inner: MultiKeyProof,
}

#[wasm_bindgen(js_class = WasmMultiKeyProof)]
impl WasmMultiKeyProof {
    pub fn verify(&self) -> Result<Object, JsValue> {
        multi_key_proof_verification_to_object(prolly::verify_multi_key_proof(&self.inner))
    }
}

#[wasm_bindgen(js_name = WasmRangeProof)]
pub struct WasmRangeProof {
    inner: RangeProof,
}

#[wasm_bindgen(js_class = WasmRangeProof)]
impl WasmRangeProof {
    pub fn verify(&self) -> Result<Object, JsValue> {
        range_proof_verification_to_object(prolly::verify_range_proof(&self.inner))
    }
}

#[wasm_bindgen(js_name = WasmProvedRangePage)]
pub struct WasmProvedRangePage {
    inner: ProvedRangePage,
}

#[wasm_bindgen(js_class = WasmProvedRangePage)]
impl WasmProvedRangePage {
    pub fn page(&self) -> Result<Object, JsValue> {
        range_page_to_object(self.inner.page.clone())
    }

    pub fn verify(&self) -> Result<Object, JsValue> {
        range_page_proof_verification_to_object(prolly::verify_range_page_proof(&self.inner.proof))
    }
}

fn maintenance_summary(item_count: u64, byte_count: u64) -> Result<Object, JsValue> {
    let object = Object::new();
    Reflect::set(&object, &"itemCount".into(), &item_count.to_string().into())?;
    Reflect::set(&object, &"byteCount".into(), &byte_count.to_string().into())?;
    Ok(object)
}

fn set_version_ids(object: &Object, name: &str, values: Vec<MapVersionId>) -> Result<(), JsValue> {
    let array = Array::new();
    for value in values {
        array.push(&Uint8Array::from(value.as_cid().as_bytes()).into());
    }
    Reflect::set(object, &name.into(), &array.into())?;
    Ok(())
}

fn version_prune_object(value: prolly::VersionPruneResult) -> Result<Object, JsValue> {
    let object = Object::new();
    set_version_ids(&object, "retained", value.retained)?;
    set_version_ids(&object, "removed", value.removed)?;
    Ok(object)
}

fn cid_array(values: &[prolly::Cid]) -> Array {
    let array = Array::new();
    for value in values {
        array.push(&Uint8Array::from(value.as_bytes()).into());
    }
    array
}

fn set_count(object: &Object, name: &str, value: usize) -> Result<(), JsValue> {
    Reflect::set(object, &name.into(), &value.to_string().into())?;
    Ok(())
}

fn named_root_retention_object(value: prolly::NamedRootRetention) -> Result<Object, JsValue> {
    let object = Object::new();
    let names = Array::new();
    Reflect::set(&object, &"names".into(), &names.into())?;
    match value {
        prolly::NamedRootRetention::All => {
            Reflect::set(&object, &"kind".into(), &"all".into())?;
        }
        prolly::NamedRootRetention::Exact { names } => {
            Reflect::set(&object, &"kind".into(), &"exact".into())?;
            let values = Array::new();
            for name in names {
                values.push(&Uint8Array::from(name.as_slice()).into());
            }
            Reflect::set(&object, &"names".into(), &values.into())?;
        }
        prolly::NamedRootRetention::Prefix { prefix } => {
            Reflect::set(&object, &"kind".into(), &"prefix".into())?;
            set_bytes(&object, "prefix", &prefix)?;
        }
        prolly::NamedRootRetention::NewestByName { prefix, count } => {
            Reflect::set(&object, &"kind".into(), &"newest_by_name".into())?;
            set_bytes(&object, "prefix", &prefix)?;
            set_count(&object, "count", count)?;
        }
        prolly::NamedRootRetention::UpdatedSince {
            prefix,
            min_updated_at_millis,
        } => {
            Reflect::set(&object, &"kind".into(), &"updated_since".into())?;
            set_bytes(&object, "prefix", &prefix)?;
            Reflect::set(
                &object,
                &"minUpdatedAtMillis".into(),
                &min_updated_at_millis.to_string().into(),
            )?;
        }
    }
    Ok(object)
}

fn catalog_verification_object(value: prolly::MapCatalogVerification) -> Result<Object, JsValue> {
    let object = Object::new();
    set_bytes(&object, "head", value.head.as_cid().as_bytes())?;
    set_count(&object, "versionCount", value.version_count)?;
    set_count(&object, "reachableNodes", value.reachable_nodes)?;
    set_count(&object, "reachableBytes", value.reachable_bytes)?;
    Ok(object)
}

fn gc_reachability_object(value: prolly::GcReachability) -> Result<Object, JsValue> {
    let object = Object::new();
    Reflect::set(
        &object,
        &"liveCids".into(),
        &cid_array(&value.live_cids).into(),
    )?;
    set_count(&object, "liveNodes", value.live_nodes)?;
    set_count(&object, "liveBytes", value.live_bytes)?;
    set_count(&object, "leafNodes", value.leaf_nodes)?;
    set_count(&object, "internalNodes", value.internal_nodes)?;
    Ok(object)
}

pub(crate) fn gc_plan_object(value: prolly::GcPlan) -> Result<Object, JsValue> {
    let object = Object::new();
    Reflect::set(
        &object,
        &"reachability".into(),
        &gc_reachability_object(value.reachability)?.into(),
    )?;
    set_count(&object, "candidateNodes", value.candidate_nodes)?;
    Reflect::set(
        &object,
        &"reclaimableCids".into(),
        &cid_array(&value.reclaimable_cids).into(),
    )?;
    set_count(&object, "reclaimableNodes", value.reclaimable_nodes)?;
    set_count(&object, "reclaimableBytes", value.reclaimable_bytes)?;
    set_count(&object, "missingCandidates", value.missing_candidates)?;
    Ok(object)
}

fn gc_sweep_object(value: prolly::GcSweep) -> Result<Object, JsValue> {
    let object = Object::new();
    Reflect::set(&object, &"plan".into(), &gc_plan_object(value.plan)?.into())?;
    set_count(&object, "deletedNodes", value.deleted_nodes)?;
    set_count(&object, "deletedBytes", value.deleted_bytes)?;
    Ok(object)
}

fn blob_ref_object(value: prolly::BlobRef) -> Result<Object, JsValue> {
    let object = Object::new();
    set_bytes(&object, "cid", value.cid.as_bytes())?;
    Reflect::set(&object, &"len".into(), &value.len.to_string().into())?;
    Ok(object)
}

fn blob_refs_array(values: Vec<prolly::BlobRef>) -> Result<Array, JsValue> {
    let array = Array::new();
    for value in values {
        array.push(&blob_ref_object(value)?.into());
    }
    Ok(array)
}

fn blob_gc_reachability_object(value: prolly::BlobGcReachability) -> Result<Object, JsValue> {
    let object = Object::new();
    Reflect::set(
        &object,
        &"liveBlobs".into(),
        &blob_refs_array(value.live_blobs)?.into(),
    )?;
    set_count(&object, "liveBlobCount", value.live_blob_count)?;
    Reflect::set(
        &object,
        &"liveBlobBytes".into(),
        &value.live_blob_bytes.to_string().into(),
    )?;
    set_count(&object, "scannedNodes", value.scanned_nodes)?;
    set_count(&object, "scannedValues", value.scanned_values)?;
    Ok(object)
}

fn blob_gc_plan_object(value: prolly::BlobGcPlan) -> Result<Object, JsValue> {
    let object = Object::new();
    Reflect::set(
        &object,
        &"reachability".into(),
        &blob_gc_reachability_object(value.reachability)?.into(),
    )?;
    set_count(&object, "candidateBlobs", value.candidate_blobs)?;
    Reflect::set(
        &object,
        &"reclaimableBlobs".into(),
        &blob_refs_array(value.reclaimable_blobs)?.into(),
    )?;
    set_count(
        &object,
        "reclaimableBlobCount",
        value.reclaimable_blob_count,
    )?;
    Reflect::set(
        &object,
        &"reclaimableBlobBytes".into(),
        &value.reclaimable_blob_bytes.to_string().into(),
    )?;
    set_count(&object, "missingCandidates", value.missing_candidates)?;
    Ok(object)
}

fn blob_gc_sweep_object(value: prolly::BlobGcSweep) -> Result<Object, JsValue> {
    let object = Object::new();
    Reflect::set(
        &object,
        &"plan".into(),
        &blob_gc_plan_object(value.plan)?.into(),
    )?;
    set_count(&object, "deletedBlobs", value.deleted_blobs)?;
    Reflect::set(
        &object,
        &"deletedBlobBytes".into(),
        &value.deleted_blob_bytes.to_string().into(),
    )?;
    Ok(object)
}

fn versioned_batch_stats_object(stats: prolly::BatchApplyStats) -> Result<Object, JsValue> {
    let object = Object::new();
    set_count(&object, "inputMutations", stats.input_mutations)?;
    set_count(&object, "effectiveMutations", stats.effective_mutations)?;
    Reflect::set(
        &object,
        &"preprocessInputSorted".into(),
        &stats.preprocess_input_sorted.into(),
    )?;
    set_count(&object, "affectedLeaves", stats.affected_leaves)?;
    set_count(&object, "changedLeaves", stats.changed_leaves)?;
    set_count(&object, "sparseLeafApplies", stats.sparse_leaf_applies)?;
    set_count(&object, "writtenNodes", stats.written_nodes)?;
    set_count(&object, "writtenBytes", stats.written_bytes)?;
    Reflect::set(
        &object,
        &"usedAppendFastPath".into(),
        &stats.used_append_fast_path.into(),
    )?;
    Reflect::set(
        &object,
        &"usedBatchedRoute".into(),
        &stats.used_batched_route.into(),
    )?;
    Reflect::set(
        &object,
        &"usedCoalescedRebuild".into(),
        &stats.used_coalesced_rebuild.into(),
    )?;
    Reflect::set(
        &object,
        &"usedDeferredRebalancing".into(),
        &stats.used_deferred_rebalancing.into(),
    )?;
    Reflect::set(
        &object,
        &"usedBottomUpRebuild".into(),
        &stats.used_bottom_up_rebuild.into(),
    )?;
    Reflect::set(
        &object,
        &"cacheWrittenNodes".into(),
        &stats.cache_written_nodes.into(),
    )?;
    Ok(object)
}

#[wasm_bindgen(js_class = WasmProllyEngine)]
impl WasmProllyEngine {
    #[wasm_bindgen(js_name = beginVersionedTransaction)]
    pub fn portable_begin_versioned_transaction(&self) -> WasmVersionedTransaction {
        WasmVersionedTransaction {
            engine: Arc::clone(&self.inner),
            edits: RefCell::new(Some(BTreeMap::new())),
        }
    }

    #[wasm_bindgen(js_name = versionedMap)]
    pub fn portable_versioned_map(&self, id: Uint8Array) -> Result<WasmVersionedMap, JsValue> {
        let id = id.to_vec();
        if id.is_empty() {
            return Err(JsValue::from_str("versioned-map id must not be empty"));
        }
        Ok(WasmVersionedMap {
            engine: Arc::clone(&self.inner),
            id,
        })
    }
}

fn map_version_object(version: prolly::MapVersion) -> Result<Object, JsValue> {
    let object = Object::new();
    set_bytes(&object, "id", version.id.as_cid().as_bytes())?;
    Reflect::set(
        &object,
        &"createdAtMillis".into(),
        &version
            .created_at_millis
            .map(|value| JsValue::from_str(&value.to_string()))
            .unwrap_or(JsValue::UNDEFINED),
    )?;
    Reflect::set(
        &object,
        &"isHead".into(),
        &JsValue::from_bool(version.is_head),
    )?;
    Ok(object)
}

fn map_update_object(update: VersionedMapUpdate) -> Result<Object, JsValue> {
    let object = Object::new();
    let (kind, previous, current) = match update {
        VersionedMapUpdate::Applied { previous, current } => (
            "applied",
            previous.map(|value| value.into_cid().0.to_vec()),
            Some(current),
        ),
        VersionedMapUpdate::Unchanged { current } => ("unchanged", None, current),
        VersionedMapUpdate::Conflict { current } => ("conflict", None, current),
    };
    Reflect::set(&object, &"kind".into(), &kind.into())?;
    Reflect::set(&object, &"previous".into(), &optional_bytes(previous))?;
    let current = current
        .map(map_version_object)
        .transpose()?
        .map(JsValue::from)
        .unwrap_or(JsValue::UNDEFINED);
    Reflect::set(&object, &"current".into(), &current)?;
    Ok(object)
}
