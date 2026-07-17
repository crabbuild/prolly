use super::{
    borrowed_entry_view_object, call_scan_visitor, diffs_to_array, entries_to_array, entry_object,
    js_error, multi_key_proof_verification_to_object, optional_bytes, range_cursor_value,
    range_page_proof_verification_to_object, range_page_to_object,
    range_proof_verification_to_object, reverse_page_to_object, scan_callback_flow,
    scan_outcome_to_object, WasmProllyEngine, WasmRangeCursor, WasmReverseCursor,
};
use crate::page::set_bytes;
use js_sys::{Array, Function, Object, Reflect, Uint8Array};
use prolly::{
    KeyProof, MapVersionId, MultiKeyProof, OwnedReadSession, ProvedRangePage, RangeProof,
    VersionedMapBackup, VersionedMapUpdate,
};
use std::cell::RefCell;
use std::sync::Arc;
use wasm_bindgen::prelude::*;

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

    pub fn put(&self, key: Uint8Array, value: Uint8Array) -> Result<Object, JsValue> {
        self.engine
            .versioned_map(&self.id)
            .put(key.to_vec(), value.to_vec())
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

    #[wasm_bindgen(js_name = keepLast)]
    pub fn keep_last(&self, count: u32) -> Result<Object, JsValue> {
        let result = self
            .engine
            .versioned_map(&self.id)
            .keep_last(count as usize)
            .map_err(js_error)?;
        let object = Object::new();
        set_version_ids(&object, "retained", result.retained)?;
        set_version_ids(&object, "removed", result.removed)?;
        Ok(object)
    }

    #[wasm_bindgen(js_name = verifyCatalog)]
    pub fn verify_catalog(&self) -> Result<Object, JsValue> {
        let value = self
            .engine
            .versioned_map(&self.id)
            .verify_catalog()
            .map_err(js_error)?;
        maintenance_summary(value.version_count as u64, value.reachable_bytes as u64)
    }

    #[wasm_bindgen(js_name = planGc)]
    pub fn plan_gc(&self) -> Result<Object, JsValue> {
        let value = self
            .engine
            .versioned_map(&self.id)
            .plan_gc()
            .map_err(js_error)?;
        maintenance_summary(
            value.reachability.live_nodes as u64,
            value.reclaimable_bytes as u64,
        )
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
    pub fn export(&self) -> Result<Object, JsValue> {
        let value = self.load()?.export().map_err(js_error)?;
        maintenance_summary(
            value.nodes.len() as u64,
            value.nodes.iter().map(|node| node.bytes.len() as u64).sum(),
        )
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

#[wasm_bindgen(js_class = WasmProllyEngine)]
impl WasmProllyEngine {
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
