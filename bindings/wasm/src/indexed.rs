use super::{js_error, optional_bytes, WasmProllyEngine};
use crate::page::{set_bytes, set_optional_bytes};
use js_sys::{Array, Function, Object, Reflect, Uint8Array};
use prolly::{
    IndexProjection, IndexedSnapshotId, SecondaryIndex, SecondaryIndexEntry, SecondaryIndexError,
    SecondaryIndexRegistry,
};
use std::sync::Arc;
use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;

#[derive(Clone)]
struct JsIndexExtractor(Function);

// Browser WASM executes these callbacks on its single JavaScript agent. The
// core trait is Send + Sync so the same registry type can be shared by native
// builds, but this adapter never dispatches a Function to another thread.
unsafe impl Send for JsIndexExtractor {}
unsafe impl Sync for JsIndexExtractor {}

impl JsIndexExtractor {
    fn extract(
        &self,
        primary_key: &[u8],
        source_value: &[u8],
    ) -> Result<Vec<SecondaryIndexEntry>, SecondaryIndexError> {
        let value = self
            .0
            .call2(
                &JsValue::NULL,
                &Uint8Array::from(primary_key).into(),
                &Uint8Array::from(source_value).into(),
            )
            .map_err(|error| SecondaryIndexError::new(js_value_message(error)))?;
        let entries = value
            .dyn_into::<Array>()
            .map_err(|_| SecondaryIndexError::new("index extractor must return an Array"))?;
        entries
            .iter()
            .map(|entry| {
                let term = Reflect::get(&entry, &"term".into())
                    .map_err(|error| SecondaryIndexError::new(js_value_message(error)))?
                    .dyn_into::<Uint8Array>()
                    .map_err(|_| SecondaryIndexError::new("index term must be a Uint8Array"))?
                    .to_vec();
                let projection = Reflect::get(&entry, &"projection".into())
                    .map_err(|error| SecondaryIndexError::new(js_value_message(error)))?;
                let projection = if projection.is_null() || projection.is_undefined() {
                    None
                } else {
                    Some(
                        projection
                            .dyn_into::<Uint8Array>()
                            .map_err(|_| {
                                SecondaryIndexError::new("index projection must be a Uint8Array")
                            })?
                            .to_vec(),
                    )
                };
                Ok(SecondaryIndexEntry { term, projection })
            })
            .collect()
    }
}

fn js_value_message(value: JsValue) -> String {
    value
        .as_string()
        .unwrap_or_else(|| "JavaScript index extractor failed".to_string())
}

#[wasm_bindgen(js_name = WasmIndexRegistry)]
pub struct WasmIndexRegistry {
    registry: SecondaryIndexRegistry,
}

#[wasm_bindgen(js_class = WasmIndexRegistry)]
impl WasmIndexRegistry {
    #[wasm_bindgen(constructor)]
    pub fn new() -> Self {
        Self {
            registry: SecondaryIndexRegistry::new(),
        }
    }

    pub fn register(
        &mut self,
        name: Uint8Array,
        generation: u64,
        extractor_id: String,
        projection: String,
        extractor: Function,
    ) -> Result<(), JsValue> {
        let projection = match projection.as_str() {
            "keys_only" => IndexProjection::KeysOnly,
            "include" => IndexProjection::Include,
            "all" => IndexProjection::All,
            _ => return Err(JsValue::from_str("invalid index projection")),
        };
        let callback = JsIndexExtractor(extractor);
        let definition = SecondaryIndex::builder(name.to_vec(), generation, extractor_id)
            .projection(projection)
            .extract(move |key, value| callback.extract(key, value))
            .map_err(js_error)?;
        self.registry = self
            .registry
            .clone()
            .register(definition)
            .map_err(js_error)?;
        Ok(())
    }
}

impl Default for WasmIndexRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[wasm_bindgen(js_name = WasmIndexedMap)]
pub struct WasmIndexedMap {
    engine: Arc<super::WasmEngine>,
    id: Vec<u8>,
    registry: SecondaryIndexRegistry,
}

#[wasm_bindgen(js_class = WasmIndexedMap)]
impl WasmIndexedMap {
    pub fn get(&self, key: Uint8Array) -> Result<JsValue, JsValue> {
        self.engine
            .indexed_map(&self.id, self.registry.clone())
            .map_err(js_error)?
            .get(&key.to_vec())
            .map(optional_bytes)
            .map_err(js_error)
    }

    pub fn put(&self, key: Uint8Array, value: Uint8Array) -> Result<Object, JsValue> {
        let version = self
            .engine
            .indexed_map(&self.id, self.registry.clone())
            .map_err(js_error)?
            .put(key.to_vec(), value.to_vec())
            .map_err(js_error)?;
        indexed_version_object(version)
    }

    pub fn delete(&self, key: Uint8Array) -> Result<Object, JsValue> {
        let version = self
            .engine
            .indexed_map(&self.id, self.registry.clone())
            .map_err(js_error)?
            .delete(key.to_vec())
            .map_err(js_error)?;
        indexed_version_object(version)
    }

    #[wasm_bindgen(js_name = ensureIndex)]
    pub fn ensure_index(&self, name: Uint8Array) -> Result<Object, JsValue> {
        let result = self
            .engine
            .indexed_map(&self.id, self.registry.clone())
            .map_err(js_error)?
            .ensure_index(name.to_vec())
            .map_err(js_error)?;
        let object = Object::new();
        set_bytes(
            &object,
            "sourceVersion",
            result.source_version.as_cid().as_bytes(),
        )?;
        set_bytes(
            &object,
            "indexVersion",
            result.index_version.as_cid().as_bytes(),
        )?;
        set_bytes(
            &object,
            "catalogVersion",
            result.catalog_version.as_cid().as_bytes(),
        )?;
        Reflect::set(
            &object,
            &"activated".into(),
            &JsValue::from_bool(result.activated),
        )?;
        Ok(object)
    }

    pub fn snapshot(&self) -> Result<WasmIndexedSnapshot, JsValue> {
        let map = self
            .engine
            .indexed_map(&self.id, self.registry.clone())
            .map_err(js_error)?;
        let snapshot_id = map.snapshot().map_err(js_error)?.id().clone();
        Ok(WasmIndexedSnapshot {
            engine: Arc::clone(&self.engine),
            id: self.id.clone(),
            registry: self.registry.clone(),
            snapshot_id,
        })
    }
}

#[wasm_bindgen(js_name = WasmIndexedSnapshot)]
pub struct WasmIndexedSnapshot {
    engine: Arc<super::WasmEngine>,
    id: Vec<u8>,
    registry: SecondaryIndexRegistry,
    snapshot_id: IndexedSnapshotId,
}

#[wasm_bindgen(js_class = WasmIndexedSnapshot)]
impl WasmIndexedSnapshot {
    pub fn index(&self, name: Uint8Array) -> Result<WasmSecondaryIndex, JsValue> {
        let map = self
            .engine
            .indexed_map(&self.id, self.registry.clone())
            .map_err(js_error)?;
        map.snapshot_by_id(&self.snapshot_id)
            .map_err(js_error)?
            .index(name.to_vec())
            .map_err(js_error)?;
        Ok(WasmSecondaryIndex {
            engine: Arc::clone(&self.engine),
            id: self.id.clone(),
            registry: self.registry.clone(),
            name: name.to_vec(),
            snapshot_id: self.snapshot_id.clone(),
        })
    }
}

#[wasm_bindgen(js_name = WasmSecondaryIndex)]
pub struct WasmSecondaryIndex {
    engine: Arc<super::WasmEngine>,
    id: Vec<u8>,
    registry: SecondaryIndexRegistry,
    name: Vec<u8>,
    snapshot_id: IndexedSnapshotId,
}

#[wasm_bindgen(js_class = WasmSecondaryIndex)]
impl WasmSecondaryIndex {
    pub fn exact(&self, term: Uint8Array) -> Result<Array, JsValue> {
        let map = self
            .engine
            .indexed_map(&self.id, self.registry.clone())
            .map_err(js_error)?;
        let snapshot = map.snapshot_by_id(&self.snapshot_id).map_err(js_error)?;
        let index = snapshot.index(&self.name).map_err(js_error)?;
        let out = Array::new();
        for matched in index.exact(&term.to_vec()).map_err(js_error)? {
            out.push(&index_match_object(matched)?.into());
        }
        Ok(out)
    }

    pub fn records(&self, term: Uint8Array) -> Result<Array, JsValue> {
        let term = term.to_vec();
        let map = self
            .engine
            .indexed_map(&self.id, self.registry.clone())
            .map_err(js_error)?;
        let snapshot = map.snapshot_by_id(&self.snapshot_id).map_err(js_error)?;
        let index = snapshot.index(&self.name).map_err(js_error)?;
        let out = Array::new();
        for (primary_key, source_value) in index.records(&term).map_err(js_error)? {
            let object = Object::new();
            set_bytes(&object, "term", &term)?;
            set_bytes(&object, "primaryKey", &primary_key)?;
            set_bytes(&object, "sourceValue", &source_value)?;
            out.push(&object.into());
        }
        Ok(out)
    }
}

#[wasm_bindgen(js_class = WasmProllyEngine)]
impl WasmProllyEngine {
    #[wasm_bindgen(js_name = indexRegistry)]
    pub fn portable_index_registry(&self) -> WasmIndexRegistry {
        WasmIndexRegistry::new()
    }

    #[wasm_bindgen(js_name = indexedMap)]
    pub fn portable_indexed_map(
        &self,
        id: Uint8Array,
        registry: &WasmIndexRegistry,
    ) -> Result<WasmIndexedMap, JsValue> {
        let id = id.to_vec();
        self.inner
            .indexed_map(&id, registry.registry.clone())
            .map_err(js_error)?;
        Ok(WasmIndexedMap {
            engine: Arc::clone(&self.inner),
            id,
            registry: registry.registry.clone(),
        })
    }
}

fn indexed_version_object(version: prolly::IndexedVersion) -> Result<Object, JsValue> {
    let object = Object::new();
    set_bytes(
        &object,
        "sourceVersion",
        version.source.id.as_cid().as_bytes(),
    )?;
    set_optional_bytes(
        &object,
        "catalogVersion",
        version
            .catalog
            .as_ref()
            .map(|version| version.id.as_cid().as_bytes()),
    )?;
    Reflect::set(
        &object,
        &"indexCount".into(),
        &JsValue::from_str(&version.indexes.len().to_string()),
    )?;
    Ok(object)
}

fn index_match_object(matched: prolly::SecondaryIndexMatch) -> Result<Object, JsValue> {
    let object = Object::new();
    set_bytes(&object, "term", &matched.term)?;
    set_bytes(&object, "primaryKey", &matched.primary_key)?;
    set_optional_bytes(&object, "projection", matched.projection.as_deref())?;
    Ok(object)
}
