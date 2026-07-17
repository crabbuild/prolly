use super::{js_error, optional_bytes, WasmProllyEngine};
use crate::page::set_bytes;
use js_sys::{Object, Reflect, Uint8Array};
use prolly::{KeyProof, MapVersionId, OwnedReadSession};
use std::sync::Arc;
use wasm_bindgen::prelude::*;

#[wasm_bindgen(js_name = WasmVersionedMap)]
pub struct WasmVersionedMap {
    engine: Arc<super::WasmEngine>,
    id: Vec<u8>,
}

#[wasm_bindgen(js_class = WasmVersionedMap)]
impl WasmVersionedMap {
    pub fn initialize(&self) -> Result<Object, JsValue> {
        self.engine
            .versioned_map(&self.id)
            .initialize()
            .map_err(js_error)
            .and_then(map_version_object)
    }

    pub fn get(&self, key: Uint8Array) -> Result<JsValue, JsValue> {
        self.engine
            .versioned_map(&self.id)
            .get(&key.to_vec())
            .map(optional_bytes)
            .map_err(js_error)
    }

    pub fn put(&self, key: Uint8Array, value: Uint8Array) -> Result<Object, JsValue> {
        self.engine
            .versioned_map(&self.id)
            .put(key.to_vec(), value.to_vec())
            .map_err(js_error)
            .and_then(map_version_object)
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

    pub fn backup(&self) -> Result<Vec<u8>, JsValue> {
        self.engine
            .versioned_map(&self.id)
            .backup()
            .and_then(|value| value.to_bytes())
            .map_err(js_error)
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
    pub fn get(&self, key: Uint8Array) -> Result<JsValue, JsValue> {
        self.load()?
            .get(&key.to_vec())
            .map(optional_bytes)
            .map_err(js_error)
    }
    #[wasm_bindgen(js_name = proveKey)]
    pub fn prove_key(&self, key: Uint8Array) -> Result<WasmKeyProof, JsValue> {
        self.load()?
            .prove_key(&key.to_vec())
            .map(|inner| WasmKeyProof { inner })
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

fn maintenance_summary(item_count: u64, byte_count: u64) -> Result<Object, JsValue> {
    let object = Object::new();
    Reflect::set(&object, &"itemCount".into(), &item_count.to_string().into())?;
    Reflect::set(&object, &"byteCount".into(), &byte_count.to_string().into())?;
    Ok(object)
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
