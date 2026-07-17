use super::{js_error, WasmProllyEngine};
use crate::page::set_bytes;
use js_sys::{Array, Float32Array, Object, Reflect, Uint8Array};
use prolly::{
    Cid, ProximityConfig, ProximityMap, ProximityMembershipProof, ProximityRecord, SearchBackend,
    SearchCompletion, SearchRequest,
};
use std::sync::Arc;
use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;

#[wasm_bindgen(js_name = WasmProximityMap)]
pub struct WasmProximityMap {
    engine: Arc<super::WasmEngine>,
    descriptor: Cid,
}

impl WasmProximityMap {
    fn load(&self) -> Result<ProximityMap<Arc<prolly::MemStore>>, JsValue> {
        ProximityMap::load(self.engine.store().clone(), self.descriptor.clone()).map_err(js_error)
    }
}

#[wasm_bindgen(js_class = WasmProximityMap)]
impl WasmProximityMap {
    pub fn search(&self, query: Float32Array, k: u32) -> Result<Object, JsValue> {
        search_map(&self.load()?, query.to_vec(), k)
    }

    pub fn read(&self) -> Result<WasmProximityReadSession, JsValue> {
        Ok(WasmProximityReadSession { map: self.load()? })
    }

    pub fn descriptor(&self) -> Vec<u8> {
        self.descriptor.as_bytes().to_vec()
    }
    pub fn verify(&self) -> Result<String, JsValue> {
        self.load()?
            .verify()
            .map(|value| value.record_count.to_string())
            .map_err(js_error)
    }
    #[wasm_bindgen(js_name = proveMembership)]
    pub fn prove_membership(&self, key: Uint8Array) -> Result<WasmProximityProof, JsValue> {
        self.load()?
            .prove_membership(&key.to_vec())
            .map(|inner| WasmProximityProof { inner })
            .map_err(js_error)
    }
}

#[wasm_bindgen(js_name = WasmProximityProof)]
pub struct WasmProximityProof {
    inner: ProximityMembershipProof,
}

#[wasm_bindgen(js_class = WasmProximityProof)]
impl WasmProximityProof {
    pub fn verify(&self, expected: Option<Uint8Array>) -> Result<JsValue, JsValue> {
        let value = match expected {
            Some(expected) => {
                let bytes = expected.to_vec();
                let raw: [u8; 32] = bytes
                    .try_into()
                    .map_err(|_| JsValue::from_str("descriptor CID must be 32 bytes"))?;
                self.inner.verify_for(&Cid(raw))
            }
            None => self.inner.verify(),
        }
        .map_err(js_error)?;
        Ok(match value.record {
            Some(record) => Uint8Array::from(record.1.as_slice()).into(),
            None => JsValue::UNDEFINED,
        })
    }
}

#[wasm_bindgen(js_name = WasmProximityReadSession)]
pub struct WasmProximityReadSession {
    map: ProximityMap<Arc<prolly::MemStore>>,
}

#[wasm_bindgen(js_class = WasmProximityReadSession)]
impl WasmProximityReadSession {
    pub fn search(&self, query: Float32Array, k: u32) -> Result<Object, JsValue> {
        search_map(&self.map, query.to_vec(), k)
    }
}

#[wasm_bindgen(js_class = WasmProllyEngine)]
impl WasmProllyEngine {
    #[wasm_bindgen(js_name = buildProximity)]
    pub fn portable_build_proximity(
        &self,
        dimensions: u32,
        records: Array,
    ) -> Result<WasmProximityMap, JsValue> {
        let records = records
            .iter()
            .map(proximity_record_from_js)
            .collect::<Result<Vec<_>, _>>()?;
        let map = ProximityMap::build(
            self.inner.store().clone(),
            ProximityConfig::new(dimensions),
            records,
        )
        .map_err(js_error)?;
        Ok(WasmProximityMap {
            engine: Arc::clone(&self.inner),
            descriptor: map.tree().descriptor.clone(),
        })
    }
}

fn proximity_record_from_js(value: JsValue) -> Result<ProximityRecord, JsValue> {
    let key = Reflect::get(&value, &"key".into())?
        .dyn_into::<Uint8Array>()
        .map_err(|_| JsValue::from_str("proximity key must be a Uint8Array"))?
        .to_vec();
    let vector = Reflect::get(&value, &"vector".into())?
        .dyn_into::<Float32Array>()
        .map_err(|_| JsValue::from_str("proximity vector must be a Float32Array"))?
        .to_vec();
    let raw_value = Reflect::get(&value, &"value".into())?;
    let value = if raw_value.is_null() || raw_value.is_undefined() {
        Vec::new()
    } else {
        raw_value
            .dyn_into::<Uint8Array>()
            .map_err(|_| JsValue::from_str("proximity value must be a Uint8Array"))?
            .to_vec()
    };
    Ok(ProximityRecord { key, vector, value })
}

fn search_map(
    map: &ProximityMap<Arc<prolly::MemStore>>,
    query: Vec<f32>,
    k: u32,
) -> Result<Object, JsValue> {
    let result = map
        .search(SearchRequest::exact(&query, k as usize))
        .map_err(js_error)?;
    let neighbors = Array::new();
    for neighbor in result.neighbors {
        let object = Object::new();
        set_bytes(&object, "key", &neighbor.key)?;
        set_bytes(&object, "value", &neighbor.value)?;
        Reflect::set(
            &object,
            &"distance".into(),
            &JsValue::from_f64(neighbor.distance),
        )?;
        neighbors.push(&object.into());
    }
    let object = Object::new();
    Reflect::set(&object, &"neighbors".into(), &neighbors.into())?;
    let completion = match result.completion {
        SearchCompletion::Exact => "exact",
        SearchCompletion::ApproximatePolicySatisfied => "approximate_policy_satisfied",
        SearchCompletion::BudgetExhausted => "budget_exhausted",
        SearchCompletion::Cancelled => "cancelled",
        SearchCompletion::DeadlineExceeded => "deadline_exceeded",
    };
    let backend = match result.plan.backend {
        SearchBackend::Native => "native",
        SearchBackend::ProductQuantized => "product_quantized",
        SearchBackend::Hnsw => "hnsw",
        SearchBackend::Composite => "composite",
        SearchBackend::Auto => "auto",
    };
    Reflect::set(&object, &"completion".into(), &completion.into())?;
    Reflect::set(&object, &"backend".into(), &backend.into())?;
    Ok(object)
}
