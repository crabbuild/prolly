use super::{js_error, WasmProllyEngine};
use crate::page::set_bytes;
use js_sys::{Array, BigInt, Float32Array, Object, Reflect, Uint8Array};
use prolly::{
    Cid, ContentGraphLimits, DistanceMetric, ProximityConfig, ProximityMap,
    ProximityMembershipProof, ProximityMutation, ProximityRecord, ProximitySearchClaim,
    ProximitySearchProof, ProximityStructuralProof, ProximityVerification, SearchBackend,
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
    pub fn count(&self) -> Result<String, JsValue> {
        Ok(self.load()?.tree().count.to_string())
    }
    pub fn config(&self) -> Result<Object, JsValue> {
        proximity_config_object(&self.load()?.tree().config)
    }
    pub fn get(&self, key: Uint8Array) -> Result<JsValue, JsValue> {
        exact_record_value(self.load()?.get(&key.to_vec()).map_err(js_error)?)
    }
    pub fn contains(&self, key: Uint8Array) -> Result<bool, JsValue> {
        self.load()?.contains_key(&key.to_vec()).map_err(js_error)
    }
    pub fn verify(&self) -> Result<Object, JsValue> {
        proximity_verification_object(self.load()?.verify().map_err(js_error)?)
    }
    pub fn mutate(&self, mutations: Array) -> Result<Object, JsValue> {
        let mutations = mutations
            .iter()
            .map(proximity_mutation_from_js)
            .collect::<Result<Vec<_>, _>>()?;
        let (map, stats) = self.load()?.mutate_batch(mutations).map_err(js_error)?;
        let result = Object::new();
        let updated = WasmProximityMap {
            engine: Arc::clone(&self.engine),
            descriptor: map.tree().descriptor.clone(),
        };
        Reflect::set(&result, &"map".into(), &JsValue::from(updated))?;
        let stats_object = Object::new();
        Reflect::set(
            &stats_object,
            &"directoryEntriesScanned".into(),
            &BigInt::from(stats.directory_entries_scanned as u64),
        )?;
        Reflect::set(
            &stats_object,
            &"directoryNodesRead".into(),
            &BigInt::from(stats.directory_nodes_read as u64),
        )?;
        Reflect::set(
            &stats_object,
            &"directoryNodesRebuilt".into(),
            &BigInt::from(stats.directory_nodes_rebuilt as u64),
        )?;
        Reflect::set(
            &stats_object,
            &"directoryNodesWritten".into(),
            &BigInt::from(stats.directory_nodes_written as u64),
        )?;
        Reflect::set(
            &stats_object,
            &"directoryNodesReused".into(),
            &BigInt::from(stats.directory_nodes_reused as u64),
        )?;
        Reflect::set(
            &stats_object,
            &"directoryLevelsRebuilt".into(),
            &BigInt::from(stats.directory_levels_rebuilt as u64),
        )?;
        Reflect::set(
            &stats_object,
            &"directoryRightEdgeRebuilt".into(),
            &stats.directory_right_edge_rebuilt.into(),
        )?;
        Reflect::set(
            &stats_object,
            &"recordsRebuilt".into(),
            &BigInt::from(stats.records_rebuilt as u64),
        )?;
        Reflect::set(
            &stats_object,
            &"nodesRead".into(),
            &BigInt::from(stats.nodes_read as u64),
        )?;
        Reflect::set(
            &stats_object,
            &"nodesWritten".into(),
            &BigInt::from(stats.nodes_written as u64),
        )?;
        Reflect::set(
            &stats_object,
            &"nodesReused".into(),
            &BigInt::from(stats.nodes_reused as u64),
        )?;
        Reflect::set(
            &stats_object,
            &"distanceEvaluations".into(),
            &BigInt::from(stats.distance_evaluations as u64),
        )?;
        Reflect::set(
            &stats_object,
            &"fullProximityRebuild".into(),
            &stats.full_proximity_rebuild.into(),
        )?;
        Reflect::set(&result, &"stats".into(), &stats_object.into())?;
        Ok(result)
    }
    pub fn rebuild(&self, mutations: Array) -> Result<WasmProximityMap, JsValue> {
        let mutations = mutations
            .iter()
            .map(proximity_mutation_from_js)
            .collect::<Result<Vec<_>, _>>()?;
        let map = self.load()?.rebuild_batch(mutations).map_err(js_error)?;
        Ok(WasmProximityMap {
            engine: Arc::clone(&self.engine),
            descriptor: map.tree().descriptor.clone(),
        })
    }
    #[wasm_bindgen(js_name = proveMembership)]
    pub fn prove_membership(&self, key: Uint8Array) -> Result<WasmProximityProof, JsValue> {
        self.load()?
            .prove_membership(&key.to_vec())
            .map(|inner| WasmProximityProof { inner })
            .map_err(js_error)
    }
    #[wasm_bindgen(js_name = proveSearch)]
    pub fn prove_search(
        &self,
        query: Float32Array,
        k: u32,
    ) -> Result<WasmProximitySearchProof, JsValue> {
        let query = query.to_vec();
        self.load()?
            .prove_search(
                SearchRequest::exact(&query, k as usize),
                &ContentGraphLimits::default(),
            )
            .map(|inner| WasmProximitySearchProof { inner })
            .map_err(js_error)
    }
    #[wasm_bindgen(js_name = proveStructure)]
    pub fn prove_structure(&self) -> Result<WasmProximityStructuralProof, JsValue> {
        self.load()?
            .prove_structure(&ContentGraphLimits::default())
            .map(|inner| WasmProximityStructuralProof { inner })
            .map_err(js_error)
    }
}

#[wasm_bindgen(js_name = WasmProximityStructuralProof)]
pub struct WasmProximityStructuralProof {
    inner: ProximityStructuralProof,
}

#[wasm_bindgen(js_class = WasmProximityStructuralProof)]
impl WasmProximityStructuralProof {
    pub fn verify(&self, expected: Option<Uint8Array>) -> Result<Object, JsValue> {
        let limits = ContentGraphLimits::default();
        let value = match expected {
            Some(expected) => {
                let bytes = expected.to_vec();
                let raw: [u8; 32] = bytes
                    .try_into()
                    .map_err(|_| JsValue::from_str("descriptor CID must be 32 bytes"))?;
                self.inner.verify_for(&Cid(raw), &limits)
            }
            None => self.inner.verify(&limits),
        }
        .map_err(js_error)?;
        let object = Object::new();
        set_bytes(&object, "descriptor", value.descriptor.as_bytes())?;
        Reflect::set(
            &object,
            &"objectCount".into(),
            &BigInt::from(value.object_count as u64),
        )?;
        Reflect::set(
            &object,
            &"summary".into(),
            &proximity_verification_object(value.summary)?.into(),
        )?;
        Ok(object)
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

#[wasm_bindgen(js_name = WasmProximitySearchProof)]
pub struct WasmProximitySearchProof {
    inner: ProximitySearchProof,
}

#[wasm_bindgen(js_class = WasmProximitySearchProof)]
impl WasmProximitySearchProof {
    pub fn verify(&self, expected: Option<Uint8Array>) -> Result<Object, JsValue> {
        let limits = ContentGraphLimits::default();
        let value = match expected {
            Some(expected) => {
                let bytes = expected.to_vec();
                let raw: [u8; 32] = bytes
                    .try_into()
                    .map_err(|_| JsValue::from_str("descriptor CID must be 32 bytes"))?;
                self.inner.verify_for_source(&Cid(raw), &limits)
            }
            None => self.inner.verify(&limits),
        }
        .map_err(js_error)?;
        let object = Object::new();
        Reflect::set(
            &object,
            &"result".into(),
            &search_result_object(value.result)?.into(),
        )?;
        match value.claim {
            ProximitySearchClaim::ExactL2Optimal {
                terminal_lower_bound,
            } => {
                Reflect::set(&object, &"claim".into(), &"exact_l2_optimal".into())?;
                Reflect::set(
                    &object,
                    &"terminalLowerBound".into(),
                    &terminal_lower_bound.into(),
                )?;
            }
            ProximitySearchClaim::HonestExecution => {
                Reflect::set(&object, &"claim".into(), &"honest_execution".into())?;
                Reflect::set(&object, &"terminalLowerBound".into(), &JsValue::UNDEFINED)?;
            }
        }
        Reflect::set(
            &object,
            &"replayedEvents".into(),
            &BigInt::from(value.replayed_events as u64).into(),
        )?;
        Ok(object)
    }
}

#[wasm_bindgen(js_name = WasmProximityReadSession)]
pub struct WasmProximityReadSession {
    map: ProximityMap<Arc<prolly::MemStore>>,
}

#[wasm_bindgen(js_class = WasmProximityReadSession)]
impl WasmProximityReadSession {
    pub fn get(&self, key: Uint8Array) -> Result<JsValue, JsValue> {
        let Some((vector, value)) = self.map.get(&key.to_vec()).map_err(js_error)? else {
            return Ok(JsValue::UNDEFINED);
        };
        let object = Object::new();
        Reflect::set(
            &object,
            &"vector".into(),
            &Float32Array::from(vector.as_slice()).into(),
        )?;
        set_bytes(&object, "value", &value)?;
        Ok(object.into())
    }

    pub fn contains(&self, key: Uint8Array) -> Result<bool, JsValue> {
        self.map.contains_key(&key.to_vec()).map_err(js_error)
    }

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

fn proximity_mutation_from_js(value: JsValue) -> Result<ProximityMutation, JsValue> {
    let key = Reflect::get(&value, &"key".into())?
        .dyn_into::<Uint8Array>()
        .map_err(|_| JsValue::from_str("proximity mutation key must be a Uint8Array"))?
        .to_vec();
    let raw_vector = Reflect::get(&value, &"vector".into())?;
    let raw_value = Reflect::get(&value, &"value".into())?;
    let value = match (
        raw_vector.is_null() || raw_vector.is_undefined(),
        raw_value.is_null() || raw_value.is_undefined(),
    ) {
        (true, true) => None,
        (false, false) => Some((
            raw_vector
                .dyn_into::<Float32Array>()
                .map_err(|_| JsValue::from_str("proximity mutation vector must be a Float32Array"))?
                .to_vec(),
            raw_value
                .dyn_into::<Uint8Array>()
                .map_err(|_| JsValue::from_str("proximity mutation value must be a Uint8Array"))?
                .to_vec(),
        )),
        _ => {
            return Err(JsValue::from_str(
                "proximity mutation vector and value must both be present or absent",
            ))
        }
    };
    Ok(ProximityMutation { key, value })
}

fn exact_record_value(record: Option<(Vec<f32>, Vec<u8>)>) -> Result<JsValue, JsValue> {
    let Some((vector, value)) = record else {
        return Ok(JsValue::UNDEFINED);
    };
    let object = Object::new();
    Reflect::set(
        &object,
        &"vector".into(),
        &Float32Array::from(vector.as_slice()).into(),
    )?;
    set_bytes(&object, "value", &value)?;
    Ok(object.into())
}

fn proximity_config_object(config: &ProximityConfig) -> Result<Object, JsValue> {
    let object = Object::new();
    Reflect::set(&object, &"dimensions".into(), &config.dimensions.into())?;
    let metric = match config.metric {
        DistanceMetric::L2Squared => "l2_squared",
        DistanceMetric::Cosine => "cosine",
        DistanceMetric::InnerProduct => "inner_product",
    };
    Reflect::set(&object, &"metric".into(), &metric.into())?;
    Reflect::set(
        &object,
        &"logChunkSize".into(),
        &config.hierarchy.log_chunk_size.into(),
    )?;
    Reflect::set(
        &object,
        &"levelHashSeed".into(),
        &BigInt::from(config.hierarchy.level_hash_seed),
    )?;
    Reflect::set(
        &object,
        &"minPageBytes".into(),
        &config.overflow.min_page_bytes.into(),
    )?;
    Reflect::set(
        &object,
        &"targetPageBytes".into(),
        &config.overflow.target_page_bytes.into(),
    )?;
    Reflect::set(
        &object,
        &"maxPageBytes".into(),
        &config.overflow.max_page_bytes.into(),
    )?;
    Reflect::set(
        &object,
        &"overflowHashSeed".into(),
        &BigInt::from(config.overflow.hash_seed),
    )?;
    Reflect::set(
        &object,
        &"inlineThresholdBytes".into(),
        &config.vector_storage.inline_threshold_bytes.into(),
    )?;
    match &config.scalar_quantization {
        Some(value) => Reflect::set(
            &object,
            &"scalarQuantizationGroupSize".into(),
            &value.group_size.into(),
        )?,
        None => Reflect::set(
            &object,
            &"scalarQuantizationGroupSize".into(),
            &JsValue::UNDEFINED,
        )?,
    };
    Ok(object)
}

fn proximity_verification_object(value: ProximityVerification) -> Result<Object, JsValue> {
    let object = Object::new();
    Reflect::set(
        &object,
        &"recordCount".into(),
        &BigInt::from(value.record_count),
    )?;
    Reflect::set(
        &object,
        &"proximityNodeCount".into(),
        &BigInt::from(value.proximity_node_count as u64),
    )?;
    Reflect::set(
        &object,
        &"externalVectorCount".into(),
        &BigInt::from(value.external_vector_count as u64),
    )?;
    Reflect::set(
        &object,
        &"quantizedNodeCount".into(),
        &BigInt::from(value.quantized_node_count as u64),
    )?;
    Reflect::set(
        &object,
        &"scalarQuantizerCount".into(),
        &BigInt::from(value.scalar_quantizer_count as u64),
    )?;
    Reflect::set(
        &object,
        &"overflowPageCount".into(),
        &BigInt::from(value.overflow_page_count as u64),
    )?;
    Reflect::set(
        &object,
        &"overflowDirectoryCount".into(),
        &BigInt::from(value.overflow_directory_count as u64),
    )?;
    Reflect::set(&object, &"maximumLevel".into(), &value.maximum_level.into())?;
    Reflect::set(
        &object,
        &"maximumNodeBytes".into(),
        &BigInt::from(value.maximum_node_bytes as u64),
    )?;
    Reflect::set(
        &object,
        &"distanceChecks".into(),
        &BigInt::from(value.distance_checks as u64),
    )?;
    Ok(object)
}

fn search_map(
    map: &ProximityMap<Arc<prolly::MemStore>>,
    query: Vec<f32>,
    k: u32,
) -> Result<Object, JsValue> {
    let result = map
        .search(SearchRequest::exact(&query, k as usize))
        .map_err(js_error)?;
    search_result_object(result)
}

fn search_result_object(result: prolly::SearchResult) -> Result<Object, JsValue> {
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
