use super::{js_error, WasmProllyEngine};
use crate::page::set_bytes;
use js_sys::{Array, BigInt, Float32Array, Object, Reflect, Uint8Array};
use prolly::{
    AdaptiveQuality, Cid, ContentGraphLimits, DistanceMetric, HnswSearchOptions, PlannerPolicy,
    PqSearchOptions, ProximityConfig, ProximityFilter, ProximityMap, ProximityMembershipProof,
    ProximityMutation, ProximityRecord, ProximitySearchClaim, ProximitySearchProof,
    ProximityStructuralProof, ProximityVerification, QueryKernel, SearchBackend, SearchBudget,
    SearchCompletion, SearchOptions, SearchPolicy, SearchRequest,
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

enum OwnedProximityFilter {
    All,
    KeyRange {
        start: Option<Vec<u8>>,
        end: Option<Vec<u8>>,
    },
    Prefix(Vec<u8>),
    EligibleKeys(Vec<Vec<u8>>),
}

struct OwnedSearchRequest {
    query: Vec<f32>,
    k: usize,
    policy: SearchPolicy,
    budget: SearchBudget,
    filter: OwnedProximityFilter,
    kernel: QueryKernel,
    options: SearchOptions,
}

impl OwnedSearchRequest {
    fn as_request(&self) -> SearchRequest<'_> {
        let filter = match &self.filter {
            OwnedProximityFilter::All => ProximityFilter::All,
            OwnedProximityFilter::KeyRange { start, end } => ProximityFilter::KeyRange {
                start: start.as_deref(),
                end: end.as_deref(),
            },
            OwnedProximityFilter::Prefix(prefix) => ProximityFilter::Prefix(prefix),
            OwnedProximityFilter::EligibleKeys(keys) => ProximityFilter::EligibleKeys(keys),
        };
        SearchRequest {
            query: &self.query,
            k: self.k,
            policy: self.policy,
            budget: self.budget.clone(),
            filter,
            kernel: self.kernel,
            options: self.options.clone(),
        }
    }
}

fn js_field(value: &JsValue, name: &str) -> Result<JsValue, JsValue> {
    Reflect::get(value, &JsValue::from_str(name))
}

fn required_string(value: &JsValue, name: &str) -> Result<String, JsValue> {
    js_field(value, name)?
        .as_string()
        .ok_or_else(|| JsValue::from_str(&format!("{name} must be a string")))
}

fn optional_field(value: &JsValue, name: &str) -> Result<Option<JsValue>, JsValue> {
    let field = js_field(value, name)?;
    Ok((!field.is_undefined() && !field.is_null()).then_some(field))
}

fn optional_string_usize(value: &JsValue, name: &str) -> Result<Option<usize>, JsValue> {
    optional_field(value, name)?
        .map(|value| {
            value
                .as_string()
                .ok_or_else(|| {
                    JsValue::from_str(&format!("{name} must be an unsigned integer string"))
                })?
                .parse::<usize>()
                .map_err(|error| JsValue::from_str(&format!("invalid {name}: {error}")))
        })
        .transpose()
}

fn optional_u32(value: &JsValue, name: &str) -> Result<Option<u32>, JsValue> {
    optional_field(value, name)?
        .map(|value| {
            let number = value
                .as_f64()
                .ok_or_else(|| JsValue::from_str(&format!("{name} must be a number")))?;
            if !number.is_finite()
                || number.fract() != 0.0
                || number < 0.0
                || number > u32::MAX as f64
            {
                return Err(JsValue::from_str(&format!("{name} must fit u32")));
            }
            Ok(number as u32)
        })
        .transpose()
}

fn optional_u16(value: &JsValue, name: &str) -> Result<Option<u16>, JsValue> {
    optional_u32(value, name)?
        .map(|value| {
            u16::try_from(value).map_err(|_| JsValue::from_str(&format!("{name} must fit u16")))
        })
        .transpose()
}

fn optional_bytes(value: &JsValue, name: &str) -> Result<Option<Vec<u8>>, JsValue> {
    optional_field(value, name)?
        .map(|value| {
            value
                .dyn_into::<Uint8Array>()
                .map(|value| value.to_vec())
                .map_err(|_| JsValue::from_str(&format!("{name} must be Uint8Array")))
        })
        .transpose()
}

fn owned_search_request(value: JsValue) -> Result<OwnedSearchRequest, JsValue> {
    let query = js_field(&value, "query")?
        .dyn_into::<Float32Array>()
        .map_err(|_| JsValue::from_str("query must be Float32Array"))?
        .to_vec();
    let k_value = js_field(&value, "k")?
        .as_f64()
        .ok_or_else(|| JsValue::from_str("k must be a number"))?;
    if !k_value.is_finite()
        || k_value.fract() != 0.0
        || k_value <= 0.0
        || k_value > usize::MAX as f64
    {
        return Err(JsValue::from_str(
            "k must be a positive platform-sized integer",
        ));
    }
    let policy = match required_string(&value, "policy")?.as_str() {
        "exact" => SearchPolicy::Exact,
        "fixed_budget" => SearchPolicy::FixedBudget,
        "adaptive" => {
            let quality = match required_string(&value, "adaptiveQuality")?.as_str() {
                "fast" => AdaptiveQuality::Fast,
                "balanced" => AdaptiveQuality::Balanced,
                "high_recall" => AdaptiveQuality::HighRecall,
                other => {
                    return Err(JsValue::from_str(&format!(
                        "unknown adaptive quality: {other}"
                    )))
                }
            };
            SearchPolicy::Adaptive(quality)
        }
        other => {
            return Err(JsValue::from_str(&format!(
                "unknown search policy: {other}"
            )))
        }
    };
    let budget_value = js_field(&value, "budget")?;
    let budget = SearchBudget {
        max_nodes: optional_string_usize(&budget_value, "maxNodes")?,
        max_committed_bytes: optional_string_usize(&budget_value, "maxCommittedBytes")?,
        max_distance_evaluations: optional_string_usize(&budget_value, "maxDistanceEvaluations")?,
        max_frontier_entries: optional_string_usize(&budget_value, "maxFrontierEntries")?,
    };
    let filter_value = js_field(&value, "filter")?;
    let filter = match required_string(&filter_value, "kind")?.as_str() {
        "all" => OwnedProximityFilter::All,
        "key_range" => OwnedProximityFilter::KeyRange {
            start: optional_bytes(&filter_value, "start")?,
            end: optional_bytes(&filter_value, "rangeEnd")?,
        },
        "prefix" => OwnedProximityFilter::Prefix(
            optional_bytes(&filter_value, "prefix")?
                .ok_or_else(|| JsValue::from_str("prefix filter requires prefix"))?,
        ),
        "eligible_keys" => {
            let array = js_field(&filter_value, "eligibleKeys")?
                .dyn_into::<Array>()
                .map_err(|_| JsValue::from_str("eligibleKeys must be an array"))?;
            let keys = array
                .iter()
                .map(|value| {
                    value
                        .dyn_into::<Uint8Array>()
                        .map(|value| value.to_vec())
                        .map_err(|_| JsValue::from_str("eligible key must be Uint8Array"))
                })
                .collect::<Result<Vec<_>, _>>()?;
            OwnedProximityFilter::EligibleKeys(keys)
        }
        other => {
            return Err(JsValue::from_str(&format!(
                "unknown proximity filter: {other}"
            )))
        }
    };
    let kernel = match required_string(&value, "kernel")?.as_str() {
        "scalar_deterministic" => QueryKernel::ScalarDeterministic,
        "simd_deterministic" => QueryKernel::SimdDeterministic,
        "auto_deterministic" => QueryKernel::AutoDeterministic,
        other => return Err(JsValue::from_str(&format!("unknown query kernel: {other}"))),
    };
    let backend = match required_string(&value, "backend")?.as_str() {
        "native" => SearchBackend::Native,
        "product_quantized" => SearchBackend::ProductQuantized,
        "hnsw" => SearchBackend::Hnsw,
        "composite" => SearchBackend::Composite,
        "auto" => SearchBackend::Auto,
        other => {
            return Err(JsValue::from_str(&format!(
                "unknown search backend: {other}"
            )))
        }
    };
    Ok(OwnedSearchRequest {
        query,
        k: k_value as usize,
        policy,
        budget,
        filter,
        kernel,
        options: SearchOptions {
            backend,
            planner: PlannerPolicy::default(),
            hnsw: HnswSearchOptions {
                ef_search: optional_u32(&value, "hnswEfSearch")?,
            },
            pq: PqSearchOptions {
                rerank_multiplier: optional_u16(&value, "pqRerankMultiplier")?,
            },
        },
    })
}

#[wasm_bindgen(js_class = WasmProximityMap)]
impl WasmProximityMap {
    pub fn search(&self, request: JsValue) -> Result<Object, JsValue> {
        let request = owned_search_request(request)?;
        search_map(&self.load()?, &request)
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
    pub fn prove_search(&self, request: JsValue) -> Result<WasmProximitySearchProof, JsValue> {
        let request = owned_search_request(request)?;
        self.load()?
            .prove_search(request.as_request(), &ContentGraphLimits::default())
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

    pub fn search(&self, request: JsValue) -> Result<Object, JsValue> {
        let request = owned_search_request(request)?;
        search_map(&self.map, &request)
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
    request: &OwnedSearchRequest,
) -> Result<Object, JsValue> {
    let result = map.search(request.as_request()).map_err(js_error)?;
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
    let stats = Object::new();
    Reflect::set(
        &stats,
        &"levelsVisited".into(),
        &BigInt::from(result.stats.levels_visited as u64),
    )?;
    Reflect::set(
        &stats,
        &"nodesRead".into(),
        &BigInt::from(result.stats.nodes_read as u64),
    )?;
    Reflect::set(
        &stats,
        &"bytesRead".into(),
        &BigInt::from(result.stats.bytes_read as u64),
    )?;
    Reflect::set(
        &stats,
        &"physicalBytesRead".into(),
        &BigInt::from(result.stats.physical_bytes_read as u64),
    )?;
    Reflect::set(
        &stats,
        &"committedBytes".into(),
        &BigInt::from(result.stats.committed_bytes as u64),
    )?;
    Reflect::set(
        &stats,
        &"distanceEvaluations".into(),
        &BigInt::from(result.stats.distance_evaluations as u64),
    )?;
    Reflect::set(
        &stats,
        &"quantizedDistanceEvaluations".into(),
        &BigInt::from(result.stats.quantized_distance_evaluations as u64),
    )?;
    Reflect::set(
        &stats,
        &"rerankedCandidates".into(),
        &BigInt::from(result.stats.reranked_candidates as u64),
    )?;
    Reflect::set(
        &stats,
        &"frontierPeak".into(),
        &BigInt::from(result.stats.frontier_peak as u64),
    )?;
    Reflect::set(
        &stats,
        &"candidateHandlesPeak".into(),
        &BigInt::from(result.stats.candidate_handles_peak as u64),
    )?;
    Reflect::set(
        &stats,
        &"candidateRetainedBytesPeak".into(),
        &BigInt::from(result.stats.candidate_retained_bytes_peak as u64),
    )?;
    Reflect::set(&object, &"stats".into(), &stats.into())?;
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
    Reflect::set(
        &object,
        &"planFormatVersion".into(),
        &JsValue::from_f64(result.plan.format_version as f64),
    )?;
    Ok(object)
}
