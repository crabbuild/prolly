use super::{js_error, WasmProllyEngine};
use crate::page::set_bytes;
use js_sys::{Array, BigInt, Float32Array, Function, Object, Reflect, Uint8Array};
use prolly::{
    AcceleratorCatalog, AcceleratorSet, AdaptiveQuality, AsyncAcceleratorCatalog,
    AsyncAcceleratorSet, AsyncCompositeAccelerator, AsyncHnswIndex, AsyncProductQuantizer,
    AsyncProximityMap, AsyncSearchControl, BuildParallelism, CancellationToken,
    CatalogAcceleratorKind, Cid, CompositeAccelerator, CompositeAcceleratorConfig, CompositeBase,
    CompositeBaseKind, CompositeBuildLimits, CompositeBuildOrRebuildOutcome, CompositeBuildOutcome,
    CompositeBuildStats, CompositeRebuildOptions, ContentGraphLimits, DistanceMetric,
    FullRebuildReason, HnswBuildLimits, HnswBuildStats, HnswConfig, HnswIndex,
    HnswRoutingVectorEncoding, HnswSearchOptions, PlannerPolicy, PqSearchOptions,
    ProductQuantizationBuildLimits, ProductQuantizationBuildStats, ProductQuantizationConfig,
    ProductQuantizationQuality, ProductQuantizer, ProximityConfig, ProximityFilter, ProximityMap,
    ProximityMembershipProof, ProximityMutation, ProximityRecord, ProximitySearchClaim,
    ProximitySearchProof, ProximityStructuralProof, ProximityVerification, QueryKernel,
    SearchBackend, SearchBudget, SearchCompletion, SearchIo, SearchOptions, SearchPolicy,
    SearchRequest, SearchRuntime, SearchRuntimePolicy,
};
use std::future::Future;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};
use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;

#[wasm_bindgen(js_name = WasmProximityMap)]
pub struct WasmProximityMap {
    engine: Arc<super::WasmEngine>,
    descriptor: Cid,
}

#[wasm_bindgen(js_name = WasmProximitySearchRuntime)]
pub struct WasmProximitySearchRuntime {
    engine: Arc<super::WasmEngine>,
    policy: SearchRuntimePolicy,
    io: SearchIo<Arc<prolly::MemStore>>,
}

#[wasm_bindgen(js_name = WasmProximityCancellationToken)]
pub struct WasmProximityCancellationToken {
    inner: CancellationToken,
}

#[wasm_bindgen(js_class = WasmProximityCancellationToken)]
impl WasmProximityCancellationToken {
    #[wasm_bindgen(constructor)]
    pub fn new() -> Self {
        Self {
            inner: CancellationToken::default(),
        }
    }

    pub fn cancel(&self) {
        self.inner.cancel();
    }

    #[wasm_bindgen(getter, js_name = isCancelled)]
    pub fn is_cancelled(&self) -> bool {
        self.inner.is_cancelled()
    }
}

struct WasmNoopWake;

impl Wake for WasmNoopWake {
    fn wake(self: Arc<Self>) {}
}

fn block_on_wasm_search<F: Future>(future: F) -> F::Output {
    let waker = Waker::from(Arc::new(WasmNoopWake));
    let mut context = Context::from_waker(&waker);
    let mut future = Box::pin(future);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(value) => return value,
            Poll::Pending => std::hint::spin_loop(),
        }
    }
}

impl WasmProximitySearchRuntime {
    fn new(engine: Arc<super::WasmEngine>, policy: SearchRuntimePolicy) -> Result<Self, JsValue> {
        let runtime = Arc::new(SearchRuntime::new(policy.clone()).map_err(js_error)?);
        let io = SearchIo::new(engine.store().clone(), runtime);
        Ok(Self { engine, policy, io })
    }

    fn ensure_engine(&self, engine: &Arc<super::WasmEngine>) -> Result<(), JsValue> {
        if Arc::ptr_eq(&self.engine, engine) {
            Ok(())
        } else {
            Err(JsValue::from_str(
                "proximity search runtime and search target must belong to the same engine",
            ))
        }
    }
}

#[wasm_bindgen(js_class = WasmProximitySearchRuntime)]
impl WasmProximitySearchRuntime {
    pub fn policy(&self) -> Result<Object, JsValue> {
        search_runtime_policy_object(&self.policy)
    }

    pub fn stats(&self) -> Result<Object, JsValue> {
        let object = Object::new();
        Reflect::set(
            &object,
            &"physicalReads".into(),
            &BigInt::from(self.io.physical_reads() as u64).into(),
        )?;
        Reflect::set(
            &object,
            &"physicalBytesRead".into(),
            &BigInt::from(self.io.physical_bytes_read() as u64).into(),
        )?;
        Ok(object)
    }

    pub fn clear(&self) {
        self.io.runtime().clear();
    }
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

fn cancellable_map_search(
    map: &ProximityMap<Arc<prolly::MemStore>>,
    io: &SearchIo<Arc<prolly::MemStore>>,
    request: &OwnedSearchRequest,
    cancellation: CancellationToken,
) -> Result<Object, JsValue> {
    let before = io.physical_bytes_read();
    let async_io = io
        .clone()
        .with_proximity_dimensions(map.tree().config.dimensions)
        .sync_store_as_async();
    let async_map = block_on_wasm_search(AsyncProximityMap::load(
        async_io,
        map.tree().descriptor.clone(),
    ))
    .map_err(js_error)?;
    let mut result = block_on_wasm_search(async_map.search(
        request.as_request(),
        AsyncSearchControl {
            cancellation: Some(cancellation),
            ..AsyncSearchControl::default()
        },
    ))
    .map_err(js_error)?;
    result.stats.physical_bytes_read = io.physical_bytes_read().saturating_sub(before);
    search_result_object(result)
}

#[derive(Clone, Copy)]
enum WasmAsyncAcceleratorKind {
    Hnsw,
    ProductQuantized,
    Composite,
    Catalog,
}

fn cancellable_accelerated_search(
    map: &ProximityMap<Arc<prolly::MemStore>>,
    io: &SearchIo<Arc<prolly::MemStore>>,
    manifest: Cid,
    kind: WasmAsyncAcceleratorKind,
    request: &OwnedSearchRequest,
    cancellation: CancellationToken,
) -> Result<Object, JsValue> {
    let before = io.physical_bytes_read();
    let store = io
        .clone()
        .with_proximity_dimensions(map.tree().config.dimensions)
        .sync_store_as_async();
    let mut result = block_on_wasm_search(async {
        let async_map =
            AsyncProximityMap::load(store.clone(), map.tree().descriptor.clone()).await?;
        let accelerators = match kind {
            WasmAsyncAcceleratorKind::Hnsw => AsyncAcceleratorSet::empty().with_hnsw(
                async_map.tree(),
                AsyncHnswIndex::load(&store, manifest).await?,
            )?,
            WasmAsyncAcceleratorKind::ProductQuantized => AsyncAcceleratorSet::empty().with_pq(
                async_map.tree(),
                AsyncProductQuantizer::load(&store, manifest).await?,
            )?,
            WasmAsyncAcceleratorKind::Composite => AsyncAcceleratorSet::empty().with_composite(
                async_map.tree(),
                AsyncCompositeAccelerator::load(&store, manifest).await?,
            )?,
            WasmAsyncAcceleratorKind::Catalog => {
                AsyncAcceleratorCatalog::load(&store, manifest, async_map.tree())
                    .await?
                    .into_accelerators()
            }
        };
        async_map
            .search_with_accelerators(
                &accelerators,
                request.as_request(),
                AsyncSearchControl {
                    cancellation: Some(cancellation),
                    ..AsyncSearchControl::default()
                },
            )
            .await
    })
    .map_err(js_error)?;
    result.stats.physical_bytes_read = io.physical_bytes_read().saturating_sub(before);
    search_result_object(result)
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

fn required_string_usize(value: &JsValue, name: &str) -> Result<usize, JsValue> {
    optional_string_usize(value, name)?
        .ok_or_else(|| JsValue::from_str(&format!("{name} is required")))
}

fn required_string_u64(value: &JsValue, name: &str) -> Result<u64, JsValue> {
    js_field(value, name)?
        .as_string()
        .ok_or_else(|| JsValue::from_str(&format!("{name} must be an unsigned integer string")))?
        .parse::<u64>()
        .map_err(|error| JsValue::from_str(&format!("invalid {name}: {error}")))
}

fn search_runtime_policy_from_js(value: &JsValue) -> Result<SearchRuntimePolicy, JsValue> {
    if value.is_null() || value.is_undefined() {
        return Ok(SearchRuntimePolicy::default());
    }
    Ok(SearchRuntimePolicy {
        max_entries: required_string_usize(value, "maxEntries")?,
        max_bytes: required_string_usize(value, "maxBytes")?,
        authoritative_max_bytes: required_string_usize(value, "authoritativeMaxBytes")?,
        hnsw_max_bytes: required_string_usize(value, "hnswMaxBytes")?,
        pq_max_bytes: required_string_usize(value, "pqMaxBytes")?,
    })
}

fn search_runtime_policy_object(policy: &SearchRuntimePolicy) -> Result<Object, JsValue> {
    let object = Object::new();
    Reflect::set(
        &object,
        &"maxEntries".into(),
        &BigInt::from(policy.max_entries as u64).into(),
    )?;
    Reflect::set(
        &object,
        &"maxBytes".into(),
        &BigInt::from(policy.max_bytes as u64).into(),
    )?;
    Reflect::set(
        &object,
        &"authoritativeMaxBytes".into(),
        &BigInt::from(policy.authoritative_max_bytes as u64).into(),
    )?;
    Reflect::set(
        &object,
        &"hnswMaxBytes".into(),
        &BigInt::from(policy.hnsw_max_bytes as u64).into(),
    )?;
    Reflect::set(
        &object,
        &"pqMaxBytes".into(),
        &BigInt::from(policy.pq_max_bytes as u64).into(),
    )?;
    Ok(object)
}

fn required_u32(value: &JsValue, name: &str) -> Result<u32, JsValue> {
    optional_u32(value, name)?.ok_or_else(|| JsValue::from_str(&format!("{name} is required")))
}

fn hnsw_config_from_js(value: &JsValue) -> Result<HnswConfig, JsValue> {
    if value.is_null() || value.is_undefined() {
        return Ok(HnswConfig::default());
    }
    let max_connections = u16::try_from(required_u32(value, "maxConnections")?)
        .map_err(|_| JsValue::from_str("maxConnections must fit u16"))?;
    let level_bits = u8::try_from(required_u32(value, "levelBits")?)
        .map_err(|_| JsValue::from_str("levelBits must fit u8"))?;
    let routing_vector_encoding = match required_string(value, "routingVectorEncoding")?.as_str() {
        "full_f32" => HnswRoutingVectorEncoding::FullF32,
        other => {
            return Err(JsValue::from_str(&format!(
                "unknown HNSW routing-vector encoding: {other}"
            )))
        }
    };
    Ok(HnswConfig {
        max_connections,
        ef_construction: required_u32(value, "efConstruction")?,
        ef_search: required_u32(value, "efSearch")?,
        level_bits,
        overfetch_multiplier: required_u32(value, "overfetchMultiplier")?,
        seed: required_string_u64(value, "seed")?,
        routing_vector_encoding,
    })
}

fn hnsw_build_limits_from_js(value: &JsValue) -> Result<HnswBuildLimits, JsValue> {
    if value.is_null() || value.is_undefined() {
        return Ok(HnswBuildLimits::default());
    }
    Ok(HnswBuildLimits {
        max_records: optional_string_usize(value, "maxRecords")?,
        max_owned_bytes: optional_string_usize(value, "maxOwnedBytes")?,
        max_distance_evaluations: optional_string_usize(value, "maxDistanceEvaluations")?,
        worker_threads: required_string_usize(value, "workerThreads")?,
        max_encoded_graph_bytes: optional_string_usize(value, "maxEncodedGraphBytes")?,
    })
}

fn hnsw_config_object(config: &HnswConfig) -> Result<Object, JsValue> {
    let object = Object::new();
    Reflect::set(
        &object,
        &"maxConnections".into(),
        &JsValue::from_f64(config.max_connections as f64),
    )?;
    Reflect::set(
        &object,
        &"efConstruction".into(),
        &JsValue::from_f64(config.ef_construction as f64),
    )?;
    Reflect::set(
        &object,
        &"efSearch".into(),
        &JsValue::from_f64(config.ef_search as f64),
    )?;
    Reflect::set(
        &object,
        &"levelBits".into(),
        &JsValue::from_f64(config.level_bits as f64),
    )?;
    Reflect::set(
        &object,
        &"overfetchMultiplier".into(),
        &JsValue::from_f64(config.overfetch_multiplier as f64),
    )?;
    Reflect::set(&object, &"seed".into(), &BigInt::from(config.seed))?;
    let encoding = match config.routing_vector_encoding {
        HnswRoutingVectorEncoding::FullF32 => "full_f32",
    };
    Reflect::set(&object, &"routingVectorEncoding".into(), &encoding.into())?;
    Ok(object)
}

fn hnsw_build_stats_object(stats: HnswBuildStats) -> Result<Object, JsValue> {
    let object = Object::new();
    Reflect::set(
        &object,
        &"records".into(),
        &BigInt::from(stats.records as u64),
    )?;
    Reflect::set(
        &object,
        &"distanceEvaluations".into(),
        &BigInt::from(stats.distance_evaluations as u64),
    )?;
    Reflect::set(
        &object,
        &"directedEdges".into(),
        &BigInt::from(stats.directed_edges as u64),
    )?;
    Reflect::set(
        &object,
        &"maximumLevel".into(),
        &JsValue::from_f64(stats.maximum_level as f64),
    )?;
    Reflect::set(
        &object,
        &"ownedBytes".into(),
        &BigInt::from(stats.owned_bytes as u64),
    )?;
    Reflect::set(
        &object,
        &"encodedGraphBytes".into(),
        &BigInt::from(stats.encoded_graph_bytes as u64),
    )?;
    Ok(object)
}

fn pq_config_from_js(value: &JsValue) -> Result<ProductQuantizationConfig, JsValue> {
    if value.is_null() || value.is_undefined() {
        return Ok(ProductQuantizationConfig::default());
    }
    Ok(ProductQuantizationConfig {
        subquantizers: required_u32(value, "subquantizers")?,
        centroids_per_subquantizer: u16::try_from(required_u32(value, "centroidsPerSubquantizer")?)
            .map_err(|_| JsValue::from_str("centroidsPerSubquantizer must fit u16"))?,
        training_iterations: u16::try_from(required_u32(value, "trainingIterations")?)
            .map_err(|_| JsValue::from_str("trainingIterations must fit u16"))?,
        rerank_multiplier: required_u32(value, "rerankMultiplier")?,
        seed: required_string_u64(value, "seed")?,
        max_training_vectors: required_string_usize(value, "maxTrainingVectors")?,
    })
}

fn pq_build_limits_from_js(value: &JsValue) -> Result<ProductQuantizationBuildLimits, JsValue> {
    if value.is_null() || value.is_undefined() {
        return Ok(ProductQuantizationBuildLimits::default());
    }
    Ok(ProductQuantizationBuildLimits {
        max_training_vectors: optional_string_usize(value, "maxTrainingVectors")?,
        max_training_bytes: optional_string_usize(value, "maxTrainingBytes")?,
        max_temporary_code_bytes: optional_string_usize(value, "maxTemporaryCodeBytes")?,
        max_distance_evaluations: optional_string_usize(value, "maxDistanceEvaluations")?,
        max_encoded_output_bytes: optional_string_usize(value, "maxEncodedOutputBytes")?,
        max_worker_threads: optional_string_usize(value, "maxWorkerThreads")?,
    })
}

fn pq_config_object(config: &ProductQuantizationConfig) -> Result<Object, JsValue> {
    let object = Object::new();
    Reflect::set(
        &object,
        &"subquantizers".into(),
        &JsValue::from_f64(config.subquantizers as f64),
    )?;
    Reflect::set(
        &object,
        &"centroidsPerSubquantizer".into(),
        &JsValue::from_f64(config.centroids_per_subquantizer as f64),
    )?;
    Reflect::set(
        &object,
        &"trainingIterations".into(),
        &JsValue::from_f64(config.training_iterations as f64),
    )?;
    Reflect::set(
        &object,
        &"rerankMultiplier".into(),
        &JsValue::from_f64(config.rerank_multiplier as f64),
    )?;
    Reflect::set(&object, &"seed".into(), &BigInt::from(config.seed))?;
    Reflect::set(
        &object,
        &"maxTrainingVectors".into(),
        &BigInt::from(config.max_training_vectors as u64),
    )?;
    Ok(object)
}

fn pq_build_stats_object(stats: ProductQuantizationBuildStats) -> Result<Object, JsValue> {
    let object = Object::new();
    for (name, value) in [
        (
            "trainingDistanceEvaluations",
            stats.training_distance_evaluations,
        ),
        (
            "encodingDistanceEvaluations",
            stats.encoding_distance_evaluations,
        ),
        ("encodedVectors", stats.encoded_vectors),
        ("trainingVectors", stats.training_vectors),
        ("trainingBytes", stats.training_bytes),
        ("encodedOutputBytes", stats.encoded_output_bytes),
    ] {
        Reflect::set(&object, &name.into(), &BigInt::from(value as u64))?;
    }
    Ok(object)
}

fn pq_quality_object(quality: ProductQuantizationQuality) -> Result<Object, JsValue> {
    let object = Object::new();
    Reflect::set(
        &object,
        &"meanSquaredError".into(),
        &JsValue::from_f64(quality.mean_squared_error),
    )?;
    Reflect::set(
        &object,
        &"maximumSquaredError".into(),
        &JsValue::from_f64(quality.maximum_squared_error),
    )?;
    Ok(object)
}

fn composite_config_from_js(value: &JsValue) -> Result<CompositeAcceleratorConfig, JsValue> {
    if value.is_null() || value.is_undefined() {
        return Ok(CompositeAcceleratorConfig::default());
    }
    Ok(CompositeAcceleratorConfig {
        max_delta_records: required_string_usize(value, "maxDeltaRecords")?,
        max_shadow_records: required_string_usize(value, "maxShadowRecords")?,
        max_delta_ratio_ppm: required_u32(value, "maxDeltaRatioPpm")?,
        max_shadow_ratio_ppm: required_u32(value, "maxShadowRatioPpm")?,
        base_overfetch_multiplier: required_u32(value, "baseOverfetchMultiplier")?,
    })
}

fn composite_config_object(config: &CompositeAcceleratorConfig) -> Result<Object, JsValue> {
    let object = Object::new();
    Reflect::set(
        &object,
        &"maxDeltaRecords".into(),
        &BigInt::from(config.max_delta_records as u64),
    )?;
    Reflect::set(
        &object,
        &"maxShadowRecords".into(),
        &BigInt::from(config.max_shadow_records as u64),
    )?;
    Reflect::set(
        &object,
        &"maxDeltaRatioPpm".into(),
        &JsValue::from_f64(config.max_delta_ratio_ppm as f64),
    )?;
    Reflect::set(
        &object,
        &"maxShadowRatioPpm".into(),
        &JsValue::from_f64(config.max_shadow_ratio_ppm as f64),
    )?;
    Reflect::set(
        &object,
        &"baseOverfetchMultiplier".into(),
        &JsValue::from_f64(config.base_overfetch_multiplier as f64),
    )?;
    Ok(object)
}

fn composite_limits_from_js(value: &JsValue) -> Result<CompositeBuildLimits, JsValue> {
    if value.is_null() || value.is_undefined() {
        return Ok(CompositeBuildLimits::default());
    }
    Ok(CompositeBuildLimits {
        max_diff_entries: optional_string_usize(value, "maxDiffEntries")?,
        max_owned_bytes: optional_string_usize(value, "maxOwnedBytes")?,
        max_encoded_output_bytes: optional_string_usize(value, "maxEncodedOutputBytes")?,
        max_distance_evaluations: optional_string_usize(value, "maxDistanceEvaluations")?,
    })
}

fn composite_stats_object(stats: &CompositeBuildStats) -> Result<Object, JsValue> {
    let object = Object::new();
    for (name, value) in [
        ("diffEntries", stats.diff_entries),
        ("insertedRecords", stats.inserted_records),
        ("vectorUpdatedRecords", stats.vector_updated_records),
        ("valueOnlyRecords", stats.value_only_records),
        ("deletedRecords", stats.deleted_records),
        ("deltaRecords", stats.delta_records),
        ("shadowRecords", stats.shadow_records),
        ("ownedBytesPeak", stats.owned_bytes_peak),
        ("encodedOutputBytes", stats.encoded_output_bytes),
        ("distanceEvaluations", stats.distance_evaluations),
    ] {
        Reflect::set(&object, &name.into(), &BigInt::from(value as u64))?;
    }
    Ok(object)
}

fn rebuild_reason_object(reason: &FullRebuildReason) -> Result<Object, JsValue> {
    let object = Object::new();
    let (kind, actual, maximum) = match reason {
        FullRebuildReason::DeltaRecords { actual, maximum } => {
            ("delta_records", *actual as u64, *maximum as u64)
        }
        FullRebuildReason::ShadowRecords { actual, maximum } => {
            ("shadow_records", *actual as u64, *maximum as u64)
        }
        FullRebuildReason::DeltaRatio {
            actual_ppm,
            maximum_ppm,
        } => (
            "delta_ratio",
            u64::from(*actual_ppm),
            u64::from(*maximum_ppm),
        ),
        FullRebuildReason::ShadowRatio {
            actual_ppm,
            maximum_ppm,
        } => (
            "shadow_ratio",
            u64::from(*actual_ppm),
            u64::from(*maximum_ppm),
        ),
    };
    Reflect::set(&object, &"kind".into(), &kind.into())?;
    Reflect::set(&object, &"actual".into(), &BigInt::from(actual))?;
    Reflect::set(&object, &"maximum".into(), &BigInt::from(maximum))?;
    Ok(object)
}

fn rebuild_reasons_array(reasons: &[FullRebuildReason]) -> Result<Array, JsValue> {
    let array = Array::new();
    for reason in reasons {
        array.push(&rebuild_reason_object(reason)?.into());
    }
    Ok(array)
}

fn composite_rebuild_options_from_js(value: &JsValue) -> Result<CompositeRebuildOptions, JsValue> {
    if value.is_null() || value.is_undefined() {
        return Ok(CompositeRebuildOptions::default());
    }
    let hnsw = optional_field(value, "hnswLimits")?.unwrap_or(JsValue::UNDEFINED);
    let pq_limits = optional_field(value, "pqLimits")?.unwrap_or(JsValue::UNDEFINED);
    let threads = optional_field(value, "pqWorkerThreads")?
        .map(|value| {
            value
                .as_string()
                .ok_or_else(|| {
                    JsValue::from_str("pqWorkerThreads must be an unsigned integer string")
                })?
                .parse::<usize>()
                .map_err(|error| JsValue::from_str(&format!("invalid pqWorkerThreads: {error}")))
        })
        .transpose()?
        .unwrap_or(1);
    if threads != 1 {
        return Err(JsValue::from_str(
            "browser-safe WASM composite PQ rebuild requires pqWorkerThreads = 1",
        ));
    }
    Ok(CompositeRebuildOptions {
        hnsw_limits: hnsw_build_limits_from_js(&hnsw)?,
        pq_parallelism: BuildParallelism::serial(),
        pq_limits: pq_build_limits_from_js(&pq_limits)?,
    })
}

fn composite_build_outcome_object(
    engine: Arc<super::WasmEngine>,
    outcome: CompositeBuildOutcome<Arc<prolly::MemStore>>,
) -> Result<Object, JsValue> {
    let object = Object::new();
    match outcome {
        CompositeBuildOutcome::Composite { accelerator, stats } => {
            Reflect::set(
                &object,
                &"accelerator".into(),
                &JsValue::from(WasmCompositeAccelerator {
                    engine,
                    inner: *accelerator,
                }),
            )?;
            Reflect::set(&object, &"reasons".into(), &Array::new().into())?;
            Reflect::set(
                &object,
                &"stats".into(),
                &composite_stats_object(&stats)?.into(),
            )?;
        }
        CompositeBuildOutcome::FullRebuildRequired { reasons, stats } => {
            Reflect::set(
                &object,
                &"reasons".into(),
                &rebuild_reasons_array(&reasons)?.into(),
            )?;
            Reflect::set(
                &object,
                &"stats".into(),
                &composite_stats_object(&stats)?.into(),
            )?;
        }
    }
    Ok(object)
}

fn composite_rebuild_outcome_object(
    engine: Arc<super::WasmEngine>,
    outcome: CompositeBuildOrRebuildOutcome<Arc<prolly::MemStore>>,
) -> Result<Object, JsValue> {
    let object = Object::new();
    match outcome {
        CompositeBuildOrRebuildOutcome::Composite { accelerator, stats } => {
            Reflect::set(&object, &"kind".into(), &"composite".into())?;
            Reflect::set(
                &object,
                &"composite".into(),
                &JsValue::from(WasmCompositeAccelerator {
                    engine,
                    inner: *accelerator,
                }),
            )?;
            Reflect::set(&object, &"reasons".into(), &Array::new().into())?;
            Reflect::set(
                &object,
                &"compositeStats".into(),
                &composite_stats_object(&stats)?.into(),
            )?;
        }
        CompositeBuildOrRebuildOutcome::NoAcceleratorRequired {
            reasons,
            composite_stats,
        } => {
            Reflect::set(&object, &"kind".into(), &"no_accelerator_required".into())?;
            Reflect::set(
                &object,
                &"reasons".into(),
                &rebuild_reasons_array(&reasons)?.into(),
            )?;
            Reflect::set(
                &object,
                &"compositeStats".into(),
                &composite_stats_object(&composite_stats)?.into(),
            )?;
        }
        CompositeBuildOrRebuildOutcome::HnswRebuilt {
            accelerator,
            reasons,
            composite_stats,
            rebuild_stats,
        } => {
            Reflect::set(&object, &"kind".into(), &"hnsw_rebuilt".into())?;
            Reflect::set(
                &object,
                &"hnsw".into(),
                &JsValue::from(WasmHnswIndex {
                    engine,
                    inner: *accelerator,
                }),
            )?;
            Reflect::set(
                &object,
                &"reasons".into(),
                &rebuild_reasons_array(&reasons)?.into(),
            )?;
            Reflect::set(
                &object,
                &"compositeStats".into(),
                &composite_stats_object(&composite_stats)?.into(),
            )?;
            Reflect::set(
                &object,
                &"hnswStats".into(),
                &hnsw_build_stats_object(rebuild_stats)?.into(),
            )?;
        }
        CompositeBuildOrRebuildOutcome::ProductQuantizedRebuilt {
            accelerator,
            reasons,
            composite_stats,
            rebuild_stats,
        } => {
            Reflect::set(&object, &"kind".into(), &"product_quantized_rebuilt".into())?;
            Reflect::set(
                &object,
                &"pq".into(),
                &JsValue::from(WasmProductQuantizer {
                    engine,
                    inner: *accelerator,
                }),
            )?;
            Reflect::set(
                &object,
                &"reasons".into(),
                &rebuild_reasons_array(&reasons)?.into(),
            )?;
            Reflect::set(
                &object,
                &"compositeStats".into(),
                &composite_stats_object(&composite_stats)?.into(),
            )?;
            Reflect::set(
                &object,
                &"pqStats".into(),
                &pq_build_stats_object(rebuild_stats)?.into(),
            )?;
        }
    }
    Ok(object)
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

    #[wasm_bindgen(js_name = cancellationToken)]
    pub fn cancellation_token(&self) -> WasmProximityCancellationToken {
        WasmProximityCancellationToken::new()
    }

    #[wasm_bindgen(js_name = searchCancellable)]
    pub fn search_cancellable(
        &self,
        request: JsValue,
        cancellation: &WasmProximityCancellationToken,
    ) -> Result<Object, JsValue> {
        let request = owned_search_request(request)?;
        let map = self.load()?;
        let io = SearchIo::new(
            self.engine.store().clone(),
            Arc::new(SearchRuntime::default()),
        );
        cancellable_map_search(&map, &io, &request, cancellation.inner.clone())
    }

    #[wasm_bindgen(js_name = searchWithRuntimeCancellable)]
    pub fn search_with_runtime_cancellable(
        &self,
        request: JsValue,
        runtime: &WasmProximitySearchRuntime,
        cancellation: &WasmProximityCancellationToken,
    ) -> Result<Object, JsValue> {
        runtime.ensure_engine(&self.engine)?;
        let request = owned_search_request(request)?;
        cancellable_map_search(
            &self.load()?,
            &runtime.io,
            &request,
            cancellation.inner.clone(),
        )
    }

    #[wasm_bindgen(js_name = searchWithRuntime)]
    pub fn search_with_runtime(
        &self,
        request: JsValue,
        runtime: &WasmProximitySearchRuntime,
    ) -> Result<Object, JsValue> {
        runtime.ensure_engine(&self.engine)?;
        let request = owned_search_request(request)?;
        self.load()?
            .search_with(&AcceleratorSet::empty(), &runtime.io, request.as_request())
            .map_err(js_error)
            .and_then(search_result_object)
    }

    pub fn read(&self) -> Result<WasmProximityReadSession, JsValue> {
        Ok(WasmProximityReadSession {
            engine: Arc::clone(&self.engine),
            map: self.load()?,
        })
    }

    pub fn descriptor(&self) -> Vec<u8> {
        self.descriptor.as_bytes().to_vec()
    }
    #[wasm_bindgen(js_name = clearContentCache)]
    pub fn clear_content_cache(&self) -> Result<(), JsValue> {
        self.load()?.clear_content_cache().map_err(js_error)
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
    #[wasm_bindgen(js_name = scanRecords)]
    pub fn scan_records(&self, visitor: &Function) -> Result<String, JsValue> {
        scan_proximity_records(&self.load()?, visitor)
    }
    #[wasm_bindgen(js_name = withSearchView)]
    pub fn with_search_view(
        &self,
        query: Float32Array,
        k: u32,
        visitor: &Function,
    ) -> Result<JsValue, JsValue> {
        with_proximity_search_view(&self.load()?, query, k, visitor)
    }
    #[wasm_bindgen(js_name = buildHnsw)]
    pub fn build_hnsw(&self, config: JsValue, limits: JsValue) -> Result<Object, JsValue> {
        let config = hnsw_config_from_js(&config)?;
        let limits = hnsw_build_limits_from_js(&limits)?;
        let map = self.load()?;
        let (index, stats) =
            HnswIndex::build_with_limits(&map, config, limits).map_err(js_error)?;
        let result = Object::new();
        Reflect::set(
            &result,
            &"index".into(),
            &JsValue::from(WasmHnswIndex {
                engine: Arc::clone(&self.engine),
                inner: index,
            }),
        )?;
        Reflect::set(
            &result,
            &"stats".into(),
            &hnsw_build_stats_object(stats)?.into(),
        )?;
        Ok(result)
    }
    #[wasm_bindgen(js_name = loadHnsw)]
    pub fn load_hnsw(&self, manifest: Uint8Array) -> Result<WasmHnswIndex, JsValue> {
        let raw: [u8; 32] = manifest
            .to_vec()
            .try_into()
            .map_err(|_| JsValue::from_str("HNSW manifest CID must be 32 bytes"))?;
        let index = HnswIndex::load(self.engine.store().clone(), Cid(raw)).map_err(js_error)?;
        if index.source_descriptor() != &self.descriptor {
            return Err(JsValue::from_str(
                "HNSW index is bound to a different source descriptor",
            ));
        }
        Ok(WasmHnswIndex {
            engine: Arc::clone(&self.engine),
            inner: index,
        })
    }
    #[wasm_bindgen(js_name = buildPq)]
    pub fn build_pq(
        &self,
        config: JsValue,
        worker_threads: String,
        limits: JsValue,
    ) -> Result<Object, JsValue> {
        let config = pq_config_from_js(&config)?;
        let worker_threads = worker_threads
            .parse::<usize>()
            .map_err(|error| JsValue::from_str(&format!("invalid workerThreads: {error}")))?;
        if worker_threads != 1 {
            return Err(JsValue::from_str(
                "browser-safe WASM product quantization requires workerThreads = 1",
            ));
        }
        let parallelism = BuildParallelism::serial();
        let limits = pq_build_limits_from_js(&limits)?;
        let map = self.load()?;
        let (index, stats) = ProductQuantizer::build_with_limits(&map, config, parallelism, limits)
            .map_err(js_error)?;
        let result = Object::new();
        Reflect::set(
            &result,
            &"index".into(),
            &JsValue::from(WasmProductQuantizer {
                engine: Arc::clone(&self.engine),
                inner: index,
            }),
        )?;
        Reflect::set(
            &result,
            &"stats".into(),
            &pq_build_stats_object(stats)?.into(),
        )?;
        Ok(result)
    }
    #[wasm_bindgen(js_name = loadPq)]
    pub fn load_pq(&self, manifest: Uint8Array) -> Result<WasmProductQuantizer, JsValue> {
        let raw: [u8; 32] = manifest
            .to_vec()
            .try_into()
            .map_err(|_| JsValue::from_str("PQ manifest CID must be 32 bytes"))?;
        let index =
            ProductQuantizer::load(self.engine.store().clone(), Cid(raw)).map_err(js_error)?;
        if index.source_descriptor() != &self.descriptor {
            return Err(JsValue::from_str(
                "product quantizer is bound to a different source descriptor",
            ));
        }
        Ok(WasmProductQuantizer {
            engine: Arc::clone(&self.engine),
            inner: index,
        })
    }
    #[wasm_bindgen(js_name = buildCompositeHnsw)]
    pub fn build_composite_hnsw(
        &self,
        base_map: &WasmProximityMap,
        base: &WasmHnswIndex,
        config: JsValue,
        limits: JsValue,
    ) -> Result<Object, JsValue> {
        let current = self.load()?;
        let base_map = base_map.load()?;
        let base = HnswIndex::load(
            self.engine.store().clone(),
            base.inner.manifest_cid().clone(),
        )
        .map_err(js_error)?;
        let outcome = CompositeAccelerator::build(
            &base_map,
            &current,
            CompositeBase::Hnsw(base),
            composite_config_from_js(&config)?,
            composite_limits_from_js(&limits)?,
        )
        .map_err(js_error)?;
        composite_build_outcome_object(Arc::clone(&self.engine), outcome)
    }
    #[wasm_bindgen(js_name = buildCompositePq)]
    pub fn build_composite_pq(
        &self,
        base_map: &WasmProximityMap,
        base: &WasmProductQuantizer,
        config: JsValue,
        limits: JsValue,
    ) -> Result<Object, JsValue> {
        let current = self.load()?;
        let base_map = base_map.load()?;
        let base = ProductQuantizer::load(
            self.engine.store().clone(),
            base.inner.manifest_cid().clone(),
        )
        .map_err(js_error)?;
        let outcome = CompositeAccelerator::build(
            &base_map,
            &current,
            CompositeBase::ProductQuantized(base),
            composite_config_from_js(&config)?,
            composite_limits_from_js(&limits)?,
        )
        .map_err(js_error)?;
        composite_build_outcome_object(Arc::clone(&self.engine), outcome)
    }
    #[wasm_bindgen(js_name = buildOrRebuildCompositeHnsw)]
    pub fn build_or_rebuild_composite_hnsw(
        &self,
        base_map: &WasmProximityMap,
        base: &WasmHnswIndex,
        config: JsValue,
        limits: JsValue,
        rebuild: JsValue,
    ) -> Result<Object, JsValue> {
        let current = self.load()?;
        let base_map = base_map.load()?;
        let base = HnswIndex::load(
            self.engine.store().clone(),
            base.inner.manifest_cid().clone(),
        )
        .map_err(js_error)?;
        let outcome = CompositeAccelerator::build_or_rebuild(
            &base_map,
            &current,
            CompositeBase::Hnsw(base),
            composite_config_from_js(&config)?,
            composite_limits_from_js(&limits)?,
            composite_rebuild_options_from_js(&rebuild)?,
        )
        .map_err(js_error)?;
        composite_rebuild_outcome_object(Arc::clone(&self.engine), outcome)
    }
    #[wasm_bindgen(js_name = buildOrRebuildCompositePq)]
    pub fn build_or_rebuild_composite_pq(
        &self,
        base_map: &WasmProximityMap,
        base: &WasmProductQuantizer,
        config: JsValue,
        limits: JsValue,
        rebuild: JsValue,
    ) -> Result<Object, JsValue> {
        let current = self.load()?;
        let base_map = base_map.load()?;
        let base = ProductQuantizer::load(
            self.engine.store().clone(),
            base.inner.manifest_cid().clone(),
        )
        .map_err(js_error)?;
        let outcome = CompositeAccelerator::build_or_rebuild(
            &base_map,
            &current,
            CompositeBase::ProductQuantized(base),
            composite_config_from_js(&config)?,
            composite_limits_from_js(&limits)?,
            composite_rebuild_options_from_js(&rebuild)?,
        )
        .map_err(js_error)?;
        composite_rebuild_outcome_object(Arc::clone(&self.engine), outcome)
    }
    #[wasm_bindgen(js_name = loadComposite)]
    pub fn load_composite(
        &self,
        manifest: Uint8Array,
    ) -> Result<WasmCompositeAccelerator, JsValue> {
        let raw: [u8; 32] = manifest
            .to_vec()
            .try_into()
            .map_err(|_| JsValue::from_str("composite manifest CID must be 32 bytes"))?;
        let inner =
            CompositeAccelerator::load(self.engine.store().clone(), Cid(raw)).map_err(js_error)?;
        if inner.current_source_descriptor() != &self.descriptor {
            return Err(JsValue::from_str(
                "composite accelerator is bound to a different source descriptor",
            ));
        }
        Ok(WasmCompositeAccelerator {
            engine: Arc::clone(&self.engine),
            inner,
        })
    }
    #[wasm_bindgen(js_name = buildAcceleratorCatalog)]
    pub fn build_accelerator_catalog(
        &self,
        hnsw: Option<Uint8Array>,
        pq: Option<Uint8Array>,
        composite: Option<Uint8Array>,
    ) -> Result<WasmAcceleratorCatalog, JsValue> {
        let map = self.load()?;
        let mut set = AcceleratorSet::empty();
        if let Some(value) = hnsw {
            set = set
                .with_hnsw(
                    map.tree(),
                    HnswIndex::load(
                        self.engine.store().clone(),
                        Cid(value.to_vec().try_into().map_err(|_| {
                            JsValue::from_str("HNSW manifest CID must be 32 bytes")
                        })?),
                    )
                    .map_err(js_error)?,
                )
                .map_err(js_error)?;
        }
        if let Some(value) = pq {
            set = set
                .with_pq(
                    map.tree(),
                    ProductQuantizer::load(
                        self.engine.store().clone(),
                        Cid(value
                            .to_vec()
                            .try_into()
                            .map_err(|_| JsValue::from_str("PQ manifest CID must be 32 bytes"))?),
                    )
                    .map_err(js_error)?,
                )
                .map_err(js_error)?;
        }
        if let Some(value) = composite {
            set = set
                .with_composite(
                    map.tree(),
                    CompositeAccelerator::load(
                        self.engine.store().clone(),
                        Cid(value.to_vec().try_into().map_err(|_| {
                            JsValue::from_str("composite manifest CID must be 32 bytes")
                        })?),
                    )
                    .map_err(js_error)?,
                )
                .map_err(js_error)?;
        }
        let inner = AcceleratorCatalog::build(self.engine.store().clone(), map.tree(), set)
            .map_err(js_error)?;
        Ok(WasmAcceleratorCatalog {
            engine: Arc::clone(&self.engine),
            inner,
        })
    }
    #[wasm_bindgen(js_name = loadAcceleratorCatalog)]
    pub fn load_accelerator_catalog(
        &self,
        manifest: Uint8Array,
    ) -> Result<WasmAcceleratorCatalog, JsValue> {
        let raw: [u8; 32] = manifest
            .to_vec()
            .try_into()
            .map_err(|_| JsValue::from_str("accelerator catalog manifest CID must be 32 bytes"))?;
        let map = self.load()?;
        let inner = AcceleratorCatalog::load(self.engine.store().clone(), Cid(raw), map.tree())
            .map_err(js_error)?;
        Ok(WasmAcceleratorCatalog {
            engine: Arc::clone(&self.engine),
            inner,
        })
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

#[wasm_bindgen(js_name = WasmHnswIndex)]
pub struct WasmHnswIndex {
    engine: Arc<super::WasmEngine>,
    inner: HnswIndex<Arc<prolly::MemStore>>,
}

#[wasm_bindgen(js_class = WasmHnswIndex)]
impl WasmHnswIndex {
    pub fn manifest(&self) -> Vec<u8> {
        self.inner.manifest_cid().as_bytes().to_vec()
    }

    #[wasm_bindgen(js_name = sourceDescriptor)]
    pub fn source_descriptor(&self) -> Vec<u8> {
        self.inner.source_descriptor().as_bytes().to_vec()
    }

    pub fn config(&self) -> Result<Object, JsValue> {
        hnsw_config_object(self.inner.config())
    }

    #[wasm_bindgen(js_name = isCanonical)]
    pub fn is_canonical(&self) -> bool {
        self.inner.is_canonical()
    }

    pub fn search(&self, map: &WasmProximityMap, request: JsValue) -> Result<Object, JsValue> {
        let request = owned_search_request(request)?;
        let map = map.load()?;
        self.inner
            .search(&map, request.as_request())
            .map_err(js_error)
            .and_then(search_result_object)
    }

    #[wasm_bindgen(js_name = searchWithRuntime)]
    pub fn search_with_runtime(
        &self,
        map: &WasmProximityMap,
        request: JsValue,
        runtime: &WasmProximitySearchRuntime,
    ) -> Result<Object, JsValue> {
        runtime.ensure_engine(&self.engine)?;
        runtime.ensure_engine(&map.engine)?;
        let request = owned_search_request(request)?;
        let map = map.load()?;
        let index = HnswIndex::load(
            runtime.io.store().clone(),
            self.inner.manifest_cid().clone(),
        )
        .map_err(js_error)?;
        let accelerators = AcceleratorSet::empty()
            .with_hnsw(map.tree(), index)
            .map_err(js_error)?;
        map.search_with(&accelerators, &runtime.io, request.as_request())
            .map_err(js_error)
            .and_then(search_result_object)
    }

    #[wasm_bindgen(js_name = searchCancellable)]
    pub fn search_cancellable(
        &self,
        map: &WasmProximityMap,
        request: JsValue,
        cancellation: &WasmProximityCancellationToken,
    ) -> Result<Object, JsValue> {
        let io = SearchIo::new(
            self.engine.store().clone(),
            Arc::new(SearchRuntime::default()),
        );
        cancellable_accelerated_search(
            &map.load()?,
            &io,
            self.inner.manifest_cid().clone(),
            WasmAsyncAcceleratorKind::Hnsw,
            &owned_search_request(request)?,
            cancellation.inner.clone(),
        )
    }

    #[wasm_bindgen(js_name = searchWithRuntimeCancellable)]
    pub fn search_with_runtime_cancellable(
        &self,
        map: &WasmProximityMap,
        request: JsValue,
        runtime: &WasmProximitySearchRuntime,
        cancellation: &WasmProximityCancellationToken,
    ) -> Result<Object, JsValue> {
        runtime.ensure_engine(&self.engine)?;
        runtime.ensure_engine(&map.engine)?;
        cancellable_accelerated_search(
            &map.load()?,
            &runtime.io,
            self.inner.manifest_cid().clone(),
            WasmAsyncAcceleratorKind::Hnsw,
            &owned_search_request(request)?,
            cancellation.inner.clone(),
        )
    }

    #[wasm_bindgen(js_name = proveSearch)]
    pub fn prove_search(
        &self,
        map: &WasmProximityMap,
        request: JsValue,
    ) -> Result<WasmProximitySearchProof, JsValue> {
        let request = owned_search_request(request)?;
        let map = map.load()?;
        self.inner
            .prove_search(&map, request.as_request(), &ContentGraphLimits::default())
            .map(|inner| WasmProximitySearchProof { inner })
            .map_err(js_error)
    }
}

#[wasm_bindgen(js_name = WasmProductQuantizer)]
pub struct WasmProductQuantizer {
    engine: Arc<super::WasmEngine>,
    inner: ProductQuantizer<Arc<prolly::MemStore>>,
}

#[wasm_bindgen(js_class = WasmProductQuantizer)]
impl WasmProductQuantizer {
    pub fn manifest(&self) -> Vec<u8> {
        self.inner.manifest_cid().as_bytes().to_vec()
    }

    #[wasm_bindgen(js_name = sourceDescriptor)]
    pub fn source_descriptor(&self) -> Vec<u8> {
        self.inner.source_descriptor().as_bytes().to_vec()
    }

    pub fn config(&self) -> Result<Object, JsValue> {
        pq_config_object(self.inner.config())
    }

    pub fn quality(&self) -> Result<Object, JsValue> {
        pq_quality_object(self.inner.quality())
    }

    pub fn search(&self, map: &WasmProximityMap, request: JsValue) -> Result<Object, JsValue> {
        let request = owned_search_request(request)?;
        let map = map.load()?;
        self.inner
            .search(&map, request.as_request())
            .map_err(js_error)
            .and_then(search_result_object)
    }

    #[wasm_bindgen(js_name = searchWithRuntime)]
    pub fn search_with_runtime(
        &self,
        map: &WasmProximityMap,
        request: JsValue,
        runtime: &WasmProximitySearchRuntime,
    ) -> Result<Object, JsValue> {
        runtime.ensure_engine(&self.engine)?;
        runtime.ensure_engine(&map.engine)?;
        let request = owned_search_request(request)?;
        let map = map.load()?;
        let index = ProductQuantizer::load(
            runtime.io.store().clone(),
            self.inner.manifest_cid().clone(),
        )
        .map_err(js_error)?;
        let accelerators = AcceleratorSet::empty()
            .with_pq(map.tree(), index)
            .map_err(js_error)?;
        map.search_with(&accelerators, &runtime.io, request.as_request())
            .map_err(js_error)
            .and_then(search_result_object)
    }

    #[wasm_bindgen(js_name = searchCancellable)]
    pub fn search_cancellable(
        &self,
        map: &WasmProximityMap,
        request: JsValue,
        cancellation: &WasmProximityCancellationToken,
    ) -> Result<Object, JsValue> {
        let io = SearchIo::new(
            self.engine.store().clone(),
            Arc::new(SearchRuntime::default()),
        );
        cancellable_accelerated_search(
            &map.load()?,
            &io,
            self.inner.manifest_cid().clone(),
            WasmAsyncAcceleratorKind::ProductQuantized,
            &owned_search_request(request)?,
            cancellation.inner.clone(),
        )
    }

    #[wasm_bindgen(js_name = searchWithRuntimeCancellable)]
    pub fn search_with_runtime_cancellable(
        &self,
        map: &WasmProximityMap,
        request: JsValue,
        runtime: &WasmProximitySearchRuntime,
        cancellation: &WasmProximityCancellationToken,
    ) -> Result<Object, JsValue> {
        runtime.ensure_engine(&self.engine)?;
        runtime.ensure_engine(&map.engine)?;
        cancellable_accelerated_search(
            &map.load()?,
            &runtime.io,
            self.inner.manifest_cid().clone(),
            WasmAsyncAcceleratorKind::ProductQuantized,
            &owned_search_request(request)?,
            cancellation.inner.clone(),
        )
    }

    #[wasm_bindgen(js_name = proveSearch)]
    pub fn prove_search(
        &self,
        map: &WasmProximityMap,
        request: JsValue,
    ) -> Result<WasmProximitySearchProof, JsValue> {
        let request = owned_search_request(request)?;
        let map = map.load()?;
        self.inner
            .prove_search(&map, request.as_request(), &ContentGraphLimits::default())
            .map(|inner| WasmProximitySearchProof { inner })
            .map_err(js_error)
    }
}

#[wasm_bindgen(js_name = WasmCompositeAccelerator)]
pub struct WasmCompositeAccelerator {
    engine: Arc<super::WasmEngine>,
    inner: CompositeAccelerator<Arc<prolly::MemStore>>,
}

#[wasm_bindgen(js_class = WasmCompositeAccelerator)]
impl WasmCompositeAccelerator {
    pub fn manifest(&self) -> Vec<u8> {
        self.inner.manifest_cid().as_bytes().to_vec()
    }
    #[wasm_bindgen(js_name = currentSourceDescriptor)]
    pub fn current_source_descriptor(&self) -> Vec<u8> {
        self.inner.current_source_descriptor().as_bytes().to_vec()
    }
    #[wasm_bindgen(js_name = baseSourceDescriptor)]
    pub fn base_source_descriptor(&self) -> Vec<u8> {
        self.inner.base_source_descriptor().as_bytes().to_vec()
    }
    #[wasm_bindgen(js_name = baseKind)]
    pub fn base_kind(&self) -> String {
        match self.inner.base_kind() {
            CompositeBaseKind::Hnsw => "hnsw",
            CompositeBaseKind::ProductQuantized => "product_quantized",
        }
        .to_string()
    }
    #[wasm_bindgen(js_name = deltaCount)]
    pub fn delta_count(&self) -> String {
        self.inner.delta_count().to_string()
    }
    #[wasm_bindgen(js_name = shadowCount)]
    pub fn shadow_count(&self) -> String {
        self.inner.shadow_count().to_string()
    }
    pub fn config(&self) -> Result<Object, JsValue> {
        composite_config_object(self.inner.config())
    }
    #[wasm_bindgen(js_name = buildStats)]
    pub fn build_stats(&self) -> Result<Object, JsValue> {
        composite_stats_object(self.inner.build_stats())
    }
    pub fn search(&self, map: &WasmProximityMap, request: JsValue) -> Result<Object, JsValue> {
        let request = owned_search_request(request)?;
        let map_value = map.load()?;
        let composite = CompositeAccelerator::load(
            self.engine.store().clone(),
            self.inner.manifest_cid().clone(),
        )
        .map_err(js_error)?;
        let accelerators = AcceleratorSet::empty()
            .with_composite(map_value.tree(), composite)
            .map_err(js_error)?;
        let io = SearchIo::new(
            self.engine.store().clone(),
            Arc::new(SearchRuntime::default()),
        );
        map_value
            .search_with(&accelerators, &io, request.as_request())
            .map_err(js_error)
            .and_then(search_result_object)
    }
    #[wasm_bindgen(js_name = searchWithRuntime)]
    pub fn search_with_runtime(
        &self,
        map: &WasmProximityMap,
        request: JsValue,
        runtime: &WasmProximitySearchRuntime,
    ) -> Result<Object, JsValue> {
        runtime.ensure_engine(&self.engine)?;
        runtime.ensure_engine(&map.engine)?;
        let request = owned_search_request(request)?;
        let map_value = map.load()?;
        let composite = CompositeAccelerator::load(
            runtime.io.store().clone(),
            self.inner.manifest_cid().clone(),
        )
        .map_err(js_error)?;
        let accelerators = AcceleratorSet::empty()
            .with_composite(map_value.tree(), composite)
            .map_err(js_error)?;
        map_value
            .search_with(&accelerators, &runtime.io, request.as_request())
            .map_err(js_error)
            .and_then(search_result_object)
    }
    #[wasm_bindgen(js_name = searchCancellable)]
    pub fn search_cancellable(
        &self,
        map: &WasmProximityMap,
        request: JsValue,
        cancellation: &WasmProximityCancellationToken,
    ) -> Result<Object, JsValue> {
        let io = SearchIo::new(
            self.engine.store().clone(),
            Arc::new(SearchRuntime::default()),
        );
        cancellable_accelerated_search(
            &map.load()?,
            &io,
            self.inner.manifest_cid().clone(),
            WasmAsyncAcceleratorKind::Composite,
            &owned_search_request(request)?,
            cancellation.inner.clone(),
        )
    }
    #[wasm_bindgen(js_name = searchWithRuntimeCancellable)]
    pub fn search_with_runtime_cancellable(
        &self,
        map: &WasmProximityMap,
        request: JsValue,
        runtime: &WasmProximitySearchRuntime,
        cancellation: &WasmProximityCancellationToken,
    ) -> Result<Object, JsValue> {
        runtime.ensure_engine(&self.engine)?;
        runtime.ensure_engine(&map.engine)?;
        cancellable_accelerated_search(
            &map.load()?,
            &runtime.io,
            self.inner.manifest_cid().clone(),
            WasmAsyncAcceleratorKind::Composite,
            &owned_search_request(request)?,
            cancellation.inner.clone(),
        )
    }
    #[wasm_bindgen(js_name = proveSearch)]
    pub fn prove_search(
        &self,
        map: &WasmProximityMap,
        request: JsValue,
    ) -> Result<WasmProximitySearchProof, JsValue> {
        let request = owned_search_request(request)?;
        let map = map.load()?;
        self.inner
            .prove_search(&map, request.as_request(), &ContentGraphLimits::default())
            .map(|inner| WasmProximitySearchProof { inner })
            .map_err(js_error)
    }
}

#[wasm_bindgen(js_name = WasmAcceleratorCatalog)]
pub struct WasmAcceleratorCatalog {
    engine: Arc<super::WasmEngine>,
    inner: AcceleratorCatalog<Arc<prolly::MemStore>>,
}

#[wasm_bindgen(js_class = WasmAcceleratorCatalog)]
impl WasmAcceleratorCatalog {
    pub fn manifest(&self) -> Vec<u8> {
        self.inner.manifest_cid().as_bytes().to_vec()
    }
    #[wasm_bindgen(js_name = sourceDescriptor)]
    pub fn source_descriptor(&self) -> Vec<u8> {
        self.inner.source_descriptor().as_bytes().to_vec()
    }
    pub fn entries(&self) -> Result<Array, JsValue> {
        let array = Array::new();
        for entry in self.inner.entries() {
            let object = Object::new();
            let kind = match entry.kind {
                CatalogAcceleratorKind::Hnsw => "hnsw",
                CatalogAcceleratorKind::ProductQuantized => "product_quantized",
                CatalogAcceleratorKind::Composite => "composite",
            };
            Reflect::set(&object, &"kind".into(), &kind.into())?;
            set_bytes(
                &object,
                "configurationFingerprint",
                entry.configuration_fingerprint.as_bytes(),
            )?;
            set_bytes(&object, "manifest", entry.manifest.as_bytes())?;
            array.push(&object);
        }
        Ok(array)
    }
    pub fn search(&self, map: &WasmProximityMap, request: JsValue) -> Result<Object, JsValue> {
        let request = owned_search_request(request)?;
        let map_value = map.load()?;
        let io = SearchIo::new(
            self.engine.store().clone(),
            Arc::new(SearchRuntime::default()),
        );
        map_value
            .search_with(self.inner.accelerators(), &io, request.as_request())
            .map_err(js_error)
            .and_then(search_result_object)
    }
    #[wasm_bindgen(js_name = searchWithRuntime)]
    pub fn search_with_runtime(
        &self,
        map: &WasmProximityMap,
        request: JsValue,
        runtime: &WasmProximitySearchRuntime,
    ) -> Result<Object, JsValue> {
        runtime.ensure_engine(&self.engine)?;
        runtime.ensure_engine(&map.engine)?;
        let request = owned_search_request(request)?;
        let map_value = map.load()?;
        let catalog = AcceleratorCatalog::load(
            runtime.io.store().clone(),
            self.inner.manifest_cid().clone(),
            map_value.tree(),
        )
        .map_err(js_error)?;
        map_value
            .search_with(catalog.accelerators(), &runtime.io, request.as_request())
            .map_err(js_error)
            .and_then(search_result_object)
    }
    #[wasm_bindgen(js_name = searchCancellable)]
    pub fn search_cancellable(
        &self,
        map: &WasmProximityMap,
        request: JsValue,
        cancellation: &WasmProximityCancellationToken,
    ) -> Result<Object, JsValue> {
        let io = SearchIo::new(
            self.engine.store().clone(),
            Arc::new(SearchRuntime::default()),
        );
        cancellable_accelerated_search(
            &map.load()?,
            &io,
            self.inner.manifest_cid().clone(),
            WasmAsyncAcceleratorKind::Catalog,
            &owned_search_request(request)?,
            cancellation.inner.clone(),
        )
    }
    #[wasm_bindgen(js_name = searchWithRuntimeCancellable)]
    pub fn search_with_runtime_cancellable(
        &self,
        map: &WasmProximityMap,
        request: JsValue,
        runtime: &WasmProximitySearchRuntime,
        cancellation: &WasmProximityCancellationToken,
    ) -> Result<Object, JsValue> {
        runtime.ensure_engine(&self.engine)?;
        runtime.ensure_engine(&map.engine)?;
        cancellable_accelerated_search(
            &map.load()?,
            &runtime.io,
            self.inner.manifest_cid().clone(),
            WasmAsyncAcceleratorKind::Catalog,
            &owned_search_request(request)?,
            cancellation.inner.clone(),
        )
    }
    #[wasm_bindgen(js_name = proveSearch)]
    pub fn prove_search(
        &self,
        map: &WasmProximityMap,
        request: JsValue,
    ) -> Result<WasmProximitySearchProof, JsValue> {
        let request = owned_search_request(request)?;
        let map = map.load()?;
        self.inner
            .prove_search(&map, request.as_request(), &ContentGraphLimits::default())
            .map(|inner| WasmProximitySearchProof { inner })
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
    engine: Arc<super::WasmEngine>,
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

    #[wasm_bindgen(js_name = scanRecords)]
    pub fn scan_records(&self, visitor: &Function) -> Result<String, JsValue> {
        scan_proximity_records(&self.map, visitor)
    }

    #[wasm_bindgen(js_name = withSearchView)]
    pub fn with_search_view(
        &self,
        query: Float32Array,
        k: u32,
        visitor: &Function,
    ) -> Result<JsValue, JsValue> {
        with_proximity_search_view(&self.map, query, k, visitor)
    }

    pub fn search(&self, request: JsValue) -> Result<Object, JsValue> {
        let request = owned_search_request(request)?;
        search_map(&self.map, &request)
    }

    #[wasm_bindgen(js_name = cancellationToken)]
    pub fn cancellation_token(&self) -> WasmProximityCancellationToken {
        WasmProximityCancellationToken::new()
    }

    #[wasm_bindgen(js_name = searchCancellable)]
    pub fn search_cancellable(
        &self,
        request: JsValue,
        cancellation: &WasmProximityCancellationToken,
    ) -> Result<Object, JsValue> {
        let io = SearchIo::new(
            self.engine.store().clone(),
            Arc::new(SearchRuntime::default()),
        );
        cancellable_map_search(
            &self.map,
            &io,
            &owned_search_request(request)?,
            cancellation.inner.clone(),
        )
    }

    #[wasm_bindgen(js_name = searchWithRuntimeCancellable)]
    pub fn search_with_runtime_cancellable(
        &self,
        request: JsValue,
        runtime: &WasmProximitySearchRuntime,
        cancellation: &WasmProximityCancellationToken,
    ) -> Result<Object, JsValue> {
        runtime.ensure_engine(&self.engine)?;
        cancellable_map_search(
            &self.map,
            &runtime.io,
            &owned_search_request(request)?,
            cancellation.inner.clone(),
        )
    }

    #[wasm_bindgen(js_name = searchWithRuntime)]
    pub fn search_with_runtime(
        &self,
        request: JsValue,
        runtime: &WasmProximitySearchRuntime,
    ) -> Result<Object, JsValue> {
        runtime.ensure_engine(&self.engine)?;
        let request = owned_search_request(request)?;
        self.map
            .search_with(&AcceleratorSet::empty(), &runtime.io, request.as_request())
            .map_err(js_error)
            .and_then(search_result_object)
    }
}

fn scan_proximity_records(
    map: &ProximityMap<Arc<prolly::MemStore>>,
    visitor: &Function,
) -> Result<String, JsValue> {
    let mut callback_error = None;
    let outcome = map
        .scan_records_until(|key, record| {
            let object = (|| -> Result<Object, JsValue> {
                let object = Object::new();
                set_bytes(&object, "key", key)?;
                let vector = record.vector.to_vec();
                Reflect::set(
                    &object,
                    &"vector".into(),
                    &Float32Array::from(vector.as_slice()).into(),
                )?;
                set_bytes(&object, "value", record.value)?;
                Ok(object)
            })();
            let should_continue = object.and_then(|object| {
                visitor
                    .call1(&JsValue::UNDEFINED, &object.into())
                    .and_then(|value| {
                        value.as_bool().ok_or_else(|| {
                            JsValue::from_str("proximity record visitor must return a boolean")
                        })
                    })
            });
            match should_continue {
                Ok(true) => std::ops::ControlFlow::Continue(()),
                Ok(false) => std::ops::ControlFlow::Break(()),
                Err(error) => {
                    callback_error = Some(error);
                    std::ops::ControlFlow::Break(())
                }
            }
        })
        .map_err(js_error)?;
    if let Some(error) = callback_error {
        return Err(error);
    }
    Ok(outcome.visited.to_string())
}

fn with_proximity_search_view(
    map: &ProximityMap<Arc<prolly::MemStore>>,
    query: Float32Array,
    k: u32,
    visitor: &Function,
) -> Result<JsValue, JsValue> {
    if query.length() == 0 || k == 0 {
        return Err(JsValue::from_str(
            "proximity search view requires a non-empty query and positive k",
        ));
    }
    let query = query.to_vec();
    let result = map
        .search(SearchRequest::exact(&query, k as usize))
        .map_err(js_error)?;
    let rows = Array::new();
    for (rank, neighbor) in result.neighbors.iter().enumerate() {
        let object = Object::new();
        // SAFETY: the search result owns both byte vectors for the complete
        // synchronous callback. The TypeScript facade poisons every view when
        // this function returns and also rejects WASM-memory growth.
        let key = unsafe { Uint8Array::view(neighbor.key.as_slice()) };
        let value = unsafe { Uint8Array::view(neighbor.value.as_slice()) };
        Reflect::set(&object, &"key".into(), &key.into())?;
        Reflect::set(&object, &"value".into(), &value.into())?;
        Reflect::set(
            &object,
            &"distance".into(),
            &JsValue::from_f64(neighbor.distance),
        )?;
        Reflect::set(&object, &"rank".into(), &JsValue::from_f64(rank as f64))?;
        rows.push(&object.into());
    }
    visitor.call1(&JsValue::UNDEFINED, &rows.into())
}

#[wasm_bindgen(js_class = WasmProllyEngine)]
impl WasmProllyEngine {
    #[wasm_bindgen(js_name = proximitySearchRuntime)]
    pub fn proximity_search_runtime(
        &self,
        policy: JsValue,
    ) -> Result<WasmProximitySearchRuntime, JsValue> {
        WasmProximitySearchRuntime::new(
            Arc::clone(&self.inner),
            search_runtime_policy_from_js(&policy)?,
        )
    }

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

    #[wasm_bindgen(js_name = loadProximity)]
    pub fn portable_load_proximity(
        &self,
        descriptor: Uint8Array,
    ) -> Result<WasmProximityMap, JsValue> {
        let raw: [u8; 32] = descriptor
            .to_vec()
            .try_into()
            .map_err(|_| JsValue::from_str("proximity descriptor must be 32 bytes"))?;
        ProximityMap::load(self.inner.store().clone(), Cid(raw)).map_err(js_error)?;
        Ok(WasmProximityMap {
            engine: Arc::clone(&self.inner),
            descriptor: Cid(raw),
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
