use super::{js_error, optional_bytes, WasmProllyEngine};
use crate::page::{set_bytes, set_optional_bytes};
use js_sys::{Array, Function, Object, Reflect, Uint8Array};
use prolly::{
    IndexProjection, IndexedMapMetricsSnapshot, IndexedMapUpdate, IndexedSnapshotBundle,
    IndexedSnapshotId, Mutation, SecondaryIndex, SecondaryIndexCursor, SecondaryIndexEntry,
    SecondaryIndexError, SecondaryIndexLimits, SecondaryIndexPage, SecondaryIndexRegistry,
};
use std::cell::RefCell;
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

fn index_projection(value: &str) -> Result<IndexProjection, JsValue> {
    match value {
        "keys_only" => Ok(IndexProjection::KeysOnly),
        "include" => Ok(IndexProjection::Include),
        "all" => Ok(IndexProjection::All),
        _ => Err(JsValue::from_str("invalid index projection")),
    }
}

fn secondary_index_limits(value: Option<JsValue>) -> Result<SecondaryIndexLimits, JsValue> {
    let Some(value) = value else {
        return Ok(SecondaryIndexLimits::default());
    };
    let field = |name: &str| -> Result<usize, JsValue> {
        let value = Reflect::get(&value, &JsValue::from_str(name))
            .map_err(|error| JsValue::from_str(&js_value_message(error)))?;
        let text = value
            .as_string()
            .ok_or_else(|| JsValue::from_str(&format!("{name} must be a decimal string")))?;
        text.parse::<u64>()
            .map_err(|error| JsValue::from_str(&format!("invalid {name}: {error}")))?
            .try_into()
            .map_err(|_| JsValue::from_str(&format!("{name} does not fit this platform")))
    };
    Ok(SecondaryIndexLimits {
        max_term_bytes: field("maxTermBytes")?,
        max_projection_bytes: field("maxProjectionBytes")?,
        max_all_value_bytes: field("maxAllValueBytes")?,
        max_terms_per_record: field("maxTermsPerRecord")?,
        max_projected_bytes_per_record: field("maxProjectedBytesPerRecord")?,
        max_derived_mutations_per_transaction: field("maxDerivedMutationsPerTransaction")?,
        max_projected_bytes_per_transaction: field("maxProjectedBytesPerTransaction")?,
        max_indexes: field("maxIndexes")?,
        build_page_size: field("buildPageSize")?,
        max_temporary_sort_bytes: field("maxTemporarySortBytes")?,
        max_bundle_nodes: field("maxBundleNodes")?,
        max_bundle_bytes: field("maxBundleBytes")?,
        max_verification_entries: field("maxVerificationEntries")?,
        max_write_retries: field("maxWriteRetries")?,
        max_build_retries: field("maxBuildRetries")?,
    })
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
        limits: Option<JsValue>,
        extractor: Function,
    ) -> Result<(), JsValue> {
        let projection = index_projection(&projection)?;
        let callback = JsIndexExtractor(extractor);
        let definition = SecondaryIndex::builder(name.to_vec(), generation, extractor_id)
            .projection(projection)
            .limits(secondary_index_limits(limits)?)
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
    registry: RefCell<SecondaryIndexRegistry>,
    metrics: RefCell<IndexedMapMetricsSnapshot>,
}

impl WasmIndexedMap {
    fn registry_snapshot(&self) -> SecondaryIndexRegistry {
        self.registry.borrow().clone()
    }

    fn capture_metrics(&self, value: IndexedMapMetricsSnapshot) {
        let mut total = self.metrics.borrow_mut();
        total.normalized_source_mutations = total
            .normalized_source_mutations
            .saturating_add(value.normalized_source_mutations);
        total.records_extracted = total
            .records_extracted
            .saturating_add(value.records_extracted);
        total.terms_emitted = total.terms_emitted.saturating_add(value.terms_emitted);
        total.projected_bytes = total.projected_bytes.saturating_add(value.projected_bytes);
        total.physical_upserts = total
            .physical_upserts
            .saturating_add(value.physical_upserts);
        total.physical_deletes = total
            .physical_deletes
            .saturating_add(value.physical_deletes);
        total.unchanged_emissions_skipped = total
            .unchanged_emissions_skipped
            .saturating_add(value.unchanged_emissions_skipped);
        total.source_nodes_written = total
            .source_nodes_written
            .saturating_add(value.source_nodes_written);
        total.index_nodes_written = total
            .index_nodes_written
            .saturating_add(value.index_nodes_written);
        total.catalog_nodes_written = total
            .catalog_nodes_written
            .saturating_add(value.catalog_nodes_written);
        total.retries = total.retries.saturating_add(value.retries);
        total.build_attempts = total.build_attempts.saturating_add(value.build_attempts);
        total.verification_outcomes = total
            .verification_outcomes
            .saturating_add(value.verification_outcomes);
        total.retained_roots = total.retained_roots.saturating_add(value.retained_roots);
    }
}

#[wasm_bindgen(js_class = WasmIndexedMap)]
impl WasmIndexedMap {
    pub fn id(&self) -> Vec<u8> {
        self.id.clone()
    }

    pub fn get(&self, key: Uint8Array) -> Result<JsValue, JsValue> {
        let map = self
            .engine
            .indexed_map(&self.id, self.registry_snapshot())
            .map_err(js_error)?;
        let result = map.get(&key.to_vec()).map(optional_bytes).map_err(js_error);
        self.capture_metrics(map.metrics());
        result
    }

    pub fn put(&self, key: Uint8Array, value: Uint8Array) -> Result<Object, JsValue> {
        let map = self
            .engine
            .indexed_map(&self.id, self.registry_snapshot())
            .map_err(js_error)?;
        let result = map
            .put(key.to_vec(), value.to_vec())
            .map_err(js_error)
            .and_then(indexed_version_object);
        self.capture_metrics(map.metrics());
        result
    }

    pub fn apply(&self, mutations: Array) -> Result<Object, JsValue> {
        let mutations = mutations_from_array(&mutations)?;
        let map = self
            .engine
            .indexed_map(&self.id, self.registry_snapshot())
            .map_err(js_error)?;
        let result = map
            .apply(mutations)
            .map_err(js_error)
            .and_then(indexed_version_object);
        self.capture_metrics(map.metrics());
        result
    }

    #[wasm_bindgen(js_name = applyIf)]
    pub fn apply_if(
        &self,
        expected_source: Option<Uint8Array>,
        mutations: Array,
    ) -> Result<Object, JsValue> {
        let expected = expected_source
            .map(|value| prolly::MapVersionId::from_bytes(&value.to_vec()))
            .transpose()
            .map_err(js_error)?;
        let mutations = mutations_from_array(&mutations)?;
        let map = self
            .engine
            .indexed_map(&self.id, self.registry_snapshot())
            .map_err(js_error)?;
        let result = map
            .apply_if(expected.as_ref(), mutations)
            .map_err(js_error)
            .and_then(indexed_update_object);
        self.capture_metrics(map.metrics());
        result
    }

    pub fn delete(&self, key: Uint8Array) -> Result<Object, JsValue> {
        let map = self
            .engine
            .indexed_map(&self.id, self.registry_snapshot())
            .map_err(js_error)?;
        let result = map
            .delete(key.to_vec())
            .map_err(js_error)
            .and_then(indexed_version_object);
        self.capture_metrics(map.metrics());
        result
    }

    #[wasm_bindgen(js_name = ensureIndex)]
    pub fn ensure_index(&self, name: Uint8Array) -> Result<Object, JsValue> {
        let map = self
            .engine
            .indexed_map(&self.id, self.registry_snapshot())
            .map_err(js_error)?;
        let result = map.ensure_index(name.to_vec()).map_err(js_error)?;
        self.capture_metrics(map.metrics());
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
            &"generation".into(),
            &result.generation.to_string().into(),
        )?;
        Reflect::set(
            &object,
            &"entries".into(),
            &result.entries.to_string().into(),
        )?;
        Reflect::set(
            &object,
            &"attempts".into(),
            &result.attempts.to_string().into(),
        )?;
        Reflect::set(
            &object,
            &"activated".into(),
            &JsValue::from_bool(result.activated),
        )?;
        Ok(object)
    }

    #[wasm_bindgen(js_name = replaceIndex)]
    pub fn replace_index(
        &self,
        name: Uint8Array,
        generation: u64,
        extractor_id: String,
        projection: String,
        limits: Option<JsValue>,
        extractor: Function,
    ) -> Result<Object, JsValue> {
        let projection = index_projection(&projection)?;
        let callback = JsIndexExtractor(extractor);
        let definition = SecondaryIndex::builder(name.to_vec(), generation, extractor_id)
            .projection(projection)
            .limits(secondary_index_limits(limits)?)
            .extract(move |key, value| callback.extract(key, value))
            .map_err(js_error)?;
        let registry = self.registry_snapshot();
        let map = self
            .engine
            .indexed_map(&self.id, registry.clone())
            .map_err(js_error)?;
        let result = map
            .replace_index(name.to_vec(), definition.clone())
            .map_err(js_error)?;
        self.capture_metrics(map.metrics());
        *self.registry.borrow_mut() = registry.replace(definition).map_err(js_error)?;

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
            &"generation".into(),
            &result.generation.to_string().into(),
        )?;
        Reflect::set(
            &object,
            &"entries".into(),
            &result.entries.to_string().into(),
        )?;
        Reflect::set(
            &object,
            &"attempts".into(),
            &result.attempts.to_string().into(),
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
            .indexed_map(&self.id, self.registry_snapshot())
            .map_err(js_error)?;
        let snapshot_id = map.snapshot().map_err(js_error)?.id().clone();
        self.capture_metrics(map.metrics());
        Ok(WasmIndexedSnapshot {
            engine: Arc::clone(&self.engine),
            id: self.id.clone(),
            registry: self.registry_snapshot(),
            snapshot_id,
        })
    }

    #[wasm_bindgen(js_name = snapshotAt)]
    pub fn snapshot_at(&self, source_version: Uint8Array) -> Result<WasmIndexedSnapshot, JsValue> {
        let version =
            prolly::MapVersionId::from_bytes(&source_version.to_vec()).map_err(js_error)?;
        let map = self
            .engine
            .indexed_map(&self.id, self.registry_snapshot())
            .map_err(js_error)?;
        let snapshot_id = map.snapshot_at(&version).map_err(js_error)?.id().clone();
        self.capture_metrics(map.metrics());
        Ok(WasmIndexedSnapshot {
            engine: Arc::clone(&self.engine),
            id: self.id.clone(),
            registry: self.registry_snapshot(),
            snapshot_id,
        })
    }

    #[wasm_bindgen(js_name = snapshotById)]
    pub fn snapshot_by_id(
        &self,
        source_version: Uint8Array,
        catalog_version: Uint8Array,
    ) -> Result<WasmIndexedSnapshot, JsValue> {
        let snapshot_id = IndexedSnapshotId {
            source_version: prolly::MapVersionId::from_bytes(&source_version.to_vec())
                .map_err(js_error)?,
            catalog_version: prolly::MapVersionId::from_bytes(&catalog_version.to_vec())
                .map_err(js_error)?,
        };
        let map = self
            .engine
            .indexed_map(&self.id, self.registry_snapshot())
            .map_err(js_error)?;
        map.snapshot_by_id(&snapshot_id).map_err(js_error)?;
        self.capture_metrics(map.metrics());
        Ok(WasmIndexedSnapshot {
            engine: Arc::clone(&self.engine),
            id: self.id.clone(),
            registry: self.registry_snapshot(),
            snapshot_id,
        })
    }

    pub fn health(&self) -> Result<Object, JsValue> {
        let map = self
            .engine
            .indexed_map(&self.id, self.registry_snapshot())
            .map_err(js_error)?;
        let result = map
            .health()
            .map_err(js_error)
            .and_then(indexed_health_object);
        self.capture_metrics(map.metrics());
        result
    }

    pub fn metrics(&self) -> Result<Object, JsValue> {
        let value = self.metrics.borrow().clone();
        let object = Object::new();
        set_u64_string(
            &object,
            "normalizedSourceMutations",
            value.normalized_source_mutations,
        )?;
        set_u64_string(&object, "recordsExtracted", value.records_extracted)?;
        set_u64_string(&object, "termsEmitted", value.terms_emitted)?;
        set_u64_string(&object, "projectedBytes", value.projected_bytes)?;
        set_u64_string(&object, "physicalUpserts", value.physical_upserts)?;
        set_u64_string(&object, "physicalDeletes", value.physical_deletes)?;
        set_u64_string(
            &object,
            "unchangedEmissionsSkipped",
            value.unchanged_emissions_skipped,
        )?;
        set_u64_string(&object, "sourceNodesWritten", value.source_nodes_written)?;
        set_u64_string(&object, "indexNodesWritten", value.index_nodes_written)?;
        set_u64_string(&object, "catalogNodesWritten", value.catalog_nodes_written)?;
        set_u64_string(&object, "retries", value.retries)?;
        set_u64_string(&object, "buildAttempts", value.build_attempts)?;
        set_u64_string(&object, "verificationOutcomes", value.verification_outcomes)?;
        set_u64_string(&object, "retainedRoots", value.retained_roots)?;
        Ok(object)
    }
    #[wasm_bindgen(js_name = verifyIndex)]
    pub fn verify_index(
        &self,
        name: Uint8Array,
        source_version: Uint8Array,
    ) -> Result<Object, JsValue> {
        let version =
            prolly::MapVersionId::from_bytes(&source_version.to_vec()).map_err(js_error)?;
        let map = self
            .engine
            .indexed_map(&self.id, self.registry_snapshot())
            .map_err(js_error)?;
        let result = map
            .verify_index(name.to_vec(), &version)
            .map_err(js_error)
            .and_then(index_verification_object);
        self.capture_metrics(map.metrics());
        result
    }

    #[wasm_bindgen(js_name = verifyAll)]
    pub fn verify_all(&self, source_version: Uint8Array) -> Result<Array, JsValue> {
        let version =
            prolly::MapVersionId::from_bytes(&source_version.to_vec()).map_err(js_error)?;
        let map = self
            .engine
            .indexed_map(&self.id, self.registry_snapshot())
            .map_err(js_error)?;
        let values = map.verify_all(&version).map_err(js_error)?;
        self.capture_metrics(map.metrics());
        let out = Array::new();
        for value in values {
            out.push(&index_verification_object(value)?.into());
        }
        Ok(out)
    }

    #[wasm_bindgen(js_name = repairIndex)]
    pub fn repair_index(
        &self,
        name: Uint8Array,
        source_version: Uint8Array,
    ) -> Result<Object, JsValue> {
        let version =
            prolly::MapVersionId::from_bytes(&source_version.to_vec()).map_err(js_error)?;
        let map = self
            .engine
            .indexed_map(&self.id, self.registry_snapshot())
            .map_err(js_error)?;
        let result = map
            .repair_index(name.to_vec(), &version)
            .map_err(js_error)
            .and_then(index_verification_object);
        self.capture_metrics(map.metrics());
        result
    }

    #[wasm_bindgen(js_name = deactivateIndex)]
    pub fn deactivate_index(&self, name: Uint8Array) -> Result<Object, JsValue> {
        let map = self
            .engine
            .indexed_map(&self.id, self.registry_snapshot())
            .map_err(js_error)?;
        let result = map
            .deactivate_index(name.to_vec())
            .map_err(js_error)
            .and_then(indexed_version_object);
        self.capture_metrics(map.metrics());
        result
    }
    #[wasm_bindgen(js_name = exportCurrent)]
    pub fn export_current(&self) -> Result<Vec<u8>, JsValue> {
        let map = self
            .engine
            .indexed_map(&self.id, self.registry_snapshot())
            .map_err(js_error)?;
        let result = map
            .export_current()
            .and_then(|value| value.to_bytes())
            .map_err(js_error);
        self.capture_metrics(map.metrics());
        result
    }

    #[wasm_bindgen(js_name = importCurrent)]
    pub fn import_current(
        &self,
        bundle: Uint8Array,
        expected_source: Option<Uint8Array>,
    ) -> Result<Object, JsValue> {
        let bundle = IndexedSnapshotBundle::from_bytes(&bundle.to_vec()).map_err(js_error)?;
        let expected = expected_source
            .map(|value| prolly::MapVersionId::from_bytes(&value.to_vec()))
            .transpose()
            .map_err(js_error)?;
        let map = self
            .engine
            .indexed_map(&self.id, self.registry_snapshot())
            .map_err(js_error)?;
        let result = map
            .import_current(&bundle, expected.as_ref())
            .map_err(js_error)
            .and_then(indexed_version_object);
        self.capture_metrics(map.metrics());
        result
    }

    #[wasm_bindgen(js_name = keepLast)]
    pub fn keep_last(&self, count: u64) -> Result<Object, JsValue> {
        let count = usize::try_from(count)
            .map_err(|_| JsValue::from_str("retention count does not fit this platform"))?;
        let map = self
            .engine
            .indexed_map(&self.id, self.registry_snapshot())
            .map_err(js_error)?;
        let result = map
            .keep_last(count)
            .map_err(js_error)
            .and_then(indexed_retention_object);
        self.capture_metrics(map.metrics());
        result
    }

    #[wasm_bindgen(js_name = planGc)]
    pub fn plan_gc(&self) -> Result<Object, JsValue> {
        let map = self
            .engine
            .indexed_map(&self.id, self.registry_snapshot())
            .map_err(js_error)?;
        let result = map
            .plan_indexed_gc()
            .map_err(js_error)
            .and_then(super::domain::gc_plan_object);
        self.capture_metrics(map.metrics());
        result
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
    pub fn id(&self) -> Result<Object, JsValue> {
        indexed_snapshot_id_object(&self.snapshot_id)
    }

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

impl WasmSecondaryIndex {
    fn with_index<R>(
        &self,
        operation: impl FnOnce(
            &prolly::SecondaryIndexSnapshot<'_, Arc<prolly::MemStore>>,
        ) -> Result<R, prolly::Error>,
    ) -> Result<R, JsValue> {
        let map = self
            .engine
            .indexed_map(&self.id, self.registry.clone())
            .map_err(js_error)?;
        let snapshot = map.snapshot_by_id(&self.snapshot_id).map_err(js_error)?;
        let index = snapshot.index(&self.name).map_err(js_error)?;
        operation(index).map_err(js_error)
    }
}

#[wasm_bindgen(js_class = WasmSecondaryIndex)]
impl WasmSecondaryIndex {
    pub fn name(&self) -> Vec<u8> {
        self.name.clone()
    }

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

    pub fn prefix(&self, prefix: Uint8Array) -> Result<Array, JsValue> {
        let map = self
            .engine
            .indexed_map(&self.id, self.registry.clone())
            .map_err(js_error)?;
        let snapshot = map.snapshot_by_id(&self.snapshot_id).map_err(js_error)?;
        let index = snapshot.index(&self.name).map_err(js_error)?;
        let out = Array::new();
        for matched in index.prefix(&prefix.to_vec()).map_err(js_error)? {
            out.push(&index_match_object(matched)?.into());
        }
        Ok(out)
    }

    pub fn range(&self, start: Uint8Array, end: Option<Uint8Array>) -> Result<Array, JsValue> {
        let end = end.map(|value| value.to_vec());
        let map = self
            .engine
            .indexed_map(&self.id, self.registry.clone())
            .map_err(js_error)?;
        let snapshot = map.snapshot_by_id(&self.snapshot_id).map_err(js_error)?;
        let index = snapshot.index(&self.name).map_err(js_error)?;
        let out = Array::new();
        for matched in index
            .range(&start.to_vec(), end.as_deref())
            .map_err(js_error)?
        {
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
        let mut callback_error = None;
        index
            .scan_records(&term, |record| {
                if callback_error.is_some() {
                    return;
                }
                let result = (|| {
                    let object = Object::new();
                    set_bytes(&object, "term", record.term)?;
                    set_bytes(&object, "primaryKey", record.primary_key)?;
                    set_optional_bytes(&object, "projection", record.projection)?;
                    set_bytes(&object, "sourceValue", record.source_value)?;
                    out.push(&object.into());
                    Ok::<(), JsValue>(())
                })();
                callback_error = result.err();
            })
            .map_err(js_error)?;
        if let Some(error) = callback_error {
            return Err(error);
        }
        Ok(out)
    }

    #[wasm_bindgen(js_name = exactPage)]
    pub fn exact_page(
        &self,
        term: Uint8Array,
        cursor: Option<Uint8Array>,
        limit: u64,
    ) -> Result<Object, JsValue> {
        let cursor = index_cursor(cursor)?;
        let limit = page_limit(limit)?;
        let term = term.to_vec();
        index_page_object(self.with_index(|index| index.exact_page(&term, cursor.as_ref(), limit))?)
    }

    #[wasm_bindgen(js_name = exactReversePage)]
    pub fn exact_reverse_page(
        &self,
        term: Uint8Array,
        cursor: Option<Uint8Array>,
        limit: u64,
    ) -> Result<Object, JsValue> {
        let cursor = index_cursor(cursor)?;
        let limit = page_limit(limit)?;
        let term = term.to_vec();
        index_page_object(
            self.with_index(|index| index.exact_reverse_page(&term, cursor.as_ref(), limit))?,
        )
    }

    #[wasm_bindgen(js_name = prefixPage)]
    pub fn prefix_page(
        &self,
        prefix: Uint8Array,
        cursor: Option<Uint8Array>,
        limit: u64,
    ) -> Result<Object, JsValue> {
        let cursor = index_cursor(cursor)?;
        let limit = page_limit(limit)?;
        let prefix = prefix.to_vec();
        index_page_object(
            self.with_index(|index| index.prefix_page(&prefix, cursor.as_ref(), limit))?,
        )
    }

    #[wasm_bindgen(js_name = prefixReversePage)]
    pub fn prefix_reverse_page(
        &self,
        prefix: Uint8Array,
        cursor: Option<Uint8Array>,
        limit: u64,
    ) -> Result<Object, JsValue> {
        let cursor = index_cursor(cursor)?;
        let limit = page_limit(limit)?;
        let prefix = prefix.to_vec();
        index_page_object(
            self.with_index(|index| index.prefix_reverse_page(&prefix, cursor.as_ref(), limit))?,
        )
    }

    #[wasm_bindgen(js_name = rangePage)]
    pub fn range_page(
        &self,
        start: Uint8Array,
        end: Option<Uint8Array>,
        cursor: Option<Uint8Array>,
        limit: u64,
    ) -> Result<Object, JsValue> {
        let cursor = index_cursor(cursor)?;
        let end = end.map(|value| value.to_vec());
        let limit = page_limit(limit)?;
        let start = start.to_vec();
        index_page_object(
            self.with_index(|index| {
                index.range_page(&start, end.as_deref(), cursor.as_ref(), limit)
            })?,
        )
    }

    #[wasm_bindgen(js_name = rangeReversePage)]
    pub fn range_reverse_page(
        &self,
        start: Uint8Array,
        end: Option<Uint8Array>,
        cursor: Option<Uint8Array>,
        limit: u64,
    ) -> Result<Object, JsValue> {
        let cursor = index_cursor(cursor)?;
        let end = end.map(|value| value.to_vec());
        let limit = page_limit(limit)?;
        let start = start.to_vec();
        index_page_object(self.with_index(|index| {
            index.range_reverse_page(&start, end.as_deref(), cursor.as_ref(), limit)
        })?)
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
            registry: RefCell::new(registry.registry.clone()),
            metrics: RefCell::new(IndexedMapMetricsSnapshot::default()),
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

fn indexed_update_object(update: IndexedMapUpdate) -> Result<Object, JsValue> {
    let object = Object::new();
    match update {
        IndexedMapUpdate::Applied { previous, current } => {
            Reflect::set(&object, &"kind".into(), &"applied".into())?;
            set_optional_bytes(
                &object,
                "previousSourceVersion",
                previous.as_ref().map(|value| value.as_cid().as_bytes()),
            )?;
            Reflect::set(
                &object,
                &"current".into(),
                &indexed_version_object(current)?.into(),
            )?;
        }
        IndexedMapUpdate::Unchanged { current } => {
            Reflect::set(&object, &"kind".into(), &"unchanged".into())?;
            Reflect::set(
                &object,
                &"previousSourceVersion".into(),
                &JsValue::UNDEFINED,
            )?;
            set_optional_indexed_version(&object, current)?;
        }
        IndexedMapUpdate::Conflict { current } => {
            Reflect::set(&object, &"kind".into(), &"conflict".into())?;
            Reflect::set(
                &object,
                &"previousSourceVersion".into(),
                &JsValue::UNDEFINED,
            )?;
            set_optional_indexed_version(&object, current)?;
        }
    }
    Ok(object)
}

fn set_optional_indexed_version(
    object: &Object,
    version: Option<prolly::IndexedVersion>,
) -> Result<(), JsValue> {
    match version {
        Some(value) => {
            Reflect::set(
                object,
                &"current".into(),
                &indexed_version_object(value)?.into(),
            )?;
        }
        None => {
            Reflect::set(object, &"current".into(), &JsValue::UNDEFINED)?;
        }
    }
    Ok(())
}

fn indexed_snapshot_id_object(id: &IndexedSnapshotId) -> Result<Object, JsValue> {
    let object = Object::new();
    set_bytes(
        &object,
        "sourceVersion",
        id.source_version.as_cid().as_bytes(),
    )?;
    set_bytes(
        &object,
        "catalogVersion",
        id.catalog_version.as_cid().as_bytes(),
    )?;
    Ok(object)
}

fn indexed_health_object(value: prolly::IndexedMapHealth) -> Result<Object, JsValue> {
    let object = Object::new();
    set_bytes(&object, "sourceMapId", &value.source_map_id)?;
    set_optional_bytes(
        &object,
        "sourceVersion",
        value
            .source_version
            .as_ref()
            .map(|version| version.as_cid().as_bytes()),
    )?;
    set_optional_bytes(
        &object,
        "catalogVersion",
        value
            .catalog_version
            .as_ref()
            .map(|version| version.as_cid().as_bytes()),
    )?;
    let indexes = Array::new();
    for index in value.active_indexes {
        let item = Object::new();
        set_bytes(&item, "name", &index.name)?;
        set_u64_string(&item, "generation", index.generation)?;
        set_bytes(&item, "fingerprint", index.fingerprint.as_bytes())?;
        let projection = match index.projection {
            IndexProjection::KeysOnly => "keys_only",
            IndexProjection::Include => "include",
            IndexProjection::All => "all",
        };
        Reflect::set(&item, &"projection".into(), &projection.into())?;
        set_bytes(&item, "indexMapId", &index.index_map_id)?;
        set_bytes(
            &item,
            "indexVersion",
            index.index_version.as_cid().as_bytes(),
        )?;
        indexes.push(&item.into());
    }
    Reflect::set(&object, &"activeIndexes".into(), &indexes.into())?;
    Reflect::set(
        &object,
        &"supportsTransactions".into(),
        &value.supports_transactions.into(),
    )?;
    Ok(object)
}

fn index_verification_object(value: prolly::IndexVerification) -> Result<Object, JsValue> {
    let valid = value.is_valid();
    let canonical = value.is_canonical();
    let object = Object::new();
    set_bytes(&object, "name", &value.name)?;
    set_bytes(
        &object,
        "sourceVersion",
        value.source_version.as_cid().as_bytes(),
    )?;
    set_bytes(
        &object,
        "expectedIndexVersion",
        value.expected_index_version.as_cid().as_bytes(),
    )?;
    set_bytes(
        &object,
        "actualIndexVersion",
        value.actual_index_version.as_cid().as_bytes(),
    )?;
    set_u64_string(&object, "expectedEntries", value.expected_entries as u64)?;
    set_u64_string(&object, "actualEntries", value.actual_entries as u64)?;
    set_u64_string(
        &object,
        "semanticDifferences",
        value.semantic_differences as u64,
    )?;
    Reflect::set(&object, &"valid".into(), &valid.into())?;
    Reflect::set(&object, &"canonical".into(), &canonical.into())?;
    Ok(object)
}

fn indexed_retention_object(value: prolly::IndexedRetentionResult) -> Result<Object, JsValue> {
    let object = Object::new();
    set_version_array(
        &object,
        "retainedSourceVersions",
        value.retained_source_versions,
    )?;
    set_version_array(
        &object,
        "removedSourceVersions",
        value.removed_source_versions,
    )?;
    set_version_array(
        &object,
        "retainedIndexVersions",
        value.retained_index_versions,
    )?;
    set_version_array(
        &object,
        "removedIndexVersions",
        value.removed_index_versions,
    )?;
    set_version_array(
        &object,
        "removedCatalogVersions",
        value.removed_catalog_versions,
    )?;
    set_u64_string(
        &object,
        "removedCheckpointRecords",
        value.removed_checkpoint_records as u64,
    )?;
    let roots = Array::new();
    for root in value.removed_named_roots {
        roots.push(&Uint8Array::from(root.as_slice()).into());
    }
    Reflect::set(&object, &"removedNamedRoots".into(), &roots.into())?;
    Ok(object)
}

fn set_version_array(
    object: &Object,
    name: &str,
    values: Vec<prolly::MapVersionId>,
) -> Result<(), JsValue> {
    let array = Array::new();
    for value in values {
        array.push(&Uint8Array::from(value.as_cid().as_bytes()).into());
    }
    Reflect::set(object, &name.into(), &array.into())?;
    Ok(())
}

pub(crate) fn mutations_from_array(values: &Array) -> Result<Vec<Mutation>, JsValue> {
    values
        .iter()
        .map(|value| {
            let kind = Reflect::get(&value, &"kind".into())?
                .as_string()
                .ok_or_else(|| JsValue::from_str("indexed mutation kind must be a string"))?;
            let key = required_bytes_property(&value, "key")?;
            match kind.as_str() {
                "upsert" => Ok(Mutation::Upsert {
                    key,
                    val: required_bytes_property(&value, "value")?,
                }),
                "delete" => Ok(Mutation::Delete { key }),
                _ => Err(JsValue::from_str(
                    "indexed mutation kind must be upsert or delete",
                )),
            }
        })
        .collect()
}

fn required_bytes_property(value: &JsValue, name: &str) -> Result<Vec<u8>, JsValue> {
    Reflect::get(value, &name.into())?
        .dyn_into::<Uint8Array>()
        .map(|bytes| bytes.to_vec())
        .map_err(|_| JsValue::from_str(&format!("indexed mutation {name} must be a Uint8Array")))
}

fn index_cursor(value: Option<Uint8Array>) -> Result<Option<SecondaryIndexCursor>, JsValue> {
    value
        .map(|bytes| SecondaryIndexCursor::from_bytes(&bytes.to_vec()))
        .transpose()
        .map_err(js_error)
}

fn page_limit(value: u64) -> Result<usize, JsValue> {
    usize::try_from(value).map_err(|_| JsValue::from_str("index page limit does not fit WASM"))
}

fn index_page_object(page: SecondaryIndexPage) -> Result<Object, JsValue> {
    let object = Object::new();
    let matches = Array::new();
    for matched in page.matches {
        matches.push(&index_match_object(matched)?.into());
    }
    Reflect::set(&object, &"matches".into(), &matches.into())?;
    let cursor = page
        .next_cursor
        .map(|cursor| cursor.to_bytes())
        .transpose()
        .map_err(js_error)?;
    set_optional_bytes(&object, "nextCursor", cursor.as_deref())?;
    Ok(object)
}

fn set_u64_string(object: &Object, name: &str, value: u64) -> Result<(), JsValue> {
    Reflect::set(object, &name.into(), &value.to_string().into())?;
    Ok(())
}

fn index_match_object(matched: prolly::SecondaryIndexMatch) -> Result<Object, JsValue> {
    let object = Object::new();
    set_bytes(&object, "term", &matched.term)?;
    set_bytes(&object, "primaryKey", &matched.primary_key)?;
    set_optional_bytes(&object, "projection", matched.projection.as_deref())?;
    Ok(object)
}
