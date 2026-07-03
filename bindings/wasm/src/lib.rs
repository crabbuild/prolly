use std::sync::Arc;

use js_sys::{Array, Object, Reflect, Uint8Array};
use prolly::{
    is_boundary_config as core_is_boundary_config, AuthenticatedProofEnvelope, BatchApplyResult,
    BatchApplyStats, Cid, Config, Conflict, Diff, DiffPageProof, Encoding, Error, KeyProof,
    MemStore, MultiKeyProof, Mutation, Node, ParallelConfig, Prolly, RangeCursor, RangePageProof,
    RangeProof, Resolver, SnapshotNamespace, StructuralDiffCursor, Tree,
};
use serde_json::Value;
use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;

type WasmEngine = Prolly<Arc<MemStore>>;

#[wasm_bindgen(js_name = WasmConfig)]
#[derive(Clone)]
pub struct WasmConfig {
    inner: Config,
}

#[wasm_bindgen(js_class = WasmConfig)]
impl WasmConfig {
    #[wasm_bindgen(constructor)]
    pub fn new() -> Self {
        Self {
            inner: Config::default(),
        }
    }

    #[wasm_bindgen(js_name = fromJson)]
    pub fn from_json(json: &str) -> Result<WasmConfig, JsValue> {
        config_from_json(json).map(|inner| Self { inner })
    }

    #[wasm_bindgen(js_name = toJson)]
    pub fn to_json(&self) -> Result<String, JsValue> {
        serde_json::to_string(&self.inner).map_err(js_error)
    }

    #[wasm_bindgen(getter, js_name = minChunkSize)]
    pub fn min_chunk_size(&self) -> u32 {
        self.inner.min_chunk_size as u32
    }

    #[wasm_bindgen(getter, js_name = maxChunkSize)]
    pub fn max_chunk_size(&self) -> u32 {
        self.inner.max_chunk_size as u32
    }

    #[wasm_bindgen(getter, js_name = chunkingFactor)]
    pub fn chunking_factor(&self) -> u32 {
        self.inner.chunking_factor
    }

    #[wasm_bindgen(getter, js_name = hashSeed)]
    pub fn hash_seed(&self) -> String {
        self.inner.hash_seed.to_string()
    }

    #[wasm_bindgen(getter)]
    pub fn encoding(&self) -> String {
        match &self.inner.encoding {
            Encoding::Raw => "raw".to_string(),
            Encoding::Cbor => "cbor".to_string(),
            Encoding::Json => "json".to_string(),
            Encoding::Custom(name) => format!("custom:{name}"),
        }
    }
}

impl Default for WasmConfig {
    fn default() -> Self {
        Self::new()
    }
}

#[wasm_bindgen(js_name = WasmTree)]
#[derive(Clone)]
pub struct WasmTree {
    inner: Tree,
}

#[wasm_bindgen(js_class = WasmTree)]
impl WasmTree {
    #[wasm_bindgen(getter)]
    pub fn root(&self) -> JsValue {
        self.inner
            .root
            .as_ref()
            .map(|cid| Uint8Array::from(cid.as_bytes()).into())
            .unwrap_or(JsValue::NULL)
    }

    #[wasm_bindgen(js_name = isEmpty)]
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
}

#[wasm_bindgen(js_name = WasmRangeCursor)]
#[derive(Clone)]
pub struct WasmRangeCursor {
    inner: RangeCursor,
}

#[wasm_bindgen(js_class = WasmRangeCursor)]
impl WasmRangeCursor {
    #[wasm_bindgen(constructor)]
    pub fn new(after_key: Option<Uint8Array>) -> WasmRangeCursor {
        let inner = after_key
            .map(|key| RangeCursor::after_key(key.to_vec()))
            .unwrap_or_else(RangeCursor::start);
        Self { inner }
    }

    #[wasm_bindgen(js_name = start)]
    pub fn start() -> WasmRangeCursor {
        Self {
            inner: RangeCursor::start(),
        }
    }

    #[wasm_bindgen(getter, js_name = afterKey)]
    pub fn after_key(&self) -> JsValue {
        self.inner
            .after()
            .map(|key| Uint8Array::from(key).into())
            .unwrap_or(JsValue::NULL)
    }
}

#[wasm_bindgen(js_name = WasmProllyEngine)]
pub struct WasmProllyEngine {
    inner: WasmEngine,
}

#[wasm_bindgen(js_class = WasmProllyEngine)]
impl WasmProllyEngine {
    #[wasm_bindgen(js_name = memory)]
    pub fn memory() -> WasmProllyEngine {
        Self::memory_with_config(WasmConfig::default())
    }

    #[wasm_bindgen(js_name = memoryWithConfig)]
    pub fn memory_with_config(config: WasmConfig) -> WasmProllyEngine {
        let store = Arc::new(MemStore::new());
        Self {
            inner: Prolly::new(store, config.inner),
        }
    }

    #[wasm_bindgen(js_name = memoryWithConfigJson)]
    pub fn memory_with_config_json(json: &str) -> Result<WasmProllyEngine, JsValue> {
        Ok(Self::memory_with_config(WasmConfig::from_json(json)?))
    }

    pub fn create(&self) -> WasmTree {
        WasmTree {
            inner: self.inner.create(),
        }
    }

    pub fn get(&self, tree: &WasmTree, key: Uint8Array) -> Result<JsValue, JsValue> {
        self.inner
            .get(&tree.inner, &key.to_vec())
            .map(optional_bytes)
            .map_err(js_error)
    }

    #[wasm_bindgen(js_name = getMany)]
    pub fn get_many(&self, tree: &WasmTree, keys: Array) -> Result<Array, JsValue> {
        let keys = bytes_array(keys)?;
        let values = self.inner.get_many(&tree.inner, &keys).map_err(js_error)?;
        let out = Array::new();
        for value in values {
            out.push(&optional_bytes(value));
        }
        Ok(out)
    }

    #[wasm_bindgen(js_name = proveKey)]
    pub fn prove_key(&self, tree: &WasmTree, key: Uint8Array) -> Result<Object, JsValue> {
        self.inner
            .prove_key(&tree.inner, &key.to_vec())
            .map_err(js_error)
            .and_then(key_proof_to_object)
    }

    #[wasm_bindgen(js_name = proveKeys)]
    pub fn prove_keys(&self, tree: &WasmTree, keys: Array) -> Result<Object, JsValue> {
        let keys = bytes_array(keys)?;
        self.inner
            .prove_keys(&tree.inner, &keys)
            .map_err(js_error)
            .and_then(multi_key_proof_to_object)
    }

    #[wasm_bindgen(js_name = proveRange)]
    pub fn prove_range(
        &self,
        tree: &WasmTree,
        start: Uint8Array,
        end: Option<Uint8Array>,
    ) -> Result<Object, JsValue> {
        let start = start.to_vec();
        let end = end.map(|value| value.to_vec());
        self.inner
            .prove_range(&tree.inner, &start, end.as_deref())
            .map_err(js_error)
            .and_then(range_proof_to_object)
    }

    #[wasm_bindgen(js_name = provePrefix)]
    pub fn prove_prefix(&self, tree: &WasmTree, prefix: Uint8Array) -> Result<Object, JsValue> {
        self.inner
            .prove_prefix(&tree.inner, &prefix.to_vec())
            .map_err(js_error)
            .and_then(range_proof_to_object)
    }

    #[wasm_bindgen(js_name = proveRangePage)]
    pub fn prove_range_page(
        &self,
        tree: &WasmTree,
        cursor: Option<WasmRangeCursor>,
        end: Option<Uint8Array>,
        limit: u32,
    ) -> Result<Object, JsValue> {
        let cursor = cursor
            .map(|cursor| cursor.inner)
            .unwrap_or_else(RangeCursor::start);
        let end = end.map(|value| value.to_vec());
        self.inner
            .prove_range_page(&tree.inner, &cursor, end.as_deref(), limit as usize)
            .map_err(js_error)
            .and_then(proved_range_page_to_object)
    }

    pub fn put(
        &self,
        tree: &WasmTree,
        key: Uint8Array,
        value: Uint8Array,
    ) -> Result<WasmTree, JsValue> {
        self.inner
            .put(&tree.inner, key.to_vec(), value.to_vec())
            .map(|inner| WasmTree { inner })
            .map_err(js_error)
    }

    pub fn delete(&self, tree: &WasmTree, key: Uint8Array) -> Result<WasmTree, JsValue> {
        self.inner
            .delete(&tree.inner, &key.to_vec())
            .map(|inner| WasmTree { inner })
            .map_err(js_error)
    }

    pub fn batch(&self, tree: &WasmTree, mutations: Array) -> Result<WasmTree, JsValue> {
        let mutations = mutations_array(mutations)?;
        self.inner
            .batch(&tree.inner, mutations)
            .map(|inner| WasmTree { inner })
            .map_err(js_error)
    }

    #[wasm_bindgen(js_name = batchWithStats)]
    pub fn batch_with_stats(&self, tree: &WasmTree, mutations: Array) -> Result<Object, JsValue> {
        let mutations = mutations_array(mutations)?;
        self.inner
            .batch_with_stats(&tree.inner, mutations)
            .map_err(js_error)
            .and_then(batch_apply_result_to_object)
    }

    #[wasm_bindgen(js_name = parallelBatch)]
    pub fn parallel_batch(
        &self,
        tree: &WasmTree,
        mutations: Array,
        config: JsValue,
    ) -> Result<WasmTree, JsValue> {
        let mutations = mutations_array(mutations)?;
        let config = parallel_config_from_js(&config)?;
        self.inner
            .parallel_batch(&tree.inner, mutations, &config)
            .map(|inner| WasmTree { inner })
            .map_err(js_error)
    }

    #[wasm_bindgen(js_name = buildFromEntries)]
    pub fn build_from_entries(&self, entries: Array) -> Result<WasmTree, JsValue> {
        let entries = entries_array(entries)?;
        self.inner
            .build_from_entries(entries)
            .map(|inner| WasmTree { inner })
            .map_err(js_error)
    }

    #[wasm_bindgen(js_name = buildFromSortedEntries)]
    pub fn build_from_sorted_entries(&self, entries: Array) -> Result<WasmTree, JsValue> {
        let entries = entries_array(entries)?;
        self.inner
            .build_from_sorted_entries(entries)
            .map(|inner| WasmTree { inner })
            .map_err(js_error)
    }

    #[wasm_bindgen(js_name = appendBatch)]
    pub fn append_batch(&self, tree: &WasmTree, mutations: Array) -> Result<WasmTree, JsValue> {
        let mutations = mutations_array(mutations)?;
        self.inner
            .append_batch(&tree.inner, mutations)
            .map(|inner| WasmTree { inner })
            .map_err(js_error)
    }

    #[wasm_bindgen(js_name = appendBatchWithStats)]
    pub fn append_batch_with_stats(
        &self,
        tree: &WasmTree,
        mutations: Array,
    ) -> Result<Object, JsValue> {
        let mutations = mutations_array(mutations)?;
        self.inner
            .append_batch_with_stats(&tree.inner, mutations)
            .map_err(js_error)
            .and_then(batch_apply_result_to_object)
    }

    pub fn range(
        &self,
        tree: &WasmTree,
        start: Uint8Array,
        end: Option<Uint8Array>,
    ) -> Result<Array, JsValue> {
        let start = start.to_vec();
        let end = end.map(|value| value.to_vec());
        let entries = self
            .inner
            .range(&tree.inner, &start, end.as_deref())
            .map_err(js_error)?;
        collect_entries(entries)
    }

    #[wasm_bindgen(js_name = rangeAfter)]
    pub fn range_after(
        &self,
        tree: &WasmTree,
        after_key: Uint8Array,
        end: Option<Uint8Array>,
    ) -> Result<Array, JsValue> {
        let after_key = after_key.to_vec();
        let end = end.map(|value| value.to_vec());
        let entries = self
            .inner
            .range_after(&tree.inner, &after_key, end.as_deref())
            .map_err(js_error)?;
        collect_entries(entries)
    }

    #[wasm_bindgen(js_name = rangeFromCursor)]
    pub fn range_from_cursor(
        &self,
        tree: &WasmTree,
        cursor: Option<WasmRangeCursor>,
        end: Option<Uint8Array>,
    ) -> Result<Array, JsValue> {
        let cursor = cursor
            .map(|cursor| cursor.inner)
            .unwrap_or_else(RangeCursor::start);
        let end = end.map(|value| value.to_vec());
        let entries = self
            .inner
            .range_from_cursor(&tree.inner, &cursor, end.as_deref())
            .map_err(js_error)?;
        collect_entries(entries)
    }

    #[wasm_bindgen(js_name = rangePage)]
    pub fn range_page(
        &self,
        tree: &WasmTree,
        cursor: Option<WasmRangeCursor>,
        end: Option<Uint8Array>,
        limit: u32,
    ) -> Result<Object, JsValue> {
        let cursor = cursor
            .map(|cursor| cursor.inner)
            .unwrap_or_else(RangeCursor::start);
        let page = self
            .inner
            .range_page(
                &tree.inner,
                &cursor,
                end.as_ref().map(Uint8Array::to_vec).as_deref(),
                limit as usize,
            )
            .map_err(js_error)?;
        let object = Object::new();
        let entries: JsValue = entries_to_array(page.entries)?.into();
        Reflect::set(&object, &"entries".into(), &entries)?;
        let cursor_value = page
            .next_cursor
            .map(|inner| WasmRangeCursor { inner }.into())
            .unwrap_or(JsValue::NULL);
        Reflect::set(&object, &"nextCursor".into(), &cursor_value)?;
        Ok(object)
    }

    pub fn diff(&self, base: &WasmTree, other: &WasmTree) -> Result<Array, JsValue> {
        let diffs = self
            .inner
            .diff(&base.inner, &other.inner)
            .map_err(js_error)?;
        diffs_to_array(diffs)
    }

    #[wasm_bindgen(js_name = rangeDiff)]
    pub fn range_diff(
        &self,
        base: &WasmTree,
        other: &WasmTree,
        start: Uint8Array,
        end: Option<Uint8Array>,
    ) -> Result<Array, JsValue> {
        let start = start.to_vec();
        let end = end.map(|value| value.to_vec());
        let diffs = self
            .inner
            .range_diff(&base.inner, &other.inner, &start, end.as_deref())
            .map_err(js_error)?;
        diffs_to_array(diffs)
    }

    #[wasm_bindgen(js_name = diffFromCursor)]
    pub fn diff_from_cursor(
        &self,
        base: &WasmTree,
        other: &WasmTree,
        cursor: Option<WasmRangeCursor>,
        end: Option<Uint8Array>,
    ) -> Result<Array, JsValue> {
        let cursor = cursor
            .map(|cursor| cursor.inner)
            .unwrap_or_else(RangeCursor::start);
        let end = end.map(|value| value.to_vec());
        let diffs = self
            .inner
            .diff_from_cursor(&base.inner, &other.inner, &cursor, end.as_deref())
            .map_err(js_error)?;
        diffs_to_array(diffs)
    }

    #[wasm_bindgen(js_name = diffPage)]
    pub fn diff_page(
        &self,
        base: &WasmTree,
        other: &WasmTree,
        cursor: Option<WasmRangeCursor>,
        end: Option<Uint8Array>,
        limit: u32,
    ) -> Result<Object, JsValue> {
        let cursor = cursor
            .map(|cursor| cursor.inner)
            .unwrap_or_else(RangeCursor::start);
        let end = end.map(|value| value.to_vec());
        let page = self
            .inner
            .diff_page(
                &base.inner,
                &other.inner,
                &cursor,
                end.as_deref(),
                limit as usize,
            )
            .map_err(js_error)?;
        let object = Object::new();
        let diffs: JsValue = diffs_to_array(page.diffs)?.into();
        Reflect::set(&object, &"diffs".into(), &diffs)?;
        Reflect::set(
            &object,
            &"nextCursor".into(),
            &range_cursor_value(page.next_cursor),
        )?;
        Ok(object)
    }

    #[wasm_bindgen(js_name = proveDiffPage)]
    pub fn prove_diff_page(
        &self,
        base: &WasmTree,
        other: &WasmTree,
        cursor: Option<WasmRangeCursor>,
        end: Option<Uint8Array>,
        limit: u32,
    ) -> Result<Object, JsValue> {
        let cursor = cursor
            .map(|cursor| cursor.inner)
            .unwrap_or_else(RangeCursor::start);
        let end = end.map(|value| value.to_vec());
        self.inner
            .prove_diff_page(
                &base.inner,
                &other.inner,
                &cursor,
                end.as_deref(),
                limit as usize,
            )
            .map_err(js_error)
            .and_then(proved_diff_page_to_object)
    }

    #[wasm_bindgen(js_name = structuralDiffPage)]
    pub fn structural_diff_page(
        &self,
        base: &WasmTree,
        other: &WasmTree,
        cursor_json: Option<String>,
        limit: u32,
    ) -> Result<Object, JsValue> {
        let cursor = cursor_json
            .map(|json| serde_json::from_str::<StructuralDiffCursor>(&json).map_err(js_error))
            .transpose()?;
        let page = self
            .inner
            .structural_diff_page(&base.inner, &other.inner, cursor.as_ref(), limit as usize)
            .map_err(js_error)?;
        let object = Object::new();
        let diffs: JsValue = diffs_to_array(page.diffs)?.into();
        Reflect::set(&object, &"diffs".into(), &diffs)?;
        let next_cursor_json = page
            .next_cursor
            .map(|cursor| serde_json::to_string(&cursor).map_err(js_error))
            .transpose()?
            .map(JsValue::from)
            .unwrap_or(JsValue::NULL);
        Reflect::set(&object, &"nextCursorJson".into(), &next_cursor_json)?;
        Reflect::set(&object, &"stats".into(), &diff_stats_to_object(page.stats)?)?;
        Ok(object)
    }

    pub fn merge(
        &self,
        base: &WasmTree,
        left: &WasmTree,
        right: &WasmTree,
        resolver: Option<String>,
    ) -> Result<WasmTree, JsValue> {
        let resolver = resolver_from_name(resolver)?;
        self.inner
            .merge(&base.inner, &left.inner, &right.inner, resolver)
            .map(|inner| WasmTree { inner })
            .map_err(js_error)
    }

    #[wasm_bindgen(js_name = mergeExplain)]
    pub fn merge_explain(
        &self,
        base: &WasmTree,
        left: &WasmTree,
        right: &WasmTree,
        resolver: Option<String>,
    ) -> Result<Object, JsValue> {
        let resolver = resolver_from_name(resolver)?;
        let explanation =
            self.inner
                .merge_explain(&base.inner, &left.inner, &right.inner, resolver);
        let object = Object::new();
        match explanation.result {
            Ok(inner) => {
                let result: JsValue = WasmTree { inner }.into();
                Reflect::set(&object, &"result".into(), &result)?;
                Reflect::set(&object, &"error".into(), &JsValue::NULL)?;
            }
            Err(error) => {
                Reflect::set(&object, &"result".into(), &JsValue::NULL)?;
                Reflect::set(&object, &"error".into(), &error.to_string().into())?;
            }
        }
        Reflect::set(
            &object,
            &"traceJson".into(),
            &serde_json::to_string(&explanation.trace)
                .map_err(js_error)?
                .into(),
        )?;
        Ok(object)
    }

    #[wasm_bindgen(js_name = mergeRange)]
    pub fn merge_range(
        &self,
        base: &WasmTree,
        left: &WasmTree,
        right: &WasmTree,
        start: Uint8Array,
        end: Option<Uint8Array>,
        resolver: Option<String>,
    ) -> Result<WasmTree, JsValue> {
        let start = start.to_vec();
        let end = end.map(|value| value.to_vec());
        let resolver = resolver_from_name(resolver)?;
        self.inner
            .merge_range(
                &base.inner,
                &left.inner,
                &right.inner,
                &start,
                end.as_deref(),
                resolver,
            )
            .map(|inner| WasmTree { inner })
            .map_err(js_error)
    }

    #[wasm_bindgen(js_name = mergePrefix)]
    pub fn merge_prefix(
        &self,
        base: &WasmTree,
        left: &WasmTree,
        right: &WasmTree,
        prefix: Uint8Array,
        resolver: Option<String>,
    ) -> Result<WasmTree, JsValue> {
        let resolver = resolver_from_name(resolver)?;
        self.inner
            .merge_prefix(
                &base.inner,
                &left.inner,
                &right.inner,
                &prefix.to_vec(),
                resolver,
            )
            .map(|inner| WasmTree { inner })
            .map_err(js_error)
    }

    #[wasm_bindgen(js_name = conflictPage)]
    pub fn conflict_page(
        &self,
        base: &WasmTree,
        left: &WasmTree,
        right: &WasmTree,
        cursor: Option<WasmRangeCursor>,
        limit: u32,
    ) -> Result<Object, JsValue> {
        let after_key = cursor.and_then(|cursor| cursor.inner.after().map(Vec::from));
        let conflicts = Array::new();
        let mut last_emitted_key: Option<Vec<u8>> = None;
        let mut has_more = false;
        let limit = limit as usize;

        if limit > 0 {
            for conflict in self
                .inner
                .stream_conflicts(&base.inner, &left.inner, &right.inner)
                .map_err(js_error)?
            {
                let conflict = conflict.map_err(js_error)?;
                if after_key
                    .as_ref()
                    .is_some_and(|after_key| conflict.key.as_slice() <= after_key.as_slice())
                {
                    continue;
                }
                if conflicts.length() as usize == limit {
                    has_more = true;
                    break;
                }
                last_emitted_key = Some(conflict.key.clone());
                let object: JsValue = conflict_to_object(conflict)?.into();
                conflicts.push(&object);
            }
        }

        let object = Object::new();
        Reflect::set(&object, &"conflicts".into(), &conflicts.into())?;
        let next_cursor = if has_more {
            last_emitted_key
                .map(|key| WasmRangeCursor {
                    inner: RangeCursor::after_key(key),
                })
                .map(JsValue::from)
                .unwrap_or(JsValue::NULL)
        } else {
            JsValue::NULL
        };
        Reflect::set(&object, &"nextCursor".into(), &next_cursor)?;
        Ok(object)
    }

    #[wasm_bindgen(js_name = collectStatsJson)]
    pub fn collect_stats_json(&self, tree: &WasmTree) -> Result<String, JsValue> {
        self.inner
            .collect_stats(&tree.inner)
            .map_err(js_error)
            .and_then(|stats| serde_json::to_string(&stats).map_err(js_error))
    }

    #[wasm_bindgen(js_name = statsDiffJson)]
    pub fn stats_diff_json(&self, before: &WasmTree, after: &WasmTree) -> Result<String, JsValue> {
        self.inner
            .stats_diff(&before.inner, &after.inner)
            .map_err(js_error)
            .and_then(|stats| serde_json::to_string(&stats).map_err(js_error))
    }

    #[wasm_bindgen(js_name = debugTreeJson)]
    pub fn debug_tree_json(&self, tree: &WasmTree) -> Result<String, JsValue> {
        self.inner
            .debug_tree(&tree.inner)
            .map_err(js_error)
            .and_then(|view| serde_json::to_string(&view).map_err(js_error))
    }

    #[wasm_bindgen(js_name = debugTreeText)]
    pub fn debug_tree_text(&self, tree: &WasmTree) -> Result<String, JsValue> {
        self.inner
            .debug_tree(&tree.inner)
            .map(|view| view.to_text())
            .map_err(js_error)
    }

    #[wasm_bindgen(js_name = debugCompareTreesJson)]
    pub fn debug_compare_trees_json(
        &self,
        left: &WasmTree,
        right: &WasmTree,
    ) -> Result<String, JsValue> {
        self.inner
            .debug_compare_trees(&left.inner, &right.inner)
            .map_err(js_error)
            .and_then(|comparison| serde_json::to_string(&comparison).map_err(js_error))
    }

    #[wasm_bindgen(js_name = debugCompareTreesText)]
    pub fn debug_compare_trees_text(
        &self,
        left: &WasmTree,
        right: &WasmTree,
    ) -> Result<String, JsValue> {
        self.inner
            .debug_compare_trees(&left.inner, &right.inner)
            .map(|comparison| comparison.to_text())
            .map_err(js_error)
    }
}

#[wasm_bindgen(js_name = cidFromBytes)]
pub fn cid_from_bytes(bytes: Uint8Array) -> Vec<u8> {
    Cid::from_bytes(&bytes.to_vec()).as_bytes().to_vec()
}

#[wasm_bindgen(js_name = nodeBytesRoundTrip)]
pub fn node_bytes_round_trip(bytes: Uint8Array) -> Result<Vec<u8>, JsValue> {
    Node::from_bytes(&bytes.to_vec())
        .map(|node| node.to_bytes())
        .map_err(js_error)
}

#[wasm_bindgen(js_name = nodeCidFromBytes)]
pub fn node_cid_from_bytes(bytes: Uint8Array) -> Result<Vec<u8>, JsValue> {
    Node::from_bytes(&bytes.to_vec())
        .map(|node| node.cid().as_bytes().to_vec())
        .map_err(js_error)
}

#[wasm_bindgen(js_name = verifyKeyProof)]
pub fn verify_key_proof_wasm(
    root: Option<Uint8Array>,
    key: Uint8Array,
    path_node_bytes: Array,
) -> Result<Object, JsValue> {
    let proof = key_proof_from_parts(root, key, path_node_bytes)?;
    key_proof_verification_to_object(prolly::verify_key_proof(&proof))
}

#[wasm_bindgen(js_name = keyProofFromNodeBytes)]
pub fn key_proof_from_node_bytes_wasm(
    root: Option<Uint8Array>,
    key: Uint8Array,
    path_node_bytes: Array,
) -> Result<Object, JsValue> {
    key_proof_from_parts(root, key, path_node_bytes).and_then(key_proof_to_object)
}

#[wasm_bindgen(js_name = keyProofToBytes)]
pub fn key_proof_to_bytes_wasm(
    root: Option<Uint8Array>,
    key: Uint8Array,
    path_node_bytes: Array,
) -> Result<Vec<u8>, JsValue> {
    key_proof_from_parts(root, key, path_node_bytes)?
        .to_bundle_bytes()
        .map_err(js_error)
}

#[wasm_bindgen(js_name = keyProofFromBytes)]
pub fn key_proof_from_bytes_wasm(bytes: Uint8Array) -> Result<Object, JsValue> {
    KeyProof::from_bundle_bytes(&bytes.to_vec())
        .map_err(js_error)
        .and_then(key_proof_to_object)
}

#[wasm_bindgen(js_name = verifyMultiKeyProof)]
pub fn verify_multi_key_proof_wasm(
    root: Option<Uint8Array>,
    keys: Array,
    path_node_bytes: Array,
) -> Result<Object, JsValue> {
    let proof = multi_key_proof_from_parts(root, keys, path_node_bytes)?;
    multi_key_proof_verification_to_object(prolly::verify_multi_key_proof(&proof))
}

#[wasm_bindgen(js_name = multiKeyProofFromNodeBytes)]
pub fn multi_key_proof_from_node_bytes_wasm(
    root: Option<Uint8Array>,
    keys: Array,
    path_node_bytes: Array,
) -> Result<Object, JsValue> {
    multi_key_proof_from_parts(root, keys, path_node_bytes).and_then(multi_key_proof_to_object)
}

#[wasm_bindgen(js_name = multiKeyProofToBytes)]
pub fn multi_key_proof_to_bytes_wasm(
    root: Option<Uint8Array>,
    keys: Array,
    path_node_bytes: Array,
) -> Result<Vec<u8>, JsValue> {
    multi_key_proof_from_parts(root, keys, path_node_bytes)?
        .to_bundle_bytes()
        .map_err(js_error)
}

#[wasm_bindgen(js_name = multiKeyProofFromBytes)]
pub fn multi_key_proof_from_bytes_wasm(bytes: Uint8Array) -> Result<Object, JsValue> {
    MultiKeyProof::from_bundle_bytes(&bytes.to_vec())
        .map_err(js_error)
        .and_then(multi_key_proof_to_object)
}

#[wasm_bindgen(js_name = verifyRangeProof)]
pub fn verify_range_proof_wasm(
    root: Option<Uint8Array>,
    start: Uint8Array,
    end: Option<Uint8Array>,
    path_node_bytes: Array,
) -> Result<Object, JsValue> {
    let proof = range_proof_from_parts(root, start, end, path_node_bytes)?;
    range_proof_verification_to_object(prolly::verify_range_proof(&proof))
}

#[wasm_bindgen(js_name = rangeProofFromNodeBytes)]
pub fn range_proof_from_node_bytes_wasm(
    root: Option<Uint8Array>,
    start: Uint8Array,
    end: Option<Uint8Array>,
    path_node_bytes: Array,
) -> Result<Object, JsValue> {
    range_proof_from_parts(root, start, end, path_node_bytes).and_then(range_proof_to_object)
}

#[wasm_bindgen(js_name = rangeProofToBytes)]
pub fn range_proof_to_bytes_wasm(
    root: Option<Uint8Array>,
    start: Uint8Array,
    end: Option<Uint8Array>,
    path_node_bytes: Array,
) -> Result<Vec<u8>, JsValue> {
    range_proof_from_parts(root, start, end, path_node_bytes)?
        .to_bundle_bytes()
        .map_err(js_error)
}

#[wasm_bindgen(js_name = rangeProofFromBytes)]
pub fn range_proof_from_bytes_wasm(bytes: Uint8Array) -> Result<Object, JsValue> {
    RangeProof::from_bundle_bytes(&bytes.to_vec())
        .map_err(js_error)
        .and_then(range_proof_to_object)
}

#[wasm_bindgen(js_name = verifyRangePageProof)]
pub fn verify_range_page_proof_wasm(
    root: Option<Uint8Array>,
    after: Option<Uint8Array>,
    end: Option<Uint8Array>,
    path_node_bytes: Array,
) -> Result<Object, JsValue> {
    let proof = range_page_proof_from_parts(root, after, end, path_node_bytes)?;
    range_page_proof_verification_to_object(prolly::verify_range_page_proof(&proof))
}

#[wasm_bindgen(js_name = rangePageProofFromNodeBytes)]
pub fn range_page_proof_from_node_bytes_wasm(
    root: Option<Uint8Array>,
    after: Option<Uint8Array>,
    end: Option<Uint8Array>,
    path_node_bytes: Array,
) -> Result<Object, JsValue> {
    range_page_proof_from_parts(root, after, end, path_node_bytes)
        .and_then(range_page_proof_to_object)
}

#[wasm_bindgen(js_name = rangePageProofToBytes)]
pub fn range_page_proof_to_bytes_wasm(
    root: Option<Uint8Array>,
    after: Option<Uint8Array>,
    end: Option<Uint8Array>,
    path_node_bytes: Array,
) -> Result<Vec<u8>, JsValue> {
    range_page_proof_from_parts(root, after, end, path_node_bytes)?
        .to_bundle_bytes()
        .map_err(js_error)
}

#[wasm_bindgen(js_name = rangePageProofFromBytes)]
pub fn range_page_proof_from_bytes_wasm(bytes: Uint8Array) -> Result<Object, JsValue> {
    RangePageProof::from_bundle_bytes(&bytes.to_vec())
        .map_err(js_error)
        .and_then(range_page_proof_to_object)
}

#[wasm_bindgen(js_name = verifyDiffPageProof)]
pub fn verify_diff_page_proof_wasm(proof: Object) -> Result<Object, JsValue> {
    let proof = diff_page_proof_from_object(&proof.into())?;
    diff_page_proof_verification_to_object(prolly::verify_diff_page_proof(&proof))
}

#[wasm_bindgen(js_name = diffPageProofToBytes)]
pub fn diff_page_proof_to_bytes_wasm(proof: Object) -> Result<Vec<u8>, JsValue> {
    diff_page_proof_from_object(&proof.into())?
        .to_bundle_bytes()
        .map_err(js_error)
}

#[wasm_bindgen(js_name = diffPageProofFromBytes)]
pub fn diff_page_proof_from_bytes_wasm(bytes: Uint8Array) -> Result<Object, JsValue> {
    DiffPageProof::from_bundle_bytes(&bytes.to_vec())
        .map_err(js_error)
        .and_then(diff_page_proof_to_object)
}

#[wasm_bindgen(js_name = inspectProofBundle)]
pub fn inspect_proof_bundle_wasm(bytes: Uint8Array) -> Result<Object, JsValue> {
    prolly::inspect_proof_bundle(&bytes.to_vec())
        .map_err(js_error)
        .and_then(proof_bundle_summary_to_object)
}

#[wasm_bindgen(js_name = verifyProofBundle)]
pub fn verify_proof_bundle_wasm(bytes: Uint8Array) -> Result<Object, JsValue> {
    prolly::verify_proof_bundle(&bytes.to_vec())
        .map_err(js_error)
        .and_then(proof_bundle_verification_to_object)
}

#[wasm_bindgen(js_name = signProofBundleHmacSha256)]
pub fn sign_proof_bundle_hmac_sha256_wasm(
    proof_bundle: Uint8Array,
    key_id: Uint8Array,
    secret: Uint8Array,
    context: Uint8Array,
    issued_at_millis: Option<String>,
    expires_at_millis: Option<String>,
    nonce: Uint8Array,
) -> Result<Object, JsValue> {
    prolly::sign_proof_bundle_hmac_sha256(
        proof_bundle.to_vec(),
        key_id.to_vec(),
        &secret.to_vec(),
        context.to_vec(),
        optional_u64_from_string(issued_at_millis)?,
        optional_u64_from_string(expires_at_millis)?,
        nonce.to_vec(),
    )
    .map_err(js_error)
    .and_then(authenticated_proof_envelope_to_object)
}

#[wasm_bindgen(js_name = verifyAuthenticatedProofEnvelope)]
pub fn verify_authenticated_proof_envelope_wasm(
    algorithm: String,
    key_id: Uint8Array,
    proof_bundle: Uint8Array,
    context: Uint8Array,
    issued_at_millis: Option<String>,
    expires_at_millis: Option<String>,
    nonce: Uint8Array,
    signature: Uint8Array,
    secret: Uint8Array,
    now_millis: Option<String>,
) -> Result<Object, JsValue> {
    let envelope = authenticated_proof_envelope_from_parts(
        algorithm,
        key_id,
        proof_bundle,
        context,
        issued_at_millis,
        expires_at_millis,
        nonce,
        signature,
    )?;
    authenticated_proof_envelope_verification_to_object(
        prolly::verify_authenticated_proof_envelope(
            &envelope,
            &secret.to_vec(),
            optional_u64_from_string(now_millis)?,
        ),
    )
}

#[wasm_bindgen(js_name = verifyAuthenticatedProofBundle)]
pub fn verify_authenticated_proof_bundle_wasm(
    envelope_bytes: Uint8Array,
    secret: Uint8Array,
    now_millis: Option<String>,
) -> Result<Object, JsValue> {
    prolly::verify_authenticated_proof_bundle(
        &envelope_bytes.to_vec(),
        &secret.to_vec(),
        optional_u64_from_string(now_millis)?,
    )
    .map_err(js_error)
    .and_then(authenticated_proof_bundle_verification_to_object)
}

#[wasm_bindgen(js_name = authenticatedProofEnvelopeToBytes)]
pub fn authenticated_proof_envelope_to_bytes_wasm(
    algorithm: String,
    key_id: Uint8Array,
    proof_bundle: Uint8Array,
    context: Uint8Array,
    issued_at_millis: Option<String>,
    expires_at_millis: Option<String>,
    nonce: Uint8Array,
    signature: Uint8Array,
) -> Result<Vec<u8>, JsValue> {
    authenticated_proof_envelope_from_parts(
        algorithm,
        key_id,
        proof_bundle,
        context,
        issued_at_millis,
        expires_at_millis,
        nonce,
        signature,
    )?
    .to_bytes()
    .map_err(js_error)
}

#[wasm_bindgen(js_name = authenticatedProofEnvelopeFromBytes)]
pub fn authenticated_proof_envelope_from_bytes_wasm(bytes: Uint8Array) -> Result<Object, JsValue> {
    AuthenticatedProofEnvelope::from_bytes(&bytes.to_vec())
        .map_err(js_error)
        .and_then(authenticated_proof_envelope_to_object)
}

#[wasm_bindgen(js_name = isBoundaryConfigJson)]
pub fn is_boundary_config_json(
    config_json: &str,
    count: u32,
    key: Uint8Array,
    value: Uint8Array,
) -> Result<bool, JsValue> {
    let config = config_from_json(config_json)?;
    Ok(core_is_boundary_config(
        &config,
        count as usize,
        &key.to_vec(),
        &value.to_vec(),
    ))
}

#[wasm_bindgen(js_name = defaultParallelConfig)]
pub fn default_parallel_config_wasm() -> Result<Object, JsValue> {
    parallel_config_to_object(&ParallelConfig::default())
}

#[wasm_bindgen(js_name = prefixEnd)]
pub fn prefix_end_wasm(prefix: Uint8Array) -> Option<Vec<u8>> {
    prolly::prefix_end(prefix.to_vec())
}

#[wasm_bindgen(js_name = prefixRange)]
pub fn prefix_range_wasm(prefix: Uint8Array) -> Result<Object, JsValue> {
    let (start, end) = prolly::prefix_range(prefix.to_vec());
    range_bounds_to_object(start, end)
}

#[wasm_bindgen(js_name = u64Key)]
pub fn u64_key_wasm(value: String) -> Result<Vec<u8>, JsValue> {
    let value = value.parse::<u64>().map_err(js_error)?;
    Ok(prolly::u64_key(value).to_vec())
}

#[wasm_bindgen(js_name = u128Key)]
pub fn u128_key_wasm(value: String) -> Result<Vec<u8>, JsValue> {
    let value = value.parse::<u128>().map_err(js_error)?;
    Ok(prolly::u128_key(value).to_vec())
}

#[wasm_bindgen(js_name = i64Key)]
pub fn i64_key_wasm(value: String) -> Result<Vec<u8>, JsValue> {
    let value = value.parse::<i64>().map_err(js_error)?;
    Ok(prolly::i64_key(value).to_vec())
}

#[wasm_bindgen(js_name = i128Key)]
pub fn i128_key_wasm(value: String) -> Result<Vec<u8>, JsValue> {
    let value = value.parse::<i128>().map_err(js_error)?;
    Ok(prolly::i128_key(value).to_vec())
}

#[wasm_bindgen(js_name = timestampMillisKey)]
pub fn timestamp_millis_key_wasm(value: String) -> Result<Vec<u8>, JsValue> {
    let value = value.parse::<u64>().map_err(js_error)?;
    Ok(prolly::timestamp_millis_key(value).to_vec())
}

#[wasm_bindgen(js_name = encodeSegment)]
pub fn encode_segment_wasm(segment: Uint8Array) -> Vec<u8> {
    prolly::encode_segment(segment.to_vec())
}

#[wasm_bindgen(js_name = decodeSegments)]
pub fn decode_segments_wasm(key: Uint8Array) -> Result<Array, JsValue> {
    let segments = prolly::decode_segments(&key.to_vec()).map_err(js_error)?;
    let out = Array::new();
    for segment in segments {
        out.push(&Uint8Array::from(segment.as_slice()).into());
    }
    Ok(out)
}

#[wasm_bindgen(js_name = debugKey)]
pub fn debug_key_wasm(key: Uint8Array) -> String {
    prolly::debug_key(&key.to_vec())
}

#[wasm_bindgen(js_name = snapshotRootName)]
pub fn snapshot_root_name_wasm(
    kind: &str,
    id: Uint8Array,
    custom_prefix: Option<Uint8Array>,
) -> Result<Vec<u8>, JsValue> {
    let namespace = snapshot_namespace(kind, custom_prefix)?;
    Ok(prolly::snapshot_root_name(&namespace, id.to_vec()))
}

#[wasm_bindgen(js_name = snapshotIdFromName)]
pub fn snapshot_id_from_name_wasm(
    kind: &str,
    name: Uint8Array,
    custom_prefix: Option<Uint8Array>,
) -> Result<JsValue, JsValue> {
    let namespace = snapshot_namespace(kind, custom_prefix)?;
    Ok(prolly::snapshot_id_from_name(&namespace, name.to_vec())
        .map(|id| Uint8Array::from(id.as_slice()).into())
        .unwrap_or(JsValue::NULL))
}

fn snapshot_namespace(
    kind: &str,
    custom_prefix: Option<Uint8Array>,
) -> Result<SnapshotNamespace, JsValue> {
    match kind {
        "branch" => Ok(SnapshotNamespace::branch()),
        "tag" => Ok(SnapshotNamespace::tag()),
        "checkpoint" => Ok(SnapshotNamespace::checkpoint()),
        "custom" => custom_prefix
            .map(|prefix| SnapshotNamespace::custom(prefix.to_vec()))
            .ok_or_else(|| JsValue::from_str("custom snapshot namespace requires prefix")),
        other => Err(JsValue::from_str(&format!(
            "unknown snapshot namespace kind {other:?}"
        ))),
    }
}

fn config_from_json(json: &str) -> Result<Config, JsValue> {
    if let Ok(config) = serde_json::from_str::<Config>(json) {
        return Ok(config);
    }

    let value = serde_json::from_str::<Value>(json).map_err(js_error)?;
    let mut config = Config::default();

    if let Some(n) = value.get("min_chunk_size").and_then(Value::as_u64) {
        config.min_chunk_size = n as usize;
    }
    if let Some(n) = value.get("max_chunk_size").and_then(Value::as_u64) {
        config.max_chunk_size = n as usize;
    }
    if let Some(n) = value.get("chunking_factor").and_then(Value::as_u64) {
        config.chunking_factor = n as u32;
    }
    if let Some(n) = value.get("hash_seed").and_then(Value::as_u64) {
        config.hash_seed = n;
    }
    config.node_cache_max_nodes = value
        .get("node_cache_max_nodes")
        .and_then(Value::as_u64)
        .map(|n| n as usize);
    config.node_cache_max_bytes = value
        .get("node_cache_max_bytes")
        .and_then(Value::as_u64)
        .map(|n| n as usize);

    if let Some(encoding) = value.get("encoding") {
        let kind = encoding
            .get("kind")
            .and_then(Value::as_str)
            .unwrap_or("raw");
        config.encoding = match kind {
            "raw" | "Raw" => Encoding::Raw,
            "cbor" | "Cbor" => Encoding::Cbor,
            "json" | "Json" => Encoding::Json,
            "custom" | "Custom" => Encoding::Custom(
                encoding
                    .get("custom_name")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string(),
            ),
            other => {
                return Err(JsValue::from_str(&format!(
                    "unknown encoding kind: {other}"
                )))
            }
        };
    }

    Ok(config)
}

fn js_error(error: impl std::fmt::Display) -> JsValue {
    JsValue::from_str(&error.to_string())
}

fn optional_bytes(value: Option<Vec<u8>>) -> JsValue {
    value
        .map(|bytes| Uint8Array::from(bytes.as_slice()).into())
        .unwrap_or(JsValue::NULL)
}

fn optional_u64_from_string(value: Option<String>) -> Result<Option<u64>, JsValue> {
    value
        .map(|value| value.parse::<u64>().map_err(js_error))
        .transpose()
}

fn optional_u64_to_js(value: Option<u64>) -> JsValue {
    value
        .map(|value| JsValue::from_str(&value.to_string()))
        .unwrap_or(JsValue::NULL)
}

fn key_proof_from_parts(
    root: Option<Uint8Array>,
    key: Uint8Array,
    path_node_bytes: Array,
) -> Result<KeyProof, JsValue> {
    let root = root
        .map(|root| {
            root.to_vec()
                .try_into()
                .map(Cid)
                .map_err(|_| JsValue::from_str("root CID must be 32 bytes"))
        })
        .transpose()?;
    KeyProof::from_node_bytes(root, key.to_vec(), bytes_array(path_node_bytes)?).map_err(js_error)
}

fn multi_key_proof_from_parts(
    root: Option<Uint8Array>,
    keys: Array,
    path_node_bytes: Array,
) -> Result<MultiKeyProof, JsValue> {
    let root = root
        .map(|root| {
            root.to_vec()
                .try_into()
                .map(Cid)
                .map_err(|_| JsValue::from_str("root CID must be 32 bytes"))
        })
        .transpose()?;
    MultiKeyProof::from_node_bytes(root, bytes_array(keys)?, bytes_array(path_node_bytes)?)
        .map_err(js_error)
}

fn range_proof_from_parts(
    root: Option<Uint8Array>,
    start: Uint8Array,
    end: Option<Uint8Array>,
    path_node_bytes: Array,
) -> Result<RangeProof, JsValue> {
    let root = root
        .map(|root| {
            root.to_vec()
                .try_into()
                .map(Cid)
                .map_err(|_| JsValue::from_str("root CID must be 32 bytes"))
        })
        .transpose()?;
    RangeProof::from_node_bytes(
        root,
        start.to_vec(),
        end.map(|value| value.to_vec()),
        bytes_array(path_node_bytes)?,
    )
    .map_err(js_error)
}

fn range_page_proof_from_parts(
    root: Option<Uint8Array>,
    after: Option<Uint8Array>,
    end: Option<Uint8Array>,
    path_node_bytes: Array,
) -> Result<RangePageProof, JsValue> {
    let root = root
        .map(|root| {
            root.to_vec()
                .try_into()
                .map(Cid)
                .map_err(|_| JsValue::from_str("root CID must be 32 bytes"))
        })
        .transpose()?;
    RangePageProof::from_node_bytes(
        root,
        after.map(|value| value.to_vec()),
        end.map(|value| value.to_vec()),
        bytes_array(path_node_bytes)?,
    )
    .map_err(js_error)
}

fn key_proof_from_object(value: &JsValue) -> Result<KeyProof, JsValue> {
    key_proof_from_parts(
        object_optional_uint8_array(value, "root")?,
        object_uint8_array(value, "key")?,
        object_array(value, "pathNodeBytes")?,
    )
}

fn range_page_proof_from_object(value: &JsValue) -> Result<RangePageProof, JsValue> {
    range_page_proof_from_parts(
        object_optional_uint8_array(value, "root")?,
        object_optional_uint8_array(value, "after")?,
        object_optional_uint8_array(value, "end")?,
        object_array(value, "pathNodeBytes")?,
    )
}

fn diff_page_proof_from_object(value: &JsValue) -> Result<DiffPageProof, JsValue> {
    let limit = object_string(value, "limit")?
        .parse::<usize>()
        .map_err(js_error)?;
    Ok(DiffPageProof {
        base: range_page_proof_from_object(&Reflect::get(value, &"base".into())?)?,
        other: range_page_proof_from_object(&Reflect::get(value, &"other".into())?)?,
        lookahead_base: object_optional_value(value, "lookaheadBase")?
            .map(|value| key_proof_from_object(&value))
            .transpose()?,
        lookahead_other: object_optional_value(value, "lookaheadOther")?
            .map(|value| key_proof_from_object(&value))
            .transpose()?,
        requested_end: object_optional_uint8_array(value, "requestedEnd")?
            .map(|value| value.to_vec()),
        limit,
    })
}

fn key_proof_to_object(proof: KeyProof) -> Result<Object, JsValue> {
    let object = Object::new();
    let root = proof
        .root
        .as_ref()
        .map(|cid| Uint8Array::from(cid.as_bytes()).into())
        .unwrap_or(JsValue::NULL);
    Reflect::set(&object, &"root".into(), &root)?;
    Reflect::set(
        &object,
        &"key".into(),
        &Uint8Array::from(proof.key.as_slice()).into(),
    )?;
    let path = Array::new();
    for bytes in proof.path_node_bytes() {
        path.push(&Uint8Array::from(bytes.as_slice()).into());
    }
    Reflect::set(&object, &"pathNodeBytes".into(), &path.into())?;
    Ok(object)
}

fn multi_key_proof_to_object(proof: MultiKeyProof) -> Result<Object, JsValue> {
    let object = Object::new();
    let root = proof
        .root
        .as_ref()
        .map(|cid| Uint8Array::from(cid.as_bytes()).into())
        .unwrap_or(JsValue::NULL);
    Reflect::set(&object, &"root".into(), &root)?;
    let keys = Array::new();
    for key in &proof.keys {
        keys.push(&Uint8Array::from(key.as_slice()).into());
    }
    Reflect::set(&object, &"keys".into(), &keys.into())?;
    let path = Array::new();
    for bytes in proof.path_node_bytes() {
        path.push(&Uint8Array::from(bytes.as_slice()).into());
    }
    Reflect::set(&object, &"pathNodeBytes".into(), &path.into())?;
    Ok(object)
}

fn range_proof_to_object(proof: RangeProof) -> Result<Object, JsValue> {
    let object = Object::new();
    let root = proof
        .root
        .as_ref()
        .map(|cid| Uint8Array::from(cid.as_bytes()).into())
        .unwrap_or(JsValue::NULL);
    Reflect::set(&object, &"root".into(), &root)?;
    Reflect::set(
        &object,
        &"start".into(),
        &Uint8Array::from(proof.start.as_slice()).into(),
    )?;
    let end = proof
        .end
        .as_ref()
        .map(|end| Uint8Array::from(end.as_slice()).into())
        .unwrap_or(JsValue::NULL);
    Reflect::set(&object, &"end".into(), &end)?;
    let path = Array::new();
    for bytes in proof.path_node_bytes() {
        path.push(&Uint8Array::from(bytes.as_slice()).into());
    }
    Reflect::set(&object, &"pathNodeBytes".into(), &path.into())?;
    Ok(object)
}

fn range_page_proof_to_object(proof: RangePageProof) -> Result<Object, JsValue> {
    let object = Object::new();
    let root = proof
        .root
        .as_ref()
        .map(|cid| Uint8Array::from(cid.as_bytes()).into())
        .unwrap_or(JsValue::NULL);
    Reflect::set(&object, &"root".into(), &root)?;
    let after = proof
        .after
        .as_ref()
        .map(|after| Uint8Array::from(after.as_slice()).into())
        .unwrap_or(JsValue::NULL);
    Reflect::set(&object, &"after".into(), &after)?;
    let end = proof
        .end
        .as_ref()
        .map(|end| Uint8Array::from(end.as_slice()).into())
        .unwrap_or(JsValue::NULL);
    Reflect::set(&object, &"end".into(), &end)?;
    let path = Array::new();
    for bytes in proof.path_node_bytes() {
        path.push(&Uint8Array::from(bytes.as_slice()).into());
    }
    Reflect::set(&object, &"pathNodeBytes".into(), &path.into())?;
    Ok(object)
}

fn diff_page_proof_to_object(proof: DiffPageProof) -> Result<Object, JsValue> {
    let object = Object::new();
    Reflect::set(
        &object,
        &"base".into(),
        &range_page_proof_to_object(proof.base)?.into(),
    )?;
    Reflect::set(
        &object,
        &"other".into(),
        &range_page_proof_to_object(proof.other)?.into(),
    )?;
    let lookahead_base = proof
        .lookahead_base
        .map(key_proof_to_object)
        .transpose()?
        .map(JsValue::from)
        .unwrap_or(JsValue::NULL);
    Reflect::set(&object, &"lookaheadBase".into(), &lookahead_base)?;
    let lookahead_other = proof
        .lookahead_other
        .map(key_proof_to_object)
        .transpose()?
        .map(JsValue::from)
        .unwrap_or(JsValue::NULL);
    Reflect::set(&object, &"lookaheadOther".into(), &lookahead_other)?;
    Reflect::set(
        &object,
        &"requestedEnd".into(),
        &optional_bytes(proof.requested_end),
    )?;
    Reflect::set(
        &object,
        &"limit".into(),
        &JsValue::from_str(&proof.limit.to_string()),
    )?;
    Ok(object)
}

fn proved_range_page_to_object(page: prolly::ProvedRangePage) -> Result<Object, JsValue> {
    let object = Object::new();
    let page_object = Object::new();
    Reflect::set(
        &page_object,
        &"entries".into(),
        &entries_to_array(page.page.entries)?.into(),
    )?;
    let cursor_value = page
        .page
        .next_cursor
        .map(|inner| WasmRangeCursor { inner }.into())
        .unwrap_or(JsValue::NULL);
    Reflect::set(&page_object, &"nextCursor".into(), &cursor_value)?;
    Reflect::set(&object, &"page".into(), &page_object)?;
    Reflect::set(
        &object,
        &"proof".into(),
        &range_page_proof_to_object(page.proof)?.into(),
    )?;
    Ok(object)
}

fn proved_diff_page_to_object(page: prolly::ProvedDiffPage) -> Result<Object, JsValue> {
    let object = Object::new();
    let page_object = Object::new();
    Reflect::set(
        &page_object,
        &"diffs".into(),
        &diffs_to_array(page.page.diffs)?.into(),
    )?;
    Reflect::set(
        &page_object,
        &"nextCursor".into(),
        &range_cursor_value(page.page.next_cursor),
    )?;
    Reflect::set(&object, &"page".into(), &page_object)?;
    Reflect::set(
        &object,
        &"proof".into(),
        &diff_page_proof_to_object(page.proof)?.into(),
    )?;
    Ok(object)
}

#[allow(clippy::too_many_arguments)]
fn authenticated_proof_envelope_from_parts(
    algorithm: String,
    key_id: Uint8Array,
    proof_bundle: Uint8Array,
    context: Uint8Array,
    issued_at_millis: Option<String>,
    expires_at_millis: Option<String>,
    nonce: Uint8Array,
    signature: Uint8Array,
) -> Result<AuthenticatedProofEnvelope, JsValue> {
    Ok(AuthenticatedProofEnvelope {
        algorithm,
        key_id: key_id.to_vec(),
        proof_bundle: proof_bundle.to_vec(),
        context: context.to_vec(),
        issued_at_millis: optional_u64_from_string(issued_at_millis)?,
        expires_at_millis: optional_u64_from_string(expires_at_millis)?,
        nonce: nonce.to_vec(),
        signature: signature.to_vec(),
    })
}

fn authenticated_proof_envelope_to_object(
    envelope: AuthenticatedProofEnvelope,
) -> Result<Object, JsValue> {
    let object = Object::new();
    Reflect::set(&object, &"algorithm".into(), &envelope.algorithm.into())?;
    Reflect::set(
        &object,
        &"keyId".into(),
        &Uint8Array::from(envelope.key_id.as_slice()).into(),
    )?;
    Reflect::set(
        &object,
        &"proofBundle".into(),
        &Uint8Array::from(envelope.proof_bundle.as_slice()).into(),
    )?;
    Reflect::set(
        &object,
        &"context".into(),
        &Uint8Array::from(envelope.context.as_slice()).into(),
    )?;
    Reflect::set(
        &object,
        &"issuedAtMillis".into(),
        &optional_u64_to_js(envelope.issued_at_millis),
    )?;
    Reflect::set(
        &object,
        &"expiresAtMillis".into(),
        &optional_u64_to_js(envelope.expires_at_millis),
    )?;
    Reflect::set(
        &object,
        &"nonce".into(),
        &Uint8Array::from(envelope.nonce.as_slice()).into(),
    )?;
    Reflect::set(
        &object,
        &"signature".into(),
        &Uint8Array::from(envelope.signature.as_slice()).into(),
    )?;
    Ok(object)
}

fn proof_bundle_summary_to_object(summary: prolly::ProofBundleSummary) -> Result<Object, JsValue> {
    let object = Object::new();
    Reflect::set(
        &object,
        &"version".into(),
        &summary.version.to_string().into(),
    )?;
    Reflect::set(&object, &"kind".into(), &summary.kind.as_str().into())?;
    Reflect::set(
        &object,
        &"root".into(),
        &optional_bytes(summary.root.map(|cid| cid.as_bytes().to_vec())),
    )?;
    Reflect::set(
        &object,
        &"otherRoot".into(),
        &optional_bytes(summary.other_root.map(|cid| cid.as_bytes().to_vec())),
    )?;
    Reflect::set(
        &object,
        &"keyCount".into(),
        &summary.key_count.to_string().into(),
    )?;
    Reflect::set(
        &object,
        &"pathNodeCount".into(),
        &summary.path_node_count.to_string().into(),
    )?;
    Reflect::set(&object, &"start".into(), &optional_bytes(summary.start))?;
    Reflect::set(&object, &"end".into(), &optional_bytes(summary.end))?;
    Reflect::set(&object, &"after".into(), &optional_bytes(summary.after))?;
    Reflect::set(
        &object,
        &"requestedEnd".into(),
        &optional_bytes(summary.requested_end),
    )?;
    Reflect::set(
        &object,
        &"limit".into(),
        &summary
            .limit
            .map(|limit| JsValue::from(limit.to_string()))
            .unwrap_or(JsValue::NULL),
    )?;
    Reflect::set(
        &object,
        &"hasLookahead".into(),
        &summary.has_lookahead.into(),
    )?;
    Ok(object)
}

fn proof_bundle_verification_to_object(
    verification: prolly::ProofBundleVerification,
) -> Result<Object, JsValue> {
    let object = Object::new();
    Reflect::set(
        &object,
        &"summary".into(),
        &proof_bundle_summary_to_object(verification.summary)?.into(),
    )?;
    Reflect::set(&object, &"valid".into(), &verification.valid.into())?;
    Reflect::set(
        &object,
        &"existsCount".into(),
        &verification.exists_count.to_string().into(),
    )?;
    Reflect::set(
        &object,
        &"absenceCount".into(),
        &verification.absence_count.to_string().into(),
    )?;
    Reflect::set(
        &object,
        &"entryCount".into(),
        &verification.entry_count.to_string().into(),
    )?;
    Reflect::set(
        &object,
        &"diffCount".into(),
        &verification.diff_count.to_string().into(),
    )?;
    Reflect::set(
        &object,
        &"nextCursor".into(),
        &range_cursor_value(verification.next_cursor),
    )?;
    Ok(object)
}

fn key_proof_verification_to_object(
    verification: prolly::KeyProofVerification,
) -> Result<Object, JsValue> {
    let object = Object::new();
    Reflect::set(&object, &"valid".into(), &verification.valid.into())?;
    Reflect::set(&object, &"exists".into(), &verification.exists().into())?;
    Reflect::set(
        &object,
        &"absence".into(),
        &verification.is_absence().into(),
    )?;
    let root = verification
        .root
        .as_ref()
        .map(|cid| Uint8Array::from(cid.as_bytes()).into())
        .unwrap_or(JsValue::NULL);
    Reflect::set(&object, &"root".into(), &root)?;
    Reflect::set(
        &object,
        &"key".into(),
        &Uint8Array::from(verification.key.as_slice()).into(),
    )?;
    Reflect::set(
        &object,
        &"value".into(),
        &optional_bytes(verification.value),
    )?;
    Ok(object)
}

fn multi_key_proof_verification_to_object(
    verification: prolly::MultiKeyProofVerification,
) -> Result<Object, JsValue> {
    let object = Object::new();
    Reflect::set(&object, &"valid".into(), &verification.valid.into())?;
    let root = verification
        .root
        .as_ref()
        .map(|cid| Uint8Array::from(cid.as_bytes()).into())
        .unwrap_or(JsValue::NULL);
    Reflect::set(&object, &"root".into(), &root)?;
    let results = Array::new();
    for result in verification.results {
        results.push(&key_proof_verification_to_object(result)?.into());
    }
    Reflect::set(&object, &"results".into(), &results.into())?;
    Ok(object)
}

fn range_proof_verification_to_object(
    verification: prolly::RangeProofVerification,
) -> Result<Object, JsValue> {
    let object = Object::new();
    Reflect::set(&object, &"valid".into(), &verification.valid.into())?;
    let root = verification
        .root
        .as_ref()
        .map(|cid| Uint8Array::from(cid.as_bytes()).into())
        .unwrap_or(JsValue::NULL);
    Reflect::set(&object, &"root".into(), &root)?;
    Reflect::set(
        &object,
        &"start".into(),
        &Uint8Array::from(verification.start.as_slice()).into(),
    )?;
    let end = verification
        .end
        .as_ref()
        .map(|end| Uint8Array::from(end.as_slice()).into())
        .unwrap_or(JsValue::NULL);
    Reflect::set(&object, &"end".into(), &end)?;
    Reflect::set(
        &object,
        &"entries".into(),
        &entries_to_array(verification.entries)?.into(),
    )?;
    Ok(object)
}

fn range_page_proof_verification_to_object(
    verification: prolly::RangePageProofVerification,
) -> Result<Object, JsValue> {
    let object = Object::new();
    Reflect::set(&object, &"valid".into(), &verification.valid.into())?;
    let root = verification
        .root
        .as_ref()
        .map(|cid| Uint8Array::from(cid.as_bytes()).into())
        .unwrap_or(JsValue::NULL);
    Reflect::set(&object, &"root".into(), &root)?;
    let after = verification
        .after
        .as_ref()
        .map(|after| Uint8Array::from(after.as_slice()).into())
        .unwrap_or(JsValue::NULL);
    Reflect::set(&object, &"after".into(), &after)?;
    let end = verification
        .end
        .as_ref()
        .map(|end| Uint8Array::from(end.as_slice()).into())
        .unwrap_or(JsValue::NULL);
    Reflect::set(&object, &"end".into(), &end)?;
    Reflect::set(
        &object,
        &"entries".into(),
        &entries_to_array(verification.entries)?.into(),
    )?;
    Ok(object)
}

fn diff_page_proof_verification_to_object(
    verification: prolly::DiffPageProofVerification,
) -> Result<Object, JsValue> {
    let object = Object::new();
    Reflect::set(&object, &"valid".into(), &verification.valid.into())?;
    Reflect::set(
        &object,
        &"baseValid".into(),
        &verification.base_valid.into(),
    )?;
    Reflect::set(
        &object,
        &"otherValid".into(),
        &verification.other_valid.into(),
    )?;
    Reflect::set(
        &object,
        &"lookaheadValid".into(),
        &verification.lookahead_valid.into(),
    )?;
    let base_root = verification
        .base_root
        .as_ref()
        .map(|cid| Uint8Array::from(cid.as_bytes()).into())
        .unwrap_or(JsValue::NULL);
    Reflect::set(&object, &"baseRoot".into(), &base_root)?;
    let other_root = verification
        .other_root
        .as_ref()
        .map(|cid| Uint8Array::from(cid.as_bytes()).into())
        .unwrap_or(JsValue::NULL);
    Reflect::set(&object, &"otherRoot".into(), &other_root)?;
    Reflect::set(
        &object,
        &"after".into(),
        &optional_bytes(verification.after),
    )?;
    Reflect::set(
        &object,
        &"requestedEnd".into(),
        &optional_bytes(verification.requested_end),
    )?;
    Reflect::set(
        &object,
        &"proofEnd".into(),
        &optional_bytes(verification.proof_end),
    )?;
    Reflect::set(
        &object,
        &"limit".into(),
        &JsValue::from_str(&verification.limit.to_string()),
    )?;
    Reflect::set(
        &object,
        &"diffs".into(),
        &diffs_to_array(verification.diffs)?.into(),
    )?;
    Reflect::set(
        &object,
        &"nextCursor".into(),
        &range_cursor_value(verification.next_cursor),
    )?;
    Ok(object)
}

fn authenticated_proof_envelope_verification_to_object(
    verification: prolly::AuthenticatedProofEnvelopeVerification,
) -> Result<Object, JsValue> {
    let object = Object::new();
    Reflect::set(&object, &"valid".into(), &verification.valid.into())?;
    Reflect::set(
        &object,
        &"signatureValid".into(),
        &verification.signature_valid.into(),
    )?;
    Reflect::set(
        &object,
        &"timeValid".into(),
        &verification.time_valid.into(),
    )?;
    Reflect::set(
        &object,
        &"notYetValid".into(),
        &verification.not_yet_valid.into(),
    )?;
    Reflect::set(&object, &"expired".into(), &verification.expired.into())?;
    Reflect::set(&object, &"algorithm".into(), &verification.algorithm.into())?;
    Reflect::set(
        &object,
        &"keyId".into(),
        &Uint8Array::from(verification.key_id.as_slice()).into(),
    )?;
    Reflect::set(
        &object,
        &"proofBundle".into(),
        &Uint8Array::from(verification.proof_bundle.as_slice()).into(),
    )?;
    Reflect::set(
        &object,
        &"context".into(),
        &Uint8Array::from(verification.context.as_slice()).into(),
    )?;
    Reflect::set(
        &object,
        &"issuedAtMillis".into(),
        &optional_u64_to_js(verification.issued_at_millis),
    )?;
    Reflect::set(
        &object,
        &"expiresAtMillis".into(),
        &optional_u64_to_js(verification.expires_at_millis),
    )?;
    Reflect::set(
        &object,
        &"nonce".into(),
        &Uint8Array::from(verification.nonce.as_slice()).into(),
    )?;
    Ok(object)
}

fn authenticated_proof_bundle_verification_to_object(
    verification: prolly::AuthenticatedProofBundleVerification,
) -> Result<Object, JsValue> {
    let object = Object::new();
    Reflect::set(&object, &"valid".into(), &verification.valid.into())?;
    Reflect::set(
        &object,
        &"envelope".into(),
        &authenticated_proof_envelope_verification_to_object(verification.envelope)?.into(),
    )?;
    let proof = match verification.proof {
        Some(proof) => proof_bundle_verification_to_object(proof)?.into(),
        None => JsValue::NULL,
    };
    Reflect::set(&object, &"proof".into(), &proof)?;
    let proof_error = verification
        .proof_error
        .map(|error| JsValue::from_str(&error))
        .unwrap_or(JsValue::NULL);
    Reflect::set(&object, &"proofError".into(), &proof_error)?;
    Ok(object)
}

fn bytes_array(values: Array) -> Result<Vec<Vec<u8>>, JsValue> {
    values
        .iter()
        .map(|value| uint8_array(value, "expected Uint8Array").map(|bytes| bytes.to_vec()))
        .collect()
}

fn entries_array(values: Array) -> Result<Vec<(Vec<u8>, Vec<u8>)>, JsValue> {
    values
        .iter()
        .map(|value| Ok((object_bytes(&value, "key")?, object_bytes(&value, "value")?)))
        .collect()
}

fn mutations_array(values: Array) -> Result<Vec<Mutation>, JsValue> {
    values.iter().map(mutation_from_js).collect()
}

fn parallel_config_from_js(value: &JsValue) -> Result<ParallelConfig, JsValue> {
    Ok(ParallelConfig::new(
        object_usize(value, "maxThreads", "max_threads")?,
        object_usize(value, "parallelismThreshold", "parallelism_threshold")?,
    ))
}

fn parallel_config_to_object(config: &ParallelConfig) -> Result<Object, JsValue> {
    let object = Object::new();
    Reflect::set(
        &object,
        &"maxThreads".into(),
        &JsValue::from_str(&config.max_threads.to_string()),
    )?;
    Reflect::set(
        &object,
        &"parallelismThreshold".into(),
        &JsValue::from_str(&config.parallelism_threshold.to_string()),
    )?;
    Ok(object)
}

fn mutation_from_js(value: JsValue) -> Result<Mutation, JsValue> {
    let kind = Reflect::get(&value, &"kind".into())?
        .as_string()
        .ok_or_else(|| JsValue::from_str("mutation.kind must be a string"))?;
    let key = object_bytes(&value, "key")?;
    match kind.as_str() {
        "upsert" => Ok(Mutation::Upsert {
            key,
            val: object_bytes(&value, "value")?,
        }),
        "delete" => Ok(Mutation::Delete { key }),
        other => Err(JsValue::from_str(&format!(
            "unknown mutation kind: {other}"
        ))),
    }
}

fn collect_entries<I>(entries: I) -> Result<Array, JsValue>
where
    I: Iterator<Item = Result<(Vec<u8>, Vec<u8>), Error>>,
{
    let out = Array::new();
    for entry in entries {
        let (key, value) = entry.map_err(js_error)?;
        let object: JsValue = entry_object(key, value)?.into();
        out.push(&object);
    }
    Ok(out)
}

fn entries_to_array(entries: Vec<(Vec<u8>, Vec<u8>)>) -> Result<Array, JsValue> {
    let out = Array::new();
    for (key, value) in entries {
        let object: JsValue = entry_object(key, value)?.into();
        out.push(&object);
    }
    Ok(out)
}

fn diffs_to_array(diffs: Vec<Diff>) -> Result<Array, JsValue> {
    let out = Array::new();
    for diff in diffs {
        let object: JsValue = diff_to_object(diff)?.into();
        out.push(&object);
    }
    Ok(out)
}

fn range_cursor_value(cursor: Option<RangeCursor>) -> JsValue {
    cursor
        .map(|inner| WasmRangeCursor { inner }.into())
        .unwrap_or(JsValue::NULL)
}

fn range_bounds_to_object(start: Vec<u8>, end: Option<Vec<u8>>) -> Result<Object, JsValue> {
    let object = Object::new();
    Reflect::set(
        &object,
        &"start".into(),
        &Uint8Array::from(start.as_slice()).into(),
    )?;
    Reflect::set(&object, &"end".into(), &optional_bytes(end))?;
    Ok(object)
}

fn entry_object(key: Vec<u8>, value: Vec<u8>) -> Result<Object, JsValue> {
    let object = Object::new();
    Reflect::set(
        &object,
        &"key".into(),
        &Uint8Array::from(key.as_slice()).into(),
    )?;
    Reflect::set(
        &object,
        &"value".into(),
        &Uint8Array::from(value.as_slice()).into(),
    )?;
    Ok(object)
}

fn conflict_to_object(conflict: Conflict) -> Result<Object, JsValue> {
    let object = Object::new();
    Reflect::set(
        &object,
        &"key".into(),
        &Uint8Array::from(conflict.key.as_slice()).into(),
    )?;
    Reflect::set(&object, &"base".into(), &optional_bytes(conflict.base))?;
    Reflect::set(&object, &"left".into(), &optional_bytes(conflict.left))?;
    Reflect::set(&object, &"right".into(), &optional_bytes(conflict.right))?;
    Ok(object)
}

fn diff_to_object(diff: Diff) -> Result<Object, JsValue> {
    let object = Object::new();
    match diff {
        Diff::Added { key, val } => {
            Reflect::set(&object, &"kind".into(), &"added".into())?;
            Reflect::set(
                &object,
                &"key".into(),
                &Uint8Array::from(key.as_slice()).into(),
            )?;
            Reflect::set(
                &object,
                &"value".into(),
                &Uint8Array::from(val.as_slice()).into(),
            )?;
        }
        Diff::Removed { key, val } => {
            Reflect::set(&object, &"kind".into(), &"removed".into())?;
            Reflect::set(
                &object,
                &"key".into(),
                &Uint8Array::from(key.as_slice()).into(),
            )?;
            Reflect::set(
                &object,
                &"value".into(),
                &Uint8Array::from(val.as_slice()).into(),
            )?;
        }
        Diff::Changed { key, old, new } => {
            Reflect::set(&object, &"kind".into(), &"changed".into())?;
            Reflect::set(
                &object,
                &"key".into(),
                &Uint8Array::from(key.as_slice()).into(),
            )?;
            Reflect::set(
                &object,
                &"old".into(),
                &Uint8Array::from(old.as_slice()).into(),
            )?;
            Reflect::set(
                &object,
                &"newValue".into(),
                &Uint8Array::from(new.as_slice()).into(),
            )?;
        }
    }
    Ok(object)
}

fn batch_apply_result_to_object(result: BatchApplyResult) -> Result<Object, JsValue> {
    let object = Object::new();
    let tree: JsValue = WasmTree { inner: result.tree }.into();
    Reflect::set(&object, &"tree".into(), &tree)?;
    Reflect::set(
        &object,
        &"stats".into(),
        &batch_apply_stats_to_object(result.stats)?,
    )?;
    Ok(object)
}

fn batch_apply_stats_to_object(stats: BatchApplyStats) -> Result<JsValue, JsValue> {
    let object = Object::new();
    Reflect::set(
        &object,
        &"inputMutations".into(),
        &JsValue::from_f64(stats.input_mutations as f64),
    )?;
    Reflect::set(
        &object,
        &"effectiveMutations".into(),
        &JsValue::from_f64(stats.effective_mutations as f64),
    )?;
    Reflect::set(
        &object,
        &"preprocessInputSorted".into(),
        &JsValue::from_bool(stats.preprocess_input_sorted),
    )?;
    Reflect::set(
        &object,
        &"affectedLeaves".into(),
        &JsValue::from_f64(stats.affected_leaves as f64),
    )?;
    Reflect::set(
        &object,
        &"changedLeaves".into(),
        &JsValue::from_f64(stats.changed_leaves as f64),
    )?;
    Reflect::set(
        &object,
        &"sparseLeafApplies".into(),
        &JsValue::from_f64(stats.sparse_leaf_applies as f64),
    )?;
    Reflect::set(
        &object,
        &"writtenNodes".into(),
        &JsValue::from_f64(stats.written_nodes as f64),
    )?;
    Reflect::set(
        &object,
        &"writtenBytes".into(),
        &JsValue::from_f64(stats.written_bytes as f64),
    )?;
    Reflect::set(
        &object,
        &"usedAppendFastPath".into(),
        &JsValue::from_bool(stats.used_append_fast_path),
    )?;
    Reflect::set(
        &object,
        &"usedBatchedRoute".into(),
        &JsValue::from_bool(stats.used_batched_route),
    )?;
    Reflect::set(
        &object,
        &"usedCoalescedRebuild".into(),
        &JsValue::from_bool(stats.used_coalesced_rebuild),
    )?;
    Reflect::set(
        &object,
        &"usedDeferredRebalancing".into(),
        &JsValue::from_bool(stats.used_deferred_rebalancing),
    )?;
    Reflect::set(
        &object,
        &"usedBottomUpRebuild".into(),
        &JsValue::from_bool(stats.used_bottom_up_rebuild),
    )?;
    Reflect::set(
        &object,
        &"cacheWrittenNodes".into(),
        &JsValue::from_bool(stats.cache_written_nodes),
    )?;
    Ok(object.into())
}

fn diff_stats_to_object(stats: prolly::DiffTraversalStats) -> Result<JsValue, JsValue> {
    let object = Object::new();
    Reflect::set(
        &object,
        &"comparedNodes".into(),
        &JsValue::from_f64(stats.compared_nodes as f64),
    )?;
    Reflect::set(
        &object,
        &"reusedSubtrees".into(),
        &JsValue::from_f64(stats.reused_subtrees as f64),
    )?;
    Reflect::set(
        &object,
        &"addedSubtrees".into(),
        &JsValue::from_f64(stats.added_subtrees as f64),
    )?;
    Reflect::set(
        &object,
        &"removedSubtrees".into(),
        &JsValue::from_f64(stats.removed_subtrees as f64),
    )?;
    Reflect::set(
        &object,
        &"collectedFallbacks".into(),
        &JsValue::from_f64(stats.collected_fallbacks as f64),
    )?;
    Reflect::set(
        &object,
        &"emittedDiffs".into(),
        &JsValue::from_f64(stats.emitted_diffs as f64),
    )?;
    Ok(object.into())
}

fn resolver_from_name(name: Option<String>) -> Result<Option<Resolver>, JsValue> {
    let Some(name) = name else {
        return Ok(None);
    };

    let resolver: Resolver = match name.as_str() {
        "prefer_left" => Box::new(prolly::resolver::prefer_left),
        "prefer_right" => Box::new(prolly::resolver::prefer_right),
        "delete_wins" => Box::new(prolly::resolver::delete_wins),
        "update_wins" => Box::new(prolly::resolver::update_wins),
        other => {
            return Err(JsValue::from_str(&format!(
                "unknown resolver name: {other}"
            )))
        }
    };
    Ok(Some(resolver))
}

fn object_bytes(value: &JsValue, field: &str) -> Result<Vec<u8>, JsValue> {
    let field_value = Reflect::get(value, &field.into())?;
    uint8_array(field_value, &format!("{field} must be a Uint8Array")).map(|bytes| bytes.to_vec())
}

fn object_uint8_array(value: &JsValue, field: &str) -> Result<Uint8Array, JsValue> {
    let field_value = Reflect::get(value, &field.into())?;
    uint8_array(field_value, &format!("{field} must be a Uint8Array"))
}

fn object_optional_uint8_array(
    value: &JsValue,
    field: &str,
) -> Result<Option<Uint8Array>, JsValue> {
    object_optional_value(value, field)?
        .map(|field_value| uint8_array(field_value, &format!("{field} must be a Uint8Array")))
        .transpose()
}

fn object_array(value: &JsValue, field: &str) -> Result<Array, JsValue> {
    Reflect::get(value, &field.into())?
        .dyn_into::<Array>()
        .map_err(|_| JsValue::from_str(&format!("{field} must be an Array")))
}

fn object_string(value: &JsValue, field: &str) -> Result<String, JsValue> {
    Reflect::get(value, &field.into())?
        .as_string()
        .ok_or_else(|| JsValue::from_str(&format!("{field} must be a string")))
}

fn object_usize(value: &JsValue, primary: &str, fallback: &str) -> Result<usize, JsValue> {
    let primary_value = Reflect::get(value, &primary.into())?;
    let field_value = if primary_value.is_undefined() {
        Reflect::get(value, &fallback.into())?
    } else {
        primary_value
    };
    js_value_to_usize(&field_value, primary)
}

fn js_value_to_usize(value: &JsValue, field: &str) -> Result<usize, JsValue> {
    if let Some(text) = value.as_string() {
        return text.parse::<usize>().map_err(js_error);
    }
    if let Some(number) = value.as_f64() {
        if number.is_finite()
            && number >= 0.0
            && number.fract() == 0.0
            && number <= usize::MAX as f64
        {
            return Ok(number as usize);
        }
    }
    Err(JsValue::from_str(&format!(
        "{field} must be a non-negative integer or decimal string"
    )))
}

fn object_optional_value(value: &JsValue, field: &str) -> Result<Option<JsValue>, JsValue> {
    let field_value = Reflect::get(value, &field.into())?;
    if field_value.is_null() || field_value.is_undefined() {
        Ok(None)
    } else {
        Ok(Some(field_value))
    }
}

fn uint8_array(value: JsValue, message: &str) -> Result<Uint8Array, JsValue> {
    value
        .dyn_into::<Uint8Array>()
        .map_err(|_| JsValue::from_str(message))
}
