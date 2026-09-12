//! Independent, deterministic TurboQuant-MSE routing accelerator.
//!
//! This implementation follows the public TurboQuant paper's rotate-then-
//! scalar-quantize construction, with Prolly's frozen structured transform.
//! It does not contain or depend on Turbovec code or formats.

use crate::prolly::builder::SortedBatchBuilder;
use crate::prolly::cid::Cid;
use crate::prolly::config::Config;
use crate::prolly::encoding::Encoding;
use crate::prolly::error::Error;
use crate::prolly::proximity::accelerator::quantized::{
    admit_quantized, rerank_authoritative, QuantizedRanked,
};
use crate::prolly::proximity::distance::canonical::sqrt_down;
use crate::prolly::proximity::distance::{fill_query_products_f64, prepare_vector};
use crate::prolly::proximity::search::{EligibilityCardinality, PreparedFilter};
use crate::prolly::proximity::storage::codec::{put_cid, put_f64, put_varint, Reader};
use crate::prolly::proximity::storage::StoredRecord;
use crate::prolly::proximity::{
    BuildParallelism, DistanceMetric, ProximityMap, ProximitySearchStats, QueryKernel,
    SearchBackend, SearchCompletion, SearchPolicy, SearchRequest, SearchResult,
};
use crate::prolly::store::{NodePublication, PublicationOrigin, Store};
use crate::prolly::tree::Tree;
use crate::prolly::Prolly;
use rayon::prelude::*;
use std::collections::BinaryHeap;

const MAGIC: &[u8; 4] = b"TQTQ";
const TURBOQUANT_FORMAT_VERSION: u8 = 1;
pub(crate) const STRUCTURED_ROTATION_ID: u8 = 1;
pub(crate) const NORMAL_LLOYD_MAX_CODEBOOK_ID: u8 = 1;
const MIN_DIMENSIONS: u32 = 8;
const MAX_DIMENSIONS: u32 = 16_384;
const ROTATION_ROUNDS: usize = 2;

const PERMUTATION_DOMAIN: u64 = 0x5451_5045_524d_0001;
const SIGN_DOMAIN: u64 = 0x5451_5349_474e_0001;

/// Frozen configuration for the TurboQuant-MSE sidecar.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TurboQuantizationConfig {
    pub bit_width: u8,
    pub rerank_multiplier: u32,
    pub seed: u64,
}

impl Default for TurboQuantizationConfig {
    fn default() -> Self {
        Self {
            bit_width: 4,
            rerank_multiplier: 8,
            seed: 0,
        }
    }
}

impl TurboQuantizationConfig {
    pub(crate) fn validate(&self, dimensions: u32) -> Result<(), Error> {
        if !matches!(self.bit_width, 2..=4) {
            return Err(invalid_config("TurboQuant bit_width must be 2, 3, or 4"));
        }
        if self.rerank_multiplier == 0 {
            return Err(invalid_config(
                "TurboQuant rerank_multiplier must be positive",
            ));
        }
        if !(MIN_DIMENSIONS..=MAX_DIMENSIONS).contains(&dimensions) || !dimensions.is_multiple_of(8)
        {
            return Err(invalid_config(
                "TurboQuant dimensions must be in 8..=16384 and divisible by eight",
            ));
        }
        Ok(())
    }
}

/// Deterministic build resource limits.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TurboQuantizationBuildLimits {
    pub max_records: Option<usize>,
    pub max_input_bytes: Option<usize>,
    pub max_temporary_bytes: Option<usize>,
    pub max_transform_operations: Option<usize>,
    pub max_encoded_output_bytes: Option<usize>,
    pub max_worker_threads: Option<usize>,
}

impl TurboQuantizationBuildLimits {
    fn validate(&self) -> Result<(), Error> {
        for (name, value) in [
            ("max_records", self.max_records),
            ("max_input_bytes", self.max_input_bytes),
            ("max_temporary_bytes", self.max_temporary_bytes),
            ("max_transform_operations", self.max_transform_operations),
            ("max_encoded_output_bytes", self.max_encoded_output_bytes),
            ("max_worker_threads", self.max_worker_threads),
        ] {
            if value == Some(0) {
                return Err(invalid_config(format!(
                    "TurboQuant {name} must be positive"
                )));
            }
        }
        Ok(())
    }
}

/// Canonical logical work performed by one TurboQuant build.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TurboQuantizationBuildStats {
    pub encoded_vectors: usize,
    pub zero_vectors: usize,
    pub transformed_components: usize,
    pub butterfly_operations: usize,
    pub input_bytes: usize,
    pub encoded_output_bytes: usize,
    pub peak_temporary_bytes: usize,
}

/// Routing reconstruction error committed to the manifest.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct TurboQuantizationQuality {
    pub mean_squared_error: f64,
    pub maximum_squared_error: f64,
}

/// Result of a full source/code-tree verification.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct TurboQuantizationVerification {
    pub encoded_vectors: u64,
    pub zero_vectors: u64,
    pub quality: TurboQuantizationQuality,
}

/// Source-bound persisted TurboQuant-MSE accelerator.
pub struct TurboQuantizer<S: Store> {
    codes: Prolly<S>,
    pub(crate) code_tree: Tree,
    manifest: Cid,
    pub(super) source: Cid,
    pub(super) dimensions: u32,
    pub(super) metric: DistanceMetric,
    pub(super) count: u64,
    config: TurboQuantizationConfig,
    quality: TurboQuantizationQuality,
    zero_vectors: u64,
    plan: StructuredRotation,
}

impl<S> TurboQuantizer<S>
where
    S: Store + Clone + Send + Sync,
    S::Error: Send + Sync,
{
    pub fn build(
        map: &ProximityMap<S>,
        config: TurboQuantizationConfig,
        parallelism: BuildParallelism,
    ) -> Result<(Self, TurboQuantizationBuildStats), Error> {
        Self::build_with_limits(
            map,
            config,
            parallelism,
            TurboQuantizationBuildLimits::default(),
        )
    }

    /// Encode the source in key order and publish the manifest only after the
    /// complete canonical code tree exists.
    pub fn build_with_limits(
        map: &ProximityMap<S>,
        config: TurboQuantizationConfig,
        parallelism: BuildParallelism,
        limits: TurboQuantizationBuildLimits,
    ) -> Result<(Self, TurboQuantizationBuildStats), Error> {
        limits.validate()?;
        let dimensions = map.tree().config.dimensions;
        config.validate(dimensions)?;
        let records = usize::try_from(map.tree().count)
            .map_err(|_| resource_limit("TurboQuant records", usize::MAX, usize::MAX))?;
        if records == 0 {
            return Err(invalid_config("TurboQuant requires a non-empty source map"));
        }
        enforce_resource("TurboQuant records", limits.max_records, records)?;
        enforce_resource(
            "TurboQuant worker threads",
            limits.max_worker_threads,
            parallelism.threads(),
        )?;

        let dimensions_usize = dimensions as usize;
        let input_bytes = records
            .checked_mul(dimensions_usize)
            .and_then(|value| value.checked_mul(4))
            .ok_or_else(|| resource_limit("TurboQuant input bytes", usize::MAX, usize::MAX))?;
        enforce_resource(
            "TurboQuant input bytes",
            limits.max_input_bytes,
            input_bytes,
        )?;
        let packed_len = packed_len(dimensions_usize, config.bit_width)?;
        let encoded_output_bytes = records
            .checked_mul(8usize.checked_add(packed_len).ok_or_else(|| {
                resource_limit("TurboQuant encoded output bytes", usize::MAX, usize::MAX)
            })?)
            .ok_or_else(|| {
                resource_limit("TurboQuant encoded output bytes", usize::MAX, usize::MAX)
            })?;
        enforce_resource(
            "TurboQuant encoded output bytes",
            limits.max_encoded_output_bytes,
            encoded_output_bytes,
        )?;

        let plan = StructuredRotation::derive(dimensions_usize, config.seed)?;
        let per_worker_buffers = dimensions_usize
            .checked_mul(25)
            .ok_or_else(|| resource_limit("TurboQuant temporary bytes", usize::MAX, usize::MAX))?;
        let required_temporary_bytes = plan
            .owned_bytes()
            .checked_add(
                per_worker_buffers
                    .checked_mul(parallelism.threads())
                    .ok_or_else(|| {
                        resource_limit("TurboQuant temporary bytes", usize::MAX, usize::MAX)
                    })?,
            )
            .and_then(|value| value.checked_add(8 + packed_len))
            .ok_or_else(|| resource_limit("TurboQuant temporary bytes", usize::MAX, usize::MAX))?;
        enforce_resource(
            "TurboQuant temporary bytes",
            limits.max_temporary_bytes,
            required_temporary_bytes,
        )?;
        // Logical build statistics are canonical across worker counts. The
        // limit above still reserves every requested worker's scratch space.
        let mut peak_temporary_bytes = plan
            .owned_bytes()
            .checked_add(per_worker_buffers)
            .and_then(|value| value.checked_add(8 + packed_len))
            .ok_or_else(|| resource_limit("TurboQuant temporary bytes", usize::MAX, usize::MAX))?;

        let codebook = codebook(config.bit_width);
        let sqrt_dimensions = sqrt_down(f64::from(dimensions));
        let store = map.store_clone();
        let code_config = turboquant_code_tree_config();
        let mut builder = SortedBatchBuilder::new_with_origin(
            store.clone(),
            code_config.clone(),
            PublicationOrigin::Maintenance,
        );
        let mut encoded_vectors = 0usize;
        let mut zero_vectors = 0usize;
        let mut transformed_components = 0usize;
        let mut butterfly_operations = 0usize;
        let mut transform_operations = 0usize;
        let mut quality_sum = 0.0f64;
        let mut quality_maximum = 0.0f64;
        let pool = (parallelism.threads() > 1)
            .then(|| {
                rayon::ThreadPoolBuilder::new()
                    .num_threads(parallelism.threads())
                    .build()
            })
            .transpose()
            .map_err(|_| invalid_config("cannot create TurboQuant worker pool"))?;
        const ENCODE_BATCH_RECORDS: usize = 128;
        let mut batch = Vec::with_capacity(ENCODE_BATCH_RECORDS);
        {
            let mut commit_batch = |batch: Vec<(Vec<u8>, Vec<f32>)>| -> Result<(), Error> {
                if batch.is_empty() {
                    return Ok(());
                }
                let batch_input_bytes = batch.iter().try_fold(0usize, |total, (key, vector)| {
                    total
                        .checked_add(key.len())
                        .and_then(|value| value.checked_add(vector.len().checked_mul(4)?))
                        .ok_or_else(|| {
                            resource_limit("TurboQuant temporary bytes", usize::MAX, usize::MAX)
                        })
                })?;
                let batch_output_bytes =
                    batch.len().checked_mul(8 + packed_len).ok_or_else(|| {
                        resource_limit("TurboQuant temporary bytes", usize::MAX, usize::MAX)
                    })?;
                let physical_peak = plan
                    .owned_bytes()
                    .checked_add(batch_input_bytes)
                    .and_then(|value| value.checked_add(batch_output_bytes))
                    .and_then(|value| {
                        value.checked_add(per_worker_buffers.checked_mul(parallelism.threads())?)
                    })
                    .ok_or_else(|| {
                        resource_limit("TurboQuant temporary bytes", usize::MAX, usize::MAX)
                    })?;
                enforce_resource(
                    "TurboQuant temporary bytes",
                    limits.max_temporary_bytes,
                    physical_peak,
                )?;
                let logical_peak = plan
                    .owned_bytes()
                    .checked_add(batch_input_bytes)
                    .and_then(|value| value.checked_add(batch_output_bytes))
                    .and_then(|value| value.checked_add(per_worker_buffers))
                    .ok_or_else(|| {
                        resource_limit("TurboQuant temporary bytes", usize::MAX, usize::MAX)
                    })?;
                peak_temporary_bytes = peak_temporary_bytes.max(logical_peak);

                let compute = || {
                    batch
                        .into_par_iter()
                        .map(|(key, vector)| {
                            let encoded = encode_vector(
                                &vector,
                                &plan,
                                codebook,
                                config.bit_width,
                                sqrt_dimensions,
                            );
                            (key, encoded)
                        })
                        .collect::<Vec<_>>()
                };
                let encoded = if let Some(pool) = &pool {
                    pool.install(compute)
                } else {
                    compute()
                };
                // Rayon indexed collection preserves input order. Errors are
                // inspected only here so the earliest source key wins.
                for (key, encoded) in encoded {
                    let encoded = encoded?;
                    if encoded.zero {
                        zero_vectors = zero_vectors.saturating_add(1);
                    } else {
                        transformed_components = transformed_components
                            .checked_add(dimensions_usize)
                            .ok_or_else(|| {
                                resource_limit(
                                    "TurboQuant transform operations",
                                    usize::MAX,
                                    usize::MAX,
                                )
                            })?;
                        butterfly_operations = butterfly_operations
                            .checked_add(plan.butterfly_operations_per_vector())
                            .ok_or_else(|| {
                                resource_limit(
                                    "TurboQuant transform operations",
                                    usize::MAX,
                                    usize::MAX,
                                )
                            })?;
                        transform_operations = transform_operations
                            .checked_add(plan.operations_per_vector())
                            .ok_or_else(|| {
                                resource_limit(
                                    "TurboQuant transform operations",
                                    usize::MAX,
                                    usize::MAX,
                                )
                            })?;
                        enforce_resource(
                            "TurboQuant transform operations",
                            limits.max_transform_operations,
                            transform_operations,
                        )?;
                    }
                    quality_sum += encoded.error;
                    quality_maximum = quality_maximum.max(encoded.error);
                    builder.add(key, encoded.bytes)?;
                    encoded_vectors += 1;
                }
                Ok(())
            };
            for entry in map
                .directory_manager()
                .range(&map.tree().directory, &[], None)?
            {
                let (key, bytes) = entry?;
                let stored = StoredRecord::decode(&bytes, dimensions)?;
                batch.push((key, stored.vector));
                if batch.len() == ENCODE_BATCH_RECORDS {
                    commit_batch(std::mem::take(&mut batch))?;
                }
            }
            commit_batch(batch)?;
        }
        if encoded_vectors != records {
            return Err(invalid_object(
                "TurboQuant source count changed during build",
            ));
        }
        let code_tree = builder.build()?;
        let code_root = code_tree
            .root
            .clone()
            .ok_or_else(|| invalid_object("TurboQuant requires a non-empty code tree"))?;
        let quality = TurboQuantizationQuality {
            mean_squared_error: quality_sum / encoded_vectors as f64,
            maximum_squared_error: quality_maximum,
        };
        let manifest_object = Manifest {
            source: map.tree().descriptor.clone(),
            dimensions,
            metric: map.tree().config.metric,
            count: map.tree().count,
            config: config.clone(),
            transform_id: STRUCTURED_ROTATION_ID,
            codebook_id: NORMAL_LLOYD_MAX_CODEBOOK_ID,
            code_root,
            quality,
            zero_vectors: zero_vectors as u64,
        };
        let manifest_bytes = manifest_object.encode()?;
        let manifest = Cid::from_bytes(&manifest_bytes);
        match store
            .get(manifest.as_bytes())
            .map_err(|error| Error::Store(Box::new(error)))?
        {
            Some(bytes) => {
                let actual = Cid::from_bytes(&bytes);
                if actual != manifest {
                    return Err(Error::CidMismatch {
                        expected: manifest,
                        actual,
                    });
                }
            }
            None => {
                let entries = [(manifest.as_bytes(), manifest_bytes.as_slice())];
                store
                    .publish_nodes(NodePublication::new(
                        &entries,
                        PublicationOrigin::Maintenance,
                    ))
                    .map_err(|error| Error::Store(Box::new(error)))?;
            }
        }

        Ok((
            Self {
                codes: Prolly::new(store, code_config),
                code_tree,
                manifest,
                source: manifest_object.source,
                dimensions,
                metric: manifest_object.metric,
                count: manifest_object.count,
                config,
                quality,
                zero_vectors: zero_vectors as u64,
                plan,
            },
            TurboQuantizationBuildStats {
                encoded_vectors,
                zero_vectors,
                transformed_components,
                butterfly_operations,
                input_bytes,
                encoded_output_bytes,
                peak_temporary_bytes,
            },
        ))
    }

    pub fn load(store: S, manifest: Cid) -> Result<Self, Error> {
        let bytes = load_content(&store, &manifest)?;
        let object = Manifest::decode(&bytes)?;
        object.config.validate(object.dimensions)?;
        let plan = StructuredRotation::derive(object.dimensions as usize, object.config.seed)?;
        let code_tree = Tree {
            root: Some(object.code_root),
            config: turboquant_code_tree_config(),
        };
        load_content(&store, code_tree.root.as_ref().expect("manifest code root"))?;
        Ok(Self {
            codes: Prolly::new(store, code_tree.config.clone()),
            code_tree,
            manifest,
            source: object.source,
            dimensions: object.dimensions,
            metric: object.metric,
            count: object.count,
            config: object.config,
            quality: object.quality,
            zero_vectors: object.zero_vectors,
            plan,
        })
    }

    pub fn manifest_cid(&self) -> &Cid {
        &self.manifest
    }

    pub fn source_descriptor(&self) -> &Cid {
        &self.source
    }

    pub fn config(&self) -> &TurboQuantizationConfig {
        &self.config
    }

    pub fn quality(&self) -> TurboQuantizationQuality {
        self.quality
    }

    pub(crate) fn rebind<T: Store>(&self, store: T) -> TurboQuantizer<T> {
        TurboQuantizer {
            codes: Prolly::new(store, self.code_tree.config.clone()),
            code_tree: self.code_tree.clone(),
            manifest: self.manifest.clone(),
            source: self.source.clone(),
            dimensions: self.dimensions,
            metric: self.metric,
            count: self.count,
            config: self.config.clone(),
            quality: self.quality,
            zero_vectors: self.zero_vectors,
            plan: self.plan.clone(),
        }
    }

    /// Fully verify source binding, code cardinality/canonicality, and quality.
    pub fn verify(&self, map: &ProximityMap<S>) -> Result<TurboQuantizationVerification, Error> {
        self.validate_binding(map, &map.tree().descriptor)?;
        let mut source = map
            .directory_manager()
            .range(&map.tree().directory, &[], None)?;
        let mut codes = self.codes.range(&self.code_tree, &[], None)?;
        let mut count = 0u64;
        let mut zeros = 0u64;
        let mut quality_sum = 0.0;
        let mut quality_maximum = 0.0f64;
        loop {
            match (source.next(), codes.next()) {
                (None, None) => break,
                (Some(source), Some(code)) => {
                    let (source_key, source_bytes) = source?;
                    let (code_key, actual) = code?;
                    if source_key != code_key {
                        return Err(invalid_object(
                            "TurboQuant source and code keys do not match",
                        ));
                    }
                    let stored = StoredRecord::decode(&source_bytes, self.dimensions)?;
                    let expected = encode_vector(
                        &stored.vector,
                        &self.plan,
                        codebook(self.config.bit_width),
                        self.config.bit_width,
                        sqrt_down(f64::from(self.dimensions)),
                    )?;
                    validate_code_value(&actual, self.dimensions as usize, self.config.bit_width)?;
                    if actual != expected.bytes {
                        return Err(invalid_object(
                            "TurboQuant code disagrees with authoritative source vector",
                        ));
                    }
                    count += 1;
                    zeros += u64::from(expected.zero);
                    quality_sum += expected.error;
                    quality_maximum = quality_maximum.max(expected.error);
                }
                _ => {
                    return Err(invalid_object(
                        "TurboQuant source and code counts do not match",
                    ))
                }
            }
        }
        if count != self.count || zeros != self.zero_vectors {
            return Err(invalid_object(
                "TurboQuant verified counts disagree with manifest",
            ));
        }
        let quality = TurboQuantizationQuality {
            mean_squared_error: quality_sum / count as f64,
            maximum_squared_error: quality_maximum,
        };
        if quality.mean_squared_error.to_bits() != self.quality.mean_squared_error.to_bits()
            || quality.maximum_squared_error.to_bits()
                != self.quality.maximum_squared_error.to_bits()
        {
            return Err(invalid_object(
                "TurboQuant quality measurements disagree with manifest",
            ));
        }
        Ok(TurboQuantizationVerification {
            encoded_vectors: count,
            zero_vectors: zeros,
            quality,
        })
    }

    pub fn search(
        &self,
        map: &ProximityMap<S>,
        request: SearchRequest<'_>,
    ) -> Result<SearchResult, Error> {
        request.validate()?;
        if request.policy == SearchPolicy::Exact {
            return Err(invalid_search("TurboQuant cannot satisfy exact search"));
        }
        if !matches!(
            request.options.backend,
            SearchBackend::TurboQuantized | SearchBackend::Auto
        ) {
            return Err(invalid_search(
                "TurboQuant requires TurboQuantized or Auto backend",
            ));
        }
        let filter = PreparedFilter::new(request.filter.clone(), &map.tree().directory)?;
        let eligible_limit = match filter.cardinality(map.tree().count) {
            EligibilityCardinality::Known(count) => count as usize,
            EligibilityCardinality::Unknown => map.tree().count as usize,
        };
        let multiplier = request
            .options
            .turboquant
            .rerank_multiplier
            .map(usize::from)
            .unwrap_or(self.config.rerank_multiplier as usize);
        let rerank_target = request
            .k
            .checked_mul(multiplier)
            .ok_or_else(|| invalid_search("TurboQuant rerank target overflow"))?
            .max(request.k)
            .min(eligible_limit);
        let plan = crate::prolly::proximity::search::SearchPlan::TurboQuantized {
            rerank_target,
            direct_lookup: filter.sorted_keys().is_some()
                && eligible_limit <= request.options.planner.eligible_exact_max_records,
        };
        self.search_planned(map, request, &plan)
    }

    pub(crate) fn search_planned(
        &self,
        map: &ProximityMap<S>,
        request: SearchRequest<'_>,
        plan: &crate::prolly::proximity::search::SearchPlan,
    ) -> Result<SearchResult, Error> {
        self.search_planned_with_exclusion(map, &map.tree().descriptor, request, plan, |_| {
            Ok(false)
        })
    }

    pub(crate) fn search_planned_with_exclusion<F>(
        &self,
        map: &ProximityMap<S>,
        expected_source: &Cid,
        request: SearchRequest<'_>,
        plan: &crate::prolly::proximity::search::SearchPlan,
        mut excluded: F,
    ) -> Result<SearchResult, Error>
    where
        F: FnMut(&[u8]) -> Result<bool, Error>,
    {
        let crate::prolly::proximity::search::SearchPlan::TurboQuantized {
            rerank_target,
            direct_lookup,
        } = plan
        else {
            return Err(invalid_search(
                "TurboQuant executor requires a TurboQuant search plan",
            ));
        };
        request.validate()?;
        if request.policy == SearchPolicy::Exact {
            return Err(invalid_search("TurboQuant cannot satisfy exact search"));
        }
        self.validate_binding(map, expected_source)?;
        let query = prepare_vector(self.metric, request.query, self.dimensions)?;
        let prepared_query = prepare_query_from_prepared(&query, &self.plan, self.dimensions);
        let filter = PreparedFilter::new(request.filter.clone(), &map.tree().directory)?;
        let mut stats = ProximitySearchStats::default();
        let mut approximate = BinaryHeap::<QuantizedRanked>::new();
        let mut completion = SearchCompletion::ApproximatePolicySatisfied;

        if *direct_lookup {
            let Some((keys, source_bound)) = filter.sorted_keys() else {
                return Err(invalid_search(
                    "TurboQuant direct-lookup plan requires sorted eligible keys",
                ));
            };
            for key in keys {
                if excluded(key)? {
                    continue;
                }
                let code = self.codes.get(&self.code_tree, key)?;
                let Some(code) = code else {
                    if source_bound {
                        return Err(invalid_object(
                            "source-bound eligible key has no TurboQuant code",
                        ));
                    }
                    continue;
                };
                if !admit_quantized(
                    key.clone(),
                    &code,
                    *rerank_target,
                    &request,
                    &mut stats,
                    &mut approximate,
                    |code| {
                        score_code_value(
                            code,
                            &prepared_query,
                            self.metric,
                            self.dimensions as usize,
                            self.config.bit_width,
                            request.kernel,
                        )
                    },
                )? {
                    completion = SearchCompletion::BudgetExhausted;
                    break;
                }
            }
        } else {
            for entry in self.codes.range(&self.code_tree, &[], None)? {
                let (key, code) = entry?;
                if !filter.contains(&key) || excluded(&key)? {
                    continue;
                }
                if !admit_quantized(
                    key,
                    &code,
                    *rerank_target,
                    &request,
                    &mut stats,
                    &mut approximate,
                    |code| {
                        score_code_value(
                            code,
                            &prepared_query,
                            self.metric,
                            self.dimensions as usize,
                            self.config.bit_width,
                            request.kernel,
                        )
                    },
                )? {
                    completion = SearchCompletion::BudgetExhausted;
                    break;
                }
            }
        }
        let neighbors = rerank_authoritative(
            map,
            &request,
            &query,
            self.dimensions,
            approximate,
            &mut stats,
            &mut completion,
            "TurboQuant code key is absent from authoritative directory",
        )?;
        Ok(SearchResult {
            neighbors,
            stats,
            completion,
            plan: plan.summary(),
        })
    }

    fn validate_binding(&self, map: &ProximityMap<S>, expected_source: &Cid) -> Result<(), Error> {
        if &self.source != expected_source
            || self.dimensions != map.tree().config.dimensions
            || self.metric != map.tree().config.metric
        {
            return Err(invalid_search(
                "TurboQuant is bound to a different source descriptor",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub(crate) struct StructuredRotation {
    dimensions: usize,
    block_width: usize,
    rounds: Vec<RotationRound>,
    normalization: f64,
}

#[derive(Clone, Debug)]
struct RotationRound {
    permutation: Vec<usize>,
    signs: Vec<i8>,
}

impl StructuredRotation {
    pub(crate) fn derive(dimensions: usize, seed: u64) -> Result<Self, Error> {
        let dimensions_u32 = u32::try_from(dimensions)
            .map_err(|_| invalid_config("TurboQuant dimensions exceed u32"))?;
        TurboQuantizationConfig {
            bit_width: 4,
            rerank_multiplier: 1,
            seed,
        }
        .validate(dimensions_u32)?;
        let block_width = 1usize << dimensions.trailing_zeros();
        let mut rounds = Vec::with_capacity(ROTATION_ROUNDS);
        for round in 0..ROTATION_ROUNDS {
            let domain_dimensions = (dimensions as u64) << 16;
            let mut permutation_stream =
                SplitMix64::new(seed ^ PERMUTATION_DOMAIN ^ domain_dimensions ^ round as u64);
            let mut permutation: Vec<_> = (0..dimensions).collect();
            for index in (1..dimensions).rev() {
                let draw = permutation_stream.next();
                let bound = (index as u64) + 1;
                let selected = multiply_high(draw, bound) as usize;
                permutation.swap(index, selected);
            }
            let mut sign_stream =
                SplitMix64::new(seed ^ SIGN_DOMAIN ^ domain_dimensions ^ round as u64);
            let signs = (0..dimensions)
                .map(|_| if sign_stream.next() & 1 == 0 { 1 } else { -1 })
                .collect();
            rounds.push(RotationRound { permutation, signs });
        }
        Ok(Self {
            dimensions,
            block_width,
            rounds,
            normalization: 1.0 / sqrt_down(block_width as f64),
        })
    }

    fn apply(&self, input: &[f64]) -> Vec<f64> {
        debug_assert_eq!(input.len(), self.dimensions);
        let mut current = input.to_vec();
        let mut work = vec![0.0; self.dimensions];
        for round in &self.rounds {
            for index in 0..self.dimensions {
                let value = current[round.permutation[index]];
                work[index] = if round.signs[index] > 0 {
                    value
                } else {
                    -value
                };
            }
            for block in work.chunks_exact_mut(self.block_width) {
                let mut width = 1usize;
                while width < self.block_width {
                    for start in (0..self.block_width).step_by(width * 2) {
                        for offset in 0..width {
                            let left = block[start + offset];
                            let right = block[start + offset + width];
                            block[start + offset] = left + right;
                            block[start + offset + width] = left - right;
                        }
                    }
                    width *= 2;
                }
                for value in block {
                    *value *= self.normalization;
                    if *value == 0.0 {
                        *value = 0.0;
                    }
                }
            }
            std::mem::swap(&mut current, &mut work);
        }
        current
    }

    fn butterfly_operations_per_vector(&self) -> usize {
        ROTATION_ROUNDS * self.dimensions * self.block_width.ilog2() as usize
    }

    fn operations_per_vector(&self) -> usize {
        self.dimensions * (ROTATION_ROUNDS * (3 + self.block_width.ilog2() as usize) + 1)
    }

    pub(crate) fn owned_bytes(&self) -> usize {
        self.rounds.len()
            * self.dimensions
            * (std::mem::size_of::<usize>() + std::mem::size_of::<i8>())
    }
}

#[derive(Clone, Copy)]
struct SplitMix64 {
    state: u64,
}

impl SplitMix64 {
    fn new(state: u64) -> Self {
        Self { state }
    }

    fn next(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut value = self.state;
        value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        value ^ (value >> 31)
    }
}

fn multiply_high(left: u64, right: u64) -> u64 {
    ((u128::from(left) * u128::from(right)) >> 64) as u64
}

#[derive(Clone, Copy)]
struct Codebook {
    thresholds: &'static [u64],
    centroids: &'static [u64],
}

// Symmetric Lloyd-Max solutions for N(0,1), generated in binary64 and frozen
// as bits. Equality with a threshold selects the lower centroid.
const THRESHOLDS_2: &[u64] = &[
    0xbfef_6941_ee8d_a039,
    0x0000_0000_0000_0000,
    0x3fef_6941_ee8d_a039,
];
const CENTROIDS_2: &[u64] = &[
    0xbff8_2aab_a77c_e5f2,
    0xbfdc_fa59_1c42_e91c,
    0x3fdc_fa59_1c42_e91c,
    0x3ff8_2aab_a77c_e5f2,
];
const THRESHOLDS_3: &[u64] = &[
    0xbffb_f782_d13d_ca9a,
    0xbff0_cca0_0132_d1ca,
    0xbfe0_0480_de16_34ea,
    0x0000_0000_0000_0000,
    0x3fe0_0480_de16_34ea,
    0x3ff0_cca0_0132_d1ca,
    0x3ffb_f782_d13d_ca9a,
];
const CENTROIDS_3: &[u64] = &[
    0xc001_372f_4f3e_0836,
    0xbff5_80a7_03ff_84c8,
    0xbfe8_3131_fccc_3d96,
    0xbfcf_5f3e_fd80_b0f4,
    0x3fcf_5f3e_fd80_b0f4,
    0x3fe8_3131_fccc_3d96,
    0x3ff5_80a7_03ff_84c8,
    0x4001_372f_4f3e_0836,
];
const THRESHOLDS_4: &[u64] = &[
    0xc003_34d8_698e_82d9,
    0xbffd_7f1b_3511_80af,
    0xbff6_fe85_3ee1_915a,
    0xbff1_96ac_bc39_0aaf,
    0xbfe9_95e9_6f9c_1822,
    0xbfe0_b787_fbb0_6a10,
    0xbfd0_86b4_2938_4834,
    0x0000_0000_0000_0000,
    0x3fd0_86b4_2938_4834,
    0x3fe0_b787_fbb0_6a10,
    0x3fe9_95e9_6f9c_1822,
    0x3ff1_96ac_bc39_0aaf,
    0x3ff6_fe85_3ee1_915a,
    0x3ffd_7f1b_3511_80af,
    0x4003_34d8_698e_82d9,
];
const CENTROIDS_4: &[u64] = &[
    0xc005_dc57_ebc6_84e8,
    0xc000_8d58_e756_80ca,
    0xbff9_e384_9b75_ffca,
    0xbff4_1985_e24d_22ea,
    0xbfee_27a7_2c49_e4e8,
    0xbfe5_042b_b2ee_4b5b,
    0xbfd8_d5c8_88e5_118c,
    0xbfc0_6f3f_9316_fdb8,
    0x3fc0_6f3f_9316_fdb8,
    0x3fd8_d5c8_88e5_118c,
    0x3fe5_042b_b2ee_4b5b,
    0x3fee_27a7_2c49_e4e8,
    0x3ff4_1985_e24d_22ea,
    0x3ff9_e384_9b75_ffca,
    0x4000_8d58_e756_80ca,
    0x4005_dc57_ebc6_84e8,
];

fn codebook(bit_width: u8) -> Codebook {
    match bit_width {
        2 => Codebook {
            thresholds: THRESHOLDS_2,
            centroids: CENTROIDS_2,
        },
        3 => Codebook {
            thresholds: THRESHOLDS_3,
            centroids: CENTROIDS_3,
        },
        4 => Codebook {
            thresholds: THRESHOLDS_4,
            centroids: CENTROIDS_4,
        },
        _ => unreachable!("validated TurboQuant bit width"),
    }
}

impl Codebook {
    fn quantize(&self, value: f64) -> u8 {
        self.thresholds
            .iter()
            .position(|threshold| value <= f64::from_bits(*threshold))
            .unwrap_or(self.centroids.len() - 1) as u8
    }

    fn centroid(&self, code: u8) -> f64 {
        f64::from_bits(self.centroids[code as usize])
    }
}

struct EncodedVector {
    bytes: Vec<u8>,
    error: f64,
    zero: bool,
}

fn encode_vector(
    vector: &[f32],
    plan: &StructuredRotation,
    codebook: Codebook,
    bit_width: u8,
    sqrt_dimensions: f64,
) -> Result<EncodedVector, Error> {
    let norm_squared = vector.iter().fold(0.0, |sum, component| {
        let value = f64::from(*component);
        sum + value * value
    });
    let norm = sqrt_down(norm_squared);
    if norm == 0.0 {
        let value_len = 8 + packed_len(vector.len(), bit_width)?;
        let mut bytes = Vec::with_capacity(value_len);
        bytes.extend_from_slice(&0.0f64.to_bits().to_le_bytes());
        bytes.resize(value_len, 0);
        return Ok(EncodedVector {
            bytes,
            error: 0.0,
            zero: true,
        });
    }
    let unit: Vec<_> = vector
        .iter()
        .map(|component| f64::from(*component) / norm)
        .collect();
    let rotated = plan.apply(&unit);
    let codes: Vec<_> = rotated
        .iter()
        .map(|value| codebook.quantize(value * sqrt_dimensions))
        .collect();
    let packed = pack_codes(&codes, bit_width)?;
    let inverse_sqrt_dimensions = 1.0 / sqrt_dimensions;
    let error = rotated.iter().zip(&codes).fold(0.0, |sum, (actual, code)| {
        let delta = actual - codebook.centroid(*code) * inverse_sqrt_dimensions;
        sum + delta * delta
    });
    let mut bytes = Vec::with_capacity(8 + packed.len());
    bytes.extend_from_slice(&norm.to_bits().to_le_bytes());
    bytes.extend_from_slice(&packed);
    Ok(EncodedVector {
        bytes,
        error,
        zero: false,
    })
}

fn packed_len(dimensions: usize, bit_width: u8) -> Result<usize, Error> {
    dimensions
        .checked_mul(bit_width as usize)
        .map(|bits| bits.div_ceil(8))
        .ok_or_else(|| resource_limit("TurboQuant packed code bytes", usize::MAX, usize::MAX))
}

fn pack_codes(codes: &[u8], bit_width: u8) -> Result<Vec<u8>, Error> {
    let mut packed = vec![0u8; packed_len(codes.len(), bit_width)?];
    let maximum = 1u8 << bit_width;
    for (index, code) in codes.iter().copied().enumerate() {
        if code >= maximum {
            return Err(invalid_object("TurboQuant centroid code is out of range"));
        }
        let bit_offset = index * bit_width as usize;
        for bit in 0..bit_width as usize {
            if code & (1 << bit) != 0 {
                let output = bit_offset + bit;
                packed[output / 8] |= 1 << (output % 8);
            }
        }
    }
    Ok(packed)
}

fn unpack_code(packed: &[u8], index: usize, bit_width: u8) -> u8 {
    let bit_offset = index * bit_width as usize;
    let byte = bit_offset / 8;
    let shift = bit_offset % 8;
    let window =
        u16::from(packed[byte]) | (packed.get(byte + 1).copied().map(u16::from).unwrap_or(0) << 8);
    ((window >> shift) & ((1u16 << bit_width) - 1)) as u8
}

fn validate_code_value(bytes: &[u8], dimensions: usize, bit_width: u8) -> Result<f64, Error> {
    let expected = 8usize
        .checked_add(packed_len(dimensions, bit_width)?)
        .ok_or_else(|| invalid_object("TurboQuant code length overflow"))?;
    if bytes.len() != expected {
        return Err(invalid_object("invalid TurboQuant code value length"));
    }
    let norm = f64::from_bits(u64::from_le_bytes(
        bytes[..8].try_into().expect("eight-byte norm"),
    ));
    if !norm.is_finite() || norm < 0.0 || norm.to_bits() == (-0.0f64).to_bits() {
        return Err(invalid_object("non-canonical TurboQuant norm"));
    }
    let packed = &bytes[8..];
    let used_bits = dimensions * bit_width as usize;
    let remainder = used_bits % 8;
    if remainder != 0 {
        let padding_mask = !((1u8 << remainder) - 1);
        if packed.last().is_some_and(|byte| byte & padding_mask != 0) {
            return Err(invalid_object("nonzero TurboQuant code padding bits"));
        }
    }
    if norm == 0.0 && packed.iter().any(|byte| *byte != 0) {
        return Err(invalid_object(
            "zero TurboQuant norm requires all-zero codes",
        ));
    }
    Ok(norm)
}

pub(crate) struct TurboQuantPreparedQuery {
    weighted: Vec<f64>,
    norm_squared: f64,
}

pub(crate) fn prepare_query_with_plan(
    metric: DistanceMetric,
    query: &[f32],
    dimensions: u32,
    plan: &StructuredRotation,
) -> Result<(Vec<f32>, TurboQuantPreparedQuery), Error> {
    if plan.dimensions != dimensions as usize {
        return Err(invalid_search(
            "TurboQuant transform plan dimensions do not match the query",
        ));
    }
    let query = prepare_vector(metric, query, dimensions)?;
    let prepared = prepare_query_from_prepared(&query, plan, dimensions);
    Ok((query, prepared))
}

fn prepare_query_from_prepared(
    query: &[f32],
    plan: &StructuredRotation,
    dimensions: u32,
) -> TurboQuantPreparedQuery {
    let query_f64: Vec<_> = query.iter().map(|value| f64::from(*value)).collect();
    let transformed = plan.apply(&query_f64);
    let inverse_sqrt_dimensions = 1.0 / sqrt_down(f64::from(dimensions));
    TurboQuantPreparedQuery {
        weighted: transformed
            .into_iter()
            .map(|value| value * inverse_sqrt_dimensions)
            .collect(),
        norm_squared: query_f64.iter().fold(0.0, |sum, value| sum + value * value),
    }
}

pub(crate) fn score_code_value(
    bytes: &[u8],
    prepared_query: &TurboQuantPreparedQuery,
    metric: DistanceMetric,
    dimensions: usize,
    bit_width: u8,
    kernel: QueryKernel,
) -> Result<f64, Error> {
    let norm = validate_code_value(bytes, dimensions, bit_width)?;
    let packed = &bytes[8..];
    let codebook = codebook(bit_width);
    let dot = if norm == 0.0 {
        0.0
    } else {
        const PRODUCT_SLOTS: usize = 64;
        let mut centroids = [0.0f64; PRODUCT_SLOTS];
        let mut products = [0.0f64; PRODUCT_SLOTS];
        let mut reduced = 0.0;
        let mut start = 0usize;
        while start < dimensions {
            let end = start.saturating_add(PRODUCT_SLOTS).min(dimensions);
            for (offset, centroid) in centroids[..end - start].iter_mut().enumerate() {
                *centroid = codebook.centroid(unpack_code(packed, start + offset, bit_width));
            }
            fill_query_products_f64(
                kernel,
                &prepared_query.weighted[start..end],
                &centroids[..end - start],
                &mut products[..end - start],
            );
            for &product in &products[..end - start] {
                reduced += product;
            }
            start = end;
        }
        norm * reduced
    };
    let distance = match metric {
        DistanceMetric::L2Squared => {
            (prepared_query.norm_squared + norm * norm - 2.0 * dot).max(0.0)
        }
        DistanceMetric::Cosine => 1.0 - dot.clamp(-1.0, 1.0),
        DistanceMetric::InnerProduct => -dot,
    };
    Ok(if distance == 0.0 { 0.0 } else { distance })
}

#[derive(Clone)]
pub(crate) struct Manifest {
    pub(crate) source: Cid,
    pub(crate) dimensions: u32,
    pub(crate) metric: DistanceMetric,
    pub(crate) count: u64,
    pub(crate) config: TurboQuantizationConfig,
    pub(crate) transform_id: u8,
    pub(crate) codebook_id: u8,
    pub(crate) code_root: Cid,
    pub(crate) quality: TurboQuantizationQuality,
    pub(crate) zero_vectors: u64,
}

impl Manifest {
    pub(crate) fn encode(&self) -> Result<Vec<u8>, Error> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(MAGIC);
        bytes.push(TURBOQUANT_FORMAT_VERSION);
        bytes.push(0);
        put_cid(&self.source, &mut bytes);
        put_varint(u64::from(self.dimensions), &mut bytes);
        bytes.push(self.metric.id());
        put_varint(self.count, &mut bytes);
        bytes.push(self.config.bit_width);
        put_varint(u64::from(self.config.rerank_multiplier), &mut bytes);
        bytes.extend_from_slice(&self.config.seed.to_le_bytes());
        bytes.push(self.transform_id);
        bytes.push(self.codebook_id);
        put_cid(&self.code_root, &mut bytes);
        put_f64(self.quality.mean_squared_error, &mut bytes)?;
        put_f64(self.quality.maximum_squared_error, &mut bytes)?;
        put_varint(self.zero_vectors, &mut bytes);
        put_cid(&config_fingerprint(&self.config), &mut bytes);
        Ok(bytes)
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, Error> {
        let mut reader = Reader::new(bytes, "TurboQuant");
        reader.exact(MAGIC)?;
        require_version(reader.u8()?)?;
        if reader.u8()? != 0 {
            return Err(reader.invalid("unknown TurboQuant flags"));
        }
        let source = reader.cid()?;
        let dimensions = u32::try_from(reader.varint()?)
            .map_err(|_| reader.invalid("TurboQuant dimensions exceed u32"))?;
        let metric = DistanceMetric::from_id(reader.u8()?)?;
        let count = reader.varint()?;
        if count == 0 {
            return Err(reader.invalid("TurboQuant source count must be positive"));
        }
        let config = TurboQuantizationConfig {
            bit_width: reader.u8()?,
            rerank_multiplier: u32::try_from(reader.varint()?)
                .map_err(|_| reader.invalid("TurboQuant rerank multiplier exceeds u32"))?,
            seed: reader.u64_le()?,
        };
        config.validate(dimensions)?;
        let transform_id = reader.u8()?;
        let codebook_id = reader.u8()?;
        if transform_id != STRUCTURED_ROTATION_ID {
            return Err(reader.invalid("unsupported TurboQuant transform ID"));
        }
        if codebook_id != NORMAL_LLOYD_MAX_CODEBOOK_ID {
            return Err(reader.invalid("unsupported TurboQuant codebook ID"));
        }
        let code_root = reader.cid()?;
        let quality = TurboQuantizationQuality {
            mean_squared_error: reader.f64()?,
            maximum_squared_error: reader.f64()?,
        };
        if quality.mean_squared_error < 0.0
            || quality.maximum_squared_error < quality.mean_squared_error
        {
            return Err(reader.invalid("invalid TurboQuant quality measurements"));
        }
        let zero_vectors = reader.varint()?;
        if zero_vectors > count {
            return Err(reader.invalid("TurboQuant zero-vector count exceeds source count"));
        }
        if reader.cid()? != config_fingerprint(&config) {
            return Err(reader.invalid("TurboQuant configuration fingerprint mismatch"));
        }
        reader.finish()?;
        Ok(Self {
            source,
            dimensions,
            metric,
            count,
            config,
            transform_id,
            codebook_id,
            code_root,
            quality,
            zero_vectors,
        })
    }
}

pub(crate) fn config_fingerprint(config: &TurboQuantizationConfig) -> Cid {
    let mut bytes = Vec::new();
    bytes.push(config.bit_width);
    put_varint(u64::from(config.rerank_multiplier), &mut bytes);
    bytes.extend_from_slice(&config.seed.to_le_bytes());
    bytes.push(STRUCTURED_ROTATION_ID);
    bytes.push(NORMAL_LLOYD_MAX_CODEBOOK_ID);
    Cid::from_bytes(&bytes)
}

pub(crate) fn turboquant_code_tree_config() -> Config {
    Config::builder()
        .min_chunk_size(4)
        .max_chunk_size(1024 * 1024)
        .chunking_factor(128)
        .hash_seed(0)
        .encoding(Encoding::Raw)
        .build()
}

fn require_version(found: u8) -> Result<(), Error> {
    if found == TURBOQUANT_FORMAT_VERSION {
        Ok(())
    } else {
        Err(Error::UnsupportedProximityVersion {
            found,
            required: TURBOQUANT_FORMAT_VERSION,
        })
    }
}

fn enforce_resource(
    resource: &'static str,
    limit: Option<usize>,
    actual: usize,
) -> Result<(), Error> {
    if limit.is_some_and(|limit| actual > limit) {
        return Err(resource_limit(
            resource,
            limit.expect("checked limit"),
            actual,
        ));
    }
    Ok(())
}

fn resource_limit(resource: &'static str, limit: usize, actual: usize) -> Error {
    Error::ProximityResourceLimitExceeded {
        resource,
        limit,
        actual,
    }
}

fn load_content<S: Store>(store: &S, cid: &Cid) -> Result<Vec<u8>, Error> {
    let bytes = store
        .get(cid.as_bytes())
        .map_err(|error| Error::Store(Box::new(error)))?
        .ok_or_else(|| Error::NotFound(cid.clone()))?;
    let actual = Cid::from_bytes(&bytes);
    if actual != *cid {
        return Err(Error::CidMismatch {
            expected: cid.clone(),
            actual,
        });
    }
    Ok(bytes)
}

fn invalid_config(reason: impl Into<String>) -> Error {
    Error::InvalidProximityConfig {
        reason: reason.into(),
    }
}

fn invalid_object(reason: impl Into<String>) -> Error {
    Error::InvalidProximityObject {
        kind: "TurboQuant",
        reason: reason.into(),
    }
}

fn invalid_search(reason: impl Into<String>) -> Error {
    Error::InvalidProximitySearch {
        reason: reason.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splitmix64_v1_is_frozen() {
        let mut stream = SplitMix64::new(0);
        assert_eq!(stream.next(), 0xe220_a839_7b1d_cdaf);
        assert_eq!(stream.next(), 0x6e78_9e6a_a1b9_65f4);
        assert_eq!(stream.next(), 0x06c4_5d18_8009_454f);
    }

    #[test]
    fn packing_round_trips_every_supported_width_and_tail() {
        for bit_width in [2, 3, 4] {
            let maximum = 1u8 << bit_width;
            for len in 1..65 {
                let codes: Vec<_> = (0..len).map(|index| index as u8 % maximum).collect();
                let packed = pack_codes(&codes, bit_width).unwrap();
                assert_eq!(packed.len(), (len * bit_width as usize).div_ceil(8));
                for (index, expected) in codes.iter().enumerate() {
                    assert_eq!(unpack_code(&packed, index, bit_width), *expected);
                }
            }
        }
    }

    #[test]
    fn thresholds_choose_the_lower_code() {
        for bit_width in [2, 3, 4] {
            let codebook = codebook(bit_width);
            for (index, threshold) in codebook.thresholds.iter().enumerate() {
                assert_eq!(codebook.quantize(f64::from_bits(*threshold)), index as u8);
            }
        }
    }

    #[test]
    fn structured_rotation_is_deterministic_and_nearly_norm_preserving() {
        for dimensions in [8, 24, 128, 200, 768, 1536, 3072] {
            let plan = StructuredRotation::derive(dimensions, 0x5eed).unwrap();
            let input: Vec<_> = (0..dimensions)
                .map(|index| (index as f64 * 0.03125).sin())
                .collect();
            let left = plan.apply(&input);
            let right = plan.apply(&input);
            assert_eq!(
                left.iter().map(|value| value.to_bits()).collect::<Vec<_>>(),
                right
                    .iter()
                    .map(|value| value.to_bits())
                    .collect::<Vec<_>>()
            );
            let before = input.iter().fold(0.0, |sum, value| sum + value * value);
            let after = left.iter().fold(0.0, |sum, value| sum + value * value);
            assert!((before - after).abs() <= before.max(1.0) * 1e-12);
        }
    }

    #[test]
    fn padding_and_zero_norm_are_canonical() {
        let mut value = vec![0u8; 8 + packed_len(9, 3).unwrap()];
        assert_eq!(validate_code_value(&value, 9, 3).unwrap(), 0.0);
        value[8] = 1;
        assert!(validate_code_value(&value, 9, 3).is_err());
        value.fill(0);
        *value.last_mut().unwrap() = 0x80;
        assert!(validate_code_value(&value, 9, 3).is_err());
    }

    #[test]
    fn scalar_and_simd_approximate_scores_are_bit_identical() {
        for dimensions in [8usize, 24, 128, 200, 768] {
            let prepared = TurboQuantPreparedQuery {
                weighted: (0..dimensions)
                    .map(|index| ((index as f64 + 0.25) * 0.03125).sin())
                    .collect(),
                norm_squared: 17.25,
            };
            for bit_width in [2, 3, 4] {
                let maximum = 1u8 << bit_width;
                let codes: Vec<_> = (0..dimensions)
                    .map(|index| ((index * 7 + 3) as u8) % maximum)
                    .collect();
                let mut encoded = 1.25f64.to_le_bytes().to_vec();
                encoded.extend_from_slice(&pack_codes(&codes, bit_width).unwrap());
                for metric in [
                    DistanceMetric::L2Squared,
                    DistanceMetric::Cosine,
                    DistanceMetric::InnerProduct,
                ] {
                    let scalar = score_code_value(
                        &encoded,
                        &prepared,
                        metric,
                        dimensions,
                        bit_width,
                        QueryKernel::ScalarDeterministic,
                    )
                    .unwrap();
                    let simd = score_code_value(
                        &encoded,
                        &prepared,
                        metric,
                        dimensions,
                        bit_width,
                        QueryKernel::SimdDeterministic,
                    )
                    .unwrap();
                    assert_eq!(
                        scalar.to_bits(),
                        simd.to_bits(),
                        "dimension={dimensions}, bits={bit_width}, metric={metric:?}",
                    );
                }
            }
        }
    }
}
