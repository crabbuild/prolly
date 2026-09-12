//! Independent, deterministic TurboQuant-MSE routing accelerator.
//!
//! This implementation follows the rotate-then-scalar-quantize construction
//! from [TurboQuant: Online Vector Quantization with Near-optimal Distortion
//! Rate](https://arxiv.org/abs/2504.19874), with Prolly's frozen structured
//! transform. The production transform is not the paper's dense
//! Gaussian-QR/Haar rotation, so the implementation does not claim that
//! theorem. It does not contain or depend on Turbovec code or formats.

use crate::prolly::builder::SortedBatchBuilder;
use crate::prolly::cid::Cid;
use crate::prolly::config::Config;
use crate::prolly::encoding::Encoding;
use crate::prolly::error::Error;
use crate::prolly::node::Node;
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
    pub(crate) fn validate(&self) -> Result<(), Error> {
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
            .and_then(|value| value.checked_add(packed_len))
            .ok_or_else(|| resource_limit("TurboQuant temporary bytes", usize::MAX, usize::MAX))?;
        let encoded_value_bytes = 8usize
            .checked_add(packed_len)
            .ok_or_else(|| resource_limit("TurboQuant temporary bytes", usize::MAX, usize::MAX))?;
        let minimum_input_bytes = dimensions_usize
            .checked_mul(4)
            .ok_or_else(|| resource_limit("TurboQuant temporary bytes", usize::MAX, usize::MAX))?;
        let required_temporary_bytes = temporary_peak_bytes(
            plan.owned_bytes(),
            per_worker_buffers,
            parallelism.threads(),
            minimum_input_bytes,
            1,
            encoded_value_bytes,
        )?;
        enforce_resource(
            "TurboQuant temporary bytes",
            limits.max_temporary_bytes,
            required_temporary_bytes,
        )?;
        // Logical build statistics are canonical across worker counts. The
        // limit above still reserves every requested worker's scratch space.
        let mut peak_temporary_bytes = temporary_peak_bytes(
            plan.owned_bytes(),
            per_worker_buffers,
            1,
            minimum_input_bytes,
            1,
            encoded_value_bytes,
        )?;

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
        let mut batch = if limits.max_temporary_bytes.is_some() {
            Vec::new()
        } else {
            Vec::with_capacity(ENCODE_BATCH_RECORDS)
        };
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
                let physical_peak = temporary_peak_bytes(
                    plan.owned_bytes(),
                    per_worker_buffers,
                    parallelism.threads(),
                    batch_input_bytes,
                    batch.len(),
                    encoded_value_bytes,
                )?;
                enforce_resource(
                    "TurboQuant temporary bytes",
                    limits.max_temporary_bytes,
                    physical_peak,
                )?;
                let logical_peak = temporary_peak_bytes(
                    plan.owned_bytes(),
                    per_worker_buffers,
                    1,
                    batch_input_bytes,
                    batch.len(),
                    encoded_value_bytes,
                )?;
                peak_temporary_bytes = peak_temporary_bytes.max(logical_peak);

                let encoded = if let Some(pool) = &pool {
                    pool.install(|| {
                        batch
                            .into_par_iter()
                            .map_init(
                                || EncodingScratch::new(dimensions_usize, packed_len),
                                |scratch, (key, vector)| {
                                    let encoded = encode_vector_reusing(
                                        &vector,
                                        &plan,
                                        codebook,
                                        config.bit_width,
                                        sqrt_dimensions,
                                        scratch,
                                    );
                                    (key, encoded)
                                },
                            )
                            .collect::<Vec<_>>()
                    })
                } else {
                    let mut scratch = EncodingScratch::new(dimensions_usize, packed_len);
                    batch
                        .into_iter()
                        .map(|(key, vector)| {
                            let encoded = encode_vector_reusing(
                                &vector,
                                &plan,
                                codebook,
                                config.bit_width,
                                sqrt_dimensions,
                                &mut scratch,
                            );
                            (key, encoded)
                        })
                        .collect()
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
            let mut batch_input_bytes = 0usize;
            for entry in map
                .directory_manager()
                .range(&map.tree().directory, &[], None)?
            {
                let (key, bytes) = entry?;
                let mut next_batch_input_bytes = batch_input_bytes;
                if let Some(limit) = limits.max_temporary_bytes {
                    let record_input_bytes =
                        key.len().checked_add(minimum_input_bytes).ok_or_else(|| {
                            resource_limit("TurboQuant temporary bytes", usize::MAX, usize::MAX)
                        })?;
                    let mut projected_input_bytes = batch_input_bytes
                        .checked_add(record_input_bytes)
                        .ok_or_else(|| {
                            resource_limit("TurboQuant temporary bytes", usize::MAX, usize::MAX)
                        })?;
                    let mut projected_peak = temporary_peak_bytes(
                        plan.owned_bytes(),
                        per_worker_buffers,
                        parallelism.threads(),
                        projected_input_bytes,
                        batch.len() + 1,
                        encoded_value_bytes,
                    )?;
                    if projected_peak > limit && !batch.is_empty() {
                        commit_batch(std::mem::take(&mut batch))?;
                        projected_input_bytes = record_input_bytes;
                        projected_peak = temporary_peak_bytes(
                            plan.owned_bytes(),
                            per_worker_buffers,
                            parallelism.threads(),
                            projected_input_bytes,
                            1,
                            encoded_value_bytes,
                        )?;
                    }
                    enforce_resource("TurboQuant temporary bytes", Some(limit), projected_peak)?;
                    next_batch_input_bytes = projected_input_bytes;
                }
                let stored = StoredRecord::decode(&bytes, dimensions)?;
                batch.push((key, stored.vector));
                batch_input_bytes = next_batch_input_bytes;
                if batch.len() == ENCODE_BATCH_RECORDS {
                    commit_batch(std::mem::take(&mut batch))?;
                    batch_input_bytes = 0;
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
        let root_bytes =
            load_content(&store, code_tree.root.as_ref().expect("manifest code root"))?;
        validate_code_tree_root(&root_bytes, object.count)?;
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
        let packed_len = packed_len(self.dimensions as usize, self.config.bit_width)?;
        let mut scratch = EncodingScratch::new(self.dimensions as usize, packed_len);
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
                    let expected = encode_vector_reusing(
                        &stored.vector,
                        &self.plan,
                        codebook(self.config.bit_width),
                        self.config.bit_width,
                        sqrt_down(f64::from(self.dimensions)),
                        &mut scratch,
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
        let prepared_query = prepare_query_from_prepared(
            &query,
            &self.plan,
            self.dimensions,
            self.config.bit_width,
            request.kernel,
        );
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
                    key.as_slice(),
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
        if &self.source != expected_source {
            return Err(invalid_search("TurboQuant source descriptor mismatch"));
        }
        if self.dimensions != map.tree().config.dimensions {
            return Err(invalid_search("TurboQuant source dimensions mismatch"));
        }
        if self.metric != map.tree().config.metric {
            return Err(invalid_search("TurboQuant source metric mismatch"));
        }
        // Direct execution has the authoritative source count available. A
        // composite executes its immutable base against a newer map and binds
        // the base count when the composite is built or loaded instead.
        if expected_source == &map.tree().descriptor && self.count != map.tree().count {
            return Err(invalid_search("TurboQuant source count mismatch"));
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
        let mut current = Vec::with_capacity(self.dimensions);
        let mut work = vec![0.0; self.dimensions];
        self.apply_with_buffers(input, &mut current, &mut work);
        current
    }

    pub(crate) fn dimensions(&self) -> usize {
        self.dimensions
    }

    fn apply_with_buffers(&self, input: &[f64], current: &mut Vec<f64>, work: &mut Vec<f64>) {
        debug_assert_eq!(input.len(), self.dimensions);
        current.clear();
        current.extend_from_slice(input);
        work.resize(self.dimensions, 0.0);
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
            std::mem::swap(current, work);
        }
    }

    pub(crate) fn butterfly_operations_per_vector(&self) -> usize {
        ROTATION_ROUNDS * self.dimensions * self.block_width.ilog2() as usize
    }

    pub(crate) fn operations_per_vector(&self) -> usize {
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
pub(crate) struct Codebook {
    thresholds: &'static [u64],
    centroids: &'static [u64],
}

// Symmetric Lloyd-Max solutions for N(0,1), generated in binary64 and frozen
// as bits. Equality with a threshold selects the lower centroid.
const THRESHOLDS_2: &[u64] = &[
    0xbfef_6941_ee8d_a043,
    0x0000_0000_0000_0000,
    0x3fef_6941_ee8d_a043,
];
const CENTROIDS_2: &[u64] = &[
    0xbff8_2aab_a77c_e5fa,
    0xbfdc_fa59_1c42_e927,
    0x3fdc_fa59_1c42_e927,
    0x3ff8_2aab_a77c_e5fa,
];
const THRESHOLDS_3: &[u64] = &[
    0xbffb_f782_d13d_cab0,
    0xbff0_cca0_0132_d1d5,
    0xbfe0_0480_de16_34f6,
    0x0000_0000_0000_0000,
    0x3fe0_0480_de16_34f6,
    0x3ff0_cca0_0132_d1d5,
    0x3ffb_f782_d13d_cab0,
];
const CENTROIDS_3: &[u64] = &[
    0xc001_372f_4f3e_0846,
    0xbff5_80a7_03ff_84d6,
    0xbfe8_3131_fccc_3da8,
    0xbfcf_5f3e_fd80_b113,
    0x3fcf_5f3e_fd80_b113,
    0x3fe8_3131_fccc_3da8,
    0x3ff5_80a7_03ff_84d6,
    0x4001_372f_4f3e_0846,
];
const THRESHOLDS_4: &[u64] = &[
    0xc003_34d8_698e_82e4,
    0xbffd_7f1b_3511_80d5,
    0xbff6_fe85_3ee1_9176,
    0xbff1_96ac_bc39_0ac5,
    0xbfe9_95e9_6f9c_1848,
    0xbfe0_b787_fbb0_6a33,
    0xbfd0_86b4_2938_4856,
    0x0000_0000_0000_0000,
    0x3fd0_86b4_2938_4856,
    0x3fe0_b787_fbb0_6a33,
    0x3fe9_95e9_6f9c_1848,
    0x3ff1_96ac_bc39_0ac5,
    0x3ff6_fe85_3ee1_9176,
    0x3ffd_7f1b_3511_80d5,
    0x4003_34d8_698e_82e4,
];
const CENTROIDS_4: &[u64] = &[
    0xc005_dc57_ebc6_84e8,
    0xc000_8d58_e756_80e1,
    0xbff9_e384_9b75_ffe8,
    0xbff4_1985_e24d_2303,
    0xbfee_27a7_2c49_e50d,
    0xbfe5_042b_b2ee_4b83,
    0xbfd8_d5c8_88e5_11c6,
    0xbfc0_6f3f_9316_fdcb,
    0x3fc0_6f3f_9316_fdcb,
    0x3fd8_d5c8_88e5_11c6,
    0x3fe5_042b_b2ee_4b83,
    0x3fee_27a7_2c49_e50d,
    0x3ff4_1985_e24d_2303,
    0x3ff9_e384_9b75_ffe8,
    0x4000_8d58_e756_80e1,
    0x4005_dc57_ebc6_84e8,
];

pub(crate) fn codebook(bit_width: u8) -> Codebook {
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
            .partition_point(|threshold| value > f64::from_bits(*threshold)) as u8
    }

    fn centroid(&self, code: u8) -> f64 {
        f64::from_bits(self.centroids[code as usize])
    }
}

pub(crate) struct EncodedVector {
    pub(crate) bytes: Vec<u8>,
    pub(crate) error: f64,
    pub(crate) zero: bool,
}

pub(crate) struct EncodingScratch {
    unit: Vec<f64>,
    current: Vec<f64>,
    work: Vec<f64>,
    codes: Vec<u8>,
    packed: Vec<u8>,
}

impl EncodingScratch {
    pub(crate) fn new(dimensions: usize, packed_len: usize) -> Self {
        Self {
            unit: Vec::with_capacity(dimensions),
            current: Vec::with_capacity(dimensions),
            work: vec![0.0; dimensions],
            codes: Vec::with_capacity(dimensions),
            packed: Vec::with_capacity(packed_len),
        }
    }
}

pub(crate) fn encode_vector_reusing(
    vector: &[f32],
    plan: &StructuredRotation,
    codebook: Codebook,
    bit_width: u8,
    sqrt_dimensions: f64,
    scratch: &mut EncodingScratch,
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
    scratch.unit.clear();
    scratch
        .unit
        .extend(vector.iter().map(|component| f64::from(*component) / norm));
    plan.apply_with_buffers(&scratch.unit, &mut scratch.current, &mut scratch.work);
    scratch.codes.clear();
    scratch.codes.extend(
        scratch
            .current
            .iter()
            .map(|value| codebook.quantize(value * sqrt_dimensions)),
    );
    pack_codes_into(&scratch.codes, bit_width, &mut scratch.packed)?;
    let inverse_sqrt_dimensions = 1.0 / sqrt_dimensions;
    let error = scratch
        .current
        .iter()
        .zip(&scratch.codes)
        .fold(0.0, |sum, (actual, code)| {
            let delta = actual - codebook.centroid(*code) * inverse_sqrt_dimensions;
            sum + delta * delta
        });
    let mut bytes = Vec::with_capacity(8 + scratch.packed.len());
    bytes.extend_from_slice(&norm.to_bits().to_le_bytes());
    bytes.extend_from_slice(&scratch.packed);
    Ok(EncodedVector {
        bytes,
        error,
        zero: false,
    })
}

pub(crate) fn packed_len(dimensions: usize, bit_width: u8) -> Result<usize, Error> {
    dimensions
        .checked_mul(bit_width as usize)
        .map(|bits| bits.div_ceil(8))
        .ok_or_else(|| resource_limit("TurboQuant packed code bytes", usize::MAX, usize::MAX))
}

#[cfg(test)]
fn pack_codes(codes: &[u8], bit_width: u8) -> Result<Vec<u8>, Error> {
    let mut packed = Vec::new();
    pack_codes_into(codes, bit_width, &mut packed)?;
    Ok(packed)
}

fn pack_codes_into(codes: &[u8], bit_width: u8, packed: &mut Vec<u8>) -> Result<(), Error> {
    packed.clear();
    packed.resize(packed_len(codes.len(), bit_width)?, 0);
    let maximum = 1u8 << bit_width;
    if codes.iter().any(|code| *code >= maximum) {
        return Err(invalid_object("TurboQuant centroid code is out of range"));
    }
    let grouped_codes = match bit_width {
        2 => {
            for (input, output) in codes.chunks_exact(4).zip(packed.iter_mut()) {
                *output = input[0] | (input[1] << 2) | (input[2] << 4) | (input[3] << 6);
            }
            codes.len() / 4 * 4
        }
        3 => {
            for (group, input) in codes.chunks_exact(8).enumerate() {
                let output = u32::from(input[0])
                    | (u32::from(input[1]) << 3)
                    | (u32::from(input[2]) << 6)
                    | (u32::from(input[3]) << 9)
                    | (u32::from(input[4]) << 12)
                    | (u32::from(input[5]) << 15)
                    | (u32::from(input[6]) << 18)
                    | (u32::from(input[7]) << 21);
                packed[group * 3] = output as u8;
                packed[group * 3 + 1] = (output >> 8) as u8;
                packed[group * 3 + 2] = (output >> 16) as u8;
            }
            codes.len() / 8 * 8
        }
        4 => {
            for (input, output) in codes.chunks_exact(2).zip(packed.iter_mut()) {
                *output = input[0] | (input[1] << 4);
            }
            codes.len() / 2 * 2
        }
        _ => unreachable!("validated TurboQuant bit width"),
    };
    for (index, code) in codes.iter().copied().enumerate().skip(grouped_codes) {
        let bit_offset = index * bit_width as usize;
        for bit in 0..bit_width as usize {
            if code & (1 << bit) != 0 {
                let output = bit_offset + bit;
                packed[output / 8] |= 1 << (output % 8);
            }
        }
    }
    Ok(())
}

#[cfg(test)]
fn unpack_code(packed: &[u8], index: usize, bit_width: u8) -> u8 {
    let bit_offset = index * bit_width as usize;
    let byte = bit_offset / 8;
    let shift = bit_offset % 8;
    let window =
        u16::from(packed[byte]) | (packed.get(byte + 1).copied().map(u16::from).unwrap_or(0) << 8);
    ((window >> shift) & ((1u16 << bit_width) - 1)) as u8
}

fn fill_centroids(
    packed: &[u8],
    start: usize,
    bit_width: u8,
    codebook: Codebook,
    output: &mut [f64],
) {
    let bit_offset = start * bit_width as usize;
    debug_assert_eq!(bit_offset % 8, 0);
    let packed = &packed[bit_offset / 8..];
    match bit_width {
        2 => {
            debug_assert_eq!(output.len() % 4, 0);
            for (codes, centroids) in packed.iter().zip(output.chunks_exact_mut(4)) {
                let codes = *codes;
                centroids[0] = codebook.centroid(codes & 0x03);
                centroids[1] = codebook.centroid((codes >> 2) & 0x03);
                centroids[2] = codebook.centroid((codes >> 4) & 0x03);
                centroids[3] = codebook.centroid(codes >> 6);
            }
        }
        3 => {
            debug_assert_eq!(output.len() % 8, 0);
            for (codes, centroids) in packed.chunks_exact(3).zip(output.chunks_exact_mut(8)) {
                let codes =
                    u32::from(codes[0]) | (u32::from(codes[1]) << 8) | (u32::from(codes[2]) << 16);
                centroids[0] = codebook.centroid((codes & 0x07) as u8);
                centroids[1] = codebook.centroid(((codes >> 3) & 0x07) as u8);
                centroids[2] = codebook.centroid(((codes >> 6) & 0x07) as u8);
                centroids[3] = codebook.centroid(((codes >> 9) & 0x07) as u8);
                centroids[4] = codebook.centroid(((codes >> 12) & 0x07) as u8);
                centroids[5] = codebook.centroid(((codes >> 15) & 0x07) as u8);
                centroids[6] = codebook.centroid(((codes >> 18) & 0x07) as u8);
                centroids[7] = codebook.centroid(((codes >> 21) & 0x07) as u8);
            }
        }
        4 => {
            debug_assert_eq!(output.len() % 2, 0);
            for (codes, centroids) in packed.iter().zip(output.chunks_exact_mut(2)) {
                let codes = *codes;
                centroids[0] = codebook.centroid(codes & 0x0f);
                centroids[1] = codebook.centroid(codes >> 4);
            }
        }
        _ => unreachable!("validated TurboQuant bit width"),
    }
}

pub(crate) fn validate_code_value(
    bytes: &[u8],
    dimensions: usize,
    bit_width: u8,
) -> Result<f64, Error> {
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
    weighted_centroids: Vec<f64>,
    centroid_count: usize,
    norm_squared: f64,
}

impl TurboQuantPreparedQuery {
    fn new(weighted: Vec<f64>, norm_squared: f64, bit_width: u8, kernel: QueryKernel) -> Self {
        let centroid_count = 1usize << bit_width;
        let weighted_centroids = if matches!(kernel, QueryKernel::SimdDeterministic) {
            Vec::new()
        } else {
            let codebook = codebook(bit_width);
            let mut products = Vec::with_capacity(weighted.len() * centroid_count);
            for weight in &weighted {
                for code in 0..centroid_count {
                    products.push(*weight * codebook.centroid(code as u8));
                }
            }
            products
        };
        Self {
            weighted,
            weighted_centroids,
            centroid_count,
            norm_squared,
        }
    }
}

pub(crate) fn prepare_query_with_plan(
    metric: DistanceMetric,
    query: &[f32],
    dimensions: u32,
    plan: &StructuredRotation,
    bit_width: u8,
    kernel: QueryKernel,
) -> Result<(Vec<f32>, TurboQuantPreparedQuery), Error> {
    if plan.dimensions != dimensions as usize {
        return Err(invalid_search(
            "TurboQuant transform plan dimensions do not match the query",
        ));
    }
    let query = prepare_vector(metric, query, dimensions)?;
    let prepared = prepare_query_from_prepared(&query, plan, dimensions, bit_width, kernel);
    Ok((query, prepared))
}

fn prepare_query_from_prepared(
    query: &[f32],
    plan: &StructuredRotation,
    dimensions: u32,
    bit_width: u8,
    kernel: QueryKernel,
) -> TurboQuantPreparedQuery {
    let query_f64: Vec<_> = query.iter().map(|value| f64::from(*value)).collect();
    let transformed = plan.apply(&query_f64);
    let inverse_sqrt_dimensions = 1.0 / sqrt_down(f64::from(dimensions));
    TurboQuantPreparedQuery::new(
        transformed
            .into_iter()
            .map(|value| value * inverse_sqrt_dimensions)
            .collect(),
        query_f64.iter().fold(0.0, |sum, value| sum + value * value),
        bit_width,
        kernel,
    )
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
    let dot = if norm == 0.0 {
        0.0
    } else if matches!(
        kernel,
        QueryKernel::ScalarDeterministic | QueryKernel::AutoDeterministic
    ) {
        norm * score_precomputed_centroids(packed, prepared_query, bit_width)
    } else {
        let codebook = codebook(bit_width);
        const PRODUCT_SLOTS: usize = 64;
        let mut centroids = [0.0f64; PRODUCT_SLOTS];
        let mut products = [0.0f64; PRODUCT_SLOTS];
        let mut reduced = 0.0;
        let mut start = 0usize;
        while start < dimensions {
            let end = start.saturating_add(PRODUCT_SLOTS).min(dimensions);
            fill_centroids(
                packed,
                start,
                bit_width,
                codebook,
                &mut centroids[..end - start],
            );
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

#[inline]
fn score_precomputed_centroids(
    packed: &[u8],
    prepared: &TurboQuantPreparedQuery,
    bit_width: u8,
) -> f64 {
    let centroid_count = 1usize << bit_width;
    debug_assert_eq!(prepared.centroid_count, centroid_count);
    debug_assert_eq!(
        prepared.weighted_centroids.len(),
        prepared.weighted.len() * centroid_count
    );
    let products = &prepared.weighted_centroids;
    let mut reduced = 0.0;
    match bit_width {
        2 => {
            const ROW_GROUP: usize = 4 * (1 << 2);
            for (rows, byte) in products.chunks_exact(ROW_GROUP).zip(packed.iter().copied()) {
                reduced += rows[usize::from(byte & 0x03)];
                reduced += rows[4 + usize::from((byte >> 2) & 0x03)];
                reduced += rows[8 + usize::from((byte >> 4) & 0x03)];
                reduced += rows[12 + usize::from(byte >> 6)];
            }
        }
        3 => {
            const CENTROIDS: usize = 1 << 3;
            const ROW_GROUP: usize = 8 * CENTROIDS;
            for (rows, bytes) in products.chunks_exact(ROW_GROUP).zip(packed.chunks_exact(3)) {
                let codes =
                    u32::from(bytes[0]) | (u32::from(bytes[1]) << 8) | (u32::from(bytes[2]) << 16);
                reduced += rows[(codes & 0x07) as usize];
                reduced += rows[CENTROIDS + ((codes >> 3) & 0x07) as usize];
                reduced += rows[2 * CENTROIDS + ((codes >> 6) & 0x07) as usize];
                reduced += rows[3 * CENTROIDS + ((codes >> 9) & 0x07) as usize];
                reduced += rows[4 * CENTROIDS + ((codes >> 12) & 0x07) as usize];
                reduced += rows[5 * CENTROIDS + ((codes >> 15) & 0x07) as usize];
                reduced += rows[6 * CENTROIDS + ((codes >> 18) & 0x07) as usize];
                reduced += rows[7 * CENTROIDS + ((codes >> 21) & 0x07) as usize];
            }
        }
        4 => {
            const CENTROIDS: usize = 1 << 4;
            const ROW_GROUP: usize = 2 * CENTROIDS;
            for (rows, byte) in products.chunks_exact(ROW_GROUP).zip(packed.iter().copied()) {
                reduced += rows[usize::from(byte & 0x0f)];
                reduced += rows[CENTROIDS + usize::from(byte >> 4)];
            }
        }
        _ => unreachable!("validated TurboQuant bit width"),
    }
    reduced
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

pub(crate) fn validate_code_tree_root(bytes: &[u8], expected_count: u64) -> Result<(), Error> {
    let root = decode_code_tree_node(bytes)?;
    let actual_count = code_tree_node_count(&root)?;
    if actual_count != expected_count {
        return Err(invalid_object(
            "TurboQuant code-tree root count disagrees with manifest",
        ));
    }
    Ok(())
}

pub(crate) fn decode_code_tree_node(bytes: &[u8]) -> Result<Node, Error> {
    let config = turboquant_code_tree_config();
    let hard_max = usize::try_from(config.format.chunking.hard_max_node_bytes)
        .map_err(|_| invalid_object("TurboQuant code-tree hard byte limit exceeds usize"))?;
    if bytes.len() > hard_max {
        return Err(invalid_object(
            "TurboQuant code-tree node exceeds its hard byte limit",
        ));
    }
    let node = Node::from_bytes_with_format(bytes, &config.format)
        .map_err(|_| invalid_object("malformed TurboQuant code-tree node"))?;
    node.validate()
        .map_err(|_| invalid_object("malformed TurboQuant code-tree node"))?;
    Ok(node)
}

pub(crate) fn code_tree_node_count(node: &Node) -> Result<u64, Error> {
    if node.leaf {
        u64::try_from(node.len())
            .map_err(|_| invalid_object("TurboQuant code-tree node count exceeds u64"))
    } else {
        node.child_counts.iter().try_fold(0u64, |count, child| {
            count
                .checked_add(*child)
                .ok_or_else(|| invalid_object("TurboQuant code-tree node count overflow"))
        })
    }
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

pub(crate) fn enforce_resource(
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

pub(crate) fn temporary_peak_bytes(
    plan_bytes: usize,
    per_worker_bytes: usize,
    workers: usize,
    input_bytes: usize,
    records: usize,
    encoded_value_bytes: usize,
) -> Result<usize, Error> {
    let worker_bytes = per_worker_bytes
        .checked_mul(workers)
        .ok_or_else(|| resource_limit("TurboQuant temporary bytes", usize::MAX, usize::MAX))?;
    let output_bytes = records
        .checked_mul(encoded_value_bytes)
        .ok_or_else(|| resource_limit("TurboQuant temporary bytes", usize::MAX, usize::MAX))?;
    plan_bytes
        .checked_add(worker_bytes)
        .and_then(|value| value.checked_add(input_bytes))
        .and_then(|value| value.checked_add(output_bytes))
        .ok_or_else(|| resource_limit("TurboQuant temporary bytes", usize::MAX, usize::MAX))
}

pub(crate) fn resource_limit(resource: &'static str, limit: usize, actual: usize) -> Error {
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

pub(crate) fn invalid_object(reason: impl Into<String>) -> Error {
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
    use crate::prolly::store::MemStore;
    use std::sync::Arc;

    fn mismatched_root_count_fixture() -> (Arc<MemStore>, Cid) {
        let store = Arc::new(MemStore::new());
        let config = turboquant_code_tree_config();
        let root = Node {
            keys: vec![b"only-code".to_vec()],
            vals: vec![vec![0; 8 + packed_len(8, 4).unwrap()]],
            child_counts: Vec::new(),
            leaf: true,
            level: 0,
            format: config.format,
        };
        let root_bytes = root.to_bytes();
        let code_root = Cid::from_bytes(&root_bytes);
        Store::put(&store, code_root.as_bytes(), &root_bytes).unwrap();
        let manifest = Manifest {
            source: Cid::from_bytes(b"source"),
            dimensions: 8,
            metric: DistanceMetric::L2Squared,
            count: 2,
            config: TurboQuantizationConfig::default(),
            transform_id: STRUCTURED_ROTATION_ID,
            codebook_id: NORMAL_LLOYD_MAX_CODEBOOK_ID,
            code_root,
            quality: TurboQuantizationQuality::default(),
            zero_vectors: 0,
        };
        let manifest_bytes = manifest.encode().unwrap();
        let manifest_cid = Cid::from_bytes(&manifest_bytes);
        Store::put(&store, manifest_cid.as_bytes(), &manifest_bytes).unwrap();
        (store, manifest_cid)
    }

    fn assert_root_count_error(error: Error) {
        assert!(matches!(
            error,
            Error::InvalidProximityObject { kind: "TurboQuant", reason }
                if reason == "TurboQuant code-tree root count disagrees with manifest"
        ));
    }

    #[test]
    fn splitmix64_v1_is_frozen() {
        let mut stream = SplitMix64::new(0);
        assert_eq!(stream.next(), 0xe220_a839_7b1d_cdaf);
        assert_eq!(stream.next(), 0x6e78_9e6a_a1b9_65f4);
        assert_eq!(stream.next(), 0x06c4_5d18_8009_454f);
        assert_eq!(multiply_high(u64::MAX, 1), 0);
        assert_eq!(multiply_high(u64::MAX, u64::MAX), u64::MAX - 1);
        assert_eq!(multiply_high(0x8000_0000_0000_0000, 8), 4);
    }

    #[test]
    fn load_rejects_a_code_tree_root_count_that_disagrees_with_the_manifest() {
        let (store, manifest_cid) = mismatched_root_count_fixture();
        let error = match TurboQuantizer::load(store, manifest_cid) {
            Ok(_) => panic!("mismatched TurboQuant root count loaded"),
            Err(error) => error,
        };
        assert_root_count_error(error);
    }

    #[test]
    fn direct_search_rejects_a_manifest_count_that_disagrees_with_the_source() {
        use crate::prolly::proximity::{
            CompositeAccelerator, CompositeAcceleratorConfig, CompositeBase, CompositeBuildLimits,
            ProximityConfig, ProximityRecord,
        };

        let store = Arc::new(MemStore::new());
        let map = ProximityMap::build(
            store.clone(),
            ProximityConfig::new(8),
            [
                ProximityRecord {
                    key: b"first".to_vec(),
                    vector: vec![1.0; 8],
                    value: b"first value".to_vec(),
                },
                ProximityRecord {
                    key: b"second".to_vec(),
                    vector: vec![2.0; 8],
                    value: b"second value".to_vec(),
                },
            ],
        )
        .unwrap();
        let (index, _) = TurboQuantizer::build(
            &map,
            TurboQuantizationConfig::default(),
            BuildParallelism::serial(),
        )
        .unwrap();

        // Construct an internally valid one-record code tree, then bind it to
        // the real two-record descriptor through a forged manifest. Loading
        // authenticates the complete derived closure; execution must still
        // reject the manifest/source cardinality disagreement before scanning.
        let (key, code) = index
            .codes
            .range(&index.code_tree, &[], None)
            .unwrap()
            .next()
            .unwrap()
            .unwrap();
        let mut builder = SortedBatchBuilder::new_with_origin(
            store.clone(),
            turboquant_code_tree_config(),
            PublicationOrigin::Maintenance,
        );
        builder.add(key, code).unwrap();
        let partial_tree = builder.build().unwrap();
        let original_manifest = Store::get(&store, index.manifest_cid().as_bytes())
            .unwrap()
            .unwrap();
        let mut forged = Manifest::decode(&original_manifest).unwrap();
        forged.count = 1;
        forged.code_root = partial_tree.root.unwrap();
        let forged_bytes = forged.encode().unwrap();
        let forged_cid = Cid::from_bytes(&forged_bytes);
        Store::put(&store, forged_cid.as_bytes(), &forged_bytes).unwrap();
        let forged = TurboQuantizer::load(store, forged_cid).unwrap();

        let mut request = SearchRequest::exact(&[1.0; 8], 1);
        request.policy = SearchPolicy::FixedBudget;
        request.options.backend = SearchBackend::TurboQuantized;
        assert!(matches!(
            forged.search(&map, request),
            Err(Error::InvalidProximitySearch { reason })
                if reason == "TurboQuant source count mismatch"
        ));
        assert!(matches!(
            CompositeAccelerator::build(
                &map,
                &map,
                CompositeBase::TurboQuantized(forged),
                CompositeAcceleratorConfig::default(),
                CompositeBuildLimits::default(),
            ),
            Err(Error::InvalidProximitySearch { reason })
                if reason
                    == "composite base/current sources or accelerator configuration disagree"
        ));
    }

    #[test]
    fn every_turboquant_manifest_binding_mutation_fails_closed() {
        use crate::prolly::proximity::{ProximityConfig, ProximityRecord};

        let store = Arc::new(MemStore::new());
        let map = ProximityMap::build(
            store.clone(),
            ProximityConfig::new(8),
            [
                ProximityRecord {
                    key: b"first".to_vec(),
                    vector: vec![1.0, -2.0, 3.0, -4.0, 5.0, -6.0, 7.0, -8.0],
                    value: Vec::new(),
                },
                ProximityRecord {
                    key: b"second".to_vec(),
                    vector: vec![-8.0, 7.0, -6.0, 5.0, -4.0, 3.0, -2.0, 1.0],
                    value: Vec::new(),
                },
            ],
        )
        .unwrap();
        let (index, _) = TurboQuantizer::build(
            &map,
            TurboQuantizationConfig::default(),
            BuildParallelism::serial(),
        )
        .unwrap();
        let manifest_bytes = Store::get(&store, index.manifest_cid().as_bytes())
            .unwrap()
            .unwrap();
        let manifest = Manifest::decode(&manifest_bytes).unwrap();

        let assert_fails_closed = |candidate: Manifest, field: &str| {
            let bytes = candidate.encode().unwrap();
            let cid = Cid::from_bytes(&bytes);
            Store::put(&store, cid.as_bytes(), &bytes).unwrap();
            if let Ok(candidate) = TurboQuantizer::load(store.clone(), cid) {
                assert!(
                    candidate.verify(&map).is_err(),
                    "mutated TurboQuant {field} passed full verification"
                );
            }
        };

        let mut source = manifest.clone();
        source.source = Cid::from_bytes(b"another source");
        assert_fails_closed(source, "source CID");

        let mut dimensions = manifest.clone();
        dimensions.dimensions = 16;
        assert_fails_closed(dimensions, "dimensions");

        let mut metric = manifest.clone();
        metric.metric = DistanceMetric::Cosine;
        assert_fails_closed(metric, "metric");

        let mut count = manifest.clone();
        count.count += 1;
        assert_fails_closed(count, "count");

        let mut seed = manifest.clone();
        seed.config.seed ^= 1;
        assert_fails_closed(seed, "seed");

        let mut transform = manifest.clone();
        transform.transform_id += 1;
        assert_fails_closed(transform, "transform ID");

        let mut codebook = manifest.clone();
        codebook.codebook_id += 1;
        assert_fails_closed(codebook, "codebook ID");

        let mut bit_width = manifest.clone();
        bit_width.config.bit_width = 3;
        assert_fails_closed(bit_width, "bit width");

        let mut quality = manifest.clone();
        quality.quality.mean_squared_error += 1.0;
        quality.quality.maximum_squared_error += 1.0;
        assert_fails_closed(quality, "quality bits");

        let mut zero_vectors = manifest.clone();
        zero_vectors.zero_vectors = 1;
        assert_fails_closed(zero_vectors, "zero-vector count");

        let (alternate, _) = TurboQuantizer::build(
            &map,
            TurboQuantizationConfig {
                seed: 1,
                ..TurboQuantizationConfig::default()
            },
            BuildParallelism::serial(),
        )
        .unwrap();
        let alternate_bytes = Store::get(&store, alternate.manifest_cid().as_bytes())
            .unwrap()
            .unwrap();
        let mut code_root = manifest;
        code_root.code_root = Manifest::decode(&alternate_bytes).unwrap().code_root;
        assert_fails_closed(code_root, "code root");

        let mut fingerprint = manifest_bytes;
        *fingerprint.last_mut().unwrap() ^= 1;
        let fingerprint_cid = Cid::from_bytes(&fingerprint);
        Store::put(&store, fingerprint_cid.as_bytes(), &fingerprint).unwrap();
        assert!(matches!(
            TurboQuantizer::load(store, fingerprint_cid),
            Err(Error::InvalidProximityObject { kind: "TurboQuant", reason })
                if reason == "TurboQuant configuration fingerprint mismatch"
        ));
    }

    #[cfg(feature = "async-store")]
    #[test]
    fn async_load_rejects_a_code_tree_root_count_that_disagrees_with_the_manifest() {
        use crate::prolly::proximity::AsyncTurboQuantizer;
        use crate::prolly::store::SyncStoreAsAsync;
        use std::future::Future;
        use std::task::{Context, Poll};

        fn block_on<F: Future>(future: F) -> F::Output {
            let waker = futures_util::task::noop_waker();
            let mut context = Context::from_waker(&waker);
            let mut future = Box::pin(future);
            loop {
                match future.as_mut().poll(&mut context) {
                    Poll::Ready(value) => return value,
                    Poll::Pending => std::thread::yield_now(),
                }
            }
        }

        let (store, manifest_cid) = mismatched_root_count_fixture();
        let store = SyncStoreAsAsync::new(store);
        let error = match block_on(AsyncTurboQuantizer::load(&store, manifest_cid)) {
            Ok(_) => panic!("mismatched async TurboQuant root count loaded"),
            Err(error) => error,
        };
        assert_root_count_error(error);
    }

    #[test]
    fn typed_walk_validates_turboquant_leaf_values_in_manifest_context() {
        use crate::prolly::content_graph::{
            walk_content_graph, ContentGraphLimits, ContentObjectKind, TypedContentRoot,
        };
        use crate::prolly::proximity::{ProximityConfig, ProximityRecord};

        let store = Arc::new(MemStore::new());
        let map = ProximityMap::build(
            store.clone(),
            ProximityConfig::new(8),
            [ProximityRecord {
                key: b"vector".to_vec(),
                vector: vec![1.0; 8],
                value: b"value".to_vec(),
            }],
        )
        .unwrap();
        let (index, _) = TurboQuantizer::build(
            &map,
            TurboQuantizationConfig::default(),
            BuildParallelism::serial(),
        )
        .unwrap();
        let manifest_bytes = Store::get(&store, index.manifest_cid().as_bytes())
            .unwrap()
            .unwrap();
        let mut manifest = Manifest::decode(&manifest_bytes).unwrap();
        let root_bytes = Store::get(&store, manifest.code_root.as_bytes())
            .unwrap()
            .unwrap();
        let mut root = decode_code_tree_node(&root_bytes).unwrap();
        assert!(root.leaf);
        root.vals[0].pop();
        let corrupt_root_bytes = root.to_bytes();
        manifest.code_root = Cid::from_bytes(&corrupt_root_bytes);
        Store::put(&store, manifest.code_root.as_bytes(), &corrupt_root_bytes).unwrap();
        let corrupt_manifest_bytes = manifest.encode().unwrap();
        let corrupt_manifest = Cid::from_bytes(&corrupt_manifest_bytes);
        Store::put(&store, corrupt_manifest.as_bytes(), &corrupt_manifest_bytes).unwrap();

        let error = walk_content_graph(
            &store,
            &[TypedContentRoot::new(
                ContentObjectKind::TurboQuantization,
                corrupt_manifest,
            )],
            &ContentGraphLimits::default(),
        )
        .unwrap_err();
        assert!(matches!(
            error,
            Error::InvalidProximityObject { kind: "TurboQuant", reason }
                if reason == "invalid TurboQuant code value length"
        ));
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
    fn grouped_centroid_decode_matches_the_canonical_bit_decoder() {
        for dimensions in [8usize, 24, 128, 200, 768] {
            for bit_width in [2, 3, 4] {
                let maximum = 1u8 << bit_width;
                let codes: Vec<_> = (0..dimensions)
                    .map(|index| ((index * 11 + 5) as u8) % maximum)
                    .collect();
                let packed = pack_codes(&codes, bit_width).unwrap();
                let codebook = codebook(bit_width);
                for start in (0..dimensions).step_by(64) {
                    let end = start.saturating_add(64).min(dimensions);
                    let mut decoded = vec![0.0; end - start];
                    fill_centroids(&packed, start, bit_width, codebook, &mut decoded);
                    for (offset, centroid) in decoded.into_iter().enumerate() {
                        assert_eq!(
                            centroid.to_bits(),
                            codebook
                                .centroid(unpack_code(&packed, start + offset, bit_width))
                                .to_bits(),
                            "dimensions={dimensions}, bits={bit_width}, index={}",
                            start + offset,
                        );
                    }
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
            for round in &plan.rounds {
                let mut permutation = round.permutation.clone();
                permutation.sort_unstable();
                assert_eq!(permutation, (0..dimensions).collect::<Vec<_>>());
                assert!(round.signs.iter().all(|sign| matches!(sign, -1 | 1)));
            }
        }
        let left = StructuredRotation::derive(128, 0x5eed).unwrap();
        let right = StructuredRotation::derive(128, 0x5eee).unwrap();
        assert_ne!(left.rounds[0].permutation, right.rounds[0].permutation);
        assert_ne!(left.rounds[0].signs, right.rounds[0].signs);
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
            for bit_width in [2, 3, 4] {
                let weighted = (0..dimensions)
                    .map(|index| ((index as f64 + 0.25) * 0.03125).sin())
                    .collect::<Vec<_>>();
                let scalar_prepared = TurboQuantPreparedQuery::new(
                    weighted.clone(),
                    17.25,
                    bit_width,
                    QueryKernel::ScalarDeterministic,
                );
                let simd_prepared = TurboQuantPreparedQuery::new(
                    weighted.clone(),
                    17.25,
                    bit_width,
                    QueryKernel::SimdDeterministic,
                );
                let automatic_prepared = TurboQuantPreparedQuery::new(
                    weighted,
                    17.25,
                    bit_width,
                    QueryKernel::AutoDeterministic,
                );
                assert_eq!(
                    scalar_prepared.weighted_centroids.len(),
                    dimensions * (1usize << bit_width)
                );
                assert!(simd_prepared.weighted_centroids.is_empty());
                assert_eq!(
                    automatic_prepared.weighted_centroids.len(),
                    dimensions * (1usize << bit_width)
                );
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
                        &scalar_prepared,
                        metric,
                        dimensions,
                        bit_width,
                        QueryKernel::ScalarDeterministic,
                    )
                    .unwrap();
                    let simd = score_code_value(
                        &encoded,
                        &simd_prepared,
                        metric,
                        dimensions,
                        bit_width,
                        QueryKernel::SimdDeterministic,
                    )
                    .unwrap();
                    let automatic = score_code_value(
                        &encoded,
                        &automatic_prepared,
                        metric,
                        dimensions,
                        bit_width,
                        QueryKernel::AutoDeterministic,
                    )
                    .unwrap();
                    assert_eq!(
                        scalar.to_bits(),
                        simd.to_bits(),
                        "dimension={dimensions}, bits={bit_width}, metric={metric:?}",
                    );
                    assert_eq!(
                        scalar.to_bits(),
                        automatic.to_bits(),
                        "automatic dimension={dimensions}, bits={bit_width}, metric={metric:?}",
                    );
                }
            }
        }
    }

    #[test]
    fn l2_approximate_score_clamps_negative_estimates_to_positive_zero() {
        let prepared = TurboQuantPreparedQuery::new(
            vec![1_000.0; 8],
            1.0,
            2,
            QueryKernel::ScalarDeterministic,
        );
        let codes = vec![3; 8];
        let mut encoded = 1.0f64.to_le_bytes().to_vec();
        encoded.extend_from_slice(&pack_codes(&codes, 2).unwrap());
        let score = score_code_value(
            &encoded,
            &prepared,
            DistanceMetric::L2Squared,
            8,
            2,
            QueryKernel::ScalarDeterministic,
        )
        .unwrap();
        assert_eq!(score.to_bits(), 0.0f64.to_bits());
    }

    #[test]
    fn turboquant_codec_transform_and_scorer_fuzz_smoke_is_bounded() {
        let mut state = 0x4d59_5df4_d0f3_3173u64;
        let mut next = || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        for case in 0..10_000 {
            let length = next() as usize % 1025;
            let bytes = (0..length).map(|_| next() as u8).collect::<Vec<_>>();
            let _ = Manifest::decode(&bytes);
            for bit_width in [2, 3, 4] {
                let _ = validate_code_value(&bytes, 8 + (next() as usize % 257), bit_width);
            }

            if case < 512 {
                let dimensions = 8 * (1 + next() as usize % 32);
                let plan = StructuredRotation::derive(dimensions, next()).unwrap();
                let input = (0..dimensions)
                    .map(|_| (next() as i16 as f64) / 4096.0)
                    .collect::<Vec<_>>();
                let transformed = plan.apply(&input);
                assert_eq!(transformed.len(), dimensions);
                assert!(transformed.iter().all(|value| value.is_finite()));
                let norm_squared = input.iter().fold(0.0, |sum, value| sum + value * value);

                for bit_width in [2, 3, 4] {
                    let mut encoded = ((next() % 2_048 + 1) as f64 / 17.0).to_le_bytes().to_vec();
                    encoded.extend(
                        (0..packed_len(dimensions, bit_width).unwrap()).map(|_| next() as u8),
                    );
                    assert!(validate_code_value(&encoded, dimensions, bit_width).is_ok());
                    for index in 0..dimensions {
                        assert!(unpack_code(&encoded[8..], index, bit_width) < 1 << bit_width);
                    }

                    let prepared = TurboQuantPreparedQuery::new(
                        transformed.clone(),
                        norm_squared,
                        bit_width,
                        QueryKernel::ScalarDeterministic,
                    );
                    for metric in [
                        DistanceMetric::L2Squared,
                        DistanceMetric::Cosine,
                        DistanceMetric::InnerProduct,
                    ] {
                        let score = score_code_value(
                            &encoded,
                            &prepared,
                            metric,
                            dimensions,
                            bit_width,
                            QueryKernel::ScalarDeterministic,
                        )
                        .unwrap();
                        assert!(score.is_finite());
                    }
                }
            }
        }
        let maximum_plan = StructuredRotation::derive(MAX_DIMENSIONS as usize, u64::MAX).unwrap();
        assert_eq!(maximum_plan.dimensions(), MAX_DIMENSIONS as usize);
        assert!(maximum_plan.operations_per_vector() > 0);
        assert!(maximum_plan.owned_bytes() > 0);
        assert_eq!(packed_len(MAX_DIMENSIONS as usize, 4).unwrap(), 8192);
        assert!(packed_len(usize::MAX, 4).is_err());
        assert!(packed_len(usize::MAX / 2 + 1, 3).is_err());
    }

    #[test]
    fn checked_in_turboquant_conformance_values_are_frozen() {
        let hex = |cid: Cid| {
            cid.as_bytes()
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>()
        };
        let bytes_hex = |bytes: &[u8]| {
            bytes
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>()
        };
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../conformance/proximity-fixtures.json"
        ))
        .unwrap();
        let fixture = &fixture["turboquant"];

        let mut splitmix = SplitMix64::new(0);
        for expected in fixture["splitmix64_seed_zero"].as_array().unwrap() {
            assert_eq!(
                format!("{:016x}", splitmix.next()),
                expected.as_str().unwrap()
            );
        }
        for dimensions in [8usize, 24, 128, 200, 768, 1536, 3072] {
            let plan = StructuredRotation::derive(dimensions, 0x5eed).unwrap();
            let mut plan_bytes = Vec::new();
            for round in &plan.rounds {
                for &index in &round.permutation {
                    plan_bytes.extend_from_slice(&(index as u64).to_le_bytes());
                }
                plan_bytes.extend(round.signs.iter().map(|sign| *sign as u8));
            }
            // Construct the input directly from IEEE-754 bits. Calling libm
            // here (for example, `sin`) makes the *test input* vary by target
            // before the deterministic transform is exercised.
            let mut input_stream = SplitMix64::new(0x5451_4649_5854_0001);
            let input: Vec<_> = (0..dimensions)
                .map(|_| {
                    let draw = input_stream.next();
                    let sign = draw & (1u64 << 63);
                    let fraction = draw & ((1u64 << 52) - 1);
                    f64::from_bits(sign | 0x3fe0_0000_0000_0000 | fraction)
                })
                .collect();
            let output = plan.apply(&input);
            let input_bytes: Vec<_> = input
                .iter()
                .flat_map(|value| value.to_bits().to_le_bytes())
                .collect();
            let output_bytes: Vec<_> = output
                .iter()
                .flat_map(|value| value.to_bits().to_le_bytes())
                .collect();
            let expected = &fixture["rotation"][dimensions.to_string()];
            assert_eq!(
                hex(Cid::from_bytes(&plan_bytes)),
                expected["plan_sha256"].as_str().unwrap(),
                "plan mismatch at dimension {dimensions}",
            );
            assert_eq!(
                hex(Cid::from_bytes(&input_bytes)),
                expected["input_sha256"].as_str().unwrap(),
                "input mismatch at dimension {dimensions}",
            );
            assert_eq!(
                hex(Cid::from_bytes(&output_bytes)),
                expected["output_sha256"].as_str().unwrap(),
                "output mismatch at dimension {dimensions}",
            );
        }
        for bit_width in [2, 3, 4] {
            let maximum = 1u8 << bit_width;
            let codes: Vec<_> = (0..19).map(|index| index as u8 % maximum).collect();
            assert_eq!(
                bytes_hex(&pack_codes(&codes, bit_width).unwrap()),
                fixture["packing_19_codes"][bit_width.to_string()]
                    .as_str()
                    .unwrap(),
            );
            let codebook = codebook(bit_width);
            let expected = &fixture["codebooks"][bit_width.to_string()];
            assert_eq!(
                codebook
                    .thresholds
                    .iter()
                    .map(|bits| format!("{bits:016x}"))
                    .collect::<Vec<_>>(),
                expected["threshold_bits"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|value| value.as_str().unwrap())
                    .collect::<Vec<_>>(),
            );
            assert_eq!(
                codebook
                    .centroids
                    .iter()
                    .map(|bits| format!("{bits:016x}"))
                    .collect::<Vec<_>>(),
                expected["centroid_bits"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|value| value.as_str().unwrap())
                    .collect::<Vec<_>>(),
            );
        }
    }
}
