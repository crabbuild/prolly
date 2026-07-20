use crate::prolly::builder::SortedBatchBuilder;
use crate::prolly::cid::Cid;
use crate::prolly::config::Config;
use crate::prolly::encoding::Encoding;
use crate::prolly::error::Error;
use crate::prolly::proximity::distance::{prepare_vector, query_score};
use crate::prolly::proximity::search::{
    retained_candidate_bytes, EligibilityCardinality, PreparedFilter, RerankCandidate,
};
use crate::prolly::proximity::storage::codec::{
    put_cid, put_f32, put_f64, put_varint, Reader, MAX_OBJECT_ENTRIES,
};
use crate::prolly::proximity::storage::StoredRecord;
use crate::prolly::proximity::{
    BuildParallelism, DistanceMetric, ProximityMap, ProximitySearchStats, SearchBackend,
    SearchCompletion, SearchPolicy, SearchRequest, SearchResult,
};
use crate::prolly::store::{NodePublication, PublicationOrigin, Store};
use crate::prolly::tree::Tree;
use crate::prolly::Prolly;
use rayon::prelude::*;
use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashSet};
use xxhash_rust::xxh64::xxh64;

const MAGIC: &[u8; 4] = b"PQPQ";
const PQ_FORMAT_VERSION: u8 = 2;
const SAMPLING_HASH_ALGORITHM_XXH64: u8 = 1;
const SAMPLING_HASH_VERSION: u8 = 1;
type Codebooks = Vec<Vec<Vec<f32>>>;
type TrainingOutput = (Codebooks, usize);

/// Deterministic offline product-quantization training and serving policy.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProductQuantizationConfig {
    pub subquantizers: u32,
    pub centroids_per_subquantizer: u16,
    pub training_iterations: u16,
    pub rerank_multiplier: u32,
    pub seed: u64,
    pub max_training_vectors: usize,
}

impl Default for ProductQuantizationConfig {
    fn default() -> Self {
        Self {
            subquantizers: 8,
            centroids_per_subquantizer: 256,
            training_iterations: 12,
            rerank_multiplier: 8,
            seed: 0,
            max_training_vectors: 65_536,
        }
    }
}

impl ProductQuantizationConfig {
    pub(crate) fn validate(&self, dimensions: u32, records: usize) -> Result<(), Error> {
        if self.subquantizers == 0 || self.subquantizers > dimensions {
            return Err(invalid_config("subquantizers must be in 1..=dimensions"));
        }
        let centroids = usize::from(self.centroids_per_subquantizer);
        if centroids == 0 || centroids > 256 || centroids > records {
            return Err(invalid_config(
                "centroids_per_subquantizer must be in 1..=min(256, record_count)",
            ));
        }
        if self.training_iterations == 0
            || self.rerank_multiplier == 0
            || self.max_training_vectors < centroids
        {
            return Err(invalid_config(
                "training_iterations/rerank_multiplier must be positive and max_training_vectors must cover all centroids",
            ));
        }
        Ok(())
    }
}

/// Reconstruction measurements committed into a PQ manifest.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct ProductQuantizationQuality {
    pub mean_squared_error: f64,
    pub maximum_squared_error: f64,
}

/// Canonical logical work performed by one PQ build.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ProductQuantizationBuildStats {
    pub training_distance_evaluations: usize,
    pub encoding_distance_evaluations: usize,
    pub encoded_vectors: usize,
    pub training_vectors: usize,
    pub training_bytes: usize,
    pub encoded_output_bytes: usize,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ProductQuantizationBuildLimits {
    pub max_training_vectors: Option<usize>,
    pub max_training_bytes: Option<usize>,
    pub max_temporary_code_bytes: Option<usize>,
    pub max_distance_evaluations: Option<usize>,
    pub max_encoded_output_bytes: Option<usize>,
    pub max_worker_threads: Option<usize>,
}

impl ProductQuantizationBuildLimits {
    fn validate(&self) -> Result<(), Error> {
        for (name, value) in [
            ("max_training_vectors", self.max_training_vectors),
            ("max_training_bytes", self.max_training_bytes),
            ("max_temporary_code_bytes", self.max_temporary_code_bytes),
            ("max_distance_evaluations", self.max_distance_evaluations),
            ("max_encoded_output_bytes", self.max_encoded_output_bytes),
            ("max_worker_threads", self.max_worker_threads),
        ] {
            if value == Some(0) {
                return Err(invalid_config(format!("PQ {name} must be positive")));
            }
        }
        Ok(())
    }
}

/// Source-bound persisted product-quantization sidecar.
pub struct ProductQuantizer<S: Store> {
    codes: Prolly<S>,
    code_tree: Tree,
    manifest: Cid,
    pub(super) source: Cid,
    pub(super) dimensions: u32,
    pub(super) metric: DistanceMetric,
    pub(super) count: u64,
    config: ProductQuantizationConfig,
    codebooks: Codebooks,
    quality: ProductQuantizationQuality,
}

impl<S> ProductQuantizer<S>
where
    S: Store + Clone + Send + Sync,
    S::Error: Send + Sync,
{
    /// Train in key order, persist encoded vectors, and publish the manifest last.
    pub fn build(
        map: &ProximityMap<S>,
        config: ProductQuantizationConfig,
        parallelism: BuildParallelism,
    ) -> Result<(Self, ProductQuantizationBuildStats), Error> {
        Self::build_with_limits(
            map,
            config,
            parallelism,
            ProductQuantizationBuildLimits::default(),
        )
    }

    pub fn build_with_limits(
        map: &ProximityMap<S>,
        config: ProductQuantizationConfig,
        parallelism: BuildParallelism,
        limits: ProductQuantizationBuildLimits,
    ) -> Result<(Self, ProductQuantizationBuildStats), Error> {
        limits.validate()?;
        let record_count = usize::try_from(map.tree().count)
            .map_err(|_| resource_limit("records", usize::MAX, usize::MAX))?;
        config.validate(map.tree().config.dimensions, record_count)?;
        if let Some(limit) = limits.max_worker_threads {
            enforce_resource("worker_threads", Some(limit), parallelism.threads())?;
        }
        let sample_target = config.max_training_vectors.min(record_count);
        enforce_resource(
            "training_vectors",
            limits.max_training_vectors,
            sample_target,
        )?;
        enforce_resource(
            "temporary_code_bytes",
            limits.max_temporary_code_bytes,
            config.subquantizers as usize,
        )?;

        let mut samples = BinaryHeap::<TrainingSample>::with_capacity(sample_target);
        for entry in map
            .directory_manager()
            .range(&map.tree().directory, &[], None)?
        {
            let (key, bytes) = entry?;
            let stored = StoredRecord::decode(&bytes, map.tree().config.dimensions)?;
            let sample = TrainingSample {
                hash: xxh64(&key, config.seed),
                key,
                vector: stored.vector,
            };
            if samples.len() < sample_target {
                samples.push(sample);
            } else if samples.peek().is_some_and(|worst| sample < *worst) {
                samples.pop();
                samples.push(sample);
            }
        }
        let mut samples = samples.into_vec();
        samples.sort_by(|left, right| left.key.cmp(&right.key));
        let sample_bytes = samples.iter().try_fold(0usize, |total, sample| {
            total
                .checked_add(sample.key.len())
                .and_then(|value| value.checked_add(sample.vector.len().checked_mul(4)?))
                .and_then(|value| value.checked_add(std::mem::size_of::<TrainingSample>()))
                .ok_or_else(|| resource_limit("training_bytes", usize::MAX, usize::MAX))
        })?;
        let assignment_bytes = samples
            .len()
            .checked_mul(config.subquantizers as usize)
            .ok_or_else(|| resource_limit("training_bytes", usize::MAX, usize::MAX))?;
        let centroid_components = (map.tree().config.dimensions as usize)
            .checked_mul(usize::from(config.centroids_per_subquantizer))
            .ok_or_else(|| resource_limit("training_bytes", usize::MAX, usize::MAX))?;
        let centroid_bytes = centroid_components
            .checked_mul(4 + 8)
            .and_then(|value| {
                value.checked_add(
                    (config.subquantizers as usize)
                        .checked_mul(usize::from(config.centroids_per_subquantizer))?
                        .checked_mul(std::mem::size_of::<usize>())?,
                )
            })
            .ok_or_else(|| resource_limit("training_bytes", usize::MAX, usize::MAX))?;
        let training_bytes = sample_bytes
            .checked_add(assignment_bytes)
            .and_then(|value| value.checked_add(centroid_bytes))
            .ok_or_else(|| resource_limit("training_bytes", usize::MAX, usize::MAX))?;
        enforce_resource("training_bytes", limits.max_training_bytes, training_bytes)?;
        let expected_training_evaluations = samples
            .len()
            .checked_mul(config.subquantizers as usize)
            .and_then(|value| value.checked_mul(usize::from(config.centroids_per_subquantizer)))
            .and_then(|value| value.checked_mul(usize::from(config.training_iterations)))
            .ok_or_else(|| resource_limit("distance_evaluations", usize::MAX, usize::MAX))?;
        enforce_resource(
            "distance_evaluations",
            limits.max_distance_evaluations,
            expected_training_evaluations,
        )?;
        let vectors: Vec<_> = samples
            .iter()
            .map(|sample| sample.vector.as_slice())
            .collect();
        let (codebooks, training_evaluations) =
            train(&vectors, map.tree().config.dimensions, &config, parallelism)?;

        let store = map.store_clone();
        let code_config = code_tree_config();
        let mut builder = SortedBatchBuilder::new_with_origin(
            store.clone(),
            code_config.clone(),
            PublicationOrigin::Maintenance,
        );
        let layout = subspace_layout(
            map.tree().config.dimensions as usize,
            config.subquantizers as usize,
        );
        let encoding_per_vector = layout
            .len()
            .checked_mul(usize::from(config.centroids_per_subquantizer))
            .ok_or_else(|| resource_limit("distance_evaluations", usize::MAX, usize::MAX))?;
        let mut encoding_evaluations = 0usize;
        let mut encoded_output_bytes = 0usize;
        let mut quality_sum = 0.0f64;
        let mut quality_maximum = 0.0f64;
        let mut encoded_vectors = 0usize;
        for entry in map
            .directory_manager()
            .range(&map.tree().directory, &[], None)?
        {
            let (key, bytes) = entry?;
            let stored = StoredRecord::decode(&bytes, map.tree().config.dimensions)?;
            encoding_evaluations = encoding_evaluations
                .checked_add(encoding_per_vector)
                .ok_or_else(|| resource_limit("distance_evaluations", usize::MAX, usize::MAX))?;
            let total_evaluations = training_evaluations
                .checked_add(encoding_evaluations)
                .ok_or_else(|| resource_limit("distance_evaluations", usize::MAX, usize::MAX))?;
            enforce_resource(
                "distance_evaluations",
                limits.max_distance_evaluations,
                total_evaluations,
            )?;
            let code = encode_vector(&stored.vector, &layout, &codebooks);
            let error = reconstruction_error(&stored.vector, &code, &codebooks);
            quality_sum += error;
            quality_maximum = quality_maximum.max(error);
            encoded_output_bytes = encoded_output_bytes
                .checked_add(key.len())
                .and_then(|value| value.checked_add(code.len()))
                .ok_or_else(|| resource_limit("encoded_output_bytes", usize::MAX, usize::MAX))?;
            enforce_resource(
                "encoded_output_bytes",
                limits.max_encoded_output_bytes,
                encoded_output_bytes,
            )?;
            builder.add(key, code)?;
            encoded_vectors += 1;
        }
        let code_tree = builder.build()?;
        let code_root = code_tree
            .root
            .clone()
            .ok_or_else(|| invalid_object("product quantization requires a non-empty code tree"))?;
        let quality = ProductQuantizationQuality {
            mean_squared_error: quality_sum / encoded_vectors as f64,
            maximum_squared_error: quality_maximum,
        };
        let manifest_object = Manifest {
            source: map.tree().descriptor.clone(),
            dimensions: map.tree().config.dimensions,
            metric: map.tree().config.metric,
            count: map.tree().count,
            config: config.clone(),
            code_root,
            codebooks: codebooks.clone(),
            quality,
            sampling_hash_algorithm: SAMPLING_HASH_ALGORITHM_XXH64,
            sampling_hash_version: SAMPLING_HASH_VERSION,
            training_sample_count: samples.len() as u64,
        };
        let manifest_bytes = manifest_object.encode()?;
        let manifest = Cid::from_bytes(&manifest_bytes);
        let existed = store
            .get(manifest.as_bytes())
            .map_err(|error| Error::Store(Box::new(error)))?;
        if let Some(bytes) = existed {
            let actual = Cid::from_bytes(&bytes);
            if actual != manifest {
                return Err(Error::CidMismatch {
                    expected: manifest,
                    actual,
                });
            }
        } else {
            let entries = [(manifest.as_bytes(), manifest_bytes.as_slice())];
            store
                .publish_nodes(NodePublication::new(
                    &entries,
                    PublicationOrigin::Maintenance,
                ))
                .map_err(|error| Error::Store(Box::new(error)))?;
        }
        let stats = ProductQuantizationBuildStats {
            training_distance_evaluations: training_evaluations,
            encoding_distance_evaluations: encoding_evaluations,
            encoded_vectors,
            training_vectors: samples.len(),
            training_bytes,
            encoded_output_bytes,
        };
        Ok((
            Self {
                codes: Prolly::new(store, code_config),
                code_tree,
                manifest,
                source: manifest_object.source,
                dimensions: manifest_object.dimensions,
                metric: manifest_object.metric,
                count: manifest_object.count,
                config,
                codebooks,
                quality,
            },
            stats,
        ))
    }

    /// Load and validate a persisted PQ manifest and its encoded-vector root.
    pub fn load(store: S, manifest: Cid) -> Result<Self, Error> {
        let bytes = load_content(&store, &manifest)?;
        let object = Manifest::decode(&bytes)?;
        object.config.validate(
            object.dimensions,
            usize::from(object.config.centroids_per_subquantizer),
        )?;
        let code_tree = Tree {
            root: Some(object.code_root),
            config: code_tree_config(),
        };
        // Validate the root eagerly; individual codes are checked during search.
        let root = code_tree.root.as_ref().expect("manifest code root");
        load_content(&store, root)?;
        Ok(Self {
            codes: Prolly::new(store.clone(), code_tree.config.clone()),
            code_tree,
            manifest,
            source: object.source,
            dimensions: object.dimensions,
            metric: object.metric,
            count: object.count,
            config: object.config,
            codebooks: object.codebooks,
            quality: object.quality,
        })
    }

    pub fn manifest_cid(&self) -> &Cid {
        &self.manifest
    }

    pub fn source_descriptor(&self) -> &Cid {
        &self.source
    }

    pub fn config(&self) -> &ProductQuantizationConfig {
        &self.config
    }

    pub fn quality(&self) -> ProductQuantizationQuality {
        self.quality
    }

    pub(crate) fn rebind<T: Store>(&self, store: T) -> ProductQuantizer<T> {
        ProductQuantizer {
            codes: Prolly::new(store, self.code_tree.config.clone()),
            code_tree: self.code_tree.clone(),
            manifest: self.manifest.clone(),
            source: self.source.clone(),
            dimensions: self.dimensions,
            metric: self.metric,
            count: self.count,
            config: self.config.clone(),
            codebooks: self.codebooks.clone(),
            quality: self.quality,
        }
    }

    /// Search the PQ code tree, then rerank the deterministic shortlist using full vectors.
    pub fn search(
        &self,
        map: &ProximityMap<S>,
        request: SearchRequest<'_>,
    ) -> Result<SearchResult, Error> {
        request.validate()?;
        if request.policy == SearchPolicy::Exact {
            return Err(invalid_search(
                "product quantization cannot satisfy exact search",
            ));
        }
        if !matches!(
            request.options.backend,
            SearchBackend::ProductQuantized | SearchBackend::Auto
        ) {
            return Err(invalid_search(
                "product quantizer requires ProductQuantized or Auto backend",
            ));
        }
        let filter = PreparedFilter::new(request.filter.clone(), &map.tree().directory)?;
        let eligible_limit = match filter.cardinality(map.tree().count) {
            EligibilityCardinality::Known(count) => count as usize,
            EligibilityCardinality::Unknown => map.tree().count as usize,
        };
        let multiplier = request
            .options
            .pq
            .rerank_multiplier
            .map(usize::from)
            .unwrap_or(self.config.rerank_multiplier as usize);
        let plan = crate::prolly::proximity::search::SearchPlan::ProductQuantized {
            rerank_target: request
                .k
                .saturating_mul(multiplier)
                .max(request.k)
                .min(eligible_limit),
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
        let crate::prolly::proximity::search::SearchPlan::ProductQuantized {
            rerank_target,
            direct_lookup,
        } = plan
        else {
            return Err(invalid_search(
                "product quantization executor requires a PQ search plan",
            ));
        };
        request.validate()?;
        if request.policy == SearchPolicy::Exact {
            return Err(invalid_search(
                "product quantization cannot satisfy exact search",
            ));
        }
        if &self.source != expected_source
            || self.dimensions != map.tree().config.dimensions
            || self.metric != map.tree().config.metric
        {
            return Err(invalid_search(
                "product quantizer is bound to a different source descriptor",
            ));
        }

        let query = prepare_vector(self.metric, request.query, self.dimensions)?;
        let filter = PreparedFilter::new(request.filter.clone(), &map.tree().directory)?;
        let lookup = build_lookup(&query, self.metric, &self.codebooks);
        let mut stats = ProximitySearchStats::default();
        let mut approximate = BinaryHeap::<PqRanked>::new();
        let mut completion = SearchCompletion::ApproximatePolicySatisfied;
        if *direct_lookup {
            let Some((keys, source_bound)) = filter.sorted_keys() else {
                return Err(invalid_search(
                    "PQ direct-lookup plan requires sorted eligible keys",
                ));
            };
            for key in keys {
                if excluded(key)? {
                    continue;
                }
                let code = self.codes.get(&self.code_tree, key)?;
                let Some(code) = code else {
                    if source_bound {
                        return Err(invalid_object("source-bound eligible key has no PQ code"));
                    }
                    continue;
                };
                if !admit_code(
                    key.clone(),
                    code,
                    &lookup,
                    self.metric,
                    &self.codebooks,
                    *rerank_target,
                    &request,
                    &mut stats,
                    &mut approximate,
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
                if !admit_code(
                    key,
                    code,
                    &lookup,
                    self.metric,
                    &self.codebooks,
                    *rerank_target,
                    &request,
                    &mut stats,
                    &mut approximate,
                )? {
                    completion = SearchCompletion::BudgetExhausted;
                    break;
                }
            }
        }
        let mut approximate = approximate.into_vec();
        approximate.sort();
        let shortlist = approximate.len();

        let mut reranked = Vec::<RerankCandidate>::with_capacity(shortlist);
        let mut vector_scratch = vec![0.0f32; map.tree().config.dimensions as usize];
        let mut directory = map.directory_manager().read(&map.tree().directory)?;
        for candidate in approximate {
            if budget_exhausted(&request, &stats)
                || request
                    .budget
                    .max_nodes
                    .is_some_and(|limit| stats.nodes_read >= limit)
            {
                completion = SearchCompletion::BudgetExhausted;
                break;
            }
            let Some(handle) = directory.get_handle(&candidate.key)? else {
                return Err(invalid_object(
                    "PQ code key is absent from authoritative directory",
                ));
            };
            let bytes = handle.value()?.len();
            if request
                .budget
                .max_committed_bytes
                .is_some_and(|limit| stats.committed_bytes.saturating_add(bytes) > limit)
            {
                completion = SearchCompletion::BudgetExhausted;
                break;
            }
            let record = crate::prolly::proximity::storage::StoredRecordRef::decode(
                handle.value()?,
                map.tree().config.dimensions,
            )?;
            crate::prolly::proximity::ProximityVectorRef::from_encoded(record.vector)
                .copy_to_slice(&mut vector_scratch)?;
            let distance = query_score(request.kernel, self.metric, &query, &vector_scratch);
            stats.nodes_read += 1;
            stats.bytes_read = stats.bytes_read.saturating_add(bytes);
            stats.committed_bytes = stats.committed_bytes.saturating_add(bytes);
            stats.distance_evaluations += 1;
            reranked.push(RerankCandidate::new(handle, &candidate.key, distance)?);
        }
        stats.reranked_candidates = reranked.len();
        stats.candidate_handles_peak = reranked.len();
        stats.candidate_retained_bytes_peak = retained_candidate_bytes(&reranked);
        reranked.sort_by(|left, right| {
            left.distance
                .total_cmp(&right.distance)
                .then_with(|| left.key().cmp(right.key()))
        });
        let neighbors = reranked
            .into_iter()
            .take(request.k)
            .map(|candidate| candidate.into_neighbor(map.tree().config.dimensions))
            .collect::<Result<Vec<_>, Error>>()?;
        Ok(SearchResult {
            neighbors,
            stats,
            completion,
            plan: plan.summary(),
        })
    }
}

#[derive(Clone, Debug)]
struct PqRanked {
    distance: f64,
    key: Vec<u8>,
}

impl PartialEq for PqRanked {
    fn eq(&self, other: &Self) -> bool {
        self.distance.to_bits() == other.distance.to_bits() && self.key == other.key
    }
}

impl Eq for PqRanked {}

impl PartialOrd for PqRanked {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PqRanked {
    fn cmp(&self, other: &Self) -> Ordering {
        self.distance
            .total_cmp(&other.distance)
            .then_with(|| self.key.cmp(&other.key))
    }
}

#[allow(clippy::too_many_arguments)]
fn admit_code(
    key: Vec<u8>,
    code: Vec<u8>,
    lookup: &[Vec<f64>],
    metric: DistanceMetric,
    codebooks: &Codebooks,
    target: usize,
    request: &SearchRequest<'_>,
    stats: &mut ProximitySearchStats,
    approximate: &mut BinaryHeap<PqRanked>,
) -> Result<bool, Error> {
    if request
        .budget
        .max_nodes
        .is_some_and(|limit| stats.nodes_read >= limit)
        || request
            .budget
            .max_committed_bytes
            .is_some_and(|limit| stats.committed_bytes.saturating_add(code.len()) > limit)
        || request
            .budget
            .max_distance_evaluations
            .is_some_and(|limit| {
                stats
                    .distance_evaluations
                    .saturating_add(stats.quantized_distance_evaluations)
                    >= limit
            })
        || request
            .budget
            .max_frontier_entries
            .is_some_and(|limit| approximate.len().saturating_add(1) > limit)
    {
        return Ok(false);
    }
    validate_code(&code, codebooks)?;
    stats.nodes_read += 1;
    stats.bytes_read = stats.bytes_read.saturating_add(code.len());
    stats.committed_bytes = stats.committed_bytes.saturating_add(code.len());
    stats.quantized_distance_evaluations += 1;
    approximate.push(PqRanked {
        distance: score_code(metric, lookup, &code),
        key,
    });
    if approximate.len() > target {
        approximate.pop();
    }
    stats.frontier_peak = stats.frontier_peak.max(approximate.len());
    Ok(true)
}

#[derive(Clone)]
pub(crate) struct Manifest {
    pub(crate) source: Cid,
    pub(crate) dimensions: u32,
    pub(crate) metric: DistanceMetric,
    pub(crate) count: u64,
    pub(crate) config: ProductQuantizationConfig,
    pub(crate) code_root: Cid,
    pub(crate) codebooks: Codebooks,
    pub(crate) quality: ProductQuantizationQuality,
    pub(crate) sampling_hash_algorithm: u8,
    pub(crate) sampling_hash_version: u8,
    pub(crate) training_sample_count: u64,
}

impl Manifest {
    fn encode(&self) -> Result<Vec<u8>, Error> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(MAGIC);
        bytes.push(PQ_FORMAT_VERSION);
        bytes.push(0);
        put_cid(&self.source, &mut bytes);
        put_varint(u64::from(self.dimensions), &mut bytes);
        bytes.push(self.metric.id());
        put_varint(self.count, &mut bytes);
        bytes.push(self.sampling_hash_algorithm);
        bytes.push(self.sampling_hash_version);
        put_varint(self.training_sample_count, &mut bytes);
        encode_config(&self.config, &mut bytes);
        put_cid(&self.code_root, &mut bytes);
        put_varint(self.codebooks.len() as u64, &mut bytes);
        for subspace in &self.codebooks {
            put_varint(subspace.len() as u64, &mut bytes);
            let width = subspace.first().map_or(0, Vec::len);
            put_varint(width as u64, &mut bytes);
            for centroid in subspace {
                if centroid.len() != width {
                    return Err(invalid_object("inconsistent PQ centroid width"));
                }
                for &component in centroid {
                    put_f32(component, &mut bytes)?;
                }
            }
        }
        put_f64(self.quality.mean_squared_error, &mut bytes)?;
        put_f64(self.quality.maximum_squared_error, &mut bytes)?;
        put_cid(&config_fingerprint(&self.config), &mut bytes);
        Ok(bytes)
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, Error> {
        let mut reader = Reader::new(bytes, "product quantizer");
        reader.exact(MAGIC)?;
        require_pq_version(reader.u8()?)?;
        if reader.u8()? != 0 {
            return Err(reader.invalid("unknown flags"));
        }
        let source = reader.cid()?;
        let dimensions =
            u32::try_from(reader.varint()?).map_err(|_| reader.invalid("dimensions exceed u32"))?;
        let metric = DistanceMetric::from_id(reader.u8()?)?;
        let count = reader.varint()?;
        if count == 0 {
            return Err(reader.invalid("PQ source count must be positive"));
        }
        let sampling_hash_algorithm = reader.u8()?;
        let sampling_hash_version = reader.u8()?;
        let training_sample_count = reader.varint()?;
        if sampling_hash_algorithm != SAMPLING_HASH_ALGORITHM_XXH64
            || sampling_hash_version != SAMPLING_HASH_VERSION
            || training_sample_count == 0
            || training_sample_count > count
        {
            return Err(reader.invalid("unsupported or invalid PQ sampling policy"));
        }
        let config = decode_config(&mut reader)?;
        if training_sample_count != count.min(config.max_training_vectors as u64) {
            return Err(reader.invalid("PQ training sample count disagrees with configuration"));
        }
        let code_root = reader.cid()?;
        let subspaces = reader.bounded_usize(MAX_OBJECT_ENTRIES)?;
        if subspaces != config.subquantizers as usize {
            return Err(reader.invalid("subquantizer count mismatch"));
        }
        let mut codebooks = Vec::with_capacity(subspaces);
        let mut total_width = 0usize;
        for _ in 0..subspaces {
            let centroids = reader.bounded_usize(256)?;
            let width = reader.bounded_usize(dimensions as usize)?;
            if centroids != usize::from(config.centroids_per_subquantizer) || width == 0 {
                return Err(reader.invalid("PQ codebook shape mismatch"));
            }
            let count = centroids
                .checked_mul(width)
                .ok_or_else(|| reader.invalid("PQ codebook length overflow"))?;
            if count
                .checked_mul(4)
                .map_or(true, |len| len > reader.remaining())
            {
                return Err(reader.invalid("impossible PQ codebook length"));
            }
            let mut subspace = Vec::with_capacity(centroids);
            for _ in 0..centroids {
                let mut centroid = Vec::with_capacity(width);
                for _ in 0..width {
                    centroid.push(reader.f32()?);
                }
                subspace.push(centroid);
            }
            total_width = total_width
                .checked_add(width)
                .ok_or_else(|| reader.invalid("PQ dimension overflow"))?;
            codebooks.push(subspace);
        }
        if total_width != dimensions as usize {
            return Err(reader.invalid("PQ subspaces do not cover dimensions"));
        }
        let quality = ProductQuantizationQuality {
            mean_squared_error: reader.f64()?,
            maximum_squared_error: reader.f64()?,
        };
        if quality.mean_squared_error < 0.0 || quality.maximum_squared_error < 0.0 {
            return Err(reader.invalid("negative PQ quality measurement"));
        }
        if reader.cid()? != config_fingerprint(&config) {
            return Err(reader.invalid("PQ configuration fingerprint mismatch"));
        }
        reader.finish()?;
        Ok(Self {
            source,
            dimensions,
            metric,
            count,
            config,
            code_root,
            codebooks,
            quality,
            sampling_hash_algorithm,
            sampling_hash_version,
            training_sample_count,
        })
    }
}

#[derive(Clone, Debug)]
struct TrainingSample {
    hash: u64,
    key: Vec<u8>,
    vector: Vec<f32>,
}

impl PartialEq for TrainingSample {
    fn eq(&self, other: &Self) -> bool {
        self.hash == other.hash && self.key == other.key
    }
}

impl Eq for TrainingSample {}

impl PartialOrd for TrainingSample {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for TrainingSample {
    fn cmp(&self, other: &Self) -> Ordering {
        self.hash
            .cmp(&other.hash)
            .then_with(|| self.key.cmp(&other.key))
    }
}

fn train(
    vectors: &[&[f32]],
    dimensions: u32,
    config: &ProductQuantizationConfig,
    parallelism: BuildParallelism,
) -> Result<TrainingOutput, Error> {
    let layout = subspace_layout(dimensions as usize, config.subquantizers as usize);
    let centroid_count = usize::from(config.centroids_per_subquantizer);
    let mut codebooks = Vec::with_capacity(layout.len());
    for (subspace, &(start, end)) in layout.iter().enumerate() {
        let mut used = HashSet::new();
        let mut centroids = Vec::with_capacity(centroid_count);
        for centroid in 0..centroid_count {
            let mut identity = [0u8; 16];
            identity[..8].copy_from_slice(&(subspace as u64).to_le_bytes());
            identity[8..].copy_from_slice(&(centroid as u64).to_le_bytes());
            let initial = (xxh64(&identity, config.seed) as usize) % vectors.len();
            let selected = (0..vectors.len())
                .map(|offset| (initial + offset) % vectors.len())
                .find(|candidate| used.insert(*candidate))
                .expect("centroid count does not exceed records");
            centroids.push(vectors[selected][start..end].to_vec());
        }
        codebooks.push(centroids);
    }

    let pool = (parallelism.threads() > 1)
        .then(|| {
            rayon::ThreadPoolBuilder::new()
                .num_threads(parallelism.threads())
                .build()
        })
        .transpose()
        .map_err(|error| invalid_config(format!("cannot create PQ worker pool: {error}")))?;
    let mut evaluations = 0usize;
    for _ in 0..config.training_iterations {
        let assignments = assign_all(vectors, &layout, &codebooks, pool.as_ref());
        evaluations = evaluations.saturating_add(
            vectors
                .len()
                .saturating_mul(layout.len())
                .saturating_mul(centroid_count),
        );
        for (subspace, &(start, end)) in layout.iter().enumerate() {
            let width = end - start;
            let mut sums = vec![vec![0.0f64; width]; centroid_count];
            let mut counts = vec![0usize; centroid_count];
            for (vector_index, vector) in vectors.iter().enumerate() {
                let centroid = assignments[vector_index][subspace] as usize;
                counts[centroid] += 1;
                for (offset, &component) in vector[start..end].iter().enumerate() {
                    sums[centroid][offset] += f64::from(component);
                }
            }
            for centroid in 0..centroid_count {
                if counts[centroid] == 0 {
                    continue;
                }
                for offset in 0..width {
                    let value = (sums[centroid][offset] / counts[centroid] as f64) as f32;
                    codebooks[subspace][centroid][offset] = if value == 0.0 { 0.0 } else { value };
                }
            }
        }
    }
    Ok((codebooks, evaluations))
}

fn assign_all(
    vectors: &[&[f32]],
    layout: &[(usize, usize)],
    codebooks: &[Vec<Vec<f32>>],
    pool: Option<&rayon::ThreadPool>,
) -> Vec<Vec<u8>> {
    let compute = || {
        vectors
            .par_iter()
            .map(|vector| {
                layout
                    .iter()
                    .zip(codebooks)
                    .map(|(&(start, end), centroids)| {
                        nearest_centroid(&vector[start..end], centroids)
                    })
                    .collect()
            })
            .collect()
    };
    if let Some(pool) = pool {
        pool.install(compute)
    } else {
        vectors
            .iter()
            .map(|vector| {
                layout
                    .iter()
                    .zip(codebooks)
                    .map(|(&(start, end), centroids)| {
                        nearest_centroid(&vector[start..end], centroids)
                    })
                    .collect()
            })
            .collect()
    }
}

fn nearest_centroid(vector: &[f32], centroids: &[Vec<f32>]) -> u8 {
    let mut best = (0usize, f64::INFINITY);
    for (index, centroid) in centroids.iter().enumerate() {
        let distance = vector.iter().zip(centroid).fold(0.0, |sum, (&a, &b)| {
            let delta = f64::from(a) - f64::from(b);
            sum + delta * delta
        });
        if distance
            .total_cmp(&best.1)
            .then_with(|| index.cmp(&best.0))
            .is_lt()
        {
            best = (index, distance);
        }
    }
    best.0 as u8
}

fn encode_vector(
    vector: &[f32],
    layout: &[(usize, usize)],
    codebooks: &[Vec<Vec<f32>>],
) -> Vec<u8> {
    layout
        .iter()
        .zip(codebooks)
        .map(|(&(start, end), centroids)| nearest_centroid(&vector[start..end], centroids))
        .collect()
}

fn subspace_layout(dimensions: usize, subquantizers: usize) -> Vec<(usize, usize)> {
    (0..subquantizers)
        .map(|index| {
            (
                index * dimensions / subquantizers,
                (index + 1) * dimensions / subquantizers,
            )
        })
        .collect()
}

fn reconstruction_error(vector: &[f32], code: &[u8], codebooks: &[Vec<Vec<f32>>]) -> f64 {
    let mut offset = 0usize;
    let mut error = 0.0f64;
    for (subspace, &centroid) in codebooks.iter().zip(code) {
        for &component in &subspace[centroid as usize] {
            let delta = f64::from(vector[offset]) - f64::from(component);
            error += delta * delta;
            offset += 1;
        }
    }
    error
}

pub(crate) fn build_lookup(
    query: &[f32],
    metric: DistanceMetric,
    codebooks: &[Vec<Vec<f32>>],
) -> Vec<Vec<f64>> {
    let mut offset = 0usize;
    codebooks
        .iter()
        .map(|subspace| {
            let width = subspace.first().map_or(0, Vec::len);
            let query = &query[offset..offset + width];
            offset += width;
            subspace
                .iter()
                .map(|centroid| match metric {
                    DistanceMetric::L2Squared => {
                        query.iter().zip(centroid).fold(0.0, |sum, (&a, &b)| {
                            let delta = f64::from(a) - f64::from(b);
                            sum + delta * delta
                        })
                    }
                    DistanceMetric::Cosine | DistanceMetric::InnerProduct => query
                        .iter()
                        .zip(centroid)
                        .fold(0.0, |sum, (&a, &b)| sum + f64::from(a) * f64::from(b)),
                })
                .collect()
        })
        .collect()
}

pub(crate) fn score_code(metric: DistanceMetric, lookup: &[Vec<f64>], code: &[u8]) -> f64 {
    let reduced = lookup
        .iter()
        .zip(code)
        .fold(0.0, |sum, (subspace, &centroid)| {
            sum + subspace[centroid as usize]
        });
    let result = match metric {
        DistanceMetric::L2Squared => reduced,
        DistanceMetric::Cosine => 1.0 - reduced.clamp(-1.0, 1.0),
        DistanceMetric::InnerProduct => -reduced,
    };
    if result == 0.0 {
        0.0
    } else {
        result
    }
}

pub(crate) fn validate_code(code: &[u8], codebooks: &[Vec<Vec<f32>>]) -> Result<(), Error> {
    if code.len() != codebooks.len()
        || code
            .iter()
            .zip(codebooks)
            .any(|(&centroid, subspace)| centroid as usize >= subspace.len())
    {
        return Err(invalid_object("invalid PQ vector code"));
    }
    Ok(())
}

fn budget_exhausted(request: &SearchRequest<'_>, stats: &ProximitySearchStats) -> bool {
    request
        .budget
        .max_distance_evaluations
        .is_some_and(|maximum| {
            stats
                .distance_evaluations
                .saturating_add(stats.quantized_distance_evaluations)
                >= maximum
        })
}

fn encode_config(config: &ProductQuantizationConfig, bytes: &mut Vec<u8>) {
    put_varint(u64::from(config.subquantizers), bytes);
    put_varint(u64::from(config.centroids_per_subquantizer), bytes);
    put_varint(u64::from(config.training_iterations), bytes);
    put_varint(u64::from(config.rerank_multiplier), bytes);
    bytes.extend_from_slice(&config.seed.to_le_bytes());
    put_varint(config.max_training_vectors as u64, bytes);
}

fn decode_config(reader: &mut Reader<'_>) -> Result<ProductQuantizationConfig, Error> {
    Ok(ProductQuantizationConfig {
        subquantizers: u32::try_from(reader.varint()?)
            .map_err(|_| reader.invalid("subquantizers exceed u32"))?,
        centroids_per_subquantizer: u16::try_from(reader.varint()?)
            .map_err(|_| reader.invalid("centroid count exceeds u16"))?,
        training_iterations: u16::try_from(reader.varint()?)
            .map_err(|_| reader.invalid("training iterations exceed u16"))?,
        rerank_multiplier: u32::try_from(reader.varint()?)
            .map_err(|_| reader.invalid("rerank multiplier exceeds u32"))?,
        seed: reader.u64_le()?,
        max_training_vectors: usize::try_from(reader.varint()?)
            .map_err(|_| reader.invalid("max training vectors exceed usize"))?,
    })
}

fn require_pq_version(found: u8) -> Result<(), Error> {
    if found == PQ_FORMAT_VERSION {
        Ok(())
    } else {
        Err(Error::UnsupportedProximityVersion {
            found,
            required: PQ_FORMAT_VERSION,
        })
    }
}

fn enforce_resource(
    resource: &'static str,
    limit: Option<usize>,
    actual: usize,
) -> Result<(), Error> {
    if let Some(limit) = limit {
        if actual > limit {
            return Err(resource_limit(resource, limit, actual));
        }
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

pub(crate) fn config_fingerprint(config: &ProductQuantizationConfig) -> Cid {
    let mut bytes = Vec::new();
    encode_config(config, &mut bytes);
    Cid::from_bytes(&bytes)
}

pub(crate) fn code_tree_config() -> Config {
    // This is a wire-level PQ constant. Do not inherit future changes to
    // the general ordered-tree defaults when loading an existing sidecar.
    Config::builder()
        .min_chunk_size(4)
        .max_chunk_size(1024 * 1024)
        .chunking_factor(128)
        .hash_seed(0)
        .encoding(Encoding::Raw)
        .build()
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
        kind: "product quantizer",
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
    fn stable_ties_choose_the_lowest_centroid() {
        assert_eq!(nearest_centroid(&[0.0], &[vec![-1.0], vec![1.0]]), 0);
    }

    #[test]
    fn uneven_subspaces_cover_every_dimension_once() {
        assert_eq!(subspace_layout(7, 3), vec![(0, 2), (2, 4), (4, 7)]);
    }

    #[test]
    fn v1_manifest_requires_rebuild() {
        assert!(matches!(
            Manifest::decode(b"PQPQ\x01"),
            Err(Error::UnsupportedProximityVersion {
                found: 1,
                required: 2
            })
        ));
    }
}
