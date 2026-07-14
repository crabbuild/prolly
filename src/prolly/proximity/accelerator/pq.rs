use crate::prolly::builder::BatchBuilder;
use crate::prolly::cid::Cid;
use crate::prolly::config::Config;
use crate::prolly::encoding::Encoding;
use crate::prolly::error::Error;
use crate::prolly::proximity::distance::{prepare_vector, query_score};
use crate::prolly::proximity::search::PreparedFilter;
use crate::prolly::proximity::storage::codec::{
    put_cid, put_f32, put_f64, put_varint, Reader, FORMAT_VERSION, MAX_OBJECT_ENTRIES,
};
use crate::prolly::proximity::{
    BuildParallelism, DistanceMetric, Neighbor, ProximityMap, ProximitySearchStats, SearchBackend,
    SearchCompletion, SearchPolicy, SearchRequest, SearchResult,
};
use crate::prolly::store::Store;
use crate::prolly::tree::Tree;
use crate::prolly::Prolly;
use rayon::prelude::*;
use std::collections::HashSet;
use xxhash_rust::xxh64::xxh64;

const MAGIC: &[u8; 4] = b"PQPQ";
type Codebooks = Vec<Vec<Vec<f32>>>;
type Codes = Vec<Vec<u8>>;
type TrainingOutput = (Codebooks, Codes, usize);

/// Deterministic offline product-quantization training and serving policy.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProductQuantizationConfig {
    pub subquantizers: u32,
    pub centroids_per_subquantizer: u16,
    pub training_iterations: u16,
    pub rerank_multiplier: u32,
    pub seed: u64,
}

impl Default for ProductQuantizationConfig {
    fn default() -> Self {
        Self {
            subquantizers: 8,
            centroids_per_subquantizer: 256,
            training_iterations: 12,
            rerank_multiplier: 8,
            seed: 0,
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
        if self.training_iterations == 0 || self.rerank_multiplier == 0 {
            return Err(invalid_config(
                "training_iterations and rerank_multiplier must be greater than zero",
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
    pub encoded_vectors: usize,
}

/// Source-bound persisted product-quantization sidecar.
pub struct ProductQuantizer<S: Store> {
    codes: Prolly<S>,
    code_tree: Tree,
    manifest: Cid,
    source: Cid,
    dimensions: u32,
    metric: DistanceMetric,
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
        let records: Vec<_> = map.collect_records()?.into_values().collect();
        config.validate(map.tree().config.dimensions, records.len())?;
        let vectors: Vec<_> = records
            .iter()
            .map(|record| record.vector.as_slice())
            .collect();
        let (codebooks, codes, evaluations) =
            train(&vectors, map.tree().config.dimensions, &config, parallelism)?;
        let quality = reconstruction_quality(&vectors, &codes, &codebooks);
        let store = map.store_clone();
        let code_config = code_tree_config();
        let mut builder = BatchBuilder::new(store.clone(), code_config.clone());
        for (record, code) in records.iter().zip(&codes) {
            builder.add(record.key.clone(), code.clone());
        }
        let code_tree = builder.build()?;
        let code_root = code_tree
            .root
            .clone()
            .ok_or_else(|| invalid_object("product quantization requires a non-empty code tree"))?;
        let manifest_object = Manifest {
            source: map.tree().descriptor.clone(),
            dimensions: map.tree().config.dimensions,
            metric: map.tree().config.metric,
            config: config.clone(),
            code_root,
            codebooks: codebooks.clone(),
            quality,
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
            store
                .put(manifest.as_bytes(), &manifest_bytes)
                .map_err(|error| Error::Store(Box::new(error)))?;
        }
        let stats = ProductQuantizationBuildStats {
            training_distance_evaluations: evaluations,
            encoded_vectors: records.len(),
        };
        Ok((
            Self {
                codes: Prolly::new(store, code_config),
                code_tree,
                manifest,
                source: manifest_object.source,
                dimensions: manifest_object.dimensions,
                metric: manifest_object.metric,
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
            request.backend,
            SearchBackend::ProductQuantized | SearchBackend::Auto
        ) {
            return Err(invalid_search(
                "product quantizer requires ProductQuantized or Auto backend",
            ));
        }
        if request.budget.max_nodes.is_some()
            || request.budget.max_committed_bytes.is_some()
            || request.budget.max_frontier_entries.is_some()
        {
            return Err(invalid_search(
                "PQ search supports the distance-evaluation budget only",
            ));
        }
        if self.source != map.tree().descriptor
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
        let mut approximate = Vec::<(Vec<u8>, f64)>::new();
        let mut completion = SearchCompletion::ApproximatePolicySatisfied;
        for entry in self.codes.range(&self.code_tree, &[], None)? {
            let (key, code) = entry?;
            if !filter.contains(&key) {
                continue;
            }
            if budget_exhausted(&request, &stats) {
                completion = SearchCompletion::BudgetExhausted;
                break;
            }
            validate_code(&code, &self.codebooks)?;
            stats.quantized_distance_evaluations += 1;
            approximate.push((key, score_code(self.metric, &lookup, &code)));
        }
        approximate.sort_by(|left, right| {
            left.1
                .total_cmp(&right.1)
                .then_with(|| left.0.cmp(&right.0))
        });
        let shortlist = request
            .k
            .saturating_mul(self.config.rerank_multiplier as usize)
            .max(request.k)
            .min(approximate.len());

        let mut reranked = Vec::<(Vec<u8>, Vec<u8>, f64)>::with_capacity(shortlist);
        for (key, _) in approximate.into_iter().take(shortlist) {
            if budget_exhausted(&request, &stats) {
                completion = SearchCompletion::BudgetExhausted;
                break;
            }
            let (vector, value) = map.get(&key)?.ok_or_else(|| {
                invalid_object("PQ code key is absent from authoritative directory")
            })?;
            stats.distance_evaluations += 1;
            let distance = query_score(request.kernel, self.metric, &query, &vector);
            reranked.push((key, value, distance));
        }
        stats.reranked_candidates = reranked.len();
        reranked.sort_by(|left, right| {
            left.2
                .total_cmp(&right.2)
                .then_with(|| left.0.cmp(&right.0))
        });
        let neighbors = reranked
            .into_iter()
            .take(request.k)
            .map(|(key, value, distance)| Neighbor {
                key,
                value,
                distance,
            })
            .collect();
        Ok(SearchResult {
            neighbors,
            stats,
            completion,
        })
    }
}

#[derive(Clone)]
pub(crate) struct Manifest {
    pub(crate) source: Cid,
    pub(crate) dimensions: u32,
    pub(crate) metric: DistanceMetric,
    pub(crate) config: ProductQuantizationConfig,
    pub(crate) code_root: Cid,
    pub(crate) codebooks: Codebooks,
    pub(crate) quality: ProductQuantizationQuality,
}

impl Manifest {
    fn encode(&self) -> Result<Vec<u8>, Error> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(MAGIC);
        bytes.push(FORMAT_VERSION);
        bytes.push(0);
        put_cid(&self.source, &mut bytes);
        put_varint(u64::from(self.dimensions), &mut bytes);
        bytes.push(self.metric.id());
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
        reader.version()?;
        if reader.u8()? != 0 {
            return Err(reader.invalid("unknown flags"));
        }
        let source = reader.cid()?;
        let dimensions =
            u32::try_from(reader.varint()?).map_err(|_| reader.invalid("dimensions exceed u32"))?;
        let metric = DistanceMetric::from_id(reader.u8()?)?;
        let config = decode_config(&mut reader)?;
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
            config,
            code_root,
            codebooks,
            quality,
        })
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
    let codes = assign_all(vectors, &layout, &codebooks, pool.as_ref());
    evaluations = evaluations.saturating_add(
        vectors
            .len()
            .saturating_mul(layout.len())
            .saturating_mul(centroid_count),
    );
    Ok((codebooks, codes, evaluations))
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

fn reconstruction_quality(
    vectors: &[&[f32]],
    codes: &[Vec<u8>],
    codebooks: &[Vec<Vec<f32>>],
) -> ProductQuantizationQuality {
    let mut sum = 0.0f64;
    let mut maximum = 0.0f64;
    for (vector, code) in vectors.iter().zip(codes) {
        let mut offset = 0usize;
        let mut error = 0.0f64;
        for (subspace, &centroid) in codebooks.iter().zip(code) {
            for &component in &subspace[centroid as usize] {
                let delta = f64::from(vector[offset]) - f64::from(component);
                error += delta * delta;
                offset += 1;
            }
        }
        sum += error;
        maximum = maximum.max(error);
    }
    ProductQuantizationQuality {
        mean_squared_error: if vectors.is_empty() {
            0.0
        } else {
            sum / vectors.len() as f64
        },
        maximum_squared_error: maximum,
    }
}

fn build_lookup(
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

fn score_code(metric: DistanceMetric, lookup: &[Vec<f64>], code: &[u8]) -> f64 {
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

fn validate_code(code: &[u8], codebooks: &[Vec<Vec<f32>>]) -> Result<(), Error> {
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
    })
}

fn config_fingerprint(config: &ProductQuantizationConfig) -> Cid {
    let mut bytes = Vec::new();
    encode_config(config, &mut bytes);
    Cid::from_bytes(&bytes)
}

fn code_tree_config() -> Config {
    // This is a wire-level PQ constant. Do not inherit future changes to
    // the general ordered-tree defaults when loading an existing sidecar.
    Config {
        min_chunk_size: 4,
        max_chunk_size: 1024 * 1024,
        chunking_factor: 128,
        hash_seed: 0,
        encoding: Encoding::Raw,
        node_cache_max_nodes: None,
        node_cache_max_bytes: None,
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
}
