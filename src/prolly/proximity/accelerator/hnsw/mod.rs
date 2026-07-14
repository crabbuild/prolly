mod build;
mod search;
mod storage;

use crate::prolly::cid::Cid;
use crate::prolly::error::Error;
use crate::prolly::proximity::{
    ProximityMap, SearchBackend, SearchPolicy, SearchRequest, SearchResult,
};
use crate::prolly::store::Store;
use crate::prolly::tree::Tree;
use crate::prolly::Prolly;

/// Deterministic HNSW shape and serving limits.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HnswConfig {
    pub max_connections: u16,
    pub ef_construction: u32,
    pub ef_search: u32,
    pub level_bits: u8,
    pub overfetch_multiplier: u32,
    pub seed: u64,
}

impl Default for HnswConfig {
    fn default() -> Self {
        Self {
            max_connections: 16,
            ef_construction: 128,
            ef_search: 64,
            level_bits: 4,
            overfetch_multiplier: 8,
            seed: 0,
        }
    }
}

impl HnswConfig {
    fn validate(&self) -> Result<(), Error> {
        if self.max_connections == 0
            || self.ef_construction < u32::from(self.max_connections)
            || self.ef_search == 0
            || self.overfetch_multiplier == 0
            || !(1..=63).contains(&self.level_bits)
        {
            return Err(Error::InvalidProximityConfig {
                reason: "HNSW requires max_connections > 0, ef_construction >= max_connections, ef_search/overfetch > 0, and level_bits in 1..=63".to_owned(),
            });
        }
        Ok(())
    }
}

/// Logical work reported by a deterministic graph build.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct HnswBuildStats {
    pub records: usize,
    pub distance_evaluations: usize,
    pub directed_edges: usize,
    pub maximum_level: u8,
}

/// Persisted HNSW serving graph bound to one proximity descriptor CID.
pub struct HnswIndex<S: Store> {
    pub(super) graph: Prolly<S>,
    pub(super) graph_tree: Tree,
    pub(super) manifest: Cid,
    pub(super) source: Cid,
    pub(super) dimensions: u32,
    pub(super) metric: crate::prolly::proximity::DistanceMetric,
    pub(super) config: HnswConfig,
    pub(super) entry_point: Vec<u8>,
    pub(super) maximum_level: u8,
    pub(super) canonical: bool,
}

impl<S> HnswIndex<S>
where
    S: Store + Clone + Send + Sync,
    S::Error: Send + Sync,
{
    /// Build and persist a canonical graph in source-key insertion order.
    pub fn build(
        map: &ProximityMap<S>,
        config: HnswConfig,
    ) -> Result<(Self, HnswBuildStats), Error> {
        Self::build_with_mode(map, config, true)
    }

    /// Build a disposable serving cache. It is source-bound but makes no canonical claim.
    pub fn build_disposable(
        map: &ProximityMap<S>,
        config: HnswConfig,
    ) -> Result<(Self, HnswBuildStats), Error> {
        Self::build_with_mode(map, config, false)
    }

    fn build_with_mode(
        map: &ProximityMap<S>,
        config: HnswConfig,
        canonical: bool,
    ) -> Result<(Self, HnswBuildStats), Error> {
        config.validate()?;
        let store = map.store_clone();
        let built = build::build_graph(map, &config, store.clone())?;
        let manifest_object = storage::Manifest {
            source: map.tree().descriptor.clone(),
            dimensions: map.tree().config.dimensions,
            metric: map.tree().config.metric,
            config: config.clone(),
            graph_root: built
                .tree
                .root
                .clone()
                .ok_or_else(|| storage::invalid("HNSW graph root is absent"))?,
            entry_point: built.entry_point.clone(),
            maximum_level: built.maximum_level,
            canonical,
        };
        let bytes = manifest_object.encode()?;
        let manifest = Cid::from_bytes(&bytes);
        storage::put_content(&store, &manifest, &bytes)?;
        let stats = built.stats;
        Ok((
            Self {
                graph: Prolly::new(store, storage::graph_config()),
                graph_tree: built.tree,
                manifest,
                source: manifest_object.source,
                dimensions: manifest_object.dimensions,
                metric: manifest_object.metric,
                config,
                entry_point: built.entry_point,
                maximum_level: built.maximum_level,
                canonical,
            },
            stats,
        ))
    }

    /// Load a graph manifest and eagerly authenticate its ordered root.
    pub fn load(store: S, manifest: Cid) -> Result<Self, Error> {
        let bytes = storage::load_content(&store, &manifest)?;
        let object = storage::Manifest::decode(&bytes)?;
        object.config.validate()?;
        storage::load_content(&store, &object.graph_root)?;
        let graph_tree = Tree {
            root: Some(object.graph_root),
            config: storage::graph_config(),
        };
        let graph = Prolly::new(store.clone(), graph_tree.config.clone());
        let entry = graph
            .get(&graph_tree, &object.entry_point)?
            .ok_or_else(|| storage::invalid("HNSW entry point is absent from graph"))?;
        let entry = storage::GraphNode::decode(&entry)?;
        if entry.level != object.maximum_level {
            return Err(storage::invalid(
                "HNSW entry-point level disagrees with manifest",
            ));
        }
        Ok(Self {
            graph,
            graph_tree,
            manifest,
            source: object.source,
            dimensions: object.dimensions,
            metric: object.metric,
            config: object.config,
            entry_point: object.entry_point,
            maximum_level: object.maximum_level,
            canonical: object.canonical,
        })
    }

    pub fn manifest_cid(&self) -> &Cid {
        &self.manifest
    }

    pub fn source_descriptor(&self) -> &Cid {
        &self.source
    }

    pub fn config(&self) -> &HnswConfig {
        &self.config
    }

    pub fn is_canonical(&self) -> bool {
        self.canonical
    }

    /// Search this graph. `Auto` falls back to native search on stale/corrupt sidecars.
    pub fn search(
        &self,
        map: &ProximityMap<S>,
        request: SearchRequest<'_>,
    ) -> Result<SearchResult, Error> {
        if request.backend == SearchBackend::Auto {
            if request.policy == SearchPolicy::Exact
                || self.source != map.tree().descriptor
                || self.dimensions != map.tree().config.dimensions
                || self.metric != map.tree().config.metric
            {
                let mut native = request;
                native.backend = SearchBackend::Native;
                return map.search(native);
            }
            return search::search(self, map, request.clone()).or_else(|_| {
                let mut native = request;
                native.backend = SearchBackend::Native;
                map.search(native)
            });
        }
        if request.backend != SearchBackend::Hnsw {
            return Err(Error::InvalidProximitySearch {
                reason: "HNSW index requires Hnsw or Auto backend".to_owned(),
            });
        }
        if request.policy == SearchPolicy::Exact {
            return Err(Error::InvalidProximitySearch {
                reason: "HNSW cannot satisfy exact search".to_owned(),
            });
        }
        if self.source != map.tree().descriptor
            || self.dimensions != map.tree().config.dimensions
            || self.metric != map.tree().config.metric
        {
            return Err(Error::InvalidProximitySearch {
                reason: "HNSW index is bound to a different source descriptor".to_owned(),
            });
        }
        search::search(self, map, request)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::prolly::proximity::{ProximityConfig, ProximityRecord};
    use crate::prolly::store::{MemStore, Store};
    use std::sync::Arc;

    #[test]
    fn auto_falls_back_when_a_loaded_graph_becomes_corrupt() {
        let store = Arc::new(MemStore::new());
        let map = ProximityMap::build(
            store.clone(),
            ProximityConfig::new(2),
            (0usize..32).map(|index| ProximityRecord {
                key: format!("key-{index:03}").into_bytes(),
                vector: vec![index as f32, (index % 3) as f32],
                value: index.to_le_bytes().to_vec(),
            }),
        )
        .unwrap();
        let config = HnswConfig {
            max_connections: 8,
            ef_construction: 16,
            ef_search: 16,
            level_bits: 4,
            overfetch_multiplier: 4,
            seed: 9,
        };
        let (index, _) = HnswIndex::build(&map, config).unwrap();
        let root = index.graph_tree.root.clone().unwrap();
        Store::put(&store, root.as_bytes(), b"corrupt graph root").unwrap();

        let query = [7.0, 1.0];
        let mut automatic = SearchRequest::exact(&query, 4);
        automatic.policy = SearchPolicy::FixedBudget;
        automatic.backend = SearchBackend::Auto;
        let fallback = index.search(&map, automatic.clone()).unwrap();
        automatic.backend = SearchBackend::Native;
        assert_eq!(fallback.neighbors, map.search(automatic).unwrap().neighbors);

        let mut explicit = SearchRequest::exact(&query, 4);
        explicit.policy = SearchPolicy::FixedBudget;
        explicit.backend = SearchBackend::Hnsw;
        assert!(index.search(&map, explicit).is_err());
    }
}
