use prolly::{
    compare_and_swap_named_content_root, content_references, copy_and_publish_content_graph,
    copy_content_graph, load_named_content_root, plan_content_gc, put_named_content_root,
    sweep_content_gc, sweep_content_gc_with_invalidator, walk_content_graph, BuildParallelism,
    ContentGraphLimits, ContentManifestUpdate, ContentObjectKind, ContentRootManifest, HnswConfig,
    HnswIndex, MemStore, ProductQuantizationConfig, ProductQuantizer, ProximityConfig,
    ProximityMap, ProximityRecord, RootManifest, ScalarQuantizationConfig, TypedContentRoot,
};
use std::collections::BTreeMap;
use std::io;
use std::sync::atomic::{AtomicBool, AtomicIsize, Ordering};
use std::sync::Arc;

#[derive(Default)]
struct FaultStore {
    inner: MemStore,
    puts_before_failure: AtomicIsize,
    fail_publication: AtomicBool,
}

impl FaultStore {
    fn allow_puts(&self) {
        self.puts_before_failure.store(-1, Ordering::SeqCst);
    }

    fn fail_after_puts(&self, successful_puts: isize) {
        self.puts_before_failure
            .store(successful_puts, Ordering::SeqCst);
    }
}

impl prolly::Store for FaultStore {
    type Error = io::Error;

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        prolly::Store::get(&self.inner, key).map_err(io_error)
    }

    fn put(&self, key: &[u8], value: &[u8]) -> Result<(), Self::Error> {
        let remaining = self.puts_before_failure.load(Ordering::SeqCst);
        if remaining == 0 {
            return Err(io::Error::other("injected content put failure"));
        }
        if remaining > 0 {
            self.puts_before_failure.fetch_sub(1, Ordering::SeqCst);
        }
        prolly::Store::put(&self.inner, key, value).map_err(io_error)
    }

    fn delete(&self, key: &[u8]) -> Result<(), Self::Error> {
        prolly::Store::delete(&self.inner, key).map_err(io_error)
    }

    fn batch(&self, ops: &[prolly::BatchOp<'_>]) -> Result<(), Self::Error> {
        prolly::Store::batch(&self.inner, ops).map_err(io_error)
    }
}

impl prolly::ManifestStore for FaultStore {
    type Error = io::Error;

    fn get_root(&self, name: &[u8]) -> Result<Option<RootManifest>, Self::Error> {
        prolly::ManifestStore::get_root(&self.inner, name).map_err(io_error)
    }

    fn put_root(&self, name: &[u8], manifest: &RootManifest) -> Result<(), Self::Error> {
        if self.fail_publication.load(Ordering::SeqCst) {
            return Err(io::Error::other("injected named publication failure"));
        }
        prolly::ManifestStore::put_root(&self.inner, name, manifest).map_err(io_error)
    }

    fn delete_root(&self, name: &[u8]) -> Result<(), Self::Error> {
        prolly::ManifestStore::delete_root(&self.inner, name).map_err(io_error)
    }

    fn compare_and_swap_root(
        &self,
        name: &[u8],
        expected: Option<&RootManifest>,
        new: Option<&RootManifest>,
    ) -> Result<prolly::ManifestUpdate, Self::Error> {
        if self.fail_publication.load(Ordering::SeqCst) {
            return Err(io::Error::other("injected named publication failure"));
        }
        prolly::ManifestStore::compare_and_swap_root(&self.inner, name, expected, new)
            .map_err(io_error)
    }
}

fn io_error(error: impl std::fmt::Display) -> io::Error {
    io::Error::other(error.to_string())
}

fn records() -> Vec<ProximityRecord> {
    (0usize..160)
        .map(|index| ProximityRecord {
            key: format!("content-{index:04}").into_bytes(),
            vector: (0..16)
                .map(|dimension| ((index * 19 + dimension * 31) % 211) as f32 / 7.0)
                .collect(),
            value: index.to_le_bytes().to_vec(),
        })
        .collect()
}

fn source() -> (Arc<MemStore>, ProximityMap<Arc<MemStore>>) {
    let store = Arc::new(MemStore::new());
    let mut config = ProximityConfig::new(16);
    config.hierarchy.log_chunk_size = 2;
    config.hierarchy.level_hash_seed = 37;
    config.vector_storage.inline_threshold_bytes = 4;
    config.overflow.min_page_bytes = 128;
    config.overflow.target_page_bytes = 256;
    config.overflow.max_page_bytes = 1024;
    config.scalar_quantization = Some(ScalarQuantizationConfig { group_size: 4 });
    let map = ProximityMap::build(store.clone(), config, records()).unwrap();
    (store, map)
}

#[test]
fn typed_walk_covers_proximity_and_accelerator_graphs_with_limits_and_sharing() {
    let (store, map) = source();
    let descriptor = TypedContentRoot::proximity_descriptor(map.tree().descriptor.clone());
    let limits = ContentGraphLimits::default();
    let proximity = walk_content_graph(&store, std::slice::from_ref(&descriptor), &limits).unwrap();
    assert_eq!(
        proximity.objects.last().unwrap().root.kind,
        ContentObjectKind::ProximityDescriptor
    );
    for kind in [
        ContentObjectKind::OrderedNode,
        ContentObjectKind::ProximityNode,
        ContentObjectKind::OverflowDirectory,
        ContentObjectKind::OverflowPage,
        ContentObjectKind::ExternalVector,
        ContentObjectKind::ScalarQuantization,
    ] {
        assert!(
            proximity.objects_by_kind.get(&kind).copied().unwrap_or(0) > 0,
            "missing {kind:?}: {:?}",
            proximity.objects_by_kind
        );
    }
    let descriptor_references = content_references(&store, &descriptor).unwrap();
    assert_eq!(descriptor_references.len(), 2);
    assert!(descriptor_references.iter().any(|reference| {
        reference.kind == ContentObjectKind::OrderedNode
            && reference.cid == map.tree().directory.root.clone().unwrap()
    }));
    assert!(descriptor_references.iter().any(|reference| {
        reference.kind == ContentObjectKind::ProximityNode
            && reference.cid == map.tree().proximity_root
            && reference.dimensions == Some(16)
    }));
    let mut tiny = limits.clone();
    tiny.max_objects = 1;
    assert!(matches!(
        walk_content_graph(&store, std::slice::from_ref(&descriptor), &tiny),
        Err(prolly::Error::ContentGraphResourceLimitExceeded { .. })
    ));

    let (pq, _) = ProductQuantizer::build(
        &map,
        ProductQuantizationConfig {
            subquantizers: 4,
            centroids_per_subquantizer: 8,
            training_iterations: 3,
            rerank_multiplier: 4,
            seed: 3,
            max_training_vectors: 65_536,
        },
        BuildParallelism::new(2).unwrap(),
    )
    .unwrap();
    let (hnsw, _) = HnswIndex::build(
        &map,
        HnswConfig {
            max_connections: 8,
            ef_construction: 32,
            ef_search: 32,
            level_bits: 4,
            overfetch_multiplier: 4,
            seed: 5,
            routing_vector_encoding: prolly::HnswRoutingVectorEncoding::FullF32,
        },
    )
    .unwrap();
    let pq_root = TypedContentRoot::new(
        ContentObjectKind::ProductQuantization,
        pq.manifest_cid().clone(),
    );
    let hnsw_root =
        TypedContentRoot::new(ContentObjectKind::HnswManifest, hnsw.manifest_cid().clone());
    let combined = walk_content_graph(
        &store,
        &[descriptor.clone(), pq_root.clone(), hnsw_root.clone()],
        &limits,
    )
    .unwrap();
    assert_eq!(
        combined.objects_by_kind[&ContentObjectKind::ProductQuantization],
        1
    );
    assert_eq!(
        combined.objects_by_kind[&ContentObjectKind::HnswManifest],
        1
    );
    assert!(combined.objects.len() < proximity.objects.len() * 3 + 10);

    let hnsw_page_bytes = b"HNSN\x02\x00\x00\x01\x01\x00\x00\x00\x00\x01\x00";
    let hnsw_page = prolly::Cid::from_bytes(hnsw_page_bytes);
    prolly::Store::put(&store, hnsw_page.as_bytes(), hnsw_page_bytes).unwrap();
    let page = TypedContentRoot::new(ContentObjectKind::HnswPage, hnsw_page);
    let page_walk = walk_content_graph(&store, &[page], &limits).unwrap();
    assert_eq!(page_walk.objects_by_kind[&ContentObjectKind::HnswPage], 1);

    let destination = Arc::new(MemStore::new());
    let copied = copy_content_graph(&store, &destination, hnsw_root, &limits).unwrap();
    assert_eq!(copied.copied_objects, copied.required_objects);
    let loaded_map =
        ProximityMap::load(destination.clone(), map.tree().descriptor.clone()).unwrap();
    loaded_map.verify().unwrap();
    HnswIndex::load(destination.clone(), hnsw.manifest_cid().clone()).unwrap();
    let reused = copy_content_graph(&store, &destination, pq_root, &limits).unwrap();
    assert!(reused.reused_objects > 0);
    ProductQuantizer::load(destination, pq.manifest_cid().clone()).unwrap();
}

#[test]
fn named_typed_manifests_use_atomic_cas_and_global_gc_preserves_reachable_closure() {
    let (store, map) = source();
    let root = TypedContentRoot::proximity_descriptor(map.tree().descriptor.clone());
    let manifest = ContentRootManifest {
        root: root.clone(),
        logical_version: 7,
        created_at_millis: 1234,
        metadata: BTreeMap::from([(b"branch".to_vec(), b"main".to_vec())]),
    };
    let published = put_named_content_root(&store, b"proximity/main", manifest.clone()).unwrap();
    assert_eq!(
        load_named_content_root(&store, b"proximity/main")
            .unwrap()
            .unwrap(),
        published
    );
    let conflict = compare_and_swap_named_content_root(
        &store,
        b"proximity/main",
        Some(&prolly::Cid::from_bytes(b"stale")),
        manifest.clone(),
    )
    .unwrap();
    assert!(matches!(conflict, ContentManifestUpdate::Conflict { .. }));
    let updated = ContentRootManifest {
        logical_version: 8,
        ..manifest
    };
    assert!(matches!(
        compare_and_swap_named_content_root(
            &store,
            b"proximity/main",
            Some(&published.manifest_cid),
            updated,
        )
        .unwrap(),
        ContentManifestUpdate::Applied(_)
    ));

    let walk = walk_content_graph(
        &store,
        std::slice::from_ref(&root),
        &ContentGraphLimits::default(),
    )
    .unwrap();
    let orphan_bytes = b"unpublished failed build".to_vec();
    let orphan = prolly::Cid::from_bytes(&orphan_bytes);
    prolly::Store::put(&store, orphan.as_bytes(), &orphan_bytes).unwrap();
    let mut candidates: Vec<_> = walk
        .objects
        .iter()
        .map(|object| object.root.cid.clone())
        .collect();
    candidates.push(orphan.clone());
    let plan = plan_content_gc(
        &store,
        std::slice::from_ref(&root),
        &candidates,
        &ContentGraphLimits::default(),
    )
    .unwrap();
    assert_eq!(plan.reclaimable_cids, vec![orphan.clone()]);
    let sweep =
        sweep_content_gc(&store, &[root], &candidates, &ContentGraphLimits::default()).unwrap();
    assert_eq!(sweep.deleted_objects, 1);
    assert!(prolly::Store::get(&store, orphan.as_bytes())
        .unwrap()
        .is_none());
    assert!(ProximityMap::load(store, map.tree().descriptor.clone()).is_ok());
}

#[test]
fn traversal_is_deterministic_bounded_and_rejects_missing_corrupt_or_conflicting_content() {
    let (store, map) = source();
    let root = TypedContentRoot::proximity_descriptor(map.tree().descriptor.clone());
    let limits = ContentGraphLimits::default();
    let walk = walk_content_graph(&store, std::slice::from_ref(&root), &limits).unwrap();

    let repeated = walk_content_graph(&store, &[root.clone(), root.clone()], &limits).unwrap();
    assert_eq!(walk, repeated);
    let mut reversed_objects = walk.objects.clone();
    reversed_objects.reverse();
    assert_ne!(walk.objects, reversed_objects);

    for (resource, constrained) in [
        (
            "bytes",
            ContentGraphLimits {
                max_bytes: 1,
                ..limits.clone()
            },
        ),
        (
            "depth",
            ContentGraphLimits {
                max_depth: 0,
                ..limits.clone()
            },
        ),
        (
            "references",
            ContentGraphLimits {
                max_references_per_object: 1,
                ..limits.clone()
            },
        ),
    ] {
        assert!(matches!(
            walk_content_graph(&store, std::slice::from_ref(&root), &constrained),
            Err(prolly::Error::ContentGraphResourceLimitExceeded { resource: actual, .. })
                if actual == resource
        ));
    }

    let proximity = TypedContentRoot::new(
        ContentObjectKind::ProximityNode,
        map.tree().proximity_root.clone(),
    )
    .with_dimensions(16);
    let conflicting = proximity.clone().with_dimensions(17);
    assert!(matches!(
        walk_content_graph(&store, &[proximity, conflicting], &limits),
        Err(prolly::Error::InvalidProximityObject { .. })
    ));

    let missing = walk.objects[0].clone();
    prolly::Store::delete(&store, missing.root.cid.as_bytes()).unwrap();
    assert!(matches!(
        walk_content_graph(&store, std::slice::from_ref(&root), &limits),
        Err(prolly::Error::NotFound(cid)) if cid == missing.root.cid
    ));
    prolly::Store::put(&store, missing.root.cid.as_bytes(), &missing.bytes).unwrap();

    let destination = MemStore::new();
    prolly::Store::put(
        &destination,
        walk.objects[0].root.cid.as_bytes(),
        b"wrong bytes under a content address",
    )
    .unwrap();
    assert!(matches!(
        copy_content_graph(&store, &destination, root, &limits),
        Err(prolly::Error::CidMismatch { .. })
    ));
}

#[test]
fn copy_publication_is_closed_and_gc_notifies_cache_invalidators() {
    let (source_store, map) = source();
    let root = TypedContentRoot::proximity_descriptor(map.tree().descriptor.clone());
    let manifest = ContentRootManifest {
        root: root.clone(),
        logical_version: 1,
        created_at_millis: 99,
        metadata: BTreeMap::new(),
    };
    let destination = MemStore::new();
    let (copy, publication) = copy_and_publish_content_graph(
        &source_store,
        &destination,
        b"replica/main",
        manifest,
        &ContentGraphLimits::default(),
    )
    .unwrap();
    assert_eq!(copy.copied_objects, copy.required_objects);
    assert_eq!(
        load_named_content_root(&destination, b"replica/main")
            .unwrap()
            .unwrap(),
        publication
    );

    let absent_root = TypedContentRoot::proximity_descriptor(prolly::Cid::from_bytes(b"absent"));
    let absent_manifest = ContentRootManifest {
        root: absent_root,
        logical_version: 2,
        created_at_millis: 100,
        metadata: BTreeMap::new(),
    };
    assert!(put_named_content_root(&destination, b"replica/broken", absent_manifest).is_err());
    assert!(load_named_content_root(&destination, b"replica/broken")
        .unwrap()
        .is_none());

    let orphan_bytes = b"failed unpublished replica";
    let orphan = prolly::Cid::from_bytes(orphan_bytes);
    prolly::Store::put(&destination, orphan.as_bytes(), orphan_bytes).unwrap();
    let mut invalidated = Vec::new();
    let sweep = sweep_content_gc_with_invalidator(
        &destination,
        std::slice::from_ref(&root),
        std::slice::from_ref(&orphan),
        &ContentGraphLimits::default(),
        |cid| invalidated.push(cid.clone()),
    )
    .unwrap();
    assert_eq!(sweep.deleted_objects, 1);
    assert_eq!(invalidated, vec![orphan]);
}

#[test]
fn failed_copy_manifest_and_named_publication_never_expose_an_incomplete_root() {
    let (source_store, map) = source();
    let root = TypedContentRoot::proximity_descriptor(map.tree().descriptor.clone());
    let manifest = ContentRootManifest {
        root,
        logical_version: 1,
        created_at_millis: 101,
        metadata: BTreeMap::new(),
    };
    let limits = ContentGraphLimits::default();
    let destination = FaultStore::default();

    destination.fail_after_puts(3);
    assert!(copy_and_publish_content_graph(
        &source_store,
        &destination,
        b"failure/copy",
        manifest.clone(),
        &limits,
    )
    .is_err());
    assert!(
        prolly::ManifestStore::get_root(&destination, b"failure/copy")
            .unwrap()
            .is_none()
    );

    destination.allow_puts();
    copy_content_graph(&source_store, &destination, manifest.root.clone(), &limits).unwrap();
    destination.fail_after_puts(0);
    assert!(copy_and_publish_content_graph(
        &source_store,
        &destination,
        b"failure/manifest",
        manifest.clone(),
        &limits,
    )
    .is_err());
    assert!(
        prolly::ManifestStore::get_root(&destination, b"failure/manifest")
            .unwrap()
            .is_none()
    );

    destination.allow_puts();
    destination.fail_publication.store(true, Ordering::SeqCst);
    assert!(copy_and_publish_content_graph(
        &source_store,
        &destination,
        b"failure/root",
        manifest.clone(),
        &limits,
    )
    .is_err());
    assert!(
        prolly::ManifestStore::get_root(&destination, b"failure/root")
            .unwrap()
            .is_none()
    );

    destination.fail_publication.store(false, Ordering::SeqCst);
    copy_and_publish_content_graph(
        &source_store,
        &destination,
        b"failure/success",
        manifest,
        &limits,
    )
    .unwrap();
    assert!(load_named_content_root(&destination, b"failure/success")
        .unwrap()
        .is_some());
}
