use prolly::{
    compare_and_swap_named_content_root, copy_content_graph, load_named_content_root,
    plan_content_gc, put_named_content_root, walk_content_graph, AcceleratorCatalog,
    AcceleratorSet, BuildParallelism, CompositeAccelerator, CompositeAcceleratorConfig,
    CompositeBase, CompositeBuildLimits, CompositeBuildOrRebuildOutcome, CompositeBuildOutcome,
    CompositeRebuildOptions, ContentGraphLimits, ContentManifestUpdate, ContentObjectKind,
    ContentRootManifest, FileNodeStore, HnswBuildLimits, HnswConfig, HnswIndex, MemStore,
    ProductQuantizationConfig, ProductQuantizer, ProximityConfig, ProximityFilter, ProximityMap,
    ProximityMutation, ProximityRecord, SearchBackend, SearchBudget, SearchCompletion, SearchIo,
    SearchPolicy, SearchRequest, SearchRuntime, Store,
};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

fn records() -> Vec<ProximityRecord> {
    (0usize..256)
        .map(|index| ProximityRecord {
            key: format!("doc-{index:04}").into_bytes(),
            vector: vec![index as f32, (index % 17) as f32, (index % 7) as f32, 1.0],
            value: format!("base-{index}").into_bytes(),
        })
        .collect()
}

fn mutations() -> Vec<ProximityMutation> {
    let mut output = Vec::new();
    for index in 0usize..5 {
        output.push(ProximityMutation {
            key: format!("doc-{index:04}").into_bytes(),
            value: None,
        });
    }
    for index in 40usize..45 {
        output.push(ProximityMutation {
            key: format!("doc-{index:04}").into_bytes(),
            value: Some((
                vec![500.0 + index as f32, 2.0, 3.0, 1.0],
                format!("vector-{index}").into_bytes(),
            )),
        });
    }
    for index in 100usize..103 {
        output.push(ProximityMutation {
            key: format!("doc-{index:04}").into_bytes(),
            value: Some((
                vec![index as f32, (index % 17) as f32, (index % 7) as f32, 1.0],
                format!("value-only-{index}").into_bytes(),
            )),
        });
    }
    for index in 256usize..261 {
        output.push(ProximityMutation {
            key: format!("doc-{index:04}").into_bytes(),
            value: Some((
                vec![index as f32, (index % 17) as f32, (index % 7) as f32, 1.0],
                format!("inserted-{index}").into_bytes(),
            )),
        });
    }
    output
}

#[test]
fn composite_build_search_catalog_and_content_closure_are_source_exact() {
    let store = Arc::new(MemStore::new());
    let base = ProximityMap::build(store.clone(), ProximityConfig::new(4), records()).unwrap();
    let (current, _) = base.mutate_batch(mutations()).unwrap();
    let (base_hnsw, _) = HnswIndex::build(&base, HnswConfig::default()).unwrap();
    let outcome = CompositeAccelerator::build(
        &base,
        &current,
        CompositeBase::Hnsw(base_hnsw),
        CompositeAcceleratorConfig::default(),
        CompositeBuildLimits::default(),
    )
    .unwrap();
    let (composite, stats) = match outcome {
        CompositeBuildOutcome::Composite { accelerator, stats } => (accelerator, stats),
        CompositeBuildOutcome::FullRebuildRequired { reasons, .. } => {
            panic!("unexpected rebuild: {reasons:?}")
        }
    };
    assert_eq!(stats.inserted_records, 5);
    assert_eq!(stats.vector_updated_records, 5);
    assert_eq!(stats.value_only_records, 3);
    assert_eq!(stats.deleted_records, 5);
    assert_eq!(stats.delta_records, 10);
    assert_eq!(stats.shadow_records, 10);

    let manifest = composite.manifest_cid().clone();
    let loaded = CompositeAccelerator::load(store.clone(), manifest.clone()).unwrap();
    let accelerators = AcceleratorSet::empty()
        .with_composite(current.tree(), loaded)
        .unwrap();
    let io = SearchIo::new(store.clone(), Arc::new(SearchRuntime::default()));
    let query = [101.0, 16.0, 3.0, 1.0];
    let exact = current.search(SearchRequest::exact(&query, 12)).unwrap();
    let mut request = SearchRequest::exact(&query, 12);
    request.policy = SearchPolicy::FixedBudget;
    request.options.backend = SearchBackend::Composite;
    let result = current.search_with(&accelerators, &io, request).unwrap();
    assert_eq!(result.plan.backend, SearchBackend::Composite);
    assert_eq!(result.neighbors, exact.neighbors);
    assert!(result
        .neighbors
        .iter()
        .all(|neighbor| !neighbor.key.starts_with(b"doc-000")));
    let value_only = result
        .neighbors
        .iter()
        .find(|neighbor| neighbor.key == b"doc-0100")
        .unwrap();
    assert_eq!(value_only.value, b"value-only-100");

    let catalog = AcceleratorCatalog::build(store.clone(), current.tree(), accelerators).unwrap();
    let catalog_cid = catalog.manifest_cid().clone();
    let loaded_catalog =
        AcceleratorCatalog::load(store.clone(), catalog_cid.clone(), current.tree()).unwrap();
    assert_eq!(loaded_catalog.entries(), catalog.entries());
    let publication = put_named_content_root(
        &store,
        b"proximity/accelerators",
        ContentRootManifest {
            root: catalog.typed_root(),
            logical_version: 1,
            created_at_millis: 10,
            metadata: BTreeMap::new(),
        },
    )
    .unwrap();
    assert_eq!(
        load_named_content_root(&store, b"proximity/accelerators")
            .unwrap()
            .unwrap()
            .manifest_cid,
        publication.manifest_cid
    );
    assert!(matches!(
        compare_and_swap_named_content_root(
            &store,
            b"proximity/accelerators",
            Some(&prolly::Cid::from_bytes(b"stale")),
            ContentRootManifest {
                root: catalog.typed_root(),
                logical_version: 2,
                created_at_millis: 20,
                metadata: BTreeMap::new(),
            },
        )
        .unwrap(),
        ContentManifestUpdate::Conflict { .. }
    ));
    let catalog_root = catalog.typed_root();
    let walk = walk_content_graph(
        &store,
        std::slice::from_ref(&catalog_root),
        &ContentGraphLimits::default(),
    )
    .unwrap();
    assert_eq!(
        walk.objects_by_kind[&ContentObjectKind::AcceleratorCatalog],
        1
    );
    assert_eq!(
        walk.objects_by_kind[&ContentObjectKind::CompositeAccelerator],
        1
    );
    assert_eq!(walk.objects_by_kind[&ContentObjectKind::HnswManifest], 1);

    let destination = Arc::new(MemStore::new());
    let copied = copy_content_graph(
        &store,
        &destination,
        catalog_root.clone(),
        &ContentGraphLimits::default(),
    )
    .unwrap();
    assert_eq!(copied.copied_objects, copied.required_objects);
    let copied_map =
        ProximityMap::load(destination.clone(), current.tree().descriptor.clone()).unwrap();
    AcceleratorCatalog::load(destination, catalog_cid, copied_map.tree()).unwrap();

    let orphan_bytes = b"unpublished composite build";
    let orphan = prolly::Cid::from_bytes(orphan_bytes);
    Store::put(&store, orphan.as_bytes(), orphan_bytes).unwrap();
    let mut candidates = walk
        .objects
        .iter()
        .map(|object| object.root.cid.clone())
        .collect::<Vec<_>>();
    candidates.push(orphan.clone());
    let gc = plan_content_gc(
        &store,
        &[catalog_root],
        &candidates,
        &ContentGraphLimits::default(),
    )
    .unwrap();
    assert_eq!(gc.reclaimable_cids, vec![orphan]);
}

#[test]
fn composite_thresholds_require_full_rebuild_before_manifest_publication() {
    let store = Arc::new(MemStore::new());
    let base = ProximityMap::build(store.clone(), ProximityConfig::new(4), records()).unwrap();
    let (current, _) = base.mutate_batch(mutations()).unwrap();
    let (base_hnsw, _) = HnswIndex::build(&base, HnswConfig::default()).unwrap();
    let outcome = CompositeAccelerator::build(
        &base,
        &current,
        CompositeBase::Hnsw(base_hnsw),
        CompositeAcceleratorConfig {
            max_delta_records: 1,
            ..CompositeAcceleratorConfig::default()
        },
        CompositeBuildLimits::default(),
    )
    .unwrap();
    match outcome {
        CompositeBuildOutcome::FullRebuildRequired { reasons, stats } => {
            assert!(!reasons.is_empty());
            assert_eq!(stats.delta_records, 10);
        }
        CompositeBuildOutcome::Composite { .. } => panic!("over-threshold composite was published"),
    }

    let (base_hnsw, _) = HnswIndex::build(&base, HnswConfig::default()).unwrap();
    let resolved = CompositeAccelerator::build_or_rebuild(
        &base,
        &current,
        CompositeBase::Hnsw(base_hnsw),
        CompositeAcceleratorConfig {
            max_delta_records: 1,
            ..CompositeAcceleratorConfig::default()
        },
        CompositeBuildLimits::default(),
        CompositeRebuildOptions {
            hnsw_limits: HnswBuildLimits {
                worker_threads: 2,
                ..HnswBuildLimits::default()
            },
            ..CompositeRebuildOptions::default()
        },
    )
    .unwrap();
    match resolved {
        CompositeBuildOrRebuildOutcome::HnswRebuilt {
            accelerator,
            reasons,
            composite_stats,
            ..
        } => {
            assert_eq!(accelerator.source_descriptor(), &current.tree().descriptor);
            assert!(!reasons.is_empty());
            assert_eq!(composite_stats.delta_records, 10);
        }
        _ => panic!("threshold crossing did not synchronously rebuild HNSW"),
    }
}

#[test]
fn pq_composite_is_exact_when_base_rerank_is_exhaustive_and_budgets_are_bounded() {
    let store = Arc::new(MemStore::new());
    let base = ProximityMap::build(store.clone(), ProximityConfig::new(4), records()).unwrap();
    let (current, _) = base.mutate_batch(mutations()).unwrap();
    let (pq, _) = ProductQuantizer::build(
        &base,
        ProductQuantizationConfig {
            subquantizers: 2,
            centroids_per_subquantizer: 8,
            training_iterations: 3,
            rerank_multiplier: u32::MAX,
            seed: 19,
            max_training_vectors: 256,
        },
        BuildParallelism::new(3).unwrap(),
    )
    .unwrap();
    let composite = match CompositeAccelerator::build(
        &base,
        &current,
        CompositeBase::ProductQuantized(pq),
        CompositeAcceleratorConfig::default(),
        CompositeBuildLimits::default(),
    )
    .unwrap()
    {
        CompositeBuildOutcome::Composite { accelerator, stats } => {
            assert_eq!(accelerator.build_stats(), &stats);
            accelerator
        }
        CompositeBuildOutcome::FullRebuildRequired { reasons, .. } => {
            panic!("unexpected rebuild: {reasons:?}")
        }
    };
    let accelerators = AcceleratorSet::empty()
        .with_composite(current.tree(), *composite)
        .unwrap();
    let io = SearchIo::new(store, Arc::new(SearchRuntime::default()));
    let query = [101.0, 16.0, 3.0, 1.0];
    let mut request = SearchRequest::exact(&query, 12);
    request.policy = SearchPolicy::FixedBudget;
    request.options.backend = SearchBackend::Composite;
    request.filter = ProximityFilter::KeyRange {
        start: Some(b"doc-0030"),
        end: Some(b"doc-0200"),
    };
    let expected = current.search(request.clone()).unwrap();
    let actual = current
        .search_with(&accelerators, &io, request.clone())
        .unwrap();
    assert_eq!(actual.neighbors, expected.neighbors);
    assert!(actual.neighbors.iter().all(|neighbor| {
        neighbor.key.as_slice() >= b"doc-0030" && neighbor.key.as_slice() < b"doc-0200"
    }));

    request.budget = SearchBudget {
        max_nodes: Some(1),
        ..SearchBudget::default()
    };
    let bounded = current.search_with(&accelerators, &io, request).unwrap();
    assert_eq!(bounded.completion, SearchCompletion::BudgetExhausted);
    assert!(bounded.stats.nodes_read <= 1);

    let mut automatic = SearchRequest::exact(&query, 12);
    automatic.policy = SearchPolicy::FixedBudget;
    automatic.budget.max_nodes = Some(1);
    let rejected = current.search_with(&accelerators, &io, automatic).unwrap();
    assert_eq!(rejected.plan.backend, SearchBackend::Native);
}

#[test]
fn value_only_changes_do_not_grow_composite_and_worker_counts_are_canonical() {
    let store = Arc::new(MemStore::new());
    let base = ProximityMap::build(store.clone(), ProximityConfig::new(4), records()).unwrap();
    let (current, _) = base
        .mutate_batch([ProximityMutation {
            key: b"doc-0100".to_vec(),
            value: Some((vec![100.0, 15.0, 2.0, 1.0], b"new-value".to_vec())),
        }])
        .unwrap();
    let reversed_base = ProximityMap::build(
        store.clone(),
        ProximityConfig::new(4),
        records().into_iter().rev(),
    )
    .unwrap();
    let (reversed_current, _) = reversed_base
        .mutate_batch([ProximityMutation {
            key: b"doc-0100".to_vec(),
            value: Some((vec![100.0, 15.0, 2.0, 1.0], b"new-value".to_vec())),
        }])
        .unwrap();
    assert_eq!(reversed_base.tree().descriptor, base.tree().descriptor);
    assert_eq!(
        reversed_current.tree().descriptor,
        current.tree().descriptor
    );
    let config = HnswConfig::default();
    let (hnsw_one, _) = HnswIndex::build_with_limits(
        &reversed_base,
        config.clone(),
        HnswBuildLimits {
            worker_threads: 1,
            ..HnswBuildLimits::default()
        },
    )
    .unwrap();
    let (hnsw_four, _) = HnswIndex::build_with_limits(
        &base,
        config,
        HnswBuildLimits {
            worker_threads: 4,
            ..HnswBuildLimits::default()
        },
    )
    .unwrap();
    assert_eq!(hnsw_one.manifest_cid(), hnsw_four.manifest_cid());
    let first = match CompositeAccelerator::build(
        &reversed_base,
        &reversed_current,
        CompositeBase::Hnsw(hnsw_one),
        CompositeAcceleratorConfig::default(),
        CompositeBuildLimits::default(),
    )
    .unwrap()
    {
        CompositeBuildOutcome::Composite { accelerator, stats } => {
            assert_eq!(stats.value_only_records, 1);
            assert_eq!(stats.delta_records, 0);
            assert_eq!(stats.shadow_records, 0);
            accelerator
        }
        _ => panic!("value-only composite unexpectedly rebuilt"),
    };
    let second = match CompositeAccelerator::build(
        &base,
        &current,
        CompositeBase::Hnsw(hnsw_four),
        CompositeAcceleratorConfig::default(),
        CompositeBuildLimits::default(),
    )
    .unwrap()
    {
        CompositeBuildOutcome::Composite { accelerator, .. } => accelerator,
        _ => panic!("value-only composite unexpectedly rebuilt"),
    };
    assert_eq!(first.manifest_cid(), second.manifest_cid());
    let set = AcceleratorSet::empty()
        .with_composite(current.tree(), *first)
        .unwrap();
    let query = [100.0, 15.0, 2.0, 1.0];
    let mut request = SearchRequest::exact(&query, 1);
    request.policy = SearchPolicy::FixedBudget;
    request.options.backend = SearchBackend::Composite;
    let result = current
        .search_with(
            &set,
            &SearchIo::new(store, Arc::new(SearchRuntime::default())),
            request,
        )
        .unwrap();
    assert_eq!(result.neighbors[0].key, b"doc-0100");
    assert_eq!(result.neighbors[0].value, b"new-value");
}

#[test]
fn durable_reopen_preserves_catalog_composite_and_missing_base_fails_closed() {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let path = std::env::temp_dir().join(format!(
        "prolly-proximity-composite-{}-{nonce}",
        std::process::id()
    ));
    let store = Arc::new(FileNodeStore::open(&path).unwrap());
    let base = ProximityMap::build(
        store.clone(),
        ProximityConfig::new(4),
        records().into_iter().take(96),
    )
    .unwrap();
    let (current, _) = base
        .mutate_batch([ProximityMutation {
            key: b"doc-0040".to_vec(),
            value: Some((vec![400.0, 2.0, 1.0, 1.0], b"durable".to_vec())),
        }])
        .unwrap();
    let (base_hnsw, _) = HnswIndex::build(&base, HnswConfig::default()).unwrap();
    let base_manifest = base_hnsw.manifest_cid().clone();
    let composite = match CompositeAccelerator::build(
        &base,
        &current,
        CompositeBase::Hnsw(base_hnsw),
        CompositeAcceleratorConfig::default(),
        CompositeBuildLimits::default(),
    )
    .unwrap()
    {
        CompositeBuildOutcome::Composite { accelerator, .. } => accelerator,
        _ => panic!("durable composite unexpectedly rebuilt"),
    };
    let composite_manifest = composite.manifest_cid().clone();
    let set = AcceleratorSet::empty()
        .with_composite(current.tree(), *composite)
        .unwrap();
    let catalog = AcceleratorCatalog::build(store.clone(), current.tree(), set).unwrap();
    let catalog_manifest = catalog.manifest_cid().clone();
    let current_descriptor = current.tree().descriptor.clone();
    drop(catalog);
    drop(current);
    drop(base);
    drop(store);

    let reopened = Arc::new(FileNodeStore::open(&path).unwrap());
    let current = ProximityMap::load(reopened.clone(), current_descriptor).unwrap();
    let catalog =
        AcceleratorCatalog::load(reopened.clone(), catalog_manifest, current.tree()).unwrap();
    let mut request = SearchRequest::exact(&[400.0, 2.0, 1.0, 1.0], 3);
    request.policy = SearchPolicy::FixedBudget;
    request.options.backend = SearchBackend::Composite;
    let result = current
        .search_with(
            catalog.accelerators(),
            &SearchIo::new(reopened.clone(), Arc::new(SearchRuntime::default())),
            request,
        )
        .unwrap();
    assert_eq!(result.neighbors[0].key, b"doc-0040");

    Store::delete(&reopened, base_manifest.as_bytes()).unwrap();
    assert!(CompositeAccelerator::load(reopened.clone(), composite_manifest).is_err());
    drop(current);
    drop(reopened);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn composite_limits_fail_before_publication_and_empty_rebuild_needs_no_sidecar() {
    let store = Arc::new(MemStore::new());
    let base = ProximityMap::build(
        store,
        ProximityConfig::new(4),
        records().into_iter().take(16),
    )
    .unwrap();
    let (one_change, _) = base
        .mutate_batch([ProximityMutation {
            key: b"doc-0008".to_vec(),
            value: Some((vec![80.0, 0.0, 0.0, 1.0], b"limited".to_vec())),
        }])
        .unwrap();
    let (base_hnsw, _) = HnswIndex::build(&base, HnswConfig::default()).unwrap();
    let error = match CompositeAccelerator::build(
        &base,
        &one_change,
        CompositeBase::Hnsw(base_hnsw),
        CompositeAcceleratorConfig {
            max_delta_records: 16,
            max_delta_ratio_ppm: 1_000_000,
            ..CompositeAcceleratorConfig::default()
        },
        CompositeBuildLimits {
            max_encoded_output_bytes: Some(1),
            ..CompositeBuildLimits::default()
        },
    ) {
        Err(error) => error,
        Ok(_) => panic!("encoded output limit did not stop publication"),
    };
    assert!(matches!(
        error,
        prolly::Error::ProximityResourceLimitExceeded {
            resource: "encoded_output_bytes",
            ..
        }
    ));

    let deletes = (0usize..16).map(|index| ProximityMutation {
        key: format!("doc-{index:04}").into_bytes(),
        value: None,
    });
    let (empty, _) = base.mutate_batch(deletes).unwrap();
    assert_eq!(empty.tree().count, 0);
    let (base_hnsw, _) = HnswIndex::build(&base, HnswConfig::default()).unwrap();
    let outcome = CompositeAccelerator::build_or_rebuild(
        &base,
        &empty,
        CompositeBase::Hnsw(base_hnsw),
        CompositeAcceleratorConfig {
            max_shadow_records: 0,
            ..CompositeAcceleratorConfig::default()
        },
        CompositeBuildLimits::default(),
        CompositeRebuildOptions::default(),
    )
    .unwrap();
    assert!(matches!(
        outcome,
        CompositeBuildOrRebuildOutcome::NoAcceleratorRequired { .. }
    ));
    assert!(empty
        .search(SearchRequest::exact(&[0.0, 0.0, 0.0, 1.0], 1))
        .unwrap()
        .neighbors
        .is_empty());
}
