use prolly::{
    compare_and_swap_named_content_root_async, load_named_content_root_async,
    put_named_content_root_async, AsyncManifestStore, AsyncProximityBuildOptions,
    AsyncProximityHead, AsyncProximityHeadCommit, AsyncProximityMap, AsyncSearchControl,
    AsyncStore, BatchOp, ContentGraphLimits, ContentManifestUpdate, ContentObjectKind,
    ContentRootManifest, Error, ManifestStore, ManifestUpdate, MemStore, MemStoreError,
    ProximityConfig, ProximityMap, ProximityMutation, ProximityRecord, RootManifest,
    ScalarQuantizationConfig, SearchRequest, Store, TypedContentRoot,
};
use std::collections::BTreeMap;
use std::future::Future;
use std::ops::ControlFlow;
use std::sync::Arc;
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

#[derive(Clone, Default)]
struct PointAsyncStore(Arc<MemStore>);

impl AsyncStore for PointAsyncStore {
    type Error = MemStoreError;

    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        Store::get(&self.0, key)
    }

    async fn put(&self, key: &[u8], value: &[u8]) -> Result<(), Self::Error> {
        Store::put(&self.0, key, value)
    }

    async fn delete(&self, key: &[u8]) -> Result<(), Self::Error> {
        Store::delete(&self.0, key)
    }

    async fn batch(&self, ops: &[BatchOp<'_>]) -> Result<(), Self::Error> {
        Store::batch(&self.0, ops)
    }

    fn read_parallelism(&self) -> usize {
        4
    }
}

impl AsyncManifestStore for PointAsyncStore {
    type Error = MemStoreError;

    async fn get_root(&self, name: &[u8]) -> Result<Option<RootManifest>, Self::Error> {
        ManifestStore::get_root(&self.0, name)
    }

    async fn put_root(&self, name: &[u8], manifest: &RootManifest) -> Result<(), Self::Error> {
        ManifestStore::put_root(&self.0, name, manifest)
    }

    async fn delete_root(&self, name: &[u8]) -> Result<(), Self::Error> {
        ManifestStore::delete_root(&self.0, name)
    }

    async fn compare_and_swap_root(
        &self,
        name: &[u8],
        expected: Option<&RootManifest>,
        new: Option<&RootManifest>,
    ) -> Result<ManifestUpdate, Self::Error> {
        ManifestStore::compare_and_swap_root(&self.0, name, expected, new)
    }
}

fn records() -> Vec<ProximityRecord> {
    (0usize..128)
        .map(|index| ProximityRecord {
            key: format!("async-api-{index:04}").into_bytes(),
            vector: vec![index as f32 / 3.0, (index % 13) as f32, (index % 7) as f32],
            value: index.to_le_bytes().to_vec(),
        })
        .collect()
}

fn config() -> ProximityConfig {
    let mut config = ProximityConfig::new(3);
    config.hierarchy.log_chunk_size = 2;
    config.hierarchy.level_hash_seed = 29;
    config
}

#[test]
fn async_store_supports_the_complete_proximity_lifecycle() {
    block_on(async {
        let store = PointAsyncStore::default();
        let map = AsyncProximityMap::build(store.clone(), config(), records())
            .await
            .unwrap();
        map.verify().await.unwrap();

        assert_eq!(
            map.get(b"async-api-0017").await.unwrap().unwrap().1,
            17usize.to_le_bytes()
        );
        assert!(map.contains_key(b"async-api-0017").await.unwrap());
        assert!(!map.contains_key(b"missing").await.unwrap());

        let mut read = map.read().await.unwrap();
        let lease = read.get_lease(b"async-api-0017").await.unwrap().unwrap();
        assert!(lease.retained_bytes() >= lease.as_bytes().unwrap().len());
        let stopped = read
            .scan_records_range_until(b"async-api-0010", Some(b"async-api-0020"), |key, _| {
                ControlFlow::Break(key.to_vec())
            })
            .await
            .unwrap();
        assert_eq!(stopped.visited, 1);
        assert_eq!(stopped.break_value, Some(b"async-api-0010".to_vec()));

        let proof = map.prove_membership(b"async-api-0017").await.unwrap();
        let verified = proof.verify_for(&map.tree().descriptor).unwrap();
        assert!(verified.record.is_some());
        let structural = map
            .prove_structure(&ContentGraphLimits::default())
            .await
            .unwrap();
        assert_eq!(
            structural
                .verify_for(&map.tree().descriptor, &ContentGraphLimits::default())
                .unwrap()
                .summary,
            map.verify().await.unwrap()
        );

        let query = [17.25, 4.0, 2.0];
        let search_proof = map
            .prove_search(
                SearchRequest::exact(&query, 8),
                &ContentGraphLimits::default(),
            )
            .await
            .unwrap();
        let proved = search_proof
            .verify_for_source(&map.tree().descriptor, &ContentGraphLimits::default())
            .unwrap();
        assert_eq!(proved.result.neighbors.len(), 8);

        let named = ContentRootManifest {
            root: TypedContentRoot::new(
                ContentObjectKind::ProximityDescriptor,
                map.tree().descriptor.clone(),
            ),
            logical_version: 1,
            created_at_millis: 100,
            metadata: BTreeMap::new(),
        };
        let published = put_named_content_root_async(&store, b"proximity/main", named.clone())
            .await
            .unwrap();
        let loaded = load_named_content_root_async(&store, b"proximity/main")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(loaded, published);
        let mut next = named;
        next.logical_version = 2;
        assert!(matches!(
            compare_and_swap_named_content_root_async(
                &store,
                b"proximity/main",
                Some(&published.manifest_cid),
                next,
            )
            .await
            .unwrap(),
            ContentManifestUpdate::Applied(_)
        ));

        let expected = map
            .search(
                SearchRequest::exact(&query, 8),
                AsyncSearchControl::default(),
            )
            .await
            .unwrap();
        let descriptor = map.tree().descriptor.clone();
        let reopened = AsyncProximityMap::load(store, descriptor).await.unwrap();
        let actual = reopened
            .search(
                SearchRequest::exact(&query, 8),
                AsyncSearchControl::default(),
            )
            .await
            .unwrap();
        assert_eq!(actual.neighbors, expected.neighbors);
    });
}

#[test]
fn async_build_and_mutation_are_canonical_with_sync_and_clean_rebuild() {
    block_on(async {
        let source = records();
        let sync =
            ProximityMap::build(Arc::new(MemStore::new()), config(), source.clone()).unwrap();
        let store = PointAsyncStore::default();
        let asynchronous = AsyncProximityMap::build(store, config(), source)
            .await
            .unwrap();
        assert_eq!(asynchronous.tree(), sync.tree());

        let value_only = ProximityMutation {
            key: b"async-api-0017".to_vec(),
            value: Some((vec![17.0 / 3.0, 4.0, 3.0], b"changed".to_vec())),
        };
        let (value_map, value_stats) = asynchronous
            .mutate_batch([value_only.clone()])
            .await
            .unwrap();
        let value_oracle = asynchronous.rebuild_batch([value_only]).await.unwrap();
        assert_eq!(value_map.tree(), value_oracle.tree());
        assert_eq!(
            value_map.tree().proximity_root,
            asynchronous.tree().proximity_root
        );
        assert!(!value_stats.full_proximity_rebuild);

        let vector_change = ProximityMutation {
            key: b"async-api-0017".to_vec(),
            value: Some((vec![0.25, 0.5, 0.75], b"moved".to_vec())),
        };
        let (mutated, stats) = value_map
            .mutate_batch([vector_change.clone()])
            .await
            .unwrap();
        let oracle = value_map.rebuild_batch([vector_change]).await.unwrap();
        assert_eq!(mutated.tree(), oracle.tree());
        assert!(!stats.full_proximity_rebuild);
        assert!(stats.records_rebuilt < mutated.tree().count as usize);
        assert!(stats.nodes_read > 0);
        mutated.verify().await.unwrap();
    });
}

#[test]
fn async_verification_covers_overflow_external_vectors_and_quantizers() {
    block_on(async {
        let store = PointAsyncStore::default();
        let mut config = ProximityConfig::new(32);
        config.hierarchy.log_chunk_size = 2;
        config.hierarchy.level_hash_seed = 37;
        config.vector_storage.inline_threshold_bytes = 64;
        config.overflow.min_page_bytes = 220;
        config.overflow.target_page_bytes = 340;
        config.overflow.max_page_bytes = 512;
        config.scalar_quantization = Some(ScalarQuantizationConfig { group_size: 4 });
        let records = (0usize..256).map(|index| ProximityRecord {
            key: format!("overflow-{index:04}").into_bytes(),
            vector: (0..32)
                .map(|dimension| (index * 13 + dimension * 7) as f32)
                .collect(),
            value: index.to_le_bytes().to_vec(),
        });
        let map = AsyncProximityMap::build(store, config, records)
            .await
            .unwrap();
        let verification = map.verify().await.unwrap();
        assert!(verification.overflow_page_count > 0);
        assert!(verification.external_vector_count > 0);
        assert!(verification.scalar_quantizer_count > 0);

        let structural = map
            .prove_structure(&ContentGraphLimits::default())
            .await
            .unwrap();
        let replayed = structural
            .verify_for(&map.tree().descriptor, &ContentGraphLimits::default())
            .unwrap();
        assert_eq!(replayed.summary, verification);
    });
}

#[test]
fn managed_async_head_builds_opens_and_commits_localized_mutations() {
    block_on(async {
        let store = PointAsyncStore::default();
        let head = AsyncProximityHead::new(store.clone(), b"proximity/managed".to_vec())
            .with_max_conflict_retries(2);
        let build = head
            .build_if_absent(
                config(),
                records(),
                AsyncProximityBuildOptions {
                    max_records: Some(256),
                    max_owned_bytes: Some(4 * 1024 * 1024),
                    publication_batch_items: 2,
                    ..Default::default()
                },
                100,
                BTreeMap::new(),
            )
            .await
            .unwrap();
        let AsyncProximityHeadCommit::Applied {
            snapshot,
            stats,
            attempts,
        } = build
        else {
            panic!("empty managed head unexpectedly conflicted");
        };
        assert_eq!(attempts, 1);
        assert!(stats.proximity_objects_written > 0);
        assert_eq!(snapshot.publication().manifest.logical_version, 1);
        snapshot.map().verify().await.unwrap();

        let reopened = head.open().await.unwrap().unwrap();
        assert_eq!(
            reopened
                .map()
                .get(b"async-api-0017")
                .await
                .unwrap()
                .unwrap()
                .1,
            17usize.to_le_bytes()
        );

        let update = head
            .mutate_with_retry(
                [ProximityMutation {
                    key: b"async-api-0017".to_vec(),
                    value: Some((vec![0.25, 0.5, 0.75], b"managed".to_vec())),
                }],
                101,
                BTreeMap::new(),
            )
            .await
            .unwrap();
        let AsyncProximityHeadCommit::Applied {
            snapshot,
            stats,
            attempts,
        } = update
        else {
            panic!("uncontended managed mutation unexpectedly conflicted");
        };
        assert_eq!(attempts, 1);
        assert!(!stats.full_proximity_rebuild);
        assert_eq!(snapshot.publication().manifest.logical_version, 2);
        assert_eq!(
            snapshot
                .map()
                .get(b"async-api-0017")
                .await
                .unwrap()
                .unwrap()
                .1,
            b"managed"
        );

        assert!(matches!(
            head.build_if_absent(
                config(),
                records(),
                AsyncProximityBuildOptions::default(),
                102,
                BTreeMap::new(),
            )
            .await
            .unwrap(),
            AsyncProximityHeadCommit::Conflict { .. }
        ));
    });
}

#[test]
fn async_build_options_enforce_resource_limits() {
    block_on(async {
        let result = AsyncProximityMap::build_with_options(
            PointAsyncStore::default(),
            config(),
            records(),
            AsyncProximityBuildOptions {
                max_records: Some(10),
                ..Default::default()
            },
        )
        .await;
        let error = match result {
            Ok(_) => panic!("record limit unexpectedly allowed the build"),
            Err(error) => error,
        };
        assert!(matches!(
            error,
            Error::ProximityResourceLimitExceeded {
                resource: "records",
                limit: 10,
                actual: 11,
            }
        ));
    });
}
