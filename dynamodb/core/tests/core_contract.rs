use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt;
use std::future::Future;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::task::{Context, Poll};

use prolly::{
    BatchOp, BlobRef, Cid, Config, Error as ProllyError, ManifestStore, ManifestStoreScan,
    ManifestUpdate, MemStore, Mutation, NamedRootManifest, NodeStoreScan, RootCondition,
    RootManifest, RootWrite, Store, SyncStoreAsAsync, TransactionConflict, TransactionNodeWrite,
    TransactionUpdate, TransactionalStore, VersionedMapUpdate,
};
use prolly_dynamodb_core::{
    encode_item, encode_primary_key, item_size, parse_key_condition, parse_projection,
    parse_update, AttributeValue, BatchGetTableRequest, BatchWriteAction, BatchWriteExecutionError,
    BlobFuture, BlobStorage, Clock, Condition, Database, DatabaseFormatRecord, DynamoNumber,
    IdGenerator, IndexQueryRequest, Item, KeyAttribute, KeyCondition, KeyKind, LargeValueConfig,
    MaintenanceContext, Result, RetentionPolicy, SecondaryIndexDefinition, SecondaryIndexKind,
    SecondaryIndexProjection, StoragePublicationMode, TableArchive, TableArchiveLimits, TableId,
    TransactGetRequest, TransactWriteAction, TransactionCancellationCode,
};

#[test]
fn logical_retry_tuning_is_bounded_and_format_neutral() {
    let database = Database::new(
        SyncStoreAsAsync::new(Arc::new(MemStore::new())),
        Config::default(),
    );
    let format = database.format_record().unwrap();
    assert_eq!(
        database.logical_retry_limit(),
        prolly_dynamodb_core::DEFAULT_LOGICAL_RETRY_LIMIT
    );

    let database = database.with_logical_retry_limit(0).unwrap();
    assert_eq!(database.logical_retry_limit(), 0);
    assert_eq!(database.format_record().unwrap(), format);

    let database = database
        .with_logical_retry_limit(prolly_dynamodb_core::MAX_LOGICAL_RETRY_LIMIT)
        .unwrap();
    assert_eq!(
        database.logical_retry_limit(),
        prolly_dynamodb_core::MAX_LOGICAL_RETRY_LIMIT
    );
    assert_eq!(database.format_record().unwrap(), format);

    assert!(matches!(
        database.with_logical_retry_limit(prolly_dynamodb_core::MAX_LOGICAL_RETRY_LIMIT + 1),
        Err(prolly_dynamodb_core::Error::Validation(message))
            if message.contains("logical retry limit")
    ));
}

#[test]
fn logical_retry_limit_controls_conflict_attempts() {
    block_on(async {
        let store = Arc::new(CommitFaultStore::default());
        let database = Database::new(SyncStoreAsAsync::new(store.clone()), Config::default())
            .with_sources(Arc::new(SequenceIds::default()), Arc::new(FixedClock));
        database
            .create_table(
                "RetryBudget",
                KeyAttribute {
                    name: "id".into(),
                    kind: KeyKind::String,
                },
                None,
            )
            .await
            .unwrap();
        let item = Item::from([("id".into(), AttributeValue::S("one".into()))]);

        let database = database.with_logical_retry_limit(0).unwrap();
        store.conflict_next_commits(1);
        assert!(matches!(
            database.put_item("RetryBudget", item.clone(), None).await,
            Err(prolly_dynamodb_core::Error::ConflictExhausted)
        ));
        assert!(database
            .get_item("RetryBudget", &item)
            .await
            .unwrap()
            .is_none());

        let database = database.with_logical_retry_limit(1).unwrap();
        store.conflict_next_commits(1);
        database
            .put_item("RetryBudget", item.clone(), None)
            .await
            .unwrap();
        assert_eq!(
            database.get_item("RetryBudget", &item).await.unwrap(),
            Some(item)
        );
    });
}

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

#[derive(Default)]
struct SequenceIds(Mutex<u8>);

impl IdGenerator for SequenceIds {
    fn generate(&self) -> Result<TableId> {
        let mut next = self.0.lock().unwrap();
        *next += 1;
        Ok(TableId([*next; 32]))
    }
}

#[derive(Default)]
struct WideSequenceIds(Mutex<u64>);

impl IdGenerator for WideSequenceIds {
    fn generate(&self) -> Result<TableId> {
        let mut next = self.0.lock().unwrap();
        *next = next.checked_add(1).expect("test ID sequence exhausted");
        let bytes = next.to_be_bytes();
        let mut id = [0; 32];
        for chunk in id.chunks_exact_mut(bytes.len()) {
            chunk.copy_from_slice(&bytes);
        }
        Ok(TableId(id))
    }
}

struct FixedClock;

impl Clock for FixedClock {
    fn now_millis(&self) -> u64 {
        42
    }
}

struct AdjustableClock(AtomicU64);

impl AdjustableClock {
    fn new(now_millis: u64) -> Self {
        Self(AtomicU64::new(now_millis))
    }

    fn set(&self, now_millis: u64) {
        self.0.store(now_millis, Ordering::SeqCst);
    }
}

impl Clock for AdjustableClock {
    fn now_millis(&self) -> u64 {
        self.0.load(Ordering::SeqCst)
    }
}

#[derive(Debug)]
struct FaultError(&'static str);

impl fmt::Display for FaultError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.0)
    }
}

impl std::error::Error for FaultError {}

#[derive(Default)]
struct CommitFaultStore {
    inner: MemStore,
    commits_until_failure: AtomicUsize,
    conflicts_until_success: AtomicUsize,
    ambiguous_after_commit: AtomicUsize,
    reconciliation_read_failures: AtomicUsize,
    root_reads_until_failure: AtomicUsize,
}

impl CommitFaultStore {
    fn fail_commit_number_from_now(&self, number: usize) {
        assert!(number > 0);
        self.commits_until_failure.store(number, Ordering::SeqCst);
    }

    fn conflict_next_commits(&self, count: usize) {
        self.conflicts_until_success.store(count, Ordering::SeqCst);
    }

    fn report_ambiguous_after_next_commit(&self) {
        self.ambiguous_after_commit.store(1, Ordering::SeqCst);
    }

    fn report_ambiguous_and_fail_reconciliation(&self) {
        self.reconciliation_read_failures.store(1, Ordering::SeqCst);
        self.report_ambiguous_after_next_commit();
    }
}

impl Store for CommitFaultStore {
    type Error = FaultError;

    fn get(&self, key: &[u8]) -> std::result::Result<Option<Vec<u8>>, Self::Error> {
        self.inner
            .get(key)
            .map_err(|_| FaultError("node read failed"))
    }

    fn put(&self, key: &[u8], value: &[u8]) -> std::result::Result<(), Self::Error> {
        self.inner
            .put(key, value)
            .map_err(|_| FaultError("node write failed"))
    }

    fn delete(&self, key: &[u8]) -> std::result::Result<(), Self::Error> {
        self.inner
            .delete(key)
            .map_err(|_| FaultError("node delete failed"))
    }

    fn batch(&self, ops: &[BatchOp<'_>]) -> std::result::Result<(), Self::Error> {
        self.inner
            .batch(ops)
            .map_err(|_| FaultError("node batch failed"))
    }
}

impl ManifestStore for CommitFaultStore {
    type Error = FaultError;

    fn get_root(&self, name: &[u8]) -> std::result::Result<Option<RootManifest>, Self::Error> {
        let remaining = self.root_reads_until_failure.load(Ordering::SeqCst);
        if remaining > 0 && self.root_reads_until_failure.fetch_sub(1, Ordering::SeqCst) == 1 {
            return Err(FaultError("injected reconciliation root read failure"));
        }
        ManifestStore::get_root(&self.inner, name).map_err(|_| FaultError("root read failed"))
    }

    fn put_root(
        &self,
        name: &[u8],
        manifest: &RootManifest,
    ) -> std::result::Result<(), Self::Error> {
        ManifestStore::put_root(&self.inner, name, manifest)
            .map_err(|_| FaultError("root write failed"))
    }

    fn delete_root(&self, name: &[u8]) -> std::result::Result<(), Self::Error> {
        ManifestStore::delete_root(&self.inner, name).map_err(|_| FaultError("root delete failed"))
    }

    fn compare_and_swap_root(
        &self,
        name: &[u8],
        expected: Option<&RootManifest>,
        new: Option<&RootManifest>,
    ) -> std::result::Result<ManifestUpdate, Self::Error> {
        ManifestStore::compare_and_swap_root(&self.inner, name, expected, new)
            .map_err(|_| FaultError("root CAS failed"))
    }
}

impl ManifestStoreScan for CommitFaultStore {
    fn list_roots(&self) -> std::result::Result<Vec<NamedRootManifest>, Self::Error> {
        self.inner
            .list_roots()
            .map_err(|_| FaultError("list roots"))
    }
}

impl TransactionalStore for CommitFaultStore {
    fn supports_transactions(&self) -> bool {
        true
    }

    fn commit_transaction(
        &self,
        node_writes: &[TransactionNodeWrite],
        root_conditions: &[RootCondition],
        root_writes: &[RootWrite],
    ) -> std::result::Result<TransactionUpdate, ProllyError> {
        if self.conflicts_until_success.load(Ordering::SeqCst) > 0 {
            self.conflicts_until_success.fetch_sub(1, Ordering::SeqCst);
            let condition = root_conditions.first();
            return Ok(TransactionUpdate::Conflict(Box::new(
                TransactionConflict::new(
                    condition.map_or_else(Vec::new, |value| value.name.clone()),
                    condition.and_then(|value| value.expected.clone()),
                    None,
                ),
            )));
        }
        let remaining = self.commits_until_failure.load(Ordering::SeqCst);
        if remaining > 0 && self.commits_until_failure.fetch_sub(1, Ordering::SeqCst) == 1 {
            return Err(ProllyError::Store(Box::new(FaultError(
                "injected transaction commit failure",
            ))));
        }
        let update = TransactionalStore::commit_transaction(
            &self.inner,
            node_writes,
            root_conditions,
            root_writes,
        )?;
        if matches!(update, TransactionUpdate::Applied { .. })
            && self.ambiguous_after_commit.swap(0, Ordering::SeqCst) > 0
        {
            self.root_reads_until_failure.store(
                self.reconciliation_read_failures.swap(0, Ordering::SeqCst),
                Ordering::SeqCst,
            );
            return Err(ProllyError::Store(Box::new(FaultError(
                "injected ambiguous response after commit",
            ))));
        }
        Ok(update)
    }
}

#[derive(Default)]
struct RecordingBlobs(Mutex<HashMap<Vec<u8>, Vec<u8>>>);

impl BlobStorage for RecordingBlobs {
    fn get_blob<'a>(&'a self, reference: &'a BlobRef) -> BlobFuture<'a, Option<Vec<u8>>> {
        Box::pin(async move {
            Ok(self
                .0
                .lock()
                .unwrap()
                .get(reference.cid.as_bytes())
                .cloned())
        })
    }

    fn put_blob<'a>(&'a self, bytes: &'a [u8]) -> BlobFuture<'a, BlobRef> {
        Box::pin(async move {
            let reference = BlobRef::from_bytes(bytes);
            self.0
                .lock()
                .unwrap()
                .insert(reference.cid.as_bytes().to_vec(), bytes.to_vec());
            Ok(reference)
        })
    }
}

#[derive(Default)]
struct FaultingBlobs {
    blobs: Mutex<HashMap<Vec<u8>, Vec<u8>>>,
    fail_before_put: AtomicUsize,
    fail_after_put: AtomicUsize,
}

impl FaultingBlobs {
    fn fail_before_next_put(&self) {
        self.fail_before_put.store(1, Ordering::SeqCst);
    }

    fn fail_after_next_put(&self) {
        self.fail_after_put.store(1, Ordering::SeqCst);
    }

    fn len(&self) -> usize {
        self.blobs.lock().unwrap().len()
    }
}

impl BlobStorage for FaultingBlobs {
    fn get_blob<'a>(&'a self, reference: &'a BlobRef) -> BlobFuture<'a, Option<Vec<u8>>> {
        Box::pin(async move {
            Ok(self
                .blobs
                .lock()
                .unwrap()
                .get(reference.cid.as_bytes())
                .cloned())
        })
    }

    fn put_blob<'a>(&'a self, bytes: &'a [u8]) -> BlobFuture<'a, BlobRef> {
        Box::pin(async move {
            if self.fail_before_put.swap(0, Ordering::SeqCst) > 0 {
                return Err(prolly_dynamodb_core::Error::Blob(
                    "injected failure before blob preparation".into(),
                ));
            }
            let reference = BlobRef::from_bytes(bytes);
            self.blobs
                .lock()
                .unwrap()
                .insert(reference.cid.as_bytes().to_vec(), bytes.to_vec());
            if self.fail_after_put.swap(0, Ordering::SeqCst) > 0 {
                return Err(prolly_dynamodb_core::Error::Blob(
                    "injected failure after blob preparation".into(),
                ));
            }
            Ok(reference)
        })
    }
}

#[derive(Default)]
struct BlockingBlobState {
    entered: bool,
    proceed: bool,
    blobs: HashMap<Vec<u8>, Vec<u8>>,
}

#[derive(Default)]
struct BlockingBlobs {
    state: Mutex<BlockingBlobState>,
    changed: Condvar,
}

impl BlockingBlobs {
    fn wait_until_put_entered(&self) {
        let mut state = self.state.lock().unwrap();
        while !state.entered {
            state = self.changed.wait(state).unwrap();
        }
    }

    fn allow_put(&self) {
        let mut state = self.state.lock().unwrap();
        state.proceed = true;
        self.changed.notify_all();
    }
}

impl BlobStorage for BlockingBlobs {
    fn get_blob<'a>(&'a self, reference: &'a BlobRef) -> BlobFuture<'a, Option<Vec<u8>>> {
        Box::pin(async move {
            Ok(self
                .state
                .lock()
                .unwrap()
                .blobs
                .get(reference.cid.as_bytes())
                .cloned())
        })
    }

    fn put_blob<'a>(&'a self, bytes: &'a [u8]) -> BlobFuture<'a, BlobRef> {
        Box::pin(async move {
            let mut state = self.state.lock().unwrap();
            state.entered = true;
            self.changed.notify_all();
            while !state.proceed {
                state = self.changed.wait(state).unwrap();
            }
            let reference = BlobRef::from_bytes(bytes);
            state
                .blobs
                .insert(reference.cid.as_bytes().to_vec(), bytes.to_vec());
            Ok(reference)
        })
    }
}

fn key(account: &str, sequence: &str) -> Item {
    Item::from([
        ("account".into(), AttributeValue::S(account.into())),
        (
            "sequence".into(),
            AttributeValue::N(DynamoNumber::parse(sequence).unwrap()),
        ),
    ])
}

fn item(account: &str, sequence: &str, status: &str) -> Item {
    let mut item = key(account, sequence);
    item.insert("status".into(), AttributeValue::S(status.into()));
    item
}

#[test]
fn decimal_normalization_and_ordered_key_encoding_are_exact() {
    let equivalent =
        ["1.20", "01.2", "+12e-1", "1200e-3"].map(|value| DynamoNumber::parse(value).unwrap());
    assert!(equivalent.iter().all(|value| value.as_str() == "1.2"));

    let description = prolly_dynamodb_core::TableDescription {
        name: "numbers".into(),
        id: TableId([1; 32]),
        partition_key: KeyAttribute {
            name: "n".into(),
            kind: KeyKind::Number,
        },
        sort_key: None,
        attribute_definitions: BTreeMap::from([("n".into(), KeyKind::Number)]),
        secondary_indexes: Vec::new(),
        status: prolly_dynamodb_core::TableStatus::Active,
        created_at_millis: 0,
    };
    let values = [
        "-100", "-1.21", "-1.2", "-0.01", "0", "0.01", "1.2", "1.21", "100",
    ];
    let encoded = values.map(|value| {
        encode_primary_key(
            &description,
            &Item::from([(
                "n".into(),
                AttributeValue::N(DynamoNumber::parse(value).unwrap()),
            )]),
        )
        .unwrap()
    });
    assert!(encoded.windows(2).all(|window| window[0] < window[1]));

    assert_eq!(
        DynamoNumber::parse("99999999999999999999.99")
            .unwrap()
            .checked_add(&DynamoNumber::parse("0.01").unwrap())
            .unwrap()
            .as_str(),
        "100000000000000000000"
    );
    assert_eq!(
        DynamoNumber::parse("1e-130")
            .unwrap()
            .checked_sub(&DynamoNumber::parse("1e-130").unwrap())
            .unwrap()
            .as_str(),
        "0"
    );
}

#[test]
fn item_encoding_is_independent_of_insertion_order() {
    let left = Item::from([
        ("z".into(), AttributeValue::Bool(true)),
        ("a".into(), AttributeValue::S("value".into())),
    ]);
    let mut right = Item::new();
    right.insert("a".into(), AttributeValue::S("value".into()));
    right.insert("z".into(), AttributeValue::Bool(true));
    assert_eq!(encode_item(&left).unwrap(), encode_item(&right).unwrap());
}

#[test]
fn item_sizing_and_document_depth_follow_dynamodb_rules() {
    let shirt = Item::from([
        ("shirt-color".into(), AttributeValue::S("R".into())),
        ("shirt-size".into(), AttributeValue::S("M".into())),
    ]);
    assert_eq!(item_size(&shirt).unwrap(), 23);

    let collections = Item::from([
        (
            "l".into(),
            AttributeValue::L(vec![
                AttributeValue::S("a".into()),
                AttributeValue::S("b".into()),
            ]),
        ),
        (
            "m".into(),
            AttributeValue::M(Item::from([("k".into(), AttributeValue::S("v".into()))])),
        ),
        (
            "n".into(),
            AttributeValue::N(
                DynamoNumber::parse("12345678901234567890123456789012345678").unwrap(),
            ),
        ),
    ]);
    assert_eq!(
        item_size(&collections).unwrap(),
        1 + 3 + 4 + 1 + 3 + 3 + 1 + 20
    );

    let mut nested = AttributeValue::S("leaf".into());
    for _ in 0..32 {
        nested = AttributeValue::L(vec![nested]);
    }
    assert!(encode_item(&Item::from([("doc".into(), nested.clone())])).is_ok());
    let too_deep = AttributeValue::L(vec![nested]);
    assert!(encode_item(&Item::from([("doc".into(), too_deep)])).is_err());
}

#[test]
fn large_items_are_blob_backed_and_version_correct_without_oversized_nodes() {
    block_on(async {
        let store = Arc::new(MemStore::new());
        let blobs = Arc::new(RecordingBlobs::default());
        let database = Database::new_with_blob_storage(
            SyncStoreAsAsync::new(store.clone()),
            Config::default(),
            blobs.clone(),
            LargeValueConfig::new(1024),
        )
        .unwrap()
        .with_sources(Arc::new(SequenceIds::default()), Arc::new(FixedClock));
        database
            .create_table(
                "Evidence",
                KeyAttribute {
                    name: "case_id".into(),
                    kind: KeyKind::String,
                },
                None,
            )
            .await
            .unwrap();
        let payload = "x".repeat(350 * 1024);
        let item = Item::from([
            ("case_id".into(), AttributeValue::S("case-1".into())),
            ("document".into(), AttributeValue::S(payload.clone())),
        ]);
        let update = database.put_item("Evidence", item, None).await.unwrap();
        let version = match update {
            VersionedMapUpdate::Applied { current, .. } => current.id,
            other => panic!("unexpected update: {other:?}"),
        };
        assert_eq!(blobs.0.lock().unwrap().len(), 1);
        let expected_blob = {
            let blobs = blobs.0.lock().unwrap();
            let (cid, bytes) = blobs.iter().next().unwrap();
            BlobRef {
                cid: Cid(cid.as_slice().try_into().unwrap()),
                len: u64::try_from(bytes.len()).unwrap(),
            }
        };
        let mut registered = Vec::new();
        for root in store.list_roots().unwrap() {
            if let Some(mut references) = database
                .expand_blob_registry_root(&root.name, &root.manifest.to_tree(), 10)
                .await
                .unwrap()
            {
                registered.append(&mut references);
            }
        }
        assert_eq!(registered, vec![expected_blob]);

        let read = database
            .get_item_with_version(
                "Evidence",
                &Item::from([("case_id".into(), AttributeValue::S("case-1".into()))]),
            )
            .await
            .unwrap();
        assert_eq!(read.version_id, version);
        assert_eq!(
            read.item.unwrap().get("document"),
            Some(&AttributeValue::S(payload))
        );

        for cid in store.list_node_cids().unwrap() {
            let bytes = store.get(cid.as_bytes()).unwrap().unwrap();
            assert!(bytes.len() <= 300 * 1024, "oversized node: {}", bytes.len());
        }
    });
}

#[test]
fn blob_prepare_failures_never_advance_logical_visibility() {
    block_on(async {
        let blobs = Arc::new(FaultingBlobs::default());
        let database = Database::new_with_blob_storage(
            SyncStoreAsAsync::new(Arc::new(MemStore::new())),
            Config::default(),
            blobs.clone(),
            LargeValueConfig::new(64),
        )
        .unwrap()
        .with_sources(Arc::new(SequenceIds::default()), Arc::new(FixedClock));
        database
            .create_table(
                "Evidence",
                KeyAttribute {
                    name: "case_id".into(),
                    kind: KeyKind::String,
                },
                None,
            )
            .await
            .unwrap();
        let original_head = database.head("Evidence").await.unwrap();
        let large_item = |case_id: &str| {
            Item::from([
                ("case_id".into(), AttributeValue::S(case_id.into())),
                (
                    "document".into(),
                    AttributeValue::S("signed:".to_string() + &"x".repeat(4096)),
                ),
            ])
        };

        blobs.fail_before_next_put();
        assert!(matches!(
            database
                .put_item_idempotent(
                    "Evidence",
                    large_item("before"),
                    None,
                    None,
                    "blob-before",
                    false,
                )
                .await,
            Err(prolly_dynamodb_core::Error::Blob(_))
        ));
        assert_eq!(database.head("Evidence").await.unwrap(), original_head);
        assert_eq!(blobs.len(), 0);

        blobs.fail_after_next_put();
        assert!(matches!(
            database
                .put_item_idempotent(
                    "Evidence",
                    large_item("after"),
                    None,
                    None,
                    "blob-after",
                    false,
                )
                .await,
            Err(prolly_dynamodb_core::Error::Blob(_))
        ));
        assert_eq!(database.head("Evidence").await.unwrap(), original_head);
        assert_eq!(
            blobs.len(),
            1,
            "post-prepare failure may leave one safe orphan"
        );
        assert!(database
            .get_item(
                "Evidence",
                &Item::from([("case_id".into(), AttributeValue::S("after".into()))]),
            )
            .await
            .unwrap()
            .is_none());
    });
}

#[test]
fn table_archive_import_is_exact_atomic_audited_and_replay_safe() {
    block_on(async {
        let store = Arc::new(MemStore::new());
        let blobs = Arc::new(RecordingBlobs::default());
        let database = Database::new_with_blob_storage(
            SyncStoreAsAsync::new(store),
            Config::default(),
            blobs,
            LargeValueConfig::new(128),
        )
        .unwrap()
        .with_sources(Arc::new(SequenceIds::default()), Arc::new(FixedClock));
        database
            .create_table(
                "Evidence",
                KeyAttribute {
                    name: "case_id".into(),
                    kind: KeyKind::String,
                },
                None,
            )
            .await
            .unwrap();

        let key = Item::from([("case_id".into(), AttributeValue::S("case-42".into()))]);
        let historical_item = Item::from([
            ("case_id".into(), AttributeValue::S("case-42".into())),
            (
                "document".into(),
                AttributeValue::S("signed-v1:".to_string() + &"x".repeat(4096)),
            ),
        ]);
        database
            .put_item("Evidence", historical_item.clone(), None)
            .await
            .unwrap();
        let historical = database.head("Evidence").await.unwrap().id;
        let mut latest_item = historical_item.clone();
        latest_item.insert("status".into(), AttributeValue::S("SUPERSEDED".into()));
        database
            .put_item("Evidence", latest_item, None)
            .await
            .unwrap();

        let limits = TableArchiveLimits::new(
            10_000,
            16 * 1024 * 1024,
            1_000,
            16 * 1024 * 1024,
            32 * 1024 * 1024,
        );
        let archive = database
            .export_table("Evidence", Some(&historical), limits)
            .await
            .unwrap();
        assert_eq!(archive.version, historical);
        assert_eq!(archive.blobs.len(), 1);
        let encoded = archive.to_bytes(limits).unwrap();
        let archive = TableArchive::from_bytes(&encoded, limits).unwrap();

        let plan = database
            .plan_import(&archive, "EvidenceRestored", limits)
            .await
            .unwrap();
        assert!(matches!(
            database.describe_table("EvidenceRestored").await,
            Err(prolly_dynamodb_core::Error::TableNotFound(_))
        ));
        let context = MaintenanceContext::new("records-officer", "court restoration")
            .change_ticket("LEGAL-42");
        let result = database
            .apply_import(&archive, &plan, context.clone(), limits)
            .await
            .unwrap();
        assert!(!result.replayed);
        assert_eq!(result.version, historical);
        assert_eq!(result.description.id, plan.target_table_id);
        assert_eq!(
            database.head("EvidenceRestored").await.unwrap().id,
            historical
        );
        assert_eq!(
            database.get_item("EvidenceRestored", &key).await.unwrap(),
            Some(historical_item)
        );
        let audit = database.import_audit(&plan.id).await.unwrap().unwrap();
        assert_eq!(audit.context, context);
        assert_eq!(audit.commit_id, result.commit_id);

        let replay = database
            .apply_import(&archive, &plan, context.clone(), limits)
            .await
            .unwrap();
        assert!(replay.replayed);
        assert_eq!(replay.commit_id, result.commit_id);
        assert_eq!(replay.description, result.description);

        let mut tampered = plan;
        tampered.target_table_name = "OtherTarget".into();
        assert!(database
            .apply_import(&archive, &tampered, context, limits)
            .await
            .is_err());
    });
}

#[test]
fn table_import_has_no_partial_logical_visibility_and_reconciles_after_restart() {
    block_on(async {
        let store = Arc::new(CommitFaultStore::default());
        let blobs = Arc::new(RecordingBlobs::default());
        let database = Database::new_with_blob_storage(
            SyncStoreAsAsync::new(store.clone()),
            Config::default(),
            blobs.clone(),
            LargeValueConfig::new(128),
        )
        .unwrap()
        .with_sources(Arc::new(SequenceIds::default()), Arc::new(FixedClock));
        database
            .create_table(
                "ImportSource",
                KeyAttribute {
                    name: "case_id".into(),
                    kind: KeyKind::String,
                },
                None,
            )
            .await
            .unwrap();
        let key = Item::from([("case_id".into(), AttributeValue::S("case-7".into()))]);
        database
            .put_item(
                "ImportSource",
                Item::from([
                    ("case_id".into(), AttributeValue::S("case-7".into())),
                    (
                        "record".into(),
                        AttributeValue::S("immutable:".to_string() + &"z".repeat(2048)),
                    ),
                ]),
                None,
            )
            .await
            .unwrap();
        let limits = TableArchiveLimits::new(
            10_000,
            16 * 1024 * 1024,
            1_000,
            16 * 1024 * 1024,
            32 * 1024 * 1024,
        );
        let archive = database
            .export_table("ImportSource", None, limits)
            .await
            .unwrap();
        let context = MaintenanceContext::new("recovery-bot", "disaster recovery test");

        let conflict_plan = database
            .plan_import(&archive, "ConflictTarget", limits)
            .await
            .unwrap();
        store.conflict_next_commits(1);
        assert!(matches!(
            database
                .apply_import(&archive, &conflict_plan, context.clone(), limits)
                .await,
            Err(prolly_dynamodb_core::Error::ImportPlanStale(_))
        ));
        assert!(matches!(
            database.describe_table("ConflictTarget").await,
            Err(prolly_dynamodb_core::Error::TableNotFound(_))
        ));
        assert!(database
            .import_audit(&conflict_plan.id)
            .await
            .unwrap()
            .is_none());

        let ambiguous_plan = database
            .plan_import(&archive, "RestartTarget", limits)
            .await
            .unwrap();
        store.report_ambiguous_and_fail_reconciliation();
        assert!(matches!(
            database
                .apply_import(&archive, &ambiguous_plan, context.clone(), limits)
                .await,
            Err(prolly_dynamodb_core::Error::Storage(_))
        ));

        let restarted = Database::new_with_blob_storage(
            SyncStoreAsAsync::new(store),
            Config::default(),
            blobs,
            LargeValueConfig::new(128),
        )
        .unwrap()
        .with_sources(Arc::new(SequenceIds::default()), Arc::new(FixedClock));
        let replay = restarted
            .apply_import(&archive, &ambiguous_plan, context, limits)
            .await
            .unwrap();
        assert!(replay.replayed);
        assert_eq!(replay.version, archive.version);
        assert!(restarted
            .get_item("RestartTarget", &key)
            .await
            .unwrap()
            .is_some());
    });
}

#[test]
fn maintenance_lease_fences_all_writes_until_explicit_release_or_expired_break() {
    block_on(async {
        let clock = Arc::new(AdjustableClock::new(1_000));
        let database = Database::new(
            SyncStoreAsAsync::new(Arc::new(MemStore::new())),
            Config::default(),
        )
        .with_sources(Arc::new(SequenceIds::default()), clock.clone());
        database
            .create_table(
                "Evidence",
                KeyAttribute {
                    name: "case_id".into(),
                    kind: KeyKind::String,
                },
                None,
            )
            .await
            .unwrap();
        let holder = MaintenanceContext::new("gc-worker", "verified global node sweep")
            .change_ticket("OPS-42");
        let lease = database
            .acquire_maintenance_lease(holder, 60_000)
            .await
            .unwrap();
        assert_eq!(
            database.maintenance_lease().await.unwrap(),
            Some(lease.clone())
        );
        assert!(matches!(
            database
                .acquire_maintenance_lease(
                    MaintenanceContext::new("other", "competing maintenance"),
                    60_000,
                )
                .await,
            Err(prolly_dynamodb_core::Error::MaintenanceInProgress { .. })
        ));
        let write = || {
            database.put_item(
                "Evidence",
                Item::from([("case_id".into(), AttributeValue::S("case-1".into()))]),
                None,
            )
        };
        assert!(matches!(
            write().await,
            Err(prolly_dynamodb_core::Error::MaintenanceInProgress { .. })
        ));
        assert!(database
            .break_expired_maintenance_lease(
                &lease.id,
                MaintenanceContext::new("incident-commander", "premature break test"),
            )
            .await
            .is_err());

        // Expiry alone never admits writers: a paused sweeper remains safe.
        clock.set(lease.expires_at_millis + 1);
        assert!(matches!(
            write().await,
            Err(prolly_dynamodb_core::Error::MaintenanceInProgress { .. })
        ));
        let breaker = MaintenanceContext::new("incident-commander", "expired worker recovery")
            .change_ticket("INC-7");
        let release = database
            .break_expired_maintenance_lease(&lease.id, breaker.clone())
            .await
            .unwrap();
        assert!(release.forced_after_expiry);
        assert!(!release.replayed);
        assert!(database.maintenance_lease().await.unwrap().is_none());
        let replay = database
            .break_expired_maintenance_lease(&lease.id, breaker)
            .await
            .unwrap();
        assert!(replay.replayed);
        assert!(write().await.is_ok());
    });
}

#[test]
fn worker_leases_fence_takeovers_and_checkpoint_monotonic_progress() {
    block_on(async {
        let store = Arc::new(CommitFaultStore::default());
        let clock = Arc::new(AdjustableClock::new(100_000));
        let database = Database::new(SyncStoreAsAsync::new(store.clone()), Config::default())
            .with_sources(Arc::new(SequenceIds::default()), clock.clone());
        let configuration = serde_cbor::to_vec(&("stream", "Evidence", "court-export")).unwrap();
        let digest = prolly_dynamodb_core::WorkerJobId::configuration_digest(&configuration);
        let job = prolly_dynamodb_core::WorkerJobId::for_configuration(
            prolly_dynamodb_core::WorkerKind::Stream,
            &configuration,
        );
        let first = database
            .acquire_worker_lease(
                job.clone(),
                prolly_dynamodb_core::WorkerKind::Stream,
                digest,
                "worker-a",
                prolly_dynamodb_core::MIN_WORKER_LEASE_MILLIS,
            )
            .await
            .unwrap();
        assert_eq!(first.fence, 1);
        assert!(matches!(
            database
                .acquire_worker_lease(
                    job.clone(),
                    prolly_dynamodb_core::WorkerKind::Stream,
                    digest,
                    "worker-b",
                    prolly_dynamodb_core::MIN_WORKER_LEASE_MILLIS,
                )
                .await,
            Err(prolly_dynamodb_core::Error::WorkerLeaseHeld { .. })
        ));
        let table_id = TableId([91; 32]);
        let checkpoint = database
            .update_worker_checkpoint(
                &first,
                None,
                prolly_dynamodb_core::WorkerProgress::Stream {
                    table_id: table_id.clone(),
                    delivered_through_sequence: 2,
                },
            )
            .await
            .unwrap();
        assert_eq!(checkpoint.revision, 1);
        assert!(database
            .update_worker_checkpoint(
                &first,
                Some(1),
                prolly_dynamodb_core::WorkerProgress::Stream {
                    table_id: table_id.clone(),
                    delivered_through_sequence: 1,
                },
            )
            .await
            .is_err());

        clock.set(101_000);
        let renewed = database
            .renew_worker_lease(&first, prolly_dynamodb_core::MIN_WORKER_LEASE_MILLIS)
            .await
            .unwrap();
        assert_eq!(renewed.fence, first.fence);
        assert!(matches!(
            database
                .update_worker_checkpoint(
                    &first,
                    Some(1),
                    prolly_dynamodb_core::WorkerProgress::Stream {
                        table_id: table_id.clone(),
                        delivered_through_sequence: 3,
                    },
                )
                .await,
            Err(prolly_dynamodb_core::Error::WorkerLeaseLost { .. })
        ));

        clock.set(renewed.expires_at_millis);
        let takeover = database
            .acquire_worker_lease(
                job.clone(),
                prolly_dynamodb_core::WorkerKind::Stream,
                digest,
                "worker-b",
                prolly_dynamodb_core::MIN_WORKER_LEASE_MILLIS,
            )
            .await
            .unwrap();
        assert_eq!(takeover.fence, 2);
        let checkpoint = database
            .update_worker_checkpoint(
                &takeover,
                Some(checkpoint.revision),
                prolly_dynamodb_core::WorkerProgress::Stream {
                    table_id,
                    delivered_through_sequence: 3,
                },
            )
            .await
            .unwrap();
        assert_eq!(checkpoint.revision, 2);
        assert_eq!(checkpoint.fence, takeover.fence);

        store.report_ambiguous_after_next_commit();
        let release = database.release_worker_lease(&takeover).await.unwrap();
        assert!(release.replayed);
        assert!(database.worker_lease(&job).await.unwrap().is_none());
        assert_eq!(
            database.worker_checkpoint(&job).await.unwrap().unwrap(),
            checkpoint
        );

        let reacquired = database
            .acquire_worker_lease(
                job.clone(),
                prolly_dynamodb_core::WorkerKind::Stream,
                digest,
                "worker-c",
                prolly_dynamodb_core::MIN_WORKER_LEASE_MILLIS,
            )
            .await
            .unwrap();
        assert_eq!(reacquired.fence, 3);
        let reacquired_release = database.release_worker_lease(&reacquired).await.unwrap();
        assert!(!reacquired_release.replayed);
        assert_eq!(
            database
                .worker_lease_release(&job, takeover.fence)
                .await
                .unwrap()
                .unwrap()
                .lease,
            takeover
        );
        assert_eq!(
            database
                .worker_lease_release(&job, reacquired.fence)
                .await
                .unwrap()
                .unwrap()
                .lease,
            reacquired
        );

        let restarted = Database::new(SyncStoreAsAsync::new(store), Config::default())
            .with_sources(Arc::new(SequenceIds::default()), clock);
        assert_eq!(
            restarted
                .worker_lease_release(&job, takeover.fence)
                .await
                .unwrap()
                .unwrap()
                .lease,
            takeover
        );
        assert_eq!(
            restarted
                .worker_lease_release(&job, reacquired.fence)
                .await
                .unwrap()
                .unwrap()
                .lease,
            reacquired
        );
        assert_eq!(
            restarted.worker_checkpoint(&job).await.unwrap().unwrap(),
            checkpoint
        );
    });
}

#[test]
fn ttl_candidates_match_dynamodb_window_and_refresh_races_never_delete() {
    block_on(async {
        let database = Database::new(
            SyncStoreAsAsync::new(Arc::new(MemStore::new())),
            Config::default(),
        )
        .with_sources(Arc::new(SequenceIds::default()), Arc::new(FixedClock));
        database
            .create_table(
                "Evidence",
                KeyAttribute {
                    name: "case_id".into(),
                    kind: KeyKind::String,
                },
                None,
            )
            .await
            .unwrap();
        let table_id = database.describe_table("Evidence").await.unwrap().id;
        let now = 2_000_000_000_u64;
        let values = [
            (
                "expired",
                AttributeValue::N(DynamoNumber::parse("1999999999").unwrap()),
            ),
            (
                "future",
                AttributeValue::N(DynamoNumber::parse("2000000001").unwrap()),
            ),
            (
                "fractional",
                AttributeValue::N(DynamoNumber::parse("1999999999.5").unwrap()),
            ),
            ("string", AttributeValue::S("1999999999".into())),
            (
                "ancient",
                AttributeValue::N(
                    DynamoNumber::parse(
                        &(now - prolly_dynamodb_core::TTL_MAX_PAST_SECONDS - 1).to_string(),
                    )
                    .unwrap(),
                ),
            ),
        ];
        for (case_id, expiration) in values {
            database
                .put_item(
                    "Evidence",
                    Item::from([
                        ("case_id".into(), AttributeValue::S(case_id.into())),
                        ("expiresAt".into(), expiration),
                    ]),
                    None,
                )
                .await
                .unwrap();
        }

        let page = database
            .ttl_candidates("Evidence", &table_id, "expiresAt", None, 100, now)
            .await
            .unwrap();
        assert_eq!(page.evaluated, 5);
        assert_eq!(page.candidates.len(), 1);
        let candidate = page.candidates[0].clone();
        assert_eq!(
            candidate.key["case_id"],
            AttributeValue::S("expired".into())
        );

        // A concurrent writer refreshes the expiration after the scan. The
        // exact-value condition turns the stale candidate into a safe no-op.
        database
            .put_item(
                "Evidence",
                Item::from([
                    ("case_id".into(), AttributeValue::S("expired".into())),
                    (
                        "expiresAt".into(),
                        AttributeValue::N(DynamoNumber::parse("2000000100").unwrap()),
                    ),
                ]),
                None,
            )
            .await
            .unwrap();
        assert!(!database
            .expire_ttl_candidate("Evidence", &table_id, "expiresAt", &candidate, now)
            .await
            .unwrap());
        assert!(database
            .get_item(
                "Evidence",
                &Item::from([("case_id".into(), AttributeValue::S("expired".into()),)]),
            )
            .await
            .unwrap()
            .is_some());

        database
            .put_item(
                "Evidence",
                Item::from([
                    ("case_id".into(), AttributeValue::S("expired".into())),
                    (
                        "expiresAt".into(),
                        AttributeValue::N(DynamoNumber::parse("1999999999").unwrap()),
                    ),
                ]),
                None,
            )
            .await
            .unwrap();
        let candidate = database
            .ttl_candidates("Evidence", &table_id, "expiresAt", None, 100, now)
            .await
            .unwrap()
            .candidates
            .into_iter()
            .next()
            .unwrap();
        assert!(database
            .expire_ttl_candidate("Evidence", &table_id, "expiresAt", &candidate, now)
            .await
            .unwrap());
        assert!(database
            .get_item("Evidence", &candidate.key)
            .await
            .unwrap()
            .is_none());
        assert!(database
            .validate_ttl_configuration("Evidence", "case_id")
            .await
            .is_err());

        database.delete_table("Evidence").await.unwrap();
        database
            .create_table(
                "Evidence",
                KeyAttribute {
                    name: "case_id".into(),
                    kind: KeyKind::String,
                },
                None,
            )
            .await
            .unwrap();
        let recreated_id = database.describe_table("Evidence").await.unwrap().id;
        assert_ne!(recreated_id, table_id);
        database
            .put_item(
                "Evidence",
                Item::from([
                    ("case_id".into(), AttributeValue::S("expired".into())),
                    (
                        "expiresAt".into(),
                        AttributeValue::N(DynamoNumber::parse("1999999999").unwrap()),
                    ),
                ]),
                None,
            )
            .await
            .unwrap();
        assert!(matches!(
            database
                .expire_ttl_candidate("Evidence", &table_id, "expiresAt", &candidate, now)
                .await,
            Err(prolly_dynamodb_core::Error::TableIncarnationChanged { .. })
        ));
        assert!(database
            .get_item("Evidence", &candidate.key)
            .await
            .unwrap()
            .is_some());
        assert!(matches!(
            database
                .commits_for_incarnation("Evidence", &table_id, None, 10)
                .await,
            Err(prolly_dynamodb_core::Error::TableIncarnationChanged { .. })
        ));
    });
}

#[test]
fn gc_execution_pins_the_lease_and_is_durably_replayable() {
    block_on(async {
        let clock = Arc::new(AdjustableClock::new(1_000));
        let database = Database::new(
            SyncStoreAsAsync::new(Arc::new(MemStore::new())),
            Config::default(),
        )
        .with_sources(Arc::new(SequenceIds::default()), clock.clone());
        let lease = database
            .acquire_maintenance_lease(
                MaintenanceContext::new("gc-worker", "bounded physical sweep")
                    .change_ticket("OPS-43"),
                60_000,
            )
            .await
            .unwrap();
        let context =
            MaintenanceContext::new("gc-worker", "apply verified GC page").change_ticket("OPS-43");
        let started = database
            .begin_gc_execution([21; 32], &lease.id, [22; 32], 4, 3, context.clone())
            .await
            .unwrap();
        assert!(!started.replayed);
        assert_eq!(started.record.started_at_millis, 1_000);
        assert!(started.record.completed_at_millis.is_none());

        let replay = database
            .begin_gc_execution([21; 32], &lease.id, [22; 32], 4, 3, context.clone())
            .await
            .unwrap();
        assert!(replay.replayed);
        assert!(matches!(
            database
                .begin_gc_execution([21; 32], &lease.id, [22; 32], 5, 3, context.clone())
                .await,
            Err(prolly_dynamodb_core::Error::IdempotentParameterMismatch)
        ));
        assert!(database
            .begin_gc_execution([23; 32], &lease.id, [22; 32], 1, 0, context.clone())
            .await
            .is_err());

        // The execution pin survives lease expiry and blocks both normal release
        // and force-break until physical deletion has been acknowledged complete.
        clock.set(lease.expires_at_millis + 1);
        let release_context = MaintenanceContext::new("gc-worker", "sweep complete");
        assert!(database
            .release_maintenance_lease(&lease.id, release_context.clone())
            .await
            .is_err());
        assert!(database
            .break_expired_maintenance_lease(&lease.id, release_context.clone())
            .await
            .is_err());

        let completed = database
            .complete_gc_execution(&[21; 32], &lease.id)
            .await
            .unwrap();
        assert!(!completed.replayed);
        assert_eq!(completed.record.completed_at_millis, Some(61_001));
        let completed_replay = database
            .complete_gc_execution(&[21; 32], &lease.id)
            .await
            .unwrap();
        assert!(completed_replay.replayed);
        assert_eq!(completed_replay.record, completed.record);

        database
            .break_expired_maintenance_lease(&lease.id, release_context)
            .await
            .unwrap();
    });
}

#[test]
fn gc_execution_reconciles_ambiguous_start_and_completion() {
    block_on(async {
        let store = Arc::new(CommitFaultStore::default());
        let database = Database::new(SyncStoreAsAsync::new(store.clone()), Config::default())
            .with_sources(Arc::new(SequenceIds::default()), Arc::new(FixedClock));
        let lease = database
            .acquire_maintenance_lease(
                MaintenanceContext::new("gc-worker", "ambiguity drill"),
                60_000,
            )
            .await
            .unwrap();
        let context = MaintenanceContext::new("gc-worker", "apply reviewed page");

        store.report_ambiguous_after_next_commit();
        let started = database
            .begin_gc_execution([31; 32], &lease.id, [32; 32], 1, 1, context)
            .await
            .unwrap();
        assert!(started.replayed);
        assert_eq!(
            started.record.state,
            prolly_dynamodb_core::GcExecutionState::InProgress
        );

        store.report_ambiguous_after_next_commit();
        let completed = database
            .complete_gc_execution(&[31; 32], &lease.id)
            .await
            .unwrap();
        assert!(completed.replayed);
        assert_eq!(
            completed.record.state,
            prolly_dynamodb_core::GcExecutionState::Complete
        );
    });
}

#[test]
fn lease_acquisition_invalidates_an_in_flight_writer_before_root_publication() {
    let store = Arc::new(MemStore::new());
    let blobs = Arc::new(BlockingBlobs::default());
    let database = Arc::new(
        Database::new_with_blob_storage(
            SyncStoreAsAsync::new(store),
            Config::default(),
            blobs.clone(),
            LargeValueConfig::new(128),
        )
        .unwrap()
        .with_sources(Arc::new(SequenceIds::default()), Arc::new(FixedClock)),
    );
    block_on(database.create_table(
        "Evidence",
        KeyAttribute {
            name: "case_id".into(),
            kind: KeyKind::String,
        },
        None,
    ))
    .unwrap();
    let before = block_on(database.head("Evidence")).unwrap().id;

    let writer_database = database.clone();
    let writer = std::thread::spawn(move || {
        block_on(writer_database.put_item(
            "Evidence",
            Item::from([
                ("case_id".into(), AttributeValue::S("case-race".into())),
                ("payload".into(), AttributeValue::S("x".repeat(4096))),
            ]),
            None,
        ))
    });
    blobs.wait_until_put_entered();

    let lease = block_on(database.acquire_maintenance_lease(
        MaintenanceContext::new("gc-worker", "race-proof sweep"),
        60_000,
    ))
    .unwrap();
    blobs.allow_put();
    assert!(matches!(
        writer.join().unwrap(),
        Err(prolly_dynamodb_core::Error::MaintenanceInProgress { .. })
    ));
    assert_eq!(block_on(database.head("Evidence")).unwrap().id, before);
    assert!(block_on(database.get_item(
        "Evidence",
        &Item::from([("case_id".into(), AttributeValue::S("case-race".into()))]),
    ))
    .unwrap()
    .is_none());
    block_on(database.release_maintenance_lease(
        &lease.id,
        MaintenanceContext::new("gc-worker", "race proof complete"),
    ))
    .unwrap();
}

#[test]
fn table_crud_history_and_recreation_are_incarnation_safe() {
    block_on(async {
        let store = SyncStoreAsAsync::new(Arc::new(MemStore::new()));
        let database = Database::new(store, Config::default())
            .with_sources(Arc::new(SequenceIds::default()), Arc::new(FixedClock));
        let first_table = database
            .create_table(
                "Orders",
                KeyAttribute {
                    name: "account".into(),
                    kind: KeyKind::String,
                },
                Some(KeyAttribute {
                    name: "sequence".into(),
                    kind: KeyKind::Number,
                }),
            )
            .await
            .unwrap();
        assert_eq!(database.list_tables().await.unwrap(), vec!["Orders"]);

        let first = database
            .put_item("Orders", item("acct-1", "1", "OPEN"), None)
            .await
            .unwrap();
        let first_version = match first {
            VersionedMapUpdate::Applied { current, .. } => current.id,
            other => panic!("unexpected update: {other:?}"),
        };
        let second = database
            .put_item("Orders", item("acct-1", "1", "CLOSED"), None)
            .await
            .unwrap();
        let second_version = match second {
            VersionedMapUpdate::Applied { current, .. } => current.id,
            other => panic!("unexpected update: {other:?}"),
        };
        assert_eq!(
            database
                .get_item("Orders", &key("acct-1", "1"))
                .await
                .unwrap()
                .unwrap()
                .get("status"),
            Some(&AttributeValue::S("CLOSED".into()))
        );
        assert_eq!(
            database
                .get_item_at("Orders", &first_version, &key("acct-1", "1"))
                .await
                .unwrap()
                .unwrap()
                .get("status"),
            Some(&AttributeValue::S("OPEN".into()))
        );
        assert_eq!(
            database
                .diff("Orders", &first_version, &second_version)
                .await
                .unwrap()
                .len(),
            1
        );

        let deleted = database.delete_table_result("Orders").await.unwrap();
        let deletion_commit = database.commit(&deleted.commit_id).await.unwrap().unwrap();
        assert_eq!(deletion_commit.transitions.len(), 1);
        assert!(deletion_commit.transitions[0].before.is_some());
        assert_eq!(deletion_commit.transitions[0].after, None);
        assert!(deletion_commit.transitions[0].applied);
        let second_table = database
            .create_table(
                "Orders",
                KeyAttribute {
                    name: "account".into(),
                    kind: KeyKind::String,
                },
                Some(KeyAttribute {
                    name: "sequence".into(),
                    kind: KeyKind::Number,
                }),
            )
            .await
            .unwrap();
        assert_ne!(first_table.id, second_table.id);
        assert_eq!(
            database
                .get_item("Orders", &key("acct-1", "1"))
                .await
                .unwrap(),
            None
        );
        assert!(database
            .get_item_at("Orders", &first_version, &key("acct-1", "1"))
            .await
            .is_err());
    });
}

#[test]
fn secondary_indexes_are_atomic_sparse_projected_and_historically_paired() {
    block_on(async {
        let store = Arc::new(CommitFaultStore::default());
        let database = Database::new(SyncStoreAsAsync::new(store.clone()), Config::default())
            .with_sources(Arc::new(SequenceIds::default()), Arc::new(FixedClock));
        database
            .create_table_with_indexes_result(
                "Orders",
                KeyAttribute {
                    name: "account".into(),
                    kind: KeyKind::String,
                },
                Some(KeyAttribute {
                    name: "sequence".into(),
                    kind: KeyKind::Number,
                }),
                BTreeMap::from([
                    ("account".into(), KeyKind::String),
                    ("sequence".into(), KeyKind::Number),
                    ("status".into(), KeyKind::String),
                    ("opened_at".into(), KeyKind::Number),
                ]),
                vec![SecondaryIndexDefinition {
                    name: "ByStatus".into(),
                    kind: SecondaryIndexKind::Global,
                    partition_key: KeyAttribute {
                        name: "status".into(),
                        kind: KeyKind::String,
                    },
                    sort_key: Some(KeyAttribute {
                        name: "opened_at".into(),
                        kind: KeyKind::Number,
                    }),
                    projection: SecondaryIndexProjection::Include(BTreeSet::from(["owner".into()])),
                }],
                None,
            )
            .await
            .unwrap();

        let order = |account: &str, sequence: &str, status: Option<&str>, owner: &str| {
            let mut item = Item::from([
                ("account".into(), AttributeValue::S(account.into())),
                (
                    "sequence".into(),
                    AttributeValue::N(DynamoNumber::parse(sequence).unwrap()),
                ),
                (
                    "opened_at".into(),
                    AttributeValue::N(DynamoNumber::parse(sequence).unwrap()),
                ),
                ("owner".into(), AttributeValue::S(owner.into())),
                ("secret".into(), AttributeValue::S("redact".into())),
            ]);
            if let Some(status) = status {
                item.insert("status".into(), AttributeValue::S(status.into()));
            }
            item
        };
        let first = database
            .put_item("Orders", order("a-1", "1", Some("OPEN"), "legal-v1"), None)
            .await
            .unwrap();
        let first_version = match first {
            VersionedMapUpdate::Applied { current, .. } => current.id,
            other => panic!("unexpected write: {other:?}"),
        };
        database
            .put_item("Orders", order("a-2", "2", Some("OPEN"), "legal-v2"), None)
            .await
            .unwrap();
        database
            .put_item("Orders", order("a-3", "3", None, "sparse"), None)
            .await
            .unwrap();

        let condition = KeyCondition {
            partition_name: "status".into(),
            partition_value: AttributeValue::S("OPEN".into()),
            sort: None,
        };
        let page_one = database
            .query_index("Orders", "ByStatus", IndexQueryRequest::new(&condition, 1))
            .await
            .unwrap();
        assert_eq!(page_one.items.len(), 1);
        assert!(!page_one.items[0].contains_key("secret"));
        for name in ["account", "sequence", "status", "opened_at", "owner"] {
            assert!(page_one.items[0].contains_key(name));
        }
        let page_two = database
            .query_index(
                "Orders",
                "ByStatus",
                IndexQueryRequest::new(&condition, 1).after(page_one.last_evaluated_key.as_ref()),
            )
            .await
            .unwrap();
        assert_eq!(page_two.items.len(), 1);
        assert!(page_two.last_evaluated_key.is_none());
        let scan_one = database
            .scan_index("Orders", "ByStatus", None, None, 1)
            .await
            .unwrap();
        assert_eq!(scan_one.items.len(), 1);
        let scan_two = database
            .scan_index(
                "Orders",
                "ByStatus",
                None,
                scan_one.last_evaluated_key.as_ref(),
                10,
            )
            .await
            .unwrap();
        assert_eq!(scan_two.items.len(), 1);
        assert!(scan_two.last_evaluated_key.is_none());

        let historical = database
            .query_index(
                "Orders",
                "ByStatus",
                IndexQueryRequest::new(&condition, 10).at(Some(&first_version)),
            )
            .await
            .unwrap();
        assert_eq!(historical.base_version_id, first_version);
        assert_eq!(historical.items.len(), 1);
        assert_eq!(
            historical.items[0]["owner"],
            AttributeValue::S("legal-v1".into())
        );
        let newest_version = database.head("Orders").await.unwrap().id;
        assert!(matches!(
            database
                .restore("Orders", &newest_version, &first_version)
                .await
                .unwrap(),
            VersionedMapUpdate::Applied { .. }
        ));
        let restored = database
            .query_index("Orders", "ByStatus", IndexQueryRequest::new(&condition, 10))
            .await
            .unwrap();
        assert_eq!(restored.base_version_id, first_version);
        assert_eq!(restored.items.len(), 1);
        assert!(matches!(
            database
                .restore("Orders", &first_version, &newest_version)
                .await
                .unwrap(),
            VersionedMapUpdate::Applied { .. }
        ));
        assert_eq!(
            database
                .query_index("Orders", "ByStatus", IndexQueryRequest::new(&condition, 10))
                .await
                .unwrap()
                .items
                .len(),
            2
        );
        assert!(database
            .verify_indexes("Orders")
            .await
            .unwrap()
            .iter()
            .all(prolly::IndexVerification::is_valid));

        let archive_limits = TableArchiveLimits::new(
            10_000,
            16 * 1024 * 1024,
            1_000,
            16 * 1024 * 1024,
            32 * 1024 * 1024,
        );
        let archive = database
            .export_table("Orders", None, archive_limits)
            .await
            .unwrap();
        let import_plan = database
            .plan_import(&archive, "OrdersCopy", archive_limits)
            .await
            .unwrap();
        database
            .apply_import(
                &archive,
                &import_plan,
                MaintenanceContext::new("index-test", "indexed import verification"),
                archive_limits,
            )
            .await
            .unwrap();
        let imported = database
            .query_index(
                "OrdersCopy",
                "ByStatus",
                IndexQueryRequest::new(&condition, 10),
            )
            .await
            .unwrap();
        assert_eq!(imported.items.len(), 2);
        assert!(database
            .verify_indexes("OrdersCopy")
            .await
            .unwrap()
            .iter()
            .all(prolly::IndexVerification::is_valid));

        let retention = database
            .plan_retention("Orders", RetentionPolicy::keep_last(0))
            .await
            .unwrap();
        assert!(retention.remove.contains(&first_version));
        database
            .apply_retention(
                &retention,
                MaintenanceContext::new("index-test", "paired index retention"),
            )
            .await
            .unwrap();
        assert!(database
            .query_index(
                "Orders",
                "ByStatus",
                IndexQueryRequest::new(&condition, 10).at(Some(&first_version)),
            )
            .await
            .is_err());
        assert!(database
            .verify_indexes("Orders")
            .await
            .unwrap()
            .iter()
            .all(prolly::IndexVerification::is_valid));

        store.fail_commit_number_from_now(1);
        assert!(database
            .put_item(
                "Orders",
                order("a-4", "4", Some("FAILED"), "must-not-show"),
                None
            )
            .await
            .is_err());
        let failed = database
            .query_index(
                "Orders",
                "ByStatus",
                IndexQueryRequest::new(
                    &KeyCondition {
                        partition_name: "status".into(),
                        partition_value: AttributeValue::S("FAILED".into()),
                        sort: None,
                    },
                    10,
                ),
            )
            .await
            .unwrap();
        assert!(failed.items.is_empty());
        assert!(database
            .get_item(
                "Orders",
                &Item::from([
                    ("account".into(), AttributeValue::S("a-4".into())),
                    (
                        "sequence".into(),
                        AttributeValue::N(DynamoNumber::parse("4").unwrap()),
                    ),
                ]),
            )
            .await
            .unwrap()
            .is_none());
    });
}

#[test]
fn indexed_history_exceeds_the_legacy_snapshot_limit_with_a_bounded_coordinator() {
    block_on(async {
        let backing = Arc::new(MemStore::new());
        let database = Database::new(SyncStoreAsAsync::new(backing.clone()), Config::default())
            .with_sources(Arc::new(WideSequenceIds::default()), Arc::new(FixedClock));
        database
            .create_table_with_indexes_result(
                "DeepHistory",
                KeyAttribute {
                    name: "id".into(),
                    kind: KeyKind::String,
                },
                None,
                BTreeMap::from([
                    ("id".into(), KeyKind::String),
                    ("status".into(), KeyKind::String),
                ]),
                vec![SecondaryIndexDefinition {
                    name: "ByStatus".into(),
                    kind: SecondaryIndexKind::Global,
                    partition_key: KeyAttribute {
                        name: "status".into(),
                        kind: KeyKind::String,
                    },
                    sort_key: None,
                    projection: SecondaryIndexProjection::All,
                }],
                None,
            )
            .await
            .unwrap();

        let mut sampled = Vec::new();
        for revision in 0..1_100usize {
            let update = database
                .put_item(
                    "DeepHistory",
                    Item::from([
                        ("id".into(), AttributeValue::S("record".into())),
                        ("status".into(), AttributeValue::S("OPEN".into())),
                        (
                            "revision".into(),
                            AttributeValue::N(DynamoNumber::parse(&revision.to_string()).unwrap()),
                        ),
                    ]),
                    None,
                )
                .await
                .unwrap();
            if matches!(revision, 0 | 1_024 | 1_099) {
                sampled.push(match update {
                    VersionedMapUpdate::Applied { current, .. } => (revision, current.id),
                    other => panic!("unexpected deep-history update: {other:?}"),
                });
            }
        }

        assert_eq!(database.versions("DeepHistory").await.unwrap().len(), 1_101);
        let roots = backing.list_roots().unwrap();
        let version_roots = roots
            .iter()
            .filter(|root| {
                root.name
                    .windows(b"/versions/".len())
                    .any(|window| window == b"/versions/")
            })
            .count();
        assert_eq!(roots.len(), 1_110);
        assert_eq!(version_roots, 1_103);
        let legacy_commit_versions = database
            .engine()
            .versioned_map(b"dynamodb/commits/v1")
            .versions_prefix()
            .to_vec();
        let table_id = database.describe_table("DeepHistory").await.unwrap().id;
        let mut legacy_table_log_id = b"dynamodb/table-commits/v1/".to_vec();
        legacy_table_log_id.extend_from_slice(&table_id.0);
        let legacy_table_log_versions = database
            .engine()
            .versioned_map(legacy_table_log_id)
            .versions_prefix()
            .to_vec();
        assert!(!roots
            .iter()
            .any(|root| root.name.starts_with(&legacy_commit_versions)));
        assert!(!roots
            .iter()
            .any(|root| root.name.starts_with(&legacy_table_log_versions)));
        assert_eq!(
            database
                .index_health("DeepHistory")
                .await
                .unwrap()
                .retained_snapshots,
            1
        );
        let condition = KeyCondition {
            partition_name: "status".into(),
            partition_value: AttributeValue::S("OPEN".into()),
            sort: None,
        };
        for (revision, version) in sampled {
            let page = database
                .query_index(
                    "DeepHistory",
                    "ByStatus",
                    IndexQueryRequest::new(&condition, 10).at(Some(&version)),
                )
                .await
                .unwrap();
            assert_eq!(page.base_version_id, version);
            assert_eq!(page.items.len(), 1);
            assert_eq!(
                page.items[0]["revision"],
                AttributeValue::N(DynamoNumber::parse(&revision.to_string()).unwrap())
            );
        }
    });
}

#[test]
fn index_reconfiguration_is_shadow_built_atomic_versioned_and_restore_safe() {
    block_on(async {
        let store = Arc::new(CommitFaultStore::default());
        let database = Database::new(SyncStoreAsAsync::new(store.clone()), Config::default())
            .with_sources(Arc::new(SequenceIds::default()), Arc::new(FixedClock));
        let original = SecondaryIndexDefinition {
            name: "ByStatus".into(),
            kind: SecondaryIndexKind::Global,
            partition_key: KeyAttribute {
                name: "status".into(),
                kind: KeyKind::String,
            },
            sort_key: Some(KeyAttribute {
                name: "opened_at".into(),
                kind: KeyKind::Number,
            }),
            projection: SecondaryIndexProjection::All,
        };
        database
            .create_table_with_indexes_result(
                "ReconfigOrders",
                KeyAttribute {
                    name: "account".into(),
                    kind: KeyKind::String,
                },
                Some(KeyAttribute {
                    name: "sequence".into(),
                    kind: KeyKind::Number,
                }),
                BTreeMap::from([
                    ("account".into(), KeyKind::String),
                    ("sequence".into(), KeyKind::Number),
                    ("status".into(), KeyKind::String),
                    ("opened_at".into(), KeyKind::Number),
                ]),
                vec![original.clone()],
                None,
            )
            .await
            .unwrap();
        let item = Item::from([
            ("account".into(), AttributeValue::S("acct-1".into())),
            (
                "sequence".into(),
                AttributeValue::N(DynamoNumber::parse("1").unwrap()),
            ),
            ("status".into(), AttributeValue::S("OPEN".into())),
            (
                "opened_at".into(),
                AttributeValue::N(DynamoNumber::parse("1").unwrap()),
            ),
            ("owner".into(), AttributeValue::S("counsel-a".into())),
        ]);
        let old_version = database
            .put_item("ReconfigOrders", item, None)
            .await
            .unwrap()
            .current()
            .unwrap()
            .id
            .clone();
        let before = database.describe_table("ReconfigOrders").await.unwrap();
        let old_index = before.secondary_indexes[0].clone();

        let replacement = SecondaryIndexDefinition {
            name: "ByStatus".into(),
            kind: SecondaryIndexKind::Global,
            partition_key: KeyAttribute {
                name: "owner".into(),
                kind: KeyKind::String,
            },
            sort_key: Some(KeyAttribute {
                name: "opened_at".into(),
                kind: KeyKind::Number,
            }),
            projection: SecondaryIndexProjection::KeysOnly,
        };
        let additional = SecondaryIndexDefinition {
            name: "ByOpenedAt".into(),
            kind: SecondaryIndexKind::Local,
            partition_key: KeyAttribute {
                name: "account".into(),
                kind: KeyKind::String,
            },
            sort_key: Some(KeyAttribute {
                name: "opened_at".into(),
                kind: KeyKind::Number,
            }),
            projection: SecondaryIndexProjection::All,
        };
        let plan = database
            .plan_index_reconfiguration(
                "ReconfigOrders",
                vec![replacement.clone(), additional.clone()],
            )
            .await
            .unwrap();
        assert_eq!(
            database.describe_table("ReconfigOrders").await.unwrap(),
            before
        );
        assert_eq!(plan.after.secondary_indexes[1].name, "ByStatus");
        let replacement_description = plan
            .after
            .secondary_indexes
            .iter()
            .find(|index| index.name == "ByStatus")
            .unwrap();
        assert_eq!(replacement_description.generation, old_index.generation + 1);
        assert_ne!(replacement_description.id, old_index.id);

        let context =
            MaintenanceContext::new("index-admin", "approved schema change").change_ticket("DB-42");
        let result = database
            .apply_index_reconfiguration(&plan, context.clone())
            .await
            .unwrap();
        assert_ne!(result.version, old_version);
        assert!(!result.replayed);
        assert_eq!(result.description, plan.after);
        assert_eq!(
            database
                .index_reconfiguration_audit(&plan.id)
                .await
                .unwrap()
                .unwrap()
                .result,
            result
        );
        assert!(
            database
                .apply_index_reconfiguration(&plan, context.clone())
                .await
                .unwrap()
                .replayed
        );

        let by_owner = KeyCondition {
            partition_name: "owner".into(),
            partition_value: AttributeValue::S("counsel-a".into()),
            sort: None,
        };
        assert_eq!(
            database
                .query_index(
                    "ReconfigOrders",
                    "ByStatus",
                    IndexQueryRequest::new(&by_owner, 10),
                )
                .await
                .unwrap()
                .items
                .len(),
            1
        );
        let by_status = KeyCondition {
            partition_name: "status".into(),
            partition_value: AttributeValue::S("OPEN".into()),
            sort: None,
        };
        let historical = database
            .query_index(
                "ReconfigOrders",
                "ByStatus",
                IndexQueryRequest::new(&by_status, 10).at(Some(&old_version)),
            )
            .await
            .unwrap();
        assert_eq!(historical.items.len(), 1);
        assert_ne!(historical.indexed_snapshot_id, result.indexed_snapshot_id);
        assert!(database
            .query_index(
                "ReconfigOrders",
                "ByStatus",
                IndexQueryRequest::new(&by_status, 10),
            )
            .await
            .is_err());

        let historical_archive = database
            .export_table(
                "ReconfigOrders",
                Some(&old_version),
                TableArchiveLimits::new(
                    10_000,
                    16 * 1024 * 1024,
                    1_000,
                    16 * 1024 * 1024,
                    32 * 1024 * 1024,
                ),
            )
            .await
            .unwrap();
        assert_eq!(historical_archive.source.secondary_indexes, vec![old_index]);

        assert!(database
            .restore("ReconfigOrders", &result.version, &old_version)
            .await
            .unwrap()
            .is_applied());
        assert_eq!(
            database
                .describe_table("ReconfigOrders")
                .await
                .unwrap()
                .secondary_indexes[0]
                .generation,
            1
        );
        assert_eq!(
            database
                .query_index(
                    "ReconfigOrders",
                    "ByStatus",
                    IndexQueryRequest::new(&by_status, 10),
                )
                .await
                .unwrap()
                .items
                .len(),
            1
        );
        assert!(database
            .restore("ReconfigOrders", &old_version, &result.version)
            .await
            .unwrap()
            .is_applied());
        assert!(database
            .verify_indexes("ReconfigOrders")
            .await
            .unwrap()
            .iter()
            .all(prolly::IndexVerification::is_valid));

        let removal = database
            .plan_index_reconfiguration("ReconfigOrders", Vec::new())
            .await
            .unwrap();
        let head_before_failed_apply = database.head("ReconfigOrders").await.unwrap().id;
        let schema_before_failed_apply = database.describe_table("ReconfigOrders").await.unwrap();
        store.fail_commit_number_from_now(1);
        assert!(database
            .apply_index_reconfiguration(
                &removal,
                MaintenanceContext::new("index-admin", "failure atomicity test"),
            )
            .await
            .is_err());
        assert_eq!(
            database.head("ReconfigOrders").await.unwrap().id,
            head_before_failed_apply
        );
        assert_eq!(
            database.describe_table("ReconfigOrders").await.unwrap(),
            schema_before_failed_apply
        );
        assert_eq!(
            database
                .query_index(
                    "ReconfigOrders",
                    "ByStatus",
                    IndexQueryRequest::new(&by_owner, 10),
                )
                .await
                .unwrap()
                .items
                .len(),
            1
        );

        // Empty source content has the same source-tree hash before and after
        // a generation replacement. Exact indexed snapshot pairing must still
        // distinguish and retain both schemas.
        database
            .create_table_with_indexes_result(
                "EmptyOrders",
                KeyAttribute {
                    name: "account".into(),
                    kind: KeyKind::String,
                },
                Some(KeyAttribute {
                    name: "sequence".into(),
                    kind: KeyKind::Number,
                }),
                BTreeMap::from([
                    ("account".into(), KeyKind::String),
                    ("sequence".into(), KeyKind::Number),
                    ("status".into(), KeyKind::String),
                    ("opened_at".into(), KeyKind::Number),
                ]),
                vec![original],
                None,
            )
            .await
            .unwrap();
        let empty_old_version = database.head("EmptyOrders").await.unwrap().id;
        let empty_before = database
            .query_index(
                "EmptyOrders",
                "ByStatus",
                IndexQueryRequest::new(&by_status, 10),
            )
            .await
            .unwrap();
        let empty_plan = database
            .plan_index_reconfiguration("EmptyOrders", vec![replacement])
            .await
            .unwrap();
        let empty_result = database
            .apply_index_reconfiguration(
                &empty_plan,
                MaintenanceContext::new("index-admin", "empty-table generation test"),
            )
            .await
            .unwrap();
        let empty_after = database
            .query_index(
                "EmptyOrders",
                "ByStatus",
                IndexQueryRequest::new(&by_owner, 10),
            )
            .await
            .unwrap();
        assert_eq!(
            empty_before.indexed_source_version_id,
            empty_after.indexed_source_version_id
        );
        assert_ne!(
            empty_before.indexed_snapshot_id,
            empty_after.indexed_snapshot_id
        );
        assert_eq!(
            empty_after.indexed_snapshot_id,
            empty_result.indexed_snapshot_id
        );
        let empty_historical = database
            .query_index(
                "EmptyOrders",
                "ByStatus",
                IndexQueryRequest::new(&by_status, 10).at(Some(&empty_old_version)),
            )
            .await
            .unwrap();
        assert_eq!(
            empty_historical.indexed_snapshot_id,
            empty_before.indexed_snapshot_id
        );

        let stale = database
            .plan_index_reconfiguration("EmptyOrders", Vec::new())
            .await
            .unwrap();
        database
            .put_item(
                "EmptyOrders",
                Item::from([
                    ("account".into(), AttributeValue::S("acct-2".into())),
                    (
                        "sequence".into(),
                        AttributeValue::N(DynamoNumber::parse("2").unwrap()),
                    ),
                    ("owner".into(), AttributeValue::S("counsel-b".into())),
                    (
                        "opened_at".into(),
                        AttributeValue::N(DynamoNumber::parse("2").unwrap()),
                    ),
                ]),
                None,
            )
            .await
            .unwrap();
        assert!(matches!(
            database
                .apply_index_reconfiguration(
                    &stale,
                    MaintenanceContext::new("index-admin", "stale-plan test"),
                )
                .await,
            Err(prolly_dynamodb_core::Error::MaintenancePlanStale(_))
        ));

        let removal = database
            .plan_index_reconfiguration("EmptyOrders", Vec::new())
            .await
            .unwrap();
        let removal_context =
            MaintenanceContext::new("index-admin", "ambiguous activation reconciliation test");
        store.report_ambiguous_after_next_commit();
        let removed = database
            .apply_index_reconfiguration(&removal, removal_context.clone())
            .await
            .unwrap();
        assert!(removed.replayed);
        assert!(database
            .describe_table("EmptyOrders")
            .await
            .unwrap()
            .secondary_indexes
            .is_empty());
        assert_eq!(
            database
                .index_reconfiguration_audit(&removal.id)
                .await
                .unwrap()
                .unwrap()
                .context,
            removal_context
        );
    });
}

#[test]
fn structural_diff_pages_are_bounded_resumable_and_version_bound() {
    block_on(async {
        let store = SyncStoreAsAsync::new(Arc::new(MemStore::new()));
        let database = Database::new(store, Config::default())
            .with_sources(Arc::new(SequenceIds::default()), Arc::new(FixedClock));
        database
            .create_table(
                "Orders",
                KeyAttribute {
                    name: "account".into(),
                    kind: KeyKind::String,
                },
                Some(KeyAttribute {
                    name: "sequence".into(),
                    kind: KeyKind::Number,
                }),
            )
            .await
            .unwrap();

        let first = database
            .put_item("Orders", item("acct-1", "1", "OPEN"), None)
            .await
            .unwrap()
            .current()
            .unwrap()
            .id
            .clone();
        let second = database
            .put_item("Orders", item("acct-1", "2", "OPEN"), None)
            .await
            .unwrap()
            .current()
            .unwrap()
            .id
            .clone();
        let third = database
            .put_item("Orders", item("acct-1", "3", "OPEN"), None)
            .await
            .unwrap()
            .current()
            .unwrap()
            .id
            .clone();

        let first_page = database
            .structural_diff_page("Orders", &first, &third, None, 1)
            .await
            .unwrap();
        assert_eq!(first_page.diffs.len(), 1);
        let checkpoint = first_page.next_cursor.expect("another diff page");

        // The checkpoint is stable application state, not an in-memory handle.
        let checkpoint = serde_cbor::from_slice(
            &serde_cbor::to_vec(&checkpoint).expect("encode structural cursor"),
        )
        .expect("decode structural cursor");
        let second_page = database
            .structural_diff_page("Orders", &first, &third, Some(&checkpoint), 1)
            .await
            .unwrap();
        assert_eq!(second_page.diffs.len(), 1);
        assert!(second_page.next_cursor.is_none());

        let mismatched = database
            .structural_diff_page("Orders", &first, &second, Some(&checkpoint), 1)
            .await;
        assert!(
            mismatched.is_err(),
            "cursor must be bound to immutable roots"
        );
        assert!(database
            .structural_diff_page("Orders", &first, &third, None, 0)
            .await
            .is_err());
        assert!(database
            .structural_diff_page(
                "Orders",
                &first,
                &third,
                None,
                prolly_dynamodb_core::MAX_DIFF_PAGE_ITEMS + 1,
            )
            .await
            .is_err());

        let versions = database.versions_page("Orders", None, 1).await.unwrap();
        assert_eq!(versions.versions.len(), 1);
        let version_cursor = versions.next_cursor.expect("more table versions");
        let version_cursor = serde_cbor::from_slice(
            &serde_cbor::to_vec(&version_cursor).expect("encode version cursor"),
        )
        .expect("decode version cursor");
        let resumed = database
            .versions_page("Orders", Some(&version_cursor), 1)
            .await
            .unwrap();
        assert_eq!(resumed.versions.len(), 1);
        assert!(database.versions_page("Orders", None, 0).await.is_err());
        assert!(database
            .versions_page(
                "Orders",
                None,
                prolly_dynamodb_core::MAX_VERSION_PAGE_ITEMS + 1,
            )
            .await
            .is_err());
    });
}

#[test]
fn retention_is_dry_run_bounded_atomic_audited_and_stale_safe() {
    block_on(async {
        let backing = Arc::new(MemStore::new());
        let store = SyncStoreAsAsync::new(backing.clone());
        let clock = Arc::new(AdjustableClock::new(100));
        let database = Database::new(store, Config::default())
            .with_sources(Arc::new(SequenceIds::default()), clock.clone());
        database
            .create_table(
                "Evidence",
                KeyAttribute {
                    name: "account".into(),
                    kind: KeyKind::String,
                },
                Some(KeyAttribute {
                    name: "sequence".into(),
                    kind: KeyKind::Number,
                }),
            )
            .await
            .unwrap();
        let initial = database.head("Evidence").await.unwrap().id;

        clock.set(200);
        let protected = database
            .put_item("Evidence", item("acct-1", "1", "OPEN"), None)
            .await
            .unwrap()
            .current()
            .unwrap()
            .id
            .clone();
        clock.set(300);
        let removable = database
            .put_item("Evidence", item("acct-1", "1", "REVIEW"), None)
            .await
            .unwrap()
            .current()
            .unwrap()
            .id
            .clone();
        clock.set(400);
        let head = database
            .put_item("Evidence", item("acct-1", "1", "CLOSED"), None)
            .await
            .unwrap()
            .current()
            .unwrap()
            .id
            .clone();

        let plan = database
            .plan_retention(
                "Evidence",
                RetentionPolicy::keep_last(1).protect(protected.clone()),
            )
            .await
            .unwrap();
        assert_eq!(plan.expected_head, head);
        assert_eq!(plan.examined_versions, 4);
        assert_eq!(plan.remove.len(), 2);
        assert!(plan.remove.contains(&initial));
        assert!(plan.remove.contains(&removable));
        assert!(!plan.more_removable);

        // Planning is a pure read: every candidate remains addressable.
        assert!(database
            .get_item_at("Evidence", &removable, &key("acct-1", "1"))
            .await
            .unwrap()
            .is_some());

        let context = MaintenanceContext::new("records-officer", "approved retention schedule")
            .change_ticket("LEGAL-2026-0042");
        let catalog_before = backing
            .list_roots()
            .unwrap()
            .into_iter()
            .find(|root| {
                root.name
                    .starts_with(b"\0dynamodb/table-snapshot-catalog/v1/")
            })
            .expect("table snapshot catalog before retention");
        let protection_before = database
            .expand_snapshot_catalog_root(
                &catalog_before.name,
                &catalog_before.manifest.to_tree(),
                100,
            )
            .await
            .unwrap()
            .expect("snapshot catalog protection before retention");
        assert_eq!(protection_before.protected_trees.len(), 8);
        let applied = database
            .apply_retention(&plan, context.clone())
            .await
            .unwrap();
        assert!(!applied.replayed);
        assert_eq!(applied.removed, plan.remove);
        assert!(database
            .get_item_at("Evidence", &removable, &key("acct-1", "1"))
            .await
            .is_err());
        assert!(database
            .get_item_at("Evidence", &protected, &key("acct-1", "1"))
            .await
            .unwrap()
            .is_some());
        assert_eq!(database.head("Evidence").await.unwrap().id, head);

        let catalog_after = backing
            .list_roots()
            .unwrap()
            .into_iter()
            .find(|root| root.name == catalog_before.name)
            .expect("table snapshot catalog after retention");
        let protection_after = database
            .expand_snapshot_catalog_root(
                &catalog_after.name,
                &catalog_after.manifest.to_tree(),
                100,
            )
            .await
            .unwrap()
            .expect("snapshot catalog protection after retention");
        assert_eq!(protection_after.protected_trees.len(), 4);
        assert!(protection_after
            .protected_trees
            .iter()
            .all(|tree| protection_before.protected_trees.contains(tree)));
        assert_eq!(
            protection_before
                .protected_trees
                .iter()
                .filter(|tree| !protection_after.protected_trees.contains(tree))
                .count(),
            4,
            "retention must make removed snapshot and detached-manifest trees collectible"
        );

        let audit = database
            .retention_audit(&plan.id)
            .await
            .unwrap()
            .expect("durable retention audit");
        assert_eq!(audit.plan, plan);
        assert_eq!(audit.context, context);
        let replay = database
            .apply_retention(&audit.plan, audit.context.clone())
            .await
            .unwrap();
        assert!(replay.replayed);
        assert!(matches!(
            database
                .apply_retention(
                    &audit.plan,
                    MaintenanceContext::new("another-actor", "different attribution"),
                )
                .await,
            Err(prolly_dynamodb_core::Error::IdempotentParameterMismatch)
        ));

        clock.set(500);
        let stale = database
            .plan_retention("Evidence", RetentionPolicy::keep_last(1))
            .await
            .unwrap();
        clock.set(600);
        database
            .put_item("Evidence", item("acct-1", "2", "OPEN"), None)
            .await
            .unwrap();
        assert!(matches!(
            database
                .apply_retention(
                    &stale,
                    MaintenanceContext::new("records-officer", "stale plan must fail"),
                )
                .await,
            Err(prolly_dynamodb_core::Error::MaintenancePlanStale(_))
        ));

        let mut tampered = stale;
        tampered.remove.push(tampered.expected_head.clone());
        assert!(matches!(
            database
                .apply_retention(
                    &tampered,
                    MaintenanceContext::new("records-officer", "tampered plan must fail"),
                )
                .await,
            Err(prolly_dynamodb_core::Error::Validation(_))
        ));
    });
}

#[test]
fn retention_preserves_versions_needed_by_live_idempotency_tokens() {
    block_on(async {
        let store = SyncStoreAsAsync::new(Arc::new(MemStore::new()));
        let clock = Arc::new(AdjustableClock::new(100));
        let database = Database::new(store, Config::default())
            .with_sources(Arc::new(SequenceIds::default()), clock.clone());
        database
            .create_table(
                "Ledger",
                KeyAttribute {
                    name: "account".into(),
                    kind: KeyKind::String,
                },
                None,
            )
            .await
            .unwrap();
        let initial = database.head("Ledger").await.unwrap().id;

        clock.set(200);
        let open = Item::from([
            ("account".into(), AttributeValue::S("cash".into())),
            ("status".into(), AttributeValue::S("OPEN".into())),
        ]);
        let before = database
            .put_item("Ledger", open.clone(), None)
            .await
            .unwrap()
            .current()
            .unwrap()
            .id
            .clone();

        clock.set(1_000_000);
        let closed = Item::from([
            ("account".into(), AttributeValue::S("cash".into())),
            ("status".into(), AttributeValue::S("CLOSED".into())),
        ]);
        let accepted = database
            .put_item_idempotent(
                "Ledger",
                closed.clone(),
                None,
                None,
                "retention-safe-token",
                true,
            )
            .await
            .unwrap();
        assert_eq!(accepted.old_item, Some(open));

        let plan = database
            .plan_retention("Ledger", RetentionPolicy::keep_last(1))
            .await
            .unwrap();
        assert!(plan.remove.contains(&initial));
        assert!(!plan.remove.contains(&before));
        database
            .apply_retention(
                &plan,
                MaintenanceContext::new("records-officer", "token replay safety"),
            )
            .await
            .unwrap();

        let replay = database
            .put_item_idempotent("Ledger", closed, None, None, "retention-safe-token", true)
            .await
            .unwrap();
        assert_eq!(replay.commit_id, accepted.commit_id);
        assert_eq!(replay.old_item, accepted.old_item);
    });
}

#[test]
fn retention_batches_never_exceed_the_atomic_removal_limit() {
    block_on(async {
        let store = SyncStoreAsAsync::new(Arc::new(MemStore::new()));
        let database = Database::new(store, Config::default())
            .with_sources(Arc::new(SequenceIds::default()), Arc::new(FixedClock));
        database
            .create_table(
                "Ledger",
                KeyAttribute {
                    name: "account".into(),
                    kind: KeyKind::String,
                },
                Some(KeyAttribute {
                    name: "sequence".into(),
                    kind: KeyKind::Number,
                }),
            )
            .await
            .unwrap();
        for sequence in 0..83 {
            database
                .put_item(
                    "Ledger",
                    item("acct-1", &sequence.to_string(), "POSTED"),
                    None,
                )
                .await
                .unwrap();
        }

        let first = database
            .plan_retention("Ledger", RetentionPolicy::keep_last(1))
            .await
            .unwrap();
        assert_eq!(
            first.remove.len(),
            prolly_dynamodb_core::MAX_RETENTION_REMOVALS
        );
        assert!(first.more_removable);
        database
            .apply_retention(
                &first,
                MaintenanceContext::new("ledger-admin", "bounded batch one"),
            )
            .await
            .unwrap();

        let second = database
            .plan_retention("Ledger", RetentionPolicy::keep_last(1))
            .await
            .unwrap();
        assert!(!second.remove.is_empty());
        assert!(second.remove.len() < prolly_dynamodb_core::MAX_RETENTION_REMOVALS);
        assert!(!second.more_removable);
        database
            .apply_retention(
                &second,
                MaintenanceContext::new("ledger-admin", "bounded batch two"),
            )
            .await
            .unwrap();
        assert!(database
            .plan_retention("Ledger", RetentionPolicy::keep_last(1))
            .await
            .unwrap()
            .remove
            .is_empty());
    });
}

#[test]
fn retention_reconciles_ambiguous_commits_and_restarts_from_durable_audit() {
    block_on(async {
        let store = Arc::new(CommitFaultStore::default());
        let database = Database::new(SyncStoreAsAsync::new(store.clone()), Config::default())
            .with_sources(Arc::new(SequenceIds::default()), Arc::new(FixedClock));
        database
            .create_table(
                "Ledger",
                KeyAttribute {
                    name: "account".into(),
                    kind: KeyKind::String,
                },
                None,
            )
            .await
            .unwrap();
        database
            .put_item(
                "Ledger",
                Item::from([("account".into(), AttributeValue::S("cash".into()))]),
                None,
            )
            .await
            .unwrap();
        let visible_plan = database
            .plan_retention("Ledger", RetentionPolicy::keep_last(1))
            .await
            .unwrap();
        assert!(!visible_plan.remove.is_empty());
        let context = MaintenanceContext::new("ledger-admin", "ambiguous retention");
        store.report_ambiguous_after_next_commit();
        let visible = database
            .apply_retention(&visible_plan, context.clone())
            .await
            .unwrap();
        assert!(visible.replayed);
        assert!(database
            .retention_audit(&visible_plan.id)
            .await
            .unwrap()
            .is_some());

        database
            .put_item(
                "Ledger",
                Item::from([("account".into(), AttributeValue::S("receivable".into()))]),
                None,
            )
            .await
            .unwrap();
        let restart_plan = database
            .plan_retention("Ledger", RetentionPolicy::keep_last(0))
            .await
            .unwrap();
        assert!(!restart_plan.remove.is_empty());
        store.report_ambiguous_and_fail_reconciliation();
        assert!(matches!(
            database
                .apply_retention(&restart_plan, context.clone())
                .await,
            Err(prolly_dynamodb_core::Error::Storage(_))
        ));

        let restarted = Database::new(SyncStoreAsAsync::new(store.clone()), Config::default())
            .with_sources(Arc::new(SequenceIds::default()), Arc::new(FixedClock));
        let reconciled = restarted
            .apply_retention(&restart_plan, context)
            .await
            .unwrap();
        assert!(reconciled.replayed);
        assert_eq!(reconciled.removed, restart_plan.remove);
    });
}

#[test]
fn ordinary_writes_record_distinct_durable_commits_including_no_ops() {
    block_on(async {
        let database = Database::new(
            SyncStoreAsAsync::new(Arc::new(MemStore::new())),
            Config::default(),
        )
        .with_sources(Arc::new(SequenceIds::default()), Arc::new(FixedClock));
        let created = database
            .create_table_result(
                "Ledger",
                KeyAttribute {
                    name: "account".into(),
                    kind: KeyKind::String,
                },
                None,
            )
            .await
            .unwrap();
        let created_commit = created.commit_id;
        let empty_head = database.head("Ledger").await.unwrap().id;
        let entry = Item::from([
            ("account".into(), AttributeValue::S("cash".into())),
            ("status".into(), AttributeValue::S("OPEN".into())),
        ]);

        let inserted = database
            .put_item_result("Ledger", entry.clone(), None)
            .await
            .unwrap();
        let inserted_commit = inserted.commit_id.clone().unwrap();
        assert!(inserted.update.is_applied());
        let inserted_version = inserted.update.current().unwrap().id.clone();

        let repeated = database
            .put_item_result("Ledger", entry.clone(), None)
            .await
            .unwrap();
        let repeated_commit = repeated.commit_id.clone().unwrap();
        assert!(!repeated.update.is_applied());
        assert_ne!(repeated_commit, inserted_commit);

        let no_change = parse_update(
            "SET #status = :open",
            None,
            &BTreeMap::from([("#status".into(), "status".into())]),
            &BTreeMap::from([(":open".into(), AttributeValue::S("OPEN".into()))]),
        )
        .unwrap();
        let updated = database
            .update_item(
                "Ledger",
                &Item::from([("account".into(), AttributeValue::S("cash".into()))]),
                None,
                None,
                &no_change.plan,
            )
            .await
            .unwrap();
        let updated_commit = updated.commit_id.clone().unwrap();
        assert!(!updated.update.is_applied());
        assert_ne!(updated_commit, repeated_commit);

        let stale = database
            .delete_item_result(
                "Ledger",
                &Item::from([("account".into(), AttributeValue::S("cash".into()))]),
                Some(&empty_head),
            )
            .await
            .unwrap();
        assert!(stale.commit_id.is_none());
        assert!(matches!(stale.update, VersionedMapUpdate::Conflict { .. }));

        let deleted = database
            .delete_item_result(
                "Ledger",
                &Item::from([("account".into(), AttributeValue::S("cash".into()))]),
                None,
            )
            .await
            .unwrap();
        let deleted_commit = deleted.commit_id.clone().unwrap();
        assert!(deleted.update.is_applied());

        let absent_delete = database
            .delete_item_result(
                "Ledger",
                &Item::from([("account".into(), AttributeValue::S("cash".into()))]),
                None,
            )
            .await
            .unwrap();
        let absent_commit = absent_delete.commit_id.clone().unwrap();
        assert!(!absent_delete.update.is_applied());
        assert_ne!(absent_commit, deleted_commit);
        let deleted_version = absent_delete.update.current().unwrap().id.clone();

        let restored = database
            .restore_result("Ledger", &deleted_version, &inserted_version)
            .await
            .unwrap();
        let restored_commit = restored.commit_id.clone().unwrap();
        assert!(restored.update.is_applied());
        assert_eq!(database.head("Ledger").await.unwrap().id, inserted_version);
        let repeated_restore = database
            .restore_result("Ledger", &inserted_version, &inserted_version)
            .await
            .unwrap();
        let repeated_restore_commit = repeated_restore.commit_id.clone().unwrap();
        assert!(!repeated_restore.update.is_applied());
        assert_ne!(repeated_restore_commit, restored_commit);

        for (commit_id, applied) in [
            (created_commit.clone(), true),
            (inserted_commit, true),
            (repeated_commit, false),
            (updated_commit, false),
            (deleted_commit, true),
            (absent_commit, false),
            (restored_commit, true),
            (repeated_restore_commit, false),
        ] {
            let commit = database.commit(&commit_id).await.unwrap().unwrap();
            assert_eq!(commit.commit_id, commit_id);
            assert_eq!(commit.transitions.len(), 1);
            assert_eq!(commit.transitions[0].table_name, "Ledger");
            assert_eq!(commit.transitions[0].applied, applied);
        }

        let mut after = None;
        let mut history = Vec::new();
        loop {
            let page = database.commits("Ledger", after, 3).await.unwrap();
            history.extend(page.commits);
            match page.last_sequence {
                Some(sequence) => after = Some(sequence),
                None => break,
            }
        }
        assert_eq!(history.len(), 8);
        assert_eq!(
            history
                .iter()
                .map(|commit| commit.sequence)
                .collect::<Vec<_>>(),
            (1_u64..=8).collect::<Vec<_>>()
        );
        assert_eq!(history[0].commit_id, created_commit);
        assert_eq!(history[0].transition.before, None);
        assert!(history[0].transition.after.is_some());
    });
}

#[test]
fn single_write_extension_tokens_replay_exact_images_and_survive_table_deletion() {
    block_on(async {
        let store = Arc::new(MemStore::new());
        let ids = Arc::new(SequenceIds::default());
        let database = Database::new(SyncStoreAsAsync::new(store.clone()), Config::default())
            .with_sources(ids.clone(), Arc::new(FixedClock));
        database
            .create_table(
                "Ledger",
                KeyAttribute {
                    name: "account".into(),
                    kind: KeyKind::String,
                },
                None,
            )
            .await
            .unwrap();
        let key = Item::from([("account".into(), AttributeValue::S("cash".into()))]);
        database
            .put_item(
                "Ledger",
                Item::from([
                    ("account".into(), AttributeValue::S("cash".into())),
                    ("status".into(), AttributeValue::S("OPEN".into())),
                ]),
                None,
            )
            .await
            .unwrap();
        let closed = Item::from([
            ("account".into(), AttributeValue::S("cash".into())),
            ("status".into(), AttributeValue::S("CLOSED".into())),
        ]);
        let first = database
            .put_item_idempotent(
                "Ledger",
                closed.clone(),
                None,
                None,
                "close-ledger-entry",
                true,
            )
            .await
            .unwrap();
        assert_eq!(
            first.old_item.as_ref().unwrap()["status"],
            AttributeValue::S("OPEN".into())
        );
        let replay = database
            .put_item_idempotent(
                "Ledger",
                closed.clone(),
                None,
                None,
                "close-ledger-entry",
                true,
            )
            .await
            .unwrap();
        assert_eq!(replay.commit_id, first.commit_id);
        assert_eq!(replay.old_item, first.old_item);

        let mut changed = closed.clone();
        changed.insert("status".into(), AttributeValue::S("CHANGED".into()));
        assert!(matches!(
            database
                .put_item_idempotent("Ledger", changed, None, None, "close-ledger-entry", true,)
                .await,
            Err(prolly_dynamodb_core::Error::IdempotentParameterMismatch)
        ));

        let paid = parse_update(
            "SET #status = :paid",
            None,
            &BTreeMap::from([("#status".into(), "status".into())]),
            &BTreeMap::from([(":paid".into(), AttributeValue::S("PAID".into()))]),
        )
        .unwrap();
        let updated = database
            .update_item_idempotent("Ledger", &key, None, None, &paid.plan, "pay-ledger-entry")
            .await
            .unwrap();
        assert_eq!(
            updated.old_item.as_ref().unwrap()["status"],
            AttributeValue::S("CLOSED".into())
        );
        assert_eq!(
            updated.new_item.as_ref().unwrap()["status"],
            AttributeValue::S("PAID".into())
        );
        let updated_replay = database
            .update_item_idempotent("Ledger", &key, None, None, &paid.plan, "pay-ledger-entry")
            .await
            .unwrap();
        assert_eq!(updated_replay.commit_id, updated.commit_id);
        assert_eq!(updated_replay.old_item, updated.old_item);
        assert_eq!(updated_replay.new_item, updated.new_item);

        database.delete_table("Ledger").await.unwrap();
        let restarted = Database::new(SyncStoreAsAsync::new(store), Config::default())
            .with_sources(ids, Arc::new(FixedClock));
        let after_deletion = restarted
            .put_item_idempotent("Ledger", closed, None, None, "close-ledger-entry", true)
            .await
            .unwrap();
        assert_eq!(after_deletion.commit_id, first.commit_id);
        assert_eq!(after_deletion.old_item, first.old_item);
    });
}

#[test]
fn fake_store_publication_faults_preserve_exact_single_write_outcomes() {
    block_on(async {
        let store = Arc::new(CommitFaultStore::default());
        let ids = Arc::new(SequenceIds::default());
        let database = Database::new(SyncStoreAsAsync::new(store.clone()), Config::default())
            .with_sources(ids.clone(), Arc::new(FixedClock));
        database
            .create_table(
                "Ledger",
                KeyAttribute {
                    name: "account".into(),
                    kind: KeyKind::String,
                },
                None,
            )
            .await
            .unwrap();
        let ledger_item = |status: &str| {
            Item::from([
                ("account".into(), AttributeValue::S("cash".into())),
                ("status".into(), AttributeValue::S(status.into())),
            ])
        };
        let ledger_key = Item::from([("account".into(), AttributeValue::S("cash".into()))]);

        let initial_head = database.head("Ledger").await.unwrap();
        store.fail_commit_number_from_now(1);
        assert!(matches!(
            database
                .put_item_idempotent(
                    "Ledger",
                    ledger_item("PREPARED"),
                    None,
                    None,
                    "before-root-publication",
                    true,
                )
                .await,
            Err(prolly_dynamodb_core::Error::Storage(_))
        ));
        assert_eq!(database.head("Ledger").await.unwrap(), initial_head);
        assert!(database
            .get_item("Ledger", &ledger_key)
            .await
            .unwrap()
            .is_none());

        let retried = database
            .put_item_idempotent(
                "Ledger",
                ledger_item("PREPARED"),
                None,
                None,
                "before-root-publication",
                true,
            )
            .await
            .unwrap();
        assert_eq!(retried.old_item, None);

        store.report_ambiguous_after_next_commit();
        let reconciled = database
            .put_item_idempotent(
                "Ledger",
                ledger_item("POSTED"),
                None,
                None,
                "accepted-response-lost",
                true,
            )
            .await
            .unwrap();
        assert_eq!(
            reconciled.old_item.as_ref().unwrap()["status"],
            AttributeValue::S("PREPARED".into())
        );
        let reconciled_head = database.head("Ledger").await.unwrap();
        let exact_replay = database
            .put_item_idempotent(
                "Ledger",
                ledger_item("POSTED"),
                None,
                None,
                "accepted-response-lost",
                true,
            )
            .await
            .unwrap();
        assert_eq!(exact_replay.commit_id, reconciled.commit_id);
        assert_eq!(exact_replay.old_item, reconciled.old_item);
        assert_eq!(database.head("Ledger").await.unwrap(), reconciled_head);

        store.report_ambiguous_and_fail_reconciliation();
        assert!(matches!(
            database
                .put_item_idempotent(
                    "Ledger",
                    ledger_item("SETTLED"),
                    None,
                    None,
                    "restart-reconciliation",
                    true,
                )
                .await,
            Err(prolly_dynamodb_core::Error::Storage(_))
        ));
        let committed_unknown_head = database.head("Ledger").await.unwrap();
        assert_ne!(committed_unknown_head, reconciled_head);

        let restarted = Database::new(SyncStoreAsAsync::new(store), Config::default())
            .with_sources(ids, Arc::new(FixedClock));
        let after_restart = restarted
            .put_item_idempotent(
                "Ledger",
                ledger_item("SETTLED"),
                None,
                None,
                "restart-reconciliation",
                true,
            )
            .await
            .unwrap();
        assert_eq!(
            restarted.head("Ledger").await.unwrap(),
            committed_unknown_head
        );
        assert_eq!(
            after_restart.old_item.as_ref().unwrap()["status"],
            AttributeValue::S("POSTED".into())
        );
        assert_eq!(
            restarted
                .get_item("Ledger", &ledger_key)
                .await
                .unwrap()
                .unwrap()["status"],
            AttributeValue::S("SETTLED".into())
        );
    });
}

#[test]
fn table_lifecycle_tokens_replay_the_original_incarnation_without_touching_recreated_names() {
    block_on(async {
        let store = Arc::new(MemStore::new());
        let ids = Arc::new(SequenceIds::default());
        let database = Database::new(SyncStoreAsAsync::new(store.clone()), Config::default())
            .with_sources(ids.clone(), Arc::new(FixedClock));
        let key = KeyAttribute {
            name: "account".into(),
            kind: KeyKind::String,
        };
        let created = database
            .create_table_idempotent_result("Ledger", key.clone(), None, "create-ledger")
            .await
            .unwrap();
        let replayed_create = database
            .create_table_idempotent_result("Ledger", key.clone(), None, "create-ledger")
            .await
            .unwrap();
        assert_eq!(replayed_create, created);
        assert!(matches!(
            database
                .create_table_idempotent_result(
                    "Ledger",
                    KeyAttribute {
                        name: "different".into(),
                        kind: KeyKind::String,
                    },
                    None,
                    "create-ledger",
                )
                .await,
            Err(prolly_dynamodb_core::Error::IdempotentParameterMismatch)
        ));
        assert!(matches!(
            database
                .delete_table_idempotent_result("Ledger", "create-ledger")
                .await,
            Err(prolly_dynamodb_core::Error::IdempotentParameterMismatch)
        ));

        let deleted = database
            .delete_table_idempotent_result("Ledger", "delete-ledger")
            .await
            .unwrap();
        assert_eq!(deleted.description.id, created.description.id);
        let restarted = Database::new(SyncStoreAsAsync::new(store), Config::default())
            .with_sources(ids, Arc::new(FixedClock));
        let replayed_delete = restarted
            .delete_table_idempotent_result("Ledger", "delete-ledger")
            .await
            .unwrap();
        assert_eq!(replayed_delete, deleted);

        let recreated = restarted
            .create_table_idempotent_result("Ledger", key, None, "recreate-ledger")
            .await
            .unwrap();
        assert_ne!(recreated.description.id, deleted.description.id);
        let old_delete_again = restarted
            .delete_table_idempotent_result("Ledger", "delete-ledger")
            .await
            .unwrap();
        assert_eq!(old_delete_again, deleted);
        assert_eq!(
            restarted.describe_table("Ledger").await.unwrap().id,
            recreated.description.id
        );
    });
}

#[test]
fn restore_tokens_replay_one_transition_across_restart_and_deletion() {
    block_on(async {
        let store = Arc::new(MemStore::new());
        let ids = Arc::new(SequenceIds::default());
        let database = Database::new(SyncStoreAsAsync::new(store.clone()), Config::default())
            .with_sources(ids.clone(), Arc::new(FixedClock));
        database
            .create_table(
                "Ledger",
                KeyAttribute {
                    name: "account".into(),
                    kind: KeyKind::String,
                },
                None,
            )
            .await
            .unwrap();
        let first = database
            .put_item_result(
                "Ledger",
                Item::from([
                    ("account".into(), AttributeValue::S("cash".into())),
                    ("status".into(), AttributeValue::S("OPEN".into())),
                ]),
                None,
            )
            .await
            .unwrap()
            .update
            .current()
            .unwrap()
            .id
            .clone();
        let second = database
            .put_item_result(
                "Ledger",
                Item::from([
                    ("account".into(), AttributeValue::S("cash".into())),
                    ("status".into(), AttributeValue::S("CLOSED".into())),
                ]),
                None,
            )
            .await
            .unwrap()
            .update
            .current()
            .unwrap()
            .id
            .clone();
        let restored = database
            .restore_idempotent_result("Ledger", &second, &first, "restore-ledger")
            .await
            .unwrap();
        assert_eq!(restored.update.current().unwrap().id, first);
        let replay = database
            .restore_idempotent_result("Ledger", &second, &first, "restore-ledger")
            .await
            .unwrap();
        assert_eq!(replay.commit_id, restored.commit_id);
        assert_eq!(replay.update.current().unwrap().id, first);
        assert!(matches!(
            database
                .restore_idempotent_result("Ledger", &first, &second, "restore-ledger")
                .await,
            Err(prolly_dynamodb_core::Error::IdempotentParameterMismatch)
        ));

        database.delete_table("Ledger").await.unwrap();
        let restarted = Database::new(SyncStoreAsAsync::new(store), Config::default())
            .with_sources(ids, Arc::new(FixedClock));
        let after_deletion = restarted
            .restore_idempotent_result("Ledger", &second, &first, "restore-ledger")
            .await
            .unwrap();
        assert_eq!(after_deletion.commit_id, restored.commit_id);
        assert_eq!(after_deletion.update.current().unwrap().id, first);
    });
}

#[test]
fn batch_get_is_pinned_ordered_projected_and_rejects_canonical_duplicates() {
    block_on(async {
        let store = SyncStoreAsAsync::new(Arc::new(MemStore::new()));
        let database = Database::new_with_blob_storage(
            store,
            Config::default(),
            Arc::new(RecordingBlobs::default()),
            LargeValueConfig::new(1024),
        )
        .unwrap()
        .with_sources(Arc::new(SequenceIds::default()), Arc::new(FixedClock));
        database
            .create_table(
                "Orders",
                KeyAttribute {
                    name: "account".into(),
                    kind: KeyKind::String,
                },
                Some(KeyAttribute {
                    name: "sequence".into(),
                    kind: KeyKind::Number,
                }),
            )
            .await
            .unwrap();
        for sequence in ["1", "2"] {
            database
                .put_item("Orders", item("acct-1", sequence, "OPEN"), None)
                .await
                .unwrap();
        }
        let pinned = database.head("Orders").await.unwrap().id;
        database
            .put_item("Orders", item("acct-1", "1", "CLOSED"), None)
            .await
            .unwrap();

        let projection = parse_projection(
            "#status",
            &BTreeMap::from([("#status".into(), "status".into())]),
        )
        .unwrap();
        let result = database
            .batch_get(BTreeMap::from([(
                "Orders".into(),
                BatchGetTableRequest {
                    keys: vec![key("acct-1", "2"), key("missing", "3"), key("acct-1", "1")],
                    projection: Some(projection),
                    version: Some(pinned),
                },
            )]))
            .await
            .unwrap();
        let orders = &result.tables["Orders"];
        assert_eq!(orders.items.len(), 2);
        assert!(orders.unprocessed_keys.is_empty());
        assert!(
            orders
                .items
                .iter()
                .all(|item| item
                    == &Item::from([("status".into(), AttributeValue::S("OPEN".into()))]))
        );
        assert_eq!(
            result.response_bytes,
            orders
                .items
                .iter()
                .map(item_size)
                .collect::<Result<Vec<_>>>()
                .unwrap()
                .into_iter()
                .sum::<usize>()
        );

        let duplicate = database
            .batch_get(BTreeMap::from([(
                "Orders".into(),
                BatchGetTableRequest {
                    keys: vec![key("acct-1", "1"), key("acct-1", "1.0")],
                    projection: None,
                    version: None,
                },
            )]))
            .await;
        assert!(
            matches!(duplicate, Err(prolly_dynamodb_core::Error::Validation(message)) if message.contains("duplicate key"))
        );

        let payload = "x".repeat(300 * 1024);
        for sequence in ["10", "11", "12", "13"] {
            let mut value = item("large", sequence, "OPEN");
            value.insert("payload".into(), AttributeValue::S(payload.clone()));
            database.put_item("Orders", value, None).await.unwrap();
        }
        let mut other_partition = item("other", "1", "OPEN");
        other_partition.insert("payload".into(), AttributeValue::S(payload));
        database
            .put_item("Orders", other_partition, None)
            .await
            .unwrap();
        let limited = database
            .batch_get(BTreeMap::from([(
                "Orders".into(),
                BatchGetTableRequest {
                    keys: vec![
                        key("large", "10"),
                        key("large", "11"),
                        key("large", "12"),
                        key("large", "13"),
                        key("other", "1"),
                    ],
                    projection: None,
                    version: None,
                },
            )]))
            .await
            .unwrap();
        assert_eq!(limited.tables["Orders"].items.len(), 4);
        assert_eq!(
            limited.tables["Orders"].unprocessed_keys,
            vec![key("large", "13")]
        );
    });
}

#[test]
fn batch_write_validation_is_global_canonical_and_write_free() {
    block_on(async {
        let store = SyncStoreAsAsync::new(Arc::new(MemStore::new()));
        let database = Database::new(store, Config::default())
            .with_sources(Arc::new(SequenceIds::default()), Arc::new(FixedClock));
        database
            .create_table(
                "Orders",
                KeyAttribute {
                    name: "account".into(),
                    kind: KeyKind::String,
                },
                Some(KeyAttribute {
                    name: "sequence".into(),
                    kind: KeyKind::Number,
                }),
            )
            .await
            .unwrap();
        let before = database.head("Orders").await.unwrap().id;

        let duplicate = BTreeMap::from([(
            "Orders".into(),
            vec![
                BatchWriteAction::Put(item("acct-1", "1.0", "OPEN")),
                BatchWriteAction::Delete(key("acct-1", "1")),
            ],
        )]);
        assert!(matches!(
            database.validate_batch_write(&duplicate).await,
            Err(prolly_dynamodb_core::Error::Validation(message))
                if message.contains("duplicate operations")
        ));
        assert_eq!(database.head("Orders").await.unwrap().id, before);

        let too_many = BTreeMap::from([(
            "Orders".into(),
            (0..26)
                .map(|sequence| BatchWriteAction::Delete(key("acct-1", &sequence.to_string())))
                .collect(),
        )]);
        assert!(matches!(
            database.validate_batch_write(&too_many).await,
            Err(prolly_dynamodb_core::Error::Validation(message))
                if message.contains("at most 25")
        ));
        assert_eq!(database.head("Orders").await.unwrap().id, before);

        let applied = database
            .batch_write(BTreeMap::from([(
                "Orders".into(),
                vec![
                    BatchWriteAction::Put(item("acct-1", "1", "OPEN")),
                    BatchWriteAction::Put(item("acct-1", "2", "OPEN")),
                    BatchWriteAction::Delete(key("acct-1", "3")),
                ],
            )]))
            .await
            .unwrap();
        assert_eq!(applied.transitions.len(), 3);
        assert_eq!(
            applied
                .transitions
                .iter()
                .map(|transition| transition.commit_id.clone())
                .collect::<BTreeSet<_>>()
                .len(),
            3
        );
        assert!(applied.transitions[0].update.is_applied());
        assert!(applied.transitions[1].update.is_applied());
        assert!(!applied.transitions[2].update.is_applied());
        for transition in &applied.transitions {
            let commit = database
                .commit(&transition.commit_id)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(commit.transitions.len(), 1);
            assert_eq!(
                commit.transitions[0].applied,
                transition.update.is_applied()
            );
        }
        assert!(database
            .get_item("Orders", &key("acct-1", "1"))
            .await
            .unwrap()
            .is_some());
        assert!(database
            .get_item("Orders", &key("acct-1", "2"))
            .await
            .unwrap()
            .is_some());
    });
}

#[test]
fn batch_write_reports_the_exact_partial_commit_boundary() {
    block_on(async {
        let store = Arc::new(CommitFaultStore::default());
        let database = Database::new(SyncStoreAsAsync::new(store.clone()), Config::default())
            .with_sources(Arc::new(SequenceIds::default()), Arc::new(FixedClock));
        database
            .create_table(
                "Orders",
                KeyAttribute {
                    name: "account".into(),
                    kind: KeyKind::String,
                },
                Some(KeyAttribute {
                    name: "sequence".into(),
                    kind: KeyKind::Number,
                }),
            )
            .await
            .unwrap();

        store.fail_commit_number_from_now(2);
        let failure = database
            .batch_write(BTreeMap::from([(
                "Orders".into(),
                vec![
                    BatchWriteAction::Put(item("acct-1", "1", "OPEN")),
                    BatchWriteAction::Put(item("acct-1", "2", "OPEN")),
                    BatchWriteAction::Put(item("acct-1", "3", "OPEN")),
                ],
            )]))
            .await
            .unwrap_err();
        let BatchWriteExecutionError::Partial {
            table_name,
            action_index,
            applied_transitions,
            source,
        } = failure
        else {
            panic!("expected a partial execution failure")
        };
        assert_eq!(table_name, "Orders");
        assert_eq!(action_index, 1);
        assert_eq!(applied_transitions.len(), 1);
        assert_eq!(applied_transitions[0].table_name, "Orders");
        assert_eq!(applied_transitions[0].action_index, 0);
        assert!(applied_transitions[0].update.is_applied());
        assert!(matches!(source, prolly_dynamodb_core::Error::Storage(_)));

        assert!(database
            .get_item("Orders", &key("acct-1", "1"))
            .await
            .unwrap()
            .is_some());
        assert_eq!(
            database
                .get_item("Orders", &key("acct-1", "2"))
                .await
                .unwrap(),
            None
        );
        assert_eq!(
            database
                .get_item("Orders", &key("acct-1", "3"))
                .await
                .unwrap(),
            None
        );
    });
}

#[test]
fn transact_get_preserves_order_projection_versions_and_retries_the_read_set() {
    block_on(async {
        let store = Arc::new(CommitFaultStore::default());
        let database = Database::new(SyncStoreAsAsync::new(store.clone()), Config::default())
            .with_sources(Arc::new(SequenceIds::default()), Arc::new(FixedClock));
        for table in ["Orders", "Accounts"] {
            database
                .create_table(
                    table,
                    KeyAttribute {
                        name: "account".into(),
                        kind: KeyKind::String,
                    },
                    None,
                )
                .await
                .unwrap();
        }
        database
            .put_item(
                "Orders",
                Item::from([
                    ("account".into(), AttributeValue::S("acct-1".into())),
                    ("status".into(), AttributeValue::S("OPEN".into())),
                    ("private".into(), AttributeValue::S("hidden".into())),
                ]),
                None,
            )
            .await
            .unwrap();
        database
            .put_item(
                "Accounts",
                Item::from([
                    ("account".into(), AttributeValue::S("acct-1".into())),
                    ("tier".into(), AttributeValue::S("LEGAL".into())),
                ]),
                None,
            )
            .await
            .unwrap();
        let order_version = database.head("Orders").await.unwrap().id;
        let account_version = database.head("Accounts").await.unwrap().id;
        let projection = parse_projection(
            "#status",
            &BTreeMap::from([("#status".into(), "status".into())]),
        )
        .unwrap();

        store.conflict_next_commits(1);
        let result = database
            .transact_get(vec![
                TransactGetRequest {
                    table_name: "Accounts".into(),
                    key: Item::from([("account".into(), AttributeValue::S("missing".into()))]),
                    projection: None,
                },
                TransactGetRequest {
                    table_name: "Orders".into(),
                    key: Item::from([("account".into(), AttributeValue::S("acct-1".into()))]),
                    projection: Some(projection),
                },
                TransactGetRequest {
                    table_name: "Accounts".into(),
                    key: Item::from([("account".into(), AttributeValue::S("acct-1".into()))]),
                    projection: None,
                },
            ])
            .await
            .unwrap();
        assert_eq!(result.responses.len(), 3);
        assert_eq!(result.responses[0].item, None);
        assert_eq!(
            result.responses[1].item,
            Some(Item::from([(
                "status".into(),
                AttributeValue::S("OPEN".into())
            )]))
        );
        assert_eq!(
            result.responses[2].item.as_ref().unwrap()["tier"],
            AttributeValue::S("LEGAL".into())
        );
        assert_eq!(result.table_versions["Orders"], order_version);
        assert_eq!(result.table_versions["Accounts"], account_version);
        assert!(result.response_bytes > 0);

        let too_many = vec![
            TransactGetRequest {
                table_name: "Orders".into(),
                key: Item::from([("account".into(), AttributeValue::S("acct-1".into()),)]),
                projection: None,
            };
            101
        ];
        assert!(matches!(
            database.transact_get(too_many).await,
            Err(prolly_dynamodb_core::Error::Validation(message))
                if message.contains("1..=100")
        ));
    });
}

#[test]
fn transact_write_is_atomic_retries_conditions_and_preserves_reason_order() {
    block_on(async {
        let store = Arc::new(CommitFaultStore::default());
        let database = Database::new(SyncStoreAsAsync::new(store.clone()), Config::default())
            .with_sources(Arc::new(SequenceIds::default()), Arc::new(FixedClock));
        for table in ["Orders", "Accounts"] {
            database
                .create_table(
                    table,
                    KeyAttribute {
                        name: "account".into(),
                        kind: KeyKind::String,
                    },
                    None,
                )
                .await
                .unwrap();
        }
        database
            .put_item(
                "Orders",
                Item::from([
                    ("account".into(), AttributeValue::S("acct-1".into())),
                    ("status".into(), AttributeValue::S("OPEN".into())),
                ]),
                None,
            )
            .await
            .unwrap();
        database
            .put_item(
                "Accounts",
                Item::from([
                    ("account".into(), AttributeValue::S("acct-1".into())),
                    ("tier".into(), AttributeValue::S("LEGAL".into())),
                ]),
                None,
            )
            .await
            .unwrap();
        let update = parse_update(
            "SET #status = :closed",
            None,
            &BTreeMap::from([("#status".into(), "status".into())]),
            &BTreeMap::from([(":closed".into(), AttributeValue::S("CLOSED".into()))]),
        )
        .unwrap()
        .plan;
        let open = Condition::Equals {
            name: "status".into(),
            value: AttributeValue::S("OPEN".into()),
        };
        let legal = Condition::Equals {
            name: "tier".into(),
            value: AttributeValue::S("LEGAL".into()),
        };

        store.conflict_next_commits(1);
        let committed = database
            .transact_write(vec![
                TransactWriteAction::Update {
                    table_name: "Orders".into(),
                    key: Item::from([("account".into(), AttributeValue::S("acct-1".into()))]),
                    condition: Some(open.clone()),
                    plan: update.clone(),
                    return_failure_old: true,
                },
                TransactWriteAction::Put {
                    table_name: "Accounts".into(),
                    item: Item::from([
                        ("account".into(), AttributeValue::S("acct-2".into())),
                        ("tier".into(), AttributeValue::S("FINANCE".into())),
                    ]),
                    condition: None,
                    return_failure_old: false,
                },
                TransactWriteAction::ConditionCheck {
                    table_name: "Accounts".into(),
                    key: Item::from([("account".into(), AttributeValue::S("acct-1".into()))]),
                    condition: legal,
                    return_failure_old: true,
                },
            ])
            .await
            .unwrap();
        assert_eq!(committed.transitions.len(), 2);
        assert!(committed.transitions.iter().all(|entry| entry.applied));
        assert_eq!(
            database
                .get_item(
                    "Orders",
                    &Item::from([("account".into(), AttributeValue::S("acct-1".into()))])
                )
                .await
                .unwrap()
                .unwrap()["status"],
            AttributeValue::S("CLOSED".into())
        );
        assert!(database
            .get_item(
                "Accounts",
                &Item::from([("account".into(), AttributeValue::S("acct-2".into()))])
            )
            .await
            .unwrap()
            .is_some());

        let orders_before = database.head("Orders").await.unwrap().id;
        let accounts_before = database.head("Accounts").await.unwrap().id;
        let failed = database
            .transact_write(vec![
                TransactWriteAction::Update {
                    table_name: "Orders".into(),
                    key: Item::from([("account".into(), AttributeValue::S("acct-1".into()))]),
                    condition: Some(open),
                    plan: update,
                    return_failure_old: true,
                },
                TransactWriteAction::Put {
                    table_name: "Accounts".into(),
                    item: Item::from([("account".into(), AttributeValue::S("acct-3".into()))]),
                    condition: None,
                    return_failure_old: false,
                },
            ])
            .await;
        let Err(prolly_dynamodb_core::Error::TransactionCanceled { reasons }) = failed else {
            panic!("expected ordered transaction cancellation reasons")
        };
        assert_eq!(reasons.len(), 2);
        assert_eq!(
            reasons[0].code,
            Some(TransactionCancellationCode::ConditionalCheckFailed)
        );
        assert_eq!(
            reasons[0].item.as_ref().unwrap()["status"],
            AttributeValue::S("CLOSED".into())
        );
        assert_eq!(reasons[1].code, None);
        assert_eq!(database.head("Orders").await.unwrap().id, orders_before);
        assert_eq!(database.head("Accounts").await.unwrap().id, accounts_before);
        assert_eq!(
            database
                .get_item(
                    "Accounts",
                    &Item::from([("account".into(), AttributeValue::S("acct-3".into()))])
                )
                .await
                .unwrap(),
            None
        );
    });
}

#[test]
fn transact_write_tokens_replay_durable_commits_and_record_no_op_participants() {
    block_on(async {
        let database = Database::new(
            SyncStoreAsAsync::new(Arc::new(MemStore::new())),
            Config::default(),
        )
        .with_sources(Arc::new(SequenceIds::default()), Arc::new(FixedClock));
        database
            .create_table(
                "Ledger",
                KeyAttribute {
                    name: "account".into(),
                    kind: KeyKind::String,
                },
                None,
            )
            .await
            .unwrap();

        let put = TransactWriteAction::Put {
            table_name: "Ledger".into(),
            item: Item::from([
                ("account".into(), AttributeValue::S("cash".into())),
                ("status".into(), AttributeValue::S("OPEN".into())),
            ]),
            condition: None,
            return_failure_old: false,
        };
        let committed = database
            .transact_write_idempotent(vec![put.clone()], Some("posting-0001"))
            .await
            .unwrap();
        assert_eq!(committed.transitions.len(), 1);
        assert!(committed.transitions[0].applied);
        let committed_head = database.head("Ledger").await.unwrap().id;

        let replayed = database
            .transact_write_idempotent(vec![put], Some("posting-0001"))
            .await
            .unwrap();
        assert_eq!(replayed, committed);
        assert_eq!(database.head("Ledger").await.unwrap().id, committed_head);
        assert_eq!(
            database.commit(&committed.commit_id).await.unwrap(),
            Some(committed.clone())
        );

        let changed_payload = TransactWriteAction::Delete {
            table_name: "Ledger".into(),
            key: Item::from([("account".into(), AttributeValue::S("cash".into()))]),
            condition: None,
            return_failure_old: false,
        };
        assert!(matches!(
            database
                .transact_write_idempotent(vec![changed_payload], Some("posting-0001"))
                .await,
            Err(prolly_dynamodb_core::Error::IdempotentParameterMismatch)
        ));
        assert_eq!(database.head("Ledger").await.unwrap().id, committed_head);

        let condition_only = database
            .transact_write_idempotent(
                vec![TransactWriteAction::ConditionCheck {
                    table_name: "Ledger".into(),
                    key: Item::from([("account".into(), AttributeValue::S("cash".into()))]),
                    condition: Condition::Equals {
                        name: "status".into(),
                        value: AttributeValue::S("OPEN".into()),
                    },
                    return_failure_old: true,
                }],
                Some("posting-check-0001"),
            )
            .await
            .unwrap();
        assert_ne!(condition_only.commit_id, committed.commit_id);
        assert_eq!(condition_only.transitions.len(), 1);
        assert!(!condition_only.transitions[0].applied);
        assert_eq!(
            condition_only.transitions[0].before,
            Some(committed_head.clone())
        );
        assert_eq!(condition_only.transitions[0].after, Some(committed_head));
        assert_eq!(
            database.commit(&condition_only.commit_id).await.unwrap(),
            Some(condition_only)
        );
        assert_eq!(
            database
                .commit(&prolly_dynamodb_core::CommitId([0; 32]))
                .await
                .unwrap(),
            None
        );
    });
}

#[test]
fn transact_write_token_expires_after_the_ten_minute_window() {
    block_on(async {
        let clock = Arc::new(AdjustableClock::new(1_000));
        let database = Database::new(
            SyncStoreAsAsync::new(Arc::new(MemStore::new())),
            Config::default(),
        )
        .with_sources(Arc::new(SequenceIds::default()), clock.clone());
        database
            .create_table(
                "Ledger",
                KeyAttribute {
                    name: "account".into(),
                    kind: KeyKind::String,
                },
                None,
            )
            .await
            .unwrap();
        let key = Item::from([("account".into(), AttributeValue::S("cash".into()))]);
        let first = database
            .transact_write_idempotent(
                vec![TransactWriteAction::Put {
                    table_name: "Ledger".into(),
                    item: key.clone(),
                    condition: None,
                    return_failure_old: false,
                }],
                Some("reusable-after-expiry"),
            )
            .await
            .unwrap();

        clock.set(1_000 + 10 * 60 * 1_000 + 1);
        let second = database
            .transact_write_idempotent(
                vec![TransactWriteAction::Delete {
                    table_name: "Ledger".into(),
                    key,
                    condition: None,
                    return_failure_old: false,
                }],
                Some("reusable-after-expiry"),
            )
            .await
            .unwrap();
        assert_ne!(second.commit_id, first.commit_id);
        assert!(second.transitions[0].applied);
        assert_eq!(
            database
                .get_item(
                    "Ledger",
                    &Item::from([("account".into(), AttributeValue::S("cash".into()))])
                )
                .await
                .unwrap(),
            None
        );
    });
}

#[test]
fn transact_write_reconciles_an_ambiguous_commit_after_process_restart() {
    block_on(async {
        let store = Arc::new(CommitFaultStore::default());
        let database = Database::new(SyncStoreAsAsync::new(store.clone()), Config::default())
            .with_sources(Arc::new(SequenceIds::default()), Arc::new(FixedClock));
        database
            .create_table(
                "Ledger",
                KeyAttribute {
                    name: "account".into(),
                    kind: KeyKind::String,
                },
                None,
            )
            .await
            .unwrap();
        let action = TransactWriteAction::Put {
            table_name: "Ledger".into(),
            item: Item::from([
                ("account".into(), AttributeValue::S("cash".into())),
                ("status".into(), AttributeValue::S("POSTED".into())),
            ]),
            condition: None,
            return_failure_old: false,
        };

        store.report_ambiguous_and_fail_reconciliation();
        assert!(matches!(
            database
                .transact_write_idempotent(vec![action.clone()], Some("journal-entry-42"))
                .await,
            Err(prolly_dynamodb_core::Error::Storage(_))
        ));
        let committed_head = database.head("Ledger").await.unwrap().id;

        // A fresh handle represents a new process: it has no in-memory retry
        // state and reconciles solely through the durable token record.
        let restarted = Database::new(SyncStoreAsAsync::new(store.clone()), Config::default())
            .with_sources(Arc::new(SequenceIds::default()), Arc::new(FixedClock));
        let reconciled = restarted
            .transact_write_idempotent(vec![action], Some("journal-entry-42"))
            .await
            .unwrap();
        assert_eq!(reconciled.table_versions["Ledger"], committed_head);
        assert_eq!(restarted.head("Ledger").await.unwrap().id, committed_head);
        let history = restarted.commits("Ledger", None, 10).await.unwrap();
        assert_eq!(history.commits.len(), 2); // create + one transaction
        assert_eq!(history.commits[1].commit_id, reconciled.commit_id);
        assert_eq!(
            restarted.commit(&reconciled.commit_id).await.unwrap(),
            Some(reconciled)
        );
    });
}

#[test]
fn transact_write_reconciles_a_visible_ambiguous_commit_before_returning() {
    block_on(async {
        let store = Arc::new(CommitFaultStore::default());
        let database = Database::new(SyncStoreAsAsync::new(store.clone()), Config::default())
            .with_sources(Arc::new(SequenceIds::default()), Arc::new(FixedClock));
        database
            .create_table(
                "Ledger",
                KeyAttribute {
                    name: "account".into(),
                    kind: KeyKind::String,
                },
                None,
            )
            .await
            .unwrap();
        store.report_ambiguous_after_next_commit();
        let committed = database
            .transact_write_idempotent(
                vec![TransactWriteAction::Put {
                    table_name: "Ledger".into(),
                    item: Item::from([("account".into(), AttributeValue::S("cash".into()))]),
                    condition: None,
                    return_failure_old: false,
                }],
                Some("visible-journal-entry"),
            )
            .await
            .unwrap();
        assert_eq!(
            database.commit(&committed.commit_id).await.unwrap(),
            Some(committed)
        );
        assert_eq!(
            database
                .commits("Ledger", None, 10)
                .await
                .unwrap()
                .commits
                .len(),
            2
        );
    });
}

#[test]
fn query_and_scan_pages_are_pinned_ordered_and_resumable() {
    block_on(async {
        let store = SyncStoreAsAsync::new(Arc::new(MemStore::new()));
        let database = Database::new(store, Config::default())
            .with_sources(Arc::new(SequenceIds::default()), Arc::new(FixedClock));
        database
            .create_table(
                "Orders",
                KeyAttribute {
                    name: "account".into(),
                    kind: KeyKind::String,
                },
                Some(KeyAttribute {
                    name: "sequence".into(),
                    kind: KeyKind::Number,
                }),
            )
            .await
            .unwrap();
        for sequence in ["10", "2", "1"] {
            database
                .put_item("Orders", item("acct-1", sequence, "OPEN"), None)
                .await
                .unwrap();
        }

        let partition = Item::from([("account".into(), AttributeValue::S("acct-1".into()))]);
        let first = database
            .query_partition("Orders", &partition, None, 2)
            .await
            .unwrap();
        assert_eq!(first.items.len(), 2);
        assert!(first.last_evaluated_key.is_some());
        assert_eq!(
            first.items[0]["sequence"],
            AttributeValue::N(DynamoNumber::parse("1").unwrap())
        );
        assert_eq!(
            first.items[1]["sequence"],
            AttributeValue::N(DynamoNumber::parse("2").unwrap())
        );
        let second = database
            .query_partition("Orders", &partition, first.last_evaluated_key.as_ref(), 2)
            .await
            .unwrap();
        assert_eq!(second.items.len(), 1);
        assert!(second.last_evaluated_key.is_none());
        assert_eq!(
            second.items[0]["sequence"],
            AttributeValue::N(DynamoNumber::parse("10").unwrap())
        );

        let bounded_condition = parse_key_condition(
            "#pk = :pk AND #sk >= :lower",
            &std::collections::BTreeMap::from([
                ("#pk".into(), "account".into()),
                ("#sk".into(), "sequence".into()),
            ]),
            &std::collections::BTreeMap::from([
                (":pk".into(), AttributeValue::S("acct-1".into())),
                (
                    ":lower".into(),
                    AttributeValue::N(DynamoNumber::parse("2").unwrap()),
                ),
            ]),
        )
        .unwrap();
        let bounded = database
            .query_key_condition("Orders", &bounded_condition, None, 10)
            .await
            .unwrap();
        assert_eq!(
            bounded
                .items
                .iter()
                .map(|item| item["sequence"].clone())
                .collect::<Vec<_>>(),
            vec![
                AttributeValue::N(DynamoNumber::parse("2").unwrap()),
                AttributeValue::N(DynamoNumber::parse("10").unwrap()),
            ]
        );
        let descending = database
            .query_key_condition_ordered("Orders", &bounded_condition, None, 10, false)
            .await
            .unwrap();
        assert_eq!(
            descending
                .items
                .iter()
                .map(|item| item["sequence"].clone())
                .collect::<Vec<_>>(),
            vec![
                AttributeValue::N(DynamoNumber::parse("10").unwrap()),
                AttributeValue::N(DynamoNumber::parse("2").unwrap()),
            ]
        );
        let descending_first = database
            .query_key_condition_ordered("Orders", &bounded_condition, None, 1, false)
            .await
            .unwrap();
        let descending_second = database
            .query_key_condition_ordered(
                "Orders",
                &bounded_condition,
                descending_first.last_evaluated_key.as_ref(),
                1,
                false,
            )
            .await
            .unwrap();
        assert_eq!(
            descending_second.items[0]["sequence"],
            AttributeValue::N(DynamoNumber::parse("2").unwrap())
        );

        for (expression, bindings, expected) in [
            ("#pk=:pk AND #sk = :bound", vec![(":bound", "2")], vec!["2"]),
            ("#pk=:pk AND #sk < :bound", vec![(":bound", "2")], vec!["1"]),
            (
                "#pk=:pk AND #sk <= :bound",
                vec![(":bound", "2")],
                vec!["1", "2"],
            ),
            (
                "#pk=:pk AND #sk BETWEEN :lower AND :upper",
                vec![(":lower", "2"), (":upper", "10")],
                vec!["2", "10"],
            ),
            (
                "#pk=:pk AND #sk > :bound",
                vec![(":bound", "2")],
                vec!["10"],
            ),
        ] {
            let mut values = std::collections::BTreeMap::from([(
                ":pk".into(),
                AttributeValue::S("acct-1".into()),
            )]);
            values.extend(bindings.into_iter().map(|(name, value)| {
                (
                    name.to_string(),
                    AttributeValue::N(DynamoNumber::parse(value).unwrap()),
                )
            }));
            let condition = parse_key_condition(
                expression,
                &std::collections::BTreeMap::from([
                    ("#pk".into(), "account".into()),
                    ("#sk".into(), "sequence".into()),
                ]),
                &values,
            )
            .unwrap();
            let page = database
                .query_key_condition("Orders", &condition, None, 10)
                .await
                .unwrap();
            assert_eq!(
                page.items
                    .iter()
                    .map(|item| match &item["sequence"] {
                        AttributeValue::N(value) => value.as_str(),
                        other => panic!("unexpected sort key: {other:?}"),
                    })
                    .collect::<Vec<_>>(),
                expected,
                "{expression}"
            );
        }

        database
            .create_table(
                "Events",
                KeyAttribute {
                    name: "account".into(),
                    kind: KeyKind::String,
                },
                Some(KeyAttribute {
                    name: "event".into(),
                    kind: KeyKind::String,
                }),
            )
            .await
            .unwrap();
        for event in ["2026-a", "2027-a", "2026-b"] {
            database
                .put_item(
                    "Events",
                    Item::from([
                        ("account".into(), AttributeValue::S("acct-1".into())),
                        ("event".into(), AttributeValue::S(event.into())),
                    ]),
                    None,
                )
                .await
                .unwrap();
        }
        let begins = parse_key_condition(
            "#pk=:pk AND begins_with(#sk,:prefix)",
            &std::collections::BTreeMap::from([
                ("#pk".into(), "account".into()),
                ("#sk".into(), "event".into()),
            ]),
            &std::collections::BTreeMap::from([
                (":pk".into(), AttributeValue::S("acct-1".into())),
                (":prefix".into(), AttributeValue::S("2026-".into())),
            ]),
        )
        .unwrap();
        let events = database
            .query_key_condition("Events", &begins, None, 10)
            .await
            .unwrap();
        assert_eq!(
            events
                .items
                .iter()
                .map(|item| match &item["event"] {
                    AttributeValue::S(value) => value.as_str(),
                    other => panic!("unexpected sort key: {other:?}"),
                })
                .collect::<Vec<_>>(),
            vec!["2026-a", "2026-b"]
        );

        let scan = database.scan("Orders", None, 2).await.unwrap();
        assert_eq!(scan.items, first.items);
        assert_eq!(scan.version_id, first.version_id);
    });
}

#[test]
fn query_plans_number_and_binary_key_ranges_exactly() {
    block_on(async {
        let database = Database::new(
            SyncStoreAsAsync::new(Arc::new(MemStore::new())),
            Config::default(),
        )
        .with_sources(Arc::new(SequenceIds::default()), Arc::new(FixedClock));

        database
            .create_table(
                "NumberKeys",
                KeyAttribute {
                    name: "account".into(),
                    kind: KeyKind::Number,
                },
                None,
            )
            .await
            .unwrap();
        for account in ["-1", "2", "10"] {
            database
                .put_item(
                    "NumberKeys",
                    Item::from([
                        (
                            "account".into(),
                            AttributeValue::N(DynamoNumber::parse(account).unwrap()),
                        ),
                        ("payload".into(), AttributeValue::S(account.into())),
                    ]),
                    None,
                )
                .await
                .unwrap();
        }
        let number_condition = parse_key_condition(
            "#pk = :pk",
            &BTreeMap::from([("#pk".into(), "account".into())]),
            &BTreeMap::from([(
                ":pk".into(),
                AttributeValue::N(DynamoNumber::parse("2.0").unwrap()),
            )]),
        )
        .unwrap();
        let number_page = database
            .query_key_condition("NumberKeys", &number_condition, None, 10)
            .await
            .unwrap();
        assert_eq!(number_page.items.len(), 1);
        assert_eq!(
            number_page.items[0]["account"],
            AttributeValue::N(DynamoNumber::parse("2").unwrap())
        );

        database
            .create_table(
                "BinaryKeys",
                KeyAttribute {
                    name: "partition".into(),
                    kind: KeyKind::Binary,
                },
                Some(KeyAttribute {
                    name: "sequence".into(),
                    kind: KeyKind::Binary,
                }),
            )
            .await
            .unwrap();
        for sequence in [vec![0, 1], vec![0, 2], vec![1], vec![1, 0]] {
            database
                .put_item(
                    "BinaryKeys",
                    Item::from([
                        ("partition".into(), AttributeValue::B(vec![0xaa, 0])),
                        ("sequence".into(), AttributeValue::B(sequence)),
                    ]),
                    None,
                )
                .await
                .unwrap();
        }
        let names = BTreeMap::from([
            ("#pk".into(), "partition".into()),
            ("#sk".into(), "sequence".into()),
        ]);
        let prefix = parse_key_condition(
            "#pk = :pk AND begins_with(#sk, :prefix)",
            &names,
            &BTreeMap::from([
                (":pk".into(), AttributeValue::B(vec![0xaa, 0])),
                (":prefix".into(), AttributeValue::B(vec![0])),
            ]),
        )
        .unwrap();
        let prefix_page = database
            .query_key_condition("BinaryKeys", &prefix, None, 10)
            .await
            .unwrap();
        assert_eq!(
            prefix_page
                .items
                .iter()
                .map(|item| item["sequence"].clone())
                .collect::<Vec<_>>(),
            vec![AttributeValue::B(vec![0, 1]), AttributeValue::B(vec![0, 2]),]
        );

        let between = parse_key_condition(
            "#pk = :pk AND #sk BETWEEN :lower AND :upper",
            &names,
            &BTreeMap::from([
                (":pk".into(), AttributeValue::B(vec![0xaa, 0])),
                (":lower".into(), AttributeValue::B(vec![0, 2])),
                (":upper".into(), AttributeValue::B(vec![1])),
            ]),
        )
        .unwrap();
        let between_page = database
            .query_key_condition_ordered("BinaryKeys", &between, None, 10, false)
            .await
            .unwrap();
        assert_eq!(
            between_page
                .items
                .iter()
                .map(|item| item["sequence"].clone())
                .collect::<Vec<_>>(),
            vec![AttributeValue::B(vec![1]), AttributeValue::B(vec![0, 2])]
        );
    });
}

#[test]
fn query_scan_enforce_one_mib_and_historical_pages_remain_pinned() {
    block_on(async {
        let store = Arc::new(MemStore::new());
        let database = Database::new_with_blob_storage(
            SyncStoreAsAsync::new(store),
            Config::default(),
            Arc::new(RecordingBlobs::default()),
            LargeValueConfig::new(1024),
        )
        .unwrap()
        .with_sources(Arc::new(SequenceIds::default()), Arc::new(FixedClock));
        database
            .create_table(
                "Records",
                KeyAttribute {
                    name: "account".into(),
                    kind: KeyKind::String,
                },
                Some(KeyAttribute {
                    name: "sequence".into(),
                    kind: KeyKind::Number,
                }),
            )
            .await
            .unwrap();

        let payload = "x".repeat(300 * 1024);
        let mut third_version = None;
        for sequence in 1..=4 {
            let mut record = key("acct-1", &sequence.to_string());
            record.insert("payload".into(), AttributeValue::S(payload.clone()));
            let update = database.put_item("Records", record, None).await.unwrap();
            if sequence == 3 {
                third_version = Some(match update {
                    VersionedMapUpdate::Applied { current, .. } => current.id,
                    other => panic!("unexpected update: {other:?}"),
                });
            }
        }
        let partition = Item::from([("account".into(), AttributeValue::S("acct-1".into()))]);

        let current = database
            .query_partition("Records", &partition, None, 1000)
            .await
            .unwrap();
        assert_eq!(current.items.len(), 3);
        assert!(current.last_evaluated_key.is_some());

        let historical = database
            .query_partition_at("Records", &third_version.unwrap(), &partition, None, 1000)
            .await
            .unwrap();
        assert_eq!(historical.items.len(), 3);
        assert!(historical.last_evaluated_key.is_none());

        let scan = database.scan("Records", None, 1000).await.unwrap();
        assert_eq!(scan.items.len(), 3);
        assert!(scan.last_evaluated_key.is_some());
    });
}

#[test]
fn moving_head_and_pinned_query_pagination_are_explicitly_distinct() {
    block_on(async {
        let database = Database::new(
            SyncStoreAsAsync::new(Arc::new(MemStore::new())),
            Config::default(),
        )
        .with_sources(Arc::new(SequenceIds::default()), Arc::new(FixedClock));
        database
            .create_table(
                "Orders",
                KeyAttribute {
                    name: "account".into(),
                    kind: KeyKind::String,
                },
                Some(KeyAttribute {
                    name: "sequence".into(),
                    kind: KeyKind::Number,
                }),
            )
            .await
            .unwrap();
        for sequence in ["1", "3"] {
            database
                .put_item("Orders", item("acct-1", sequence, "OPEN"), None)
                .await
                .unwrap();
        }
        let pinned_version = database.head("Orders").await.unwrap().id;
        let condition = parse_key_condition(
            "#pk = :pk",
            &BTreeMap::from([("#pk".into(), "account".into())]),
            &BTreeMap::from([(":pk".into(), AttributeValue::S("acct-1".into()))]),
        )
        .unwrap();
        let first = database
            .query_key_condition("Orders", &condition, None, 1)
            .await
            .unwrap();
        assert_eq!(
            first.items[0]["sequence"],
            AttributeValue::N(DynamoNumber::parse("1").unwrap())
        );

        database
            .put_item("Orders", item("acct-1", "2", "OPEN"), None)
            .await
            .unwrap();

        let moving = database
            .query_key_condition("Orders", &condition, first.last_evaluated_key.as_ref(), 1)
            .await
            .unwrap();
        assert_eq!(
            moving.items[0]["sequence"],
            AttributeValue::N(DynamoNumber::parse("2").unwrap())
        );
        assert_ne!(moving.version_id, pinned_version);

        let pinned = database
            .query_key_condition_at(
                "Orders",
                &pinned_version,
                &condition,
                first.last_evaluated_key.as_ref(),
                1,
            )
            .await
            .unwrap();
        assert_eq!(
            pinned.items[0]["sequence"],
            AttributeValue::N(DynamoNumber::parse("3").unwrap())
        );
        assert_eq!(pinned.version_id, pinned_version);
    });
}

#[test]
fn database_open_negotiates_and_rejects_tree_format_drift() {
    block_on(async {
        let backing = Arc::new(MemStore::new());
        Database::open(SyncStoreAsAsync::new(backing.clone()), Config::default())
            .await
            .unwrap();
        Database::open(SyncStoreAsAsync::new(backing.clone()), Config::default())
            .await
            .unwrap();

        let incompatible = Config::builder().max_chunk_size(128).build();
        let error = Database::open(SyncStoreAsAsync::new(backing), incompatible)
            .await
            .err()
            .expect("format drift must fail closed");
        assert!(matches!(
            error,
            prolly_dynamodb_core::Error::FormatMismatch(_)
        ));
    });
}

#[test]
fn database_open_rejects_large_value_policy_drift() {
    block_on(async {
        let backing = Arc::new(MemStore::new());
        Database::open_with_blob_storage(
            SyncStoreAsAsync::new(backing.clone()),
            Config::default(),
            Arc::new(RecordingBlobs::default()),
            LargeValueConfig::new(64 * 1024),
        )
        .await
        .unwrap();

        let error = Database::open_with_blob_storage(
            SyncStoreAsAsync::new(backing),
            Config::default(),
            Arc::new(RecordingBlobs::default()),
            LargeValueConfig::new(32 * 1024),
        )
        .await
        .err()
        .expect("large-value policy drift must fail closed");
        assert!(matches!(
            error,
            prolly_dynamodb_core::Error::FormatMismatch(_)
        ));
    });
}

#[test]
fn database_open_rejects_publication_mode_drift() {
    block_on(async {
        let backing = Arc::new(MemStore::new());
        Database::open_with_blob_storage_and_mode(
            SyncStoreAsAsync::new(backing.clone()),
            Config::default(),
            Arc::new(RecordingBlobs::default()),
            LargeValueConfig::default(),
            StoragePublicationMode::AtomicNodesAndRoots,
        )
        .await
        .unwrap();

        let error = Database::open_with_blob_storage_and_mode(
            SyncStoreAsAsync::new(backing),
            Config::default(),
            Arc::new(RecordingBlobs::default()),
            LargeValueConfig::default(),
            StoragePublicationMode::PrepublishImmutableNodes,
        )
        .await
        .err()
        .expect("publication mode drift must fail closed");
        assert!(matches!(
            error,
            prolly_dynamodb_core::Error::FormatMismatch(_)
        ));
    });
}

#[test]
fn database_open_rejects_every_durable_format_field_drift() {
    block_on(async {
        let template_store = SyncStoreAsAsync::new(Arc::new(MemStore::new()));
        let template = Database::new(template_store, Config::default())
            .format_record()
            .unwrap();

        let mut incompatible = Vec::<(&str, DatabaseFormatRecord)>::new();
        for version in [template.format_version - 1, template.format_version + 1] {
            let mut record = template.clone();
            record.format_version = version;
            incompatible.push((
                if version < template.format_version {
                    "older format version"
                } else {
                    "newer format version"
                },
                record,
            ));
        }

        let mut record = template.clone();
        record.logical_protocol_major += 1;
        incompatible.push(("logical protocol major", record));

        let mut record = template.clone();
        record.logical_protocol_minor += 1;
        incompatible.push(("logical protocol minor", record));

        type FormatMutation = fn(&mut DatabaseFormatRecord);
        let digest_mutations: [(&str, FormatMutation); 5] = [
            ("item codec digest", |record: &mut DatabaseFormatRecord| {
                record.item_codec_digest = Cid::from_bytes(b"incompatible-item-codec")
            }),
            ("key codec digest", |record: &mut DatabaseFormatRecord| {
                record.key_codec_digest = Cid::from_bytes(b"incompatible-key-codec")
            }),
            (
                "catalog codec digest",
                |record: &mut DatabaseFormatRecord| {
                    record.catalog_codec_digest = Cid::from_bytes(b"incompatible-catalog-codec")
                },
            ),
            (
                "commit codec digest",
                |record: &mut DatabaseFormatRecord| {
                    record.commit_codec_digest = Cid::from_bytes(b"incompatible-commit-codec")
                },
            ),
            ("tree format digest", |record: &mut DatabaseFormatRecord| {
                record.tree_format_digest = Cid::from_bytes(b"incompatible-tree-format")
            }),
        ];
        for (name, mutate) in digest_mutations {
            let mut record = template.clone();
            mutate(&mut record);
            incompatible.push((name, record));
        }

        let mut record = template.clone();
        record.publication_mode = StoragePublicationMode::PrepublishImmutableNodes;
        incompatible.push(("publication mode", record));

        let mut record = template.clone();
        record.large_value_inline_threshold -= 1;
        incompatible.push(("large-value inline threshold", record));

        let mut record = template.clone();
        record.minimum_reader_version -= 1;
        incompatible.push(("minimum reader version", record));

        let mut record = template.clone();
        record.minimum_writer_version -= 1;
        incompatible.push(("minimum writer version", record));

        for (name, stored_record) in incompatible {
            let backing = Arc::new(MemStore::new());
            let database =
                Database::open(SyncStoreAsAsync::new(backing.clone()), Config::default())
                    .await
                    .unwrap();
            database
                .engine()
                .versioned_map(b"dynamodb/format/v1")
                .apply(vec![Mutation::Upsert {
                    key: b"database".to_vec(),
                    val: stored_record.encode(),
                }])
                .await
                .unwrap();
            drop(database);

            let error = Database::open(SyncStoreAsAsync::new(backing), Config::default())
                .await
                .err()
                .unwrap_or_else(|| panic!("{name} drift must fail closed"));
            assert!(
                matches!(error, prolly_dynamodb_core::Error::FormatMismatch(_)),
                "{name} drift returned {error:?}"
            );
        }
    });
}

#[test]
fn conditional_write_is_evaluated_in_the_same_transaction_as_publication() {
    block_on(async {
        let store = SyncStoreAsAsync::new(Arc::new(MemStore::new()));
        let database = Database::new(store, Config::default())
            .with_sources(Arc::new(SequenceIds::default()), Arc::new(FixedClock));
        database
            .create_table(
                "Orders",
                KeyAttribute {
                    name: "account".into(),
                    kind: KeyKind::String,
                },
                Some(KeyAttribute {
                    name: "sequence".into(),
                    kind: KeyKind::Number,
                }),
            )
            .await
            .unwrap();
        database
            .put_item("Orders", item("acct-1", "1", "OPEN"), None)
            .await
            .unwrap();
        let pinned = database
            .get_item_with_version(
                "Orders",
                &Item::from([
                    ("account".into(), AttributeValue::S("acct-1".into())),
                    (
                        "sequence".into(),
                        AttributeValue::N(DynamoNumber::parse("1").unwrap()),
                    ),
                ]),
            )
            .await
            .unwrap();
        assert_eq!(pinned.version_id, database.head("Orders").await.unwrap().id);
        assert_eq!(
            pinned.item.unwrap().get("status"),
            Some(&AttributeValue::S("OPEN".into()))
        );
        let before = database.head("Orders").await.unwrap();

        let result = database
            .put_item_conditionally(
                "Orders",
                item("acct-1", "1", "CLOSED"),
                None,
                &Condition::Equals {
                    name: "status".into(),
                    value: AttributeValue::S("PENDING".into()),
                },
            )
            .await;
        let Err(prolly_dynamodb_core::Error::ConditionalCheckFailed { old_item }) = result else {
            panic!("expected conditional failure with old image")
        };
        assert_eq!(
            old_item.unwrap().get("status"),
            Some(&AttributeValue::S("OPEN".into()))
        );
        assert_eq!(database.head("Orders").await.unwrap().id, before.id);

        let replaced = database
            .put_item_with_old("Orders", item("acct-1", "1", "CLOSED"), None)
            .await
            .unwrap();
        assert_eq!(
            replaced.old_item.unwrap().get("status"),
            Some(&AttributeValue::S("OPEN".into()))
        );
        let deleted = database
            .delete_item_with_old("Orders", &key("acct-1", "1"), None)
            .await
            .unwrap();
        assert_eq!(
            deleted.old_item.unwrap().get("status"),
            Some(&AttributeValue::S("CLOSED".into()))
        );
    });
}

#[test]
fn update_item_condition_plan_and_publication_are_one_atomic_operation() {
    block_on(async {
        let store = SyncStoreAsAsync::new(Arc::new(MemStore::new()));
        let database = Database::new(store, Config::default())
            .with_sources(Arc::new(SequenceIds::default()), Arc::new(FixedClock));
        database
            .create_table(
                "Counters",
                KeyAttribute {
                    name: "account".into(),
                    kind: KeyKind::String,
                },
                None,
            )
            .await
            .unwrap();
        let original = Item::from([
            ("account".into(), AttributeValue::S("acct-1".into())),
            (
                "count".into(),
                AttributeValue::N(DynamoNumber::parse("99999999999999999999.99").unwrap()),
            ),
            ("state".into(), AttributeValue::S("OPEN".into())),
        ]);
        database
            .put_item("Counters", original.clone(), None)
            .await
            .unwrap();

        let parsed = parse_update(
            "SET #count = #count + :delta, #state = :closed",
            Some("#state = :open"),
            &std::collections::BTreeMap::from([
                ("#count".into(), "count".into()),
                ("#state".into(), "state".into()),
            ]),
            &std::collections::BTreeMap::from([
                (
                    ":delta".into(),
                    AttributeValue::N(DynamoNumber::parse("0.01").unwrap()),
                ),
                (":closed".into(), AttributeValue::S("CLOSED".into())),
                (":open".into(), AttributeValue::S("OPEN".into())),
            ]),
        )
        .unwrap();
        let key = Item::from([("account".into(), AttributeValue::S("acct-1".into()))]);
        let result = database
            .update_item(
                "Counters",
                &key,
                None,
                parsed.condition.as_ref(),
                &parsed.plan,
            )
            .await
            .unwrap();
        assert_eq!(result.old_item, Some(original.clone()));
        let updated = result.new_item.unwrap();
        assert_eq!(
            updated.get("count"),
            Some(&AttributeValue::N(
                DynamoNumber::parse("100000000000000000000").unwrap()
            ))
        );
        assert_eq!(
            updated.get("state"),
            Some(&AttributeValue::S("CLOSED".into()))
        );
        assert_eq!(
            database.get_item("Counters", &key).await.unwrap(),
            Some(updated)
        );

        let before = database.head("Counters").await.unwrap();
        let failed = database
            .update_item(
                "Counters",
                &key,
                None,
                parsed.condition.as_ref(),
                &parsed.plan,
            )
            .await;
        assert!(matches!(
            failed,
            Err(prolly_dynamodb_core::Error::ConditionalCheckFailed { .. })
        ));
        assert_eq!(database.head("Counters").await.unwrap().id, before.id);
    });
}
