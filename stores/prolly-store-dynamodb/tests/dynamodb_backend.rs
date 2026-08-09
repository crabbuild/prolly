use std::sync::atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use aws_smithy_runtime_api::box_error::BoxError;
use aws_smithy_runtime_api::client::interceptors::context::{
    AfterDeserializationInterceptorContextRef, BeforeSerializationInterceptorContextRef,
};
use aws_smithy_runtime_api::client::interceptors::Intercept;
use aws_smithy_types::config_bag::ConfigBag;

const OPERATION_NONE: u8 = 0;
const OPERATION_BATCH_WRITE: u8 = 1;
const OPERATION_TRANSACTION_WRITE: u8 = 2;

#[derive(Clone, Copy, Debug)]
enum PublicationFaultCut {
    AfterFirstBatchAcceptance,
    BeforeEveryTransactionExecution,
    AfterEveryTransactionAcceptance,
    BlockAndLoseFirstTransactionAcceptance,
}

#[derive(Debug, Default)]
struct PublicationBlockState {
    entered: bool,
    released: bool,
}

#[derive(Debug, Default)]
struct PublicationFaultState {
    last_operation: AtomicU8,
    batch_executions: AtomicUsize,
    transaction_executions: AtomicUsize,
    first_batch_response_lost: AtomicBool,
    transaction_tokens: Mutex<Vec<String>>,
    transaction_block: Mutex<PublicationBlockState>,
    transaction_block_changed: Condvar,
}

impl PublicationFaultState {
    fn wait_until_transaction_is_blocked(&self) {
        let mut block = self.transaction_block.lock().unwrap();
        while !block.entered {
            block = self.transaction_block_changed.wait(block).unwrap();
        }
    }

    fn release_blocked_transaction(&self) {
        let mut block = self.transaction_block.lock().unwrap();
        block.released = true;
        self.transaction_block_changed.notify_all();
    }
}

#[derive(Debug)]
struct PublicationFaultInjector {
    cut: PublicationFaultCut,
    state: Arc<PublicationFaultState>,
}

impl Intercept for PublicationFaultInjector {
    fn name(&self) -> &'static str {
        "PublicationFaultInjector"
    }

    fn read_before_execution(
        &self,
        context: &BeforeSerializationInterceptorContextRef<'_>,
        _cfg: &mut ConfigBag,
    ) -> Result<(), BoxError> {
        let input = context.inner().input();
        if input.is_some_and(|input| {
            input
                .downcast_ref::<aws_sdk_dynamodb::operation::batch_write_item::BatchWriteItemInput>(
                )
                .is_some()
        }) {
            self.state
                .last_operation
                .store(OPERATION_BATCH_WRITE, Ordering::SeqCst);
            self.state.batch_executions.fetch_add(1, Ordering::SeqCst);
            return Ok(());
        }

        if let Some(input) = input.and_then(|input| {
            input.downcast_ref::<
                aws_sdk_dynamodb::operation::transact_write_items::TransactWriteItemsInput,
            >()
        }) {
            self.state
                .last_operation
                .store(OPERATION_TRANSACTION_WRITE, Ordering::SeqCst);
            self.state
                .transaction_executions
                .fetch_add(1, Ordering::SeqCst);
            self.state.transaction_tokens.lock().unwrap().push(
                input
                    .client_request_token()
                    .expect("provider transactions must carry an idempotency token")
                    .to_owned(),
            );
            if matches!(
                self.cut,
                PublicationFaultCut::BeforeEveryTransactionExecution
            ) {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::ConnectionReset,
                    "injected loss before transaction execution",
                )
                .into());
            }
            return Ok(());
        }

        self.state
            .last_operation
            .store(OPERATION_NONE, Ordering::SeqCst);
        Ok(())
    }

    fn read_after_deserialization(
        &self,
        context: &AfterDeserializationInterceptorContextRef<'_>,
        _runtime_components: &aws_sdk_dynamodb::config::RuntimeComponents,
        _cfg: &mut ConfigBag,
    ) -> Result<(), BoxError> {
        if context.output_or_error().is_err() {
            return Ok(());
        }
        let operation = self.state.last_operation.load(Ordering::SeqCst);
        let lose_response = match self.cut {
            PublicationFaultCut::AfterFirstBatchAcceptance => {
                operation == OPERATION_BATCH_WRITE
                    && !self
                        .state
                        .first_batch_response_lost
                        .swap(true, Ordering::SeqCst)
            }
            PublicationFaultCut::BeforeEveryTransactionExecution => false,
            PublicationFaultCut::AfterEveryTransactionAcceptance => {
                operation == OPERATION_TRANSACTION_WRITE
            }
            PublicationFaultCut::BlockAndLoseFirstTransactionAcceptance => {
                if operation != OPERATION_TRANSACTION_WRITE {
                    false
                } else {
                    let mut block = self.state.transaction_block.lock().unwrap();
                    if block.entered {
                        false
                    } else {
                        block.entered = true;
                        self.state.transaction_block_changed.notify_all();
                        while !block.released {
                            block = self.state.transaction_block_changed.wait(block).unwrap();
                        }
                        true
                    }
                }
            }
        };
        if lose_response {
            return Err(std::io::Error::new(
                std::io::ErrorKind::ConnectionReset,
                "injected loss after provider acceptance",
            )
            .into());
        }
        Ok(())
    }
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap()
}

fn unique_prefix(provider: &str) -> Vec<u8> {
    format!(
        "prolly:test:{provider}:{}:",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
    .into_bytes()
}

fn env_var(primary: &str, legacy: &str) -> Option<String> {
    std::env::var(primary)
        .or_else(|_| std::env::var(legacy))
        .ok()
}

#[test]
fn dynamodb_backend_satisfies_remote_backend_contract_when_table_is_set() {
    let Some(table_name) = env_var(
        "PROLLY_STORE_DYNAMODB_TABLE",
        "PROLLY_ADAPTERS_DYNAMODB_TABLE",
    ) else {
        return;
    };

    runtime().block_on(async {
        use prolly::remote_conformance::{
            assert_remote_backend_async_indexed_map_contract, assert_remote_backend_contract,
            assert_remote_backend_indexed_map_contract, assert_remote_backend_transaction_contract,
        };
        use prolly_store_dynamodb::{DynamoDbBackend, TransactionPublicationMode};

        let client = dynamodb_client().await;
        let backend = DynamoDbBackend::new(client, table_name)
            .with_key_prefix(unique_prefix("dynamodb"))
            .with_transaction_publication_mode(TransactionPublicationMode::AtomicNodesAndRoots);

        backend.initialize_schema().await.unwrap();
        backend.clear_namespace().await.unwrap();
        assert_remote_backend_contract(&backend).await;
        assert_remote_backend_transaction_contract(&backend).await;
        backend.clear_namespace().await.unwrap();
        assert_remote_backend_async_indexed_map_contract(backend.clone()).await;
        backend.clear_namespace().await.unwrap();
        assert_remote_backend_indexed_map_contract(backend.clone());
        backend.clear_namespace().await.unwrap();
    });
}

#[test]
fn dynamodb_backend_immutable_prepublish_conflict_leaves_only_unreachable_content() {
    let Some(table_name) = env_var(
        "PROLLY_STORE_DYNAMODB_TABLE",
        "PROLLY_ADAPTERS_DYNAMODB_TABLE",
    ) else {
        return;
    };

    runtime().block_on(async {
        use prolly::{
            Cid, RemoteBatchOp, RemoteRootCondition, RemoteRootWrite, RemoteStoreBackend,
            RemoteTransactionConflict, RemoteTransactionUpdate,
        };
        use prolly_store_dynamodb::{DynamoDbBackend, TransactionPublicationMode};

        let client = dynamodb_client().await;
        let backend = DynamoDbBackend::new(client, table_name)
            .with_key_prefix(unique_prefix("prepublish-conflict"))
            .with_transaction_publication_mode(
                TransactionPublicationMode::PrepublishImmutableNodes,
            );
        backend.initialize_schema().await.unwrap();

        let first = b"published-node";
        let orphan = b"unreachable-conflict-node";
        let first_cid = Cid::from_bytes(first);
        let orphan_cid = Cid::from_bytes(orphan);
        let root_v1 = b"root-v1".to_vec();
        let root_v2 = b"root-v2".to_vec();
        assert_eq!(
            backend
                .commit_transaction(
                    &[RemoteBatchOp::Upsert {
                        key: first_cid.as_bytes(),
                        value: first,
                    }],
                    &[RemoteRootCondition::new(b"head".to_vec(), None)],
                    &[RemoteRootWrite::Put {
                        name: b"head".to_vec(),
                        manifest: root_v1.clone(),
                    }],
                )
                .await
                .unwrap(),
            RemoteTransactionUpdate::Applied
        );

        assert_eq!(
            backend
                .commit_transaction(
                    &[RemoteBatchOp::Upsert {
                        key: orphan_cid.as_bytes(),
                        value: orphan,
                    }],
                    &[RemoteRootCondition::new(b"head".to_vec(), None)],
                    &[RemoteRootWrite::Put {
                        name: b"head".to_vec(),
                        manifest: root_v2,
                    }],
                )
                .await
                .unwrap(),
            RemoteTransactionUpdate::Conflict(RemoteTransactionConflict::new(
                b"head".to_vec(),
                None,
                Some(root_v1.clone()),
            ))
        );
        assert_eq!(
            backend.get_root_manifest(b"head").await.unwrap(),
            Some(root_v1)
        );
        assert_eq!(
            backend.get_node(first_cid.as_bytes()).await.unwrap(),
            Some(first.to_vec())
        );
        assert_eq!(
            backend.get_node(orphan_cid.as_bytes()).await.unwrap(),
            Some(orphan.to_vec()),
            "immutable prepublication may leave only unreachable GC-safe content"
        );
        backend.clear_namespace().await.unwrap();
    });
}

#[test]
fn dynamodb_backend_atomic_conflict_publishes_neither_nodes_nor_new_root() {
    let Some(table_name) = env_var(
        "PROLLY_STORE_DYNAMODB_TABLE",
        "PROLLY_ADAPTERS_DYNAMODB_TABLE",
    ) else {
        return;
    };

    runtime().block_on(async {
        use prolly::{
            Cid, RemoteBatchOp, RemoteRootCondition, RemoteRootWrite, RemoteStoreBackend,
            RemoteTransactionConflict, RemoteTransactionUpdate,
        };
        use prolly_store_dynamodb::{DynamoDbBackend, TransactionPublicationMode};

        let backend = DynamoDbBackend::new(dynamodb_client().await, table_name)
            .with_key_prefix(unique_prefix("atomic-conflict"))
            .with_transaction_publication_mode(TransactionPublicationMode::AtomicNodesAndRoots);
        backend.initialize_schema().await.unwrap();

        let root_v1 = b"root-v1".to_vec();
        assert_eq!(
            backend
                .commit_transaction(
                    &[],
                    &[RemoteRootCondition::new(b"head".to_vec(), None)],
                    &[RemoteRootWrite::Put {
                        name: b"head".to_vec(),
                        manifest: root_v1.clone(),
                    }],
                )
                .await
                .unwrap(),
            RemoteTransactionUpdate::Applied
        );

        let rejected = b"atomic-conflict-node";
        let rejected_cid = Cid::from_bytes(rejected);
        assert_eq!(
            backend
                .commit_transaction(
                    &[RemoteBatchOp::Upsert {
                        key: rejected_cid.as_bytes(),
                        value: rejected,
                    }],
                    &[RemoteRootCondition::new(b"head".to_vec(), None)],
                    &[RemoteRootWrite::Put {
                        name: b"head".to_vec(),
                        manifest: b"root-v2".to_vec(),
                    }],
                )
                .await
                .unwrap(),
            RemoteTransactionUpdate::Conflict(RemoteTransactionConflict::new(
                b"head".to_vec(),
                None,
                Some(root_v1.clone()),
            ))
        );
        assert_eq!(
            backend.get_root_manifest(b"head").await.unwrap(),
            Some(root_v1)
        );
        assert_eq!(
            backend.get_node(rejected_cid.as_bytes()).await.unwrap(),
            None,
            "an atomic root-condition conflict must roll back its staged node"
        );
        backend.clear_namespace().await.unwrap();
    });
}

#[test]
fn dynamodb_backend_lost_immutable_prepare_response_cannot_publish_a_root() {
    let Some(table_name) = env_var(
        "PROLLY_STORE_DYNAMODB_TABLE",
        "PROLLY_ADAPTERS_DYNAMODB_TABLE",
    ) else {
        return;
    };
    let Some(endpoint) = env_var(
        "PROLLY_STORE_DYNAMODB_ENDPOINT",
        "PROLLY_ADAPTERS_DYNAMODB_ENDPOINT",
    ) else {
        eprintln!("skipping publication fault injection without DynamoDB Local endpoint");
        return;
    };

    runtime().block_on(async {
        use prolly::{
            Cid, RemoteBatchOp, RemoteRootCondition, RemoteRootWrite, RemoteStoreBackend,
        };
        use prolly_store_dynamodb::{
            DynamoDbBackend, DynamoDbBackendError, TransactionPublicationMode,
            WriteFailureDisposition,
        };

        let bootstrap = DynamoDbBackend::new(dynamodb_client().await, &table_name)
            .with_key_prefix(unique_prefix("prepare-bootstrap"));
        bootstrap.initialize_schema().await.unwrap();

        let state = Arc::new(PublicationFaultState::default());
        let backend = DynamoDbBackend::new(
            fault_injected_client(
                &endpoint,
                PublicationFaultCut::AfterFirstBatchAcceptance,
                state.clone(),
            ),
            table_name,
        )
        .with_key_prefix(unique_prefix("prepare-response-loss"))
        .with_transaction_publication_mode(TransactionPublicationMode::PrepublishImmutableNodes);
        let value = b"prepared-but-unreachable-node";
        let cid = Cid::from_bytes(value);
        let error = backend
            .commit_transaction(
                &[RemoteBatchOp::Upsert {
                    key: cid.as_bytes(),
                    value,
                }],
                &[RemoteRootCondition::new(b"head".to_vec(), None)],
                &[RemoteRootWrite::Put {
                    name: b"head".to_vec(),
                    manifest: b"must-not-be-visible".to_vec(),
                }],
            )
            .await
            .unwrap_err();
        assert!(matches!(&error, DynamoDbBackendError::Sdk(_)));
        assert_eq!(error.write_disposition(), WriteFailureDisposition::Terminal);
        assert_eq!(state.batch_executions.load(Ordering::SeqCst), 1);
        assert_eq!(state.transaction_executions.load(Ordering::SeqCst), 0);
        assert!(state.first_batch_response_lost.load(Ordering::SeqCst));
        assert_eq!(backend.get_root_manifest(b"head").await.unwrap(), None);
        assert_eq!(
            backend.get_node(cid.as_bytes()).await.unwrap(),
            Some(value.to_vec()),
            "accepted immutable preparation may leave only an unreachable GC-safe node"
        );
        backend.clear_namespace().await.unwrap();
    });
}

#[test]
fn dynamodb_backend_reports_unknown_after_every_pre_execution_attempt_fails() {
    let Some(table_name) = env_var(
        "PROLLY_STORE_DYNAMODB_TABLE",
        "PROLLY_ADAPTERS_DYNAMODB_TABLE",
    ) else {
        return;
    };
    let Some(endpoint) = env_var(
        "PROLLY_STORE_DYNAMODB_ENDPOINT",
        "PROLLY_ADAPTERS_DYNAMODB_ENDPOINT",
    ) else {
        eprintln!("skipping publication fault injection without DynamoDB Local endpoint");
        return;
    };

    runtime().block_on(async {
        use prolly::{
            Cid, RemoteBatchOp, RemoteRootCondition, RemoteRootWrite, RemoteStoreBackend,
        };
        use prolly_store_dynamodb::{
            DynamoDbBackend, DynamoDbBackendError, TransactionPublicationMode,
            WriteFailureDisposition,
        };

        let bootstrap = DynamoDbBackend::new(dynamodb_client().await, &table_name)
            .with_key_prefix(unique_prefix("pre-execution-bootstrap"));
        bootstrap.initialize_schema().await.unwrap();

        let state = Arc::new(PublicationFaultState::default());
        let backend = DynamoDbBackend::new(
            fault_injected_client(
                &endpoint,
                PublicationFaultCut::BeforeEveryTransactionExecution,
                state.clone(),
            ),
            table_name,
        )
        .with_key_prefix(unique_prefix("pre-execution-exhaustion"))
        .with_transaction_publication_mode(TransactionPublicationMode::AtomicNodesAndRoots);
        let value = b"never-executed-node";
        let cid = Cid::from_bytes(value);
        let error = backend
            .commit_transaction(
                &[RemoteBatchOp::Upsert {
                    key: cid.as_bytes(),
                    value,
                }],
                &[RemoteRootCondition::new(b"head".to_vec(), None)],
                &[RemoteRootWrite::Put {
                    name: b"head".to_vec(),
                    manifest: b"never-executed-root".to_vec(),
                }],
            )
            .await
            .unwrap_err();
        assert_eq!(
            error.write_disposition(),
            WriteFailureDisposition::OutcomeUnknown
        );
        assert!(matches!(
            &error,
            DynamoDbBackendError::OutcomeUnknown { .. }
        ));
        assert_stable_transaction_retries(&state, 3, error.transaction_token().unwrap());
        assert_eq!(backend.get_root_manifest(b"head").await.unwrap(), None);
        assert_eq!(backend.get_node(cid.as_bytes()).await.unwrap(), None);
        backend.clear_namespace().await.unwrap();
    });
}

#[test]
fn dynamodb_backend_exhausted_accepted_replays_report_unknown_but_commit_once() {
    let Some(table_name) = env_var(
        "PROLLY_STORE_DYNAMODB_TABLE",
        "PROLLY_ADAPTERS_DYNAMODB_TABLE",
    ) else {
        return;
    };
    let Some(endpoint) = env_var(
        "PROLLY_STORE_DYNAMODB_ENDPOINT",
        "PROLLY_ADAPTERS_DYNAMODB_ENDPOINT",
    ) else {
        eprintln!("skipping publication fault injection without DynamoDB Local endpoint");
        return;
    };

    runtime().block_on(async {
        use prolly::{
            Cid, RemoteBatchOp, RemoteRootCondition, RemoteRootWrite, RemoteStoreBackend,
        };
        use prolly_store_dynamodb::{
            DynamoDbBackend, DynamoDbBackendError, TransactionPublicationMode,
            WriteFailureDisposition,
        };

        let bootstrap = DynamoDbBackend::new(dynamodb_client().await, &table_name)
            .with_key_prefix(unique_prefix("accepted-exhaustion-bootstrap"));
        bootstrap.initialize_schema().await.unwrap();

        let state = Arc::new(PublicationFaultState::default());
        let backend = DynamoDbBackend::new(
            fault_injected_client(
                &endpoint,
                PublicationFaultCut::AfterEveryTransactionAcceptance,
                state.clone(),
            ),
            table_name,
        )
        .with_key_prefix(unique_prefix("accepted-exhaustion"))
        .with_transaction_publication_mode(TransactionPublicationMode::AtomicNodesAndRoots);
        let value = b"accepted-once-node";
        let cid = Cid::from_bytes(value);
        let manifest = b"accepted-once-root".to_vec();
        let error = backend
            .commit_transaction(
                &[RemoteBatchOp::Upsert {
                    key: cid.as_bytes(),
                    value,
                }],
                &[RemoteRootCondition::new(b"head".to_vec(), None)],
                &[RemoteRootWrite::Put {
                    name: b"head".to_vec(),
                    manifest: manifest.clone(),
                }],
            )
            .await
            .unwrap_err();
        assert_eq!(
            error.write_disposition(),
            WriteFailureDisposition::OutcomeUnknown
        );
        assert!(matches!(
            &error,
            DynamoDbBackendError::OutcomeUnknown { .. }
        ));
        assert_stable_transaction_retries(&state, 3, error.transaction_token().unwrap());
        assert_eq!(
            backend.get_root_manifest(b"head").await.unwrap(),
            Some(manifest)
        );
        assert_eq!(
            backend.get_node(cid.as_bytes()).await.unwrap(),
            Some(value.to_vec())
        );
        backend.clear_namespace().await.unwrap();
    });
}

#[test]
fn dynamodb_backend_task_cancellation_after_acceptance_replays_exactly_once() {
    let Some(table_name) = env_var(
        "PROLLY_STORE_DYNAMODB_TABLE",
        "PROLLY_ADAPTERS_DYNAMODB_TABLE",
    ) else {
        return;
    };
    let Some(endpoint) = env_var(
        "PROLLY_STORE_DYNAMODB_ENDPOINT",
        "PROLLY_ADAPTERS_DYNAMODB_ENDPOINT",
    ) else {
        eprintln!("skipping publication fault injection without DynamoDB Local endpoint");
        return;
    };

    runtime().block_on(async {
        use prolly::{
            Cid, RemoteBatchOp, RemoteRootCondition, RemoteRootWrite, RemoteStoreBackend,
            RemoteTransactionUpdate,
        };
        use prolly_store_dynamodb::{DynamoDbBackend, TransactionPublicationMode};

        let bootstrap = DynamoDbBackend::new(dynamodb_client().await, &table_name)
            .with_key_prefix(unique_prefix("cancellation-bootstrap"));
        bootstrap.initialize_schema().await.unwrap();

        let state = Arc::new(PublicationFaultState::default());
        let backend = DynamoDbBackend::new(
            fault_injected_client(
                &endpoint,
                PublicationFaultCut::BlockAndLoseFirstTransactionAcceptance,
                state.clone(),
            ),
            table_name,
        )
        .with_key_prefix(unique_prefix("accepted-cancellation"))
        .with_transaction_publication_mode(TransactionPublicationMode::AtomicNodesAndRoots);
        let value = b"accepted-before-cancellation";
        let cid = Cid::from_bytes(value);
        let manifest = b"accepted-before-cancellation-root".to_vec();

        let interrupted_backend = backend.clone();
        let interrupted_cid = cid.clone();
        let interrupted_manifest = manifest.clone();
        let interrupted = tokio::spawn(async move {
            interrupted_backend
                .commit_transaction(
                    &[RemoteBatchOp::Upsert {
                        key: interrupted_cid.as_bytes(),
                        value,
                    }],
                    &[RemoteRootCondition::new(b"head".to_vec(), None)],
                    &[RemoteRootWrite::Put {
                        name: b"head".to_vec(),
                        manifest: interrupted_manifest,
                    }],
                )
                .await
        });
        state.wait_until_transaction_is_blocked();
        interrupted.abort();
        state.release_blocked_transaction();
        assert!(interrupted.await.unwrap_err().is_cancelled());

        assert_eq!(
            backend
                .commit_transaction(
                    &[RemoteBatchOp::Upsert {
                        key: cid.as_bytes(),
                        value,
                    }],
                    &[RemoteRootCondition::new(b"head".to_vec(), None)],
                    &[RemoteRootWrite::Put {
                        name: b"head".to_vec(),
                        manifest: manifest.clone(),
                    }],
                )
                .await
                .unwrap(),
            RemoteTransactionUpdate::Applied
        );
        {
            let tokens = state.transaction_tokens.lock().unwrap();
            assert_eq!(tokens.len(), 2);
            assert_eq!(tokens[0], tokens[1]);
        }
        assert_eq!(
            backend.get_root_manifest(b"head").await.unwrap(),
            Some(manifest)
        );
        assert_eq!(
            backend.get_node(cid.as_bytes()).await.unwrap(),
            Some(value.to_vec())
        );
        backend.clear_namespace().await.unwrap();
    });
}

#[test]
fn dynamodb_backend_reconciles_a_response_lost_after_transaction_acceptance() {
    let Some(table_name) = env_var(
        "PROLLY_STORE_DYNAMODB_TABLE",
        "PROLLY_ADAPTERS_DYNAMODB_TABLE",
    ) else {
        return;
    };
    let Some(endpoint) = env_var(
        "PROLLY_STORE_DYNAMODB_ENDPOINT",
        "PROLLY_ADAPTERS_DYNAMODB_ENDPOINT",
    ) else {
        eprintln!("skipping post-acceptance failure injection without DynamoDB Local endpoint");
        return;
    };

    runtime().block_on(async {
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::sync::{Arc, Mutex};

        use aws_sdk_dynamodb::config::{BehaviorVersion, Credentials, Region};
        use aws_smithy_runtime_api::box_error::BoxError;
        use aws_smithy_runtime_api::client::interceptors::context::{
            AfterDeserializationInterceptorContextRef, BeforeSerializationInterceptorContextRef,
        };
        use aws_smithy_runtime_api::client::interceptors::Intercept;
        use aws_smithy_types::config_bag::ConfigBag;
        use prolly::{
            Cid, RemoteBatchOp, RemoteRootCondition, RemoteRootWrite, RemoteStoreBackend,
            RemoteTransactionUpdate,
        };
        use prolly_store_dynamodb::{DynamoDbBackend, TransactionPublicationMode};

        #[derive(Debug)]
        struct LoseFirstAcceptedTransactionResponse {
            armed: Arc<AtomicBool>,
            execution_tokens: Arc<Mutex<Vec<String>>>,
        }

        impl Intercept for LoseFirstAcceptedTransactionResponse {
            fn name(&self) -> &'static str {
                "LoseFirstAcceptedTransactionResponse"
            }

            fn read_before_execution(
                &self,
                context: &BeforeSerializationInterceptorContextRef<'_>,
                _cfg: &mut ConfigBag,
            ) -> Result<(), BoxError> {
                if let Some(input) = context.inner().input().and_then(|input| {
                    input.downcast_ref::<
                        aws_sdk_dynamodb::operation::transact_write_items::TransactWriteItemsInput,
                    >()
                }) {
                    self.execution_tokens.lock().unwrap().push(
                        input
                            .client_request_token()
                            .expect("provider transactions must carry an idempotency token")
                            .to_owned(),
                    );
                }
                Ok(())
            }

            fn read_after_deserialization(
                &self,
                context: &AfterDeserializationInterceptorContextRef<'_>,
                _runtime_components: &aws_sdk_dynamodb::config::RuntimeComponents,
                _cfg: &mut ConfigBag,
            ) -> Result<(), BoxError> {
                // The Smithy after-deserialization phase intentionally no longer
                // exposes the input. `read_before_execution` records the exact
                // transaction token, and this client performs no prior request.
                let transaction_started = !self.execution_tokens.lock().unwrap().is_empty();
                if transaction_started
                    && context.output_or_error().is_ok()
                    && self.armed.swap(false, Ordering::SeqCst)
                {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::ConnectionReset,
                        "injected loss after accepted transaction response",
                    )
                    .into());
                }
                Ok(())
            }
        }

        let normal = dynamodb_client().await;
        let bootstrap = DynamoDbBackend::new(normal, &table_name)
            .with_key_prefix(unique_prefix("post-accept-bootstrap"));
        bootstrap.initialize_schema().await.unwrap();

        let armed = Arc::new(AtomicBool::new(true));
        let execution_tokens = Arc::new(Mutex::new(Vec::new()));
        let config = aws_sdk_dynamodb::config::Builder::new()
            .behavior_version(BehaviorVersion::latest())
            .region(Region::new("us-east-1"))
            .endpoint_url(endpoint)
            .credentials_provider(Credentials::new("test", "test", None, None, "local"))
            .interceptor(LoseFirstAcceptedTransactionResponse {
                armed: armed.clone(),
                execution_tokens: execution_tokens.clone(),
            })
            .build();
        let backend = DynamoDbBackend::new(aws_sdk_dynamodb::Client::from_conf(config), table_name)
            .with_key_prefix(unique_prefix("post-accept"))
            .with_transaction_publication_mode(TransactionPublicationMode::AtomicNodesAndRoots);

        let value = b"accepted-exactly-once-node";
        let cid = Cid::from_bytes(value);
        let manifest = b"accepted-exactly-once-root".to_vec();
        let update = backend
            .commit_transaction(
                &[RemoteBatchOp::Upsert {
                    key: cid.as_bytes(),
                    value,
                }],
                &[RemoteRootCondition::new(b"head".to_vec(), None)],
                &[RemoteRootWrite::Put {
                    name: b"head".to_vec(),
                    manifest: manifest.clone(),
                }],
            )
            .await
            .unwrap();
        assert_eq!(update, RemoteTransactionUpdate::Applied);
        assert!(!armed.load(Ordering::SeqCst));
        {
            let tokens = execution_tokens.lock().unwrap();
            assert_eq!(
                tokens.len(),
                2,
                "provider must reconcile with one new SDK execution"
            );
            assert_eq!(
                tokens[0], tokens[1],
                "reconciliation must reuse the exact token"
            );
        }
        assert_eq!(
            backend.get_node(cid.as_bytes()).await.unwrap(),
            Some(value.to_vec())
        );
        assert_eq!(
            backend.get_root_manifest(b"head").await.unwrap(),
            Some(manifest)
        );
        backend.clear_namespace().await.unwrap();
    });
}

#[test]
fn dynamodb_backend_hard_cutover_ignores_legacy_primary_table_roots() {
    let Some(table_name) = env_var(
        "PROLLY_STORE_DYNAMODB_TABLE",
        "PROLLY_ADAPTERS_DYNAMODB_TABLE",
    ) else {
        return;
    };

    runtime().block_on(async {
        use aws_sdk_dynamodb::primitives::Blob;
        use aws_sdk_dynamodb::types::AttributeValue;
        use prolly::RemoteStoreBackend;
        use prolly_store_dynamodb::DynamoDbBackend;

        let client = dynamodb_client().await;
        let bootstrap = DynamoDbBackend::new(client.clone(), &table_name)
            .with_key_prefix(unique_prefix("bootstrap"));
        bootstrap.initialize_schema().await.unwrap();

        let prefix = unique_prefix("legacy-roots");
        let backend =
            DynamoDbBackend::new(client.clone(), &table_name).with_key_prefix(prefix.clone());
        let name = b"branches/main";
        let legacy_manifest = b"legacy-manifest";
        let mut legacy_key = prefix;
        legacy_key.extend_from_slice(b"root:");
        legacy_key.extend_from_slice(name);
        client
            .put_item()
            .table_name(&table_name)
            .item("pk", AttributeValue::B(Blob::new(legacy_key)))
            .item("value", AttributeValue::B(Blob::new(legacy_manifest)))
            .send()
            .await
            .unwrap();

        backend.initialize_schema().await.unwrap();
        assert_eq!(backend.get_root_manifest(name).await.unwrap(), None);
        assert!(backend.list_root_manifests().await.unwrap().is_empty());

        backend
            .put_root_manifest(name, b"current-manifest")
            .await
            .unwrap();
        assert_eq!(
            backend.get_root_manifest(name).await.unwrap(),
            Some(b"current-manifest".to_vec())
        );
        assert_eq!(
            backend.list_root_manifests().await.unwrap()[0].manifest,
            b"current-manifest"
        );

        backend.delete_root_manifest(name).await.unwrap();
        assert!(backend.list_root_manifests().await.unwrap().is_empty());
        backend.clear_namespace().await.unwrap();
    });
}

#[test]
fn dynamodb_backend_scan_pages_cross_unrelated_physical_keys_without_crossing_namespaces() {
    let Some(table_name) = env_var(
        "PROLLY_STORE_DYNAMODB_TABLE",
        "PROLLY_ADAPTERS_DYNAMODB_TABLE",
    ) else {
        return;
    };

    runtime().block_on(async {
        use std::collections::BTreeSet;

        use prolly::{Cid, RemoteStoreBackend};
        use prolly_store_dynamodb::{DynamoDbBackend, DynamoDbBackendError};

        let client = dynamodb_client().await;
        let first = DynamoDbBackend::new(client.clone(), &table_name)
            .with_key_prefix(unique_prefix("scan-page-first"));
        let second = DynamoDbBackend::new(client, table_name)
            .with_key_prefix(unique_prefix("scan-page-second"));
        first.initialize_schema().await.unwrap();

        let expected = [b"first-a".as_slice(), b"first-b".as_slice()]
            .into_iter()
            .map(|value| {
                let cid = Cid::from_bytes(value);
                (cid, value)
            })
            .collect::<Vec<_>>();
        for (cid, value) in &expected {
            first.put_node(cid.as_bytes(), value).await.unwrap();
        }
        for index in 0..16 {
            let value = format!("unrelated-{index:02}").into_bytes();
            let cid = Cid::from_bytes(&value);
            second.put_node(cid.as_bytes(), &value).await.unwrap();
        }

        let first_page = first.list_node_cids_page(None, 1).await.unwrap();
        let namespace_cursor = first_page
            .next_cursor
            .clone()
            .expect("shared physical table must have another evaluated key");
        assert!(matches!(
            second.list_node_cids_page(Some(&namespace_cursor), 1).await,
            Err(DynamoDbBackendError::InvalidConfiguration(_))
        ));

        let mut found = first_page.cids.into_iter().collect::<BTreeSet<_>>();
        let mut cursor = first_page.next_cursor;
        for _ in 0..10_000 {
            let Some(current) = cursor else {
                break;
            };
            let page = first.list_node_cids_page(Some(&current), 1).await.unwrap();
            found.extend(page.cids);
            cursor = page.next_cursor;
        }
        assert!(cursor.is_none(), "physical scan did not terminate");
        assert_eq!(
            found,
            expected
                .iter()
                .map(|(cid, _)| cid.clone())
                .collect::<BTreeSet<_>>()
        );
        first.clear_namespace().await.unwrap();
        second.clear_namespace().await.unwrap();
    });
}

async fn dynamodb_client() -> aws_sdk_dynamodb::Client {
    if let Some(endpoint) = env_var(
        "PROLLY_STORE_DYNAMODB_ENDPOINT",
        "PROLLY_ADAPTERS_DYNAMODB_ENDPOINT",
    ) {
        use aws_sdk_dynamodb::config::{BehaviorVersion, Credentials, Region};

        let region = std::env::var("AWS_REGION").unwrap_or_else(|_| "us-west-2".to_string());
        let config = aws_sdk_dynamodb::config::Builder::new()
            .behavior_version(BehaviorVersion::latest())
            .region(Region::new(region))
            .endpoint_url(endpoint)
            .credentials_provider(Credentials::new("test", "test", None, None, "local"))
            .build();
        aws_sdk_dynamodb::Client::from_conf(config)
    } else {
        let config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
        aws_sdk_dynamodb::Client::new(&config)
    }
}

fn fault_injected_client(
    endpoint: &str,
    cut: PublicationFaultCut,
    state: Arc<PublicationFaultState>,
) -> aws_sdk_dynamodb::Client {
    use aws_sdk_dynamodb::config::{BehaviorVersion, Credentials, Region};

    let region = std::env::var("AWS_REGION").unwrap_or_else(|_| "us-west-2".to_string());
    let config = aws_sdk_dynamodb::config::Builder::new()
        .behavior_version(BehaviorVersion::latest())
        .region(Region::new(region))
        .endpoint_url(endpoint)
        .credentials_provider(Credentials::new("test", "test", None, None, "local"))
        .interceptor(PublicationFaultInjector { cut, state })
        .build();
    aws_sdk_dynamodb::Client::from_conf(config)
}

fn assert_stable_transaction_retries(
    state: &PublicationFaultState,
    expected_executions: usize,
    exposed_token: &str,
) {
    assert_eq!(state.batch_executions.load(Ordering::SeqCst), 0);
    assert_eq!(
        state.transaction_executions.load(Ordering::SeqCst),
        expected_executions
    );
    let tokens = state.transaction_tokens.lock().unwrap();
    assert_eq!(tokens.len(), expected_executions);
    assert!(tokens.iter().all(|token| token == &tokens[0]));
    assert_eq!(tokens[0], exposed_token);
}
