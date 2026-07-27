use futures_util::stream::{self, StreamExt, TryStreamExt};
use prolly::{AsyncProlly, AsyncStore, Config, Diff, Mutation, Tree};
use prolly_backend_workload_contract::{
    digest_diffs, key, DiffRecord, Digest, DigestBuilder, ExpectedTree, MutationRecord, Workload,
};

use crate::{measure, EvidenceRow, Operation, RunConfig, RESULT_SCHEMA, TIMED_SCOPE_VERSION};

pub async fn run_workload<S>(
    store: S,
    config: &RunConfig,
    workload: &Workload,
) -> Result<Vec<EvidenceRow>, String>
where
    S: AsyncStore + Clone,
    S::Error: Send + Sync,
{
    config.validate()?;
    if config.workload != workload.spec {
        return Err("run configuration and generated workload differ".to_string());
    }

    let build_manager = manager(store.clone());
    let base_mutations = workload
        .base_entries()
        .map(|(key, val)| Mutation::Upsert { key, val })
        .collect::<Vec<_>>();
    let measured_build = measure(build_manager.batch(&build_manager.create(), base_mutations))
        .await
        .map_err(context("build"))?;
    let base_evidence = validate_tree(
        &build_manager,
        &measured_build.value,
        workload,
        ExpectedTree::Base,
    )
    .await?;
    let mut rows = vec![make_row(
        config,
        workload,
        Operation::Build,
        workload.spec.records,
        measured_build.elapsed_ns,
        &measured_build.value,
        base_evidence,
    )?];
    let base = measured_build.value;

    let batch_manager = manager(store.clone());
    let measured_batch =
        measure(batch_manager.batch(&base, to_prolly_mutations(&workload.batch_mutations)))
            .await
            .map_err(context("batch"))?;
    let batch_evidence = validate_tree(
        &batch_manager,
        &measured_batch.value,
        workload,
        ExpectedTree::Batch,
    )
    .await?;
    rows.push(make_row(
        config,
        workload,
        Operation::Batch,
        workload.spec.changes,
        measured_batch.elapsed_ns,
        &measured_batch.value,
        batch_evidence,
    )?);

    let query_keys = workload.query_keys();
    let query_manager = manager(store.clone());
    let measured_query = measure(query_manager.get_many(&base, &query_keys))
        .await
        .map_err(context("query"))?;
    let query_digest = validate_query(workload, &query_keys, &measured_query.value)?;
    rows.push(make_row(
        config,
        workload,
        Operation::Query,
        workload.spec.samples,
        measured_query.elapsed_ns,
        &base,
        (measured_query.value.len(), query_digest),
    )?);

    let concurrent_manager = manager(store.clone());
    let measured_concurrent = measure(async {
        stream::iter(query_keys.iter().enumerate())
            .map(|(position, key)| {
                let manager = &concurrent_manager;
                let base = &base;
                async move {
                    manager
                        .get(base, key)
                        .await
                        .map(|value| (position, value))
                        .map_err(|error| error.to_string())
                }
            })
            .buffer_unordered(workload.spec.concurrency)
            .try_collect::<Vec<_>>()
            .await
    })
    .await
    .map_err(context("concurrent query"))?;
    let mut concurrent_values = measured_concurrent.value;
    concurrent_values.sort_by_key(|(position, _)| *position);
    let concurrent_values = concurrent_values
        .into_iter()
        .map(|(_, value)| value)
        .collect::<Vec<_>>();
    let concurrent_digest = validate_query(workload, &query_keys, &concurrent_values)?;
    rows.push(make_row(
        config,
        workload,
        Operation::ConcurrentQuery,
        workload.spec.samples,
        measured_concurrent.elapsed_ns,
        &base,
        (concurrent_values.len(), concurrent_digest),
    )?);

    let diff_setup_manager = manager(store.clone());
    let diff_target = diff_setup_manager
        .batch(&base, to_prolly_mutations(&workload.diff_mutations))
        .await
        .map_err(|error| format!("diff setup failed: {error}"))?;
    let diff_manager = manager(store.clone());
    let measured_diff = measure(diff_manager.diff(&base, &diff_target))
        .await
        .map_err(context("diff"))?;
    let diff_digest = validate_diff(workload, &measured_diff.value)?;
    rows.push(make_row(
        config,
        workload,
        Operation::Diff,
        workload.spec.changes,
        measured_diff.elapsed_ns,
        &diff_target,
        (measured_diff.value.len(), diff_digest),
    )?);

    let merge_setup_manager = manager(store.clone());
    let left = merge_setup_manager
        .batch(&base, to_prolly_mutations(&workload.merge.left))
        .await
        .map_err(|error| format!("left merge setup failed: {error}"))?;
    let right = merge_setup_manager
        .batch(&base, to_prolly_mutations(&workload.merge.right))
        .await
        .map_err(|error| format!("right merge setup failed: {error}"))?;
    let merge_manager = manager(store);
    let measured_merge = measure(merge_manager.merge(&base, &left, &right, None))
        .await
        .map_err(context("merge"))?;
    let merge_evidence = validate_tree(
        &merge_manager,
        &measured_merge.value,
        workload,
        ExpectedTree::Merged,
    )
    .await?;
    rows.push(make_row(
        config,
        workload,
        Operation::Merge,
        workload.spec.changes,
        measured_merge.elapsed_ns,
        &measured_merge.value,
        merge_evidence,
    )?);

    Ok(rows)
}

fn manager<S>(store: S) -> AsyncProlly<S>
where
    S: AsyncStore,
    S::Error: Send + Sync,
{
    AsyncProlly::new(store, Config::default())
}

fn to_prolly_mutations(mutations: &[MutationRecord]) -> Vec<Mutation> {
    mutations
        .iter()
        .map(|mutation| match mutation {
            MutationRecord::Upsert { key, value } => Mutation::Upsert {
                key: key.clone(),
                val: value.clone(),
            },
            MutationRecord::Delete { key } => Mutation::Delete { key: key.clone() },
        })
        .collect()
}

async fn validate_tree<S>(
    manager: &AsyncProlly<S>,
    tree: &Tree,
    workload: &Workload,
    expected_tree: ExpectedTree,
) -> Result<(usize, Digest), String>
where
    S: AsyncStore,
    S::Error: Send + Sync,
{
    let expected_count = match expected_tree {
        ExpectedTree::Base => workload.expected.base_count,
        ExpectedTree::Batch => workload.expected.batch_count,
        ExpectedTree::DiffTarget => workload.expected.diff_target_count,
        ExpectedTree::Merged => workload.expected.merged_count,
    };
    let mut position = 0usize;
    let mut mismatch = None;
    let mut digest = DigestBuilder::new(b"entries");
    let visited = manager
        .scan_range(tree, b"", None, |entry| {
            if mismatch.is_none() {
                let expected_key = key(position);
                let expected_value = workload.expected_value(expected_tree, position);
                if entry.key() != expected_key || expected_value.as_deref() != Some(entry.value()) {
                    mismatch = Some(format!(
                        "tree content differs at position {position}: key={:?}",
                        String::from_utf8_lossy(entry.key())
                    ));
                }
            }
            digest.field(entry.key());
            digest.field(entry.value());
            position += 1;
        })
        .await
        .map_err(|error| format!("complete tree validation failed: {error}"))?;
    if let Some(error) = mismatch {
        return Err(error);
    }
    if visited as usize != expected_count || position != expected_count {
        return Err(format!(
            "tree count differs: expected {expected_count}, visited {visited}"
        ));
    }
    Ok((position, digest.finish()))
}

fn validate_query(
    workload: &Workload,
    keys: &[Vec<u8>],
    values: &[Option<Vec<u8>>],
) -> Result<Digest, String> {
    if keys.len() != workload.query_ids.len() || values.len() != keys.len() {
        return Err("query result length differs from the workload".to_string());
    }
    let mut digest = DigestBuilder::new(b"entries");
    for (position, ((id, expected_key), actual_value)) in
        workload.query_ids.iter().zip(keys).zip(values).enumerate()
    {
        if expected_key != &key(*id) {
            return Err(format!("query key differs at position {position}"));
        }
        let expected_value = workload
            .expected_value(ExpectedTree::Base, *id)
            .expect("query identifiers are in range");
        if actual_value.as_deref() != Some(expected_value.as_slice()) {
            return Err(format!("query value differs at position {position}"));
        }
        digest.field(expected_key);
        digest.field(&expected_value);
    }
    Ok(digest.finish())
}

fn validate_diff(workload: &Workload, actual: &[Diff]) -> Result<Digest, String> {
    let actual = actual.iter().map(diff_record).collect::<Vec<_>>();
    let expected = workload.expected_diff_records();
    if actual != expected {
        let position = actual
            .iter()
            .zip(&expected)
            .position(|(left, right)| left != right)
            .unwrap_or(actual.len().min(expected.len()));
        return Err(format!(
            "diff differs at position {position}: expected {} rows, observed {}",
            expected.len(),
            actual.len()
        ));
    }
    Ok(digest_diffs(&actual))
}

fn diff_record(diff: &Diff) -> DiffRecord {
    match diff {
        Diff::Added { key, val } => DiffRecord {
            key: key.clone(),
            before: None,
            after: Some(val.clone()),
        },
        Diff::Removed { key, val } => DiffRecord {
            key: key.clone(),
            before: Some(val.clone()),
            after: None,
        },
        Diff::Changed { key, old, new } => DiffRecord {
            key: key.clone(),
            before: Some(old.clone()),
            after: Some(new.clone()),
        },
    }
}

#[allow(clippy::too_many_arguments)]
fn make_row(
    config: &RunConfig,
    workload: &Workload,
    operation: Operation,
    logical_operations: usize,
    total_ns: u128,
    tree: &Tree,
    outcome: (usize, Digest),
) -> Result<EvidenceRow, String> {
    let root = tree
        .root
        .as_ref()
        .ok_or_else(|| format!("{operation} produced an empty root"))?;
    let row = EvidenceRow {
        schema: RESULT_SCHEMA.to_string(),
        timed_scope_version: TIMED_SCOPE_VERSION.to_string(),
        contract_version: workload.contract_version.clone(),
        run_id: config.run_id.clone(),
        backend: config.backend,
        repetition: config.repetition,
        operation,
        revision: config.revision.clone(),
        tree_hash: config.tree_hash.clone(),
        binary_sha256: config.binary_sha256.clone(),
        records: workload.spec.records as u64,
        value_bytes: workload.spec.value_bytes as u64,
        changes: workload.spec.changes as u64,
        samples: workload.spec.samples as u64,
        concurrency: workload.spec.concurrency as u64,
        seed: workload.spec.seed,
        logical_operations: logical_operations as u64,
        observed_items: outcome.0 as u64,
        total_ns,
        ops_per_sec: logical_operations as f64 * 1_000_000_000.0 / total_ns as f64,
        root: hex(root.as_bytes()),
        workload_digest: workload.workload_digest.to_hex(),
        outcome_digest: outcome.1.to_hex(),
        validated: true,
        error: String::new(),
    };
    row.validate()?;
    Ok(row)
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn context(operation: &'static str) -> impl FnOnce(String) -> String {
    move |error| format!("{operation} failed: {error}")
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use prolly::{MemStore, SyncStoreAsAsync};
    use prolly_backend_workload_contract::{WorkloadSpec, DEFAULT_SEED};

    use super::*;
    use crate::Backend;

    fn fixture() -> (RunConfig, Workload) {
        let spec = WorkloadSpec {
            records: 100,
            value_bytes: 27,
            changes: 10,
            samples: 10,
            concurrency: 4,
            seed: DEFAULT_SEED,
        };
        (
            RunConfig {
                backend: Backend::Postgres,
                output: PathBuf::from("unused.csv"),
                run_id: "run-1".to_string(),
                repetition: 1,
                revision: "a".repeat(40),
                tree_hash: "b".repeat(40),
                binary_sha256: "c".repeat(64),
                workload: spec,
            },
            Workload::generate(spec).unwrap(),
        )
    }

    #[tokio::test]
    async fn common_runner_validates_every_operation() {
        let (config, workload) = fixture();
        let rows = run_workload(
            SyncStoreAsAsync::new(std::sync::Arc::new(MemStore::new())),
            &config,
            &workload,
        )
        .await
        .unwrap();

        assert_eq!(
            rows.iter().map(|row| row.operation).collect::<Vec<_>>(),
            Operation::ALL
        );
        assert!(rows.iter().all(|row| row.validated));
        assert_eq!(rows[0].root, rows[2].root);
        assert_eq!(rows[2].outcome_digest, rows[3].outcome_digest);
    }

    #[test]
    fn query_validation_rejects_one_wrong_byte() {
        let (_, workload) = fixture();
        let keys = workload.query_keys();
        let mut values = workload
            .query_ids
            .iter()
            .map(|id| workload.expected_value(ExpectedTree::Base, *id))
            .collect::<Vec<_>>();
        values[3].as_mut().unwrap()[0] ^= 1;
        assert!(validate_query(&workload, &keys, &values)
            .unwrap_err()
            .contains("position 3"));
    }
}
