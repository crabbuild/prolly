use std::collections::BTreeMap;
use std::error::Error as StdError;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use aws_sdk_dynamodb::config::{BehaviorVersion, Credentials, Region};
use prolly::{
    resolver, AsyncProlly, Cid, Config, Conflict, Diff, MemStore, Mutation, NodeStoreScan, Prolly,
    RemoteProllyStore, RemoteStoreBackend, SnapshotBundle, Store,
};
use prolly_store_dynamodb::DynamoDbBackend;
use prolly_store_postgres::PostgresBackend;

const DEFAULT_RECORDS: usize = 10_000;
const DEFAULT_CHANGES_PER_KIND: usize = 200;
const VALUE_BYTES: usize = 47;

type State = BTreeMap<Vec<u8>, Vec<u8>>;
type ConflictRecord = (Vec<u8>, Option<Vec<u8>>, Option<Vec<u8>>, Option<Vec<u8>>);

#[derive(Debug)]
struct Fixture {
    base_entries: Vec<(Vec<u8>, Vec<u8>)>,
    base_state: State,
    left_mutations: Vec<Mutation>,
    left_state: State,
    right_mutations: Vec<Mutation>,
    right_state: State,
    merged_state: State,
    conflict_left_mutations: Vec<Mutation>,
    conflict_right_mutations: Vec<Mutation>,
    conflict_right_state: State,
}

#[derive(Debug)]
struct Evidence {
    roots: Vec<Option<Cid>>,
    diffs: Vec<Vec<Diff>>,
    snapshots: Vec<SnapshotBundle>,
    states: Vec<Vec<(Vec<u8>, Vec<u8>)>>,
    conflicts: Vec<ConflictRecord>,
    raw_nodes: BTreeMap<Vec<u8>, Vec<u8>>,
}

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("backend correctness verification failed: {error}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), String> {
    let records = env_usize("PROLLY_CORRECTNESS_RECORDS", DEFAULT_RECORDS)?;
    let changes = env_usize(
        "PROLLY_CORRECTNESS_CHANGES_PER_KIND",
        DEFAULT_CHANGES_PER_KIND,
    )?;
    if records < changes.saturating_mul(4).saturating_add(2) {
        return Err(format!(
            "PROLLY_CORRECTNESS_RECORDS must be at least {}",
            changes.saturating_mul(4).saturating_add(2)
        ));
    }

    let fixture = fixture(records, changes);
    let oracle = exercise_sync(&fixture)?;

    let postgres_url = std::env::var("PROLLY_STORE_POSTGRES_URL")
        .unwrap_or_else(|_| "postgres://prolly:prolly@127.0.0.1:55432/prolly".to_string());
    let postgres = PostgresBackend::connect(&postgres_url)
        .await
        .map_err(error)?;
    postgres.initialize_schema().await.map_err(error)?;
    sqlx::query("TRUNCATE TABLE prolly_nodes, prolly_hints, prolly_roots")
        .execute(postgres.pool())
        .await
        .map_err(error)?;
    let postgres_evidence = exercise_async(postgres.clone(), &fixture).await?;

    let dynamodb_endpoint = std::env::var("PROLLY_STORE_DYNAMODB_ENDPOINT")
        .unwrap_or_else(|_| "http://127.0.0.1:8000".to_string());
    let dynamodb_table = std::env::var("PROLLY_STORE_DYNAMODB_TABLE")
        .unwrap_or_else(|_| "prolly_backend_correctness".to_string());
    let dynamodb_config = aws_sdk_dynamodb::config::Builder::new()
        .behavior_version(BehaviorVersion::latest())
        .region(Region::new("us-west-2"))
        .endpoint_url(dynamodb_endpoint)
        .credentials_provider(Credentials::new("test", "test", None, None, "local"))
        .build();
    let prefix = format!(
        "prolly:correctness:{}:",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    )
    .into_bytes();
    let dynamodb = DynamoDbBackend::new(
        aws_sdk_dynamodb::Client::from_conf(dynamodb_config),
        dynamodb_table,
    )
    .with_key_prefix(prefix)
    .with_read_parallelism(16)
    .with_batch_get_parallelism(16)
    .with_batch_write_parallelism(16)
    .with_scan_parallelism(8);
    dynamodb.initialize_schema().await.map_err(error)?;
    dynamodb.clear_namespace().await.map_err(error)?;
    let dynamodb_evidence = exercise_async(dynamodb.clone(), &fixture).await?;

    compare_evidence("PostgreSQL", &oracle, &postgres_evidence)?;
    compare_evidence("DynamoDB", &oracle, &dynamodb_evidence)?;
    compare_evidence(
        "PostgreSQL vs DynamoDB",
        &postgres_evidence,
        &dynamodb_evidence,
    )?;

    let merged = &oracle.snapshots[3];
    let stored_bytes = oracle.raw_nodes.values().map(Vec::len).sum::<usize>();
    println!("result=byte-for-byte-identical");
    println!("records={records}");
    println!("changes_per_kind_per_branch={changes}");
    println!("base_root={}", root_hex(&oracle.roots[0]));
    println!("merged_root={}", root_hex(&oracle.roots[3]));
    println!("conflict_merged_root={}", root_hex(&oracle.roots[6]));
    println!("left_diff_entries={}", oracle.diffs[0].len());
    println!("right_diff_entries={}", oracle.diffs[1].len());
    println!("merged_diff_entries={}", oracle.diffs[2].len());
    println!("conflicts_observed={}", oracle.conflicts.len());
    println!("merged_reachable_nodes={}", merged.nodes.len());
    println!("all_stored_nodes={}", oracle.raw_nodes.len());
    println!("all_stored_node_bytes={stored_bytes}");

    dynamodb.clear_namespace().await.map_err(error)?;
    Ok(())
}

fn fixture(records: usize, changes: usize) -> Fixture {
    let mut base_entries = (0..records)
        .map(|index| (key(index), value(index, 0)))
        .collect::<Vec<_>>();
    shuffle(&mut base_entries, 0x6a09_e667_f3bc_c909);
    for index in 0..32.min(records) {
        base_entries.push((key(index), value(index, 1)));
    }
    let base_state = state_from_entries(&base_entries);

    let mut left_mutations = Vec::with_capacity(changes * 3 + 32);
    let mut right_mutations = Vec::with_capacity(changes * 3 + 32);
    for index in 0..changes {
        left_mutations.push(Mutation::Upsert {
            key: key(index * 4),
            val: value(index * 4, 2),
        });
        left_mutations.push(Mutation::Delete {
            key: key(changes * 4 + index * 4),
        });
        left_mutations.push(Mutation::Upsert {
            key: key(records + index * 2),
            val: value(records + index * 2, 2),
        });

        right_mutations.push(Mutation::Upsert {
            key: key(index * 4 + 1),
            val: value(index * 4 + 1, 3),
        });
        right_mutations.push(Mutation::Delete {
            key: key(changes * 4 + index * 4 + 1),
        });
        right_mutations.push(Mutation::Upsert {
            key: key(records + index * 2 + 1),
            val: value(records + index * 2 + 1, 3),
        });
    }
    shuffle(&mut left_mutations, 0xbb67_ae85_84ca_a73b);
    shuffle(&mut right_mutations, 0x3c6e_f372_fe94_f82b);
    for index in 0..32.min(changes) {
        left_mutations.push(Mutation::Upsert {
            key: key(index * 4),
            val: value(index * 4, 4),
        });
        right_mutations.push(Mutation::Upsert {
            key: key(index * 4 + 1),
            val: value(index * 4 + 1, 5),
        });
    }

    let left_state = apply(&base_state, &left_mutations);
    let right_state = apply(&base_state, &right_mutations);
    let merged_state = apply(&left_state, &right_mutations);
    let conflict_left_mutations = vec![
        Mutation::Upsert {
            key: key(42),
            val: value(42, 10),
        },
        Mutation::Delete { key: key(43) },
    ];
    let conflict_right_mutations = vec![
        Mutation::Upsert {
            key: key(42),
            val: value(42, 11),
        },
        Mutation::Upsert {
            key: key(43),
            val: value(43, 12),
        },
    ];
    let conflict_right_state = apply(&base_state, &conflict_right_mutations);

    Fixture {
        base_entries,
        base_state,
        left_mutations,
        left_state,
        right_mutations,
        right_state,
        merged_state,
        conflict_left_mutations,
        conflict_right_mutations,
        conflict_right_state,
    }
}

fn exercise_sync(fixture: &Fixture) -> Result<Evidence, String> {
    let manager = Prolly::new(MemStore::new(), Config::default());
    let base = manager
        .build_from_entries(fixture.base_entries.clone())
        .map_err(error)?;
    let left = manager
        .batch(&base, fixture.left_mutations.clone())
        .map_err(error)?;
    let right = manager
        .batch(&base, fixture.right_mutations.clone())
        .map_err(error)?;
    let merged = manager.merge(&base, &left, &right, None).map_err(error)?;
    let conflict_left = manager
        .batch(&base, fixture.conflict_left_mutations.clone())
        .map_err(error)?;
    let conflict_right = manager
        .batch(&base, fixture.conflict_right_mutations.clone())
        .map_err(error)?;
    let conflicts = Arc::new(Mutex::new(Vec::new()));
    let captured = conflicts.clone();
    let conflict_merged = manager
        .merge(
            &base,
            &conflict_left,
            &conflict_right,
            Some(Box::new(move |conflict| {
                captured.lock().unwrap().push(conflict_record(conflict));
                resolver::prefer_right(conflict)
            })),
        )
        .map_err(error)?;

    let trees = [
        &base,
        &left,
        &right,
        &merged,
        &conflict_left,
        &conflict_right,
        &conflict_merged,
    ];
    let states = trees
        .iter()
        .map(|tree| {
            manager
                .range(tree, &[], None)
                .map_err(error)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(error)
        })
        .collect::<Result<Vec<_>, _>>()?;
    verify_logical_states(fixture, &states)?;
    let diffs = vec![
        manager.diff(&base, &left).map_err(error)?,
        manager.diff(&base, &right).map_err(error)?,
        manager.diff(&base, &merged).map_err(error)?,
        manager.diff(&base, &conflict_merged).map_err(error)?,
    ];
    verify_diffs(fixture, &diffs)?;
    let snapshots = trees
        .iter()
        .map(|tree| manager.export_snapshot(tree).map_err(error))
        .collect::<Result<Vec<_>, _>>()?;
    for snapshot in &snapshots {
        if !snapshot.verify().map_err(error)?.valid {
            return Err("sync oracle produced an invalid snapshot".to_string());
        }
    }
    let raw_nodes = manager
        .store()
        .list_node_cids()
        .map_err(error)?
        .into_iter()
        .map(|cid| {
            let bytes = manager
                .store()
                .get(cid.as_bytes())
                .map_err(error)?
                .ok_or_else(|| format!("oracle listed missing CID {}", hex(cid.as_bytes())))?;
            if Cid::from_bytes(&bytes) != cid {
                return Err(format!("oracle stored corrupt CID {}", hex(cid.as_bytes())));
            }
            Ok((cid.as_bytes().to_vec(), bytes))
        })
        .collect::<Result<BTreeMap<_, _>, String>>()?;

    let conflicts = conflicts.lock().unwrap().clone();
    Ok(Evidence {
        roots: trees.iter().map(|tree| tree.root.clone()).collect(),
        diffs,
        snapshots,
        states,
        conflicts,
        raw_nodes,
    })
}

async fn exercise_async<B>(backend: B, fixture: &Fixture) -> Result<Evidence, String>
where
    B: RemoteStoreBackend + Clone,
{
    let manager = AsyncProlly::new(RemoteProllyStore::new(backend.clone()), Config::default());
    let base = manager
        .build_from_entries(fixture.base_entries.clone())
        .await
        .map_err(error)?;
    let left = manager
        .batch(&base, fixture.left_mutations.clone())
        .await
        .map_err(error)?;
    let right = manager
        .batch(&base, fixture.right_mutations.clone())
        .await
        .map_err(error)?;
    let merged = manager
        .merge(&base, &left, &right, None)
        .await
        .map_err(error)?;
    let conflict_left = manager
        .batch(&base, fixture.conflict_left_mutations.clone())
        .await
        .map_err(error)?;
    let conflict_right = manager
        .batch(&base, fixture.conflict_right_mutations.clone())
        .await
        .map_err(error)?;
    let conflicts = Arc::new(Mutex::new(Vec::new()));
    let captured = conflicts.clone();
    let conflict_merged = manager
        .merge(
            &base,
            &conflict_left,
            &conflict_right,
            Some(Box::new(move |conflict| {
                captured.lock().unwrap().push(conflict_record(conflict));
                resolver::prefer_right(conflict)
            })),
        )
        .await
        .map_err(error)?;

    let trees = [
        &base,
        &left,
        &right,
        &merged,
        &conflict_left,
        &conflict_right,
        &conflict_merged,
    ];
    let mut states = Vec::with_capacity(trees.len());
    for tree in trees {
        states.push(
            manager
                .range(tree, &[], None)
                .await
                .map_err(error)?
                .collect()
                .await
                .map_err(error)?,
        );
    }
    let diffs = vec![
        manager.diff(&base, &left).await.map_err(error)?,
        manager.diff(&base, &right).await.map_err(error)?,
        manager.diff(&base, &merged).await.map_err(error)?,
        manager.diff(&base, &conflict_merged).await.map_err(error)?,
    ];
    verify_logical_states(fixture, &states)?;
    verify_diffs(fixture, &diffs)?;

    let cold = AsyncProlly::new(RemoteProllyStore::new(backend.clone()), Config::default());
    let mut snapshots = Vec::with_capacity(trees.len());
    for tree in trees {
        let hot = manager.export_snapshot(tree).await.map_err(error)?;
        let cold_snapshot = cold.export_snapshot(tree).await.map_err(error)?;
        if hot != cold_snapshot {
            return Err(format!(
                "hot/cold snapshot mismatch for root {}",
                root_hex(&tree.root)
            ));
        }
        if !cold_snapshot.verify().map_err(error)?.valid {
            return Err(format!(
                "invalid snapshot for root {}",
                root_hex(&tree.root)
            ));
        }
        snapshots.push(cold_snapshot);
    }

    let cids = backend.list_node_cids().await.map_err(error)?;
    let keys = cids.iter().map(Vec::as_slice).collect::<Vec<_>>();
    let values = backend
        .batch_get_nodes_ordered(&keys)
        .await
        .map_err(error)?;
    let mut raw_nodes = BTreeMap::new();
    for (cid, value) in cids.into_iter().zip(values) {
        let bytes = value.ok_or_else(|| format!("backend listed missing CID {}", hex(&cid)))?;
        if Cid::from_bytes(&bytes).as_bytes() != cid {
            return Err(format!("backend stored corrupt CID {}", hex(&cid)));
        }
        raw_nodes.insert(cid, bytes);
    }

    let conflicts = conflicts.lock().unwrap().clone();
    Ok(Evidence {
        roots: trees.iter().map(|tree| tree.root.clone()).collect(),
        diffs,
        snapshots,
        states,
        conflicts,
        raw_nodes,
    })
}

fn verify_logical_states(
    fixture: &Fixture,
    states: &[Vec<(Vec<u8>, Vec<u8>)>],
) -> Result<(), String> {
    let expected = [
        &fixture.base_state,
        &fixture.left_state,
        &fixture.right_state,
        &fixture.merged_state,
        &apply(&fixture.base_state, &fixture.conflict_left_mutations),
        &fixture.conflict_right_state,
        &fixture.conflict_right_state,
    ];
    for (index, (actual, expected)) in states.iter().zip(expected).enumerate() {
        let expected = expected
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect::<Vec<_>>();
        if *actual != expected {
            return Err(format!("logical state mismatch at tree index {index}"));
        }
    }
    Ok(())
}

fn verify_diffs(fixture: &Fixture, diffs: &[Vec<Diff>]) -> Result<(), String> {
    let expected = [
        expected_diff(&fixture.base_state, &fixture.left_state),
        expected_diff(&fixture.base_state, &fixture.right_state),
        expected_diff(&fixture.base_state, &fixture.merged_state),
        expected_diff(&fixture.base_state, &fixture.conflict_right_state),
    ];
    for (index, (actual, expected)) in diffs.iter().zip(expected).enumerate() {
        if *actual != expected {
            let first = actual
                .iter()
                .zip(&expected)
                .position(|(actual, expected)| actual != expected)
                .unwrap_or_else(|| actual.len().min(expected.len()));
            return Err(format!(
                "diff mismatch at diff index {index}, entry {first}: actual_count={}, expected_count={}",
                actual.len(),
                expected.len()
            ));
        }
    }
    Ok(())
}

fn compare_evidence(label: &str, expected: &Evidence, actual: &Evidence) -> Result<(), String> {
    if expected.roots != actual.roots {
        return Err(format!("{label}: tree root mismatch"));
    }
    if expected.diffs != actual.diffs {
        return Err(format!("{label}: ordered diff payload mismatch"));
    }
    if expected.states != actual.states {
        return Err(format!("{label}: logical key/value state mismatch"));
    }
    if expected.conflicts != actual.conflicts {
        return Err(format!("{label}: merge conflict payload mismatch"));
    }
    for (index, (expected, actual)) in expected.snapshots.iter().zip(&actual.snapshots).enumerate()
    {
        if expected != actual {
            return Err(format!(
                "{label}: reachable snapshot mismatch at tree index {index}"
            ));
        }
    }
    if expected.raw_nodes != actual.raw_nodes {
        let first = expected
            .raw_nodes
            .iter()
            .zip(&actual.raw_nodes)
            .position(|(expected, actual)| expected != actual)
            .unwrap_or_else(|| expected.raw_nodes.len().min(actual.raw_nodes.len()));
        return Err(format!(
            "{label}: raw node table mismatch at sorted entry {first}: expected_count={}, actual_count={}",
            expected.raw_nodes.len(),
            actual.raw_nodes.len()
        ));
    }
    Ok(())
}

fn expected_diff(base: &State, other: &State) -> Vec<Diff> {
    let mut keys = base.keys().chain(other.keys()).cloned().collect::<Vec<_>>();
    keys.sort();
    keys.dedup();
    keys.into_iter()
        .filter_map(|key| match (base.get(&key), other.get(&key)) {
            (None, Some(value)) => Some(Diff::Added {
                key,
                val: value.clone(),
            }),
            (Some(value), None) => Some(Diff::Removed {
                key,
                val: value.clone(),
            }),
            (Some(old), Some(new)) if old != new => Some(Diff::Changed {
                key,
                old: old.clone(),
                new: new.clone(),
            }),
            _ => None,
        })
        .collect()
}

fn state_from_entries(entries: &[(Vec<u8>, Vec<u8>)]) -> State {
    entries.iter().cloned().collect()
}

fn apply(base: &State, mutations: &[Mutation]) -> State {
    let mut state = base.clone();
    for mutation in mutations {
        match mutation {
            Mutation::Upsert { key, val } => {
                state.insert(key.clone(), val.clone());
            }
            Mutation::Delete { key } => {
                state.remove(key);
            }
        }
    }
    state
}

fn conflict_record(conflict: &Conflict) -> ConflictRecord {
    (
        conflict.key.clone(),
        conflict.base.clone(),
        conflict.left.clone(),
        conflict.right.clone(),
    )
}

fn key(index: usize) -> Vec<u8> {
    format!("key-{index:020}").into_bytes()
}

fn value(index: usize, generation: u64) -> Vec<u8> {
    let mut seed = Vec::with_capacity(24);
    seed.extend_from_slice(&(index as u64).to_be_bytes());
    seed.extend_from_slice(&generation.to_le_bytes());
    seed.extend_from_slice(&(index as u64 ^ generation.rotate_left(17)).to_le_bytes());
    seed.iter().copied().cycle().take(VALUE_BYTES).collect()
}

fn shuffle<T>(values: &mut [T], mut state: u64) {
    for index in (1..values.len()).rev() {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        values.swap(index, state as usize % (index + 1));
    }
}

fn env_usize(name: &str, default: usize) -> Result<usize, String> {
    match std::env::var(name) {
        Ok(value) => value
            .parse::<usize>()
            .map_err(|error| format!("{name}={value:?} is invalid: {error}")),
        Err(_) => Ok(default),
    }
}

fn root_hex(root: &Option<Cid>) -> String {
    root.as_ref()
        .map(|cid| hex(cid.as_bytes()))
        .unwrap_or_else(|| "empty".to_string())
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn error(error: impl StdError) -> String {
    error.to_string()
}
