mod model;
mod sync_runner;
mod turso_runner;

use std::fs::{self, OpenOptions};
use std::path::Path;
use std::sync::Arc;

use prolly::{FileNodeStore, MemStore, Prolly};
use prolly_store_pglite::PgliteStore;
use prolly_store_rocksdb::RocksDBStore;
use prolly_store_slatedb::SlateDbStore;
use prolly_store_sqlite::SqliteStore;
use slatedb::object_store::memory::InMemory;
use slatedb::object_store::ObjectStore;

use crate::model::{base_entries, Adapter, CellSpec, ResultRow, RunConfig};

fn main() {
    if let Err(error) = run() {
        eprintln!("local-store-publication benchmark failed: {error}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let config = RunConfig::parse(std::env::args())?;
    if let Some(parent) = config.output.parent() {
        fs::create_dir_all(parent)
            .map_err(|error| format!("failed to create {}: {error}", parent.display()))?;
    }
    let fixture_manager = Prolly::new(MemStore::new(), prolly::Config::default());
    let fixture_tree = fixture_manager
        .build_from_entries(base_entries(config.records))
        .map_err(|error| format!("failed to build canonical fixture: {error}"))?;
    let fixture = fixture_manager
        .export_snapshot(&fixture_tree)
        .map_err(|error| format!("failed to export canonical fixture: {error}"))?;
    if !fixture
        .verify()
        .map_err(|error| format!("failed to verify canonical fixture: {error}"))?
        .valid
    {
        return Err("canonical fixture is not self-contained".to_string());
    }

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|error| format!("failed to create Turso runtime: {error}"))?;
    let mut writer = open_writer(&config.output)?;
    let work_root = config.output.with_extension("work");
    if work_root.exists() {
        return Err(format!(
            "generated work directory already exists: {}",
            work_root.display()
        ));
    }
    fs::create_dir_all(&work_root)
        .map_err(|error| format!("failed to create {}: {error}", work_root.display()))?;

    for run in 1..=config.runs {
        for &adapter in &config.adapters {
            for &api in &config.apis {
                for &pattern in &config.patterns {
                    let spec = CellSpec {
                        adapter,
                        records: config.records,
                        changes: config.changes,
                        run,
                        api,
                        pattern,
                    };
                    let cell_dir = work_root
                        .join(format!("run-{run}"))
                        .join(adapter.as_str())
                        .join(api.as_str())
                        .join(pattern.as_str());
                    fs::create_dir_all(&cell_dir).map_err(|error| {
                        format!("failed to create cell {}: {error}", cell_dir.display())
                    })?;
                    let row = run_cell(&runtime, &config.revision, spec, &fixture, &cell_dir)?;
                    writer
                        .serialize(row)
                        .map_err(|error| format!("failed to write result row: {error}"))?;
                    writer
                        .flush()
                        .map_err(|error| format!("failed to flush results: {error}"))?;
                    remove_generated_cell(&work_root, &cell_dir)?;
                }
            }
        }
    }
    drop(writer);
    fs::remove_dir_all(&work_root)
        .map_err(|error| format!("failed to remove {}: {error}", work_root.display()))?;
    Ok(())
}

fn run_cell(
    runtime: &tokio::runtime::Runtime,
    revision: &str,
    spec: CellSpec,
    fixture: &prolly::SnapshotBundle,
    cell_dir: &Path,
) -> Result<ResultRow, String> {
    match spec.adapter {
        Adapter::MemorySync => {
            let store = Arc::new(MemStore::new());
            sync_runner::run(revision, spec, fixture, || Ok(store.clone()))
        }
        Adapter::FileSync => {
            let path = cell_dir.join("file-store");
            sync_runner::run(revision, spec, fixture, || {
                FileNodeStore::open(&path)
                    .map(Arc::new)
                    .map_err(|error| format!("failed to open file store: {error}"))
            })
        }
        Adapter::SqliteSync => {
            let path = cell_dir.join("prolly.sqlite3");
            sync_runner::run(revision, spec, fixture, || {
                SqliteStore::open(&path)
                    .map(Arc::new)
                    .map_err(|error| format!("failed to open SQLite store: {error}"))
            })
        }
        Adapter::RocksdbSync => {
            let path = cell_dir.join("rocksdb");
            sync_runner::run(revision, spec, fixture, || {
                RocksDBStore::open(&path)
                    .map(Arc::new)
                    .map_err(|error| format!("failed to open RocksDB store: {error}"))
            })
        }
        Adapter::SlatedbSync => {
            let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            let database_path = format!(
                "publication/{}/{}/{}/{}",
                spec.run,
                spec.adapter.as_str(),
                spec.api.as_str(),
                spec.pattern.as_str()
            );
            sync_runner::run(revision, spec, fixture, || {
                SlateDbStore::open(database_path.clone(), object_store.clone())
                    .map(Arc::new)
                    .map_err(|error| format!("failed to open SlateDB store: {error}"))
            })
        }
        Adapter::PgliteSync => {
            let path = cell_dir.join("pglite").to_string_lossy().into_owned();
            sync_runner::run(revision, spec, fixture, || {
                PgliteStore::open(path.clone())
                    .map(Arc::new)
                    .map_err(|error| format!("failed to open PGlite store: {error}"))
            })
        }
        Adapter::TursoAsync => runtime.block_on(turso_runner::run(
            revision,
            spec,
            fixture,
            &cell_dir.join("prolly.db"),
        )),
    }
}

fn open_writer(path: &Path) -> Result<csv::Writer<std::fs::File>, String> {
    let has_rows = path
        .metadata()
        .map(|metadata| metadata.len() > 0)
        .unwrap_or(false);
    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(|error| format!("failed to open {}: {error}", path.display()))?;
    Ok(csv::WriterBuilder::new()
        .has_headers(!has_rows)
        .from_writer(file))
}

fn remove_generated_cell(work_root: &Path, cell_dir: &Path) -> Result<(), String> {
    if cell_dir == work_root || !cell_dir.starts_with(work_root) {
        return Err(format!(
            "refusing to remove path outside benchmark work root: {}",
            cell_dir.display()
        ));
    }
    fs::remove_dir_all(cell_dir)
        .map_err(|error| format!("failed to remove {}: {error}", cell_dir.display()))
}
