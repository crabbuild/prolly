use std::cmp::Reverse;
use std::collections::{BTreeMap, BinaryHeap};
use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use super::super::builder::SortedBatchBuilder;
use super::super::error::Error;
use super::super::store::{PublicationOrigin, Store};
use super::super::tree::Tree;
use super::super::Config;
use super::budget::{BudgetCounter, MaintenanceBudget};

static NEXT_WORKSPACE: AtomicU64 = AtomicU64::new(1);

pub(crate) struct IndexBuildWorkspace {
    budget: MaintenanceBudget,
    counter: BudgetCounter,
    memory: BTreeMap<Vec<u8>, Vec<u8>>,
    memory_bytes: usize,
    spill_bytes: usize,
    runs: Vec<PathBuf>,
    directory: Option<PathBuf>,
    next_run: u64,
}

impl IndexBuildWorkspace {
    pub(crate) fn new(budget: &MaintenanceBudget) -> Result<Self, Error> {
        budget.validate()?;
        Ok(Self {
            budget: budget.clone(),
            counter: BudgetCounter::new(),
            memory: BTreeMap::new(),
            memory_bytes: 0,
            spill_bytes: 0,
            runs: Vec::new(),
            directory: None,
            next_run: 0,
        })
    }

    pub(crate) fn add(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<(), Error> {
        self.counter
            .check_elapsed("maintenance_elapsed_millis", self.budget.max_elapsed)?;
        let bytes = key
            .len()
            .checked_add(value.len())
            .and_then(|value| value.checked_add(16))
            .ok_or(Error::IndexResourceLimitExceeded {
                resource: "maintenance_entry_bytes",
                limit: self.budget.max_accounted_memory_bytes,
                actual: usize::MAX,
            })?;
        if bytes > self.budget.max_accounted_memory_bytes {
            return Err(Error::IndexResourceLimitExceeded {
                resource: "maintenance_entry_bytes",
                limit: self.budget.max_accounted_memory_bytes,
                actual: bytes,
            });
        }
        if self.memory_bytes.saturating_add(bytes) > self.budget.max_accounted_memory_bytes {
            self.spill_memory()?;
        }
        match self.memory.insert(key.clone(), value.clone()) {
            Some(previous) if previous != value => {
                return Err(Error::InvalidVersionedMap(
                    "maintenance emitted conflicting values for one physical key".to_string(),
                ))
            }
            Some(previous) => {
                self.memory_bytes = self
                    .memory_bytes
                    .saturating_sub(key.len() + previous.len() + 16);
            }
            None => {}
        }
        self.memory_bytes = self.memory_bytes.saturating_add(bytes);
        Ok(())
    }

    pub(crate) fn finish<S: Store + Clone>(
        mut self,
        store: S,
        config: Config,
    ) -> Result<(Tree, usize), Error> {
        if self.runs.is_empty() {
            let count = self.memory.len();
            let mut builder =
                SortedBatchBuilder::new_with_origin(store, config, PublicationOrigin::Maintenance);
            for (key, value) in std::mem::take(&mut self.memory) {
                builder.add(key, value)?;
            }
            return Ok((builder.build()?, count));
        }
        self.spill_memory()?;
        while self.runs.len() > self.budget.max_merge_fan_in {
            let old = std::mem::take(&mut self.runs);
            for group in old.chunks(self.budget.max_merge_fan_in) {
                let output = self.next_run_path()?;
                self.merge_to_run(group, &output)?;
                self.runs.push(output);
                for path in group {
                    remove_file(path)?;
                }
            }
        }
        let mut readers = self
            .runs
            .iter()
            .map(|path| RunReader::open(path, self.budget.max_accounted_memory_bytes))
            .collect::<Result<Vec<_>, Error>>()?;
        let mut heap = BinaryHeap::new();
        for (position, reader) in readers.iter_mut().enumerate() {
            if let Some((key, value)) = reader.next_entry()? {
                heap.push(Reverse((key, position, value)));
            }
        }
        let mut builder =
            SortedBatchBuilder::new_with_origin(store, config, PublicationOrigin::Maintenance);
        let mut count = 0usize;
        let mut pending: Option<(Vec<u8>, Vec<u8>)> = None;
        while let Some(Reverse((key, position, value))) = heap.pop() {
            self.counter
                .check_elapsed("maintenance_elapsed_millis", self.budget.max_elapsed)?;
            if let Some((previous_key, previous_value)) = pending.take() {
                if previous_key == key {
                    if previous_value != value {
                        return Err(Error::InvalidVersionedMap(
                            "spilled runs disagree for one physical key".to_string(),
                        ));
                    }
                    pending = Some((key, value));
                } else {
                    builder.add(previous_key, previous_value)?;
                    count = count.saturating_add(1);
                    pending = Some((key, value));
                }
            } else {
                pending = Some((key, value));
            }
            if let Some((next_key, next_value)) = readers[position].next_entry()? {
                heap.push(Reverse((next_key, position, next_value)));
            }
        }
        if let Some((key, value)) = pending {
            builder.add(key, value)?;
            count = count.saturating_add(1);
        }
        Ok((builder.build()?, count))
    }

    fn spill_memory(&mut self) -> Result<(), Error> {
        if self.memory.is_empty() {
            return Ok(());
        }
        if self.runs.len() == self.budget.max_spill_runs {
            return Err(Error::IndexResourceLimitExceeded {
                resource: "maintenance_spill_runs",
                limit: self.budget.max_spill_runs,
                actual: self.runs.len().saturating_add(1),
            });
        }
        let path = self.next_run_path()?;
        let entries = std::mem::take(&mut self.memory);
        self.memory_bytes = 0;
        self.write_run(&path, entries)?;
        self.runs.push(path);
        Ok(())
    }

    fn merge_to_run(&mut self, inputs: &[PathBuf], output: &Path) -> Result<(), Error> {
        let mut readers = inputs
            .iter()
            .map(|path| RunReader::open(path, self.budget.max_accounted_memory_bytes))
            .collect::<Result<Vec<_>, Error>>()?;
        let mut heap = BinaryHeap::new();
        for (position, reader) in readers.iter_mut().enumerate() {
            if let Some((key, value)) = reader.next_entry()? {
                heap.push(Reverse((key, position, value)));
            }
        }
        let entries = std::iter::from_fn(move || {
            let Reverse((key, position, value)) = heap.pop()?;
            match readers[position].next_entry() {
                Ok(Some((next_key, next_value))) => {
                    heap.push(Reverse((next_key, position, next_value)));
                }
                Ok(None) => {}
                Err(error) => return Some(Err(error)),
            }
            Some(Ok((key, value)))
        });
        self.write_run(output, entries)
    }

    fn write_run<I, E>(&mut self, path: &Path, entries: I) -> Result<(), Error>
    where
        I: IntoIterator<Item = E>,
        E: IntoRunEntry,
    {
        let file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(path)
            .map_err(store_error)?;
        let mut writer = BufWriter::new(file);
        for entry in entries {
            let (key, value) = entry.into_entry()?;
            let bytes = key.len().saturating_add(value.len()).saturating_add(16);
            if self.spill_bytes.saturating_add(bytes) > self.budget.max_spill_bytes {
                return Err(Error::IndexResourceLimitExceeded {
                    resource: "maintenance_spill_bytes",
                    limit: self.budget.max_spill_bytes,
                    actual: self.spill_bytes.saturating_add(bytes),
                });
            }
            writer
                .write_all(&(key.len() as u64).to_be_bytes())
                .and_then(|_| writer.write_all(&(value.len() as u64).to_be_bytes()))
                .and_then(|_| writer.write_all(&key))
                .and_then(|_| writer.write_all(&value))
                .map_err(store_error)?;
            self.spill_bytes += bytes;
        }
        writer.flush().map_err(store_error)
    }

    fn next_run_path(&mut self) -> Result<PathBuf, Error> {
        if self.directory.is_none() {
            let id = NEXT_WORKSPACE.fetch_add(1, Ordering::Relaxed);
            let directory = std::env::temp_dir().join(format!(
                "prolly-index-workspace-{}-{id}",
                std::process::id()
            ));
            fs::create_dir(&directory).map_err(store_error)?;
            self.directory = Some(directory);
        }
        let run = self.next_run;
        self.next_run = self.next_run.saturating_add(1);
        Ok(self
            .directory
            .as_ref()
            .expect("created workspace directory")
            .join(format!("run-{run:08}")))
    }
}

impl Drop for IndexBuildWorkspace {
    fn drop(&mut self) {
        if let Some(directory) = &self.directory {
            let _ = fs::remove_dir_all(directory);
        }
    }
}

trait IntoRunEntry {
    fn into_entry(self) -> Result<(Vec<u8>, Vec<u8>), Error>;
}

impl IntoRunEntry for (Vec<u8>, Vec<u8>) {
    fn into_entry(self) -> Result<(Vec<u8>, Vec<u8>), Error> {
        Ok(self)
    }
}

impl IntoRunEntry for Result<(Vec<u8>, Vec<u8>), Error> {
    fn into_entry(self) -> Result<(Vec<u8>, Vec<u8>), Error> {
        self
    }
}

struct RunReader {
    reader: BufReader<File>,
    max_entry_bytes: usize,
}

type EncodedIndexEntry = (Vec<u8>, Vec<u8>);

impl RunReader {
    fn open(path: &Path, max_entry_bytes: usize) -> Result<Self, Error> {
        Ok(Self {
            reader: BufReader::new(File::open(path).map_err(store_error)?),
            max_entry_bytes,
        })
    }

    fn next_entry(&mut self) -> Result<Option<EncodedIndexEntry>, Error> {
        let mut key_length = [0u8; 8];
        match self.reader.read(&mut key_length[..1]) {
            Ok(0) => return Ok(None),
            Ok(1) => self
                .reader
                .read_exact(&mut key_length[1..])
                .map_err(store_error)?,
            Ok(_) => unreachable!("one-byte read returned more than one byte"),
            Err(error) => return Err(store_error(error)),
        }
        let mut value_length = [0u8; 8];
        self.reader
            .read_exact(&mut value_length)
            .map_err(store_error)?;
        let key_length = usize::try_from(u64::from_be_bytes(key_length)).map_err(|_| {
            Error::InvalidVersionedMap("spill key length exceeds platform limits".to_string())
        })?;
        let value_length = usize::try_from(u64::from_be_bytes(value_length)).map_err(|_| {
            Error::InvalidVersionedMap("spill value length exceeds platform limits".to_string())
        })?;
        let entry_bytes = key_length
            .checked_add(value_length)
            .and_then(|bytes| bytes.checked_add(16))
            .ok_or(Error::IndexResourceLimitExceeded {
                resource: "maintenance_spill_entry_bytes",
                limit: self.max_entry_bytes,
                actual: usize::MAX,
            })?;
        if entry_bytes > self.max_entry_bytes {
            return Err(Error::IndexResourceLimitExceeded {
                resource: "maintenance_spill_entry_bytes",
                limit: self.max_entry_bytes,
                actual: entry_bytes,
            });
        }
        let mut key = vec![0; key_length];
        let mut value = vec![0; value_length];
        self.reader.read_exact(&mut key).map_err(store_error)?;
        self.reader.read_exact(&mut value).map_err(store_error)?;
        Ok(Some((key, value)))
    }
}

fn remove_file(path: &Path) -> Result<(), Error> {
    fs::remove_file(path).map_err(store_error)
}

fn store_error(error: std::io::Error) -> Error {
    Error::Store(Box::new(error))
}
