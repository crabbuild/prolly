use std::path::{Path, PathBuf};

use crate::model::CellSpec;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FixtureLayout {
    output: PathBuf,
    records: usize,
    repetition: usize,
}

impl FixtureLayout {
    pub fn new(output: PathBuf, records: usize, repetition: usize) -> Self {
        Self {
            output,
            records,
            repetition,
        }
    }

    pub fn source_dir(&self) -> PathBuf {
        self.output
            .join("fixtures")
            .join(self.records.to_string())
            .join(format!("run-{}", self.repetition))
    }

    pub fn source_database(&self) -> PathBuf {
        self.source_dir().join("prolly.db")
    }

    pub fn cell_dir(&self, spec: &CellSpec) -> PathBuf {
        self.output
            .join("cells")
            .join(self.records.to_string())
            .join(format!("run-{}", self.repetition))
            .join(spec.operation.as_str())
            .join(spec.pattern.as_str())
            .join(spec.cache_state.as_str())
    }

    pub fn cell_database(&self, spec: &CellSpec) -> PathBuf {
        self.cell_dir(spec).join("prolly.db")
    }

    pub fn clone_for(&self, spec: &CellSpec) -> Result<(), String> {
        clone_fixture(&self.source_dir(), &self.cell_dir(spec))
    }

    pub fn remove_cell(&self, spec: &CellSpec) -> Result<(), String> {
        remove_generated_dir(&self.output.join("cells"), &self.cell_dir(spec))
    }

    pub fn remove_source(&self) -> Result<(), String> {
        remove_generated_dir(&self.output.join("fixtures"), &self.source_dir())
    }
}

pub fn clone_fixture(source: &Path, destination: &Path) -> Result<(), String> {
    let metadata = std::fs::symlink_metadata(source)
        .map_err(|error| format!("failed to inspect {}: {error}", source.display()))?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(format!(
            "fixture source must be a real directory: {}",
            source.display()
        ));
    }
    if destination.exists() {
        return Err(format!(
            "fixture destination already exists: {}",
            destination.display()
        ));
    }
    copy_directory(source, destination)
}

fn copy_directory(source: &Path, destination: &Path) -> Result<(), String> {
    std::fs::create_dir_all(destination)
        .map_err(|error| format!("failed to create {}: {error}", destination.display()))?;
    for entry in std::fs::read_dir(source)
        .map_err(|error| format!("failed to read {}: {error}", source.display()))?
    {
        let entry = entry.map_err(|error| format!("failed to read fixture entry: {error}"))?;
        let source_path = entry.path();
        let destination_path = destination.join(entry.file_name());
        let metadata = std::fs::symlink_metadata(&source_path)
            .map_err(|error| format!("failed to inspect {}: {error}", source_path.display()))?;
        if metadata.file_type().is_symlink() {
            return Err(format!(
                "fixture contains a symlink: {}",
                source_path.display()
            ));
        }
        if metadata.is_dir() {
            copy_directory(&source_path, &destination_path)?;
        } else if metadata.is_file() {
            std::fs::copy(&source_path, &destination_path).map_err(|error| {
                format!(
                    "failed to copy {} to {}: {error}",
                    source_path.display(),
                    destination_path.display()
                )
            })?;
        } else {
            return Err(format!(
                "fixture contains an unsupported entry: {}",
                source_path.display()
            ));
        }
    }
    Ok(())
}

pub fn directory_bytes(path: &Path) -> Result<u64, String> {
    if !path.exists() {
        return Ok(0);
    }
    let mut total = 0u64;
    for entry in std::fs::read_dir(path)
        .map_err(|error| format!("failed to read {}: {error}", path.display()))?
    {
        let entry = entry.map_err(|error| format!("failed to read directory entry: {error}"))?;
        let metadata = entry
            .metadata()
            .map_err(|error| format!("failed to inspect {}: {error}", entry.path().display()))?;
        if metadata.is_dir() {
            total = total.saturating_add(directory_bytes(&entry.path())?);
        } else if metadata.is_file() {
            total = total.saturating_add(metadata.len());
        }
    }
    Ok(total)
}

pub fn remove_generated_dir(root: &Path, target: &Path) -> Result<(), String> {
    if target == root || !target.starts_with(root) {
        return Err(format!(
            "refusing to remove path outside generated root: {}",
            target.display()
        ));
    }
    if !target.exists() {
        return Ok(());
    }
    let metadata = std::fs::symlink_metadata(target)
        .map_err(|error| format!("failed to inspect {}: {error}", target.display()))?;
    if metadata.file_type().is_symlink() {
        return Err(format!("refusing to remove symlink: {}", target.display()));
    }
    std::fs::remove_dir_all(target)
        .map_err(|error| format!("failed to remove {}: {error}", target.display()))
}

pub fn database_file_bytes(database: &Path) -> Result<(u64, u64, u64, u64), String> {
    let length = |path: &Path| -> Result<u64, String> {
        match path.metadata() {
            Ok(metadata) => Ok(metadata.len()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(0),
            Err(error) => Err(format!("failed to inspect {}: {error}", path.display())),
        }
    };
    let db = length(database)?;
    let wal = length(&PathBuf::from(format!("{}-wal", database.display())))?;
    let shm = length(&PathBuf::from(format!("{}-shm", database.display())))?;
    Ok((db, wal, shm, db.saturating_add(wal).saturating_add(shm)))
}
