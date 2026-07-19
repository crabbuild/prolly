//! Closed-database fixture layout, cloning, accounting, and cleanup.

use std::path::{Path, PathBuf};

use crate::model::{Adapter, Api, Pattern};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FixtureLayout {
    output: PathBuf,
    adapter: Adapter,
    records: usize,
    repetition: usize,
}

impl FixtureLayout {
    pub fn new(output: PathBuf, adapter: Adapter, records: usize, repetition: usize) -> Self {
        Self {
            output,
            adapter,
            records,
            repetition,
        }
    }

    pub fn source_dir(&self) -> PathBuf {
        self.output
            .join("fixtures")
            .join(self.adapter.as_str())
            .join(self.records.to_string())
            .join(format!("run-{}", self.repetition))
    }

    pub fn source_database(&self) -> PathBuf {
        self.source_dir().join("prolly.db")
    }

    pub fn fixtures_root(&self) -> PathBuf {
        self.output.join("fixtures")
    }

    pub fn cells_root(&self) -> PathBuf {
        self.output.join("cells")
    }

    pub fn cell_dir(&self, api: Api, pattern: Pattern) -> PathBuf {
        self.cells_root()
            .join(self.adapter.as_str())
            .join(self.records.to_string())
            .join(format!("run-{}", self.repetition))
            .join(api.as_str())
            .join(pattern.as_str())
    }

    pub fn cell_database(&self, api: Api, pattern: Pattern) -> PathBuf {
        self.cell_dir(api, pattern).join("prolly.db")
    }

    pub fn validate_source_destination(&self) -> Result<(), String> {
        validate_generated_destination(&self.output, &self.source_dir(), "fixture")
    }

    pub fn validate_cell_destination(&self, api: Api, pattern: Pattern) -> Result<(), String> {
        validate_generated_destination(&self.output, &self.cell_dir(api, pattern), "cell")
    }
}

pub fn clone_fixture(source: &Path, destination: &Path) -> Result<(), String> {
    let source_metadata = std::fs::symlink_metadata(source)
        .map_err(|error| format!("failed to inspect {}: {error}", source.display()))?;
    if source_metadata.file_type().is_symlink() {
        return Err(format!(
            "fixture source must not be a symlink: {}",
            source.display()
        ));
    }
    if !source.is_dir() {
        return Err(format!(
            "fixture source is not a directory: {}",
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

pub fn directory_bytes(path: &Path) -> Result<u64, String> {
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
        } else {
            return Err(format!(
                "fixture contains unsupported entry: {}",
                entry.path().display()
            ));
        }
    }
    Ok(total)
}

pub fn remove_cell_dir(layout: &FixtureLayout, cell_dir: &Path) -> Result<(), String> {
    let cells_root = layout.cells_root();
    if cell_dir == cells_root || !cell_dir.starts_with(&cells_root) {
        return Err(format!(
            "refusing to remove path outside generated cells: {}",
            cell_dir.display()
        ));
    }
    if cell_dir.exists() {
        if std::fs::symlink_metadata(cell_dir)
            .map_err(|error| format!("failed to inspect {}: {error}", cell_dir.display()))?
            .file_type()
            .is_symlink()
        {
            return Err(format!(
                "refusing to remove symlink: {}",
                cell_dir.display()
            ));
        }
        let canonical_root = cells_root
            .canonicalize()
            .map_err(|error| format!("failed to resolve {}: {error}", cells_root.display()))?;
        let canonical_cell = cell_dir
            .canonicalize()
            .map_err(|error| format!("failed to resolve {}: {error}", cell_dir.display()))?;
        if canonical_cell == canonical_root || !canonical_cell.starts_with(&canonical_root) {
            return Err(format!(
                "refusing to remove redirected cell: {}",
                cell_dir.display()
            ));
        }
        std::fs::remove_dir_all(cell_dir)
            .map_err(|error| format!("failed to remove {}: {error}", cell_dir.display()))?;
    }
    Ok(())
}

pub fn remove_source_dir(layout: &FixtureLayout) -> Result<(), String> {
    let source_dir = layout.source_dir();
    let fixtures_root = layout.fixtures_root();
    if source_dir == fixtures_root || !source_dir.starts_with(&fixtures_root) {
        return Err(format!(
            "refusing to remove path outside generated fixtures: {}",
            source_dir.display()
        ));
    }
    if source_dir.exists() {
        if std::fs::symlink_metadata(&source_dir)
            .map_err(|error| format!("failed to inspect {}: {error}", source_dir.display()))?
            .file_type()
            .is_symlink()
        {
            return Err(format!(
                "refusing to remove symlink: {}",
                source_dir.display()
            ));
        }
        let canonical_root = fixtures_root
            .canonicalize()
            .map_err(|error| format!("failed to resolve {}: {error}", fixtures_root.display()))?;
        let canonical_source = source_dir
            .canonicalize()
            .map_err(|error| format!("failed to resolve {}: {error}", source_dir.display()))?;
        if canonical_source == canonical_root || !canonical_source.starts_with(&canonical_root) {
            return Err(format!(
                "refusing to remove redirected fixture: {}",
                source_dir.display()
            ));
        }
        std::fs::remove_dir_all(&source_dir)
            .map_err(|error| format!("failed to remove {}: {error}", source_dir.display()))?;
    }
    Ok(())
}

fn copy_directory(source: &Path, destination: &Path) -> Result<(), String> {
    std::fs::create_dir_all(destination)
        .map_err(|error| format!("failed to create {}: {error}", destination.display()))?;
    for entry in std::fs::read_dir(source)
        .map_err(|error| format!("failed to read {}: {error}", source.display()))?
    {
        let entry = entry.map_err(|error| format!("failed to read directory entry: {error}"))?;
        let source_path = entry.path();
        let destination_path = destination.join(entry.file_name());
        let metadata = entry
            .path()
            .symlink_metadata()
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
                "fixture contains unsupported entry: {}",
                source_path.display()
            ));
        }
    }
    Ok(())
}

fn validate_generated_destination(output: &Path, target: &Path, label: &str) -> Result<(), String> {
    let canonical_output = output
        .canonicalize()
        .map_err(|error| format!("failed to resolve {}: {error}", output.display()))?;
    let mut existing = target;
    while !existing.exists() {
        existing = existing.parent().ok_or_else(|| {
            format!(
                "generated {label} has no existing parent: {}",
                target.display()
            )
        })?;
    }
    if std::fs::symlink_metadata(existing)
        .map_err(|error| format!("failed to inspect {}: {error}", existing.display()))?
        .file_type()
        .is_symlink()
    {
        return Err(format!(
            "generated {label} parent is a symlink: {}",
            existing.display()
        ));
    }
    let canonical_parent = existing
        .canonicalize()
        .map_err(|error| format!("failed to resolve {}: {error}", existing.display()))?;
    if !canonical_parent.starts_with(&canonical_output) {
        return Err(format!(
            "generated {label} is redirected outside output: {}",
            target.display()
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::*;

    #[test]
    fn fixture_clone_copies_sidecars_and_nested_files() {
        let temp = tempfile::tempdir().unwrap();
        let source = temp.path().join("source");
        let destination = temp.path().join("destination");
        fs::create_dir_all(source.join("metadata")).unwrap();
        fs::write(source.join("prolly.db"), b"database").unwrap();
        fs::write(source.join("prolly.db-wal"), b"wal").unwrap();
        fs::write(source.join("metadata/config"), b"config").unwrap();

        clone_fixture(&source, &destination).unwrap();

        assert_eq!(
            fs::read(destination.join("prolly.db")).unwrap(),
            b"database"
        );
        assert_eq!(fs::read(destination.join("prolly.db-wal")).unwrap(), b"wal");
        assert_eq!(
            fs::read(destination.join("metadata/config")).unwrap(),
            b"config"
        );
        assert_eq!(directory_bytes(&destination).unwrap(), 17);
    }

    #[test]
    fn fixture_clone_rejects_existing_destination() {
        let temp = tempfile::tempdir().unwrap();
        let source = temp.path().join("source");
        let destination = temp.path().join("destination");
        fs::create_dir_all(&source).unwrap();
        fs::create_dir_all(&destination).unwrap();
        assert!(clone_fixture(&source, &destination)
            .unwrap_err()
            .contains("already exists"));
    }

    #[test]
    fn cleanup_is_confined_to_the_generated_cells_root() {
        let temp = tempfile::tempdir().unwrap();
        let layout = FixtureLayout::new(temp.path().to_path_buf(), Adapter::SqliteSync, 100, 1);
        let cell = layout.cell_dir(Api::Put, Pattern::Append);
        fs::create_dir_all(&cell).unwrap();
        remove_cell_dir(&layout, &cell).unwrap();
        assert!(!cell.exists());
        assert!(remove_cell_dir(&layout, temp.path()).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn cleanup_rejects_a_redirecting_symlink() {
        use std::os::unix::fs::symlink;

        let temp = tempfile::tempdir().unwrap();
        let layout = FixtureLayout::new(temp.path().to_path_buf(), Adapter::SqliteSync, 100, 1);
        let outside = temp.path().join("outside");
        fs::create_dir_all(&outside).unwrap();
        fs::write(outside.join("keep"), b"keep").unwrap();
        let cell = layout.cell_dir(Api::Put, Pattern::Append);
        fs::create_dir_all(cell.parent().unwrap()).unwrap();
        symlink(&outside, &cell).unwrap();

        assert!(remove_cell_dir(&layout, &cell).is_err());
        assert_eq!(fs::read(outside.join("keep")).unwrap(), b"keep");
    }
}
