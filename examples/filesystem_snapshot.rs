use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use std::{fs, io};

use prolly::{
    BatchBuilder, BlobStore, Config, FileBlobStore, MemStore, NamedRootUpdate, Prolly, ValueRef,
};

struct Cleanup {
    path: PathBuf,
}

impl Cleanup {
    fn new(path: PathBuf) -> Self {
        Self { path }
    }
}

impl Drop for Cleanup {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = example_dir();
    let _cleanup = Cleanup::new(root.clone());

    let workspace = root.join("workspace");
    fs::create_dir_all(workspace.join("src"))?;
    fs::write(workspace.join("README.md"), b"# Demo\n")?;
    fs::write(
        workspace.join("src").join("lib.rs"),
        b"pub fn answer() -> u8 { 42 }\n",
    )?;

    let store = Arc::new(MemStore::new());
    let config = Config::default();
    let blobs = FileBlobStore::open(root.join("blobs"))?;

    let mut builder = BatchBuilder::new(store.clone(), config.clone());
    add_files_to_snapshot(&workspace, &workspace, &blobs, &mut builder)?;

    let first_snapshot = builder.build()?;
    let prolly = Prolly::new(store, config);

    // Publish the named root only after every file blob and tree node was written.
    prolly.publish_named_root(b"refs/heads/main", &first_snapshot)?;

    let loaded = prolly
        .load_named_root(b"refs/heads/main")?
        .expect("snapshot was published");
    assert_eq!(
        prolly.get_large_value(&blobs, &loaded, b"path/README.md")?,
        Some(b"# Demo\n".to_vec())
    );

    // Create a second snapshot by updating one file and moving the named root
    // with compare-and-swap, like a branch head.
    let updated_bytes = b"pub fn answer() -> u8 { 43 }\n".to_vec();
    fs::write(workspace.join("src").join("lib.rs"), &updated_bytes)?;

    let updated_blob = blobs.put_blob(&updated_bytes)?;
    let second_snapshot = prolly.put(
        &loaded,
        b"path/src/lib.rs".to_vec(),
        ValueRef::Blob(updated_blob).to_bytes(),
    )?;

    let update = prolly.compare_and_swap_named_root(
        b"refs/heads/main",
        Some(&loaded),
        Some(&second_snapshot),
    )?;
    assert_eq!(update, NamedRootUpdate::Applied);

    let current = prolly
        .load_named_root(b"refs/heads/main")?
        .expect("branch head exists");
    assert_eq!(
        prolly.get_large_value(&blobs, &current, b"path/src/lib.rs")?,
        Some(updated_bytes)
    );

    println!("published filesystem snapshot at refs/heads/main");
    Ok(())
}

fn add_files_to_snapshot(
    root: &Path,
    dir: &Path,
    blobs: &FileBlobStore,
    builder: &mut BatchBuilder<Arc<MemStore>>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut entries = fs::read_dir(dir)?.collect::<Result<Vec<_>, io::Error>>()?;
    entries.sort_by_key(|entry| entry.path());

    for entry in entries {
        let path = entry.path();
        let file_type = entry.file_type()?;
        if file_type.is_dir() {
            add_files_to_snapshot(root, &path, blobs, builder)?;
        } else if file_type.is_file() {
            let bytes = fs::read(&path)?;
            let blob_ref = blobs.put_blob(&bytes)?;
            let rel = path.strip_prefix(root)?;
            let key = format!("path/{}", relative_path_key(rel)).into_bytes();
            builder.add(key, ValueRef::Blob(blob_ref).to_bytes());
        }
    }

    Ok(())
}

fn relative_path_key(path: &Path) -> String {
    path.components()
        .map(|component| component.as_os_str().to_string_lossy())
        .collect::<Vec<_>>()
        .join("/")
}

fn example_dir() -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    std::env::temp_dir().join(format!(
        "prolly-filesystem-snapshot-example-{}-{nanos}",
        std::process::id()
    ))
}
