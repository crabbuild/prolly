use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use prolly::{BlobStore, Config, FileBlobStore, LargeValueConfig, MemStore, Prolly, ValueRef};

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
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let prolly = Prolly::new(MemStore::new(), Config::default());
    let blob_dir = example_blob_dir();
    let _cleanup = Cleanup::new(blob_dir.clone());
    let blobs = FileBlobStore::open(&blob_dir)?;
    let policy = LargeValueConfig::new(1024);

    let tree = prolly.create();
    let tree = prolly.put_large_value(
        &blobs,
        &tree,
        b"doc/body".to_vec(),
        vec![7; 4096],
        policy.clone(),
    )?;
    let ValueRef::Blob(original_ref) = prolly.get_value_ref(&tree, b"doc/body")?.unwrap() else {
        unreachable!("payload is larger than the inline threshold");
    };

    let updated =
        prolly.put_large_value(&blobs, &tree, b"doc/body".to_vec(), vec![9; 4096], policy)?;
    let ValueRef::Blob(updated_ref) = prolly.get_value_ref(&updated, b"doc/body")?.unwrap() else {
        unreachable!("payload is larger than the inline threshold");
    };

    let reopened = FileBlobStore::open(&blob_dir)?;
    assert_eq!(
        prolly.get_large_value(&reopened, &updated, b"doc/body")?,
        Some(vec![9; 4096])
    );

    let plan = prolly.plan_blob_store_gc(&reopened, std::slice::from_ref(&updated))?;
    assert_eq!(
        plan.reclaimable_blobs(),
        std::slice::from_ref(&original_ref)
    );

    let sweep = prolly.sweep_blob_store_gc(&reopened, std::slice::from_ref(&updated))?;
    assert_eq!(sweep.deleted_blobs, 1);
    assert!(reopened.get_blob(&original_ref)?.is_none());
    assert!(reopened.get_blob(&updated_ref)?.is_some());

    println!(
        "stored {}, reclaimed {}",
        display_path(&blob_dir),
        sweep.deleted_blob_bytes
    );
    Ok(())
}

fn example_blob_dir() -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    std::env::temp_dir().join(format!(
        "prolly-file-blob-store-example-{}-{nanos}",
        std::process::id()
    ))
}

fn display_path(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}
