//! Filesystem-backed content-addressed node store.
//!
//! This backend is intentionally small and object-store-shaped: immutable node
//! bytes live under a sharded CID namespace, while optional hints and named root
//! manifests live under separate namespaces. It is useful as a local durable
//! store and as a reference layout for S3/R2/GCS-style adapters.

use std::fs::{self, DirEntry, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use super::super::cid::Cid;
use super::super::manifest::{
    sort_named_root_manifests, ManifestStore, ManifestStoreScan, ManifestUpdate, NamedRootManifest,
    RootManifest,
};
use super::{sort_cids, BatchOp, NodeStoreScan, Store};

/// Durable filesystem node store using content-addressed object layout.
///
/// Nodes are stored under `nodes/sha256/aa/bb/<cid-hex>` below the configured
/// root. Hints are stored under `hints/<namespace-hex>/<key-hex>`, and named
/// roots are stored under `roots/<name-hex>`. Node reads and writes verify that
/// bytes hash to the CID key.
#[derive(Clone, Debug)]
pub struct FileNodeStore {
    root: PathBuf,
    manifest_lock: Arc<Mutex<()>>,
}

impl FileNodeStore {
    /// Open or create a filesystem node store rooted at `root`.
    pub fn open(root: impl Into<PathBuf>) -> Result<Self, FileNodeStoreError> {
        let store = Self {
            root: root.into(),
            manifest_lock: Arc::new(Mutex::new(())),
        };
        store.ensure_namespace_dirs()?;
        Ok(store)
    }

    /// Return the root directory for this store.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Return the namespace directory containing immutable node objects.
    pub fn node_namespace_dir(&self) -> PathBuf {
        self.root.join("nodes").join("sha256")
    }

    /// Return the namespace directory containing optional performance hints.
    pub fn hint_namespace_dir(&self) -> PathBuf {
        self.root.join("hints")
    }

    /// Return the namespace directory containing named root manifests.
    pub fn root_namespace_dir(&self) -> PathBuf {
        self.root.join("roots")
    }

    /// Return the filesystem path for a content-addressed node.
    pub fn path_for_cid(&self, cid: &Cid) -> PathBuf {
        self.node_path(cid)
    }

    fn ensure_namespace_dirs(&self) -> Result<(), FileNodeStoreError> {
        for path in [
            self.node_namespace_dir(),
            self.hint_namespace_dir(),
            self.root_namespace_dir(),
        ] {
            fs::create_dir_all(&path).map_err(|source| FileNodeStoreError::Io { path, source })?;
        }
        Ok(())
    }

    fn node_path(&self, cid: &Cid) -> PathBuf {
        let hex = cid_hex(cid);
        self.node_namespace_dir()
            .join(&hex[0..2])
            .join(&hex[2..4])
            .join(hex)
    }

    fn hint_path(&self, namespace: &[u8], key: &[u8]) -> PathBuf {
        self.hint_namespace_dir()
            .join(hex_label(namespace))
            .join(hex_label(key))
    }

    fn root_path(&self, name: &[u8]) -> PathBuf {
        self.root_namespace_dir().join(hex_label(name))
    }

    fn read_node_path(
        &self,
        cid: &Cid,
        path: &Path,
    ) -> Result<Option<Vec<u8>>, FileNodeStoreError> {
        match fs::read(path) {
            Ok(bytes) => {
                validate_node_bytes(cid, path, &bytes)?;
                Ok(Some(bytes))
            }
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(source) => Err(FileNodeStoreError::Io {
                path: path.to_path_buf(),
                source,
            }),
        }
    }

    fn write_node(&self, cid: &Cid, bytes: &[u8]) -> Result<(), FileNodeStoreError> {
        validate_node_bytes(cid, &self.node_path(cid), bytes)?;
        let path = self.node_path(cid);
        if self.read_node_path(cid, &path)?.is_some() {
            return Ok(());
        }
        write_file_atomically(&path, bytes)
    }

    fn read_root_path(&self, path: &Path) -> Result<Option<RootManifest>, FileNodeStoreError> {
        match fs::read(path) {
            Ok(bytes) => RootManifest::from_bytes(&bytes).map(Some).map_err(|err| {
                FileNodeStoreError::Manifest {
                    path: path.to_path_buf(),
                    message: err.to_string(),
                }
            }),
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(source) => Err(FileNodeStoreError::Io {
                path: path.to_path_buf(),
                source,
            }),
        }
    }

    fn write_root_path(
        &self,
        path: &Path,
        manifest: &RootManifest,
    ) -> Result<(), FileNodeStoreError> {
        let bytes = manifest
            .to_bytes()
            .map_err(|err| FileNodeStoreError::Manifest {
                path: path.to_path_buf(),
                message: err.to_string(),
            })?;
        write_file_atomically(path, &bytes)
    }
}

impl Store for FileNodeStore {
    type Error = FileNodeStoreError;

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        let cid = cid_from_node_key(key)?;
        self.read_node_path(&cid, &self.node_path(&cid))
    }

    fn put(&self, key: &[u8], value: &[u8]) -> Result<(), Self::Error> {
        let cid = cid_from_node_key(key)?;
        self.write_node(&cid, value)
    }

    fn delete(&self, key: &[u8]) -> Result<(), Self::Error> {
        let cid = cid_from_node_key(key)?;
        let path = self.node_path(&cid);
        match fs::remove_file(&path) {
            Ok(()) => {
                remove_empty_dirs(&path, &self.node_namespace_dir());
                Ok(())
            }
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(source) => Err(FileNodeStoreError::Io { path, source }),
        }
    }

    fn batch(&self, ops: &[BatchOp<'_>]) -> Result<(), Self::Error> {
        let mut validated = Vec::with_capacity(ops.len());
        for op in ops {
            match op {
                BatchOp::Upsert { key, value } => {
                    let cid = cid_from_node_key(key)?;
                    validate_node_bytes(&cid, &self.node_path(&cid), value)?;
                    validated.push(BatchOpOwned::Upsert {
                        cid,
                        value: (*value).to_vec(),
                    });
                }
                BatchOp::Delete { key } => {
                    validated.push(BatchOpOwned::Delete {
                        cid: cid_from_node_key(key)?,
                    });
                }
            }
        }

        for op in validated {
            match op {
                BatchOpOwned::Upsert { cid, value } => self.write_node(&cid, &value)?,
                BatchOpOwned::Delete { cid } => self.delete(cid.as_bytes())?,
            }
        }
        Ok(())
    }

    fn batch_get(
        &self,
        keys: &[&[u8]],
    ) -> Result<std::collections::HashMap<Vec<u8>, Vec<u8>>, Self::Error> {
        let mut results = std::collections::HashMap::with_capacity(keys.len());
        for key in keys {
            if results.contains_key(*key) {
                continue;
            }
            if let Some(value) = self.get(key)? {
                results.insert((*key).to_vec(), value);
            }
        }
        Ok(results)
    }

    fn batch_get_ordered(&self, keys: &[&[u8]]) -> Result<Vec<Option<Vec<u8>>>, Self::Error> {
        keys.iter().map(|key| self.get(key)).collect()
    }

    fn batch_get_ordered_unique(
        &self,
        keys: &[&[u8]],
    ) -> Result<Vec<Option<Vec<u8>>>, Self::Error> {
        keys.iter().map(|key| self.get(key)).collect()
    }

    fn batch_put(&self, entries: &[(&[u8], &[u8])]) -> Result<(), Self::Error> {
        let mut validated = Vec::with_capacity(entries.len());
        for (key, value) in entries {
            let cid = cid_from_node_key(key)?;
            validate_node_bytes(&cid, &self.node_path(&cid), value)?;
            validated.push((cid, (*value).to_vec()));
        }

        for (cid, value) in validated {
            self.write_node(&cid, &value)?;
        }
        Ok(())
    }

    fn supports_hints(&self) -> bool {
        true
    }

    fn get_hint(&self, namespace: &[u8], key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        let path = self.hint_path(namespace, key);
        match fs::read(&path) {
            Ok(bytes) => Ok(Some(bytes)),
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(source) => Err(FileNodeStoreError::Io { path, source }),
        }
    }

    fn put_hint(&self, namespace: &[u8], key: &[u8], value: &[u8]) -> Result<(), Self::Error> {
        write_file_atomically(&self.hint_path(namespace, key), value)
    }

    fn batch_put_with_hint(
        &self,
        entries: &[(&[u8], &[u8])],
        namespace: &[u8],
        key: &[u8],
        value: &[u8],
    ) -> Result<(), Self::Error> {
        self.batch_put(entries)?;
        self.put_hint(namespace, key, value)
    }
}

impl NodeStoreScan for FileNodeStore {
    type Error = FileNodeStoreError;

    fn list_node_cids(&self) -> Result<Vec<Cid>, Self::Error> {
        let namespace = self.node_namespace_dir();
        if !namespace.exists() {
            return Ok(Vec::new());
        }

        let mut cids = Vec::new();
        for first in read_visible_dir_entries(&namespace)? {
            ensure_shard_dir(&first, 2)?;
            for second in read_visible_dir_entries(&first.path())? {
                ensure_shard_dir(&second, 2)?;
                for node in read_visible_dir_entries(&second.path())? {
                    let path = node.path();
                    let file_type = node.file_type().map_err(|source| FileNodeStoreError::Io {
                        path: path.clone(),
                        source,
                    })?;
                    if !file_type.is_file() {
                        return Err(FileNodeStoreError::InvalidPath {
                            path,
                            message: "node namespace entry is not a file".to_string(),
                        });
                    }

                    let name = entry_name(&node)?;
                    let cid =
                        parse_cid_hex(&name).ok_or_else(|| FileNodeStoreError::InvalidPath {
                            path: path.clone(),
                            message: "node filename is not a 64-byte hex CID".to_string(),
                        })?;
                    if name[0..2] != entry_name(&first)? || name[2..4] != entry_name(&second)? {
                        return Err(FileNodeStoreError::InvalidPath {
                            path,
                            message: "node CID does not match shard directories".to_string(),
                        });
                    }
                    cids.push(cid);
                }
            }
        }

        sort_cids(&mut cids);
        Ok(cids)
    }
}

impl ManifestStore for FileNodeStore {
    type Error = FileNodeStoreError;

    fn get_root(&self, name: &[u8]) -> Result<Option<RootManifest>, Self::Error> {
        self.read_root_path(&self.root_path(name))
    }

    fn put_root(&self, name: &[u8], manifest: &RootManifest) -> Result<(), Self::Error> {
        let _guard = self
            .manifest_lock
            .lock()
            .map_err(|err| FileNodeStoreError::LockPoisoned(err.to_string()))?;
        self.write_root_path(&self.root_path(name), manifest)
    }

    fn delete_root(&self, name: &[u8]) -> Result<(), Self::Error> {
        let _guard = self
            .manifest_lock
            .lock()
            .map_err(|err| FileNodeStoreError::LockPoisoned(err.to_string()))?;
        let path = self.root_path(name);
        match fs::remove_file(&path) {
            Ok(()) => Ok(()),
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(source) => Err(FileNodeStoreError::Io { path, source }),
        }
    }

    fn compare_and_swap_root(
        &self,
        name: &[u8],
        expected: Option<&RootManifest>,
        new: Option<&RootManifest>,
    ) -> Result<ManifestUpdate, Self::Error> {
        let _guard = self
            .manifest_lock
            .lock()
            .map_err(|err| FileNodeStoreError::LockPoisoned(err.to_string()))?;
        let path = self.root_path(name);
        let current = self.read_root_path(&path)?;
        if current.as_ref() != expected {
            return Ok(ManifestUpdate::Conflict { current });
        }

        match new {
            Some(manifest) => self.write_root_path(&path, manifest)?,
            None => match fs::remove_file(&path) {
                Ok(()) => {}
                Err(source) if source.kind() == std::io::ErrorKind::NotFound => {}
                Err(source) => return Err(FileNodeStoreError::Io { path, source }),
            },
        }

        Ok(ManifestUpdate::Applied)
    }
}

impl ManifestStoreScan for FileNodeStore {
    fn list_roots(&self) -> Result<Vec<NamedRootManifest>, Self::Error> {
        let namespace = self.root_namespace_dir();
        if !namespace.exists() {
            return Ok(Vec::new());
        }

        let mut roots = Vec::new();
        for entry in read_visible_dir_entries(&namespace)? {
            let path = entry.path();
            let file_type = entry.file_type().map_err(|source| FileNodeStoreError::Io {
                path: path.clone(),
                source,
            })?;
            if !file_type.is_file() {
                return Err(FileNodeStoreError::InvalidPath {
                    path,
                    message: "root namespace entry is not a file".to_string(),
                });
            }

            let name_hex = entry_name(&entry)?;
            let name =
                decode_hex_label(&name_hex).ok_or_else(|| FileNodeStoreError::InvalidPath {
                    path: entry.path(),
                    message: "root filename is not an encoded name".to_string(),
                })?;
            let Some(manifest) = self.read_root_path(&entry.path())? else {
                continue;
            };
            roots.push(NamedRootManifest::new(name, manifest));
        }

        sort_named_root_manifests(&mut roots);
        Ok(roots)
    }
}

#[derive(Debug)]
enum BatchOpOwned {
    Upsert { cid: Cid, value: Vec<u8> },
    Delete { cid: Cid },
}

/// Error type for [`FileNodeStore`].
#[derive(Debug)]
pub enum FileNodeStoreError {
    /// Filesystem I/O failed for the given path.
    Io {
        /// Path involved in the failed operation.
        path: PathBuf,
        /// Underlying I/O error.
        source: std::io::Error,
    },
    /// A store key was not a 32-byte CID.
    InvalidKey {
        /// Validation failure description.
        message: String,
    },
    /// A node object's bytes do not match its CID key.
    CidMismatch {
        /// Path involved in the validation failure.
        path: PathBuf,
        /// CID requested by the caller or encoded in the path.
        expected: Cid,
        /// CID computed from the stored bytes.
        actual: Cid,
    },
    /// The store namespace contains an invalid path or filename.
    InvalidPath {
        /// Invalid path.
        path: PathBuf,
        /// Validation failure description.
        message: String,
    },
    /// A named root manifest could not be encoded or decoded.
    Manifest {
        /// Manifest path involved in the failure.
        path: PathBuf,
        /// Validation failure description.
        message: String,
    },
    /// A local manifest lock was poisoned.
    LockPoisoned(String),
}

impl std::fmt::Display for FileNodeStoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io { path, source } => {
                write!(
                    f,
                    "file node store I/O error at {}: {source}",
                    path.display()
                )
            }
            Self::InvalidKey { message } => write!(f, "invalid file node key: {message}"),
            Self::CidMismatch {
                path,
                expected,
                actual,
            } => write!(
                f,
                "file node CID mismatch at {}: expected {:?}, got {:?}",
                path.display(),
                expected,
                actual
            ),
            Self::InvalidPath { path, message } => {
                write!(f, "invalid file node path {}: {message}", path.display())
            }
            Self::Manifest { path, message } => {
                write!(f, "invalid file node root {}: {message}", path.display())
            }
            Self::LockPoisoned(message) => write!(f, "file node manifest lock poisoned: {message}"),
        }
    }
}

impl std::error::Error for FileNodeStoreError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io { source, .. } => Some(source),
            Self::InvalidKey { .. }
            | Self::CidMismatch { .. }
            | Self::InvalidPath { .. }
            | Self::Manifest { .. }
            | Self::LockPoisoned(_) => None,
        }
    }
}

fn cid_from_node_key(key: &[u8]) -> Result<Cid, FileNodeStoreError> {
    let bytes: [u8; 32] = key.try_into().map_err(|_| FileNodeStoreError::InvalidKey {
        message: format!("node key has length {}, expected 32", key.len()),
    })?;
    Ok(Cid(bytes))
}

fn validate_node_bytes(
    expected: &Cid,
    path: &Path,
    bytes: &[u8],
) -> Result<(), FileNodeStoreError> {
    let actual = Cid::from_bytes(bytes);
    if &actual == expected {
        Ok(())
    } else {
        Err(FileNodeStoreError::CidMismatch {
            path: path.to_path_buf(),
            expected: expected.clone(),
            actual,
        })
    }
}

fn write_file_atomically(path: &Path, bytes: &[u8]) -> Result<(), FileNodeStoreError> {
    let Some(parent) = path.parent() else {
        return Err(FileNodeStoreError::InvalidPath {
            path: path.to_path_buf(),
            message: "path has no parent directory".to_string(),
        });
    };
    fs::create_dir_all(parent).map_err(|source| FileNodeStoreError::Io {
        path: parent.to_path_buf(),
        source,
    })?;

    let mut temp_path = temp_path_for(path);
    let mut file = match OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temp_path)
    {
        Ok(file) => file,
        Err(source) if source.kind() == std::io::ErrorKind::AlreadyExists => {
            temp_path = temp_path_for(path);
            OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&temp_path)
                .map_err(|source| FileNodeStoreError::Io {
                    path: temp_path.clone(),
                    source,
                })?
        }
        Err(source) => {
            return Err(FileNodeStoreError::Io {
                path: temp_path,
                source,
            });
        }
    };

    if let Err(source) = file.write_all(bytes) {
        let _ = fs::remove_file(&temp_path);
        return Err(FileNodeStoreError::Io {
            path: temp_path,
            source,
        });
    }
    if let Err(source) = file.sync_all() {
        let _ = fs::remove_file(&temp_path);
        return Err(FileNodeStoreError::Io {
            path: temp_path,
            source,
        });
    }
    drop(file);

    if let Err(source) = fs::rename(&temp_path, path) {
        let _ = fs::remove_file(&temp_path);
        return Err(FileNodeStoreError::Io {
            path: path.to_path_buf(),
            source,
        });
    }
    Ok(())
}

fn temp_path_for(path: &Path) -> PathBuf {
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("object");
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    path.with_file_name(format!(".{file_name}.{}.{}.tmp", std::process::id(), nanos))
}

fn read_visible_dir_entries(path: &Path) -> Result<Vec<DirEntry>, FileNodeStoreError> {
    let mut entries = Vec::new();
    let dir = fs::read_dir(path).map_err(|source| FileNodeStoreError::Io {
        path: path.to_path_buf(),
        source,
    })?;

    for entry in dir {
        let entry = entry.map_err(|source| FileNodeStoreError::Io {
            path: path.to_path_buf(),
            source,
        })?;
        let name = entry_name(&entry)?;
        if name.starts_with('.') {
            continue;
        }
        entries.push(entry);
    }

    entries.sort_by_key(|entry| entry.file_name());
    Ok(entries)
}

fn ensure_shard_dir(entry: &DirEntry, len: usize) -> Result<(), FileNodeStoreError> {
    let path = entry.path();
    let file_type = entry.file_type().map_err(|source| FileNodeStoreError::Io {
        path: path.clone(),
        source,
    })?;
    if !file_type.is_dir() {
        return Err(FileNodeStoreError::InvalidPath {
            path,
            message: "node shard entry is not a directory".to_string(),
        });
    }

    let name = entry_name(entry)?;
    if name.len() != len || !name.as_bytes().iter().all(|byte| is_lower_hex_byte(*byte)) {
        return Err(FileNodeStoreError::InvalidPath {
            path,
            message: "node shard directory is not lowercase hex".to_string(),
        });
    }
    Ok(())
}

fn entry_name(entry: &DirEntry) -> Result<String, FileNodeStoreError> {
    entry
        .file_name()
        .into_string()
        .map_err(|name| FileNodeStoreError::InvalidPath {
            path: entry.path(),
            message: format!("path is not valid UTF-8: {name:?}"),
        })
}

fn remove_empty_dirs(path: &Path, namespace: &Path) {
    let Some(second) = path.parent() else {
        return;
    };
    let _ = fs::remove_dir(second);
    let Some(first) = second.parent() else {
        return;
    };
    if first != namespace {
        let _ = fs::remove_dir(first);
    }
}

fn cid_hex(cid: &Cid) -> String {
    bytes_hex(cid.as_bytes())
}

fn parse_cid_hex(input: &str) -> Option<Cid> {
    if input.len() != 64 || !input.as_bytes().iter().all(|byte| is_lower_hex_byte(*byte)) {
        return None;
    }
    let bytes = decode_hex(input)?;
    let bytes: [u8; 32] = bytes.try_into().ok()?;
    Some(Cid(bytes))
}

fn hex_label(bytes: &[u8]) -> String {
    if bytes.is_empty() {
        "_".to_string()
    } else {
        bytes_hex(bytes)
    }
}

fn decode_hex_label(input: &str) -> Option<Vec<u8>> {
    if input == "_" {
        return Some(Vec::new());
    }
    if input.is_empty() {
        return None;
    }
    decode_hex(input)
}

fn bytes_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

fn decode_hex(input: &str) -> Option<Vec<u8>> {
    if input.len() % 2 != 0 || !input.as_bytes().iter().all(|byte| is_lower_hex_byte(*byte)) {
        return None;
    }
    input
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| Some((hex_value(pair[0])? << 4) | hex_value(pair[1])?))
        .collect()
}

fn is_lower_hex_byte(byte: u8) -> bool {
    byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)
}

fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        _ => None,
    }
}
