//! PGlite storage backend implementation.
//!
//! PGlite runs as a JavaScript/WASM PostgreSQL runtime. The Rust store owns a
//! Node.js sidecar and exchanges JSONL requests over stdio.

use std::collections::HashMap;
use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::sync::{Mutex, MutexGuard};

use serde_json::{json, Map, Value};

use super::super::manifest::{
    sort_named_root_manifests, ManifestStore, ManifestStoreScan, ManifestUpdate, NamedRootManifest,
    RootManifest,
};
use super::{cid_from_store_key, sort_cids, BatchOp, NodeStoreScan, OrderedBatchReadPlan, Store};

const SIDECAR_SCRIPT: &str = r#"
import { PGlite } from '@electric-sql/pglite';
import readline from 'node:readline';

const dataDir = process.env.PROLLY_PGLITE_DATA_DIR || 'memory://';
const db = await PGlite.create({ dataDir });

await db.exec(`
CREATE TABLE IF NOT EXISTS prolly_nodes (
  cid bytea PRIMARY KEY,
  node bytea NOT NULL
);
CREATE TABLE IF NOT EXISTS prolly_hints (
  namespace bytea NOT NULL,
  key bytea NOT NULL,
  value bytea NOT NULL,
  PRIMARY KEY(namespace, key)
);
CREATE TABLE IF NOT EXISTS prolly_roots (
  name bytea PRIMARY KEY,
  manifest bytea NOT NULL
);
`);

function send(value) {
  process.stdout.write(JSON.stringify(value) + '\n');
}

async function readNode(key) {
  const result = await db.query(
    "SELECT encode(node, 'hex') AS node FROM prolly_nodes WHERE cid = decode($1, 'hex')",
    [key]
  );
  return result.rows.length === 0 ? null : result.rows[0].node;
}

async function upsertNode(key, value) {
  await db.query(
    "INSERT INTO prolly_nodes (cid, node) VALUES (decode($1, 'hex'), decode($2, 'hex')) " +
      "ON CONFLICT(cid) DO UPDATE SET node = excluded.node",
    [key, value]
  );
}

async function deleteNode(key) {
  await db.query("DELETE FROM prolly_nodes WHERE cid = decode($1, 'hex')", [key]);
}

async function listNodeCids() {
  const result = await db.query(
    "SELECT encode(cid, 'hex') AS cid FROM prolly_nodes ORDER BY cid"
  );
  return result.rows.map((row) => row.cid);
}

async function readHint(namespace, key) {
  const result = await db.query(
    "SELECT encode(value, 'hex') AS value FROM prolly_hints " +
      "WHERE namespace = decode($1, 'hex') AND key = decode($2, 'hex')",
    [namespace, key]
  );
  return result.rows.length === 0 ? null : result.rows[0].value;
}

async function upsertHint(namespace, key, value) {
  await db.query(
    "INSERT INTO prolly_hints (namespace, key, value) " +
      "VALUES (decode($1, 'hex'), decode($2, 'hex'), decode($3, 'hex')) " +
      "ON CONFLICT(namespace, key) DO UPDATE SET value = excluded.value",
    [namespace, key, value]
  );
}

async function readRoot(name) {
  const result = await db.query(
    "SELECT encode(manifest, 'hex') AS manifest FROM prolly_roots WHERE name = decode($1, 'hex')",
    [name]
  );
  return result.rows.length === 0 ? null : result.rows[0].manifest;
}

async function listRoots() {
  const result = await db.query(
    "SELECT encode(name, 'hex') AS name, encode(manifest, 'hex') AS manifest " +
      "FROM prolly_roots ORDER BY name"
  );
  return result.rows;
}

async function upsertRoot(name, manifest) {
  await db.query(
    "INSERT INTO prolly_roots (name, manifest) VALUES (decode($1, 'hex'), decode($2, 'hex')) " +
      "ON CONFLICT(name) DO UPDATE SET manifest = excluded.manifest",
    [name, manifest]
  );
}

async function deleteRoot(name) {
  await db.query("DELETE FROM prolly_roots WHERE name = decode($1, 'hex')", [name]);
}

async function inTransaction(fn) {
  await db.exec('BEGIN');
  try {
    const value = await fn();
    await db.exec('COMMIT');
    return value;
  } catch (error) {
    try {
      await db.exec('ROLLBACK');
    } catch {
      // Preserve the original error.
    }
    throw error;
  }
}

async function handle(request) {
  switch (request.op) {
    case 'get':
      return { value: await readNode(request.key) };
    case 'put':
      await upsertNode(request.key, request.value);
      return {};
    case 'delete':
      await deleteNode(request.key);
      return {};
    case 'batch':
      await inTransaction(async () => {
        for (const op of request.ops || []) {
          if (op.kind === 'upsert') {
            await upsertNode(op.key, op.value);
          } else if (op.kind === 'delete') {
            await deleteNode(op.key);
          } else {
            throw new Error(`unknown batch op: ${op.kind}`);
          }
        }
      });
      return {};
    case 'batch_get': {
      const values = {};
      for (const key of request.keys || []) {
        const value = await readNode(key);
        if (value !== null) {
          values[key] = value;
        }
      }
      return { values };
    }
    case 'batch_get_ordered': {
      const values = [];
      for (const key of request.keys || []) {
        values.push(await readNode(key));
      }
      return { values };
    }
    case 'list_node_cids':
      return { cids: await listNodeCids() };
    case 'batch_put':
      await inTransaction(async () => {
        for (const entry of request.entries || []) {
          await upsertNode(entry.key, entry.value);
        }
      });
      return {};
    case 'get_hint':
      return { value: await readHint(request.namespace, request.key) };
    case 'put_hint':
      await upsertHint(request.namespace, request.key, request.value);
      return {};
    case 'batch_put_with_hint':
      await inTransaction(async () => {
        for (const entry of request.entries || []) {
          await upsertNode(entry.key, entry.value);
        }
        await upsertHint(request.namespace, request.key, request.value);
      });
      return {};
    case 'get_root':
      return { manifest: await readRoot(request.name) };
    case 'list_roots':
      return { roots: await listRoots() };
    case 'put_root':
      await upsertRoot(request.name, request.manifest);
      return {};
    case 'delete_root':
      await deleteRoot(request.name);
      return {};
    case 'compare_and_swap_root':
      return await inTransaction(async () => {
        const current = await readRoot(request.name);
        if (current !== request.expected) {
          return { applied: false, current };
        }
        if (request.new === null) {
          await deleteRoot(request.name);
        } else {
          await upsertRoot(request.name, request.new);
        }
        return { applied: true, current: request.new };
      });
    case 'shutdown':
      await db.close();
      return { shutdown: true };
    default:
      throw new Error(`unknown op: ${request.op}`);
  }
}

send({ ready: true });

const rl = readline.createInterface({ input: process.stdin, crlfDelay: Infinity });
for await (const line of rl) {
  if (!line.trim()) {
    continue;
  }
  let request;
  try {
    request = JSON.parse(line);
    const result = await handle(request);
    send({ id: request.id, ok: true, result });
    if (request.op === 'shutdown') {
      process.exit(0);
    }
  } catch (error) {
    send({
      id: request?.id ?? null,
      ok: false,
      error: error && error.stack ? error.stack : String(error)
    });
  }
}
"#;

/// Configuration options for [`PgliteStore`].
#[derive(Debug, Clone)]
pub struct PgliteStoreConfig {
    /// PGlite data directory. Use `memory://` for an in-memory store.
    pub data_dir: String,
    /// Node.js executable.
    pub node_command: PathBuf,
    /// Working directory used for Node module resolution.
    pub node_working_dir: Option<PathBuf>,
}

impl Default for PgliteStoreConfig {
    fn default() -> Self {
        Self {
            data_dir: "memory://".to_string(),
            node_command: std::env::var_os("PROLLY_PGLITE_NODE")
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from("node")),
            node_working_dir: std::env::var_os("PROLLY_PGLITE_NODE_CWD").map(PathBuf::from),
        }
    }
}

/// Error type for PGlite store operations.
#[derive(Debug)]
pub struct PgliteStoreError {
    message: String,
    source: Option<Box<dyn std::error::Error + Send + Sync>>,
}

impl PgliteStoreError {
    /// Create a new error with a message.
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            source: None,
        }
    }

    fn with_source(
        message: impl Into<String>,
        source: impl std::error::Error + Send + Sync + 'static,
    ) -> Self {
        Self {
            message: message.into(),
            source: Some(Box::new(source)),
        }
    }
}

impl std::fmt::Display for PgliteStoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "PGlite error: {}", self.message)
    }
}

impl std::error::Error for PgliteStoreError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source
            .as_ref()
            .map(|e| e.as_ref() as &(dyn std::error::Error + 'static))
    }
}

/// PGlite-backed storage backend for Prolly Trees.
pub struct PgliteStore {
    process: Mutex<PgliteProcess>,
}

struct PgliteProcess {
    child: Child,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
    next_id: u64,
}

impl PgliteStore {
    /// Open an in-memory PGlite store with default configuration.
    pub fn open_in_memory() -> Result<Self, PgliteStoreError> {
        Self::open_with_config(PgliteStoreConfig::default())
    }

    /// Open a PGlite store at the given data directory.
    pub fn open(data_dir: impl Into<String>) -> Result<Self, PgliteStoreError> {
        Self::open_with_config(PgliteStoreConfig {
            data_dir: data_dir.into(),
            ..PgliteStoreConfig::default()
        })
    }

    /// Open a PGlite store with custom configuration.
    pub fn open_with_config(config: PgliteStoreConfig) -> Result<Self, PgliteStoreError> {
        let mut command = Command::new(&config.node_command);
        command
            .arg("--input-type=module")
            .arg("-e")
            .arg(SIDECAR_SCRIPT)
            .env("PROLLY_PGLITE_DATA_DIR", &config.data_dir)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        if let Some(working_dir) = config.node_working_dir.as_deref() {
            command.current_dir(working_dir);
        }

        let mut child = command.spawn().map_err(|e| {
            PgliteStoreError::with_source(
                format!(
                    "failed to spawn `{}` PGlite sidecar",
                    config.node_command.display()
                ),
                e,
            )
        })?;
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| PgliteStoreError::new("PGlite sidecar stdin unavailable"))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| PgliteStoreError::new("PGlite sidecar stdout unavailable"))?;

        let mut process = PgliteProcess {
            child,
            stdin,
            stdout: BufReader::new(stdout),
            next_id: 1,
        };
        let ready = process.read_response_line()?;
        if ready.get("ready").and_then(Value::as_bool) != Some(true) {
            return Err(PgliteStoreError::new(format!(
                "PGlite sidecar returned invalid startup response: {ready}"
            )));
        }

        Ok(Self {
            process: Mutex::new(process),
        })
    }

    fn process(&self) -> Result<MutexGuard<'_, PgliteProcess>, PgliteStoreError> {
        self.process
            .lock()
            .map_err(|e| PgliteStoreError::new(format!("lock poisoned: {}", e)))
    }

    fn request(&self, op: &str, mut fields: Map<String, Value>) -> Result<Value, PgliteStoreError> {
        let mut process = self.process()?;
        let id = process.next_id;
        process.next_id += 1;
        fields.insert("id".to_string(), json!(id));
        fields.insert("op".to_string(), json!(op));

        let line = serde_json::to_string(&Value::Object(fields))
            .map_err(|e| PgliteStoreError::with_source("failed to encode PGlite request", e))?;
        process
            .stdin
            .write_all(line.as_bytes())
            .map_err(|e| PgliteStoreError::with_source("failed to write PGlite request", e))?;
        process
            .stdin
            .write_all(b"\n")
            .map_err(|e| PgliteStoreError::with_source("failed to terminate PGlite request", e))?;
        process
            .stdin
            .flush()
            .map_err(|e| PgliteStoreError::with_source("failed to flush PGlite request", e))?;

        let response = process.read_response_line()?;
        if response.get("id").and_then(Value::as_u64) != Some(id) {
            return Err(PgliteStoreError::new(format!(
                "PGlite sidecar response id mismatch for request {id}: {response}"
            )));
        }
        if response.get("ok").and_then(Value::as_bool) != Some(true) {
            let message = response
                .get("error")
                .and_then(Value::as_str)
                .unwrap_or("unknown PGlite sidecar error");
            return Err(PgliteStoreError::new(message.to_string()));
        }
        Ok(response.get("result").cloned().unwrap_or(Value::Null))
    }
}

impl Drop for PgliteStore {
    fn drop(&mut self) {
        if let Ok(process) = self.process.get_mut() {
            let id = process.next_id;
            let line = json!({ "id": id, "op": "shutdown" }).to_string();
            let _ = process.stdin.write_all(line.as_bytes());
            let _ = process.stdin.write_all(b"\n");
            let _ = process.stdin.flush();
            let _ = process.read_response_line();
            if process.child.try_wait().ok().flatten().is_none() {
                let _ = process.child.kill();
                let _ = process.child.wait();
            }
        }
    }
}

impl PgliteProcess {
    fn read_response_line(&mut self) -> Result<Value, PgliteStoreError> {
        let mut line = String::new();
        let bytes = self
            .stdout
            .read_line(&mut line)
            .map_err(|e| PgliteStoreError::with_source("failed to read PGlite response", e))?;
        if bytes == 0 {
            let mut stderr = String::new();
            if let Some(stream) = self.child.stderr.as_mut() {
                let _ = std::io::Read::read_to_string(stream, &mut stderr);
            }
            let detail = if stderr.trim().is_empty() {
                "sidecar closed stdout".to_string()
            } else {
                format!("sidecar closed stdout; stderr: {}", stderr.trim())
            };
            return Err(PgliteStoreError::new(detail));
        }
        serde_json::from_str(line.trim_end())
            .map_err(|e| PgliteStoreError::with_source("failed to decode PGlite response", e))
    }
}

impl Store for PgliteStore {
    type Error = PgliteStoreError;

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        let result = self.request("get", map_with_key(key))?;
        hex_option(result.get("value"))
    }

    fn put(&self, key: &[u8], value: &[u8]) -> Result<(), Self::Error> {
        let mut fields = map_with_key(key);
        fields.insert("value".to_string(), json!(hex::encode(value)));
        self.request("put", fields)?;
        Ok(())
    }

    fn delete(&self, key: &[u8]) -> Result<(), Self::Error> {
        self.request("delete", map_with_key(key))?;
        Ok(())
    }

    fn batch(&self, ops: &[BatchOp]) -> Result<(), Self::Error> {
        let ops = ops
            .iter()
            .map(|op| match op {
                BatchOp::Upsert { key, value } => json!({
                    "kind": "upsert",
                    "key": hex::encode(key),
                    "value": hex::encode(value)
                }),
                BatchOp::Delete { key } => json!({
                    "kind": "delete",
                    "key": hex::encode(key)
                }),
            })
            .collect::<Vec<_>>();
        let mut fields = Map::new();
        fields.insert("ops".to_string(), Value::Array(ops));
        self.request("batch", fields)?;
        Ok(())
    }

    fn batch_get(&self, keys: &[&[u8]]) -> Result<HashMap<Vec<u8>, Vec<u8>>, Self::Error> {
        let plan = OrderedBatchReadPlan::new(keys);
        let mut fields = Map::new();
        let key_hex = plan
            .unique_keys()
            .iter()
            .map(hex::encode)
            .collect::<Vec<_>>();
        fields.insert("keys".to_string(), json!(key_hex));
        let result = self.request("batch_get", fields)?;
        let values = result
            .get("values")
            .and_then(Value::as_object)
            .ok_or_else(|| PgliteStoreError::new("PGlite batch_get response missing values"))?;
        let mut decoded = HashMap::with_capacity(values.len());
        for (key, value) in values {
            decoded.insert(
                hex_decode(key, "batch_get key")?,
                hex_value(value, "value")?,
            );
        }
        Ok(decoded)
    }

    fn batch_get_ordered(&self, keys: &[&[u8]]) -> Result<Vec<Option<Vec<u8>>>, Self::Error> {
        let plan = OrderedBatchReadPlan::new(keys);
        let mut fields = Map::new();
        let key_hex = plan
            .unique_keys()
            .iter()
            .map(hex::encode)
            .collect::<Vec<_>>();
        fields.insert("keys".to_string(), json!(key_hex));
        let result = self.request("batch_get_ordered", fields)?;
        let values = result
            .get("values")
            .and_then(Value::as_array)
            .ok_or_else(|| {
                PgliteStoreError::new("PGlite batch_get_ordered response missing values")
            })?;
        let unique_values = values
            .iter()
            .map(|value| hex_option(Some(value)))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(plan.expand_owned(unique_values))
    }

    fn batch_get_ordered_unique(
        &self,
        keys: &[&[u8]],
    ) -> Result<Vec<Option<Vec<u8>>>, Self::Error> {
        let mut fields = Map::new();
        let key_hex = keys.iter().map(hex::encode).collect::<Vec<_>>();
        fields.insert("keys".to_string(), json!(key_hex));
        let result = self.request("batch_get_ordered", fields)?;
        let values = result
            .get("values")
            .and_then(Value::as_array)
            .ok_or_else(|| {
                PgliteStoreError::new("PGlite unique ordered batch response missing values")
            })?;
        values
            .iter()
            .map(|value| hex_option(Some(value)))
            .collect::<Result<Vec<_>, _>>()
    }

    fn prefers_batch_reads(&self) -> bool {
        true
    }

    fn batch_put(&self, entries: &[(&[u8], &[u8])]) -> Result<(), Self::Error> {
        let entries = entries
            .iter()
            .map(|(key, value)| {
                json!({
                    "key": hex::encode(key),
                    "value": hex::encode(value)
                })
            })
            .collect::<Vec<_>>();
        let mut fields = Map::new();
        fields.insert("entries".to_string(), Value::Array(entries));
        self.request("batch_put", fields)?;
        Ok(())
    }

    fn supports_hints(&self) -> bool {
        true
    }

    fn get_hint(&self, namespace: &[u8], key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        let result = self.request("get_hint", hint_fields(namespace, key))?;
        hex_option(result.get("value"))
    }

    fn put_hint(&self, namespace: &[u8], key: &[u8], value: &[u8]) -> Result<(), Self::Error> {
        let mut fields = hint_fields(namespace, key);
        fields.insert("value".to_string(), json!(hex::encode(value)));
        self.request("put_hint", fields)?;
        Ok(())
    }

    fn batch_put_with_hint(
        &self,
        entries: &[(&[u8], &[u8])],
        namespace: &[u8],
        key: &[u8],
        value: &[u8],
    ) -> Result<(), Self::Error> {
        let entries = entries
            .iter()
            .map(|(key, value)| {
                json!({
                    "key": hex::encode(key),
                    "value": hex::encode(value)
                })
            })
            .collect::<Vec<_>>();
        let mut fields = hint_fields(namespace, key);
        fields.insert("entries".to_string(), Value::Array(entries));
        fields.insert("value".to_string(), json!(hex::encode(value)));
        self.request("batch_put_with_hint", fields)?;
        Ok(())
    }
}

impl NodeStoreScan for PgliteStore {
    type Error = PgliteStoreError;

    fn list_node_cids(&self) -> Result<Vec<super::super::cid::Cid>, Self::Error> {
        let result = self.request("list_node_cids", Map::new())?;
        let raw_cids = result
            .get("cids")
            .and_then(Value::as_array)
            .ok_or_else(|| PgliteStoreError::new("PGlite list_node_cids response missing cids"))?;
        let mut cids = raw_cids
            .iter()
            .map(|value| {
                let key = hex_value(value, "cid")?;
                cid_from_store_key(&key, "PGlite node").map_err(PgliteStoreError::new)
            })
            .collect::<Result<Vec<_>, _>>()?;
        sort_cids(&mut cids);
        Ok(cids)
    }
}

impl ManifestStore for PgliteStore {
    type Error = PgliteStoreError;

    fn get_root(&self, name: &[u8]) -> Result<Option<RootManifest>, Self::Error> {
        let result = self.request("get_root", root_fields(name))?;
        root_manifest_option(result.get("manifest"))
    }

    fn put_root(&self, name: &[u8], manifest: &RootManifest) -> Result<(), Self::Error> {
        let mut fields = root_fields(name);
        fields.insert("manifest".to_string(), json!(root_manifest_hex(manifest)?));
        self.request("put_root", fields)?;
        Ok(())
    }

    fn delete_root(&self, name: &[u8]) -> Result<(), Self::Error> {
        self.request("delete_root", root_fields(name))?;
        Ok(())
    }

    fn compare_and_swap_root(
        &self,
        name: &[u8],
        expected: Option<&RootManifest>,
        new: Option<&RootManifest>,
    ) -> Result<ManifestUpdate, Self::Error> {
        let mut fields = root_fields(name);
        fields.insert(
            "expected".to_string(),
            optional_root_manifest_json(expected)?,
        );
        fields.insert("new".to_string(), optional_root_manifest_json(new)?);

        let result = self.request("compare_and_swap_root", fields)?;
        let applied = result
            .get("applied")
            .and_then(Value::as_bool)
            .ok_or_else(|| PgliteStoreError::new("PGlite CAS response missing applied flag"))?;
        if applied {
            return Ok(ManifestUpdate::Applied);
        }

        Ok(ManifestUpdate::Conflict {
            current: root_manifest_option(result.get("current"))?,
        })
    }
}

impl ManifestStoreScan for PgliteStore {
    fn list_roots(&self) -> Result<Vec<NamedRootManifest>, Self::Error> {
        let result = self.request("list_roots", Map::new())?;
        let raw_roots = result
            .get("roots")
            .and_then(Value::as_array)
            .ok_or_else(|| PgliteStoreError::new("PGlite list_roots response missing roots"))?;
        let mut roots = Vec::with_capacity(raw_roots.len());
        for value in raw_roots {
            let name = hex_value(
                value
                    .get("name")
                    .ok_or_else(|| PgliteStoreError::new("PGlite root listing missing name"))?,
                "root name",
            )?;
            let manifest = root_manifest_option(value.get("manifest"))?
                .ok_or_else(|| PgliteStoreError::new("PGlite root listing missing manifest"))?;
            roots.push(NamedRootManifest::new(name, manifest));
        }
        sort_named_root_manifests(&mut roots);
        Ok(roots)
    }
}

fn map_with_key(key: &[u8]) -> Map<String, Value> {
    let mut fields = Map::new();
    fields.insert("key".to_string(), json!(hex::encode(key)));
    fields
}

fn root_fields(name: &[u8]) -> Map<String, Value> {
    let mut fields = Map::new();
    fields.insert("name".to_string(), json!(hex::encode(name)));
    fields
}

fn hint_fields(namespace: &[u8], key: &[u8]) -> Map<String, Value> {
    let mut fields = Map::new();
    fields.insert("namespace".to_string(), json!(hex::encode(namespace)));
    fields.insert("key".to_string(), json!(hex::encode(key)));
    fields
}

fn optional_root_manifest_json(manifest: Option<&RootManifest>) -> Result<Value, PgliteStoreError> {
    manifest
        .map(root_manifest_hex)
        .transpose()
        .map(|manifest| manifest.map_or(Value::Null, Value::String))
}

fn root_manifest_hex(manifest: &RootManifest) -> Result<String, PgliteStoreError> {
    manifest
        .to_bytes()
        .map(hex::encode)
        .map_err(|e| PgliteStoreError::new(format!("failed to encode root manifest: {e}")))
}

fn root_manifest_option(value: Option<&Value>) -> Result<Option<RootManifest>, PgliteStoreError> {
    let Some(value) = value else {
        return Ok(None);
    };
    if value.is_null() {
        return Ok(None);
    }
    let bytes = hex_value(value, "manifest")?;
    RootManifest::from_bytes(&bytes)
        .map(Some)
        .map_err(|e| PgliteStoreError::new(format!("failed to decode root manifest: {e}")))
}

fn hex_option(value: Option<&Value>) -> Result<Option<Vec<u8>>, PgliteStoreError> {
    let Some(value) = value else {
        return Ok(None);
    };
    if value.is_null() {
        return Ok(None);
    }
    hex_value(value, "value").map(Some)
}

fn hex_value(value: &Value, name: &str) -> Result<Vec<u8>, PgliteStoreError> {
    let raw = value.as_str().ok_or_else(|| {
        PgliteStoreError::new(format!(
            "PGlite response field `{name}` is not a hex string"
        ))
    })?;
    hex_decode(raw, name)
}

fn hex_decode(raw: &str, name: &str) -> Result<Vec<u8>, PgliteStoreError> {
    hex::decode(raw).map_err(|e| {
        PgliteStoreError::with_source(format!("failed to decode PGlite `{name}` hex"), e)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pglite_store_round_trips_nodes_and_hints_when_enabled() {
        if std::env::var("PROLLY_PGLITE_TEST").ok().as_deref() != Some("1") {
            return;
        }

        let store = PgliteStore::open_in_memory().unwrap();
        store.put(b"a", b"1").unwrap();
        store.put(b"b", b"2").unwrap();
        assert_eq!(store.get(b"a").unwrap(), Some(b"1".to_vec()));
        assert_eq!(
            store.batch_get_ordered(&[b"a", b"missing", b"b"]).unwrap(),
            vec![Some(b"1".to_vec()), None, Some(b"2".to_vec())]
        );
        store.put_hint(b"ns", b"k", b"hint").unwrap();
        assert_eq!(store.get_hint(b"ns", b"k").unwrap(), Some(b"hint".to_vec()));
        store.delete(b"a").unwrap();
        assert_eq!(store.get(b"a").unwrap(), None);
    }

    #[test]
    fn pglite_store_persists_across_reopen_when_enabled() {
        if std::env::var("PROLLY_PGLITE_TEST").ok().as_deref() != Some("1") {
            return;
        }

        let path =
            std::env::temp_dir().join(format!("crabdb-pglite-store-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&path);
        {
            let store = PgliteStore::open(path.to_string_lossy().to_string()).unwrap();
            store.put(b"a", b"1").unwrap();
        }
        {
            let store = PgliteStore::open(path.to_string_lossy().to_string()).unwrap();
            assert_eq!(store.get(b"a").unwrap(), Some(b"1".to_vec()));
        }
        let _ = std::fs::remove_dir_all(&path);
    }
}
