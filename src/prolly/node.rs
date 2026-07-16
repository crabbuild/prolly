//! Node structure and builder for Prolly Trees

use serde::{Deserialize, Serialize};
use std::mem;
use std::sync::Arc;

use super::cid::Cid;
use super::encoding::{Encoding, INIT_LEVEL};
use super::error::Error;
use super::format::{BoundaryRule, NodeLayoutSpec, TreeFormat};

const COMPACT_MAGIC: &[u8; 4] = b"CRAB";
const COMPACT_VERSION: u64 = 2;

/// A node in the Prolly Tree
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Node {
    /// Keys (sorted, lexicographic byte order)
    pub keys: Vec<Vec<u8>>,
    /// Values: raw bytes for leaves, CIDs for internal nodes
    pub vals: Vec<Vec<u8>>,
    /// Logical entry counts for internal children. Empty for leaf nodes.
    pub child_counts: Vec<u64>,
    /// Leaf node (true) or internal node (false)
    pub leaf: bool,
    /// Tree level (0 = leaf level)
    pub level: u8,
    /// Persisted settings that determine tree shape and node bytes.
    pub format: TreeFormat,
}

#[derive(Clone, Copy, Debug)]
struct ReadEntryMeta {
    key_start: u32,
    key_len: u32,
    value_start: u32,
    value_len: u32,
    child_count: u64,
    key_order: u64,
}

/// Immutable packed node used by read-only traversals.
///
/// Values, and keys in the plain and offset-table layouts, point directly into
/// the retained encoded object. Prefix-compressed keys are reconstructed once
/// into one contiguous arena instead of one allocation per key.
#[derive(Debug)]
pub(crate) struct ReadNode {
    bytes: Arc<[u8]>,
    prefix_keys: Option<Arc<[u8]>>,
    entries: Box<[ReadEntryMeta]>,
    common_prefix_len: u32,
    leaf: bool,
    level: u8,
    format: TreeFormat,
}

impl ReadNode {
    pub(crate) fn from_shared(bytes: Arc<[u8]>) -> Result<Self, Error> {
        let (format, leaf, level, entries, prefix_keys) = {
            let mut cursor = CompactCursor::new(&bytes);
            cursor.expect_magic()?;
            let version = cursor.read_varint()?;
            if version != COMPACT_VERSION {
                return Err(compact_error(format!(
                    "unsupported compact node version {version}"
                )));
            }
            let format_len = cursor.read_usize("tree format length")?;
            let format = if format_len == 0 {
                TreeFormat::default()
            } else {
                TreeFormat::from_canonical_bytes(cursor.read_bytes(format_len)?)?
            };
            format.validate()?;
            let leaf = match cursor.read_varint()? {
                0 => false,
                1 => true,
                other => return Err(compact_error(format!("invalid leaf flag {other}"))),
            };
            let level = cursor.read_u8_varint("level")?;
            let entry_count = cursor.read_usize("entry_count")?;
            let mut entries = Vec::with_capacity(entry_count);
            let mut prefix_keys: Option<Arc<[u8]>> = None;

            match &format.node_layout {
                NodeLayoutSpec::PrefixCompressed => {
                    let mut arena = Vec::<u8>::new();
                    let mut previous_start = 0usize;
                    let mut previous_len = 0usize;
                    for _ in 0..entry_count {
                        let shared = cursor.read_usize("shared key prefix length")?;
                        if shared > previous_len {
                            return Err(compact_error("shared key prefix exceeds previous key"));
                        }
                        let suffix_len = cursor.read_usize("key suffix length")?;
                        let key_start = arena.len();
                        if shared > 0 {
                            arena.extend_from_within(previous_start..previous_start + shared);
                        }
                        arena.extend_from_slice(cursor.read_bytes(suffix_len)?);
                        let key_len = shared
                            .checked_add(suffix_len)
                            .ok_or_else(|| compact_error("key length overflow"))?;
                        let value_len = cursor.read_usize("value length")?;
                        let value_start = cursor.position();
                        cursor.read_bytes(value_len)?;
                        let child_count = if leaf { 0 } else { cursor.read_varint()? };
                        entries.push(ReadEntryMeta {
                            key_start: compact_u32(key_start, "key offset")?,
                            key_len: compact_u32(key_len, "key length")?,
                            value_start: compact_u32(value_start, "value offset")?,
                            value_len: compact_u32(value_len, "value length")?,
                            child_count,
                            key_order: 0,
                        });
                        previous_start = key_start;
                        previous_len = key_len;
                    }
                    prefix_keys = Some(Arc::from(arena.into_boxed_slice()));
                }
                NodeLayoutSpec::Plain => {
                    for _ in 0..entry_count {
                        let key_len = cursor.read_usize("key length")?;
                        let key_start = cursor.position();
                        cursor.read_bytes(key_len)?;
                        let value_len = cursor.read_usize("value length")?;
                        let value_start = cursor.position();
                        cursor.read_bytes(value_len)?;
                        let child_count = if leaf { 0 } else { cursor.read_varint()? };
                        entries.push(ReadEntryMeta {
                            key_start: compact_u32(key_start, "key offset")?,
                            key_len: compact_u32(key_len, "key length")?,
                            value_start: compact_u32(value_start, "value offset")?,
                            value_len: compact_u32(value_len, "value length")?,
                            child_count,
                            key_order: 0,
                        });
                    }
                }
                NodeLayoutSpec::OffsetTable => {
                    let mut offsets = Vec::with_capacity(entry_count);
                    for _ in 0..entry_count {
                        let key_offset = cursor.read_usize("key offset")?;
                        let key_len = cursor.read_usize("key length")?;
                        let value_offset = cursor.read_usize("value offset")?;
                        let value_len = cursor.read_usize("value length")?;
                        let child_count = if leaf { 0 } else { cursor.read_varint()? };
                        offsets.push((key_offset, key_len, value_offset, value_len, child_count));
                    }
                    let payload_len = cursor.read_usize("payload length")?;
                    let payload_start = cursor.position();
                    let payload = cursor.read_bytes(payload_len)?;
                    for (key_offset, key_len, value_offset, value_len, child_count) in offsets {
                        slice_payload(payload, key_offset, key_len, "key")?;
                        slice_payload(payload, value_offset, value_len, "value")?;
                        let key_start = payload_start
                            .checked_add(key_offset)
                            .ok_or_else(|| compact_error("key offset overflow"))?;
                        let value_start = payload_start
                            .checked_add(value_offset)
                            .ok_or_else(|| compact_error("value offset overflow"))?;
                        entries.push(ReadEntryMeta {
                            key_start: compact_u32(key_start, "key offset")?,
                            key_len: compact_u32(key_len, "key length")?,
                            value_start: compact_u32(value_start, "value offset")?,
                            value_len: compact_u32(value_len, "value length")?,
                            child_count,
                            key_order: 0,
                        });
                    }
                }
                NodeLayoutSpec::Custom { id, .. } => {
                    return Err(Error::InvalidFormat(format!(
                        "node layout '{id}' has no registered codec"
                    )));
                }
            }
            if !cursor.is_done() {
                return Err(compact_error("trailing bytes in compact node"));
            }
            (format, leaf, level, entries, prefix_keys)
        };

        let mut entries = entries;
        let key_source = prefix_keys.as_deref().unwrap_or(bytes.as_ref());
        let common_prefix_len = match (entries.first(), entries.last()) {
            (Some(first), Some(last)) => {
                let first = read_slice(key_source, first.key_start, first.key_len)
                    .ok_or(Error::InvalidNode)?;
                let last = read_slice(key_source, last.key_start, last.key_len)
                    .ok_or(Error::InvalidNode)?;
                first
                    .iter()
                    .zip(last)
                    .take_while(|(left, right)| left == right)
                    .count()
            }
            _ => 0,
        };
        for entry in &mut entries {
            let key =
                read_slice(key_source, entry.key_start, entry.key_len).ok_or(Error::InvalidNode)?;
            entry.key_order = key_order_word(key, common_prefix_len);
        }

        let node = Self {
            bytes,
            prefix_keys,
            entries: entries.into_boxed_slice(),
            common_prefix_len: compact_u32(common_prefix_len, "common key prefix length")?,
            leaf,
            level,
            format,
        };
        node.validate()?;
        Ok(node)
    }

    #[inline]
    pub(crate) fn len(&self) -> usize {
        self.entries.len()
    }

    #[inline]
    pub(crate) fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    #[inline]
    pub(crate) fn is_leaf(&self) -> bool {
        self.leaf
    }

    #[inline]
    pub(crate) fn level(&self) -> u8 {
        self.level
    }

    #[inline]
    pub(crate) fn format(&self) -> &TreeFormat {
        &self.format
    }

    #[inline]
    pub(crate) fn key(&self, index: usize) -> Option<&[u8]> {
        let entry = self.entries.get(index)?;
        let source = self.prefix_keys.as_deref().unwrap_or(&self.bytes);
        read_slice(source, entry.key_start, entry.key_len)
    }

    #[inline]
    pub(crate) fn value(&self, index: usize) -> Option<&[u8]> {
        let entry = self.entries.get(index)?;
        read_slice(&self.bytes, entry.value_start, entry.value_len)
    }

    #[inline]
    pub(crate) fn child_count(&self, index: usize) -> Option<u64> {
        (!self.leaf)
            .then(|| self.entries.get(index).map(|entry| entry.child_count))
            .flatten()
    }

    pub(crate) fn child_cid(&self, index: usize) -> Result<Cid, Error> {
        let value = self.value(index).ok_or(Error::InvalidNode)?;
        let bytes: [u8; 32] = value.try_into().map_err(|_| Error::InvalidNode)?;
        Ok(Cid(bytes))
    }

    #[inline]
    pub(crate) fn search(&self, key: &[u8]) -> Result<usize, usize> {
        let source = self.prefix_keys.as_deref().unwrap_or(&self.bytes);
        let common_prefix_len = self.common_prefix_len as usize;
        if common_prefix_len > 0 {
            // Every key has this prefix because the first and last sorted keys
            // have it. A query that diverges inside it is outside the node's
            // complete key interval and needs no entry-level comparisons.
            let first = unsafe { self.entries.get_unchecked(0) };
            let common = unsafe {
                std::slice::from_raw_parts(
                    source.as_ptr().add(first.key_start as usize),
                    common_prefix_len,
                )
            };
            let shared = key.len().min(common_prefix_len);
            match key[..shared].cmp(&common[..shared]) {
                std::cmp::Ordering::Less => return Err(0),
                std::cmp::Ordering::Greater => return Err(self.len()),
                std::cmp::Ordering::Equal if key.len() < common_prefix_len => return Err(0),
                std::cmp::Ordering::Equal => {}
            }
        }
        let query_order = key_order_word(key, common_prefix_len);
        let mut left = 0usize;
        let mut right = self.len();
        while left < right {
            let mid = left + (right - left) / 2;
            // SAFETY: `mid < self.len()` by the binary-search invariant, and
            // construction validated this immutable metadata range against
            // `source` before the node was published.
            let candidate = unsafe {
                let entry = self.entries.get_unchecked(mid);
                match entry.key_order.cmp(&query_order) {
                    std::cmp::Ordering::Less => {
                        left = mid + 1;
                        continue;
                    }
                    std::cmp::Ordering::Greater => {
                        right = mid;
                        continue;
                    }
                    std::cmp::Ordering::Equal => {}
                }
                std::slice::from_raw_parts(
                    source.as_ptr().add(entry.key_start as usize),
                    entry.key_len as usize,
                )
            };
            match candidate.cmp(key) {
                std::cmp::Ordering::Less => left = mid + 1,
                std::cmp::Ordering::Greater => right = mid,
                std::cmp::Ordering::Equal => return Ok(mid),
            }
        }
        Err(left)
    }

    pub(crate) fn to_owned(&self) -> Node {
        Node {
            keys: (0..self.len())
                .map(|index| self.key(index).expect("validated read node").to_vec())
                .collect(),
            vals: (0..self.len())
                .map(|index| self.value(index).expect("validated read node").to_vec())
                .collect(),
            child_counts: if self.leaf {
                Vec::new()
            } else {
                self.entries.iter().map(|entry| entry.child_count).collect()
            },
            leaf: self.leaf,
            level: self.level,
            format: self.format.clone(),
        }
    }

    pub(crate) fn retained_bytes(&self) -> usize {
        self.bytes
            .len()
            .saturating_add(self.prefix_keys.as_ref().map_or(0, |keys| keys.len()))
            .saturating_add(
                self.entries
                    .len()
                    .saturating_mul(mem::size_of::<ReadEntryMeta>()),
            )
    }

    fn validate(&self) -> Result<(), Error> {
        if !self.leaf && self.entries.is_empty() {
            return Err(Error::InvalidNode);
        }
        let mut previous: Option<&[u8]> = None;
        for index in 0..self.len() {
            let key = self.key(index).ok_or(Error::InvalidNode)?;
            self.value(index).ok_or(Error::InvalidNode)?;
            if previous.is_some_and(|previous| previous >= key) {
                return Err(Error::InvalidNode);
            }
            previous = Some(key);
        }
        Ok(())
    }
}

#[inline]
fn key_order_word(key: &[u8], prefix_len: usize) -> u64 {
    let suffix = key.get(prefix_len..).unwrap_or_default();
    let mut word = [0u8; 8];
    let copied = suffix.len().min(word.len());
    word[..copied].copy_from_slice(&suffix[..copied]);
    u64::from_be_bytes(word)
}

fn compact_u32(value: usize, field: &str) -> Result<u32, Error> {
    u32::try_from(value).map_err(|_| compact_error(format!("{field} exceeds u32")))
}

#[inline]
fn read_slice(bytes: &[u8], start: u32, len: u32) -> Option<&[u8]> {
    let start = start as usize;
    let end = start.checked_add(len as usize)?;
    bytes.get(start..end)
}

impl Default for Node {
    fn default() -> Self {
        Self {
            keys: Vec::new(),
            vals: Vec::new(),
            child_counts: Vec::new(),
            leaf: true,
            level: INIT_LEVEL,
            format: TreeFormat::default(),
        }
    }
}

impl Node {
    /// Create a new leaf node with default settings
    pub fn new_leaf() -> Self {
        Self::default()
    }

    /// Create a new internal node at the specified level
    pub fn new_internal(level: u8) -> Self {
        Self {
            leaf: false,
            level,
            ..Default::default()
        }
    }

    /// Create a builder for constructing a Node
    pub fn builder() -> NodeBuilder {
        NodeBuilder::default()
    }

    /// Get the number of keys in this node
    pub fn len(&self) -> usize {
        self.keys.len()
    }

    /// Check if this node is empty
    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    /// Binary search for key index
    /// Returns Ok(index) if found, Err(index) for insertion point
    pub fn search(&self, key: &[u8]) -> Result<usize, usize> {
        self.keys.binary_search_by(|k| k.as_slice().cmp(key))
    }

    /// Minimum size before a content boundary may be selected.
    pub fn min_chunk_size(&self) -> usize {
        usize::try_from(self.format.chunking.min).unwrap_or(usize::MAX)
    }

    /// Maximum soft chunk size.
    pub fn max_chunk_size(&self) -> usize {
        usize::try_from(self.format.chunking.max).unwrap_or(usize::MAX)
    }

    /// Compatibility accessor for threshold-based chunking.
    pub fn chunking_factor(&self) -> u32 {
        match self.format.chunking.rule {
            BoundaryRule::HashThreshold { factor } => factor,
            _ => u32::try_from(self.format.chunking.target).unwrap_or(u32::MAX),
        }
    }

    /// Boundary hash seed.
    pub fn hash_seed(&self) -> u64 {
        self.format.chunking.hash_seed
    }

    /// Value encoding.
    pub fn encoding(&self) -> &Encoding {
        &self.format.value_encoding
    }

    /// Validate structural invariants independent of storage.
    pub fn validate(&self) -> Result<(), Error> {
        self.format.validate()?;
        if self.keys.len() != self.vals.len() || self.keys.windows(2).any(|pair| pair[0] >= pair[1])
        {
            return Err(Error::InvalidNode);
        }
        if self.leaf {
            if !self.child_counts.is_empty() {
                return Err(Error::InvalidNode);
            }
        } else if self.child_counts.len() != self.keys.len() || self.child_counts.contains(&0) {
            return Err(Error::InvalidNode);
        }
        Ok(())
    }

    /// Serialize to compact, deterministic bytes.
    pub fn to_bytes(&self) -> Vec<u8> {
        self.try_to_bytes()
            .expect("node uses a registered, valid persisted format")
    }

    /// Serialize to deterministic bytes, returning format errors to the caller.
    pub fn try_to_bytes(&self) -> Result<Vec<u8>, Error> {
        self.to_compact_bytes()
    }

    /// Return the exact length of the serialized form.
    pub fn encoded_len(&self) -> usize {
        self.try_encoded_len()
            .expect("node uses a registered, valid persisted format")
    }

    /// Deserialize from current node bytes.
    pub fn from_bytes(data: &[u8]) -> Result<Self, Error> {
        Self::from_compact_bytes(data)
    }

    /// Deserialize and require an exact persisted tree format.
    pub fn from_bytes_with_format(data: &[u8], expected: &TreeFormat) -> Result<Self, Error> {
        let node = Self::from_bytes(data)?;
        if node.format != *expected {
            return Err(Error::FormatMismatch {
                expected: expected.digest()?,
                actual: node.format.digest()?,
            });
        }
        Ok(node)
    }

    /// Compute CID of this node
    pub fn cid(&self) -> Cid {
        Cid::from_bytes(&self.to_bytes())
    }

    fn to_compact_bytes(&self) -> Result<Vec<u8>, Error> {
        let format_bytes = if self.format == TreeFormat::default() {
            Vec::new()
        } else {
            self.format.canonical_bytes()?
        };
        // Encoding already visits every key and value. Use a bounded O(1)
        // capacity estimate here so serialization does not scan the node twice;
        // `encoded_len()` remains exact for callers that explicitly request it.
        let estimated_entries = self.keys.len().saturating_mul(48);
        let estimated_capacity = format_bytes
            .len()
            .saturating_add(COMPACT_MAGIC.len())
            .saturating_add(32)
            .saturating_add(estimated_entries);
        let mut out = Vec::with_capacity(estimated_capacity);
        out.extend_from_slice(COMPACT_MAGIC);
        write_varint(COMPACT_VERSION, &mut out);
        write_varint(format_bytes.len() as u64, &mut out);
        out.extend_from_slice(&format_bytes);
        write_varint(if self.leaf { 1 } else { 0 }, &mut out);
        write_varint(self.level as u64, &mut out);
        write_varint(self.keys.len() as u64, &mut out);

        match &self.format.node_layout {
            NodeLayoutSpec::PrefixCompressed => {
                let mut previous_key: &[u8] = &[];
                for (index, (key, val)) in self.keys.iter().zip(&self.vals).enumerate() {
                    let shared = common_prefix_len(previous_key, key);
                    let suffix = &key[shared..];
                    write_varint(shared as u64, &mut out);
                    write_varint(suffix.len() as u64, &mut out);
                    out.extend_from_slice(suffix);
                    write_varint(val.len() as u64, &mut out);
                    out.extend_from_slice(val);
                    if !self.leaf {
                        write_varint(*self.child_counts.get(index).unwrap_or(&0), &mut out);
                    }
                    previous_key = key;
                }
            }
            NodeLayoutSpec::Plain => {
                for (index, (key, val)) in self.keys.iter().zip(&self.vals).enumerate() {
                    write_varint(key.len() as u64, &mut out);
                    out.extend_from_slice(key);
                    write_varint(val.len() as u64, &mut out);
                    out.extend_from_slice(val);
                    if !self.leaf {
                        write_varint(*self.child_counts.get(index).unwrap_or(&0), &mut out);
                    }
                }
            }
            NodeLayoutSpec::OffsetTable => {
                let mut payload = Vec::new();
                for (index, (key, val)) in self.keys.iter().zip(&self.vals).enumerate() {
                    write_varint(payload.len() as u64, &mut out);
                    write_varint(key.len() as u64, &mut out);
                    payload.extend_from_slice(key);
                    write_varint(payload.len() as u64, &mut out);
                    write_varint(val.len() as u64, &mut out);
                    payload.extend_from_slice(val);
                    if !self.leaf {
                        write_varint(*self.child_counts.get(index).unwrap_or(&0), &mut out);
                    }
                }
                write_varint(payload.len() as u64, &mut out);
                out.extend_from_slice(&payload);
            }
            NodeLayoutSpec::Custom { id, .. } => {
                return Err(Error::InvalidFormat(format!(
                    "node layout '{id}' has no registered codec"
                )));
            }
        }

        Ok(out)
    }

    fn try_encoded_len(&self) -> Result<usize, Error> {
        let format_bytes = if self.format == TreeFormat::default() {
            Vec::new()
        } else {
            self.format.canonical_bytes()?
        };
        self.try_encoded_len_with_format(&format_bytes)
    }

    fn try_encoded_len_with_format(&self, format_bytes: &[u8]) -> Result<usize, Error> {
        let mut len = COMPACT_MAGIC.len()
            + varint_len(COMPACT_VERSION)
            + varint_len(format_bytes.len() as u64)
            + format_bytes.len()
            + varint_len(if self.leaf { 1 } else { 0 })
            + varint_len(self.level as u64)
            + varint_len(self.keys.len() as u64);

        match &self.format.node_layout {
            NodeLayoutSpec::PrefixCompressed => {
                let mut previous_key: &[u8] = &[];
                for (index, (key, val)) in self.keys.iter().zip(&self.vals).enumerate() {
                    let shared = common_prefix_len(previous_key, key);
                    let suffix_len = key.len() - shared;
                    len += varint_len(shared as u64)
                        + varint_len(suffix_len as u64)
                        + suffix_len
                        + varint_len(val.len() as u64)
                        + val.len();
                    if !self.leaf {
                        len += varint_len(*self.child_counts.get(index).unwrap_or(&0));
                    }
                    previous_key = key;
                }
            }
            NodeLayoutSpec::Plain => {
                for (index, (key, val)) in self.keys.iter().zip(&self.vals).enumerate() {
                    len += varint_len(key.len() as u64)
                        + key.len()
                        + varint_len(val.len() as u64)
                        + val.len();
                    if !self.leaf {
                        len += varint_len(*self.child_counts.get(index).unwrap_or(&0));
                    }
                }
            }
            NodeLayoutSpec::OffsetTable => {
                let mut payload_len = 0usize;
                for (index, (key, val)) in self.keys.iter().zip(&self.vals).enumerate() {
                    len += varint_len(payload_len as u64)
                        + varint_len(key.len() as u64)
                        + varint_len((payload_len + key.len()) as u64)
                        + varint_len(val.len() as u64);
                    if !self.leaf {
                        len += varint_len(*self.child_counts.get(index).unwrap_or(&0));
                    }
                    payload_len = payload_len
                        .saturating_add(key.len())
                        .saturating_add(val.len());
                }
                len += varint_len(payload_len as u64) + payload_len;
            }
            NodeLayoutSpec::Custom { id, .. } => {
                return Err(Error::InvalidFormat(format!(
                    "node layout '{id}' has no registered codec"
                )));
            }
        }
        Ok(len)
    }

    fn from_compact_bytes(data: &[u8]) -> Result<Self, Error> {
        let mut cursor = CompactCursor::new(data);
        cursor.expect_magic()?;
        let version = cursor.read_varint()?;
        if version != COMPACT_VERSION {
            return Err(compact_error(format!(
                "unsupported compact node version {version}"
            )));
        }
        let format_len = cursor.read_usize("tree format length")?;
        let format = if format_len == 0 {
            TreeFormat::default()
        } else {
            TreeFormat::from_canonical_bytes(cursor.read_bytes(format_len)?)?
        };
        let leaf = match cursor.read_varint()? {
            0 => false,
            1 => true,
            other => return Err(compact_error(format!("invalid leaf flag {other}"))),
        };
        let level = cursor.read_u8_varint("level")?;
        let entry_count = cursor.read_usize("entry_count")?;
        let mut keys = Vec::with_capacity(entry_count);
        let mut vals = Vec::with_capacity(entry_count);
        let mut child_counts = Vec::with_capacity(if leaf { 0 } else { entry_count });

        match &format.node_layout {
            NodeLayoutSpec::PrefixCompressed => {
                let mut previous_key = Vec::new();
                for _ in 0..entry_count {
                    let shared = cursor.read_usize("shared key prefix length")?;
                    if shared > previous_key.len() {
                        return Err(compact_error("shared key prefix exceeds previous key"));
                    }
                    let suffix_len = cursor.read_usize("key suffix length")?;
                    let mut key = Vec::with_capacity(shared.saturating_add(suffix_len));
                    key.extend_from_slice(&previous_key[..shared]);
                    key.extend_from_slice(cursor.read_bytes(suffix_len)?);
                    let value_len = cursor.read_usize("value length")?;
                    let val = cursor.read_bytes(value_len)?.to_vec();
                    if !leaf {
                        child_counts.push(cursor.read_varint()?);
                    }
                    previous_key.clear();
                    previous_key.extend_from_slice(&key);
                    keys.push(key);
                    vals.push(val);
                }
            }
            NodeLayoutSpec::Plain => {
                for _ in 0..entry_count {
                    let key_len = cursor.read_usize("key length")?;
                    keys.push(cursor.read_bytes(key_len)?.to_vec());
                    let value_len = cursor.read_usize("value length")?;
                    vals.push(cursor.read_bytes(value_len)?.to_vec());
                    if !leaf {
                        child_counts.push(cursor.read_varint()?);
                    }
                }
            }
            NodeLayoutSpec::OffsetTable => {
                let mut offsets = Vec::with_capacity(entry_count);
                for _ in 0..entry_count {
                    let key_offset = cursor.read_usize("key offset")?;
                    let key_len = cursor.read_usize("key length")?;
                    let value_offset = cursor.read_usize("value offset")?;
                    let value_len = cursor.read_usize("value length")?;
                    offsets.push((key_offset, key_len, value_offset, value_len));
                    if !leaf {
                        child_counts.push(cursor.read_varint()?);
                    }
                }
                let payload_len = cursor.read_usize("payload length")?;
                let payload = cursor.read_bytes(payload_len)?;
                for (key_offset, key_len, value_offset, value_len) in offsets {
                    keys.push(slice_payload(payload, key_offset, key_len, "key")?.to_vec());
                    vals.push(slice_payload(payload, value_offset, value_len, "value")?.to_vec());
                }
            }
            NodeLayoutSpec::Custom { id, .. } => {
                return Err(Error::InvalidFormat(format!(
                    "node layout '{id}' has no registered codec"
                )));
            }
        }

        if !cursor.is_done() {
            return Err(compact_error("trailing bytes in compact node"));
        }
        Ok(Self {
            keys,
            vals,
            child_counts,
            leaf,
            level,
            format,
        })
    }
}

fn slice_payload<'a>(
    payload: &'a [u8],
    offset: usize,
    len: usize,
    field: &str,
) -> Result<&'a [u8], Error> {
    let end = offset
        .checked_add(len)
        .ok_or_else(|| compact_error(format!("{field} offset overflow")))?;
    payload
        .get(offset..end)
        .ok_or_else(|| compact_error(format!("{field} offset outside payload")))
}

fn compact_error(message: impl Into<String>) -> Error {
    Error::Deserialize(format!("compact node: {}", message.into()))
}

fn write_varint(mut value: u64, out: &mut Vec<u8>) {
    while value >= 0x80 {
        out.push(((value as u8) & 0x7f) | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}

fn varint_len(mut value: u64) -> usize {
    let mut len = 1;
    while value >= 0x80 {
        value >>= 7;
        len += 1;
    }
    len
}

fn common_prefix_len(left: &[u8], right: &[u8]) -> usize {
    left.iter()
        .zip(right)
        .take_while(|(left, right)| left == right)
        .count()
}

struct CompactCursor<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> CompactCursor<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self { data, pos: 0 }
    }

    fn expect_magic(&mut self) -> Result<(), Error> {
        if self.data.len() < COMPACT_MAGIC.len()
            || &self.data[..COMPACT_MAGIC.len()] != COMPACT_MAGIC
        {
            return Err(compact_error("missing compact node magic"));
        }
        self.pos = COMPACT_MAGIC.len();
        Ok(())
    }

    fn read_u8_varint(&mut self, field: &str) -> Result<u8, Error> {
        let value = self.read_varint()?;
        u8::try_from(value).map_err(|_| compact_error(format!("{field} exceeds u8")))
    }

    fn read_usize(&mut self, field: &str) -> Result<usize, Error> {
        let value = self.read_varint()?;
        usize::try_from(value).map_err(|_| compact_error(format!("{field} exceeds usize")))
    }

    fn read_varint(&mut self) -> Result<u64, Error> {
        let mut value = 0u64;
        let mut shift = 0u32;

        for _ in 0..10 {
            let byte = self.read_byte()?;
            let part = u64::from(byte & 0x7f);
            if shift == 63 && part > 1 {
                return Err(compact_error("varint overflow"));
            }
            value |= part << shift;
            if byte & 0x80 == 0 {
                return Ok(value);
            }
            shift += 7;
        }

        Err(compact_error("varint overflow"))
    }

    fn read_byte(&mut self) -> Result<u8, Error> {
        let byte = *self
            .data
            .get(self.pos)
            .ok_or_else(|| compact_error("unexpected end of bytes"))?;
        self.pos += 1;
        Ok(byte)
    }

    fn read_bytes(&mut self, len: usize) -> Result<&'a [u8], Error> {
        let end = self
            .pos
            .checked_add(len)
            .ok_or_else(|| compact_error("byte range overflow"))?;
        let bytes = self
            .data
            .get(self.pos..end)
            .ok_or_else(|| compact_error("unexpected end of bytes"))?;
        self.pos = end;
        Ok(bytes)
    }

    fn is_done(&self) -> bool {
        self.pos == self.data.len()
    }

    #[inline]
    fn position(&self) -> usize {
        self.pos
    }
}

/// Builder pattern for Node construction
#[derive(Default)]
pub struct NodeBuilder {
    keys: Vec<Vec<u8>>,
    vals: Vec<Vec<u8>>,
    child_counts: Vec<u64>,
    leaf: bool,
    level: u8,
    format: TreeFormat,
}

impl NodeBuilder {
    /// Create a new NodeBuilder with default values
    pub fn new() -> Self {
        Self {
            leaf: true,
            level: INIT_LEVEL,
            format: TreeFormat::default(),
            keys: Vec::new(),
            vals: Vec::new(),
            child_counts: Vec::new(),
        }
    }

    /// Set the keys
    pub fn keys(mut self, keys: Vec<Vec<u8>>) -> Self {
        self.keys = keys;
        self
    }

    /// Set the values
    pub fn vals(mut self, vals: Vec<Vec<u8>>) -> Self {
        self.vals = vals;
        self
    }

    /// Set logical entry counts for internal children.
    pub fn child_counts(mut self, child_counts: Vec<u64>) -> Self {
        self.child_counts = child_counts;
        self
    }

    /// Set whether this is a leaf node
    pub fn leaf(mut self, leaf: bool) -> Self {
        self.leaf = leaf;
        self
    }

    /// Set the tree level
    pub fn level(mut self, level: u8) -> Self {
        self.level = level;
        self
    }

    /// Set the minimum chunk size
    pub fn min_chunk_size(mut self, size: usize) -> Self {
        self.format.chunking.min = size as u64;
        self.format.chunking.target = self.format.chunking.target.max(size as u64);
        self.format.chunking.max = self.format.chunking.max.max(size as u64);
        self
    }

    /// Set the maximum chunk size
    pub fn max_chunk_size(mut self, size: usize) -> Self {
        self.format.chunking.max = size as u64;
        self.format.chunking.target = self.format.chunking.target.min(size as u64);
        self.format.chunking.min = self.format.chunking.min.min(size as u64);
        self
    }

    /// Set the chunking factor
    pub fn chunking_factor(mut self, factor: u32) -> Self {
        self.format.chunking.rule = BoundaryRule::HashThreshold { factor };
        self
    }

    /// Set the hash seed
    pub fn hash_seed(mut self, seed: u64) -> Self {
        self.format.chunking.hash_seed = seed;
        self
    }

    /// Set the encoding type
    pub fn encoding(mut self, encoding: Encoding) -> Self {
        self.format.value_encoding = encoding;
        self
    }

    /// Set the complete persisted tree format.
    pub fn tree_format(mut self, format: TreeFormat) -> Self {
        self.format = format;
        self
    }

    /// Build the Node
    pub fn build(self) -> Node {
        Node {
            keys: self.keys,
            vals: self.vals,
            child_counts: self.child_counts,
            leaf: self.leaf,
            level: self.level,
            format: self.format,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn non_current_node_bytes_are_rejected() {
        assert!(Node::from_bytes(&serde_cbor::to_vec(&Node::default()).unwrap()).is_err());
    }

    #[test]
    fn test_new_leaf() {
        let node = Node::new_leaf();
        assert!(node.leaf);
        assert_eq!(node.level, INIT_LEVEL);
        assert!(node.is_empty());
    }

    #[test]
    fn test_new_internal() {
        let node = Node::new_internal(2);
        assert!(!node.leaf);
        assert_eq!(node.level, 2);
    }

    #[test]
    fn test_builder() {
        let node = Node::builder()
            .keys(vec![b"key1".to_vec(), b"key2".to_vec()])
            .vals(vec![b"val1".to_vec(), b"val2".to_vec()])
            .leaf(true)
            .level(0)
            .min_chunk_size(2)
            .max_chunk_size(100)
            .chunking_factor(64)
            .hash_seed(42)
            .encoding(Encoding::Cbor)
            .build();

        assert_eq!(node.len(), 2);
        assert!(node.leaf);
        assert_eq!(node.level, 0);
        assert_eq!(node.min_chunk_size(), 2);
        assert_eq!(node.max_chunk_size(), 100);
        assert_eq!(node.chunking_factor(), 64);
        assert_eq!(node.hash_seed(), 42);
        assert_eq!(node.encoding(), &Encoding::Cbor);
    }

    #[test]
    fn test_search() {
        let node = Node::builder()
            .keys(vec![b"a".to_vec(), b"c".to_vec(), b"e".to_vec()])
            .vals(vec![b"1".to_vec(), b"2".to_vec(), b"3".to_vec()])
            .build();

        assert_eq!(node.search(b"a"), Ok(0));
        assert_eq!(node.search(b"c"), Ok(1));
        assert_eq!(node.search(b"e"), Ok(2));
        assert_eq!(node.search(b"b"), Err(1));
        assert_eq!(node.search(b"d"), Err(2));
    }

    #[test]
    fn test_len_is_empty() {
        let empty = Node::new_leaf();
        assert!(empty.is_empty());
        assert_eq!(empty.len(), 0);

        let node = Node::builder()
            .keys(vec![b"key".to_vec()])
            .vals(vec![b"val".to_vec()])
            .build();
        assert!(!node.is_empty());
        assert_eq!(node.len(), 1);
    }

    #[test]
    fn compact_leaf_serialization_roundtrip() {
        let node = Node::builder()
            .keys(vec![b"key1".to_vec(), b"key2".to_vec()])
            .vals(vec![b"val1".to_vec(), b"val2".to_vec()])
            .leaf(true)
            .level(0)
            .build();

        let bytes = node.to_bytes();
        assert!(bytes.starts_with(COMPACT_MAGIC));
        let restored = Node::from_bytes(&bytes).unwrap();
        assert_eq!(node, restored);
    }

    #[test]
    fn packed_read_node_matches_owned_node_for_every_builtin_layout() {
        for layout in [
            NodeLayoutSpec::PrefixCompressed,
            NodeLayoutSpec::Plain,
            NodeLayoutSpec::OffsetTable,
        ] {
            let format = TreeFormat {
                node_layout: layout.clone(),
                ..TreeFormat::default()
            };
            let node = Node::builder()
                .keys(vec![
                    b"alpha/0001".to_vec(),
                    b"alpha/0002".to_vec(),
                    b"beta/0003".to_vec(),
                ])
                .vals(vec![b"one".to_vec(), b"two".to_vec(), b"three".to_vec()])
                .leaf(true)
                .tree_format(format)
                .build();
            let bytes = Arc::<[u8]>::from(node.to_bytes());
            let packed = ReadNode::from_shared(bytes)
                .unwrap_or_else(|error| panic!("packed {layout:?} failed: {error}"));

            assert_eq!(packed.to_owned(), node);
            assert_eq!(packed.search(b"alpha/0002"), Ok(1));
            assert_eq!(packed.search(b"alpha/0003"), Err(2));
            assert!(packed.retained_bytes() >= node.to_bytes().len());
        }
    }

    #[test]
    fn packed_search_accelerator_preserves_full_byte_order() {
        let keys = vec![
            b"shared-prefix-00000000".to_vec(),
            b"shared-prefix-00000000\0".to_vec(),
            b"shared-prefix-00000000x".to_vec(),
            b"shared-prefix-00000001".to_vec(),
            b"shared-prefix-99999999".to_vec(),
        ];
        let queries: &[&[u8]] = &[
            b"before-prefix",
            b"shared",
            b"shared-prefix-00000000",
            b"shared-prefix-00000000\0",
            b"shared-prefix-00000000a",
            b"shared-prefix-00000001",
            b"shared-prefix-50000000",
            b"z-after-prefix",
        ];

        for layout in [
            NodeLayoutSpec::PrefixCompressed,
            NodeLayoutSpec::Plain,
            NodeLayoutSpec::OffsetTable,
        ] {
            let node = Node::builder()
                .keys(keys.clone())
                .vals(vec![Vec::new(); keys.len()])
                .tree_format(TreeFormat {
                    node_layout: layout,
                    ..TreeFormat::default()
                })
                .build();
            let packed = ReadNode::from_shared(Arc::from(node.to_bytes())).unwrap();
            for query in queries {
                assert_eq!(packed.search(query), node.search(query), "query={query:?}");
            }
        }
    }

    #[test]
    fn packed_read_node_rejects_structural_corruption() {
        let node = Node::builder()
            .keys(vec![b"a".to_vec(), b"b".to_vec()])
            .vals(vec![b"1".to_vec(), b"2".to_vec()])
            .build();
        let mut trailing = node.to_bytes();
        trailing.push(0);
        assert!(ReadNode::from_shared(Arc::from(trailing)).is_err());

        let unsorted = Node::builder()
            .keys(vec![b"b".to_vec(), b"a".to_vec()])
            .vals(vec![b"1".to_vec(), b"2".to_vec()])
            .build();
        assert!(ReadNode::from_shared(Arc::from(unsorted.to_bytes())).is_err());

        let invalid_internal = Node::builder()
            .keys(Vec::new())
            .vals(Vec::new())
            .child_counts(Vec::new())
            .leaf(false)
            .level(1)
            .build();
        assert!(ReadNode::from_shared(Arc::from(invalid_internal.to_bytes())).is_err());
    }

    #[test]
    fn compact_default_format_uses_the_reserved_short_form() {
        let node = Node::builder()
            .keys(vec![b"key".to_vec()])
            .vals(vec![b"value".to_vec()])
            .leaf(true)
            .build();

        let bytes = node.to_bytes();

        assert_eq!(bytes[COMPACT_MAGIC.len() + 1], 0);
        assert_eq!(Node::from_bytes(&bytes).unwrap(), node);
    }

    #[test]
    fn compact_internal_serialization_roundtrip() {
        let mut cid_a = [0u8; 32];
        cid_a[0] = 1;
        let mut cid_b = [0u8; 32];
        cid_b[0] = 2;
        let node = Node::builder()
            .keys(vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()])
            .vals(vec![cid_a.to_vec(), cid_b.to_vec(), b"fallback".to_vec()])
            .child_counts(vec![3, 5, 8])
            .leaf(false)
            .level(1)
            .min_chunk_size(2)
            .max_chunk_size(128)
            .chunking_factor(64)
            .hash_seed(42)
            .encoding(Encoding::Raw)
            .build();

        let bytes = node.to_bytes();
        assert!(bytes.starts_with(COMPACT_MAGIC));
        let restored = Node::from_bytes(&bytes).unwrap();
        assert_eq!(node, restored);
    }

    #[test]
    fn compact_serialization_is_deterministic() {
        let node = Node::builder()
            .keys(vec![b"key1".to_vec(), b"key2".to_vec(), b"key3".to_vec()])
            .vals(vec![b"val1".to_vec(), b"val2".to_vec(), b"val3".to_vec()])
            .leaf(true)
            .level(0)
            .min_chunk_size(2)
            .max_chunk_size(128)
            .chunking_factor(64)
            .hash_seed(42)
            .encoding(Encoding::Raw)
            .build();

        let compact_bytes = node.to_bytes();

        assert_eq!(Node::from_bytes(&compact_bytes).unwrap(), node);
        assert_eq!(compact_bytes, node.to_bytes());
    }

    #[test]
    fn malformed_compact_serialization_returns_error() {
        assert!(Node::from_bytes(COMPACT_MAGIC).is_err());

        let mut bytes = Vec::new();
        bytes.extend_from_slice(COMPACT_MAGIC);
        bytes.push(99);
        assert!(Node::from_bytes(&bytes).is_err());
    }

    #[test]
    fn compact_serialization_prefix_compresses_path_like_keys() {
        let keys = (0..32)
            .map(|i| format!("crates/trail/src/db/storage/path/to/file_{i:04}.rs").into_bytes())
            .collect::<Vec<_>>();
        let vals = (0..32)
            .map(|i| format!("value-{i:04}").into_bytes())
            .collect::<Vec<_>>();
        let node = Node::builder()
            .keys(keys)
            .vals(vals)
            .leaf(true)
            .level(0)
            .min_chunk_size(16)
            .max_chunk_size(512)
            .chunking_factor(256)
            .hash_seed(42)
            .encoding(Encoding::Raw)
            .build();

        let legacy_packed_bytes = serde_cbor::ser::to_vec_packed(&node).unwrap();
        let compact_bytes = node.to_bytes();

        assert_eq!(Node::from_bytes(&compact_bytes).unwrap(), node);
        assert!(
            compact_bytes.len() < legacy_packed_bytes.len(),
            "compact={} legacy_packed={}",
            compact_bytes.len(),
            legacy_packed_bytes.len()
        );
    }

    #[test]
    fn compact_encoded_len_matches_serialized_leaf_len() {
        let node = Node::builder()
            .keys(vec![
                b"crates/prolly/src/a.rs".to_vec(),
                b"crates/prolly/src/b.rs".to_vec(),
                b"crates/prolly/src/c.rs".to_vec(),
            ])
            .vals(vec![
                b"value-a".to_vec(),
                b"value-b".to_vec(),
                b"value-c".to_vec(),
            ])
            .leaf(true)
            .level(0)
            .min_chunk_size(16)
            .max_chunk_size(512)
            .chunking_factor(256)
            .hash_seed(42)
            .encoding(Encoding::Raw)
            .build();

        assert_eq!(node.encoded_len(), node.to_bytes().len());
    }

    #[test]
    fn compact_encoded_len_matches_serialized_internal_len() {
        let mut cid_a = [0u8; 32];
        cid_a[0] = 1;
        let mut cid_b = [0u8; 32];
        cid_b[0] = 2;
        let node = Node::builder()
            .keys(vec![
                b"crates/prolly/src/a.rs".to_vec(),
                b"crates/prolly/src/b.rs".to_vec(),
                b"crates/prolly/src/c.rs".to_vec(),
            ])
            .vals(vec![
                cid_a.to_vec(),
                cid_b.to_vec(),
                b"legacy-child".to_vec(),
            ])
            .leaf(false)
            .level(2)
            .min_chunk_size(16)
            .max_chunk_size(512)
            .chunking_factor(256)
            .hash_seed(42)
            .encoding(Encoding::Raw)
            .build();

        assert_eq!(node.encoded_len(), node.to_bytes().len());
    }

    #[test]
    fn compact_encoded_len_matches_serialized_custom_encoding_len() {
        let node = Node::builder()
            .keys(vec![b"a".to_vec(), b"b".to_vec()])
            .vals(vec![b"1".to_vec(), b"2".to_vec()])
            .leaf(true)
            .level(0)
            .min_chunk_size(2)
            .max_chunk_size(128)
            .chunking_factor(64)
            .hash_seed(42)
            .encoding(Encoding::Custom(
                "application/x-trail-node-test".to_string(),
            ))
            .build();

        assert_eq!(node.encoded_len(), node.to_bytes().len());
    }

    #[test]
    fn test_cid_deterministic() {
        let node1 = Node::builder()
            .keys(vec![b"key".to_vec()])
            .vals(vec![b"val".to_vec()])
            .build();

        let node2 = Node::builder()
            .keys(vec![b"key".to_vec()])
            .vals(vec![b"val".to_vec()])
            .build();

        assert_eq!(node1.cid(), node2.cid());
    }
}
