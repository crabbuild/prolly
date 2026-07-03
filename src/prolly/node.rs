//! Node structure and builder for Prolly Trees

use serde::{Deserialize, Serialize};

use super::cid::Cid;
use super::encoding::{
    Encoding, DEFAULT_CHUNKING_FACTOR, DEFAULT_HASH_SEED, DEFAULT_MAX_CHUNK_SIZE,
    DEFAULT_MIN_CHUNK_SIZE, INIT_LEVEL,
};
use super::error::Error;

const COMPACT_MAGIC: &[u8; 4] = b"CRAB";
const COMPACT_VERSION: u64 = 1;
const ENCODING_RAW: u8 = 0;
const ENCODING_CBOR: u8 = 1;
const ENCODING_JSON: u8 = 2;
const ENCODING_CUSTOM: u8 = 3;
const INTERNAL_VALUE_CID: u8 = 0;
const INTERNAL_VALUE_BYTES: u8 = 1;

/// A node in the Prolly Tree
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Node {
    /// Keys (sorted, lexicographic byte order)
    pub keys: Vec<Vec<u8>>,
    /// Values: raw bytes for leaves, CIDs for internal nodes
    pub vals: Vec<Vec<u8>>,
    /// Leaf node (true) or internal node (false)
    pub leaf: bool,
    /// Tree level (0 = leaf level)
    pub level: u8,
    /// Minimum entries before considering boundary
    pub min_chunk_size: usize,
    /// Maximum entries in a node
    pub max_chunk_size: usize,
    /// Chunking factor (higher = larger nodes)
    pub chunking_factor: u32,
    /// Hash seed for boundary detection
    pub hash_seed: u64,
    /// Value encoding type
    pub encoding: Encoding,
}

impl Default for Node {
    fn default() -> Self {
        Self {
            keys: Vec::new(),
            vals: Vec::new(),
            leaf: true,
            level: INIT_LEVEL,
            min_chunk_size: DEFAULT_MIN_CHUNK_SIZE,
            max_chunk_size: DEFAULT_MAX_CHUNK_SIZE,
            chunking_factor: DEFAULT_CHUNKING_FACTOR,
            hash_seed: DEFAULT_HASH_SEED,
            encoding: Encoding::Raw,
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

    /// Serialize to compact, deterministic bytes.
    pub fn to_bytes(&self) -> Vec<u8> {
        self.to_compact_bytes()
    }

    /// Return the exact length of the compact serialized form without allocating it.
    pub fn encoded_len(&self) -> usize {
        let mut len = COMPACT_MAGIC.len()
            + varint_len(COMPACT_VERSION)
            + varint_len(if self.leaf { 1 } else { 0 })
            + varint_len(self.level as u64)
            + varint_len(self.min_chunk_size as u64)
            + varint_len(self.max_chunk_size as u64)
            + varint_len(self.chunking_factor as u64)
            + varint_len(self.hash_seed)
            + encoding_len(&self.encoding)
            + varint_len(self.keys.len() as u64);

        let mut previous_key: &[u8] = &[];
        for (key, val) in self.keys.iter().zip(&self.vals) {
            let shared = common_prefix_len(previous_key, key);
            let suffix = &key[shared..];
            len += varint_len(shared as u64) + varint_len(suffix.len() as u64) + suffix.len();

            if self.leaf {
                len += varint_len(val.len() as u64) + val.len();
            } else if val.len() == 32 {
                len += 1 + val.len();
            } else {
                len += 1 + varint_len(val.len() as u64) + val.len();
            }

            previous_key = key;
        }

        len
    }

    /// Deserialize from compact bytes, falling back to legacy CBOR.
    pub fn from_bytes(data: &[u8]) -> Result<Self, Error> {
        if data.starts_with(COMPACT_MAGIC) {
            return Self::from_compact_bytes(data);
        }

        serde_cbor::from_slice(data).map_err(|e| Error::Deserialize(e.to_string()))
    }

    /// Compute CID of this node
    pub fn cid(&self) -> Cid {
        Cid::from_bytes(&self.to_bytes())
    }

    fn to_compact_bytes(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.encoded_len());
        out.extend_from_slice(COMPACT_MAGIC);
        write_varint(COMPACT_VERSION, &mut out);
        write_varint(if self.leaf { 1 } else { 0 }, &mut out);
        write_varint(self.level as u64, &mut out);
        write_varint(self.min_chunk_size as u64, &mut out);
        write_varint(self.max_chunk_size as u64, &mut out);
        write_varint(self.chunking_factor as u64, &mut out);
        write_varint(self.hash_seed, &mut out);
        write_encoding(&self.encoding, &mut out);
        write_varint(self.keys.len() as u64, &mut out);

        let mut previous_key: &[u8] = &[];
        for (key, val) in self.keys.iter().zip(&self.vals) {
            let shared = common_prefix_len(previous_key, key);
            let suffix = &key[shared..];
            write_varint(shared as u64, &mut out);
            write_varint(suffix.len() as u64, &mut out);
            out.extend_from_slice(suffix);

            if self.leaf {
                write_varint(val.len() as u64, &mut out);
                out.extend_from_slice(val);
            } else if val.len() == 32 {
                out.push(INTERNAL_VALUE_CID);
                out.extend_from_slice(val);
            } else {
                out.push(INTERNAL_VALUE_BYTES);
                write_varint(val.len() as u64, &mut out);
                out.extend_from_slice(val);
            }

            previous_key = key;
        }

        out
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

        let leaf = match cursor.read_varint()? {
            0 => false,
            1 => true,
            other => return Err(compact_error(format!("invalid leaf flag {other}"))),
        };
        let level = cursor.read_u8_varint("level")?;
        let min_chunk_size = cursor.read_usize("min_chunk_size")?;
        let max_chunk_size = cursor.read_usize("max_chunk_size")?;
        let chunking_factor = cursor.read_u32("chunking_factor")?;
        let hash_seed = cursor.read_varint()?;
        let encoding = cursor.read_encoding()?;
        let entry_count = cursor.read_usize("entry_count")?;

        let mut keys = Vec::with_capacity(entry_count);
        let mut vals = Vec::with_capacity(entry_count);
        let mut previous_key = Vec::new();

        for _ in 0..entry_count {
            let shared = cursor.read_usize("shared key prefix length")?;
            if shared > previous_key.len() {
                return Err(compact_error("shared key prefix exceeds previous key"));
            }
            let suffix_len = cursor.read_usize("key suffix length")?;
            let suffix = cursor.read_bytes(suffix_len)?.to_vec();
            let mut key = previous_key[..shared].to_vec();
            key.extend_from_slice(&suffix);

            let val = if leaf {
                let value_len = cursor.read_usize("value length")?;
                cursor.read_bytes(value_len)?.to_vec()
            } else {
                match cursor.read_byte()? {
                    INTERNAL_VALUE_CID => cursor.read_bytes(32)?.to_vec(),
                    INTERNAL_VALUE_BYTES => {
                        let value_len = cursor.read_usize("internal value length")?;
                        cursor.read_bytes(value_len)?.to_vec()
                    }
                    tag => return Err(compact_error(format!("invalid internal value tag {tag}"))),
                }
            };

            previous_key = key.clone();
            keys.push(key);
            vals.push(val);
        }

        if !cursor.is_done() {
            return Err(compact_error("trailing bytes in compact node"));
        }

        Ok(Self {
            keys,
            vals,
            leaf,
            level,
            min_chunk_size,
            max_chunk_size,
            chunking_factor,
            hash_seed,
            encoding,
        })
    }
}

fn compact_error(message: impl Into<String>) -> Error {
    Error::Deserialize(format!("compact node: {}", message.into()))
}

fn write_encoding(encoding: &Encoding, out: &mut Vec<u8>) {
    match encoding {
        Encoding::Raw => out.push(ENCODING_RAW),
        Encoding::Cbor => out.push(ENCODING_CBOR),
        Encoding::Json => out.push(ENCODING_JSON),
        Encoding::Custom(name) => {
            out.push(ENCODING_CUSTOM);
            write_varint(name.len() as u64, out);
            out.extend_from_slice(name.as_bytes());
        }
    }
}

fn encoding_len(encoding: &Encoding) -> usize {
    match encoding {
        Encoding::Raw | Encoding::Cbor | Encoding::Json => 1,
        Encoding::Custom(name) => 1 + varint_len(name.len() as u64) + name.len(),
    }
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
        len += 1;
        value >>= 7;
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

    fn read_encoding(&mut self) -> Result<Encoding, Error> {
        match self.read_byte()? {
            ENCODING_RAW => Ok(Encoding::Raw),
            ENCODING_CBOR => Ok(Encoding::Cbor),
            ENCODING_JSON => Ok(Encoding::Json),
            ENCODING_CUSTOM => {
                let len = self.read_usize("custom encoding length")?;
                let bytes = self.read_bytes(len)?;
                let name = String::from_utf8(bytes.to_vec())
                    .map_err(|e| compact_error(format!("custom encoding is not UTF-8: {e}")))?;
                Ok(Encoding::Custom(name))
            }
            tag => Err(compact_error(format!("invalid encoding tag {tag}"))),
        }
    }

    fn read_u8_varint(&mut self, field: &str) -> Result<u8, Error> {
        let value = self.read_varint()?;
        u8::try_from(value).map_err(|_| compact_error(format!("{field} exceeds u8")))
    }

    fn read_u32(&mut self, field: &str) -> Result<u32, Error> {
        let value = self.read_varint()?;
        u32::try_from(value).map_err(|_| compact_error(format!("{field} exceeds u32")))
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
}

/// Builder pattern for Node construction
#[derive(Default)]
pub struct NodeBuilder {
    keys: Vec<Vec<u8>>,
    vals: Vec<Vec<u8>>,
    leaf: bool,
    level: u8,
    min_chunk_size: usize,
    max_chunk_size: usize,
    chunking_factor: u32,
    hash_seed: u64,
    encoding: Encoding,
}

impl NodeBuilder {
    /// Create a new NodeBuilder with default values
    pub fn new() -> Self {
        Self {
            leaf: true,
            level: INIT_LEVEL,
            min_chunk_size: DEFAULT_MIN_CHUNK_SIZE,
            max_chunk_size: DEFAULT_MAX_CHUNK_SIZE,
            chunking_factor: DEFAULT_CHUNKING_FACTOR,
            hash_seed: DEFAULT_HASH_SEED,
            encoding: Encoding::Raw,
            keys: Vec::new(),
            vals: Vec::new(),
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
        self.min_chunk_size = size;
        self
    }

    /// Set the maximum chunk size
    pub fn max_chunk_size(mut self, size: usize) -> Self {
        self.max_chunk_size = size;
        self
    }

    /// Set the chunking factor
    pub fn chunking_factor(mut self, factor: u32) -> Self {
        self.chunking_factor = factor;
        self
    }

    /// Set the hash seed
    pub fn hash_seed(mut self, seed: u64) -> Self {
        self.hash_seed = seed;
        self
    }

    /// Set the encoding type
    pub fn encoding(mut self, encoding: Encoding) -> Self {
        self.encoding = encoding;
        self
    }

    /// Build the Node
    pub fn build(self) -> Node {
        Node {
            keys: self.keys,
            vals: self.vals,
            leaf: self.leaf,
            level: self.level,
            min_chunk_size: self.min_chunk_size,
            max_chunk_size: self.max_chunk_size,
            chunking_factor: self.chunking_factor,
            hash_seed: self.hash_seed,
            encoding: self.encoding,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const COMPACT_LEAF_FIXTURE_HEX: &str = concat!(
        "4352414201010010800480022a000300166372617465732f70726f6c6c792f7372632f612e7273",
        "0776616c75652d611204622e72730776616c75652d621204632e72730776616c75652d63"
    );
    const COMPACT_INTERNAL_FIXTURE_HEX: &str = concat!(
        "4352414201000210800480022a000300166372617465732f70726f6c6c792f7372632f612e7273",
        "0001000000000000000000000000000000000000000000000000000000000000001204622e7273",
        "0002000000000000000000000000000000000000000000000000000000000000001204632e7273",
        "010c6c65676163792d6368696c64"
    );
    const COMPACT_CUSTOM_ENCODING_FIXTURE_HEX: &str = concat!(
        "43524142010100028001402a031e6170706c69636174696f6e2f782d6372616264622d6e6f6465",
        "2d746573740200016101310001620132"
    );
    const LEGACY_CBOR_LEAF_FIXTURE_HEX: &str = concat!(
        "a9008396186318721861187418651873182f18701872186f186c186c1879182f187318721863182f",
        "1861182e1872187396186318721861187418651873182f18701872186f186c186c1879182f1873",
        "18721863182f1862182e1872187396186318721861187418651873182f18701872186f186c186c",
        "1879182f187318721863182f1863182e1872187301838718761861186c18751865182d1861",
        "8718761861186c18751865182d18628718761861186c18751865182d186302f503000410051",
        "902000619010007182a0800"
    );

    fn fixture_hex(bytes: &[u8]) -> String {
        bytes.iter().map(|byte| format!("{byte:02x}")).collect()
    }

    fn bytes_from_hex(hex: &str) -> Vec<u8> {
        assert_eq!(hex.len() % 2, 0, "hex fixture must have an even length");
        hex.as_bytes()
            .chunks_exact(2)
            .map(|pair| {
                let digits = std::str::from_utf8(pair).expect("fixture hex is ASCII");
                u8::from_str_radix(digits, 16).expect("fixture hex is valid")
            })
            .collect()
    }

    fn leaf_fixture_node() -> Node {
        Node::builder()
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
            .build()
    }

    fn internal_fixture_node() -> Node {
        let mut cid_a = [0u8; 32];
        cid_a[0] = 1;
        let mut cid_b = [0u8; 32];
        cid_b[0] = 2;
        Node::builder()
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
            .build()
    }

    fn custom_encoding_fixture_node() -> Node {
        Node::builder()
            .keys(vec![b"a".to_vec(), b"b".to_vec()])
            .vals(vec![b"1".to_vec(), b"2".to_vec()])
            .leaf(true)
            .level(0)
            .min_chunk_size(2)
            .max_chunk_size(128)
            .chunking_factor(64)
            .hash_seed(42)
            .encoding(Encoding::Custom(
                "application/x-crabdb-node-test".to_string(),
            ))
            .build()
    }

    #[test]
    fn compact_leaf_serialization_matches_fixture() {
        let node = leaf_fixture_node();
        let bytes = node.to_bytes();

        assert_eq!(fixture_hex(&bytes), COMPACT_LEAF_FIXTURE_HEX);
        assert_eq!(bytes.len(), node.encoded_len());
        assert_eq!(Node::from_bytes(&bytes).unwrap(), node);
    }

    #[test]
    fn compact_internal_serialization_matches_fixture() {
        let node = internal_fixture_node();
        let bytes = node.to_bytes();

        assert_eq!(fixture_hex(&bytes), COMPACT_INTERNAL_FIXTURE_HEX);
        assert_eq!(bytes.len(), node.encoded_len());
        assert_eq!(Node::from_bytes(&bytes).unwrap(), node);
    }

    #[test]
    fn compact_custom_encoding_serialization_matches_fixture() {
        let node = custom_encoding_fixture_node();
        let bytes = node.to_bytes();

        assert_eq!(fixture_hex(&bytes), COMPACT_CUSTOM_ENCODING_FIXTURE_HEX);
        assert_eq!(bytes.len(), node.encoded_len());
        assert_eq!(Node::from_bytes(&bytes).unwrap(), node);
    }

    #[test]
    fn legacy_cbor_leaf_fixture_remains_readable() {
        let node = leaf_fixture_node();
        let legacy_bytes = bytes_from_hex(LEGACY_CBOR_LEAF_FIXTURE_HEX);

        assert!(!legacy_bytes.starts_with(COMPACT_MAGIC));
        assert_eq!(Node::from_bytes(&legacy_bytes).unwrap(), node);
        assert_eq!(
            fixture_hex(&serde_cbor::ser::to_vec_packed(&node).unwrap()),
            LEGACY_CBOR_LEAF_FIXTURE_HEX
        );
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
        assert_eq!(node.min_chunk_size, 2);
        assert_eq!(node.max_chunk_size, 100);
        assert_eq!(node.chunking_factor, 64);
        assert_eq!(node.hash_seed, 42);
        assert_eq!(node.encoding, Encoding::Cbor);
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
    fn compact_internal_serialization_roundtrip() {
        let mut cid_a = [0u8; 32];
        cid_a[0] = 1;
        let mut cid_b = [0u8; 32];
        cid_b[0] = 2;
        let node = Node::builder()
            .keys(vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()])
            .vals(vec![cid_a.to_vec(), cid_b.to_vec(), b"fallback".to_vec()])
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
    fn compact_serialization_reads_legacy_cbor_and_reduces_size() {
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

        let legacy_bytes = serde_cbor::to_vec(&node).unwrap();
        let legacy_packed_bytes = serde_cbor::ser::to_vec_packed(&node).unwrap();
        let compact_bytes = node.to_bytes();

        assert_eq!(Node::from_bytes(&legacy_bytes).unwrap(), node);
        assert_eq!(Node::from_bytes(&legacy_packed_bytes).unwrap(), node);
        assert_eq!(Node::from_bytes(&compact_bytes).unwrap(), node);
        assert!(compact_bytes.len() < legacy_bytes.len());
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
            .map(|i| format!("crates/crabdb/src/db/storage/path/to/file_{i:04}.rs").into_bytes())
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
                "application/x-crabdb-node-test".to_string(),
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
