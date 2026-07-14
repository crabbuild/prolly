//! Persisted tree-shape and node-layout descriptors.

use serde::{Deserialize, Serialize};

use super::cid::Cid;
use super::encoding::{
    Encoding, DEFAULT_CHUNKING_FACTOR, DEFAULT_HASH_SEED, DEFAULT_MAX_CHUNK_SIZE,
    DEFAULT_MIN_CHUNK_SIZE,
};
use super::error::Error;

const FORMAT_MAGIC: &[u8; 4] = b"CRFT";
const FORMAT_VERSION: u8 = 1;

/// Quantity used to enforce a chunk's soft size bounds.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChunkMeasure {
    /// Count ordered entries.
    EntryCount,
    /// Count uncompressed key and value bytes.
    LogicalBytes,
    /// Count bytes produced by the selected node layout.
    EncodedBytes,
}

/// Entry bytes supplied to the boundary hash.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum BoundaryInput {
    /// Hash only the ordered key or internal separator.
    Key,
    /// Hash both key and value bytes.
    KeyValue,
}

/// Stable hash algorithms available to persisted chunking policies.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum HashAlgorithm {
    /// xxHash64 with a deterministic seed.
    XxHash64,
}

/// Rule used to select a content-defined boundary.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum BoundaryRule {
    /// Split when the hash falls below the factor-derived threshold.
    HashThreshold { factor: u32 },
    /// Split according to a bounded Weibull distribution.
    Weibull { shape: u32 },
    /// Split from a rolling BuzHash window.
    RollingBuzHash { window: u16 },
}

/// Persisted content-defined chunking configuration.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChunkingSpec {
    pub measure: ChunkMeasure,
    pub input: BoundaryInput,
    pub hash: HashAlgorithm,
    pub rule: BoundaryRule,
    pub min: u64,
    pub target: u64,
    pub max: u64,
    pub hash_seed: u64,
    pub level_salt: bool,
    pub hard_max_node_bytes: u64,
}

impl Default for ChunkingSpec {
    fn default() -> Self {
        Self {
            measure: ChunkMeasure::EntryCount,
            input: BoundaryInput::Key,
            hash: HashAlgorithm::XxHash64,
            rule: BoundaryRule::HashThreshold {
                factor: DEFAULT_CHUNKING_FACTOR,
            },
            min: DEFAULT_MIN_CHUNK_SIZE as u64,
            target: DEFAULT_CHUNKING_FACTOR as u64,
            max: DEFAULT_MAX_CHUNK_SIZE as u64,
            hash_seed: DEFAULT_HASH_SEED,
            level_salt: true,
            hard_max_node_bytes: 16 * 1024 * 1024,
        }
    }
}

impl ChunkingSpec {
    /// Validate bounds and rule parameters before the policy is persisted or run.
    pub fn validate(&self) -> Result<(), Error> {
        if self.min == 0 || self.min > self.target || self.target > self.max {
            return Err(Error::InvalidFormat(
                "chunk bounds must satisfy 0 < min <= target <= max".to_string(),
            ));
        }
        if self.hard_max_node_bytes == 0 {
            return Err(Error::InvalidFormat(
                "hard maximum node bytes must be nonzero".to_string(),
            ));
        }
        match self.rule {
            BoundaryRule::HashThreshold { factor: 0 } => Err(Error::InvalidFormat(
                "hash threshold factor must be nonzero".to_string(),
            )),
            BoundaryRule::Weibull { shape: 0 } => Err(Error::InvalidFormat(
                "Weibull shape must be nonzero".to_string(),
            )),
            BoundaryRule::RollingBuzHash { window: 0 } => Err(Error::InvalidFormat(
                "rolling hash window must be nonzero".to_string(),
            )),
            _ => Ok(()),
        }
    }
}

/// Physical encoding used for nodes in a tree.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeLayoutSpec {
    /// Prefix-compress adjacent keys.
    #[default]
    PrefixCompressed,
    /// Encode complete keys and values directly.
    Plain,
    /// Store offsets into a validated shared payload.
    OffsetTable,
    /// Application-provided layout resolved by a stable identifier.
    Custom { id: String, parameters: Vec<u8> },
}

/// Persisted settings that determine tree shape and content IDs.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TreeFormat {
    pub chunking: ChunkingSpec,
    pub node_layout: NodeLayoutSpec,
    pub value_encoding: Encoding,
}

impl Default for TreeFormat {
    fn default() -> Self {
        Self {
            chunking: ChunkingSpec::default(),
            node_layout: NodeLayoutSpec::default(),
            value_encoding: Encoding::Raw,
        }
    }
}

impl TreeFormat {
    /// Validate persisted identifiers and structural bounds known at this layer.
    pub fn validate(&self) -> Result<(), Error> {
        self.chunking.validate()?;
        if let NodeLayoutSpec::Custom { id, .. } = &self.node_layout {
            if id.is_empty() {
                return Err(Error::InvalidFormat(
                    "custom node layout identifier cannot be empty".to_string(),
                ));
            }
        }
        Ok(())
    }

    /// Encode the descriptor without maps or platform-sized integers.
    pub fn canonical_bytes(&self) -> Result<Vec<u8>, Error> {
        self.validate()?;
        let mut out = Vec::new();
        out.extend_from_slice(FORMAT_MAGIC);
        out.push(FORMAT_VERSION);
        encode_chunking(&self.chunking, &mut out);
        encode_layout(&self.node_layout, &mut out);
        encode_value_encoding(&self.value_encoding, &mut out);
        Ok(out)
    }

    /// Hash the canonical persisted descriptor.
    pub fn digest(&self) -> Result<Cid, Error> {
        Ok(Cid::from_bytes(&self.canonical_bytes()?))
    }

    /// Decode a canonical persisted descriptor.
    pub fn from_canonical_bytes(bytes: &[u8]) -> Result<Self, Error> {
        let mut cursor = FormatCursor::new(bytes);
        cursor.expect(FORMAT_MAGIC)?;
        if cursor.read_u8()? != FORMAT_VERSION {
            return Err(Error::InvalidFormat(
                "unsupported tree format version".to_string(),
            ));
        }

        let measure = match cursor.read_u8()? {
            0 => ChunkMeasure::EntryCount,
            1 => ChunkMeasure::LogicalBytes,
            2 => ChunkMeasure::EncodedBytes,
            _ => return Err(Error::InvalidFormat("invalid chunk measure".to_string())),
        };
        let input = match cursor.read_u8()? {
            0 => BoundaryInput::Key,
            1 => BoundaryInput::KeyValue,
            _ => return Err(Error::InvalidFormat("invalid boundary input".to_string())),
        };
        let hash = match cursor.read_u8()? {
            0 => HashAlgorithm::XxHash64,
            _ => return Err(Error::InvalidFormat("invalid hash algorithm".to_string())),
        };
        let rule = match cursor.read_u8()? {
            0 => BoundaryRule::HashThreshold {
                factor: cursor.read_u32()?,
            },
            1 => BoundaryRule::Weibull {
                shape: cursor.read_u32()?,
            },
            2 => BoundaryRule::RollingBuzHash {
                window: cursor.read_u16()?,
            },
            _ => return Err(Error::InvalidFormat("invalid boundary rule".to_string())),
        };
        let chunking = ChunkingSpec {
            measure,
            input,
            hash,
            rule,
            min: cursor.read_u64()?,
            target: cursor.read_u64()?,
            max: cursor.read_u64()?,
            hash_seed: cursor.read_u64()?,
            level_salt: match cursor.read_u8()? {
                0 => false,
                1 => true,
                _ => return Err(Error::InvalidFormat("invalid level salt flag".to_string())),
            },
            hard_max_node_bytes: cursor.read_u64()?,
        };
        let node_layout = match cursor.read_u8()? {
            0 => NodeLayoutSpec::PrefixCompressed,
            1 => NodeLayoutSpec::Plain,
            2 => NodeLayoutSpec::OffsetTable,
            3 => NodeLayoutSpec::Custom {
                id: cursor.read_string()?,
                parameters: cursor.read_vec()?,
            },
            _ => return Err(Error::InvalidFormat("invalid node layout".to_string())),
        };
        let value_encoding = match cursor.read_u8()? {
            0 => Encoding::Raw,
            1 => Encoding::Cbor,
            2 => Encoding::Json,
            3 => Encoding::Custom(cursor.read_string()?),
            _ => return Err(Error::InvalidFormat("invalid value encoding".to_string())),
        };
        if !cursor.is_done() {
            return Err(Error::InvalidFormat(
                "trailing tree format bytes".to_string(),
            ));
        }
        let format = Self {
            chunking,
            node_layout,
            value_encoding,
        };
        format.validate()?;
        Ok(format)
    }
}

fn encode_chunking(spec: &ChunkingSpec, out: &mut Vec<u8>) {
    out.push(match spec.measure {
        ChunkMeasure::EntryCount => 0,
        ChunkMeasure::LogicalBytes => 1,
        ChunkMeasure::EncodedBytes => 2,
    });
    out.push(match spec.input {
        BoundaryInput::Key => 0,
        BoundaryInput::KeyValue => 1,
    });
    out.push(match spec.hash {
        HashAlgorithm::XxHash64 => 0,
    });
    match spec.rule {
        BoundaryRule::HashThreshold { factor } => {
            out.push(0);
            out.extend_from_slice(&factor.to_be_bytes());
        }
        BoundaryRule::Weibull { shape } => {
            out.push(1);
            out.extend_from_slice(&shape.to_be_bytes());
        }
        BoundaryRule::RollingBuzHash { window } => {
            out.push(2);
            out.extend_from_slice(&window.to_be_bytes());
        }
    }
    out.extend_from_slice(&spec.min.to_be_bytes());
    out.extend_from_slice(&spec.target.to_be_bytes());
    out.extend_from_slice(&spec.max.to_be_bytes());
    out.extend_from_slice(&spec.hash_seed.to_be_bytes());
    out.push(u8::from(spec.level_salt));
    out.extend_from_slice(&spec.hard_max_node_bytes.to_be_bytes());
}

fn encode_layout(layout: &NodeLayoutSpec, out: &mut Vec<u8>) {
    match layout {
        NodeLayoutSpec::PrefixCompressed => out.push(0),
        NodeLayoutSpec::Plain => out.push(1),
        NodeLayoutSpec::OffsetTable => out.push(2),
        NodeLayoutSpec::Custom { id, parameters } => {
            out.push(3);
            encode_bytes(id.as_bytes(), out);
            encode_bytes(parameters, out);
        }
    }
}

fn encode_value_encoding(encoding: &Encoding, out: &mut Vec<u8>) {
    match encoding {
        Encoding::Raw => out.push(0),
        Encoding::Cbor => out.push(1),
        Encoding::Json => out.push(2),
        Encoding::Custom(name) => {
            out.push(3);
            encode_bytes(name.as_bytes(), out);
        }
    }
}

fn encode_bytes(bytes: &[u8], out: &mut Vec<u8>) {
    out.extend_from_slice(&(bytes.len() as u64).to_be_bytes());
    out.extend_from_slice(bytes);
}

struct FormatCursor<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> FormatCursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }

    fn expect(&mut self, expected: &[u8]) -> Result<(), Error> {
        if self.read(expected.len())? != expected {
            return Err(Error::InvalidFormat(
                "invalid tree format magic".to_string(),
            ));
        }
        Ok(())
    }

    fn read(&mut self, len: usize) -> Result<&'a [u8], Error> {
        let end = self
            .position
            .checked_add(len)
            .ok_or_else(|| Error::InvalidFormat("tree format offset overflow".to_string()))?;
        let value = self
            .bytes
            .get(self.position..end)
            .ok_or_else(|| Error::InvalidFormat("truncated tree format".to_string()))?;
        self.position = end;
        Ok(value)
    }

    fn read_u8(&mut self) -> Result<u8, Error> {
        Ok(self.read(1)?[0])
    }

    fn read_u16(&mut self) -> Result<u16, Error> {
        Ok(u16::from_be_bytes(self.read(2)?.try_into().map_err(
            |_| Error::InvalidFormat("invalid u16".to_string()),
        )?))
    }

    fn read_u32(&mut self) -> Result<u32, Error> {
        Ok(u32::from_be_bytes(self.read(4)?.try_into().map_err(
            |_| Error::InvalidFormat("invalid u32".to_string()),
        )?))
    }

    fn read_u64(&mut self) -> Result<u64, Error> {
        Ok(u64::from_be_bytes(self.read(8)?.try_into().map_err(
            |_| Error::InvalidFormat("invalid u64".to_string()),
        )?))
    }

    fn read_vec(&mut self) -> Result<Vec<u8>, Error> {
        let len = usize::try_from(self.read_u64()?)
            .map_err(|_| Error::InvalidFormat("tree format length overflow".to_string()))?;
        Ok(self.read(len)?.to_vec())
    }

    fn read_string(&mut self) -> Result<String, Error> {
        String::from_utf8(self.read_vec()?)
            .map_err(|_| Error::InvalidFormat("tree format string is not UTF-8".to_string()))
    }

    fn is_done(&self) -> bool {
        self.position == self.bytes.len()
    }
}
