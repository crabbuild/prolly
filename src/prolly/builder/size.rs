use crate::prolly::error::Error;
use crate::prolly::format::{NodeLayoutSpec, TreeFormat};

const COMPACT_MAGIC_LEN: u64 = 4;
const COMPACT_VERSION: u64 = 2;

/// Exact incremental size of one compact-encoded node.
///
/// This mirrors `Node::to_bytes` without serializing or rescanning prior
/// entries. It is the source of truth for hard byte-cap decisions.
#[derive(Clone, Debug)]
pub(crate) struct EncodedNodeSizer {
    format: TreeFormat,
    leaf: bool,
    level: u8,
    count: u64,
    size: u64,
    empty_size: u64,
    payload_len: u64,
    previous_key: Vec<u8>,
}

impl EncodedNodeSizer {
    pub(crate) fn new(format: TreeFormat, leaf: bool, level: u8) -> Result<Self, Error> {
        format.validate()?;
        if let NodeLayoutSpec::Custom { id, .. } = &format.node_layout {
            return Err(Error::InvalidFormat(format!(
                "node layout '{id}' has no registered codec"
            )));
        }

        let format_len = if format == TreeFormat::default() {
            0
        } else {
            u64::try_from(format.canonical_bytes()?.len()).map_err(|_| Error::InvalidNode)?
        };
        let mut empty_size = COMPACT_MAGIC_LEN
            .checked_add(varint_len(COMPACT_VERSION))
            .and_then(|size| size.checked_add(varint_len(format_len)))
            .and_then(|size| size.checked_add(format_len))
            .and_then(|size| size.checked_add(varint_len(u64::from(leaf))))
            .and_then(|size| size.checked_add(varint_len(u64::from(level))))
            .and_then(|size| size.checked_add(varint_len(0)))
            .ok_or(Error::InvalidNode)?;
        if matches!(format.node_layout, NodeLayoutSpec::OffsetTable) {
            empty_size = empty_size
                .checked_add(varint_len(0))
                .ok_or(Error::InvalidNode)?;
        }

        Ok(Self {
            format,
            leaf,
            level,
            count: 0,
            size: empty_size,
            empty_size,
            payload_len: 0,
            previous_key: Vec::new(),
        })
    }

    #[inline]
    pub(crate) fn size(&self) -> u64 {
        self.size
    }

    pub(crate) fn size_after(
        &self,
        key: &[u8],
        value: &[u8],
        child_count: Option<u64>,
    ) -> Result<u64, Error> {
        validate_child_count(self.leaf, child_count)?;
        let key_len = u64::try_from(key.len()).map_err(|_| Error::InvalidNode)?;
        let value_len = u64::try_from(value.len()).map_err(|_| Error::InvalidNode)?;
        let next_count = self.count.checked_add(1).ok_or(Error::InvalidNode)?;
        let count_width_delta = varint_len(next_count)
            .checked_sub(varint_len(self.count))
            .ok_or(Error::InvalidNode)?;

        let entry_delta = match &self.format.node_layout {
            NodeLayoutSpec::PrefixCompressed => {
                let shared = common_prefix_len(&self.previous_key, key);
                let shared = u64::try_from(shared).map_err(|_| Error::InvalidNode)?;
                let suffix_len = key_len.checked_sub(shared).ok_or(Error::InvalidNode)?;
                checked_sum(&[
                    varint_len(shared),
                    varint_len(suffix_len),
                    suffix_len,
                    varint_len(value_len),
                    value_len,
                    child_count.map(varint_len).unwrap_or(0),
                ])?
            }
            NodeLayoutSpec::Plain => checked_sum(&[
                varint_len(key_len),
                key_len,
                varint_len(value_len),
                value_len,
                child_count.map(varint_len).unwrap_or(0),
            ])?,
            NodeLayoutSpec::OffsetTable => {
                let value_offset = self
                    .payload_len
                    .checked_add(key_len)
                    .ok_or(Error::InvalidNode)?;
                let next_payload_len = value_offset
                    .checked_add(value_len)
                    .ok_or(Error::InvalidNode)?;
                let payload_width_delta = varint_len(next_payload_len)
                    .checked_sub(varint_len(self.payload_len))
                    .ok_or(Error::InvalidNode)?;
                checked_sum(&[
                    varint_len(self.payload_len),
                    varint_len(key_len),
                    varint_len(value_offset),
                    varint_len(value_len),
                    child_count.map(varint_len).unwrap_or(0),
                    payload_width_delta,
                    key_len,
                    value_len,
                ])?
            }
            NodeLayoutSpec::Custom { .. } => return Err(Error::InvalidNode),
        };

        self.size
            .checked_add(count_width_delta)
            .and_then(|size| size.checked_add(entry_delta))
            .ok_or(Error::InvalidNode)
    }

    #[allow(dead_code)]
    pub(crate) fn push(
        &mut self,
        key: &[u8],
        value: &[u8],
        child_count: Option<u64>,
    ) -> Result<(), Error> {
        let size = self.size_after(key, value, child_count)?;
        self.push_sized(key, value, child_count, size)
    }

    pub(crate) fn push_sized(
        &mut self,
        key: &[u8],
        value: &[u8],
        child_count: Option<u64>,
        size: u64,
    ) -> Result<(), Error> {
        debug_assert_eq!(self.size_after(key, value, child_count)?, size);
        self.size = size;
        self.count = self.count.checked_add(1).ok_or(Error::InvalidNode)?;
        let key_len = u64::try_from(key.len()).map_err(|_| Error::InvalidNode)?;
        let value_len = u64::try_from(value.len()).map_err(|_| Error::InvalidNode)?;
        self.payload_len = self
            .payload_len
            .checked_add(key_len)
            .and_then(|len| len.checked_add(value_len))
            .ok_or(Error::InvalidNode)?;
        if matches!(self.format.node_layout, NodeLayoutSpec::PrefixCompressed) {
            self.previous_key.clear();
            self.previous_key.extend_from_slice(key);
        }
        Ok(())
    }

    pub(crate) fn reset(&mut self) {
        self.count = 0;
        self.size = self.empty_size;
        self.payload_len = 0;
        self.previous_key.clear();
    }

    #[allow(dead_code)]
    pub(crate) fn level(&self) -> u8 {
        self.level
    }
}

fn validate_child_count(leaf: bool, child_count: Option<u64>) -> Result<(), Error> {
    match (leaf, child_count) {
        (true, None) | (false, Some(1..)) => Ok(()),
        _ => Err(Error::InvalidNode),
    }
}

fn checked_sum(values: &[u64]) -> Result<u64, Error> {
    values
        .iter()
        .try_fold(0_u64, |sum, value| sum.checked_add(*value))
        .ok_or(Error::InvalidNode)
}

fn common_prefix_len(left: &[u8], right: &[u8]) -> usize {
    left.iter()
        .zip(right)
        .take_while(|(left, right)| left == right)
        .count()
}

fn varint_len(mut value: u64) -> u64 {
    let mut len = 1;
    while value >= 0x80 {
        value >>= 7;
        len += 1;
    }
    len
}
