use prolly::Cid;

use crate::{Error, Result};

const MAGIC: &[u8; 4] = b"DDBF";
const RECORD_VERSION: u8 = 1;
const DIGEST_BYTES: usize = 32;
const ENCODED_BYTES: usize = 4 + 1 + 4 + 2 + 2 + (5 * DIGEST_BYTES) + 1 + 8 + 4 + 4;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StoragePublicationMode {
    AtomicNodesAndRoots,
    PrepublishImmutableNodes,
}

impl StoragePublicationMode {
    fn tag(self) -> u8 {
        match self {
            Self::AtomicNodesAndRoots => 0,
            Self::PrepublishImmutableNodes => 1,
        }
    }

    fn from_tag(tag: u8) -> Result<Self> {
        match tag {
            0 => Ok(Self::AtomicNodesAndRoots),
            1 => Ok(Self::PrepublishImmutableNodes),
            _ => Err(Error::CorruptData(format!(
                "unknown database publication mode {tag}"
            ))),
        }
    }
}

/// Fixed-width durable namespace format negotiated before logical operations.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DatabaseFormatRecord {
    pub format_version: u32,
    pub logical_protocol_major: u16,
    pub logical_protocol_minor: u16,
    pub item_codec_digest: Cid,
    pub key_codec_digest: Cid,
    pub catalog_codec_digest: Cid,
    pub commit_codec_digest: Cid,
    pub tree_format_digest: Cid,
    pub publication_mode: StoragePublicationMode,
    pub large_value_inline_threshold: u64,
    pub minimum_reader_version: u32,
    pub minimum_writer_version: u32,
}

impl DatabaseFormatRecord {
    pub(crate) fn current(
        tree_format_digest: Cid,
        publication_mode: StoragePublicationMode,
        large_value_inline_threshold: usize,
    ) -> Self {
        Self {
            format_version: crate::DATABASE_FORMAT_VERSION,
            logical_protocol_major: 1,
            logical_protocol_minor: 0,
            item_codec_digest: Cid::from_bytes(b"DDBI-v1-canonical-cbor"),
            key_codec_digest: Cid::from_bytes(b"DDBK-v1-ordered-components"),
            catalog_codec_digest: Cid::from_bytes(
            b"DDBC-v10-schema-record-v1-detached-snapshot-manifest-tree-v1-snapshot-locator-v2-current-only-snapshot-catalog-v1-indexed-snapshot-manifest-v1-current-only-commit-catalog-v1-table-log-v1-append-only-blob-registry-v1-canonical-cbor",
            ),
            commit_codec_digest: Cid::from_bytes(
                b"DDBAudit-v7-commit-maintenance-import-fence-gc-index-reconfiguration-worker-lease-fence-checkpoint-canonical-cbor",
            ),
            tree_format_digest,
            publication_mode,
            large_value_inline_threshold: u64::try_from(large_value_inline_threshold)
                .unwrap_or(u64::MAX),
            minimum_reader_version: crate::DATABASE_FORMAT_VERSION,
            minimum_writer_version: crate::DATABASE_FORMAT_VERSION,
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut output = Vec::with_capacity(ENCODED_BYTES);
        output.extend_from_slice(MAGIC);
        output.push(RECORD_VERSION);
        output.extend_from_slice(&self.format_version.to_be_bytes());
        output.extend_from_slice(&self.logical_protocol_major.to_be_bytes());
        output.extend_from_slice(&self.logical_protocol_minor.to_be_bytes());
        for digest in [
            &self.item_codec_digest,
            &self.key_codec_digest,
            &self.catalog_codec_digest,
            &self.commit_codec_digest,
            &self.tree_format_digest,
        ] {
            output.extend_from_slice(digest.as_bytes());
        }
        output.push(self.publication_mode.tag());
        output.extend_from_slice(&self.large_value_inline_threshold.to_be_bytes());
        output.extend_from_slice(&self.minimum_reader_version.to_be_bytes());
        output.extend_from_slice(&self.minimum_writer_version.to_be_bytes());
        output
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() != ENCODED_BYTES || &bytes[..4] != MAGIC || bytes[4] != RECORD_VERSION {
            return Err(Error::CorruptData(
                "malformed or unsupported DDBF format record".into(),
            ));
        }
        let mut offset = 5;
        let format_version = read_u32(bytes, &mut offset)?;
        let logical_protocol_major = read_u16(bytes, &mut offset)?;
        let logical_protocol_minor = read_u16(bytes, &mut offset)?;
        let item_codec_digest = read_cid(bytes, &mut offset)?;
        let key_codec_digest = read_cid(bytes, &mut offset)?;
        let catalog_codec_digest = read_cid(bytes, &mut offset)?;
        let commit_codec_digest = read_cid(bytes, &mut offset)?;
        let tree_format_digest = read_cid(bytes, &mut offset)?;
        let publication_mode =
            StoragePublicationMode::from_tag(read_array::<1>(bytes, &mut offset)?[0])?;
        let large_value_inline_threshold = read_u64(bytes, &mut offset)?;
        let minimum_reader_version = read_u32(bytes, &mut offset)?;
        let minimum_writer_version = read_u32(bytes, &mut offset)?;
        debug_assert_eq!(offset, ENCODED_BYTES);
        Ok(Self {
            format_version,
            logical_protocol_major,
            logical_protocol_minor,
            item_codec_digest,
            key_codec_digest,
            catalog_codec_digest,
            commit_codec_digest,
            tree_format_digest,
            publication_mode,
            large_value_inline_threshold,
            minimum_reader_version,
            minimum_writer_version,
        })
    }
}

fn read_array<const N: usize>(bytes: &[u8], offset: &mut usize) -> Result<[u8; N]> {
    let end = offset
        .checked_add(N)
        .ok_or_else(|| Error::CorruptData("DDBF format record offset overflow".into()))?;
    let value = bytes
        .get(*offset..end)
        .ok_or_else(|| Error::CorruptData("truncated DDBF format record".into()))?
        .try_into()
        .map_err(|_| Error::CorruptData("malformed DDBF format record field".into()))?;
    *offset = end;
    Ok(value)
}

fn read_u16(bytes: &[u8], offset: &mut usize) -> Result<u16> {
    Ok(u16::from_be_bytes(read_array(bytes, offset)?))
}

fn read_u32(bytes: &[u8], offset: &mut usize) -> Result<u32> {
    Ok(u32::from_be_bytes(read_array(bytes, offset)?))
}

fn read_u64(bytes: &[u8], offset: &mut usize) -> Result<u64> {
    Ok(u64::from_be_bytes(read_array(bytes, offset)?))
}

fn read_cid(bytes: &[u8], offset: &mut usize) -> Result<Cid> {
    Ok(Cid(read_array::<DIGEST_BYTES>(bytes, offset)?))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_round_trips_and_rejects_trailing_bytes() {
        let record = DatabaseFormatRecord::current(
            Cid::from_bytes(b"tree"),
            StoragePublicationMode::PrepublishImmutableNodes,
            65_536,
        );
        let encoded = record.encode();
        assert_eq!(
            hex(&encoded),
            concat!(
                "44444246010000000c00010000",
                "b7634b782bbeede79bb1e3bc4cf9c09d87bc25e4c5d0e4b0cf4b22c8b6497b08",
                "29baf85dbdc57671178597c88d234550b1dcfd38d9b7a385c887b9fc0e62ba54",
                "49c889882eeee206599d6a755fb7223def4412eaa4d43e035b51ca7547ef6a19",
                "8fd2be6d05910d62ce8b3cbbe53742ab8ab8171ab6ad9e18c0eb9b4ce45e6dba",
                "dc9c5edb8b2d479e697b4b0b8ab874f32b325138598ce9e7b759eb8292110622",
                "0100000000000100000000000c0000000c",
            )
        );
        assert_eq!(DatabaseFormatRecord::decode(&encoded).unwrap(), record);
        assert!(DatabaseFormatRecord::decode(&[encoded, vec![0]].concat()).is_err());
    }

    #[test]
    fn decode_rejects_every_record_envelope_violation() {
        let record = DatabaseFormatRecord::current(
            Cid::from_bytes(b"tree"),
            StoragePublicationMode::PrepublishImmutableNodes,
            65_536,
        );
        let encoded = record.encode();

        assert!(DatabaseFormatRecord::decode(&encoded[..encoded.len() - 1]).is_err());

        let mut wrong_magic = encoded.clone();
        wrong_magic[0] ^= 0xff;
        assert!(DatabaseFormatRecord::decode(&wrong_magic).is_err());

        let mut wrong_record_version = encoded.clone();
        wrong_record_version[4] = 2;
        assert!(DatabaseFormatRecord::decode(&wrong_record_version).is_err());

        let mut unknown_publication_mode = encoded;
        let publication_mode_offset = 4 + 1 + 4 + 2 + 2 + (5 * DIGEST_BYTES);
        unknown_publication_mode[publication_mode_offset] = 0xff;
        assert!(DatabaseFormatRecord::decode(&unknown_publication_mode).is_err());
    }

    fn hex(bytes: &[u8]) -> String {
        use std::fmt::Write;
        let mut output = String::with_capacity(bytes.len() * 2);
        for byte in bytes {
            write!(&mut output, "{byte:02x}").unwrap();
        }
        output
    }
}
