use std::sync::Arc;

use crate::prolly::cid::Cid;
use crate::prolly::error::Error;
use crate::prolly::format::TreeFormat;
use crate::prolly::node::{Node, ReadNode};

/// Reject bytes that are not the content addressed by `expected`.
pub(crate) fn validate_cid(expected: &Cid, bytes: &[u8]) -> Result<(), Error> {
    let actual = Cid::from_bytes(bytes);
    if actual == *expected {
        Ok(())
    } else {
        Err(Error::CidMismatch {
            expected: expected.clone(),
            actual,
        })
    }
}

/// Validate identity, persisted format, and structure before cache admission.
pub(crate) fn decode_owned(
    expected_cid: &Cid,
    expected_format: &TreeFormat,
    bytes: &[u8],
) -> Result<Node, Error> {
    validate_cid(expected_cid, bytes)?;
    // The compact decoder validates the persisted format and structural
    // invariants while materializing entries, avoiding a second node scan.
    Node::from_bytes_with_format(bytes, expected_format)
}

/// Validate identity, persisted format, and structure while retaining shared bytes.
pub(crate) fn decode_read(
    expected_cid: &Cid,
    expected_format: &TreeFormat,
    bytes: Arc<[u8]>,
) -> Result<ReadNode, Error> {
    validate_cid(expected_cid, &bytes)?;
    let node = ReadNode::from_shared(bytes).map_err(|error| match error {
        Error::Deserialize(_) => Error::InvalidNode,
        other => other,
    })?;
    if node.format() != expected_format {
        return Err(Error::FormatMismatch {
            expected: expected_format.digest()?,
            actual: node.format().digest()?,
        });
    }
    Ok(node)
}
