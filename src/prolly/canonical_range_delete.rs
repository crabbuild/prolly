//! Canonical half-open range deletion.

use super::builder::SortedBatchBuilder;
use super::canonical::CanonicalWriteStats;
use super::error::Error;
use super::store::Store;
use super::{Prolly, Tree};

pub(crate) fn apply<S: Store>(
    manager: &Prolly<S>,
    tree: &Tree,
    start: &[u8],
    end: &[u8],
) -> Result<(Tree, CanonicalWriteStats), Error> {
    if start >= end || tree.root.is_none() {
        return Ok((tree.clone(), CanonicalWriteStats::default()));
    }
    if let Some(root) = &tree.root {
        let node = manager.load_arc(root)?;
        if node.format != tree.config.format {
            return Err(Error::FormatMismatch {
                expected: tree.config.format.digest()?,
                actual: node.format.digest()?,
            });
        }
    }
    if manager
        .range(tree, start, Some(end))?
        .next()
        .transpose()?
        .is_none()
    {
        return Ok((tree.clone(), CanonicalWriteStats::default()));
    }

    let mut saw_deleted = false;
    let mut builder = SortedBatchBuilder::new(manager.store(), tree.config.clone());
    for entry in manager.range(tree, &[], None)? {
        let (key, value) = entry?;
        if key.as_slice() >= start && key.as_slice() < end {
            saw_deleted = true;
        } else {
            builder.add(key, value)?;
        }
    }
    debug_assert!(saw_deleted, "the existence probe found a key in the range");
    let written = builder.build()?;
    Ok((written, CanonicalWriteStats::default()))
}

pub(crate) fn apply_tree<S: Store>(
    manager: &Prolly<S>,
    tree: &Tree,
    start: &[u8],
    end: &[u8],
) -> Result<Tree, Error> {
    Ok(apply(manager, tree, start, end)?.0)
}
