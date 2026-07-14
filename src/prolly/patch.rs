//! Portable logical patches and format-bound structural patch envelopes.

use serde::{Deserialize, Serialize};

use super::cid::Cid;
use super::error::{Diff, Error, Mutation};
use super::store::Store;
use super::{Prolly, Tree};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum LogicalPatch {
    Upsert {
        key: Vec<u8>,
        old: Option<Vec<u8>>,
        new: Vec<u8>,
    },
    Delete {
        key: Vec<u8>,
        old: Vec<u8>,
    },
}

impl LogicalPatch {
    pub fn key(&self) -> &[u8] {
        match self {
            Self::Upsert { key, .. } | Self::Delete { key, .. } => key,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum StructuralEdit {
    Point(LogicalPatch),
    Subtree {
        start_exclusive: Option<Vec<u8>>,
        end_inclusive: Vec<u8>,
        level: u16,
        cid: Option<Cid>,
        logical_count: u64,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StructuralPatch {
    pub base_root: Option<Cid>,
    pub format_digest: Cid,
    pub edits: Vec<StructuralEdit>,
}

impl StructuralPatch {
    pub fn validate(&self) -> Result<(), Error> {
        let mut previous_end: Option<&[u8]> = None;
        for edit in &self.edits {
            let (start, end) = match edit {
                StructuralEdit::Point(point) => (None, point.key()),
                StructuralEdit::Subtree {
                    start_exclusive,
                    end_inclusive,
                    level,
                    cid,
                    logical_count,
                } => {
                    if *level == 0
                        || *logical_count == 0
                        || cid.is_none()
                        || start_exclusive
                            .as_ref()
                            .map(|start| start >= end_inclusive)
                            .unwrap_or(false)
                    {
                        return Err(Error::InvalidStructuralPatch(
                            "invalid subtree replacement".to_string(),
                        ));
                    }
                    (start_exclusive.as_deref(), end_inclusive.as_slice())
                }
            };
            if previous_end
                .map(|previous| previous >= end)
                .unwrap_or(false)
                || start
                    .zip(previous_end)
                    .map(|(start, previous)| start < previous)
                    .unwrap_or(false)
            {
                return Err(Error::InvalidStructuralPatch(
                    "patch edits must be strictly ordered and non-overlapping".to_string(),
                ));
            }
            previous_end = Some(end);
        }
        Ok(())
    }
}

impl<S: Store> Prolly<S> {
    /// Produce a portable format-bound patch from the existing structural diff.
    pub fn diff_patch(&self, base: &Tree, target: &Tree) -> Result<StructuralPatch, Error> {
        let edits = self
            .diff(base, target)?
            .into_iter()
            .map(|diff| match diff {
                Diff::Added { key, val } => StructuralEdit::Point(LogicalPatch::Upsert {
                    key,
                    old: None,
                    new: val,
                }),
                Diff::Removed { key, val } => {
                    StructuralEdit::Point(LogicalPatch::Delete { key, old: val })
                }
                Diff::Changed { key, old, new } => StructuralEdit::Point(LogicalPatch::Upsert {
                    key,
                    old: Some(old),
                    new,
                }),
            })
            .collect();
        Ok(StructuralPatch {
            base_root: base.root.clone(),
            format_digest: base.config.format.digest()?,
            edits,
        })
    }

    /// Validate and apply a patch through the canonical writer.
    pub fn apply_patch(&self, base: &Tree, patch: &StructuralPatch) -> Result<Tree, Error> {
        patch.validate()?;
        if patch.base_root != base.root || patch.format_digest != base.config.format.digest()? {
            return Err(Error::PatchBaseMismatch);
        }
        let mut mutations = Vec::with_capacity(patch.edits.len());
        for edit in &patch.edits {
            let point = match edit {
                StructuralEdit::Point(point) => point,
                StructuralEdit::Subtree { .. } => {
                    return Err(Error::InvalidStructuralPatch(
                        "subtree replacement requires a verified imported subtree".to_string(),
                    ));
                }
            };
            match point {
                LogicalPatch::Upsert { key, old, new } => {
                    if self.get(base, key)? != *old {
                        return Err(Error::PatchBaseMismatch);
                    }
                    mutations.push(Mutation::Upsert {
                        key: key.clone(),
                        val: new.clone(),
                    });
                }
                LogicalPatch::Delete { key, old } => {
                    if self.get(base, key)?.as_ref() != Some(old) {
                        return Err(Error::PatchBaseMismatch);
                    }
                    mutations.push(Mutation::Delete { key: key.clone() });
                }
            }
        }
        self.batch(base, mutations)
    }
}
