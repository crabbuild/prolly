//! Portable logical patches and format-bound structural patch envelopes.

use serde::{Deserialize, Serialize};

use super::cid::Cid;
use super::error::{Error, Mutation};
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
        if let [StructuralEdit::Subtree {
            start_exclusive: None,
            end_inclusive,
            level,
            cid,
            logical_count,
        }] = self.edits.as_slice()
        {
            if end_inclusive.is_empty() {
                let valid_root = match cid {
                    Some(_) => *logical_count == 0 && *level == 0,
                    None => *logical_count == 0 && *level == 0,
                };
                if !valid_root {
                    return Err(Error::InvalidStructuralPatch(
                        "invalid root subtree replacement".to_string(),
                    ));
                }
                return Ok(());
            }
        }

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
    /// Produce a format-bound patch that reuses the target's content-addressed
    /// root subtree.
    ///
    /// The referenced target subtree must already exist in the destination
    /// store before application. Snapshot synchronization can transfer missing
    /// nodes independently, while same-store version operations only need this
    /// compact root envelope.
    pub fn diff_patch(&self, base: &Tree, target: &Tree) -> Result<StructuralPatch, Error> {
        if base.config.format != target.config.format {
            return Err(Error::PatchBaseMismatch);
        }
        let edits = if base.root == target.root {
            Vec::new()
        } else if let Some(cid) = &target.root {
            vec![StructuralEdit::Subtree {
                start_exclusive: None,
                end_inclusive: Vec::new(),
                level: 0,
                cid: Some(cid.clone()),
                logical_count: 0,
            }]
        } else {
            vec![StructuralEdit::Subtree {
                start_exclusive: None,
                end_inclusive: Vec::new(),
                level: 0,
                cid: None,
                logical_count: 0,
            }]
        };
        Ok(StructuralPatch {
            base_root: base.root.clone(),
            format_digest: self.format_digest()?,
            edits,
        })
    }

    /// Validate and apply a patch through the canonical writer.
    pub fn apply_patch(&self, base: &Tree, patch: &StructuralPatch) -> Result<Tree, Error> {
        patch.validate()?;
        if patch.base_root != base.root || patch.format_digest != self.format_digest()? {
            return Err(Error::PatchBaseMismatch);
        }

        if patch.edits.is_empty() {
            return Ok(base.clone());
        }
        if let [StructuralEdit::Subtree {
            start_exclusive: None,
            end_inclusive,
            level: _,
            cid,
            logical_count,
        }] = patch.edits.as_slice()
        {
            if end_inclusive.is_empty() {
                let target = Tree {
                    root: cid.clone(),
                    config: base.config.clone(),
                };
                match cid {
                    Some(cid) => {
                        let _ = self.load_arc(cid)?;
                    }
                    None if *logical_count == 0 => {}
                    None => {
                        return Err(Error::InvalidStructuralPatch(
                            "empty root subtree metadata is inconsistent".to_string(),
                        ));
                    }
                }
                return Ok(target);
            }
        }

        let keys = patch
            .edits
            .iter()
            .filter_map(|edit| match edit {
                StructuralEdit::Point(point) => Some(point.key()),
                StructuralEdit::Subtree { .. } => None,
            })
            .collect::<Vec<_>>();
        if keys.len() != patch.edits.len() {
            return Err(Error::InvalidStructuralPatch(
                "partial subtree replacement requires an imported subtree plan".to_string(),
            ));
        }
        let existing = self.get_many(base, &keys)?;
        let mut mutations = Vec::with_capacity(patch.edits.len());
        for (edit, existing) in patch.edits.iter().zip(existing) {
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
                    if existing != *old {
                        return Err(Error::PatchBaseMismatch);
                    }
                    mutations.push(Mutation::Upsert {
                        key: key.clone(),
                        val: new.clone(),
                    });
                }
                LogicalPatch::Delete { key, old } => {
                    if existing.as_ref() != Some(old) {
                        return Err(Error::PatchBaseMismatch);
                    }
                    mutations.push(Mutation::Delete { key: key.clone() });
                }
            }
        }
        self.batch(base, mutations)
    }
}
