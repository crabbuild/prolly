use std::fmt;

use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

use crate::{DiffRecord, MutationRecord};

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Digest([u8; 32]);

impl Digest {
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    pub fn to_hex(self) -> String {
        self.0.iter().map(|byte| format!("{byte:02x}")).collect()
    }
}

impl fmt::Debug for Digest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.to_hex())
    }
}

impl fmt::Display for Digest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.to_hex())
    }
}

pub struct DigestBuilder(Sha256);

impl DigestBuilder {
    pub fn new(domain: &[u8]) -> Self {
        let mut digest = Sha256::new();
        digest.update((domain.len() as u64).to_le_bytes());
        digest.update(domain);
        Self(digest)
    }

    pub fn field(&mut self, bytes: &[u8]) {
        self.0.update((bytes.len() as u64).to_le_bytes());
        self.0.update(bytes);
    }

    pub fn finish(self) -> Digest {
        Digest::from_bytes(self.0.finalize().into())
    }
}

pub fn digest_entries<'a>(entries: impl IntoIterator<Item = (&'a [u8], &'a [u8])>) -> Digest {
    let mut digest = DigestBuilder::new(b"entries");
    for (key, value) in entries {
        digest.field(key);
        digest.field(value);
    }
    digest.finish()
}

pub fn digest_mutations(mutations: &[MutationRecord]) -> Digest {
    let mut digest = DigestBuilder::new(b"mutations");
    for mutation in mutations {
        match mutation {
            MutationRecord::Upsert { key, value } => {
                digest.field(b"upsert");
                digest.field(key);
                digest.field(value);
            }
            MutationRecord::Delete { key } => {
                digest.field(b"delete");
                digest.field(key);
            }
        }
    }
    digest.finish()
}

pub fn digest_diffs(diffs: &[DiffRecord]) -> Digest {
    let mut digest = DigestBuilder::new(b"diffs");
    for diff in diffs {
        digest.field(&diff.key);
        digest.field(diff.before.as_deref().unwrap_or_default());
        digest.field(diff.after.as_deref().unwrap_or_default());
        digest.field(&[
            u8::from(diff.before.is_some()),
            u8::from(diff.after.is_some()),
        ]);
    }
    digest.finish()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn framing_distinguishes_field_boundaries() {
        let mut first = DigestBuilder::new(b"test");
        first.field(b"a");
        first.field(b"bc");
        let mut second = DigestBuilder::new(b"test");
        second.field(b"ab");
        second.field(b"c");
        assert_ne!(first.finish(), second.finish());
    }
}
