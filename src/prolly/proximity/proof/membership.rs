use super::invalid;
use crate::prolly::cid::Cid;
use crate::prolly::error::Error;
use crate::prolly::proof::KeyProof;
use crate::prolly::proximity::storage::{Descriptor, StoredRecord};
use crate::prolly::proximity::{ExactProximityRecord, ProximityMap};
use crate::prolly::store::Store;

/// Store-independent exact presence or absence proof bound to one PRXI CID.
#[derive(Clone, Debug, PartialEq)]
pub struct ProximityMembershipProof {
    pub descriptor: Cid,
    pub descriptor_bytes: Vec<u8>,
    pub directory_proof: KeyProof,
    pub record_bytes: Option<Vec<u8>>,
}

/// Verified logical outcome of a descriptor-bound membership proof.
#[derive(Clone, Debug, PartialEq)]
pub struct ProximityMembershipVerification {
    pub descriptor: Cid,
    pub key: Vec<u8>,
    pub record: Option<ExactProximityRecord>,
}

impl ProximityMembershipProof {
    /// Verify against a descriptor CID already trusted by the caller.
    pub fn verify_for(
        &self,
        expected_descriptor: &Cid,
    ) -> Result<ProximityMembershipVerification, Error> {
        if &self.descriptor != expected_descriptor {
            return Err(invalid("membership proof targets an unexpected descriptor"));
        }
        self.verify()
    }

    /// Authenticate the descriptor, ordered path, PRVR bytes, vector, and value.
    pub fn verify(&self) -> Result<ProximityMembershipVerification, Error> {
        if Cid::from_bytes(&self.descriptor_bytes) != self.descriptor {
            return Err(invalid("membership descriptor CID mismatch"));
        }
        let descriptor = Descriptor::decode(&self.descriptor_bytes)?;
        let verified = self.directory_proof.verify();
        if !verified.valid
            || verified.root != descriptor.directory.root
            || verified.key != self.directory_proof.key
            || verified.value != self.record_bytes
        {
            return Err(invalid(
                "ordered membership proof is not bound to the descriptor and PRVR bytes",
            ));
        }
        let record = self
            .record_bytes
            .as_deref()
            .map(|bytes| StoredRecord::decode(bytes, descriptor.config.dimensions))
            .transpose()?
            .map(|record| (record.vector, record.value));
        Ok(ProximityMembershipVerification {
            descriptor: self.descriptor.clone(),
            key: verified.key,
            record,
        })
    }
}

impl<S> ProximityMap<S>
where
    S: Store + Clone + Send + Sync,
    S::Error: Send + Sync,
{
    /// Prove exact presence or absence without requiring the verifier's store.
    pub fn prove_membership(&self, key: &[u8]) -> Result<ProximityMembershipProof, Error> {
        let descriptor_bytes = self.load_descriptor_bytes()?;
        let directory_proof = self
            .directory_manager()
            .prove_key(&self.tree().directory, key)?;
        let record_bytes = directory_proof.verify().value;
        Ok(ProximityMembershipProof {
            descriptor: self.tree().descriptor.clone(),
            descriptor_bytes,
            directory_proof,
            record_bytes,
        })
    }
}
