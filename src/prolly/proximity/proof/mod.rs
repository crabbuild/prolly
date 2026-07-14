mod membership;
mod search;
mod structural;

pub use membership::{ProximityMembershipProof, ProximityMembershipVerification};
pub use search::{
    ProximityProofFilter, ProximitySearchClaim, ProximitySearchEvent, ProximitySearchProof,
    ProximitySearchRequest, ProximitySearchVerification,
};
pub use structural::{ProximityStructuralProof, ProximityStructuralVerification};

use crate::prolly::error::Error;

fn invalid(reason: impl Into<String>) -> Error {
    Error::InvalidProximityObject {
        kind: "proximity proof",
        reason: reason.into(),
    }
}
