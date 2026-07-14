#[cfg(feature = "async-store")]
mod r#async;
mod engine;
mod filter;
mod policy;
mod sync;

use super::{QueryKernel, SearchBackend, SearchBudget, SearchPolicy};
use crate::prolly::error::Error;
use crate::prolly::tree::Tree;

pub(crate) use engine::{insert_top_k, FrontierEntry, SearchCandidate};
pub(crate) use filter::PreparedFilter;
pub(crate) use policy::{adaptive_should_stop, AdaptiveContext};
#[cfg(feature = "async-store")]
pub use r#async::{AsyncIoConfig, AsyncProximityMap, AsyncSearchControl, CancellationToken};

/// Canonical structural restriction applied before leaf scoring.
#[derive(Clone, Debug)]
pub enum ProximityFilter<'a> {
    All,
    /// Inclusive start and exclusive end. `None` is unbounded.
    KeyRange {
        start: Option<&'a [u8]>,
        end: Option<&'a [u8]>,
    },
    Prefix(&'a [u8]),
    /// Keys must be strictly ascending and unique.
    EligibleKeys(&'a [Vec<u8>]),
    /// Secondary-derived keys bound to the exact source snapshot.
    SecondaryEligible {
        keys: &'a [Vec<u8>],
        source_directory: &'a Tree,
    },
}

/// One deterministic native proximity search.
#[derive(Clone, Debug)]
pub struct SearchRequest<'a> {
    pub query: &'a [f32],
    pub k: usize,
    pub policy: SearchPolicy,
    pub budget: SearchBudget,
    pub filter: ProximityFilter<'a>,
    pub backend: SearchBackend,
    pub kernel: QueryKernel,
}

impl<'a> SearchRequest<'a> {
    pub fn exact(query: &'a [f32], k: usize) -> Self {
        Self {
            query,
            k,
            policy: SearchPolicy::Exact,
            budget: SearchBudget::default(),
            filter: ProximityFilter::All,
            backend: SearchBackend::Native,
            kernel: QueryKernel::AutoDeterministic,
        }
    }

    pub(crate) fn validate(&self) -> Result<(), Error> {
        if self.k == 0 {
            return Err(Error::InvalidProximitySearch {
                reason: "k must be greater than zero".to_owned(),
            });
        }
        self.budget.validate()?;
        Ok(())
    }
}
