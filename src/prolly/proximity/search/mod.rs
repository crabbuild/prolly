#[cfg(feature = "async-store")]
mod r#async;
mod engine;
mod filter;
mod planner;
mod policy;
mod runtime;
mod sync;

use super::{QueryKernel, SearchBackend, SearchBudget, SearchPolicy};
use crate::prolly::error::Error;
use crate::prolly::tree::Tree;

pub(crate) use engine::{insert_top_k, FrontierEntry, SearchCandidate};
pub use filter::EligibilityCardinality;
pub(crate) use filter::PreparedEligibility as PreparedFilter;
pub(crate) use planner::plan_search;
#[cfg(feature = "async-store")]
pub(crate) use planner::plan_search_capabilities;
pub use planner::{SearchPlan, SearchPlanSummary, SEARCH_PLAN_FORMAT_VERSION};
pub(crate) use policy::{adaptive_should_stop, AdaptiveContext};
#[cfg(feature = "async-store")]
pub use r#async::{AsyncIoConfig, AsyncProximityMap, AsyncSearchControl, CancellationToken};
pub use runtime::{SearchIo, SearchRuntime, SearchRuntimePolicy, StoreCacheNamespace};

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
    pub kernel: QueryKernel,
    pub options: SearchOptions,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ApproximatePreference {
    HnswFirst,
    ProductQuantizedFirst,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PlannerPolicy {
    pub allow_exact_for_approximate: bool,
    pub eligible_exact_max_records: usize,
    pub eligible_exact_ratio_ppm: u32,
    pub approximate_preference: ApproximatePreference,
}

impl Default for PlannerPolicy {
    fn default() -> Self {
        Self {
            allow_exact_for_approximate: true,
            eligible_exact_max_records: 4_096,
            eligible_exact_ratio_ppm: 10_000,
            approximate_preference: ApproximatePreference::HnswFirst,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct HnswSearchOptions {
    pub ef_search: Option<u32>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PqSearchOptions {
    pub rerank_multiplier: Option<u16>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SearchOptions {
    pub backend: SearchBackend,
    pub planner: PlannerPolicy,
    pub hnsw: HnswSearchOptions,
    pub pq: PqSearchOptions,
}

impl Default for SearchOptions {
    fn default() -> Self {
        Self {
            backend: SearchBackend::Auto,
            planner: PlannerPolicy::default(),
            hnsw: HnswSearchOptions::default(),
            pq: PqSearchOptions::default(),
        }
    }
}

impl<'a> SearchRequest<'a> {
    pub fn exact(query: &'a [f32], k: usize) -> Self {
        Self {
            query,
            k,
            policy: SearchPolicy::Exact,
            budget: SearchBudget::default(),
            filter: ProximityFilter::All,
            kernel: QueryKernel::AutoDeterministic,
            options: SearchOptions::default(),
        }
    }

    pub(crate) fn validate(&self) -> Result<(), Error> {
        if self.k == 0 {
            return Err(Error::InvalidProximitySearch {
                reason: "k must be greater than zero".to_owned(),
            });
        }
        self.budget.validate()?;
        if self.options.planner.eligible_exact_ratio_ppm > 1_000_000 {
            return Err(Error::InvalidProximitySearch {
                reason: "eligible_exact_ratio_ppm must not exceed 1,000,000".to_owned(),
            });
        }
        if self.options.hnsw.ef_search == Some(0) || self.options.pq.rerank_multiplier == Some(0) {
            return Err(Error::InvalidProximitySearch {
                reason: "HNSW ef_search and PQ rerank_multiplier overrides must be positive"
                    .to_owned(),
            });
        }
        Ok(())
    }
}
