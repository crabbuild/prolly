use super::{ApproximatePreference, EligibilityCardinality, PreparedFilter, SearchRequest};
use crate::prolly::error::Error;
use crate::prolly::proximity::accelerator::AcceleratorSet;
use crate::prolly::proximity::{CompositeAcceleratorConfig, CompositeBaseKind};
use crate::prolly::proximity::{HnswConfig, ProductQuantizationConfig};
use crate::prolly::proximity::{ProximityTree, SearchBackend, SearchPolicy};
use crate::prolly::store::Store;

pub const SEARCH_PLAN_FORMAT_VERSION: u8 = 3;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SearchPlan {
    Native,
    EligibleExact {
        key_count: u64,
        source_bound: bool,
    },
    ProductQuantized {
        rerank_target: usize,
        direct_lookup: bool,
    },
    Hnsw {
        ef_search: u32,
        expansion_target: usize,
        rerank_target: usize,
    },
    Composite {
        base: Box<SearchPlan>,
        delta_records: usize,
        shadow_records: usize,
        merge_target: usize,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SearchPlanSummary {
    pub format_version: u8,
    pub backend: SearchBackend,
    pub eligible_exact_records: Option<u64>,
    pub hnsw_ef_search: Option<u32>,
    pub expansion_target: Option<usize>,
    pub rerank_target: Option<usize>,
    pub direct_lookup: bool,
    pub composite_base_backend: Option<SearchBackend>,
    pub composite_base: Option<Box<SearchPlanSummary>>,
    pub delta_records: Option<usize>,
    pub shadow_records: Option<usize>,
}

impl SearchPlan {
    pub fn summary(&self) -> SearchPlanSummary {
        let mut summary = SearchPlanSummary {
            format_version: SEARCH_PLAN_FORMAT_VERSION,
            backend: SearchBackend::Native,
            eligible_exact_records: None,
            hnsw_ef_search: None,
            expansion_target: None,
            rerank_target: None,
            direct_lookup: false,
            composite_base_backend: None,
            composite_base: None,
            delta_records: None,
            shadow_records: None,
        };
        match self {
            Self::Native => {}
            Self::EligibleExact { key_count, .. } => {
                summary.eligible_exact_records = Some(*key_count);
            }
            Self::ProductQuantized {
                rerank_target,
                direct_lookup,
            } => {
                summary.backend = SearchBackend::ProductQuantized;
                summary.rerank_target = Some(*rerank_target);
                summary.direct_lookup = *direct_lookup;
            }
            Self::Hnsw {
                ef_search,
                expansion_target,
                rerank_target,
            } => {
                summary.backend = SearchBackend::Hnsw;
                summary.hnsw_ef_search = Some(*ef_search);
                summary.expansion_target = Some(*expansion_target);
                summary.rerank_target = Some(*rerank_target);
            }
            Self::Composite {
                base,
                delta_records,
                shadow_records,
                merge_target,
            } => {
                summary.backend = SearchBackend::Composite;
                summary.composite_base_backend = Some(base.summary().backend);
                summary.composite_base = Some(Box::new(base.summary()));
                summary.delta_records = Some(*delta_records);
                summary.shadow_records = Some(*shadow_records);
                summary.rerank_target = Some(*merge_target);
            }
        }
        summary
    }
}

pub(crate) struct CompositePlanInput<'a> {
    pub base_kind: CompositeBaseKind,
    pub hnsw: Option<&'a HnswConfig>,
    pub pq: Option<&'a ProductQuantizationConfig>,
    pub base_count: u64,
    pub delta_count: u64,
    pub shadow_count: u64,
    pub config: &'a CompositeAcceleratorConfig,
}

pub(crate) fn plan_search<S>(
    tree: &ProximityTree,
    accelerators: &AcceleratorSet<S>,
    request: &SearchRequest<'_>,
    eligibility: &PreparedFilter<'_>,
) -> Result<SearchPlan, Error>
where
    S: Store + Clone + Send + Sync,
    S::Error: Send + Sync,
{
    plan_search_capabilities(
        tree,
        accelerators.hnsw().map(|index| index.config()),
        accelerators.pq().map(|index| index.config()),
        accelerators
            .composite()
            .map(|composite| CompositePlanInput {
                base_kind: composite.base_kind(),
                hnsw: composite.base.hnsw().map(|index| index.config()),
                pq: composite.base.pq().map(|index| index.config()),
                base_count: composite.base_count,
                delta_count: composite.delta_count,
                shadow_count: composite.shadow_count,
                config: &composite.config,
            }),
        request,
        eligibility,
    )
}

pub(crate) fn plan_search_capabilities(
    tree: &ProximityTree,
    hnsw: Option<&HnswConfig>,
    pq: Option<&ProductQuantizationConfig>,
    composite: Option<CompositePlanInput<'_>>,
    request: &SearchRequest<'_>,
    eligibility: &PreparedFilter<'_>,
) -> Result<SearchPlan, Error> {
    let cardinality = eligibility.cardinality(tree.count);
    match request.options.backend {
        SearchBackend::Native => return Ok(SearchPlan::Native),
        SearchBackend::Hnsw => {
            ensure_approximate(request, "HNSW")?;
            let config = hnsw.ok_or_else(|| invalid("forced HNSW backend is unavailable"))?;
            return hnsw_plan(tree, config, request, cardinality);
        }
        SearchBackend::ProductQuantized => {
            ensure_approximate(request, "product quantization")?;
            let config =
                pq.ok_or_else(|| invalid("forced product-quantized backend is unavailable"))?;
            return pq_plan(tree, config, request, eligibility, cardinality);
        }
        SearchBackend::Composite => {
            ensure_approximate(request, "composite")?;
            return composite_plan(
                tree,
                composite.ok_or_else(|| invalid("forced composite backend is unavailable"))?,
                request,
                eligibility,
                cardinality,
            );
        }
        SearchBackend::Auto => {}
    }

    if let Some(plan) = eligible_exact_plan(tree, request, eligibility, cardinality)? {
        if matches!(plan, SearchPlan::EligibleExact { key_count: 0, .. })
            || request.policy == SearchPolicy::Exact
            || request.options.planner.allow_exact_for_approximate
        {
            return Ok(plan);
        }
    }
    if request.policy == SearchPolicy::Exact {
        return Ok(SearchPlan::Native);
    }

    let preferences = match request.options.planner.approximate_preference {
        ApproximatePreference::HnswFirst => [SearchBackend::Hnsw, SearchBackend::ProductQuantized],
        ApproximatePreference::ProductQuantizedFirst => {
            [SearchBackend::ProductQuantized, SearchBackend::Hnsw]
        }
    };
    for backend in preferences {
        let plan = match backend {
            SearchBackend::Hnsw => hnsw
                .map(|config| hnsw_plan(tree, config, request, cardinality))
                .transpose()?,
            SearchBackend::ProductQuantized => pq
                .map(|config| pq_plan(tree, config, request, eligibility, cardinality))
                .transpose()?,
            SearchBackend::Native | SearchBackend::Composite | SearchBackend::Auto => None,
        };
        if plan
            .as_ref()
            .is_some_and(|plan| budget_admissible(plan, request))
        {
            return Ok(plan.expect("checked plan"));
        }
    }
    if let Some(composite) = composite {
        let plan = composite_plan(tree, composite, request, eligibility, cardinality)?;
        if budget_admissible(&plan, request) {
            return Ok(plan);
        }
    }
    Ok(SearchPlan::Native)
}

fn composite_plan(
    tree: &ProximityTree,
    input: CompositePlanInput<'_>,
    request: &SearchRequest<'_>,
    eligibility: &PreparedFilter<'_>,
    cardinality: EligibilityCardinality,
) -> Result<SearchPlan, Error> {
    let mut base = match input.base_kind {
        CompositeBaseKind::Hnsw => hnsw_plan_for_count(
            input.base_count,
            input
                .hnsw
                .ok_or_else(|| invalid("composite HNSW configuration is absent"))?,
            request,
            cardinality,
        )?,
        CompositeBaseKind::ProductQuantized => pq_plan_for_count(
            input.base_count,
            input
                .pq
                .ok_or_else(|| invalid("composite PQ configuration is absent"))?,
            request,
            eligibility,
            cardinality,
        )?,
    };
    inflate_composite_base(
        &mut base,
        input.base_count,
        input.shadow_count,
        input.config.base_overfetch_multiplier,
    )?;
    let delta_records = usize::try_from(input.delta_count).unwrap_or(usize::MAX);
    Ok(SearchPlan::Composite {
        base: Box::new(base),
        delta_records,
        shadow_records: usize::try_from(input.shadow_count).unwrap_or(usize::MAX),
        merge_target: request.k.min(tree.count as usize),
    })
}

fn inflate_composite_base(
    plan: &mut SearchPlan,
    base_count: u64,
    shadow_count: u64,
    multiplier: u32,
) -> Result<(), Error> {
    let surviving = base_count.saturating_sub(shadow_count).max(1);
    let inflate = |value: usize| -> Result<usize, Error> {
        let scaled = (value as u128)
            .checked_mul(u128::from(base_count))
            .and_then(|value| value.checked_mul(u128::from(multiplier)))
            .ok_or_else(|| invalid("composite base inflation overflow"))?
            .div_ceil(u128::from(surviving));
        Ok(usize::try_from(scaled.min(u128::from(base_count))).unwrap_or(usize::MAX))
    };
    match plan {
        SearchPlan::Hnsw {
            expansion_target,
            rerank_target,
            ..
        } => {
            *expansion_target = inflate(*expansion_target)?;
            *rerank_target = inflate(*rerank_target)?;
        }
        SearchPlan::ProductQuantized {
            rerank_target,
            direct_lookup,
        } => {
            *rerank_target = inflate(*rerank_target)?;
            *direct_lookup = false;
        }
        _ => return Err(invalid("composite base plan is not approximate")),
    }
    Ok(())
}

fn eligible_exact_plan(
    tree: &ProximityTree,
    request: &SearchRequest<'_>,
    eligibility: &PreparedFilter<'_>,
    cardinality: EligibilityCardinality,
) -> Result<Option<SearchPlan>, Error> {
    let EligibilityCardinality::Known(eligible) = cardinality else {
        return Ok(None);
    };
    let Some((_, source_bound)) = eligibility.sorted_keys() else {
        return Ok(None);
    };
    if eligible == 0 {
        return Ok(Some(SearchPlan::EligibleExact {
            key_count: 0,
            source_bound,
        }));
    }
    let ratio_numerator = u128::from(tree.count)
        .checked_mul(u128::from(request.options.planner.eligible_exact_ratio_ppm))
        .ok_or_else(|| invalid("eligible exact ratio overflow"))?;
    let ratio_limit = ratio_numerator
        .checked_add(999_999)
        .ok_or_else(|| invalid("eligible exact ratio overflow"))?
        / 1_000_000;
    let ratio_limit = usize::try_from(ratio_limit).unwrap_or(usize::MAX);
    let threshold = request.k.max(
        request
            .options
            .planner
            .eligible_exact_max_records
            .min(ratio_limit),
    );
    Ok(
        (eligible <= threshold as u64).then_some(SearchPlan::EligibleExact {
            key_count: eligible,
            source_bound,
        }),
    )
}

fn hnsw_plan(
    tree: &ProximityTree,
    config: &HnswConfig,
    request: &SearchRequest<'_>,
    cardinality: EligibilityCardinality,
) -> Result<SearchPlan, Error> {
    hnsw_plan_for_count(tree.count, config, request, cardinality)
}

fn hnsw_plan_for_count(
    total_count: u64,
    config: &HnswConfig,
    request: &SearchRequest<'_>,
    cardinality: EligibilityCardinality,
) -> Result<SearchPlan, Error> {
    let ef_search = request.options.hnsw.ef_search.unwrap_or(config.ef_search);
    let base = usize::try_from(ef_search).unwrap_or(usize::MAX).max(
        request
            .k
            .checked_mul(config.overfetch_multiplier as usize)
            .ok_or_else(|| invalid("HNSW expansion target overflow"))?,
    );
    let expansion_target = match cardinality {
        EligibilityCardinality::Known(0) => 0,
        EligibilityCardinality::Known(eligible) => {
            let numerator = (base as u128)
                .checked_mul(u128::from(total_count))
                .ok_or_else(|| invalid("HNSW selective expansion overflow"))?;
            let target = numerator
                .checked_add(u128::from(eligible) - 1)
                .ok_or_else(|| invalid("HNSW selective expansion overflow"))?
                / u128::from(eligible);
            usize::try_from(target.min(u128::from(total_count))).unwrap_or(usize::MAX)
        }
        EligibilityCardinality::Unknown => base.min(total_count as usize),
    };
    let known_limit = match cardinality {
        EligibilityCardinality::Known(count) => count,
        EligibilityCardinality::Unknown => total_count,
    };
    let rerank_target = request
        .k
        .checked_mul(config.overfetch_multiplier as usize)
        .ok_or_else(|| invalid("HNSW rerank target overflow"))?
        .max(request.k)
        .min(known_limit as usize)
        .min(total_count as usize);
    Ok(SearchPlan::Hnsw {
        ef_search,
        expansion_target,
        rerank_target,
    })
}

fn pq_plan(
    tree: &ProximityTree,
    config: &ProductQuantizationConfig,
    request: &SearchRequest<'_>,
    eligibility: &PreparedFilter<'_>,
    cardinality: EligibilityCardinality,
) -> Result<SearchPlan, Error> {
    pq_plan_for_count(tree.count, config, request, eligibility, cardinality)
}

fn pq_plan_for_count(
    total_count: u64,
    config: &ProductQuantizationConfig,
    request: &SearchRequest<'_>,
    eligibility: &PreparedFilter<'_>,
    cardinality: EligibilityCardinality,
) -> Result<SearchPlan, Error> {
    let multiplier = request
        .options
        .pq
        .rerank_multiplier
        .map(usize::from)
        .unwrap_or(config.rerank_multiplier as usize);
    let known_limit = match cardinality {
        EligibilityCardinality::Known(count) => count,
        EligibilityCardinality::Unknown => total_count,
    };
    let rerank_target = request
        .k
        .checked_mul(multiplier)
        .ok_or_else(|| invalid("PQ rerank target overflow"))?
        .max(request.k)
        .min(known_limit as usize)
        .min(total_count as usize);
    let direct_lookup = eligibility.sorted_keys().is_some()
        && known_limit <= request.options.planner.eligible_exact_max_records as u64;
    Ok(SearchPlan::ProductQuantized {
        rerank_target,
        direct_lookup,
    })
}

fn budget_admissible(plan: &SearchPlan, request: &SearchRequest<'_>) -> bool {
    match plan {
        SearchPlan::Hnsw {
            expansion_target,
            rerank_target,
            ..
        } => {
            request
                .budget
                .max_nodes
                .map_or(true, |limit| *expansion_target <= limit)
                && request
                    .budget
                    .max_distance_evaluations
                    .map_or(true, |limit| {
                        expansion_target.saturating_add(*rerank_target) <= limit
                    })
                && request
                    .budget
                    .max_frontier_entries
                    .map_or(true, |limit| request.k <= limit)
        }
        SearchPlan::ProductQuantized { rerank_target, .. } => request
            .budget
            .max_distance_evaluations
            .map_or(true, |limit| *rerank_target <= limit),
        SearchPlan::Composite {
            base,
            delta_records,
            shadow_records,
            ..
        } => {
            let node_work = estimated_node_work(base)
                .saturating_add(*shadow_records)
                .saturating_add(delta_records.saturating_mul(2));
            let distance_work = estimated_distance_work(base).saturating_add(*delta_records);
            budget_admissible(base, request)
                && request
                    .budget
                    .max_nodes
                    .map_or(true, |limit| node_work <= limit)
                && request
                    .budget
                    .max_distance_evaluations
                    .map_or(true, |limit| distance_work <= limit)
        }
        SearchPlan::Native | SearchPlan::EligibleExact { .. } => true,
    }
}

fn estimated_node_work(plan: &SearchPlan) -> usize {
    match plan {
        SearchPlan::Hnsw {
            expansion_target,
            rerank_target,
            ..
        } => expansion_target.saturating_add(*rerank_target),
        SearchPlan::ProductQuantized { rerank_target, .. } => *rerank_target,
        SearchPlan::Composite {
            base,
            delta_records,
            shadow_records,
            ..
        } => estimated_node_work(base)
            .saturating_add(*shadow_records)
            .saturating_add(delta_records.saturating_mul(2)),
        SearchPlan::Native | SearchPlan::EligibleExact { .. } => 0,
    }
}

fn estimated_distance_work(plan: &SearchPlan) -> usize {
    match plan {
        SearchPlan::Hnsw {
            expansion_target,
            rerank_target,
            ..
        } => expansion_target.saturating_add(*rerank_target),
        SearchPlan::ProductQuantized { rerank_target, .. } => *rerank_target,
        SearchPlan::Composite {
            base,
            delta_records,
            ..
        } => estimated_distance_work(base).saturating_add(*delta_records),
        SearchPlan::Native | SearchPlan::EligibleExact { .. } => 0,
    }
}

fn ensure_approximate(request: &SearchRequest<'_>, backend: &str) -> Result<(), Error> {
    if request.policy == SearchPolicy::Exact {
        Err(invalid(format!("{backend} cannot satisfy exact search")))
    } else {
        Ok(())
    }
}

fn invalid(reason: impl Into<String>) -> Error {
    Error::InvalidProximitySearch {
        reason: reason.into(),
    }
}
