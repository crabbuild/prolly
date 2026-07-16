use super::ProximityFilter;
use crate::prolly::error::Error;
use crate::prolly::key::prefix_end;
use crate::prolly::tree::Tree;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EligibilityCardinality {
    Known(u64),
    Unknown,
}

/// Validated, non-copying search eligibility. Large sorted eligible-key lists
/// remain borrowed from the request for planning and execution.
pub(crate) enum PreparedEligibility<'a> {
    All,
    Range {
        start: Option<&'a [u8]>,
        end: Option<&'a [u8]>,
    },
    Prefix(&'a [u8]),
    SortedKeys {
        keys: &'a [Vec<u8>],
        source_bound: bool,
    },
}

impl<'a> PreparedEligibility<'a> {
    pub(crate) fn new(filter: ProximityFilter<'a>, directory: &Tree) -> Result<Self, Error> {
        match filter {
            ProximityFilter::All => Ok(Self::All),
            ProximityFilter::KeyRange { start, end } => {
                if start.zip(end).is_some_and(|(start, end)| start > end) {
                    return Err(invalid("range start must not exceed range end"));
                }
                Ok(Self::Range { start, end })
            }
            ProximityFilter::Prefix(prefix) => Ok(Self::Prefix(prefix)),
            ProximityFilter::EligibleKeys(keys) => Self::eligible(keys, false),
            ProximityFilter::SecondaryEligible {
                keys,
                source_directory,
            } => {
                if source_directory != directory {
                    return Err(invalid(
                        "secondary eligible keys are bound to a stale directory",
                    ));
                }
                Self::eligible(keys, true)
            }
        }
    }

    fn eligible(keys: &'a [Vec<u8>], source_bound: bool) -> Result<Self, Error> {
        if keys.windows(2).any(|pair| pair[0] >= pair[1]) {
            return Err(invalid(
                "eligible keys must be strictly ascending and unique",
            ));
        }
        Ok(Self::SortedKeys { keys, source_bound })
    }

    pub(crate) fn contains(&self, key: &[u8]) -> bool {
        match self {
            Self::All => true,
            Self::Range { start, end } => {
                start.map_or(true, |start| key >= start) && end.map_or(true, |end| key < end)
            }
            Self::Prefix(prefix) => key.starts_with(prefix),
            Self::SortedKeys { keys, .. } => keys
                .binary_search_by(|candidate| candidate.as_slice().cmp(key))
                .is_ok(),
        }
    }

    pub(crate) fn intersects(&self, minimum: &[u8], maximum: &[u8]) -> bool {
        match self {
            Self::All => true,
            Self::Range { start, end } => {
                start.map_or(true, |start| maximum >= start)
                    && end.map_or(true, |end| minimum < end)
            }
            Self::Prefix(prefix) => {
                let end = prefix_end(prefix);
                maximum >= *prefix && end.as_deref().map_or(true, |end| minimum < end)
            }
            Self::SortedKeys { keys, .. } => {
                let index = keys.partition_point(|key| key.as_slice() < minimum);
                keys.get(index).is_some_and(|key| key.as_slice() <= maximum)
            }
        }
    }

    pub(crate) fn cardinality(&self, source_count: u64) -> EligibilityCardinality {
        match self {
            Self::All => EligibilityCardinality::Known(source_count),
            Self::SortedKeys { keys, .. } => EligibilityCardinality::Known(keys.len() as u64),
            Self::Range { .. } | Self::Prefix(_) => EligibilityCardinality::Unknown,
        }
    }

    pub(crate) fn sorted_keys(&self) -> Option<(&'a [Vec<u8>], bool)> {
        match self {
            Self::SortedKeys { keys, source_bound } => Some((keys, *source_bound)),
            _ => None,
        }
    }
}

fn invalid(reason: impl Into<String>) -> Error {
    Error::InvalidProximitySearch {
        reason: reason.into(),
    }
}
