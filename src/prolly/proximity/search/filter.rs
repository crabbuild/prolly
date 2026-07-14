use super::ProximityFilter;
use crate::prolly::error::Error;
use crate::prolly::key::prefix_end;
use crate::prolly::tree::Tree;

pub(crate) enum PreparedFilter {
    Range {
        start: Option<Vec<u8>>,
        end: Option<Vec<u8>>,
    },
    Eligible(Vec<Vec<u8>>),
}

impl PreparedFilter {
    pub(crate) fn new(filter: ProximityFilter<'_>, directory: &Tree) -> Result<Self, Error> {
        match filter {
            ProximityFilter::All => Ok(Self::Range {
                start: None,
                end: None,
            }),
            ProximityFilter::KeyRange { start, end } => {
                if start.zip(end).is_some_and(|(start, end)| start > end) {
                    return Err(invalid("range start must not exceed range end"));
                }
                Ok(Self::Range {
                    start: start.map(<[u8]>::to_vec),
                    end: end.map(<[u8]>::to_vec),
                })
            }
            ProximityFilter::Prefix(prefix) => Ok(Self::Range {
                start: Some(prefix.to_vec()),
                end: prefix_end(prefix),
            }),
            ProximityFilter::EligibleKeys(keys) => Self::eligible(keys),
            ProximityFilter::SecondaryEligible {
                keys,
                source_directory,
            } => {
                if source_directory != directory {
                    return Err(invalid(
                        "secondary eligible keys are bound to a stale directory",
                    ));
                }
                Self::eligible(keys)
            }
        }
    }

    fn eligible(keys: &[Vec<u8>]) -> Result<Self, Error> {
        if keys.windows(2).any(|pair| pair[0] >= pair[1]) {
            return Err(invalid(
                "eligible keys must be strictly ascending and unique",
            ));
        }
        Ok(Self::Eligible(keys.to_vec()))
    }

    pub(crate) fn contains(&self, key: &[u8]) -> bool {
        match self {
            Self::Range { start, end } => {
                start.as_ref().map_or(true, |start| key >= start)
                    && end.as_ref().map_or(true, |end| key < end)
            }
            Self::Eligible(keys) => keys
                .binary_search_by(|candidate| candidate.as_slice().cmp(key))
                .is_ok(),
        }
    }

    pub(crate) fn intersects(&self, minimum: &[u8], maximum: &[u8]) -> bool {
        match self {
            Self::Range { start, end } => {
                start.as_ref().map_or(true, |start| maximum >= start)
                    && end.as_ref().map_or(true, |end| minimum < end)
            }
            Self::Eligible(keys) => {
                let index = keys.partition_point(|key| key.as_slice() < minimum);
                keys.get(index).is_some_and(|key| key.as_slice() <= maximum)
            }
        }
    }
}

fn invalid(reason: impl Into<String>) -> Error {
    Error::InvalidProximitySearch {
        reason: reason.into(),
    }
}
