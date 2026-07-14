use crate::prolly::proximity::{ProximityConfig, SearchPolicy};

pub(crate) fn enabled(config: &ProximityConfig, policy: SearchPolicy) -> bool {
    config.scalar_quantization.is_some() && policy != SearchPolicy::Exact
}
