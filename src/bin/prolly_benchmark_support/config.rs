use std::env;

use prolly::Config;

pub const CACHE_PROFILE_ENV: &str = "PROLLY_BENCH_CACHE_PROFILE";

pub fn benchmark_config() -> Config {
    match env::var(CACHE_PROFILE_ENV) {
        Ok(value) => config_for_profile(Some(&value)),
        Err(env::VarError::NotPresent) => config_for_profile(None),
        Err(error) => panic!("cannot read {CACHE_PROFILE_ENV}: {error}"),
    }
}

fn config_for_profile(profile: Option<&str>) -> Config {
    match profile {
        Some("unbounded") => Config::builder().unbounded_node_cache().build(),
        Some("bounded") | None => Config::default(),
        Some(value) => {
            panic!("invalid {CACHE_PROFILE_ENV}={value:?}; expected bounded or unbounded")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::config_for_profile;

    #[test]
    fn bounded_is_the_default_profile() {
        let implicit = config_for_profile(None);
        let explicit = config_for_profile(Some("bounded"));
        assert_eq!(implicit, explicit);
        assert!(explicit.runtime.node_cache_max_nodes.is_some());
        assert!(explicit.runtime.node_cache_max_bytes.is_some());
    }

    #[test]
    fn unbounded_removes_both_cache_limits() {
        let config = config_for_profile(Some("unbounded"));
        assert_eq!(config.runtime.node_cache_max_nodes, None);
        assert_eq!(config.runtime.node_cache_max_bytes, None);
    }

    #[test]
    #[should_panic(expected = "expected bounded or unbounded")]
    fn invalid_profile_fails_closed() {
        let _ = config_for_profile(Some("other"));
    }
}
