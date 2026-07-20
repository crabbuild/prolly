use prolly::{Config, Error, ExecutionConfig};

#[test]
fn execution_config_rejects_every_zero_limit() {
    for (field, values) in [
        ("read_parallelism", [0, 1, 1, 1]),
        ("max_in_flight_bytes", [1, 0, 1, 1]),
        ("node_cache_max_nodes", [1, 1, 0, 1]),
        ("node_cache_max_bytes", [1, 1, 1, 0]),
    ] {
        assert!(matches!(
            ExecutionConfig::try_new(values[0], values[1], values[2], values[3]),
            Err(Error::InvalidExecutionConfig {
                field: found_field,
                value: 0,
            }) if found_field == field
        ));
    }
}

#[test]
fn execution_defaults_are_finite_and_nonzero() {
    let config = ExecutionConfig::default();
    assert!(config.read_parallelism().get() > 0);
    assert!(config.max_in_flight_bytes().get() > 0);
    assert!(config.node_cache_max_nodes().get() > 0);
    assert!(config.node_cache_max_bytes().get() > 0);

    let legacy_defaults = Config::default();
    assert!(legacy_defaults.runtime.node_cache_max_nodes.is_some());
    assert!(legacy_defaults.runtime.node_cache_max_bytes.is_some());
    assert!(legacy_defaults.runtime.read_parallelism > 0);
}
