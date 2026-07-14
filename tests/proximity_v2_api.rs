use prolly::{
    AdaptiveQuality, DistanceMetric, HierarchyConfig, OverflowConfig, ProximityConfig,
    ScalarQuantizationConfig, SearchBackend, SearchBudget, SearchCompletion, SearchPolicy,
    VectorStorageConfig,
};

#[test]
fn v2_config_is_explicit_and_validated() {
    let config = ProximityConfig {
        dimensions: 3,
        metric: DistanceMetric::Cosine,
        hierarchy: HierarchyConfig {
            log_chunk_size: 7,
            level_hash_seed: 42,
        },
        overflow: OverflowConfig {
            min_page_bytes: 4 * 1024,
            target_page_bytes: 64 * 1024,
            max_page_bytes: 256 * 1024,
            hash_seed: 11,
        },
        vector_storage: VectorStorageConfig {
            inline_threshold_bytes: 16 * 1024,
        },
        scalar_quantization: Some(ScalarQuantizationConfig { group_size: 16 }),
    };
    config.validate().unwrap();

    assert_eq!(
        SearchPolicy::Adaptive(AdaptiveQuality::Balanced),
        SearchPolicy::Adaptive(AdaptiveQuality::Balanced)
    );
    assert_eq!(SearchCompletion::Exact, SearchCompletion::Exact);
    assert_eq!(SearchBackend::Auto, SearchBackend::Auto);
    SearchBudget::default().validate().unwrap();
}

#[test]
fn v2_config_rejects_invalid_limits() {
    let mut config = ProximityConfig::new(3);
    config.hierarchy.log_chunk_size = 0;
    assert!(config.validate().is_err());

    let mut config = ProximityConfig::new(3);
    config.overflow.target_page_bytes = config.overflow.max_page_bytes + 1;
    assert!(config.validate().is_err());

    let mut config = ProximityConfig::new(3);
    config.scalar_quantization = Some(ScalarQuantizationConfig { group_size: 0 });
    assert!(config.validate().is_err());

    let budget = SearchBudget {
        max_nodes: Some(0),
        ..SearchBudget::default()
    };
    assert!(budget.validate().is_err());
}
