use prolly::{
    chunking, BoundaryDetector, BoundaryInput, BoundaryRule, ChunkMeasure, ChunkingSpec, Error,
    HashAlgorithm,
};

fn cuts(mut detector: BoundaryDetector, values: &[&[u8]]) -> Vec<usize> {
    let mut result = Vec::new();
    for (index, value) in values.iter().enumerate() {
        let key = format!("key-{index:04}");
        if detector
            .observe(key.as_bytes(), value, key.len() + value.len() + 2)
            .unwrap()
        {
            result.push(index);
        }
    }
    result
}

#[test]
fn built_in_presets_validate() {
    for spec in [
        chunking::entry_count_key_value_hash(),
        chunking::entry_count_key_hash(),
        chunking::logical_bytes_key_weibull(),
        chunking::logical_bytes_rolling_hash(),
    ] {
        spec.validate().unwrap();
    }
}

#[test]
fn invalid_policy_parameters_are_rejected() {
    let base = chunking::entry_count_key_hash();
    for spec in [
        ChunkingSpec {
            min: 0,
            ..base.clone()
        },
        ChunkingSpec {
            min: 10,
            target: 9,
            ..base.clone()
        },
        ChunkingSpec {
            target: 10,
            max: 9,
            ..base.clone()
        },
        ChunkingSpec {
            rule: BoundaryRule::HashThreshold { factor: 0 },
            ..base.clone()
        },
        ChunkingSpec {
            rule: BoundaryRule::Weibull { shape: 0 },
            ..base.clone()
        },
        ChunkingSpec {
            rule: BoundaryRule::RollingBuzHash { window: 0 },
            ..base.clone()
        },
        ChunkingSpec {
            hard_max_node_bytes: 0,
            ..base
        },
    ] {
        assert!(matches!(spec.validate(), Err(Error::InvalidFormat(_))));
    }
}

#[test]
fn key_only_threshold_cuts_do_not_depend_on_values() {
    let spec = ChunkingSpec {
        measure: ChunkMeasure::EntryCount,
        input: BoundaryInput::Key,
        hash: HashAlgorithm::XxHash64,
        rule: BoundaryRule::HashThreshold { factor: 8 },
        min: 2,
        target: 8,
        max: 32,
        hash_seed: 91,
        level_salt: true,
        hard_max_node_bytes: 1_000_000,
    };
    let short = vec![b"a".as_slice(); 128];
    let long = vec![b"a much longer and different value".as_slice(); 128];

    assert_eq!(
        cuts(BoundaryDetector::new(spec.clone(), 0).unwrap(), &short),
        cuts(BoundaryDetector::new(spec, 0).unwrap(), &long)
    );
}

#[test]
fn every_policy_is_deterministic_and_resets_after_a_cut() {
    let values = vec![b"payload".as_slice(); 5_000];
    for spec in [
        chunking::entry_count_key_value_hash(),
        chunking::entry_count_key_hash(),
        chunking::logical_bytes_key_weibull(),
        chunking::logical_bytes_rolling_hash(),
    ] {
        let first = cuts(BoundaryDetector::new(spec.clone(), 3).unwrap(), &values);
        let second = cuts(BoundaryDetector::new(spec, 3).unwrap(), &values);
        assert_eq!(first, second);
        assert!(!first.is_empty());
    }
}

#[test]
fn one_entry_larger_than_the_hard_byte_cap_is_rejected() {
    let mut spec = chunking::entry_count_key_hash();
    spec.hard_max_node_bytes = 8;
    let mut detector = BoundaryDetector::new(spec, 0).unwrap();

    assert!(matches!(
        detector.observe(b"key", b"value", 9),
        Err(Error::EntryTooLarge { .. })
    ));
}
