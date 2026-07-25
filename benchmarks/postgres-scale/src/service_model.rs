use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::config::ServiceConfig;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ServiceOperation {
    PointRead,
    MultiRead,
    Commit,
    Diff,
    Merge,
}

impl ServiceOperation {
    pub const ALL: [Self; 5] = [
        Self::PointRead,
        Self::MultiRead,
        Self::Commit,
        Self::Diff,
        Self::Merge,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::PointRead => "point_read",
            Self::MultiRead => "multi_read",
            Self::Commit => "commit",
            Self::Diff => "diff",
            Self::Merge => "merge",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TenantClass {
    Independent,
    Hot,
}

impl TenantClass {
    pub const ALL: [Self; 2] = [Self::Independent, Self::Hot];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Independent => "independent",
            Self::Hot => "hot",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct ServiceCell {
    pub clients: usize,
    pub pool_size: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TraceItem {
    pub sequence: u64,
    pub operation: ServiceOperation,
    pub tenant: usize,
    pub tenant_class: TenantClass,
    pub root_name: Vec<u8>,
    pub key_ids: Vec<usize>,
    pub generation: u64,
}

pub fn enumerate_service_cells(config: &ServiceConfig) -> Vec<ServiceCell> {
    config
        .clients
        .iter()
        .flat_map(|clients| {
            config.pool_sizes.iter().map(|pool_size| ServiceCell {
                clients: *clients,
                pool_size: *pool_size,
            })
        })
        .collect()
}

pub fn trace_item(config: &ServiceConfig, seed: u64, sequence: u64) -> TraceItem {
    let operation = operation_for(config, sequence);
    let tenant_class = tenant_class_for(config, sequence);
    let tenant = tenant_for(config, seed, sequence, tenant_class);
    let key_count = match operation {
        ServiceOperation::PointRead => 1,
        ServiceOperation::MultiRead => config.multi_read_keys,
        ServiceOperation::Commit | ServiceOperation::Diff => config.commit_keys,
        ServiceOperation::Merge => config.commit_keys.saturating_mul(2),
    };
    TraceItem {
        sequence,
        operation,
        tenant,
        tenant_class,
        root_name: root_name(tenant, "main"),
        key_ids: deterministic_ids(config.records, key_count, seed, sequence),
        generation: sequence.saturating_add(1),
    }
}

pub fn generate_trace(config: &ServiceConfig, seed: u64, count: usize) -> Vec<TraceItem> {
    (0..count)
        .map(|sequence| trace_item(config, seed, sequence as u64))
        .collect()
}

pub fn root_name(tenant: usize, branch: &str) -> Vec<u8> {
    format!("tenant/{tenant:06}/{branch}").into_bytes()
}

pub fn value_sized(id: usize, generation: u64, bytes: usize) -> Vec<u8> {
    let mut output = Vec::with_capacity(bytes);
    let mut block = 0u64;
    while output.len() < bytes {
        let mut hasher = Sha256::new();
        hasher.update((id as u64).to_be_bytes());
        hasher.update(generation.to_be_bytes());
        hasher.update(block.to_be_bytes());
        output.extend_from_slice(&hasher.finalize());
        block = block.saturating_add(1);
    }
    output.truncate(bytes);
    output
}

fn operation_for(config: &ServiceConfig, sequence: u64) -> ServiceOperation {
    let bucket = ((sequence % 100) * 37 % 100) as u32;
    let mix = &config.operation_mix;
    if bucket < mix.point_read {
        ServiceOperation::PointRead
    } else if bucket < mix.point_read + mix.multi_read {
        ServiceOperation::MultiRead
    } else if bucket < mix.point_read + mix.multi_read + mix.commit {
        ServiceOperation::Commit
    } else if bucket < mix.point_read + mix.multi_read + mix.commit + mix.diff {
        ServiceOperation::Diff
    } else {
        ServiceOperation::Merge
    }
}

fn tenant_class_for(config: &ServiceConfig, sequence: u64) -> TenantClass {
    let threshold = (config.hot_root_share * 10_000.0).round() as u64;
    let bucket = (sequence.saturating_mul(4_099)) % 10_000;
    if bucket < threshold {
        TenantClass::Hot
    } else {
        TenantClass::Independent
    }
}

fn tenant_for(config: &ServiceConfig, seed: u64, sequence: u64, class: TenantClass) -> usize {
    let mixed = mix64(seed ^ sequence.rotate_left(19)) as usize;
    match class {
        TenantClass::Hot => mixed % config.hot_roots.max(1),
        TenantClass::Independent => {
            let independent = config.tenants.saturating_sub(config.hot_roots);
            config.hot_roots + mixed % independent.max(1)
        }
    }
}

fn deterministic_ids(records: usize, count: usize, seed: u64, sequence: u64) -> Vec<usize> {
    let count = count.min(records);
    let mut state = seed ^ sequence.rotate_left(23);
    let mut ids = BTreeSet::new();
    while ids.len() < count {
        state = mix64(state);
        ids.insert((state as usize) % records);
    }
    ids.into_iter().collect()
}

fn mix64(mut value: u64) -> u64 {
    value ^= value >> 30;
    value = value.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value ^= value >> 27;
    value = value.wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::WorkloadConfig;

    #[test]
    fn service_matrix_is_the_cartesian_product() {
        let config = WorkloadConfig::default().service;
        let cells = enumerate_service_cells(&config);
        assert_eq!(cells.len(), 8);
        assert!(cells.contains(&ServiceCell {
            clients: 64,
            pool_size: 32,
        }));
    }

    #[test]
    fn trace_is_stable_and_exactly_weighted() {
        let config = WorkloadConfig::default().service;
        let first = generate_trace(&config, 7, 10_000);
        let second = generate_trace(&config, 7, 10_000);
        assert_eq!(first, second);
        for (operation, expected) in [
            (ServiceOperation::PointRead, 4_500),
            (ServiceOperation::MultiRead, 1_500),
            (ServiceOperation::Commit, 2_500),
            (ServiceOperation::Diff, 1_000),
            (ServiceOperation::Merge, 500),
        ] {
            assert_eq!(
                first
                    .iter()
                    .filter(|item| item.operation == operation)
                    .count(),
                expected
            );
        }
        assert_eq!(
            first
                .iter()
                .filter(|item| item.tenant_class == TenantClass::Hot)
                .count(),
            2_000
        );
        let first_ten = &first[..10];
        assert!(ServiceOperation::ALL
            .iter()
            .all(|operation| first_ten.iter().any(|item| item.operation == *operation)));
    }

    #[test]
    fn sized_values_are_exact_and_deterministic() {
        assert_eq!(value_sized(7, 3, 256).len(), 256);
        assert_eq!(value_sized(7, 3, 256), value_sized(7, 3, 256));
        assert_ne!(value_sized(7, 3, 256), value_sized(7, 4, 256));
    }
}
