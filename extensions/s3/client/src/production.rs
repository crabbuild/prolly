use std::{path::PathBuf, time::Duration};

use prolly_s3_core::{
    BoundaryInput, BoundaryRule, ChunkMeasure, ChunkingSpec, HashAlgorithm, NodeCachePrewarmReport,
    NodeCacheSnapshot, NodeLayoutSpec, TreeFormat,
};

use crate::S3OperationMetrics;

/// Cardinality-derived cache capacities for one repository process.
///
/// These are starting points, not provider-independent SLO guarantees. Use
/// the exported cache and byte-amplification metrics to tune them against the
/// actual key distribution and read mix.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CacheSizingRecommendation {
    pub expected_objects: u64,
    pub memory_capacity_bytes: usize,
    pub disk_capacity_bytes: usize,
    pub disk_block_size_bytes: usize,
    pub memory_shards: usize,
    pub max_cached_node_pack_bytes: usize,
    pub max_cached_node_locations: usize,
    pub startup_prewarm_levels: usize,
}

impl CacheSizingRecommendation {
    pub fn for_object_count(expected_objects: u64) -> Self {
        const MIB: usize = 1024 * 1024;
        const GIB: usize = 1024 * MIB;
        let expected_objects = expected_objects.max(1);
        match expected_objects {
            1..=100_000 => Self {
                expected_objects,
                memory_capacity_bytes: 128 * MIB,
                disk_capacity_bytes: 2 * GIB,
                disk_block_size_bytes: MIB,
                memory_shards: 4,
                max_cached_node_pack_bytes: 64 * MIB,
                max_cached_node_locations: 131_072,
                startup_prewarm_levels: 2,
            },
            100_001..=500_000 => Self {
                expected_objects,
                memory_capacity_bytes: 256 * MIB,
                disk_capacity_bytes: 8 * GIB,
                disk_block_size_bytes: MIB,
                memory_shards: 8,
                max_cached_node_pack_bytes: 128 * MIB,
                max_cached_node_locations: 262_144,
                startup_prewarm_levels: 3,
            },
            500_001..=1_000_000 => Self {
                expected_objects,
                memory_capacity_bytes: 512 * MIB,
                disk_capacity_bytes: 16 * GIB,
                disk_block_size_bytes: MIB,
                memory_shards: 16,
                max_cached_node_pack_bytes: 256 * MIB,
                max_cached_node_locations: 524_288,
                startup_prewarm_levels: 3,
            },
            _ => {
                let millions = expected_objects.saturating_add(999_999) / 1_000_000;
                let scale = usize::try_from(millions).unwrap_or(usize::MAX);
                Self {
                    expected_objects,
                    memory_capacity_bytes: (512 * MIB)
                        .saturating_mul(scale)
                        .clamp(512 * MIB, 4 * GIB),
                    disk_capacity_bytes: (16 * GIB)
                        .saturating_mul(scale)
                        .clamp(16 * GIB, 128 * GIB),
                    disk_block_size_bytes: MIB,
                    memory_shards: 16,
                    max_cached_node_pack_bytes: (256 * MIB)
                        .saturating_mul(scale)
                        .clamp(256 * MIB, 2 * GIB),
                    max_cached_node_locations: 524_288usize
                        .saturating_mul(scale)
                        .clamp(524_288, 4_194_304),
                    startup_prewarm_levels: 4,
                }
            }
        }
    }
}

/// Persistent-cache and bounded cold-start policy for a production client.
///
/// Applying this profile always opens a hybrid memory/disk node cache. It is
/// intentionally explicit because the application must choose a directory
/// with one filesystem owner and an appropriate storage budget.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProductionCacheProfile {
    pub directory: PathBuf,
    pub sizing: CacheSizingRecommendation,
    pub startup_prewarm_levels: usize,
    pub startup_prewarm_timeout: Duration,
    pub require_successful_prewarm: bool,
}

impl ProductionCacheProfile {
    pub fn new(directory: impl Into<PathBuf>, expected_objects: u64) -> Self {
        let sizing = CacheSizingRecommendation::for_object_count(expected_objects);
        Self {
            directory: directory.into(),
            startup_prewarm_levels: sizing.startup_prewarm_levels,
            sizing,
            startup_prewarm_timeout: Duration::from_secs(30),
            require_successful_prewarm: true,
        }
    }

    pub fn startup_prewarm(mut self, levels: usize, timeout: Duration) -> Self {
        self.startup_prewarm_levels = levels;
        self.startup_prewarm_timeout = timeout;
        self
    }

    pub fn require_successful_prewarm(mut self, required: bool) -> Self {
        self.require_successful_prewarm = required;
        self
    }
}

/// Bounded startup evidence exported by each client opened with a production
/// cache profile.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ClientStartupMetrics {
    pub total_open_millis: u64,
    pub index_catchup_millis: u64,
    pub prewarm_millis: u64,
    pub prewarm_timed_out: bool,
    pub prewarm_failed: bool,
    pub prewarm_report: Option<NodeCachePrewarmReport>,
    pub cache_activity: NodeCacheSnapshot,
    pub provider_activity: S3OperationMetrics,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProviderDeployment {
    AwsGeneralPurpose,
    RustFs,
    OtherS3Compatible,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SupportStatus {
    /// Suitable for a controlled pilot when the listed release gates pass.
    ControlledPilot,
    /// The architecture supports the size, but provider-specific evidence is
    /// required before production promotion.
    QualificationRequired,
    /// Current measured performance does not support a production claim.
    PerformanceGateFailed,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SupportedEnvelope {
    pub provider: ProviderDeployment,
    pub object_count: u64,
    pub status: SupportStatus,
    pub persistent_cache_required: bool,
    pub aws_qualification_required: bool,
    pub maintenance_scale_gate_required: bool,
    pub required_release_gates: Vec<&'static str>,
}

impl SupportedEnvelope {
    /// Return the explicit support posture represented by the published 2026
    /// qualification evidence. This deliberately does not turn architectural
    /// capacity into an unmeasured production claim.
    pub fn for_deployment(provider: ProviderDeployment, object_count: u64) -> Self {
        let object_count = object_count.max(1);
        let aws_qualification_required = provider == ProviderDeployment::AwsGeneralPurpose;
        let maintenance_scale_gate_required = object_count > 100_000;
        let status = if object_count >= 1_000_000 {
            SupportStatus::PerformanceGateFailed
        } else if aws_qualification_required || object_count > 100_000 {
            SupportStatus::QualificationRequired
        } else {
            SupportStatus::ControlledPilot
        };
        let mut required_release_gates = vec![
            "provider capability attestation",
            "cold and warm read/list SLOs",
            "authority takeover and restart fault injection",
            "fsck and GC recovery drill",
        ];
        if aws_qualification_required {
            required_release_gates.push("AWS lifecycle, replication, throttling, and cost matrix");
        }
        if maintenance_scale_gate_required {
            required_release_gates.push("cardinality-matched fsck and GC timing");
        }
        Self {
            provider,
            object_count,
            status,
            persistent_cache_required: object_count >= 100_000,
            aws_qualification_required,
            maintenance_scale_gate_required,
            required_release_gates,
        }
    }
}

/// Metadata-tree geometry for newly initialized production repositories.
///
/// Logical payloads remain one complete immutable object. This only bounds
/// canonical Prolly metadata nodes so exact node-pack range reads remain
/// sub-megabyte even for wide logical records.
pub fn production_metadata_tree_format() -> TreeFormat {
    TreeFormat {
        chunking: ChunkingSpec {
            measure: ChunkMeasure::EncodedBytes,
            input: BoundaryInput::Key,
            hash: HashAlgorithm::XxHash64,
            rule: BoundaryRule::HashThreshold { factor: 32 * 1024 },
            min: 8 * 1024,
            target: 32 * 1024,
            max: 64 * 1024,
            hash_seed: 0x243f_6a88_85a3_08d3,
            level_salt: true,
            hard_max_node_bytes: 256 * 1024,
        },
        node_layout: NodeLayoutSpec::PrefixCompressed,
        value_encoding: prolly_s3_core::Encoding::Raw,
    }
}

#[cfg(any(feature = "foyer-cache", test))]
pub(crate) fn cache_block_size_for_tree(recommended: usize, format: &TreeFormat) -> usize {
    let hard_max = usize::try_from(format.chunking.hard_max_node_bytes).unwrap_or(usize::MAX);
    // Leave room for Foyer's aligned block index and entry/key envelopes.
    hard_max
        .saturating_add(8 * 1024)
        .checked_next_power_of_two()
        .unwrap_or(usize::MAX)
        .max(recommended)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recommendations_scale_monotonically_through_one_million_objects() {
        let small = CacheSizingRecommendation::for_object_count(100_000);
        let medium = CacheSizingRecommendation::for_object_count(500_000);
        let large = CacheSizingRecommendation::for_object_count(1_000_000);
        assert!(small.memory_capacity_bytes < medium.memory_capacity_bytes);
        assert!(medium.memory_capacity_bytes < large.memory_capacity_bytes);
        assert!(small.disk_capacity_bytes < medium.disk_capacity_bytes);
        assert!(medium.disk_capacity_bytes < large.disk_capacity_bytes);
        assert!(small.max_cached_node_locations < large.max_cached_node_locations);
    }

    #[test]
    fn production_metadata_nodes_are_bounded_for_range_reads() {
        let format = production_metadata_tree_format();
        format.validate().unwrap();
        assert_eq!(format.chunking.measure, ChunkMeasure::EncodedBytes);
        assert_eq!(format.chunking.hard_max_node_bytes, 256 * 1024);
        assert!(format.chunking.max <= format.chunking.hard_max_node_bytes);
    }

    #[test]
    fn legacy_wide_tree_formats_receive_a_large_enough_foyer_block() {
        let format = TreeFormat::default();
        assert_eq!(
            cache_block_size_for_tree(1024 * 1024, &format),
            32 * 1024 * 1024
        );
    }

    #[test]
    fn support_envelope_never_promotes_unqualified_aws_or_million_plus_scale() {
        assert_eq!(
            SupportedEnvelope::for_deployment(ProviderDeployment::AwsGeneralPurpose, 100_000)
                .status,
            SupportStatus::QualificationRequired
        );
        assert_eq!(
            SupportedEnvelope::for_deployment(ProviderDeployment::RustFs, 1_000_001).status,
            SupportStatus::PerformanceGateFailed
        );
    }
}
