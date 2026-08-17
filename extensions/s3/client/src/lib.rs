//! AWS SDK-shaped adapter for [`prolly_s3_core`].

mod aws_object;
#[cfg(feature = "foyer-cache")]
mod cache;
mod client;
mod production;
mod provider;
mod telemetry;
mod wire_metrics;

pub use aws_object::{AwsS3ObjectPlane, S3OperationMetrics};
#[cfg(feature = "foyer-cache")]
pub use cache::{FoyerNodeCache, FoyerNodeCacheConfig};
pub use client::*;
pub use production::{
    production_metadata_tree_format, CacheSizingRecommendation, ClientStartupMetrics,
    ProductionCacheProfile, ProviderDeployment, SupportStatus, SupportedEnvelope,
};
pub use prolly_s3_core as core;
pub use prolly_s3_core::{Error, ErrorCode, Result};
pub use provider::*;
#[cfg(feature = "opentelemetry")]
pub use telemetry::OpenTelemetryClientMetrics;
pub use telemetry::{
    ClientPerformanceSnapshot, ClientTelemetry, ClientTelemetryContext, ClientTelemetryInterval,
};
pub use wire_metrics::*;
