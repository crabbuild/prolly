//! AWS SDK-shaped adapter for [`prolly_s3_core`].

mod advisory;
mod aws_object;
#[cfg(feature = "foyer-cache")]
mod cache;
mod client;
mod provider;
mod wire_metrics;

pub use advisory::*;
pub use aws_object::{AwsS3ObjectPlane, S3OperationMetrics};
#[cfg(feature = "foyer-cache")]
pub use cache::{FoyerNodeCache, FoyerNodeCacheConfig};
pub use client::*;
pub use prolly_s3_core as core;
pub use prolly_s3_core::{Error, ErrorCode, Result};
pub use provider::*;
pub use wire_metrics::*;
