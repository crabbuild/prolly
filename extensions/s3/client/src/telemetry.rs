#[cfg(feature = "opentelemetry")]
use std::sync::Arc;

use prolly_s3_core::NodeCacheSnapshot;

use crate::{ClientStartupMetrics, S3OperationMetrics};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClientTelemetryContext {
    pub repository_id: String,
    pub provider: String,
    pub expected_objects: Option<u64>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ClientTelemetryInterval {
    pub cache: NodeCacheSnapshot,
    pub provider: S3OperationMetrics,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ClientPerformanceSnapshot {
    pub cache: NodeCacheSnapshot,
    pub provider: S3OperationMetrics,
}

impl ClientPerformanceSnapshot {
    pub fn delta_since(self, earlier: Self) -> Self {
        Self {
            cache: self.cache.delta_since(earlier.cache),
            provider: self.provider.delta_since(earlier.provider),
        }
    }

    /// Provider response bytes per canonical metadata-node byte returned.
    /// Measure around metadata-only operations (list/diff/merge planning) so
    /// logical payload downloads do not enter the numerator.
    pub fn metadata_download_amplification(self) -> f64 {
        if self.cache.requested_bytes == 0 {
            0.0
        } else {
            self.provider.downloaded_body_bytes as f64 / self.cache.requested_bytes as f64
        }
    }
}

/// Application-owned telemetry target. Implementations must return quickly;
/// collection runs on a lightweight periodic maintenance task.
pub trait ClientTelemetry: Send + Sync + 'static {
    fn record_startup(&self, context: &ClientTelemetryContext, startup: ClientStartupMetrics);
    fn record_interval(&self, context: &ClientTelemetryContext, interval: ClientTelemetryInterval);
}

pub(crate) struct ClientTelemetryMaintenance {
    task: tokio::task::JoinHandle<()>,
}

impl ClientTelemetryMaintenance {
    pub(crate) fn new(task: tokio::task::JoinHandle<()>) -> Self {
        Self { task }
    }
}

impl Drop for ClientTelemetryMaintenance {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[cfg(feature = "opentelemetry")]
pub struct OpenTelemetryClientMetrics {
    cache_lookups: opentelemetry::metrics::Counter<u64>,
    metadata_bytes: opentelemetry::metrics::Counter<u64>,
    cache_events: opentelemetry::metrics::Counter<u64>,
    provider_operations: opentelemetry::metrics::Counter<u64>,
    provider_bytes: opentelemetry::metrics::Counter<u64>,
    startup_duration: opentelemetry::metrics::Histogram<u64>,
}

#[cfg(feature = "opentelemetry")]
impl OpenTelemetryClientMetrics {
    pub fn new(meter: opentelemetry::metrics::Meter) -> Arc<Self> {
        Arc::new(Self {
            cache_lookups: meter
                .u64_counter("prolly.s3.metadata.cache.lookups")
                .build(),
            metadata_bytes: meter.u64_counter("prolly.s3.metadata.bytes").build(),
            cache_events: meter.u64_counter("prolly.s3.metadata.cache.events").build(),
            provider_operations: meter.u64_counter("prolly.s3.provider.operations").build(),
            provider_bytes: meter.u64_counter("prolly.s3.provider.bytes").build(),
            startup_duration: meter
                .u64_histogram("prolly.s3.client.startup.duration")
                .build(),
        })
    }

    fn attributes(
        context: &ClientTelemetryContext,
        name: &'static str,
        value: &'static str,
    ) -> Vec<opentelemetry::KeyValue> {
        let mut attributes = vec![
            opentelemetry::KeyValue::new("prolly.repository.id", context.repository_id.clone()),
            opentelemetry::KeyValue::new("prolly.provider", context.provider.clone()),
            opentelemetry::KeyValue::new(name, value),
        ];
        if let Some(objects) = context.expected_objects {
            attributes.push(opentelemetry::KeyValue::new(
                "prolly.repository.expected_objects",
                i64::try_from(objects).unwrap_or(i64::MAX),
            ));
        }
        attributes
    }

    fn add_event(&self, context: &ClientTelemetryContext, event: &'static str, value: u64) {
        if value > 0 {
            self.cache_events.add(
                value,
                &Self::attributes(context, "prolly.cache.event", event),
            );
        }
    }
}

#[cfg(feature = "opentelemetry")]
impl ClientTelemetry for OpenTelemetryClientMetrics {
    fn record_startup(&self, context: &ClientTelemetryContext, startup: ClientStartupMetrics) {
        for (phase, duration) in [
            ("total", startup.total_open_millis),
            ("index_catchup", startup.index_catchup_millis),
            ("prewarm", startup.prewarm_millis),
        ] {
            self.startup_duration.record(
                duration,
                &Self::attributes(context, "prolly.startup.phase", phase),
            );
        }
        self.record_interval(
            context,
            ClientTelemetryInterval {
                cache: startup.cache_activity,
                provider: startup.provider_activity,
            },
        );
        self.add_event(
            context,
            "prewarm_timeout",
            u64::from(startup.prewarm_timed_out),
        );
        self.add_event(
            context,
            "prewarm_failure",
            u64::from(startup.prewarm_failed),
        );
    }

    fn record_interval(&self, context: &ClientTelemetryContext, interval: ClientTelemetryInterval) {
        for (outcome, value) in [
            ("hit", interval.cache.hits),
            ("miss", interval.cache.misses),
        ] {
            if value > 0 {
                self.cache_lookups.add(
                    value,
                    &Self::attributes(context, "prolly.cache.outcome", outcome),
                );
            }
        }
        for (kind, value) in [
            ("requested", interval.cache.requested_bytes),
            ("provider_fetched", interval.cache.fetched_bytes),
            ("cache_avoided", interval.cache.avoided_bytes),
            ("pinned", interval.cache.pinned_bytes),
        ] {
            if value > 0 {
                self.metadata_bytes.add(
                    value,
                    &Self::attributes(context, "prolly.metadata.bytes.kind", kind),
                );
            }
        }
        for (event, value) in [
            ("admission_rejection", interval.cache.admission_rejections),
            ("cache_error", interval.cache.errors),
            ("cache_corruption", interval.cache.corruptions),
            ("coalesced_wait", interval.cache.coalesced_waits),
            ("prefetch_batch", interval.cache.prefetch_batches),
            ("prefetched_node", interval.cache.prefetched_nodes),
            ("pinned_node", interval.cache.pinned_nodes),
        ] {
            self.add_event(context, event, value);
        }
        for (operation, value) in [
            ("get_object", interval.provider.get_object),
            ("head_object", interval.provider.head_object),
            ("put_object", interval.provider.put_object),
            ("list_objects_v2", interval.provider.list_objects_v2),
            (
                "list_object_versions",
                interval.provider.list_object_versions,
            ),
            ("delete_object", interval.provider.delete_object),
            ("delete_objects", interval.provider.delete_objects),
        ] {
            if value > 0 {
                self.provider_operations.add(
                    value,
                    &Self::attributes(context, "prolly.provider.operation", operation),
                );
            }
        }
        for (direction, value) in [
            ("uploaded", interval.provider.uploaded_body_bytes),
            ("downloaded", interval.provider.downloaded_body_bytes),
        ] {
            if value > 0 {
                self.provider_bytes.add(
                    value,
                    &Self::attributes(context, "prolly.provider.direction", direction),
                );
            }
        }
    }
}

#[cfg(all(test, feature = "opentelemetry"))]
mod tests {
    use super::*;

    #[test]
    fn opentelemetry_sink_accepts_startup_and_interval_metrics() {
        let sink = OpenTelemetryClientMetrics::new(opentelemetry::global::meter("test-prolly-s3"));
        let context = ClientTelemetryContext {
            repository_id: "pr_test".to_string(),
            provider: "s3-compatible".to_string(),
            expected_objects: Some(100_000),
        };
        sink.record_startup(
            &context,
            ClientStartupMetrics {
                total_open_millis: 20,
                index_catchup_millis: 5,
                prewarm_millis: 10,
                cache_activity: NodeCacheSnapshot {
                    hits: 2,
                    misses: 1,
                    requested_bytes: 300,
                    fetched_bytes: 100,
                    avoided_bytes: 200,
                    ..NodeCacheSnapshot::default()
                },
                provider_activity: S3OperationMetrics {
                    get_object: 1,
                    downloaded_body_bytes: 100,
                    ..S3OperationMetrics::default()
                },
                ..ClientStartupMetrics::default()
            },
        );
        sink.record_interval(
            &context,
            ClientTelemetryInterval {
                cache: NodeCacheSnapshot {
                    admission_rejections: 1,
                    prefetch_batches: 1,
                    prefetched_nodes: 4,
                    ..NodeCacheSnapshot::default()
                },
                provider: S3OperationMetrics {
                    get_object: 2,
                    downloaded_body_bytes: 128,
                    ..S3OperationMetrics::default()
                },
            },
        );
    }
}
