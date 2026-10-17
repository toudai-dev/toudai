//! Delivery accounting for the SDK's batch exporters.
//!
//! The SDK's batch processors log export failures and still return success from
//! `shutdown`, so a run that the destination rejected would look clean. Each
//! exporter is wrapped to count the items whose export call returned success
//! and the ones that failed, and `emit` compares the totals with what the model
//! sent. Success here means the destination answered the OTLP export request
//! with success: acceptance, not confirmation that anything was stored.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use opentelemetry_otlp::{LogExporter, MetricExporter, SpanExporter};
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::error::OTelSdkResult;
use opentelemetry_sdk::logs::{LogBatch, LogExporter as SdkLogExporter};
use opentelemetry_sdk::metrics::Temporality;
use opentelemetry_sdk::metrics::data::ResourceMetrics;
use opentelemetry_sdk::metrics::exporter::PushMetricExporter;
use opentelemetry_sdk::trace::{SpanData, SpanExporter as SdkSpanExporter};

#[derive(Debug, Default)]
pub struct Tally {
    exported: AtomicUsize,
    failed: AtomicUsize,
    errors: Mutex<Vec<String>>,
}

impl Tally {
    fn record(&self, items: usize, result: &OTelSdkResult) {
        match result {
            Ok(()) => {
                self.exported.fetch_add(items, Ordering::SeqCst);
            }
            Err(e) => {
                self.failed.fetch_add(items, Ordering::SeqCst);
                self.errors.lock().unwrap().push(e.to_string());
            }
        }
    }

    /// `Ok` only when exactly `expected` items were exported and none failed.
    pub fn verify(&self, signal: &str, expected: usize) -> Result<(), String> {
        let (exported, failed) = (
            self.exported.load(Ordering::SeqCst),
            self.failed.load(Ordering::SeqCst),
        );
        if failed == 0 && exported == expected {
            return Ok(());
        }
        let errors = self.errors.lock().unwrap();
        Err(format!(
            "{signal}: {exported} of {expected} items exported, {failed} failed ({})",
            errors.first().map_or("no export error", String::as_str)
        ))
    }
}

#[derive(Debug)]
pub struct CountedSpans(pub SpanExporter, pub Arc<Tally>);

impl SdkSpanExporter for CountedSpans {
    async fn export(&self, batch: Vec<SpanData>) -> OTelSdkResult {
        let items = batch.len();
        let result = self.0.export(batch).await;
        self.1.record(items, &result);
        result
    }

    fn shutdown_with_timeout(&self, timeout: Duration) -> OTelSdkResult {
        self.0.shutdown_with_timeout(timeout)
    }

    fn force_flush(&self) -> OTelSdkResult {
        self.0.force_flush()
    }

    fn set_resource(&mut self, resource: &Resource) {
        self.0.set_resource(resource);
    }
}

#[derive(Debug)]
pub struct CountedLogs(pub LogExporter, pub Arc<Tally>);

impl SdkLogExporter for CountedLogs {
    async fn export(&self, batch: LogBatch<'_>) -> OTelSdkResult {
        let items = batch.iter().count();
        let result = self.0.export(batch).await;
        self.1.record(items, &result);
        result
    }

    fn shutdown_with_timeout(&self, timeout: Duration) -> OTelSdkResult {
        self.0.shutdown_with_timeout(timeout)
    }

    fn set_resource(&mut self, resource: &Resource) {
        self.0.set_resource(resource);
    }
}

/// Metrics are counted per export call: one call carries all the data points.
#[derive(Debug)]
pub struct CountedMetrics(pub MetricExporter, pub Arc<Tally>);

impl PushMetricExporter for CountedMetrics {
    async fn export(&self, metrics: &ResourceMetrics) -> OTelSdkResult {
        let result = self.0.export(metrics).await;
        self.1.record(1, &result);
        result
    }

    fn force_flush(&self) -> OTelSdkResult {
        self.0.force_flush()
    }

    fn shutdown_with_timeout(&self, timeout: Duration) -> OTelSdkResult {
        self.0.shutdown_with_timeout(timeout)
    }

    fn temporality(&self) -> Temporality {
        self.0.temporality()
    }
}
