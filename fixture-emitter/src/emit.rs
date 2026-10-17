//! Emission through the standard OpenTelemetry Rust SDK and OTLP exporters.
//!
//! The model fixes every timestamp and identifier, so the only SDK behavior
//! left to configure is transport: the same fixture goes out over OTLP/gRPC or
//! OTLP/HTTP (protobuf) through the SDK's batch processors and `opentelemetry-otlp`.

use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use opentelemetry::logs::{AnyValue, LogRecord as _, Logger as _, LoggerProvider as _, Severity};
use opentelemetry::metrics::MeterProvider as _;
use opentelemetry::trace::{
    Span as _, SpanContext, SpanId, SpanKind as OtelSpanKind, Status as OtelStatus,
    TraceContextExt, TraceFlags, TraceId, TraceState, Tracer as _, TracerProvider as _,
};
use opentelemetry::{Array, Context, InstrumentationScope, Key, KeyValue, StringValue, Value};
use opentelemetry_otlp::{LogExporter, MetricExporter, Protocol, SpanExporter, WithExportConfig};
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::logs::SdkLoggerProvider;
use opentelemetry_sdk::metrics::SdkMeterProvider;
use opentelemetry_sdk::trace::{IdGenerator, SdkTracerProvider};

use crate::model::{Ctx, Fixture, SpanKind, SpanRec, Status, WORKSPACE_ATTR};
use crate::value::{Attrs, Val};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Transport {
    Grpc,
    Http,
}

impl Transport {
    pub fn parse(text: &str) -> Result<Self, String> {
        match text {
            "grpc" => Ok(Transport::Grpc),
            "http" => Ok(Transport::Http),
            other => Err(format!("unknown protocol {other:?}; use grpc or http")),
        }
    }

    pub fn default_endpoint(self) -> &'static str {
        match self {
            Transport::Grpc => "http://127.0.0.1:4317",
            Transport::Http => "http://127.0.0.1:4318",
        }
    }
}

fn time(ns: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_nanos(ns)
}

/// A span `Value` for an attribute. The SDK's span value type has no bytes,
/// map, or empty kind and only homogeneous arrays; the model never asks for
/// them on spans, so a mismatch is a fixture bug.
fn to_value(v: &Val) -> Result<Value, String> {
    Ok(match v {
        Val::Str(s) => Value::String(StringValue::from(s.clone())),
        Val::Int(i) => Value::I64(*i),
        Val::Double(d) => Value::F64(*d),
        Val::Bool(b) => Value::Bool(*b),
        Val::Array(items) => {
            if items.iter().all(|i| matches!(i, Val::Str(_))) {
                Value::Array(Array::String(
                    items
                        .iter()
                        .map(|i| match i {
                            Val::Str(s) => StringValue::from(s.clone()),
                            _ => unreachable!(),
                        })
                        .collect(),
                ))
            } else if items.iter().all(|i| matches!(i, Val::Int(_))) {
                Value::Array(Array::I64(
                    items
                        .iter()
                        .map(|i| {
                            if let Val::Int(n) = i {
                                *n
                            } else {
                                unreachable!()
                            }
                        })
                        .collect(),
                ))
            } else {
                return Err("span arrays must be homogeneous strings or integers".into());
            }
        }
        other => {
            return Err(format!(
                "the SDK span value type cannot carry {}",
                other.kind()
            ));
        }
    })
}

fn to_any(v: &Val) -> Result<AnyValue, String> {
    Ok(match v {
        Val::Str(s) => AnyValue::String(StringValue::from(s.clone())),
        Val::Int(i) => AnyValue::Int(*i),
        Val::Double(d) => AnyValue::Double(*d),
        Val::Bool(b) => AnyValue::Boolean(*b),
        Val::Bytes(y) => AnyValue::Bytes(Box::new(y.clone())),
        Val::Array(items) => AnyValue::ListAny(Box::new(
            items.iter().map(to_any).collect::<Result<_, _>>()?,
        )),
        Val::Map(items) => AnyValue::Map(Box::new(
            items
                .iter()
                .map(|(k, v)| Ok((Key::from(k.clone()), to_any(v)?)))
                .collect::<Result<_, String>>()?,
        )),
        Val::Empty => return Err("the SDK log value type cannot carry an empty value".into()),
    })
}

fn key_values(attrs: &Attrs) -> Result<Vec<KeyValue>, String> {
    attrs
        .iter()
        .map(|(k, v)| Ok(KeyValue::new(k.clone(), to_value(v)?)))
        .collect()
}

fn resource(ctx: &Ctx) -> Result<Resource, String> {
    // Empty builder: the default detectors would add telemetry.sdk.* and host
    // attributes, which vary by build and machine.
    Ok(Resource::builder_empty()
        .with_attributes(key_values(&ctx.resource)?)
        .build())
}

fn scope(ctx: &Ctx) -> Result<InstrumentationScope, String> {
    Ok(InstrumentationScope::builder(ctx.scope_name.clone())
        .with_version(ctx.scope_version.clone())
        .with_attributes(key_values(&ctx.scope_attrs)?)
        .build())
}

fn severity(number: i32) -> Result<Severity, String> {
    match number {
        9 => Ok(Severity::Info),
        13 => Ok(Severity::Warn),
        17 => Ok(Severity::Error),
        other => Err(format!("fixture severity {other} has no mapping")),
    }
}

/// Hands the SDK the model's identifiers in the order it asks for them.
#[derive(Debug, Default)]
struct Queued {
    traces: Mutex<VecDeque<TraceId>>,
    spans: Mutex<VecDeque<SpanId>>,
}

#[derive(Debug)]
struct QueuedIds(Arc<Queued>);

impl IdGenerator for QueuedIds {
    fn new_trace_id(&self) -> TraceId {
        self.0
            .traces
            .lock()
            .unwrap()
            .pop_front()
            .expect("a root span was started without a queued trace id")
    }

    fn new_span_id(&self) -> SpanId {
        self.0
            .spans
            .lock()
            .unwrap()
            .pop_front()
            .expect("a span was started without a queued span id")
    }
}

fn log_exporter(protocol: Transport, endpoint: &str) -> Result<LogExporter, String> {
    match protocol {
        Transport::Grpc => LogExporter::builder()
            .with_tonic()
            .with_endpoint(endpoint)
            .build(),
        Transport::Http => LogExporter::builder()
            .with_http()
            .with_protocol(Protocol::HttpBinary)
            .with_endpoint(format!("{endpoint}/v1/logs"))
            .build(),
    }
    .map_err(|e| e.to_string())
}

fn span_exporter(protocol: Transport, endpoint: &str) -> Result<SpanExporter, String> {
    match protocol {
        Transport::Grpc => SpanExporter::builder()
            .with_tonic()
            .with_endpoint(endpoint)
            .build(),
        Transport::Http => SpanExporter::builder()
            .with_http()
            .with_protocol(Protocol::HttpBinary)
            .with_endpoint(format!("{endpoint}/v1/traces"))
            .build(),
    }
    .map_err(|e| e.to_string())
}

fn metric_exporter(protocol: Transport, endpoint: &str) -> Result<MetricExporter, String> {
    match protocol {
        Transport::Grpc => MetricExporter::builder()
            .with_tonic()
            .with_endpoint(endpoint)
            .build(),
        Transport::Http => MetricExporter::builder()
            .with_http()
            .with_protocol(Protocol::HttpBinary)
            .with_endpoint(format!("{endpoint}/v1/metrics"))
            .build(),
    }
    .map_err(|e| e.to_string())
}

fn workspace_of(ctx: &Ctx) -> String {
    match ctx.resource.iter().find(|(k, _)| k == WORKSPACE_ATTR) {
        Some((_, Val::Str(s))) => s.clone(),
        _ => String::new(),
    }
}

/// Send the whole fixture. Each tenant gets its own providers and resource, as
/// separate producers (or a gateway stamping each tenant's scope) would.
pub fn emit(fixture: &Fixture, protocol: Transport, endpoint: &str) -> Result<(), String> {
    let endpoint = endpoint.trim_end_matches('/');
    let mut workspaces: Vec<String> = Vec::new();
    for ctx in fixture
        .logs
        .iter()
        .map(|r| &r.ctx)
        .chain(fixture.spans.iter().map(|r| &r.ctx))
        .chain(fixture.metrics.iter().map(|r| &r.ctx))
    {
        let w = workspace_of(ctx);
        if !workspaces.contains(&w) {
            workspaces.push(w);
        }
    }
    for workspace in &workspaces {
        emit_tenant(fixture, workspace, protocol, endpoint)?;
    }
    Ok(())
}

fn emit_tenant(
    fixture: &Fixture,
    workspace: &str,
    protocol: Transport,
    endpoint: &str,
) -> Result<(), String> {
    let logs: Vec<_> = fixture
        .logs
        .iter()
        .filter(|r| workspace_of(&r.ctx) == workspace)
        .collect();
    let spans: Vec<_> = fixture
        .spans
        .iter()
        .filter(|r| workspace_of(&r.ctx) == workspace)
        .collect();
    let metrics: Vec<_> = fixture
        .metrics
        .iter()
        .filter(|r| workspace_of(&r.ctx) == workspace)
        .collect();

    // Traces first: log records carry the trace context of spans the model
    // already identified, so no ordering between the signals is required.
    let mut errors = Vec::new();
    if let Some(first) = spans.first() {
        let queued = Arc::new(Queued::default());
        let provider = SdkTracerProvider::builder()
            .with_id_generator(QueuedIds(queued.clone()))
            .with_resource(resource(&first.ctx)?)
            .with_batch_exporter(span_exporter(protocol, endpoint)?)
            .build();
        for rec in &spans {
            emit_span(&provider, &queued, rec)?;
        }
        if let Err(e) = provider.shutdown() {
            errors.push(format!("trace shutdown: {e}"));
        }
    }

    if let Some(first) = logs.first() {
        let provider = SdkLoggerProvider::builder()
            .with_resource(resource(&first.ctx)?)
            .with_batch_exporter(log_exporter(protocol, endpoint)?)
            .build();
        let mut loggers = BTreeMap::new();
        for rec in &logs {
            if !loggers.contains_key(&rec.ctx.scope_name) {
                loggers.insert(
                    rec.ctx.scope_name.clone(),
                    provider.logger_with_scope(scope(&rec.ctx)?),
                );
            }
            let logger = &loggers[&rec.ctx.scope_name];
            let mut record = logger.create_log_record();
            record.set_timestamp(time(rec.timestamp_ns));
            record.set_observed_timestamp(time(rec.observed_ns));
            record.set_severity_number(severity(rec.severity_number)?);
            record.set_severity_text(rec.severity_text);
            if !rec.event_name.is_empty() {
                // Static by API; the fixture is small and short-lived.
                record.set_event_name(Box::leak(rec.event_name.clone().into_boxed_str()));
            }
            if let Some(body) = &rec.body {
                record.set_body(to_any(body)?);
            }
            if let Some((trace_id, span_id)) = &rec.trace {
                record.set_trace_context(
                    TraceId::from_bytes(*trace_id),
                    SpanId::from_bytes(*span_id),
                    Some(TraceFlags::SAMPLED),
                );
            }
            for (k, v) in &rec.attrs {
                record.add_attribute(Key::from(k.clone()), to_any(v)?);
            }
            logger.emit(record);
        }
        if let Err(e) = provider.shutdown() {
            errors.push(format!("log shutdown: {e}"));
        }
    }

    if let Some(first) = metrics.first() {
        let provider = SdkMeterProvider::builder()
            .with_resource(resource(&first.ctx)?)
            .with_periodic_exporter(metric_exporter(protocol, endpoint)?)
            .build();
        for rec in &metrics {
            let meter = provider.meter_with_scope(scope(&rec.ctx)?);
            let counter = meter
                .u64_counter(rec.name.clone())
                .with_unit(rec.unit.clone())
                .with_description(rec.description.clone())
                .build();
            counter.add(rec.value, &key_values(&rec.attrs)?);
        }
        // The periodic reader's interval is far longer than this run; shutdown
        // performs the one export.
        if let Err(e) = provider.shutdown() {
            errors.push(format!("metric shutdown: {e}"));
        }
    }

    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors.join("; "))
    }
}

fn emit_span(provider: &SdkTracerProvider, queued: &Queued, rec: &SpanRec) -> Result<(), String> {
    let tracer = provider.tracer_with_scope(scope(&rec.ctx)?);
    queued
        .spans
        .lock()
        .unwrap()
        .push_back(SpanId::from_bytes(rec.span_id));
    let trace_id = TraceId::from_bytes(rec.trace_id);
    let parent_cx = match rec.parent_span_id {
        Some(parent) => Context::new().with_remote_span_context(SpanContext::new(
            trace_id,
            SpanId::from_bytes(parent),
            TraceFlags::SAMPLED,
            true,
            TraceState::default(),
        )),
        None => {
            queued.traces.lock().unwrap().push_back(trace_id);
            Context::new()
        }
    };
    let kind = match rec.kind {
        SpanKind::Internal => OtelSpanKind::Internal,
        SpanKind::Server => OtelSpanKind::Server,
        SpanKind::Client => OtelSpanKind::Client,
    };
    let builder = tracer
        .span_builder(rec.name.clone())
        .with_kind(kind)
        .with_start_time(time(rec.start_ns))
        .with_attributes(key_values(&rec.attrs)?);
    let mut span = tracer.build_with_context(builder, &parent_cx);
    for event in &rec.events {
        span.add_event_with_timestamp(
            event.name.clone(),
            time(event.time_ns),
            key_values(&event.attrs)?,
        );
    }
    match rec.status {
        Status::Unset => {}
        Status::Ok => span.set_status(OtelStatus::Ok),
        Status::Error => span.set_status(OtelStatus::error("")),
    }
    // An end of 0 is the OTLP encoding of "unset"; the SDK passes it through.
    span.end_with_timestamp(time(rec.end_ns));
    Ok(())
}
