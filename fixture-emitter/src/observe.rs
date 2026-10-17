//! Decode received OTLP requests back into fixture records, so the same
//! expected-contents computation can be applied to what the wire carried.

use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value};
use opentelemetry_proto::tonic::metrics::v1::{metric, number_data_point};

use crate::model::{Ctx, LogRec, MetricRec, Notes, SpanEventRec, SpanKind, SpanRec, Status};
use crate::receiver::Received;
use crate::value::{Attrs, Val};

pub struct Observed {
    pub logs: Vec<LogRec>,
    pub spans: Vec<SpanRec>,
    pub metrics: Vec<MetricRec>,
}

fn val(v: &AnyValue) -> Val {
    match &v.value {
        None => Val::Empty,
        Some(any_value::Value::StringValue(s)) => Val::Str(s.clone()),
        Some(any_value::Value::BoolValue(b)) => Val::Bool(*b),
        Some(any_value::Value::IntValue(i)) => Val::Int(*i),
        Some(any_value::Value::DoubleValue(d)) => Val::Double(*d),
        Some(any_value::Value::BytesValue(y)) => Val::Bytes(y.clone()),
        Some(any_value::Value::ArrayValue(a)) => Val::Array(a.values.iter().map(val).collect()),
        Some(any_value::Value::KvlistValue(m)) => Val::Map(attrs(&m.values)),
        // Profiling-only reference; treated as absent by receivers of other signals.
        Some(any_value::Value::StringValueStrindex(_)) => Val::Empty,
    }
}

fn attrs(kvs: &[KeyValue]) -> Attrs {
    kvs.iter()
        .map(|kv| {
            (
                kv.key.clone(),
                kv.value.as_ref().map(val).unwrap_or(Val::Empty),
            )
        })
        .collect()
}

fn id<const N: usize>(bytes: &[u8]) -> Option<[u8; N]> {
    let id: [u8; N] = bytes.try_into().ok()?;
    id.iter().any(|b| *b != 0).then_some(id)
}

fn static_severity(text: &str) -> &'static str {
    match text {
        "INFO" => "INFO",
        "WARN" => "WARN",
        "ERROR" => "ERROR",
        _ => "OTHER",
    }
}

pub fn observe(received: &Received) -> Result<Observed, String> {
    let mut out = Observed {
        logs: Vec::new(),
        spans: Vec::new(),
        metrics: Vec::new(),
    };
    for request in &received.logs {
        for rl in &request.resource_logs {
            let resource = rl
                .resource
                .as_ref()
                .map(|r| attrs(&r.attributes))
                .unwrap_or_default();
            for sl in &rl.scope_logs {
                let ctx = scope_ctx(&resource, sl.scope.as_ref());
                for lr in &sl.log_records {
                    out.logs.push(LogRec {
                        ctx: ctx.clone(),
                        timestamp_ns: lr.time_unix_nano,
                        observed_ns: lr.observed_time_unix_nano,
                        severity_number: lr.severity_number,
                        severity_text: static_severity(&lr.severity_text),
                        event_name: lr.event_name.clone(),
                        body: lr.body.as_ref().map(val),
                        trace: id::<16>(&lr.trace_id)
                            .map(|t| (t, id::<8>(&lr.span_id).unwrap_or([0; 8]))),
                        attrs: attrs(&lr.attributes),
                        notes: Notes::default(),
                    });
                }
            }
        }
    }
    for request in &received.traces {
        for rs in &request.resource_spans {
            let resource = rs
                .resource
                .as_ref()
                .map(|r| attrs(&r.attributes))
                .unwrap_or_default();
            for ss in &rs.scope_spans {
                let ctx = scope_ctx(&resource, ss.scope.as_ref());
                for sp in &ss.spans {
                    out.spans.push(SpanRec {
                        ctx: ctx.clone(),
                        trace_id: id::<16>(&sp.trace_id).ok_or("span without a trace id")?,
                        span_id: id::<8>(&sp.span_id).ok_or("span without a span id")?,
                        parent_span_id: id::<8>(&sp.parent_span_id),
                        name: sp.name.clone(),
                        kind: match sp.kind {
                            1 => SpanKind::Internal,
                            2 => SpanKind::Server,
                            3 => SpanKind::Client,
                            other => return Err(format!("unexpected span kind {other}")),
                        },
                        start_ns: sp.start_time_unix_nano,
                        end_ns: sp.end_time_unix_nano,
                        status: match sp.status.as_ref().map(|s| s.code).unwrap_or(0) {
                            0 => Status::Unset,
                            1 => Status::Ok,
                            2 => Status::Error,
                            other => return Err(format!("unexpected status code {other}")),
                        },
                        attrs: attrs(&sp.attributes),
                        events: sp
                            .events
                            .iter()
                            .map(|e| SpanEventRec {
                                name: e.name.clone(),
                                time_ns: e.time_unix_nano,
                                attrs: attrs(&e.attributes),
                            })
                            .collect(),
                        notes: Notes::default(),
                    });
                }
            }
        }
    }
    for request in &received.metrics {
        for rm in &request.resource_metrics {
            let resource = rm
                .resource
                .as_ref()
                .map(|r| attrs(&r.attributes))
                .unwrap_or_default();
            for sm in &rm.scope_metrics {
                let ctx = scope_ctx(&resource, sm.scope.as_ref());
                for m in &sm.metrics {
                    let Some(metric::Data::Sum(sum)) = &m.data else {
                        return Err(format!("metric {} is not a sum", m.name));
                    };
                    if !sum.is_monotonic {
                        return Err(format!("metric {} is not monotonic", m.name));
                    }
                    for point in &sum.data_points {
                        let Some(number_data_point::Value::AsInt(value)) = point.value else {
                            return Err(format!("metric {} point is not an integer", m.name));
                        };
                        out.metrics.push(MetricRec {
                            ctx: ctx.clone(),
                            name: m.name.clone(),
                            unit: m.unit.clone(),
                            description: m.description.clone(),
                            value: u64::try_from(value).map_err(|_| "negative counter")?,
                            attrs: attrs(&point.attributes),
                        });
                    }
                }
            }
        }
    }
    Ok(out)
}

fn scope_ctx(
    resource: &Attrs,
    scope: Option<&opentelemetry_proto::tonic::common::v1::InstrumentationScope>,
) -> Ctx {
    Ctx {
        resource: resource.clone(),
        scope_name: scope.map(|s| s.name.clone()).unwrap_or_default(),
        scope_version: scope.map(|s| s.version.clone()).unwrap_or_default(),
        scope_attrs: scope.map(|s| attrs(&s.attributes)).unwrap_or_default(),
    }
}
