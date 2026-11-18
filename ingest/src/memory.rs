//! Conservative mapping charges, including shared attributes copied into
//! every row. Walk the OTLP tree without allocating another representation.
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value};

use crate::pipeline::RECORD_BYTES;

// A node becomes a typed value, BTreeMap entries, a canonical {t,v} JSON
// object, and serialized payload/serving copies. Wire bytes miss these costs
// (the measured attribute-heavy case expanded 181x). Text also needs room for
// JSON escaping, base64, buffer growth and the copies alive during mapping.
const NODE_BYTES: usize = 1024;
const TEXT_COPIES: usize = 32;

fn text(bytes: usize) -> usize {
    bytes.saturating_mul(TEXT_COPIES)
}

fn attributes(values: &[KeyValue]) -> usize {
    entries(values, true)
}

fn entries(values: &[KeyValue], drop_reserved: bool) -> usize {
    values
        .iter()
        .filter(|kv| !drop_reserved || !kv.key.starts_with("toudai."))
        .fold(0usize, |sum, kv| {
            sum.saturating_add(NODE_BYTES)
                .saturating_add(text(kv.key.len()))
                .saturating_add(value(kv.value.as_ref()))
        })
}

fn value(v: Option<&AnyValue>) -> usize {
    use any_value::Value as V;
    NODE_BYTES.saturating_add(match v.and_then(|v| v.value.as_ref()) {
        Some(V::StringValue(s)) => text(s.len()),
        Some(V::BytesValue(b)) => text(b.len()),
        Some(V::ArrayValue(a)) => a
            .values
            .iter()
            .fold(0usize, |n, v| n.saturating_add(value(Some(v)))),
        Some(V::KvlistValue(m)) => entries(&m.values, false),
        _ => 0,
    })
}

// Dropped fields still exist in the decoded request, but never receive the
// mapping multipliers or fan-out. Include their tree overhead once only.
fn decoded_attributes(values: &[KeyValue]) -> usize {
    values.iter().fold(0usize, |n, kv| {
        n.saturating_add(256)
            .saturating_add(kv.key.len())
            .saturating_add(decoded_value(kv.value.as_ref()))
    })
}

fn decoded_value(v: Option<&AnyValue>) -> usize {
    use any_value::Value as V;
    256usize.saturating_add(match v.and_then(|v| v.value.as_ref()) {
        Some(V::StringValue(s)) => s.len(),
        Some(V::BytesValue(b)) => b.len(),
        Some(V::ArrayValue(a)) => a
            .values
            .iter()
            .fold(0usize, |n, v| n.saturating_add(decoded_value(Some(v)))),
        Some(V::KvlistValue(m)) => decoded_attributes(&m.values),
        _ => 0,
    })
}

pub(crate) fn logs(request: &ExportLogsServiceRequest) -> usize {
    request.resource_logs.iter().fold(0usize, |sum, resource| {
        let r = resource
            .resource
            .as_ref()
            .map_or(0, |r| attributes(&r.attributes));
        sum.saturating_add(r)
            .saturating_add(
                resource
                    .resource
                    .as_ref()
                    .map_or(0, |r| decoded_attributes(&r.attributes)),
            )
            .saturating_add(resource.scope_logs.iter().fold(0usize, |sum, scope| {
                let s = scope
                    .scope
                    .as_ref()
                    .map_or(0, |s| attributes(&s.attributes));
                sum.saturating_add(s)
                    .saturating_add(
                        scope
                            .scope
                            .as_ref()
                            .map_or(0, |s| decoded_attributes(&s.attributes)),
                    )
                    .saturating_add(scope.log_records.iter().fold(0usize, |sum, log| {
                        sum.saturating_add(RECORD_BYTES)
                            .saturating_add(r)
                            .saturating_add(s)
                            .saturating_add(attributes(&log.attributes))
                            .saturating_add(decoded_attributes(&log.attributes))
                            .saturating_add(log.body.as_ref().map_or(0, |v| decoded_value(Some(v))))
                            .saturating_add(log.body.as_ref().map_or(0, |v| value(Some(v))))
                            .saturating_add(text(log.severity_text.len()))
                            .saturating_add(text(log.event_name.len()))
                    }))
            }))
    })
}

pub(crate) fn traces(request: &ExportTraceServiceRequest) -> usize {
    request.resource_spans.iter().fold(0usize, |sum, resource| {
        let r = resource
            .resource
            .as_ref()
            .map_or(0, |r| attributes(&r.attributes));
        sum.saturating_add(r)
            .saturating_add(
                resource
                    .resource
                    .as_ref()
                    .map_or(0, |r| decoded_attributes(&r.attributes)),
            )
            .saturating_add(resource.scope_spans.iter().fold(0usize, |sum, scope| {
                let s = scope
                    .scope
                    .as_ref()
                    .map_or(0, |s| attributes(&s.attributes));
                sum.saturating_add(s)
                    .saturating_add(
                        scope
                            .scope
                            .as_ref()
                            .map_or(0, |s| decoded_attributes(&s.attributes)),
                    )
                    .saturating_add(scope.spans.iter().fold(0usize, |sum, span| {
                        let events = span.events.iter().fold(0usize, |sum, event| {
                            sum.saturating_add(RECORD_BYTES)
                                .saturating_add(text(event.name.len()))
                                .saturating_add(attributes(&event.attributes))
                                .saturating_add(decoded_attributes(&event.attributes))
                        });
                        sum.saturating_add(RECORD_BYTES)
                            .saturating_add(r)
                            .saturating_add(s)
                            .saturating_add(attributes(&span.attributes))
                            .saturating_add(decoded_attributes(&span.attributes))
                            .saturating_add(events)
                            .saturating_add(text(span.name.len()))
                            .saturating_add(span.status.as_ref().map_or(0, |s| s.message.len()))
                            // Links are decoded but not mapped; still charge their values.
                            .saturating_add(span.links.iter().fold(0usize, |n, l| {
                                n.saturating_add(256)
                                    .saturating_add(decoded_attributes(&l.attributes))
                            }))
                    }))
            }))
    })
}
