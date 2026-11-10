//! Request checks and `event_id` assignment (Decision 0002 D3, Decision 0001
//! D5).
//!
//! Every resource is checked before any record, and identifiers are assigned
//! only once every check has passed, so a refused request is never partly
//! stamped. The request itself is not modified: the dataset and the
//! `event_id`s are typed values handed to the mapping, not attributes written
//! into the request for a later stage to read back.

use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::{KeyValue, any_value};
use opentelemetry_proto::tonic::resource::v1::Resource;
use uuid::Uuid;

use crate::Rejection;
use crate::auth::Scope;

/// The namespace producers may not set on a resource.
pub const RESERVED_PREFIX: &str = "toudai.";
/// The resource attribute holding the request's dataset. A producer copy is
/// accepted only when it equals the resolved dataset.
pub const DATASET_ATTR: &str = "toudai.dataset";
/// The record and span attribute name of the gateway-assigned identity. A
/// producer copy rejects the whole request.
pub const EVENT_ID_ATTR: &str = "toudai.event_id";

/// Check a logs request and assign one `event_id` per log record, in request
/// order (resource, then scope, then record).
pub fn admit_logs(
    request: &ExportLogsServiceRequest,
    scope: &Scope,
) -> Result<Vec<Uuid>, Rejection> {
    let resources = request.resource_logs.iter().map(|r| r.resource.as_ref());
    let records = request
        .resource_logs
        .iter()
        .flat_map(|r| &r.scope_logs)
        .flat_map(|s| &s.log_records)
        .map(|record| record.attributes.as_slice());
    admit(resources, records, scope)
}

/// Check a traces request and assign one `event_id` per span, in request
/// order (resource, then scope, then span).
pub fn admit_traces(
    request: &ExportTraceServiceRequest,
    scope: &Scope,
) -> Result<Vec<Uuid>, Rejection> {
    let resources = request.resource_spans.iter().map(|r| r.resource.as_ref());
    let spans = request
        .resource_spans
        .iter()
        .flat_map(|r| &r.scope_spans)
        .flat_map(|s| &s.spans)
        .map(|span| span.attributes.as_slice());
    admit(resources, spans, scope)
}

fn admit<'a>(
    resources: impl Iterator<Item = Option<&'a Resource>>,
    records: impl Iterator<Item = &'a [KeyValue]>,
    scope: &Scope,
) -> Result<Vec<Uuid>, Rejection> {
    // Resources first, so a dataset the key may not use is a 403 wherever it
    // sits in the request.
    for resource in resources.flatten() {
        check_resource(&resource.attributes, scope)?;
    }
    let mut count = 0;
    for attributes in records {
        // Identity is the gateway's alone. Rejecting rather than overwriting
        // lets a producer relying on its own IDs find out.
        if attributes.iter().any(|kv| kv.key == EVENT_ID_ATTR) {
            return Err(Rejection::ProducerEventId);
        }
        count += 1;
    }
    // UUIDv7 is time-ordered and random rather than content-derived, so two
    // identical events or a producer retry always get distinct IDs. `now_v7`
    // keeps IDs monotonic within the process.
    Ok((0..count).map(|_| Uuid::now_v7()).collect())
}

/// Refuse a resource that sets gateway-owned values. Every attribute is
/// checked, including each copy of a key the wire repeats.
fn check_resource(attributes: &[KeyValue], scope: &Scope) -> Result<(), Rejection> {
    for kv in attributes {
        if !kv.key.starts_with(RESERVED_PREFIX) {
            continue;
        }
        if kv.key != DATASET_ATTR {
            return Err(Rejection::ReservedResource);
        }
        let value = kv.value.as_ref().and_then(|v| v.value.as_ref());
        let Some(any_value::Value::StringValue(dataset)) = value else {
            return Err(Rejection::DatasetMismatch);
        };
        if !scope.access.allows(dataset) {
            return Err(Rejection::DatasetNotPermitted);
        }
        if *dataset != scope.dataset {
            return Err(Rejection::DatasetMismatch);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use opentelemetry_proto::tonic::common::v1::AnyValue;
    use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
    use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};
    use toudai::auth::DatasetAccess;

    use super::*;

    fn scope(dataset: &str, access: &[&str]) -> Scope {
        Scope {
            dataset: dataset.into(),
            access: if access.is_empty() {
                DatasetAccess::All
            } else {
                DatasetAccess::Only(access.iter().map(|d| d.to_string()).collect())
            },
        }
    }

    fn kv(key: &str, value: any_value::Value) -> KeyValue {
        KeyValue {
            key: key.into(),
            value: Some(AnyValue { value: Some(value) }),
            ..Default::default()
        }
    }

    fn s(key: &str, value: &str) -> KeyValue {
        kv(key, any_value::Value::StringValue(value.into()))
    }

    /// One resource per entry, each holding `records[i]` log records whose
    /// attributes are the given lists.
    fn logs(resources: Vec<(Vec<KeyValue>, Vec<Vec<KeyValue>>)>) -> ExportLogsServiceRequest {
        ExportLogsServiceRequest {
            resource_logs: resources
                .into_iter()
                .map(|(resource, records)| ResourceLogs {
                    resource: Some(Resource {
                        attributes: resource,
                        ..Default::default()
                    }),
                    scope_logs: vec![ScopeLogs {
                        log_records: records
                            .into_iter()
                            .map(|attributes| LogRecord {
                                attributes,
                                ..Default::default()
                            })
                            .collect(),
                        ..Default::default()
                    }],
                    ..Default::default()
                })
                .collect(),
        }
    }

    fn traces(resource: Vec<KeyValue>, spans: Vec<Vec<KeyValue>>) -> ExportTraceServiceRequest {
        ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                resource: Some(Resource {
                    attributes: resource,
                    ..Default::default()
                }),
                scope_spans: vec![ScopeSpans {
                    spans: spans
                        .into_iter()
                        .map(|attributes| Span {
                            attributes,
                            ..Default::default()
                        })
                        .collect(),
                    ..Default::default()
                }],
                ..Default::default()
            }],
        }
    }

    /// Ported from the Go gateway processor suite (collector `f780574`): a
    /// producer `toudai.dataset` must agree with the resolved dataset and the
    /// key's list, every other reserved resource key is refused, and a producer
    /// `toudai.event_id` on any record or span refuses the whole request.
    #[test]
    fn checks_resources_and_records() {
        use Rejection::*;
        let svc = || s("service.name", "svc");
        let ds = |v: &str| s(DATASET_ATTR, v);
        let forged = || s(EVENT_ID_ATTR, "00000000-0000-7000-8000-000000000000");
        let keep = || s("toudai.anything_else", "x");
        let one = |resource: Vec<KeyValue>| vec![(resource, vec![vec![]])];
        // (name, scope, request, expected record count or rejection)
        let cases: Vec<(
            &str,
            Scope,
            ExportLogsServiceRequest,
            Result<usize, Rejection>,
        )> = vec![
            (
                "plain request",
                scope("prod", &["prod"]),
                logs(one(vec![svc()])),
                Ok(1),
            ),
            (
                "resource dataset equal to the resolved one",
                scope("prod", &["prod"]),
                logs(one(vec![ds("prod")])),
                Ok(1),
            ),
            (
                "resource dataset equal to the header, unrestricted key",
                scope("x1", &[]),
                logs(one(vec![ds("x1")])),
                Ok(1),
            ),
            (
                "duplicate matching copies",
                scope("prod", &["prod", "staging"]),
                logs(one(vec![ds("prod"), ds("prod")])),
                Ok(1),
            ),
            (
                "resource dataset outside the key's list",
                scope("prod", &["prod"]),
                logs(one(vec![ds("staging")])),
                Err(DatasetNotPermitted),
            ),
            (
                "resource dataset permitted but not resolved",
                scope("prod", &["prod", "staging"]),
                logs(one(vec![ds("staging")])),
                Err(DatasetMismatch),
            ),
            (
                "resource dataset differs, unrestricted key",
                scope("prod", &[]),
                logs(one(vec![ds("staging")])),
                Err(DatasetMismatch),
            ),
            (
                "duplicate conflicting copies",
                scope("prod", &["prod", "staging"]),
                logs(one(vec![ds("prod"), ds("staging")])),
                Err(DatasetMismatch),
            ),
            (
                "resource dataset is not a string",
                scope("prod", &["prod"]),
                logs(one(vec![kv(DATASET_ATTR, any_value::Value::IntValue(1))])),
                Err(DatasetMismatch),
            ),
            (
                "resource dataset has no value",
                scope("prod", &["prod"]),
                logs(one(vec![KeyValue {
                    key: DATASET_ATTR.into(),
                    value: None,
                    ..Default::default()
                }])),
                Err(DatasetMismatch),
            ),
            (
                "second resource mismatched",
                scope("prod", &["prod", "staging"]),
                logs(vec![
                    (vec![], vec![vec![]]),
                    (vec![ds("staging")], vec![vec![]]),
                ]),
                Err(DatasetMismatch),
            ),
            (
                "second resource outside the key's list",
                scope("prod", &["prod", "staging"]),
                logs(vec![
                    (vec![], vec![vec![]]),
                    (vec![ds("other")], vec![vec![]]),
                ]),
                Err(DatasetNotPermitted),
            ),
            // The retired workspace and scope-marker attributes are ordinary
            // reserved keys now.
            (
                "retired workspace_id",
                scope("prod", &["prod"]),
                logs(one(vec![s("toudai.workspace_id", "ws-1")])),
                Err(ReservedResource),
            ),
            (
                "retired scope_source",
                scope("prod", &["prod"]),
                logs(one(vec![s("toudai.scope_source", "gateway")])),
                Err(ReservedResource),
            ),
            (
                "event_id on a resource",
                scope("prod", &["prod"]),
                logs(one(vec![forged()])),
                Err(ReservedResource),
            ),
            (
                "any other reserved resource key",
                scope("prod", &["prod"]),
                logs(one(vec![svc(), keep()])),
                Err(ReservedResource),
            ),
            (
                "reserved key on a later resource",
                scope("prod", &["prod"]),
                logs(vec![
                    (vec![svc()], vec![vec![]]),
                    (vec![keep()], vec![vec![]]),
                ]),
                Err(ReservedResource),
            ),
            // Other reserved record keys are the mapping's to drop and count.
            (
                "reserved record keys other than event_id",
                scope("prod", &["prod"]),
                logs(vec![(vec![], vec![vec![keep(), s("keep", "me")]])]),
                Ok(1),
            ),
            (
                "record event_id",
                scope("prod", &["prod"]),
                logs(vec![(vec![], vec![vec![forged()]])]),
                Err(ProducerEventId),
            ),
            (
                "duplicated record event_id",
                scope("prod", &["prod"]),
                logs(vec![(vec![], vec![vec![forged(), keep(), forged()]])]),
                Err(ProducerEventId),
            ),
            (
                "event_id on a later record",
                scope("prod", &["prod"]),
                logs(vec![(vec![], vec![vec![keep()], vec![keep(), forged()]])]),
                Err(ProducerEventId),
            ),
            (
                "event_id on a later resource",
                scope("prod", &["prod"]),
                logs(vec![(vec![], vec![vec![]]), (vec![], vec![vec![forged()]])]),
                Err(ProducerEventId),
            ),
            // Resources are checked first: a dataset outside the key's list is
            // a 403 even when an earlier record would be a 400.
            (
                "event_id before a disallowed dataset",
                scope("prod", &["prod"]),
                logs(vec![
                    (vec![], vec![vec![forged()]]),
                    (vec![ds("other")], vec![vec![]]),
                ]),
                Err(DatasetNotPermitted),
            ),
            (
                "no resources",
                scope("prod", &["prod"]),
                logs(vec![]),
                Ok(0),
            ),
            (
                "resource without records",
                scope("prod", &["prod"]),
                logs(vec![(vec![svc()], vec![])]),
                Ok(0),
            ),
        ];
        for (name, scope, request, want) in cases {
            let got = admit_logs(&request, &scope);
            assert_eq!(got.as_ref().map(Vec::len), want.as_ref().copied(), "{name}");
        }
    }

    #[test]
    fn checks_spans_like_log_records() {
        let scope = scope("prod", &["prod"]);
        let forged = s(EVENT_ID_ATTR, "00000000-0000-7000-8000-000000000000");
        let cases = [
            ("accepted", traces(vec![], vec![vec![], vec![]]), Ok(2)),
            (
                "span event_id",
                traces(vec![], vec![vec![], vec![forged.clone()]]),
                Err(Rejection::ProducerEventId),
            ),
            (
                "reserved resource attribute",
                traces(vec![s("toudai.anything_else", "x")], vec![vec![]]),
                Err(Rejection::ReservedResource),
            ),
            (
                "resource dataset outside the key's list",
                traces(vec![s(DATASET_ATTR, "staging")], vec![vec![]]),
                Err(Rejection::DatasetNotPermitted),
            ),
        ];
        for (name, request, want) in cases {
            let got = admit_traces(&request, &scope);
            assert_eq!(got.as_ref().map(Vec::len), want.as_ref().copied(), "{name}");
        }
    }

    /// Identical content everywhere, so only a random ID tells the records
    /// apart. IDs are canonical lowercase UUIDv7 text, unique, and sort in
    /// assignment order, which is request order.
    #[test]
    fn assigns_unique_ordered_uuid_v7_per_record() {
        let resources = (0..3)
            .map(|_| (vec![s("service.name", "svc")], vec![vec![]; 4]))
            .collect();
        let request = logs(resources);
        let mut all = Vec::new();
        // Several requests, so ordering also holds across calls.
        for _ in 0..50 {
            let ids = admit_logs(&request, &scope("prod", &["prod"])).unwrap();
            assert_eq!(ids.len(), 12);
            all.extend(ids);
        }
        let text: Vec<String> = all.iter().map(|id| id.to_string()).collect();
        for (id, t) in all.iter().zip(&text) {
            assert_eq!(id.get_version_num(), 7, "{t}");
            assert_eq!(Uuid::parse_str(t).unwrap(), *id);
            assert_eq!(*t, t.to_lowercase());
            assert_eq!(t.len(), 36);
        }
        assert!(
            text.windows(2).all(|w| w[0] < w[1]),
            "IDs must be unique and strictly increasing"
        );
    }
}
