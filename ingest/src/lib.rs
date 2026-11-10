//! `toudai-ingest`: the server-side OTLP endpoint (RFD 0001 Decision 0003).
//!
//! It replaces the Go `toudai-collector`. A request is authenticated by its
//! write key ([`auth`]), its dataset is resolved and its resources and records
//! are checked before anything is stamped ([`admit`]), and every log record and
//! span is assigned a UUIDv7 `event_id`. Refusals are [`Rejection`]s with fixed
//! messages that never echo producer values.

pub mod admit;
pub mod auth;
pub mod batcher;
#[cfg(test)]
mod fuzzing;
pub mod grpc;
pub mod http;
pub mod mapping;
pub mod metrics;
pub mod pipeline;
pub mod protojson;
mod rejection;
pub mod value;
pub mod writer;

pub use rejection::{Refusal, Rejection, StatusKind};
