//! `toudai-ingest`: the server-side OTLP endpoint (RFD 0001 Decision 0003).
//!
//! It replaces the Go `toudai-collector`. A request is authenticated by its
//! write key ([`auth`]), its dataset is resolved and its resources and records
//! are checked before anything is stamped ([`admit`]), and every log record and
//! span is assigned a UUIDv7 `event_id`. Refusals are [`Rejection`]s with fixed
//! messages that never echo producer values.

pub mod admit;
pub mod auth;
pub mod mapping;
pub mod metrics;
mod rejection;
pub mod value;
pub mod writer;

pub use rejection::{Rejection, StatusKind};
