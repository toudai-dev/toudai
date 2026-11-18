//! Toudai query service.
//!
//! The query layer enforces the [PoC typed-value contract of RFD 0001]
//! on top of the
//! ClickHouse serving layout selected by Decision 0001. ClickHouse itself does
//! not enforce the contract: a missing Map key reads as the type default and
//! `sum(Int64)` wraps, so this crate decides what a query may mean and rejects
//! the rest with an explicit error.
//!
//! [PoC typed-value contract of RFD 0001]: https://github.com/toudai-dev/toudai/blob/ba8f98da50642b6f1212ca941845cf7100728bb9/rfd/0001/product-contract.adoc#_poc_typed_value_contract

pub mod api;
pub mod auth;
pub mod clickhouse;
pub mod events;
pub mod query;
