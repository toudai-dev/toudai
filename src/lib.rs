//! Toudai query service.
//!
//! The query layer enforces the PoC typed-value contract of RFD 0001
//! (`product-contract.adoc`, "PoC typed-value contract") on top of the
//! ClickHouse serving layout selected by Decision 0001. ClickHouse itself does
//! not enforce the contract: a missing Map key reads as the type default and
//! `sum(Int64)` wraps, so this crate decides what a query may mean and rejects
//! the rest with an explicit error.

pub mod query;
