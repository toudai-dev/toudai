//! Typed query contract enforcement and ClickHouse SQL generation.

pub mod error;
pub mod value;

pub use error::{Operation, QueryError, UnsupportedValue};
pub use value::{Literal, TypedValue, ValueType};
