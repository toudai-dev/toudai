//! Typed query contract enforcement and ClickHouse SQL generation.

pub mod error;
pub mod model;
pub mod plan;
pub mod result;
pub mod schema;
pub mod sql;
pub mod value;

pub use error::{Operation, QueryError, UnsupportedValue};
pub use model::{Aggregate, AttrRef, CmpOp, Namespace, Predicate, Query, Scope, Signal, TimeRange};
pub use plan::{
    ApproximationMode, Guard, Plan, PlanDiagnostics, StoragePath, diagnostics, plan, validate,
};
pub use result::{GroupValue, QueryResult, ResultRow, Sum, ValueStats, interpret};
pub use schema::ServingTables;
pub use sql::{Param, SqlStatement};
pub use value::{Literal, TypedValue, ValueType};
