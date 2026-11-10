use http::StatusCode;

/// Why a whole request is refused. Each maps to a client status and a fixed
/// message: a message never carries a producer value, so nothing a producer
/// sent (least of all its key) can be reflected back or logged.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Rejection {
    MissingKey,
    InvalidKey,
    MultipleKeys,
    MultipleDatasetHeaders,
    DatasetRequired,
    InvalidDataset,
    DatasetNotPermitted,
    DatasetMismatch,
    ReservedResource,
    ProducerEventId,
    InvalidId,
}

/// The class of a refused request, which each transport maps to its own
/// status: HTTP 401, 400, 403, 503, or 500, and gRPC `UNAUTHENTICATED`,
/// `INVALID_ARGUMENT`, `PERMISSION_DENIED`, `UNAVAILABLE`, or `INTERNAL`.
/// Only `Unavailable` tells an OTLP producer to retry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatusKind {
    Unauthenticated,
    InvalidArgument,
    PermissionDenied,
    Unavailable,
    Internal,
}

impl StatusKind {
    pub fn http_status(self) -> StatusCode {
        match self {
            StatusKind::Unauthenticated => StatusCode::UNAUTHORIZED,
            StatusKind::InvalidArgument => StatusCode::BAD_REQUEST,
            StatusKind::PermissionDenied => StatusCode::FORBIDDEN,
            StatusKind::Unavailable => StatusCode::SERVICE_UNAVAILABLE,
            StatusKind::Internal => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    /// The counter label of the class.
    pub fn label(self) -> &'static str {
        match self {
            StatusKind::Unauthenticated => "unauthenticated",
            StatusKind::InvalidArgument => "invalid_argument",
            StatusKind::PermissionDenied => "permission_denied",
            StatusKind::Unavailable => "unavailable",
            StatusKind::Internal => "internal",
        }
    }
}

/// The answer to a request that stored nothing: a status class and a fixed
/// message, never a producer value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Refusal {
    pub kind: StatusKind,
    pub message: &'static str,
}

impl Refusal {
    /// ClickHouse could not take the write before the deadline; the producer
    /// retries.
    pub const UNAVAILABLE: Refusal = Refusal {
        kind: StatusKind::Unavailable,
        message: "storage is unavailable, retry later",
    };
    /// ClickHouse refused the rows in a way a retry cannot fix, such as a
    /// schema that does not match the service.
    pub const INTERNAL: Refusal = Refusal {
        kind: StatusKind::Internal,
        message: "storage refused the write",
    };
}

impl From<Rejection> for Refusal {
    fn from(rejection: Rejection) -> Refusal {
        Refusal {
            kind: rejection.kind(),
            message: rejection.message(),
        }
    }
}

impl Rejection {
    pub fn kind(self) -> StatusKind {
        use Rejection::*;
        match self {
            MissingKey | InvalidKey | MultipleKeys => StatusKind::Unauthenticated,
            // Two dataset headers are a malformed request, not a bad
            // credential; the Go collector answered 401 only because its auth
            // extension was the component that noticed.
            MultipleDatasetHeaders
            | DatasetRequired
            | InvalidDataset
            | DatasetMismatch
            | ReservedResource
            | ProducerEventId
            | InvalidId => StatusKind::InvalidArgument,
            DatasetNotPermitted => StatusKind::PermissionDenied,
        }
    }

    pub fn message(self) -> &'static str {
        use Rejection::*;
        match self {
            MissingKey => "missing ingestion key",
            InvalidKey => "invalid ingestion key",
            MultipleKeys => "multiple ingestion keys supplied",
            MultipleDatasetHeaders => "multiple dataset headers supplied",
            DatasetRequired => {
                "dataset is required: the key does not name a single dataset and no default dataset is configured"
            }
            InvalidDataset => "dataset is not a valid dataset name",
            DatasetNotPermitted => "dataset is not permitted for this ingestion key",
            DatasetMismatch => "resource toudai.dataset does not match the request's dataset",
            ReservedResource => {
                "resource attribute in the reserved toudai. namespace is not accepted from producers"
            }
            InvalidId => "a trace or span ID does not have the OTLP length of 16 or 8 bytes",
            ProducerEventId => {
                "record attribute toudai.event_id is assigned by the gateway and is not accepted from producers"
            }
        }
    }
}

impl std::fmt::Display for Rejection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.message())
    }
}

impl std::error::Error for Rejection {}
