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
}

/// The class of a rejection, which each transport maps to its own status:
/// HTTP 401, 400, or 403, and gRPC `UNAUTHENTICATED`, `INVALID_ARGUMENT`, or
/// `PERMISSION_DENIED`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatusKind {
    Unauthenticated,
    InvalidArgument,
    PermissionDenied,
}

impl StatusKind {
    pub fn http_status(self) -> StatusCode {
        match self {
            StatusKind::Unauthenticated => StatusCode::UNAUTHORIZED,
            StatusKind::InvalidArgument => StatusCode::BAD_REQUEST,
            StatusKind::PermissionDenied => StatusCode::FORBIDDEN,
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
            | ProducerEventId => StatusKind::InvalidArgument,
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
