//! Write-key authentication and dataset resolution (Decision 0002 D3).
//!
//! Write keys live in their own file in the shared key-file format
//! (`toudai::auth`), so a read key is simply unknown here. A key maps to
//! permissions, never to a customer: the deployment is the customer. Both
//! headers are checked before the body is decoded.

use http::HeaderMap;
use toudai::auth::{DatasetAccess, Keys, valid_dataset};

use crate::Rejection;

/// The request header carrying the write key. `HeaderMap` and gRPC metadata
/// both match names case-insensitively.
pub const KEY_HEADER: &str = "x-toudai-key";
/// The request header optionally naming the dataset a request writes to.
pub const DATASET_HEADER: &str = "x-toudai-dataset";

/// The trusted scope of one request: the dataset its records are stored
/// under, and the datasets its key may use.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Scope {
    pub dataset: String,
    pub access: DatasetAccess,
}

/// Authenticate a request's write key and resolve its dataset: the
/// `X-Toudai-Dataset` header, else the key's only dataset, else
/// `default_dataset`. Whichever source wins, the name must be valid and in the
/// key's list.
pub fn authorize(
    keys: &Keys,
    headers: &HeaderMap,
    default_dataset: Option<&str>,
) -> Result<Scope, Rejection> {
    let access = match single(headers, KEY_HEADER) {
        Header::Absent => return Err(Rejection::MissingKey),
        Header::Repeated => return Err(Rejection::MultipleKeys),
        Header::NotUtf8 => return Err(Rejection::InvalidKey),
        Header::Value(key) => keys.access_for(key).ok_or(Rejection::InvalidKey)?,
    };
    let requested = match single(headers, DATASET_HEADER) {
        Header::Absent => None,
        Header::Repeated => return Err(Rejection::MultipleDatasetHeaders),
        Header::NotUtf8 => return Err(Rejection::InvalidDataset),
        Header::Value(dataset) => Some(dataset),
    };
    let dataset = match (requested, access) {
        (Some(dataset), _) => dataset,
        (None, DatasetAccess::Only(datasets)) if datasets.len() == 1 => &datasets[0],
        (None, _) => default_dataset.ok_or(Rejection::DatasetRequired)?,
    };
    // The header is producer input and a key without a list accepts any name,
    // so the name itself is checked before it becomes a stored value.
    if !valid_dataset(dataset) {
        return Err(Rejection::InvalidDataset);
    }
    if !access.allows(dataset) {
        return Err(Rejection::DatasetNotPermitted);
    }
    Ok(Scope {
        dataset: dataset.to_owned(),
        access: access.clone(),
    })
}

/// A header that may appear at most once.
enum Header<'h> {
    /// Absent, or present but blank.
    Absent,
    /// The trimmed value.
    Value(&'h str),
    NotUtf8,
    Repeated,
}

fn single<'h>(headers: &'h HeaderMap, name: &str) -> Header<'h> {
    let mut values = headers.get_all(name).iter();
    let Some(value) = values.next() else {
        return Header::Absent;
    };
    if values.next().is_some() {
        return Header::Repeated;
    }
    match std::str::from_utf8(value.as_bytes()) {
        Ok(text) if text.trim().is_empty() => Header::Absent,
        Ok(text) => Header::Value(text.trim()),
        Err(_) => Header::NotUtf8,
    }
}

#[cfg(test)]
mod tests {
    use http::{HeaderName, HeaderValue};

    use super::*;
    use crate::StatusKind;

    const MULTI: &str = "tk_test_multi_dataset_0123456789";
    const PROD: &str = "tk_test_prod_only_0123456789";
    const ALL: &str = "tk_test_every_dataset_0123456789";

    fn keys() -> Keys {
        Keys::parse(&format!("{MULTI} prod staging\n{PROD} prod\n{ALL}\n")).unwrap()
    }

    fn headers(pairs: &[(&str, &[u8])]) -> HeaderMap {
        let mut map = HeaderMap::new();
        for (name, value) in pairs {
            map.append(
                HeaderName::from_bytes(name.as_bytes()).unwrap(),
                HeaderValue::from_bytes(value).unwrap(),
            );
        }
        map
    }

    /// (name, headers, default dataset, expected dataset or rejection)
    type Case<'a> = (
        &'a str,
        Vec<(&'a str, &'a [u8])>,
        Option<&'a str>,
        Result<&'a str, Rejection>,
    );

    /// Ported from the Go auth extension and gateway processor suites
    /// (collector `f780574`): which key and dataset headers authenticate, and
    /// which dataset each resolves to.
    #[test]
    fn authorizes_keys_and_resolves_datasets() {
        let k = |key: &'static str| ("X-Toudai-Key", key.as_bytes());
        let d = |dataset: &'static str| ("X-Toudai-Dataset", dataset.as_bytes());
        let cases: &[Case] = &[
            (
                "single-dataset key uses its dataset",
                vec![k(PROD)],
                None,
                Ok("prod"),
            ),
            (
                "key's only dataset wins over the default",
                vec![k(PROD)],
                Some("staging"),
                Ok("prod"),
            ),
            (
                "header inside the key's list",
                vec![k(MULTI), d("staging")],
                None,
                Ok("staging"),
            ),
            (
                "header wins over the default",
                vec![k(MULTI), d("staging")],
                Some("prod"),
                Ok("staging"),
            ),
            (
                "multi-dataset key falls back to the default",
                vec![k(MULTI)],
                Some("staging"),
                Ok("staging"),
            ),
            (
                "unrestricted key falls back to the default",
                vec![k(ALL)],
                Some("prod"),
                Ok("prod"),
            ),
            (
                "unrestricted key accepts any valid header",
                vec![k(ALL), d("anything-1")],
                None,
                Ok("anything-1"),
            ),
            (
                "header names are case-insensitive",
                vec![
                    ("x-TOUDAI-key", PROD.as_bytes()),
                    ("X-TOUDAI-DATASET", b"prod"),
                ],
                None,
                Ok("prod"),
            ),
            (
                "surrounding whitespace is trimmed",
                vec![
                    ("X-Toudai-Key", b"  tk_test_prod_only_0123456789 "),
                    d(" prod "),
                ],
                None,
                Ok("prod"),
            ),
            (
                "blank dataset header counts as absent",
                vec![k(PROD), d("  ")],
                None,
                Ok("prod"),
            ),
            (
                "multi-dataset key without header or default",
                vec![k(MULTI)],
                None,
                Err(Rejection::DatasetRequired),
            ),
            (
                "unrestricted key without header or default",
                vec![k(ALL)],
                None,
                Err(Rejection::DatasetRequired),
            ),
            (
                "default outside the key's list",
                vec![k(MULTI)],
                Some("other"),
                Err(Rejection::DatasetNotPermitted),
            ),
            (
                "header outside the key's list",
                vec![k(PROD), d("staging")],
                None,
                Err(Rejection::DatasetNotPermitted),
            ),
            (
                "dataset match is exact",
                vec![k(PROD), d("Prod")],
                None,
                Err(Rejection::DatasetNotPermitted),
            ),
            (
                "invalid header name, unrestricted key",
                vec![k(ALL), d("a b")],
                None,
                Err(Rejection::InvalidDataset),
            ),
            (
                "invalid header name, restricted key",
                vec![k(PROD), d("a b")],
                None,
                Err(Rejection::InvalidDataset),
            ),
            (
                "invalid default, unrestricted key",
                vec![k(ALL)],
                Some("-lead"),
                Err(Rejection::InvalidDataset),
            ),
            (
                "dataset header that is not UTF-8",
                vec![k(ALL), ("X-Toudai-Dataset", b"pr\xffd")],
                None,
                Err(Rejection::InvalidDataset),
            ),
            (
                "two dataset headers",
                vec![k(MULTI), d("prod"), d("staging")],
                None,
                Err(Rejection::MultipleDatasetHeaders),
            ),
            (
                "missing key",
                vec![d("prod")],
                None,
                Err(Rejection::MissingKey),
            ),
            (
                "no headers",
                vec![],
                Some("prod"),
                Err(Rejection::MissingKey),
            ),
            (
                "blank key",
                vec![("X-Toudai-Key", b"  ")],
                None,
                Err(Rejection::MissingKey),
            ),
            (
                "unknown key",
                vec![k("tk_test_not_a_real_key_000")],
                None,
                Err(Rejection::InvalidKey),
            ),
            (
                "prefix of a real key",
                vec![k(&PROD[..PROD.len() - 1])],
                None,
                Err(Rejection::InvalidKey),
            ),
            (
                "key that is not UTF-8",
                vec![("X-Toudai-Key", b"tk_test_\xff_0123456789")],
                None,
                Err(Rejection::InvalidKey),
            ),
            (
                "authorization header is not the key",
                vec![("Authorization", b"Bearer tk_test_prod_only_0123456789")],
                None,
                Err(Rejection::MissingKey),
            ),
            (
                "two key headers",
                vec![k(MULTI), k(PROD)],
                None,
                Err(Rejection::MultipleKeys),
            ),
            (
                "two key headers, one blank",
                vec![k(PROD), ("X-Toudai-Key", b" ")],
                None,
                Err(Rejection::MultipleKeys),
            ),
            // The key is checked first: a bad dataset header behind a bad key
            // is still a 401.
            (
                "unknown key with two dataset headers",
                vec![k("tk_test_not_a_real_key_000"), d("a"), d("b")],
                None,
                Err(Rejection::InvalidKey),
            ),
        ];
        let keys = keys();
        for (name, pairs, default, want) in cases {
            let got = authorize(&keys, &headers(pairs), *default);
            assert_eq!(
                got.as_ref().map(|s| s.dataset.as_str()),
                want.as_ref().copied(),
                "{name}"
            );
        }
    }

    #[test]
    fn scope_carries_the_keys_access() {
        let scope = authorize(
            &keys(),
            &headers(&[("X-Toudai-Key", PROD.as_bytes())]),
            None,
        )
        .unwrap();
        assert_eq!(scope.access, DatasetAccess::Only(vec!["prod".into()]));
        let scope = authorize(
            &keys(),
            &headers(&[("X-Toudai-Key", ALL.as_bytes()), ("X-Toudai-Dataset", b"x")]),
            None,
        )
        .unwrap();
        assert_eq!(scope.access, DatasetAccess::All);
    }

    #[test]
    fn rejections_carry_client_statuses() {
        use Rejection::*;
        let cases = [
            (MissingKey, StatusKind::Unauthenticated, 401),
            (InvalidKey, StatusKind::Unauthenticated, 401),
            (MultipleKeys, StatusKind::Unauthenticated, 401),
            (MultipleDatasetHeaders, StatusKind::InvalidArgument, 400),
            (DatasetRequired, StatusKind::InvalidArgument, 400),
            (InvalidDataset, StatusKind::InvalidArgument, 400),
            (DatasetMismatch, StatusKind::InvalidArgument, 400),
            (ReservedResource, StatusKind::InvalidArgument, 400),
            (ProducerEventId, StatusKind::InvalidArgument, 400),
            (DatasetNotPermitted, StatusKind::PermissionDenied, 403),
        ];
        for (rejection, kind, status) in cases {
            assert_eq!(rejection.kind(), kind, "{rejection:?}");
            assert_eq!(
                rejection.kind().http_status().as_u16(),
                status,
                "{rejection:?}"
            );
        }
    }
}
