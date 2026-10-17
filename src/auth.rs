//! Bearer read-key authentication.
//!
//! PLACEHOLDER for the tenant-policy registry (RFD 0001 tenant policy and
//! isolation). Until the registry exists, a static file maps read keys to the
//! tenant (workspace) ID a key may read. The file holds one `<key> <tenant_id>`
//! pair per line; blank lines and `#` comments are ignored. It is read once at
//! startup, so adding or revoking a key means restarting the service, and keys
//! are stored in the clear. None of that is the production design: key
//! resolution, rotation, revocation, and per-dataset authorization all belong to
//! the registry. What this module fixes is the contract callers rely on: the
//! tenant scope of a request comes only from the credential, never from the
//! request.

use std::collections::HashSet;
use std::fmt;

/// The tenant (globally unique workspace ID) a read key is bound to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TenantId(String);

impl TenantId {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadKeys {
    entries: Vec<(String, TenantId)>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadKeysError {
    pub line: usize,
    pub detail: &'static str,
}

impl fmt::Display for ReadKeysError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "read keys line {}: {}", self.line, self.detail)
    }
}

impl std::error::Error for ReadKeysError {}

impl ReadKeys {
    pub fn parse(text: &str) -> Result<ReadKeys, ReadKeysError> {
        let mut entries: Vec<(String, TenantId)> = Vec::new();
        let mut seen = HashSet::new();
        for (index, line) in text.lines().enumerate() {
            let line_no = index + 1;
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let mut fields = line.split_whitespace();
            let (Some(key), Some(tenant), None) = (fields.next(), fields.next(), fields.next())
            else {
                return Err(ReadKeysError {
                    line: line_no,
                    detail: "expected `<key> <tenant_id>`",
                });
            };
            if !seen.insert(key) {
                return Err(ReadKeysError {
                    line: line_no,
                    detail: "duplicate key",
                });
            }
            entries.push((key.to_owned(), TenantId(tenant.to_owned())));
        }
        if entries.is_empty() {
            return Err(ReadKeysError {
                line: 0,
                detail: "no keys: every request would be rejected",
            });
        }
        Ok(ReadKeys { entries })
    }

    /// The tenant bound to `key`. The scan visits every entry and compares in
    /// constant time per entry, so timing does not reveal which key matched.
    pub fn tenant_for(&self, key: &str) -> Option<&TenantId> {
        let mut found = None;
        for (candidate, tenant) in &self.entries {
            if constant_time_eq(candidate.as_bytes(), key.as_bytes()) {
                found = Some(tenant);
            }
        }
        found
    }
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

/// The key of an `Authorization: Bearer <key>` header value.
pub fn bearer_token(header: &str) -> Option<&str> {
    let (scheme, token) = header.split_once(' ')?;
    let token = token.trim();
    (scheme.eq_ignore_ascii_case("bearer") && !token.is_empty()).then_some(token)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_keys_and_looks_up_tenants() {
        let keys =
            ReadKeys::parse("# comment\n\nkey-a  tenant-a\n  key-b\ttenant-b  \nkey-c tenant-a\n")
                .unwrap();
        let cases = [
            ("key-a", Some("tenant-a")),
            ("key-b", Some("tenant-b")),
            ("key-c", Some("tenant-a")),
            ("key-", None),
            ("key-aa", None),
            ("KEY-A", None),
            ("tenant-a", None),
            ("", None),
        ];
        for (key, tenant) in cases {
            assert_eq!(
                keys.tenant_for(key).map(TenantId::as_str),
                tenant,
                "{key:?}"
            );
        }
    }

    #[test]
    fn rejects_malformed_files() {
        let cases = [
            ("", 0, "no keys"),
            ("# only a comment\n", 0, "no keys"),
            ("key-only\n", 1, "expected"),
            ("a t\nb t extra\n", 2, "expected"),
            ("a t\nb u\na v\n", 3, "duplicate key"),
        ];
        for (text, line, detail) in cases {
            let err = ReadKeys::parse(text).unwrap_err();
            assert_eq!(err.line, line, "{text:?}");
            assert!(err.detail.contains(detail), "{text:?}: {err}");
        }
    }

    #[test]
    fn bearer_tokens() {
        let cases = [
            ("Bearer abc", Some("abc")),
            ("bearer abc", Some("abc")),
            ("BEARER  abc ", Some("abc")),
            ("Bearer", None),
            ("Bearer ", None),
            ("Basic abc", None),
            ("abc", None),
        ];
        for (header, token) in cases {
            assert_eq!(bearer_token(header), token, "{header:?}");
        }
    }
}
