//! Bearer read-key authentication.
//!
//! One deployment serves one customer (Decision 0002 D1), so a read key maps to
//! permissions, never to a customer: a valid key admits queries, optionally
//! restricted to a list of datasets (D3). Which customer a key belongs to is
//! private control-plane state outside the engine.
//!
//! PLACEHOLDER for the key store. A static file holds one `<key> [<dataset>...]`
//! entry per line: a key alone may read every dataset, and a key followed by
//! dataset names may read only those. Blank lines and `#` comments are ignored.
//! It is read once at startup, so adding or revoking a key means restarting the
//! service, and keys are stored in the clear.

use std::collections::HashSet;
use std::fmt;

/// The datasets a read key may query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DatasetAccess {
    All,
    Only(Vec<String>),
}

impl DatasetAccess {
    pub fn allows(&self, dataset: &str) -> bool {
        match self {
            DatasetAccess::All => true,
            DatasetAccess::Only(datasets) => datasets.iter().any(|d| d == dataset),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadKeys {
    entries: Vec<(String, DatasetAccess)>,
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
        let mut entries: Vec<(String, DatasetAccess)> = Vec::new();
        let mut seen = HashSet::new();
        for (index, line) in text.lines().enumerate() {
            let line_no = index + 1;
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let mut fields = line.split_whitespace();
            let key = fields.next().expect("a non-empty line has a field");
            if !seen.insert(key) {
                return Err(ReadKeysError {
                    line: line_no,
                    detail: "duplicate key",
                });
            }
            let datasets: Vec<String> = fields.map(str::to_owned).collect();
            // A trailing comment would otherwise silently become dataset names
            // and restrict the key; refuse it so the operator notices.
            if datasets.iter().any(|d| d.starts_with('#')) {
                return Err(ReadKeysError {
                    line: line_no,
                    detail: "a dataset name cannot start with `#`; comments go on their own line",
                });
            }
            let access = if datasets.is_empty() {
                DatasetAccess::All
            } else {
                DatasetAccess::Only(datasets)
            };
            entries.push((key.to_owned(), access));
        }
        if entries.is_empty() {
            return Err(ReadKeysError {
                line: 0,
                detail: "no keys: every request would be rejected",
            });
        }
        Ok(ReadKeys { entries })
    }

    /// The datasets `key` may read, or `None` for an unknown key. The scan
    /// visits every entry and compares in constant time per entry, so timing
    /// does not reveal which key matched.
    pub fn access_for(&self, key: &str) -> Option<&DatasetAccess> {
        let mut found = None;
        for (candidate, access) in &self.entries {
            if constant_time_eq(candidate.as_bytes(), key.as_bytes()) {
                found = Some(access);
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
    fn parses_keys_and_their_datasets() {
        let keys =
            ReadKeys::parse("# comment\n\nkey-all\n  key-one\tprod  \nkey-two prod staging\n")
                .unwrap();
        // (key, dataset, Some(allowed) for a known key, None for an unknown one)
        let cases = [
            ("key-all", "prod", Some(true)),
            ("key-all", "anything", Some(true)),
            ("key-one", "prod", Some(true)),
            ("key-one", "staging", Some(false)),
            ("key-one", "Prod", Some(false)),
            ("key-two", "staging", Some(true)),
            ("key-two", "dev", Some(false)),
            ("key-", "prod", None),
            ("key-alll", "prod", None),
            ("KEY-ALL", "prod", None),
            ("prod", "prod", None),
            ("", "prod", None),
        ];
        for (key, dataset, allowed) in cases {
            assert_eq!(
                keys.access_for(key).map(|access| access.allows(dataset)),
                allowed,
                "{key:?} {dataset:?}"
            );
        }
    }

    #[test]
    fn rejects_malformed_files() {
        let cases = [
            ("", 0, "no keys"),
            ("# only a comment\n", 0, "no keys"),
            ("a\nb prod\na staging\n", 3, "duplicate key"),
            ("a\nb prod # prod only\n", 2, "cannot start with `#`"),
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
