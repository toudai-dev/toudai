//! The static key file shared by read keys (query service) and write keys
//! (`toudai-ingest`).
//!
//! One deployment serves one customer (Decision 0002 D1), so a key maps to
//! permissions, never to a customer: a valid key admits the request, optionally
//! restricted to a list of datasets (D3). Which file a key is in decides
//! whether it reads or writes (Decision 0003 D4). Which customer a key belongs
//! to is private control-plane state outside the engine.
//!
//! PLACEHOLDER for the key store. A static file holds one `<key> [<dataset>...]`
//! entry per line: a key alone may use every dataset, and a key followed by
//! dataset names may use only those. Blank lines and `#` comments are ignored.
//! It is read once at startup, so adding or revoking a key means restarting the
//! service. Keys are held only as SHA-256 digests, and no error names a key.

use std::collections::HashSet;
use std::fmt;

use sha2::{Digest, Sha256};

/// Keys shorter than this are refused at load: they could be guessed, and
/// generated keys are far longer.
pub const MIN_KEY_LENGTH: usize = 16;

/// Whether `name` is an acceptable dataset name:
/// `^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`. Key files, the ingest default
/// dataset, and the `X-Toudai-Dataset` header are all held to it, so a dataset
/// is safe in logs, headers, and stored rows.
pub fn valid_dataset(name: &str) -> bool {
    let bytes = name.as_bytes();
    let tail_ok = |b: &u8| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b':' | b'-');
    (1..=128).contains(&bytes.len())
        && bytes[0].is_ascii_alphanumeric()
        && bytes[1..].iter().all(tail_ok)
}

/// The datasets a key may use.
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

type KeyDigest = [u8; 32];

fn digest(key: &str) -> KeyDigest {
    Sha256::digest(key.as_bytes()).into()
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Keys {
    entries: Vec<(KeyDigest, DatasetAccess)>,
}

/// A key-file load error. It names the line, never the key text: the file is
/// the credential store, and errors reach logs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeysError {
    pub line: usize,
    pub detail: &'static str,
}

impl fmt::Display for KeysError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "line {}: {}", self.line, self.detail)
    }
}

impl std::error::Error for KeysError {}

impl Keys {
    pub fn parse(text: &str) -> Result<Keys, KeysError> {
        let mut entries: Vec<(KeyDigest, DatasetAccess)> = Vec::new();
        let mut seen = HashSet::new();
        for (index, line) in text.lines().enumerate() {
            let err = |detail| KeysError {
                line: index + 1,
                detail,
            };
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let mut fields = line.split_whitespace();
            let key = fields.next().expect("a non-empty line has a field");
            if key.len() < MIN_KEY_LENGTH {
                return Err(err("key is shorter than 16 characters"));
            }
            if !seen.insert(digest(key)) {
                return Err(err("duplicate key"));
            }
            let datasets: Vec<String> = fields.map(str::to_owned).collect();
            // A trailing comment would otherwise silently become dataset names
            // and restrict the key; refuse it so the operator notices.
            if datasets.iter().any(|d| d.starts_with('#')) {
                return Err(err(
                    "a dataset name cannot start with `#`; comments go on their own line",
                ));
            }
            if !datasets.iter().all(|d| valid_dataset(d)) {
                return Err(err("a dataset name is not valid"));
            }
            if datasets.iter().collect::<HashSet<_>>().len() != datasets.len() {
                return Err(err("a dataset is listed twice"));
            }
            let access = if datasets.is_empty() {
                DatasetAccess::All
            } else {
                DatasetAccess::Only(datasets)
            };
            entries.push((digest(key), access));
        }
        if entries.is_empty() {
            return Err(KeysError {
                line: 0,
                detail: "no keys: every request would be rejected",
            });
        }
        Ok(Keys { entries })
    }

    /// The datasets `key` may use, or `None` for an unknown key. The scan
    /// visits every entry and compares digests in constant time, so timing
    /// does not reveal which entry matched or how close a guess came.
    pub fn access_for(&self, key: &str) -> Option<&DatasetAccess> {
        let wanted = digest(key);
        let mut found = None;
        for (candidate, access) in &self.entries {
            if constant_time_eq(candidate, &wanted) {
                found = Some(access);
            }
        }
        found
    }
}

fn constant_time_eq(a: &KeyDigest, b: &KeyDigest) -> bool {
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

    const ALL: &str = "key-all-0123456789";
    const ONE: &str = "key-one-0123456789";
    const TWO: &str = "key-two-0123456789";

    #[test]
    fn parses_keys_and_their_datasets() {
        let text = format!("# comment\n\n{ALL}\n  {ONE}\tprod  \n{TWO} prod staging:v2\n");
        let keys = Keys::parse(&text).unwrap();
        // (key, dataset, Some(allowed) for a known key, None for an unknown one)
        let cases = [
            (ALL, "prod", Some(true)),
            (ALL, "anything", Some(true)),
            (ONE, "prod", Some(true)),
            (ONE, "staging", Some(false)),
            (ONE, "Prod", Some(false)),
            (TWO, "staging:v2", Some(true)),
            (TWO, "dev", Some(false)),
            ("key-all-012345678", "prod", None),
            ("key-all-01234567890", "prod", None),
            ("KEY-ALL-0123456789", "prod", None),
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
        const A: &str = "aaaaaaaaaaaaaaaa";
        const B: &str = "bbbbbbbbbbbbbbbb";
        let cases = [
            (String::new(), 0, "no keys"),
            ("# only a comment\n".into(), 0, "no keys"),
            (format!("{A}\n{B} prod\n{A} staging\n"), 3, "duplicate key"),
            (
                format!("{A}\n{B} prod # prod only\n"),
                2,
                "cannot start with `#`",
            ),
            (format!("{A}\nshort-key-01234 prod\n"), 2, "shorter than 16"),
            (format!("{A} -prod\n"), 1, "not valid"),
            (format!("{A} prod st/aging\n"), 1, "not valid"),
            (format!("{A} prod staging prod\n"), 1, "listed twice"),
        ];
        for (text, line, detail) in cases {
            let err = Keys::parse(&text).unwrap_err();
            assert_eq!(err.line, line, "{text:?}");
            assert!(err.detail.contains(detail), "{text:?}: {err}");
            // The file is the credential store; no error may echo a key.
            for key in [A, B, "short-key"] {
                assert!(!err.to_string().contains(key), "{err} names a key");
            }
        }
    }

    #[test]
    fn dataset_names() {
        let longest = "a".repeat(128);
        let too_long = "a".repeat(129);
        let cases = [
            ("prod", true),
            ("p", true),
            ("0", true),
            ("prod.eu-1:v2_b", true),
            (longest.as_str(), true),
            (too_long.as_str(), false),
            ("", false),
            ("-prod", false),
            (".prod", false),
            ("_prod", false),
            ("pr od", false),
            ("prod/eu", false),
            ("prod\n", false),
            ("pr\u{f6}d", false),
        ];
        for (name, valid) in cases {
            assert_eq!(valid_dataset(name), valid, "{name:?}");
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
