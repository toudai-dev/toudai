//! Deterministic OTLP fixture emitter for Toudai.
//!
//! ```text
//! fixture-emitter expected [--seed N] [--out FILE]
//! ```

// The scope fields are consumed by the OTLP emitter added in the next commit.
#![allow(dead_code)]

mod expected;
mod model;
mod value;

use std::process::ExitCode;

const DEFAULT_SEED: u64 = 20_260_901;

fn usage() -> String {
    "usage: fixture-emitter expected [--seed N] [--out FILE]".to_string()
}

fn run() -> Result<(), String> {
    let mut it = std::env::args().skip(1);
    if it.next().as_deref() != Some("expected") {
        return Err(usage());
    }
    let (mut seed, mut out) = (DEFAULT_SEED, None);
    while let Some(flag) = it.next() {
        let value = it
            .next()
            .ok_or_else(|| format!("{flag} needs a value\n{}", usage()))?;
        match flag.as_str() {
            "--seed" => seed = value.parse().map_err(|_| format!("bad --seed {value:?}"))?,
            "--out" => out = Some(value),
            other => return Err(format!("unexpected argument {other:?}\n{}", usage())),
        }
    }
    let fixture = model::build(seed);
    let rows = expected::rows(&fixture.logs, &fixture.spans, &fixture.metrics)?;
    let text = expected::to_jsonl(&rows);
    match out {
        Some(path) => std::fs::write(&path, text).map_err(|e| format!("write {path}: {e}")),
        None => {
            print!("{text}");
            Ok(())
        }
    }
}

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::FAILURE
        }
    }
}
