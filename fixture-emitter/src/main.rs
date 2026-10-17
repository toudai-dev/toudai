//! Deterministic OTLP fixture emitter for Toudai.
//!
//! ```text
//! fixture-emitter expected  [--seed N] [--out FILE]
//! fixture-emitter emit      [--seed N] [--protocol grpc|http] [--endpoint URL] [--expected-out FILE]
//! fixture-emitter selfcheck [--seed N] [--protocol grpc|http] [--expected-out FILE] [--observed-out FILE]
//! ```

mod check;
mod emit;
mod expected;
mod model;
mod observe;
mod receiver;
mod value;

use std::collections::BTreeMap;
use std::process::ExitCode;

use emit::Transport;

const DEFAULT_SEED: u64 = 20_260_901;

struct Args {
    command: String,
    flags: BTreeMap<String, String>,
}

fn parse_args() -> Result<Args, String> {
    let mut it = std::env::args().skip(1);
    let command = it.next().ok_or_else(usage)?;
    let mut flags = BTreeMap::new();
    while let Some(flag) = it.next() {
        let name = flag
            .strip_prefix("--")
            .ok_or_else(|| format!("unexpected argument {flag:?}\n{}", usage()))?;
        let value = it.next().ok_or_else(|| format!("--{name} needs a value"))?;
        flags.insert(name.to_string(), value);
    }
    Ok(Args { command, flags })
}

fn usage() -> String {
    "usage: fixture-emitter <expected|emit|selfcheck> [--seed N] [--protocol grpc|http] \
     [--endpoint URL] [--out FILE] [--expected-out FILE] [--observed-out FILE]"
        .to_string()
}

impl Args {
    fn seed(&self) -> Result<u64, String> {
        match self.flags.get("seed") {
            Some(s) => s.parse().map_err(|_| format!("bad --seed {s:?}")),
            None => Ok(DEFAULT_SEED),
        }
    }

    fn transport(&self) -> Result<Transport, String> {
        Transport::parse(self.flags.get("protocol").map_or("grpc", String::as_str))
    }

    fn check_flags(&self, allowed: &[&str]) -> Result<(), String> {
        for name in self.flags.keys() {
            if !allowed.contains(&name.as_str()) {
                return Err(format!(
                    "--{name} does not apply to {}\n{}",
                    self.command,
                    usage()
                ));
            }
        }
        Ok(())
    }
}

fn write(path: &str, text: &str) -> Result<(), String> {
    std::fs::write(path, text).map_err(|e| format!("write {path}: {e}"))
}

fn expected_text(seed: u64) -> Result<(model::Fixture, Vec<expected::Row>), String> {
    let fixture = model::build(seed);
    let rows = expected::rows(&fixture.logs, &fixture.spans, &fixture.metrics)?;
    Ok((fixture, rows))
}

fn run() -> Result<(), String> {
    let args = parse_args()?;
    let seed = args.seed()?;
    match args.command.as_str() {
        "expected" => {
            args.check_flags(&["seed", "out"])?;
            let (_, rows) = expected_text(seed)?;
            let text = expected::to_jsonl(&rows);
            match args.flags.get("out") {
                Some(path) => write(path, &text)?,
                None => print!("{text}"),
            }
            Ok(())
        }
        "emit" => {
            args.check_flags(&["seed", "protocol", "endpoint", "expected-out"])?;
            let transport = args.transport()?;
            let endpoint = args
                .flags
                .get("endpoint")
                .map_or(transport.default_endpoint(), String::as_str);
            let (fixture, rows) = expected_text(seed)?;
            if let Some(path) = args.flags.get("expected-out") {
                write(path, &expected::to_jsonl(&rows))?;
            }
            let rt = check::runtime()?;
            let _enter = (transport == Transport::Grpc).then(|| rt.enter());
            emit::emit(&fixture, transport, endpoint)?;
            eprintln!(
                "sent {} log records, {} spans, {} metrics over {transport:?} to {endpoint}",
                fixture.logs.len(),
                fixture.spans.len(),
                fixture.metrics.len()
            );
            Ok(())
        }
        "selfcheck" => {
            args.check_flags(&["seed", "protocol", "expected-out", "observed-out"])?;
            let transport = args.transport()?;
            let (rows, outcome) = check::selfcheck(seed, transport)?;
            if let Some(path) = args.flags.get("expected-out") {
                write(path, &expected::to_jsonl(&rows))?;
            }
            if let Some(path) = args.flags.get("observed-out") {
                write(path, &expected::to_jsonl(&outcome.rows))?;
            }
            if outcome.problems.is_empty() {
                eprintln!(
                    "selfcheck ok over {transport:?}: {} expected events, {} observed, 0 mismatches",
                    rows.len(),
                    outcome.rows.len()
                );
                Ok(())
            } else {
                for problem in &outcome.problems {
                    eprintln!("MISMATCH {problem}");
                }
                Err(format!("{} mismatches", outcome.problems.len()))
            }
        }
        other => Err(format!("unknown command {other:?}\n{}", usage())),
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
