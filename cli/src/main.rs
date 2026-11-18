//! `toudai-cli`: the operator client of the Toudai query service.
//!
//! A thin client over the public HTTP API (`POST /v1/query` and
//! `GET /v1/events/{dataset}/{event_id}`): it builds requests from command
//! lines and renders responses, and owns no query semantics of its own. See
//! `toudai-cli help` and `cli/README.adoc`.
//!
//! The read key comes from `TOUDAI_READ_KEY`, never from an argument, so it
//! stays out of shell history and process listings.
//!
//! Exit status: 0 on success, 1 when the service refused or failed the
//! request (or an event's checksum does not match), 2 on a usage error.

mod args;
mod render;
mod time;

use std::process::ExitCode;

use serde::Deserialize;
use serde_json::Value;
use serde_json::value::RawValue;
use sha2::{Digest, Sha256};

use args::{Command, USAGE};

/// A canonical event as `GET /v1/events` serves it. The payload stays the
/// exact text served, so its checksum can be verified.
#[derive(Debug, Deserialize)]
pub struct Event {
    pub dataset: String,
    pub event_id: String,
    pub signal: String,
    pub timestamp_ns: String,
    pub schema_version: String,
    pub payload: Box<RawValue>,
    pub payload_sha256: String,
}

fn env(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|v| !v.is_empty())
}

/// The service's error body as one line: its stable code and message.
fn refusal(status: reqwest::StatusCode, body: &str) -> String {
    let parsed: Option<Value> = serde_json::from_str(body).ok();
    match parsed.as_ref().map(|b| &b["error"]) {
        Some(error) if error["code"].is_string() => format!(
            "{} ({}): {}",
            render::plain(error["code"].as_str().unwrap_or_default()),
            status.as_u16(),
            render::plain(error["message"].as_str().unwrap_or_default())
        ),
        _ => format!("HTTP {status}: {}", render::plain(body.trim())),
    }
}

async fn run(command: Command) -> Result<String, String> {
    if command == Command::Help {
        return Ok(USAGE.to_owned());
    }
    let base = env("TOUDAI_URL").unwrap_or_else(|| "http://127.0.0.1:8080".into());
    let key = env("TOUDAI_READ_KEY").ok_or("TOUDAI_READ_KEY is required")?;
    let client = reqwest::Client::new();
    let base: reqwest::Url = base
        .trim_end_matches('/')
        .parse()
        .map_err(|e| format!("TOUDAI_URL: {e}"))?;
    let send = |request: reqwest::RequestBuilder| async {
        let response = request
            .bearer_auth(&key)
            .send()
            .await
            .map_err(|e| format!("cannot reach the query service at {base}: {e}"))?;
        let status = response.status();
        let body = response.text().await.map_err(|e| e.to_string())?;
        if !status.is_success() {
            return Err(refusal(status, &body));
        }
        Ok(body)
    };
    match command {
        Command::Help => unreachable!("answered above"),
        Command::Query { body, view, json } => {
            let url = base.join("/v1/query").map_err(|e| e.to_string())?;
            let text = send(client.post(url).json(&body)).await?;
            if json {
                return Ok(text);
            }
            let response: Value =
                serde_json::from_str(&text).map_err(|e| format!("unreadable response: {e}"))?;
            render::query(&view, &body, &response)
        }
        Command::Event {
            dataset,
            event_id,
            json,
        } => {
            let mut url = base.join("/v1/events").map_err(|e| e.to_string())?;
            // Each identifier is one percent-encoded path segment.
            url.path_segments_mut()
                .map_err(|_| "TOUDAI_URL cannot be a base URL")?
                .extend([&dataset, &event_id]);
            let text = send(client.get(url)).await?;
            let event: Event =
                serde_json::from_str(&text).map_err(|e| format!("unreadable event: {e}"))?;
            let digest = Sha256::digest(event.payload.get().as_bytes());
            let verified = format!("{digest:x}") == event.payload_sha256;
            let out = if json {
                text
            } else {
                render::event(&event, verified)
            };
            if verified {
                Ok(out)
            } else {
                Err(format!(
                    "{out}\npayload_sha256 does not match the payload served"
                ))
            }
        }
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let command = match args::parse(&args) {
        Ok(command) => command,
        Err(error) => {
            eprintln!("toudai-cli: {error}\n\n{USAGE}");
            return ExitCode::from(2);
        }
    };
    match run(command).await {
        Ok(out) => {
            print!("{out}");
            if !out.ends_with('\n') {
                println!();
            }
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("toudai-cli: {error}");
            ExitCode::FAILURE
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn refusals_print_on_one_line() {
        let body = r#"{"error":{"code":"x","message":"a\nb\u0085c"}}"#;
        assert_eq!(
            refusal(reqwest::StatusCode::BAD_REQUEST, body),
            "x (400): a\\nb\\u{85}c"
        );
    }
}
