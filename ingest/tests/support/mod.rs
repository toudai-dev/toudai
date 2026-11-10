//! The `toudai-ingest` binary as the end-to-end tests run it: on ports the
//! system picks, against a test ClickHouse, with its log kept for scanning
//! once it stops.

use std::io::{BufRead, BufReader};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex, mpsc};
use std::thread::JoinHandle;
use std::time::Duration;

use crate::server::{self, ClickHouseServer};

pub struct Service {
    child: Child,
    pub http: String,
    pub grpc: String,
    log: Arc<Mutex<Vec<String>>>,
    reader: Option<JoinHandle<()>>,
}

impl Service {
    /// Stop the service and return every line it wrote to stderr.
    pub fn stop(&mut self) -> Vec<String> {
        let _ = self.child.kill();
        let _ = self.child.wait();
        if let Some(reader) = self.reader.take() {
            reader.join().expect("the log reader panicked");
        }
        self.log.lock().unwrap().clone()
    }
}

impl Drop for Service {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Start the service with `keys` as its write-key file, writing to
/// `database`, with any further environment in `env`.
pub fn spawn(
    clickhouse: &ClickHouseServer,
    keys: &str,
    database: &str,
    env: &[(&str, &str)],
) -> Service {
    let keys_file = std::path::Path::new(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("ingest-write-keys-{database}.txt"));
    std::fs::write(&keys_file, keys).unwrap();
    let mut child = Command::new(env!("CARGO_BIN_EXE_toudai-ingest"))
        .env("TOUDAI_WRITE_KEYS_FILE", &keys_file)
        .env("TOUDAI_OTLP_GRPC_LISTEN", "127.0.0.1:0")
        .env("TOUDAI_OTLP_HTTP_LISTEN", "127.0.0.1:0")
        .env("TOUDAI_CLICKHOUSE_URL", &clickhouse.url)
        .env("TOUDAI_CLICKHOUSE_USER", server::USER)
        .env("TOUDAI_CLICKHOUSE_PASSWORD", server::PASSWORD)
        .env("TOUDAI_CLICKHOUSE_DATABASE", database)
        .envs(env.iter().copied())
        .stderr(Stdio::piped())
        .spawn()
        .expect("start toudai-ingest");
    let stderr = child.stderr.take().unwrap();
    // Guard the child before waiting on it, so a hang or early exit below
    // still reaps it.
    let mut service = Service {
        child,
        http: String::new(),
        grpc: String::new(),
        log: Arc::default(),
        reader: None,
    };
    let log = service.log.clone();
    let (sender, receiver) = mpsc::channel();
    service.reader = Some(std::thread::spawn(move || {
        for line in BufReader::new(stderr).lines().map_while(Result::ok) {
            eprintln!("[toudai-ingest] {line}");
            log.lock().unwrap().push(line.clone());
            if let Some(rest) = line.strip_prefix("toudai-ingest listening on grpc://")
                && let Some((grpc, http)) = rest.split_once(" and ")
            {
                let _ = sender.send((format!("http://{grpc}"), http.to_owned()));
            }
        }
    }));
    (service.grpc, service.http) = receiver
        .recv_timeout(Duration::from_secs(30))
        .expect("toudai-ingest did not report its addresses");
    service
}
