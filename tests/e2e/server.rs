//! A throwaway ClickHouse server for the end-to-end test.
//!
//! Two runtimes, chosen by `TOUDAI_E2E_RUNTIME` (`docker` or `native`):
//!
//! * `docker`: a `clickhouse/clickhouse-server` container, removed on drop. The
//!   image is `TOUDAI_E2E_CLICKHOUSE_IMAGE` (default: the version pinned by CI).
//! * `native`: `clickhouse server` from `TOUDAI_CLICKHOUSE_BIN` (default
//!   `clickhouse` on `PATH`, provided by the Nix development shell) on a free
//!   loopback port, for machines without a Docker daemon.
//!
//! Unset, Docker is used when its daemon answers, else the native binary. The
//! test is skipped when neither is available unless `TOUDAI_REQUIRE_CLICKHOUSE`
//! is set, which turns that into a failure (CI sets it).
//!
//! Both runtimes load the same test storage policy (see
//! `tests/fixtures/e2e/storage-test.xml`) so the product DDL applies verbatim.

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

pub const USER: &str = "toudai";
pub const PASSWORD: &str = "toudai-test";
/// A second user whose profile silently truncates results (`break` overflow
/// mode), to prove the service refuses to report a truncated answer as exact.
pub const LIMITED_USER: &str = "limited";
const LIMITED_PROFILE: &str = "<limited><max_result_rows>1</max_result_rows>\
    <result_overflow_mode>break</result_overflow_mode><max_block_size>1</max_block_size></limited>";
const LIMITED_USER_XML: &str = "<limited><password>toudai-test</password>\
    <networks><ip>::/0</ip></networks><profile>limited</profile><quota>default</quota></limited>";
const DEFAULT_IMAGE: &str = "clickhouse/clickhouse-server:26.8.2.7";
const STORAGE_TEMPLATE: &str = include_str!("../fixtures/e2e/storage-test.xml");

enum Runtime {
    Docker { container: String },
    Native { child: Child },
}

pub struct ClickHouseServer {
    pub url: String,
    runtime: Runtime,
}

impl Drop for ClickHouseServer {
    fn drop(&mut self) {
        match &mut self.runtime {
            Runtime::Docker { container } => {
                let _ = Command::new("docker")
                    .args(["rm", "--force", "--volumes", container])
                    .stdout(Stdio::null())
                    .stderr(Stdio::null())
                    .status();
            }
            Runtime::Native { child } => {
                let _ = child.kill();
                let _ = child.wait();
            }
        }
    }
}

fn runnable(program: &str, args: &[&str]) -> bool {
    Command::new(program)
        .args(args)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|status| status.success())
}

fn native_binary() -> String {
    std::env::var("TOUDAI_CLICKHOUSE_BIN").unwrap_or_else(|_| "clickhouse".into())
}

fn scratch_dir(name: &str) -> PathBuf {
    let dir = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(name);
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create scratch dir");
    dir
}

/// Start a server, or `None` when no runtime is available (the caller skips).
pub async fn start() -> Option<ClickHouseServer> {
    let requested = std::env::var("TOUDAI_E2E_RUNTIME").ok();
    let runtime = match requested.as_deref() {
        Some(runtime @ ("docker" | "native")) => runtime,
        Some(other) => panic!("TOUDAI_E2E_RUNTIME must be docker or native, not {other:?}"),
        None if runnable("docker", &["info"]) => "docker",
        None if runnable(&native_binary(), &["--version"]) => "native",
        None => {
            assert!(
                std::env::var_os("TOUDAI_REQUIRE_CLICKHOUSE").is_none(),
                "TOUDAI_REQUIRE_CLICKHOUSE is set but neither a Docker daemon nor `{}` is available",
                native_binary()
            );
            eprintln!("skipping the end-to-end test: no Docker daemon or ClickHouse binary");
            return None;
        }
    };
    let server = if runtime == "docker" {
        start_docker()
    } else {
        start_native()
    };
    wait_until_ready(&server.url).await;
    Some(server)
}

fn start_docker() -> ClickHouseServer {
    let dir = scratch_dir("e2e-docker");
    let storage = dir.join("storage-test.xml");
    std::fs::write(
        &storage,
        STORAGE_TEMPLATE.replace("@DATA_DIR@", "/var/lib/clickhouse"),
    )
    .expect("write storage config");
    let limited = dir.join("limited-user.xml");
    std::fs::write(
        &limited,
        format!(
            "<clickhouse><profiles>{LIMITED_PROFILE}</profiles><users>{LIMITED_USER_XML}</users></clickhouse>"
        ),
    )
    .expect("write limited user config");
    let image = std::env::var("TOUDAI_E2E_CLICKHOUSE_IMAGE").unwrap_or(DEFAULT_IMAGE.into());
    let output = Command::new("docker")
        .args(["run", "--detach", "--publish", "127.0.0.1::8123"])
        .arg("--volume")
        .arg(format!(
            "{}:/etc/clickhouse-server/config.d/toudai-storage-test.xml:ro",
            storage.display()
        ))
        .arg("--volume")
        .arg(format!(
            "{}:/etc/clickhouse-server/users.d/limited-user.xml:ro",
            limited.display()
        ))
        .args(["--env", &format!("CLICKHOUSE_USER={USER}")])
        .args(["--env", &format!("CLICKHOUSE_PASSWORD={PASSWORD}")])
        .args(["--ulimit", "nofile=262144:262144"])
        .arg(&image)
        .output()
        .expect("run docker");
    assert!(
        output.status.success(),
        "docker run {image} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let container = String::from_utf8(output.stdout).unwrap().trim().to_owned();
    // From here on `Drop` removes the container, including when the lookup panics.
    let mut server = ClickHouseServer {
        url: String::new(),
        runtime: Runtime::Docker {
            container: container.clone(),
        },
    };
    let port = Command::new("docker")
        .args(["port", &container, "8123/tcp"])
        .output()
        .expect("docker port");
    let mapped = String::from_utf8(port.stdout).unwrap();
    // `127.0.0.1:49153`, one line per address family.
    let address = mapped.lines().next().unwrap_or_default().trim().to_owned();
    assert!(!address.is_empty(), "no published port for {container}");
    server.url = format!("http://{address}");
    server
}

fn start_native() -> ClickHouseServer {
    let dir = scratch_dir("e2e-native");
    // A free loopback port: bind to port 0, then release it for the server.
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .and_then(|l| l.local_addr())
        .expect("free port")
        .port();
    let data = dir.join("data");
    let config_d = dir.join("config.d");
    std::fs::create_dir_all(&config_d).unwrap();
    std::fs::write(
        config_d.join("storage-test.xml"),
        STORAGE_TEMPLATE.replace("@DATA_DIR@", data.to_str().unwrap()),
    )
    .unwrap();
    // Only what a bare server needs; the user mirrors the container's.
    std::fs::write(
        dir.join("config.xml"),
        format!(
            r#"<clickhouse>
    <logger><level>warning</level><console>1</console></logger>
    <listen_host>127.0.0.1</listen_host>
    <http_port>{port}</http_port>
    <path>{data}/</path>
    <tmp_path>{data}/tmp/</tmp_path>
    <user_files_path>{data}/user_files/</user_files_path>
    <default_profile>default</default_profile>
    <profiles><default/>{LIMITED_PROFILE}</profiles>
    <quotas><default/></quotas>
    <users>
        <{USER}>
            <password>{PASSWORD}</password>
            <networks><ip>::/0</ip></networks>
            <profile>default</profile>
            <quota>default</quota>
        </{USER}>
        {LIMITED_USER_XML}
    </users>
</clickhouse>
"#,
            data = data.display()
        ),
    )
    .unwrap();
    let log = std::fs::File::create(dir.join("server.log")).unwrap();
    let child = Command::new(native_binary())
        .arg("server")
        .arg(format!(
            "--config-file={}",
            dir.join("config.xml").display()
        ))
        .stdout(log.try_clone().unwrap())
        .stderr(log)
        .spawn()
        .expect("start clickhouse server");
    ClickHouseServer {
        url: format!("http://127.0.0.1:{port}"),
        runtime: Runtime::Native { child },
    }
}

async fn wait_until_ready(url: &str) {
    let client = reqwest::Client::new();
    let deadline = Instant::now() + Duration::from_secs(120);
    loop {
        let ready = client
            .get(format!("{url}/ping"))
            .timeout(Duration::from_secs(2))
            .send()
            .await
            .is_ok_and(|response| response.status().is_success());
        if ready {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "ClickHouse at {url} was not ready within 120s"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

/// Run one statement over the HTTP interface; the optional body carries
/// `INSERT ... FORMAT JSONEachRow` data.
pub async fn execute(server: &ClickHouseServer, sql: &str, data: Option<&str>) -> String {
    execute_as(server, USER, sql, data).await
}

pub async fn execute_as(
    server: &ClickHouseServer,
    user: &str,
    sql: &str,
    data: Option<&str>,
) -> String {
    let request = reqwest::Client::new()
        .post(&server.url)
        .basic_auth(user, Some(PASSWORD));
    let request = match data {
        Some(data) => request.query(&[("query", sql)]).body(data.to_owned()),
        None => request.body(sql.to_owned()),
    };
    let response = request.send().await.expect("clickhouse request");
    let status = response.status();
    let body = response.text().await.expect("clickhouse response");
    assert!(status.is_success(), "clickhouse rejected {sql:.80}: {body}");
    body
}

/// The product DDL, in apply order.
pub fn product_ddl() -> Vec<(String, String)> {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("clickhouse/schema");
    let mut files: Vec<PathBuf> = std::fs::read_dir(&dir)
        .expect("read clickhouse/schema")
        .map(|entry| entry.expect("schema entry").path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "sql"))
        .collect();
    files.sort();
    files
        .into_iter()
        .map(|path| {
            let name = path.file_name().unwrap().to_string_lossy().into_owned();
            (name, std::fs::read_to_string(&path).expect("read schema"))
        })
        .collect()
}
