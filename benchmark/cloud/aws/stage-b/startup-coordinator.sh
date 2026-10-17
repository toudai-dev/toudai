#!/usr/bin/env bash
# Query coordinator startup for the AWS Stage B topology.
#
# The coordinator holds no corpus data; it only reduces leaf results, so its
# state directory stays on the root volume.
set -Eeuo pipefail
exec > >(tee -a /var/log/lighthouse-stage-b-startup.log) 2>&1

state=/var/lib/lighthouse-stage-b

# See startup-leaf.sh: this timer is what bounds the cost of an abandoned node.
shutdown -h "+${TOUDAI_MAX_LIFETIME_MINUTES}" "toudai experiment 015 stage B lifetime reached"

install -d -m 0755 "${state}" /etc/clickhouse-server
install -d -o lighthouse -g lighthouse /var/lib/lighthouse-clickhouse

printf '%s' "${TOUDAI_CLICKHOUSE_CONFIG_B64}" | base64 -d > /etc/clickhouse-server/config.xml
printf '%s' "${TOUDAI_CLICKHOUSE_USERS_B64}" | base64 -d > /etc/clickhouse-server/clickhouse-users.xml
chown -R lighthouse:lighthouse /etc/clickhouse-server

# Resolve the ClickHouse binary once, before the server starts. Two concurrent
# first activations of the same Flox environment race on its activation state
# directory and the loser exits with EACCES, so probing through `flox activate`
# while systemd starts the server restarted the server twice before it settled.
# One activation, reused, removes the race.
clickhouse_bin="$(flox activate --dir /opt/lighthouse -- bash -c 'command -v clickhouse')"
[[ -x "${clickhouse_bin}" ]] || { echo "clickhouse binary not found in the image" >&2; exit 1; }

cat > /etc/systemd/system/lighthouse-clickhouse.service <<'UNIT'
[Unit]
Description=Toudai ClickHouse 26.8.2.7 coordinator
After=network-online.target
[Service]
User=lighthouse
Group=lighthouse
Environment=FLOX_DISABLE_METRICS=true
# Flox stores activation state under the runtime directory, which does not exist
# for a user without a login session; without it the first activation can fail
# with EACCES and the server restarts before it settles.
RuntimeDirectory=lighthouse-clickhouse
RuntimeDirectoryMode=0700
Environment=XDG_RUNTIME_DIR=/run/lighthouse-clickhouse
ExecStart=/usr/bin/flox activate --dir /opt/lighthouse -- clickhouse server --config-file=/etc/clickhouse-server/config.xml
Restart=on-failure
LimitNOFILE=1048576
[Install]
WantedBy=multi-user.target
UNIT

{
  date --utc --iso-8601=seconds
  cat /etc/lighthouse-benchmark-image
  imds_token="$(curl -fsS -X PUT 'http://169.254.169.254/latest/api/token' -H 'X-aws-ec2-metadata-token-ttl-seconds: 300')"
  for key in instance-id instance-type placement/availability-zone ami-id; do
    printf '%s=' "${key}"
    curl -fsS -H "X-aws-ec2-metadata-token: ${imds_token}" "http://169.254.169.254/latest/meta-data/${key}"
    echo
  done
  lscpu
  free --bytes
  lsblk --bytes --output NAME,TYPE,SIZE,FSTYPE,MOUNTPOINTS,MODEL
  "${clickhouse_bin}" local --version
} > "${state}/hardware.txt"

# EC2 exposes the same evidence through `aws ec2 get-console-output`, so mirror
# it there for retrieval when Session Manager is unavailable.
cat "${state}/hardware.txt"
systemctl daemon-reload
systemctl enable --now lighthouse-clickhouse.service
for _attempt in $(seq 1 60); do
  if "${clickhouse_bin}" client --query "SELECT 1" >/dev/null 2>&1; then
    touch "${state}/READY"
    exit 0
  fi
  sleep 2
done
systemctl status lighthouse-clickhouse.service --no-pager >&2 || true
echo "ClickHouse did not become ready within 120 seconds" >&2
exit 1
