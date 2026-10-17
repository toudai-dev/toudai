#!/usr/bin/env bash
set -euo pipefail
exec > >(tee -a /var/log/lighthouse-stage-b-startup.log) 2>&1

state=/var/lib/lighthouse-stage-b
install -d -m 0755 "${state}" /etc/clickhouse-server
disk="$(find /dev/disk/by-id -maxdepth 1 -type l -name 'google-local-nvme-ssd-*' | sort | head -n 1)"
[[ -n "${disk}" ]] || { echo "integrated Titanium SSD not found" >&2; exit 1; }

# Serving state is intentionally disposable, but leaves are on-demand so a run
# cannot lose it to Spot preemption.
if ! blkid "${disk}" >/dev/null 2>&1; then
  mkfs.ext4 -F -m 0 -E lazy_itable_init=0,lazy_journal_init=0 "${disk}"
fi
install -d -o lighthouse -g lighthouse /mnt/disks/clickhouse
mountpoint -q /mnt/disks/clickhouse || mount -o noatime,nodiratime "${disk}" /mnt/disks/clickhouse
chown lighthouse:lighthouse /mnt/disks/clickhouse

metadata=http://metadata.google.internal/computeMetadata/v1/instance/attributes
curl -fsS -H 'Metadata-Flavor: Google' "${metadata}/clickhouse-config" | base64 -d > /etc/clickhouse-server/config.xml
curl -fsS -H 'Metadata-Flavor: Google' "${metadata}/clickhouse-users" | base64 -d > /etc/clickhouse-server/clickhouse-users.xml
chown -R lighthouse:lighthouse /etc/clickhouse-server

cat > /etc/systemd/system/lighthouse-clickhouse.service <<'UNIT'
[Unit]
Description=Toudai ClickHouse 26.8.2.7
After=network-online.target
[Service]
User=lighthouse
Group=lighthouse
Environment=FLOX_DISABLE_METRICS=true
ExecStart=/usr/bin/flox activate --dir /opt/lighthouse -- clickhouse server --config-file=/etc/clickhouse-server/config.xml
Restart=on-failure
LimitNOFILE=1048576
[Install]
WantedBy=multi-user.target
UNIT

{
  date --utc --iso-8601=seconds; cat /etc/lighthouse-benchmark-image
  lscpu; free --bytes; lsblk --bytes --output NAME,TYPE,SIZE,FSTYPE,MOUNTPOINTS,MODEL
  findmnt /mnt/disks/clickhouse; flox activate --dir /opt/lighthouse -- clickhouse local --version
} > "${state}/hardware.txt"

# The metadata agent copies startup-script output to the serial console, so
# mirror the host evidence there for retrieval when IAP SSH is unavailable.
cat "${state}/hardware.txt"
systemctl daemon-reload
systemctl enable --now lighthouse-clickhouse.service
for _attempt in $(seq 1 60); do
  if flox activate --dir /opt/lighthouse -- clickhouse client --query "SELECT 1" >/dev/null 2>&1; then
    touch "${state}/READY"
    exit 0
  fi
  sleep 2
done
systemctl status lighthouse-clickhouse.service --no-pager >&2 || true
echo "ClickHouse did not become ready within 120 seconds" >&2
exit 1
