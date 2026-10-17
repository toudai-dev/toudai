#!/usr/bin/env bash
# Serving leaf startup for the AWS Stage B topology.
#
# ClickHouse state lives on the instance-store NVMe volume, which is destroyed
# with the host. Leaves are on-demand instances so a run cannot lose its serving
# state to Spot preemption.
set -Eeuo pipefail
exec > >(tee -a /var/log/lighthouse-stage-b-startup.log) 2>&1

state=/var/lib/lighthouse-stage-b

# The halt is scheduled before anything else can fail. EC2 has no
# provider-enforced maximum run duration, so this timer is the only thing that
# bounds the cost of a node that never becomes healthy; the topology sets
# `instance_initiated_shutdown_behavior = "terminate"`, so the halt also deletes
# the instance and its root volume.
shutdown -h "+${TOUDAI_MAX_LIFETIME_MINUTES}" "lighthouse experiment 015 stage B lifetime reached"

install -d -m 0755 "${state}" /etc/clickhouse-server

# Nitro instance-store volumes carry a stable by-id alias that the EBS root
# volume never has. AWS publishes several aliases for the same device (a plain
# name, a `-ns-1` namespace alias, and a `_1` alias), so the targets are resolved
# and deduplicated before counting. The fallback selects the only non-root block
# device so a renamed alias fails loudly rather than silently mounting the wrong
# disk.
mapfile -t store_disks < <(
  find /dev/disk/by-id -maxdepth 1 -type l -name 'nvme-Amazon_EC2_NVMe_Instance_Storage_*' -print0 2>/dev/null |
    xargs -0 -r readlink -f | sort -u
)
if (( ${#store_disks[@]} == 0 )); then
  root_source="$(findmnt -no SOURCE /)"
  root_disk="/dev/$(lsblk -dno PKNAME "${root_source}" 2>/dev/null | head -n 1)"
  [[ "${root_disk}" != "/dev/" ]] || root_disk="${root_source}"
  mapfile -t store_disks < <(lsblk -dno NAME,TYPE | awk '$2 == "disk" { print "/dev/" $1 }' | grep -vx "${root_disk}")
fi
# i4i.4xlarge exposes exactly one store volume. A different count means the
# shape changed and disposable capacity would be silently left unused.
(( ${#store_disks[@]} == 1 )) || { echo "expected exactly one instance-store device, found ${#store_disks[@]}" >&2; exit 1; }
disk="${store_disks[0]}"
echo "instance store device: ${disk}"

if ! blkid "${disk}" >/dev/null 2>&1; then
  # lazy_itable_init keeps its default: the i4i store is 3.75 TB, and forcing
  # eager inode-table initialisation would add tens of minutes of billed
  # instance time for a filesystem that is discarded with the host anyway.
  mkfs.ext4 -F -m 0 "${disk}"
fi
install -d -o lighthouse -g lighthouse /mnt/disks/clickhouse
mountpoint -q /mnt/disks/clickhouse || mount -o noatime,nodiratime "${disk}" /mnt/disks/clickhouse
chown lighthouse:lighthouse /mnt/disks/clickhouse

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
Description=Lighthouse ClickHouse 26.8.2.7
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
  findmnt /mnt/disks/clickhouse
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
