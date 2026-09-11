#!/usr/bin/env bash
set -euo pipefail

exec > >(tee -a /var/log/lighthouse-stage-a-startup.log) 2>&1

install -d -m 0755 /var/lib/lighthouse-stage-a
local_ssd="$(find /dev/disk/by-id -maxdepth 1 -type l -name 'google-local-nvme-ssd-*' | sort | head -n 1)"
if [[ -z "${local_ssd}" ]]; then
  echo "integrated Local SSD was not found" >&2
  exit 1
fi

# The serving disk is disposable by design; initialize it on every new Spot VM.
if ! blkid "${local_ssd}" >/dev/null 2>&1; then
  mkfs.ext4 -F -m 0 -E lazy_itable_init=0,lazy_journal_init=0 "${local_ssd}"
fi
install -d -o lighthouse -g lighthouse /mnt/disks/clickhouse
mount -o noatime,nodiratime "${local_ssd}" /mnt/disks/clickhouse
chown lighthouse:lighthouse /mnt/disks/clickhouse

{
  date --utc --iso-8601=seconds
  cat /etc/lighthouse-benchmark-image
  lscpu
  free --bytes
  lsblk --bytes --output NAME,TYPE,SIZE,FSTYPE,MOUNTPOINTS,MODEL
  findmnt /mnt/disks/clickhouse
} > /var/lib/lighthouse-stage-a/hardware.txt

touch /var/lib/lighthouse-stage-a/READY
