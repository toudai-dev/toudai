#!/usr/bin/env bash
set -euo pipefail

exec > >(tee -a /var/log/lighthouse-stage-a-startup.log) 2>&1

install -d -m 0755 /var/lib/lighthouse-stage-a
{
  date --utc --iso-8601=seconds
  cat /etc/lighthouse-benchmark-image
  lscpu
  free --bytes
  lsblk --bytes --output NAME,TYPE,SIZE,FSTYPE,MOUNTPOINTS,MODEL
} > /var/lib/lighthouse-stage-a/hardware.txt

touch /var/lib/lighthouse-stage-a/READY
