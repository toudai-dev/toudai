#!/usr/bin/env bash
set -euo pipefail
exec > >(tee -a /var/log/lighthouse-stage-b-startup.log) 2>&1

state=/var/lib/lighthouse-stage-b
install -d -m 0755 "${state}"
metadata=http://metadata.google.internal/computeMetadata/v1/instance/attributes
{
  date --utc --iso-8601=seconds; cat /etc/lighthouse-benchmark-image
  lscpu; free --bytes; lsblk --bytes --output NAME,TYPE,SIZE,FSTYPE,MOUNTPOINTS,MODEL
  printf 'coordinator_ip='; curl -fsS -H 'Metadata-Flavor: Google' "${metadata}/coordinator-ip"; echo
  printf 'leaf_ips='; curl -fsS -H 'Metadata-Flavor: Google' "${metadata}/leaf-ips"; echo
} > "${state}/hardware.txt"

# The metadata agent copies startup-script output to the serial console, so
# mirror the host evidence there for retrieval when IAP SSH is unavailable.
cat "${state}/hardware.txt"
touch "${state}/READY"
