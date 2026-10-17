#!/usr/bin/env bash
# Load generator startup for the AWS Stage B topology.
#
# The load generator runs no ClickHouse process; it only needs the endpoint map
# so the benchmark scripts can be driven without a separate inventory lookup.
set -Eeuo pipefail
exec > >(tee -a /var/log/lighthouse-stage-b-startup.log) 2>&1

state=/var/lib/lighthouse-stage-b

# See startup-leaf.sh: this timer is what bounds the cost of an abandoned node.
shutdown -h "+${TOUDAI_MAX_LIFETIME_MINUTES}" "lighthouse experiment 015 stage B lifetime reached"

install -d -m 0755 "${state}"

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
  printf 'coordinator_ip=%s\n' "${TOUDAI_COORDINATOR_IP}"
  printf 'leaf_ips=%s\n' "${TOUDAI_LEAF_IPS}"
  flox activate --dir /opt/lighthouse -- clickhouse local --version
} > "${state}/hardware.txt"

# EC2 exposes the same evidence through `aws ec2 get-console-output`, so mirror
# it there for retrieval when Session Manager is unavailable.
cat "${state}/hardware.txt"
touch "${state}/READY"
