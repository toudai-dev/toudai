#!/usr/bin/env bash
# Dependency-only image provisioner for the AWS benchmark nodes.
#
# Mirrors benchmark/cloud/gcp/image/provision.sh so both clouds run the same
# locked ClickHouse closure. Adds the Session Manager agent, which the official
# Debian AMI does not ship, so no inbound management port is required.
set -euo pipefail

flox_version="1.15.0"
flox_sha256="9eb747a4f3d81fc1b944dfa83b313f703248c009df740be495568b621edc3d4c"
flox_deb="/tmp/flox-${flox_version}.x86_64-linux.deb"

# AWS publishes only a rolling "latest" agent; pin the digest observed at image
# build time so an unexpected rotation fails the build loudly instead of
# silently changing the management plane.
ssm_agent_sha256="777df4e0ac4bbc8d7d1b159db76cdb6614666bcc73141874414b8be68fb9cba3"
ssm_agent_deb="/tmp/amazon-ssm-agent.deb"

sudo apt-get update
sudo apt-get install --yes ca-certificates curl

curl --fail --location --silent --show-error \
  "https://s3.amazonaws.com/ec2-downloads-windows/SSMAgent/latest/debian_amd64/amazon-ssm-agent.deb" \
  --output "${ssm_agent_deb}"
printf '%s  %s\n' "${ssm_agent_sha256}" "${ssm_agent_deb}" | sha256sum --check
sudo apt-get install --yes "${ssm_agent_deb}"
sudo systemctl enable amazon-ssm-agent.service

curl --fail --location --silent --show-error \
  "https://downloads.flox.dev/by-env/stable/deb/flox-${flox_version}.x86_64-linux.deb" \
  --output "${flox_deb}"
printf '%s  %s\n' "${flox_sha256}" "${flox_deb}" | sha256sum --check
sudo apt-get install --yes "${flox_deb}"

sudo useradd --create-home --shell /bin/bash lighthouse
sudo install --directory --owner lighthouse --group lighthouse /opt/lighthouse/.flox/env
sudo chown --recursive lighthouse:lighthouse /opt/lighthouse
sudo install --owner lighthouse --group lighthouse --mode 0644 \
  /tmp/toudai-manifest.toml /opt/lighthouse/.flox/env/manifest.toml
sudo install --owner lighthouse --group lighthouse --mode 0644 \
  /tmp/toudai-manifest.lock /opt/lighthouse/.flox/env/manifest.lock
sudo install --owner lighthouse --group lighthouse --mode 0644 \
  /tmp/toudai-env.json /opt/lighthouse/.flox/env.json

sudo --user lighthouse --set-home env FLOX_DISABLE_METRICS=true \
  flox activate --dir /opt/lighthouse -- \
  bash -c 'clickhouse local --version && aws --version'

sudo tee /etc/lighthouse-benchmark-image >/dev/null <<EOF
cloud=aws
flox_version=${flox_version}
clickhouse_version=26.8.2.7
EOF

rm -f "${flox_deb}" "${ssm_agent_deb}" /tmp/toudai-manifest.toml \
  /tmp/toudai-manifest.lock /tmp/toudai-env.json
sudo apt-get clean
sudo rm -rf /var/lib/apt/lists/*

# Only cloud-init's logs and state are cleared, never the machine identity.
# `cloud-init clean --machine-id` removes /etc/machine-id, and systemd on
# Debian 12 refuses to boot when it is missing while /etc is still read-only:
#
#   System cannot boot: Missing /etc/machine-id and /etc is mounted read-only.
#
# Instances launched from an image generalized that way never reach
# network-online.target, so the Session Manager agent never registers and the
# node is unusable. The first boot regenerates the identity on its own, which is
# what an EC2 image is expected to do.
if command -v cloud-init >/dev/null 2>&1; then
  sudo cloud-init clean --logs
fi
