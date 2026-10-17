#!/usr/bin/env bash
set -euo pipefail

flox_version="1.15.0"
flox_sha256="9eb747a4f3d81fc1b944dfa83b313f703248c009df740be495568b621edc3d4c"
flox_deb="/tmp/flox-${flox_version}.x86_64-linux.deb"

sudo apt-get update
sudo apt-get install --yes ca-certificates curl

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
  bash -c 'clickhouse local --version && tofu version && packer version && gcloud version'

sudo tee /etc/lighthouse-benchmark-image >/dev/null <<EOF
flox_version=${flox_version}
clickhouse_version=26.8.2.7
EOF

rm -f "${flox_deb}" /tmp/toudai-manifest.toml \
  /tmp/toudai-manifest.lock /tmp/toudai-env.json
sudo apt-get clean
sudo rm -rf /var/lib/apt/lists/*
if command -v cloud-init >/dev/null 2>&1; then
  sudo cloud-init clean --logs --machine-id
fi
