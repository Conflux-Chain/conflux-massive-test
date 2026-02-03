#!/usr/bin/env bash
set -euo pipefail

apt-get update -y
apt-get install -y docker.io ca-certificates curl
systemctl enable --now docker

# Allow perf recordings from containers (required for flamegraph/perf)
# Set to 1 to permit user-space perf events while keeping some kernel protections.
echo 'kernel.perf_event_paranoid=1' > /etc/sysctl.d/99-perf.conf
sysctl --system

echo "LABEL=cloudimg-rootfs / ext4 defaults,noatime,nodiratime,barrier=0 0 0" > /tmp/fstab
cp /tmp/fstab /etc/fstab
