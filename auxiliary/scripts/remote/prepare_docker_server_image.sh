#!/usr/bin/env bash
set -euo pipefail

apt-get update -y
apt-get install -y docker.io ca-certificates curl p7zip-full
# Ensure '7zz' command is available (some distributions provide '7z' only)
if ! command -v 7zz >/dev/null 2>&1; then
  if command -v 7z >/dev/null 2>&1; then
    ln -sf /usr/bin/7z /usr/bin/7zz || true
  fi
fi
systemctl enable --now docker

# Private registry (insecure HTTP on :5000)
mkdir -p /opt/registry/data
docker pull registry:2
docker rm -f conflux-registry >/dev/null 2>&1 || true
docker run -d --restart=always \
	--name conflux-registry \
	-p 5000:5000 \
	-v /opt/registry/data:/var/lib/registry \
	registry:2

# Preload Conflux node image into the registry
docker pull lylcx2007/conflux-node:latest
docker tag lylcx2007/conflux-node:latest conflux-node:base
docker tag conflux-node:base localhost:5000/conflux-node:base
docker push localhost:5000/conflux-node:base

# Sanity: registry can serve the image
docker pull localhost:5000/conflux-node:base

# Sanity: can pull other images
# docker pull busybox:latest

echo "LABEL=cloudimg-rootfs / ext4 defaults,noatime,nodiratime,barrier=0 0 0" > /tmp/fstab
cp /tmp/fstab /etc/fstab
