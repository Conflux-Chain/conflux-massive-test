#!/usr/bin/env bash
set -euo pipefail

REGISTRY_HOST="${1:?registry host is required}"
IMAGE_TAG="${2:-conflux-node:latest}"
REGISTRY_IMAGE="${3:-conflux-node:base}"
REGISTRY_PORT="${4:-5000}"

ensure_registry_running() {
  sudo systemctl start docker
  sudo systemctl is-active --quiet docker || sudo systemctl restart docker

  if ! sudo docker ps -a --format '{{.Names}}' | grep -qx conflux-registry; then
    sudo docker run -d --restart=always --name conflux-registry \
      -p "${REGISTRY_PORT}:${REGISTRY_PORT}" \
      -v /opt/registry/data:/var/lib/registry \
      registry:2
  fi

  sudo docker start conflux-registry >/dev/null 2>&1 || true
}

configure_insecure_registry() {
  local daemon_json
  daemon_json=$(printf '{"insecure-registries":["%s:%s","localhost:%s"]}' "${REGISTRY_HOST}" "${REGISTRY_PORT}" "${REGISTRY_PORT}")
  sudo mkdir -p /etc/docker
  printf '%s\n' "${daemon_json}" | sudo tee /etc/docker/daemon.json >/dev/null
  sudo systemctl restart docker
}

wait_registry_ready() {
  local registry_host="$1"
  for _ in $(seq 1 40); do
    if curl -fsS "http://${registry_host}:${REGISTRY_PORT}/v2/" >/dev/null; then
      break
    fi
    sleep 3
  done
}

remote_registry_image="${REGISTRY_HOST}:${REGISTRY_PORT}/${REGISTRY_IMAGE}"
local_registry_image="localhost:${REGISTRY_PORT}/${REGISTRY_IMAGE}"

configure_insecure_registry
ensure_registry_running
wait_registry_ready "${REGISTRY_HOST}"
sudo docker pull "${remote_registry_image}"
sudo docker tag "${remote_registry_image}" "${IMAGE_TAG}"
sudo docker tag "${remote_registry_image}" "${local_registry_image}"
wait_registry_ready "localhost"
sudo docker push "${local_registry_image}"
