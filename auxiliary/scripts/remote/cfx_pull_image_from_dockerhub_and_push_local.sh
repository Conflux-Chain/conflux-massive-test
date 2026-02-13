#!/usr/bin/env bash
set -euo pipefail

REMOTE_IMAGE_TAG="${1:-lylcx2007/conflux-node:latest}"
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

wait_registry_ready() {
  local registry_host="$1"
  for _ in $(seq 1 40); do
    if curl -fsS "http://${registry_host}:${REGISTRY_PORT}/v2/" >/dev/null; then
      break
    fi
    sleep 3
  done
}

registry_image="localhost:${REGISTRY_PORT}/${REGISTRY_IMAGE}"

ensure_registry_running
wait_registry_ready "localhost"
sudo docker pull "${REMOTE_IMAGE_TAG}"
sudo docker tag "${REMOTE_IMAGE_TAG}" "${IMAGE_TAG}"
sudo docker tag "${REMOTE_IMAGE_TAG}" "${registry_image}"
sudo docker push "${registry_image}"
