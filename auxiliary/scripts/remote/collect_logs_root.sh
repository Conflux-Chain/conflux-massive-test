#!/usr/bin/env bash
set -euo pipefail

INDEX="${1:?node index required}"
IMAGE_TAG="${2:-conflux-node:latest}"
CONTAINER="conflux_node_${INDEX}"

if docker ps -aq --filter "name=^${CONTAINER}$" | grep -q .; then
  # Send SIGINT first to give flamegraph wrapper time to finalize and write the SVG.
  docker kill --signal=SIGINT "$CONTAINER" || true
  sleep 5
  # If still running, force remove
  if docker ps -aq --filter "name=^${CONTAINER}$" | grep -q .; then
    docker rm -f "$CONTAINER" || true
  fi
fi

rm -rf "/root/output${INDEX}"
mkdir -p "/root/output${INDEX}"

docker run --rm --name "${CONTAINER}_collect" \
  -v "/root/log${INDEX}:/root/log:ro" \
  -v "/root/output${INDEX}:/root/output" \
  -w /root \
  "$IMAGE_TAG" \
  /bin/bash -c "./collect_logs.sh"

# Ensure profiler artifacts are collected even if the container image's collect script is outdated.
cp "/root/log${INDEX}"/*.svg "/root/output${INDEX}/" 2>/dev/null || true
cp "/root/log${INDEX}"/perf.data "/root/output${INDEX}/" 2>/dev/null || true
cp "/root/log${INDEX}"/*.err "/root/output${INDEX}/" 2>/dev/null || true
cp "/root/log${INDEX}"/flame_start_*.txt "/root/output${INDEX}/" 2>/dev/null || true
cp "/root/log${INDEX}"/flame_exit_*.txt "/root/output${INDEX}/" 2>/dev/null || true
cp "/root/log${INDEX}"/*.perf.err "/root/output${INDEX}/" 2>/dev/null || true
cp "/root/log${INDEX}"/flame_cmd_*.out "/root/output${INDEX}/" 2>/dev/null || true
cp "/root/log${INDEX}"/start_profiler_*.sh "/root/output${INDEX}/" 2>/dev/null || true
