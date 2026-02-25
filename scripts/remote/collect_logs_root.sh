#!/usr/bin/env bash
set -euo pipefail

INDEX="${1:?node index required}"
IMAGE_TAG="${2:-conflux-node:latest}"
CONTAINER="conflux_node_${INDEX}"

LOG_DIR="/root/log${INDEX}"
if [ -f "/home/ubuntu/log${INDEX}/conflux.log" ] || [ -f "/home/ubuntu/log${INDEX}/metrics.log" ]; then
  LOG_DIR="/home/ubuntu/log${INDEX}"
elif [ ! -d "${LOG_DIR}" ] && [ -d "/home/ubuntu/log${INDEX}" ]; then
  LOG_DIR="/home/ubuntu/log${INDEX}"
fi

OUTPUT_BASE="/root"
case "${LOG_DIR}" in
  /home/*) OUTPUT_BASE="/home/ubuntu" ;;
esac

if docker ps -aq --filter "name=^${CONTAINER}$" | grep -q .; then
  docker stop "$CONTAINER" || true
fi

rm -rf "${OUTPUT_BASE}/output${INDEX}"
mkdir -p "${OUTPUT_BASE}/output${INDEX}"

docker run --rm --name "${CONTAINER}_collect" \
  -v "${LOG_DIR}:/root/log:ro" \
  -v "${OUTPUT_BASE}/output${INDEX}:/root/output" \
  -w /root \
  "$IMAGE_TAG" \
  /bin/bash -c "./collect_logs.sh"

# Compress output directory into a 7z archive (max compression). Prefer '7zz' if available.
ARCHIVE="${OUTPUT_BASE}/output${INDEX}.7z"
if command -v 7zz >/dev/null 2>&1; then
  7zz a -t7z -mx=9 -m0=lzma2 -ms=on "$ARCHIVE" "${OUTPUT_BASE}/output${INDEX}"
elif command -v 7z >/dev/null 2>&1; then
  7z a -t7z -mx=9 -m0=lzma2 -ms=on "$ARCHIVE" "${OUTPUT_BASE}/output${INDEX}"
else
  echo "Warning: 7z not available, skipping compression" >&2
fi
# Remove uncompressed output to save space only if the archive was successfully created
# if [ -s "${ARCHIVE}" ]; then
#   rm -rf "${OUTPUT_BASE}/output${INDEX}"
# else
#   echo "Warning: Archive ${ARCHIVE} missing or empty; keeping ${OUTPUT_BASE}/output${INDEX}" >&2
# fi