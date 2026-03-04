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

# If profiler was started for this node, wait briefly for flamegraph artifacts to finish writing.
if ls "${LOG_DIR}"/flame_start_*.txt >/dev/null 2>&1; then
  for _ in $(seq 1 120); do
    if ls "${LOG_DIR}"/flame_exit_*.txt >/dev/null 2>&1 || ls "${LOG_DIR}"/*.svg >/dev/null 2>&1; then
      break
    fi
    sleep 2
  done
fi

# Ensure profiler artifacts are collected even if image's collect_logs.sh does not include them.
cp "${LOG_DIR}"/*.svg "${OUTPUT_BASE}/output${INDEX}/" 2>/dev/null || true
cp "${LOG_DIR}"/perf.data "${OUTPUT_BASE}/output${INDEX}/" 2>/dev/null || true
cp "${LOG_DIR}"/*.err "${OUTPUT_BASE}/output${INDEX}/" 2>/dev/null || true
cp "${LOG_DIR}"/flame_start_*.txt "${OUTPUT_BASE}/output${INDEX}/" 2>/dev/null || true
cp "${LOG_DIR}"/flame_exit_*.txt "${OUTPUT_BASE}/output${INDEX}/" 2>/dev/null || true
cp "${LOG_DIR}"/*.perf.err "${OUTPUT_BASE}/output${INDEX}/" 2>/dev/null || true
cp "${LOG_DIR}"/flame_cmd_*.out "${OUTPUT_BASE}/output${INDEX}/" 2>/dev/null || true
cp "${LOG_DIR}"/start_profiler_*.sh "${OUTPUT_BASE}/output${INDEX}/" 2>/dev/null || true

# Compress output directory into a 7z archive (max compression). Prefer '7zz' if available.
# ARCHIVE="${OUTPUT_BASE}/output${INDEX}.7z"
# if command -v 7zz >/dev/null 2>&1; then
#   7zz a -t7z -mx=9 -m0=lzma2 -ms=on "$ARCHIVE" "${OUTPUT_BASE}/output${INDEX}"
# elif command -v 7z >/dev/null 2>&1; then
#   7z a -t7z -mx=9 -m0=lzma2 -ms=on "$ARCHIVE" "${OUTPUT_BASE}/output${INDEX}"
# else
#   echo "Warning: 7z not available, skipping compression" >&2
# fi

COMPRESS_BIN=""
if command -v 7zz >/dev/null 2>&1; then
  COMPRESS_BIN="7zz"
elif command -v 7z >/dev/null 2>&1; then
  COMPRESS_BIN="7z"
fi

if [ -n "${COMPRESS_BIN}" ]; then
  while IFS= read -r -d '' file; do
    "${COMPRESS_BIN}" a -t7z -mx=9 -m0=lzma2 -ms=on -bso0 -bsp0 "${file}.7z" "${file}" >/dev/null 2>&1 && rm -f "${file}" || true
  done < <(find "${OUTPUT_BASE}/output${INDEX}" -maxdepth 1 -type f -print0)
else
  echo "Warning: 7z not found, skip compression for output${INDEX}" >&2
fi


# Remove uncompressed output to save space only if the archive was successfully created
# if [ -s "${ARCHIVE}" ]; then
#   rm -rf "${OUTPUT_BASE}/output${INDEX}"
# else
#   echo "Warning: Archive ${ARCHIVE} missing or empty; keeping ${OUTPUT_BASE}/output${INDEX}" >&2
# fi