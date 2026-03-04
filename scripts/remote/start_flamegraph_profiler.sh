#!/usr/bin/env bash
set -euo pipefail

INDEX="${1:?node index is required}"
DURATION_S="${2:-60}"
CONTAINER_NAME="${3:?container name is required}"
FLAMEGRAPH_IMAGE_TAG="${4:?flamegraph image tag is required}"

LOG_DIR="/root/log${INDEX}"
SCRIPT_PATH="${LOG_DIR}/start_profiler_${INDEX}.sh"

mkdir -p "${LOG_DIR}"

cat >"${SCRIPT_PATH}" <<EOF
#!/bin/bash
set -euo pipefail

echo "SCRIPT_STARTED \\$(date '+%Y-%m-%d %H:%M:%S')" >> "${LOG_DIR}/flame_cmd_${INDEX}.out" || true
docker image inspect "${FLAMEGRAPH_IMAGE_TAG}" >/dev/null 2>&1 || echo "IMAGE_MISSING ${FLAMEGRAPH_IMAGE_TAG}" >> "${LOG_DIR}/flame_cmd_${INDEX}.out" || true

for i in \\$(seq 1 60); do
  docker ps -q --filter "name=${CONTAINER_NAME}" | grep -q . && break || sleep 1
done

if [ -z "\\$(docker ps -q --filter \"name=${CONTAINER_NAME}\")" ]; then
  echo "No such container: ${CONTAINER_NAME}" >> "${LOG_DIR}/flame_cmd_${INDEX}.out"
  exit 0
fi

echo "DOCKER_RUN_START \\$(date '+%Y-%m-%d %H:%M:%S')" >> "${LOG_DIR}/flame_cmd_${INDEX}.out" || true
docker run --rm \\
  --pid="container:${CONTAINER_NAME}" \\
  --privileged \\
  -v "${LOG_DIR}:/root/log" \\
  -w /root \\
  "${FLAMEGRAPH_IMAGE_TAG}" \\
  /bin/bash -lc "echo 'profiler started \\$(date '+%Y-%m-%d %H:%M:%S')' > /root/log/flame_start_${INDEX}.txt; perf record -o /root/log/perf.data -g -p 1 -- sleep ${DURATION_S} 2> /root/log/flame_${INDEX}.perf.err || true; flamegraph --perfdata /root/log/perf.data --output /root/log/flame_${INDEX}.svg 2> /root/log/flame_${INDEX}.err || true; echo \\$? > /root/log/flame_exit_${INDEX}.txt"
echo "DOCKER_RUN_EXIT \\$(date '+%Y-%m-%d %H:%M:%S')" >> "${LOG_DIR}/flame_cmd_${INDEX}.out" || true
EOF

chmod +x "${SCRIPT_PATH}"
nohup bash "${SCRIPT_PATH}" > "${LOG_DIR}/flame_cmd_${INDEX}.out" 2>&1 &
