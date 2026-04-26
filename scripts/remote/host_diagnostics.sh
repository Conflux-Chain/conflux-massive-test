#!/usr/bin/env bash
set -euo pipefail

ACTION="${1:?action required}"
BASE_DIR="${2:-$HOME/host_diagnostics}"
CONTAINER_PREFIX="${3:-conflux_node_}"
SCRIPT_PATH="$(readlink -f "${BASH_SOURCE[0]}")"

STATE_DIR="${BASE_DIR}/state"
TRACE_DIR="${BASE_DIR}/trace"
OUTPUT_DIR="${BASE_DIR}/output"
PID_DIR="${STATE_DIR}/pids"
TRACE_PID_DIR="${STATE_DIR}/trace_pids"
TRACE_TARGET_DIR="${STATE_DIR}/trace_targets"
PACKAGE_DIR="${STATE_DIR}/package"

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing command: $1" >&2
    exit 1
  fi
}

ensure_layout() {
  mkdir -p "${STATE_DIR}" "${TRACE_DIR}" "${OUTPUT_DIR}" "${PID_DIR}" "${TRACE_PID_DIR}" "${TRACE_TARGET_DIR}"
}

sanitize_name() {
  printf '%s' "$1" | tr -c 'A-Za-z0-9._-' '_'
}

wait_for_stop() {
  local pid="$1"
  local attempts=50

  while kill -0 "${pid}" >/dev/null 2>&1 && [ "${attempts}" -gt 0 ]; do
    sleep 0.1
    attempts=$((attempts - 1))
  done
}

stop_process_from_file() {
  local pid_file="$1"
  local pid

  if [ ! -f "${pid_file}" ]; then
    return
  fi

  pid="$(cat "${pid_file}" 2>/dev/null || true)"
  if [ -n "${pid}" ] && kill -0 "${pid}" >/dev/null 2>&1; then
    kill "${pid}" >/dev/null 2>&1 || true
    wait_for_stop "${pid}"
  fi

  rm -f "${pid_file}"
}

stop_trace_processes() {
  if [ -d "${TRACE_PID_DIR}" ]; then
    for pid_file in "${TRACE_PID_DIR}"/*; do
      [ -e "${pid_file}" ] || continue
      stop_process_from_file "${pid_file}"
    done
  fi

  rm -f "${TRACE_TARGET_DIR}"/* 2>/dev/null || true
}

reset_base_dir() {
  stop_process_from_file "${PID_DIR}/ss.pid"
  stop_process_from_file "${PID_DIR}/mpstat.pid"
  stop_process_from_file "${PID_DIR}/monitor.pid"
  stop_trace_processes

  rm -rf "${BASE_DIR}"
  ensure_layout
}

copy_if_exists() {
  local source_path="$1"
  local target_dir="$2"

  if [ -f "${source_path}" ]; then
    cp "${source_path}" "${target_dir}/"
  fi
}

build_archive() {
  local archive_path="$1"

  if command -v 7z >/dev/null 2>&1; then
    (
      cd "${STATE_DIR}"
      7z a -t7z -mx=9 -m0=lzma2 -ms=on -bso0 -bsp0 "${archive_path}" "$(basename "${PACKAGE_DIR}")"
    )
    return 0
  fi

  return 1
}

run_ss_loop() {
  ensure_layout

  while true; do
    printf '===== %s =====\n' "$(date '+%F %T')"
    sudo ss -tanpimoe || true
    sleep 1
  done >> "${BASE_DIR}/ss_11000_$(hostname).log" 2>&1
}

run_mpstat_loop() {
  ensure_layout
  mpstat -P ALL 1 > "${BASE_DIR}/mpstat_$(hostname).txt" 2>&1
}

run_strace_monitor() {
  local container
  local trace_name
  local target_pid
  local target_file
  local tracer_pid_file
  local current_target_pid
  local tracer_pid
  local tracer_alive

  ensure_layout

  while true; do
    while IFS= read -r container; do
      [ -n "${container}" ] || continue
      case "${container}" in
        *_collect)
          continue
          ;;
      esac

      target_pid="$(sudo docker inspect -f '{{.State.Pid}}' "${container}" 2>/dev/null || true)"
      if [ -z "${target_pid}" ] || [ "${target_pid}" = "0" ]; then
        continue
      fi

      trace_name="$(sanitize_name "${container}")"
      target_file="${TRACE_TARGET_DIR}/${trace_name}.target"
      tracer_pid_file="${TRACE_PID_DIR}/${trace_name}.pid"
      current_target_pid=""
      tracer_alive=0

      if [ -f "${target_file}" ]; then
        current_target_pid="$(cat "${target_file}" 2>/dev/null || true)"
      fi

      if [ -f "${tracer_pid_file}" ]; then
        tracer_pid="$(cat "${tracer_pid_file}" 2>/dev/null || true)"
        if [ -n "${tracer_pid}" ] && kill -0 "${tracer_pid}" >/dev/null 2>&1; then
          tracer_alive=1
        fi
      fi

      if [ "${current_target_pid}" = "${target_pid}" ] && [ "${tracer_alive}" -eq 1 ]; then
        continue
      fi

      stop_process_from_file "${tracer_pid_file}"

      sudo strace -ff -ttt -T -o "${TRACE_DIR}/${trace_name}" -p "${target_pid}" >/dev/null 2>&1 &
      tracer_pid="$!"

      printf '%s\n' "${tracer_pid}" > "${tracer_pid_file}"
      printf '%s\n' "${target_pid}" > "${target_file}"
      printf '[%s] attached %s pid=%s\n' "$(date '+%F %T')" "${container}" "${target_pid}"
    done < <(sudo docker ps --format '{{.Names}}' --filter "name=^${CONTAINER_PREFIX}" || true)

    sleep 2
  done >> "${STATE_DIR}/monitor.log" 2>&1
}

start_all() {
  require_cmd sudo
  require_cmd docker
  require_cmd ss
  require_cmd mpstat
  require_cmd strace

  reset_base_dir

  nohup bash "${SCRIPT_PATH}" _ss "${BASE_DIR}" "${CONTAINER_PREFIX}" >/dev/null 2>&1 &
  printf '%s\n' "$!" > "${PID_DIR}/ss.pid"

  nohup bash "${SCRIPT_PATH}" _mpstat "${BASE_DIR}" "${CONTAINER_PREFIX}" >/dev/null 2>&1 &
  printf '%s\n' "$!" > "${PID_DIR}/mpstat.pid"

  nohup bash "${SCRIPT_PATH}" _monitor "${BASE_DIR}" "${CONTAINER_PREFIX}" >/dev/null 2>&1 &
  printf '%s\n' "$!" > "${PID_DIR}/monitor.pid"
}

collect_artifacts() {
  local archive_name="host_diagnostics_$(hostname).7z"

  ensure_layout

  stop_process_from_file "${PID_DIR}/ss.pid"
  stop_process_from_file "${PID_DIR}/mpstat.pid"
  stop_process_from_file "${PID_DIR}/monitor.pid"
  stop_trace_processes

  sudo chown -R "$(id -un):$(id -gn)" "${BASE_DIR}" >/dev/null 2>&1 || true

  rm -rf "${OUTPUT_DIR}"
  mkdir -p "${OUTPUT_DIR}"
  rm -rf "${PACKAGE_DIR}"
  mkdir -p "${PACKAGE_DIR}"

  cp -R "${TRACE_DIR}" "${PACKAGE_DIR}/trace"

  for file in "${BASE_DIR}"/ss_11000_*.log "${BASE_DIR}"/mpstat_*.txt; do
    [ -f "${file}" ] || continue
    cp "${file}" "${PACKAGE_DIR}/"
  done

  copy_if_exists "${STATE_DIR}/monitor.log" "${PACKAGE_DIR}"

  sudo docker ps -a --format '{{.Names}}\t{{.Status}}\t{{.Image}}' > "${PACKAGE_DIR}/docker_ps_a.txt" 2>&1 || true
  find "${BASE_DIR}" \
    \( -path "${OUTPUT_DIR}" -o -path "${PACKAGE_DIR}" \) -prune -o \
    -type f -print | sort > "${PACKAGE_DIR}/diagnostics_manifest.txt"

  if build_archive "${OUTPUT_DIR}/${archive_name}"; then
    rm -rf "${PACKAGE_DIR}"
  else
    printf '7z_unavailable\n' > "${OUTPUT_DIR}/status.txt"
    find "${PACKAGE_DIR}" -mindepth 1 -maxdepth 1 -exec mv {} "${OUTPUT_DIR}/" \;
    rm -rf "${PACKAGE_DIR}"
  fi
}

case "${ACTION}" in
  start)
    start_all
    ;;
  collect)
    collect_artifacts
    ;;
  _ss)
    run_ss_loop
    ;;
  _mpstat)
    run_mpstat_loop
    ;;
  _monitor)
    run_strace_monitor
    ;;
  *)
    echo "unknown action: ${ACTION}" >&2
    exit 1
    ;;
esac