from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import shlex
from typing import Iterable

from loguru import logger

from cloud_provisioner.host_spec import HostSpec
from utils import shell_cmds
from utils.counter import AtomicCounter

from .port_allocation import evm_rpc_ws_port, p2p_port


CAPTURE_DIR_NAME = "remote_simulation_pcap"
OUTPUT_DIR_NAME = "remote_simulation_pcap_output"


def _unique_hosts(host_specs: Iterable[HostSpec]) -> list[HostSpec]:
    unique_hosts: dict[str, HostSpec] = {}
    for host in host_specs:
        unique_hosts.setdefault(host.ip, host)
    return list(unique_hosts.values())


def _remote_output_dir_for_rsync() -> str:
    return f"~/{OUTPUT_DIR_NAME}"


def _sudo_prefix(host: HostSpec) -> str:
    return "" if host.ssh_user == "root" else "sudo "


def _port_range(host: HostSpec) -> str:
    last_node_index = max(0, host.nodes_per_host - 1)
    return f"{p2p_port(0)}-{evm_rpc_ws_port(last_node_index)}"


def _run_remote_bash(host: HostSpec, script: str):
    return shell_cmds.ssh(host.ip, host.ssh_user, f"bash -lc {shlex.quote(script)}")


def _parse_status(stdout: str) -> str:
    lines = [line.strip() for line in stdout.splitlines() if line.strip()]
    if not lines:
        return "unknown"
    return lines[-1]


def _build_start_script(host: HostSpec) -> str:
    capture_dir_name = shlex.quote(CAPTURE_DIR_NAME)
    output_dir_name = shlex.quote(OUTPUT_DIR_NAME)
    port_range = _port_range(host)
    sudo_prefix = _sudo_prefix(host)
    tcpdump_start = shlex.quote(
      'nohup "$6" -i any tcp and portrange "$1" -s 150 -W 10 -C 100 -Z root -w "$2" > "$3" 2> "$4" < /dev/null & echo $! > "$5"'
    )

    return f"""set -euo pipefail
capture_dir_name={capture_dir_name}
output_dir_name={output_dir_name}
capture_root="$HOME/$capture_dir_name"
output_dir="$HOME/$output_dir_name"
pid_file="$capture_root/tcpdump.pid"
status_file="$capture_root/status.txt"
metadata_file="$capture_root/metadata.txt"
stdout_file="$capture_root/tcpdump.stdout.log"
stderr_file="$capture_root/tcpdump.stderr.log"
pcap_path="$capture_root/$(hostname).pcap"

if [ -f "$pid_file" ]; then
  {sudo_prefix}kill -INT "$(cat "$pid_file")" >/dev/null 2>&1 || true
fi

rm -rf "$capture_root" "$output_dir"
mkdir -p "$capture_root"

tcpdump_bin="$(command -v tcpdump || true)"
if [ -z "$tcpdump_bin" ] && [ -x /usr/sbin/tcpdump ]; then
  tcpdump_bin=/usr/sbin/tcpdump
fi

if [ -z "$tcpdump_bin" ]; then
  printf 'missing_tcpdump\n' > "$status_file"
  cat > "$metadata_file" <<'EOF'
provider={host.provider}
region={host.region}
zone={host.zone}
nodes_per_host={host.nodes_per_host}
port_range={port_range}
interface=any
status=missing_tcpdump
EOF
  cat "$status_file"
  exit 0
fi

{sudo_prefix}sh -c {tcpdump_start} _ {shlex.quote(port_range)} "$pcap_path" "$stdout_file" "$stderr_file" "$pid_file" "$tcpdump_bin"

sleep 1

if ! {sudo_prefix}kill -0 "$(cat "$pid_file")" >/dev/null 2>&1; then
  printf 'start_failed\n' > "$status_file"
  cat > "$metadata_file" <<'EOF'
provider={host.provider}
region={host.region}
zone={host.zone}
nodes_per_host={host.nodes_per_host}
port_range={port_range}
interface=any
status=start_failed
EOF
  cat "$status_file"
  exit 0
fi

printf 'started\n' > "$status_file"
cat > "$metadata_file" <<EOF
provider={host.provider}
region={host.region}
zone={host.zone}
nodes_per_host={host.nodes_per_host}
port_range={port_range}
interface=any
tcpdump_bin=$tcpdump_bin
pcap_path=$pcap_path
status=started
EOF
cat "$status_file"
"""


def _build_collect_script(host: HostSpec) -> str:
    capture_dir_name = shlex.quote(CAPTURE_DIR_NAME)
    output_dir_name = shlex.quote(OUTPUT_DIR_NAME)
    sudo_prefix = _sudo_prefix(host)

    return f"""set -euo pipefail
capture_dir_name={capture_dir_name}
output_dir_name={output_dir_name}
capture_root="$HOME/$capture_dir_name"
output_dir="$HOME/$output_dir_name"
pid_file="$capture_root/tcpdump.pid"
status_file="$capture_root/status.txt"
status="capture_not_started"

rm -rf "$output_dir"
mkdir -p "$output_dir"

if [ -f "$status_file" ]; then
  status="$(tr -d '\\r\\n' < "$status_file")"
fi

if [ -f "$pid_file" ]; then
  pid="$(cat "$pid_file")"
  {sudo_prefix}kill -INT "$pid" >/dev/null 2>&1 || true
  for _ in $(seq 1 50); do
    if ! {sudo_prefix}kill -0 "$pid" >/dev/null 2>&1; then
      break
    fi
    sleep 0.1
  done
fi

if [ -d "$capture_root" ]; then
  copied=0
  shopt -s nullglob
  for path in "$capture_root"/*; do
    if [ ! -f "$path" ] || [ "$(basename "$path")" = "tcpdump.pid" ]; then
      continue
    fi
    {sudo_prefix}cp "$path" "$output_dir/$(basename "$path")"
    copied=1
  done

  if [ "$copied" -eq 0 ]; then
    printf '%s\n' "$status" > "$output_dir/status.txt"
  fi

  {sudo_prefix}rm -rf "$capture_root"
else
  printf '%s\n' "$status" > "$output_dir/status.txt"
fi

{sudo_prefix}chown -R {host.ssh_user}:{host.ssh_user} "$output_dir"

if command -v 7z >/dev/null 2>&1; then
  find "$output_dir" -maxdepth 1 -type f ! -name '*.7z' -print0 | xargs -0 -P4 -I{{}} sh -c '7z a -t7z -mx=9 -m0=lzma2 -ms=on -bso0 -bsp0 "$1.7z" "$1" >/dev/null && rm -f "$1"' _ "{{}}" || true
fi

printf '%s\n' "$status"
"""


def start_pcap_capture(host_specs: Iterable[HostSpec], *, max_workers: int = 64) -> None:
    hosts = _unique_hosts(host_specs)
    if not hosts:
        return

    counter = AtomicCounter()

    def _start(host: HostSpec) -> str:
        try:
            result = _run_remote_bash(host, _build_start_script(host))
            if result is None:
                raise RuntimeError("SSH command returned no result")

            status = _parse_status(result.stdout)
            if status != "started":
                logger.warning(f"主机 {host.ip} 抓包未正常启动: {status}")
            cnt = counter.increment()
            logger.debug(f"主机 {host.ip} 抓包启动完成 ({cnt}/{len(hosts)})")
            return status
        except Exception as exc:
            logger.warning(f"主机 {host.ip} 抓包启动失败: {exc}")
            return "ssh_failed"

    with ThreadPoolExecutor(max_workers=min(max_workers, len(hosts))) as executor:
        statuses = list(executor.map(_start, hosts))

    started_count = statuses.count("started")
    logger.info(f"抓包启动阶段完成: 正常启动 {started_count}/{len(hosts)}")


def collect_pcap_artifacts(host_specs: Iterable[HostSpec], local_path: str, *, max_workers: int = 32) -> None:
    hosts = _unique_hosts(host_specs)
    if not hosts:
        return

    total_hosts = len(hosts)
    counter = AtomicCounter()
    local_root = Path(local_path)
    local_root.mkdir(parents=True, exist_ok=True)

    def _collect(host: HostSpec) -> int:
        try:
            result = _run_remote_bash(host, _build_collect_script(host))
            if result is None:
                raise RuntimeError("SSH command returned no result")
            status = _parse_status(result.stdout)
            local_host_path = local_root / host.ip
            local_host_path.mkdir(parents=True, exist_ok=True)
            shell_cmds.rsync_download(
                f"{_remote_output_dir_for_rsync()}/",
                str(local_host_path),
                host.ip,
                user=host.ssh_user,
                compress_level=3,
                timeout_sec=600,
            )
            cnt = counter.increment()
            logger.debug(f"主机 {host.ip} pcap 回收完成 ({cnt}/{total_hosts})")
            if status != "started":
                logger.warning(f"主机 {host.ip} pcap 状态为 {status}")
            return 0
        except Exception as exc:
            logger.warning(f"主机 {host.ip} pcap 回收失败: {exc}")
            return 1

    with ThreadPoolExecutor(max_workers=min(max_workers, total_hosts)) as executor:
        results = list(executor.map(_collect, hosts))

    failure_count = sum(results)
    logger.info(f"pcap 回收完成: 成功 {total_hosts - failure_count}/{total_hosts}")