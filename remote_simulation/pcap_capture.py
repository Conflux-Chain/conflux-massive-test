from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import time
from typing import Iterable

from loguru import logger

from cloud_provisioner.host_spec import HostSpec
from utils import shell_cmds

from .port_allocation import evm_rpc_ws_port, p2p_port


CAPTURE_DIR_NAME = "remote_simulation_pcap"
OUTPUT_DIR_NAME = "remote_simulation_pcap_output"
SCRIPT_DIR = Path(__file__).resolve().parent.parent / "scripts" / "remote"
START_SCRIPT = SCRIPT_DIR / "start_pcap_capture.sh"
COLLECT_SCRIPT = SCRIPT_DIR / "collect_pcap_capture.sh"


def _unique_hosts(host_specs: Iterable[HostSpec]) -> list[HostSpec]:
    unique_hosts: dict[str, HostSpec] = {}
    for host in host_specs:
        unique_hosts.setdefault(host.ip, host)
    return list(unique_hosts.values())


def _archive_base(host: HostSpec) -> str:
    return "/root" if host.ssh_user == "root" else f"/home/{host.ssh_user}"


def _remote_capture_root(host: HostSpec) -> str:
    return f"{_archive_base(host)}/{CAPTURE_DIR_NAME}"


def _remote_output_dir(host: HostSpec) -> str:
    return f"{_archive_base(host)}/{OUTPUT_DIR_NAME}"


def _run_remote_script(host: HostSpec, script_local: Path, args: list[str]):
    if not script_local.exists():
        raise FileNotFoundError(f"missing {script_local}")

    remote_script = f"/tmp/{script_local.name}.{time.time_ns()}.sh"
    shell_cmds.scp(str(script_local), host.ip, host.ssh_user, remote_script)
    try:
        return shell_cmds.ssh(host.ip, host.ssh_user, ["bash", remote_script, *args])
    finally:
        try:
            shell_cmds.ssh(host.ip, host.ssh_user, ["rm", "-f", remote_script], max_retries=1, retry_delay=0)
        except Exception as exc:
            logger.debug(f"主机 {host.ip} 清理远端脚本失败: {exc}")


def _parse_status(stdout: str) -> str:
    lines = [line.strip() for line in stdout.splitlines() if line.strip()]
    if not lines:
        return "unknown"
    return lines[-1]


def _port_range(host: HostSpec) -> str:
    last_node_index = max(0, host.nodes_per_host - 1)
    return f"{p2p_port(0)}-{evm_rpc_ws_port(last_node_index)}"


def _start_args(host: HostSpec) -> list[str]:
    return [
        _port_range(host),
        _remote_capture_root(host),
        _remote_output_dir(host),
        str(host.provider or "unknown"),
        str(host.region or "unknown"),
        str(host.zone or "unknown"),
        str(host.nodes_per_host),
    ]


def start_pcap_capture(host_specs: Iterable[HostSpec], *, max_workers: int = 64) -> None:
    hosts = _unique_hosts(host_specs)
    if not hosts:
        return

    def _start(host: HostSpec) -> str:
        try:
            result = _run_remote_script(host, START_SCRIPT, _start_args(host))
            if result is None:
                raise RuntimeError("SSH command returned no result")

            status = _parse_status(result.stdout)
            if status != "started":
                logger.warning(f"主机 {host.ip} 抓包未正常启动: {status}")
            logger.debug(f"主机 {host.ip} 抓包启动完成: {status}")
            return status
        except Exception as exc:
            logger.warning(f"主机 {host.ip} 抓包启动失败: {exc}")
            return "ssh_failed"

    with ThreadPoolExecutor(max_workers=max(1, min(max_workers, len(hosts)))) as executor:
        statuses = list(executor.map(_start, hosts))

    logger.info(f"抓包启动阶段完成: 正常启动 {statuses.count('started')}/{len(hosts)}")


def collect_pcap_artifacts(host_specs: Iterable[HostSpec], local_path: str, *, max_workers: int = 64) -> None:
    hosts = _unique_hosts(host_specs)
    if not hosts:
        logger.info("没有可回收 pcap 的实例")
        return

    local_root = Path(local_path)
    local_root.mkdir(parents=True, exist_ok=True)

    def _collect(host: HostSpec) -> int:
        try:
            result = _run_remote_script(host, COLLECT_SCRIPT, [_remote_capture_root(host), _remote_output_dir(host)])
            if result is None:
                raise RuntimeError("SSH command returned no result")

            status = _parse_status(result.stdout)
            local_host_path = local_root / host.ip
            local_host_path.mkdir(parents=True, exist_ok=True)
            shell_cmds.rsync_download(
                f"{_remote_output_dir(host)}/",
                str(local_host_path),
                host.ip,
                user=host.ssh_user,
                compress_level=3,
                timeout_sec=600,
            )
            for cache_file_name in (".metadata.txt", ".status.txt"):
                cache_file_path = local_host_path / cache_file_name
                if cache_file_path.exists():
                    cache_file_path.unlink()
            logger.debug(f"主机 {host.ip} pcap 回收完成")
            if status != "started":
                logger.warning(f"主机 {host.ip} pcap 状态为 {status}")
            return 0
        except Exception as exc:
            logger.warning(f"主机 {host.ip} pcap 回收失败: {exc}")
            return 1

    with ThreadPoolExecutor(max_workers=max(1, min(max_workers, len(hosts)))) as executor:
        failures = sum(executor.map(_collect, hosts))

    logger.info(f"pcap 回收完成: 成功 {len(hosts) - failures}/{len(hosts)}（{failures} 失败）")