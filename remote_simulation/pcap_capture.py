from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import time
from typing import Iterable

from loguru import logger

from cloud_provisioner.host_spec import HostSpec
from utils import shell_cmds
from utils.counter import AtomicCounter

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


def _remote_output_dir_for_rsync() -> str:
    return f"~/{OUTPUT_DIR_NAME}"


def _port_range(host: HostSpec) -> str:
    last_node_index = max(0, host.nodes_per_host - 1)
    return f"{p2p_port(0)}-{evm_rpc_ws_port(last_node_index)}"


def _run_remote_script(host: HostSpec, script_local: Path, args: list[str]):
        if not script_local.exists():
                raise FileNotFoundError(f"missing {script_local}")

        remote_script = f"/tmp/{script_local.name}.{time.time_ns()}.sh"
        shell_cmds.scp(str(script_local), host.ip, host.ssh_user, remote_script)
        try:
                return shell_cmds.ssh(host.ip, host.ssh_user, ["bash", remote_script, *args])
        finally:
                try:
                        shell_cmds.ssh(host.ip, host.ssh_user, ["rm", "-f", remote_script], max_retries=1)
                except Exception as exc:
                        logger.debug(f"主机 {host.ip} 清理远端脚本失败: {exc}")


def _parse_status(stdout: str) -> str:
    lines = [line.strip() for line in stdout.splitlines() if line.strip()]
    if not lines:
        return "unknown"
    return lines[-1]


def _start_script_args(host: HostSpec) -> list[str]:
    return [
        _port_range(host),
        CAPTURE_DIR_NAME,
        OUTPUT_DIR_NAME,
        str(host.provider or "unknown"),
        str(host.region or "unknown"),
        str(host.zone or "unknown"),
        str(host.nodes_per_host),
    ]


def _collect_script_args() -> list[str]:
    return [CAPTURE_DIR_NAME, OUTPUT_DIR_NAME]


def start_pcap_capture(host_specs: Iterable[HostSpec], *, max_workers: int = 64) -> None:
    hosts = _unique_hosts(host_specs)
    if not hosts:
        return

    counter = AtomicCounter()

    def _start(host: HostSpec) -> str:
        try:
            result = _run_remote_script(host, START_SCRIPT, _start_script_args(host))
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
            result = _run_remote_script(host, COLLECT_SCRIPT, _collect_script_args())
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