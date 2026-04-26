from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import time
from typing import Iterable, List

from loguru import logger

from cloud_provisioner.host_spec import HostSpec
from utils import shell_cmds

from . import docker_cmds


SCRIPT_LOCAL = Path(__file__).resolve().parent.parent / "scripts" / "remote" / "host_diagnostics.sh"


def _dedupe_hosts(host_specs: Iterable[HostSpec]) -> List[HostSpec]:
    unique_hosts: dict[str, HostSpec] = {}
    for host in host_specs:
        unique_hosts[host.ip] = host
    return list(unique_hosts.values())


def _archive_base(host: HostSpec) -> str:
    return "/root" if host.ssh_user == "root" else f"/home/{host.ssh_user}"


def _remote_base_dir(host: HostSpec) -> str:
    return f"{_archive_base(host)}/host_diagnostics"


def _run_remote_script(host: HostSpec, action: str) -> None:
    if not SCRIPT_LOCAL.exists():
        raise FileNotFoundError(f"missing {SCRIPT_LOCAL}")

    remote_script = f"/tmp/{SCRIPT_LOCAL.name}.{time.time_ns()}.sh"
    shell_cmds.scp(str(SCRIPT_LOCAL), host.ip, host.ssh_user, remote_script)

    try:
        shell_cmds.ssh(
            host.ip,
            host.ssh_user,
            ["bash", remote_script, action, _remote_base_dir(host), docker_cmds.CONTAINER_PREFIX],
        )
    finally:
        try:
            shell_cmds.ssh(host.ip, host.ssh_user, ["rm", "-f", remote_script], max_retries=1, retry_delay=0)
        except Exception:
            pass


def start_host_diagnostics(host: HostSpec) -> bool:
    try:
        _run_remote_script(host, "start")
        logger.debug(f"实例 {host.ip} 主机诊断已启动")
        return True
    except Exception as exc:
        logger.warning(f"实例 {host.ip} 主机诊断启动失败: {exc}")
        return False


def collect_host_diagnostics(host_specs: Iterable[HostSpec], local_path: str) -> None:
    hosts = _dedupe_hosts(host_specs)
    if not hosts:
        logger.info("没有可收集主机诊断数据的实例")
        return

    Path(local_path).mkdir(parents=True, exist_ok=True)

    def _collect(host: HostSpec) -> int:
        local_host_path = Path(local_path) / host.ip
        local_host_path.mkdir(parents=True, exist_ok=True)

        try:
            _run_remote_script(host, "collect")
            shell_cmds.rsync_download(
                f"{_remote_base_dir(host)}/output/",
                str(local_host_path),
                host.ip,
                user=host.ssh_user,
                compress_level=3,
                timeout_sec=600,
            )
            logger.debug(f"实例 {host.ip} 主机诊断已同步")
            return 0
        except Exception as exc:
            logger.warning(f"实例 {host.ip} 主机诊断收集遇到问题: {exc}")
            return 1

    with ThreadPoolExecutor(max_workers=max(1, min(64, len(hosts)))) as executor:
        failures = sum(executor.map(_collect, hosts))

    logger.info(f"主机诊断收集完成: 成功 {len(hosts) - failures}/{len(hosts)}（{failures} 失败）")