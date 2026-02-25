import ipaddress
from collections import defaultdict
from concurrent.futures import Future, ThreadPoolExecutor
from pathlib import Path
from typing import Dict, List

from loguru import logger

from cloud_provisioner.host_spec import HostSpec
from remote_simulation import docker_cmds
from utils import shell_cmds


def _sorted_hosts_by_private_ip(hosts: List[HostSpec]) -> List[HostSpec]:
    def _key(host: HostSpec):
        try:
            return ipaddress.ip_address(host.private_ip or host.ip)
        except ValueError:
            return ipaddress.ip_address(host.ip)

    return sorted(hosts, key=_key)


def _script_paths() -> tuple[Path, Path]:
    root = Path(__file__).resolve().parent.parent
    script_dir = root / "scripts" / "remote"
    dockerhub_script = script_dir / "cfx_pull_image_from_dockerhub_and_push_local.sh"
    registry_script = script_dir / "cfx_pull_image_from_registry_and_push_local.sh"

    if not dockerhub_script.exists():
        raise FileNotFoundError(f"missing {dockerhub_script}")
    if not registry_script.exists():
        raise FileNotFoundError(f"missing {registry_script}")

    return dockerhub_script, registry_script


def _sync_prepare_scripts(host: HostSpec, dockerhub_script: Path, registry_script: Path) -> None:
    shell_cmds.scp(str(dockerhub_script), host.ip, host.ssh_user, docker_cmds.REMOTE_SCRIPT_PULL_DOCKERHUB)
    shell_cmds.scp(str(registry_script), host.ip, host.ssh_user, docker_cmds.REMOTE_SCRIPT_PULL_REGISTRY)
    shell_cmds.ssh(
        host.ip,
        host.ssh_user,
        [
            "chmod",
            "+x",
            docker_cmds.REMOTE_SCRIPT_PULL_DOCKERHUB,
            docker_cmds.REMOTE_SCRIPT_PULL_REGISTRY,
        ],
    )


def _nearest_ready_ancestor(index: int, ordered: List[HostSpec], futures: List[Future | None]):
    ancestor = (index - 1) // 2
    while ancestor is not None and ancestor >= 0:
        future = futures[ancestor]
        parent_ok = False
        if future is not None:
            try:
                parent_ok = future.result()
            except Exception:
                parent_ok = False

        if parent_ok:
            parent = ordered[ancestor]
            return parent.private_ip or parent.ip

        if ancestor == 0:
            ancestor = None
        else:
            ancestor = (ancestor - 1) // 2
    return None


def prepare_host_images(
    index: int,
    host: HostSpec,
    ordered: List[HostSpec],
    futures: List[Future | None],
    dockerhub_script: Path,
    registry_script: Path,
) -> bool:
    host_ip = host.private_ip or host.ip
    try:
        _sync_prepare_scripts(host, dockerhub_script, registry_script)

        if index == 0:
            logger.debug(f"zone {host.zone}: seed {host_ip} pulls from dockerhub")
            shell_cmds.ssh(host.ip, host.ssh_user, docker_cmds.pull_image_from_dockerhub_and_push_local())
            return True

        registry_host = _nearest_ready_ancestor(index, ordered, futures)
        if registry_host is not None:
            logger.debug(f"zone {host.zone}: {host_ip} pulls from {registry_host}")
            try:
                shell_cmds.ssh(
                    host.ip,
                    host.ssh_user,
                    docker_cmds.pull_image_from_registry_and_push_local(registry_host),
                )
                return True
            except Exception as exc:
                logger.warning(f"zone {host.zone}: {host_ip} failed pulling from {registry_host}: {exc}")

        logger.info(f"zone {host.zone}: {host_ip} fallback to dockerhub")
        shell_cmds.ssh(host.ip, host.ssh_user, docker_cmds.pull_image_from_dockerhub_and_push_local())
        return True
    except Exception as exc:
        logger.warning(f"zone {host.zone}: {host_ip} image prepare failed: {exc}")
        return False


def prepare_zone_images(zone_hosts: List[HostSpec], dockerhub_script: Path, registry_script: Path) -> None:
    ordered = _sorted_hosts_by_private_ip(zone_hosts)
    if not ordered:
        return

    with ThreadPoolExecutor(max_workers=min(128, max(1, len(ordered)))) as zone_executor:
        futures: List[Future | None] = [None] * len(ordered)
        for i, host in enumerate(ordered):
            futures[i] = zone_executor.submit(
                prepare_host_images,
                i,
                host,
                ordered,
                futures,
                dockerhub_script,
                registry_script,
            )

        for future in futures:
            if future is None:
                continue
            try:
                future.result()
            except Exception:
                pass


def prepare_images_by_zone(hosts: List[HostSpec]) -> None:
    dockerhub_script, registry_script = _script_paths()

    zones: Dict[str, List[HostSpec]] = defaultdict(list)
    for host in hosts:
        zones[host.zone].append(host)

    with ThreadPoolExecutor(max_workers=min(32, max(1, len(zones)))) as executor:
        futures = [
            executor.submit(prepare_zone_images, zone_hosts, dockerhub_script, registry_script)
            for zone_hosts in zones.values()
        ]
        for future in futures:
            future.result()
