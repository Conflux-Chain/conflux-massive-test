import ipaddress
from collections import defaultdict
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List

from loguru import logger

from cloud_provisioner.host_spec import HostSpec
from remote_simulation import docker_cmds
from utils import shell_cmds
from utils.counter import get_global_counter


@dataclass(frozen=True)
class ImageDistributionSpec:
    remote_image_tag: str
    local_image_tag: str
    registry_image: str
    counter_key: str
    description: str


def _sorted_hosts_by_private_ip(hosts: List[HostSpec]) -> List[HostSpec]:
    def _key(host: HostSpec):
        try:
            return ipaddress.ip_address(host.private_ip or host.ip)
        except ValueError:
            return ipaddress.ip_address(host.ip)

    return sorted(hosts, key=_key)


def _script_paths() -> tuple[Path, Path, Path]:
    root = Path(__file__).resolve().parent.parent
    script_dir = root / "scripts" / "remote"
    dockerhub_script = script_dir / "cfx_pull_image_from_dockerhub_and_push_local.sh"
    registry_script = script_dir / "cfx_pull_image_from_registry_and_push_local.sh"
    flamegraph_script = script_dir / "start_flamegraph_profiler.sh"

    if not dockerhub_script.exists():
        raise FileNotFoundError(f"missing {dockerhub_script}")
    if not registry_script.exists():
        raise FileNotFoundError(f"missing {registry_script}")
    if not flamegraph_script.exists():
        raise FileNotFoundError(f"missing {flamegraph_script}")

    return dockerhub_script, registry_script, flamegraph_script


def _sync_prepare_scripts(
    host: HostSpec,
    dockerhub_script: Path,
    registry_script: Path,
    *,
    include_flamegraph: bool,
    flamegraph_script: Path,
) -> None:
    shell_cmds.scp(str(dockerhub_script), host.ip, host.ssh_user, docker_cmds.REMOTE_SCRIPT_PULL_DOCKERHUB)
    shell_cmds.scp(str(registry_script), host.ip, host.ssh_user, docker_cmds.REMOTE_SCRIPT_PULL_REGISTRY)

    chmod_targets = [
        docker_cmds.REMOTE_SCRIPT_PULL_DOCKERHUB,
        docker_cmds.REMOTE_SCRIPT_PULL_REGISTRY,
    ]

    if include_flamegraph:
        shell_cmds.scp(str(flamegraph_script), host.ip, host.ssh_user, docker_cmds.REMOTE_SCRIPT_START_FLAMEGRAPH)
        chmod_targets.append(docker_cmds.REMOTE_SCRIPT_START_FLAMEGRAPH)

    shell_cmds.ssh(
        host.ip,
        host.ssh_user,
        [
            "chmod",
            "+x",
            *chmod_targets,
        ],
    )


def _distribution_specs(include_flamegraph: bool) -> List[ImageDistributionSpec]:
    specs = [
        ImageDistributionSpec(
            remote_image_tag=docker_cmds.REMOTE_IMAGE_TAG,
            local_image_tag=docker_cmds.IMAGE_TAG,
            registry_image=docker_cmds.REGISTRY_IMAGE,
            counter_key="pull_docker_node",
            description="conflux-node",
        )
    ]

    if include_flamegraph:
        specs.append(
            ImageDistributionSpec(
                remote_image_tag=docker_cmds.FLAMEGRAPH_REMOTE_IMAGE_TAG,
                local_image_tag=docker_cmds.FLAMEGRAPH_IMAGE_TAG,
                registry_image=docker_cmds.FLAMEGRAPH_REGISTRY_IMAGE,
                counter_key="pull_docker_flamegraph",
                description="flamegraph",
            )
        )

    return specs


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


def _pull_from_dockerhub(host: HostSpec, spec: ImageDistributionSpec) -> None:
    host_ip = host.private_ip or host.ip
    logger.debug(
        f"zone {host.zone}: seed {host_ip} pulls {spec.description} from dockerhub "
        f"({get_global_counter(spec.counter_key).increment()})"
    )
    shell_cmds.ssh(
        host.ip,
        host.ssh_user,
        docker_cmds.pull_image_from_dockerhub_and_push_local(
            remote_image_tag=spec.remote_image_tag,
            image_tag=spec.local_image_tag,
            registry_image=spec.registry_image,
        ),
    )


def _pull_from_ancestor_registry(host: HostSpec, spec: ImageDistributionSpec, registry_host: str) -> None:
    host_ip = host.private_ip or host.ip
    logger.debug(
        f"zone {host.zone}: {host_ip} pulls {spec.description} from {registry_host} "
        f"({get_global_counter(spec.counter_key).increment()})"
    )
    shell_cmds.ssh(
        host.ip,
        host.ssh_user,
        docker_cmds.pull_image_from_registry_and_push_local(
            registry_host=registry_host,
            image_tag=spec.local_image_tag,
            registry_image=spec.registry_image,
        ),
    )


def _prepare_image_for_host(
    index: int,
    host: HostSpec,
    ordered: List[HostSpec],
    futures: List[Future | None],
    spec: ImageDistributionSpec,
) -> None:
    host_ip = host.private_ip or host.ip
    if index == 0:
        _pull_from_dockerhub(host, spec)
        return

    registry_host = _nearest_ready_ancestor(index, ordered, futures)
    if registry_host is not None:
        try:
            _pull_from_ancestor_registry(host, spec, registry_host)
            return
        except Exception as exc:
            logger.warning(
                f"zone {host.zone}: {host_ip} failed pulling {spec.description} "
                f"from {registry_host}: {exc}"
            )

    logger.info(f"zone {host.zone}: {host_ip} fallback {spec.description} to dockerhub")
    _pull_from_dockerhub(host, spec)


def prepare_host_images(
    index: int,
    host: HostSpec,
    ordered: List[HostSpec],
    futures: List[Future | None],
    dockerhub_script: Path,
    registry_script: Path,
    flamegraph_script: Path,
    image_specs: List[ImageDistributionSpec],
    include_flamegraph: bool,
) -> bool:
    try:
        _sync_prepare_scripts(
            host,
            dockerhub_script,
            registry_script,
            include_flamegraph=include_flamegraph,
            flamegraph_script=flamegraph_script,
        )
        for spec in image_specs:
            _prepare_image_for_host(index, host, ordered, futures, spec)
        return True
    except Exception as exc:
        host_ip = host.private_ip or host.ip
        logger.warning(f"zone {host.zone}: {host_ip} image prepare failed: {exc}")
        return False


def prepare_zone_images(
    zone_hosts: List[HostSpec],
    dockerhub_script: Path,
    registry_script: Path,
    flamegraph_script: Path,
    image_specs: List[ImageDistributionSpec],
    include_flamegraph: bool,
) -> None:
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
                flamegraph_script,
                image_specs,
                include_flamegraph,
            )

        for future in futures:
            if future is None:
                continue
            try:
                future.result()
            except Exception:
                pass


def prepare_images_by_zone(hosts: List[HostSpec], *, include_flamegraph: bool = False) -> None:
    dockerhub_script, registry_script, flamegraph_script = _script_paths()
    image_specs = _distribution_specs(include_flamegraph=include_flamegraph)

    zones: Dict[str, List[HostSpec]] = defaultdict(list)
    for host in hosts:
        zones[host.zone].append(host)

    with ThreadPoolExecutor(max_workers=min(32, max(1, len(zones)))) as executor:
        futures = [
            executor.submit(
                prepare_zone_images,
                zone_hosts,
                dockerhub_script,
                registry_script,
                flamegraph_script,
                image_specs,
                include_flamegraph,
            )
            for zone_hosts in zones.values()
        ]
        for future in futures:
            future.result()
