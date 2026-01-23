"""Aliyun provisioning helpers based on instance-region.json."""
from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from loguru import logger

from ali_instances.config import AliCredentials, EcsConfig, client, DEFAULT_USER_TAG_VALUE
from ali_instances.image_build import DEFAULT_IMAGE_NAME, ensure_images_in_regions
from ali_instances.instance_prep import (
    ensure_keypair,
    ensure_vpc_and_vswitch,
    list_zones_for_instance_type,
    provision_instance_with_type,
)
from ali_instances.host_spec import HostSpec
from remote_simulation.port_allocation import (
    evm_rpc_port,
    evm_rpc_ws_port,
    p2p_port,
    pubsub_port,
    remote_rpc_port,
    rpc_port,
)


@dataclass
class AliTypeSpec:
    name: str
    nodes_per_host: int


@dataclass
class AliInstancePlan:
    instance_type: str
    nodes_per_host: int


CleanupTarget = Tuple[List[str], AliCredentials, str, str]


def load_json(path: Path) -> Dict:
    return json.loads(path.read_text())


def load_hardware_defaults(path: Path) -> Dict[str, int]:
    data = json.loads(path.read_text())
    return {item["name"]: item["nodes"] for item in data}


def resolve_nodes_per_host(type_name: str, nodes: Optional[int], defaults: Dict[str, int]) -> int:
    if nodes is not None:
        return nodes
    return defaults.get(type_name, 1)


def resolve_aliyun_types(
    region_cfg: Dict,
    account_cfg: Dict,
    hardware_defaults: Dict[str, int],
) -> List[AliTypeSpec]:
    types_cfg = region_cfg.get("type") or account_cfg.get("type") or [{"name": "ecs.g8i.xlarge"}]
    specs: List[AliTypeSpec] = []
    for item in types_cfg:
        name = item["name"]
        raw_nodes = item.get("nodes")
        nodes = int(raw_nodes) if raw_nodes is not None else None
        nodes_per_host = resolve_nodes_per_host(name, nodes, hardware_defaults)
        specs.append(AliTypeSpec(name=name, nodes_per_host=nodes_per_host))
    return specs


def preferred_zones(region_cfg: Dict) -> Optional[List[str]]:
    zones_cfg = region_cfg.get("zones")
    if not zones_cfg:
        return None
    return [z["name"] for z in zones_cfg if "name" in z]


def zone_subnet_map(region_cfg: Dict) -> Dict[str, str]:
    zones_cfg = region_cfg.get("zones") or []
    mapping: Dict[str, str] = {}
    for z in zones_cfg:
        name = z.get("name")
        subnet = z.get("subnet")
        if name and subnet:
            mapping[name] = subnet
    return mapping




def ports_for_nodes(max_nodes_per_host: int) -> List[int]:
    ports: List[int] = [22]
    for i in range(max_nodes_per_host):
        ports.extend(
            [
                p2p_port(i),
                rpc_port(i),
                remote_rpc_port(i),
                pubsub_port(i),
                evm_rpc_port(i),
                evm_rpc_ws_port(i),
            ]
        )
    return sorted(set(ports))


def resolve_aliyun_credentials(cfg: Dict) -> AliCredentials:
    ak = cfg.get("access_key_id", "").strip()
    sk = cfg.get("access_key_secret", "").strip()
    if ak and sk:
        return AliCredentials(ak, sk)
    return EcsConfig().credentials


def provision_aliyun_hosts(
    *,
    config_path: Path,
    hardware_path: Path,
    common_tag: str = "conflux-massive-test",
    network_only: bool = False,
    allow_create_vpc: bool = True,
    allow_create_vswitch: bool = True,
    allow_create_sg: bool = True,
    allow_create_keypair: bool = True,
) -> Tuple[List[HostSpec], List[CleanupTarget]]:
    config = load_json(config_path)
    hardware_defaults = load_hardware_defaults(hardware_path)

    aliyun_cfgs = config.get("aliyun", [])
    if not aliyun_cfgs:
        raise RuntimeError("missing aliyun config")

    hosts: List[HostSpec] = []
    cleanup_targets: List[CleanupTarget] = []

    for account_cfg in aliyun_cfgs:
        regions = account_cfg.get("regions", [])
        creds = resolve_aliyun_credentials(account_cfg)
        user_tag = account_cfg.get("user_tag", DEFAULT_USER_TAG_VALUE)
        prefix = f"{common_tag}-{user_tag}"
        regions_used: List[str] = []

        active_regions = [r for r in regions if int(r.get("count", 0)) > 0]
        image_ids_by_region: Dict[str, str] = {}

        if active_regions and not network_only:
            image_name_groups: Dict[str, List[str]] = {}
            for region_cfg in active_regions:
                region_name = region_cfg["name"]
                image_id = region_cfg.get("image") or account_cfg.get("image")
                if image_id:
                    image_ids_by_region[region_name] = image_id
                    continue
                base_image_name = (
                    region_cfg.get("base_image_name")
                    or account_cfg.get("base_image_name")
                    or DEFAULT_IMAGE_NAME
                )
                image_name_groups.setdefault(base_image_name, []).append(region_name)

            cfg_template = EcsConfig(credentials=creds)
            all_region_names = [r["name"] for r in regions]
            for image_name, region_list in image_name_groups.items():
                image_ids_by_region.update(
                    ensure_images_in_regions(
                        creds=creds,
                        target_regions=region_list,
                        image_name=image_name,
                        search_regions=all_region_names,
                        poll_interval=cfg_template.poll_interval,
                        wait_timeout=cfg_template.wait_timeout,
                    )
                )

        def _provision_region(region_cfg: Dict) -> tuple[str, List[HostSpec]]:
            region_name = region_cfg["name"]
            node_count = int(region_cfg.get("count", 0))
            logger.info(f"准备在 {region_name} 启动 {node_count} 个 Conflux 节点")

            region_client = client(creds, region_name, None)
            type_specs = resolve_aliyun_types(region_cfg, account_cfg, hardware_defaults)
            preferred = preferred_zones(region_cfg)
            subnet_map = zone_subnet_map(region_cfg)

            max_nodes_per_host = max(spec.nodes_per_host for spec in type_specs)
            ports = ports_for_nodes(max_nodes_per_host)

            cfg = EcsConfig(credentials=creds, region_id=region_name)
            cfg.ssh_username = "root"
            cfg.instance_name_prefix = prefix
            cfg.vpc_name = prefix
            cfg.vswitch_name = prefix
            cfg.security_group_name = prefix
            cfg.common_tag_key = common_tag
            cfg.common_tag_value = "true"
            cfg.user_tag_value = user_tag
            cfg.security_group_id = region_cfg.get("security_group_id") or account_cfg.get("security_group_id")
            zones_cfg = region_cfg.get("zones") or []
            if len(zones_cfg) == 1:
                cfg.zone_id = zones_cfg[0].get("name") or cfg.zone_id
                cfg.v_switch_id = zones_cfg[0].get("subnet") or cfg.v_switch_id

            ensure_keypair(
                region_client,
                region_name,
                cfg.key_pair_name,
                cfg.ssh_private_key_path,
                allow_create=allow_create_keypair,
            )

            cfg.image_id = image_ids_by_region.get(region_name)
            if not cfg.image_id:
                raise RuntimeError(f"image not prepared for {region_name}")

            def _no_stock(exc: Exception) -> bool:
                return "OperationDenied.NoStock" in str(exc)

            remaining_nodes = node_count
            last_successful_type: Optional[str] = None
            last_successful_nodes: Optional[int] = None
            region_hosts: List[HostSpec] = []

            for spec in type_specs:
                if remaining_nodes <= 0:
                    break
                zone_candidates = list_zones_for_instance_type(region_client, region_name, spec.name, preferred)
                if not zone_candidates:
                    logger.warning(f"instance type {spec.name} not available in {region_name}")
                    continue

                while remaining_nodes > 0:
                    created = False
                    for zone_id in zone_candidates:
                        zone_subnet = subnet_map.get(zone_id)
                        try:
                            handle = provision_instance_with_type(
                                cfg,
                                spec.name,
                                zone_id,
                                ports,
                                v_switch_id=zone_subnet,
                                allow_create_vpc=allow_create_vpc,
                                allow_create_vswitch=allow_create_vswitch,
                                allow_create_sg=allow_create_sg,
                            )
                            region_hosts.append(
                                HostSpec(
                                    ip=handle.public_ip,
                                    nodes_per_host=spec.nodes_per_host,
                                    ssh_user=cfg.ssh_username,
                                    ssh_key_path=str(Path(cfg.ssh_private_key_path).expanduser()),
                                    provider="aliyun",
                                    region=region_name,
                                    instance_id=handle.instance_id,
                                )
                            )
                            remaining_nodes -= spec.nodes_per_host
                            last_successful_type = spec.name
                            last_successful_nodes = spec.nodes_per_host
                            created = True
                            break
                        except Exception as exc:
                            if _no_stock(exc):
                                logger.warning(
                                    f"no stock for {spec.name} in {region_name}/{zone_id}, trying next zone"
                                )
                                continue
                            raise

                    if not created:
                        logger.warning(f"no stock in any zone for {spec.name} in {region_name}, trying next type")
                        break

            if remaining_nodes > 0:
                if not last_successful_type or not last_successful_nodes:
                    raise RuntimeError(f"no stock for all preferred types in {region_name}")
                raise RuntimeError(f"not enough stock to reach {node_count} nodes in {region_name}")

            return region_name, region_hosts

        def _ensure_region_network(region_cfg: Dict) -> tuple[str, List[HostSpec]]:
            region_name = region_cfg["name"]
            logger.info(f"准备在 {region_name} 创建 VPC/VSwitch")

            region_client = client(creds, region_name, None)
            cfg = EcsConfig(credentials=creds, region_id=region_name)
            cfg.instance_name_prefix = prefix
            cfg.vpc_name = prefix
            cfg.vswitch_name = prefix
            cfg.security_group_name = prefix
            cfg.common_tag_key = common_tag
            cfg.common_tag_value = "true"
            cfg.user_tag_value = user_tag

            zones_cfg = region_cfg.get("zones") or []
            if zones_cfg:
                for zone_cfg in zones_cfg:
                    cfg.zone_id = zone_cfg.get("name")
                    cfg.v_switch_id = zone_cfg.get("subnet")
                    ensure_vpc_and_vswitch(
                        region_client,
                        cfg,
                        allow_create_vpc=allow_create_vpc,
                        allow_create_vswitch=allow_create_vswitch,
                    )
                    cfg.v_switch_id = None
            else:
                cfg.zone_id = None
                cfg.v_switch_id = None
                ensure_vpc_and_vswitch(
                    region_client,
                    cfg,
                    allow_create_vpc=allow_create_vpc,
                    allow_create_vswitch=allow_create_vswitch,
                )

            return region_name, []

        active_regions = [r for r in regions if int(r.get("count", 0)) > 0]
        if active_regions and network_only:
            async def _ensure_all_regions() -> List[tuple[str, List[HostSpec]]]:
                tasks = [asyncio.to_thread(_ensure_region_network, region_cfg) for region_cfg in active_regions]
                return await asyncio.gather(*tasks)

            for region_name, _ in asyncio.run(_ensure_all_regions()):
                if region_name not in regions_used:
                    regions_used.append(region_name)

        if active_regions and not network_only:
            async def _provision_all_regions() -> List[tuple[str, List[HostSpec]]]:
                tasks = [asyncio.to_thread(_provision_region, region_cfg) for region_cfg in active_regions]
                return await asyncio.gather(*tasks)

            for region_name, region_hosts in asyncio.run(_provision_all_regions()):
                hosts.extend(region_hosts)
                if region_name not in regions_used:
                    regions_used.append(region_name)

        if regions_used:
            cleanup_targets.append((regions_used, creds, user_tag, prefix))

    return hosts, cleanup_targets


def cleanup_targets(targets: List[CleanupTarget], common_tag: str = "conflux-massive-test") -> None:
    from ali_instances.cleanup_resources import cleanup_all_regions

    for regions, creds, user_tag, prefix in targets:
        cleanup_all_regions(
            regions=regions,
            credentials=creds,
            common_tag=common_tag,
            user_tag=user_tag,
            name_prefix=prefix,
        )
