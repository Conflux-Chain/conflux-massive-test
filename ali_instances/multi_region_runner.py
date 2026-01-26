"""Aliyun provisioning helpers based on instance-region.json."""
from __future__ import annotations

import asyncio
import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple
import traceback

from loguru import logger
from alibabacloud_ecs20140526 import models as ecs_models

from ali_instances.config import AliCredentials, EcsConfig, client, DEFAULT_USER_TAG_VALUE
from ali_instances.image_build import DEFAULT_IMAGE_NAME, ensure_images_in_regions
from ali_instances.instance_prep import (
    allocate_public_ip,
    create_instance,
    ensure_keypair,
    ensure_net,
    ensure_vpc_and_vswitch,
    list_zones_for_instance_type,
    start_instance,
    wait_running,
    wait_status,
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
class RegionProvisionPlan:
    region_name: str
    instance_type: str
    nodes_per_host: int
    hosts_needed: int
    zone_id: str
    v_switch_id: Optional[str]


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


def load_config(config_path: Path, hardware_path: Path) -> tuple[Dict, Dict[str, int]]:
    config = load_json(config_path)
    hardware_defaults = load_hardware_defaults(hardware_path)
    return config, hardware_defaults


def active_regions(regions: Iterable[Dict]) -> List[Dict]:
    return [r for r in regions if int(r.get("count", 0)) > 0]


def build_base_cfg(
    *,
    creds: AliCredentials,
    region_name: str,
    prefix: str,
    common_tag: str,
    user_tag: str,
    region_cfg: Dict,
    account_cfg: Dict,
) -> EcsConfig:
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
    return cfg


def ensure_images_for_regions(
    regions: List[Dict],
    account_cfg: Dict,
    creds: AliCredentials,
) -> Dict[str, str]:
    image_ids_by_region: Dict[str, str] = {}
    image_name_groups: Dict[str, List[str]] = {}
    for region_cfg in regions:
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

    if image_name_groups:
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
    return image_ids_by_region


def zones_with_stock(
    region_client,
    region_name: str,
    instance_type: str,
    preferred: Optional[List[str]],
) -> List[str]:
    # Query available resources in this region for the given instance type.
    # We use DescribeAvailableResource to inspect per-zone availability for
    # a specific instance type (destination_resource="InstanceType").
    # The API returns available zones with a status_category field that
    # indicates stock level. We consider zones in "WithStock" and
    # "ClosedWithStock" as having capacity to create instances.
    req = ecs_models.DescribeAvailableResourceRequest(
        region_id=region_name,
        destination_resource="InstanceType",
        resource_type="instance",
        instance_charge_type="PostPaid",
        instance_type=instance_type,
    )
    try:
        resp = region_client.describe_available_resource(req)
    except Exception as exc:
        logger.warning(f"describe_available_resource failed for {region_name}/{instance_type}: {exc}")
        return []

    zones: List[str] = []
    # Safely access nested response fields. API responses may omit
    # available_zones (None), so use getattr with defaults to avoid
    # AttributeError like 'NoneType' object has no attribute 'available_zone'.
    az = getattr(resp.body, "available_zones", None)
    available = getattr(az, "available_zone", None) or []
    for z in available:
        zid = getattr(z, "zone_id", None)
        if not zid:
            continue
        # Respect any zone preferences from config.
        if preferred and zid not in preferred:
            continue
        # Ensure the specific instance type is reported as in-stock in this zone.
        resources = getattr(z, "available_resources", None)
        resource_list = getattr(resources, "available_resource", None) or []
        type_resource = next((r for r in resource_list if getattr(r, "type", None) == "InstanceType"), None)
        supported = getattr(type_resource, "supported_resources", None)
        supported_list = getattr(supported, "supported_resource", None) or []
        has_stock = any(
            getattr(sr, "value", None) == instance_type
            and getattr(sr, "status_category", None) in {"WithStock", "ClosedWithStock"}
            for sr in supported_list
        )
        if not has_stock:
            continue
        zones.append(zid)
    return zones


def build_region_plan(
    region_client,
    region_cfg: Dict,
    account_cfg: Dict,
    hardware_defaults: Dict[str, int],
) -> Optional[RegionProvisionPlan]:
    """Build a simple provisioning plan for a region.

    This function iterates over preferred instance types (from config
    or account defaults) and checks per-type per-zone stock via
    `zones_with_stock`. If a zone with stock is found for a type, we
    compute how many hosts are needed to satisfy the requested node
    count by dividing requested nodes by nodes_per_host and rounding
    up (math.ceil). Returning a RegionProvisionPlan means we judged
    that the region has sufficient stock for at least one host group
    of the selected type; returning None indicates we found no
    suitable type/zone with reported stock.
    """
    region_name = region_cfg["name"]
    node_count = int(region_cfg.get("count", 0))
    if node_count <= 0:
        return None
    type_specs = resolve_aliyun_types(region_cfg, account_cfg, hardware_defaults)
    preferred = preferred_zones(region_cfg)
    subnet_map = zone_subnet_map(region_cfg)

    for spec in type_specs:
        # Check which zones report stock for this instance type.
        zones = zones_with_stock(region_client, region_name, spec.name, preferred)
        if not zones:
            # No zones with stock for this type; try the next type.
            continue
        # Choose the first acceptable zone. This is a simple heuristic
        # — a more advanced solver could distribute across zones/types.
        zone_id = zones[0]
        # For judgement of capacity we compute how many hosts are needed
        # to reach requested node count (nodes are packed into hosts).
        hosts_needed = math.ceil(node_count / max(spec.nodes_per_host, 1))
        return RegionProvisionPlan(
            region_name=region_name,
            instance_type=spec.name,
            nodes_per_host=spec.nodes_per_host,
            hosts_needed=hosts_needed,
            zone_id=zone_id,
            v_switch_id=subnet_map.get(zone_id),
        )
    # No suitable type/zone found with stock -> region considered out of stock.
    return None


def confirm_force_continue(missing_regions: List[str]) -> bool:
    missing_list = ", ".join(missing_regions)
    logger.warning(f"Insufficient stock in regions: {missing_list}")
    answer = input("Force continue provisioning? [y/N]: ").strip().lower()
    if answer.lower() in {"y", "yes"}:
        return True
    raise RuntimeError("provision cancelled by user")


def build_plans_parallel(
    *,
    active: List[Dict],
    creds: AliCredentials,
    account_cfg: Dict,
    hardware_defaults: Dict[str, int],
) -> Dict[str, Optional[RegionProvisionPlan]]:
    async def _build_all_plans() -> Dict[str, Optional[RegionProvisionPlan]]:
        async def _plan_for(region_cfg: Dict):
            region_name = region_cfg["name"]
            try:
                region_client = client(creds, region_name, None)
                plan = await asyncio.to_thread(
                    build_region_plan, region_client, region_cfg, account_cfg, hardware_defaults
                )
                return region_name, plan
            except Exception as exc:
                logger.warning(f"failed to query stock for {region_name}: {exc}")
                return region_name, None

        tasks = [_plan_for(region_cfg) for region_cfg in active]
        results = await asyncio.gather(*tasks)
        return {name: plan for name, plan in results}

    return asyncio.run(_build_all_plans())


def build_force_region_plan(
    region_client,
    region_cfg: Dict,
    account_cfg: Dict,
    hardware_defaults: Dict[str, int],
) -> Optional[RegionProvisionPlan]:
    region_name = region_cfg["name"]
    node_count = int(region_cfg.get("count", 0))
    if node_count <= 0:
        return None
    type_specs = resolve_aliyun_types(region_cfg, account_cfg, hardware_defaults)
    preferred = preferred_zones(region_cfg)
    subnet_map = zone_subnet_map(region_cfg)

    for spec in type_specs:
        zones = list_zones_for_instance_type(region_client, region_name, spec.name, preferred)
        if not zones:
            continue
        zone_id = zones[0]
        hosts_needed = math.ceil(node_count / max(spec.nodes_per_host, 1))
        return RegionProvisionPlan(
            region_name=region_name,
            instance_type=spec.name,
            nodes_per_host=spec.nodes_per_host,
            hosts_needed=hosts_needed,
            zone_id=zone_id,
            v_switch_id=subnet_map.get(zone_id),
        )
    return None


def wait_instance_ready(region_client, cfg: EcsConfig, instance_id: str) -> str:
    st = wait_status(
        region_client,
        cfg.region_id,
        instance_id,
        ["Stopped", "Running"],
        cfg.poll_interval,
        cfg.wait_timeout,
    )
    if st == "Stopped":
        try:
            start_instance(region_client, instance_id)
        except Exception as exc:
            logger.warning(f"start_instance failed for {instance_id}: {exc}. Will wait for instance to become Running.")
    allocate_public_ip(region_client, cfg.region_id, instance_id, cfg.poll_interval, cfg.wait_timeout)
    return wait_running(region_client, cfg.region_id, instance_id, cfg.poll_interval, cfg.wait_timeout)


def ensure_region_network(
    *,
    region_client,
    region_cfg: Dict,
    account_cfg: Dict,
    creds: AliCredentials,
    prefix: str,
    common_tag: str,
    user_tag: str,
    allow_create_vpc: bool,
    allow_create_vswitch: bool,
) -> tuple[str, List[HostSpec]]:
    region_name = region_cfg["name"]
    logger.info(f"准备在 {region_name} 创建 VPC/VSwitch")

    cfg = build_base_cfg(
        creds=creds,
        region_name=region_name,
        prefix=prefix,
        common_tag=common_tag,
        user_tag=user_tag,
        region_cfg=region_cfg,
        account_cfg=account_cfg,
    )

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


def provision_region_batch_wrapped(*, region_client, region_cfg: Dict, account_cfg: Dict, plan: RegionProvisionPlan, image_id: str, creds: AliCredentials, prefix: str, common_tag: str, user_tag: str, allow_create_vpc: bool, allow_create_vswitch: bool, allow_create_sg: bool, allow_create_keypair: bool) -> tuple[str, List[HostSpec]]:
    try:
        return provision_region_batch(region_client=region_client, region_cfg=region_cfg, account_cfg=account_cfg, plan=plan, image_id=image_id, creds=creds, prefix=prefix, common_tag=common_tag, user_tag=user_tag, allow_create_vpc=allow_create_vpc, allow_create_vswitch=allow_create_vswitch, allow_create_sg=allow_create_sg, allow_create_keypair=allow_create_keypair)
    except Exception:
        logger.error(traceback.format_exc())
        return plan.region_name, []



def provision_region_batch(
    *,
    region_client,
    region_cfg: Dict,
    account_cfg: Dict,
    plan: RegionProvisionPlan,
    image_id: str,
    creds: AliCredentials,
    prefix: str,
    common_tag: str,
    user_tag: str,
    allow_create_vpc: bool,
    allow_create_vswitch: bool,
    allow_create_sg: bool,
    allow_create_keypair: bool,
) -> tuple[str, List[HostSpec]]:
    region_name = plan.region_name
    logger.info(
        f"准备在 {region_name} 启动 {plan.hosts_needed} 台实例 (type={plan.instance_type}, zone={plan.zone_id})"
    )

    cfg = build_base_cfg(
        creds=creds,
        region_name=region_name,
        prefix=prefix,
        common_tag=common_tag,
        user_tag=user_tag,
        region_cfg=region_cfg,
        account_cfg=account_cfg,
    )
    cfg.zone_id = plan.zone_id
    if plan.v_switch_id:
        cfg.v_switch_id = plan.v_switch_id
    cfg.image_id = image_id
    cfg.instance_type = plan.instance_type

    ensure_keypair(
        region_client,
        region_name,
        cfg.key_pair_name,
        cfg.ssh_private_key_path,
        allow_create=allow_create_keypair,
    )

    ports = ports_for_nodes(plan.nodes_per_host)
    ensure_net(
        region_client,
        cfg,
        ports,
        allow_create_vpc=allow_create_vpc,
        allow_create_vswitch=allow_create_vswitch,
        allow_create_sg=allow_create_sg,
    )

    # Create all required hosts in a single RunInstances request by
    # passing `amount=plan.hosts_needed`. This results in one batch API
    # call per region/zone and avoids creating instances one-by-one.
    instance_ids = create_instance(region_client, cfg, disk_size=100, amount=plan.hosts_needed)
    region_hosts: List[HostSpec] = []
    for iid in instance_ids:
        # Wait for instance to become ready and acquire a public IP.
        ip = wait_instance_ready(region_client, cfg, iid)
        region_hosts.append(
            HostSpec(
                ip=ip,
                nodes_per_host=plan.nodes_per_host,
                ssh_user=cfg.ssh_username,
                ssh_key_path=str(Path(cfg.ssh_private_key_path).expanduser()),
                provider="aliyun",
                region=region_name,
                instance_id=iid,
            )
        )
    return region_name, region_hosts


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
    config, hardware_defaults = load_config(config_path, hardware_path)

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

        active = active_regions(regions)
        image_ids_by_region: Dict[str, str] = {}
        if active and not network_only:
            image_ids_by_region = ensure_images_for_regions(active, account_cfg, creds)

        if active and network_only:
            async def _ensure_all_regions() -> List[tuple[str, List[HostSpec]]]:
                tasks = [
                    asyncio.to_thread(
                        ensure_region_network,
                        region_client=client(creds, region_cfg["name"], None),
                        region_cfg=region_cfg,
                        account_cfg=account_cfg,
                        creds=creds,
                        prefix=prefix,
                        common_tag=common_tag,
                        user_tag=user_tag,
                        allow_create_vpc=allow_create_vpc,
                        allow_create_vswitch=allow_create_vswitch,
                    )
                    for region_cfg in active
                ]
                return await asyncio.gather(*tasks)

            for region_name, _ in asyncio.run(_ensure_all_regions()):
                if region_name not in regions_used:
                    regions_used.append(region_name)

        if active and not network_only:
            # Build provisioning plans for all active regions in parallel.
            # Each plan inspects the region's stock via DescribeAvailableResource
            # (performed inside build_region_plan). We call build_region_plan in
            # separate threads so slow network/API calls don't block other checks.
            plans = build_plans_parallel(
                active=active,
                creds=creds,
                account_cfg=account_cfg,
                hardware_defaults=hardware_defaults,
            )

            async def _provision_all_regions() -> List[tuple[str, List[HostSpec]]]:
                tasks = []
                for region_cfg in active:
                    region_name = region_cfg["name"]
                    plan = plans.get(region_name)
                    if plan is None:
                        logger.warning(f"{region_name} 无可用库存，跳过该区域")
                        continue
                    image_id = image_ids_by_region.get(region_name)
                    if not image_id:
                        raise RuntimeError(f"image not prepared for {region_name}")
                    region_client = client(creds, region_name, None)
                    tasks.append(
                        asyncio.to_thread(
                            provision_region_batch,
                            region_client=region_client,
                            region_cfg=region_cfg,
                            account_cfg=account_cfg,
                            plan=plan,
                            image_id=image_id,
                            creds=creds,
                            prefix=prefix,
                            common_tag=common_tag,
                            user_tag=user_tag,
                            allow_create_vpc=allow_create_vpc,
                            allow_create_vswitch=allow_create_vswitch,
                            allow_create_sg=allow_create_sg,
                            allow_create_keypair=allow_create_keypair,
                        )
                    )
                return await asyncio.gather(*tasks)

            missing_regions = [name for name, plan in plans.items() if plan is None]
            if missing_regions and not confirm_force_continue(missing_regions):
                raise RuntimeError("provision cancelled by user")
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
