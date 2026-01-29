"""Instance preparation and lifecycle helpers for Aliyun ECS."""
import asyncio
import copy
import ipaddress
import json
import subprocess
import time
import traceback
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Sequence

import asyncssh
from alibabacloud_ecs20140526 import models as ecs_models
from alibabacloud_ecs20140526.client import Client as EcsClient
from loguru import logger

from utils.wait_until import wait_until
from .config import EcsRuntimeConfig, InstanceTypeConfig, client, RUN_INSTANCES_MAX_AMOUNT


async def wait_ssh(host: str, user: str, key: str, timeout: int, interval: int = 3) -> None:
    """Wait until SSH is ready on a host."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            conn = await asyncssh.connect(host, username=user, client_keys=[key], known_hosts=None)
            conn.close()
            await conn.wait_closed()
            return
        except Exception:
            await asyncio.sleep(interval)
    raise TimeoutError(f"SSH not ready for {host}")


def _tag_dict(cfg: EcsRuntimeConfig) -> dict[str, str]:
    return {
        cfg.common_tag_key: cfg.common_tag_value,
        cfg.user_tag_key: cfg.user_tag_value,
    }


def _tag_resource(c: EcsClient, r: str, resource_type: str, resource_id: str, tags: dict[str, str]) -> None:
    tag_list = [ecs_models.TagResourcesRequestTag(key=k, value=v) for k, v in tags.items()]
    c.tag_resources(
        ecs_models.TagResourcesRequest(
            region_id=r,
            resource_type=resource_type,
            resource_id=[resource_id],
            tag=tag_list,
        )
    )


def _instance_info(c: EcsClient, r: str, iid: str) -> tuple[Optional[str], Optional[str]]:
    resp = c.describe_instances(ecs_models.DescribeInstancesRequest(region_id=r, instance_ids=json.dumps([iid])))
    instances = resp.body.instances.instance if resp.body and resp.body.instances else []
    if not instances:
        return None, None
    ips = instances[0].public_ip_address.ip_address if instances[0].public_ip_address else []
    return instances[0].status, ips[0] if ips else None


def wait_status(c: EcsClient, r: str, iid: str, want: Sequence[str], poll: int, timeout: int) -> str:
    h = {"s": None}

    def chk() -> bool:
        h["s"], _ = _instance_info(c, r, iid)
        return h["s"] in want

    wait_until(chk, timeout=timeout, retry_interval=poll)
    return h["s"] or ""


def wait_running(c: EcsClient, r: str, iid: str, poll: int, timeout: int) -> str:
    h = {"ip": None}

    def chk() -> bool:
        s, ip = _instance_info(c, r, iid)
        h["ip"] = ip
        logger.info(f"{iid}: {s}, ip={ip}")
        return s == "Running" and bool(ip)

    wait_until(chk, timeout=timeout, retry_interval=poll)
    return h["ip"] or ""


def start_instance(c: EcsClient, iid: str) -> None:
    c.start_instance(ecs_models.StartInstanceRequest(instance_id=iid))


def stop_instance(c: EcsClient, iid: str, mode: Optional[str] = None) -> None:
    c.stop_instance(ecs_models.StopInstanceRequest(instance_id=iid, force_stop=True, stopped_mode=mode))


def delete_instance(c: EcsClient, r: str, iid: str) -> None:
    s, _ = _instance_info(c, r, iid)
    if s:
        c.delete_instance(ecs_models.DeleteInstanceRequest(instance_id=iid, force=True, force_stop=True))


def allocate_public_ip(c: EcsClient, r: str, iid: str, poll: int = 3, timeout: int = 120) -> Optional[str]:
    wait_status(c, r, iid, ["Running", "Stopped"], poll, timeout)
    resp = c.allocate_public_ip_address(ecs_models.AllocatePublicIpAddressRequest(instance_id=iid))
    return resp.body.ip_address if resp.body else None


def pick_zone(c: EcsClient, r: str) -> str:
    resp = c.describe_zones(ecs_models.DescribeZonesRequest(region_id=r))
    zones = resp.body.zones.zone if resp.body and resp.body.zones else []
    if not zones:
        raise RuntimeError(f"no zones in {r}")
    return zones[0].zone_id


def ensure_vpc(c: EcsClient, r: str, name: str, cidr: str, allow_create: bool = True) -> str:
    resp = c.describe_vpcs(ecs_models.DescribeVpcsRequest(region_id=r, page_size=50))
    for v in resp.body.vpcs.vpc or []:
        if v.vpc_name == name:
            return v.vpc_id
    if not allow_create:
        raise RuntimeError(f"vpc {name} not found in {r}")
    cr = c.create_vpc(ecs_models.CreateVpcRequest(region_id=r, vpc_name=name, cidr_block=cidr))
    vid = cr.body.vpc_id
    wait_until(lambda: _vpc_available(c, r, vid), timeout=120, retry_interval=3)
    return vid


def _vpc_available(c: EcsClient, r: str, vid: str) -> bool:
    resp = c.describe_vpcs(ecs_models.DescribeVpcsRequest(region_id=r, vpc_id=vid))
    vpcs = resp.body.vpcs.vpc if resp.body and resp.body.vpcs else []
    return vpcs and vpcs[0].status == "Available"


def ensure_vswitch(
    c: EcsClient,
    r: str,
    vpc: str,
    zone: str,
    name: str,
    cidr: str,
    vpc_cidr: str,
    allow_create: bool = True,
) -> str:
    resp = c.describe_vswitches(ecs_models.DescribeVSwitchesRequest(region_id=r, vpc_id=vpc, page_size=50))
    vsws = resp.body.v_switches.v_switch if resp.body and resp.body.v_switches else []
    for v in vsws:
        if v.v_switch_name == name and v.zone_id == zone:
            return v.v_switch_id
    if not allow_create:
        raise RuntimeError(f"vswitch {name} not found in {r}/{zone}")
    existing = [v.cidr_block for v in vsws if v.cidr_block]
    net = ipaddress.ip_network(cidr)
    if any(net.overlaps(ipaddress.ip_network(e)) for e in existing if e):
        used = {ipaddress.ip_network(e) for e in existing if e}
        for sub in ipaddress.ip_network(vpc_cidr).subnets(new_prefix=24):
            if all(not sub.overlaps(u) for u in used):
                cidr = str(sub)
                break
    cr = c.create_vswitch(ecs_models.CreateVSwitchRequest(region_id=r, vpc_id=vpc, zone_id=zone, v_switch_name=name, cidr_block=cidr))
    vsid = cr.body.v_switch_id
    wait_until(lambda: _vswitch_ok(c, r, vsid), timeout=120, retry_interval=3)
    return vsid


def _vswitch_ok(c: EcsClient, r: str, vsid: str) -> bool:
    resp = c.describe_vswitches(ecs_models.DescribeVSwitchesRequest(region_id=r, v_switch_id=vsid))
    vsws = resp.body.v_switches.v_switch if resp.body and resp.body.v_switches else []
    return vsws and vsws[0].status == "Available"


def _vswitch_zone(c: EcsClient, r: str, vsid: str) -> Optional[str]:
    resp = c.describe_vswitches(ecs_models.DescribeVSwitchesRequest(region_id=r, v_switch_id=vsid))
    vsws = resp.body.v_switches.v_switch if resp.body and resp.body.v_switches else []
    if not vsws:
        return None
    return vsws[0].zone_id


def ensure_sg(c: EcsClient, r: str, vpc: str, name: str, allow_create: bool = True) -> str:
    resp = c.describe_security_groups(ecs_models.DescribeSecurityGroupsRequest(region_id=r, vpc_id=vpc, page_size=50))
    for g in resp.body.security_groups.security_group or []:
        if g.security_group_name == name:
            return g.security_group_id
    if not allow_create:
        raise RuntimeError(f"security group {name} not found in {r}")
    return c.create_security_group(
        ecs_models.CreateSecurityGroupRequest(region_id=r, vpc_id=vpc, security_group_name=name, description="conflux")
    ).body.security_group_id


def auth_port(c: EcsClient, r: str, sg: str, port: int) -> None:
    resp = c.describe_security_group_attribute(ecs_models.DescribeSecurityGroupAttributeRequest(region_id=r, security_group_id=sg))
    for p in resp.body.permissions.permission or []:
        if p.ip_protocol == "tcp" and p.port_range == f"{port}/{port}" and p.source_cidr_ip == "0.0.0.0/0":
            return
    c.authorize_security_group(
        ecs_models.AuthorizeSecurityGroupRequest(
            region_id=r,
            security_group_id=sg,
            ip_protocol="tcp",
            port_range=f"{port}/{port}",
            source_cidr_ip="0.0.0.0/0",
        )
    )


def ensure_vpc_and_vswitch(
    c: EcsClient,
    cfg: EcsRuntimeConfig,
    allow_create_vpc: bool = True,
    allow_create_vswitch: bool = True,
) -> tuple[str, str]:
    cfg.zone_id = cfg.zone_id or pick_zone(c, cfg.region_id)
    vpc = ensure_vpc(c, cfg.region_id, cfg.vpc_name, cfg.vpc_cidr, allow_create=allow_create_vpc)
    if cfg.v_switch_id:
        vs_zone = _vswitch_zone(c, cfg.region_id, cfg.v_switch_id)
        if not vs_zone:
            if not allow_create_vswitch:
                raise RuntimeError(f"vswitch {cfg.v_switch_id} not found in {cfg.region_id}")
            logger.warning(f"vswitch {cfg.v_switch_id} missing; will create a new vswitch in {cfg.zone_id}")
            cfg.v_switch_id = None
        elif vs_zone != cfg.zone_id:
            msg = (
                f"vswitch {cfg.v_switch_id} is in zone {vs_zone}, "
                f"but target zone is {cfg.zone_id}"
            )
            if not allow_create_vswitch:
                raise RuntimeError(msg)
            logger.warning(f"{msg}; will create a new vswitch in {cfg.zone_id}")
            cfg.v_switch_id = None

    if not cfg.v_switch_id:
        cfg.v_switch_id = ensure_vswitch(
            c,
            cfg.region_id,
            vpc,
            cfg.zone_id,
            cfg.vswitch_name,
            cfg.vswitch_cidr,
            cfg.vpc_cidr,
            allow_create=allow_create_vswitch,
        )
    return vpc, cfg.v_switch_id


def ensure_net(
    c: EcsClient,
    cfg: EcsRuntimeConfig,
    ports: Sequence[int] = (),
    allow_create_vpc: bool = True,
    allow_create_vswitch: bool = True,
    allow_create_sg: bool = True,
) -> None:
    vpc, _ = ensure_vpc_and_vswitch(
        c,
        cfg,
        allow_create_vpc=allow_create_vpc,
        allow_create_vswitch=allow_create_vswitch,
    )
    cfg.security_group_id = cfg.security_group_id or ensure_sg(
        c,
        cfg.region_id,
        vpc,
        cfg.security_group_name,
        allow_create=allow_create_sg,
    )
    tags = _tag_dict(cfg)
    try:
        if cfg.security_group_id:
            _tag_resource(c, cfg.region_id, "securitygroup", cfg.security_group_id, tags)
    except Exception as exc:
        logger.warning(f"failed to tag security group {cfg.security_group_id}: {exc}")
    auth_port(c, cfg.region_id, cfg.security_group_id, 22)
    for p in ports:
        auth_port(c, cfg.region_id, cfg.security_group_id, p)


def list_zones_for_instance_type(
    c: EcsClient, r: str, instance_type: str, preferred_zones: Optional[Sequence[str]] = None
) -> list[str]:
    resp = c.describe_zones(ecs_models.DescribeZonesRequest(region_id=r))
    zones: list[str] = []
    for z in resp.body.zones.zone or []:
        if not z.zone_id:
            continue
        if preferred_zones and z.zone_id not in preferred_zones:
            continue
        zones.append(z.zone_id)
    return zones


def _disk_category(c: EcsClient, r: str, zone: str) -> Optional[str]:
    resp = c.describe_zones(ecs_models.DescribeZonesRequest(region_id=r))
    for z in resp.body.zones.zone or []:
        if z.zone_id != zone:
            continue
        for info in z.available_resources.resources_info or []:
            cats = info.system_disk_categories.supported_system_disk_category if info.system_disk_categories else []
            if cats:
                for pref in ["cloud_essd", "cloud_ssd", "cloud_efficiency", "cloud"]:
                    if pref in cats:
                        return pref
                return cats[0]
    return None


def _history_event_failed(event: object) -> bool:
    status = (getattr(event, "event_cycle_status", "") or "").lower()
    if status == "failed":
        return True
    name = (getattr(event, "event_name", "") or "").lower()
    etype = (getattr(event, "event_type", "") or "").lower()
    return any(keyword in name for keyword in ("fail", "error")) or any(keyword in etype for keyword in ("fail", "error"))


def _instances_failed_by_history(c: EcsClient, r: str, instance_ids: Sequence[str]) -> set[str]:
    failed: set[str] = set()
    for iid in instance_ids:
        try:
            resp = c.describe_instance_history_events(
                ecs_models.DescribeInstanceHistoryEventsRequest(region_id=r, instance_id=iid)
            )
            events = []
            if resp.body and getattr(resp.body, "instance_system_event_set", None):
                events = getattr(resp.body.instance_system_event_set, "instance_system_event", None) or []
            if any(_history_event_failed(e) for e in events or []):
                failed.add(iid)
        except Exception as exc:
            logger.warning(f"failed to query instance history events for {iid}: {exc}")
    return failed


def _run_instances_once(
    c: EcsClient,
    cfg: EcsRuntimeConfig,
    instance_type: str,
    disk_size: int,
    amount: int,
    allow_partial_success: bool = False,
) -> list[str]:
    dcat = _disk_category(c, cfg.region_id, cfg.zone_id)
    disk = ecs_models.RunInstancesRequestSystemDisk(category=dcat, size=str(disk_size)) if dcat else None
    name = f"{cfg.instance_name_prefix}-{int(time.time())}"
    tags = [ecs_models.RunInstancesRequestTag(key=k, value=v) for k, v in _tag_dict(cfg).items()]
    req = ecs_models.RunInstancesRequest(
        region_id=cfg.region_id,
        zone_id=cfg.zone_id,
        image_id=cfg.image_id,
        instance_type=instance_type,
        security_group_id=cfg.security_group_id,
        v_switch_id=cfg.v_switch_id,
        key_pair_name=cfg.key_pair_name,
        instance_name=name,
        internet_max_bandwidth_out=cfg.internet_max_bandwidth_out,
        internet_charge_type="PayByTraffic",
        instance_charge_type="PostPaid",
        tag=tags,
        amount=amount,
    )
    if allow_partial_success:
        req.min_amount = 1
    if cfg.use_spot:
        req.spot_strategy = cfg.spot_strategy
    if disk:
        req.system_disk = disk
    try:
        resp = c.run_instances(req)
        ids = resp.body.instance_id_sets.instance_id_set if resp.body and resp.body.instance_id_sets else []
        return list(ids or [])
    except Exception as exc:
        e = traceback.format_exc()
        if "OperationDenied.NoStock" in e:
            logger.error(f"run_instances got no stock for {cfg.region_id}/{cfg.zone_id}: {exc}")
            return []
        logger.error(f"run_instances failed for {cfg.region_id}/{cfg.zone_id}: {exc}")
        logger.error(e)
        return []


def create_instance(
    c: EcsClient,
    cfg: EcsRuntimeConfig,
    disk_size: int = 20,
    amount: int = 1,
    instance_types: Optional[Sequence[str]] = None,
) -> list[str]:
    if not cfg.instance_type and not instance_types:
        raise ValueError("instance_type required")
    if not cfg.zone_id:
        raise ValueError("zone_id required")
    img = cfg.image_id
    if not img:
        raise ValueError("image_id required")
    if not cfg.security_group_id:
        raise ValueError("security_group_id required")
    if not cfg.v_switch_id:
        raise ValueError("v_switch_id required")

    configured_types = [t.name for t in (cfg.instance_type or [])]
    types = [t for t in (instance_types or configured_types) if t]
    if not types:
        raise ValueError("instance_type required")
    selected_type = types[0]

    zones = list_zones_for_instance_type(c, cfg.region_id, selected_type)
    zone_candidates = [cfg.zone_id] + [z for z in zones if z != cfg.zone_id]

    remaining = amount
    region_instance_ids: list[str] = []

    # 尝试在相同 zone 开所有节点
    if amount <= RUN_INSTANCES_MAX_AMOUNT:
        for zone_id in zone_candidates:
            if remaining <= 0:
                break

            zone_instance_ids = _create_instance_in_zone(
                c,
                copy.deepcopy(cfg),
                zone_id,
                amount=remaining,
                disk_size=disk_size,
                allow_partial_success=False,
                instance_types=types,
            )
            
            region_instance_ids.extend(zone_instance_ids)
            remaining = amount - len(region_instance_ids)

    # 尝试在不同 zone 开节点
    for zone_id in zone_candidates:
        if remaining <= 0:
            break

        zone_instance_ids = _create_instance_in_zone(
            c,
            copy.deepcopy(cfg),
            zone_id,
            amount=remaining,
            disk_size=disk_size,
            allow_partial_success=False,
            instance_types=types,
        )
        
        region_instance_ids.extend(zone_instance_ids)
        remaining = amount - len(region_instance_ids)

    if remaining > 0:
        logger.warning(
            f"not enough instances provisioned in {cfg.region_id}; remaining required instances: {remaining}"
        )

    return region_instance_ids

def _create_instance_in_zone(
    c: EcsClient,
    cfg: EcsRuntimeConfig,
    zone_id: str,
    *,
    amount: int = 1,
    disk_size: int = 20,
    allow_partial_success: bool = False,
    instance_types: Optional[Sequence[str]] = None,
) -> List[str]:
    default_zone_id = cfg.zone_id
    
    if zone_id != default_zone_id:
        cfg.zone_id = zone_id
        cfg.v_switch_id = None
        try:
            ensure_vpc_and_vswitch(c, cfg, allow_create_vpc=True, allow_create_vswitch=True)
        except Exception as exc:
            logger.warning(f"failed to ensure vswitch for zone {zone_id}: {exc}")
            return []

    configured_types = [t.name for t in (cfg.instance_type or [])]
    types = [t for t in (instance_types or configured_types) if t]
    if not types:
        logger.warning(f"no instance types available for {cfg.region_id}/{zone_id}")
        return []

    zone_instance_ids: list[str] = []

    for instance_type in types:
        if len(zone_instance_ids) >= amount:
            break
        while len(zone_instance_ids) < amount:
            target = min(amount - len(zone_instance_ids), RUN_INSTANCES_MAX_AMOUNT)
            ids = _run_instances_once(
                c,
                cfg,
                instance_type,
                disk_size,
                amount=target,
                allow_partial_success=allow_partial_success,
            )
            zone_instance_ids.extend(ids)
            if len(ids) < target:
                break

    if not zone_instance_ids:
        logger.warning(f"no instances returned for {cfg.region_id}/{zone_id}")
        return []

    failed = _instances_failed_by_history(c, cfg.region_id, zone_instance_ids)
    if failed:
        logger.warning(
            f"{len(failed)} instances reported failure in {cfg.region_id}/{zone_id}: {sorted(failed)}"
        )
    return [iid for iid in zone_instance_ids if iid not in failed]

def ensure_keypair(c: EcsClient, r: str, name: str, key_path: str, allow_create: bool = True) -> None:
    resp = c.describe_key_pairs(ecs_models.DescribeKeyPairsRequest(region_id=r, key_pair_name=name))
    if resp.body.key_pairs.key_pair:
        return
    if not allow_create:
        raise RuntimeError(f"keypair {name} not found in {r}")
    kp = Path(key_path).expanduser().resolve()
    if not kp.exists():
        kp.parent.mkdir(parents=True, exist_ok=True)
        subprocess.run(
            ["ssh-keygen", "-t", "rsa", "-b", "2048", "-m", "PEM", "-f", str(kp), "-N", ""],
            capture_output=True,
            check=True,
        )
    res = subprocess.run(["ssh-keygen", "-y", "-f", str(kp)], capture_output=True, text=True, check=True)
    c.import_key_pair(ecs_models.ImportKeyPairRequest(region_id=r, key_pair_name=name, public_key_body=res.stdout.strip()))


# --- Instance ---
@dataclass
class InstanceHandle:
    client: EcsClient
    config: EcsRuntimeConfig
    instance_id: str
    public_ip: str


