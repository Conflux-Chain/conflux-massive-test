"""Instance preparation and lifecycle helpers for Aliyun ECS."""
import asyncio
import ipaddress
import json
import socket
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Sequence

import asyncssh
from alibabacloud_ecs20140526 import models as ecs_models
from alibabacloud_ecs20140526.client import Client as EcsClient
from loguru import logger

from utils.wait_until import wait_until
from .config import EcsConfig, client


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


def ensure_vpc(c: EcsClient, r: str, name: str, cidr: str) -> str:
    resp = c.describe_vpcs(ecs_models.DescribeVpcsRequest(region_id=r, page_size=50))
    for v in resp.body.vpcs.vpc or []:
        if v.vpc_name == name:
            return v.vpc_id
    cr = c.create_vpc(ecs_models.CreateVpcRequest(region_id=r, vpc_name=name, cidr_block=cidr))
    vid = cr.body.vpc_id
    wait_until(lambda: _vpc_available(c, r, vid), timeout=120, retry_interval=3)
    return vid


def _vpc_available(c: EcsClient, r: str, vid: str) -> bool:
    resp = c.describe_vpcs(ecs_models.DescribeVpcsRequest(region_id=r, vpc_id=vid))
    vpcs = resp.body.vpcs.vpc if resp.body and resp.body.vpcs else []
    return vpcs and vpcs[0].status == "Available"


def ensure_vswitch(c: EcsClient, r: str, vpc: str, zone: str, name: str, cidr: str, vpc_cidr: str) -> str:
    resp = c.describe_vswitches(ecs_models.DescribeVSwitchesRequest(region_id=r, vpc_id=vpc, page_size=50))
    vsws = resp.body.v_switches.v_switch if resp.body and resp.body.v_switches else []
    for v in vsws:
        if v.v_switch_name == name and v.zone_id == zone:
            return v.v_switch_id
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


def ensure_sg(c: EcsClient, r: str, vpc: str, name: str) -> str:
    resp = c.describe_security_groups(ecs_models.DescribeSecurityGroupsRequest(region_id=r, vpc_id=vpc, page_size=50))
    for g in resp.body.security_groups.security_group or []:
        if g.security_group_name == name:
            return g.security_group_id
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


def ensure_net(c: EcsClient, cfg: EcsConfig, ports: Sequence[int] = ()) -> None:
    cfg.zone_id = cfg.zone_id or pick_zone(c, cfg.region_id)
    vpc = ensure_vpc(c, cfg.region_id, cfg.vpc_name, cfg.vpc_cidr)
    cfg.v_switch_id = cfg.v_switch_id or ensure_vswitch(
        c, cfg.region_id, vpc, cfg.zone_id, cfg.vswitch_name, cfg.vswitch_cidr, cfg.vpc_cidr
    )
    cfg.security_group_id = cfg.security_group_id or ensure_sg(c, cfg.region_id, vpc, cfg.security_group_name)
    auth_port(c, cfg.region_id, cfg.security_group_id, 22)
    for p in ports:
        auth_port(c, cfg.region_id, cfg.security_group_id, p)


def pick_instance_type(c: EcsClient, cfg: EcsConfig) -> Optional[tuple[str, str, Optional[str]]]:
    spot = cfg.spot_strategy if cfg.use_spot else None
    req = ecs_models.DescribeAvailableResourceRequest(
        region_id=cfg.region_id,
        destination_resource="InstanceType",
        resource_type="instance",
        instance_charge_type="PostPaid",
        spot_strategy=spot,
        cores=cfg.min_cpu_cores,
        memory=cfg.min_memory_gb,
    )
    resp = c.describe_available_resource(req)
    for z in resp.body.available_zones.available_zone or []:
        if z.status_category not in {"WithStock", "ClosedWithStock"}:
            continue
        types = [
            i.value
            for r in (z.available_resources.available_resource or [])
            if r.type == "InstanceType"
            for i in (r.supported_resources.supported_resource or [])
            if i.status_category in {"WithStock", "ClosedWithStock"}
        ]
        if not types:
            continue
        tresp = c.describe_instance_types(ecs_models.DescribeInstanceTypesRequest(instance_types=types))
        tmap = {t.instance_type_id: t for t in (tresp.body.instance_types.instance_type or []) if t.instance_type_id}
        cands = [
            t
            for t in tmap.values()
            if t.cpu_core_count == cfg.min_cpu_cores
            and t.memory_size
            and cfg.min_memory_gb <= t.memory_size <= cfg.max_memory_gb
            and (not cfg.cpu_vendor or cfg.cpu_vendor.lower() in (t.physical_processor_model or "").lower())
        ]
        if cands:
            cands.sort(key=lambda t: (t.memory_size, t.instance_type_id))
            s = cands[0]
            v = (
                "intel"
                if "intel" in (s.physical_processor_model or "").lower()
                else ("amd" if "amd" in (s.physical_processor_model or "").lower() else None)
            )
            return z.zone_id, s.instance_type_id, v
    return None


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


def create_instance(c: EcsClient, cfg: EcsConfig, disk_size: int = 40) -> str:
    if not cfg.instance_type:
        raise ValueError("instance_type required")
    dcat = _disk_category(c, cfg.region_id, cfg.zone_id)
    disk = ecs_models.CreateInstanceRequestSystemDisk(category=dcat, size=disk_size) if dcat else None
    name = f"{cfg.instance_name_prefix}-{int(time.time())}"
    img = cfg.image_id or cfg.base_image_id
    req = ecs_models.CreateInstanceRequest(
        region_id=cfg.region_id,
        zone_id=cfg.zone_id,
        image_id=img,
        instance_type=cfg.instance_type,
        security_group_id=cfg.security_group_id,
        v_switch_id=cfg.v_switch_id,
        key_pair_name=cfg.key_pair_name,
        instance_name=name,
        internet_max_bandwidth_out=cfg.internet_max_bandwidth_out,
        internet_charge_type="PayByTraffic",
        instance_charge_type="PostPaid",
        spot_strategy=cfg.spot_strategy if cfg.use_spot else None,
        system_disk=disk,
    )
    resp = c.create_instance(req)
    if not resp.body or not resp.body.instance_id:
        raise RuntimeError("instance creation failed")
    return resp.body.instance_id


def ensure_keypair(c: EcsClient, r: str, name: str, key_path: str) -> None:
    resp = c.describe_key_pairs(ecs_models.DescribeKeyPairsRequest(region_id=r, key_pair_name=name))
    if resp.body.key_pairs.key_pair:
        return
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


async def _wait_tcp(host: str, port: int, timeout: int, interval: int = 3) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(2)
            if s.connect_ex((host, port)) == 0:
                return
        await asyncio.sleep(interval)
    raise TimeoutError(f"port {port} not open on {host}")


async def wait_ssh(host: str, user: str, key: str, timeout: int, interval: int = 3) -> None:
    await _wait_tcp(host, 22, timeout, interval)
    kp = str(Path(key).expanduser())
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            conn = await asyncssh.connect(host, username=user, client_keys=[kp], known_hosts=None)
            conn.close()
            await conn.wait_closed()
            return
        except Exception:
            await asyncio.sleep(interval)
    raise TimeoutError(f"SSH not ready for {host}")


# --- Instance ---
@dataclass
class InstanceHandle:
    client: EcsClient
    config: EcsConfig
    instance_id: str
    public_ip: str


def provision_instance(cfg: EcsConfig) -> InstanceHandle:
    c = client(cfg.credentials, cfg.region_id, cfg.endpoint)
    sel = pick_instance_type(c, cfg)
    if not sel:
        raise RuntimeError("no instance type")
    cfg.zone_id, cfg.instance_type, cfg.cpu_vendor = sel
    ensure_net(c, cfg)
    iid = create_instance(c, cfg, disk_size=100)
    logger.info(f"instance: {iid}")
    st = wait_status(c, cfg.region_id, iid, ["Stopped", "Running"], cfg.poll_interval, cfg.wait_timeout)
    if st == "Stopped":
        start_instance(c, iid)
    allocate_public_ip(c, cfg.region_id, iid, cfg.poll_interval, cfg.wait_timeout)
    ip = wait_running(c, cfg.region_id, iid, cfg.poll_interval, cfg.wait_timeout)
    logger.info(f"instance ready: {ip}")
    return InstanceHandle(client=c, config=cfg, instance_id=iid, public_ip=ip)


def cleanup_instance(h: InstanceHandle) -> None:
    delete_instance(h.client, h.config.region_id, h.instance_id)
    logger.info(f"deleted: {h.instance_id}")
