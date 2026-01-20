"""Aliyun ECS utilities and image/instance management for Conflux deployment."""
import argparse
import asyncio
import ipaddress
import json
import os
import socket
import subprocess
import tarfile
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Coroutine, Optional, Sequence

import asyncssh
from alibabacloud_ecs20140526 import models as ecs_models
from alibabacloud_ecs20140526.client import Client as EcsClient
from alibabacloud_tea_openapi.models import Config as AliyunConfig
from dotenv import load_dotenv
from loguru import logger

from remote_simulation.config_builder import SingleNodeConfig, single_node_config_text
from utils.wait_until import wait_until

DEFAULT_REGION = "ap-southeast-3"
DEFAULT_KEYPAIR = "chenxinghao-conflux-image-builder"
DEFAULT_SSH_KEY = "./keys/chenxinghao-conflux-image-builder.pem"
DEFAULT_VPC = "conflux-image-builder"
DEFAULT_VPC_CIDR = "10.0.0.0/16"
DEFAULT_VSWITCH_CIDR = "10.0.0.0/24"


@dataclass
class AliCredentials:
    access_key_id: str
    access_key_secret: str


@dataclass
class EcsConfig:
    credentials: AliCredentials = field(default_factory=lambda: load_credentials())
    region_id: str = DEFAULT_REGION
    zone_id: Optional[str] = None
    endpoint: Optional[str] = None
    base_image_id: Optional[str] = None
    image_id: Optional[str] = None
    instance_type: Optional[str] = None
    min_cpu_cores: int = 4
    min_memory_gb: float = 8.0
    max_memory_gb: float = 8.0
    cpu_vendor: Optional[str] = None
    use_spot: bool = True
    spot_strategy: str = "SpotAsPriceGo"
    v_switch_id: Optional[str] = None
    security_group_id: Optional[str] = None
    vpc_name: str = DEFAULT_VPC
    vswitch_name: str = DEFAULT_VPC
    security_group_name: str = DEFAULT_VPC
    vpc_cidr: str = DEFAULT_VPC_CIDR
    vswitch_cidr: str = DEFAULT_VSWITCH_CIDR
    key_pair_name: str = DEFAULT_KEYPAIR
    ssh_username: str = "root"
    ssh_private_key_path: str = DEFAULT_SSH_KEY
    conflux_git_ref: str = "v3.0.2"
    image_prefix: str = "conflux"
    instance_name_prefix: str = "conflux-builder"
    internet_max_bandwidth_out: int = 10
    search_all_regions: bool = False
    cleanup_builder_instance: bool = True
    poll_interval: int = 5
    wait_timeout: int = 1800


def load_credentials() -> AliCredentials:
    load_dotenv()
    ak, sk = os.getenv("ALI_ACCESS_KEY_ID", "").strip(), os.getenv("ALI_ACCESS_KEY_SECRET", "").strip()
    if not ak or not sk:
        raise ValueError("Missing ALI_ACCESS_KEY_ID or ALI_ACCESS_KEY_SECRET")
    return AliCredentials(ak, sk)


def load_endpoint() -> Optional[str]:
    return os.getenv("ALI_ECS_ENDPOINT", "").strip() or None


def client(creds: AliCredentials, region: str, endpoint: Optional[str] = None) -> EcsClient:
    if endpoint and "cloudcontrol" in endpoint:
        endpoint = f"ecs.{region}.aliyuncs.com"
    return EcsClient(AliyunConfig(access_key_id=creds.access_key_id, access_key_secret=creds.access_key_secret, region_id=region, endpoint=endpoint))


def _instance_info(c: EcsClient, r: str, iid: str) -> tuple[Optional[str], Optional[str]]:
    resp = c.describe_instances(ecs_models.DescribeInstancesRequest(region_id=r, instance_ids=json.dumps([iid])))
    instances = resp.body.instances.instance if resp.body and resp.body.instances else []
    if not instances:
        return None, None
    ips = instances[0].public_ip_address.ip_address if instances[0].public_ip_address else []
    return instances[0].status, ips[0] if ips else None


def wait_status(c: EcsClient, r: str, iid: str, want: Sequence[str], poll: int, timeout: int) -> str:
    h = {"s": None}

    def chk():
        h["s"], _ = _instance_info(c, r, iid)
        return h["s"] in want

    wait_until(chk, timeout=timeout, retry_interval=poll)
    return h["s"] or ""


def wait_running(c: EcsClient, r: str, iid: str, poll: int, timeout: int) -> str:
    h = {"ip": None}

    def chk():
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
    return c.create_security_group(ecs_models.CreateSecurityGroupRequest(region_id=r, vpc_id=vpc, security_group_name=name, description="conflux")).body.security_group_id


def auth_port(c: EcsClient, r: str, sg: str, port: int) -> None:
    resp = c.describe_security_group_attribute(ecs_models.DescribeSecurityGroupAttributeRequest(region_id=r, security_group_id=sg))
    for p in resp.body.permissions.permission or []:
        if p.ip_protocol == "tcp" and p.port_range == f"{port}/{port}" and p.source_cidr_ip == "0.0.0.0/0":
            return
    c.authorize_security_group(ecs_models.AuthorizeSecurityGroupRequest(region_id=r, security_group_id=sg, ip_protocol="tcp", port_range=f"{port}/{port}", source_cidr_ip="0.0.0.0/0"))


def ensure_net(c: EcsClient, cfg: EcsConfig, ports: Sequence[int] = ()) -> None:
    cfg.zone_id = cfg.zone_id or pick_zone(c, cfg.region_id)
    vpc = ensure_vpc(c, cfg.region_id, cfg.vpc_name, cfg.vpc_cidr)
    cfg.v_switch_id = cfg.v_switch_id or ensure_vswitch(c, cfg.region_id, vpc, cfg.zone_id, cfg.vswitch_name, cfg.vswitch_cidr, cfg.vpc_cidr)
    cfg.security_group_id = cfg.security_group_id or ensure_sg(c, cfg.region_id, vpc, cfg.security_group_name)
    auth_port(c, cfg.region_id, cfg.security_group_id, 22)
    for p in ports:
        auth_port(c, cfg.region_id, cfg.security_group_id, p)


def pick_instance_type(c: EcsClient, cfg: EcsConfig) -> Optional[tuple[str, str, Optional[str]]]:
    spot = cfg.spot_strategy if cfg.use_spot else None
    req = ecs_models.DescribeAvailableResourceRequest(region_id=cfg.region_id, destination_resource="InstanceType", resource_type="instance", instance_charge_type="PostPaid", spot_strategy=spot, cores=cfg.min_cpu_cores, memory=cfg.min_memory_gb)
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
            v = "intel" if "intel" in (s.physical_processor_model or "").lower() else ("amd" if "amd" in (s.physical_processor_model or "").lower() else None)
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
        subprocess.run(["ssh-keygen", "-t", "rsa", "-b", "2048", "-m", "PEM", "-f", str(kp), "-N", ""], capture_output=True, check=True)
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


# --- Image ---

def _img_name(prefix: str, ref: str) -> str:
    return f"{prefix}-conflux-{ref.replace('/', '-').replace(':', '-')}"


def find_ubuntu(c: EcsClient, r: str) -> str:
    resp = c.describe_images(ecs_models.DescribeImagesRequest(region_id=r, image_owner_alias="system", ostype="linux", architecture="x86_64", page_size=100))
    imgs = [i for i in (resp.body.images.image or []) if i.image_name and i.image_name.startswith("ubuntu_20_04")]
    if not imgs:
        raise RuntimeError("no Ubuntu 20.04 image")
    imgs.sort(key=lambda i: i.creation_time or "", reverse=True)
    return imgs[0].image_id


def find_img(c: EcsClient, r: str, name: str) -> Optional[str]:
    resp = c.describe_images(ecs_models.DescribeImagesRequest(region_id=r, image_name=name, image_owner_alias="self"))
    for i in resp.body.images.image or []:
        if i.image_name == name:
            return i.image_id
    return None


def wait_img(c: EcsClient, r: str, img: str, poll: int, timeout: int) -> None:
    def chk():
        resp = c.describe_images(ecs_models.DescribeImagesRequest(region_id=r, image_id=img))
        imgs = resp.body.images.image if resp.body and resp.body.images else []
        if not imgs:
            return False
        st = imgs[0].status
        logger.info(f"image {img}: {st}")
        if st in {"CreateFailed", "UnAvailable", "Deprecated"}:
            raise RuntimeError(f"image failed: {st}")
        return st == "Available"

    wait_until(chk, timeout=timeout, retry_interval=poll)


async def default_prepare(host: str, cfg: EcsConfig) -> None:
    key = str(Path(cfg.ssh_private_key_path).expanduser())
    await _wait_tcp(host, 22, cfg.wait_timeout, 3)
    async with asyncssh.connect(host, username=cfg.ssh_username, client_keys=[key], known_hosts=None) as conn:
        async def run(cmd: str) -> None:
            logger.info(f"remote: {cmd}")
            r = await conn.run(cmd, check=False)
            if r.stdout:
                logger.info(r.stdout.strip())
            if r.stderr:
                logger.warning(r.stderr.strip())
            if r.exit_status != 0:
                raise RuntimeError(f"failed: {cmd}")

        await run("sudo apt-get update -y")
        await run("sudo apt-get install -y build-essential clang cmake pkg-config libssl-dev git curl ca-certificates")
        await run("sudo mkdir -p /opt/conflux/src /opt/conflux/config")
        await run("if [ ! -d /opt/conflux/src/conflux-rust ]; then sudo git clone --depth 1 https://github.com/Conflux-Chain/conflux-rust.git /opt/conflux/src/conflux-rust; fi")
        await run(
            "sudo bash -lc 'cd /opt/conflux/src/conflux-rust; git fetch --depth 1 origin "
            f"{cfg.conflux_git_ref} || true; git checkout {cfg.conflux_git_ref} || git checkout FETCH_HEAD; "
            "git submodule update --init --recursive; curl https://sh.rustup.rs -sSf | sh -s -- -y; "
            "source $HOME/.cargo/env; cargo build --release --bin conflux; install -m 0755 target/release/conflux /usr/local/bin/conflux'"
        )


def create_server_image(
    cfg: EcsConfig,
    dry_run: bool = False,
    prepare_fn: Callable[[str, EcsConfig], Coroutine[Any, Any, None]] = default_prepare,
) -> str:
    name = _img_name(cfg.image_prefix, cfg.conflux_git_ref)
    c = client(cfg.credentials, cfg.region_id, cfg.endpoint)
    if not cfg.base_image_id:
        cfg.base_image_id = find_ubuntu(c, cfg.region_id)
    existing = find_img(c, cfg.region_id, name)
    if existing:
        logger.info(f"image exists: {existing}")
        if not dry_run:
            wait_img(c, cfg.region_id, existing, cfg.poll_interval, cfg.wait_timeout)
        return f"dry-run:{existing}" if dry_run else existing
    if dry_run:
        return f"dry-run:{name}"
    sel = pick_instance_type(c, cfg)
    if not sel and cfg.use_spot:
        cfg.use_spot = False
        sel = pick_instance_type(c, cfg)
    if not sel:
        raise RuntimeError("no instance type")
    cfg.zone_id, cfg.instance_type, cfg.cpu_vendor = sel
    ensure_net(c, cfg)
    ensure_keypair(c, cfg.region_id, cfg.key_pair_name, cfg.ssh_private_key_path)
    iid = ""
    try:
        iid = create_instance(c, cfg)
        logger.info(f"builder: {iid}")
        st = wait_status(c, cfg.region_id, iid, ["Stopped", "Running"], cfg.poll_interval, cfg.wait_timeout)
        if st == "Stopped":
            start_instance(c, iid)
        wait_status(c, cfg.region_id, iid, ["Running"], cfg.poll_interval, cfg.wait_timeout)
        allocate_public_ip(c, cfg.region_id, iid, cfg.poll_interval, cfg.wait_timeout)
        ip = wait_running(c, cfg.region_id, iid, cfg.poll_interval, cfg.wait_timeout)
        logger.info(f"builder ready: {ip}")
        asyncio.run(prepare_fn(ip, cfg))
        logger.info("stopping builder")
        stop_instance(c, iid, "StopCharging")
        wait_status(c, cfg.region_id, iid, ["Stopped"], cfg.poll_interval, cfg.wait_timeout)
        cr = c.create_image(ecs_models.CreateImageRequest(region_id=cfg.region_id, instance_id=iid, image_name=name))
        if not cr.body or not cr.body.image_id:
            stop_instance(c, iid, None)
            wait_status(c, cfg.region_id, iid, ["Stopped"], cfg.poll_interval, cfg.wait_timeout)
            cr = c.create_image(ecs_models.CreateImageRequest(region_id=cfg.region_id, instance_id=iid, image_name=name))
        img = cr.body.image_id
        logger.info(f"image started: {img}")
        wait_img(c, cfg.region_id, img, cfg.poll_interval, cfg.wait_timeout)
        return img
    finally:
        if cfg.cleanup_builder_instance and iid:
            try:
                delete_instance(c, cfg.region_id, iid)
                logger.info(f"builder deleted: {iid}")
            except Exception as e:
                logger.warning(f"delete failed: {e}")


# --- Docker ---
DEFAULT_DOCKER_TAG = "conflux-single-node:latest"
DEFAULT_DOCKER_REPO = "https://github.com/Conflux-Chain/conflux-rust.git"
DEFAULT_DOCKER_BRANCH = "v3.0.2"
DEFAULT_DOCKER_CTX = Path(__file__).resolve().parents[1] / "node_docker_image"
DEFAULT_SVC = "conflux-docker"


def _pos_config_source() -> Path:
    return Path(__file__).resolve().parents[1] / "ref" / "zero-gravity-swap" / "pos_config"


def make_docker_prepare(node: SingleNodeConfig, ctx: Path, tag: str, repo: str, branch: str, svc: str):
    async def prepare(host: str, cfg: EcsConfig) -> None:
        key = str(Path(cfg.ssh_private_key_path).expanduser())
        await _wait_tcp(host, 22, cfg.wait_timeout, 3)
        async with asyncssh.connect(host, username=cfg.ssh_username, client_keys=[key], known_hosts=None) as conn:
            async def run(cmd: str, check: bool = True) -> None:
                logger.info(f"remote: {cmd}")
                r = await conn.run(cmd, check=False)
                if r.stdout:
                    logger.info(r.stdout.strip())
                if r.stderr:
                    logger.warning(r.stderr.strip())
                if check and r.exit_status != 0:
                    raise RuntimeError(f"failed: {cmd}")

            await run("sudo apt-get update -y")
            await run("sudo apt-get install -y docker.io ca-certificates curl tar")
            await run("sudo systemctl enable --now docker")
            for d in ["/opt/conflux/config", node.data_dir, "/opt/conflux/logs", "/opt/conflux/pos_config"]:
                await run(f"sudo mkdir -p {d}")
            cfgtxt = single_node_config_text(node)
            local = Path(f"/tmp/cfx_{int(time.time())}.toml")
            local.write_text(cfgtxt)
            await asyncssh.scp(str(local), (conn, "/opt/conflux/config/conflux_0.toml"))
            local.unlink(missing_ok=True)
            pos = _pos_config_source()
            if not pos.exists():
                raise FileNotFoundError(f"pos_config not found: {pos}")
            pa = Path(f"/tmp/pos_{int(time.time())}.tar.gz")
            with tarfile.open(pa, "w:gz") as t:
                t.add(pos, arcname="pos_config")
            await asyncssh.scp(str(pa), (conn, f"/tmp/{pa.name}"))
            pa.unlink(missing_ok=True)
            await run(f"sudo tar -xzf /tmp/{pa.name} -C /opt/conflux/pos_config --strip-components=1")
            await run("sudo mkdir -p /opt/conflux/pos_config/log")
            if not ctx.exists():
                raise FileNotFoundError(f"docker ctx not found: {ctx}")
            ca = Path(f"/tmp/dctx_{int(time.time())}.tar.gz")
            with tarfile.open(ca, "w:gz") as t:
                t.add(ctx, arcname=".")
            await asyncssh.scp(str(ca), (conn, f"/tmp/{ca.name}"))
            ca.unlink(missing_ok=True)
            await run("sudo rm -rf /opt/conflux/docker && sudo mkdir -p /opt/conflux/docker")
            await run(f"sudo tar -xzf /tmp/{ca.name} -C /opt/conflux/docker")
            try:
                await run(
                    "sudo bash -lc 'set -o pipefail; DOCKER_BUILDKIT=0 docker build --build-arg CACHEBUST="
                    f"{int(time.time())} --build-arg BRANCH={branch} --build-arg REPO_URL={repo} -t {tag} /opt/conflux/docker 2>&1 | tee /tmp/docker.log'"
                )
            except Exception:
                await run("sudo tail -n 200 /tmp/docker.log || true", check=False)
                raise
            script = (
                "#!/usr/bin/env bash\n"
                "set -euo pipefail\n"
                f"docker rm -f {svc} >/dev/null 2>&1 || true\n"
                "exec docker run --rm --name "
                f"{svc} --net=host --privileged --ulimit nofile=65535:65535 --ulimit nproc=65535:65535 "
                "--ulimit core=-1 -v /opt/conflux/config:/opt/conflux/config -v "
                f"{node.data_dir}:{node.data_dir} -v /opt/conflux/logs:/opt/conflux/logs -v /opt/conflux/pos_config:/opt/conflux/pos_config "
                f"-v /opt/conflux/pos_config:/app/pos_config -w /opt/conflux/logs {tag} /root/conflux --config /opt/conflux/config/conflux_0.toml"
            )
            unit = (
                "[Unit]\n"
                f"Description=Conflux ({svc})\n"
                "After=docker.service\n"
                "Requires=docker.service\n\n"
                "[Service]\n"
                "Type=simple\n"
                f"ExecStart=/usr/local/bin/{svc}-run.sh\n"
                f"ExecStop=/usr/bin/docker stop {svc}\n"
                "Restart=always\n"
                "RestartSec=5\n\n"
                "[Install]\n"
                "WantedBy=multi-user.target"
            )
            await run(
                "sudo bash -lc 'cat > /usr/local/bin/"
                f"{svc}-run.sh << \"EOF\"\n{script}\nEOF\nchmod +x /usr/local/bin/{svc}-run.sh'"
            )
            await run(
                "sudo bash -lc 'cat > /etc/systemd/system/"
                f"{svc}.service << \"EOF\"\n{unit}\nEOF\nsystemctl daemon-reload; systemctl enable {svc}.service'"
            )

    return prepare


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


# --- CLI ---
def add_args(p: argparse.ArgumentParser) -> None:
    p.add_argument("--conflux-git-ref", default="v3.0.2")
    p.add_argument("--base-image-id", default=None)
    p.add_argument("--instance-type", default=None)
    p.add_argument("--min-cpu-cores", type=int, default=4)
    p.add_argument("--min-memory-gb", type=float, default=8.0)
    p.add_argument("--max-memory-gb", type=float, default=8.0)
    p.add_argument("--v-switch-id", default=None)
    p.add_argument("--security-group-id", default=None)
    p.add_argument("--key-pair-name", default=DEFAULT_KEYPAIR)
    p.add_argument("--region-id", default=DEFAULT_REGION)
    p.add_argument("--zone-id", default=None)
    p.add_argument("--endpoint", default=None)
    p.add_argument("--image-prefix", default="conflux")
    p.add_argument("--ssh-username", default="root")
    p.add_argument("--ssh-private-key", default=DEFAULT_SSH_KEY)
    p.add_argument("--internet-max-bandwidth-out", type=int, default=10)
    p.add_argument("--poll-interval", type=int, default=5)
    p.add_argument("--wait-timeout", type=int, default=1800)
    p.add_argument("--search-all-regions", action="store_true")
    p.add_argument("--spot", dest="spot", action="store_true")
    p.add_argument("--no-spot", dest="spot", action="store_false")
    p.set_defaults(spot=True)
    p.add_argument("--spot-strategy", default="SpotAsPriceGo")
    p.add_argument("--vpc-name", default="conflux-image-builder")
    p.add_argument("--vswitch-name", default="conflux-image-builder")
    p.add_argument("--security-group-name", default="conflux-image-builder")
    p.add_argument("--vpc-cidr", default="10.0.0.0/16")
    p.add_argument("--vswitch-cidr", default="10.0.0.0/24")
    p.add_argument("--no-cleanup", action="store_true")
    p.add_argument("--dry-run", action="store_true")


def cfg_from_args(a) -> EcsConfig:
    return EcsConfig(
        credentials=load_credentials(),
        region_id=a.region_id,
        zone_id=a.zone_id,
        endpoint=a.endpoint or load_endpoint(),
        base_image_id=a.base_image_id,
        instance_type=a.instance_type,
        min_cpu_cores=a.min_cpu_cores,
        min_memory_gb=a.min_memory_gb,
        max_memory_gb=a.max_memory_gb,
        use_spot=a.spot,
        spot_strategy=a.spot_strategy,
        v_switch_id=a.v_switch_id,
        security_group_id=a.security_group_id,
        vpc_name=a.vpc_name,
        vswitch_name=a.vswitch_name,
        security_group_name=a.security_group_name,
        vpc_cidr=a.vpc_cidr,
        vswitch_cidr=a.vswitch_cidr,
        key_pair_name=a.key_pair_name,
        ssh_username=a.ssh_username,
        ssh_private_key_path=a.ssh_private_key,
        conflux_git_ref=a.conflux_git_ref,
        image_prefix=a.image_prefix,
        internet_max_bandwidth_out=a.internet_max_bandwidth_out,
        search_all_regions=a.search_all_regions,
        cleanup_builder_instance=not a.no_cleanup,
        poll_interval=a.poll_interval,
        wait_timeout=a.wait_timeout,
    )


def main() -> None:
    p = argparse.ArgumentParser(description="Create Alibaba Cloud image for Conflux")
    add_args(p)
    a = p.parse_args()
    logger.info(f"image: {create_server_image(cfg_from_args(a), dry_run=a.dry_run)}")


def main_docker() -> None:
    p = argparse.ArgumentParser(description="Create Alibaba Cloud image with Docker Conflux")
    add_args(p)
    p.set_defaults(image_prefix="conflux-docker")
    p.add_argument("--rpc-port", type=int, default=12537)
    p.add_argument("--ws-port", type=int, default=12538)
    p.add_argument("--evm-rpc-port", type=int, default=12539)
    p.add_argument("--evm-ws-port", type=int, default=12540)
    p.add_argument("--chain-id", type=int, default=1024)
    p.add_argument("--evm-chain-id", type=int, default=1025)
    p.add_argument("--mining-author", default=None)
    p.add_argument("--docker-context", default=str(DEFAULT_DOCKER_CTX))
    p.add_argument("--docker-tag", default=DEFAULT_DOCKER_TAG)
    p.add_argument("--docker-repo-url", default=DEFAULT_DOCKER_REPO)
    p.add_argument("--docker-branch", default=DEFAULT_DOCKER_BRANCH)
    p.add_argument("--service-name", default=DEFAULT_SVC)
    a = p.parse_args()
    node = SingleNodeConfig(
        rpc_port=a.rpc_port,
        ws_port=a.ws_port,
        evm_rpc_port=a.evm_rpc_port,
        evm_ws_port=a.evm_ws_port,
        chain_id=a.chain_id,
        evm_chain_id=a.evm_chain_id,
        mining_author=a.mining_author,
    )
    prep = make_docker_prepare(
        node,
        Path(a.docker_context).expanduser(),
        a.docker_tag,
        a.docker_repo_url,
        a.docker_branch,
        a.service_name,
    )
    logger.info(f"image: {create_server_image(cfg_from_args(a), dry_run=a.dry_run, prepare_fn=prep)}")


if __name__ == "__main__":
    main()
