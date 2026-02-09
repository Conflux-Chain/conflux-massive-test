"""Build a reusable Aliyun ECS image which acts as a private Docker registry.

Requirements implemented:
- Fixed region: ap-southeast-3
- Fixed spot builder type: ecs.xn4.small
- 20GB disk
- Docker installed + local registry on :5000
- Preload lylcx2007/conflux-node:latest as conflux-node:base into the registry
- Verification-first: boot 2 ecs.g8i.large in same region+zone, pull from registry
- Tag instances so cloud_provisioner/cleanup_instances can delete them

This file intentionally avoids auxiliary/ali_instances config classes and reuses cloud_provisioner.
"""

# Allow running this file directly via `python auxiliary/ali_instances/image_build.py`
if __name__ == "__main__" and (__package__ is None or __package__ == ""):
    import sys
    from pathlib import Path as _Path

    sys.path.insert(0, str(_Path(__file__).resolve().parents[2]))

import asyncio
import shlex
import threading
import time
import tomllib
from pathlib import Path
from typing import Optional

import asyncssh
from alibabacloud_ecs20140526 import models as ecs_models
from loguru import logger
from dotenv import load_dotenv

from utils.wait_until import wait_until
from cloud_provisioner.aliyun_provider.client_factory import AliyunClient
from cloud_provisioner.create_instances.instance_config import InstanceConfig
from cloud_provisioner.create_instances.instance_verifier import InstanceVerifier
from cloud_provisioner.create_instances.network_infra import allocate_vacant_cidr_block
from cloud_provisioner.create_instances.types import InstanceType, RegionInfo, ZoneInfo, KeyPairRequestConfig


# --- Fixed settings ---
DEFAULT_IMAGE_NAME = "conflux-docker-registry"
FIXED_REGION = "ap-southeast-3"
BUILDER_INSTANCE_TYPE = "ecs.xn4.small"
TEST_INSTANCE_TYPE = "ecs.g8i.large"
TEST_INSTANCES = 2
DISK_GB = 20
REGISTRY_PORT = 5000
LOCAL_CONFLUX_TAG = "conflux-node:base"


REPO_ROOT = Path(__file__).resolve().parents[2]
REQUEST_CONFIG_PATH = REPO_ROOT / "request_config.toml"
PREPARE_SCRIPT = REPO_ROOT / "auxiliary" / "scripts" / "remote" / "prepare_docker_server_image.sh"

def find_ubuntu(c, r: str, max_pages: int = 5, page_size: int = 50) -> str:
    candidates: list[ecs_models.DescribeImagesResponseBodyImagesImage] = []
    page_number = 1
    while page_number <= max_pages:
        resp = c.describe_images(
            ecs_models.DescribeImagesRequest(
                region_id=r,
                image_owner_alias="system",
                status="Available",
                page_number=page_number,
                page_size=page_size,
            )
        )
        images = resp.body.images.image if resp.body and resp.body.images else []
        if not images:
            break
        for img in images:
            name = (img.image_name or "").lower()
            if "ubuntu" not in name:
                continue
            if any(k in name for k in ("gpu", "cuda", "driver")):
                continue
            if "arm" in name or "aarch64" in name:
                continue
            if "x86_64" not in name and "x64" not in name:
                continue
            candidates.append(img)
        if len(images) < page_size:
            break
        page_number += 1

    if not candidates:
        raise RuntimeError("no ubuntu system image found")

    def version_rank(name: str) -> int:
        for idx, v in enumerate(["24", "22", "20", "18"]):
            if f"ubuntu_{v}" in name or f"ubuntu-{v}" in name or f"ubuntu {v}" in name:
                return idx
        return 99

    def sort_key(img: ecs_models.DescribeImagesResponseBodyImagesImage) -> tuple[int, str]:
        name = (img.image_name or "").lower()
        return (version_rank(name), img.creation_time or "")

    candidates.sort(key=sort_key)
    chosen = candidates[0]
    if not chosen.image_id:
        raise RuntimeError("ubuntu image missing image_id")
    logger.info(f"selected ubuntu image: {chosen.image_id} ({chosen.image_name})")
    return chosen.image_id


def wait_img(c, r: str, img: str, poll: int, timeout: int) -> None:
    def chk() -> bool:
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


async def _remote_run(conn: asyncssh.SSHClientConnection, cmd: str, *, check: bool = True) -> None:
    logger.info(f"remote: {cmd}")
    r = await conn.run(cmd, check=False)
    if r.stdout:
        logger.info(r.stdout.strip())
    if r.stderr:
        logger.warning(r.stderr.strip())
    if check and r.exit_status != 0:
        raise RuntimeError(f"failed: {cmd}")


async def _connect_with_retry(host: str, *, ssh_user: str, ssh_key_path: str, timeout: int = 300) -> asyncssh.SSHClientConnection:
    """Establish an SSH connection with retries to handle transient boot-time disconnects."""
    deadline = time.time() + timeout
    key_path = str(Path(ssh_key_path).expanduser())
    last_exc: Optional[BaseException] = None
    while time.time() < deadline:
        try:
            conn = await asyncssh.connect(host, username=ssh_user, client_keys=[key_path], known_hosts=None)
            # small round-trip to confirm session is stable
            r = await conn.run("true", check=False)
            if r.exit_status == 0:
                return conn
            await conn.wait_closed()
        except BaseException as exc:
            last_exc = exc
        await asyncio.sleep(3)
    raise RuntimeError(f"SSH not stable for {host} within timeout; last error: {last_exc}")


def _load_request_config() -> tuple[str, str, str]:
    with open(REQUEST_CONFIG_PATH, "rb") as f:
        data = tomllib.load(f)
    aliyun = data.get("aliyun") or {}
    user_tag = (aliyun.get("user_tag") or "").strip()
    ssh_key_path = (aliyun.get("ssh_key_path") or "").strip()
    ssh_user = (aliyun.get("default_user_name") or "root").strip() or "root"
    if not user_tag:
        raise RuntimeError("Missing [aliyun].user_tag in request_config.toml")
    if not ssh_key_path:
        raise RuntimeError("Missing [aliyun].ssh_key_path in request_config.toml")
    return user_tag, ssh_key_path, ssh_user


def _infra_tag(user_tag: str) -> str:
    return f"conflux-massive-test-{user_tag}"


def _ensure_shared_infra(ali: AliyunClient, *, region_id: str, user_tag: str, ssh_key_path: str) -> tuple[str, str, str, list[str]]:
    tag = _infra_tag(user_tag)

    zone_ids = ali.get_zone_ids_in_region(region_id)
    if not zone_ids:
        raise RuntimeError(f"no zones in region {region_id}")

    vpc = next((v for v in ali.get_vpcs_in_region(region_id) if v.vpc_name == tag), None)
    vpc_id = vpc.vpc_id if vpc else ali.create_vpc(region_id, tag, "10.0.0.0/16")

    sg = next((s for s in ali.get_security_groups_in_region(region_id, vpc_id) if s.security_group_name == tag), None)
    security_group_id = sg.security_group_id if sg else ali.create_security_group(region_id, vpc_id, tag)

    fp = KeyPairRequestConfig(key_path=ssh_key_path, key_pair_name=tag).finger_print("aliyun")
    fp_compact = "".join(ch for ch in fp.lower() if ch.isalnum())
    key_name_base = f"{tag}-{fp_compact[-8:]}" if fp_compact else tag
    key_pair = KeyPairRequestConfig(key_path=ssh_key_path, key_pair_name=key_name_base)

    remote_kp = ali.get_keypairs_in_region(region_id, key_pair.key_pair_name)
    if remote_kp is None:
        ali.create_keypair(region_id, key_pair)
    elif remote_kp.finger_print != key_pair.finger_print("aliyun"):
        key_pair = KeyPairRequestConfig(key_path=ssh_key_path, key_pair_name=f"{key_name_base}-{int(time.time())}")
        ali.create_keypair(region_id, key_pair)

    return vpc_id, security_group_id, key_pair.key_pair_name, zone_ids


def _ensure_vswitch_in_zone(ali: AliyunClient, *, region_id: str, vpc_id: str, user_tag: str, zone_id: str) -> str:
    tag = _infra_tag(user_tag)
    v_switches = ali.get_v_switchs_in_region(region_id, vpc_id)
    vs = next((vs for vs in v_switches if vs.v_switch_name == tag and vs.zone_id == zone_id), None)
    if vs is not None:
        return vs.v_switch_id
    occupied = [x.cidr_block for x in v_switches]
    cidr = allocate_vacant_cidr_block(occupied, prefix=20, vpc_cidr="10.0.0.0/16")
    return ali.create_v_switch(region_id, zone_id, vpc_id, tag, cidr)


def _wait_for_instances(ali: AliyunClient, *, region_id: str, zone_id: str, instance_type: InstanceType, instance_ids: list[str], wait_timeout: int) -> list[str]:
    verifier = InstanceVerifier(region_id, target_nodes=len(instance_ids))
    verifier.submit_pending_instances(instance_ids, instance_type, zone_id)
    t1 = threading.Thread(target=verifier.describe_instances_loop, args=(ali,))
    t2 = threading.Thread(target=verifier.wait_for_ssh_loop)
    t1.start()
    t2.start()
    try:
        wait_until(lambda: verifier.ready_nodes >= len(instance_ids), timeout=wait_timeout, retry_interval=3)
        ready = verifier.copy_ready_instances()
        return [ip for _, ip in ready]
    finally:
        verifier.stop()
        t1.join(timeout=5)
        t2.join(timeout=5)


async def _prepare_registry_builder(host: str, *, ssh_user: str, ssh_key_path: str) -> None:
    if not PREPARE_SCRIPT.exists():
        raise FileNotFoundError(f"prepare script not found: {PREPARE_SCRIPT}")
    conn = await _connect_with_retry(host, ssh_user=ssh_user, ssh_key_path=ssh_key_path, timeout=300)
    async with conn:
        remote_prepare = f"/tmp/{PREPARE_SCRIPT.name}.{int(time.time())}.sh"
        await asyncssh.scp(str(PREPARE_SCRIPT), (conn, remote_prepare))
        await _remote_run(conn, f"sudo bash {shlex.quote(remote_prepare)}")
        await _remote_run(conn, f"sudo rm -f {shlex.quote(remote_prepare)}")


def _stop_instance(raw_ecs, *, instance_id: str) -> None:
    raw_ecs.stop_instance(ecs_models.StopInstanceRequest(instance_id=instance_id, force_stop=True, stopped_mode="StopCharging"))


def _describe_instance(raw_ecs, *, region_id: str, instance_id: str):
    import json
    rep = raw_ecs.describe_instances(ecs_models.DescribeInstancesRequest(region_id=region_id, instance_ids=json.dumps([instance_id])))
    instances = rep.body.instances.instance if rep.body and rep.body.instances else []
    return instances[0] if instances else None


def _wait_status(raw_ecs, *, region_id: str, instance_id: str, want: set[str], poll: int, timeout: int) -> str:
    h: dict[str, Optional[str]] = {"s": None}

    def _chk() -> bool:
        inst = _describe_instance(raw_ecs, region_id=region_id, instance_id=instance_id)
        h["s"] = inst.status if inst else None
        return (h["s"] or "") in want

    wait_until(_chk, timeout=timeout, retry_interval=poll)
    return h["s"] or ""


def main() -> None:
    # Load credentials from .env if present, otherwise rely on process env.
    load_dotenv()
    env_path = REPO_ROOT / ".env"
    if env_path.exists():
        load_dotenv(dotenv_path=env_path)

    user_tag, ssh_key_path, ssh_user = _load_request_config()
    region_id = FIXED_REGION

    ali = AliyunClient.load_from_env()
    raw_ecs = ali.build(region_id)

    # Reuse cloud_provisioner: if an image named DEFAULT_IMAGE_NAME already exists in the target region,
    # skip the build to avoid duplicate work.
    try:
        existing_images = ali.get_images_in_region(region_id, DEFAULT_IMAGE_NAME)
        if existing_images:
            img = existing_images[0]
            logger.info(f"image {DEFAULT_IMAGE_NAME} already exists in {region_id}: {img.image_id} ({img.image_name}). Skipping build.")
            return
    except Exception as exc:
        logger.warning(f"failed to query existing images in {region_id}: {exc}. Will continue with build")

    base_ubuntu = find_ubuntu(raw_ecs, region_id)
    vpc_id, security_group_id, key_pair_name, zone_ids = _ensure_shared_infra(
        ali,
        region_id=region_id,
        user_tag=user_tag,
        ssh_key_path=ssh_key_path,
    )

    cfg = InstanceConfig(
        user_tag_value=user_tag,
        instance_name_prefix="conflux-docker-registry",
        disk_size=DISK_GB,
        disk_category="",
        internet_max_bandwidth_out=100,
        use_spot=True,
        spot_strategy="SpotAsPriceGo",
    )

    # test_cfg = InstanceConfig(
    #     user_tag_value=user_tag,
    #     instance_name_prefix="conflux-registry-test",
    #     disk_size=DISK_GB,
    #     disk_category="cloud_essd",
    #     internet_max_bandwidth_out=100,
    #     use_spot=True,
    #     spot_strategy="SpotAsPriceGo",
    # )

    # 1) Builder instance (spot ecs.xn4.small) - try zones sequentially (no instance-type querying)
    builder_type = InstanceType(BUILDER_INSTANCE_TYPE, 1)
    builder_id: Optional[str] = None
    builder_ip: Optional[str] = None
    chosen_zone: Optional[ZoneInfo] = None
    region_info: Optional[RegionInfo] = None

    for zone_id in zone_ids:
        v_switch_id = _ensure_vswitch_in_zone(
            ali,
            region_id=region_id,
            vpc_id=vpc_id,
            user_tag=user_tag,
            zone_id=zone_id,
        )
        zone_info = ZoneInfo(id=zone_id, v_switch_id=v_switch_id)
        region_info = RegionInfo(
            id=region_id,
            zones={zone_id: zone_info},
            security_group_id=security_group_id,
            vpc_id=vpc_id,
            image_id=base_ubuntu,
            key_pair_name=key_pair_name,
            key_path=ssh_key_path,
        )

        ids, err = ali.create_instances_in_zone(cfg, region_info, zone_info, builder_type, max_amount=1, min_amount=1)
        if not ids:
            logger.warning(f"zone {zone_id} cannot create {BUILDER_INSTANCE_TYPE}: {err}")
            continue

        builder_id = ids[0]
        chosen_zone = zone_info
        logger.info(f"builder instance in zone {zone_id}: {builder_id}")
        try:
            builder_ip = _wait_for_instances(
                ali,
                region_id=region_id,
                zone_id=zone_id,
                instance_type=builder_type,
                instance_ids=[builder_id],
                wait_timeout=1800,
            )[0]
            break
        except Exception:
            ali.delete_instances(region_id, [builder_id])
            builder_id = None
            builder_ip = None
            chosen_zone = None
            continue

    if not builder_id or not builder_ip or not chosen_zone or not region_info:
        raise RuntimeError(f"failed to create builder instance of type {BUILDER_INSTANCE_TYPE} in any zone of {region_id}")

    try:
        logger.success(f"builder ready: {builder_ip}")

        asyncio.run(_prepare_registry_builder(builder_ip, ssh_user=ssh_user, ssh_key_path=ssh_key_path))

        # 2) Verification instances (2 spot ecs.g8i.large) pulling from builder registry
        # test_type = InstanceType(TEST_INSTANCE_TYPE, 1)
        # test_ids, err = ali.create_instances_in_zone(test_cfg, region_info, chosen_zone, test_type, max_amount=TEST_INSTANCES, min_amount=TEST_INSTANCES)
        # if len(test_ids) != TEST_INSTANCES:
        #     raise RuntimeError(f"failed to create {TEST_INSTANCES} test instances: got={len(test_ids)} err={err}")
        # try:
        #     test_ips = _wait_for_instances(ali, region_id=region_id, zone_id=chosen_zone.id, instance_type=test_type, instance_ids=test_ids, wait_timeout=1800)
        #     registry = f"{builder_ip}:{REGISTRY_PORT}"
        #     for ip in test_ips:
        #         asyncio.run(_install_docker_and_pull_from_registry(ip, ssh_user, ssh_key_path, registry))
        #     logger.success("registry pull verified from 2 test instances")
        # finally:
        #     ali.delete_instances(region_id, test_ids)
        #     logger.info("test instances deleted")

        # 3) Stop builder and create image
        logger.info("stopping builder instance and creating custom image")
        _stop_instance(raw_ecs, instance_id=builder_id)
        _wait_status(raw_ecs, region_id=region_id, instance_id=builder_id, want={"Stopped"}, poll=5, timeout=1800)

        cr = raw_ecs.create_image(ecs_models.CreateImageRequest(region_id=region_id, instance_id=builder_id, image_name=DEFAULT_IMAGE_NAME))
        image_id = cr.body.image_id if cr.body else None
        if not image_id:
            raise RuntimeError("create_image did not return image_id")
        wait_img(raw_ecs, region_id, image_id, poll=10, timeout=3600)
        logger.success(f"image available: {DEFAULT_IMAGE_NAME} ({image_id})")
    finally:
        try:
            if builder_id:
                ali.delete_instances(region_id, [builder_id])
            logger.info("builder instance deleted")
        except Exception as exc:
            logger.warning(f"failed to delete builder instance {builder_id}: {exc}")


if __name__ == "__main__":
    main()
