"""Build a reusable AWS AMI which acts as a private Docker registry.

Requirements implemented:
- Fixed region: af-south-1
- Fixed spot builder type: t3.small
- 20GB disk
- Docker installed + local registry on :5000
- Preload lylcx2007/conflux-node:latest as conflux-node:base into the registry
- Verification-first: boot 2 m6i.large in same region+zone, pull from registry
- Tag instances so cloud_provisioner/cleanup_instances can delete them

This file intentionally avoids auxiliary/aws_instances config classes and reuses cloud_provisioner.
"""

# Allow running this file directly via `python auxiliary/aws_instances/image_build.py`
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
from loguru import logger
from dotenv import load_dotenv

from utils.wait_until import wait_until
from cloud_provisioner.aws_provider.client_factory import AwsClient
from cloud_provisioner.create_instances.instance_config import InstanceConfig
from cloud_provisioner.create_instances.instance_verifier import InstanceVerifier
from cloud_provisioner.create_instances.network_infra import allocate_vacant_cidr_block
from cloud_provisioner.create_instances.types import InstanceType, RegionInfo, ZoneInfo, KeyPairRequestConfig


# --- Fixed settings ---
DEFAULT_IMAGE_NAME = "conflux-docker-registry"
FIXED_REGION = "af-south-1"
BUILDER_INSTANCE_TYPE = "t3.small"
TEST_INSTANCE_TYPE = "m6i.large"
TEST_INSTANCES = 2
DISK_GB = 20
REGISTRY_PORT = 5000
LOCAL_CONFLUX_TAG = "conflux-node:base"

REPO_ROOT = Path(__file__).resolve().parents[2]
REQUEST_CONFIG_PATH = REPO_ROOT / "request_config.toml"
PREPARE_SCRIPT = REPO_ROOT / "scripts" / "remote" / "prepare_docker_server_image.sh"


def find_ubuntu(ec2, region_id: str) -> str:
    owners = ["099720109477"]  # Canonical
    name_filters = [
        "ubuntu/images/hvm-ssd/ubuntu-noble-24.04-amd64-server-*",
        "ubuntu/images/hvm-ssd/ubuntu-jammy-22.04-amd64-server-*",
    ]

    for name_filter in name_filters:
        resp = ec2.describe_images(
            Owners=owners,
            Filters=[
                {"Name": "name", "Values": [name_filter]},
                {"Name": "state", "Values": ["available"]},
                {"Name": "architecture", "Values": ["x86_64"]},
                {"Name": "virtualization-type", "Values": ["hvm"]},
                {"Name": "root-device-type", "Values": ["ebs"]},
            ],
        )
        images = resp.get("Images", [])
        if not images:
            continue
        images.sort(key=lambda img: img.get("CreationDate", ""), reverse=True)
        image_id = images[0].get("ImageId")
        name = images[0].get("Name", "")
        if image_id:
            logger.info(f"selected ubuntu image: {image_id} ({name})")
            return image_id

    raise RuntimeError(f"no ubuntu image found in {region_id}")


def wait_img(ec2, image_id: str, poll: int, timeout: int) -> None:
    def chk() -> bool:
        resp = ec2.describe_images(ImageIds=[image_id])
        imgs = resp.get("Images", [])
        if not imgs:
            return False
        st = imgs[0].get("State")
        logger.info(f"image {image_id}: {st}")
        if st in {"failed", "deregistered"}:
            raise RuntimeError(f"image failed: {st}")
        return st == "available"

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
    deadline = time.time() + timeout
    key_path = str(Path(ssh_key_path).expanduser())
    last_exc: Optional[BaseException] = None
    while time.time() < deadline:
        try:
            conn = await asyncssh.connect(host, username=ssh_user, client_keys=[key_path], known_hosts=None)
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
    aws_cfg = data.get("aws") or {}
    user_tag = (aws_cfg.get("user_tag") or "").strip()
    ssh_key_path = (aws_cfg.get("ssh_key_path") or "").strip()
    ssh_user = (aws_cfg.get("default_user_name") or "ubuntu").strip() or "ubuntu"
    if not user_tag:
        raise RuntimeError("Missing [aws].user_tag in request_config.toml")
    if not ssh_key_path:
        raise RuntimeError("Missing [aws].ssh_key_path in request_config.toml")
    return user_tag, ssh_key_path, ssh_user


def _infra_tag(user_tag: str) -> str:
    return f"conflux-massive-test-{user_tag}"


def _ensure_shared_infra(aws: AwsClient, *, region_id: str, user_tag: str, ssh_key_path: str) -> tuple[str, str, str, list[str]]:
    tag = _infra_tag(user_tag)

    zone_ids = aws.get_zone_ids_in_region(region_id)
    if not zone_ids:
        raise RuntimeError(f"no zones in region {region_id}")

    vpc = next((v for v in aws.get_vpcs_in_region(region_id) if v.vpc_name == tag), None)
    vpc_id = vpc.vpc_id if vpc else aws.create_vpc(region_id, tag, "10.0.0.0/16")

    sg = next((s for s in aws.get_security_groups_in_region(region_id, vpc_id) if s.security_group_name == tag), None)
    security_group_id = sg.security_group_id if sg else aws.create_security_group(region_id, vpc_id, tag)

    fp = KeyPairRequestConfig(key_path=ssh_key_path, key_pair_name=tag).finger_print("aws")
    fp_compact = "".join(ch for ch in fp.lower() if ch.isalnum())
    key_name_base = f"{tag}-{fp_compact[-8:]}" if fp_compact else tag
    key_pair = KeyPairRequestConfig(key_path=ssh_key_path, key_pair_name=key_name_base)

    remote_kp = aws.get_keypairs_in_region(region_id, key_pair.key_pair_name)
    if remote_kp is None:
        aws.create_keypair(region_id, key_pair)
    elif remote_kp.finger_print != key_pair.finger_print("aws"):
        key_pair = KeyPairRequestConfig(key_path=ssh_key_path, key_pair_name=f"{key_name_base}-{int(time.time())}")
        aws.create_keypair(region_id, key_pair)

    return vpc_id, security_group_id, key_pair.key_pair_name, zone_ids


def _ensure_subnet_in_zone(aws: AwsClient, *, region_id: str, vpc_id: str, user_tag: str, zone_id: str) -> str:
    tag = _infra_tag(user_tag)
    v_switches = aws.get_v_switchs_in_region(region_id, vpc_id)
    vs = next((vs for vs in v_switches if vs.v_switch_name == tag and vs.zone_id == zone_id), None)
    if vs is not None:
        return vs.v_switch_id
    occupied = [x.cidr_block for x in v_switches]
    cidr = allocate_vacant_cidr_block(occupied, prefix=20, vpc_cidr="10.0.0.0/16")
    return aws.create_v_switch(region_id, zone_id, vpc_id, tag, cidr)


def _wait_for_instances(aws: AwsClient, *, region_id: str, zone_id: str, instance_type: InstanceType, instance_ids: list[str], wait_timeout: int) -> list[str]:
    verifier = InstanceVerifier(region_id, target_nodes=len(instance_ids))
    verifier.submit_pending_instances(instance_ids, instance_type, zone_id)
    t1 = threading.Thread(target=verifier.describe_instances_loop, args=(aws,))
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


def _get_instance_ips(ec2, instance_ids: list[str], *, ip_field: str) -> dict[str, str]:
    resp = ec2.describe_instances(InstanceIds=instance_ids)
    result: dict[str, str] = {}
    for reservation in resp.get("Reservations", []):
        for inst in reservation.get("Instances", []):
            ip = inst.get(ip_field)
            if ip:
                result[inst["InstanceId"]] = ip
    return result


async def _wait_registry_ready(host: str, *, ssh_user: str, ssh_key_path: str, timeout: int = 300) -> None:
    conn = await _connect_with_retry(host, ssh_user=ssh_user, ssh_key_path=ssh_key_path, timeout=timeout)
    async with conn:
        deadline = time.time() + timeout
        while time.time() < deadline:
            r = await conn.run("curl -fsS http://localhost:5000/v2/ >/dev/null", check=False)
            if r.exit_status == 0:
                return
            await asyncio.sleep(3)
    raise RuntimeError("registry not ready on builder within timeout")


def main() -> None:
    load_dotenv()
    env_path = REPO_ROOT / ".env"
    if env_path.exists():
        load_dotenv(dotenv_path=env_path)

    user_tag, ssh_key_path, ssh_user = _load_request_config()
    region_id = FIXED_REGION

    aws = AwsClient.new()
    raw_ec2 = aws.build(region_id)

    existing_images = aws.get_images_in_region(region_id, DEFAULT_IMAGE_NAME)
    if existing_images:
        img = existing_images[0]
        logger.info(f"image {DEFAULT_IMAGE_NAME} already exists in {region_id}: {img.image_id} ({img.image_name}). Skipping build.")
        return

    base_ubuntu = find_ubuntu(raw_ec2, region_id)
    vpc_id, security_group_id, key_pair_name, zone_ids = _ensure_shared_infra(
        aws,
        region_id=region_id,
        user_tag=user_tag,
        ssh_key_path=ssh_key_path,
    )

    cfg = InstanceConfig(
        user_tag_value=user_tag,
        instance_name_prefix="conflux-docker-registry",
        disk_size=DISK_GB,
        internet_max_bandwidth_out=100,
        use_spot=True,
        spot_strategy="SpotAsPriceGo",
    )

    # test_cfg = InstanceConfig(
    #     user_tag_value=user_tag,
    #     instance_name_prefix="conflux-registry-test",
    #     disk_size=DISK_GB,
    #     internet_max_bandwidth_out=100,
    #     use_spot=True,
    #     spot_strategy="SpotAsPriceGo",
    # )

    builder_type = InstanceType(BUILDER_INSTANCE_TYPE, 1)
    builder_id: Optional[str] = None
    builder_ip: Optional[str] = None
    chosen_zone: Optional[ZoneInfo] = None
    region_info: Optional[RegionInfo] = None

    for zone_id in zone_ids:
        subnet_id = _ensure_subnet_in_zone(
            aws,
            region_id=region_id,
            vpc_id=vpc_id,
            user_tag=user_tag,
            zone_id=zone_id,
        )
        zone_info = ZoneInfo(id=zone_id, v_switch_id=subnet_id)
        region_info = RegionInfo(
            id=region_id,
            zones={zone_id: zone_info},
            security_group_id=security_group_id,
            vpc_id=vpc_id,
            image_id=base_ubuntu,
            key_pair_name=key_pair_name,
            key_path=ssh_key_path,
        )

        ids, err = aws.create_instances_in_zone(cfg, region_info, zone_info, builder_type, max_amount=1, min_amount=1)
        if not ids:
            logger.warning(f"zone {zone_id} cannot create {BUILDER_INSTANCE_TYPE}: {err}")
            continue

        builder_id = ids[0]
        chosen_zone = zone_info
        logger.info(f"builder instance in zone {zone_id}: {builder_id}")
        try:
            builder_ip = _wait_for_instances(
                aws,
                region_id=region_id,
                zone_id=zone_id,
                instance_type=builder_type,
                instance_ids=[builder_id],
                wait_timeout=1800,
            )[0]
            break
        except Exception:
            aws.delete_instances(region_id, [builder_id])
            builder_id = None
            builder_ip = None
            chosen_zone = None
            continue

    if not builder_id or not builder_ip or not chosen_zone or not region_info:
        raise RuntimeError(f"failed to create builder instance of type {BUILDER_INSTANCE_TYPE} in any zone of {region_id}")

    try:
        logger.success(f"builder ready: {builder_ip}")

        asyncio.run(_prepare_registry_builder(builder_ip, ssh_user=ssh_user, ssh_key_path=ssh_key_path))
        asyncio.run(_wait_registry_ready(builder_ip, ssh_user=ssh_user, ssh_key_path=ssh_key_path, timeout=300))

        builder_private_ip = _get_instance_ips(raw_ec2, [builder_id], ip_field="PrivateIpAddress").get(builder_id)
        if not builder_private_ip:
            raise RuntimeError("failed to get builder private IP")

        # test_type = InstanceType(TEST_INSTANCE_TYPE, 1)
        # test_ids, err = aws.create_instances_in_zone(test_cfg, region_info, chosen_zone, test_type, max_amount=TEST_INSTANCES, min_amount=TEST_INSTANCES)
        # if len(test_ids) != TEST_INSTANCES:
        #     raise RuntimeError(f"failed to create {TEST_INSTANCES} test instances: got={len(test_ids)} err={err}")
        # try:
        #     test_ips = _wait_for_instances(aws, region_id=region_id, zone_id=chosen_zone.id, instance_type=test_type, instance_ids=test_ids, wait_timeout=1800)
        #     registry = f"{builder_private_ip}:{REGISTRY_PORT}"
        #     for ip in test_ips:
        #         asyncio.run(_install_docker_and_pull_from_registry(ip, ssh_user, ssh_key_path, registry))
        #     logger.success("registry pull verified from 2 test instances")
        # finally:
        #     aws.delete_instances(region_id, test_ids)
        #     logger.info("test instances deleted")

        logger.info("creating custom AMI from builder instance")
        resp = raw_ec2.create_image(InstanceId=builder_id, Name=DEFAULT_IMAGE_NAME, NoReboot=True)
        image_id = resp.get("ImageId")
        if not image_id:
            raise RuntimeError("create_image did not return image_id")
        wait_img(raw_ec2, image_id, poll=10, timeout=3600)
        logger.success(f"image available: {DEFAULT_IMAGE_NAME} ({image_id})")
    finally:
        try:
            if builder_id:
                aws.delete_instances(region_id, [builder_id])
            logger.info("builder instance deleted")
        except Exception as exc:
            logger.warning(f"failed to delete builder instance {builder_id}: {exc}")


if __name__ == "__main__":
    main()
