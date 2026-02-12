"""Server image building utilities for Tencent CVM."""
import asyncio
import shlex
import time
from pathlib import Path
from typing import Any, Callable, Coroutine, Optional, Sequence, Tuple

import asyncssh
from loguru import logger
from dotenv import load_dotenv
from tencentcloud.cvm.v20170312 import models as cvm_models

from utils.wait_until import wait_until
from cloud_provisioner.create_instances.instance_verifier import _check_port
from cloud_provisioner.tencent_provider.client_factory import TencentClient
from cloud_provisioner.tencent_provider.instance import delete_instances, describe_instance_status


DEFAULT_IMAGE_NAME = "conflux-docker-registry"
FIXED_REGION = "ap-singapore"
BUILDER_INSTANCE_TYPE = "S5.MEDIUM2"
TEST_INSTANCE_TYPE = "S5.LARGE2"
TEST_INSTANCES = 2
DISK_GB = 20
REGISTRY_PORT = 5000
LOCAL_CONFLUX_TAG = "conflux-node:base"

REPO_ROOT = Path(__file__).resolve().parents[2]
REQUEST_CONFIG_PATH = REPO_ROOT / "request_config.toml"
# Use provider-specific prepare script for Tencent CVM
PREPARE_SCRIPT = REPO_ROOT / "auxiliary" / "scripts" / "remote" / "prepare_docker_server_image_tencent.sh"


class CvmRuntimeConfig:
    def __init__(self, region_id: str):
        self.region_id = region_id
        self.zone_id: Optional[str] = None
        self.poll_interval = 5
        self.wait_timeout = 1800
        self.ssh_username = "ubuntu"
        self.ssh_private_key_path = str(REPO_ROOT / "keys" / "ssh-key.pem")


def _load_env() -> None:
    load_dotenv()


def find_img_info(c, name: str) -> Optional[Tuple[str, str]]:
    f1 = cvm_models.Filter()
    f1.Name = "image-name"
    f1.Values = [name]
    f2 = cvm_models.Filter()
    f2.Name = "image-type"
    f2.Values = ["PRIVATE_IMAGE"]
    req = cvm_models.DescribeImagesRequest()
    req.Filters = [f1, f2]
    req.Limit = 100
    resp = c.DescribeImages(req)
    images = resp.ImageSet or []
    for img in images:
        if img.ImageName == name and img.ImageId:
            return img.ImageId, img.ImageState or ""
    return None


def find_img(c, name: str) -> Optional[str]:
    info = find_img_info(c, name)
    return info[0] if info else None


def find_ubuntu_22_x86(c, instance_type: Optional[str] = None) -> str:
    f1 = cvm_models.Filter()
    f1.Name = "image-type"
    f1.Values = ["PUBLIC_IMAGE"]
    f2 = cvm_models.Filter()
    f2.Name = "platform"
    f2.Values = ["Ubuntu"]
    req = cvm_models.DescribeImagesRequest()
    req.Filters = [f1, f2]
    if instance_type:
        req.InstanceType = instance_type
    req.Limit = 100
    resp = c.DescribeImages(req)
    images = resp.ImageSet or []
    candidates = []
    for img in images:
        name = (img.OsName or img.ImageName or "").lower()
        if "ubuntu" not in name:
            continue
        if "22.04" not in name and "22" not in name:
            continue
        if (img.Architecture or "").lower() != "x86_64":
            continue
        if img.ImageId:
            candidates.append(img)
    if not candidates:
        raise RuntimeError("no ubuntu 22 x86_64 image found")
    candidates.sort(key=lambda x: x.CreatedTime or "")
    chosen = candidates[-1]
    logger.info(f"selected ubuntu image: {chosen.ImageId} ({chosen.ImageName})")
    return chosen.ImageId


def pick_zone(client: TencentClient, region: str) -> str:
    from cloud_provisioner.tencent_provider.zone import get_zone_ids_in_region
    zones = get_zone_ids_in_region(client.build_cvm(region))
    if not zones:
        raise RuntimeError(f"no zones found in region {region}")
    return zones[0]


def wait_img(c, image_id: str, poll: int, timeout: int) -> None:
    def _chk() -> bool:
        req = cvm_models.DescribeImagesRequest()
        req.ImageIds = [image_id]
        resp = c.DescribeImages(req)
        images = resp.ImageSet or []
        if not images:
            return False
        st = images[0].ImageState
        logger.info(f"image {image_id}: {st}")
        if st in {"CREATEFAILED", "IMPORTFAILED"}:
            raise RuntimeError(f"image failed: {st}")
        return st == "NORMAL"

    wait_until(_chk, timeout=timeout, retry_interval=poll)


def get_public_ip(client: TencentClient, region: str, instance_id: str) -> Optional[str]:
    req = cvm_models.DescribeInstancesRequest()
    req.InstanceIds = [instance_id]
    resp = client.build_cvm(region).DescribeInstances(req)
    instances = resp.InstanceSet or []
    if not instances:
        return None
    instance = instances[0]
    if instance.PublicIpAddresses:
        return instance.PublicIpAddresses[0]
    return None


def start_builder_instance(client: TencentClient, cfg: CvmRuntimeConfig, zone_id: str, base_image_id: str, instance_type: str) -> str:
    # Generate SSH key pair
    import tempfile
    import os
    from cryptography.hazmat.primitives import serialization as crypto_serialization
    from cryptography.hazmat.primitives.asymmetric import rsa
    from cryptography.hazmat.backends import default_backend as crypto_default_backend
    
    # Generate private key
    key = rsa.generate_private_key(
        backend=crypto_default_backend(),
        public_exponent=65537,
        key_size=2048
    )
    
    # Create temporary file for private key that will persist
    with tempfile.NamedTemporaryFile(mode='wb', suffix='.pem', delete=False) as f:
        f.write(key.private_bytes(
            crypto_serialization.Encoding.PEM,
            crypto_serialization.PrivateFormat.PKCS8,
            crypto_serialization.NoEncryption()
        ))
        private_key_path = f.name
    
    try:
        os.chmod(private_key_path, 0o600)
        
        # Create key pair name
        key_name = f"builder_key_{int(time.time())}"
        
        # Create KeyPairRequestConfig
        from cloud_provisioner.create_instances.types import KeyPairRequestConfig
        key_pair_config = KeyPairRequestConfig(
            key_path=private_key_path,
            key_pair_name=key_name
        )
        
        # Import key pair to Tencent Cloud
        req = cvm_models.ImportKeyPairRequest()
        req.KeyName = key_name
        req.ProjectId = 0
        req.PublicKey = key_pair_config.public_key
        resp = client.build_cvm(cfg.region_id).ImportKeyPair(req)
        key_id = resp.KeyId
        if not key_id:
            raise RuntimeError("failed to get key ID")
        logger.info(f"imported key pair: {key_id}")
        
        # Create instance request
        req = cvm_models.RunInstancesRequest()
        req.InstanceChargeType = "POSTPAID_BY_HOUR"
        req.InstanceType = instance_type
        req.ImageId = base_image_id
        req.InstanceCount = 1
        req.InstanceName = f"builder-{int(time.time())}"
        placement = cvm_models.Placement()
        placement.Zone = zone_id
        req.Placement = placement
        system_disk = cvm_models.SystemDisk()
        system_disk.DiskType = "CLOUD_SSD"
        system_disk.DiskSize = DISK_GB
        req.SystemDisk = system_disk
        
        internet_accessible = cvm_models.InternetAccessible()
        internet_accessible.InternetChargeType = "TRAFFIC_POSTPAID_BY_HOUR"
        internet_accessible.InternetMaxBandwidthOut = 10
        internet_accessible.PublicIpAssigned = True
        req.InternetAccessible = internet_accessible
        
        # Set login settings with SSH key
        login_settings = cvm_models.LoginSettings()
        login_settings.KeyIds = [key_id]
        req.LoginSettings = login_settings
        
        # Run instance
        resp = client.build_cvm(cfg.region_id).RunInstances(req)
        instance_ids = resp.InstanceIdSet or []
        if not instance_ids:
            raise RuntimeError("failed to create instance")
        
        # Update cfg with temporary key path for SSH connection
        cfg.ssh_private_key_path = private_key_path
        
        return instance_ids[0]
    except Exception as e:
        # Clean up temporary key file if there's an error
        if 'private_key_path' in locals() and os.path.exists(private_key_path):
            os.unlink(private_key_path)
        raise


async def prepare_docker_server_image(host: str, cfg: CvmRuntimeConfig) -> None:
    wait_until_ssh_ready(host, timeout=cfg.wait_timeout)
    key_path = str(Path(cfg.ssh_private_key_path).expanduser())
    async with asyncssh.connect(host, username=cfg.ssh_username, client_keys=[key_path], known_hosts=None) as conn:
        async def run(cmd: str, check: bool = True) -> None:
            logger.info(f"remote: {cmd}")
            r = await conn.run(cmd, check=False)
            if r.stdout:
                logger.info(r.stdout.strip())
            if r.stderr:
                logger.warning(r.stderr.strip())
            if check and r.exit_status != 0:
                raise RuntimeError(f"failed: {cmd}")

        if not PREPARE_SCRIPT.exists():
            raise FileNotFoundError(f"prepare script not found: {PREPARE_SCRIPT}")
        remote_prepare = f"/tmp/{PREPARE_SCRIPT.name}.{int(time.time())}.sh"
        await asyncssh.scp(str(PREPARE_SCRIPT), (conn, remote_prepare))
        await run(f"sudo bash {shlex.quote(remote_prepare)}")
        await run(f"sudo rm -f {shlex.quote(remote_prepare)}")


def wait_until_ssh_ready(host: str, timeout: int = 1800) -> None:
    wait_until(lambda: _check_port(host), timeout=timeout, retry_interval=3)


def create_server_image(
    cfg: CvmRuntimeConfig,
    *, 
    base_image_id: str,
    prepare_fn: Callable[[str, CvmRuntimeConfig], Coroutine[Any, Any, None]] = prepare_docker_server_image,
) -> str:
    name = DEFAULT_IMAGE_NAME
    _load_env()
    client = TencentClient.load_from_env()
    c = client.build_cvm(cfg.region_id)
    existing = find_img(c, name)
    if existing:
        logger.info(f"image exists: {existing}")
        wait_img(c, existing, cfg.poll_interval, cfg.wait_timeout)
        return existing

    zone = cfg.zone_id or pick_zone(client, cfg.region_id)
    instance_id = ""
    temp_key_path = None
    try:
        logger.info(f"using instance type: {BUILDER_INSTANCE_TYPE}")
        instance_id = start_builder_instance(client, cfg, zone, base_image_id, BUILDER_INSTANCE_TYPE)
        temp_key_path = cfg.ssh_private_key_path
        logger.info(f"builder: {instance_id}")
        ip = ""
        def _ip_ready() -> bool:
            nonlocal ip
            ip = get_public_ip(client, cfg.region_id, instance_id) or ""
            return bool(ip)
        wait_until(_ip_ready, timeout=cfg.wait_timeout, retry_interval=cfg.poll_interval)
        logger.info(f"builder ready: {ip}")
        asyncio.run(prepare_fn(ip, cfg))
        creq = cvm_models.CreateImageRequest()
        creq.InstanceId = instance_id
        creq.ImageName = name
        resp = c.CreateImage(creq)
        img_id = resp.ImageId
        if not img_id:
            raise RuntimeError("image_id missing from CreateImage response")
        logger.info(f"server image building started: {img_id}")
        wait_img(c, img_id, cfg.poll_interval, cfg.wait_timeout)
        return img_id
    finally:
        if instance_id:
            try:
                delete_instances(client.build_cvm(cfg.region_id), [instance_id])
                logger.info(f"builder deleted: {instance_id}")
            except Exception as exc:
                logger.warning(f"delete failed: {exc}")
        if temp_key_path:
            try:
                import os
                if os.path.exists(temp_key_path):
                    os.unlink(temp_key_path)
                    logger.info(f"temporary SSH key deleted: {temp_key_path}")
            except Exception as exc:
                logger.warning(f"failed to delete temporary SSH key: {exc}")


def build_base_image_in_region(
    *, 
    region: str,
    image_name: str,
) -> str:
    _load_env()
    cfg = CvmRuntimeConfig(region_id=region)
    client = TencentClient.load_from_env()
    base_image_id = find_ubuntu_22_x86(client.build_cvm(region), BUILDER_INSTANCE_TYPE)
    logger.info(f"building base image {image_name} in {region}")
    return create_server_image(cfg, base_image_id=base_image_id, prepare_fn=prepare_docker_server_image)


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Build Tencent CVM image with Docker and Registry")
    parser.add_argument("--region", type=str, default=FIXED_REGION)
    parser.add_argument("--image-name", type=str, default=DEFAULT_IMAGE_NAME)
    args = parser.parse_args()

    _load_env()

    img = build_base_image_in_region(region=args.region, image_name=args.image_name)
    logger.success(f"image built: {img}")


if __name__ == "__main__":
    main()
