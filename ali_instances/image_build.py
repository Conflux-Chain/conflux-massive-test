"""Server image building utilities for Aliyun ECS."""
import asyncio
from pathlib import Path
from typing import Any, Callable, Coroutine, Optional

import asyncssh
from alibabacloud_ecs20140526 import models as ecs_models
from alibabacloud_ecs20140526.client import Client as EcsClient
from loguru import logger

from utils.wait_until import wait_until
from .config import EcsConfig, client
from .instance_prep import (
    _wait_tcp,
    allocate_public_ip,
    create_instance,
    delete_instance,
    ensure_keypair,
    ensure_net,
    pick_instance_type,
    start_instance,
    stop_instance,
    wait_ssh,
    wait_running,
    wait_status,
)


# --- Image ---
DEFAULT_IMAGE_NAME = "conflux-docker-base"

def _img_name(prefix: str, ref: str) -> str:
    return DEFAULT_IMAGE_NAME


def find_img(c: EcsClient, r: str, name: str) -> Optional[str]:
    resp = c.describe_images(ecs_models.DescribeImagesRequest(region_id=r, image_name=name, image_owner_alias="self"))
    for i in resp.body.images.image or []:
        if i.image_name == name:
            return i.image_id
    return None


def find_ubuntu(c: EcsClient, r: str, max_pages: int = 5, page_size: int = 50) -> str:
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


def wait_img(c: EcsClient, r: str, img: str, poll: int, timeout: int) -> None:
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
        await run(
            "if [ ! -d /opt/conflux/src/conflux-rust ]; then sudo git clone --depth 1 "
            "https://github.com/Conflux-Chain/conflux-rust.git /opt/conflux/src/conflux-rust; fi"
        )
        await run(
            "sudo bash -lc 'cd /opt/conflux/src/conflux-rust; git fetch --depth 1 origin "
            f"{cfg.conflux_git_ref} || true; git checkout {cfg.conflux_git_ref} || git checkout FETCH_HEAD; "
            "git submodule update --init --recursive; curl https://sh.rustup.rs -sSf | sh -s -- -y; "
            "source $HOME/.cargo/env; cargo build --release --bin conflux; install -m 0755 target/release/conflux /usr/local/bin/conflux'"
        )


async def prepare_docker_server_image(host: str, cfg: EcsConfig) -> None:
    await wait_ssh(host, cfg.ssh_username, cfg.ssh_private_key_path, cfg.wait_timeout)
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

        await run("sudo apt-get update -y")
        await run("sudo apt-get install -y docker.io ca-certificates curl")
        await run("sudo systemctl enable --now docker")
        await run('echo "LABEL=cloudimg-rootfs / ext4 defaults,noatime,nodiratime,barrier=0 0 0" > fstab')
        await run("sudo cp fstab /etc/fstab")


def create_server_image(
    cfg: EcsConfig,
    dry_run: bool = False,
    prepare_fn: Callable[[str, EcsConfig], Coroutine[Any, Any, None]] = default_prepare,
) -> str:
    name = _img_name(cfg.image_prefix, cfg.conflux_git_ref)
    c = client(cfg.credentials, cfg.region_id, cfg.endpoint)
    if not cfg.base_image_id:
        raise RuntimeError("base_image_id is required")
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
        logger.info("stopping builder instance")
        stop_instance(c, iid, "StopCharging")
        wait_status(c, cfg.region_id, iid, ["Stopped"], cfg.poll_interval, cfg.wait_timeout)
        cr = c.create_image(ecs_models.CreateImageRequest(region_id=cfg.region_id, instance_id=iid, image_name=name))
        if not cr.body or not cr.body.image_id:
            stop_instance(c, iid, None)
            wait_status(c, cfg.region_id, iid, ["Stopped"], cfg.poll_interval, cfg.wait_timeout)
            cr = c.create_image(ecs_models.CreateImageRequest(region_id=cfg.region_id, instance_id=iid, image_name=name))
        img = cr.body.image_id
        if not img:
            raise RuntimeError("image_id missing from create_image response")
        logger.info(f"server image building started: {img}")
        wait_img(c, cfg.region_id, img, cfg.poll_interval, cfg.wait_timeout)
        return img
    finally:
        if cfg.cleanup_builder_instance and iid:
            try:
                delete_instance(c, cfg.region_id, iid)
                logger.info(f"builder deleted: {iid}")
            except Exception as e:
                logger.warning(f"delete failed: {e}")


