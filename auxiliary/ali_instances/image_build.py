"""Server image building utilities for Aliyun ECS."""
import asyncio
import shlex
import time
from pathlib import Path
from typing import Any, Callable, Coroutine, Optional, Sequence, Tuple

import asyncssh
from alibabacloud_ecs20140526 import models as ecs_models
from alibabacloud_ecs20140526.client import Client as EcsClient
from loguru import logger

from utils.wait_until import wait_until
from .config import AliCredentials, EcsRuntimeConfig, InstanceTypeConfig, client
from .instance_prep import (
    allocate_public_ip,
    create_instance,
    delete_instance,
    ensure_keypair,
    ensure_net,
    start_instance,
    stop_instance,
    wait_ssh,
    wait_running,
    wait_status,
)


# --- Image ---
DEFAULT_IMAGE_NAME = "conflux-docker-base"

# Instance type selection defaults for image-building flow
DEFAULT_MIN_CPU_CORES = 4
DEFAULT_MIN_MEMORY_GB = 8.0
DEFAULT_MAX_MEMORY_GB = 8.0


def pick_instance_type_for_building_image(c: EcsClient, cfg: EcsRuntimeConfig) -> Optional[tuple[str, str]]:
    spot = cfg.spot_strategy if cfg.use_spot else None
    req = ecs_models.DescribeAvailableResourceRequest(
        region_id=cfg.region_id,
        destination_resource="InstanceType",
        resource_type="instance",
        instance_charge_type="PostPaid",
        spot_strategy=spot,
        cores=DEFAULT_MIN_CPU_CORES,
        memory=DEFAULT_MIN_MEMORY_GB,
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
            if t.cpu_core_count == DEFAULT_MIN_CPU_CORES
            and t.memory_size
            and DEFAULT_MIN_MEMORY_GB <= t.memory_size <= DEFAULT_MAX_MEMORY_GB
        ]
        if cands:
            cands.sort(key=lambda t: (t.memory_size, t.instance_type_id))
            s = cands[0]
            return z.zone_id, s.instance_type_id
    return None


def _build_conflux_script_source() -> Path:
    return Path(__file__).resolve().parent.parent / "auxiliary" / "scripts" / "remote" / "build_conflux_binary.sh"

def _img_name() -> str:
    return DEFAULT_IMAGE_NAME


def find_img_info(c: EcsClient, r: str, name: str) -> Optional[tuple[str, str]]:
    resp = c.describe_images(ecs_models.DescribeImagesRequest(region_id=r, image_name=name, image_owner_alias="self"))
    for i in resp.body.images.image or []:
        if i.image_name == name and i.image_id:
            return i.image_id, i.status or ""
    return None


def find_img(c: EcsClient, r: str, name: str) -> Optional[str]:
    info = find_img_info(c, r, name)
    return info[0] if info else None


def find_img_in_regions(
    creds: AliCredentials,
    regions: Sequence[str],
    name: str,
) -> Optional[Tuple[str, str]]:
    for region in regions:
        c = client(creds, region)
        img = find_img(c, region, name)
        if img:
            return region, img
    return None


async def ensure_image_in_region(
    *,
    creds: AliCredentials,
    region: str,
    image_name: str,
    search_regions: Sequence[str],
    poll_interval: int,
    wait_timeout: int,
) -> str:
    c = client(creds, region)
    existing = find_img_info(c, region, image_name)
    if existing:
        image_id, status = existing
        if status != "Available":
            await wait_images_available({(region, image_id): c}, poll_interval, wait_timeout)
        return image_id

    lookup_regions = [r for r in search_regions if r]
    if region not in lookup_regions:
        lookup_regions.append(region)

    found = find_img_in_regions(creds, lookup_regions, image_name)
    if not found:
        raise RuntimeError(f"image {image_name} not found in any region")
    src_region, src_image_id = found
    if src_region == region:
        return src_image_id

    logger.info(f"copying image {image_name} from {src_region} to {region}")
    image_id = _copy_image(
        creds=creds,
        src_region=src_region,
        dest_region=region,
        image_id=src_image_id,
        image_name=image_name,
        poll_interval=poll_interval,
        wait_timeout=wait_timeout,
    )
    wait_img(c, region, image_id, poll_interval, wait_timeout)
    return image_id


def build_base_image_in_region(
    *,
    creds: AliCredentials,
    region: str,
    image_name: str,
    poll_interval: int,
    wait_timeout: int,
) -> str:
    cfg = EcsRuntimeConfig(credentials=creds, region_id=region)
    cfg.poll_interval = poll_interval
    cfg.wait_timeout = wait_timeout
    base_image_id = find_ubuntu(client(creds, region), region)
    logger.info(f"building base image {image_name} in {region}")
    return create_server_image(cfg, base_image_id=base_image_id, prepare_fn=prepare_docker_server_image)


def _copy_image(
    *,
    creds: AliCredentials,
    src_region: str,
    dest_region: str,
    image_id: str,
    image_name: str,
    poll_interval: int,
    wait_timeout: int,
) -> str:
    src_client = client(creds, src_region)
    try:
        resp = src_client.copy_image(
            ecs_models.CopyImageRequest(
                region_id=src_region,
                destination_region_id=dest_region,
                image_id=image_id,
                destination_image_name=image_name,
            )
        )
    except Exception as exc:
        if "InvalidImageName.Duplicated" in str(exc):
            # Image with the same name already exists in the destination region
            # but is not available because it is still being copied.
            # query the image ID by name.
            dest_client = client(creds, dest_region)
            existing = find_img_info(dest_client, dest_region, image_name)
            if existing:
                logger.info(f"found existing image {existing[0]} of name {image_name} in {dest_region} of state {existing[1]}. Probably being copied. Will wait for it to be available.")
                return existing[0]
        raise
    copied_id = resp.body.image_id if resp.body else None
    if not copied_id:
        raise RuntimeError(f"failed to copy image {image_name} to {dest_region}")
    return copied_id


async def wait_images_available(
    pending: dict[tuple[str, str], EcsClient],
    poll_interval: int,
    wait_timeout: int,
) -> None:
    start = asyncio.get_event_loop().time()
    while pending:
        if asyncio.get_event_loop().time() - start > wait_timeout:
            raise RuntimeError("timeout waiting for image copies")
        done: list[tuple[str, str]] = []
        for (region, image_id), c in pending.items():
            resp = c.describe_images(ecs_models.DescribeImagesRequest(region_id=region, image_id=image_id))
            imgs = resp.body.images.image if resp.body and resp.body.images else []
            if not imgs:
                continue
            st = imgs[0].status
            logger.info(f"image {image_id} in {region}: {st}")
            if st in {"CreateFailed", "Deprecated"}:
                raise RuntimeError(f"image failed: {st}")
            if st == "Available":
                done.append((region, image_id))
        for key in done:
            pending.pop(key, None)
        if pending:
            await asyncio.sleep(poll_interval)


def ensure_images_in_regions(
    *,
    creds: AliCredentials,
    target_regions: Sequence[str],
    image_name: str,
    search_regions: Optional[Sequence[str]] = None,
    poll_interval: int,
    wait_timeout: int,
) -> dict[str, str]:
    region_list = [r for r in target_regions if r]
    if not region_list:
        return {}
    lookup_regions = [r for r in (search_regions or region_list) if r]
    image_map: dict[str, str] = {}

    found = find_img_in_regions(creds, lookup_regions, image_name)
    if not found:
        build_region = region_list[0]
        built_id = build_base_image_in_region(
            creds=creds,
            region=build_region,
            image_name=image_name,
            poll_interval=poll_interval,
            wait_timeout=wait_timeout,
        )
        image_map[build_region] = built_id
        if build_region not in lookup_regions:
            lookup_regions.append(build_region)
    else:
        src_region, src_image_id = found
        image_map[src_region] = src_image_id

    async def _ensure_all() -> dict[str, str]:
        tasks = [
            ensure_image_in_region(
                creds=creds,
                region=region,
                image_name=image_name,
                search_regions=lookup_regions,
                poll_interval=poll_interval,
                wait_timeout=wait_timeout,
            )
            for region in region_list
        ]
        results = await asyncio.gather(*tasks)
        return {region: image_id for region, image_id in zip(region_list, results)}

    image_map.update(asyncio.run(_ensure_all()))
    return image_map


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


async def prepare_docker_server_image(host: str, cfg: EcsRuntimeConfig) -> None:
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

        prepare_script = Path(__file__).resolve().parent.parent / "scripts" / "remote" / "prepare_docker_server_image.sh"
        if not prepare_script.exists():
            raise FileNotFoundError(f"prepare script not found: {prepare_script}")
        remote_prepare = f"/tmp/{prepare_script.name}.{int(time.time())}.sh"
        await asyncssh.scp(str(prepare_script), (conn, remote_prepare))
        await run(f"sudo bash {shlex.quote(remote_prepare)}")
        await run(f"sudo rm -f {shlex.quote(remote_prepare)}")


def create_server_image(
    cfg: EcsRuntimeConfig,
    *,
    base_image_id: str,
    dry_run: bool = False,
    prepare_fn: Callable[[str, EcsRuntimeConfig], Coroutine[Any, Any, None]] = prepare_docker_server_image,
) -> str:
    name = _img_name()
    c = client(cfg.credentials, cfg.region_id)
    existing = find_img(c, cfg.region_id, name)
    if existing:
        logger.info(f"image exists: {existing}")
        if not dry_run:
            wait_img(c, cfg.region_id, existing, cfg.poll_interval, cfg.wait_timeout)
        return f"dry-run:{existing}" if dry_run else existing
    if dry_run:
        return f"dry-run:{name}"
    sel = pick_instance_type_for_building_image(c, cfg)
    if not sel and cfg.use_spot:
        cfg.use_spot = False
        sel = pick_instance_type_for_building_image(c, cfg)
    if not sel:
        raise RuntimeError("no instance type")
    cfg.zone_id, selected_type = sel
    cfg.instance_type = [InstanceTypeConfig(name=selected_type)]
    ensure_net(c, cfg)
    ensure_keypair(c, cfg.region_id, cfg.key_pair_name, cfg.ssh_private_key_path)
    iid = ""
    try:
        cfg.image_id = base_image_id
        iid = create_instance(c, cfg, disk_size=20)[0]
        logger.info(f"builder: {iid}")
        st = wait_status(c, cfg.region_id, iid, ["Stopped", "Running"], cfg.poll_interval, cfg.wait_timeout)
        if st == "Stopped":
            try:
                start_instance(c, iid)
            except Exception as exc:
                logger.warning(f"start_instance failed for {iid}: {exc}. Will wait for instance to become Running.")
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
        if iid:
            try:
                delete_instance(c, cfg.region_id, iid)
                logger.info(f"builder deleted: {iid}")
            except Exception as e:
                logger.warning(f"delete failed: {e}")


