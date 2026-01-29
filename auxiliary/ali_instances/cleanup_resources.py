"""Cleanup tagged Aliyun ECS resources across regions."""
from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional

from alibabacloud_ecs20140526 import models as ecs_models
from loguru import logger

from .config import EcsRuntimeConfig, DEFAULT_COMMON_TAG_KEY, DEFAULT_COMMON_TAG_VALUE, DEFAULT_USER_TAG_KEY, DEFAULT_USER_TAG_VALUE
from .instance_prep import delete_instance
from .config import client


@dataclass
class TagFilter:
    common_key: str = DEFAULT_COMMON_TAG_KEY
    common_value: str = DEFAULT_COMMON_TAG_VALUE
    user_key: str = DEFAULT_USER_TAG_KEY
    user_value: str = DEFAULT_USER_TAG_VALUE

    def matches(self, tags: Iterable[ecs_models.DescribeInstancesResponseBodyInstancesInstanceTagsTag]) -> bool:
        tag_map = {t.tag_key: t.tag_value for t in tags if t.tag_key}
        return tag_map.get(self.common_key) == self.common_value and tag_map.get(self.user_key) == self.user_value


def _list_regions(cfg: EcsRuntimeConfig) -> List[str]:
    c = client(cfg.credentials, cfg.region_id)
    resp = c.describe_regions(ecs_models.DescribeRegionsRequest())
    return [r.region_id for r in resp.body.regions.region if r.region_id]


def _iter_instances(c, region_id: str):
    page = 1
    while True:
        resp = c.describe_instances(ecs_models.DescribeInstancesRequest(region_id=region_id, page_size=100, page_number=page))
        items = resp.body.instances.instance if resp.body and resp.body.instances else []
        if not items:
            break
        for inst in items:
            yield inst
        if len(items) < 100:
            break
        page += 1


def cleanup_all_regions(
    *,
    regions: Optional[List[str]] = None,
    credentials=None,
    common_tag: str = DEFAULT_COMMON_TAG_KEY,
    common_tag_value: str = DEFAULT_COMMON_TAG_VALUE,
    user_tag: str = DEFAULT_USER_TAG_VALUE,
    user_tag_key: str = DEFAULT_USER_TAG_KEY,
    name_prefix: Optional[str] = None,
    delete_network: bool = False,
) -> None:
    cfg = EcsRuntimeConfig(credentials=credentials or EcsRuntimeConfig().credentials)
    if regions is None:
        regions = _list_regions(cfg)

    tag_filter = TagFilter(common_key=common_tag, common_value=common_tag_value, user_key=user_tag_key, user_value=user_tag)
    prefix = name_prefix or f"{common_tag}-{user_tag}"

    total_deleted = 0
    observed_user_pairs: set[tuple[str, Optional[str]]] = set()
    regions = [
        "cn-wulanchabu",
        "ap-southeast-5",  # Indonesia
        "ap-southeast-3",  # Malaysia
        "ap-southeast-6",  # Philippines
        "ap-southeast-7",  # Thailand
        "ap-northeast-2",  # Korea
        "ap-southeast-1",  # Singapore
        "me-east-1",  
    ]

    for region_id in regions:
        logger.info(f"cleanup region {region_id}")
        c = client(cfg.credentials, region_id)

        try:
            # Delete instances by tags
            for inst in _iter_instances(c, region_id):
                tags = inst.tags.tag if inst.tags else []
                # record instances that have the common tag (regardless of user tag value)
                tag_map = {t.tag_key: t.tag_value for t in tags if t.tag_key}
                if tag_map.get(tag_filter.common_key) == tag_filter.common_value:
                    observed_user_pairs.add((user_tag_key, tag_map.get(user_tag_key)))

                if not tag_filter.matches(tags):
                    continue
                if not inst.instance_id:
                    continue
                try:
                    delete_instance(c, region_id, inst.instance_id)
                    total_deleted += 1
                    logger.info(f"deleted instance {inst.instance_id} in {region_id}")
                except Exception as exc:
                    logger.warning(f"failed to delete instance {inst.instance_id}: {exc}")
        except Exception as exc:
            logger.warning(f"failed to list/delete instances in {region_id}: {exc}")

        # Best-effort cleanup of security groups with the same prefix
        try:
            sg_resp = c.describe_security_groups(
                ecs_models.DescribeSecurityGroupsRequest(region_id=region_id, page_size=50)
            )
            sgs = sg_resp.body.security_groups.security_group if sg_resp.body and sg_resp.body.security_groups else []
            for sg in sgs:
                if not sg.security_group_name or not sg.security_group_id:
                    continue
                if not sg.security_group_name.startswith(prefix):
                    continue
                try:
                    c.delete_security_group(
                        ecs_models.DeleteSecurityGroupRequest(region_id=region_id, security_group_id=sg.security_group_id)
                    )
                    logger.info(f"deleted security group {sg.security_group_id}")
                except Exception as exc:
                    logger.warning(f"failed to delete security group {sg.security_group_id}: {exc}")
        except Exception as exc:
            logger.warning(f"failed to list/delete security groups in {region_id}: {exc}")

        if not delete_network:
            continue
        # Best-effort cleanup of vpcs/vswitches with the same prefix
        # try-catch is needed
        try:
            vpc_resp = c.describe_vpcs(ecs_models.DescribeVpcsRequest(region_id=region_id, page_size=50))
            vpcs = vpc_resp.body.vpcs.vpc if vpc_resp.body and vpc_resp.body.vpcs else []
            for vpc in vpcs:
                if not vpc.vpc_id or not vpc.vpc_name:
                    continue
                if not vpc.vpc_name.startswith(prefix):
                    continue
                try:
                    vsw_resp = c.describe_vswitches(
                        ecs_models.DescribeVSwitchesRequest(region_id=region_id, vpc_id=vpc.vpc_id, page_size=50)
                    )
                    vsws = vsw_resp.body.v_switches.v_switch if vsw_resp.body and vsw_resp.body.v_switches else []
                    for vsw in vsws:
                        if not vsw.v_switch_id:
                            continue
                        try:
                            c.delete_vswitch(
                                ecs_models.DeleteVSwitchRequest(region_id=region_id, v_switch_id=vsw.v_switch_id)
                            )
                            logger.info(f"deleted vswitch {vsw.v_switch_id}")
                        except Exception as exc:
                            logger.warning(f"failed to delete vswitch {vsw.v_switch_id}: {exc}")

                    c.delete_vpc(ecs_models.DeleteVpcRequest(region_id=region_id, vpc_id=vpc.vpc_id))
                    logger.info(f"deleted vpc {vpc.vpc_id}")
                except Exception as exc:
                    logger.warning(f"failed to delete vpc {vpc.vpc_id}: {exc}")
        except Exception as exc:
            logger.warning(f"failed to list/delete vpcs in {region_id}: {exc}")
    # If we deleted nothing, print observed user tag values for instances that had the common tag
    if total_deleted == 0:
        if observed_user_pairs:
            pairs = ", ".join(f"{k}={v if v is not None else '<missing>'}" for k, v in sorted(observed_user_pairs))
            logger.warning(
                f"No instances deleted for filter user_tag_key={user_tag_key}, user_tag_value={user_tag}. "
                f"However, the following user tag(s) were observed on resources with {common_tag}={common_tag_value}: {pairs}"
            )
        else:
            logger.warning(f"No instances found with {common_tag}={common_tag_value} in the queried regions.")

def cleanup_from_json(
    json_path: Path,
    *,
    credentials=None,
) -> None:
    data = json.loads(json_path.read_text())
    # Import loader locally to avoid cyclic imports at module import time
    from .create_servers import load_host_specs

    hosts = load_host_specs(data)

    cfg = EcsRuntimeConfig(credentials=credentials or EcsRuntimeConfig().credentials)
    by_region: Dict[str, List[str]] = {}
    for h in hosts:
        region = h.region
        instance_id = h.instance_id
        if not region or not instance_id:
            logger.warning(f"skip entry without region/instance_id: {h}")
            continue
        by_region.setdefault(region, []).append(instance_id)

    for region_id, instance_ids in by_region.items():
        logger.info(f"cleanup instances in region {region_id}")
        c = client(cfg.credentials, region_id)
        for iid in instance_ids:
            try:
                delete_instance(c, region_id, iid)
                logger.info(f"deleted instance {iid} in {region_id}")
            except Exception as exc:
                logger.warning(f"failed to delete instance {iid}: {exc}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Cleanup Aliyun ECS resources.")
    parser.add_argument(
        "--instances-json",
        help="Path to ali_servers.json to cleanup only listed instances",
    )
    parser.add_argument(
        "--delete-network",
        action="store_true",
        help="Also delete VPC/VSwitch resources with the matching prefix",
    )
    args = parser.parse_args()

    if args.instances_json:
        cleanup_from_json(Path(args.instances_json))
    else:
        cleanup_all_regions()
