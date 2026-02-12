"""Standalone image-builder instance helpers backed by cloud_provisioner.tencent_provider."""
from dataclasses import dataclass
from typing import Optional, Tuple

from loguru import logger

from cloud_provisioner.create_instances.instance_config import InstanceConfig
from cloud_provisioner.create_instances.types import InstanceType, RegionInfo, ZoneInfo, KeyPairRequestConfig
from cloud_provisioner.tencent_provider.client_factory import TencentClient
from cloud_provisioner.tencent_provider.instance import create_spot_builder_instance, get_instance_public_ip
from cloud_provisioner.tencent_provider.key_pair import create_keypair, get_keypairs_in_region
from cloud_provisioner.tencent_provider.security_group import create_security_group, get_security_groups_in_region
from cloud_provisioner.tencent_provider.v_switch import create_v_switch, get_v_switchs_in_region
from cloud_provisioner.tencent_provider.vpc import create_vpc, get_vpcs_in_region
from cloud_provisioner.tencent_provider.zone import get_zone_ids_in_region

from .config import CvmRuntimeConfig


@dataclass
class NetworkInfo:
    vpc_id: str
    subnet_id: str
    security_group_id: str


def pick_zone(client: TencentClient, region: str) -> str:
    cvm = client.build_cvm(region)
    zones = get_zone_ids_in_region(cvm)
    if not zones:
        raise RuntimeError(f"no available zones in {region}")
    return zones[0]


def ensure_network(client: TencentClient, cfg: CvmRuntimeConfig, zone_id: str) -> NetworkInfo:
    vpc = client.build_vpc(cfg.region_id)
    vpcs = get_vpcs_in_region(vpc)
    vpc_id = next((v.vpc_id for v in vpcs if v.vpc_name == cfg.vpc_name), None)
    if not vpc_id:
        vpc_id = create_vpc(vpc, cfg.vpc_name, cfg.vpc_cidr)

    subnets = get_v_switchs_in_region(vpc, vpc_id)
    subnet_id = next(
        (s.v_switch_id for s in subnets if s.v_switch_name == cfg.subnet_name and s.zone_id == zone_id),
        None,
    )
    if not subnet_id:
        subnet_id = create_v_switch(vpc, zone_id, vpc_id, cfg.subnet_name, cfg.subnet_cidr)

    sgs = get_security_groups_in_region(vpc, vpc_id)
    sg_id = next((g.security_group_id for g in sgs if g.security_group_name == cfg.security_group_name), None)
    if not sg_id:
        sg_id = create_security_group(vpc, vpc_id, cfg.security_group_name)

    return NetworkInfo(vpc_id=vpc_id, subnet_id=subnet_id, security_group_id=sg_id)


def ensure_keypair(client: TencentClient, cfg: CvmRuntimeConfig) -> str:
    cvm = client.build_cvm(cfg.region_id)
    info = get_keypairs_in_region(cvm, cfg.key_pair_name)
    if info:
        return cfg.key_pair_name
    req = KeyPairRequestConfig(key_path=cfg.ssh_private_key_path, key_pair_name=cfg.key_pair_name)
    create_keypair(cvm, req)
    return cfg.key_pair_name


def build_region_info(cfg: CvmRuntimeConfig, network: NetworkInfo, zone_id: str, image_id: str) -> RegionInfo:
    zones = {zone_id: ZoneInfo(id=zone_id, v_switch_id=network.subnet_id)}
    return RegionInfo(
        id=cfg.region_id,
        zones=zones,
        security_group_id=network.security_group_id,
        vpc_id=network.vpc_id,
        image_id=image_id,
        key_pair_name=cfg.key_pair_name,
        key_path=cfg.ssh_private_key_path,
    )


def start_builder_instance(
    client: TencentClient,
    cfg: CvmRuntimeConfig,
    *,
    zone_id: str,
    image_id: str,
    instance_type: str,
) -> Tuple[str, RegionInfo, ZoneInfo]:
    network = ensure_network(client, cfg, zone_id)
    ensure_keypair(client, cfg)

    region_info = build_region_info(cfg, network, zone_id, image_id)
    zone_info = region_info.get_zone(zone_id)

    instance_cfg = InstanceConfig(user_tag_value=cfg.user_tag_value)
    instance_cfg.instance_name_prefix = cfg.instance_name_prefix
    instance_cfg.disk_size = 20
    instance_cfg.internet_max_bandwidth_out = cfg.internet_max_bandwidth_out

    instance_id = create_spot_builder_instance(
        client.build_cvm(cfg.region_id),
        instance_cfg,
        region_info=region_info,
        zone_info=zone_info,
        instance_type=InstanceType(name=instance_type, nodes=1),
    )
    return instance_id, region_info, zone_info


def get_public_ip(client: TencentClient, region_id: str, instance_id: str) -> Optional[str]:
    return get_instance_public_ip(client.build_cvm(region_id), instance_id)


def terminate_instance(client: TencentClient, region_id: str, instance_id: str) -> None:
    cvm = client.build_cvm(region_id)
    from tencentcloud.cvm.v20170312 import models as cvm_models
    req = cvm_models.TerminateInstancesRequest()
    req.InstanceIds = [instance_id]
    cvm.TerminateInstances(req)
    logger.info(f"builder deleted: {instance_id}")

