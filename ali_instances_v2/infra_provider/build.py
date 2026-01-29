from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
import os
from typing import Callable, Dict, List, Tuple, TypeVar

from alibabacloud_ecs20140526 import models as ecs_models
from alibabacloud_ecs20140526.client import Client as EcsClient
from alibabacloud_tea_openapi.models import Config as AliyunConfig

from loguru import logger

from ali_instances_v2.client_factory import ClientFactory
from request_config import AliyunRequestConfig

from .security_group import get_security_groups_in_region, create_security_group
from .v_switch import get_v_switchs_in_region, create_v_switch, allocate_vacant_cidr_block
from .vpc import get_vpcs_in_region, create_vpc
from .key_pair import KeyPairRequestConfig, get_keypairs_in_region, create_keypair
from .image import get_images_in_region
from ..types import RegionInfo, ZoneInfo


@dataclass
class InfraProvider:
    regions: Dict[str, RegionInfo]

    def get_region(self, region_id: str) -> RegionInfo:
        return self.regions[region_id]



def get_zone_ids_in_region(c: ClientFactory, region_id: str) -> List[str]:
    client = c.build(region_id)
    rep = client.describe_zones(ecs_models.DescribeZonesRequest(
        region_id=region_id, verbose=False))
    return [zone.zone_id for zone in rep.body.zones.zone]


T = TypeVar('T')


def _find(inputs: List[T], cond: Callable[[T], bool]):
    for i in inputs:
        if cond(i):
            return i
    return None

@dataclass
class InfraRequest:
    region_ids: List[str]

    vpc_name: str
    v_switch_name: str
    security_group_name: str
    image_name: str
    key_pair: KeyPairRequestConfig

    allow_create: bool

    @classmethod
    def from_aliyun_config(cls, config: AliyunRequestConfig, allow_create=False) -> 'InfraRequest':
        infra_tag = f"conflux-massive-test-{config.user_tag}"
        return InfraRequest(region_ids=[r.name for r in config.regions],
                            vpc_name=infra_tag,
                            v_switch_name=infra_tag,
                            security_group_name=infra_tag,
                            image_name=config.image_name,
                            key_pair=KeyPairRequestConfig(key_path=config.ssh_key_path, key_pair_name=infra_tag),
                            allow_create=allow_create
                            )

    def ensure_infras(self, client_factory: ClientFactory) -> InfraProvider:
        with ThreadPoolExecutor(max_workers=5) as executor:
            regions = list(executor.map(lambda region_id: self._ensure_region(
                client_factory, region_id), self.region_ids))

        return InfraProvider(regions={reg.id: reg for reg in regions})

    def _ensure_region(self, client: ClientFactory, region_id: str) -> RegionInfo:
        zone_ids = get_zone_ids_in_region(client, region_id)
        image_id = self._ensure_image_in_region(client, region_id)

        vpc_id = self._ensure_vpc_in_region(client, region_id)

        security_group_id = self._ensure_security_group_in_region(
            client, region_id, vpc_id)

        self._ensure_key_pair_in_region(client, region_id)

        zones = self._ensure_v_switches_in_region(
            client, region_id, zone_ids, vpc_id)

        return RegionInfo(id=region_id, zones=zones, image_id=image_id, security_group_id=security_group_id, vpc_id=vpc_id, key_pair_name=self.key_pair.key_pair_name, key_path=self.key_pair.key_path)

    def _ensure_image_in_region(self, client: ClientFactory, region_id: str):
        images = _find(get_images_in_region(
            client, region_id, self.image_name), lambda im: im.image_name == self.image_name)
        if images is not None:
            return images.image_id
        else:
            raise Exception(
                f"Image {self.image_name} not found in region {region_id}")

    def _ensure_vpc_in_region(self, client: ClientFactory, region_id: str) -> str:
        vpc = _find(get_vpcs_in_region(client, region_id),
                    lambda vpc: vpc.vpc_name == self.vpc_name)
        if vpc is not None:
            logger.info(
                f"Get VPC {self.vpc_name} in {region_id}: {vpc.vpc_id}")
            return vpc.vpc_id
        elif self.allow_create:
            logger.info(
                f"Cannot find VPC {self.vpc_name} in {region_id}, creating...")
            vpc_id = create_vpc(client, region_id, self.vpc_name)
            logger.info(
                f"Created VPC {self.vpc_name} in {region_id}: {vpc_id}")
            return vpc_id
        else:
            raise Exception(
                f"VPC {self.vpc_name} not found in region {region_id}")

    def _ensure_security_group_in_region(self, client: ClientFactory, region_id: str, vpc_id: str) -> str:
        sg = _find(get_security_groups_in_region(client, region_id, vpc_id),
                   lambda sg: sg.security_group_name == self.security_group_name)
        if sg is not None:
            logger.info(
                f"Get Security Group {self.security_group_name} in {region_id}/{vpc_id}: {sg.security_group_id}")
            return sg.security_group_id
        elif self.allow_create:
            logger.info(
                f"Cannot find Security Group {self.security_group_name} in {region_id}/{vpc_id}, creating...")
            security_group_id = create_security_group(
                client, region_id, vpc_id, self.security_group_name)
            logger.info(
                f"Created Security Group {self.security_group_name} in {region_id}/{vpc_id}: {security_group_id}")
            return security_group_id
        else:
            raise Exception(
                f"Security group {self.security_group_name} not found in {region_id}/{vpc_id}")

    def _ensure_key_pair_in_region(self, client: ClientFactory, region_id: str):
        key_pair = get_keypairs_in_region(
            client, region_id, self.key_pair.key_pair_name)

        if key_pair is not None and key_pair.finger_print == self.key_pair.finger_print:
            logger.info(
                f"Get KeyPair {self.key_pair.key_pair_name} in {region_id}")
            return
        elif self.allow_create and key_pair is None:
            # TODO: 支持在指纹不一致的时候删除重建
            logger.info(
                f"Cannot find KeyPair {self.key_pair.key_pair_name} in {region_id}, creating...")
            create_keypair(client, region_id, self.key_pair)
            logger.info(
                f"Created KeyPair {self.key_pair.key_pair_name} in {region_id}")
            return
        else:
            if key_pair is None:
                raise Exception(
                    f"Key pair {self.key_pair.key_pair_name} not found in region {region_id}")
            else:
                raise Exception(
                    f"Key pair {self.key_pair.key_pair_name} has inconsistent finger print in region {region_id}")

    def _ensure_v_switches_in_region(self, client: ClientFactory, region_id: str, zone_ids: List[str], vpc_id: str) -> Dict[str, ZoneInfo]:
        v_switches = get_v_switchs_in_region(client, region_id, vpc_id)

        zones: List[ZoneInfo] = []

        occupied_blocks = [vs.cidr_block for vs in v_switches]

        for zone_id in zone_ids:
            v_switch = _find(v_switches, lambda vs: vs.v_switch_name ==
                             self.v_switch_name and vs.zone_id == zone_id)
            if v_switch is not None:
                # TODO: check status availbility
                if v_switch.status != "Available":
                    raise Exception(
                        f"v-switch {self.v_switch_name} in region {region_id} zone {zone_id} has unexpected status: {v_switch.status}")
                logger.info(
                    f"Get VSwitch {self.v_switch_name} in region {region_id} zone {zone_id}: {v_switch.v_switch_id}")
                zones.append(
                    ZoneInfo(id=zone_id, v_switch_id=v_switch.v_switch_id))
            elif self.allow_create:
                logger.info(
                    f"Cannot find VSwitch {self.v_switch_name} in region {region_id} zone {zone_id}, creating...")

                allocated_cidr_block = allocate_vacant_cidr_block(
                    occupied_blocks, prefix=20)
                occupied_blocks.append(allocated_cidr_block)
                v_switch_id = create_v_switch(
                    client, region_id, zone_id, vpc_id, self.v_switch_name, allocated_cidr_block)

                logger.info(
                    f"Create VSwitch {self.v_switch_name} in region {region_id} zone {zone_id}: {v_switch_id}")
                zones.append(ZoneInfo(id=zone_id, v_switch_id=v_switch_id))
            else:
                raise Exception(
                    f"Cannot found v-switch {self.v_switch_name} in region {region_id} zone {zone_id}")

        return {zone.id: zone for zone in zones}