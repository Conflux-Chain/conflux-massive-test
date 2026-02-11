from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Callable, Dict, List, TypeVar

from loguru import logger

from .types import KeyPairRequestConfig, RegionInfo, ZoneInfo
from ..provider_interface import IEcsClient
from cloud_provisioner.create_instances.provision_config import CloudConfig

DEFAULT_VPC_CIDR = "10.0.0.0/16"

@dataclass
class InfraProvider:
    regions: Dict[str, RegionInfo]

    def get_region(self, region_id: str) -> RegionInfo:
        return self.regions[region_id]

@dataclass
class InfraRequest:
    region_ids: List[str]

    provider: str
    vpc_name: str
    v_switch_name: str
    security_group_name: str
    image_name: str
    key_pair: KeyPairRequestConfig

    allow_create: bool

    @classmethod
    def from_config(cls, config: CloudConfig, allow_create=False) -> 'InfraRequest':
        infra_tag = f"conflux-massive-test-{config.user_tag}"
        # Use shorter key pair name for Tencent Cloud to avoid 25-character limit and use underscores instead of hyphens
        if config.provider == "tencent":
            key_pair_name = f"cfx_test_{config.user_tag}"
        else:
            key_pair_name = infra_tag
        return InfraRequest(region_ids=[r.name for r in config.regions],
                            provider=config.provider,
                            vpc_name=infra_tag,
                            v_switch_name=infra_tag,
                            security_group_name=infra_tag,
                            image_name=config.image_name,
                            key_pair=KeyPairRequestConfig(key_path=config.ssh_key_path, key_pair_name=key_pair_name),
                            allow_create=allow_create
                            )

    def ensure_infras(self, client: IEcsClient) -> InfraProvider:
        with ThreadPoolExecutor(max_workers=5) as executor:
            regions = list(executor.map(lambda region_id: self._ensure_region(
                client, region_id), self.region_ids))

        return InfraProvider(regions={reg.id: reg for reg in regions})

    def _ensure_region(self, client: IEcsClient, region_id: str) -> RegionInfo:
        zone_ids = client.get_zone_ids_in_region(region_id)
        image_id = self._ensure_image_in_region(client, region_id)

        vpc_id = self._ensure_vpc_in_region(client, region_id)

        security_group_id = self._ensure_security_group_in_region(
            client, region_id, vpc_id)

        self._ensure_key_pair_in_region(client, region_id)

        zones = self._ensure_v_switches_in_region(
            client, region_id, zone_ids, vpc_id)

        return RegionInfo(id=region_id, zones=zones, image_id=image_id, security_group_id=security_group_id, vpc_id=vpc_id, key_pair_name=self.key_pair.key_pair_name, key_path=self.key_pair.key_path)

    def _ensure_image_in_region(self, client: IEcsClient, region_id: str):
        images = _find(client.get_images_in_region(
            region_id, self.image_name), lambda im: im.image_name == self.image_name)
        if images is not None:
            logger.info(f"Get Image {self.image_name}: {images.image_id}")
            return images.image_id
        else:
            raise Exception(
                f"Image {self.image_name} not found in region {region_id}")

    def _ensure_vpc_in_region(self, client: IEcsClient, region_id: str) -> str:
        vpc = _find(client.get_vpcs_in_region(region_id),
                    lambda vpc: vpc.vpc_name == self.vpc_name)
        if vpc is not None:
            logger.info(
                f"Get VPC {self.vpc_name} in {region_id}: {vpc.vpc_id}")
            return vpc.vpc_id
        elif self.allow_create:
            logger.info(
                f"Cannot find VPC {self.vpc_name} in {region_id}, creating...")
            vpc_id = client.create_vpc(region_id, self.vpc_name, DEFAULT_VPC_CIDR)
            logger.info(
                f"Created VPC {self.vpc_name} in {region_id}: {vpc_id}")
            return vpc_id
        else:
            raise Exception(
                f"VPC {self.vpc_name} not found in region {region_id}")

    def _ensure_security_group_in_region(self, client: IEcsClient, region_id: str, vpc_id: str) -> str:
        sg = _find(client.get_security_groups_in_region(region_id, vpc_id),
                   lambda sg: sg.security_group_name == self.security_group_name)
        if sg is not None:
            logger.info(
                f"Get Security Group {self.security_group_name} in {region_id}/{vpc_id}: {sg.security_group_id}")
            return sg.security_group_id
        elif self.allow_create:
            logger.info(
                f"Cannot find Security Group {self.security_group_name} in {region_id}/{vpc_id}, creating...")
            security_group_id = client.create_security_group(
                region_id, vpc_id, self.security_group_name)
            logger.info(
                f"Created Security Group {self.security_group_name} in {region_id}/{vpc_id}: {security_group_id}")
            return security_group_id
        else:
            raise Exception(
                f"Security group {self.security_group_name} not found in {region_id}/{vpc_id}")

    def _ensure_key_pair_in_region(self, client: IEcsClient, region_id: str):
        key_pair = client.get_keypairs_in_region(
            region_id, self.key_pair.key_pair_name)

        if key_pair is not None and key_pair.finger_print == self.key_pair.finger_print(self.provider):
            logger.info(
                f"Get KeyPair {self.key_pair.key_pair_name} in {region_id}")
            return
        elif self.allow_create and key_pair is None:
            # TODO: 支持在指纹不一致的时候删除重建
            logger.info(
                f"Cannot find KeyPair {self.key_pair.key_pair_name} in {region_id}, creating...")
            client.create_keypair(region_id, self.key_pair)
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

    def _ensure_v_switches_in_region(self, client: IEcsClient, region_id: str, zone_ids: List[str], vpc_id: str) -> Dict[str, ZoneInfo]:
        v_switches = client.get_v_switchs_in_region(region_id, vpc_id)

        zones: List[ZoneInfo] = []

        occupied_blocks = [vs.cidr_block for vs in v_switches]

        for zone_id in zone_ids:
            v_switch = _find(v_switches, lambda vs: vs.v_switch_name ==
                             self.v_switch_name and vs.zone_id == zone_id)
            if v_switch is not None:
                if v_switch.status.lower() != "available":
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
                v_switch_id = client.create_v_switch(
                    region_id, zone_id, vpc_id, self.v_switch_name, allocated_cidr_block)

                logger.info(
                    f"Create VSwitch {self.v_switch_name} in region {region_id} zone {zone_id}: {v_switch_id}")
                zones.append(ZoneInfo(id=zone_id, v_switch_id=v_switch_id))
            else:
                raise Exception(
                    f"Cannot found v-switch {self.v_switch_name} in region {region_id} zone {zone_id}")

        return {zone.id: zone for zone in zones}
    
def allocate_vacant_cidr_block(occupied_blocks: List[str], prefix: int = 24, vpc_cidr: str = DEFAULT_VPC_CIDR):
    import ipaddress
    
    # 将已占用的 CIDR 转换为网络对象集合
    occupied = {ipaddress.ip_network(block) for block in occupied_blocks if block}
    
    # 遍历 VPC CIDR 的所有指定前缀长度的子网
    for subnet in ipaddress.ip_network(vpc_cidr).subnets(new_prefix=prefix):
        # 检查该子网是否与所有已占用的块都不重叠
        if all(not subnet.overlaps(used) for used in occupied):
            return str(subnet)
    
    # 如果没有找到可用的子网
    raise RuntimeError(
        f"No available /{prefix} subnet found in {vpc_cidr}. "
        f"All subnets are occupied or overlapping."
    )
    
T = TypeVar('T')

def _find(inputs: List[T], cond: Callable[[T], bool]):
    for i in inputs:
        if cond(i):
            return i
    return None