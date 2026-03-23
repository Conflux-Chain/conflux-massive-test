from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Dict, List

from loguru import logger

from .infra_common import DEFAULT_VPC_CIDR, allocate_vacant_cidr_block, build_infra_names, find_first
from .types import KeyPairRequestConfig, RegionInfo, ZoneInfo
from ..provider_interface import IEcsClient
from cloud_provisioner.create_instances.provision_config import CloudConfig


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
    def from_config(cls, config: CloudConfig, allow_create=False) -> "InfraRequest":
        names = build_infra_names(
            provider=config.provider,
            user_tag=config.user_tag,
            image_name=config.image_name,
            key_pair_tag=config.get_key_pair_tag(),
        )
        return InfraRequest(
            region_ids=[region.name for region in config.regions],
            provider=config.provider,
            vpc_name=names.vpc_name,
            v_switch_name=names.v_switch_name,
            security_group_name=names.security_group_name,
            image_name=names.image_name,
            key_pair=KeyPairRequestConfig(
                key_path=config.ssh_key_path,
                key_pair_name=names.key_pair_name,
            ),
            allow_create=allow_create,
        )

    def ensure_infras(self, client: IEcsClient) -> InfraProvider:
        with ThreadPoolExecutor(max_workers=5) as executor:
            regions = list(executor.map(lambda region_id: self._ensure_region(client, region_id), self.region_ids))

        return InfraProvider(regions={region.id: region for region in regions})

    def _ensure_region(self, client: IEcsClient, region_id: str) -> RegionInfo:
        zone_ids = client.get_zone_ids_in_region(region_id)
        image_id = self._ensure_image_in_region(client, region_id)

        vpc_id = self._ensure_vpc_in_region(client, region_id)

        security_group_id = self._ensure_security_group_in_region(client, region_id, vpc_id)

        self._ensure_key_pair_in_region(client, region_id)

        zones = self._ensure_v_switches_in_region(client, region_id, zone_ids, vpc_id)

        return RegionInfo(
            id=region_id,
            zones=zones,
            image_id=image_id,
            security_group_id=security_group_id,
            vpc_id=vpc_id,
            key_pair_name=self.key_pair.key_pair_name,
            key_path=self.key_pair.key_path,
        )

    def _ensure_image_in_region(self, client: IEcsClient, region_id: str):
        image = find_first(
            client.get_images_in_region(region_id, self.image_name),
            lambda item: item.image_name == self.image_name,
        )
        if image is not None:
            logger.info(f"Get Image {self.image_name}: {image.image_id}")
            return image.image_id

        raise Exception(f"Image {self.image_name} not found in region {region_id}")

    def _ensure_vpc_in_region(self, client: IEcsClient, region_id: str) -> str:
        vpc = find_first(client.get_vpcs_in_region(region_id), lambda item: item.vpc_name == self.vpc_name)
        if vpc is not None:
            logger.info(f"Get VPC {self.vpc_name} in {region_id}: {vpc.vpc_id}")
            return vpc.vpc_id
        if self.allow_create:
            logger.info(f"Cannot find VPC {self.vpc_name} in {region_id}, creating...")
            vpc_id = client.create_vpc(region_id, self.vpc_name, DEFAULT_VPC_CIDR)
            logger.info(f"Created VPC {self.vpc_name} in {region_id}: {vpc_id}")
            return vpc_id

        raise Exception(f"VPC {self.vpc_name} not found in region {region_id}")

    def _ensure_security_group_in_region(self, client: IEcsClient, region_id: str, vpc_id: str) -> str:
        security_group = find_first(
            client.get_security_groups_in_region(region_id, vpc_id),
            lambda item: item.security_group_name == self.security_group_name,
        )
        if security_group is not None:
            logger.info(
                f"Get Security Group {self.security_group_name} in {region_id}/{vpc_id}: {security_group.security_group_id}"
            )
            return security_group.security_group_id
        if self.allow_create:
            logger.info(
                f"Cannot find Security Group {self.security_group_name} in {region_id}/{vpc_id}, creating..."
            )
            security_group_id = client.create_security_group(region_id, vpc_id, self.security_group_name)
            logger.info(
                f"Created Security Group {self.security_group_name} in {region_id}/{vpc_id}: {security_group_id}"
            )
            return security_group_id

        raise Exception(f"Security group {self.security_group_name} not found in {region_id}/{vpc_id}")

    def _ensure_key_pair_in_region(self, client: IEcsClient, region_id: str):
        key_pair = client.get_keypairs_in_region(region_id, self.key_pair.key_pair_name)

        if key_pair is not None and key_pair.finger_print == self.key_pair.finger_print(self.provider):
            logger.info(f"Get KeyPair {self.key_pair.key_pair_name} in {region_id}")
            return
        if self.allow_create and key_pair is None:
            logger.info(f"Cannot find KeyPair {self.key_pair.key_pair_name} in {region_id}, creating...")
            client.create_keypair(region_id, self.key_pair)
            logger.info(f"Created KeyPair {self.key_pair.key_pair_name} in {region_id}")
            return
        if key_pair is None:
            raise Exception(f"Key pair {self.key_pair.key_pair_name} not found in region {region_id}")

        raise Exception(
            f"Key pair {self.key_pair.key_pair_name} has inconsistent finger print in region {region_id}"
        )

    def _ensure_v_switches_in_region(
        self,
        client: IEcsClient,
        region_id: str,
        zone_ids: List[str],
        vpc_id: str,
    ) -> Dict[str, ZoneInfo]:
        v_switches = client.get_v_switchs_in_region(region_id, vpc_id)

        zones: List[ZoneInfo] = []
        occupied_blocks = [item.cidr_block for item in v_switches]

        for zone_id in zone_ids:
            v_switch = find_first(
                v_switches,
                lambda item: item.v_switch_name == self.v_switch_name and item.zone_id == zone_id,
            )
            if v_switch is not None:
                if v_switch.status.lower() != "available":
                    raise Exception(
                        f"v-switch {self.v_switch_name} in region {region_id} zone {zone_id} has unexpected status: {v_switch.status}"
                    )
                logger.info(
                    f"Get VSwitch {self.v_switch_name} in region {region_id} zone {zone_id}: {v_switch.v_switch_id}"
                )
                zones.append(ZoneInfo(id=zone_id, v_switch_id=v_switch.v_switch_id))
                continue

            if self.allow_create:
                logger.info(
                    f"Cannot find VSwitch {self.v_switch_name} in region {region_id} zone {zone_id}, creating..."
                )
                allocated_cidr_block = allocate_vacant_cidr_block(occupied_blocks, prefix=20)
                occupied_blocks.append(allocated_cidr_block)
                v_switch_id = client.create_v_switch(
                    region_id,
                    zone_id,
                    vpc_id,
                    self.v_switch_name,
                    allocated_cidr_block,
                )

                logger.info(
                    f"Create VSwitch {self.v_switch_name} in region {region_id} zone {zone_id}: {v_switch_id}"
                )
                zones.append(ZoneInfo(id=zone_id, v_switch_id=v_switch_id))
                continue

            raise Exception(f"Cannot found v-switch {self.v_switch_name} in region {region_id} zone {zone_id}")

        return {zone.id: zone for zone in zones}