from dataclasses import dataclass
import os
from typing import List, Optional
from alibabacloud_ecs20140526.client import Client as EcsClient
from alibabacloud_tea_openapi.models import Config as AliyunConfig

from .image import get_images_in_region
from .key_pair import create_keypair, get_keypairs_in_region
from .security_group import create_security_group, get_security_groups_in_region
from .v_switch import create_v_switch, get_v_switchs_in_region
from .vpc import create_vpc, get_vpcs_in_region
from .zone import get_zone_ids_in_region
from .instance import create_instances_in_zone, describe_instance_status

from ..create_instances.provider_interface import IEcsClient
from ..create_instances.instance_config import InstanceConfig
from ..create_instances.types import ImageInfo, InstanceStatus, KeyPairInfo, KeyPairRequestConfig, SecurityGroupInfo, VSwitchInfo, VpcInfo, InstanceType, RegionInfo, ZoneInfo


@dataclass
class AliyunClient(IEcsClient):
    access_key_id: str
    access_key_secret: str

    @classmethod
    def load_from_env(cls) -> 'AliyunClient':
        access_key_id = os.environ["ALI_ACCESS_KEY_ID"]
        access_key_secret = os.environ["ALI_ACCESS_KEY_SECRET"]
        return AliyunClient(access_key_id=access_key_id, access_key_secret=access_key_secret)

    def build(self, region_id: str) -> EcsClient:
        return EcsClient(
            AliyunConfig(
                access_key_id=self.access_key_id,
                access_key_secret=self.access_key_secret,
                region_id=region_id,
                read_timeout=120_000,
                connect_timeout=120_000
            )
        )
        
    def get_zone_ids_in_region(self, region_id: str) -> List[str]:
        client = self.build(region_id)
        return get_zone_ids_in_region(client, region_id)
        
    def describe_instance_status(self, region_id: str, instance_ids: List[str]) -> InstanceStatus:
        client = self.build(region_id)
        return describe_instance_status(client, region_id, instance_ids)
        
    def get_images_in_region(self, region_id: str, image_name: str) -> List[ImageInfo]:
        client = self.build(region_id)
        return get_images_in_region(client, region_id, image_name)
        
    def get_keypairs_in_region(self, region_id: str, key_pair_name: str) -> Optional[KeyPairInfo]:
        client = self.build(region_id)
        return get_keypairs_in_region(client, region_id, key_pair_name)
        
    def get_security_groups_in_region(self, region_id: str, vpc_id: str) -> List[SecurityGroupInfo]:
        client = self.build(region_id)
        return get_security_groups_in_region(client, region_id, vpc_id)
        
    def get_v_switchs_in_region(self, region_id: str, vpc_id: str) -> List[VSwitchInfo]:
        client = self.build(region_id)
        return get_v_switchs_in_region(client, region_id, vpc_id)
        
    def get_vpcs_in_region(self, region_id: str) -> List[VpcInfo]:
        client = self.build(region_id)
        return get_vpcs_in_region(client, region_id)
        
    def create_instances_in_zone(
        self,
        cfg: InstanceConfig,
        region_info: RegionInfo,
        zone_info: ZoneInfo,
        instance_type: InstanceType,
        amount: int,
        allow_partial_success: bool = False,
    ) -> list[str]:
        client = self.build(region_info.id)
        return create_instances_in_zone(client, cfg, region_info, zone_info, instance_type, amount, allow_partial_success)
        
    def create_keypair(self, region_id: str, key_pair: KeyPairRequestConfig):
        client = self.build(region_id)
        return create_keypair(client, region_id, key_pair)

    def create_security_group(self, region_id: str, vpc_id: str, security_group_name: str):
        client = self.build(region_id)
        return create_security_group(client, region_id, vpc_id, security_group_name)
        
    def create_v_switch(self, region_id: str, zone_id: str, vpc_id: str, v_switch_name: str, cidr_block: str):
        client = self.build(region_id)
        return create_v_switch(client, region_id, zone_id, vpc_id, v_switch_name, cidr_block)
        
    def create_vpc(self, region_id: str, vpc_name: str, cidr_block: str):
        client = self.build(region_id)
        return create_vpc(client, region_id, vpc_name, cidr_block)