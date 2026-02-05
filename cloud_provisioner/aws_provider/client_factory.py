from dataclasses import dataclass
import os
from typing import List, Optional, Tuple
import boto3

from cloud_provisioner.cleanup_instances.types import InstanceInfoWithTag

from .image import get_images_in_region
from .key_pair import create_keypair, get_keypairs_in_region
from .security_group import create_security_group, get_security_groups_in_region
from .v_switch import create_v_switch, get_v_switchs_in_region
from .vpc import create_vpc, get_vpcs_in_region
from .zone import get_zone_ids_in_region
from .instance import create_instances_in_zone, delete_instances, describe_instance_status, get_instances_with_tag

from ..provider_interface import IEcsClient
from ..create_instances.instance_config import InstanceConfig
from ..create_instances.types import CreateInstanceError, ImageInfo, InstanceStatus, KeyPairInfo, KeyPairRequestConfig, SecurityGroupInfo, VSwitchInfo, VpcInfo, InstanceType, RegionInfo, ZoneInfo

from mypy_boto3_ec2.client import EC2Client

@dataclass
class AwsClient(IEcsClient):
    pass

    @classmethod
    def new(cls) -> 'AwsClient':
        return AwsClient()

    def build(self, region_id: str) -> EC2Client:
        return boto3.client('ec2', region_name=region_id)
        
    def get_zone_ids_in_region(self, region_id: str) -> List[str]:
        client = self.build(region_id)
        return get_zone_ids_in_region(client)
        
    def describe_instance_status(self, region_id: str, instance_ids: List[str]) -> InstanceStatus:
        client = self.build(region_id)
        return describe_instance_status(client, instance_ids)
    
    def get_instances_with_tag(self, region_id: str) -> List[InstanceInfoWithTag]:
        client = self.build(region_id)
        return get_instances_with_tag(client)
        
    def get_images_in_region(self, region_id: str, image_name: str) -> List[ImageInfo]:
        client = self.build(region_id)
        return get_images_in_region(client, image_name)
        
    def get_keypairs_in_region(self, region_id: str, key_pair_name: str) -> Optional[KeyPairInfo]:
        client = self.build(region_id)
        return get_keypairs_in_region(client, region_id, key_pair_name)
        
    def get_security_groups_in_region(self, region_id: str, vpc_id: str) -> List[SecurityGroupInfo]:
        client = self.build(region_id)
        return get_security_groups_in_region(client, vpc_id)
        
    def get_v_switchs_in_region(self, region_id: str, vpc_id: str) -> List[VSwitchInfo]:
        client = self.build(region_id)
        return get_v_switchs_in_region(client, vpc_id)
        
    def get_vpcs_in_region(self, region_id: str) -> List[VpcInfo]:
        client = self.build(region_id)
        return get_vpcs_in_region(client)
        
    def create_instances_in_zone(
        self,
        cfg: InstanceConfig,
        region_info: RegionInfo,
        zone_info: ZoneInfo,
        instance_type: InstanceType,
        max_amount: int,
        min_amount: int,
    ) -> Tuple[list[str], CreateInstanceError]:
        client = self.build(region_info.id)
        return create_instances_in_zone(client, cfg, region_info, zone_info, instance_type, max_amount, min_amount)
    
    def delete_instances(self, region_id: str, instances_ids: List[str]):
        client = self.build(region_id)
        return delete_instances(client, instances_ids)
        
    def create_keypair(self, region_id: str, key_pair: KeyPairRequestConfig):
        client = self.build(region_id)
        return create_keypair(client, region_id, key_pair)

    def create_security_group(self, region_id: str, vpc_id: str, security_group_name: str):
        client = self.build(region_id)
        return create_security_group(client, vpc_id, security_group_name)
        
    def create_v_switch(self, region_id: str, zone_id: str, vpc_id: str, v_switch_name: str, cidr_block: str):
        client = self.build(region_id)
        return create_v_switch(client, zone_id, vpc_id, v_switch_name, cidr_block)
        
    def create_vpc(self, region_id: str, vpc_name: str, cidr_block: str):
        client = self.build(region_id)
        return create_vpc(client, vpc_name, cidr_block)