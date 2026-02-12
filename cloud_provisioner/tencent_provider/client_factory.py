from dataclasses import dataclass
import os
from typing import List, Optional

from tencentcloud.common import credential
from tencentcloud.common.profile.client_profile import ClientProfile
from tencentcloud.common.profile.http_profile import HttpProfile
from tencentcloud.cvm.v20170312 import cvm_client
from tencentcloud.vpc.v20170312 import vpc_client

from .image import get_images_in_region
from .key_pair import create_keypair, get_keypairs_in_region
from .security_group import create_security_group, get_security_groups_in_region
from .v_switch import create_v_switch, get_v_switchs_in_region
from .vpc import create_vpc, get_vpcs_in_region
from .zone import get_zone_ids_in_region
from .instance import create_instances_in_zone, delete_instances, describe_instance_status, get_instances_with_tag
from ..provider_interface import IEcsClient
from ..create_instances.instance_config import InstanceConfig
from ..cleanup_instances.types import InstanceInfoWithTag
from ..create_instances.types import ImageInfo, InstanceStatus, KeyPairInfo, KeyPairRequestConfig, SecurityGroupInfo, VSwitchInfo, VpcInfo, InstanceType, RegionInfo, ZoneInfo, CreateInstanceError


def _build_profile(endpoint: str) -> ClientProfile:
    http_profile = HttpProfile(endpoint=endpoint, reqTimeout=120)
    return ClientProfile(httpProfile=http_profile)


@dataclass
class TencentClient(IEcsClient):
    secret_id: str
    secret_key: str

    @classmethod
    def load_from_env(cls) -> "TencentClient":
        secret_id = os.environ["TENCENTCLOUD_SECRET_ID"]
        secret_key = os.environ["TENCENTCLOUD_SECRET_KEY"]
        return TencentClient(secret_id=secret_id, secret_key=secret_key)

    def _credential(self) -> credential.Credential:
        return credential.Credential(self.secret_id, self.secret_key)

    def build_cvm(self, region_id: str) -> cvm_client.CvmClient:
        return cvm_client.CvmClient(self._credential(), region_id, _build_profile("cvm.tencentcloudapi.com"))

    def build_vpc(self, region_id: str) -> vpc_client.VpcClient:
        return vpc_client.VpcClient(self._credential(), region_id, _build_profile("vpc.tencentcloudapi.com"))

    def get_zone_ids_in_region(self, region_id: str) -> List[str]:
        client = self.build_cvm(region_id)
        return get_zone_ids_in_region(client)

    def describe_instance_status(self, region_id: str, instance_ids: List[str]) -> InstanceStatus:
        client = self.build_cvm(region_id)
        return describe_instance_status(client, instance_ids)

    def get_instances_with_tag(self, region_id: str) -> List[InstanceInfoWithTag]:
        client = self.build_cvm(region_id)
        return get_instances_with_tag(client)

    def get_images_in_region(self, region_id: str, image_name: str) -> List[ImageInfo]:
        client = self.build_cvm(region_id)
        return get_images_in_region(client, image_name)

    def get_keypairs_in_region(self, region_id: str, key_pair_name: str) -> Optional[KeyPairInfo]:
        client = self.build_cvm(region_id)
        return get_keypairs_in_region(client, key_pair_name)

    def get_security_groups_in_region(self, region_id: str, vpc_id: str) -> List[SecurityGroupInfo]:
        client = self.build_vpc(region_id)
        return get_security_groups_in_region(client, vpc_id)

    def get_v_switchs_in_region(self, region_id: str, vpc_id: str) -> List[VSwitchInfo]:
        client = self.build_vpc(region_id)
        return get_v_switchs_in_region(client, vpc_id)

    def get_vpcs_in_region(self, region_id: str) -> List[VpcInfo]:
        client = self.build_vpc(region_id)
        return get_vpcs_in_region(client)

    def create_instances_in_zone(
        self,
        cfg: InstanceConfig,
        region_info: RegionInfo,
        zone_info: ZoneInfo,
        instance_type: InstanceType,
        max_amount: int,
        min_amount: int,
    ) -> tuple[list[str], CreateInstanceError]:
        client = self.build_cvm(region_info.id)
        return create_instances_in_zone(client, cfg, region_info, zone_info, instance_type, max_amount, min_amount)

    def delete_instances(self, region_id: str, instances_ids: List[str]):
        client = self.build_cvm(region_id)
        return delete_instances(client, instances_ids)

    def create_keypair(self, region_id: str, key_pair: KeyPairRequestConfig):
        client = self.build_cvm(region_id)
        return create_keypair(client, key_pair)

    def create_security_group(self, region_id: str, vpc_id: str, security_group_name: str):
        client = self.build_vpc(region_id)
        return create_security_group(client, vpc_id, security_group_name)

    def create_v_switch(self, region_id: str, zone_id: str, vpc_id: str, v_switch_name: str, cidr_block: str):
        client = self.build_vpc(region_id)
        return create_v_switch(client, zone_id, vpc_id, v_switch_name, cidr_block)

    def create_vpc(self, region_id: str, vpc_name: str, cidr_block: str):
        client = self.build_vpc(region_id)
        return create_vpc(client, vpc_name, cidr_block)
