from typing import List, Optional, Protocol, Tuple

from .create_instances.instance_config import InstanceConfig
from .create_instances.types import ImageInfo, InstanceStatus, KeyPairInfo, KeyPairRequestConfig, SecurityGroupInfo, VSwitchInfo, VpcInfo, InstanceType, RegionInfo, ZoneInfo, CreateInstanceError
from .cleanup_instances.types import InstanceInfoWithTag

from abc import ABC, abstractmethod
from typing import List, Optional


class IEcsClient(ABC):
    @abstractmethod
    def get_zone_ids_in_region(self, region_id: str) -> List[str]:
        ...

    @abstractmethod
    def describe_instance_status(self, region_id: str, instance_ids: List[str]) -> InstanceStatus:
        ...
        
    @abstractmethod
    def get_instances_with_tag(self, region_id: str) -> List[InstanceInfoWithTag]:
        ...

    @abstractmethod
    def get_images_in_region(self, region_id: str, image_name: str) -> List[ImageInfo]:
        ...

    @abstractmethod
    def get_keypairs_in_region(self, region_id: str, key_pair_name: str) -> Optional[KeyPairInfo]:
        ...

    @abstractmethod
    def get_security_groups_in_region(self, region_id: str, vpc_id: str) -> List[SecurityGroupInfo]:
        ...

    @abstractmethod
    def get_v_switchs_in_region(self, region_id: str, vpc_id: str) -> List[VSwitchInfo]:
        ...

    @abstractmethod
    def get_vpcs_in_region(self, region_id: str) -> List[VpcInfo]:
        ...

    @abstractmethod
    def create_instances_in_zone(
        self,
        cfg: InstanceConfig,
        region_info: RegionInfo,
        zone_info: ZoneInfo,
        instance_type: InstanceType,
        max_amount: int,
        min_amount: int,
    ) -> Tuple[list[str], CreateInstanceError]:
        ...
        
    @abstractmethod
    def delete_instances(self, region_id: str, instances_ids: List[str]):
        ...

    @abstractmethod
    def create_keypair(self, region_id: str, key_pair: KeyPairRequestConfig):
        ...

    @abstractmethod
    def create_security_group(self, region_id: str, vpc_id: str, security_group_name: str) -> str:
        ...

    @abstractmethod
    def create_v_switch(self, region_id: str, zone_id: str, vpc_id: str, v_switch_name: str, cidr_block: str) -> str:
        ...

    @abstractmethod
    def create_vpc(self, region_id: str, vpc_name: str, cidr_block: str) -> str:
        ...
