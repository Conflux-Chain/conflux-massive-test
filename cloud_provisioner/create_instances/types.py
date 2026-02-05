from dataclasses import dataclass
from typing import Dict, Set
from enum import Enum

from cloud_provisioner.create_instances.crypto import get_fingerprint_from_key, get_public_key_body

@dataclass
class ZoneInfo:
    id: str
    v_switch_id: str
    
@dataclass
class RegionInfo:
    id: str
    zones: Dict[str, ZoneInfo]
    security_group_id: str
    vpc_id: str
    image_id: str
    key_pair_name: str
    key_path: str
    
    def get_zone(self, zone_id: str)-> ZoneInfo:
        return self.zones[zone_id]

@dataclass(frozen=True)
class InstanceType:
    name: str
    nodes: int
    
class CreateInstanceError(Enum):
    Nil = 0
    NoStock = 1
    NoInstanceType = 2
    Others = 99
    
@dataclass(frozen=True)
class Instance:
    instance_id: str
    zone_id: str
    type: InstanceType

@dataclass
class InstanceStatus:
    running_instances: Dict[str, str]
    pending_instances: Set[str]
    
@dataclass
class KeyPairRequestConfig:
    key_path: str
    key_pair_name: str
    
    def finger_print(self, provider: str):
        return get_fingerprint_from_key(self.key_path, provider)
        
    @property
    def public_key(self):
        return get_public_key_body(self.key_path)

@dataclass
class ImageInfo:
    image_id: str
    image_name: str
    
@dataclass
class KeyPairInfo:
    finger_print: str
    
    
@dataclass
class SecurityGroupInfo:
    security_group_id: str
    security_group_name: str
    
    
@dataclass
class VSwitchInfo:
    v_switch_id: str
    v_switch_name: str
    zone_id: str
    cidr_block: str
    status: str
    
    
@dataclass
class VpcInfo:
    vpc_id: str
    vpc_name: str