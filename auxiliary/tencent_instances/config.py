"""Tencent CVM configuration (runtime settings only)."""
from dataclasses import dataclass
from typing import Optional


DEFAULT_REGION = "ap-singapore"
DEFAULT_VPC = "conflux-image-builder"
DEFAULT_VPC_CIDR = "10.0.0.0/16"
DEFAULT_SUBNET_CIDR = "10.0.0.0/24"
DEFAULT_SECURITY_GROUP = "conflux-image-builder"
DEFAULT_KEYPAIR = "conflux_builder_key"
DEFAULT_SSH_KEY = "./keys/ssh-key.pem"
DEFAULT_COMMON_TAG_KEY = "conflux-massive-test"
DEFAULT_COMMON_TAG_VALUE = "true"
DEFAULT_USER_TAG_KEY = "user"
DEFAULT_USER_TAG_VALUE = "your_name"


@dataclass
class CvmRuntimeConfig:
    region_id: str = DEFAULT_REGION
    zone_id: Optional[str] = None
    vpc_name: str = DEFAULT_VPC
    vpc_cidr: str = DEFAULT_VPC_CIDR
    subnet_name: str = DEFAULT_VPC
    subnet_cidr: str = DEFAULT_SUBNET_CIDR
    security_group_name: str = DEFAULT_SECURITY_GROUP
    key_pair_name: str = DEFAULT_KEYPAIR
    ssh_username: str = "ubuntu"
    ssh_private_key_path: str = DEFAULT_SSH_KEY
    instance_name_prefix: str = "conflux-builder"
    internet_max_bandwidth_out: int = 1
    poll_interval: int = 5
    wait_timeout: int = 1800
    common_tag_key: str = DEFAULT_COMMON_TAG_KEY
    common_tag_value: str = DEFAULT_COMMON_TAG_VALUE
    user_tag_key: str = DEFAULT_USER_TAG_KEY
    user_tag_value: str = DEFAULT_USER_TAG_VALUE
