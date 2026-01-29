"""Aliyun ECS configuration and client helpers."""
from dataclasses import dataclass, field
import os
from typing import List, Optional

from alibabacloud_ecs20140526.client import Client as EcsClient
from alibabacloud_tea_openapi.models import Config as AliyunOpenApiConfig
from dotenv import load_dotenv

DEFAULT_REGION = "ap-southeast-3"
DEFAULT_KEYPAIR = "conflux-image-builder-ssh-key-2"
DEFAULT_SSH_KEY = "./keys/ssh-key.pem"
DEFAULT_VPC = "conflux-image-builder"
DEFAULT_VPC_CIDR = "10.0.0.0/16"
DEFAULT_VSWITCH_CIDR = "10.0.0.0/24"
DEFAULT_COMMON_TAG_KEY = "conflux-massive-test"
DEFAULT_COMMON_TAG_VALUE = "true"
DEFAULT_USER_TAG_KEY = "user"
DEFAULT_USER_TAG_VALUE = "your_name"
RUN_INSTANCES_MAX_AMOUNT = 100


@dataclass
class AliCredentials:
    access_key_id: str
    access_key_secret: str


@dataclass
class InstanceTypeConfig:
    name: str
    nodes: Optional[int] = None


@dataclass
class ZoneConfig:
    name: Optional[str] = None
    subnet: Optional[str] = None


@dataclass
class RegionConfig:
    name: str
    count: int = 0
    image: Optional[str] = None
    base_image_name: Optional[str] = None
    security_group_id: Optional[str] = None
    zones: List[ZoneConfig] = field(default_factory=list)
    type: Optional[List[InstanceTypeConfig]] = None


@dataclass
class AccountConfig:
    access_key_id: str = ""
    access_key_secret: str = ""
    user_tag: Optional[str] = None
    type: Optional[List[InstanceTypeConfig]] = None
    regions: List[RegionConfig] = field(default_factory=list)
    image: Optional[str] = None
    base_image_name: Optional[str] = None
    security_group_id: Optional[str] = None


@dataclass
class AliyunConfig:
    aliyun: List[AccountConfig] = field(default_factory=list)


@dataclass
class EcsRuntimeConfig:
    credentials: AliCredentials = field(default_factory=lambda: load_credentials())
    region_id: str = DEFAULT_REGION
    zone_id: Optional[str] = None
    image_id: Optional[str] = None
    instance_type: Optional[List[InstanceTypeConfig]] = None
    use_spot: bool = True
    spot_strategy: str = "SpotAsPriceGo"
    v_switch_id: Optional[str] = None
    security_group_id: Optional[str] = None
    vpc_name: str = DEFAULT_VPC
    vswitch_name: str = DEFAULT_VPC
    security_group_name: str = DEFAULT_VPC
    vpc_cidr: str = DEFAULT_VPC_CIDR
    vswitch_cidr: str = DEFAULT_VSWITCH_CIDR
    key_pair_name: str = DEFAULT_KEYPAIR
    ssh_username: str = "root"
    ssh_private_key_path: str = DEFAULT_SSH_KEY
    instance_name_prefix: str = "conflux-builder"
    internet_max_bandwidth_out: int = 100
    poll_interval: int = 5
    wait_timeout: int = 1800
    common_tag_key: str = DEFAULT_COMMON_TAG_KEY
    common_tag_value: str = DEFAULT_COMMON_TAG_VALUE
    user_tag_key: str = DEFAULT_USER_TAG_KEY
    user_tag_value: str = DEFAULT_USER_TAG_VALUE


def load_credentials() -> AliCredentials:
    load_dotenv()
    ak, sk = os.getenv("ALI_ACCESS_KEY_ID", "").strip(), os.getenv("ALI_ACCESS_KEY_SECRET", "").strip()
    if not ak or not sk:
        raise ValueError("Missing ALI_ACCESS_KEY_ID or ALI_ACCESS_KEY_SECRET")
    return AliCredentials(ak, sk)


def client(creds: AliCredentials, region: str) -> EcsClient:
    config = AliyunOpenApiConfig(
        access_key_id=creds.access_key_id,
        access_key_secret=creds.access_key_secret,
        region_id=region,
        read_timeout=120_000,
        connect_timeout=120_000,
    )
    return EcsClient(config)
