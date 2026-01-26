"""Aliyun ECS configuration and client helpers."""
from dataclasses import dataclass, field
import os
from typing import Optional

from alibabacloud_ecs20140526.client import Client as EcsClient
from alibabacloud_tea_openapi.models import Config as AliyunConfig
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
class EcsConfig:
    credentials: AliCredentials = field(default_factory=lambda: load_credentials())
    region_id: str = DEFAULT_REGION
    zone_id: Optional[str] = None
    endpoint: Optional[str] = None
    base_image_id: Optional[str] = None
    image_id: Optional[str] = None
    instance_type: Optional[str] = None
    min_cpu_cores: int = 4
    min_memory_gb: float = 8.0
    max_memory_gb: float = 8.0
    cpu_vendor: Optional[str] = None
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
    conflux_git_ref: str = "v3.0.2"
    image_prefix: str = "conflux"
    instance_name_prefix: str = "conflux-builder"
    internet_max_bandwidth_out: int = 100
    search_all_regions: bool = False
    cleanup_builder_instance: bool = True
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


def load_endpoint() -> Optional[str]:
    return os.getenv("ALI_ECS_ENDPOINT", "").strip() or None


def client(creds: AliCredentials, region: str, endpoint: Optional[str] = None) -> EcsClient:
    if endpoint and "cloudcontrol" in endpoint:
        endpoint = f"ecs.{region}.aliyuncs.com"
    return EcsClient(
        AliyunConfig(
            access_key_id=creds.access_key_id,
            access_key_secret=creds.access_key_secret,
            region_id=region,
            endpoint=endpoint,
            read_timeout=120_000,
            connect_timeout=120_000
        )
    )
