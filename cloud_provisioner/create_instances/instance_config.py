from dataclasses import dataclass


DEFAULT_COMMON_TAG_KEY = "conflux-massive-test"
DEFAULT_COMMON_TAG_VALUE = "true"
DEFAULT_USER_TAG_KEY = "user"    
    
@dataclass
class InstanceConfig:
    user_tag_value: str
    user_tag_key: str = DEFAULT_USER_TAG_KEY
    
    instance_name_prefix: str = "conflux-massive-test"
    
    disk_size: int = 40
    # Aliyun system disk category. Use "cloud_essd" in production.
    # Set to "" to let cloud decide default supported category.
    disk_category: str = "cloud_essd"
    internet_max_bandwidth_out: int = 100

    # Aliyun spot instances (optional)
    use_spot: bool = False
    spot_strategy: str = "SpotAsPriceGo"