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
    internet_max_bandwidth_out: int = 100

    # Spot instances (optional, used by Aliyun/AWS image build flows)
    use_spot: bool = False
    spot_strategy: str = "SpotAsPriceGo"