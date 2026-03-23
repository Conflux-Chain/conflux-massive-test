from pydantic import BaseModel, Field
from typing import List, Optional

import tomllib


class ProvisionRegionConfig(BaseModel):
    name: str
    count: int = 0
    # 尝试在同一个 zone 申请全部节点的阈值，超出则跳过同一个 zone 申请，0 代表跳过 _try_create_in_single_zone 的逻辑
    zone_max_nodes: int = 20
    # 每次 API 调用请求的最大节点数量，0 代表没有限制
    max_nodes: int = 100

class CandidateInstanceType(BaseModel):
    name: str
    nodes: int


class EnterpriseNetworkConfig(BaseModel):
    enabled: bool = False
    name: Optional[str] = None
    description: Optional[str] = None
    peer_bandwidth_mbps: int = 5
    bandwidth_type: str = "DataTransfer"
    allocate_instance_private_ip: bool = True
    prefer_p2p_ip: bool = True

class CloudConfig(BaseModel):
    provider: str
    default_user_name: str
    user_tag: str
    image_name: str
    ssh_key_path: str
    # 专用于腾讯云，因为对 key_pair 有长度限制，其他云不生效
    key_pair_tag: Optional[str] = None
    regions: List[ProvisionRegionConfig] = []
    instance_types: List[CandidateInstanceType] = []
    enterprise_network: EnterpriseNetworkConfig = Field(default_factory=EnterpriseNetworkConfig)
    
    @property
    def total_nodes(self):
        return sum([region.count for region in self.regions])

    @property
    def active_regions(self) -> List[ProvisionRegionConfig]:
        return [region for region in self.regions if region.count > 0]
    
    def get_key_pair_tag(self) -> str:
        if self.key_pair_tag:
            return self.key_pair_tag
        else:
            return self.user_tag

    def get_enterprise_network_name(self) -> str:
        if self.enterprise_network.name:
            return self.enterprise_network.name
        return f"conflux-massive-test-{self.user_tag}-cen"

    def get_enterprise_network_description(self) -> str:
        if self.enterprise_network.description:
            return self.enterprise_network.description
        return f"Conflux massive test enterprise network for {self.user_tag}"

class ProvisionConfig(BaseModel):
    aliyun: CloudConfig
    aws: CloudConfig
    tencent: CloudConfig

if __name__=="__main__":
    with open("request_config.toml", "rb") as f:
        data = tomllib.load(f)
    print(ProvisionConfig(**data))