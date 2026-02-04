from pydantic import BaseModel
from typing import List

import tomllib


class Region(BaseModel):
    name: str
    count: int = 0

class CandidateInstanceType(BaseModel):
    name: str
    nodes: int

class CloudConfig(BaseModel):
    provider: str
    default_user_name: str
    user_tag: str
    image_name: str
    ssh_key_path: str
    regions: List[Region] = []
    instance_types: List[CandidateInstanceType] = []
    
    @property
    def total_nodes(self):
        return sum([region.count for region in self.regions])

class ProvisionConfig(BaseModel):
    aliyun: CloudConfig
    aws: CloudConfig
    tencent: CloudConfig

if __name__=="__main__":
    with open("request_config.toml", "rb") as f:
        data = tomllib.load(f)
    print(ProvisionConfig(**data))