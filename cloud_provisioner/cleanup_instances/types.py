from dataclasses import dataclass
from typing import Dict


@dataclass
class InstanceInfoWithTag:
    instance_id: str
    instance_name: str
    tags: Dict[str, str]