
from dataclasses import dataclass
from typing import List
from alibabacloud_ecs20140526.models import DescribeVSwitchesResponseBodyVSwitchesVSwitch, DescribeVSwitchesRequest, CreateVSwitchRequest
from alibabacloud_ecs20140526.client import Client as EcsClient

from ali_instances_v2.client_factory import ClientFactory

from .vpc import DEFAULT_VPC_CIDR
from utils.wait_until import wait_until


@dataclass
class VSwitchInfo:
    v_switch_id: str
    v_switch_name: str
    zone_id: str
    cidr_block: str
    status: str
    
    
    @classmethod
    def from_api_response(cls, rep: DescribeVSwitchesResponseBodyVSwitchesVSwitch):
        assert type(rep.v_switch_id) is str
        assert type(rep.v_switch_name) is str
        assert type(rep.zone_id) is str
        assert type(rep.status) is str
        assert type(rep.cidr_block) is str
        
        return VSwitchInfo(v_switch_id=rep.v_switch_id, v_switch_name=rep.v_switch_name, zone_id=rep.zone_id, status=rep.status, cidr_block=rep.cidr_block)
    


def get_v_switchs_in_region(c: ClientFactory, region_id: str, vpc_id: str) -> List[VSwitchInfo]:
    client = c.build(region_id)

    result = []
    
    page_number = 1
    while True:
        rep = client.describe_vswitches(DescribeVSwitchesRequest(region_id=region_id, vpc_id=vpc_id, page_number=page_number, page_size=50))
        result.extend([VSwitchInfo.from_api_response(v_switch) for v_switch in rep.body.v_switches.v_switch])
        if rep.body.total_count <= page_number * 50:
            break
        page_number += 1
        
    return result

def allocate_vacant_cidr_block(occupied_blocks: List[str], prefix: int = 24, vpc_cidr: str = DEFAULT_VPC_CIDR):
    import ipaddress
    
    # 将已占用的 CIDR 转换为网络对象集合
    occupied = {ipaddress.ip_network(block) for block in occupied_blocks if block}
    
    # 遍历 VPC CIDR 的所有指定前缀长度的子网
    for subnet in ipaddress.ip_network(vpc_cidr).subnets(new_prefix=prefix):
        # 检查该子网是否与所有已占用的块都不重叠
        if all(not subnet.overlaps(used) for used in occupied):
            return str(subnet)
    
    # 如果没有找到可用的子网
    raise RuntimeError(
        f"No available /{prefix} subnet found in {vpc_cidr}. "
        f"All subnets are occupied or overlapping."
    )



def create_v_switch(c: ClientFactory, region_id: str, zone_id: str, vpc_id: str, v_switch_name: str, cidr_block: str):
    client = c.build(region_id)
    
    rep = client.create_vswitch(CreateVSwitchRequest(region_id=region_id, vpc_id=vpc_id, zone_id=zone_id, v_switch_name=v_switch_name, cidr_block=cidr_block))
    v_switch_id = rep.body.v_switch_id
    
    assert type(v_switch_id) is str
    
    def _available():
        resp = client.describe_vswitches(DescribeVSwitchesRequest(region_id=region_id, v_switch_id=v_switch_id))
        v_switches = resp.body.v_switches.v_switch
        return len(v_switches) > 0 and v_switches[0].status == "Available"
    
    wait_until(_available, timeout=120, retry_interval=3)
    
    return v_switch_id
