# pyright: reportOptionalOperand=false

from typing import List
from alibabacloud_ecs20140526.models import (
    DescribeVpcsResponseBodyVpcsVpc,
    DescribeVpcsRequest,
    CreateVpcRequest,
    DeleteVpcRequest,
    DeleteVSwitchRequest,
    DeleteSecurityGroupRequest,
)
from loguru import logger

from cloud_provisioner.create_instances.types import VpcInfo
from alibabacloud_ecs20140526.client import Client
from utils.wait_until import wait_until

from .v_switch import get_v_switchs_in_region
from .security_group import get_security_groups_in_region
    
def as_vpc_info(rep: DescribeVpcsResponseBodyVpcsVpc):
    assert type(rep.vpc_id) is str
    assert type(rep.vpc_name) is str
    assert type(rep.status) is str
    
    return VpcInfo(vpc_id=rep.vpc_id, vpc_name=rep.vpc_name)

def get_vpcs_in_region(client: Client, region_id: str) -> List[VpcInfo]:
    result = []
    
    page_number = 1
    while True:
        rep = client.describe_vpcs(DescribeVpcsRequest(region_id=region_id, page_number=page_number, page_size=50))
        result.extend([as_vpc_info(vpc) for vpc in rep.body.vpcs.vpc])
        if rep.body.total_count <= page_number * 50:
            break
        page_number += 1
    
    return result


def create_vpc(client: Client, region_id: str, vpc_name: str, cidr_block: str):
    rep = client.create_vpc(CreateVpcRequest(region_id=region_id, vpc_name=vpc_name, cidr_block=cidr_block))
    vpc_id = rep.body.vpc_id
    
    assert type(vpc_id) is str
    
    def _available() -> bool:
        resp = client.describe_vpcs(DescribeVpcsRequest(region_id=region_id, vpc_id=vpc_id))
        vpcs = resp.body.vpcs.vpc 
        return len(vpcs) > 0 and vpcs[0].status == "Available"
    
    wait_until(_available, timeout=120, retry_interval=3)
    
    return vpc_id


def delete_vpc(client: Client, region_id: str, vpc_id: str):
    v_switches = get_v_switchs_in_region(client, region_id, vpc_id)
    for v_switch in v_switches:
        logger.info(f"Deleting v-switch {v_switch.v_switch_id} in {region_id}")
        client.delete_vswitch(DeleteVSwitchRequest(region_id=region_id, v_switch_id=v_switch.v_switch_id))

    security_groups = get_security_groups_in_region(client, region_id, vpc_id)
    for security_group in security_groups:
        logger.info(f"Deleting security group {security_group.security_group_id} in {region_id}")
        client.delete_security_group(DeleteSecurityGroupRequest(region_id=region_id, security_group_id=security_group.security_group_id))

    logger.info(f"Deleting VPC {vpc_id} in {region_id}")
    client.delete_vpc(DeleteVpcRequest(region_id=region_id, vpc_id=vpc_id))
    