# pyright: reportOptionalOperand=false

from typing import List
from alibabacloud_ecs20140526.models import DescribeSecurityGroupsRequest, DescribeSecurityGroupsResponseBodySecurityGroupsSecurityGroup, CreateSecurityGroupRequest, AuthorizeSecurityGroupRequest, AuthorizeSecurityGroupRequestPermissions

from alibabacloud_ecs20140526.client import Client
from cloud_provisioner.create_instances.types import SecurityGroupInfo
    
def as_security_group_info(rep: DescribeSecurityGroupsResponseBodySecurityGroupsSecurityGroup):
    assert type(rep.security_group_id) is str
    assert type(rep.security_group_name) is str
    
    return SecurityGroupInfo(security_group_id=rep.security_group_id, security_group_name=rep.security_group_name)

def get_security_groups_in_region(client: Client, region_id: str, vpc_id: str) -> List[SecurityGroupInfo]:
    result = []
    
    page_number = 1
    while True:
        rep = client.describe_security_groups(DescribeSecurityGroupsRequest(region_id=region_id, vpc_id=vpc_id, page_number=page_number, page_size=50))
        result.extend([as_security_group_info(vpc) for vpc in rep.body.security_groups.security_group])
        if rep.body.total_count <= page_number * 50:
            break
        page_number += 1
    
    return result

def create_security_group(client: Client, region_id: str, vpc_id: str, security_group_name: str):
    rep = client.create_security_group(CreateSecurityGroupRequest(region_id=region_id, vpc_id=vpc_id, security_group_name=security_group_name))
    
    security_group_id = rep.body.security_group_id
    assert type(security_group_id) is str
    
    client.authorize_security_group(AuthorizeSecurityGroupRequest(region_id=region_id, security_group_id=security_group_id, permissions=[
        AuthorizeSecurityGroupRequestPermissions(ip_protocol="tcp", port_range="22/22", source_cidr_ip="0.0.0.0/0"),
        AuthorizeSecurityGroupRequestPermissions(ip_protocol="tcp", port_range="1024/49151", source_cidr_ip="0.0.0.0/0"),
    ]))
    
    return security_group_id
    
    # TODO: 检查 security group
