from dataclasses import dataclass
from typing import List
from alibabacloud_ecs20140526.models import DescribeSecurityGroupsRequest, DescribeSecurityGroupsResponseBodySecurityGroupsSecurityGroup, CreateSecurityGroupRequest, AuthorizeSecurityGroupRequest, AuthorizeSecurityGroupRequestPermissions
from alibabacloud_ecs20140526.client import Client as EcsClient

from ali_instances_v2.client_factory import ClientFactory


@dataclass
class SecurityGroupInfo:
    security_group_id: str
    security_group_name: str
    
    @classmethod
    def from_api_response(cls, rep: DescribeSecurityGroupsResponseBodySecurityGroupsSecurityGroup):
        assert type(rep.security_group_id) is str
        assert type(rep.security_group_name) is str
        
        return SecurityGroupInfo(security_group_id=rep.security_group_id, security_group_name=rep.security_group_name)

def get_security_groups_in_region(c: ClientFactory, region_id: str, vpc_id: str) -> List[SecurityGroupInfo]:
    client = c.build(region_id)

    result = []
    
    page_number = 1
    while True:
        rep = client.describe_security_groups(DescribeSecurityGroupsRequest(region_id=region_id, vpc_id=vpc_id, page_number=page_number, page_size=50))
        result.extend([SecurityGroupInfo.from_api_response(vpc) for vpc in rep.body.security_groups.security_group])
        if rep.body.total_count <= page_number * 50:
            break
        page_number += 1
    
    return result

def create_security_group(c: ClientFactory, region_id: str, vpc_id: str, security_group_name: str):
    client = c.build(region_id)
    rep = client.create_security_group(CreateSecurityGroupRequest(region_id=region_id, vpc_id=vpc_id, security_group_name=security_group_name, description="conflux"))
    
    security_group_id = rep.body.security_group_id
    assert type(security_group_id) is str
    
    client.authorize_security_group(AuthorizeSecurityGroupRequest(region_id=region_id, security_group_id=security_group_id, permissions=[
        AuthorizeSecurityGroupRequestPermissions(ip_protocol="tcp", port_range="22/22", source_cidr_ip="0.0.0.0/0"),
        AuthorizeSecurityGroupRequestPermissions(ip_protocol="tcp", port_range="1024/49151", source_cidr_ip="0.0.0.0/0"),
    ]))
    
    return security_group_id
    
    # TODO: 检查 security group
