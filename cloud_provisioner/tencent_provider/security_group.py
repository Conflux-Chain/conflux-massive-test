from typing import List

from tencentcloud.vpc.v20170312 import models as vpc_models
from tencentcloud.vpc.v20170312.vpc_client import VpcClient

from cloud_provisioner.create_instances.types import SecurityGroupInfo


def as_security_group_info(rep: vpc_models.SecurityGroup) -> SecurityGroupInfo:
    assert isinstance(rep.SecurityGroupId, str)
    assert isinstance(rep.SecurityGroupName, str)
    return SecurityGroupInfo(security_group_id=rep.SecurityGroupId, security_group_name=rep.SecurityGroupName)


def get_security_groups_in_region(client: VpcClient, vpc_id: str) -> List[SecurityGroupInfo]:
    result: List[SecurityGroupInfo] = []
    offset = 0
    limit = 100

    while True:
        req = vpc_models.DescribeSecurityGroupsRequest()
        req.Offset = str(offset)
        req.Limit = str(limit)
        resp = client.DescribeSecurityGroups(req)

        if resp.SecurityGroupSet:
            result.extend([as_security_group_info(sg) for sg in resp.SecurityGroupSet])

        if resp.TotalCount is None or resp.TotalCount <= offset + limit:
            break
        offset += limit

    return result


def _allow_ingress_policy(port_range: str) -> vpc_models.SecurityGroupPolicy:
    policy = vpc_models.SecurityGroupPolicy()
    policy.Protocol = "TCP"
    policy.Port = port_range
    policy.CidrBlock = "0.0.0.0/0"
    policy.Action = "ACCEPT"
    return policy


def _allow_all_egress() -> vpc_models.SecurityGroupPolicy:
    policy = vpc_models.SecurityGroupPolicy()
    policy.Protocol = "ALL"
    policy.Port = "all"
    policy.CidrBlock = "0.0.0.0/0"
    policy.Action = "ACCEPT"
    return policy


def create_security_group(client: VpcClient, vpc_id: str, security_group_name: str) -> str:
    req = vpc_models.CreateSecurityGroupRequest()
    req.GroupName = security_group_name
    req.GroupDescription = "conflux"

    rep = client.CreateSecurityGroup(req)
    assert rep.SecurityGroup is not None
    security_group_id = rep.SecurityGroup.SecurityGroupId
    assert isinstance(security_group_id, str)

    policy_set = vpc_models.SecurityGroupPolicySet()
    policy_set.Ingress = [
        _allow_ingress_policy("22"),
        _allow_ingress_policy("1024-49151"),
    ]
    policy_set.Egress = [_allow_all_egress()]

    policy_req = vpc_models.CreateSecurityGroupPoliciesRequest()
    policy_req.SecurityGroupId = security_group_id
    policy_req.SecurityGroupPolicySet = policy_set
    client.CreateSecurityGroupPolicies(policy_req)

    return security_group_id
