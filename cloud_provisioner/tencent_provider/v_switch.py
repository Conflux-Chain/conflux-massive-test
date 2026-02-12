from typing import List

from tencentcloud.vpc.v20170312 import models as vpc_models
from tencentcloud.vpc.v20170312.vpc_client import VpcClient

from ..create_instances.types import VSwitchInfo
from utils.wait_until import wait_until


def as_vswitch_info(rep: vpc_models.Subnet) -> VSwitchInfo:
    assert isinstance(rep.SubnetId, str)
    assert isinstance(rep.SubnetName, str)
    assert isinstance(rep.Zone, str)
    assert isinstance(rep.CidrBlock, str)
    return VSwitchInfo(
        v_switch_id=rep.SubnetId,
        v_switch_name=rep.SubnetName,
        zone_id=rep.Zone,
        status="Available",
        cidr_block=rep.CidrBlock,
    )


def get_v_switchs_in_region(client: VpcClient, vpc_id: str) -> List[VSwitchInfo]:
    result: List[VSwitchInfo] = []
    offset = 0
    limit = 100

    filter_vpc = vpc_models.Filter()
    filter_vpc.Name = "vpc-id"
    filter_vpc.Values = [vpc_id]

    while True:
        req = vpc_models.DescribeSubnetsRequest()
        req.Filters = [filter_vpc]
        req.Offset = str(offset)
        req.Limit = str(limit)

        resp = client.DescribeSubnets(req)
        if resp.SubnetSet:
            result.extend([as_vswitch_info(subnet) for subnet in resp.SubnetSet])

        if resp.TotalCount is None or resp.TotalCount <= offset + limit:
            break
        offset += limit

    return result


def create_v_switch(client: VpcClient, zone_id: str, vpc_id: str, v_switch_name: str, cidr_block: str):
    req = vpc_models.CreateSubnetRequest()
    req.VpcId = vpc_id
    req.SubnetName = v_switch_name
    req.CidrBlock = cidr_block
    req.Zone = zone_id

    rep = client.CreateSubnet(req)
    assert rep.Subnet is not None
    subnet_id = rep.Subnet.SubnetId
    assert isinstance(subnet_id, str)

    def _available() -> bool:
        describe = vpc_models.DescribeSubnetsRequest()
        describe.SubnetIds = [subnet_id]
        resp = client.DescribeSubnets(describe)
        return bool(resp.SubnetSet)

    wait_until(_available, timeout=120, retry_interval=3)

    return subnet_id
