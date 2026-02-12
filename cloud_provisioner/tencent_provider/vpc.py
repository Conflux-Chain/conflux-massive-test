from typing import List

from tencentcloud.vpc.v20170312 import models as vpc_models
from tencentcloud.vpc.v20170312.vpc_client import VpcClient

from cloud_provisioner.create_instances.types import VpcInfo
from utils.wait_until import wait_until


def as_vpc_info(rep: vpc_models.Vpc) -> VpcInfo:
    assert isinstance(rep.VpcId, str)
    assert isinstance(rep.VpcName, str)
    return VpcInfo(vpc_id=rep.VpcId, vpc_name=rep.VpcName)


def get_vpcs_in_region(client: VpcClient) -> List[VpcInfo]:
    result: List[VpcInfo] = []
    offset = 0
    limit = 100

    while True:
        req = vpc_models.DescribeVpcsRequest()
        req.Offset = str(offset)
        req.Limit = str(limit)

        resp = client.DescribeVpcs(req)
        if resp.VpcSet:
            result.extend([as_vpc_info(vpc) for vpc in resp.VpcSet])

        if resp.TotalCount is None or resp.TotalCount <= offset + limit:
            break
        offset += limit

    return result


def create_vpc(client: VpcClient, vpc_name: str, cidr_block: str):
    req = vpc_models.CreateVpcRequest()
    req.VpcName = vpc_name
    req.CidrBlock = cidr_block

    resp = client.CreateVpc(req)
    assert resp.Vpc is not None
    vpc_id = resp.Vpc.VpcId
    assert isinstance(vpc_id, str)

    def _available() -> bool:
        describe = vpc_models.DescribeVpcsRequest()
        describe.VpcIds = [vpc_id]
        rep = client.DescribeVpcs(describe)
        return bool(rep.VpcSet)

    wait_until(_available, timeout=120, retry_interval=3)

    return vpc_id
