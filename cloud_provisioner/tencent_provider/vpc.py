from typing import List

from tencentcloud.vpc.v20170312 import models as vpc_models
from tencentcloud.vpc.v20170312.vpc_client import VpcClient
from loguru import logger
from tencentcloud.common.exception.tencent_cloud_sdk_exception import TencentCloudSDKException

from cloud_provisioner.create_instances.types import VpcInfo
from utils.wait_until import wait_until
from .v_switch import get_v_switchs_in_region
from .security_group import get_security_groups_in_region


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


def delete_vpc(client: VpcClient, vpc_id: str):
    def _retry_tencent(fn, *, op_name: str, retries: int = 10, sleep_seconds: float = 2.0):
        import time

        for idx in range(retries):
            try:
                fn()
                return
            except TencentCloudSDKException as exc:
                code = exc.code or ""
                if code not in {
                    "ResourceInUse",
                    "ResourceBusy",
                    "InvalidSubnet.NotFound",
                    "InvalidSecurityGroupId.NotFound",
                    "InvalidVpcID.NotFound",
                }:
                    raise
                if code in {"InvalidSubnet.NotFound", "InvalidSecurityGroupId.NotFound", "InvalidVpcID.NotFound"}:
                    return
                if idx == retries - 1:
                    raise
                logger.warning(f"{op_name} transient dependency error ({code}), retrying {idx + 1}/{retries}")
                time.sleep(sleep_seconds)

    v_switches = get_v_switchs_in_region(client, vpc_id)
    for v_switch in v_switches:
        logger.info(f"Deleting subnet {v_switch.v_switch_id} in vpc {vpc_id}")

        def _delete_subnet(subnet_id: str):
            req = vpc_models.DeleteSubnetRequest()
            req.SubnetId = subnet_id
            client.DeleteSubnet(req)

        _retry_tencent(
            lambda subnet_id=v_switch.v_switch_id: _delete_subnet(subnet_id),
            op_name=f"delete subnet {v_switch.v_switch_id}",
        )

    security_groups = get_security_groups_in_region(client, vpc_id)
    for security_group in security_groups:
        if security_group.security_group_name == "default":
            continue
        logger.info(f"Deleting security group {security_group.security_group_id} in vpc {vpc_id}")

        def _delete_security_group(sg_id: str):
            req = vpc_models.DeleteSecurityGroupRequest()
            req.SecurityGroupId = sg_id
            client.DeleteSecurityGroup(req)

        _retry_tencent(
            lambda sg_id=security_group.security_group_id: _delete_security_group(sg_id),
            op_name=f"delete security group {security_group.security_group_id}",
        )

    logger.info(f"Deleting VPC {vpc_id}")

    def _delete_vpc(vpc_id: str):
        req = vpc_models.DeleteVpcRequest()
        req.VpcId = vpc_id
        client.DeleteVpc(req)

    _retry_tencent(
        lambda vpc_id=vpc_id: _delete_vpc(vpc_id),
        op_name=f"delete vpc {vpc_id}",
    )
