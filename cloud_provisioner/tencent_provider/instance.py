import time
import traceback
from typing import List

from loguru import logger
logger = logger.patch(lambda record: record.update(message=f"[Tencent] {record['message']}"))
from tencentcloud.cvm.v20170312 import models as cvm_models
from tencentcloud.cvm.v20170312.cvm_client import CvmClient
from tencentcloud.common.exception.tencent_cloud_sdk_exception import TencentCloudSDKException

from cloud_provisioner.cleanup_instances.types import InstanceInfoWithTag
from ..create_instances.types import InstanceStatus, RegionInfo, ZoneInfo, InstanceType
from ..create_instances.instance_config import InstanceConfig, DEFAULT_COMMON_TAG_KEY, DEFAULT_COMMON_TAG_VALUE


def _instance_tags(cfg: InstanceConfig) -> List[cvm_models.Tag]:
    common_tag = cvm_models.Tag()
    common_tag.Key = DEFAULT_COMMON_TAG_KEY
    common_tag.Value = DEFAULT_COMMON_TAG_VALUE

    user_tag = cvm_models.Tag()
    user_tag.Key = cfg.user_tag_key
    user_tag.Value = cfg.user_tag_value

    return [common_tag, user_tag]


def _get_key_id_by_name(client: CvmClient, key_pair_name: str) -> str:
    filter_name = cvm_models.Filter()
    filter_name.Name = "key-name"
    filter_name.Values = [key_pair_name]

    req = cvm_models.DescribeKeyPairsRequest()
    req.Filters = [filter_name]
    req.Limit = 100
    req.Offset = 0

    resp = client.DescribeKeyPairs(req)
    if not resp.KeyPairSet:
        raise Exception(f"Key pair not found: {key_pair_name}")
    return resp.KeyPairSet[0].KeyId


def as_instance_info_with_tag(rep: cvm_models.Instance) -> InstanceInfoWithTag:
    tags = {tag.Key: tag.Value for tag in rep.Tags} if rep.Tags else dict()
    assert isinstance(rep.InstanceId, str)
    instance_name = rep.InstanceName or ""
    return InstanceInfoWithTag(instance_id=rep.InstanceId, instance_name=instance_name, tags=tags)


def create_instances_in_zone(
    client: CvmClient,
    cfg: InstanceConfig,
    region_info: RegionInfo,
    zone_info: ZoneInfo,
    instance_type: InstanceType,
    amount: int,
    allow_partial_success: bool = False,
) -> list[str]:
    name = f"{cfg.instance_name_prefix}-{int(time.time())}"

    placement = cvm_models.Placement()
    placement.Zone = zone_info.id

    system_disk = cvm_models.SystemDisk()
    system_disk.DiskType = "CLOUD_HSSD"
    system_disk.DiskSize = cfg.disk_size

    vpc = cvm_models.VirtualPrivateCloud()
    vpc.VpcId = region_info.vpc_id
    vpc.SubnetId = zone_info.v_switch_id

    internet = cvm_models.InternetAccessible()
    internet.InternetChargeType = "TRAFFIC_POSTPAID_BY_HOUR"
    internet.InternetMaxBandwidthOut = cfg.internet_max_bandwidth_out
    internet.PublicIpAssigned = cfg.internet_max_bandwidth_out > 0

    key_id = _get_key_id_by_name(client, region_info.key_pair_name)
    login = cvm_models.LoginSettings()
    login.KeyIds = [key_id]

    tag_spec = cvm_models.TagSpecification()
    tag_spec.ResourceType = "instance"
    tag_spec.Tags = _instance_tags(cfg)

    req = cvm_models.RunInstancesRequest()
    req.InstanceChargeType = "POSTPAID_BY_HOUR"
    req.Placement = placement
    req.InstanceType = instance_type.name
    req.ImageId = region_info.image_id
    req.SystemDisk = system_disk
    req.VirtualPrivateCloud = vpc
    req.InternetAccessible = internet
    req.InstanceCount = amount
    req.InstanceName = name
    req.LoginSettings = login
    req.SecurityGroupIds = [region_info.security_group_id]
    req.TagSpecification = [tag_spec]

    if allow_partial_success:
        req.MinCount = 1

    try:
        resp = client.RunInstances(req)
        ids = resp.InstanceIdSet or []
        logger.success(f"Created instances at {region_info.id}/{zone_info.id}: instance_type={instance_type.name}, amount={len(ids)}, ids={ids}")
        return ids
    except TencentCloudSDKException as exc:
        e = traceback.format_exc()
        code = exc.code
        if code and any(key in code for key in ["ResourceInsufficient", "Insufficient", "NoStock"]):
            logger.warning(f"No stock for {region_info.id}/{zone_info.id}, instance_type={instance_type.name}, amount={amount}")
            return []
        logger.error(f"run_instances failed for {region_info.id}/{zone_info.id}: {exc}")
        logger.error(e)
        return []
    except Exception as exc:
        e = traceback.format_exc()
        logger.error(f"run_instances failed for {region_info.id}/{zone_info.id}: {exc}")
        logger.error(e)
        return []


def describe_instance_status(client: CvmClient, instance_ids: List[str]) -> InstanceStatus:
    running_instances = dict()
    pending_instances = set()

    for i in range(0, len(instance_ids), 100):
        query_chunk = instance_ids[i: i + 100]

        req = cvm_models.DescribeInstancesRequest()
        req.InstanceIds = query_chunk
        req.Limit = len(query_chunk)

        rep = client.DescribeInstances(req)
        instance_status = rep.InstanceSet or []

        for ins in instance_status:
            state = ins.InstanceState
            if state == "RUNNING":
                public_ip = None
                if ins.PublicIpAddresses:
                    public_ip = ins.PublicIpAddresses[0]
                if public_ip:
                    running_instances[ins.InstanceId] = public_ip
            elif state in {"PENDING", "STARTING", "STOPPING", "STOPPED", "REBOOTING", "LAUNCH_FAILED"}:
                pending_instances.add(ins.InstanceId)
        time.sleep(0.5)

    return InstanceStatus(running_instances=running_instances, pending_instances=pending_instances)


def get_instances_with_tag(client: CvmClient) -> List[InstanceInfoWithTag]:
    instances: List[InstanceInfoWithTag] = []
    offset = 0
    limit = 100

    while True:
        req = cvm_models.DescribeInstancesRequest()
        req.Offset = offset
        req.Limit = limit

        rep = client.DescribeInstances(req)
        if rep.InstanceSet:
            instances.extend([as_instance_info_with_tag(instance) for instance in rep.InstanceSet])

        if rep.TotalCount is None or rep.TotalCount <= offset + limit:
            break
        offset += limit

    return instances


def delete_instances(client: CvmClient, instances_ids: List[str]):
    for i in range(0, len(instances_ids), 100):
        chunks = instances_ids[i:i + 100]
        while True:
            try:
                req = cvm_models.TerminateInstancesRequest()
                req.InstanceIds = chunks
                client.TerminateInstances(req)
                break
            except TencentCloudSDKException as e:
                logger.error(f"Cannot delete: {e}")
            except Exception as e:
                logger.error(f"Cannot delete: {e}")
            # time.sleep(5)
