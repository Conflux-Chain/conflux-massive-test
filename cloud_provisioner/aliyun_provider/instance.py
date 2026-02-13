# pyright: reportOptionalOperand=false

import json
import time
import traceback
from typing import List, Tuple

from alibabacloud_ecs20140526.models import DescribeInstancesRequest, RunInstancesRequestTag, RunInstancesRequestSystemDisk, RunInstancesRequest, DescribeInstancesResponseBodyInstancesInstance, DeleteInstancesRequest
from loguru import logger

from cloud_provisioner.cleanup_instances.types import InstanceInfoWithTag

from ..create_instances.types import InstanceStatus, RegionInfo, ZoneInfo, InstanceType, CreateInstanceError
from ..create_instances.instance_config import InstanceConfig, DEFAULT_COMMON_TAG_KEY, DEFAULT_COMMON_TAG_VALUE
from alibabacloud_ecs20140526.client import Client
    
MAX_CREATE_PER_REQUEST = 100


def _instance_tags(cfg: InstanceConfig) -> List[RunInstancesRequestTag]:
    return [
        RunInstancesRequestTag(key=DEFAULT_COMMON_TAG_KEY, value=DEFAULT_COMMON_TAG_VALUE),
        RunInstancesRequestTag(key=cfg.user_tag_key, value=cfg.user_tag_value)
    ]
    
def as_instance_info_with_tag(rep: DescribeInstancesResponseBodyInstancesInstance):
    if rep.tags:
        tags = {tag.tag_key: tag.tag_value for tag in rep.tags.tag}
    else:
        tags = dict()
    return InstanceInfoWithTag(instance_id=rep.instance_id, instance_name=rep.instance_name, tags=tags) # pyright: ignore[reportArgumentType]
    

def create_instances_in_zone(
    client: Client,
    cfg: InstanceConfig,
    region_info: RegionInfo,
    zone_info: ZoneInfo,
    instance_type: InstanceType,
    max_amount: int,
    min_amount: int,
) -> Tuple[list[str], CreateInstanceError]:
    def _create_single(single_max: int, single_min: int) -> Tuple[list[str], CreateInstanceError]:
        disk = RunInstancesRequestSystemDisk(category="cloud_essd", size=str(cfg.disk_size))
        name = f"{cfg.instance_name_prefix}-{int(time.time())}"

        req = RunInstancesRequest(
            region_id=region_info.id,
            zone_id=zone_info.id,
            image_id=region_info.image_id,
            instance_type=instance_type.name,
            security_group_id=region_info.security_group_id,
            v_switch_id=zone_info.v_switch_id,
            key_pair_name=region_info.key_pair_name,
            instance_name=name,
            internet_max_bandwidth_out=cfg.internet_max_bandwidth_out,
            internet_charge_type="PayByTraffic",
            instance_charge_type="PostPaid",
            tag=_instance_tags(cfg),
            amount=single_max,
            min_amount=single_min,
            system_disk=disk,
        )

        try:
            resp = client.run_instances(req)
            ids = resp.body.instance_id_sets.instance_id_set
            assert ids is not None
            logger.success(f"Create instances at {region_info.id}/{zone_info.id}: instance_type={instance_type.name}, amount={len(ids)}, ids={ids}")
            return ids, CreateInstanceError.Nil
        except Exception as exc:
            code = getattr(exc, "code", None)
            error_type = CreateInstanceError.Others

            if code == "OperationDenied.NoStock":
                error_type = CreateInstanceError.NoStock
                logger.warning(f"No stock for {region_info.id}/{zone_info.id}, instance_type={instance_type.name}, amount={req.min_amount}~{req.amount}")

            elif code == "InvalidResourceType.NotSupported":
                error_type = CreateInstanceError.NoInstanceType
                logger.warning(f"Request not supported in {region_info.id}/{zone_info.id}, trying other zones... instance_type={instance_type.name}, amount={req.min_amount}~{req.amount}")

            else:
                logger.error(f"run_instances failed for {region_info.id}/{zone_info.id}: {exc}")
                logger.error(traceback.format_exc())

            return [], error_type

    if max_amount <= 0:
        return [], CreateInstanceError.Nil

    all_ids: list[str] = []
    while len(all_ids) < max_amount:
        remaining = max_amount - len(all_ids)
        chunk_max = min(MAX_CREATE_PER_REQUEST, remaining)
        remaining_min = max(min_amount - len(all_ids), 0)
        chunk_min = min(remaining_min, chunk_max)
        if chunk_min <= 0:
            chunk_min = 1

        ids, err = _create_single(chunk_max, chunk_min)
        if ids:
            all_ids.extend(ids)
            continue

        if not all_ids:
            return [], err

        logger.warning(f"Stop creating more instances after partial success: requested={max_amount}, actual={len(all_ids)}")
        break

    if len(all_ids) < max_amount:
        logger.warning(f"Partially created instances at {region_info.id}/{zone_info.id}: requested={max_amount}, actual={len(all_ids)}")
    return all_ids, CreateInstanceError.Nil
    
def describe_instance_status(client: Client, region_id: str, instance_ids: List[str]):
    running_instances = dict()
    pending_instances = set()
    
    for i in range(0, len(instance_ids), 100):
        query_chunk = instance_ids[i: i+100]
        
        rep = client.describe_instances(DescribeInstancesRequest(
            region_id=region_id, page_size=100, instance_ids=json.dumps(query_chunk)))
        instance_status = rep.body.instances.instance

        for instance in instance_status:
            if instance.status not in ["Running"]:
                continue
            public_ip = instance.public_ip_address.ip_address[0]
            private_ip = instance.vpc_attributes.private_ip_address.ip_address[0] or instance.inner_ip_address.ip_address[0]
            running_instances[instance.instance_id] = (public_ip, private_ip)
        
        # 阿里云启动阶段也可能读到 instance 是 stopped 的状态
        pending_instances.update({i.instance_id for i in instance_status if i.status in [
                                 "Starting", "Pending", "Stopped"]})
        time.sleep(0.5)
    return InstanceStatus(running_instances=running_instances, pending_instances=pending_instances)


def get_instances_with_tag(client: Client, region_id: str) -> List[InstanceInfoWithTag]:
    instances = []
    page_number = 1
    
    while True:
        rep = client.describe_instances(DescribeInstancesRequest(region_id=region_id, page_number=page_number, page_size=50))
        instances.extend([as_instance_info_with_tag(instance) for instance in rep.body.instances.instance])
        
        if rep.body.total_count <= page_number * 50:
            break
        page_number += 1
        
    return instances

def delete_instances(client: Client, region_id: str, instances_ids: List[str]):
    for i in range(0, len(instances_ids), 100):
        chunks = instances_ids[i:i+100]
        while True: 
            try: 
                client.delete_instances(DeleteInstancesRequest(region_id = region_id, force_stop=True, force=True, instance_id=chunks))
                break
            except Exception as e:
                code = getattr(e, "code", None)
                if code == "IncorrectInstanceStatus.Initializing":
                    logger.warning(f"Some instances in region {region_id} is still initializing, waiting_retry")
            time.sleep(5)