from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
import time
from typing import Callable, Dict, List, Any
from alibabacloud_ecs20140526.models import DescribeInstancesRequest, DeleteInstancesRequest, DescribeInstancesResponseBodyInstancesInstance
from alibabacloud_ecs20140526.client import Client as EcsClient
from dotenv import load_dotenv
from loguru import logger

from cloud_provisioner.aliyun_provider.client_factory import AliyunClient
from cloud_provisioner.aws_provider.client_factory import AwsClient
from cloud_provisioner.create_instances.instance_config import DEFAULT_COMMON_TAG_KEY, DEFAULT_COMMON_TAG_VALUE, DEFAULT_USER_TAG_KEY
from mypy_boto3_ec2.client import EC2Client

ALI_REGIONS = [
    "ap-southeast-5",  # Indonesia
    "ap-southeast-3",  # Malaysia
    "ap-southeast-6",  # Philippines
    "ap-southeast-7",  # Thailand
    "ap-northeast-2",  # Korea
    "ap-southeast-1",  # Singapore
    "me-east-1",       # United Arab Emirates
]
AWS_REGIONS = [
    "us-west-2",   # Oregon
    "af-south-1",  # Cape Town
    "me-south-1",  # Bahrain
]


@dataclass
class InstanceInfo:
    instance_id: str
    instance_name: str
    tags: Dict[str, str]
    
    @classmethod
    def from_api(cls, rep: DescribeInstancesResponseBodyInstancesInstance):
        if rep.tags:
            tags = {tag.tag_key: tag.tag_value for tag in rep.tags.tag}
        else:
            tags = dict()
        return InstanceInfo(instance_id=rep.instance_id, instance_name=rep.instance_name, tags=tags)
    
def _get_ali_instances(c: EcsClient, region_id: str) -> List[InstanceInfo]:
    instances = []
    page_number = 1
    while True:
        rep = c.describe_instances(DescribeInstancesRequest(region_id=region_id, page_number=page_number, page_size=50))
        instances.extend([InstanceInfo.from_api(instance) for instance in rep.body.instances.instance])
        
        if rep.body.total_count <= page_number * 50:
            break
        page_number += 1
        
    return instances
    
def _delete_ali_instances(c: EcsClient, region_id: str, instances: List[InstanceInfo]):
    for i in range(0, len(instances), 100):
        instance_ids = [instance.instance_id for instance in instances[i:i+100]]
        while True: 
            try: 
                c.delete_instances(DeleteInstancesRequest(region_id = region_id, force_stop=True, force=True, instance_id=instance_ids))
                break
            except Exception as e:
                code = getattr(e, "code")
                if code == "IncorrectInstanceStatus.Initializing":
                    logger.warning(f"Some instances in region {region_id} is still initializing, waiting_retry")
            time.sleep(5)
    
    
def _delete_ali_in_region(region_id: str, factory: AliyunClient, predicate: Callable[[InstanceInfo], bool]):
    logger.info(f"Cleanup region {region_id}")
    client = factory.build(region_id)
    instances = _get_ali_instances(client, region_id)
    instances = list(filter(predicate, instances))
    if len(instances) > 0:
        logger.debug(f"{len(instances)} instances to stop: {instances}")
        _delete_ali_instances(client, region_id, instances)
    logger.success(f"Cleanup region {region_id} done")

def delete_ali_instances(factory: AliyunClient, predicate: Callable[[InstanceInfo], bool]):
    with ThreadPoolExecutor(max_workers=10) as executor:
        _ = list(executor.map(lambda region: _delete_ali_in_region(region, factory, predicate), ALI_REGIONS))
        

def _get_aws_instances(c: EC2Client) -> List[InstanceInfo]:
    instances: List[InstanceInfo] = []
    paginator = c.get_paginator('describe_instances')
    for page in paginator.paginate():
        for reservation in page.get('Reservations', []):
            for inst in reservation.get('Instances', []):
                tags = {t['Key']: t['Value'] for t in inst.get('Tags', [])} if inst.get('Tags') else {}
                instance_id = inst.get('InstanceId')
                instance_name = inst.get('InstanceId')
                instances.append(InstanceInfo(instance_id=instance_id, instance_name=instance_name, tags=tags))
    return instances


def _delete_aws_instances(c: EC2Client, region_id: str, instances: List[InstanceInfo]):
    for i in range(0, len(instances), 100):
        instance_ids = [instance.instance_id for instance in instances[i:i+100]]
        while True:
            try:
                c.terminate_instances(InstanceIds=instance_ids)
                break
            except Exception as e:
                logger.warning(f"Error terminating instances in {region_id}: {e}")
            time.sleep(5)


def _delete_aws_in_region(region_id: str, factory: AwsClient, predicate: Callable[[InstanceInfo], bool]):
    logger.info(f"Cleanup AWS region {region_id}")
    client = factory.build(region_id)
    instances = _get_aws_instances(client)
    instances = list(filter(predicate, instances))
    if len(instances) > 0:
        logger.debug(f"{len(instances)} instances to terminate: {instances}")
        _delete_aws_instances(client, region_id, instances)
    logger.success(f"Cleanup AWS region {region_id} done")


def delete_aws_instances(factory: AwsClient, predicate: Callable[[InstanceInfo], bool]):
    with ThreadPoolExecutor(max_workers=5) as executor:
        _ = list(executor.map(lambda region: _delete_aws_in_region(region, factory, predicate), AWS_REGIONS))


def check_tag(instance: InstanceInfo, user_prefix: str):
    return instance.tags.get(DEFAULT_COMMON_TAG_KEY) == DEFAULT_COMMON_TAG_VALUE and instance.tags.get(DEFAULT_USER_TAG_KEY, "").startswith(user_prefix)
    
        
if __name__ == "__main__":
    load_dotenv()
    aliyun_factory = AliyunClient.load_from_env()
    aws_factory = AwsClient.new()
    user_prefix = "lichenxing-alpha"

    with ThreadPoolExecutor(max_workers=2) as executor:
        _ = list(executor.map(
            lambda fn: fn(),
            [
                lambda: delete_ali_instances(aliyun_factory, lambda instance: check_tag(instance, user_prefix)),
                lambda: delete_aws_instances(aws_factory, lambda instance: check_tag(instance, user_prefix)),
            ],
        ))
        
