from concurrent.futures import ThreadPoolExecutor
from typing import Callable, List
from dotenv import load_dotenv
from loguru import logger
import argparse

from alibabacloud_ecs20140526.models import DescribeInstancesRequest

from cloud_provisioner.args_check import check_user_prefix_with_config_file, check_empty_user_prefix
from ..aliyun_provider.client_factory import AliyunClient
from ..aliyun_provider.instance import as_instance_info_with_tag
from ..aliyun_provider.eip import cleanup_user_public_network_artifacts
from ..aws_provider.client_factory import AwsClient
from ..tencent_provider.client_factory import TencentClient
from .types import InstanceInfoWithTag
from ..create_instances.instance_config import DEFAULT_COMMON_TAG_KEY, DEFAULT_COMMON_TAG_VALUE, DEFAULT_USER_TAG_KEY
from ..provider_interface import IEcsClient

ALIYUN_INSTANCE_LIST_PAGE_SIZE = 50
DELETE_LIST_BATCH_SIZE = 100

ALI_REGIONS = [
    "ap-southeast-5",  # Indonesia
    "ap-southeast-3",  # Malaysia
    "ap-southeast-6",  # Philippines
    "ap-southeast-7",  # Thailand
    "ap-northeast-2",  # Korea
    "ap-southeast-1",  # Singapore
    "me-east-1",       # United Arab Emirates
    "cn-hongkong",     # Hong Kong
]
AWS_REGIONS = [
    "us-west-2",   # Oregon
    "ap-east-1",   # Hong Kong
    "sa-east-1",   # São Paulo
    "af-south-1",  # Cape Town
    "me-south-1",  # Bahrain
]
TENCENT_REGIONS = [
    "ap-hongkong",
    "ap-singapore",  # Singapore
    "ap-bangkok",
    "ap-jakarta",    # Jakarta
    "me-saudi-arabia",  # Riyadh
    "ap-seoul",      # Seoul
    "sa-saopaulo",   # São Paulo
]
        

def _list_target_aliyun_instance_ids(
    client: AliyunClient,
    region_id: str,
    predicate: Callable[[InstanceInfoWithTag], bool],
    *,
    limit: int,
) -> list[str]:
    ecs_client = client.build(region_id)
    page_number = 1
    target_ids: list[str] = []

    while True:
        rep = ecs_client.describe_instances(
            DescribeInstancesRequest(
                region_id=region_id,
                page_number=page_number,
                page_size=ALIYUN_INSTANCE_LIST_PAGE_SIZE,
            )
        )
        instances = rep.body.instances.instance or []
        for raw_instance in instances:
            instance = as_instance_info_with_tag(raw_instance)
            if not predicate(instance):
                continue
            target_ids.append(instance.instance_id)
            if len(target_ids) >= limit:
                return target_ids

        total_count = rep.body.total_count or 0
        if total_count <= page_number * ALIYUN_INSTANCE_LIST_PAGE_SIZE:
            return target_ids
        page_number += 1


def _list_target_instance_ids(
    client: IEcsClient,
    region_id: str,
    predicate: Callable[[InstanceInfoWithTag], bool],
    *,
    limit: int,
) -> list[str]:
    if isinstance(client, AliyunClient):
        return _list_target_aliyun_instance_ids(
            client,
            region_id,
            predicate,
            limit=limit,
        )

    target_ids: list[str] = []
    for instance in client.get_instances_with_tag(region_id):
        if not predicate(instance):
            continue
        target_ids.append(instance.instance_id)
        if len(target_ids) >= limit:
            break
    return target_ids


def _delete_instance_batch(client: IEcsClient, region_id: str, instance_ids: List[str]):
    logger.info(f"Cleaning region {region_id}")
    if len(instance_ids) > 0:
        logger.debug(f"{len(instance_ids)} instances to terminate in region {region_id}: {instance_ids}")
        if isinstance(client, AliyunClient):
            client.delete_instances(region_id, instance_ids, release_public_network=False)
        else:
            client.delete_instances(region_id, instance_ids)


def _delete_in_region(client: IEcsClient, region_id: str, predicate: Callable[[InstanceInfoWithTag], bool]):
    issued_instance_ids: set[str] = set()
    total_deleted = 0

    while True:
        instance_ids = _list_target_instance_ids(
            client,
            region_id,
            lambda instance: instance.instance_id not in issued_instance_ids and predicate(instance),
            limit=DELETE_LIST_BATCH_SIZE,
        )
        if not instance_ids:
            logger.success(f"Cleanup region {region_id} done, delete_submitted={total_deleted}")
            return

        _delete_instance_batch(client, region_id, instance_ids)
        issued_instance_ids.update(instance_ids)
        total_deleted += len(instance_ids)


def _cleanup_aliyun_eips_in_region(client: AliyunClient, region_id: str, user_prefix: str):
    logger.info(f"Cleaning Aliyun EIPs in region {region_id}")
    released_eips = cleanup_user_public_network_artifacts(
        client.build_vpc(region_id),
        region_id,
        user_prefix,
    )
    if released_eips > 0:
        logger.info(
            f"Aliyun extra cleanup in {region_id}: released_eips={released_eips}"
        )
    logger.success(f"Aliyun EIP cleanup region {region_id} done")


def delete_instances(client: IEcsClient, regions: List[str], predicate: Callable[[InstanceInfoWithTag], bool]):
    max_workers = max(1, len(regions))
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        _ = list(executor.map(lambda region: _delete_in_region(client, region, predicate), regions))


def cleanup_aliyun_eips(client: AliyunClient, regions: List[str], user_prefix: str):
    with ThreadPoolExecutor(max_workers=5) as executor:
        _ = list(executor.map(lambda region: _cleanup_aliyun_eips_in_region(client, region, user_prefix), regions))


def check_tag(instance: InstanceInfoWithTag, user_prefix: str):
    return instance.tags.get(DEFAULT_COMMON_TAG_KEY) == DEFAULT_COMMON_TAG_VALUE and instance.tags.get(DEFAULT_USER_TAG_KEY, "").startswith(user_prefix)
    
    
if __name__ == "__main__":
    load_dotenv()

    parser = argparse.ArgumentParser(description="Cleanup instances by user prefix")
    parser.add_argument("-u", "--user-prefix", type=str, required=True, help="Prefix to match the user tag on instances")
    parser.add_argument("-c", "--config", type=str, default="request_config.toml", help="Configuration file to check if the user-prefix matches with it")
    parser.add_argument("--no-check", action="store_true", help="Skip check if the user-prefix matches configuration")
    parser.add_argument("-y", "--yes", action="store_true", help="Assume yes to confirmation prompt and proceed")
    args = parser.parse_args()
            
    from utils.logger import configure_logger
    configure_logger()


    if not args.no_check:
        check_user_prefix_with_config_file(args.config, args.user_prefix, args.yes)
        
    check_empty_user_prefix(args.user_prefix, args.yes, f"Empty --user-prefix will match ALL instances (filtered only by common tag: '{DEFAULT_COMMON_TAG_KEY}={DEFAULT_COMMON_TAG_VALUE}')!")

    aliyun_client = AliyunClient.load_from_env()
    aws_client = AwsClient.new()
    tencent_client = TencentClient.load_from_env()
    
    user_prefix = args.user_prefix

    predicate = lambda instance: check_tag(instance, user_prefix)

    with ThreadPoolExecutor() as executor:
        futures = [
            executor.submit(delete_instances, aliyun_client, ALI_REGIONS, predicate=predicate),
            executor.submit(cleanup_aliyun_eips, aliyun_client, ALI_REGIONS, user_prefix),
            # executor.submit(delete_instances, aws_client, AWS_REGIONS, predicate=predicate),
            # executor.submit(delete_instances, tencent_client, TENCENT_REGIONS, predicate=predicate),
        ]
        from concurrent.futures import wait

        wait(futures)
