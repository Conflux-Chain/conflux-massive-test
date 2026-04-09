from concurrent.futures import ThreadPoolExecutor
from typing import Callable, List
from dotenv import load_dotenv
from loguru import logger
import argparse

from cloud_provisioner.args_check import check_user_prefix_with_config_file, check_empty_user_prefix
from ..aliyun_provider.client_factory import AliyunClient
from ..aws_provider.client_factory import AwsClient
from ..tencent_provider.client_factory import TencentClient
from ..tencent_provider.eip import cleanup_user_public_network_artifacts as cleanup_tencent_public_network_artifacts
from .types import InstanceInfoWithTag
from ..create_instances.instance_config import DEFAULT_COMMON_TAG_KEY, DEFAULT_COMMON_TAG_VALUE, DEFAULT_USER_TAG_KEY
from ..provider_interface import IEcsClient

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
        

def _list_target_instance_ids(client: IEcsClient, region_id: str, predicate: Callable[[InstanceInfoWithTag], bool]) -> list[str]:
    instances = client.get_instances_with_tag(region_id)
    return [instance.instance_id for instance in instances if predicate(instance)]


def _delete_in_region(client: IEcsClient, region_id: str, instance_ids: List[str]):
    logger.info(f"Cleaning region {region_id}")
    if len(instance_ids) > 0:
        logger.debug(f"{len(instance_ids)} instances to terminate in region {region_id}: {instance_ids}")
        if isinstance(client, TencentClient):
            client.delete_instances(region_id, instance_ids, release_public_network=False)
        else:
            client.delete_instances(region_id, instance_ids)
    logger.success(f"Cleanup region {region_id} done")


def delete_instances(client: IEcsClient, regions: List[str], predicate: Callable[[InstanceInfoWithTag], bool]):
    region_targets = [
        (region, _list_target_instance_ids(client, region, predicate))
        for region in regions
    ]

    with ThreadPoolExecutor(max_workers=5) as executor:
        _ = list(executor.map(lambda item: _delete_in_region(client, item[0], item[1]), region_targets))


def _cleanup_tencent_eips_in_region(client: TencentClient, region_id: str, user_prefix: str):
    logger.info(f"Cleaning Tencent EIPs in region {region_id}")
    released_eips = cleanup_tencent_public_network_artifacts(
        client.build_vpc(region_id),
        user_prefix,
    )
    if released_eips > 0:
        logger.info(f"Tencent extra cleanup in {region_id}: released_eips={released_eips}")
    logger.success(f"Tencent EIP cleanup region {region_id} done")


def cleanup_tencent_eips(client: TencentClient, regions: List[str], user_prefix: str):
    with ThreadPoolExecutor(max_workers=5) as executor:
        _ = list(executor.map(lambda region: _cleanup_tencent_eips_in_region(client, region, user_prefix), regions))


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

    with ThreadPoolExecutor() as executor:
        predicate = lambda instance: check_tag(instance, user_prefix)
        futures = [
            executor.submit(delete_instances, aliyun_client, ALI_REGIONS, predicate=predicate),
            executor.submit(delete_instances, aws_client, AWS_REGIONS, predicate=predicate),
            executor.submit(delete_instances, tencent_client, TENCENT_REGIONS, predicate=predicate),
            executor.submit(cleanup_tencent_eips, tencent_client, TENCENT_REGIONS, user_prefix),
        ]
        from concurrent.futures import wait

        wait(futures)
        for future in futures:
            future.result()
        
        
