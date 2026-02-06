from concurrent.futures import ThreadPoolExecutor, wait
from typing import Callable, List, Tuple
import argparse
import sys
import tomllib

from dotenv import load_dotenv
from loguru import logger

from cloud_provisioner.create_instances.provision_config import ProvisionConfig
from cloud_provisioner.args_check import check_user_prefix_with_config_file, check_empty_user_prefix
from cloud_provisioner.create_instances.types import VpcInfo
from ..aliyun_provider.client_factory import AliyunClient
from ..aws_provider.client_factory import AwsClient
from ..provider_interface import IEcsClient


DEFAULT_INFRA_TAG_PREFIX = "conflux-massive-test-"

ALI_DEFAULT_REGIONS = [
    "ap-southeast-5",  # Indonesia
    "ap-southeast-3",  # Malaysia
    "ap-southeast-6",  # Philippines
    "ap-southeast-7",  # Thailand
    "ap-northeast-2",  # Korea
    "ap-southeast-1",  # Singapore
    "me-east-1",       # United Arab Emirates
]
AWS_DEFAULT_REGIONS = [
    "us-west-2",   # Oregon
    "af-south-1",  # Cape Town
    "me-south-1",  # Bahrain
]


def _load_regions_from_config(config_file: str) -> Tuple[List[str], List[str]]:
    if config_file == "":
        return ALI_DEFAULT_REGIONS, AWS_DEFAULT_REGIONS
    with open(config_file, "rb") as f:
        data = tomllib.load(f)
        config = ProvisionConfig(**data)
    aliyun_regions = [region.name for region in config.aliyun.regions]
    aws_regions = [region.name for region in config.aws.regions]

    return aliyun_regions, aws_regions


def _delete_vpcs_in_region(
    client: IEcsClient,
    region_id: str,
    predicate: Callable[[VpcInfo], bool],
):
    logger.info(f"Cleanup VPCs in region {region_id}")
    vpcs = list(filter(predicate, client.get_vpcs_in_region(region_id)))
    if len(vpcs) == 0:
        logger.info(f"No VPCs to delete in {region_id}")
        return

    for vpc in vpcs:
        logger.debug(f"Deleting VPC {vpc.vpc_name} ({vpc.vpc_id}) in {region_id}")
        try:
            client.delete_vpc(region_id, vpc.vpc_id)
            logger.success(f"Deleted VPC {vpc.vpc_name} ({vpc.vpc_id}) in {region_id}")
        except Exception as exc:
            logger.exception(f"Failed to delete VPC {vpc.vpc_name} ({vpc.vpc_id}) in {region_id}: {exc}")

            # Try to collect more info about resources attached to this VPC to aid debugging
            try:
                v_switches = client.get_v_switchs_in_region(region_id, vpc.vpc_id)
                logger.debug(f"VSwitches in VPC {vpc.vpc_id}: {v_switches}")
            except Exception:
                logger.debug(f"Unable to list v-switches for VPC {vpc.vpc_id} in {region_id}")

            try:
                sgs = client.get_security_groups_in_region(region_id, vpc.vpc_id)
                logger.debug(f"Security groups in VPC {vpc.vpc_id}: {sgs}")
            except Exception:
                logger.debug(f"Unable to list security groups for VPC {vpc.vpc_id} in {region_id}")

            # Continue with next VPC
            continue
    logger.success(f"Cleanup VPCs in region {region_id} done")


def delete_vpcs(client: IEcsClient, regions: List[str], predicate: Callable[[VpcInfo], bool]):
    with ThreadPoolExecutor(max_workers=5) as executor:
        _ = list(executor.map(lambda region: _delete_vpcs_in_region(client, region, predicate), regions))
    

if __name__ == "__main__":
    load_dotenv()

    parser = argparse.ArgumentParser(description="Cleanup network infra (VPC) by user prefix")
    parser.add_argument("-u", "--user-prefix", type=str, required=True, help="Prefix to match the user tag on infra names")
    parser.add_argument("-c", "--config", type=str, default="request_config.toml", help="Configuration file path to specify the regions to delete. Use empty string to fallback to all regions.")
    parser.add_argument("--no-check", action="store_true", help="Skip check if the user-prefix matches configuration")
    parser.add_argument("-y", "--yes", action="store_true", help="Assume yes to confirmation prompt and proceed")
    args = parser.parse_args()

    vpc_name_prefix = f"{DEFAULT_INFRA_TAG_PREFIX}{args.user_prefix}"

    if not args.no_check:
        check_user_prefix_with_config_file(args.config, args.user_prefix, args.yes)
    check_empty_user_prefix(args.user_prefix, args.yes, f"Empty --user-prefix will match ALL VPCs with name prefix '{vpc_name_prefix}')!")

    aliyun_regions, aws_regions = _load_regions_from_config(args.config)

    aliyun_client = AliyunClient.load_from_env()
    aws_client = AwsClient.new()

    predicate = lambda vpc: vpc.vpc_name.startswith(vpc_name_prefix)

    with ThreadPoolExecutor() as executor:
        futures = [
            executor.submit(delete_vpcs, aliyun_client, aliyun_regions, predicate=predicate),
            executor.submit(delete_vpcs, aws_client, aws_regions, predicate=predicate),
        ]
        wait(futures)
