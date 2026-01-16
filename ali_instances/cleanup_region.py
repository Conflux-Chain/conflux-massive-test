import argparse
import json
import os
import time
from dataclasses import dataclass
from typing import Optional

from dotenv import load_dotenv
from loguru import logger
from alibabacloud_ecs20140526.client import Client as EcsClient
from alibabacloud_ecs20140526 import models as ecs_models
from alibabacloud_tea_openapi.models import Config as AliyunConfig


@dataclass(frozen=True)
class AliCredentials:
    access_key_id: str
    access_key_secret: str


def load_ali_credentials() -> AliCredentials:
    load_dotenv()
    access_key_id = os.getenv("ALI_ACCESS_KEY_ID", "").strip()
    access_key_secret = os.getenv("ALI_ACCESS_KEY_SECRET", "").strip()
    if not access_key_id or not access_key_secret:
        raise ValueError("Missing ALI_ACCESS_KEY_ID or ALI_ACCESS_KEY_SECRET in .env")
    return AliCredentials(access_key_id=access_key_id, access_key_secret=access_key_secret)


def normalize_endpoint(region_id: str, endpoint: Optional[str]) -> Optional[str]:
    if not endpoint:
        return None
    if "cloudcontrol.aliyuncs.com" in endpoint:
        return f"ecs.{region_id}.aliyuncs.com"
    return endpoint


def create_client(credentials: AliCredentials, region_id: str, endpoint: Optional[str]) -> EcsClient:
    endpoint = normalize_endpoint(region_id, endpoint)
    config = AliyunConfig(
        access_key_id=credentials.access_key_id,
        access_key_secret=credentials.access_key_secret,
        region_id=region_id,
        endpoint=endpoint,
    )
    return EcsClient(config)


def wait_instance_stopped(client: EcsClient, region_id: str, instance_id: str, timeout: int = 180) -> None:
    start_time = time.time()
    while True:
        request = ecs_models.DescribeInstancesRequest(region_id=region_id, instance_ids=json.dumps([instance_id]))
        response = client.describe_instances(request)
        instances = response.body.instances.instance if response.body and response.body.instances else []
        status = instances[0].status if instances else None
        if status == "Stopped":
            return
        if time.time() - start_time > timeout:
            raise TimeoutError(f"instance {instance_id} not stopped after {timeout}s")
        time.sleep(3)


def list_instances(client: EcsClient, region_id: str) -> list[str]:
    request = ecs_models.DescribeInstancesRequest(region_id=region_id, page_size=100)
    response = client.describe_instances(request)
    instances = response.body.instances.instance if response.body and response.body.instances else []
    return [instance.instance_id for instance in instances if instance.instance_id]


def release_instances(client: EcsClient, region_id: str) -> None:
    instance_ids = list_instances(client, region_id)
    logger.info(f"found {len(instance_ids)} instances in {region_id}")
    for instance_id in instance_ids:
        logger.info(f"releasing instance {instance_id}")
        # request = ecs_models.StopInstanceRequest(
        #     instance_id=instance_id,
        #     force_stop=True,
        #     stopped_mode="StopCharging",
        # )
        # try:
        #     client.stop_instance(request)
        #     wait_instance_stopped(client, region_id, instance_id)
        # except Exception as exc:
        #     logger.warning(f"stop instance failed: {exc}")
        delete_request = ecs_models.DeleteInstanceRequest(instance_id=instance_id, force=True, force_stop=True)
        client.delete_instance(delete_request)


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Release all ECS instances in a region")
    parser.add_argument("--region-id", required=True)
    parser.add_argument("--endpoint", default=None)
    return parser


def main() -> None:
    parser = build_arg_parser()
    args = parser.parse_args()
    credentials = load_ali_credentials()
    client = create_client(credentials, args.region_id, args.endpoint)
    release_instances(client, args.region_id)


if __name__ == "__main__":
    main()
