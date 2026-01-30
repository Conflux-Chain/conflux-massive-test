
import argparse
from concurrent.futures import ThreadPoolExecutor
from itertools import chain
import threading
import tomllib
import traceback

from dotenv import load_dotenv
from loguru import logger

from ..aliyun_provider.client_factory import AliyunClient
from ..aws_provider.client_factory import AwsClient
from ..host_spec import save_hosts
from ..provider_interface import IEcsClient

from .instance_config import InstanceConfig
from .instance_provisioner import create_instances_in_region
from .network_infra import InfraRequest
from .types import InstanceType
from .provision_config import CloudConfig, ProvisionConfig


def _ensure_network_infra(client: IEcsClient, cloud_config: CloudConfig, allow_create):
    request = InfraRequest.from_config(cloud_config, allow_create=allow_create)
    infra_provider = request.ensure_infras(client)
    logger.success(f"{cloud_config.provider} infra check pass")
    
    return infra_provider

def create_instances(client: IEcsClient, cloud_config: CloudConfig, barrier: threading.Barrier, allow_create: bool, infra_only: bool):
    try:
        infra_provider = _ensure_network_infra(client, cloud_config, allow_create)
        barrier.wait()
    except threading.BrokenBarrierError:
        logger.debug(f"{cloud_config.provider} quit due to other cloud providers fails")
        barrier.abort()
        return []
    except Exception as e:
        logger.error(f"Fail to build network infra: {e}")
        barrier.abort()
        print(traceback.format_exc())
        return []

    if infra_only:
        return []
    
    instance_config = InstanceConfig(user_tag_value=cloud_config.user_tag)
    instance_types = [InstanceType(i.name, i.nodes)
                      for i in cloud_config.instance_types]

    def _create_in_region(region_id: str, nodes: int):
        return create_instances_in_region(client, 
                                          instance_config, 
                                          region_info=infra_provider.get_region(region_id), 
                                          instance_types=instance_types, 
                                          nodes=nodes, 
                                          ssh_user=cloud_config.default_user_name, 
                                          provider=cloud_config.provider)

    with ThreadPoolExecutor(max_workers=10) as executor:
        results = list(executor.map(lambda reg: _create_in_region(
            reg.name, reg.count), cloud_config.regions))
        hosts = list(chain.from_iterable(results))

    return hosts


def make_parser():
    parser = argparse.ArgumentParser(description="运行区块链节点模拟")
    parser.add_argument(
        "-c", "--request-config",
        type=str,
        default=f"./request_config.toml",
        help="节点需求配置文件路径"
    )
    parser.add_argument(
        "-o", "--output-json",
        type=str,
        default=f"./hosts.json",
        help="输出的 hosts 文件路径"
    )
    parser.add_argument(
        "--allow-create",
        action="store_true",
        help="在 Network Infra 不存在时允许创建"
    )
    parser.add_argument(
        "--network-only",
        action="store_true",
        help="只进行 Network Infra 阶段，不创建实例"
    )
    return parser


if __name__ == "__main__":
    parser = make_parser()
    args = parser.parse_args()

    load_dotenv()

    with open("request_config.toml", "rb") as f:
        data = tomllib.load(f)
        config = ProvisionConfig(**data)

    cloud_tasks = []
    
    if config.aws.total_nodes > 0:
        aws_client = AwsClient.new()
        cloud_tasks.append((aws_client, config.aws))
    
    if config.aliyun.total_nodes > 0:
        ali_client = AliyunClient.load_from_env()
        cloud_tasks.append((ali_client, config.aliyun))
        
    barrier = threading.Barrier(len(cloud_tasks))
        
    with ThreadPoolExecutor() as executor:
        futures = [
            executor.submit(create_instances, client, cloud_config, barrier, args.allow_create, args.network_only)
            for client, cloud_config in cloud_tasks
        ]
        
        hosts = list(chain.from_iterable(future.result() for future in futures))
        
    save_hosts(hosts, args.output_json)
