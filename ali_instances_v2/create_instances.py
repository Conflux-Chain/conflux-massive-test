
import argparse
from concurrent.futures import ThreadPoolExecutor
from itertools import chain
import tomllib

from dotenv import load_dotenv
from loguru import logger

from ali_instances_v2.client_factory import ClientFactory
from ali_instances_v2.create_instance_in_region import create_instances_in_region
from ali_instances_v2.infra_provider.build import InfraRequest
from .types import InstanceConfig, InstanceType
from request_config import AliyunRequestConfig, RequestConfig
from host_spec import save_hosts


def create_instances(config: AliyunRequestConfig, allow_create: bool, infra_only: bool, output_json: str):
    request = InfraRequest.from_aliyun_config(config, allow_create=allow_create)
    provider = request.ensure_infras(client_factory)
    logger.success("Aliyun infra check pass")

    if infra_only:
        return
    
    cfg = InstanceConfig(user_tag_value=config.user_tag)
    instance_types = [InstanceType(i.name, i.nodes)
                      for i in config.instance_types]

    def _create_in_region(region_id: str, nodes: int):
        client = client_factory.build(region_id)
        return create_instances_in_region(client, cfg, provider.get_region(region_id), instance_types, nodes)

    with ThreadPoolExecutor(max_workers=10) as executor:
        results = list(executor.map(lambda reg: _create_in_region(
            reg.name, reg.count), config.regions))
        hosts = list(chain.from_iterable(results))

    save_hosts(hosts, output_json)


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
        help="在 Infra 不存在时允许创建"
    )
    parser.add_argument(
        "--infra-only",
        action="store_true",
        help="只进行 Infra 阶段，不创建实例"
    )
    return parser


if __name__ == "__main__":
    parser = make_parser()
    args = parser.parse_args()

    load_dotenv()
    client_factory = ClientFactory.load_from_env()

    with open("request_config.toml", "rb") as f:
        data = tomllib.load(f)
        config = RequestConfig(**data).aliyun

    create_instances(config, args.allow_create,
                     args.infra_only, args.output_json)
