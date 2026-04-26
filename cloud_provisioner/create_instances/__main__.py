
import argparse
from concurrent.futures import ThreadPoolExecutor
from itertools import chain
import sys
import threading
import tomllib
import os

from dotenv import load_dotenv
from loguru import logger

from ..aliyun_provider.client_factory import AliyunClient
from ..aws_provider.client_factory import AwsClient
from ..tencent_provider.client_factory import TencentClient
from ..host_spec import save_hosts
from .cloud_workflow import create_instances
from .provision_config import ProvisionConfig


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
    parser.add_argument(
        "--no-backfill",
        action="store_false",
        dest="allow_backfill",
        help="禁用跨区域补足节点"
    )
    parser.add_argument(
        "-l", "--log-path",
        type=str,
        default=None,
        help="日志存储路径"
    )
    parser.set_defaults(allow_backfill=True)
    return parser


if __name__ == "__main__":    
    parser = make_parser()
    args = parser.parse_args()

    load_dotenv()

    from utils.logger import configure_logger
    configure_logger()

    if args.log_path:
        logger.add(f"{args.log_path}/provision.log", encoding="utf-8")

    with open(args.request_config, "rb") as f:
        data = tomllib.load(f)
        config = ProvisionConfig(**data)
        
    user_tag_prefix = os.getenv("USER_TAG_PREFIX", "")

    cloud_tasks = []
    
    if config.aws.total_nodes > 0:
        aws_client = AwsClient.new()
        cloud_tasks.append((aws_client, config.aws))
        if not config.aws.user_tag.startswith(user_tag_prefix):
            logger.error(f"AWS user tag {config.aws.user_tag} in config file does not match the prefix in environment variable USER_TAG_PREFIX='{user_tag_prefix}'")
            sys.exit(1)
     
    if config.aliyun.total_nodes > 0:
        ali_client = AliyunClient.load_from_env()
        cloud_tasks.append((ali_client, config.aliyun))
        if not config.aliyun.user_tag.startswith(user_tag_prefix):
            logger.error(f"Aliyun User tag {config.aliyun.user_tag} in config file does not match the prefix in environment variable USER_TAG_PREFIX='{user_tag_prefix}'")
            sys.exit(1)
    
    if not args.network_only:
        total_nodes = config.aws.total_nodes + config.aliyun.total_nodes + config.tencent.total_nodes
        logger.success(f"计划启动 {total_nodes} 个节点，aws {config.aws.total_nodes}, aliyun {config.aliyun.total_nodes}, tencent {config.tencent.total_nodes}")
        
    if config.tencent.total_nodes > 0:
        tencent_client = TencentClient.load_from_env()
        cloud_tasks.append((tencent_client, config.tencent))
        
    barrier = threading.Barrier(len(cloud_tasks))
        
    with ThreadPoolExecutor() as executor:
        futures = [
            executor.submit(create_instances, client, cloud_config, barrier, args.allow_create, args.network_only, args.allow_backfill)
            for client, cloud_config in cloud_tasks
        ]
        
        hosts = list(chain.from_iterable(future.result() for future in futures))
        
    if not args.network_only:
        save_hosts(hosts, args.output_json)
        logger.success(f"节点启动完成，节点信息已写入 {args.output_json}")
