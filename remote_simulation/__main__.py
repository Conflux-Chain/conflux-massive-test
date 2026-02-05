#!/usr/bin/env python3
import argparse
import shutil

from dotenv import load_dotenv

from cloud_provisioner.host_spec import HostSpec, load_hosts

from .block_generator import generate_blocks_async
from .launch_conflux_node import launch_remote_nodes
from .network_connector import connect_nodes
from .network_topology import NetworkTopology
from .config_builder import SimulateOptions, ConfluxOptions, generate_config_file
from .tools import collect_logs, init_tx_gen, wait_for_nodes_synced


from loguru import logger

# from utils.tempfile import TempFile
# from aws_instances.launch_ec2_instances import Instances, LaunchConfig

import os
import datetime
from pathlib import Path

from utils.wait_until import WaitUntilTimeoutError

def generate_timestamp():
    """
    生成当前时间戳，格式为 YYYYMMDDHHMMSS
    例如: 20250102121314
    """
    now = datetime.datetime.now()
    # %Y: 年, %m: 月, %d: 日, %H: 时, %M: 分, %S: 秒
    timestamp = now.strftime("%Y%m%d%H%M%S")
    return timestamp

def make_parser():
    parser = argparse.ArgumentParser(description="运行区块链节点模拟")
    parser.add_argument(
        "-s", "--host-spec",
        type=str,
        default=f"./hosts.json",
        help="启动日志的路径"
    )
    parser.add_argument(
        "-l", "--log-path",
        type=str,
        default=f"logs/{generate_timestamp()}",
        help="日志存储路径 (默认: logs/YYYYMMDDHHMMSS)"
    )
    return parser


if __name__ == "__main__":
    load_dotenv()

    parser = make_parser()
    args = parser.parse_args()
    
    from utils.logger import configure_logger
    configure_logger()
    
    log_path = args.log_path

    Path(log_path).mkdir(parents=True, exist_ok=True)
    logger.add(f"{log_path}/remote_simulate.log", encoding="utf-8")
    
    # 1. 启动远程服务器
    # 从配置文件中读取已经启动好的服务器

    host_specs = load_hosts(args.host_spec)
    shutil.copy(args.host_spec, f"{log_path}/host.json")
    
    
    logger.info(f"实例列表集合 {[s.ip for s in host_specs]}")
    # ip_addresses: List[str] = instances.ip_addresses # pyright: ignore[reportAssignmentType]

    # 2. 生成配置
    num_target_nodes = sum([s.nodes_per_host for s in host_specs])
    connect_peers = min(7, num_target_nodes - 1)

    simulation_config = SimulateOptions(target_nodes=num_target_nodes, num_blocks=2000, connect_peers=connect_peers, target_tps=17000, storage_memory_gb=16, generation_period_ms=175)
    node_config = ConfluxOptions(send_tx_period_ms=200, tx_pool_size=2_000_000, target_block_gas_limit=120_000_000, max_block_size_in_bytes=450*1024, txgen_account_count = 500)
    assert node_config.txgen_account_count * simulation_config.target_nodes <= 100_000

    config_file = generate_config_file(simulation_config, node_config)

    logger.success(f"完成配置文件 {config_file.path}")
    shutil.copy(config_file.path, f"{log_path}/config.toml")

    # 3. 启动节点
    nodes = launch_remote_nodes(host_specs, config_file, pull_docker_image=True)
    if len(nodes) < simulation_config.target_nodes:
        raise Exception("Not all nodes started")
    logger.success("所有节点已启动，准备连接拓扑网络")

    # 4. 手动连接网络
    topology = NetworkTopology.generate_random_topology(len(nodes), simulation_config.connect_peers, latency_max = 0)
    for k, v in topology.peers.items():
        logger.debug(f"Node {nodes[k].id}({k}) has {len(v)} peers: {", ".join([str(i) for i in v])}")
    connect_nodes(nodes, topology, min_peers=simulation_config.connect_peers - 1)
    logger.success("拓扑网络构建完毕")
    wait_for_nodes_synced(nodes)

    # 5. 开始运行实验
    init_tx_gen(nodes, node_config.txgen_account_count)
    logger.success("开始运行区块链系统")
    generate_blocks_async(nodes, simulation_config.num_blocks, node_config.max_block_size_in_bytes, simulation_config.generation_period_ms, min_node_interval_ms=100)
    logger.info(f"Node goodput: {nodes[0].rpc.test_getGoodPut()}")
    try:
        wait_for_nodes_synced(nodes)
        logger.success("测试完毕，准备采集日志数据")
    except WaitUntilTimeoutError as e:
        logger.warning("部分节点没有完全同步，准备采集日志数据")
    
    # 6. 获取结果
    logger.info(f"Node goodput: {nodes[0].rpc.test_getGoodPut()}")
    
    nodes_log_path = f"{log_path}/nodes"
    Path(nodes_log_path).mkdir(parents=True, exist_ok=True)
    
    collect_logs(nodes, nodes_log_path)
    logger.info(f"日志收集完毕")
    logger.success(f"实验完毕，日志路径 {os.path.abspath(log_path)}")

    # shutil.copy(args.host_spec, f"{log_path}/servers.json")

    # stop_remote_nodes(ip_addresses)
    # destory_remote_nodes(ip_addresses)

