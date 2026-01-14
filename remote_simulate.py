#!/usr/bin/env python3
import pickle
from typing import List

from remote_simulation.block_generator import generate_blocks_async
from remote_simulation.launch_conflux_node import launch_remote_nodes
from remote_simulation.network_connector import connect_nodes
from remote_simulation.network_topology import NetworkTopology
from remote_simulation.config_builder import SimulateOptions, ConfluxOptions, generate_config_file
from remote_simulation.tools import collect_logs, init_tx_gen, wait_for_nodes_synced


from loguru import logger

from utils.tempfile import TempFile
from aws_instances.launch_ec2_instances import Instances, LaunchConfig

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


if __name__ == "__main__":
    # 1. 启动远程服务器
    # 为了快速实验，从 pickle 文件中读取已经创建好的服务器

    with open("instances.pkl", "rb") as file:
        instances: Instances = pickle.load(file)
    
    logger.info(f"实例列表集合 {instances.ip_addresses}")
    ip_addresses: List[str] = instances.ip_addresses # pyright: ignore[reportAssignmentType]

    # 2. 生成配置
    nodes_per_host = 1

    simulation_config = SimulateOptions(target_nodes=len(ip_addresses) * nodes_per_host, nodes_per_host=nodes_per_host, num_blocks=1000, connect_peers=8, target_tps=17000, storage_memory_gb=16, generation_period_ms=175)
    assert simulation_config.target_nodes == simulation_config.nodes_per_host * len(ip_addresses)
    node_config = ConfluxOptions(send_tx_period_ms=200, tx_pool_size=2_000_000, target_block_gas_limit=120_000_000, max_block_size_in_bytes=450*1024, txgen_account_count = 500) # send_tx_period_ms=200,
    assert node_config.txgen_account_count * simulation_config.target_nodes <= 100_000

    config_file = generate_config_file(simulation_config, node_config)

    logger.success(f"完成配置文件 {config_file.path}")

    log_path = f"logs/{generate_timestamp()}"
    Path(log_path).mkdir(parents=True, exist_ok=True)

    # 3. 启动节点
    nodes = launch_remote_nodes(ip_addresses, simulation_config.nodes_per_host, config_file, pull_docker_image=True)
    if len(nodes) < simulation_config.target_nodes:
        raise Exception("Not all nodes started")
    logger.success("所有节点已启动，准备连接拓扑网络")

    # 4. 手动连接网络
    topology = NetworkTopology.generate_random_topology(len(nodes), simulation_config.connect_peers)
    for k, v in topology.peers.items():
        logger.debug(f"Node {nodes[k].id}({k}) has {len(v)} peers: {', '.join([str(i) for i in v])}")
    connect_nodes(nodes, topology, min_peers=7)
    logger.success("拓扑网络构建完毕")
    wait_for_nodes_synced(nodes)

    # 5. 开始运行实验
    init_tx_gen(nodes, node_config.txgen_account_count)
    logger.success("开始运行区块链系统")
    generate_blocks_async(nodes, simulation_config.num_blocks, node_config.max_block_size_in_bytes, simulation_config.generation_period_ms)
    logger.info(f"Node goodput: {nodes[0].rpc.test_getGoodPut()}")
    try:
        wait_for_nodes_synced(nodes)
        logger.success("测试完毕，准备采集日志数据")
    except WaitUntilTimeoutError as e:
        logger.warning("部分节点没有完全同步，准备采集日志数据")
    
    # 6. 获取结果
    logger.info(f"Node goodput: {nodes[0].rpc.test_getGoodPut()}")
    
    collect_logs(nodes, log_path)
    logger.success(f"日志收集完毕，路径 {os.path.abspath(log_path)}")

    # stop_remote_nodes(ip_addresses)
    # destory_remote_nodes(ip_addresses)

