#!/usr/bin/env python3
"""Run a Conflux simulation on Aliyun.

This script provisions hosts, launches nodes, runs the experiment, and collects logs.
Configuration is read from instance-region.json.
"""
import datetime
import json
import os
from pathlib import Path
from typing import List

from loguru import logger

from remote_simulation.launch_conflux_node import HostSpec
from remote_simulation.block_generator import generate_blocks_async
from remote_simulation.config_builder import ConfluxOptions, SimulateOptions, generate_config_file
from remote_simulation.launch_conflux_node import launch_remote_nodes
from remote_simulation.network_connector import connect_nodes
from remote_simulation.network_topology import NetworkTopology
from remote_simulation.tools import collect_logs, init_tx_gen, wait_for_nodes_synced
from utils.wait_until import WaitUntilTimeoutError
from ali_instances.create_servers import load_host_specs, generate_timestamp


if __name__ == "__main__":
    root = Path(__file__).resolve().parent
    servers_json = root / "ali_servers.json"
    if not servers_json.exists():
        raise FileNotFoundError(f"missing {servers_json}")

    raw = json.loads(servers_json.read_text())
    hosts = load_host_specs(raw)
    if not hosts:
        raise RuntimeError("no hosts found in ali_servers.json")

    total_nodes = sum(h.nodes_per_host for h in hosts)
    max_nodes_per_host = max(h.nodes_per_host for h in hosts)
    if total_nodes <= 0:
        raise RuntimeError("no nodes scheduled to start")

    simulation_config = SimulateOptions(
        target_nodes=total_nodes,
        nodes_per_host=max_nodes_per_host,
        num_blocks=1000,
        connect_peers=8,
        target_tps=17000,
        storage_memory_gb=16,
        generation_period_ms=175,
    )
    node_config = ConfluxOptions(
        send_tx_period_ms=200,
        tx_pool_size=2_000_000,
        target_block_gas_limit=120_000_000,
        max_block_size_in_bytes=450 * 1024,
        txgen_account_count=500,
    )
    assert node_config.txgen_account_count * simulation_config.target_nodes <= 100_000

    config_file = generate_config_file(simulation_config, node_config)
    logger.success(f"完成配置文件 {config_file.path}")

    log_path = raw.get("log_dir") if isinstance(raw, dict) else None
    if not log_path:
        log_path = f"logs/{generate_timestamp()}"
    Path(log_path).mkdir(parents=True, exist_ok=True)

    nodes = launch_remote_nodes(hosts, config_file, pull_docker_image=True)
    if len(nodes) < simulation_config.target_nodes:
        raise RuntimeError("Not all nodes started")
    logger.success("所有节点已启动，准备连接拓扑网络")

    topology = NetworkTopology.generate_random_topology(len(nodes), simulation_config.connect_peers)
    for k, v in topology.peers.items():
        peer_list = ", ".join([str(i) for i in v])
        logger.debug(f"Node {nodes[k].id}({k}) has {len(v)} peers: {peer_list}")
    min_peers = min(simulation_config.connect_peers, max(1, len(nodes) - 1))
    connect_nodes(nodes, topology, min_peers=min_peers)
    logger.success("拓扑网络构建完毕")
    wait_for_nodes_synced(nodes)

    init_tx_gen(nodes, node_config.txgen_account_count)
    logger.success("开始运行区块链系统")
    generate_blocks_async(
        nodes,
        simulation_config.num_blocks,
        node_config.max_block_size_in_bytes,
        simulation_config.generation_period_ms,
        min_node_interval_ms=10,
        max_failures=3,
    )
    logger.info(f"Node goodput: {nodes[0].rpc.test_getGoodPut()}")

    try:
        wait_for_nodes_synced(nodes)
        logger.success("测试完毕，准备采集日志数据")
    except WaitUntilTimeoutError:
        logger.warning("部分节点没有完全同步，准备采集日志数据")

    logger.info(f"Node goodput: {nodes[0].rpc.test_getGoodPut()}")
    collect_logs(nodes, log_path)
    logger.success(f"日志收集完毕，路径 {os.path.abspath(log_path)}")

