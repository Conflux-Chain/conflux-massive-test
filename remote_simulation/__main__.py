#!/usr/bin/env python3
import argparse
import random
import shutil

from dotenv import load_dotenv

from cloud_provisioner.host_spec import HostSpec, load_hosts

from .block_generator import generate_blocks_async
from .launch_conflux_node import launch_remote_nodes
from .network_connector import connect_nodes
from .network_topology import NetworkTopology
from .pcap_capture import collect_pcap_artifacts, start_pcap_capture
from .config_builder import SimulateOptions, ConfluxOptions, generate_config_file
from .remote_node import RemoteNode
from .tools import init_tx_gen, wait_for_nodes_synced, collect_logs_v2
from .image_prepare import prepare_images_by_zone


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
    parser.add_argument(
        "-b", "--num-blocks",
        type=int,
        default=2000,
        help="实验区块数量"
    )
    parser.add_argument(
        "--capture-pcap",
        action="store_true",
        help="在实验期间抓取远程主机 Conflux 端口流量，并在收日志阶段回收压缩后的 pcap",
    )
    return parser


def _log_goodput(sample_node: RemoteNode | None) -> None:
    if sample_node is None:
        return

    try:
        logger.info(f"Node goodput: {sample_node.rpc.test_getGoodPut()}")
    except Exception as exc:
        logger.warning(f"无法读取观察节点 {sample_node.desc} 的 goodput: {exc}")


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
    shutil.copy(args.host_spec, f"{log_path}/hosts.json")
    
    
    logger.info(f"实例列表集合 {[s.ip for s in host_specs]}")
    # ip_addresses: List[str] = instances.ip_addresses # pyright: ignore[reportAssignmentType]

    # 2. 生成配置
    num_target_nodes = sum([s.nodes_per_host for s in host_specs])
    connect_peers = min(8, num_target_nodes - 1)

    simulation_config = SimulateOptions(target_nodes=num_target_nodes, num_blocks=args.num_blocks, connect_peers=connect_peers, target_tps=17000, storage_memory_gb=16, generation_period_ms=175)
    node_config = ConfluxOptions(send_tx_period_ms=200, tx_pool_size=2_000_000, target_block_gas_limit=120_000_000, max_block_size_in_bytes=450*1024, txgen_account_count = min(100, 100_000 // num_target_nodes), max_outgoing_peers = 10 * connect_peers)
    assert node_config.txgen_account_count * simulation_config.target_nodes <= 100_000

    config_file = generate_config_file(simulation_config, node_config)

    logger.success(f"完成配置文件 {config_file.path}")
    shutil.copy(config_file.path, f"{log_path}/config.toml")

    nodes: list[RemoteNode] = []
    sample_node: RemoteNode | None = None

    try:
        # 3. 启动节点
        logger.info("准备分区内镜像拉取 (dockerhub -> zone peers -> local registry)")
        prepare_images_by_zone(host_specs)

        nodes = launch_remote_nodes(host_specs, config_file, pull_docker_image=False, clear_environment=True)
        if not nodes:
            raise RuntimeError("没有任何节点成功启动，无法继续执行实验")

        if len(nodes) < simulation_config.target_nodes:
            logger.warning(f"启动了{len(nodes)}个节点，少于预期的{simulation_config.target_nodes}个节点")
            logger.warning("部分节点启动失败，继续进行测试")

        chosen_node = random.choice(nodes)
        sample_node = chosen_node
        logger.info(f"随机选择观察节点 {sample_node.host_spec.ip} 来自 {sample_node.host_spec.provider} {sample_node.host_spec.zone}")
        logger.success("所有节点已启动，准备连接拓扑网络")

        if args.capture_pcap:
            logger.info("开始在远程主机抓取 Conflux 端口流量")
            start_pcap_capture(host_specs)

        # 4. 手动连接网络
        topology = NetworkTopology.generate_random_topology(len(nodes), simulation_config.connect_peers, latency_max=0)
        for k, v in topology.peers.items():
            logger.debug(f"Node {nodes[k].id}({k}) has {len(v)} peers: {", ".join([str(i) for i in v])}")
        logger.success("拓扑网络方案构建完成")
        nodes = connect_nodes(nodes, topology, min_peers=simulation_config.connect_peers - 2, max_workers=1000)
        logger.success("拓扑网络构建完毕")
        try:
            wait_for_nodes_synced(nodes, max_workers=2000)
        except WaitUntilTimeoutError as exc:
            logger.warning(f"等待节点同步超时: {exc}")

        # 5. 开始运行实验
        init_tx_gen(nodes, node_config.txgen_account_count)
        logger.success("开始运行区块链系统")
        success_complete = False
        try:
            generate_blocks_async(
                nodes,
                simulation_config.num_blocks,
                node_config.max_block_size_in_bytes,
                simulation_config.generation_period_ms,
                min_node_interval_ms=100,
            )
            success_complete = True
        except Exception as exc:
            logger.warning(f"出块过程出现异常: {exc}")

        try:
            if success_complete:
                wait_for_nodes_synced(
                    nodes,
                    max_workers=2000,
                    timeout=300,
                    retry_interval=int(max(5, num_target_nodes / 250)),
                )
                logger.success("测试完毕，准备采集日志数据")
            else:
                logger.warning("测试中断，准备采集日志数据")
        except WaitUntilTimeoutError:
            logger.warning("部分节点没有完全同步，准备采集日志数据")

        _log_goodput(sample_node)
    finally:
        nodes_log_path = f"{log_path}/nodes"
        Path(nodes_log_path).mkdir(parents=True, exist_ok=True)

        if nodes:
            try:
                collect_logs_v2(nodes, nodes_log_path)
                logger.info("节点日志收集完毕")
            except Exception as exc:
                logger.warning(f"节点日志收集失败: {exc}")

        if args.capture_pcap:
            pcap_log_path = f"{log_path}/pcap"
            Path(pcap_log_path).mkdir(parents=True, exist_ok=True)
            try:
                collect_pcap_artifacts(host_specs, pcap_log_path)
                logger.info("pcap 收集完毕")
            except Exception as exc:
                logger.warning(f"pcap 收集失败: {exc}")

        logger.success(f"实验完毕，日志路径 {os.path.abspath(log_path)}")

    # shutil.copy(args.host_spec, f"{log_path}/servers.json")

    # stop_remote_nodes(ip_addresses)
    # destory_remote_nodes(ip_addresses)

