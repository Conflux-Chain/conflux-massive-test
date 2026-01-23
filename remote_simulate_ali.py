#!/usr/bin/env python3
"""Run a Conflux simulation on Aliyun.

This script provisions hosts, launches nodes, runs the experiment, and collects logs.
Configuration is read from instance-region.json.
"""
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor
from itertools import chain
from pathlib import Path
from typing import List

import requests
from loguru import logger

from ali_instances.host_spec import HostSpec
from remote_simulation import docker_cmds
from remote_simulation.block_generator import generate_blocks_async
from remote_simulation.config_builder import ConfluxOptions, SimulateOptions, generate_config_file
from remote_simulation.network_connector import connect_nodes
from remote_simulation.network_topology import NetworkTopology
from remote_simulation.port_allocation import remote_rpc_port
from remote_simulation.remote_node import RemoteNode
from remote_simulation.tools import init_tx_gen, wait_for_nodes_synced
from utils.counter import AtomicCounter
from utils.wait_until import WaitUntilTimeoutError
from utils import shell_cmds
from ali_instances.create_servers import load_host_specs, generate_timestamp


HOST_CONNECT_POOL = ThreadPoolExecutor(max_workers=200)
NODE_CONNECT_POOL = ThreadPoolExecutor(max_workers=200)


def _test_say_hello(
    port: int,
    host: str,
    timeout: float = 5.0,
    max_retries: int = 3,
    retry_delay: float = 10.0,
) -> bool:
    url = f"http://{host}:{port}"
    payload = {"jsonrpc": "2.0", "method": "test_sayHello", "params": [], "id": 1}
    for attempt in range(max_retries):
        try:
            response = requests.post(url, json=payload, timeout=timeout)
            response.raise_for_status()
            data = response.json()
            return "result" in data and "error" not in data
        except Exception:
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
    return False


def _launch_node(ip_address: str, index: int, counter: AtomicCounter) -> RemoteNode | None:
    try:
        shell_cmds.ssh(ip_address, "root", docker_cmds.launch_node(index))
    except Exception as exc:
        logger.info(f"实例 {ip_address} 节点 {index} 启动失败：{exc}")
        return None

    if not _test_say_hello(remote_rpc_port(index), ip_address):
        logger.info(f"实例 {ip_address} 节点 {index} 无法建立连接")
        return None

    node = RemoteNode(host=ip_address, index=index)
    if not node.wait_for_ready():
        logger.info(f"实例 {ip_address} 节点 {index} 无法进入就绪状态")
        return None

    cnt = counter.increment()
    logger.info(f"节点 {node.id} 启动成功，节点累计 {cnt}")
    return node


def _execute_instance(ip_address: str, nodes_per_host: int, config_file, pull_docker_image: bool) -> List[RemoteNode]:
    try:
        shell_cmds.scp(config_file.path, ip_address, "root", "~/config.toml")
        logger.debug(f"实例 {ip_address} 同步配置完成")
        if pull_docker_image:
            shell_cmds.ssh(ip_address, "root", docker_cmds.pull_image())
            logger.debug(f"实例 {ip_address} 拉取 docker 镜像完成")
        shell_cmds.ssh(ip_address, "root", docker_cmds.destory_all_nodes())
        logger.debug(f"实例 {ip_address} 状态初始化完成，开始启动节点")
    except Exception as exc:
        logger.warning(f"无法初始化实例 {ip_address}: {exc}")
        return []

    counter = AtomicCounter()
    launch_future = NODE_CONNECT_POOL.map(lambda idx: _launch_node(ip_address, idx, counter), range(nodes_per_host))
    return [n for n in launch_future if n is not None]


def launch_remote_nodes_root(hosts: List[HostSpec], config_file, pull_docker_image: bool = True) -> List[RemoteNode]:
    logger.info("开始启动所有 Conflux 节点")

    def _run_host(host: HostSpec):
        return _execute_instance(host.ip, host.nodes_per_host, config_file, pull_docker_image)

    launch_future = HOST_CONNECT_POOL.map(_run_host, hosts)
    nodes = list(chain.from_iterable(launch_future))
    expected_nodes_cnt = sum(h.nodes_per_host for h in hosts)
    logger.info(f"节点初始化完成，成功数量 {len(nodes)} 失败数量 {expected_nodes_cnt - len(nodes)}")
    return nodes


def collect_logs_root(nodes: List[RemoteNode], local_path: str) -> None:
    total_cnt = len(nodes)
    counter1 = AtomicCounter()
    counter2 = AtomicCounter()
    script_local = Path(__file__).resolve().parent / "auxiliary" / "scripts" / "remote" / "collect_logs_root.sh"
    if not script_local.exists():
        raise FileNotFoundError(f"missing {script_local}")

    Path(local_path).mkdir(parents=True, exist_ok=True)

    def _stop_and_collect(node: RemoteNode) -> int:
        try:
            remote_script = f"/tmp/{script_local.name}.{int(time.time())}.sh"
            shell_cmds.scp(str(script_local), node.host, "root", remote_script)
            shell_cmds.ssh(node.host, "root", ["bash", remote_script, str(node.index), docker_cmds.IMAGE_TAG])
            shell_cmds.ssh(node.host, "root", ["rm", "-f", remote_script])
            cnt1 = counter1.increment()
            logger.debug(f"节点 {node.id} 已完成日志生成 ({cnt1}/{total_cnt})")
            local_node_path = str(Path(local_path) / node.id)
            Path(local_node_path).mkdir(parents=True, exist_ok=True)
            shell_cmds.rsync_download(
                f"/root/output{node.index}/",
                local_node_path,
                node.host,
                user="root",
            )
            cnt2 = counter2.increment()
            logger.debug(f"节点 {node.id} 已完成日志同步 ({cnt2}/{total_cnt})")
            return 0
        except Exception as exc:
            logger.warning(f"节点 {node.id} 日志生成遇到问题: {exc}")
            return 1

    with ThreadPoolExecutor(max_workers=200) as executor:
        results = executor.map(_stop_and_collect, nodes)
    _ = sum(results)


if __name__ == "__main__":
    root = Path(__file__).resolve().parent
    servers_json = root / "ali_servers.json"
    if not servers_json.exists():
        raise FileNotFoundError(f"missing {servers_json}")

    raw = json.loads(servers_json.read_text())
    hosts = load_host_specs(raw)
    if not hosts:
        raise RuntimeError("no hosts found in ali_servers.json")

    default_key = root / "keys" / "ssh-key.pem"
    if "SSH_KEY_PATH" not in os.environ and default_key.exists():
        os.environ["SSH_KEY_PATH"] = str(default_key)

    key_path = next((h.ssh_key_path for h in hosts if h.ssh_key_path), None)
    if key_path and "SSH_KEY_PATH" not in os.environ:
        os.environ["SSH_KEY_PATH"] = str(Path(key_path).expanduser())

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

    nodes = launch_remote_nodes_root(hosts, config_file, pull_docker_image=True)
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
    try:
        wait_for_nodes_synced(nodes)
    except WaitUntilTimeoutError as exc:
        logger.warning(f"等待节点同步超时: {exc}")

    try:
        init_tx_gen(nodes, node_config.txgen_account_count)
    except Exception as exc:
        logger.warning(f"交易生成初始化异常: {exc}")
    logger.success("开始运行区块链系统")
    try:
        generate_blocks_async(
            nodes,
            simulation_config.num_blocks,
            node_config.max_block_size_in_bytes,
            simulation_config.generation_period_ms,
            min_node_interval_ms=10,
        )
    except Exception as exc:
        logger.warning(f"出块过程出现异常: {exc}")
    logger.info(f"Node goodput: {nodes[0].rpc.test_getGoodPut()}")

    try:
        wait_for_nodes_synced(nodes)
        logger.success("测试完毕，准备采集日志数据")
    except WaitUntilTimeoutError:
        logger.warning("部分节点没有完全同步，准备采集日志数据")

    logger.info(f"Node goodput: {nodes[0].rpc.test_getGoodPut()}")
    collect_logs_root(nodes, log_path)
    logger.success(f"日志收集完毕，路径 {os.path.abspath(log_path)}")

