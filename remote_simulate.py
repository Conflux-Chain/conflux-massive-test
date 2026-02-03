#!/usr/bin/env python3
"""Run a Conflux simulation on provisioned cloud instances.

This script reads an inventory (by default `hosts.json`), launches nodes, runs the experiment, and collects logs.
"""
import os
import time
from concurrent.futures import ThreadPoolExecutor
from itertools import chain
from pathlib import Path
from typing import List
import traceback

import requests
from loguru import logger

from cloud_provisioner.host_spec import HostSpec, load_hosts
import datetime
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


HOST_CONNECT_POOL = ThreadPoolExecutor(max_workers=200)
NODE_CONNECT_POOL = ThreadPoolExecutor(max_workers=200)
COUNTER = AtomicCounter()


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


def _launch_node(host: HostSpec, index: int, enable_flamegraph: bool = False) -> RemoteNode | None:
    try:
        shell_cmds.ssh(host.ip, "root", docker_cmds.launch_node(index))
    except Exception as exc:
        logger.info(f"{host.region} 实例 {host.ip} 节点 {index} 启动失败：{exc}")
        return None

    if not _test_say_hello(remote_rpc_port(index), host.ip):
        logger.info(f"{host.region} 实例 {host.ip} 节点 {index} 无法建立连接")
        return None

    node = RemoteNode(host_spec=host, index=index)
    if not node.wait_for_ready():
        logger.warning(f"{host.region} 实例 {host.ip} 节点 {index} 无法进入就绪状态")
        return None

    cnt = COUNTER.increment()
    logger.info(f"节点 {node.id} 启动成功，节点累计 {cnt}")
    return node


def _execute_instance(host: HostSpec, nodes_per_host: int, config_file, pull_docker_image: bool, enable_flamegraph: bool = False) -> List[RemoteNode]:
    try:
        shell_cmds.scp(config_file.path, host.ip, "root", "~/config.toml")
        logger.debug(f"实例 {host.ip} 同步配置完成")
        if pull_docker_image:
            shell_cmds.ssh(host.ip, "root", docker_cmds.pull_image())
            logger.debug(f"实例 {host.ip} 拉取 docker 镜像完成")
        shell_cmds.ssh(host.ip, "root", docker_cmds.destory_all_nodes())
        logger.debug(f"实例 {host.ip} 状态初始化完成，开始启动节点")
    except Exception as exc:
        logger.warning(f"{host.region} 无法初始化实例 {host.ip}: {exc}")
        return []

    launch_future = NODE_CONNECT_POOL.map(lambda idx: _launch_node(host, idx, enable_flamegraph), range(nodes_per_host))
    return [n for n in launch_future if n is not None]


def launch_remote_nodes_root(hosts: List[HostSpec], config_file, pull_docker_image: bool = True, enable_flamegraph: bool = False) -> List[RemoteNode]:
    logger.info("开始启动所有 Conflux 节点")

    def _run_host(host: HostSpec):
        return _execute_instance(host, host.nodes_per_host, config_file, pull_docker_image, enable_flamegraph)

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
            shell_cmds.scp(str(script_local), node.host_spec.ip, "root", remote_script)
            shell_cmds.ssh(node.host_spec.ip, "root", ["bash", remote_script, str(node.index), docker_cmds.IMAGE_TAG])
            shell_cmds.ssh(node.host_spec.ip, "root", ["rm", "-f", remote_script])
            cnt1 = counter1.increment()
            logger.debug(f"节点 {node.id} 已完成日志生成 ({cnt1}/{total_cnt})")
            local_node_path = str(Path(local_path) / node.id)
            Path(local_node_path).mkdir(parents=True, exist_ok=True)
            shell_cmds.rsync_download(
                f"/root/output{node.index}/",
                local_node_path,
                node.host_spec.ip,
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
    servers_json = root / "hosts.json"

    if not servers_json.exists():
        raise FileNotFoundError(f"missing {servers_json}")

    hosts = load_hosts(str(servers_json))
    if not hosts:
        raise RuntimeError("no hosts found in hosts.json")

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

    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--enable-flamegraph", action="store_true", help="Enable flamegraph profiling in nodes")
    args = parser.parse_args()

    simulation_config = SimulateOptions(
        target_nodes=total_nodes,
        # nodes_per_host=max_nodes_per_host,
        num_blocks=100,
        connect_peers=8,
        target_tps=17000,
        storage_memory_gb=16,
        generation_period_ms=175,
        enable_flamegraph=args.enable_flamegraph or False,
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

    log_path = f"logs/{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}"
    Path(log_path).mkdir(parents=True, exist_ok=True)

    nodes = launch_remote_nodes_root(hosts, config_file, pull_docker_image=True, enable_flamegraph=simulation_config.enable_flamegraph)
    if len(nodes) < simulation_config.target_nodes:
        # raise RuntimeError("Not all nodes started")
        logger.warning(f"启动了{len(nodes)}个节点，少于预期的{simulation_config.target_nodes}个节点")
        logger.warning("部分节点启动失败，继续进行测试")
    else:
        logger.success("所有节点已启动")
    logger.info("准备连接拓扑网络")

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

    # Start per-node profilers in parallel if requested. Use a separate privileged container that
    # attaches to the node container's PID namespace and runs flamegraph --pid 1.
    if simulation_config.enable_flamegraph:
        duration_s = int(simulation_config.num_blocks * simulation_config.generation_period_ms / 1000) + 30
        logger.info(f"Starting flamegraph profilers (duration {duration_s}s) on {len(nodes)} nodes. This may take a few minutes as images are pulled and containers are scheduled.")
        # Launch profiler requests concurrently, then poll each host for a start marker.
        from concurrent.futures import ThreadPoolExecutor, as_completed

        def _start_and_wait(node):
            profiler_cmd = docker_cmds.start_profiler(node.index, duration_s)
            start_ts = time.time()
            try:
                shell_cmds.ssh(node.host_spec.ip, "root", profiler_cmd)
                logger.debug(f"Profiler start requested on node {node.id}")
            except Exception:
                logger.warning(f"无法在节点 {node.id} 启动 profiler: {traceback.format_exc()}")
                return (node, False, None)
            # Poll for flame_start marker (up to ~3 minutes)
            for attempt in range(36):
                try:
                    res = shell_cmds.ssh(node.host_spec.ip, "root", f"test -f ~/log{node.index}/flame_start_{node.index}.txt && echo ok || echo no")
                    if res.stdout.strip() == "ok":
                        delta = time.time() - start_ts
                        logger.info(f"Profiler on node {node.id} reported flame_start after {delta:.1f}s")
                        return (node, True, delta)
                except Exception as e:
                    logger.debug(f"Polling profiler on {node.id} attempt {attempt+1} failed: {e}")
                time.sleep(5)
            logger.warning(f"Profiler on node {node.id} did not report start within timeout")
            return (node, False, None)

        with ThreadPoolExecutor(max_workers=min(32, max(1, len(nodes)))) as executor:
            futures = [executor.submit(_start_and_wait, node) for node in nodes]
            for fut in as_completed(futures):
                node, ok, delta = fut.result()
                if ok:
                    logger.debug(f"Profiler confirmed running on {node.id} (start delta {delta:.1f}s)")
                else:
                    logger.warning(f"Profiler failed to start on {node.id}")

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
    try:
        logger.info(f"Node goodput: {nodes[0].rpc.test_getGoodPut()}")
    except Exception as exc:
        logger.warning(f"无法获取 Node goodput: {exc}")

    try:
        wait_for_nodes_synced(nodes)
        logger.success("测试完毕，准备采集日志数据")
    except WaitUntilTimeoutError:
        logger.warning("部分节点没有完全同步，准备采集日志数据")

    try:
        logger.info(f"Node goodput: {nodes[0].rpc.test_getGoodPut()}")
    except Exception as exc:
        logger.warning(f"无法获取 Node goodput: {exc}")
    collect_logs_root(nodes, log_path)
    logger.success(f"日志收集完毕，路径 {os.path.abspath(log_path)}")

