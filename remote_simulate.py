#!/usr/bin/env python3
"""Run a Conflux simulation on provisioned cloud instances.

This script reads an inventory (by default `hosts.json`), launches nodes, runs the experiment, and collects logs.
"""
import ipaddress
import os
import time
from concurrent.futures import ThreadPoolExecutor
from collections import defaultdict
from itertools import chain
from pathlib import Path
from typing import Dict, List

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


def _launch_node(host: HostSpec, index: int) -> RemoteNode | None:
    try:
        shell_cmds.ssh(host.ip, host.ssh_user, docker_cmds.launch_node(index))
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


def _execute_instance(host: HostSpec, nodes_per_host: int, config_file, pull_docker_image: bool) -> List[RemoteNode]:
    try:
        shell_cmds.scp(config_file.path, host.ip, host.ssh_user, "~/config.toml")
        logger.debug(f"实例 {host.ip} 同步配置完成")
        if pull_docker_image:
            shell_cmds.ssh(host.ip, host.ssh_user, docker_cmds.pull_image())
            logger.debug(f"实例 {host.ip} 拉取 docker 镜像完成")
        shell_cmds.ssh(host.ip, host.ssh_user, docker_cmds.destory_all_nodes())
        logger.debug(f"实例 {host.ip} 状态初始化完成，开始启动节点")
    except Exception as exc:
        logger.warning(f"{host.region} 无法初始化实例 {host.ip}: {exc}")
        return []

    launch_future = NODE_CONNECT_POOL.map(lambda idx: _launch_node(host, idx), range(nodes_per_host))
    return [n for n in launch_future if n is not None]


def launch_remote_nodes(hosts: List[HostSpec], config_file, pull_docker_image: bool = True) -> List[RemoteNode]:
    logger.info("开始启动所有 Conflux 节点")

    def _run_host(host: HostSpec):
        return _execute_instance(host, host.nodes_per_host, config_file, pull_docker_image)

    launch_future = HOST_CONNECT_POOL.map(_run_host, hosts)
    nodes = list(chain.from_iterable(launch_future))
    expected_nodes_cnt = sum(h.nodes_per_host for h in hosts)
    logger.info(f"节点初始化完成，成功数量 {len(nodes)} 失败数量 {expected_nodes_cnt - len(nodes)}")
    return nodes


def _sorted_hosts_by_private_ip(hosts: List[HostSpec]) -> List[HostSpec]:
    def _key(h: HostSpec):
        try:
            return ipaddress.ip_address(h.private_ip or h.ip)
        except ValueError:
            return ipaddress.ip_address(h.ip)

    return sorted(hosts, key=_key)


def _prepare_images_by_zone(hosts: List[HostSpec]) -> None:
    script_dir = Path(__file__).resolve().parent / "auxiliary" / "scripts" / "remote"
    dockerhub_script = script_dir / "cfx_pull_image_from_dockerhub_and_push_local.sh"
    registry_script = script_dir / "cfx_pull_image_from_registry_and_push_local.sh"
    if not dockerhub_script.exists():
        raise FileNotFoundError(f"missing {dockerhub_script}")
    if not registry_script.exists():
        raise FileNotFoundError(f"missing {registry_script}")

    zones: Dict[str, List[HostSpec]] = defaultdict(list)
    for host in hosts:
        zones[host.zone].append(host)

    def _prepare_zone(zone_hosts: List[HostSpec]) -> None:
        ordered = _sorted_hosts_by_private_ip(zone_hosts)
        if not ordered:
            return

        def _prepare_host(i: int, host: HostSpec, futures: list) -> bool:
            host_ip = host.private_ip or host.ip
            try:
                shell_cmds.scp(str(dockerhub_script), host.ip, host.ssh_user, docker_cmds.REMOTE_SCRIPT_PULL_DOCKERHUB)
                shell_cmds.scp(str(registry_script), host.ip, host.ssh_user, docker_cmds.REMOTE_SCRIPT_PULL_REGISTRY)
                shell_cmds.ssh(
                    host.ip,
                    host.ssh_user,
                    [
                        "chmod",
                        "+x",
                        docker_cmds.REMOTE_SCRIPT_PULL_DOCKERHUB,
                        docker_cmds.REMOTE_SCRIPT_PULL_REGISTRY,
                    ],
                )

                if i == 0:
                    logger.info(f"zone {host.zone}: seed {host_ip} pulls from dockerhub")
                    shell_cmds.ssh(host.ip, host.ssh_user, docker_cmds.pull_image_from_dockerhub_and_push_local())
                    return True

                ancestor = (i - 1) // 2
                while ancestor is not None and ancestor >= 0:
                    try:
                        parent_ok = futures[ancestor].result()
                    except Exception:
                        parent_ok = False

                    if parent_ok:
                        parent = ordered[ancestor]
                        registry_host = parent.private_ip or parent.ip
                        logger.info(f"zone {host.zone}: {host_ip} pulls from {registry_host}")
                        try:
                            shell_cmds.ssh(
                                host.ip,
                                host.ssh_user,
                                docker_cmds.pull_image_from_registry_and_push_local(registry_host),
                            )
                            return True
                        except Exception as exc:
                            logger.warning(f"zone {host.zone}: {host_ip} failed pulling from {registry_host}: {exc}")

                    if ancestor == 0:
                        ancestor = None
                    else:
                        ancestor = (ancestor - 1) // 2

                logger.info(f"zone {host.zone}: {host_ip} fallback to dockerhub")
                shell_cmds.ssh(host.ip, host.ssh_user, docker_cmds.pull_image_from_dockerhub_and_push_local())
                return True
            except Exception as exc:
                logger.warning(f"zone {host.zone}: {host_ip} image prepare failed: {exc}")
                return False

        with ThreadPoolExecutor(max_workers=min(128, max(1, len(ordered)))) as zone_executor:
            futures: list = [None] * len(ordered)
            for i, host in enumerate(ordered):
                futures[i] = zone_executor.submit(_prepare_host, i, host, futures)
            for f in futures:
                try:
                    f.result()
                except Exception:
                    pass

    with ThreadPoolExecutor(max_workers=min(32, max(1, len(zones)))) as executor:
        futures = [executor.submit(_prepare_zone, zone_hosts) for zone_hosts in zones.values()]
        for f in futures:
            f.result()


def collect_logs(nodes: List[RemoteNode], local_path: str) -> None:
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
            shell_cmds.scp(str(script_local), node.host_spec.ip, node.host_spec.ssh_user, remote_script)
            shell_cmds.ssh(node.host_spec.ip, node.host_spec.ssh_user, ["bash", remote_script, str(node.index), docker_cmds.IMAGE_TAG])
            shell_cmds.ssh(node.host_spec.ip, node.host_spec.ssh_user, ["rm", "-f", remote_script])
            cnt1 = counter1.increment()
            logger.debug(f"节点 {node.id} 已完成日志生成 ({cnt1}/{total_cnt})")
            local_node_path = str(Path(local_path) / node.id)
            Path(local_node_path).mkdir(parents=True, exist_ok=True)
            shell_cmds.rsync_download(
                f"/root/output{node.index}/",
                local_node_path,
                node.host_spec.ip,
                user=node.host_spec.ssh_user,
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

    simulation_config = SimulateOptions(
        target_nodes=total_nodes,
        # nodes_per_host=max_nodes_per_host,
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

    log_path = f"logs/{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}"
    Path(log_path).mkdir(parents=True, exist_ok=True)

    logger.info("准备分区内镜像拉取 (dockerhub -> zone peers -> local registry)")
    _prepare_images_by_zone(hosts)

    nodes = launch_remote_nodes(hosts, config_file, pull_docker_image=False)
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
    collect_logs(nodes, log_path)
    logger.success(f"日志收集完毕，路径 {os.path.abspath(log_path)}")

