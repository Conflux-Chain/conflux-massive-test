import asyncio
from dataclasses import dataclass
from itertools import chain
from typing import Iterable, List, Sequence

import time
import requests

from loguru import logger

from . import docker_cmds
from .remote_node import RemoteNode
from remote_simulation.port_allocation import remote_rpc_port
from remote_simulation.ssh_utils import run_ssh, scp_upload
from utils.counter import AtomicCounter
from utils.tempfile import TempFile

@dataclass
class InstanceExecutionContext:
    counter: AtomicCounter
    config_file: TempFile
    pull_docker_image: bool
    nodes_per_host: int
    ssh_user: str


@dataclass
class HostSpec:
    ip: str
    nodes_per_host: int
    ssh_user: str = "ubuntu"
    ssh_key_path: str | None = None
    provider: str | None = None
    region: str | None = None
    instance_id: str | None = None


def launch_remote_nodes(hosts: Sequence[HostSpec], config_file: TempFile, pull_docker_image: bool = True) -> List[RemoteNode]:
    logger.info("开始启动所有 Conflux 节点")

    counter = AtomicCounter()

    async def _run():
        tasks = [_execute_instance(host, counter, config_file, pull_docker_image) for host in hosts]
        results = await asyncio.gather(*tasks)
        return list(chain.from_iterable(results))

    nodes = asyncio.run(_run())
    expected_nodes_cnt = sum(h.nodes_per_host for h in hosts)
    nodes_cnt = len(nodes)
    logger.info(f"节点初始化完成，成功数量 {nodes_cnt} 失败数量 {expected_nodes_cnt - nodes_cnt}")
    return nodes


def stop_remote_nodes(hosts: Iterable[HostSpec]):
    async def _stop_instance(host: HostSpec):
        try:
            await run_ssh(
                host.ip,
                host.ssh_user,
                docker_cmds.stop_all_nodes(),
                key_path=host.ssh_key_path,
            )
            logger.debug(f"实例 {host.ip} 已停止所有节点")
            return 0
        except Exception as e:
            logger.warning(f"停止实例 {host.ip} 上节点遇到问题: {e}")
            return 1

    async def _run():
        tasks = [_stop_instance(host) for host in hosts]
        return await asyncio.gather(*tasks, return_exceptions=True)

    results = asyncio.run(_run())
    fail_cnt = sum(1 for res in results if isinstance(res, Exception) or res == 1)


def destory_remote_nodes(hosts: Iterable[HostSpec]):
    async def _stop_instance(host: HostSpec):
        try:
            await run_ssh(
                host.ip,
                host.ssh_user,
                docker_cmds.destory_all_nodes(),
                key_path=host.ssh_key_path,
            )
            logger.debug(f"实例 {host.ip} 已销毁所有节点")
            return 0
        except Exception as e:
            logger.warning(f"停止实例 {host.ip} 上节点遇到问题: {e}")
            return 1

    async def _run():
        tasks = [_stop_instance(host) for host in hosts]
        return await asyncio.gather(*tasks, return_exceptions=True)

    results = asyncio.run(_run())
    fail_cnt = sum(1 for res in results if isinstance(res, Exception) or res == 1)

    


async def _execute_instance(host: HostSpec, counter: AtomicCounter, config_file: TempFile, pull_docker_image: bool) -> List[RemoteNode]:
    # 返回失败节点数量
    try:
        await scp_upload(
            config_file.path,
            host.ip,
            host.ssh_user,
            "~/config.toml",
            key_path=host.ssh_key_path,
        )
        logger.debug(f"实例 {host.ip} 同步配置完成")
        if pull_docker_image:
            await run_ssh(
                host.ip,
                host.ssh_user,
                docker_cmds.pull_image(),
                key_path=host.ssh_key_path,
            )
            logger.debug(f"实例 {host.ip} 拉取 docker 镜像完成")

        # 清理之前实验的残留数据        
        await run_ssh(
            host.ip,
            host.ssh_user,
            docker_cmds.destory_all_nodes(),
            key_path=host.ssh_key_path,
        )
        logger.debug(f"实例 {host.ip} 状态初始化完成，开始启动节点")
    except Exception as e:
        logger.warning(f"无法初始化实例 {host.ip}: {e}")
        return list()
    
    tasks = [
        _launch_node(host.ip, host.ssh_user, index, counter, host.ssh_key_path, host.provider)
        for index in range(host.nodes_per_host)
    ]
    results = await asyncio.gather(*tasks)
    return [n for n in results if n is not None]



async def _launch_node(ip_address: str, user: str, index: int, counter: AtomicCounter, key_path: str | None, provider: str | None):
    try:
        await run_ssh(
            ip_address,
            user,
            docker_cmds.launch_node(index),
            key_path=key_path,
        )
    except Exception as e:
        logger.info(f"实例 {ip_address} 节点 {index} 启动失败：{e}")
        return None
    
    # TODO: 是否需要清理未成功启动的 node?
    
    ok = await asyncio.to_thread(test_say_hello, remote_rpc_port(index), ip_address)
    if not ok:
        logger.info(f"实例 {ip_address} 节点 {index} 无法建立连接")
        return None

    node = RemoteNode(host=ip_address, index=index, ssh_user=user, ssh_key_path=key_path)

    ready = await asyncio.to_thread(node.wait_for_ready)
    if not ready:
        logger.info(f"实例 {ip_address} 节点 {index} 无法进入就绪状态")
        return None

    cnt = counter.increment()
    logger.info(f"节点 {node.id} 启动成功，节点累计 {cnt}")
    return node
        

def test_say_hello(
    port: int,
    host: str = "localhost",
    timeout: float = 5.0,
    max_retries: int = 3,
    retry_delay: float = 15.0
) -> bool:
    """
    测试节点的 test_sayHello 方法
    
    Args:
        port: 节点端口
        host: 主机地址，默认 localhost
        timeout: 请求超时时间（秒）
        max_retries: 最大重试次数
        retry_delay: 重试间隔（秒）
    
    Returns:
        bool: 成功返回 True，失败返回 False
    """
    url = f"http://{host}:{port}"
    payload = {
        "jsonrpc": "2.0",
        "method": "test_sayHello",
        "params": [],
        "id": 1
    }
    
    for attempt in range(max_retries):
        try:
            response = requests.post(url, json=payload, timeout=timeout)
            response.raise_for_status()
            data = response.json()
            
            # 检查是否有错误
            if "error" in data:
                return False
            
            # 检查是否有结果
            if "result" in data:
                return True
            
            return False
            
        except Exception:
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
            continue
    
    return False