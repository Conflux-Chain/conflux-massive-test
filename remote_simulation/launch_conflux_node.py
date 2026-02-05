from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass

from cloud_provisioner.host_spec import HostSpec
from . import docker_cmds
from .remote_node import RemoteNode
from utils import shell_cmds
from remote_simulation.port_allocation import remote_rpc_port
from utils.counter import AtomicCounter
from utils.tempfile import TempFile
from utils.in_china import region_in_china
from itertools import chain




from typing import List

import time
import requests

from loguru import logger


HOST_CONNECT_POOL = ThreadPoolExecutor(max_workers=400)
NODE_CONNECT_POOL = ThreadPoolExecutor(max_workers=400)

@dataclass
class InstanceExecutionContext:
    counter: AtomicCounter
    config_file: TempFile
    pull_docker_image: bool


def launch_remote_nodes(host_specs: List[HostSpec], config_file: TempFile, pull_docker_image: bool = True) -> List[RemoteNode]:
    logger.info("开始启动所有 Conflux 节点")

    counter = AtomicCounter()
    context = InstanceExecutionContext(counter=counter, config_file=config_file, pull_docker_image=pull_docker_image)

    launch_instance_future = HOST_CONNECT_POOL.map(lambda spec: _execute_instance(spec, context), host_specs)

    # expected_nodes_cnt = len(ips) * nodes_per_host
    expected_nodes_cnt = sum([s.nodes_per_host for s in host_specs])


    nodes = list(chain.from_iterable(launch_instance_future))

    nodes_cnt = len(nodes)

    logger.info(f"节点初始化完成，成功数量 {nodes_cnt} 失败数量 {expected_nodes_cnt - nodes_cnt}")

    return nodes


def stop_remote_nodes(host_specs: List[HostSpec]):
    def _stop_instance(ip_address: str, user: str):
        try:
            shell_cmds.ssh(ip_address, user, docker_cmds.stop_all_nodes())
            logger.debug(f"实例 {ip_address} 已停止所有节点")
            return 0
        except Exception as e:
            logger.warning(f"停止实例 {ip_address} 上节点遇到问题: {e}")
            return 1

    fail_cnt = sum(HOST_CONNECT_POOL.map(lambda spec: _stop_instance(spec.ip, spec.ssh_user), host_specs))


def destory_remote_nodes(host_specs: List[HostSpec]):
    def _stop_instance(ip_address: str, user: str):
        try:
            shell_cmds.ssh(ip_address, user, docker_cmds.destory_all_nodes())
            logger.debug(f"实例 {ip_address} 已销毁所有节点")
            return 0
        except Exception as e:
            logger.warning(f"停止实例 {ip_address} 上节点遇到问题: {e}")
            return 1

    fail_cnt = sum(HOST_CONNECT_POOL.map(lambda spec: _stop_instance(spec.ip, spec.ssh_user), host_specs))

    


def _execute_instance(host_spec: HostSpec, ctx: InstanceExecutionContext) -> List[RemoteNode]:
    # 返回失败节点数量
    try:
        ip_address = host_spec.ip
        user = host_spec.ssh_user

        shell_cmds.scp("./setup_image.sh", ip_address, user, "~/setup_image.sh")
        logger.debug(f"实例 {ip_address} 上传初始化脚本完成")
        shell_cmds.ssh(ip_address, user, "./setup_image.sh")
        logger.debug(f"实例 {ip_address} 初始化完成")
        shell_cmds.scp(ctx.config_file.path, ip_address, user, "~/config.toml")

        logger.debug(f"实例 {ip_address} 同步配置完成")
        if ctx.pull_docker_image:
            if host_spec.region and region_in_china(host_spec.region):
                shell_cmds.inject_dockerhub_mirrors(host_spec.ip, user=user)
            shell_cmds.ssh(ip_address, user, docker_cmds.pull_image())
            logger.debug(f"实例 {ip_address} 拉取 docker 镜像完成")

        # 清理之前实验的残留数据        
        shell_cmds.ssh(ip_address, user, docker_cmds.destory_all_nodes())
        logger.debug(f"实例 {ip_address} 状态初始化完成，开始启动节点")
    except Exception as e:
        logger.warning(f"无法初始化实例 {ip_address}: {e}")
        return list()
    
    launch_nodes_future = NODE_CONNECT_POOL.map(lambda index: _launch_node(host_spec, index, ctx.counter), range(host_spec.nodes_per_host))
    return [n for n in launch_nodes_future if n is not None]



def _launch_node(host_spec: HostSpec, index: int, counter: AtomicCounter):
    ip_address = host_spec.ip
    user = host_spec.ssh_user

    try:
        shell_cmds.ssh(ip_address, user, docker_cmds.launch_node(index))
    except Exception as e:
        logger.info(f"实例 {ip_address} 节点 {index} 启动失败：{e}")
        return None
    
    # TODO: 是否需要清理未成功启动的 node?
    
    if not test_say_hello(remote_rpc_port(index), ip_address):
        logger.info(f"实例 {ip_address} 节点 {index} 无法建立连接")
        return None

    node = RemoteNode(host_spec=host_spec, index=index)

    if not node.wait_for_ready():
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