from concurrent.futures import ThreadPoolExecutor
import copy
import queue
import socket
import time
import threading
from typing import Dict, List, Set, Tuple
from queue import Queue

from loguru import logger

from ..provider_interface import IEcsClient
from .types import Instance, InstanceType
from utils.wait_until import WaitUntilTimeoutError, wait_until

SSH_CHECK_POOL = ThreadPoolExecutor(max_workers=2000)


class InstanceVerifier:
    region_id: str
    target_nodes: int
    request_nodes: int
    ready_instances: List[Tuple[Instance, str, str]]
    pending_instances: Dict[str, Instance]

    _event: threading.Event
    _stop: threading.Event
    _lock: threading.RLock
    _running_queue: Queue[Dict[str, Tuple[str, str]]]

    def __init__(self, region_id: str, target_nodes: int, additional_nodes: int = 0):
        self.region_id = region_id
        self.target_nodes = target_nodes
        self.request_nodes = target_nodes + additional_nodes
        self.ready_instances = []
        self.pending_instances = dict()
        
        self._stop = threading.Event()
        self._event = threading.Event()
        self._lock = threading.RLock()
        self._running_queue = Queue(maxsize=10000)
        
    def stop(self):
        self._stop.set()
        
    def is_running(self):
        return not self._stop.is_set()

    def submit_pending_instances(self, ids: List[str], type: InstanceType, zone_id: str):
        self.pending_instances.update(
            {id: Instance(instance_id=id, type=type, zone_id=zone_id) for id in ids})

    @property
    def ready_nodes(self):
        with self._lock:
            return sum([instance.type.nodes for instance, _, _ in self.ready_instances])

    def copy_ready_instances(self):
        with self._lock:
            return copy.copy(self.ready_instances)

    @property
    def pending_nodes(self):
        with self._lock:
            return sum([instance.type.nodes for instance in self.pending_instances.values()])

    def get_rest_nodes(self, *, wait_for_pendings=False):
        while True:
            with self._lock:
                ready_nodes = self.ready_nodes
                pending_nodes = self.pending_nodes

                # 如果 ready 满足目标，任务已完成
                if ready_nodes >= self.target_nodes:
                    return 0

                # 如果 ready 和 pending 不足目标，直接返回差值
                if ready_nodes + pending_nodes < self.request_nodes and (not wait_for_pendings or pending_nodes == 0):
                    return self.request_nodes - ready_nodes - pending_nodes

            # 剩下的情况里，ready 不满足，但 ready + pending 满足，或者 wait_for_pendings 是 true，需要等待 pending 的结果
            normal = self._event.wait(timeout=180)
            if not normal:
                raise Exception(
                    f"Region {self.region_id} wait for event timeout")

    def describe_instances_loop(self, client: IEcsClient, check_interval: float = 3.0):
        processed_instances: Set[str] = set()

        while self.is_running():
            # 获取当前 pending instance
            with self._lock:
                to_check_instances = set(
                    self.pending_instances) - processed_instances

            instance_status = client.describe_instance_status(self.region_id, instance_ids=list(to_check_instances))

            if len(instance_status.pending_instances) > 0:
                logger.debug(
                    f"Instances {instance_status.pending_instances} pending in region {self.region_id}")

            # 将 running instance 转入下一阶段
            if len(instance_status.running_instances) > 0:
                logger.success(
                    f"Instances {instance_status.running_instances} running in region {self.region_id}")
                processed_instances |= set(instance_status.running_instances)
                self._running_queue.put(instance_status.running_instances)

            # 将 lost instance 删除
            lost_instances = to_check_instances - \
                set(instance_status.running_instances) - instance_status.pending_instances

            with self._lock:
                if len(lost_instances) > 0:
                    logger.info(
                        f"Instances {lost_instances} lost or stopped in region {self.region_id}")
                    for instance_id in lost_instances:
                        del self.pending_instances[instance_id]
                    self._event.set()

                if self.ready_nodes >= self.target_nodes:
                    logger.info(
                        f"Region {self.region_id} reach target nodes, thread describe_instances loop exit")
                    return

            time.sleep(check_interval)
        logger.info(f"Region {self.region_id} not reach target nodes, thread describe_instances is stopped manually.")

    def wait_for_ssh_loop(self):
        future_set = dict()
        while self.is_running():
            # 从队列获取任务并提交
            try:
                running_instances = self._running_queue.get_nowait()
                for instance_id, (public_ip, private_ip) in running_instances.items():
                    check_future = SSH_CHECK_POOL.submit(
                        _wait_for_ssh_port_ready, public_ip)
                    future_set[instance_id] = (public_ip, private_ip, check_future)
            except queue.Empty:
                pass

            # 查看线程池结果
            to_clear_instance_ids = set()
            for instance_id, (public_ip, private_ip, future) in future_set.items():
                if not future.done():
                    continue

                to_clear_instance_ids.add(instance_id)
                is_success = future.result()

                if is_success:
                    logger.info(
                        f"Region {self.region_id} Instance {instance_id} IP {public_ip} connect success")

                    with self._lock:
                        instance = self.pending_instances[instance_id]
                        del self.pending_instances[instance_id]
                        self.ready_instances.append((instance, public_ip, private_ip))
                else:
                    logger.info(
                        f"Region {self.region_id} Instance {instance_id} IP {public_ip} connect fail (timeout)")
                    with self._lock:
                        del self.pending_instances[instance_id]

            for instance_id in to_clear_instance_ids:
                del future_set[instance_id]

            if to_clear_instance_ids:
                self._event.set()

            with self._lock:
                if self.ready_nodes >= self.target_nodes:
                    logger.info(
                        f"Region {self.region_id} reach target nodes, thread wait_for_ssh_loop exit")
                    return

            time.sleep(1)
        logger.info(f"Region {self.region_id} not reach target nodes, thread wait_for_ssh_loop is stopped manually.")

def _check_port(ip: str, timeout: int = 5):
    """
    处理单个IP端口检查任务

    Args:
        ip: IP地址
        port: 端口号
        attempt: 当前尝试次数
    """

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(timeout)

    try:
        result = sock.connect_ex((ip, 22))
        return result == 0
    except (socket.timeout, socket.error):
        return False
    finally:
        sock.close()


def _wait_for_ssh_port_ready(ip: str):
    try:
        wait_until(lambda: _check_port(ip), timeout=180)
        return True
    except WaitUntilTimeoutError:
        logger.warning(f"Cannot connect to IP {ip}")
        return False
