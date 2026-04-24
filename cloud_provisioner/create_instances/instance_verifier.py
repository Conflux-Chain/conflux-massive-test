from collections import deque
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
import copy
import os
import queue
import socket
import time
import threading
from typing import Dict, List, Set, Tuple
from queue import Queue

from loguru import logger

from ..provider_interface import IEcsClient
from .types import Instance, InstanceType
from utils.counter import get_global_counter
from utils.wait_until import WaitUntilTimeoutError, wait_until


def _read_env_int(name: str, default: int, *, min_value: int = 0) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return max(min_value, int(raw))
    except ValueError:
        return default


SSH_CHECK_THREAD_WORKERS = _read_env_int("SSH_CHECK_THREAD_WORKERS", 2000, min_value=1)
_DEFAULT_SSH_CHECK_PROCESS_WORKERS = max(1, os.cpu_count() or 1)
SSH_CHECK_PROCESS_WORKERS = _read_env_int(
    "SSH_CHECK_PROCESS_WORKERS",
    _DEFAULT_SSH_CHECK_PROCESS_WORKERS,
    min_value=1,
)
_DEFAULT_SSH_CHECK_MAX_IN_FLIGHT = (
    max(128, SSH_CHECK_PROCESS_WORKERS * 8)
    if SSH_CHECK_PROCESS_WORKERS > 0
    else max(4000, SSH_CHECK_THREAD_WORKERS * 2)
)
SSH_CHECK_MAX_IN_FLIGHT = _read_env_int(
    "SSH_CHECK_MAX_IN_FLIGHT",
    _DEFAULT_SSH_CHECK_MAX_IN_FLIGHT,
    min_value=1,
)

_SSH_CHECK_POOL = None
_SSH_CHECK_POOL_LOCK = threading.Lock()


def _get_ssh_check_pool():
    global _SSH_CHECK_POOL
    if _SSH_CHECK_POOL is None:
        with _SSH_CHECK_POOL_LOCK:
            if _SSH_CHECK_POOL is None:
                if SSH_CHECK_PROCESS_WORKERS > 0:
                    _SSH_CHECK_POOL = ProcessPoolExecutor(max_workers=SSH_CHECK_PROCESS_WORKERS)
                else:
                    _SSH_CHECK_POOL = ThreadPoolExecutor(max_workers=SSH_CHECK_THREAD_WORKERS)
    return _SSH_CHECK_POOL


def _summarize_instance_ids(instance_ids: Set[str], *, sample_size: int = 8) -> str:
    if len(instance_ids) <= sample_size:
        return str(sorted(instance_ids))

    sample = ", ".join(sorted(instance_ids)[:sample_size])
    return f"count={len(instance_ids)}, sample=[{sample}, ...]"


class InstanceVerifier:
    region_id: str
    target_nodes: int
    request_nodes: int
    ready_instances: List[Tuple[Instance, str, str]]
    pending_instances: Dict[str, Instance]

    _state_changed: threading.Condition
    _stop: threading.Event
    _lock: threading.RLock
    _running_queue: Queue[Dict[str, Tuple[str, str]]]
    _ssh_result_queue: Queue[Tuple[str, str, str, bool]]

    def __init__(self, region_id: str, target_nodes: int, additional_nodes: int = 0):
        self.region_id = region_id
        self.target_nodes = target_nodes
        self.request_nodes = target_nodes + additional_nodes
        self.ready_instances = []
        self.pending_instances = dict()
        self._ready_nodes_count = 0
        self._pending_nodes_count = 0
        
        self._stop = threading.Event()
        self._lock = threading.RLock()
        self._state_changed = threading.Condition(self._lock)
        self._running_queue = Queue(maxsize=10000)
        self._ssh_result_queue = Queue()
        
    def stop(self):
        with self._state_changed:
            self._stop.set()
            self._state_changed.notify_all()
        
    def is_running(self):
        return not self._stop.is_set()

    def submit_pending_instances(self, ids: List[str], type: InstanceType, zone_id: str):
        new_instances = {
            instance_id: Instance(instance_id=instance_id, type=type, zone_id=zone_id)
            for instance_id in ids
        }
        if not new_instances:
            return

        with self._state_changed:
            for instance_id, instance in new_instances.items():
                previous = self.pending_instances.get(instance_id)
                if previous is not None:
                    self._pending_nodes_count -= previous.type.nodes
                self.pending_instances[instance_id] = instance
                self._pending_nodes_count += instance.type.nodes
            self._state_changed.notify_all()

    @property
    def ready_nodes(self):
        with self._lock:
            return self._ready_nodes_count

    def copy_ready_instances(self):
        with self._lock:
            return copy.copy(self.ready_instances)

    @property
    def pending_nodes(self):
        with self._lock:
            return self._pending_nodes_count

    def get_rest_nodes(self, *, wait_for_pendings=False):
        with self._state_changed:
            while True:
                ready_nodes = self._ready_nodes_count
                pending_nodes = self._pending_nodes_count

                # 如果 ready 满足目标，任务已完成
                if ready_nodes >= self.target_nodes:
                    return 0

                # 如果 ready 和 pending 不足目标，直接返回差值
                if ready_nodes + pending_nodes < self.request_nodes and (not wait_for_pendings or pending_nodes == 0):
                    return self.request_nodes - ready_nodes - pending_nodes

                # 剩下的情况里，ready 不满足，但 ready + pending 满足，或者 wait_for_pendings 是 true，需要等待 pending 的结果
                if not self._state_changed.wait(timeout=180):
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
                    f"Instances {_summarize_instance_ids(instance_status.pending_instances)} pending in region {self.region_id}")

            # 将 running instance 转入下一阶段
            if len(instance_status.running_instances) > 0:
                logger.success(
                    f"Instances {instance_status.running_instances} running in region {self.region_id}")
                processed_instances |= set(instance_status.running_instances)
                self._running_queue.put(instance_status.running_instances)

            # 将 lost instance 删除
            lost_instances = to_check_instances - \
                set(instance_status.running_instances) - instance_status.pending_instances

            with self._state_changed:
                if len(lost_instances) > 0:
                    logger.info(
                        f"Instances {lost_instances} lost or stopped in region {self.region_id}")
                    for instance_id in lost_instances:
                        instance = self.pending_instances.pop(instance_id, None)
                        if instance is not None:
                            self._pending_nodes_count -= instance.type.nodes
                    self._state_changed.notify_all()

                if self._ready_nodes_count >= self.target_nodes:
                    logger.info(
                        f"Region {self.region_id} reach target nodes, thread describe_instances loop exit")
                    return

            time.sleep(check_interval)
        logger.info(f"Region {self.region_id} not reach target nodes, thread describe_instances is stopped manually.")

    def wait_for_ssh_loop(self):
        submit_queue: deque[Tuple[str, str, str]] = deque()
        inflight_instance_ids: Set[str] = set()

        while self.is_running():
            progressed = False
            state_changed = False

            # 从队列获取任务并提交
            try:
                while True:
                    running_instances = self._running_queue.get_nowait()
                    for instance_id, (public_ip, private_ip) in running_instances.items():
                        submit_queue.append((instance_id, public_ip, private_ip))
                    progressed = True
            except queue.Empty:
                pass

            while submit_queue and len(inflight_instance_ids) < SSH_CHECK_MAX_IN_FLIGHT:
                instance_id, public_ip, private_ip = submit_queue.popleft()
                if instance_id in inflight_instance_ids:
                    continue
                future = _get_ssh_check_pool().submit(_wait_for_ssh_port_ready, public_ip)
                future.add_done_callback(
                    lambda future, instance_id=instance_id, public_ip=public_ip, private_ip=private_ip:
                    self._enqueue_ssh_result(instance_id, public_ip, private_ip, future)
                )
                inflight_instance_ids.add(instance_id)
                progressed = True

            while True:
                try:
                    instance_id, public_ip, private_ip, is_success = self._ssh_result_queue.get_nowait()
                except queue.Empty:
                    break

                inflight_instance_ids.discard(instance_id)
                progressed = True
                if is_success:
                    logger.info(
                        f"Region {self.region_id} Instance {instance_id} IP {public_ip} connect success ({get_global_counter('ssh_check').increment()})")

                    with self._state_changed:
                        instance = self.pending_instances.pop(instance_id, None)
                        if instance is None:
                            continue
                        self._pending_nodes_count -= instance.type.nodes
                        self.ready_instances.append((instance, public_ip, private_ip))
                        self._ready_nodes_count += instance.type.nodes
                        state_changed = True
                else:
                    logger.info(
                        f"Region {self.region_id} Instance {instance_id} IP {public_ip} connect fail (timeout)")
                    with self._state_changed:
                        instance = self.pending_instances.pop(instance_id, None)
                        if instance is None:
                            continue
                        self._pending_nodes_count -= instance.type.nodes
                        state_changed = True

            if state_changed:
                with self._state_changed:
                    self._state_changed.notify_all()

            with self._lock:
                if self._ready_nodes_count >= self.target_nodes:
                    logger.info(
                        f"Region {self.region_id} reach target nodes, thread wait_for_ssh_loop exit")
                    return

            if not progressed:
                time.sleep(0.2)
        logger.info(f"Region {self.region_id} not reach target nodes, thread wait_for_ssh_loop is stopped manually.")

    def _enqueue_ssh_result(self, instance_id: str, public_ip: str, private_ip: str, future):
        try:
            is_success = future.result()
        except Exception:
            logger.exception(f"SSH check worker failed for region {self.region_id}, instance {instance_id}, ip {public_ip}")
            is_success = False
        self._ssh_result_queue.put((instance_id, public_ip, private_ip, is_success))

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
