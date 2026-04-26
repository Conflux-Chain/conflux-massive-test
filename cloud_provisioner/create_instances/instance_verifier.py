from collections import deque
import copy
import queue
from queue import Queue
import threading
import time
from typing import Dict, List, Set, Tuple

from loguru import logger

from ..provider_interface import IEcsClient
from .ssh_check import (
    SSH_CHECK_MAX_IN_FLIGHT,
    SSH_CHECK_THREADS_PER_PROCESS,
    SSHCheckResult,
    SSHCheckTask,
    submit_ssh_check_batch,
)
from .types import Instance, InstanceType
from utils.counter import get_global_counter


def _summarize_instance_ids(instance_ids: Set[str], *, sample_size: int = 8) -> str:
    if len(instance_ids) <= sample_size:
        return str(sorted(instance_ids))

    sample = ", ".join(sorted(instance_ids)[:sample_size])
    return f"count={len(instance_ids)}, sample=[{sample}, ...]"


class RegionProvisioningTimeoutError(RuntimeError):
    pass


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

    def _remove_pending_instance(self, instance_id: str):
        instance = self.pending_instances.pop(instance_id, None)
        if instance is not None:
            self._pending_nodes_count -= instance.type.nodes
        return instance

    def _mark_ready_instance(self, instance: Instance, public_ip: str, private_ip: str):
        self.ready_instances.append((instance, public_ip, private_ip))
        self._ready_nodes_count += instance.type.nodes

    def _has_reached_target(self):
        return self._ready_nodes_count >= self.target_nodes

    def _current_pending_instance_ids(self) -> Set[str]:
        with self._lock:
            return set(self.pending_instances)

    def _drain_running_queue(self, submit_queue: deque[SSHCheckTask]) -> bool:
        progressed = False
        try:
            while True:
                running_instances = self._running_queue.get_nowait()
                for instance_id, (public_ip, private_ip) in running_instances.items():
                    submit_queue.append((instance_id, public_ip, private_ip))
                progressed = True
        except queue.Empty:
            return progressed

    def _next_ssh_batch(
        self,
        submit_queue: deque[SSHCheckTask],
        inflight_instance_ids: Set[str],
    ) -> list[SSHCheckTask]:
        remaining_capacity = max(0, SSH_CHECK_MAX_IN_FLIGHT - len(inflight_instance_ids))
        if remaining_capacity == 0 or not submit_queue:
            return []

        batch_limit = min(
            SSH_CHECK_THREADS_PER_PROCESS,
            remaining_capacity,
            len(submit_queue),
        )
        batch: list[SSHCheckTask] = []
        while submit_queue and len(batch) < batch_limit:
            instance_id, public_ip, private_ip = submit_queue.popleft()
            if instance_id in inflight_instance_ids:
                continue
            inflight_instance_ids.add(instance_id)
            batch.append((instance_id, public_ip, private_ip))
        return batch

    def _submit_ssh_batches(
        self,
        submit_queue: deque[SSHCheckTask],
        inflight_instance_ids: Set[str],
    ) -> bool:
        progressed = False
        while submit_queue:
            batch = self._next_ssh_batch(submit_queue, inflight_instance_ids)
            if not batch:
                break

            future = submit_ssh_check_batch(batch)
            future.add_done_callback(
                lambda future, batch=batch: self._enqueue_ssh_batch_result(batch, future)
            )
            progressed = True
        return progressed

    def _apply_ssh_result(self, result: SSHCheckResult) -> bool:
        instance_id, public_ip, private_ip, is_success = result
        if is_success:
            logger.info(
                f"Region {self.region_id} Instance {instance_id} IP {public_ip} connect success ({get_global_counter('ssh_check').increment()})"
            )
            with self._state_changed:
                instance = self._remove_pending_instance(instance_id)
                if instance is None:
                    return False
                self._mark_ready_instance(instance, public_ip, private_ip)
                return True

        logger.info(
            f"Region {self.region_id} Instance {instance_id} IP {public_ip} connect fail (timeout)"
        )
        with self._state_changed:
            return self._remove_pending_instance(instance_id) is not None

    def _drain_ssh_results(self, inflight_instance_ids: Set[str]) -> tuple[bool, bool]:
        progressed = False
        state_changed = False

        while True:
            try:
                result = self._ssh_result_queue.get_nowait()
            except queue.Empty:
                return progressed, state_changed

            inflight_instance_ids.discard(result[0])
            progressed = True
            if self._apply_ssh_result(result):
                state_changed = True

    def _remove_lost_instances(self, lost_instances: Set[str]) -> bool:
        if not lost_instances:
            return False

        logger.info(
            f"Instances {lost_instances} lost or stopped in region {self.region_id}"
        )
        with self._state_changed:
            state_changed = False
            for instance_id in lost_instances:
                if self._remove_pending_instance(instance_id) is not None:
                    state_changed = True
            if state_changed:
                self._state_changed.notify_all()
            return state_changed

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
                    raise RegionProvisioningTimeoutError(
                        f"Region {self.region_id} wait for event timeout")

    def describe_instances_loop(self, client: IEcsClient, check_interval: float = 3.0):
        processed_instances: Set[str] = set()

        while self.is_running():
            to_check_instances = self._current_pending_instance_ids() - processed_instances
            if not to_check_instances:
                time.sleep(check_interval)
                continue

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

            self._remove_lost_instances(lost_instances)

            with self._state_changed:
                if self._has_reached_target():
                    logger.info(
                        f"Region {self.region_id} reach target nodes, thread describe_instances loop exit")
                    return

            time.sleep(check_interval)
        logger.info(f"Region {self.region_id} not reach target nodes, thread describe_instances is stopped manually.")

    def wait_for_ssh_loop(self):
        submit_queue: deque[SSHCheckTask] = deque()
        inflight_instance_ids: Set[str] = set()

        while self.is_running():
            progressed = self._drain_running_queue(submit_queue)
            if self._submit_ssh_batches(submit_queue, inflight_instance_ids):
                progressed = True

            result_progressed, state_changed = self._drain_ssh_results(inflight_instance_ids)
            if result_progressed:
                progressed = True

            if state_changed:
                with self._state_changed:
                    self._state_changed.notify_all()

            with self._lock:
                if self._has_reached_target():
                    logger.info(
                        f"Region {self.region_id} reach target nodes, thread wait_for_ssh_loop exit")
                    return

            if not progressed:
                time.sleep(0.2)
        logger.info(f"Region {self.region_id} not reach target nodes, thread wait_for_ssh_loop is stopped manually.")

    def _enqueue_ssh_batch_result(self, batch: list[SSHCheckTask], future):
        try:
            results = future.result()
        except Exception:
            logger.exception(
                f"SSH check batch worker failed for region {self.region_id}, batch_size={len(batch)}"
            )
            results = [
                (instance_id, public_ip, private_ip, False)
                for instance_id, public_ip, private_ip in batch
            ]

        for instance_id, public_ip, private_ip, is_success in results:
            self._ssh_result_queue.put((instance_id, public_ip, private_ip, is_success))
