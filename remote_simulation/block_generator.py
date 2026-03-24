from dataclasses import dataclass
import math
import re
from typing import List, Optional
from queue import Queue
import threading
import time
import random
import logging

from loguru import logger

from remote_simulation.remote_node import RemoteNode
from utils.wait_until import wait_until

@dataclass
class BlockTask:
    """单个区块生成任务"""
    block_id: int
    node_id: str
    scheduled_time: float  # 计划执行的绝对时间戳

@dataclass
class BlockResult:
    """区块生成结果"""
    block_id: int
    node_id: str
    success: bool
    rpc_time: float
    error_msg: Optional[str] = None


class BlockGenerationPlan:
    """区块生成计划器"""
    
    def __init__(self, nodes: List[RemoteNode], num_blocks: int, 
                 generation_period_ms: int, min_node_interval_ms: int):
        self.nodes = {node.id: node for node in nodes}
        self.num_blocks = num_blocks
        self.generation_period_ms = generation_period_ms
        self.min_node_interval_ms = min_node_interval_ms
        
    def generate(self) -> List[BlockTask]:
        """生成完整的出块计划"""
        tasks = []
        current_time = time.time()
        node_last_scheduled = {}  # 记录每个节点最后一次调度时间
        
        for i in range(self.num_blocks):
            # 生成出块时间间隔（指数分布）
            wait_sec = random.expovariate(1000 / self.generation_period_ms)
            scheduled_time = current_time + wait_sec
            
            # 选择节点，确保该节点距离上次出块至少 min_node_interval_ms
            node_id = self._select_available_node(
                scheduled_time, node_last_scheduled
            )
            
            tasks.append(BlockTask(
                block_id=i + 1,
                node_id=node_id,
                scheduled_time=scheduled_time
            ))
            
            node_last_scheduled[node_id] = scheduled_time
            current_time = scheduled_time
            
        return tasks
    
    def _select_available_node(self, scheduled_time: float, 
                               node_last_scheduled: dict) -> str:
        """选择一个可用节点（距离上次出块时间足够长）"""
        min_interval_sec = self.min_node_interval_ms / 1000.0
        available_nodes = []
        
        for node_id in self.nodes.keys():
            last_time = node_last_scheduled.get(node_id, 0)
            if scheduled_time - last_time >= min_interval_sec:
                available_nodes.append(node_id)
        
        # 如果没有可用节点，选择距离上次出块时间最长的节点
        if not available_nodes:
            raise Exception("No node available, consider change the config")
        
        return random.choice(available_nodes)
    
    def validate(self, tasks: List[BlockTask]) -> bool:
        """验证出块计划是否满足约束条件"""
        min_interval_sec = self.min_node_interval_ms / 1000.0
        node_times = {}
        
        for task in tasks:
            if task.node_id not in node_times:
                node_times[task.node_id] = []
            node_times[task.node_id].append(task.scheduled_time)
        
        # 检查每个节点的出块时间间隔
        for node_id, times in node_times.items():
            sorted_times = sorted(times)
            for i in range(1, len(sorted_times)):
                interval = sorted_times[i] - sorted_times[i-1]
                if interval < min_interval_sec:
                    return False
        
        return True


class ResultCollector:
    """结果收集器"""
    
    def __init__(self, max_failures: int):
        self.result_queue = Queue()
        self.max_failures = max_failures
        
        self.total_submitted = 0
        self.total_completed = 0
        self.total_failures = 0
        self.rpc_times = []

        self._lock = threading.Lock()
        
    def submit_result(self, result: BlockResult):
        """提交一个结果"""
        self.result_queue.put(result)
    
    def process_results_and_assert_healthy(self):
        """
        处理队列中的所有结果，返回是否应该继续运行（失败次数未超限）
        """
        while not self.result_queue.empty():
            result: BlockResult = self.result_queue.get()
            with self._lock:
                self._process_result(result)
                if self.total_failures > self.max_failures:
                    raise Exception(f"Too many block generation fails: {self.total_failures}")
    
    def _process_result(self, result: BlockResult):
        self.total_completed += 1
        
        if result.success:
            self.rpc_times.append(result.rpc_time)
        else:
            self.total_failures += 1
            logger.info(
                f"Block {result.block_id} generation failed on node {result.node_id}: "
                f"{result.error_msg}"
            )
            
    def increment_submitted(self):
        """增加已提交计数"""
        with self._lock:
            self.total_submitted += 1
    
    def get_stats(self) -> dict:
        """获取当前统计信息"""
        with self._lock:
            return {
                'submitted': self.total_submitted,
                'completed': self.total_completed,
                'failures': self.total_failures,
                'success_rate': (self.total_completed - self.total_failures) / max(1, self.total_completed),
                'avg_rpc_time': sum(self.rpc_times) / len(self.rpc_times) if self.rpc_times else 0
            }


class BlockGenerationScheduler:
    """区块生成调度器"""
    
    def __init__(self, nodes: List[RemoteNode], max_block_size_in_bytes, *, max_failures=0):
        self.nodes = {node.id: node for node in nodes}
        self.collector = ResultCollector(max_failures=max_failures)
        self.max_block_size_in_bytes = max_block_size_in_bytes
        
    def execute(self, tasks: List[BlockTask]):
        """执行区块生成计划"""

        # 启动定时统计线程
        stats_reporter = StatisticsReporter(
            collector=self.collector,
            interval_sec=5.0  
        )
        stats_reporter.start()
        
        for task in tasks:
            self._execute_next_task(task)

        # 等待所有任务完成
        self._wait_all_complete()

        stats_reporter.stop()
        stats_reporter.join(timeout=1.0)
        
        # 最终统计
        self._report_final_stats()

    def _execute_next_task(self, task: BlockTask):
        # 等待到计划时间
        current_time = time.time()
        wait_time = task.scheduled_time - current_time

        if wait_time > 0:
            time.sleep(wait_time)
        elif wait_time < -0.01:
            logger.warning(f"生成区块 {task.block_id} 晚于计划时间 {wait_time*1000} 毫秒")
        
        # 启动生成线程
        self._spawn_generation_thread(task)
        
        # 查看是否 health
        self.collector.process_results_and_assert_healthy()
    
    
    def _spawn_generation_thread(self, task: BlockTask):
        """启动区块生成线程"""
        thread = SimpleGenerateThread(
            node=self.nodes[task.node_id],
            block_id=task.block_id,
            max_block_size=self.max_block_size_in_bytes,
            collector=self.collector,
        )
        thread.start()
        self.collector.increment_submitted()

    
    def _wait_all_complete(self):
        """等待所有生成任务完成"""
        def check():
            self.collector.process_results_and_assert_healthy()
            return self.collector.total_completed == self.collector.total_submitted
            
        wait_until(check, timeout=60)        
        
    
    def _report_progress(self, block_id: int, start_time: float):
        """报告进度"""
        stats = self.collector.get_stats()
        elapsed = time.time() - start_time
        
        logger.info(
            f"[PROGRESS] Block {block_id}: "
            f"Submitted={stats['submitted']}, "
            f"Completed={stats['completed']}, "
            f"Failures={stats['failures']}, "
            f"Success Rate={stats['success_rate']:.2%}, "
            f"Avg RPC Time={stats['avg_rpc_time']:.3f}s, "
            f"Elapsed={elapsed:.1f}s"
        )
    
    def _report_final_stats(self):
        """报告最终统计"""
        stats = self.collector.get_stats()
        logger.info(
            f"[FINAL] Block generation completed: "
            f"Total={stats['submitted']}, "
            f"Success={stats['completed'] - stats['failures']}, "
            f"Failures={stats['failures']}, "
            f"Success Rate={stats['success_rate']:.2%}"
        )


def is_hex_hash(input) -> bool:
    if type(input) is not str:
        return False
    
    pattern_strict = r'^0x[0-9a-f]{64}$'

    return re.match(pattern_strict, input) is not None



class SimpleGenerateThread(threading.Thread):
    def __init__(self, node: RemoteNode, block_id: int, max_block_size: int, collector: ResultCollector):
        threading.Thread.__init__(self, daemon=True)
        self.node = node        
        self.block_id = block_id
        self.collector = collector
        self.max_block_size = max_block_size

    def run(self):
        try:
            start = time.time()
            hash = self.node.rpc.test_generateOneBlock(10000000, self.max_block_size)
            
            if not is_hex_hash(hash):
                raise Exception(f"Unexpected return valu {hash}")

            rpc_time = round(time.time() - start, 3)
            logger.debug(f"node {self.node.id} generate block {hash}, rpc time {rpc_time}")
            success = True
            error_msg = None
        except Exception as e:
            rpc_time = math.inf
            success = False
            error_msg = str(e)
        finally:
            res = BlockResult(
                block_id=self.block_id,
                node_id=self.node.id,
                success=success,
                rpc_time=rpc_time,
                error_msg=error_msg
            )
            self.collector.submit_result(res)



class StatisticsReporter(threading.Thread):
    """定时统计报告线程"""
    
    def __init__(self, collector: ResultCollector, interval_sec: float = 5.0):
        super().__init__(daemon=True)
        self.collector = collector
        self.logger = logger
        self.interval_sec = interval_sec
        self.should_stop = threading.Event()
        self.start_time = time.time()
        
    def run(self):
        while not self.should_stop.is_set():
            time.sleep(self.interval_sec)
            self._report()
    
    def _report(self):
        """输出当前统计"""
        stats = self.collector.get_stats()
        elapsed = time.time() - self.start_time
        
        self.logger.info(
            f"[STATS] Elapsed={elapsed:.1f}s, "
            f"Submitted={stats['submitted']}, "
            f"Completed={stats['completed']}, "
            f"Failures={stats['failures']}, "
            f"Pending={stats['submitted'] - stats['completed']}, "
            f"Success Rate={stats['success_rate']:.2%}, "
            f"Avg RPC Time={stats['avg_rpc_time']:.3f}s"
        )
    
    def stop(self):
        """停止统计线程"""
        self.should_stop.set()

# 主函数
def generate_blocks_async(nodes: List[RemoteNode], num_blocks: int, max_block_size_in_bytes: int, generation_period_ms:int, min_node_interval_ms: int=100):
    """重构后的异步区块生成函数"""
    
    # 1. 生成出块计划
    planner = BlockGenerationPlan(
        nodes=nodes,
        num_blocks=num_blocks,
        generation_period_ms=generation_period_ms,
        min_node_interval_ms=min_node_interval_ms 
    )
    
    tasks = planner.generate()
    
    # 2. 验证计划
    if not planner.validate(tasks):
        logger.error("Generated plan violates node interval constraints")
        return
    
    logger.info(
        f"Generated block plan: {len(tasks)} blocks across {len(nodes)} nodes"
    )
    
    # 3. 执行计划
    scheduler = BlockGenerationScheduler(nodes=nodes, max_block_size_in_bytes=max_block_size_in_bytes)
    scheduler.execute(tasks)