from collections import Counter
from concurrent.futures import ThreadPoolExecutor
import threading
from typing import List, Tuple

from loguru import logger

from remote_simulation import docker_cmds
from utils import shell_cmds
from utils.counter import AtomicCounter
from utils.wait_until import wait_until

from .remote_node import RemoteNode


from collections import Counter, defaultdict


def check_nodes_synced(executor: ThreadPoolExecutor, nodes: List[RemoteNode]):
    def get_best_block(node: RemoteNode):
        try: 
            return node.rpc.cfx_getBestBlockHash()
        except Exception as e:
            logger.info(f"Fail to connect {node.rpc.addr}: {e}")
            return None

    best_blocks = list(executor.map(get_best_block, nodes))

    logger.debug("best blocks: {}".format(Counter(best_blocks).most_common(5)))
    
    # 建立 block hash 到节点的映射
    block_to_nodes = defaultdict(list)
    for node, block_hash in zip(nodes, best_blocks):
        if block_hash is not None:
            block_to_nodes[block_hash].append(node.id)
    
    # 找出 cnt <= 5 的 block hash 及其对应的节点 id
    rare_blocks_info = [(block_hash, len(node_ids), node_ids) for block_hash, node_ids in block_to_nodes.items() if len(node_ids) <= 5]
    
    if len(rare_blocks_info) > 0:
        logger.debug("出现次数不超过5的区块及其节点:")
        for (block_hash, cnt, node_ids) in rare_blocks_info:
            logger.debug(f"  区块 {block_hash}: 出现 {cnt} 次, 节点 ID: {",".join(node_ids)}")
    
    most_common = Counter(best_blocks).most_common(1)
    if not most_common:
        logger.warning("无法获取任何节点的最佳区块")
        return False
    
    most_common_cnt = most_common[0][1]

    if most_common_cnt == len(nodes):
        logger.info("所有节点已同步")
        return True
    else:
        return False


def wait_for_nodes_synced(nodes: List[RemoteNode], *, max_workers: int = 300, retry_interval: int = 5, timeout: int = 120):
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        wait_until(lambda: check_nodes_synced(executor, nodes), timeout=timeout, retry_interval=retry_interval)

def init_tx_gen(nodes: List[RemoteNode], txgen_account_count:int, max_workers: int = 300):
    def execute(args: Tuple[int, RemoteNode]):
        idx, node = args
        return node.init_tx_gen(idx * txgen_account_count)
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        results = executor.map(execute, enumerate(nodes))

    fail_cnt = list(results).count(False)

    if fail_cnt == len(nodes):
        logger.error(f"全部节点设置交易生成失败")
    elif fail_cnt > 0:
        logger.warning(f"部分节点设置交易生成失败，数量 {fail_cnt}")
    else:
        logger.success(f"全部节点设置交易生成成功")

def _stop_node_and_collect_log(node: RemoteNode, *, counter1: AtomicCounter, counter2: AtomicCounter, total_cnt: int, local_path: str = "./logs"):
    try:
        shell_cmds.ssh(node.host_spec.ip, node.host_spec.ssh_user, docker_cmds.stop_node_and_collect_log(node.index, user = node.host_spec.ssh_user))
        cnt1 = counter1.increment()
        logger.debug(f"节点 {node.id} 已完成日志生成 ({cnt1}/{total_cnt})")

        shell_cmds.rsync_download(f"./output{node.index}/", f"./{local_path}/{node.id}/", node.host_spec.ip, user = node.host_spec.ssh_user)
        cnt2 = counter2.increment()
        logger.debug(f"节点 {node.id} 已完成日志同步 ({cnt2}/{total_cnt})")

        return 0
    except Exception as e:
        logger.warning(f"节点 {node.id} 日志生成遇到问题: {e}")
        return 1
    
def collect_logs(nodes: List[RemoteNode], local_path: str = "./logs", *, max_workers: int = 100):
    counter1 = AtomicCounter()
    counter2 = AtomicCounter()
    total_cnt = len(nodes)
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        results = executor.map(lambda node: _stop_node_and_collect_log(node, local_path=local_path, counter1=counter1, counter2=counter2, total_cnt=total_cnt), nodes)
    
    fail_cnt = sum(results)