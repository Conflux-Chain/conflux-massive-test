from concurrent.futures import ThreadPoolExecutor, Future
from typing import List, Tuple

from loguru import logger

from remote_simulation.network_topology import NetworkTopology
from remote_simulation.remote_node import RemoteNode
from utils.wait_until import wait_until


def connect_nodes(nodes: List[RemoteNode],
                    topology: NetworkTopology,
                    connection_timeout: int = 120,
                    handshake_timeout: int = 120,
                    max_workers: int = 300,
                    min_peers: int = 3,
                    ):
    return NetworkConnector(nodes, topology, connection_timeout,
                     handshake_timeout, max_workers)._connect_topology(min_peers)


class NetworkConnector:
    """网络连接管理器"""

    def __init__(
        self,
        nodes: List[RemoteNode],
        topology: NetworkTopology,
        connection_timeout: int = 60,
        handshake_timeout: int = 60,
        max_workers: int = 300
    ):
        """
        Args:
            nodes: 节点列表
            connection_timeout: 单个连接的超时时间（秒）
            handshake_timeout: 握手超时时间（秒）
            max_workers: 线程池最大线程数，默认为节点数
        """
        self.nodes: List[RemoteNode] = nodes
        self.topology = topology
        self.connection_timeout = connection_timeout
        self.handshake_timeout = handshake_timeout
        self.max_workers = max_workers

    def _connect_topology(
        self,
        min_peers: int = 3
    ) -> None:
        """
        根据拓扑结构建立网络连接

        Args:
            topology: 网络拓扑
            min_peers: 最小对等节点数量要求

        Raises:
            Exception: 连接失败节点数超过阈值时抛出
        """
        # 第一步：提交所有连接任务
        executor = ThreadPoolExecutor(max_workers=self.max_workers)
        futures = self._submit_connection_tasks(executor, min_peers)

        # 第二步：收集结果
        success_count, failed_nodes = self._collect_results(futures)

        # 第三步：清理资源
        executor.shutdown(wait=True)

        if len(failed_nodes) > 0:
            raise Exception("部分节点建立连接失败")

    def _submit_connection_tasks(
        self,
        executor: ThreadPoolExecutor,
        min_peers: int
    ) -> List[Tuple[int, Future]]:
        """提交所有连接任务"""
        futures = []

        for node_idx in range(len(self.nodes)):
            peers_with_latencies = self.topology.get_peers_with_latency(node_idx)

            future = executor.submit(
                self._connect_node,
                node_idx,
                peers_with_latencies,
                min_peers
            )

            futures.append((node_idx, future))

        return futures

    def _connect_node(
        self,
        node_idx: int,
        peers_with_latencies: List[Tuple[int, int]],
        min_peers: int
    ) -> bool:
        """
        连接单个节点到其所有对等节点

        Args:
            node_idx: 节点索引
            peers: 对等节点索引列表
            latencies: 延迟映射
            min_peers: 最小对等节点数

        Returns:
            bool: 是否成功
        """
        node = self.nodes[node_idx]
        
        try:
            # 建立所有连接
            for peer_idx, latency in peers_with_latencies:
                self._establish_connection(node_idx, peer_idx, latency)

            valid_peers = node.rpc.test_getPeerInfo()

            # 等待连接稳定
            if len(valid_peers) < min_peers:
                logger.warning(f"Node {node.id} build p2p connection error: not enough peers {len(valid_peers)} < {min_peers}")
                return False

            return True

        except Exception as e:
            logger.warning(
                f"Node {node.id} build p2p connection error: {e}")
            return False

    def _collect_results(
        self,
        futures: List[tuple[int, Future]]
    ) -> tuple[int, List[int]]:
        """收集连接结果"""
        success_count = 0
        failed_nodes = []

        for node_idx, future in futures:
            node = self.nodes[node_idx]
            try:
                is_success = future.result(timeout=self.connection_timeout)

                if is_success:
                    success_count += 1
                else:
                    failed_nodes.append(node_idx)

            except Exception as e:
                logger.warning(
                    f"Node {node.id} connection raised exception: {e}")
                failed_nodes.append(node_idx)

        return success_count, failed_nodes

    def _establish_connection(self, from_idx: int, to_idx: int, latency: int) -> None:
        """建立两个节点间的连接"""
        from_node = self.nodes[from_idx]
        to_node = self.nodes[to_idx]

        from_node.rpc.test_addNode(to_node.key, to_node.p2p_addr)
        wait_until(lambda: _check_handshake(from_node, to_node.key), timeout=self.handshake_timeout)

        # 配置网络延迟
        if latency > 0:
            from_node.rpc.test_addLatency(self.nodes[to_idx].key, latency)


def _check_handshake(node: RemoteNode, peer_key: str) -> bool:
    """等待握手完成"""

    peers = node.rpc.test_getPeerInfo()
    # Too many logs in thousands of 
    # logger.debug(f"{node.id} get peers {peer_key}, len {len(peers)}")

    for peer in peers:
        has_valid_protocol = len(peer.get('protocols', [])) > 0
        if peer["nodeid"] == peer_key and has_valid_protocol:
            return True

    return False
