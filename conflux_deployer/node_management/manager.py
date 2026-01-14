"""
Conflux Node Management Module

Handles Conflux-specific operations:
- Generating and deploying node configurations
- Starting and stopping Conflux nodes
- Monitoring node status
- Connecting nodes in the P2P network
"""

import json
import time
import random
from typing import Dict, List, Optional, Tuple, Any, Set
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field

from loguru import logger

from ..configs import (
    DeploymentConfig,
    InstanceInfo,
    NodeInfo,
    ConfluxNodeConfig,
    NetworkConfig,
)
from ..utils.remote import RemoteExecutor
from .rpc import ConfluxRpcClient


@dataclass
class ConfluxTomlConfig:
    """Conflux node TOML configuration"""
    # Basic
    mode: str = "full"
    public_address: str = ""
    
    # Network
    chain_id: int = 1
    tcp_port: int = 32323
    udp_port: int = 32323
    jsonrpc_http_port: int = 12537
    jsonrpc_ws_port: int = 12538
    bootnodes: str = ""
    
    # Performance
    tx_pool_size: int = 500000
    
    # Storage
    conflux_data_dir: str = "/data"
    db_cache_size: int = 128
    ledger_cache_size: int = 1024
    
    # Mining
    mining_author: Optional[str] = None
    start_mining: bool = False
    
    # Transaction generation
    generate_tx: bool = False
    generate_tx_period_us: int = 100000
    genesis_secrets: Optional[str] = None
    txgen_account_count: int = 1000
    
    # Additional settings
    extra: Dict[str, Any] = field(default_factory=dict)
    
    def to_toml(self) -> str:
        """Convert to TOML format string"""
        lines = []
        
        lines.append(f'mode = "{self.mode}"')
        if self.public_address:
            lines.append(f'public_address = "{self.public_address}"')
        
        lines.append("")
        lines.append(f"chain_id = {self.chain_id}")
        lines.append(f"tcp_port = {self.tcp_port}")
        lines.append(f"udp_port = {self.udp_port}")
        lines.append(f"jsonrpc_http_port = {self.jsonrpc_http_port}")
        lines.append(f"jsonrpc_ws_port = {self.jsonrpc_ws_port}")
        
        if self.bootnodes:
            lines.append(f'bootnodes = "{self.bootnodes}"')
        
        lines.append("")
        lines.append(f"tx_pool_size = {self.tx_pool_size}")
        
        lines.append("")
        lines.append(f'conflux_data_dir = "{self.conflux_data_dir}"')
        lines.append(f"db_cache_size = {self.db_cache_size}")
        lines.append(f"ledger_cache_size = {self.ledger_cache_size}")
        
        if self.mining_author:
            lines.append("")
            lines.append(f'mining_author = "{self.mining_author}"')
            if self.start_mining:
                lines.append("start_mining = true")
        
        if self.generate_tx:
            lines.append("")
            lines.append("generate_tx = true")
            lines.append(f"generate_tx_period_us = {self.generate_tx_period_us}")
            if self.genesis_secrets:
                lines.append(f'genesis_secrets = "{self.genesis_secrets}"')
            lines.append(f"txgen_account_count = {self.txgen_account_count}")
        
        # Add extra settings
        if self.extra:
            lines.append("")
            for key, value in self.extra.items():
                if isinstance(value, str):
                    lines.append(f'{key} = "{value}"')
                elif isinstance(value, bool):
                    lines.append(f'{key} = {"true" if value else "false"}')
                else:
                    lines.append(f'{key} = {value}')
        
        return "\n".join(lines)


class NodeManager:
    """
    Manages Conflux nodes across deployed instances.
    
    Responsibilities:
    - Generate node configurations
    - Deploy configurations to instances
    - Start/stop nodes
    - Monitor node status
    - Connect nodes in P2P network
    """
    
    def __init__(
        self,
        config: DeploymentConfig,
        instances: List[InstanceInfo],
        ssh_key_path: Optional[str] = None,
    ):
        """
        Initialize the node manager.
        
        Args:
            config: Deployment configuration
            instances: List of deployed instances
            ssh_key_path: Path to SSH private key
        """
        self.config = config
        self.instances = instances
        self.ssh_key_path = ssh_key_path or config.ssh_private_key_path
        self.executor = RemoteExecutor(ssh_key_path=self.ssh_key_path)
        
        self._nodes: List[NodeInfo] = []
        self._rpc_clients: Dict[str, ConfluxRpcClient] = {}
    
    @property
    def nodes(self) -> List[NodeInfo]:
        """Get all nodes"""
        return self._nodes.copy()
    
    @property
    def hosts(self) -> List[str]:
        """Get all host IPs"""
        return [i.public_ip for i in self.instances if i.public_ip]
    
    def _build_nodes_list(self) -> List[NodeInfo]:
        """Build the list of nodes from instances"""
        nodes = []
        node_id = 0
        
        import hashlib

        ports_block = int(getattr(self.config.conflux_node, "ports_block_size", 1000))
        base_p2p = self.config.conflux_node.p2p_port_base
        base_jsonrpc = self.config.conflux_node.jsonrpc_port_base

        # Compute maximum number of non-overlapping blocks that fit in port range
        max_port = 65535
        max_blocks = max(1, (max_port - max(base_p2p, base_jsonrpc)) // ports_block)

        for instance in self.instances:
            if not instance.public_ip:
                continue

            # Deterministic block index per instance using hash, avoids state persistence
            digest = hashlib.sha256(instance.instance_id.encode()).digest()[:4]
            block_index = int.from_bytes(digest, "big") % max_blocks
            allocation_offset = block_index * ports_block

            for node_index in range(instance.nodes_count):
                p2p_port = base_p2p + allocation_offset + node_index
                jsonrpc_port = base_jsonrpc + allocation_offset + node_index

                node = NodeInfo(
                    node_id=f"node-{node_id}",
                    instance_info=instance,
                    node_index=node_index,
                    p2p_port=p2p_port,
                    jsonrpc_port=jsonrpc_port,
                )
                nodes.append(node)
                node_id += 1

        return nodes

    def list_nodes_by_instance(self, instance_id: str) -> List[NodeInfo]:
        """Return all nodes that belong to the specified instance."""
        return [n for n in self._nodes if getattr(n.instance_info, "instance_id", None) == instance_id
        ]
    
    def initialize_nodes(self) -> List[NodeInfo]:
        """
        Initialize the node list from instances.
        
        Returns:
            List of NodeInfo
        """
        self._nodes = self._build_nodes_list()
        logger.info(f"Initialized {len(self._nodes)} nodes across {len(self.instances)} instances")
        return self._nodes
    
    def generate_node_config(
        self, 
        node: NodeInfo,
        bootnodes: Optional[List[str]] = None,
        enable_tx_gen: bool = False,
        tx_gen_period_us: int = 100000,
    ) -> ConfluxTomlConfig:
        """
        Generate configuration for a single node.
        
        Args:
            node: Node information
            bootnodes: List of bootnode addresses
            enable_tx_gen: Whether to enable transaction generation
            tx_gen_period_us: Transaction generation period
            
        Returns:
            ConfluxTomlConfig
        """
        base_config = self.config.conflux_node
        
        config = ConfluxTomlConfig(
            public_address=f"{node.instance_info.public_ip}:{node.p2p_port}",
            chain_id=base_config.chain_id,
            tcp_port=node.p2p_port,
            udp_port=node.p2p_port,
            jsonrpc_http_port=node.jsonrpc_port,
            jsonrpc_ws_port=node.jsonrpc_port + 1,
            tx_pool_size=base_config.tx_pool_size,
            conflux_data_dir=f"/data/conflux/data/node_{node.node_index}",
        )
        
        # Set bootnodes
        if bootnodes:
            config.bootnodes = ",".join(bootnodes)
        
        # Set mining author if provided
        if base_config.mining_author:
            config.mining_author = base_config.mining_author
        
        # Enable tx generation
        if enable_tx_gen:
            config.generate_tx = True
            config.generate_tx_period_us = tx_gen_period_us
            if self.config.network and self.config.network.genesis_secrets_path:
                config.genesis_secrets = "/data/conflux/config/genesis_secrets.txt"
        
        # Add extra config
        config.extra.update(base_config.extra_config)
        
        return config
    
    def deploy_configurations(
        self, 
        bootnodes: Optional[List[str]] = None,
        enable_tx_gen: bool = False,
    ) -> Dict[str, bool]:
        """
        Deploy configurations to all nodes.
        
        Args:
            bootnodes: List of bootnode addresses
            enable_tx_gen: Whether to enable transaction generation
            
        Returns:
            Dict mapping node_id to success status
        """
        results: Dict[str, bool] = {}

        # Group nodes by host and build remote path -> content mapping
        host_to_files: Dict[str, Dict[str, str]] = {}
        host_path_to_node: Dict[tuple[str, str], str] = {}

        # Calculate tx generation period
        total_nodes = len(self._nodes)
        tx_gen_period = 1000000 * total_nodes // self.config.network.target_tps

        for node in self._nodes:
            host = node.instance_info.public_ip
            if not host:
                continue
            try:
                config = self.generate_node_config(
                    node,
                    bootnodes=bootnodes,
                    enable_tx_gen=enable_tx_gen,
                    tx_gen_period_us=tx_gen_period,
                )
                config_path = f"/data/conflux/config/conflux_{node.node_index}.toml"
                host_to_files.setdefault(host, {})[config_path] = config.to_toml()
                host_path_to_node[(host, config_path)] = node.node_id
            except Exception as e:
                logger.error(f"Error generating config for {node.node_id}: {e}")
                results[node.node_id] = False

        if host_to_files:
            by_host = self.executor.copy_contents_on_hosts(host_to_files, max_workers=50, retry=3, timeout=300)
            for host, file_results in by_host.items():
                for path, ok in file_results.items():
                    node_id = host_path_to_node.get((host, path))
                    if node_id:
                        results[node_id] = bool(ok)

        success_count = sum(1 for v in results.values() if v)
        logger.info(f"Deployed configurations to {success_count}/{len(results)} nodes")
        return results
    
    def start_nodes(self) -> Dict[str, bool]:
        """
        Start all Conflux nodes.
        
        Returns:
            Dict mapping node_id to success status
        """
        results: Dict[str, bool] = {}

        # Build per-host command map (node_id -> command)
        host_to_commands: Dict[str, Dict[str, str]] = {}
        for node in self._nodes:
            host = node.instance_info.public_ip
            if not host:
                continue
            host_to_commands.setdefault(host, {})[node.node_id] = f"/usr/local/bin/start_conflux.sh {node.node_index}"

        if host_to_commands:
            res = self.executor.execute_commands_on_hosts(host_to_commands, max_workers=50, retry=3, timeout=300)
            for host_results in res.values():
                for node_id, cmd_res in host_results.items():
                    results[node_id] = bool(cmd_res.success)
                    if cmd_res.success:
                        logger.debug(f"Started node {node_id}")
                    else:
                        logger.warning(f"Failed to start node {node_id}: {cmd_res.stderr}")

        success_count = sum(1 for v in results.values() if v)
        logger.info(f"Started {success_count}/{len(results)} nodes")
        return results
    
    def stop_nodes(self) -> Dict[str, bool]:
        """
        Stop all Conflux nodes.
        
        Returns:
            Dict mapping node_id to success status
        """
        results: Dict[str, bool] = {}

        hosts = [h for h in self.hosts if h]
        if not hosts:
            return results

        cmd = "/usr/local/bin/stop_all_conflux.sh"
        host_results = self.executor.execute_on_all(hosts, cmd, max_workers=50, retry=3, timeout=300)

        for node in self._nodes:
            host = node.instance_info.public_ip
            if not host:
                continue
            host_res = host_results.get(host)
            results[node.node_id] = bool(host_res.success) if host_res is not None else False

        success_count = sum(1 for v in results.values() if v)
        logger.info(f"Stopped {success_count}/{len(results)} nodes")
        return results
    
    def get_rpc_client(self, node: NodeInfo) -> ConfluxRpcClient:
        """
        Get or create an RPC client for a node.
        
        Args:
            node: Node information
            
        Returns:
            ConfluxRpcClient
        """
        if node.node_id not in self._rpc_clients:
            self._rpc_clients[node.node_id] = ConfluxRpcClient(node.rpc_url)
        return self._rpc_clients[node.node_id]
    
    def wait_for_nodes_ready(
        self, 
        timeout_seconds: int = 120,
        required_phase: str = "NormalSyncPhase",
    ) -> Dict[str, bool]:
        """
        Wait for all nodes to be ready.
        
        Args:
            timeout_seconds: Maximum time to wait
            required_phase: Required sync phase
            
        Returns:
            Dict mapping node_id to ready status
        """
        results: Dict[str, bool] = {}
        start_time = time.time()
        
        remaining_nodes = set(n.node_id for n in self._nodes)
        
        while remaining_nodes and (time.time() - start_time) < timeout_seconds:
            for node in self._nodes:
                if node.node_id not in remaining_nodes:
                    continue
                
                try:
                    client = self.get_rpc_client(node)
                    phase = client.get_sync_phase()
                    
                    if phase == required_phase:
                        results[node.node_id] = True
                        remaining_nodes.remove(node.node_id)
                        node.is_ready = True
                        logger.debug(f"Node {node.node_id} is ready")
                        
                except Exception as e:
                    logger.debug(f"Node {node.node_id} not ready: {e}")
            
            if remaining_nodes:
                time.sleep(2)
        
        # Mark remaining as not ready
        for node_id in remaining_nodes:
            results[node_id] = False
        
        ready_count = sum(1 for v in results.values() if v)
        logger.info(f"{ready_count}/{len(results)} nodes are ready")
        
        return results
    
    def get_node_ids(self) -> Dict[str, str]:
        """
        Get public keys (node IDs) for all nodes.
        
        Returns:
            Dict mapping node_id to public key
        """
        results: Dict[str, str] = {}
        
        for node in self._nodes:
            try:
                client = self.get_rpc_client(node)
                node_key = client.get_node_id()
                node.public_key = node_key
                results[node.node_id] = node_key
                logger.debug(f"Got node ID for {node.node_id}: {node_key[:16]}...")
            except Exception as e:
                logger.warning(f"Failed to get node ID for {node.node_id}: {e}")
        
        logger.info(f"Got node IDs for {len(results)}/{len(self._nodes)} nodes")
        return results
    
    def connect_nodes(
        self, 
        connect_count: int = 3,
        timeout_seconds: int = 120,
    ) -> bool:
        """
        Connect nodes in the P2P network.
        
        Each node connects to `connect_count` random peers.
        
        Args:
            connect_count: Number of peers each node should connect to
            timeout_seconds: Timeout for connection
            
        Returns:
            True if all nodes connected successfully
        """
        if len(self._nodes) < 2:
            logger.warning("Not enough nodes to connect")
            return False
        
        # Get node IDs first
        self.get_node_ids()
        
        # Build enode addresses
        for node in self._nodes:
            if node.public_key:
                node.enode = f"cfxnode://{node.public_key}@{node.p2p_address}"
        
        # Connect each node to random peers
        connection_success = True
        
        for node in self._nodes:
            if not node.public_key:
                continue
            
            # Select random peers (excluding self)
            other_nodes = [n for n in self._nodes if n.node_id != node.node_id and n.enode]
            
            if len(other_nodes) < connect_count:
                peers = other_nodes
            else:
                peers = random.sample(other_nodes, connect_count)
            
            client = self.get_rpc_client(node)
            
            for peer in peers:
                try:
                    client.add_peer(peer.enode)
                    logger.debug(f"Connected {node.node_id} to {peer.node_id}")
                except Exception as e:
                    logger.warning(f"Failed to connect {node.node_id} to {peer.node_id}: {e}")
                    connection_success = False
        
        # Wait for connections to establish
        logger.info("Waiting for P2P connections to establish...")
        time.sleep(10)
        
        # Verify connections
        connected_count = 0
        for node in self._nodes:
            try:
                client = self.get_rpc_client(node)
                peers = client.get_peer_count()
                if peers > 0:
                    connected_count += 1
                logger.debug(f"Node {node.node_id} has {peers} peers")
            except Exception as e:
                logger.warning(f"Failed to check peers for {node.node_id}: {e}")
        
        logger.info(f"{connected_count}/{len(self._nodes)} nodes have peers")
        
        return connected_count == len(self._nodes)
    
    def wait_for_sync(self, timeout_seconds: int = 300) -> bool:
        """
        Wait for all nodes to sync.
        
        Args:
            timeout_seconds: Maximum time to wait
            
        Returns:
            True if all nodes synced
        """
        start_time = time.time()
        
        while (time.time() - start_time) < timeout_seconds:
            epochs = []
            
            for node in self._nodes:
                try:
                    client = self.get_rpc_client(node)
                    epoch = client.get_epoch_number()
                    epochs.append(epoch)
                except Exception:
                    pass
            
            if len(epochs) == len(self._nodes):
                max_epoch = max(epochs)
                min_epoch = min(epochs)
                
                if max_epoch - min_epoch <= 1:
                    logger.info(f"All nodes synced at epoch {max_epoch}")
                    return True
            
            time.sleep(5)
        
        logger.warning("Nodes did not sync within timeout")
        return False
    
    def get_bootnodes(self, count: int = 5) -> List[str]:
        """
        Get bootnode addresses from ready nodes.
        
        Args:
            count: Number of bootnodes to return
            
        Returns:
            List of enode addresses
        """
        bootnodes = []
        
        for node in self._nodes:
            if node.enode:
                bootnodes.append(node.enode)
                if len(bootnodes) >= count:
                    break
        
        return bootnodes
    
    def collect_metrics(self) -> Dict[str, Dict[str, Any]]:
        """
        Collect metrics from all nodes.
        
        Returns:
            Dict mapping node_id to metrics
        """
        metrics: Dict[str, Dict[str, Any]] = {}
        
        for node in self._nodes:
            try:
                client = self.get_rpc_client(node)
                
                metrics[node.node_id] = {
                    "epoch": client.get_epoch_number(),
                    "block_count": client.get_block_count(),
                    "peer_count": client.get_peer_count(),
                    "instance_type": node.instance_info.instance_type,
                    "location": node.instance_info.location_name,
                    "provider": node.instance_info.provider.value,
                }
            except Exception as e:
                logger.warning(f"Failed to collect metrics from {node.node_id}: {e}")
                metrics[node.node_id] = {"error": str(e)}
        
        return metrics
