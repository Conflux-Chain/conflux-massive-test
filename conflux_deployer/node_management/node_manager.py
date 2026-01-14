"""Node Manager

NOTE: This module is a legacy/simple node manager used by some tests and scripts.
The main production node management lives in conflux_deployer/node_management/manager.py.

This implementation now mirrors the remote_simulation startup logic used by remote_simulate.py:
- generate a config file via remote_simulation.config_builder
- copy it to each host as ~/config.toml
- (optionally) pull docker image
- destroy any existing nodes
- docker run nodes and wait for ready
"""

import time
import pickle
from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass
from pathlib import Path

from loguru import logger

from conflux_deployer.configs.config_manager import ConfigManager
from conflux_deployer.server_deployment.server_deployer import ServerInstance

from remote_simulation.config_builder import ConfluxOptions, SimulateOptions, generate_config_file
from remote_simulation.launch_conflux_node import launch_remote_nodes, stop_remote_nodes, destory_remote_nodes
from remote_simulation.port_allocation import remote_rpc_port
from remote_simulation.remote_node import RemoteNode


@dataclass
class NodeInfo:
    """Conflux node information"""
    node_id: str
    instance_id: str
    ip_address: str
    port: int
    rpc_port: int
    p2p_port: int
    region: str
    cloud_provider: str
    instance_type: str
    status: str
    created_at: str
    last_updated: str


class NodeManager:
    """Node Manager for Conflux nodes"""
    
    def __init__(self, config_manager: ConfigManager):
        """Initialize Node Manager"""
        self.config_manager = config_manager
        self.nodes: Dict[str, NodeInfo] = {}
        self.instance_nodes: Dict[str, List[str]] = {}
        # Port allocations per instance_id -> allocation offset (int)
        self.port_allocations: Dict[str, int] = {}
        self.state_file = Path("node_state.pkl")
        self._load_state()
    
    def collect_node_info(self, instance: ServerInstance) -> List[NodeInfo]:
        """Collect node information from server instance"""
        logger.info(f"Collecting node information from instance {instance.instance_id}")
        
        node_info_list = []
        conflux_config = self.config_manager.get_conflux_config()
        base_port = conflux_config.get("base_port", 12537)
        base_rpc_port = conflux_config.get("base_rpc_port", 12539)
        base_p2p_port = conflux_config.get("base_p2p_port", 12538)
        ports_block = int(conflux_config.get("ports_block_size", 1000))

        # Allocate a per-instance block of ports (persisted) to avoid collisions
        if instance.instance_id not in self.port_allocations:
            # Find next available offset (start from 0)
            used_offsets = sorted(self.port_allocations.values())
            next_offset = (used_offsets[-1] + ports_block) if used_offsets else 0
            self.port_allocations[instance.instance_id] = next_offset
        allocation_offset = self.port_allocations[instance.instance_id]

        # Generate node info for each node on the instance
        for i in range(instance.nodes_count):
            node_id = f"node-{instance.instance_id}-{i}"
            port = base_port + allocation_offset + i
            rpc_port = base_rpc_port + allocation_offset + i
            p2p_port = base_p2p_port + allocation_offset + i
            
            node_info = NodeInfo(
                node_id=node_id,
                instance_id=instance.instance_id,
                ip_address=instance.ip_address,
                port=port,
                rpc_port=rpc_port,
                p2p_port=p2p_port,
                region=instance.region,
                cloud_provider=instance.cloud_provider,
                instance_type=instance.instance_type,
                status="pending",
                created_at=time.strftime("%Y-%m-%d %H:%M:%S"),
                last_updated=time.strftime("%Y-%m-%d %H:%M:%S")
            )
            
            node_info_list.append(node_info)
            self.nodes[node_id] = node_info
        
        # Store instance to nodes mapping
        self.instance_nodes[instance.instance_id] = [node.node_id for node in node_info_list]
        
        # Save state
        self._save_state()
        
        logger.info(f"Collected information for {len(node_info_list)} nodes on instance {instance.instance_id}")
        return node_info_list
    
    @staticmethod
    def _safe_conflux_options_from_dict(node_cfg: Dict[str, Any]) -> ConfluxOptions:
        allowed = set(ConfluxOptions.__annotations__.keys())
        filtered = {k: v for k, v in (node_cfg or {}).items() if k in allowed}
        return ConfluxOptions(**filtered)

    def _build_remote_simulation_config(self) -> Tuple[SimulateOptions, ConfluxOptions]:
        conflux_cfg = self.config_manager.get_conflux_config() or {}
        node_cfg = conflux_cfg.get("node_config") or {}

        # Best-effort mapping from config.json structure.
        conflux_opts = self._safe_conflux_options_from_dict(node_cfg)

        total_nodes = len(self.nodes)
        if total_nodes <= 0:
            # Fall back to counts from instance_nodes map
            total_nodes = sum(len(v) for v in self.instance_nodes.values())

        # Derive target_tps if present in test configs
        target_tps = 1000
        try:
            tps_cfg = self.config_manager.get_test_config("tps")
            if isinstance(tps_cfg, dict) and tps_cfg.get("tx_rate"):
                target_tps = int(tps_cfg["tx_rate"])
        except Exception:
            pass

        sim_opts = SimulateOptions(
            target_tps=target_tps,
            target_nodes=max(1, int(total_nodes)),
            nodes_per_host=1,
            storage_memory_gb=int(conflux_cfg.get("storage_memory_gb", 2)),
        )

        return sim_opts, conflux_opts

    def start_all_nodes(self, *, pull_docker_image: bool = True):
        """Start all Conflux nodes on all instances using remote_simulation logic."""
        logger.info(f"Starting all {len(self.nodes)} Conflux nodes")

        if not self.instance_nodes:
            logger.warning("No instance->nodes mapping found. Did you call collect_node_info()? Skipping.")
            return

        # Generate config once (remote_simulation uses per-node port mapping at runtime).
        sim_opts, conflux_opts = self._build_remote_simulation_config()
        config_file = generate_config_file(sim_opts, conflux_opts)
        logger.info(f"Generated config file {config_file.path}")

        # Group hosts by nodes_per_host (remote_simulation launcher assumes uniform nodes_per_host).
        hosts_by_nph: Dict[int, List[str]] = {}
        for instance_id, node_ids in self.instance_nodes.items():
            if not node_ids:
                continue
            # Derive host ip from any node
            any_node = self.nodes.get(node_ids[0])
            if not any_node:
                continue
            nph = len(node_ids)
            hosts_by_nph.setdefault(nph, []).append(any_node.ip_address)

            # Mark nodes as starting
            for node_id in node_ids:
                if node_id in self.nodes:
                    self.nodes[node_id].status = "starting"
                    self.nodes[node_id].last_updated = time.strftime("%Y-%m-%d %H:%M:%S")

        started_nodes: List[RemoteNode] = []
        for nph, ips in hosts_by_nph.items():
            started_nodes.extend(
                launch_remote_nodes(
                    ips,
                    nph,
                    config_file,
                    pull_docker_image=pull_docker_image,
                )
            )

        # Update statuses based on started nodes
        started_set = {(n.host, n.index) for n in started_nodes}
        for node in self.nodes.values():
            # node_id format: node-<instance_id>-<i>
            try:
                index = int(node.node_id.rsplit("-", 1)[-1])
            except Exception:
                continue
            if (node.ip_address, index) in started_set:
                node.status = "running"
            else:
                node.status = "failed"
            node.last_updated = time.strftime("%Y-%m-%d %H:%M:%S")

        self._save_state()
    
    def wait_for_nodes_ready(self, timeout: int = 300):
        """Wait for all nodes to be ready (NormalSyncPhase) via remote RPC."""
        logger.info("Waiting for all nodes to be ready")

        start_time = time.time()
        pending: Dict[str, RemoteNode] = {}
        for node in self.nodes.values():
            try:
                index = int(node.node_id.rsplit("-", 1)[-1])
            except Exception:
                continue
            pending[node.node_id] = RemoteNode(host=node.ip_address, index=index)

        while pending and (time.time() - start_time) < timeout:
            done: List[str] = []
            for node_id, rn in pending.items():
                if rn.wait_for_ready():
                    done.append(node_id)
            for node_id in done:
                if node_id in self.nodes:
                    self.nodes[node_id].status = "running"
                    self.nodes[node_id].last_updated = time.strftime("%Y-%m-%d %H:%M:%S")
                pending.pop(node_id, None)

            if pending:
                time.sleep(5)

        if pending:
            logger.error(f"Timeout waiting for nodes to be ready after {timeout} seconds")
            for node_id in pending:
                if node_id in self.nodes:
                    self.nodes[node_id].status = "failed"
                    self.nodes[node_id].last_updated = time.strftime("%Y-%m-%d %H:%M:%S")
            self._save_state()
            raise TimeoutError(f"Timeout waiting for nodes to be ready after {timeout} seconds")

        logger.info("All nodes are ready")
        self._save_state()
    
    def get_node(self, node_id: str) -> Optional[NodeInfo]:
        """Get node by ID"""
        return self.nodes.get(node_id)
    
    def get_nodes_by_instance(self, instance_id: str) -> List[NodeInfo]:
        """Get nodes by instance ID"""
        node_ids = self.instance_nodes.get(instance_id, [])
        return [self.nodes[node_id] for node_id in node_ids if node_id in self.nodes]
    
    def list_nodes(self, region: Optional[str] = None, cloud_provider: Optional[str] = None, status: Optional[str] = None) -> List[NodeInfo]:
        """List nodes with filters"""
        nodes = []
        for node in self.nodes.values():
            if region and node.region != region:
                continue
            if cloud_provider and node.cloud_provider != cloud_provider:
                continue
            if status and node.status != status:
                continue
            nodes.append(node)
        return nodes
    
    def update_node_status(self, node_id: str, status: str):
        """Update node status"""
        if node_id in self.nodes:
            self.nodes[node_id].status = status
            self.nodes[node_id].last_updated = time.strftime("%Y-%m-%d %H:%M:%S")
            self._save_state()
            logger.info(f"Updated node {node_id} status to {status}")
    
    def stop_all_nodes(self):
        """Stop all Conflux nodes"""
        logger.info(f"Stopping all {len(self.nodes)} Conflux nodes")

        if not self.instance_nodes:
            logger.warning("No instances known; nothing to stop")
            return

        ips: List[str] = []
        for instance_id, node_ids in self.instance_nodes.items():
            if not node_ids:
                continue
            any_node = self.nodes.get(node_ids[0])
            if any_node:
                ips.append(any_node.ip_address)

        stop_remote_nodes(ips)

        for node in self.nodes.values():
            node.status = "stopped"
            node.last_updated = time.strftime("%Y-%m-%d %H:%M:%S")

        self._save_state()

    def destroy_all_nodes(self):
        """Destroy all Conflux nodes (docker rm -f + cleanup logs) on all instances."""
        if not self.instance_nodes:
            logger.warning("No instances known; nothing to destroy")
            return

        ips: List[str] = []
        for instance_id, node_ids in self.instance_nodes.items():
            if not node_ids:
                continue
            any_node = self.nodes.get(node_ids[0])
            if any_node:
                ips.append(any_node.ip_address)

        destory_remote_nodes(ips)

        for node in self.nodes.values():
            node.status = "stopped"
            node.last_updated = time.strftime("%Y-%m-%d %H:%M:%S")

        self._save_state()
    
    def cleanup_nodes(self, instance_ids: List[str]):
        """Cleanup nodes associated with instances"""
        logger.info(f"Cleaning up nodes for instances: {instance_ids}")
        
        for instance_id in instance_ids:
            node_ids = self.instance_nodes.get(instance_id, [])
            for node_id in node_ids:
                if node_id in self.nodes:
                    del self.nodes[node_id]
                    logger.info(f"Cleaned up node {node_id}")
            
            if instance_id in self.instance_nodes:
                del self.instance_nodes[instance_id]
        
        # Save state
        self._save_state()
    
    def _save_state(self):
        """Save node state to file"""
        try:
            state = {
                "nodes": self.nodes,
                "instance_nodes": self.instance_nodes,
                "port_allocations": self.port_allocations,
            }
            with open(self.state_file, 'wb') as f:
                pickle.dump(state, f)
            logger.debug(f"Node state saved to {self.state_file}")
        except Exception as e:
            logger.error(f"Failed to save node state: {e}")
    
    def _load_state(self):
        """Load node state from file"""
        if self.state_file.exists():
            try:
                with open(self.state_file, 'rb') as f:
                    state = pickle.load(f)
                self.nodes = state.get("nodes", {})
                self.instance_nodes = state.get("instance_nodes", {})
                self.port_allocations = state.get("port_allocations", {})
                logger.info(f"Node state loaded from {self.state_file}")
                logger.info(f"Loaded {len(self.nodes)} nodes from state")
            except Exception as e:
                logger.error(f"Failed to load node state: {e}")
                # Reset state if load fails
                self.nodes = {}
                self.instance_nodes = {}
    
    def clear_state(self):
        """Clear node state"""
        self.nodes = {}
        self.instance_nodes = {}
        if self.state_file.exists():
            self.state_file.unlink()
        logger.info("Node state cleared")
