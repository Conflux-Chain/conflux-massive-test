"""Node Manager"""

import time
import pickle
from typing import Dict, Any, List, Optional
from dataclasses import dataclass
from pathlib import Path

from loguru import logger
from conflux_deployer.configs.config_manager import ConfigManager
from conflux_deployer.server_deployment.server_deployer import ServerInstance


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
    
    def start_all_nodes(self):
        """Start all Conflux nodes"""
        logger.info(f"Starting all {len(self.nodes)} Conflux nodes")
        
        # TODO: Implement node startup logic
        # For now, just simulate the process
        for node_id, node in self.nodes.items():
            node.status = "starting"
            node.last_updated = time.strftime("%Y-%m-%d %H:%M:%S")
            logger.info(f"Starting node {node_id}")
        
        # Save state
        self._save_state()
    
    def wait_for_nodes_ready(self, timeout: int = 300):
        """Wait for all nodes to be ready"""
        logger.info("Waiting for all nodes to be ready")
        
        start_time = time.time()
        while time.time() - start_time < timeout:
            # TODO: Implement node readiness check
            # For now, just simulate the process
            all_ready = True
            for node_id, node in self.nodes.items():
                if node.status != "running":
                    node.status = "running"
                    node.last_updated = time.strftime("%Y-%m-%d %H:%M:%S")
                    logger.info(f"Node {node_id} is now running")
                
                if node.status != "running":
                    all_ready = False
            
            if all_ready:
                logger.info("All nodes are ready")
                # Save state
                self._save_state()
                return
            
            time.sleep(5)
        
        logger.error(f"Timeout waiting for nodes to be ready after {timeout} seconds")
        raise TimeoutError(f"Timeout waiting for nodes to be ready after {timeout} seconds")
    
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
        
        # TODO: Implement node stop logic
        # For now, just simulate the process
        for node_id, node in self.nodes.items():
            node.status = "stopped"
            node.last_updated = time.strftime("%Y-%m-%d %H:%M:%S")
            logger.info(f"Stopped node {node_id}")
        
        # Save state
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
