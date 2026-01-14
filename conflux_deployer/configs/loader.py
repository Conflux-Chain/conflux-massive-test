"""
Configuration Loader and Manager

Handles loading, validating, and managing configurations from files.
"""

import json
import os
from typing import Dict, Any, Optional
from datetime import datetime
import uuid

from .types import (
    DeploymentConfig,
    DeploymentState,
    CloudProvider,
    CloudCredentials,
    RegionConfig,
    ImageConfig,
    ConfluxNodeConfig,
    NetworkConfig,
    TestConfig,
    CleanupConfig,
    InstanceInfo,
)


class ConfigLoader:
    """Loads and validates configuration from files"""
    
    @staticmethod
    def load_from_file(config_path: str) -> DeploymentConfig:
        """Load deployment configuration from a JSON file"""
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Configuration file not found: {config_path}")
        
        with open(config_path, 'r') as f:
            data = json.load(f)
        
        return ConfigLoader.from_dict(data)
    
    @staticmethod
    def from_dict(data: Dict[str, Any]) -> DeploymentConfig:
        """Create DeploymentConfig from a dictionary"""
        
        # Parse credentials
        credentials: Dict[CloudProvider, CloudCredentials] = {}
        creds_data = data.get("credentials", {})
        
        if "aws" in creds_data:
            aws_creds = creds_data["aws"]
            credentials[CloudProvider.AWS] = CloudCredentials(
                access_key_id=aws_creds.get("access_key_id", ""),
                secret_access_key=aws_creds.get("secret_access_key", ""),
                session_token=aws_creds.get("session_token"),
            )
        
        if "alibaba" in creds_data:
            ali_creds = creds_data["alibaba"]
            credentials[CloudProvider.ALIBABA] = CloudCredentials(
                access_key_id=ali_creds.get("access_key_id", ""),
                secret_access_key=ali_creds.get("secret_access_key", ""),
            )
        
        # Parse regions
        regions = []
        for region_data in data.get("regions", []):
            provider = CloudProvider(region_data.get("provider", "aws"))
            regions.append(RegionConfig(
                provider=provider,
                region_id=region_data["region_id"],
                location_name=region_data.get("location_name", region_data["region_id"]),
                instance_count=region_data.get("instance_count", 1),
                instance_type=region_data["instance_type"],
                nodes_per_instance=region_data.get("nodes_per_instance", 1),
                availability_zone=region_data.get("availability_zone"),
                vpc_id=region_data.get("vpc_id"),
                subnet_id=region_data.get("subnet_id"),
                security_group_id=region_data.get("security_group_id"),
            ))
        
        # Parse image config
        image_data = data.get("image", {})
        image_config = ImageConfig(
            base_image_id=image_data.get("base_image_id"),
            image_name_prefix=image_data.get("image_name_prefix", "conflux-test-node"),
            conflux_docker_image=image_data.get("conflux_docker_image", "confluxchain/conflux-rust:latest"),
            ubuntu_version=image_data.get("ubuntu_version", "22.04"),
            additional_packages=image_data.get("additional_packages", [
                "docker.io", "docker-compose", "htop", "iotop", "net-tools"
            ]),
            existing_images=image_data.get("existing_images", {}),
        )
        
        # Parse network config
        network_data = data.get("network", {})
        network_config = NetworkConfig(
            connect_peers=network_data.get("connect_peers", 3),
            bandwidth_mbit=network_data.get("bandwidth_mbit", 20),
            enable_tx_propagation=network_data.get("enable_tx_propagation", True),
            block_generation_period_ms=network_data.get("block_generation_period_ms", 500),
            target_tps=network_data.get("target_tps", 1000),
            genesis_secrets_path=network_data.get("genesis_secrets_path"),
            debug_mode=network_data.get("debug_mode", False),
        )

        # Parse Conflux node config
        node_data = data.get("conflux_node", {})
        conflux_node_config = ConfluxNodeConfig(
            node_index=0,  # Base config, actual index set per node
            p2p_port_base=node_data.get("p2p_port_base", 32323),
            jsonrpc_port_base=node_data.get("jsonrpc_port_base", 12537),
            storage_memory_gb=node_data.get("storage_memory_gb", 2),
            tx_pool_size=node_data.get("tx_pool_size", 500000),
            mining_author=node_data.get("mining_author"),
            chain_id=node_data.get("chain_id", 1),
            extra_config=node_data.get("extra_config", {}),
            ports_block_size=node_data.get("ports_block_size", 1000),
        )
        
        # Parse test config
        test_data = data.get("test", {})
        test_config = TestConfig(
            test_type=test_data.get("test_type", "stress"),
            num_blocks=test_data.get("num_blocks", 1000),
            txs_per_block=test_data.get("txs_per_block", 1),
            tx_data_length=test_data.get("tx_data_length", 0),
            report_interval=test_data.get("report_interval", 100),
            timeout_seconds=test_data.get("timeout_seconds", 3600),
            custom_params=test_data.get("custom_params", {}),
        )
        
        # Parse cleanup config
        cleanup_data = data.get("cleanup", {})
        cleanup_config = CleanupConfig(
            auto_terminate=cleanup_data.get("auto_cleanup", cleanup_data.get("auto_terminate", True)),
            delete_images=cleanup_data.get("delete_images", False),
            grace_period_seconds=cleanup_data.get("grace_period_seconds", 60),
            retry_attempts=cleanup_data.get("retry_attempts", 3),
        )
        
        # Create deployment config
        deployment_id = data.get("deployment_id") or f"deploy-{datetime.now().strftime('%Y%m%d-%H%M%S')}-{uuid.uuid4().hex[:8]}"
        
        return DeploymentConfig(
            deployment_id=deployment_id,
            instance_name_prefix=data.get("instance_name_prefix", "conflux-test"),
            credentials=credentials,
            regions=regions,
            image=image_config,
            conflux_node=conflux_node_config,
            network=network_config,
            test=test_config,
            cleanup=cleanup_config,
            state_file_path=data.get("state_file_path", "./deployment_state.json"),
            ssh_key_name=data.get("ssh_key_name"),
            ssh_private_key_path=data.get("ssh_private_key_path"),
            log_level=data.get("log_level", "INFO"),
        )
    
    @staticmethod
    def to_dict(config: DeploymentConfig) -> Dict[str, Any]:
        """Convert DeploymentConfig to a dictionary"""
        credentials_dict = {}
        for provider, creds in config.credentials.items():
            credentials_dict[provider.value] = creds.to_dict()
        
        regions_list = []
        for region in config.regions:
            regions_list.append({
                "provider": region.provider.value,
                "region_id": region.region_id,
                "location_name": region.location_name,
                "instance_count": region.instance_count,
                "instance_type": region.instance_type,
                "nodes_per_instance": region.nodes_per_instance,
                "availability_zone": region.availability_zone,
                "vpc_id": region.vpc_id,
                "subnet_id": region.subnet_id,
                "security_group_id": region.security_group_id,
            })
        
        return {
            "deployment_id": config.deployment_id,
            "instance_name_prefix": config.instance_name_prefix,
            "credentials": credentials_dict,
            "regions": regions_list,
            "image": {
                "base_image_id": config.image.base_image_id,
                "image_name_prefix": config.image.image_name_prefix,
                "conflux_docker_image": config.image.conflux_docker_image,
                "ubuntu_version": config.image.ubuntu_version,
                "additional_packages": config.image.additional_packages,
                "existing_images": config.image.existing_images,
            },
            "conflux_node": {
                "p2p_port_base": config.conflux_node.p2p_port_base,
                "jsonrpc_port_base": config.conflux_node.jsonrpc_port_base,
                "storage_memory_gb": config.conflux_node.storage_memory_gb,
                "tx_pool_size": config.conflux_node.tx_pool_size,
                "mining_author": config.conflux_node.mining_author,
                "chain_id": config.conflux_node.chain_id,
                "extra_config": config.conflux_node.extra_config,
            },
            "network": {
                "connect_peers": config.network.connect_peers,
                "bandwidth_mbit": config.network.bandwidth_mbit,
                "enable_tx_propagation": config.network.enable_tx_propagation,
                "block_generation_period_ms": config.network.block_generation_period_ms,
                "target_tps": config.network.target_tps,
                "genesis_secrets_path": config.network.genesis_secrets_path,
            },
            "test": {
                "test_type": config.test.test_type,
                "num_blocks": config.test.num_blocks,
                "txs_per_block": config.test.txs_per_block,
                "tx_data_length": config.test.tx_data_length,
                "report_interval": config.test.report_interval,
                "timeout_seconds": config.test.timeout_seconds,
                "custom_params": config.test.custom_params,
            },
            "cleanup": {
                "auto_cleanup": config.cleanup.auto_terminate,
                "delete_images": config.cleanup.delete_images,
                "grace_period_seconds": config.cleanup.grace_period_seconds,
                "retry_attempts": config.cleanup.retry_attempts,
            },
            "state_file_path": config.state_file_path,
            "ssh_key_name": config.ssh_key_name,
            "ssh_private_key_path": config.ssh_private_key_path,
            "log_level": config.log_level,
        }
    
    @staticmethod
    def save_to_file(config: DeploymentConfig, config_path: str) -> None:
        """Save deployment configuration to a JSON file"""
        data = ConfigLoader.to_dict(config)
        
        # Ensure directory exists
        os.makedirs(os.path.dirname(config_path) or ".", exist_ok=True)
        
        with open(config_path, 'w') as f:
            json.dump(data, f, indent=2)


class StateManager:
    """Manages deployment state persistence for recovery"""
    
    def __init__(self, state_file_path: str):
        self.state_file_path = state_file_path
        self._state: Optional[DeploymentState] = None
    
    def initialize(self, deployment_id: str) -> DeploymentState:
        """Initialize a new deployment state"""
        self._state = DeploymentState(
            deployment_id=deployment_id,
            phase="initialized",
            created_at=datetime.now().isoformat(),
            updated_at=datetime.now().isoformat(),
        )
        self.save()
        return self._state
    
    def load(self) -> Optional[DeploymentState]:
        """Load state from file if exists"""
        if not os.path.exists(self.state_file_path):
            return None
        
        try:
            with open(self.state_file_path, 'r') as f:
                data = json.load(f)
            self._state = DeploymentState.from_dict(data)
            return self._state
        except (json.JSONDecodeError, KeyError) as e:
            raise ValueError(f"Invalid state file: {e}")
    
    def save(self) -> None:
        """Save current state to file"""
        if self._state is None:
            return
        
        self._state.updated_at = datetime.now().isoformat()
        
        # Ensure directory exists
        os.makedirs(os.path.dirname(self.state_file_path) or ".", exist_ok=True)
        
        with open(self.state_file_path, 'w') as f:
            json.dump(self._state.to_dict(), f, indent=2)
    
    @property
    def state(self) -> Optional[DeploymentState]:
        return self._state
    
    def update_phase(self, phase: str) -> None:
        """Update deployment phase"""
        if self._state:
            self._state.phase = phase
            self.save()
    
    def add_instance(self, instance: 'InstanceInfo') -> None:
        """Add an instance to state"""
        if self._state:
            self._state.instances.append(instance)
            self.save()
    
    def update_instance(self, instance_id: str, **kwargs) -> None:
        """Update instance information"""
        if self._state:
            for instance in self._state.instances:
                if instance.instance_id == instance_id:
                    for key, value in kwargs.items():
                        if hasattr(instance, key):
                            setattr(instance, key, value)
                    break
            self.save()
    
    def remove_instance(self, instance_id: str) -> None:
        """Remove an instance from state"""
        if self._state:
            self._state.instances = [
                i for i in self._state.instances 
                if i.instance_id != instance_id
            ]
            self.save()
    
    def add_image(self, provider: str, region: str, image_id: str) -> None:
        """Add an image to state"""
        if self._state:
            if provider not in self._state.images:
                self._state.images[provider] = {}
            self._state.images[provider][region] = image_id
            self.save()
    
    def add_error(self, error: str) -> None:
        """Record an error"""
        if self._state:
            self._state.errors.append(f"{datetime.now().isoformat()}: {error}")
            self.save()
    
    def set_test_results(self, results: Dict[str, Any]) -> None:
        """Set test results"""
        if self._state:
            self._state.test_results = results
            self.save()
    
    def delete_state_file(self) -> None:
        """Delete the state file"""
        if os.path.exists(self.state_file_path):
            os.remove(self.state_file_path)
        self._state = None
