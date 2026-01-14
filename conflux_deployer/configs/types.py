"""
Configuration Type Definitions

All configuration classes with full Python type annotations.
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any, Literal
from enum import Enum


class CloudProvider(str, Enum):
    """Supported cloud providers"""
    AWS = "aws"
    ALIBABA = "alibaba"


class InstanceState(str, Enum):
    """Instance lifecycle states"""
    PENDING = "pending"
    RUNNING = "running"
    STOPPING = "stopping"
    STOPPED = "stopped"
    TERMINATED = "terminated"
    UNKNOWN = "unknown"


@dataclass
class CloudCredentials:
    """Cloud provider credentials"""
    access_key_id: str
    secret_access_key: str
    session_token: Optional[str] = None
    
    def to_dict(self) -> Dict[str, str]:
        result = {
            "access_key_id": self.access_key_id,
            "secret_access_key": self.secret_access_key,
        }
        if self.session_token:
            result["session_token"] = self.session_token
        return result


@dataclass
class RegionConfig:
    """Configuration for a specific cloud region"""
    provider: CloudProvider
    region_id: str
    # Human-readable location name (e.g., "US East", "Singapore")
    location_name: str
    # Number of instances to launch in this region
    instance_count: int
    # Instance type (e.g., "m6i.2xlarge" for AWS, "ecs.g7.2xlarge" for Alibaba)
    instance_type: str
    # Number of Conflux nodes per instance
    nodes_per_instance: int = 1
    # Optional: specific availability zone
    availability_zone: Optional[str] = None
    # Optional: VPC/VSwitch configuration
    vpc_id: Optional[str] = None
    subnet_id: Optional[str] = None
    security_group_id: Optional[str] = None
    
    def total_nodes(self) -> int:
        """Total number of Conflux nodes in this region"""
        return self.instance_count * self.nodes_per_instance


@dataclass
class InstanceTypeSpec:
    """Specification for an instance type"""
    provider: CloudProvider
    instance_type: str
    vcpus: int
    memory_gb: float
    # Maximum recommended Conflux nodes for this instance type
    max_conflux_nodes: int
    # Price per hour (USD) for reference
    price_per_hour: Optional[float] = None


# Pre-defined instance type specifications
AWS_INSTANCE_SPECS: Dict[str, InstanceTypeSpec] = {
    "m6i.2xlarge": InstanceTypeSpec(
        provider=CloudProvider.AWS,
        instance_type="m6i.2xlarge",
        vcpus=8,
        memory_gb=32,
        max_conflux_nodes=1,
        price_per_hour=0.384
    ),
    "m7i.2xlarge": InstanceTypeSpec(
        provider=CloudProvider.AWS,
        instance_type="m7i.2xlarge",
        vcpus=8,
        memory_gb=32,
        max_conflux_nodes=2,
        price_per_hour=0.403
    ),
    "m6i.4xlarge": InstanceTypeSpec(
        provider=CloudProvider.AWS,
        instance_type="m6i.4xlarge",
        vcpus=16,
        memory_gb=64,
        max_conflux_nodes=3,
        price_per_hour=0.768
    ),
    "m7i.4xlarge": InstanceTypeSpec(
        provider=CloudProvider.AWS,
        instance_type="m7i.4xlarge",
        vcpus=16,
        memory_gb=64,
        max_conflux_nodes=4,
        price_per_hour=0.806
    ),
}


ALIBABA_INSTANCE_SPECS: Dict[str, InstanceTypeSpec] = {
    "ecs.g7.2xlarge": InstanceTypeSpec(
        provider=CloudProvider.ALIBABA,
        instance_type="ecs.g7.2xlarge",
        vcpus=8,
        memory_gb=32,
        max_conflux_nodes=1,
    ),
    "ecs.g7.4xlarge": InstanceTypeSpec(
        provider=CloudProvider.ALIBABA,
        instance_type="ecs.g7.4xlarge",
        vcpus=16,
        memory_gb=64,
        max_conflux_nodes=3,
    ),
    "ecs.g8i.2xlarge": InstanceTypeSpec(
        provider=CloudProvider.ALIBABA,
        instance_type="ecs.g8i.2xlarge",
        vcpus=8,
        memory_gb=32,
        max_conflux_nodes=2,
    ),
}


@dataclass
class ImageConfig:
    """Configuration for server image"""
    # Base OS image ID (AMI for AWS, Image ID for Alibaba)
    base_image_id: Optional[str] = None
    # Our custom image name pattern
    image_name_prefix: str = "conflux-test-node"
    # Docker image for Conflux
    conflux_docker_image: str = "confluxchain/conflux-rust:latest"
    # Ubuntu version to use if creating new image
    ubuntu_version: str = "22.04"
    # Additional packages to install
    additional_packages: List[str] = field(default_factory=lambda: [
        "docker.io", "docker-compose", "htop", "iotop", "net-tools"
    ])
    # Custom image ID if already exists (provider -> region -> image_id)
    existing_images: Dict[str, Dict[str, str]] = field(default_factory=dict)


@dataclass
class ConfluxNodeConfig:
    """Configuration for a single Conflux node"""
    # Node index on the host (0-based)
    node_index: int
    # Base P2P port (actual port = base + node_index)
    p2p_port_base: int = 32323
    # Base JSON-RPC port
    jsonrpc_port_base: int = 12537
    # Storage memory in GB
    storage_memory_gb: int = 2
    # Transaction pool size
    tx_pool_size: int = 500000
    # Mining related
    mining_author: Optional[str] = None
    # Network ID
    chain_id: int = 1
    # Additional config parameters
    extra_config: Dict[str, Any] = field(default_factory=dict)
    # Block size of ports reserved per instance (to avoid cross-instance collision)
    ports_block_size: int = 1000
    
    @property
    def p2p_port(self) -> int:
        return self.p2p_port_base + self.node_index * 10
    
    @property
    def jsonrpc_http_port(self) -> int:
        return self.jsonrpc_port_base + self.node_index * 10
    
    @property
    def jsonrpc_ws_port(self) -> int:
        return self.jsonrpc_port_base + self.node_index * 10 + 1


@dataclass
class NetworkConfig:
    """Configuration for the Conflux test network"""
    # Number of peers each node connects to
    connect_peers: int = 3
    # Bandwidth limit in Mbit/s
    bandwidth_mbit: int = 20
    # Whether to enable transaction propagation
    enable_tx_propagation: bool = True
    # Block generation period in milliseconds
    block_generation_period_ms: int = 500
    # Target TPS for transaction generation
    target_tps: int = 1000
    # Genesis secrets file path
    genesis_secrets_path: Optional[str] = None
    # Debug mode to increase logging verbosity
    debug_mode: bool = False


@dataclass
class TestConfig:
    """Configuration for test execution"""
    # Test type: "stress", "latency", "fork", "custom"
    test_type: str
    # Number of blocks to generate
    num_blocks: int = 1000
    # Transactions per block (for stress test)
    txs_per_block: int = 1
    # Transaction data length
    tx_data_length: int = 0
    # Report progress every N blocks
    report_interval: int = 100
    # Test timeout in seconds
    timeout_seconds: int = 3600
    # Custom test parameters
    custom_params: Dict[str, Any] = field(default_factory=dict)


@dataclass
class CleanupConfig:
    """Configuration for resource cleanup"""
    # Whether to terminate instances on test completion
    auto_terminate: bool = True
    # Whether to delete images after test
    delete_images: bool = False
    # Grace period before force cleanup (seconds)
    grace_period_seconds: int = 60
    # Retry attempts for cleanup operations
    retry_attempts: int = 3

    @property
    def auto_cleanup(self) -> bool:
        """Backward compatible alias for auto_terminate"""
        return self.auto_terminate


@dataclass
class DeploymentConfig:
    """Main deployment configuration"""
    # Unique deployment ID (auto-generated if not provided)
    deployment_id: Optional[str] = None
    # Instance name prefix
    instance_name_prefix: str = "conflux-test"
    # Cloud credentials for each provider
    credentials: Dict[CloudProvider, CloudCredentials] = field(default_factory=dict)
    # Region configurations
    regions: List[RegionConfig] = field(default_factory=list)
    # Image configuration
    image: ImageConfig = field(default_factory=ImageConfig)
    # Conflux node base configuration
    conflux_node: ConfluxNodeConfig = field(default_factory=lambda: ConfluxNodeConfig(node_index=0))
    # Network configuration
    network: NetworkConfig = field(default_factory=NetworkConfig)
    # Test configuration
    test: TestConfig = field(default_factory=lambda: TestConfig(test_type="stress"))
    # Cleanup configuration
    cleanup: CleanupConfig = field(default_factory=CleanupConfig)
    # State file path for recovery
    state_file_path: str = "./deployment_state.json"
    # SSH key name (for AWS) or key pair name
    ssh_key_name: Optional[str] = None
    # SSH private key path for connecting to instances
    ssh_private_key_path: Optional[str] = None
    # Log level
    log_level: str = "INFO"
    
    def total_nodes(self) -> int:
        """Total number of Conflux nodes across all regions"""
        return sum(r.total_nodes() for r in self.regions)
    
    def total_instances(self) -> int:
        """Total number of cloud instances across all regions"""
        return sum(r.instance_count for r in self.regions)


@dataclass
class InstanceInfo:
    """Information about a cloud instance"""
    instance_id: str
    provider: CloudProvider
    region_id: str
    location_name: str
    instance_type: str
    public_ip: Optional[str] = None
    private_ip: Optional[str] = None
    state: InstanceState = InstanceState.PENDING
    # Number of Conflux nodes on this instance
    nodes_count: int = 1
    # Instance name/tag
    name: str = ""
    # Launch time (ISO format)
    launch_time: Optional[str] = None
    # Additional metadata
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "instance_id": self.instance_id,
            "provider": self.provider.value,
            "region_id": self.region_id,
            "location_name": self.location_name,
            "instance_type": self.instance_type,
            "public_ip": self.public_ip,
            "private_ip": self.private_ip,
            "state": self.state.value,
            "nodes_count": self.nodes_count,
            "name": self.name,
            "launch_time": self.launch_time,
            "metadata": self.metadata,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "InstanceInfo":
        return cls(
            instance_id=data["instance_id"],
            provider=CloudProvider(data["provider"]),
            region_id=data["region_id"],
            location_name=data["location_name"],
            instance_type=data["instance_type"],
            public_ip=data.get("public_ip"),
            private_ip=data.get("private_ip"),
            state=InstanceState(data.get("state", "unknown")),
            nodes_count=data.get("nodes_count", 1),
            name=data.get("name", ""),
            launch_time=data.get("launch_time"),
            metadata=data.get("metadata", {}),
        )


@dataclass
class NodeInfo:
    """Information about a Conflux node"""
    # Unique node ID
    node_id: str
    # Host instance
    instance_info: InstanceInfo
    # Node index on the instance
    node_index: int
    # P2P port
    p2p_port: int
    # JSON-RPC port
    jsonrpc_port: int
    # Node public key (set after node starts)
    public_key: Optional[str] = None
    # Node enode address
    enode: Optional[str] = None
    # Whether node is ready
    is_ready: bool = False
    
    @property
    def rpc_url(self) -> str:
        """HTTP JSON-RPC URL"""
        return f"http://{self.instance_info.public_ip}:{self.jsonrpc_port}"
    
    @property
    def p2p_address(self) -> str:
        """P2P address for peer connection"""
        return f"{self.instance_info.public_ip}:{self.p2p_port}"
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "node_id": self.node_id,
            "instance_id": self.instance_info.instance_id,
            "node_index": self.node_index,
            "p2p_port": self.p2p_port,
            "jsonrpc_port": self.jsonrpc_port,
            "public_key": self.public_key,
            "enode": self.enode,
            "is_ready": self.is_ready,
        }


@dataclass
class DeploymentState:
    """Persistent state for deployment recovery"""
    deployment_id: str
    # Current deployment phase
    phase: str = "initialized"  # initialized, images_ready, instances_launched, nodes_started, test_running, completed, cleanup
    # Created images (provider -> region -> image_id)
    images: Dict[str, Dict[str, str]] = field(default_factory=dict)
    # Launched instances
    instances: List[InstanceInfo] = field(default_factory=list)
    # Node information
    nodes: List[Dict[str, Any]] = field(default_factory=list)
    # Test results
    test_results: Dict[str, Any] = field(default_factory=dict)
    # Error information
    errors: List[str] = field(default_factory=list)
    # Timestamps
    created_at: Optional[str] = None
    updated_at: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "deployment_id": self.deployment_id,
            "phase": self.phase,
            "images": self.images,
            "instances": [i.to_dict() for i in self.instances],
            "nodes": self.nodes,
            "test_results": self.test_results,
            "errors": self.errors,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "DeploymentState":
        instances = [InstanceInfo.from_dict(i) for i in data.get("instances", [])]
        return cls(
            deployment_id=data["deployment_id"],
            phase=data.get("phase", "initialized"),
            images=data.get("images", {}),
            instances=instances,
            nodes=data.get("nodes", []),
            test_results=data.get("test_results", {}),
            errors=data.get("errors", []),
            created_at=data.get("created_at"),
            updated_at=data.get("updated_at"),
        )
