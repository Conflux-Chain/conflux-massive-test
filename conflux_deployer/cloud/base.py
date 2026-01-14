"""
Cloud Provider Abstract Base Class

Defines the interface that all cloud providers must implement.
This keeps cloud-specific logic separate from Conflux logic.
"""

from abc import ABC, abstractmethod
from typing import List, Dict, Optional, Any
from dataclasses import dataclass

from ..configs import (
    CloudProvider,
    CloudCredentials,
    InstanceInfo,
    InstanceState,
    RegionConfig,
)


@dataclass
class ImageInfo:
    """Information about a cloud image"""
    image_id: str
    name: str
    provider: CloudProvider
    region_id: str
    state: str  # available, pending, failed, etc.
    creation_date: Optional[str] = None
    description: Optional[str] = None


@dataclass
class SecurityGroupRule:
    """Security group rule definition"""
    protocol: str  # tcp, udp, icmp, -1 (all)
    from_port: int
    to_port: int
    cidr_blocks: List[str]  # e.g., ["0.0.0.0/0"]
    description: str = ""


class CloudProviderBase(ABC):
    """
    Abstract base class for cloud providers.
    
    Each cloud provider (AWS, Alibaba Cloud) must implement this interface.
    This ensures consistent behavior across different cloud platforms.
    """
    
    def __init__(self, credentials: CloudCredentials, region_id: str):
        self.credentials = credentials
        self.region_id = region_id
        self._client: Any = None
    
    @property
    @abstractmethod
    def provider_type(self) -> CloudProvider:
        """Return the cloud provider type"""
        pass
    
    @abstractmethod
    def initialize_client(self) -> None:
        """Initialize the cloud provider client/SDK"""
        pass
    
    # ==================== Instance Operations ====================
    
    @abstractmethod
    def launch_instances(
        self,
        image_id: str,
        instance_type: str,
        count: int,
        name_prefix: str,
        security_group_id: Optional[str] = None,
        subnet_id: Optional[str] = None,
        key_name: Optional[str] = None,
        user_data: Optional[str] = None,
        tags: Optional[Dict[str, str]] = None,
    ) -> List[InstanceInfo]:
        """
        Launch cloud instances.
        
        Args:
            image_id: ID of the image to use
            instance_type: Instance type (e.g., m6i.2xlarge)
            count: Number of instances to launch
            name_prefix: Prefix for instance names
            security_group_id: Security group ID
            subnet_id: Subnet/VSwitch ID
            key_name: SSH key pair name
            user_data: User data script (base64 encoded)
            tags: Additional tags for the instances
            
        Returns:
            List of InstanceInfo for launched instances
        """
        pass
    
    @abstractmethod
    def terminate_instances(self, instance_ids: List[str]) -> Dict[str, bool]:
        """
        Terminate cloud instances.
        
        Args:
            instance_ids: List of instance IDs to terminate
            
        Returns:
            Dict mapping instance_id to success status
        """
        pass
    
    @abstractmethod
    def stop_instances(self, instance_ids: List[str]) -> Dict[str, bool]:
        """
        Stop cloud instances.
        
        Args:
            instance_ids: List of instance IDs to stop
            
        Returns:
            Dict mapping instance_id to success status
        """
        pass
    
    @abstractmethod
    def start_instances(self, instance_ids: List[str]) -> Dict[str, bool]:
        """
        Start stopped cloud instances.
        
        Args:
            instance_ids: List of instance IDs to start
            
        Returns:
            Dict mapping instance_id to success status
        """
        pass
    
    @abstractmethod
    def get_instance_status(self, instance_ids: List[str]) -> Dict[str, InstanceInfo]:
        """
        Get current status of instances.
        
        Args:
            instance_ids: List of instance IDs to check
            
        Returns:
            Dict mapping instance_id to InstanceInfo
        """
        pass
    
    @abstractmethod
    def wait_for_instances_running(
        self, 
        instance_ids: List[str], 
        timeout_seconds: int = 300
    ) -> Dict[str, InstanceInfo]:
        """
        Wait for instances to reach running state with public IPs.
        
        Args:
            instance_ids: List of instance IDs to wait for
            timeout_seconds: Maximum time to wait
            
        Returns:
            Dict mapping instance_id to InstanceInfo with public IPs
            
        Raises:
            TimeoutError: If instances don't become running within timeout
        """
        pass
    
    @abstractmethod
    def list_instances_by_tag(
        self, 
        tag_key: str, 
        tag_value: str
    ) -> List[InstanceInfo]:
        """
        List instances matching a specific tag.
        
        Args:
            tag_key: Tag key to filter by
            tag_value: Tag value to filter by
            
        Returns:
            List of matching InstanceInfo
        """
        pass

    @abstractmethod
    def list_instances_by_name_prefix(self, name_prefix: str) -> List[InstanceInfo]:
        """List instances whose Name/instance_name starts with the given prefix."""
        pass
    
    # ==================== Image Operations ====================
    
    @abstractmethod
    def create_image(
        self,
        instance_id: str,
        image_name: str,
        description: str = "",
        wait_for_available: bool = True,
        timeout_seconds: int = 1800,
    ) -> ImageInfo:
        """
        Create an image from an instance.
        
        Args:
            instance_id: ID of the source instance
            image_name: Name for the new image
            description: Image description
            wait_for_available: Whether to wait for image to be available
            timeout_seconds: Timeout for waiting
            
        Returns:
            ImageInfo for the created image
        """
        pass
    
    @abstractmethod
    def delete_image(self, image_id: str) -> bool:
        """
        Delete an image.
        
        Args:
            image_id: ID of the image to delete
            
        Returns:
            True if deleted successfully
        """
        pass
    
    @abstractmethod
    def find_image_by_name(self, name_pattern: str) -> Optional[ImageInfo]:
        """
        Find an image by name pattern.
        
        Args:
            name_pattern: Name pattern to search for
            
        Returns:
            ImageInfo if found, None otherwise
        """
        pass
    
    @abstractmethod
    def get_base_ubuntu_image(self, ubuntu_version: str = "22.04") -> str:
        """
        Get the base Ubuntu image ID for this region.
        
        Args:
            ubuntu_version: Ubuntu version (e.g., "22.04", "20.04")
            
        Returns:
            Image ID of the Ubuntu image
        """
        pass
    
    # ==================== Security Group Operations ====================
    
    @abstractmethod
    def create_security_group(
        self,
        name: str,
        description: str,
        vpc_id: Optional[str] = None,
        rules: Optional[List[SecurityGroupRule]] = None,
    ) -> str:
        """
        Create a security group with rules.
        
        Args:
            name: Security group name
            description: Security group description
            vpc_id: VPC ID (if applicable)
            rules: List of security group rules
            
        Returns:
            Security group ID
        """
        pass
    
    @abstractmethod
    def delete_security_group(self, security_group_id: str) -> bool:
        """
        Delete a security group.
        
        Args:
            security_group_id: ID of the security group to delete
            
        Returns:
            True if deleted successfully
        """
        pass
    
    @abstractmethod
    def find_security_group_by_name(self, name: str) -> Optional[str]:
        """
        Find a security group by name.
        
        Args:
            name: Security group name
            
        Returns:
            Security group ID if found, None otherwise
        """
        pass
    
    # ==================== Key Pair Operations ====================
    
    @abstractmethod
    def create_key_pair(self, key_name: str) -> str:
        """
        Create an SSH key pair.
        
        Args:
            key_name: Name for the key pair
            
        Returns:
            Private key material (PEM format)
        """
        pass
    
    @abstractmethod
    def delete_key_pair(self, key_name: str) -> bool:
        """
        Delete an SSH key pair.
        
        Args:
            key_name: Name of the key pair to delete
            
        Returns:
            True if deleted successfully
        """
        pass
    
    @abstractmethod
    def key_pair_exists(self, key_name: str) -> bool:
        """
        Check if a key pair exists.
        
        Args:
            key_name: Name of the key pair
            
        Returns:
            True if exists
        """
        pass
    
    # ==================== Utility Methods ====================
    
    @abstractmethod
    def get_available_regions(self) -> List[str]:
        """
        Get list of available regions for this provider.
        
        Returns:
            List of region IDs
        """
        pass
    
    @abstractmethod
    def validate_instance_type(self, instance_type: str) -> bool:
        """
        Check if an instance type is valid in this region.
        
        Args:
            instance_type: Instance type to validate
            
        Returns:
            True if valid
        """
        pass


def get_default_security_rules(
    p2p_port_start: int = 32323,
    p2p_port_end: int = 32423,
    rpc_port_start: int = 12537,
    rpc_port_end: int = 12637,
) -> List[SecurityGroupRule]:
    """
    Get default security group rules for Conflux nodes.
    
    Args:
        p2p_port_start: Start of P2P port range
        p2p_port_end: End of P2P port range
        rpc_port_start: Start of RPC port range
        rpc_port_end: End of RPC port range
        
    Returns:
        List of SecurityGroupRule for Conflux
    """
    return [
        # SSH access
        SecurityGroupRule(
            protocol="tcp",
            from_port=22,
            to_port=22,
            cidr_blocks=["0.0.0.0/0"],
            description="SSH access",
        ),
        # P2P ports
        SecurityGroupRule(
            protocol="tcp",
            from_port=p2p_port_start,
            to_port=p2p_port_end,
            cidr_blocks=["0.0.0.0/0"],
            description="Conflux P2P ports",
        ),
        SecurityGroupRule(
            protocol="udp",
            from_port=p2p_port_start,
            to_port=p2p_port_end,
            cidr_blocks=["0.0.0.0/0"],
            description="Conflux P2P UDP ports",
        ),
        # JSON-RPC ports
        SecurityGroupRule(
            protocol="tcp",
            from_port=rpc_port_start,
            to_port=rpc_port_end,
            cidr_blocks=["0.0.0.0/0"],
            description="Conflux JSON-RPC ports",
        ),
        # ICMP for diagnostics
        SecurityGroupRule(
            protocol="icmp",
            from_port=-1,
            to_port=-1,
            cidr_blocks=["0.0.0.0/0"],
            description="ICMP for diagnostics",
        ),
    ]
