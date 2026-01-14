"""
Alibaba Cloud Provider Implementation

Implements CloudProviderBase for Alibaba Cloud (Aliyun) using the official SDK.
"""

import base64
import time
from datetime import datetime
from typing import List, Dict, Optional, Any, cast

from alibabacloud_tea_openapi import models as open_api_models
from alibabacloud_ecs20140526.client import Client as EcsClient
from alibabacloud_ecs20140526 import models as ecs_models

from ..configs import (
    CloudProvider,
    CloudCredentials,
    InstanceInfo,
    InstanceState,
)
from .base import CloudProviderBase, ImageInfo, SecurityGroupRule


# Mapping Alibaba instance states to our InstanceState enum
ALIBABA_STATE_MAP = {
    "Pending": InstanceState.PENDING,
    "Running": InstanceState.RUNNING,
    "Starting": InstanceState.PENDING,
    "Stopping": InstanceState.STOPPING,
    "Stopped": InstanceState.STOPPED,
}

# Ubuntu image name patterns by region
# These are Alibaba Cloud marketplace Ubuntu images
UBUNTU_IMAGE_PATTERNS = {
    "cn-hangzhou": "ubuntu_22_04_x64*alibase*",
    "cn-shanghai": "ubuntu_22_04_x64*alibase*",
    "cn-beijing": "ubuntu_22_04_x64*alibase*",
    "cn-shenzhen": "ubuntu_22_04_x64*alibase*",
    "cn-hongkong": "ubuntu_22_04_x64*alibase*",
    "ap-southeast-1": "ubuntu_22_04_x64*alibase*",  # Singapore
    "ap-southeast-2": "ubuntu_22_04_x64*alibase*",  # Sydney
    "ap-southeast-3": "ubuntu_22_04_x64*alibase*",  # Kuala Lumpur
    "ap-southeast-5": "ubuntu_22_04_x64*alibase*",  # Jakarta
    "ap-northeast-1": "ubuntu_22_04_x64*alibase*",  # Tokyo
    "ap-south-1": "ubuntu_22_04_x64*alibase*",      # Mumbai
    "eu-central-1": "ubuntu_22_04_x64*alibase*",    # Frankfurt
    "eu-west-1": "ubuntu_22_04_x64*alibase*",       # London
    "us-west-1": "ubuntu_22_04_x64*alibase*",       # Silicon Valley
    "us-east-1": "ubuntu_22_04_x64*alibase*",       # Virginia
}


class AlibabaProvider(CloudProviderBase):
    """
    Alibaba Cloud Provider implementation using the official SDK.
    
    This class handles all Alibaba Cloud-specific operations for:
    - ECS instance management
    - Image management
    - Security groups
    - Key pairs
    """
    
    def __init__(self, credentials: CloudCredentials, region_id: str):
        super().__init__(credentials, region_id)
        self._ecs_client: Optional[EcsClient] = None
    
    @property
    def provider_type(self) -> CloudProvider:
        return CloudProvider.ALIBABA
    
    def initialize_client(self) -> None:
        """Initialize Alibaba Cloud ECS client"""
        config = open_api_models.Config(
            access_key_id=self.credentials.access_key_id,
            access_key_secret=self.credentials.secret_access_key,
        )
        # Set endpoint based on region
        config.endpoint = f"ecs.{self.region_id}.aliyuncs.com"
        self._ecs_client = EcsClient(config)
    
    @property
    def ecs_client(self) -> EcsClient:
        if self._ecs_client is None:
            self.initialize_client()
        # At this point _ecs_client is guaranteed to be non-None
        return cast(EcsClient, self._ecs_client)
    
    # ==================== Instance Operations ====================
    
    def launch_instances(
        self,
        image_id: str,
        instance_type: str,
        count: int,
        name_prefix: str,
        security_group_id: Optional[str] = None,
        subnet_id: Optional[str] = None,  # VSwitch ID in Alibaba
        key_name: Optional[str] = None,
        user_data: Optional[str] = None,
        tags: Optional[Dict[str, str]] = None,
    ) -> List[InstanceInfo]:
        """Launch ECS instances"""
        
        instances = []
        
        for i in range(count):
            instance_name = f"{name_prefix}-{i}"
            
            # Build request
            request = ecs_models.RunInstancesRequest(
                region_id=self.region_id,
                image_id=image_id,
                instance_type=instance_type,
                instance_name=instance_name,
                host_name=instance_name.replace("_", "-"),  # Hostname can't have underscore
                amount=1,
                internet_charge_type="PayByTraffic",
                internet_max_bandwidth_out=100,  # Allow public IP
                instance_charge_type="PostPaid",  # Pay-as-you-go
            )
            
            # Add optional parameters
            if security_group_id:
                request.security_group_id = security_group_id
            
            if subnet_id:
                request.v_switch_id = subnet_id
            
            if key_name:
                request.key_pair_name = key_name
            
            if user_data:
                # Encode user data to base64 if not already encoded
                try:
                    base64.b64decode(user_data)
                    request.user_data = user_data
                except Exception:
                    request.user_data = base64.b64encode(user_data.encode()).decode()
            
            # Add tags
            tag_list = [
                ecs_models.RunInstancesRequestTag(
                    key="Name",
                    value=instance_name,
                ),
                ecs_models.RunInstancesRequestTag(
                    key="DeploymentPrefix",
                    value=name_prefix,
                ),
                ecs_models.RunInstancesRequestTag(
                    key="CreatedBy",
                    value="conflux-deployer",
                ),
            ]
            
            if tags:
                for key, value in tags.items():
                    tag_list.append(ecs_models.RunInstancesRequestTag(key=key, value=value))
            
            request.tag = tag_list
            
            # Launch instance
            response = self.ecs_client.run_instances(request)
            
            # Validate response
            if not (response and getattr(response, "body", None)):
                raise RuntimeError("Invalid response from run_instances")

            ids_container = getattr(response.body, "instance_id_sets", None)
            if not ids_container or not getattr(ids_container, "instance_id_set", None):
                raise RuntimeError("No instance IDs returned when launching instances")

            instance_id = ids_container.instance_id_set[0]
            if not instance_id:
                raise RuntimeError("Empty instance id returned")

            instance_info = InstanceInfo(
                instance_id=str(instance_id),
                provider=CloudProvider.ALIBABA,
                region_id=str(self.region_id),
                location_name=str(self.region_id),
                instance_type=str(instance_type),
                state=InstanceState.PENDING,
                name=str(instance_name),
                launch_time=datetime.now().isoformat(),
            )
            instances.append(instance_info)
        
        return instances
    
    def terminate_instances(self, instance_ids: List[str]) -> Dict[str, bool]:
        """Terminate ECS instances"""
        results = {}
        
        if not instance_ids:
            return results
        
        try:
            request = ecs_models.DeleteInstancesRequest(
                region_id=self.region_id,
                instance_id=instance_ids,
                force=True,
            )
            
            self.ecs_client.delete_instances(request)
            
            # Mark all as successful (API throws exception on failure)
            for instance_id in instance_ids:
                results[instance_id] = True
                
        except Exception as e:
            # Mark all as failed on error
            for instance_id in instance_ids:
                results[instance_id] = False
            raise RuntimeError(f"Failed to terminate instances: {e}")
        
        return results
    
    def stop_instances(self, instance_ids: List[str]) -> Dict[str, bool]:
        """Stop ECS instances"""
        results = {}
        
        if not instance_ids:
            return results
        
        try:
            request = ecs_models.StopInstancesRequest(
                region_id=self.region_id,
                instance_id=instance_ids,
                force_stop=True,
            )
            
            self.ecs_client.stop_instances(request)
            
            for instance_id in instance_ids:
                results[instance_id] = True
                
        except Exception as e:
            for instance_id in instance_ids:
                results[instance_id] = False
            raise RuntimeError(f"Failed to stop instances: {e}")
        
        return results
    
    def start_instances(self, instance_ids: List[str]) -> Dict[str, bool]:
        """Start stopped ECS instances"""
        results = {}
        
        if not instance_ids:
            return results
        
        try:
            request = ecs_models.StartInstancesRequest(
                region_id=self.region_id,
                instance_id=instance_ids,
            )
            
            self.ecs_client.start_instances(request)
            
            for instance_id in instance_ids:
                results[instance_id] = True
                
        except Exception as e:
            for instance_id in instance_ids:
                results[instance_id] = False
            raise RuntimeError(f"Failed to start instances: {e}")
        
        return results
    
    def get_instance_status(self, instance_ids: List[str]) -> Dict[str, InstanceInfo]:
        """Get current status of ECS instances"""
        results = {}
        
        if not instance_ids:
            return results
        
        try:
            request = ecs_models.DescribeInstancesRequest(
                region_id=self.region_id,
                instance_ids=str(instance_ids),  # JSON array string
            )
            
            response = self.ecs_client.describe_instances(request)
            
            if response.body and response.body.instances:
                for instance in response.body.instances.instance:
                    instance_id = instance.instance_id
                    
                    # Get public IP
                    public_ip = None
                    if instance.public_ip_address and instance.public_ip_address.ip_address:
                        public_ip = instance.public_ip_address.ip_address[0]
                    elif instance.eip_address and instance.eip_address.ip_address:
                        public_ip = instance.eip_address.ip_address
                    
                    # Get private IP
                    private_ip = None
                    if instance.vpc_attributes and instance.vpc_attributes.private_ip_address:
                        if instance.vpc_attributes.private_ip_address.ip_address:
                            private_ip = instance.vpc_attributes.private_ip_address.ip_address[0]
                    
                    # Extract info from tags
                    name = instance.instance_name or ""
                    location_name = str(self.region_id)
                    nodes_count = 1
                    
                    if instance.tags and instance.tags.tag:
                        for tag in instance.tags.tag:
                            if getattr(tag, "tag_key", None) == "LocationName" and getattr(tag, "tag_value", None):
                                location_name = str(tag.tag_value)
                            elif getattr(tag, "tag_key", None) == "NodesCount":
                                tag_val = getattr(tag, "tag_value", None)
                                if tag_val is not None:
                                    try:
                                        nodes_count = int(str(tag_val))
                                    except Exception:
                                        nodes_count = 1
                    
                    # Determine state safely
                    status_val = getattr(instance, "status", None)
                    status_key = str(status_val) if status_val is not None else ""
                    state_val = ALIBABA_STATE_MAP.get(status_key, InstanceState.UNKNOWN)

                    results[str(instance_id)] = InstanceInfo(
                        instance_id=str(instance_id),
                        provider=CloudProvider.ALIBABA,
                        region_id=str(self.region_id),
                        location_name=location_name,
                        instance_type=str(getattr(instance, "instance_type", "")),
                        public_ip=public_ip,
                        private_ip=private_ip,
                        state=state_val,
                        nodes_count=nodes_count,
                        name=name,
                        launch_time=getattr(instance, "creation_time", None),
                    )
                    
        except Exception as e:
            raise RuntimeError(f"Failed to get instance status: {e}")
        
        return results
    
    def wait_for_instances_running(
        self, 
        instance_ids: List[str], 
        timeout_seconds: int = 300
    ) -> Dict[str, InstanceInfo]:
        """Wait for ECS instances to reach running state with public IPs"""
        
        if not instance_ids:
            return {}
        
        start_time = time.time()
        
        while time.time() - start_time < timeout_seconds:
            statuses = self.get_instance_status(instance_ids)
            
            all_running = True
            all_have_ips = True
            
            for instance_id in instance_ids:
                if instance_id not in statuses:
                    all_running = False
                    break
                    
                info = statuses[instance_id]
                if info.state != InstanceState.RUNNING:
                    all_running = False
                if not info.public_ip:
                    all_have_ips = False
            
            if all_running and all_have_ips:
                return statuses
            
            time.sleep(5)
        
        raise TimeoutError(
            f"Instances did not become running within {timeout_seconds} seconds"
        )
    
    def list_instances_by_tag(
        self, 
        tag_key: str, 
        tag_value: str
    ) -> List[InstanceInfo]:
        """List ECS instances matching a specific tag"""
        
        try:
            request = ecs_models.DescribeInstancesRequest(
                region_id=self.region_id,
                tag=[
                    ecs_models.DescribeInstancesRequestTag(
                        key=tag_key,
                        value=tag_value,
                    )
                ],
            )
            
            response = self.ecs_client.describe_instances(request)
            
            instances = []
            if response.body and response.body.instances:
                for instance in response.body.instances.instance:
                    # Skip terminated instances
                    if instance.status in ["Deleted", "Released"]:
                        continue
                    
                    # Get public IP
                    public_ip = None
                    if instance.public_ip_address and instance.public_ip_address.ip_address:
                        public_ip = instance.public_ip_address.ip_address[0]
                    elif instance.eip_address and instance.eip_address.ip_address:
                        public_ip = instance.eip_address.ip_address
                    
                    # Get private IP
                    private_ip = None
                    if instance.vpc_attributes and instance.vpc_attributes.private_ip_address:
                        if instance.vpc_attributes.private_ip_address.ip_address:
                            private_ip = instance.vpc_attributes.private_ip_address.ip_address[0]
                    
                    # Extract info from tags
                    name = instance.instance_name or ""
                    location_name = str(self.region_id)
                    nodes_count = 1

                    if instance.tags and instance.tags.tag:
                        for tag in instance.tags.tag:
                            if getattr(tag, "tag_key", None) == "LocationName" and getattr(tag, "tag_value", None):
                                location_name = str(tag.tag_value)
                            elif getattr(tag, "tag_key", None) == "NodesCount":
                                tag_val = getattr(tag, "tag_value", None)
                                if tag_val is not None:
                                    try:
                                        nodes_count = int(str(tag_val))
                                    except Exception:
                                        nodes_count = 1

                    status_val = getattr(instance, "status", None)
                    status_key = str(status_val) if status_val is not None else ""
                    state_val = ALIBABA_STATE_MAP.get(status_key, InstanceState.UNKNOWN)

                    instances.append(InstanceInfo(
                        instance_id=str(getattr(instance, "instance_id", "")),
                        provider=CloudProvider.ALIBABA,
                        region_id=str(self.region_id),
                        location_name=location_name,
                        instance_type=str(getattr(instance, "instance_type", "")),
                        public_ip=public_ip,
                        private_ip=private_ip,
                        state=state_val,
                        nodes_count=nodes_count,
                        name=name,
                        launch_time=getattr(instance, "creation_time", None),
                    ))
            
            return instances
            
        except Exception as e:
            raise RuntimeError(f"Failed to list instances by tag: {e}")

    def list_instances_by_name_prefix(self, name_prefix: str) -> List[InstanceInfo]:
        """List ECS instances whose instance_name starts with the given prefix.

        Alibaba ECS doesn't provide an efficient prefix filter in this SDK wrapper,
        so we list instances tagged as CreatedBy=conflux-deployer and filter locally.
        """

        candidates = self.list_instances_by_tag("CreatedBy", "conflux-deployer")
        return [i for i in candidates if (i.name or "").startswith(name_prefix)]
    
    # ==================== Image Operations ====================
    
    def create_image(
        self,
        instance_id: str,
        image_name: str,
        description: str = "",
        wait_for_available: bool = True,
        timeout_seconds: int = 1800,
    ) -> ImageInfo:
        """Create an image from an ECS instance"""
        
        try:
            request = ecs_models.CreateImageRequest(
                region_id=self.region_id,
                instance_id=instance_id,
                image_name=image_name,
                description=description or f"Conflux test node image created at {datetime.now().isoformat()}",
                tag=[
                    ecs_models.CreateImageRequestTag(
                        key="Name",
                        value=image_name,
                    ),
                    ecs_models.CreateImageRequestTag(
                        key="CreatedBy",
                        value="conflux-deployer",
                    ),
                ]
            )
            
            response = self.ecs_client.create_image(request)
            image_id = getattr(response.body, "image_id", None)
            if not image_id:
                raise RuntimeError("Failed to create image: no image_id returned")
            
            if wait_for_available:
                start_time = time.time()
                while time.time() - start_time < timeout_seconds:
                    describe_request = ecs_models.DescribeImagesRequest(
                        region_id=self.region_id,
                        image_id=image_id,
                    )
                    describe_response = self.ecs_client.describe_images(describe_request)
                    
                    if describe_response.body and describe_response.body.images and describe_response.body.images.image:
                        state = getattr(describe_response.body.images.image[0], "status", None)
                        if state == "Available":
                            break
                        elif state == "CreateFailed":
                            raise RuntimeError(f"Image creation failed: {image_id}")
                    
                    time.sleep(10)
                else:
                    raise TimeoutError(f"Image did not become available within {timeout_seconds} seconds")
            
            return ImageInfo(
                image_id=str(image_id),
                name=str(image_name),
                provider=CloudProvider.ALIBABA,
                region_id=str(self.region_id),
                state="Available" if wait_for_available else "Creating",
                creation_date=datetime.now().isoformat(),
                description=description,
            )
            
        except Exception as e:
            raise RuntimeError(f"Failed to create image: {e}")
    
    def delete_image(self, image_id: str) -> bool:
        """Delete an image"""
        
        try:
            request = ecs_models.DeleteImageRequest(
                region_id=self.region_id,
                image_id=image_id,
                force=True,
            )
            
            self.ecs_client.delete_image(request)
            return True
            
        except Exception as e:
            raise RuntimeError(f"Failed to delete image: {e}")
    
    def find_image_by_name(self, name_pattern: str) -> Optional[ImageInfo]:
        """Find an image by name pattern"""
        
        try:
            request = ecs_models.DescribeImagesRequest(
                region_id=self.region_id,
                image_name=name_pattern,
                image_owner_alias="self",
                status="Available",
            )
            
            response = self.ecs_client.describe_images(request)
            
            if response.body and response.body.images and response.body.images.image:
                # Return the most recent image
                images = sorted(
                    response.body.images.image,
                    key=lambda x: x.creation_time or "",
                    reverse=True
                )
                image = images[0]
                
                return ImageInfo(
                    image_id=str(getattr(image, "image_id", "")),
                    name=str(getattr(image, "image_name", "")),
                    provider=CloudProvider.ALIBABA,
                    region_id=str(self.region_id),
                    state=str(getattr(image, "status", "")),
                    creation_date=str(getattr(image, "creation_time", "")),
                    description=str(getattr(image, "description", "")),
                )
            
            return None
            
        except Exception as e:
            raise RuntimeError(f"Failed to find image: {e}")
    
    def get_base_ubuntu_image(self, ubuntu_version: str = "22.04") -> str:
        """Get the base Ubuntu image ID for this region"""
        
        try:
            # Search for official Ubuntu images
            request = ecs_models.DescribeImagesRequest(
                region_id=self.region_id,
                image_owner_alias="system",  # Official images
                ostype="linux",
                status="Available",
                image_name=f"ubuntu_{ubuntu_version.replace('.', '_')}*",
            )
            
            response = self.ecs_client.describe_images(request)
            
            if response.body and response.body.images and response.body.images.image:
                # Return the most recent image
                images = sorted(
                    response.body.images.image,
                    key=lambda x: x.creation_time or "",
                    reverse=True
                )
                img_id = getattr(images[0], "image_id", None)
                if not img_id:
                    raise RuntimeError(f"Found image without image_id in region {self.region_id}")
                return str(img_id)
            
            raise RuntimeError(f"No Ubuntu {ubuntu_version} image found in region {self.region_id}")
            
        except Exception as e:
            raise RuntimeError(f"Failed to find Ubuntu image: {e}")
    
    # ==================== Security Group Operations ====================
    
    def create_security_group(
        self,
        name: str,
        description: str,
        vpc_id: Optional[str] = None,
        rules: Optional[List[SecurityGroupRule]] = None,
    ) -> str:
        """Create an ECS security group with rules"""
        
        try:
            request = ecs_models.CreateSecurityGroupRequest(
                region_id=self.region_id,
                security_group_name=name,
                description=description,
                tag=[
                    ecs_models.CreateSecurityGroupRequestTag(
                        key="Name",
                        value=name,
                    ),
                    ecs_models.CreateSecurityGroupRequestTag(
                        key="CreatedBy",
                        value="conflux-deployer",
                    ),
                ]
            )
            
            if vpc_id:
                request.vpc_id = vpc_id
            
            response = self.ecs_client.create_security_group(request)
            sgid = getattr(response.body, "security_group_id", None)
            if not sgid:
                raise RuntimeError("Failed to create security group: no id returned")
            security_group_id = str(sgid)
            
            # Add rules
            if rules:
                for rule in rules:
                    # Convert protocol
                    protocol = rule.protocol.upper()
                    if protocol == "-1":
                        protocol = "all"
                    
                    for cidr in rule.cidr_blocks:
                        authorize_request = ecs_models.AuthorizeSecurityGroupRequest(
                            region_id=self.region_id,
                            security_group_id=security_group_id,
                            ip_protocol=protocol,
                            port_range=f"{rule.from_port}/{rule.to_port}" if protocol != "ICMP" else "-1/-1",
                            source_cidr_ip=cidr,
                            description=rule.description,
                        )
                        
                        self.ecs_client.authorize_security_group(authorize_request)
            
            return security_group_id
            
        except Exception as e:
            raise RuntimeError(f"Failed to create security group: {e}")
    
    def delete_security_group(self, security_group_id: str) -> bool:
        """Delete an ECS security group"""
        
        try:
            request = ecs_models.DeleteSecurityGroupRequest(
                region_id=self.region_id,
                security_group_id=security_group_id,
            )
            
            self.ecs_client.delete_security_group(request)
            return True
            
        except Exception as e:
            raise RuntimeError(f"Failed to delete security group: {e}")
    
    def find_security_group_by_name(self, name: str) -> Optional[str]:
        """Find a security group by name"""
        
        try:
            request = ecs_models.DescribeSecurityGroupsRequest(
                region_id=self.region_id,
                security_group_name=name,
            )
            
            response = self.ecs_client.describe_security_groups(request)
            
            if response.body and response.body.security_groups:
                if response.body.security_groups.security_group:
                    return response.body.security_groups.security_group[0].security_group_id
            
            return None
            
        except Exception as e:
            raise RuntimeError(f"Failed to find security group: {e}")
    
    # ==================== Key Pair Operations ====================
    
    def create_key_pair(self, key_name: str) -> str:
        """Create an ECS key pair"""
        
        try:
            request = ecs_models.CreateKeyPairRequest(
                region_id=self.region_id,
                key_pair_name=key_name,
            )
            
            response = self.ecs_client.create_key_pair(request)
            private_key = getattr(response.body, "private_key_body", None)
            if not private_key:
                raise RuntimeError("Create key pair returned no private key")
            return str(private_key)
            
        except Exception as e:
            raise RuntimeError(f"Failed to create key pair: {e}")
    
    def delete_key_pair(self, key_name: str) -> bool:
        """Delete an ECS key pair"""
        
        try:
            request = ecs_models.DeleteKeyPairsRequest(
                region_id=self.region_id,
                key_pair_names=f'["{key_name}"]',
            )
            
            self.ecs_client.delete_key_pairs(request)
            return True
            
        except Exception as e:
            raise RuntimeError(f"Failed to delete key pair: {e}")
    
    def key_pair_exists(self, key_name: str) -> bool:
        """Check if a key pair exists"""
        
        try:
            request = ecs_models.DescribeKeyPairsRequest(
                region_id=self.region_id,
                key_pair_name=key_name,
            )
            
            response = self.ecs_client.describe_key_pairs(request)
            
            if response.body and response.body.key_pairs:
                if response.body.key_pairs.key_pair:
                    return len(response.body.key_pairs.key_pair) > 0
            
            return False
            
        except Exception:
            return False
    
    # ==================== Utility Methods ====================
    
    def get_available_regions(self) -> List[str]:
        """Get list of available Alibaba Cloud regions"""
        
        try:
            request = ecs_models.DescribeRegionsRequest()
            response = self.ecs_client.describe_regions(request)
            
            regions = []
            if response.body and response.body.regions:
                for region in response.body.regions.region:
                    regions.append(region.region_id)
            
            return regions
            
        except Exception as e:
            raise RuntimeError(f"Failed to get regions: {e}")
    
    def validate_instance_type(self, instance_type: str) -> bool:
        """Check if an instance type is valid in this region"""
        
        try:
            request = ecs_models.DescribeInstanceTypesRequest(
                instance_type_family=instance_type.rsplit(".", 1)[0] if "." in instance_type else "",
            )
            
            response = self.ecs_client.describe_instance_types(request)
            
            if response.body and response.body.instance_types:
                for it in response.body.instance_types.instance_type:
                    if it.instance_type_id == instance_type:
                        return True
            
            return False
            
        except Exception:
            return False
