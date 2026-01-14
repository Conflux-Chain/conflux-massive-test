"""
AWS Cloud Provider Implementation

Implements CloudProviderBase for Amazon Web Services using boto3.
"""

import base64
import time
from datetime import datetime
from typing import List, Dict, Optional, Any

import boto3
from botocore.exceptions import ClientError

from ..configs import (
    CloudProvider,
    CloudCredentials,
    InstanceInfo,
    InstanceState,
)
from .base import CloudProviderBase, ImageInfo, SecurityGroupRule


# Mapping AWS instance states to our InstanceState enum
AWS_STATE_MAP = {
    "pending": InstanceState.PENDING,
    "running": InstanceState.RUNNING,
    "stopping": InstanceState.STOPPING,
    "stopped": InstanceState.STOPPED,
    "shutting-down": InstanceState.TERMINATED,
    "terminated": InstanceState.TERMINATED,
}

# Ubuntu AMI IDs by region (Ubuntu 22.04 LTS)
# These are official Canonical AMIs - they may need updating
UBUNTU_AMIS = {
    "us-east-1": "ami-0c7217cdde317cfec",
    "us-east-2": "ami-0b8b44ec9a8f90422",
    "us-west-1": "ami-0ce2cb35386fc22e9",
    "us-west-2": "ami-008fe2fc65df48dac",
    "eu-west-1": "ami-0905a3c97561e0b69",
    "eu-west-2": "ami-0e5f882be1900e43b",
    "eu-west-3": "ami-0d3c032f5934e1b41",
    "eu-central-1": "ami-0faab6bdbac9486fb",
    "eu-north-1": "ami-0014ce3e52359afbd",
    "ap-northeast-1": "ami-0d52744d6551d851e",
    "ap-northeast-2": "ami-0c9c942bd7bf113a2",
    "ap-northeast-3": "ami-0e0c8aacbcb3e6b1c",
    "ap-southeast-1": "ami-078c1149d8ad719a7",
    "ap-southeast-2": "ami-04f5097681773b989",
    "ap-south-1": "ami-0287a05f0ef0e9d9a",
    "sa-east-1": "ami-0fb4cf3a99aa89f72",
    "ca-central-1": "ami-0a2e7efb4257c0907",
}


class AWSProvider(CloudProviderBase):
    """
    AWS Cloud Provider implementation using boto3.
    
    This class handles all AWS-specific operations for:
    - EC2 instance management
    - AMI (image) management
    - Security groups
    - Key pairs
    """
    
    def __init__(self, credentials: CloudCredentials, region_id: str):
        super().__init__(credentials, region_id)
        self._ec2_client: Any = None
        self._ec2_resource: Any = None
    
    @property
    def provider_type(self) -> CloudProvider:
        return CloudProvider.AWS
    
    def initialize_client(self) -> None:
        """Initialize boto3 EC2 client and resource"""
        session = boto3.Session(
            aws_access_key_id=self.credentials.access_key_id,
            aws_secret_access_key=self.credentials.secret_access_key,
            aws_session_token=self.credentials.session_token,
            region_name=self.region_id,
        )
        self._ec2_client = session.client('ec2')
        self._ec2_resource = session.resource('ec2')
    
    @property
    def ec2_client(self):
        if self._ec2_client is None:
            self.initialize_client()
        return self._ec2_client
    
    @property
    def ec2_resource(self):
        if self._ec2_resource is None:
            self.initialize_client()
        return self._ec2_resource
    
    # ==================== Instance Operations ====================
    
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
        """Launch EC2 instances"""
        
        # Build launch parameters
        launch_params: Dict[str, Any] = {
            "ImageId": image_id,
            "InstanceType": instance_type,
            "MinCount": count,
            "MaxCount": count,
        }
        
        # Add optional parameters
        if security_group_id:
            launch_params["SecurityGroupIds"] = [security_group_id]
        
        if subnet_id:
            launch_params["SubnetId"] = subnet_id
        
        if key_name:
            launch_params["KeyName"] = key_name
        
        if user_data:
            # Encode user data to base64 if not already encoded
            try:
                base64.b64decode(user_data)
                launch_params["UserData"] = user_data
            except Exception:
                launch_params["UserData"] = base64.b64encode(user_data.encode()).decode()
        
        # Build tags
        tag_specs = [
            {
                "ResourceType": "instance",
                "Tags": [
                    {"Key": "Name", "Value": f"{name_prefix}-{i}"}
                    for i in range(count)
                ]
            }
        ]
        
        if tags:
            for key, value in tags.items():
                tag_specs[0]["Tags"].append({"Key": key, "Value": value})
        
        # Add deployment tag for identification
        tag_specs[0]["Tags"].append({"Key": "DeploymentPrefix", "Value": name_prefix})
        tag_specs[0]["Tags"].append({"Key": "CreatedBy", "Value": "conflux-deployer"})
        
        launch_params["TagSpecifications"] = tag_specs
        
        # Launch instances
        response = self.ec2_client.run_instances(**launch_params)
        
        # Convert to InstanceInfo
        instances = []
        for i, instance in enumerate(response["Instances"]):
            instance_info = InstanceInfo(
                instance_id=instance["InstanceId"],
                provider=CloudProvider.AWS,
                region_id=self.region_id,
                location_name=self.region_id,  # Will be updated later with friendly name
                instance_type=instance_type,
                public_ip=instance.get("PublicIpAddress"),
                private_ip=instance.get("PrivateIpAddress"),
                state=AWS_STATE_MAP.get(instance["State"]["Name"], InstanceState.UNKNOWN),
                name=f"{name_prefix}-{i}",
                launch_time=instance["LaunchTime"].isoformat() if instance.get("LaunchTime") else None,
            )
            instances.append(instance_info)
        
        return instances
    
    def terminate_instances(self, instance_ids: List[str]) -> Dict[str, bool]:
        """Terminate EC2 instances"""
        results = {}
        
        if not instance_ids:
            return results
        
        try:
            response = self.ec2_client.terminate_instances(InstanceIds=instance_ids)
            
            for change in response.get("TerminatingInstances", []):
                instance_id = change["InstanceId"]
                results[instance_id] = True
            
            # Mark any not in response as failed
            for instance_id in instance_ids:
                if instance_id not in results:
                    results[instance_id] = False
                    
        except ClientError as e:
            # Mark all as failed on error
            for instance_id in instance_ids:
                results[instance_id] = False
            raise RuntimeError(f"Failed to terminate instances: {e}")
        
        return results
    
    def stop_instances(self, instance_ids: List[str]) -> Dict[str, bool]:
        """Stop EC2 instances"""
        results = {}
        
        if not instance_ids:
            return results
        
        try:
            response = self.ec2_client.stop_instances(InstanceIds=instance_ids)
            
            for change in response.get("StoppingInstances", []):
                instance_id = change["InstanceId"]
                results[instance_id] = True
            
            for instance_id in instance_ids:
                if instance_id not in results:
                    results[instance_id] = False
                    
        except ClientError as e:
            for instance_id in instance_ids:
                results[instance_id] = False
            raise RuntimeError(f"Failed to stop instances: {e}")
        
        return results
    
    def start_instances(self, instance_ids: List[str]) -> Dict[str, bool]:
        """Start stopped EC2 instances"""
        results = {}
        
        if not instance_ids:
            return results
        
        try:
            response = self.ec2_client.start_instances(InstanceIds=instance_ids)
            
            for change in response.get("StartingInstances", []):
                instance_id = change["InstanceId"]
                results[instance_id] = True
            
            for instance_id in instance_ids:
                if instance_id not in results:
                    results[instance_id] = False
                    
        except ClientError as e:
            for instance_id in instance_ids:
                results[instance_id] = False
            raise RuntimeError(f"Failed to start instances: {e}")
        
        return results
    
    def get_instance_status(self, instance_ids: List[str]) -> Dict[str, InstanceInfo]:
        """Get current status of EC2 instances"""
        results = {}
        
        if not instance_ids:
            return results
        
        try:
            response = self.ec2_client.describe_instances(InstanceIds=instance_ids)
            
            for reservation in response.get("Reservations", []):
                for instance in reservation.get("Instances", []):
                    instance_id = instance["InstanceId"]
                    
                    # Extract name from tags
                    name = ""
                    location_name = self.region_id
                    nodes_count = 1
                    for tag in instance.get("Tags", []):
                        if tag["Key"] == "Name":
                            name = tag["Value"]
                        elif tag["Key"] == "LocationName":
                            location_name = tag["Value"]
                        elif tag["Key"] == "NodesCount":
                            nodes_count = int(tag["Value"])
                    
                    results[instance_id] = InstanceInfo(
                        instance_id=instance_id,
                        provider=CloudProvider.AWS,
                        region_id=self.region_id,
                        location_name=location_name,
                        instance_type=instance["InstanceType"],
                        public_ip=instance.get("PublicIpAddress"),
                        private_ip=instance.get("PrivateIpAddress"),
                        state=AWS_STATE_MAP.get(instance["State"]["Name"], InstanceState.UNKNOWN),
                        nodes_count=nodes_count,
                        name=name,
                        launch_time=instance["LaunchTime"].isoformat() if instance.get("LaunchTime") else None,
                    )
                    
        except ClientError as e:
            raise RuntimeError(f"Failed to get instance status: {e}")
        
        return results
    
    def wait_for_instances_running(
        self, 
        instance_ids: List[str], 
        timeout_seconds: int = 300
    ) -> Dict[str, InstanceInfo]:
        """Wait for EC2 instances to reach running state with public IPs"""
        
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
        """List EC2 instances matching a specific tag"""
        
        try:
            response = self.ec2_client.describe_instances(
                Filters=[
                    {"Name": f"tag:{tag_key}", "Values": [tag_value]},
                    {"Name": "instance-state-name", "Values": ["pending", "running", "stopping", "stopped"]},
                ]
            )
            
            instances = []
            for reservation in response.get("Reservations", []):
                for instance in reservation.get("Instances", []):
                    # Extract name from tags
                    name = ""
                    location_name = self.region_id
                    nodes_count = 1
                    for tag in instance.get("Tags", []):
                        if tag["Key"] == "Name":
                            name = tag["Value"]
                        elif tag["Key"] == "LocationName":
                            location_name = tag["Value"]
                        elif tag["Key"] == "NodesCount":
                            nodes_count = int(tag["Value"])
                    
                    instances.append(InstanceInfo(
                        instance_id=instance["InstanceId"],
                        provider=CloudProvider.AWS,
                        region_id=self.region_id,
                        location_name=location_name,
                        instance_type=instance["InstanceType"],
                        public_ip=instance.get("PublicIpAddress"),
                        private_ip=instance.get("PrivateIpAddress"),
                        state=AWS_STATE_MAP.get(instance["State"]["Name"], InstanceState.UNKNOWN),
                        nodes_count=nodes_count,
                        name=name,
                        launch_time=instance["LaunchTime"].isoformat() if instance.get("LaunchTime") else None,
                    ))
            
            return instances
            
        except ClientError as e:
            raise RuntimeError(f"Failed to list instances by tag: {e}")

    def list_instances_by_name_prefix(self, name_prefix: str) -> List[InstanceInfo]:
        """List EC2 instances whose Name tag starts with the given prefix."""
        try:
            response = self.ec2_client.describe_instances(
                Filters=[
                    {"Name": "tag:Name", "Values": [f"{name_prefix}*"]},
                    {"Name": "instance-state-name", "Values": ["pending", "running", "stopping", "stopped"]},
                ]
            )

            instances: List[InstanceInfo] = []
            for reservation in response.get("Reservations", []):
                for instance in reservation.get("Instances", []):
                    name = ""
                    location_name = self.region_id
                    nodes_count = 1
                    for tag in instance.get("Tags", []):
                        if tag["Key"] == "Name":
                            name = tag["Value"]
                        elif tag["Key"] == "LocationName":
                            location_name = tag["Value"]
                        elif tag["Key"] == "NodesCount":
                            nodes_count = int(tag["Value"])

                    instances.append(
                        InstanceInfo(
                            instance_id=instance["InstanceId"],
                            provider=CloudProvider.AWS,
                            region_id=self.region_id,
                            location_name=location_name,
                            instance_type=instance["InstanceType"],
                            public_ip=instance.get("PublicIpAddress"),
                            private_ip=instance.get("PrivateIpAddress"),
                            state=AWS_STATE_MAP.get(instance["State"]["Name"], InstanceState.UNKNOWN),
                            nodes_count=nodes_count,
                            name=name,
                            launch_time=instance["LaunchTime"].isoformat() if instance.get("LaunchTime") else None,
                        )
                    )

            return instances
        except ClientError as e:
            raise RuntimeError(f"Failed to list instances by name prefix: {e}")
    
    # ==================== Image Operations ====================
    
    def create_image(
        self,
        instance_id: str,
        image_name: str,
        description: str = "",
        wait_for_available: bool = True,
        timeout_seconds: int = 1800,
    ) -> ImageInfo:
        """Create an AMI from an EC2 instance"""
        
        try:
            response = self.ec2_client.create_image(
                InstanceId=instance_id,
                Name=image_name,
                Description=description or f"Conflux test node image created at {datetime.now().isoformat()}",
                NoReboot=False,
            )
            
            image_id = response["ImageId"]
            
            # Add tags to the image
            self.ec2_client.create_tags(
                Resources=[image_id],
                Tags=[
                    {"Key": "Name", "Value": image_name},
                    {"Key": "CreatedBy", "Value": "conflux-deployer"},
                ]
            )
            
            if wait_for_available:
                start_time = time.time()
                while time.time() - start_time < timeout_seconds:
                    image_response = self.ec2_client.describe_images(ImageIds=[image_id])
                    if image_response["Images"]:
                        state = image_response["Images"][0]["State"]
                        if state == "available":
                            break
                        elif state == "failed":
                            raise RuntimeError(f"Image creation failed: {image_id}")
                    time.sleep(10)
                else:
                    raise TimeoutError(f"Image did not become available within {timeout_seconds} seconds")
            
            return ImageInfo(
                image_id=image_id,
                name=image_name,
                provider=CloudProvider.AWS,
                region_id=self.region_id,
                state="available" if wait_for_available else "pending",
                creation_date=datetime.now().isoformat(),
                description=description,
            )
            
        except ClientError as e:
            raise RuntimeError(f"Failed to create image: {e}")
    
    def delete_image(self, image_id: str) -> bool:
        """Delete an AMI"""
        
        try:
            # First, get the snapshot IDs associated with this AMI
            response = self.ec2_client.describe_images(ImageIds=[image_id])
            snapshot_ids = []
            
            if response["Images"]:
                for block_device in response["Images"][0].get("BlockDeviceMappings", []):
                    if "Ebs" in block_device and "SnapshotId" in block_device["Ebs"]:
                        snapshot_ids.append(block_device["Ebs"]["SnapshotId"])
            
            # Deregister the AMI
            self.ec2_client.deregister_image(ImageId=image_id)
            
            # Delete associated snapshots
            for snapshot_id in snapshot_ids:
                try:
                    self.ec2_client.delete_snapshot(SnapshotId=snapshot_id)
                except ClientError:
                    pass  # Ignore errors deleting snapshots
            
            return True
            
        except ClientError as e:
            raise RuntimeError(f"Failed to delete image: {e}")
    
    def find_image_by_name(self, name_pattern: str) -> Optional[ImageInfo]:
        """Find an AMI by name pattern"""
        
        try:
            response = self.ec2_client.describe_images(
                Owners=["self"],
                Filters=[
                    {"Name": "name", "Values": [name_pattern]},
                    {"Name": "state", "Values": ["available"]},
                ]
            )
            
            if response["Images"]:
                # Return the most recent image
                images = sorted(
                    response["Images"],
                    key=lambda x: x.get("CreationDate", ""),
                    reverse=True
                )
                image = images[0]
                
                return ImageInfo(
                    image_id=image["ImageId"],
                    name=image["Name"],
                    provider=CloudProvider.AWS,
                    region_id=self.region_id,
                    state=image["State"],
                    creation_date=image.get("CreationDate"),
                    description=image.get("Description"),
                )
            
            return None
            
        except ClientError as e:
            raise RuntimeError(f"Failed to find image: {e}")
    
    def get_base_ubuntu_image(self, ubuntu_version: str = "22.04") -> str:
        """Get the base Ubuntu AMI ID for this region"""
        
        # Try to use the pre-defined AMI
        if self.region_id in UBUNTU_AMIS:
            return UBUNTU_AMIS[self.region_id]
        
        # Otherwise, search for the latest Ubuntu AMI
        try:
            response = self.ec2_client.describe_images(
                Owners=["099720109477"],  # Canonical
                Filters=[
                    {"Name": "name", "Values": [f"ubuntu/images/hvm-ssd/ubuntu-jammy-{ubuntu_version.replace('.', '')}-amd64-server-*"]},
                    {"Name": "state", "Values": ["available"]},
                    {"Name": "architecture", "Values": ["x86_64"]},
                ]
            )
            
            if response["Images"]:
                # Return the most recent image
                images = sorted(
                    response["Images"],
                    key=lambda x: x.get("CreationDate", ""),
                    reverse=True
                )
                return images[0]["ImageId"]
            
            raise RuntimeError(f"No Ubuntu {ubuntu_version} AMI found in region {self.region_id}")
            
        except ClientError as e:
            raise RuntimeError(f"Failed to find Ubuntu AMI: {e}")
    
    # ==================== Security Group Operations ====================
    
    def create_security_group(
        self,
        name: str,
        description: str,
        vpc_id: Optional[str] = None,
        rules: Optional[List[SecurityGroupRule]] = None,
    ) -> str:
        """Create an EC2 security group with rules"""
        
        try:
            create_params = {
                "GroupName": name,
                "Description": description,
            }
            
            if vpc_id:
                create_params["VpcId"] = vpc_id
            
            response = self.ec2_client.create_security_group(**create_params)
            security_group_id = response["GroupId"]
            
            # Add rules
            if rules:
                ingress_rules = []
                for rule in rules:
                    ip_permissions: Dict[str, Any] = {
                        "IpProtocol": rule.protocol if rule.protocol != "icmp" else "icmp",
                        "IpRanges": [{"CidrIp": cidr, "Description": rule.description} for cidr in rule.cidr_blocks],
                    }
                    
                    if rule.protocol != "icmp":
                        ip_permissions["FromPort"] = rule.from_port
                        ip_permissions["ToPort"] = rule.to_port
                    else:
                        ip_permissions["FromPort"] = -1
                        ip_permissions["ToPort"] = -1
                    
                    ingress_rules.append(ip_permissions)
                
                self.ec2_client.authorize_security_group_ingress(
                    GroupId=security_group_id,
                    IpPermissions=ingress_rules,
                )
            
            # Add tags
            self.ec2_client.create_tags(
                Resources=[security_group_id],
                Tags=[
                    {"Key": "Name", "Value": name},
                    {"Key": "CreatedBy", "Value": "conflux-deployer"},
                ]
            )
            
            return security_group_id
            
        except ClientError as e:
            raise RuntimeError(f"Failed to create security group: {e}")
    
    def delete_security_group(self, security_group_id: str) -> bool:
        """Delete an EC2 security group"""
        
        try:
            self.ec2_client.delete_security_group(GroupId=security_group_id)
            return True
        except ClientError as e:
            raise RuntimeError(f"Failed to delete security group: {e}")
    
    def find_security_group_by_name(self, name: str) -> Optional[str]:
        """Find a security group by name"""
        
        try:
            response = self.ec2_client.describe_security_groups(
                Filters=[
                    {"Name": "group-name", "Values": [name]},
                ]
            )
            
            if response["SecurityGroups"]:
                return response["SecurityGroups"][0]["GroupId"]
            
            return None
            
        except ClientError as e:
            # Return None if not found
            if "InvalidGroup.NotFound" in str(e):
                return None
            raise RuntimeError(f"Failed to find security group: {e}")
    
    # ==================== Key Pair Operations ====================
    
    def create_key_pair(self, key_name: str) -> str:
        """Create an EC2 key pair"""
        
        try:
            response = self.ec2_client.create_key_pair(KeyName=key_name)
            return response["KeyMaterial"]
        except ClientError as e:
            raise RuntimeError(f"Failed to create key pair: {e}")
    
    def delete_key_pair(self, key_name: str) -> bool:
        """Delete an EC2 key pair"""
        
        try:
            self.ec2_client.delete_key_pair(KeyName=key_name)
            return True
        except ClientError as e:
            raise RuntimeError(f"Failed to delete key pair: {e}")
    
    def key_pair_exists(self, key_name: str) -> bool:
        """Check if a key pair exists"""
        
        try:
            response = self.ec2_client.describe_key_pairs(
                Filters=[{"Name": "key-name", "Values": [key_name]}]
            )
            return len(response["KeyPairs"]) > 0
        except ClientError:
            return False
    
    # ==================== Utility Methods ====================
    
    def get_available_regions(self) -> List[str]:
        """Get list of available AWS regions"""
        
        try:
            response = self.ec2_client.describe_regions()
            return [region["RegionName"] for region in response["Regions"]]
        except ClientError as e:
            raise RuntimeError(f"Failed to get regions: {e}")
    
    def validate_instance_type(self, instance_type: str) -> bool:
        """Check if an instance type is valid in this region"""
        
        try:
            response = self.ec2_client.describe_instance_type_offerings(
                LocationType="region",
                Filters=[
                    {"Name": "instance-type", "Values": [instance_type]},
                ]
            )
            return len(response["InstanceTypeOfferings"]) > 0
        except ClientError:
            return False
