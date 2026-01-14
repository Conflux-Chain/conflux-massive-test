"""Cloud Server Deployer"""

import time
from typing import Dict, Any, List, Optional
from dataclasses import dataclass

from loguru import logger
from conflux_deployer.configs.config_manager import ConfigManager
from conflux_deployer.cloud_accounts.account_manager import CloudAccountManager
from conflux_deployer.image_management import ImageManager


@dataclass
class ServerInstance:
    """Server instance information"""
    instance_id: str
    cloud_provider: str
    region: str
    instance_type: str
    ip_address: str
    status: str
    purpose: str
    created_at: str
    nodes_count: int = 1


class ServerDeployer:
    """Server Deployer for AWS and Alibaba Cloud"""
    
    def __init__(self, config_manager: ConfigManager, cloud_account_manager: CloudAccountManager, image_manager: ImageManager):
        """Initialize Server Deployer"""
        self.config_manager = config_manager
        self.cloud_account_manager = cloud_account_manager
        self.image_manager = image_manager
        self.instances: Dict[str, ServerInstance] = {}
    
    def deploy_servers(self, cloud_provider: str, region: str, instance_type: str, count: int, purpose: str = "conflux-node") -> List[ServerInstance]:
        """Deploy servers across regions"""
        logger.info(f"Deploying {count} {instance_type} servers in {region} ({cloud_provider}) for {purpose}")
        
        # Get instance configuration
        instance_config = self.config_manager.get_instance_config(instance_type)
        
        # Determine nodes per instance
        nodes_per_instance = self._calculate_nodes_per_instance(instance_type)
        
        # Get or create server image
        base_image_id = instance_config.get("base_image_id")
        image_name = f"conflux-node-{cloud_provider}-{region}-{int(time.time())}"
        # Create or find an image for this provider/region
        image_info = self.image_manager.create_image(CloudProvider(cloud_provider), region)
        
        # Deploy instances
        instances = []
        if cloud_provider == "aws":
            instances = self._deploy_aws_servers(region, instance_type, count, image_info.image_id, purpose, nodes_per_instance)
        elif cloud_provider == "alibaba":
            instances = self._deploy_alibaba_servers(region, instance_type, count, image_info.image_id, purpose, nodes_per_instance)
        else:
            raise ValueError(f"Unsupported cloud provider: {cloud_provider}")
        
        # Store instances
        for instance in instances:
            self.instances[instance.instance_id] = instance
        
        logger.info(f"Successfully deployed {len(instances)} servers")
        return instances
    
    def _calculate_nodes_per_instance(self, instance_type: str) -> int:
        """Calculate number of Conflux nodes per instance based on instance type"""
        # AWS instance types
        if instance_type == "m6i.2xlarge":
            return 1
        elif instance_type == "m7i.2xlarge":
            return 2
        elif instance_type == "m6i.4xlarge":
            return 4
        elif instance_type == "m7i.4xlarge":
            return 6
        elif instance_type == "m6i.8xlarge":
            return 8
        elif instance_type == "m7i.8xlarge":
            return 12
        # Alibaba Cloud instance types
        elif instance_type.startswith("ecs.c6"):
            # c6.xlarge
            if "2xlarge" in instance_type:
                return 2
            elif "4xlarge" in instance_type:
                return 4
            elif "8xlarge" in instance_type:
                return 8
            else:
                return 1
        else:
            # Default to 1 node per instance
            return 1
    
    def _deploy_aws_servers(self, region: str, instance_type: str, count: int, image_id: str, purpose: str, nodes_per_instance: int) -> List[ServerInstance]:
        """Deploy AWS servers"""
        ec2_client = self.cloud_account_manager.get_aws_client("ec2", region)
        
        # Get instance configuration
        instance_config = self.config_manager.get_instance_config(instance_type)
        
        # Launch instances
        try:
            response = ec2_client.run_instances(
                ImageId=image_id,
                InstanceType=instance_type,
                MinCount=count,
                MaxCount=count,
                KeyName=instance_config.get("key_name"),
                SecurityGroupIds=instance_config.get("security_group_ids", []),
                SubnetId=instance_config.get("subnet_id"),
                BlockDeviceMappings=[
                    {
                        'DeviceName': '/dev/sda1',
                        'Ebs': {
                            'VolumeSize': instance_config.get("volume_size", 100),
                            'VolumeType': 'gp3',
                            'DeleteOnTermination': True
                        }
                    }
                ],
                TagSpecifications=[
                    {
                        'ResourceType': 'instance',
                        'Tags': [
                            {'Key': 'Name', 'Value': f"conflux-{purpose}-{region}-{int(time.time())}"},
                            {'Key': 'Purpose', 'Value': purpose},
                            {'Key': 'ConfluxNodesCount', 'Value': str(nodes_per_instance)}
                        ]
                    }
                ]
            )
        except Exception as e:
            logger.error(f"Failed to launch AWS instances: {e}")
            # Try with alternative instance type if available
            alternative_type = instance_config.get("alternative_instance_type")
            if alternative_type:
                logger.info(f"Trying alternative instance type: {alternative_type}")
                alternative_config = self.config_manager.get_instance_config(alternative_type)
                alternative_nodes_per_instance = self._calculate_nodes_per_instance(alternative_type)
                # Calculate required count for alternative type
                required_capacity = count * nodes_per_instance
                alternative_count = (required_capacity + alternative_nodes_per_instance - 1) // alternative_nodes_per_instance
                logger.info(f"Need {alternative_count} {alternative_type} instances to match capacity")
                return self._deploy_aws_servers(region, alternative_type, alternative_count, image_id, purpose, alternative_nodes_per_instance)
            raise
        
        instances = []
        for instance in response['Instances']:
            instance_id = instance['InstanceId']
            # Wait for instance to be running
            self._wait_for_aws_instance_running(ec2_client, instance_id)
            # Get instance details
            instance_details = ec2_client.describe_instances(InstanceIds=[instance_id])['Reservations'][0]['Instances'][0]
            ip_address = instance_details.get('PublicIpAddress', instance_details.get('PrivateIpAddress'))
            
            server_instance = ServerInstance(
                instance_id=instance_id,
                cloud_provider="aws",
                region=region,
                instance_type=instance_type,
                ip_address=ip_address,
                status="running",
                purpose=purpose,
                created_at=time.strftime("%Y-%m-%d %H:%M:%S"),
                nodes_count=nodes_per_instance
            )
            instances.append(server_instance)
        
        return instances
    
    def _deploy_alibaba_servers(self, region: str, instance_type: str, count: int, image_id: str, purpose: str, nodes_per_instance: int) -> List[ServerInstance]:
        """Deploy Alibaba Cloud servers"""
        ecs_client = self.cloud_account_manager.get_alibaba_client(region)
        
        # Get instance configuration
        instance_config = self.config_manager.get_instance_config(instance_type)
        
        # Launch instances
        try:
            # TODO: Implement Alibaba Cloud instance creation
            # For now, return mock instances
            instances = []
            for i in range(count):
                instance_id = f"i-{int(time.time())}-{i}"
                server_instance = ServerInstance(
                    instance_id=instance_id,
                    cloud_provider="alibaba",
                    region=region,
                    instance_type=instance_type,
                    ip_address=f"192.168.1.{i+100}",
                    status="running",
                    purpose=purpose,
                    created_at=time.strftime("%Y-%m-%d %H:%M:%S"),
                    nodes_count=nodes_per_instance
                )
                instances.append(server_instance)
            return instances
        except Exception as e:
            logger.error(f"Failed to launch Alibaba Cloud instances: {e}")
            # Try with alternative instance type if available
            alternative_type = instance_config.get("alternative_instance_type")
            if alternative_type:
                logger.info(f"Trying alternative instance type: {alternative_type}")
                alternative_config = self.config_manager.get_instance_config(alternative_type)
                alternative_nodes_per_instance = self._calculate_nodes_per_instance(alternative_type)
                # Calculate required count for alternative type
                required_capacity = count * nodes_per_instance
                alternative_count = (required_capacity + alternative_nodes_per_instance - 1) // alternative_nodes_per_instance
                logger.info(f"Need {alternative_count} {alternative_type} instances to match capacity")
                return self._deploy_alibaba_servers(region, alternative_type, alternative_count, image_id, purpose, alternative_nodes_per_instance)
            raise
    
    def _wait_for_aws_instance_running(self, ec2_client: Any, instance_id: str):
        """Wait for AWS instance to be running"""
        while True:
            response = ec2_client.describe_instances(InstanceIds=[instance_id])
            state = response['Reservations'][0]['Instances'][0]['State']['Name']
            if state == 'running':
                break
            time.sleep(5)
        logger.info(f"Instance {instance_id} is now running")
    
    def terminate_instances(self, instances: List[ServerInstance]):
        """Terminate server instances"""
        for instance in instances:
            if instance.cloud_provider == "aws":
                self._terminate_aws_instance(instance.region, instance.instance_id)
            elif instance.cloud_provider == "alibaba":
                self._terminate_alibaba_instance(instance.region, instance.instance_id)
            
            # Remove from cache
            if instance.instance_id in self.instances:
                del self.instances[instance.instance_id]
            
            logger.info(f"Terminated instance {instance.instance_id}")
    
    def _terminate_aws_instance(self, region: str, instance_id: str):
        """Terminate AWS instance"""
        ec2_client = self.cloud_account_manager.get_aws_client("ec2", region)
        ec2_client.terminate_instances(InstanceIds=[instance_id])
    
    def _terminate_alibaba_instance(self, region: str, instance_id: str):
        """Terminate Alibaba Cloud instance"""
        # TODO: Implement Alibaba Cloud instance termination
        pass
    
    def get_instance(self, instance_id: str) -> Optional[ServerInstance]:
        """Get server instance by ID"""
        return self.instances.get(instance_id)
    
    def list_instances(self, cloud_provider: Optional[str] = None, region: Optional[str] = None, purpose: Optional[str] = None) -> List[ServerInstance]:
        """List server instances"""
        instances = []
        for instance in self.instances.values():
            if cloud_provider and instance.cloud_provider != cloud_provider:
                continue
            if region and instance.region != region:
                continue
            if purpose and instance.purpose != purpose:
                continue
            instances.append(instance)
        return instances
    
    def collect_instance_info(self, instance: ServerInstance) -> Dict[str, Any]:
        """Collect detailed instance information"""
        return {
            "instance_id": instance.instance_id,
            "cloud_provider": instance.cloud_provider,
            "region": instance.region,
            "instance_type": instance.instance_type,
            "ip_address": instance.ip_address,
            "status": instance.status,
            "purpose": instance.purpose,
            "created_at": instance.created_at,
            "nodes_count": instance.nodes_count
        }
