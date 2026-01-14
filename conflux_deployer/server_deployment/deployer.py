"""
Server Deployment Module

Handles deployment of cloud server instances for Conflux nodes.
This module is responsible for:
- Launching instances across multiple regions
- Managing security groups and networking
- Tracking instance state for recovery
- Handling instance scaling when resources are limited
"""

import time
from typing import Dict, List, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass

from loguru import logger

from ..configs import (
    DeploymentConfig,
    CloudProvider,
    RegionConfig,
    InstanceInfo,
    InstanceState,
    AWS_INSTANCE_SPECS,
    ALIBABA_INSTANCE_SPECS,
)
from ..cloud import (
    CloudProviderBase,
    get_cloud_factory,
    get_default_security_rules,
)
from ..configs.loader import StateManager
from ..utils.naming import build_instance_name_prefix


@dataclass
class DeploymentPlan:
    """Plan for deploying instances in a region"""
    provider: CloudProvider
    region_id: str
    location_name: str
    # List of (instance_type, count, nodes_per_instance)
    instance_specs: List[Tuple[str, int, int]]
    image_id: str
    security_group_id: Optional[str] = None
    subnet_id: Optional[str] = None
    
    @property
    def total_instances(self) -> int:
        return sum(count for _, count, _ in self.instance_specs)
    
    @property
    def total_nodes(self) -> int:
        return sum(count * nodes for _, count, nodes in self.instance_specs)


class ServerDeployer:
    """
    Manages deployment of cloud server instances.
    
    Key features:
    - Multi-region deployment
    - Instance tracking for recovery
    - Automatic scaling to larger instances when needed
    - Security group management
    """
    
    def __init__(
        self, 
        config: DeploymentConfig, 
        state_manager: StateManager,
        images: Optional[Dict[str, Dict[str, str]]] = None,
    ):
        """
        Initialize the server deployer.
        
        Args:
            config: Deployment configuration
            state_manager: State manager for persistence
            images: Dict of provider -> region -> image_id
        """
        self.config = config
        self.state_manager = state_manager
        self.images = images or {}
        self.factory = get_cloud_factory()
        self._security_groups: Dict[str, str] = {}  # region_key -> sg_id
    
    def _get_provider(self, provider: CloudProvider, region_id: str) -> CloudProviderBase:
        """Get a cloud provider instance"""
        return self.factory.get_provider(
            provider,
            self.config.credentials[provider],
            region_id,
        )
    
    def _get_region_key(self, provider: CloudProvider, region_id: str) -> str:
        """Get a unique key for a provider/region combination"""
        return f"{provider.value}:{region_id}"
    
    def _get_security_group_name(self) -> str:
        """Get the security group name for this deployment"""
        return f"conflux-{self.config.deployment_id}"
    
    def _ensure_security_group(
        self, 
        provider: CloudProvider, 
        region_id: str,
        vpc_id: Optional[str] = None,
    ) -> str:
        """
        Ensure security group exists in the region.
        
        Args:
            provider: Cloud provider
            region_id: Region ID
            vpc_id: Optional VPC ID
            
        Returns:
            Security group ID
        """
        region_key = self._get_region_key(provider, region_id)
        
        # Check cache
        if region_key in self._security_groups:
            return self._security_groups[region_key]
        
        cloud = self._get_provider(provider, region_id)
        sg_name = self._get_security_group_name()
        
        # Check if exists
        existing = cloud.find_security_group_by_name(sg_name)
        if existing:
            self._security_groups[region_key] = existing
            return existing
        
        # Create new security group
        logger.info(f"Creating security group {sg_name} in {provider.value}/{region_id}")
        
        rules = get_default_security_rules(
            p2p_port_start=self.config.conflux_node.p2p_port_base,
            p2p_port_end=self.config.conflux_node.p2p_port_base + 100,
            rpc_port_start=self.config.conflux_node.jsonrpc_port_base,
            rpc_port_end=self.config.conflux_node.jsonrpc_port_base + 100,
        )
        
        sg_id = cloud.create_security_group(
            name=sg_name,
            description=f"Security group for Conflux deployment {self.config.deployment_id}",
            vpc_id=vpc_id,
            rules=rules,
        )
        
        self._security_groups[region_key] = sg_id
        return sg_id
    
    def _create_deployment_plan(
        self, 
        region_config: RegionConfig,
        image_id: str,
    ) -> DeploymentPlan:
        """
        Create a deployment plan for a region.
        
        If the requested instance type is not available or quota is limited,
        this will try to use larger instances with multiple nodes per instance.
        
        Args:
            region_config: Region configuration
            image_id: Image ID to use
            
        Returns:
            DeploymentPlan
        """
        provider = region_config.provider
        
        # Get instance specs for this provider
        specs = AWS_INSTANCE_SPECS if provider == CloudProvider.AWS else ALIBABA_INSTANCE_SPECS
        
        # Default plan: use specified instance type and count
        instance_specs = [(
            region_config.instance_type,
            region_config.instance_count,
            region_config.nodes_per_instance,
        )]
        
        return DeploymentPlan(
            provider=provider,
            region_id=region_config.region_id,
            location_name=region_config.location_name,
            instance_specs=instance_specs,
            security_group_id=region_config.security_group_id,
            subnet_id=region_config.subnet_id,
            image_id=image_id,
        )
    
    def _deploy_region(self, plan: DeploymentPlan) -> List[InstanceInfo]:
        """
        Deploy instances in a single region according to the plan.
        
        Args:
            plan: Deployment plan
            
        Returns:
            List of InstanceInfo for launched instances
        """
        cloud = self._get_provider(plan.provider, plan.region_id)
        instances: List[InstanceInfo] = []
        
        # Ensure security group
        security_group_id = plan.security_group_id
        if not security_group_id:
            security_group_id = self._ensure_security_group(
                plan.provider, 
                plan.region_id,
            )
        
        # Deploy each instance spec
        for instance_type, count, nodes_per_instance in plan.instance_specs:
            if count == 0:
                continue
            
            logger.info(
                f"Launching {count} x {instance_type} "
                f"({nodes_per_instance} nodes each) in {plan.location_name}"
            )
            
            name_prefix = build_instance_name_prefix(
                deployment_id=str(self.config.deployment_id),
                region_id=str(plan.region_id),
                user_prefix=str(self.config.instance_name_prefix),
            )
            
            try:
                launched = cloud.launch_instances(
                    image_id=plan.image_id,
                    instance_type=instance_type,
                    count=count,
                    name_prefix=name_prefix,
                    security_group_id=security_group_id,
                    subnet_id=plan.subnet_id,
                    key_name=self.config.ssh_key_name,
                    tags={
                        "DeploymentId": str(self.config.deployment_id),
                        "LocationName": str(plan.location_name),
                        "NodesCount": str(nodes_per_instance),
                    },
                )
                
                # Update with additional info
                for instance in launched:
                    instance.location_name = plan.location_name
                    instance.nodes_count = nodes_per_instance
                    
                    # Save to state immediately
                    self.state_manager.add_instance(instance)
                
                instances.extend(launched)
                
            except Exception as e:
                logger.error(f"Failed to launch instances in {plan.region_id}: {e}")
                self.state_manager.add_error(f"Launch failed in {plan.region_id}: {e}")
                raise
        
        return instances
    
    def deploy_all(self) -> List[InstanceInfo]:
        """
        Deploy instances to all configured regions.
        
        Returns:
            List of all InstanceInfo
        """
        all_instances: List[InstanceInfo] = []
        
        # Update phase
        self.state_manager.update_phase("deploying_instances")
        
        # Create deployment plans
        plans: List[DeploymentPlan] = []
        for region_config in self.config.regions:
            provider_key = region_config.provider.value
            region_id = region_config.region_id
            
            # Get image ID
            if provider_key not in self.images or region_id not in self.images[provider_key]:
                raise RuntimeError(f"No image found for {provider_key}/{region_id}")
            
            image_id = self.images[provider_key][region_id]
            
            plan = self._create_deployment_plan(region_config, image_id)
            plans.append(plan)
            
            logger.info(
                f"Plan for {plan.location_name}: "
                f"{plan.total_instances} instances, {plan.total_nodes} nodes"
            )
        
        # Deploy to all regions (can be parallelized)
        for plan in plans:
            try:
                instances = self._deploy_region(plan)
                all_instances.extend(instances)
            except Exception as e:
                logger.error(f"Failed to deploy to {plan.location_name}: {e}")
                # Continue with other regions but record error
                self.state_manager.add_error(str(e))
        
        logger.info(f"Launched {len(all_instances)} instances total")
        
        # Wait for all instances to be running
        if all_instances:
            all_instances = self._wait_for_all_running(all_instances)
        
        # Update phase
        self.state_manager.update_phase("instances_launched")
        
        return all_instances
    
    def _wait_for_all_running(
        self, 
        instances: List[InstanceInfo],
        timeout_seconds: int = 300,
    ) -> List[InstanceInfo]:
        """
        Wait for all instances to be running with public IPs.
        
        Args:
            instances: List of instances to wait for
            timeout_seconds: Timeout in seconds
            
        Returns:
            Updated list of InstanceInfo
        """
        logger.info("Waiting for all instances to be running...")
        
        # Group by provider/region
        by_region: Dict[str, List[InstanceInfo]] = {}
        for instance in instances:
            region_key = self._get_region_key(instance.provider, instance.region_id)
            if region_key not in by_region:
                by_region[region_key] = []
            by_region[region_key].append(instance)
        
        # Wait for each region
        updated_instances: List[InstanceInfo] = []
        
        for region_key, region_instances in by_region.items():
            provider = region_instances[0].provider
            region_id = region_instances[0].region_id
            instance_ids = [i.instance_id for i in region_instances]
            
            cloud = self._get_provider(provider, region_id)
            
            try:
                statuses = cloud.wait_for_instances_running(
                    instance_ids, 
                    timeout_seconds=timeout_seconds,
                )
                
                for instance in region_instances:
                    if instance.instance_id in statuses:
                        updated = statuses[instance.instance_id]
                        # Preserve our metadata
                        updated.location_name = instance.location_name
                        updated.nodes_count = instance.nodes_count
                        updated.name = instance.name
                        updated_instances.append(updated)
                        
                        # Update state
                        self.state_manager.update_instance(
                            instance.instance_id,
                            state=updated.state,
                            public_ip=updated.public_ip,
                            private_ip=updated.private_ip,
                        )
                        
            except TimeoutError as e:
                logger.error(f"Timeout waiting for instances in {region_key}: {e}")
                self.state_manager.add_error(str(e))
                # Add instances as-is
                updated_instances.extend(region_instances)
        
        logger.info(f"All {len(updated_instances)} instances are running")
        return updated_instances
    
    def get_all_instances(self) -> List[InstanceInfo]:
        """
        Get all instances from state.
        
        Returns:
            List of InstanceInfo
        """
        state = self.state_manager.state
        if state:
            return state.instances.copy()
        return []
    
    def refresh_instance_status(self) -> List[InstanceInfo]:
        """
        Refresh status of all instances from cloud providers.
        
        Returns:
            Updated list of InstanceInfo
        """
        instances = self.get_all_instances()
        
        if not instances:
            return []
        
        # Group by provider/region
        by_region: Dict[str, List[InstanceInfo]] = {}
        for instance in instances:
            region_key = self._get_region_key(instance.provider, instance.region_id)
            if region_key not in by_region:
                by_region[region_key] = []
            by_region[region_key].append(instance)
        
        # Refresh each region
        updated_instances: List[InstanceInfo] = []
        
        for region_key, region_instances in by_region.items():
            provider = region_instances[0].provider
            region_id = region_instances[0].region_id
            instance_ids = [i.instance_id for i in region_instances]
            
            cloud = self._get_provider(provider, region_id)
            
            try:
                statuses = cloud.get_instance_status(instance_ids)
                
                for instance in region_instances:
                    if instance.instance_id in statuses:
                        updated = statuses[instance.instance_id]
                        # Preserve our metadata
                        updated.location_name = instance.location_name
                        updated.nodes_count = instance.nodes_count
                        updated.name = instance.name
                        updated_instances.append(updated)
                        
                        # Update state
                        self.state_manager.update_instance(
                            instance.instance_id,
                            state=updated.state,
                            public_ip=updated.public_ip,
                            private_ip=updated.private_ip,
                        )
                    else:
                        # Instance may have been terminated
                        updated_instances.append(instance)
                        
            except Exception as e:
                logger.error(f"Failed to refresh instances in {region_key}: {e}")
                updated_instances.extend(region_instances)
        
        return updated_instances
    
    def recover_from_state(self) -> List[InstanceInfo]:
        """
        Recover deployment from saved state.
        
        This is useful when the deployment process was interrupted.
        
        Returns:
            List of recovered InstanceInfo
        """
        state = self.state_manager.state
        if not state:
            return []
        
        logger.info(f"Recovering from state: {state.deployment_id}, phase: {state.phase}")
        
        if not state.instances:
            logger.info("No instances in state to recover")
            return []
        
        # Refresh status
        instances = self.refresh_instance_status()
        
        # Filter out terminated instances
        active = [i for i in instances if i.state != InstanceState.TERMINATED]
        
        logger.info(f"Recovered {len(active)} active instances")
        return active
    
    def find_existing_instances(self) -> List[InstanceInfo]:
        """
        Find existing instances created by this deployment.
        
        Searches for instances by deployment tag.
        
        Returns:
            List of found InstanceInfo
        """
        all_instances: List[InstanceInfo] = []
        
        # Search in each configured region
        for region_config in self.config.regions:
            cloud = self._get_provider(region_config.provider, region_config.region_id)
            
            try:
                deployment_id_str = str(self.config.deployment_id) if self.config.deployment_id is not None else ""
                instances = cloud.list_instances_by_tag(
                    "DeploymentId",
                    deployment_id_str,
                )
                all_instances.extend(instances)
            except Exception as e:
                logger.warning(
                    f"Failed to search instances in "
                    f"{region_config.provider.value}/{region_config.region_id}: {e}"
                )
        
        return all_instances
