"""
Image Management Module

Handles creation and management of server images for Conflux nodes.
Responsible for:
- Creating base images with Docker and Conflux image pre-loaded
- Finding existing images
- Managing image lifecycle
"""

import os
import shlex
import time
from typing import Dict, Optional, List
from datetime import datetime

from loguru import logger

from ..configs import (
    DeploymentConfig,
    CloudProvider,
    InstanceInfo,
    InstanceState,
)
from ..cloud import (
    CloudProviderBase,
    ImageInfo,
    get_cloud_factory,
    get_default_security_rules,
)
from ..configs.loader import StateManager
from ..utils.remote import RemoteExecutor


def generate_user_data_script(
    conflux_docker_image: str,
    additional_packages: List[str],
) -> str:
    """
    Generate the user data script for initializing a new instance.
    
    This script:
    1. Updates system packages
    2. Installs Docker and other required packages
    3. Pulls the Conflux Docker image
    4. Creates necessary directories and scripts
    
    Args:
        conflux_docker_image: Docker image to pull
        additional_packages: Additional packages to install
        
    Returns:
        Shell script as string
    """
    packages_str = " ".join(additional_packages)
    
    script = f'''#!/bin/bash
set -euo pipefail

# If anything fails, mark it so the image builder can detect the failure.
trap 'echo "=== ERROR during Conflux node image setup ==="; touch /var/lib/conflux-setup-failed' ERR

# Log all output
exec > >(tee /var/log/user-data.log|logger -t user-data -s 2>/dev/console) 2>&1

echo "=== Starting Conflux node image setup ==="
echo "Timestamp: $(date)"

# Wait for cloud-init to finish
cloud-init status --wait || true

# Update system (with basic retries)
echo "=== Updating system packages ==="
export DEBIAN_FRONTEND=noninteractive
for i in 1 2 3; do apt-get update -y && break || sleep 10; done
for i in 1 2 3; do apt-get upgrade -y && break || sleep 10; done

# Install required packages
echo "=== Installing required packages ==="
for i in 1 2 3; do apt-get install -y {packages_str} && break || sleep 10; done

# Start Docker
echo "=== Starting Docker service ==="
systemctl enable docker
systemctl start docker

# Wait for Docker to be ready
sleep 5

# Pull Conflux Docker image (retry for flaky networks)
echo "=== Pulling Conflux Docker image: {conflux_docker_image} ==="
for i in 1 2 3 4 5; do docker pull {conflux_docker_image} && break || sleep 15; done

# Create Conflux data directory
echo "=== Creating directories ==="
mkdir -p /data/conflux
mkdir -p /data/conflux/config
mkdir -p /data/conflux/logs
mkdir -p /data/conflux/data

# Create helper scripts
echo "=== Creating helper scripts ==="

# Script to start Conflux node
cat > /usr/local/bin/start_conflux.sh << 'SCRIPT_EOF'
#!/bin/bash
NODE_INDEX=${{1:-0}}
CONFIG_DIR=${{2:-/data/conflux/config}}
DATA_DIR=/data/conflux/data/node_$NODE_INDEX
LOG_DIR=/data/conflux/logs/node_$NODE_INDEX

mkdir -p $DATA_DIR $LOG_DIR

# Calculate ports
P2P_PORT=$((32323 + NODE_INDEX * 10))
RPC_PORT=$((12537 + NODE_INDEX * 10))
WS_PORT=$((12538 + NODE_INDEX * 10))

docker run -d \\
    --name conflux_node_$NODE_INDEX \\
    --restart unless-stopped \\
    --network host \\
    -v $CONFIG_DIR:/config:ro \\
    -v $DATA_DIR:/data \\
    -v $LOG_DIR:/logs \\
    {conflux_docker_image} \\
    --config /config/conflux_$NODE_INDEX.toml
SCRIPT_EOF
chmod +x /usr/local/bin/start_conflux.sh

# Script to stop Conflux node
cat > /usr/local/bin/stop_conflux.sh << 'SCRIPT_EOF'
#!/bin/bash
NODE_INDEX=${{1:-0}}
docker stop conflux_node_$NODE_INDEX 2>/dev/null || true
docker rm conflux_node_$NODE_INDEX 2>/dev/null || true
SCRIPT_EOF
chmod +x /usr/local/bin/stop_conflux.sh

# Script to stop all Conflux nodes
cat > /usr/local/bin/stop_all_conflux.sh << 'SCRIPT_EOF'
#!/bin/bash
docker ps -a --filter "name=conflux_node_" -q | xargs -r docker stop
docker ps -a --filter "name=conflux_node_" -q | xargs -r docker rm
SCRIPT_EOF
chmod +x /usr/local/bin/stop_all_conflux.sh

# Script to view logs
cat > /usr/local/bin/conflux_logs.sh << 'SCRIPT_EOF'
#!/bin/bash
NODE_INDEX=${{1:-0}}
docker logs -f conflux_node_$NODE_INDEX
SCRIPT_EOF
chmod +x /usr/local/bin/conflux_logs.sh

# Clean up
echo "=== Cleaning up ==="
apt-get clean
rm -rf /var/lib/apt/lists/*

# Create marker file to indicate setup is complete
touch /var/lib/conflux-setup-complete

echo "=== Conflux node image setup complete ==="
echo "Timestamp: $(date)"
'''
    
    return script


class ImageManager:
    """
    Manages server images for Conflux deployment.
    
    Responsibilities:
    - Check if required images exist in each region
    - Create new images if needed
    - Track created images in state
    """
    
    def __init__(self, config: DeploymentConfig, state_manager: StateManager):
        self.config = config
        self.state_manager = state_manager
        self.factory = get_cloud_factory()
        self._temp_instances: Dict[str, List[str]] = {}  # region -> instance_ids
        self._temp_security_groups: Dict[str, str] = {}  # region -> sg_id

    def _resolve_ssh_key_path(self) -> Optional[str]:
        key_path = self.config.ssh_private_key_path
        if not key_path:
            return None
        return os.path.expanduser(str(key_path))

    def _pick_reachable_host(self, instance: "InstanceInfo") -> Optional[str]:
        # Prefer public IP; fall back to private only if caller can route.
        return instance.public_ip or instance.private_ip

    def _wait_for_image_builder_ready(
        self,
        host: str,
        conflux_docker_image: str,
        timeout_seconds: int = 1800,
    ) -> None:
        """Wait until user-data has completed and docker image is present.

        This makes image creation deterministic instead of relying on fixed sleeps.
        """

        ssh_key_path = self._resolve_ssh_key_path()
        if not ssh_key_path:
            logger.warning(
                "ssh_private_key_path is not set; falling back to fixed sleeps and cannot verify image build completion"
            )
            time.sleep(420)
            return

        def _try_with_user(user: str) -> Optional[RemoteExecutor]:
            executor = RemoteExecutor(
                ssh_key_path=ssh_key_path,
                ssh_user=user,
                known_hosts=None,
                connect_timeout=15.0,
                keepalive_interval=30.0,
            )
            probe = executor.execute_on_host(host, "true", retry=1, timeout=20)
            return executor if probe.success else None

        executor = _try_with_user("ubuntu") or _try_with_user("root")
        if executor is None:
            raise RuntimeError(
                f"Unable to SSH into image builder instance at {host}. "
                "Check public IP routing, security group ingress (tcp/22), and ssh_private_key_path."
            )

        image_q = shlex.quote(conflux_docker_image)

        start = time.time()
        last_tail = ""
        last_status = ""
        poll_interval = 10
        while time.time() - start < timeout_seconds:
            # If setup explicitly failed, surface the logs.
            failed = executor.execute_on_host(host, "sudo test -f /var/lib/conflux-setup-failed", retry=0, timeout=20)
            if failed.success:
                tail = executor.execute_on_host(host, "sudo tail -n 200 /var/log/user-data.log || true", retry=0, timeout=30)
                raise RuntimeError(
                    "Image builder user-data reported failure. "
                    f"Last logs:\n{tail.stdout.strip()}\n{tail.stderr.strip()}"
                )

            # Best-effort cloud-init status (some images may not have it).
            ci = executor.execute_on_host(host, "sudo cloud-init status --long || true", retry=0, timeout=30)
            if ci.stdout:
                last_status = ci.stdout.strip()

            complete = executor.execute_on_host(host, "sudo test -f /var/lib/conflux-setup-complete", retry=0, timeout=20)
            if complete.success:
                # Ensure docker is running and image is present.
                docker_ok = executor.execute_on_host(
                    host,
                    "sudo systemctl is-active --quiet docker && sudo docker info >/dev/null 2>&1",
                    retry=1,
                    timeout=60,
                )
                image_ok = executor.execute_on_host(
                    host,
                    f"sudo docker image inspect {image_q} >/dev/null 2>&1",
                    retry=1,
                    timeout=60,
                )
                script_ok = executor.execute_on_host(
                    host,
                    "sudo test -x /usr/local/bin/start_conflux.sh",
                    retry=0,
                    timeout=20,
                )

                if docker_ok.success and image_ok.success and script_ok.success:
                    logger.info("Image builder setup complete (marker + docker image verified)")
                    return

            # Periodically capture tail for debugging.
            if int(time.time() - start) % 60 < poll_interval:
                tail = executor.execute_on_host(host, "sudo tail -n 50 /var/log/user-data.log || true", retry=0, timeout=30)
                if tail.stdout:
                    last_tail = tail.stdout.strip()

            time.sleep(poll_interval)

        msg = f"Timeout waiting for image builder setup after {timeout_seconds}s."
        if last_status:
            msg += f"\ncloud-init status:\n{last_status}"
        if last_tail:
            msg += f"\nLast /var/log/user-data.log tail:\n{last_tail}"
        raise TimeoutError(msg)
    
    def _get_image_name(self, provider: CloudProvider, region_id: str) -> str:
        """Generate the image name for a provider/region combination"""
        return f"{self.config.image.image_name_prefix}-{provider.value}-{region_id}"
    
    def _get_provider(self, provider: CloudProvider, region_id: str) -> CloudProviderBase:
        """Get a cloud provider instance"""
        return self.factory.get_provider(
            provider,
            self.config.credentials[provider],
            region_id,
        )
    
    def find_existing_image(
        self, 
        provider: CloudProvider, 
        region_id: str
    ) -> Optional[ImageInfo]:
        """
        Find an existing image in the specified region.
        
        First checks the config for pre-specified image IDs,
        then searches for images by name pattern.
        
        Args:
            provider: Cloud provider
            region_id: Region ID
            
        Returns:
            ImageInfo if found, None otherwise
        """
        # Check if image ID is specified in config
        existing = self.config.image.existing_images
        provider_key = provider.value
        
        if provider_key in existing and region_id in existing[provider_key]:
            image_id = existing[provider_key][region_id]
            logger.info(f"Using pre-specified image {image_id} for {provider.value}/{region_id}")
            return ImageInfo(
                image_id=image_id,
                name=f"pre-specified-{image_id}",
                provider=provider,
                region_id=region_id,
                state="available",
            )
        
        # Check if we have this image in state
        state = self.state_manager.state
        if state and provider_key in state.images and region_id in state.images[provider_key]:
            image_id = state.images[provider_key][region_id]
            logger.info(f"Found image {image_id} in state for {provider.value}/{region_id}")
            return ImageInfo(
                image_id=image_id,
                name=f"state-{image_id}",
                provider=provider,
                region_id=region_id,
                state="available",
            )
        
        # Search for existing image by name
        cloud = self._get_provider(provider, region_id)
        image_name = self._get_image_name(provider, region_id)
        
        # Try exact name first
        image = cloud.find_image_by_name(image_name)
        if image:
            logger.info(f"Found existing image {image.image_id} for {provider.value}/{region_id}")
            # Save to state
            self.state_manager.add_image(provider_key, region_id, image.image_id)
            return image
        
        # Try with wildcard
        image = cloud.find_image_by_name(f"{self.config.image.image_name_prefix}*")
        if image:
            logger.info(f"Found existing image {image.image_id} (wildcard) for {provider.value}/{region_id}")
            self.state_manager.add_image(provider_key, region_id, image.image_id)
            return image
        
        return None
    
    def create_image(
        self, 
        provider: CloudProvider, 
        region_id: str,
        wait_for_available: bool = True,
        image_name: Optional[str] = None,
    ) -> ImageInfo:
        """
        Create a new image in the specified region.
        
        This process:
        1. Launches a temporary instance
        2. Waits for setup to complete
        3. Creates an image from the instance
        4. Terminates the temporary instance
        
        Args:
            provider: Cloud provider
            region_id: Region ID
            wait_for_available: Whether to wait for image to be available
            
        Returns:
            ImageInfo for the created image
        """
        cloud = self._get_provider(provider, region_id)
        image_name = image_name or self._get_image_name(provider, region_id)
        region_key = f"{provider.value}:{region_id}"
        
        logger.info(f"Creating new image {image_name} in {provider.value}/{region_id}")
        
        try:
            # Create security group for the temp instance
            sg_name = f"conflux-image-builder-{self.config.deployment_id}"
            existing_sg = cloud.find_security_group_by_name(sg_name)
            
            if existing_sg:
                security_group_id = existing_sg
            else:
                security_group_id = cloud.create_security_group(
                    name=sg_name,
                    description="Temporary security group for image building",
                    rules=get_default_security_rules(),
                )
            self._temp_security_groups[region_key] = security_group_id
            
            # Get base Ubuntu image
            base_image_id = self.config.image.base_image_id
            if not base_image_id:
                base_image_id = cloud.get_base_ubuntu_image(self.config.image.ubuntu_version)
            
            logger.info(f"Using base image: {base_image_id}")
            
            # Generate user data script
            user_data = generate_user_data_script(
                conflux_docker_image=self.config.image.conflux_docker_image,
                additional_packages=self.config.image.additional_packages,
            )
            
            # Launch temporary instance
            # Use a small instance type for image building
            instance_type = "t3.medium" if provider == CloudProvider.AWS else "ecs.t5-lc1m2.small"
            
            instances = cloud.launch_instances(
                image_id=base_image_id,
                instance_type=instance_type,
                count=1,
                name_prefix=f"conflux-image-builder-{self.config.deployment_id}",
                security_group_id=security_group_id,
                key_name=self.config.ssh_key_name,
                user_data=user_data,
                tags={"Purpose": "image-building"},
            )
            
            if not instances:
                raise RuntimeError("Failed to launch temporary instance")
            
            temp_instance = instances[0]
            instance_id = temp_instance.instance_id
            
            # Track for cleanup
            if region_key not in self._temp_instances:
                self._temp_instances[region_key] = []
            self._temp_instances[region_key].append(instance_id)
            
            logger.info(f"Launched temporary instance {instance_id}")
            
            # Wait for instance to be running
            logger.info("Waiting for instance to be running...")
            statuses = cloud.wait_for_instances_running([instance_id], timeout_seconds=600)
            if instance_id in statuses:
                temp_instance = statuses[instance_id]

            host = self._pick_reachable_host(temp_instance)
            if host:
                logger.info(
                    "Waiting for image builder setup to complete (cloud-init + marker + docker image; this may take 5-20 minutes)..."
                )
                self._wait_for_image_builder_ready(
                    host=host,
                    conflux_docker_image=self.config.image.conflux_docker_image,
                    timeout_seconds=1800,
                )
            else:
                logger.warning(
                    "Image builder instance has no IP (public/private). Falling back to fixed sleep and cannot verify build completion."
                )
                time.sleep(420)
            
            # Create image
            logger.info(f"Creating image {image_name}...")
            image = cloud.create_image(
                instance_id=instance_id,
                image_name=image_name,
                description=f"Conflux test node image created by {self.config.deployment_id}",
                wait_for_available=wait_for_available,
            )
            
            logger.info(f"Created image {image.image_id}")
            
            # Save to state
            self.state_manager.add_image(provider.value, region_id, image.image_id)
            
            return image
            
        finally:
            # Clean up temporary resources
            self._cleanup_temp_resources(provider, region_id)
    
    def _cleanup_temp_resources(self, provider: CloudProvider, region_id: str) -> None:
        """Clean up temporary instances and security groups"""
        region_key = f"{provider.value}:{region_id}"
        cloud = self._get_provider(provider, region_id)
        
        # Terminate temp instances
        if region_key in self._temp_instances:
            instance_ids = self._temp_instances[region_key]
            if instance_ids:
                logger.info(f"Terminating temporary instances: {instance_ids}")
                try:
                    cloud.terminate_instances(instance_ids)
                except Exception as e:
                    logger.warning(f"Failed to terminate temp instances: {e}")
            del self._temp_instances[region_key]
        
        # Delete temp security group (wait for instances to terminate first)
        if region_key in self._temp_security_groups:
            sg_id = self._temp_security_groups[region_key]
            time.sleep(30)  # Wait for instances to fully terminate
            logger.info(f"Deleting temporary security group: {sg_id}")
            try:
                cloud.delete_security_group(sg_id)
            except Exception as e:
                logger.warning(f"Failed to delete temp security group: {e}")
            del self._temp_security_groups[region_key]
    
    def ensure_images_exist(self, force_recreate: bool = False) -> Dict[str, Dict[str, str]]:
        """
        Ensure images exist in all required regions.
        
        For each region in the deployment config:
        1. Check if an image already exists
        2. If not, create one
        
        Returns:
            Dict mapping provider -> region -> image_id
        """
        images: Dict[str, Dict[str, str]] = {}
        
        # Collect unique provider/region combinations
        regions_needed: Dict[CloudProvider, List[str]] = {}
        for region_config in self.config.regions:
            provider = region_config.provider
            region_id = region_config.region_id
            
            if provider not in regions_needed:
                regions_needed[provider] = []
            
            if region_id not in regions_needed[provider]:
                regions_needed[provider].append(region_id)
        
        # Process each provider/region
        for provider, region_ids in regions_needed.items():
            provider_key = provider.value
            if provider_key not in images:
                images[provider_key] = {}
            
            for region_id in region_ids:
                logger.info(f"Checking image for {provider.value}/{region_id}")

                if not force_recreate:
                    # Try to find existing image
                    image = self.find_existing_image(provider, region_id)
                    if image:
                        images[provider_key][region_id] = image.image_id
                        logger.info(f"Using existing image: {image.image_id}")
                        continue

                # Create new image (use unique name to avoid cloud-side name collisions)
                logger.info(f"Creating new image for {provider.value}/{region_id}")
                unique_name = f"{self._get_image_name(provider, region_id)}-{datetime.now().strftime('%Y%m%d-%H%M%S')}"
                image = self.create_image(provider, region_id, image_name=unique_name)
                images[provider_key][region_id] = image.image_id
        
        return images
    
    def delete_image(self, provider: CloudProvider, region_id: str, image_id: str) -> bool:
        """
        Delete an image.
        
        Args:
            provider: Cloud provider
            region_id: Region ID
            image_id: Image ID to delete
            
        Returns:
            True if deleted successfully
        """
        cloud = self._get_provider(provider, region_id)
        logger.info(f"Deleting image {image_id} in {provider.value}/{region_id}")
        return cloud.delete_image(image_id)
    
    def delete_all_images(self) -> Dict[str, bool]:
        """
        Delete all images created by this deployment.
        
        Returns:
            Dict mapping image_id to deletion success
        """
        results = {}
        
        state = self.state_manager.state
        if not state or not state.images:
            return results
        
        for provider_key, regions in state.images.items():
            provider = CloudProvider(provider_key)
            for region_id, image_id in regions.items():
                try:
                    success = self.delete_image(provider, region_id, image_id)
                    results[image_id] = success
                except Exception as e:
                    logger.error(f"Failed to delete image {image_id}: {e}")
                    results[image_id] = False
        
        return results
