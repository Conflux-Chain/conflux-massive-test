"""
Main Orchestrator Module

This module ties together all the components to provide a unified
interface for deploying and testing Conflux nodes across multiple
cloud providers.
"""

import sys
import signal
import atexit
from typing import Dict, List, Optional, Any
from datetime import datetime
from pathlib import Path
from contextlib import contextmanager

from loguru import logger

from .configs import (
    DeploymentConfig,
    InstanceInfo,
    NodeInfo,
)
from .configs.loader import ConfigLoader, StateManager
from .image_management import ImageManager
from .server_deployment import ServerDeployer
from .node_management import NodeManager
from .test_control import TestController, TestResult
from .resource_cleanup import ResourceCleanupManager


class ConfluxDeployer:
    """
    Main orchestrator for Conflux node deployment and testing.
    
    This class provides a unified interface for:
    - Creating cloud images with Docker and Conflux pre-installed
    - Deploying servers across multiple cloud providers and regions
    - Starting and managing Conflux nodes
    - Running various tests (stress, latency, fork)
    - Cleaning up all resources
    
    It handles automatic cleanup on errors and interrupts to prevent
    resource leakage.
    """
    
    def __init__(
        self,
        config: DeploymentConfig,
        state_path: Optional[Path] = None,
    ):
        """
        Initialize the deployer.
        
        Args:
            config: Deployment configuration
            state_path: Path to save/load state (defaults to ./state/<deployment_id>.json)
        """
        # Ensure deployment_id is always set (ConfigLoader typically provides it,
        # but the type allows None).
        if not config.deployment_id:
            config.deployment_id = f"deploy-{datetime.now().strftime('%Y%m%d-%H%M%S')}"
        self.config = config
        
        # Set up state path
        if state_path is None:
            state_path = Path("./state") / f"{self.config.deployment_id}.json"
        
        self.state_path = state_path
        self.state_manager = StateManager(state_path)
        
        # Initialize managers (lazily)
        self._image_manager: Optional[ImageManager] = None
        # Allow test fakes to be injected.
        self._server_deployer: Optional[Any] = None
        self._node_manager: Optional[NodeManager] = None
        self._test_controller: Optional[TestController] = None
        self._cleanup_manager: Optional[ResourceCleanupManager] = None
        
        # Auto-cleanup flag
        self._auto_cleanup_registered = False
        self._cleanup_on_error = config.cleanup.auto_cleanup
        
        # Setup logging
        self._setup_logging()
    
    def _setup_logging(self) -> None:
        """Configure logging"""
        log_level = "DEBUG" if self.config.network.debug_mode else "INFO"
        
        # Remove default handler
        logger.remove()
        
        # Add console handler
        logger.add(
            sys.stderr,
            level=log_level,
            format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
                   "<level>{level: <8}</level> | "
                   "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> | "
                   "<level>{message}</level>",
        )
        
        # Add file handler
        log_path = Path("./logs") / f"{self.config.deployment_id}.log"
        log_path.parent.mkdir(parents=True, exist_ok=True)
        logger.add(log_path, level="DEBUG", rotation="10 MB")
    
    @property
    def image_manager(self) -> ImageManager:
        """Get or create image manager"""
        if self._image_manager is None:
            self._image_manager = ImageManager(self.config, self.state_manager)
        return self._image_manager
    
    @property
    def server_deployer(self) -> ServerDeployer:
        """Get or create server deployer"""
        if self._server_deployer is None:
            images = self.state_manager.state.images if self.state_manager.state else {}
            self._server_deployer = ServerDeployer(self.config, self.state_manager, images=images)
        return self._server_deployer
    
    @property
    def node_manager(self) -> NodeManager:
        """Get or create node manager"""
        if self._node_manager is None:
            instances = self.state_manager.state.instances if self.state_manager.state else []
            self._node_manager = NodeManager(self.config, instances, ssh_key_path=self.config.ssh_private_key_path)
        return self._node_manager
    
    @property
    def test_controller(self) -> TestController:
        """Get or create test controller"""
        if self._test_controller is None:
            self._test_controller = TestController(self.config, self.node_manager)
        return self._test_controller
    
    @property
    def cleanup_manager(self) -> ResourceCleanupManager:
        """Get or create cleanup manager"""
        if self._cleanup_manager is None:
            self._cleanup_manager = ResourceCleanupManager(self.config, self.state_manager)
        return self._cleanup_manager
    
    def _register_cleanup(self) -> None:
        """Register automatic cleanup handlers"""
        if self._auto_cleanup_registered:
            return
        
        def cleanup_handler(signum=None, frame=None):
            logger.warning("Received interrupt signal, cleaning up...")
            try:
                self.cleanup(force=True)
            except Exception as e:
                logger.error(f"Cleanup failed: {e}")
            sys.exit(1)
        
        # Register signal handlers
        signal.signal(signal.SIGINT, cleanup_handler)
        signal.signal(signal.SIGTERM, cleanup_handler)
        
        # Register atexit handler
        atexit.register(self._atexit_cleanup)
        
        self._auto_cleanup_registered = True
    
    def _atexit_cleanup(self) -> None:
        """Cleanup on exit"""
        if self._cleanup_on_error:
            try:
                state = self.state_manager.state
                if state and state.phase not in ["completed", "cleanup"]:
                    logger.warning("Deployment incomplete, cleaning up...")
                    self.cleanup(force=True)
            except Exception as e:
                logger.error(f"Atexit cleanup failed: {e}")
    
    @contextmanager
    def auto_cleanup(self):
        """
        Context manager for automatic cleanup.
        
        Usage:
            with deployer.auto_cleanup():
                deployer.deploy_all()
                deployer.run_tests()
        """
        self._register_cleanup()
        try:
            yield self
        except Exception as e:
            logger.error(f"Error during deployment: {e}")
            if self._cleanup_on_error:
                self.cleanup(force=True)
            raise
    
    # === Image Management ===
    
    def create_images(
        self,
        force_recreate: bool = False,
    ) -> Dict[str, Dict[str, str]]:
        """
        Create server images in all configured regions.
        
        Args:
            force_recreate: If True, recreate images even if they exist
            
        Returns:
            Dict mapping provider -> region -> image_id
        """
        logger.info("Creating server images...")
        deployment_id = self.config.deployment_id
        if not deployment_id:
            raise ValueError("deployment_id is required")
        self.state_manager.initialize(str(deployment_id))
        self.state_manager.update_phase("creating_images")
        
        images = self.image_manager.ensure_images_exist(force_recreate=force_recreate)
        
        # Update state
        for provider_key, regions in images.items():
            for region_id, image_id in regions.items():
                self.state_manager.add_image(provider_key, region_id, image_id)
        
        return images
    
    def find_existing_images(self) -> Dict[str, Dict[str, str]]:
        """
        Find existing images that match the configuration.
        
        Returns:
            Dict mapping provider -> region -> image_id
        """
        images: Dict[str, Dict[str, str]] = {}
        seen: set[tuple[str, str]] = set()

        for region in self.config.regions:
            provider_key = region.provider.value
            region_id = region.region_id
            if (provider_key, region_id) in seen:
                continue
            seen.add((provider_key, region_id))

            found = self.image_manager.find_existing_image(region.provider, region_id)
            if not found:
                continue
            images.setdefault(provider_key, {})[region_id] = found.image_id

        return images
    
    def delete_images(self) -> Dict[str, bool]:
        """
        Delete all images created by this deployment.
        
        Returns:
            Dict mapping image_id to deletion success
        """
        return self.cleanup_manager.delete_images()
    
    # === Server Deployment ===
    
    def deploy_servers(self) -> List[InstanceInfo]:
        """
        Deploy servers in all configured regions.
        
        Returns:
            List of deployed instance info
        """
        logger.info("Deploying servers...")
        self.state_manager.update_phase("deploying_servers")
        
        # Ensure deployer sees latest images from state (create_images writes them).
        if self.state_manager.state:
            self.server_deployer.images = self.state_manager.state.images

        instances = self.server_deployer.deploy_all()
        
        return instances
    
    def recover_deployment(self) -> List[InstanceInfo]:
        """
        Recover from a previous deployment.
        
        Returns:
            List of recovered instances
        """
        logger.info("Recovering deployment...")
        return self.server_deployer.recover_from_state()
    
    # === Node Management ===
    
    def initialize_nodes(
        self,
        instances: Optional[List[InstanceInfo]] = None,
    ) -> List[NodeInfo]:
        """
        Initialize Conflux nodes on deployed instances.
        
        Args:
            instances: List of instances to initialize (defaults to state)
            
        Returns:
            List of node info
        """
        logger.info("Initializing Conflux nodes...")
        
        if instances:
            # Update node manager with new instances
            self._node_manager = NodeManager(self.config, instances, ssh_key_path=self.config.ssh_private_key_path)
        
        self.state_manager.update_phase("initializing_nodes")
        
        return self.node_manager.initialize_nodes()
    
    def start_nodes(self) -> Dict[str, bool]:
        """
        Start all Conflux nodes.
        
        Returns:
            Dict mapping node_id to start success
        """
        logger.info("Starting Conflux nodes...")
        self.state_manager.update_phase("starting_nodes")
        
        return self.node_manager.start_nodes()
    
    def stop_nodes(self) -> Dict[str, bool]:
        """
        Stop all Conflux nodes.
        
        Returns:
            Dict mapping node_id to stop success
        """
        logger.info("Stopping Conflux nodes...")
        return self.node_manager.stop_nodes()
    
    def wait_for_nodes_ready(
        self,
        timeout: int = 300,
    ) -> bool:
        """
        Wait for all nodes to be ready.
        
        Args:
            timeout: Timeout in seconds
            
        Returns:
            True if all nodes are ready
        """
        logger.info("Waiting for nodes to be ready...")
        results = self.node_manager.wait_for_nodes_ready(timeout_seconds=timeout)
        return all(results.values()) if results else True
    
    def connect_nodes(self) -> bool:
        """
        Connect all nodes in a mesh network.
        
        Returns:
            Dict mapping node_id to peer count
        """
        logger.info("Connecting nodes...")
        return bool(self.node_manager.connect_nodes(connect_count=self.config.network.connect_peers))
    
    def collect_metrics(self) -> Dict[str, Any]:
        """
        Collect metrics from all nodes.
        
        Returns:
            Dict with aggregated metrics
        """
        return self.node_manager.collect_metrics()
    
    # === Test Execution ===
    
    def run_stress_test(self, duration_seconds: int = 300) -> TestResult:
        """
        Run stress test.
        
        Args:
            duration_seconds: Test duration
            
        Returns:
            Test result
        """
        logger.info(f"Running stress test for {duration_seconds} seconds...")
        self.state_manager.update_phase("running_tests")
        
        return self.test_controller.run_stress_test(duration_seconds)
    
    def run_latency_test(
        self,
        sample_count: int = 100,
    ) -> TestResult:
        """
        Run latency test.
        
        Args:
            sample_count: Number of samples to collect
            
        Returns:
            Test result
        """
        logger.info(f"Running latency test with {sample_count} samples...")
        self.state_manager.update_phase("running_tests")
        
        return self.test_controller.run_latency_test(sample_count)
    
    def run_fork_test(
        self,
        target_depth: int = 10,
    ) -> TestResult:
        """
        Run fork test.
        
        Args:
            target_depth: Target fork depth
            
        Returns:
            Test result
        """
        logger.info(f"Running fork test with target depth {target_depth}...")
        self.state_manager.update_phase("running_tests")
        
        return self.test_controller.run_fork_test(target_depth)
    
    def run_custom_test(self, test_name: str) -> TestResult:
        """
        Run a custom test.
        
        Args:
            test_name: Name of the test
            
        Returns:
            Test result
        """
        logger.info(f"Running custom test: {test_name}")
        self.state_manager.update_phase("running_tests")
        
        return self.test_controller.run_test(test_name)
    
    # === Resource Cleanup ===
    
    def cleanup(
        self,
        force: bool = False,
        delete_images: Optional[bool] = None,
    ) -> Dict[str, Any]:
        """
        Clean up all resources.
        
        Args:
            force: If True, force cleanup
            delete_images: Whether to delete images
            
        Returns:
            Cleanup results
        """
        logger.info("Cleaning up resources...")
        return self.cleanup_manager.cleanup_all(
            force=force,
            delete_images=delete_images,
        )
    
    def estimate_cost(self) -> Dict[str, float]:
        """
        Estimate the running cost of current resources.
        
        Returns:
            Cost estimates
        """
        return self.cleanup_manager.estimate_running_cost()

    def get_inventory(self) -> List[Dict[str, Any]]:
        """Return an inventory of servers and node ports for external use.

        Each entry contains:
            - instance_id, provider, region_id, location_name
            - instance_type, hardware (spec dict where available)
            - public_ip, private_ip
            - nodes: list of {node_id, jsonrpc_port, p2p_port, node_index, status}
        """
        from .configs import AWS_INSTANCE_SPECS, ALIBABA_INSTANCE_SPECS

        inv: List[Dict[str, Any]] = []
        sd = self.server_deployer
        if hasattr(sd, "get_all_instances"):
            instances = sd.get_all_instances()
        elif hasattr(sd, "list_instances"):
            instances = sd.list_instances()
        else:
            instances = []

        # Ensure node list is initialized
        nodes = self.node_manager.initialize_nodes()

        for inst in instances:
            # Support both InstanceInfo and ServerInstance shapes
            provider_attr = getattr(inst, "cloud_provider", None) or getattr(inst, "provider", None)
            provider_str = provider_attr.value if not isinstance(provider_attr, str) and provider_attr is not None else provider_attr
            region = getattr(inst, "region", None) or getattr(inst, "region_id", None)

            # Find location name from config regions
            location_name = None
            for r in self.config.regions:
                if r.region_id == region and r.provider.value == provider_str:
                    location_name = r.location_name
                    break

            # Hardware/spec lookup
            hw = None
            if provider_str == "aws":
                hw = AWS_INSTANCE_SPECS.get(inst.instance_type)
            elif provider_str == "alibaba":
                hw = ALIBABA_INSTANCE_SPECS.get(inst.instance_type)

            instance_id = getattr(inst, "instance_id", None)
            if not instance_id:
                continue
            node_objs = self.node_manager.list_nodes_by_instance(str(instance_id))
            node_list = [
                {
                    "node_id": n.node_id,
                    "node_index": n.node_index,
                    "jsonrpc_port": n.jsonrpc_port,
                    "p2p_port": n.p2p_port,
                    "status": getattr(n, "is_ready", getattr(n, "status", None)),
                }
                for n in node_objs
            ]

            inv.append({
                "instance_id": inst.instance_id,
                "provider": provider_str,
                "region_id": region,
                "location_name": location_name,
                "instance_type": inst.instance_type,
                "hardware": hw,
                "public_ip": getattr(inst, "public_ip", None) or getattr(inst, "ip_address", None),
                "private_ip": getattr(inst, "private_ip", None),
                "nodes": node_list,
            })

        return inv
    
    # === Full Workflow ===
    
    def deploy_all(self) -> Dict[str, Any]:
        """
        Run the full deployment workflow:
        1. Create images (if needed)
        2. Deploy servers
        3. Initialize nodes
        4. Start nodes
        5. Wait for nodes to be ready
        6. Connect nodes
        
        Returns:
            Deployment summary
        """
        logger.info("Starting full deployment...")
        start_time = datetime.now()
        
        result = {
            "deployment_id": self.config.deployment_id,
            "started_at": start_time.isoformat(),
            "images": {},
            "instances": [],
            "nodes": [],
            "success": False,
        }
        
        try:
            # Initialize state
            deployment_id = self.config.deployment_id
            if not deployment_id:
                raise ValueError("deployment_id is required")
            self.state_manager.initialize(str(deployment_id))
            
            # 1. Create images
            result["images"] = self.create_images()
            
            # 2. Deploy servers
            instances = self.deploy_servers()
            result["instances"] = [
                {
                    "instance_id": i.instance_id,
                    "provider": i.provider.value,
                    "region_id": i.region_id,
                    "public_ip": i.public_ip,
                }
                for i in instances
            ]
            
            # 3. Initialize nodes
            nodes = self.initialize_nodes(instances)
            result["nodes"] = [
                {
                    "node_id": n.node_id,
                    "instance_id": n.instance_info.instance_id,
                    "rpc_port": n.jsonrpc_port,
                    "p2p_port": n.p2p_port,
                }
                for n in nodes
            ]
            
            # 4. Start nodes
            self.start_nodes()
            
            # 5. Wait for ready
            ready = self.wait_for_nodes_ready()
            if not ready:
                raise RuntimeError("Nodes failed to become ready in time")
            
            # 6. Connect nodes
            self.connect_nodes()
            
            result["success"] = True
            self.state_manager.update_phase("deployed")
            
        except Exception as e:
            result["error"] = str(e)
            self.state_manager.add_error(str(e))
            raise
        
        finally:
            end_time = datetime.now()
            result["completed_at"] = end_time.isoformat()
            result["duration_seconds"] = (end_time - start_time).total_seconds()
        
        logger.info(f"Deployment completed in {result['duration_seconds']:.1f} seconds")
        return result
    
    def run_all_tests(self) -> Dict[str, TestResult]:
        """
        Run all configured tests.
        
        Returns:
            Dict mapping test name to result
        """
        results = {}
        
        test_config = self.config.test

        test_type = getattr(test_config, "test_type", "stress")
        if test_type == "all":
            results["stress"] = self.run_stress_test()
            results["latency"] = self.run_latency_test()
            results["fork"] = self.run_fork_test()
        elif test_type == "stress":
            results["stress"] = self.run_stress_test()
        elif test_type == "latency":
            results["latency"] = self.run_latency_test()
        elif test_type == "fork":
            results["fork"] = self.run_fork_test()
        else:
            results[str(test_type)] = self.run_custom_test(str(test_type))
        
        return results
    
    def full_workflow(self) -> Dict[str, Any]:
        """
        Run the complete workflow:
        1. Deploy all
        2. Run tests
        3. Cleanup (if auto_cleanup is enabled)
        
        Returns:
            Complete workflow results
        """
        result = {
            "deployment": None,
            "tests": None,
            "cleanup": None,
            "success": False,
        }
        
        try:
            with self.auto_cleanup():
                # Deploy
                result["deployment"] = self.deploy_all()
                
                # Run tests
                result["tests"] = self.run_all_tests()
                
                # Check test results
                all_passed = all(
                    r.success for r in result["tests"].values()
                )
                
                result["success"] = all_passed
                
        finally:
            # Cleanup
            if self.config.cleanup.auto_cleanup:
                result["cleanup"] = self.cleanup()
        
        return result
    
    @classmethod
    def from_config_file(
        cls,
        config_path: str,
        state_path: Optional[str] = None,
    ) -> "ConfluxDeployer":
        """
        Create a deployer from a configuration file.
        
        Args:
            config_path: Path to configuration file
            state_path: Path to state file
            
        Returns:
            ConfluxDeployer instance
        """
        config = ConfigLoader.load_from_file(config_path)
        state = Path(state_path) if state_path else None
        return cls(config, state)
    
    @classmethod
    def recover(
        cls,
        state_path: str,
        config_path: Optional[str] = None,
    ) -> "ConfluxDeployer":
        """
        Recover a deployer from a state file.
        
        Args:
            state_path: Path to state file
            config_path: Optional config path override
            
        Returns:
            ConfluxDeployer instance
        """
        state_manager = StateManager(Path(state_path))
        state = state_manager.load()
        
        if not state:
            raise ValueError(f"No state found at {state_path}")
        
        # Load config from file if provided
        if config_path:
            config = ConfigLoader.load_from_file(config_path)
        else:
            # Try to load from default location
            default_config = Path(state_path).parent.parent / "config.json"
            if default_config.exists():
                config = ConfigLoader.load_from_file(str(default_config))
            else:
                raise ValueError("No config file found, please provide config_path")
        
        deployer = cls(config, Path(state_path))
        deployer.state_manager._state = state
        
        return deployer


def deploy_and_test(
    config_path: str,
    auto_cleanup: bool = True,
) -> Dict[str, Any]:
    """
    Convenience function to deploy and test in one call.
    
    Args:
        config_path: Path to configuration file
        auto_cleanup: Whether to cleanup after tests
        
    Returns:
        Complete workflow results
    """
    deployer = ConfluxDeployer.from_config_file(config_path)
    deployer.config.cleanup.auto_cleanup = auto_cleanup
    
    return deployer.full_workflow()
