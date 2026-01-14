"""
Command Line Interface for Conflux Deployer

Provides commands for:
- deploy: Deploy servers and start Conflux nodes
- test: Run tests on deployed network
- cleanup: Clean up all cloud resources
- recover: Recover from interrupted deployment
- images: Manage server images
- status: Check deployment status
"""

import argparse
import sys
import json
from pathlib import Path
from typing import Optional

from loguru import logger

from .main import ConfluxDeployer, deploy_and_test
from .configs import DeploymentConfig
from .configs.loader import ConfigLoader
from .configs.loader import StateManager
from .resource_cleanup import ResourceCleanupManager


def setup_logger(verbose: bool = False):
    """Setup logger configuration"""
    logger.remove()
    level = "DEBUG" if verbose else "INFO"
    logger.add(
        sys.stderr,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
               "<level>{level: <8}</level> | "
               "<level>{message}</level>",
        level=level,
    )


def get_deployer(args) -> ConfluxDeployer:
    """Get deployer from args"""
    config_path = Path(args.config)
    state_path = Path(args.state) if hasattr(args, 'state') and args.state else None
    
    return ConfluxDeployer.from_config_file(str(config_path), str(state_path) if state_path else None)


# === Deploy Command ===

def deploy_command(args):
    """Deploy command handler"""
    setup_logger(args.verbose)
    
    logger.info("Starting deployment...")
    
    try:
        deployer = get_deployer(args)
        
        if args.full:
            # Full workflow: deploy + test + cleanup
            result = deployer.full_workflow()
        else:
            # Just deploy
            result = deployer.deploy_all()
        
        logger.info("Deployment completed successfully!")
        
        if args.output:
            with open(args.output, 'w') as f:
                json.dump(result, f, indent=2, default=str)
            logger.info(f"Results saved to {args.output}")
        
        return 0
        
    except Exception as e:
        logger.error(f"Deployment failed: {e}")
        return 1


# === Test Command ===

def test_command(args):
    """Test command handler"""
    setup_logger(args.verbose)
    
    logger.info(f"Running {args.test_type} test...")
    
    try:
        deployer = get_deployer(args)
        
        if args.test_type == "all":
            results = deployer.run_all_tests()
        elif args.test_type == "stress":
            results = {"stress": deployer.run_stress_test(args.duration)}
        elif args.test_type == "latency":
            results = {"latency": deployer.run_latency_test(args.samples)}
        elif args.test_type == "fork":
            results = {"fork": deployer.run_fork_test(args.depth)}
        else:
            results = {"custom": deployer.run_custom_test(args.test_type)}
        
        # Print results
        for test_name, result in results.items():
            status = "✓ PASSED" if result.success else "✗ FAILED"
            logger.info(f"{test_name}: {status}")
            # Support both TestResult formats (legacy uses .metrics, newer uses .node_metrics)
            metrics_obj = getattr(result, "metrics", getattr(result, "node_metrics", {}))
            if metrics_obj:
                for key, value in metrics_obj.items():
                    logger.info(f"  {key}: {value}")
        
        if args.output:
            output_data = {
                name: {
                    "success": r.success,
                    "metrics": getattr(r, "metrics", getattr(r, "node_metrics", {})),
                    "errors": getattr(r, "errors", []),
                }
                for name, r in results.items()
            }
            with open(args.output, 'w') as f:
                json.dump(output_data, f, indent=2, default=str)
        
        all_passed = all(r.success for r in results.values())
        return 0 if all_passed else 1
        
    except Exception as e:
        logger.error(f"Test failed: {e}")
        return 1


# === Cleanup Command ===

def cleanup_command(args):
    """Cleanup command handler"""
    setup_logger(args.verbose)
    
    logger.info("Starting resource cleanup...")
    
    try:
        deployer = get_deployer(args)
        
        result = deployer.cleanup(
            force=args.force,
            delete_images=args.delete_images,
        )
        
        # Summary
        instances_cleaned = sum(1 for v in result["instances"].values() if v)
        sg_cleaned = sum(1 for v in result["security_groups"].values() if v)
        images_cleaned = sum(1 for v in result.get("images", {}).values() if v)
        
        logger.info(f"Cleanup completed:")
        logger.info(f"  Instances terminated: {instances_cleaned}/{len(result['instances'])}")
        logger.info(f"  Security groups deleted: {sg_cleaned}/{len(result['security_groups'])}")
        logger.info(f"  Images deleted: {images_cleaned}/{len(result.get('images', {}))}")
        
        if result.get("errors"):
            logger.warning(f"Encountered {len(result['errors'])} errors:")
            for error in result["errors"]:
                logger.warning(f"  - {error}")
            return 1
        
        return 0
        
    except Exception as e:
        logger.error(f"Cleanup failed: {e}")
        return 1


# === Force Instances Cleanup Command ===

def force_instances_command(args):
    """Force stop and delete instances (two modes: state file or naming pattern)."""

    setup_logger(args.verbose)

    try:
        config = ConfigLoader.load_from_file(args.config)

        if args.mode == "state":
            state_file_path = args.state_file or config.state_file_path
            cleanup_mgr = ResourceCleanupManager(config, StateManager(state_file_path))
            results = cleanup_mgr.force_stop_and_delete_instances_from_state_file(
                state_file_path,
                dry_run=bool(args.dry_run),
            )

        elif args.mode == "pattern":
            # Requires config for credentials/regions; uses naming pattern instead of state.
            cleanup_mgr = ResourceCleanupManager(config, StateManager(config.state_file_path))
            results = cleanup_mgr.force_stop_and_delete_instances_by_naming_pattern(
                deployment_id=args.deployment_id,
                pattern_prefix=args.pattern_prefix,
                dry_run=bool(args.dry_run),
            )
        else:
            raise ValueError(f"Unsupported mode: {args.mode}")

        ok = sum(1 for v in results.values() if v)
        total = len(results)
        if args.dry_run:
            logger.info(f"Dry-run completed: {total} instances matched")
            return 0
        logger.info(f"Force instance cleanup completed: {ok}/{total} terminated")
        return 0 if ok == total else 1

    except Exception as e:
        logger.error(f"Force instance cleanup failed: {e}")
        return 1


# === Recover Command ===

def recover_command(args):
    """Recover command handler"""
    setup_logger(args.verbose)
    
    logger.info(f"Recovering deployment from {args.state}...")
    
    try:
        deployer = ConfluxDeployer.recover(
            args.state,
            args.config if hasattr(args, 'config') else None,
        )
        
        instances = deployer.recover_deployment()
        
        logger.info(f"Recovered {len(instances)} instances")
        
        for instance in instances:
            logger.info(
                f"  {instance.instance_id}: {instance.provider.value}/{instance.region_id} "
                f"- {instance.public_ip}"
            )
        
        return 0
        
    except Exception as e:
        logger.error(f"Recovery failed: {e}")
        return 1


# === Images Command ===

def images_command(args):
    """Images command handler"""
    setup_logger(args.verbose)
    
    try:
        deployer = get_deployer(args)
        
        if args.action == "create":
            logger.info("Creating server images...")
            images = deployer.create_images(force_recreate=args.force)
            
            for provider, regions in images.items():
                for region_id, image_id in regions.items():
                    logger.info(f"  {provider}/{region_id}: {image_id}")
            
        elif args.action == "find":
            logger.info("Finding existing images...")
            images = deployer.find_existing_images()
            
            if not any(images.values()):
                logger.info("No existing images found")
            else:
                for provider, regions in images.items():
                    for region_id, image_id in regions.items():
                        logger.info(f"  {provider}/{region_id}: {image_id}")
            
        elif args.action == "delete":
            logger.info("Deleting images...")
            result = deployer.delete_images()
            
            deleted = sum(1 for v in result.values() if v)
            logger.info(f"Deleted {deleted}/{len(result)} images")
        
        return 0
        
    except Exception as e:
        logger.error(f"Image operation failed: {e}")
        return 1


# === Status Command ===

def status_command(args):
    """Status command handler"""
    setup_logger(args.verbose)
    
    try:
        deployer = get_deployer(args)
        
        state = deployer.state_manager.load()
        
        if not state:
            logger.info("No deployment state found")
            return 0
        
        logger.info(f"Deployment: {state.deployment_id}")
        logger.info(f"Phase: {state.phase}")
        logger.info(f"Started: {state.created_at}")
        
        if state.instances:
            logger.info(f"Instances: {len(state.instances)}")
            for instance in state.instances:
                logger.info(
                    f"  {instance.instance_id}: {instance.state.value} "
                    f"({instance.provider.value}/{instance.region_id})"
                )
        
        if state.nodes:
            logger.info(f"Nodes: {len(state.nodes)}")
            for node in state.nodes:
                logger.info(
                    f"  {node.node_id}: RPC={node.rpc_port}, P2P={node.p2p_port}"
                )
        
        # Cost estimate
        cost = deployer.estimate_cost()
        logger.info(f"Estimated cost: ${cost['hourly_cost_usd']:.2f}/hour, ${cost['daily_cost_usd']:.2f}/day")
        
        if state.errors:
            logger.warning(f"Errors: {len(state.errors)}")
            for error in state.errors[-5:]:  # Last 5 errors
                logger.warning(f"  - {error}")
        
        return 0
        
    except Exception as e:
        logger.error(f"Status check failed: {e}")
        return 1


# === Init Command ===

def init_command(args):
    """Initialize a new configuration file"""
    setup_logger(args.verbose)
    
    output_path = Path(args.output)
    
    if output_path.exists() and not args.force:
        logger.error(f"File {args.output} already exists. Use --force to overwrite.")
        return 1
    
    # Create example config
    example_config = {
        "deployment_id": "conflux-test-001",
        "instance_name_prefix": "conflux-node",
        "credentials": {
            "aws": {
                "access_key_id": "YOUR_AWS_ACCESS_KEY",
                "secret_access_key": "YOUR_AWS_SECRET_KEY",
                "key_pair_name": "conflux-keypair",
                "key_pair_path": "~/.ssh/conflux-keypair.pem"
            },
            "alibaba": {
                "access_key_id": "YOUR_ALIBABA_ACCESS_KEY",
                "secret_access_key": "YOUR_ALIBABA_SECRET_KEY",
                "key_pair_name": "conflux-keypair",
                "key_pair_path": "~/.ssh/conflux-keypair.pem"
            }
        },
        "regions": [
            {
                "provider": "aws",
                "region_id": "us-west-2",
                "instance_count": 3,
                "nodes_per_instance": 1,
                "instance_type": "m6i.2xlarge"
            },
            {
                "provider": "aws",
                "region_id": "ap-northeast-1",
                "instance_count": 2,
                "nodes_per_instance": 1,
                "instance_type": "m6i.2xlarge"
            }
        ],
        "image": {
            "name": "conflux-node-image",
            "conflux_image": "confluxchain/conflux-rust:latest",
            "base_image": "ubuntu:22.04",
            "reuse_existing": True
        },
        "conflux_node": {
            "chain_id": 1,
            "log_level": "info",
            "mining_enabled": True,
            "stratum_listen_address": "127.0.0.1",
            "stratum_port": 32525,
            "bootnodes": []
        },
        "network": {
            "p2p_port_base": 32323,
            "rpc_port_base": 12537,
            "connect_all": True,
            "debug_mode": False
        },
        "test": {
            "stress_test_enabled": True,
            "latency_test_enabled": True,
            "fork_test_enabled": False,
            "test_duration_seconds": 300,
            "tps_target": 1000
        },
        "cleanup": {
            "auto_cleanup": True,
            "delete_images": False,
            "grace_period_seconds": 30,
            "retry_attempts": 3
        }
    }
    
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_path, 'w') as f:
        json.dump(example_config, f, indent=2)
    
    logger.info(f"Created example configuration at {args.output}")
    logger.info("Please edit the file and update credentials before deployment.")
    
    return 0


def main():
    """Main CLI entry point"""
    parser = argparse.ArgumentParser(
        description="Conflux Multi-Cloud Deployment Tool",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
# Initialize a new config file
conflux-deployer init -o config.json

# Deploy servers and nodes
conflux-deployer deploy -c config.json

# Run all tests
conflux-deployer test all -c config.json

# Run stress test only
conflux-deployer test stress -c config.json --duration 600

# Check deployment status
conflux-deployer status -c config.json

# Clean up all resources
conflux-deployer cleanup -c config.json

# Force cleanup including images
conflux-deployer cleanup -c config.json --force --delete-images

# Force stop+delete instances recorded in a local state file
conflux-deployer force-instances --mode state -c config.json --state-file ./deployment_state.json

# Force stop+delete instances by naming pattern (no local state required)
conflux-deployer force-instances --mode pattern -c config.json --deployment-id deploy-20250101-010101-deadbeef

# Recover from interrupted deployment
conflux-deployer recover -s state/deployment-id.json
"""
    )
    
    # Global arguments
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose output")
    parser.add_argument("-c", "--config", default="config.json", help="Configuration file path")
    parser.add_argument("-s", "--state", help="State file path (default: state/<deployment_id>.json)")
    
    # Subcommands
    subparsers = parser.add_subparsers(dest="command", required=True)
    
    # Init command
    init_parser = subparsers.add_parser("init", help="Initialize a new configuration file")
    init_parser.add_argument("-o", "--output", default="config.json", help="Output config file path")
    init_parser.add_argument("-f", "--force", action="store_true", help="Overwrite existing file")
    init_parser.set_defaults(func=init_command)
    
    # Deploy command
    deploy_parser = subparsers.add_parser("deploy", help="Deploy servers and start nodes")
    deploy_parser.add_argument("--full", action="store_true", help="Run full workflow (deploy + test + cleanup)")
    deploy_parser.add_argument("-o", "--output", help="Output results to JSON file")
    deploy_parser.set_defaults(func=deploy_command)
    
    # Test command
    test_parser = subparsers.add_parser("test", help="Run tests on deployed network")
    test_parser.add_argument("test_type", choices=["all", "stress", "latency", "fork"], help="Test type to run")
    test_parser.add_argument("--duration", type=int, default=300, help="Stress test duration (seconds)")
    test_parser.add_argument("--samples", type=int, default=100, help="Latency test sample count")
    test_parser.add_argument("--depth", type=int, default=10, help="Fork test target depth")
    test_parser.add_argument("-o", "--output", help="Output results to JSON file")
    test_parser.set_defaults(func=test_command)
    
    # Cleanup command
    cleanup_parser = subparsers.add_parser("cleanup", help="Clean up all cloud resources")
    cleanup_parser.add_argument("-f", "--force", action="store_true", help="Force cleanup")
    cleanup_parser.add_argument("--delete-images", action="store_true", help="Also delete server images")
    cleanup_parser.set_defaults(func=cleanup_command)

    # Force instances cleanup command
    fi_parser = subparsers.add_parser(
        "force-instances",
        help="Force stop and delete server instances (by state file or naming pattern)",
    )
    fi_parser.add_argument(
        "--mode",
        choices=["state", "pattern"],
        required=True,
        help="Cleanup mode: state=use local state file; pattern=search by instance naming pattern",
    )
    fi_parser.add_argument(
        "--state-file",
        help="State file path for mode=state (default: config.state_file_path)",
    )
    fi_parser.add_argument(
        "--deployment-id",
        help="Optional deployment id to narrow mode=pattern",
    )
    fi_parser.add_argument(
        "--pattern-prefix",
        help="Optional explicit name prefix override for mode=pattern",
    )
    fi_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Do not stop/terminate; only report matched instances",
    )
    fi_parser.set_defaults(func=force_instances_command)
    
    # Recover command
    recover_parser = subparsers.add_parser("recover", help="Recover from interrupted deployment")
    recover_parser.set_defaults(func=recover_command)
    
    # Images command
    images_parser = subparsers.add_parser("images", help="Manage server images")
    images_parser.add_argument("action", choices=["create", "find", "delete"], help="Image action")
    images_parser.add_argument("-f", "--force", action="store_true", help="Force recreate images")
    images_parser.set_defaults(func=images_command)
    
    # Status command
    status_parser = subparsers.add_parser("status", help="Check deployment status")
    status_parser.set_defaults(func=status_command)
    
    # Parse and execute
    args = parser.parse_args()
    
    if hasattr(args, "func"):
        sys.exit(args.func(args))
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
