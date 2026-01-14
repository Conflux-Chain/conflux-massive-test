#!/usr/bin/env python3
"""
Remote Simulation Script (New Version)

This script uses the new conflux_deployer framework for instance management
while keeping the existing remote_simulation test logic.

Usage:
    # Create instances and run test
    python remote_simulate_v2.py run --instance-count 10
    
    # Create instances only (save state for later)
    python remote_simulate_v2.py create --instance-count 10 -o state.json
    
    # Run test on existing instances
    python remote_simulate_v2.py test -s state.json
    
    # Cleanup instances
    python remote_simulate_v2.py cleanup -s state.json
"""

import argparse
import datetime
import os
import pickle
import sys
from pathlib import Path
from typing import List, Optional

from loguru import logger

from remote_simulation.block_generator import generate_blocks_async
from remote_simulation.network_connector import connect_nodes
from remote_simulation.network_topology import NetworkTopology
from remote_simulation.config_builder import SimulateOptions, ConfluxOptions, generate_config_file
from remote_simulation.tools import init_tx_gen, wait_for_nodes_synced

# AsyncSSH-based remote ops (no spawning大量 ssh/scp 子进程)
from remote_simulation.async_remote import (
    launch_remote_nodes_asyncssh,
    stop_remote_nodes_asyncssh,
    destroy_remote_nodes_asyncssh,
    collect_logs_asyncssh,
)

from utils.wait_until import WaitUntilTimeoutError

# Import the adapter for the new framework
from conflux_deployer.adapter import DeployerAdapter, LegacyInstances


def generate_timestamp():
    """生成当前时间戳"""
    return datetime.datetime.now().strftime("%Y%m%d%H%M%S")


def run_simulation(
    ip_addresses: List[str],
    nodes_per_host: int = 1,
    pull_docker_image: bool = True,
    log_path: Optional[str] = None,
    simulation_config: Optional[SimulateOptions] = None,
    node_config: Optional[ConfluxOptions] = None,
    ssh_key_path: Optional[str] = None,
):
    """
    Run the Conflux simulation on given instances.
    
    Args:
        ip_addresses: List of instance IP addresses
        nodes_per_host: Number of nodes per instance
        pull_docker_image: Whether to pull Docker image
        log_path: Path to save logs
        simulation_config: Simulation configuration
        node_config: Node configuration
    """
    # Default configs
    if simulation_config is None:
        simulation_config = SimulateOptions(
            target_nodes=len(ip_addresses) * nodes_per_host,
            nodes_per_host=nodes_per_host,
            num_blocks=1000,
            connect_peers=8,
            target_tps=17000,
            storage_memory_gb=16,
            generation_period_ms=175,
        )
    
    if node_config is None:
        node_config = ConfluxOptions(
            send_tx_period_ms=200,
            tx_pool_size=2_000_000,
            target_block_gas_limit=120_000_000,
            max_block_size_in_bytes=450 * 1024,
            txgen_account_count=500,
        )
    
    # Validate
    assert simulation_config.target_nodes == simulation_config.nodes_per_host * len(ip_addresses)
    assert node_config.txgen_account_count * simulation_config.target_nodes <= 100_000
    
    # Generate config file
    config_file = generate_config_file(simulation_config, node_config)
    logger.success(f"配置文件已生成: {config_file.path}")
    
    # Setup log path
    if log_path is None:
        log_path = f"logs/{generate_timestamp()}"
    Path(log_path).mkdir(parents=True, exist_ok=True)
    
    # 3. Launch nodes
    # Requirement: use server image to start; do NOT docker pull at runtime.
    if pull_docker_image:
        logger.warning("Ignoring pull_docker_image=True: v2 enforces server-image startup (no runtime docker pull)")

    launch = launch_remote_nodes_asyncssh(
        ip_addresses,
        simulation_config.nodes_per_host,
        str(config_file.path),
        ssh_key_path=ssh_key_path,
    )
    nodes = launch.nodes
    if launch.failed_hosts:
        logger.warning(f"Some hosts failed during launch: {len(launch.failed_hosts)}")
    
    if len(nodes) < simulation_config.target_nodes:
        raise Exception(f"Not all nodes started: {len(nodes)}/{simulation_config.target_nodes}")
    logger.success("所有节点已启动，准备连接拓扑网络")
    
    # 4. Connect network topology
    topology = NetworkTopology.generate_random_topology(len(nodes), simulation_config.connect_peers)
    for k, v in topology.peers.items():
        logger.debug(f"Node {nodes[k].id}({k}) has {len(v)} peers: {', '.join([str(i) for i in v])}")
    connect_nodes(nodes, topology, min_peers=7)
    logger.success("拓扑网络构建完毕")
    wait_for_nodes_synced(nodes)
    
    # 5. Run experiment
    init_tx_gen(nodes, node_config.txgen_account_count)
    logger.success("开始运行区块链系统")
    generate_blocks_async(
        nodes,
        simulation_config.num_blocks,
        node_config.max_block_size_in_bytes,
        simulation_config.generation_period_ms,
    )
    
    logger.info(f"Node goodput: {nodes[0].rpc.test_getGoodPut()}")
    
    try:
        wait_for_nodes_synced(nodes)
        logger.success("测试完毕，准备采集日志数据")
    except WaitUntilTimeoutError:
        logger.warning("部分节点没有完全同步，准备采集日志数据")
    
    # 6. Collect results
    logger.info(f"Node goodput: {nodes[0].rpc.test_getGoodPut()}")
    collect_logs_asyncssh(nodes, log_path, ssh_key_path=ssh_key_path)
    logger.success(f"日志收集完毕，路径 {os.path.abspath(log_path)}")
    
    return nodes


def cmd_run(args):
    """Run full workflow: create instances, run test, cleanup"""
    adapter = DeployerAdapter()
    
    regions = None
    if args.regions:
        import json
        regions = json.loads(args.regions)
    
    try:
        # Create instances
        instances = adapter.launch(
            instance_count=args.instance_count,
            instance_type=args.instance_type,
            regions=regions,
            pull_docker_image=True,
        )
        
        logger.info(f"实例列表: {instances.ip_addresses}")
        
        # Run simulation
        run_simulation(
            ip_addresses=instances.ip_addresses,
            nodes_per_host=args.nodes_per_host,
            pull_docker_image=not args.skip_pull,
            ssh_key_path=adapter.deployer.config.ssh_private_key_path,
        )
        
    finally:
        if not args.no_cleanup:
            logger.info("清理资源...")
            adapter.terminate()
            logger.success("清理完成")
        else:
            # Save state for later cleanup
            adapter.save_state()
            logger.info(f"状态已保存，可以使用 cleanup 命令清理")


def cmd_create(args):
    """Create instances only"""
    adapter = DeployerAdapter()
    
    regions = None
    if args.regions:
        import json
        regions = json.loads(args.regions)
    
    instances = adapter.launch(
        instance_count=args.instance_count,
        instance_type=args.instance_type,
        regions=regions,
    )
    
    # Save for later
    if args.output:
        # Save as pickle for compatibility
        import pickle
        
        # Create a compatible object
        from dataclasses import dataclass
        from typing import Literal
        
        @dataclass
        class CompatInstances:
            ip_addresses: List[str]
            instance_ids: List[str]
            deployer_state_path: str
        
        compat = CompatInstances(
            ip_addresses=instances.ip_addresses,
            instance_ids=instances.instance_ids,
            deployer_state_path=str(adapter.deployer.state_path),
        )
        
        with open(args.output, 'wb') as f:
            pickle.dump(compat, f)
        logger.info(f"实例信息已保存到 {args.output}")
    
    adapter.save_state()
    
    cost = adapter.estimate_cost()
    logger.info(f"预估费用: ${cost['hourly_cost_usd']:.2f}/小时, ${cost['daily_cost_usd']:.2f}/天")
    
    return instances


def cmd_test(args):
    """Run test on existing instances"""
    # Try to load from pickle file (old format)
    if args.instances_file and Path(args.instances_file).exists():
        with open(args.instances_file, 'rb') as f:
            data = pickle.load(f)
        
        if hasattr(data, 'ip_addresses'):
            ip_addresses = data.ip_addresses
        elif isinstance(data, dict):
            ip_addresses = data.get('ip_addresses', [])
        else:
            raise ValueError(f"Unknown instances file format")
        
        logger.info(f"从 {args.instances_file} 加载了 {len(ip_addresses)} 个实例")
    
    # Or recover from state
    elif args.state:
        adapter = DeployerAdapter(state_path=args.state)
        instances = adapter.recover()
        ip_addresses = instances.ip_addresses
        logger.info(f"从 {args.state} 恢复了 {len(ip_addresses)} 个实例")
    
    else:
        raise ValueError("必须提供 --instances-file 或 --state")
    
    # Determine ssh key path (if recovering from state, adapter is available)
    ssh_key_path: Optional[str] = None
    if args.state:
        adapter = DeployerAdapter(state_path=args.state)
        instances = adapter.recover()
        ssh_key_path = getattr(adapter.deployer.config, "ssh_private_key_path", None)
    
    # Run simulation
    run_simulation(
        ip_addresses=ip_addresses,
        nodes_per_host=args.nodes_per_host,
        pull_docker_image=not args.skip_pull,
        ssh_key_path=ssh_key_path,
    )


def cmd_cleanup(args):
    """Cleanup instances"""
    # Try to cleanup using new framework
    if args.state:
        adapter = DeployerAdapter(state_path=args.state)
        try:
            adapter.recover()
        except Exception as e:
            logger.warning(f"无法恢复状态: {e}")
        
        adapter.terminate()
        logger.success("实例已终止")
        return
    
    # Or cleanup using old pickle file
    if args.instances_file:
        with open(args.instances_file, 'rb') as f:
            data = pickle.load(f)
        
        # Check if it has the new deployer_state_path
        if hasattr(data, 'deployer_state_path'):
            adapter = DeployerAdapter(state_path=data.deployer_state_path)
            try:
                adapter.recover()
            except Exception:
                pass
            adapter.terminate()
            logger.success("实例已终止")
            return
        
        # Old format - use old termination
        if hasattr(data, 'terminate'):
            data.terminate()
            logger.success("实例已终止")
            return
        
        raise ValueError("无法识别实例文件格式")
    
    raise ValueError("必须提供 --state 或 --instances-file")


def cmd_stop_nodes(args):
    """Stop Conflux nodes without terminating instances"""
    if args.instances_file:
        with open(args.instances_file, 'rb') as f:
            data = pickle.load(f)
        ip_addresses = data.ip_addresses if hasattr(data, 'ip_addresses') else data.get('ip_addresses', [])
    elif args.state:
        adapter = DeployerAdapter(state_path=args.state)
        instances = adapter.recover()
        ip_addresses = instances.ip_addresses
    else:
        raise ValueError("必须提供 --instances-file 或 --state")
    
    # Avoid spawning ssh processes; use asyncssh.
    # Determine ssh key path for cleanup commands
    ssh_key_path: Optional[str] = None
    if args.state:
        adapter = DeployerAdapter(state_path=args.state)
        try:
            adapter.recover()
        except Exception:
            pass
        ssh_key_path = getattr(adapter.deployer.config, "ssh_private_key_path", None)

    if args.destroy:
        destroy_remote_nodes_asyncssh(ip_addresses, ssh_key_path=ssh_key_path)
        logger.success("所有节点已销毁")
    else:
        stop_remote_nodes_asyncssh(ip_addresses, ssh_key_path=ssh_key_path)
        logger.success("所有节点已停止")


def main():
    parser = argparse.ArgumentParser(
        description="Conflux Remote Simulation Tool (v2 - using conflux_deployer)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    
    subparsers = parser.add_subparsers(dest="command", required=True)
    
    # Run command
    run_parser = subparsers.add_parser("run", help="Run full workflow: create, test, cleanup")
    run_parser.add_argument("-n", "--instance-count", type=int, default=10, help="Number of instances")
    run_parser.add_argument("-t", "--instance-type", default="m6i.2xlarge", help="Instance type")
    run_parser.add_argument("--nodes-per-host", type=int, default=1, help="Nodes per instance")
    run_parser.add_argument("--regions", help="Region configs as JSON")
    run_parser.add_argument("--skip-pull", action="store_true", help="Skip Docker image pull")
    run_parser.add_argument("--no-cleanup", action="store_true", help="Don't cleanup after test")
    run_parser.set_defaults(func=cmd_run)
    
    # Create command
    create_parser = subparsers.add_parser("create", help="Create instances only")
    create_parser.add_argument("-n", "--instance-count", type=int, default=10, help="Number of instances")
    create_parser.add_argument("-t", "--instance-type", default="m6i.2xlarge", help="Instance type")
    create_parser.add_argument("--regions", help="Region configs as JSON")
    create_parser.add_argument("-o", "--output", default="instances.pkl", help="Output pickle file")
    create_parser.set_defaults(func=cmd_create)
    
    # Test command
    test_parser = subparsers.add_parser("test", help="Run test on existing instances")
    test_parser.add_argument("-f", "--instances-file", help="Pickle file with instances")
    test_parser.add_argument("-s", "--state", help="State file from deployer")
    test_parser.add_argument("--nodes-per-host", type=int, default=1, help="Nodes per instance")
    test_parser.add_argument("--skip-pull", action="store_true", help="Skip Docker image pull")
    test_parser.set_defaults(func=cmd_test)
    
    # Cleanup command
    cleanup_parser = subparsers.add_parser("cleanup", help="Cleanup instances")
    cleanup_parser.add_argument("-f", "--instances-file", help="Pickle file with instances")
    cleanup_parser.add_argument("-s", "--state", help="State file from deployer")
    cleanup_parser.set_defaults(func=cmd_cleanup)
    
    # Stop nodes command
    stop_parser = subparsers.add_parser("stop-nodes", help="Stop Conflux nodes (keep instances)")
    stop_parser.add_argument("-f", "--instances-file", help="Pickle file with instances")
    stop_parser.add_argument("-s", "--state", help="State file from deployer")
    stop_parser.add_argument("--destroy", action="store_true", help="Destroy nodes instead of stopping")
    stop_parser.set_defaults(func=cmd_stop_nodes)
    
    args = parser.parse_args()
    
    if hasattr(args, 'func'):
        try:
            args.func(args)
        except KeyboardInterrupt:
            logger.warning("用户中断")
            sys.exit(1)
        except Exception as e:
            logger.error(f"错误: {e}")
            raise
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
