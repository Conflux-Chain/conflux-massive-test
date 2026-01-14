from __future__ import annotations

from pathlib import Path
from typing import Set, Tuple, cast

from conflux_deployer.node_management.node_manager import NodeManager
from conflux_deployer.server_deployment.server_deployer import ServerInstance
from conflux_deployer.configs.config_manager import ConfigManager


class FakeConfigManager:
    def get_conflux_config(self):
        return {
            "base_port": 12537,
            "base_rpc_port": 12539,
            "base_p2p_port": 12538,
            # Small block for test to make collisions obvious if they occur
            "ports_block_size": 10,
        }


def ports_from_nodes(nodes) -> Tuple[Set[int], Set[int], Set[int]]:
    return set(n.port for n in nodes), set(n.rpc_port for n in nodes), set(n.p2p_port for n in nodes)


def test_port_blocks_are_non_overlapping(tmp_path: Path):
    cfg = FakeConfigManager()
    nm = NodeManager(cast('ConfigManager', cfg))
    # Use a temp state file
    nm.state_file = tmp_path / "node_state.pkl"

    inst1 = ServerInstance(
        instance_id="inst-1",
        cloud_provider="aws",
        region="r1",
        instance_type="t1",
        ip_address="10.0.0.1",
        status="running",
        purpose="test",
        created_at="now",
        nodes_count=3,
    )

    inst2 = ServerInstance(
        instance_id="inst-2",
        cloud_provider="aws",
        region="r1",
        instance_type="t1",
        ip_address="10.0.0.2",
        status="running",
        purpose="test",
        created_at="now",
        nodes_count=2,
    )

    nodes1 = nm.collect_node_info(inst1)
    nodes2 = nm.collect_node_info(inst2)

    p1, r1, pp1 = ports_from_nodes(nodes1)
    p2, r2, pp2 = ports_from_nodes(nodes2)

    # Ensure no overlaps across instances
    assert p1.isdisjoint(p2)
    assert r1.isdisjoint(r2)
    assert pp1.isdisjoint(pp2)

    # Save state and reload manager to ensure allocations persist
    nm._save_state()

    nm2 = NodeManager(cast('ConfigManager', cfg))
    nm2.state_file = tmp_path / "node_state.pkl"
    nm2._load_state()

    assert nm.port_allocations == nm2.port_allocations
    # Collecting again for same instance should not change allocation
    before_alloc = nm2.port_allocations["inst-1"]
    nm2.collect_node_info(inst1)
    assert nm2.port_allocations["inst-1"] == before_alloc
