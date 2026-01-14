from __future__ import annotations

from conflux_deployer.main import ConfluxDeployer
from conflux_deployer.configs.types import DeploymentConfig, RegionConfig, InstanceInfo
from conflux_deployer.node_management.manager import NodeManager


def test_inventory_exports_node_ports_and_hardware(tmp_path):
    # Build a minimal deployment config with regions
    cfg = DeploymentConfig(
        deployment_id="deploy-inv",
        instance_name_prefix="ci",
        regions=[
            RegionConfig(provider=__import__('conflux_deployer.configs.types', fromlist=['CloudProvider']).CloudProvider.AWS, region_id="us-west-2", location_name="US West", instance_count=0, instance_type="m5.large"),
        ],
    )

    # Create a fake server list (InstanceInfo objects)
    inst1 = InstanceInfo(
        instance_id="i-1",
        provider=__import__('conflux_deployer.configs.types', fromlist=['CloudProvider']).CloudProvider.AWS,
        region_id="us-west-2",
        location_name="US West",
        instance_type="m5.large",
        public_ip="52.1.1.1",
        private_ip="10.0.0.1",
        state=__import__('conflux_deployer.configs.types', fromlist=['InstanceState']).InstanceState.RUNNING,
        nodes_count=2,
        name="conflux-deployer-ci-deploy-inv-us-west-2-0",
    )

    inst2 = InstanceInfo(
        instance_id="i-2",
        provider=__import__('conflux_deployer.configs.types', fromlist=['CloudProvider']).CloudProvider.AWS,
        region_id="us-west-2",
        location_name="US West",
        instance_type="m5.large",
        public_ip="52.1.1.2",
        private_ip="10.0.0.2",
        state=__import__('conflux_deployer.configs.types', fromlist=['InstanceState']).InstanceState.RUNNING,
        nodes_count=1,
        name="conflux-deployer-ci-deploy-inv-us-west-2-1",
    )

    # Construct deployer and inject fake server_deployer
    deployer = ConfluxDeployer(cfg)

    class FakeServerDeployer:
        def list_instances(self):
            return [inst1, inst2]

    deployer._server_deployer = FakeServerDeployer()

    # Create and attach NodeManager to generate nodes
    nm = NodeManager(cfg, [inst1, inst2])
    nm.initialize_nodes()
    deployer._node_manager = nm

    inv = deployer.get_inventory()

    # Two instances present
    assert len(inv) == 2

    for server in inv:
        assert "instance_id" in server
        assert "public_ip" in server
        assert "instance_type" in server
        # nodes should be a list with port information
        assert isinstance(server["nodes"], list)
        for n in server["nodes"]:
            assert "jsonrpc_port" in n and "p2p_port" in n

    # Ensure that nodes per instance match nodes_count
    assert len(inv[0]["nodes"]) == 2
    assert len(inv[1]["nodes"]) == 1
