from cloud_provisioner.host_spec import HostSpec

from remote_simulation.port_allocation import p2p_port
from remote_simulation.remote_node import RemoteNode


def should_use_private_peer_address(source_host: HostSpec, target_host: HostSpec) -> bool:
    return (
        source_host.provider == target_host.provider
        and source_host.region == target_host.region
        and bool(source_host.private_ip)
        and bool(target_host.private_ip)
    )


def peer_host_for_connection(source_host: HostSpec, target_host: HostSpec) -> str:
    if should_use_private_peer_address(source_host, target_host):
        return target_host.private_ip
    return target_host.ip


def peer_p2p_address(source_node: RemoteNode, target_node: RemoteNode) -> str:
    host = peer_host_for_connection(source_node.host_spec, target_node.host_spec)
    return f"{host}:{p2p_port(target_node.index)}"