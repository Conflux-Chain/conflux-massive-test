from typing import List, Optional

from loguru import logger

from cloud_provisioner.host_spec import HostSpec
from .provision_config import CloudConfig
from .regional_infra import InfraProvider
from ..provider_interface import IEcsClient


def ensure_enterprise_network(client: IEcsClient, cloud_config: CloudConfig, infra_provider: InfraProvider):
    if not cloud_config.enterprise_network.enabled:
        return None

    if cloud_config.provider != "aliyun":
        logger.warning(f"Enterprise network is only implemented for aliyun, skip provider={cloud_config.provider}")
        return None

    from ..aliyun_provider.client_factory import AliyunClient
    from ..aliyun_provider.enterprise_network import ensure_enterprise_network as ensure_aliyun_enterprise_network

    if not isinstance(client, AliyunClient):
        raise TypeError(f"Aliyun enterprise network requires AliyunClient, got {type(client).__name__}")

    return ensure_aliyun_enterprise_network(client, cloud_config, infra_provider)


def assign_enterprise_network_addresses(client: IEcsClient, cloud_config: CloudConfig, hosts: List[HostSpec]) -> List[HostSpec]:
    if not cloud_config.enterprise_network.enabled:
        return hosts

    if cloud_config.provider != "aliyun":
        return hosts

    from ..aliyun_provider.client_factory import AliyunClient
    from ..aliyun_provider.enterprise_network import assign_instance_private_mesh_ips

    if not isinstance(client, AliyunClient):
        raise TypeError(f"Aliyun enterprise network requires AliyunClient, got {type(client).__name__}")

    updated_hosts = assign_instance_private_mesh_ips(client, hosts, cloud_config)
    if cloud_config.enterprise_network.prefer_p2p_ip:
        return updated_hosts

    result: List[HostSpec] = []
    for host in updated_hosts:
        result.append(
            HostSpec(
                ip=host.ip,
                nodes_per_host=host.nodes_per_host,
                ssh_user=host.ssh_user,
                ssh_key_path=host.ssh_key_path,
                provider=host.provider,
                region=host.region,
                zone=host.zone,
                instance_id=host.instance_id,
                private_ip=host.private_ip,
                p2p_ip=None,
            )
        )
    return result