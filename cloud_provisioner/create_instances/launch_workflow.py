from itertools import chain
from typing import Callable

from loguru import logger

from ..provider_interface import IEcsClient
from .instance_config import InstanceConfig
from .instance_provisioner import create_instances_in_region
from .network_infra import InfraProvider
from .provision_config import AliyunCloudConfig, CloudConfig, ProvisionRegionConfig
from .region_backfill import backfill_shortfall, count_nodes, healthy_regions_for_backfill, run_regions_with_config
from .types import InstanceType


def build_instance_config(cloud_config: CloudConfig) -> InstanceConfig:
    cfg = InstanceConfig(user_tag_value=cloud_config.user_tag)
    if cloud_config.provider == "aliyun" and isinstance(cloud_config, AliyunCloudConfig):
        cfg.use_aliyun_eip = cloud_config.use_eip
    return cfg


def calculate_shortfall(region_results, target_total_nodes: int) -> int:
    created_nodes = sum(result["actual_nodes"] for result in region_results)
    shortfall = target_total_nodes - created_nodes

    underfilled = [
        result for result in region_results
        if result["actual_nodes"] < result["requested_nodes"]
    ]
    if underfilled:
        detail = ", ".join(
            f"{result['region']}({result['actual_nodes']}/{result['requested_nodes']})"
            for result in underfilled
        )
        logger.warning(f"Regions under target: {detail}")

    return shortfall


def apply_shortfall_backfill(
    create_in_region: Callable[[ProvisionRegionConfig], list],
    region_results,
    hosts: list,
    shortfall: int,
) -> int:
    if shortfall > 0:
        healthy_regions = healthy_regions_for_backfill(region_results)
        if healthy_regions:
            logger.warning(f"Total nodes shortfall={shortfall}, try backfill in healthy regions")
            extra_hosts, remaining = backfill_shortfall(create_in_region, healthy_regions, shortfall)
            hosts.extend(extra_hosts)
            return remaining

    return shortfall


def create_hosts_with_optional_backfill(
    create_in_region: Callable[[ProvisionRegionConfig], list],
    regions: list[ProvisionRegionConfig],
    target_total_nodes: int,
    allow_backfill: bool,
) -> tuple[list, int]:
    region_results = run_regions_with_config(create_in_region, regions)
    hosts = list(chain.from_iterable(result["hosts"] for result in region_results))
    shortfall = calculate_shortfall(region_results, target_total_nodes)
    if allow_backfill:
        shortfall = apply_shortfall_backfill(
            create_in_region,
            region_results,
            hosts,
            shortfall,
        )

    return hosts, shortfall


def create_instances_in_multi_region(
    client: IEcsClient,
    cloud_config: CloudConfig,
    infra_provider: InfraProvider,
    *,
    allow_backfill: bool = True,
):
    instance_config = build_instance_config(cloud_config)
    instance_types = [InstanceType(i.name, i.nodes) for i in cloud_config.instance_types]
    regions = [region for region in cloud_config.regions if region.count > 0]

    def _create_in_region(provision_config: ProvisionRegionConfig):
        region_id = provision_config.name
        return create_instances_in_region(
            client,
            instance_config,
            provision_config,
            region_info=infra_provider.get_region(region_id),
            instance_types=instance_types,
            ssh_user=cloud_config.default_user_name,
            provider=cloud_config.provider,
        )

    target_total_nodes = cloud_config.total_nodes
    hosts, shortfall = create_hosts_with_optional_backfill(
        _create_in_region,
        regions,
        target_total_nodes,
        allow_backfill,
    )

    final_nodes = count_nodes(hosts)
    if shortfall <= 0:
        logger.success(
            f"{cloud_config.provider} launch complete: target_nodes={target_total_nodes}, actual_nodes={final_nodes}"
        )
    else:
        logger.error(
            f"{cloud_config.provider} launch incomplete: target_nodes={target_total_nodes}, actual_nodes={final_nodes}, shortfall={shortfall}"
        )

    return hosts