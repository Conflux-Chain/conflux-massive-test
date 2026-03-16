from itertools import product
import math
import threading
import time
from typing import List

from loguru import logger

from .instance_config import InstanceConfig
from .provision_config import ProvisionRegionConfig
from ..provider_interface import IEcsClient
from .types import CreateInstanceError, InstanceType, RegionInfo, ZoneInfo
from cloud_provisioner.host_spec import HostSpec

from .instance_verifier import InstanceVerifier


def _next_zone_plan_candidate(zone_plan, blocked_zone_ids: set[str]):
    while True:
        instance_type, zone_info = next(zone_plan)
        if zone_info.id in blocked_zone_ids:
            continue
        return instance_type, zone_info


def _create_instances_in_zone_with_blocked_zone_tracking(client: IEcsClient, blocked_zone_ids: set[str], cfg: InstanceConfig, region_info: RegionInfo, zone_info: ZoneInfo, instance_type: InstanceType, amount: int):
    instance_ids, err = client.create_instances_in_zone(
        cfg, region_info, zone_info, instance_type, amount, min_amount=1)
    if err == CreateInstanceError.ZoneUnavailable:
        blocked_zone_ids.add(zone_info.id)
    return instance_ids, err


def create_instances_in_region(client: IEcsClient, cfg: InstanceConfig, provision_config: ProvisionRegionConfig, *, region_info: RegionInfo, instance_types: List[InstanceType], ssh_user: str, provider: str):
    nodes = provision_config.count
    blocked_zone_ids: set[str] = set()
    
    verifier = InstanceVerifier(region_info.id, nodes)
    thread1 = threading.Thread(
        target=verifier.describe_instances_loop, args=(client,))
    thread1.start()
    thread2 = threading.Thread(target=verifier.wait_for_ssh_loop)
    thread2.start()

    default_instance_type = instance_types[0]
    hosts_to_request = math.ceil(nodes / default_instance_type.nodes)
    if hosts_to_request <= provision_config.zone_max_nodes:
        blocked_zone_ids.update(_try_create_in_single_zone(
            client, verifier, cfg, region_info, default_instance_type, hosts_to_request))

    # 排列组合所有区域，可以在这里配置更复杂的尝试策略
    usable_zones = [zone for zone in region_info.zones.values() if zone.id not in blocked_zone_ids]
    if not usable_zones:
        logger.error(f"Region {region_info.id} has no usable zones for provisioning")
        verifier.stop()
        thread1.join()
        thread2.join()
        return []

    zone_plan = product(instance_types, usable_zones)

    instance_type, zone_info = _next_zone_plan_candidate(zone_plan, blocked_zone_ids)

    while True:
        rest_nodes = verifier.get_rest_nodes()
        if rest_nodes <= 0:
            logger.success(f"Region {region_info.id} launch complete")
            break

        # Compute how many hosts we actually need for this instance type
        hosts_to_request = math.ceil(rest_nodes / instance_type.nodes)
        if hosts_to_request <= 0:
            # nothing required
            break
        
        if provision_config.max_nodes > 0 and hosts_to_request > provision_config.max_nodes:
            hosts_to_request = provision_config.max_nodes

        instance_ids, err = _create_instances_in_zone_with_blocked_zone_tracking(
            client, blocked_zone_ids, cfg, region_info, zone_info, instance_type, hosts_to_request)
        
        if len(instance_ids) > 0:
            verifier.submit_pending_instances(instance_ids, instance_type, zone_info.id)

        if len(instance_ids) < hosts_to_request or err == CreateInstanceError.ZoneUnavailable:
            # 当前实例组合可用已经耗尽，尝试下一组
            try:
                instance_type, zone_info = _next_zone_plan_candidate(zone_plan, blocked_zone_ids)
            except StopIteration:
                # 全部实例组合耗尽，等待 pending 的结果
                rest_nodes = verifier.get_rest_nodes(wait_for_pendings=True)

                if rest_nodes > 0:
                    logger.error(
                        f"Cannot launch enough nodes at {region_info.id}, request {nodes}, actual {verifier.ready_nodes}")
                    logger.debug(f"Region {region_info.id} create_instance thread exit")
                break

    verifier.stop()
    thread1.join()
    thread2.join()

    ready_instances = verifier.copy_ready_instances()
    return [HostSpec(ip=ip,
                     private_ip=private_ip,
                     nodes_per_host=instance.type.nodes,
                     ssh_user=ssh_user,
                     ssh_key_path=region_info.key_path,
                     provider=provider,
                     region=region_info.id,
                     zone=instance.zone_id,
                     instance_id=instance.instance_id)
            for (instance, ip, private_ip) in ready_instances]


def _try_create_in_single_zone(client: IEcsClient, verifier: InstanceVerifier, cfg: InstanceConfig, region_info: RegionInfo, instance_type: InstanceType, amount: int):
    blocked_zone_ids: set[str] = set()
    for zone_info in region_info.zones.values():
        ids, err = _create_instances_in_zone_with_blocked_zone_tracking(
            client, blocked_zone_ids, cfg, region_info, zone_info, instance_type, amount)
        if len(ids) == 0:
            continue
        elif len(ids) < amount:
            # TODO: 关闭部分成功的 instance?
            logger.warning(
                f"Only partial create instance success, even if minimum required ({region_info.id}/{zone_info.id})")
        else:
            verifier.submit_pending_instances(ids, instance_type, zone_info.id)
            # 无论这些实例是否都成功，不会再走 create_in_single_zone 的逻辑
            return blocked_zone_ids

    return blocked_zone_ids
