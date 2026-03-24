import time
from concurrent.futures import ThreadPoolExecutor
from typing import Iterable, List, Optional, Set

from alibabacloud_vpc20160428.client import Client as VpcClient
from alibabacloud_vpc20160428.models import AddCommonBandwidthPackageIpRequest, AllocateEipAddressRequest, AllocateEipAddressRequestTag, AssociateEipAddressRequest, CreateCommonBandwidthPackageRequest, CreateCommonBandwidthPackageRequestTag, DeleteCommonBandwidthPackageRequest, DescribeCommonBandwidthPackagesRequest, DescribeEipAddressesRequest, RemoveCommonBandwidthPackageIpRequest, ReleaseEipAddressRequest, UnassociateEipAddressRequest
from loguru import logger

from ..create_instances.instance_config import DEFAULT_COMMON_TAG_KEY, DEFAULT_COMMON_TAG_VALUE, DEFAULT_USER_TAG_KEY, InstanceConfig
from utils.wait_until import WaitUntilTimeoutError, wait_until

ECS_INSTANCE_TYPE = "EcsInstance"
MAX_PARALLEL_EIP_OPS = 16


def _shared_bandwidth_name(cfg: InstanceConfig) -> str:
    if cfg.aliyun_shared_bandwidth_name:
        return cfg.aliyun_shared_bandwidth_name
    return f"{cfg.instance_name_prefix}-{cfg.user_tag_value}-shared-bw"


def _eip_name(cfg: InstanceConfig, instance_id: str) -> str:
    return f"{cfg.instance_name_prefix}-{instance_id}-eip"


def _shared_bandwidth_tags(cfg: InstanceConfig) -> list[CreateCommonBandwidthPackageRequestTag]:
    return [
        CreateCommonBandwidthPackageRequestTag(key=DEFAULT_COMMON_TAG_KEY, value=DEFAULT_COMMON_TAG_VALUE),
        CreateCommonBandwidthPackageRequestTag(key=DEFAULT_USER_TAG_KEY, value=cfg.user_tag_value),
        CreateCommonBandwidthPackageRequestTag(key="team", value="core"),
    ]


def _eip_tags(cfg: InstanceConfig) -> list[AllocateEipAddressRequestTag]:
    return [
        AllocateEipAddressRequestTag(key=DEFAULT_COMMON_TAG_KEY, value=DEFAULT_COMMON_TAG_VALUE),
        AllocateEipAddressRequestTag(key=DEFAULT_USER_TAG_KEY, value=cfg.user_tag_value),
        AllocateEipAddressRequestTag(key="team", value="core"),
    ]


def _iter_resource_tags(resource) -> list[tuple[str, str]]:
    candidates = []
    tags = getattr(resource, "tags", None)
    if tags is not None:
        nested = getattr(tags, "tag", None)
        if nested is not None:
            candidates = nested
        elif isinstance(tags, list):
            candidates = tags
    if not candidates:
        direct = getattr(resource, "tag", None)
        if direct is not None:
            candidates = direct

    result = []
    for tag in candidates or []:
        if isinstance(tag, dict):
            key = tag.get("key") or tag.get("tag_key")
            value = tag.get("value") or tag.get("tag_value")
        else:
            key = getattr(tag, "key", None) or getattr(tag, "tag_key", None)
            value = getattr(tag, "value", None) or getattr(tag, "tag_value", None)
        if key and value is not None:
            result.append((key, value))
    return result


def _matches_user_prefix(resource, user_prefix: str) -> bool:
    tags = dict(_iter_resource_tags(resource))
    if tags.get(DEFAULT_COMMON_TAG_KEY) == DEFAULT_COMMON_TAG_VALUE:
        return tags.get(DEFAULT_USER_TAG_KEY, "").startswith(user_prefix)

    name = getattr(resource, "name", None) or ""
    prefix = f"{InstanceConfig.instance_name_prefix}-{user_prefix}"
    if name.startswith(prefix):
        return True

    shared_bw_prefix = f"{InstanceConfig.instance_name_prefix}-"
    shared_bw_suffix = "-shared-bw"
    if name.startswith(shared_bw_prefix) and name.endswith(shared_bw_suffix):
        tagged_user = name[len(shared_bw_prefix):-len(shared_bw_suffix)]
        return tagged_user.startswith(user_prefix)

    return False


def _iter_all_common_bandwidth_packages(vpc_client: VpcClient, region_id: str, *, name: Optional[str] = None):
    page_number = 1
    while True:
        request_kwargs = {
            "region_id": region_id,
            "page_number": page_number,
            "page_size": 50,
        }
        if name is not None:
            request_kwargs["name"] = name
        resp = vpc_client.describe_common_bandwidth_packages(
            DescribeCommonBandwidthPackagesRequest(**request_kwargs)
        )
        packages = resp.body.common_bandwidth_packages.common_bandwidth_package or []
        for package in packages:
            yield package

        total_count = resp.body.total_count or 0
        if total_count <= page_number * 50:
            return
        page_number += 1


def _describe_eips(vpc_client: VpcClient, region_id: str, *, allocation_id: Optional[str] = None, associated_instance_id: Optional[str] = None, eip_name: Optional[str] = None):
    page_number = 1
    results = []
    while True:
        request_kwargs = {
            "region_id": region_id,
            "page_number": page_number,
            "page_size": 50,
        }
        if allocation_id is not None:
            request_kwargs["allocation_id"] = allocation_id
        if associated_instance_id is not None:
            request_kwargs["associated_instance_id"] = associated_instance_id
            request_kwargs["associated_instance_type"] = ECS_INSTANCE_TYPE
        if eip_name is not None:
            request_kwargs["eip_name"] = eip_name
        resp = vpc_client.describe_eip_addresses(
            DescribeEipAddressesRequest(**request_kwargs)
        )
        eips = resp.body.eip_addresses.eip_address or []
        results.extend(eips)

        total_count = resp.body.total_count or 0
        if total_count <= page_number * 50:
            return results
        page_number += 1


def _get_bandwidth_package(vpc_client: VpcClient, region_id: str, package_name: str):
    for package in _iter_all_common_bandwidth_packages(vpc_client, region_id, name=package_name):
        if package.name == package_name:
            return package
    return None


def _get_eip_by_allocation_id(vpc_client: VpcClient, region_id: str, allocation_id: str):
    eips = _describe_eips(vpc_client, region_id, allocation_id=allocation_id)
    return eips[0] if eips else None


def _get_instance_eip(vpc_client: VpcClient, region_id: str, instance_id: str):
    eips = _describe_eips(vpc_client, region_id, associated_instance_id=instance_id)
    return eips[0] if eips else None


def _collect_relevant_eips(
    vpc_client: VpcClient,
    region_id: str,
    cfg: InstanceConfig,
    instance_ids: Iterable[str],
) -> tuple[dict[str, object], dict[str, object]]:
    target_ids = set(instance_ids)
    if not target_ids:
        return {}, {}

    target_name_to_instance = {
        _eip_name(cfg, instance_id): instance_id for instance_id in target_ids
    }
    eips_by_instance: dict[str, object] = {}
    eips_by_name: dict[str, object] = {}

    for eip in _describe_eips(vpc_client, region_id):
        attached_instance_id = getattr(eip, "instance_id", None)
        if attached_instance_id in target_ids and attached_instance_id not in eips_by_instance:
            eips_by_instance[attached_instance_id] = eip
            continue

        name = getattr(eip, "name", None)
        if not name:
            continue

        target_instance_id = target_name_to_instance.get(name)
        if target_instance_id is not None and target_instance_id not in eips_by_name:
            eips_by_name[target_instance_id] = eip

    return eips_by_instance, eips_by_name


def _collect_instance_release_eips(
    vpc_client: VpcClient,
    region_id: str,
    instance_ids: Iterable[str],
) -> dict[str, object]:
    target_ids = set(instance_ids)
    if not target_ids:
        return {}

    suffix_to_instance = {
        f"-{instance_id}-eip": instance_id for instance_id in target_ids
    }
    eips_by_instance: dict[str, object] = {}

    for eip in _describe_eips(vpc_client, region_id):
        attached_instance_id = getattr(eip, "instance_id", None)
        if attached_instance_id in target_ids and attached_instance_id not in eips_by_instance:
            eips_by_instance[attached_instance_id] = eip
            continue

        name = getattr(eip, "name", None)
        if not name:
            continue

        for suffix, instance_id in suffix_to_instance.items():
            if name.endswith(suffix) and instance_id not in eips_by_instance:
                eips_by_instance[instance_id] = eip
                break

    return eips_by_instance


def _release_eips_in_parallel(
    vpc_client: VpcClient,
    region_id: str,
    eips: Iterable[object],
    context_builder,
) -> int:
    target_eips = [eip for eip in eips if getattr(eip, "allocation_id", None)]
    if not target_eips:
        return 0

    max_workers = min(MAX_PARALLEL_EIP_OPS, len(target_eips))
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        results = list(executor.map(
            lambda eip: _release_eip_resource(
                vpc_client,
                region_id,
                eip,
                context_builder(eip),
            ),
            target_eips,
        ))
    return sum(1 for released in results if released)


def _delete_shared_bandwidth_packages_in_parallel(
    vpc_client: VpcClient,
    region_id: str,
    packages: Iterable[object],
    user_prefix: str,
) -> int:
    target_packages = [
        package for package in packages
        if getattr(package, "bandwidth_package_id", None)
    ]
    if not target_packages:
        return 0

    def _delete_package(package) -> bool:
        package_id = getattr(package, "bandwidth_package_id", None)
        if not package_id:
            return False
        try:
            vpc_client.delete_common_bandwidth_package(
                DeleteCommonBandwidthPackageRequest(
                    region_id=region_id,
                    bandwidth_package_id=package_id,
                )
            )
            logger.info(
                f"Deleted Aliyun shared bandwidth package {package_id} in {region_id} for user-prefix {user_prefix}"
            )
            return True
        except Exception as exc:
            logger.warning(
                f"Failed to delete Aliyun shared bandwidth package {package_id} in {region_id}: {exc}"
            )
            return False

    max_workers = min(MAX_PARALLEL_EIP_OPS, len(target_packages))
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        results = list(executor.map(_delete_package, target_packages))
    return sum(1 for deleted in results if deleted)


def get_eip_public_ip_map(vpc_client: VpcClient, region_id: str, instance_ids: Iterable[str]) -> dict[str, str]:
    target_ids = set(instance_ids)
    if not target_ids:
        return {}

    page_number = 1
    mapping: dict[str, str] = {}
    while True:
        resp = vpc_client.describe_eip_addresses(
            DescribeEipAddressesRequest(
                region_id=region_id,
                page_number=page_number,
                page_size=50,
            )
        )
        eips = resp.body.eip_addresses.eip_address or []
        for eip in eips:
            if eip.instance_id in target_ids and eip.ip_address:
                mapping[eip.instance_id] = eip.ip_address

        total_count = resp.body.total_count or 0
        if total_count <= page_number * 50 or len(mapping) == len(target_ids):
            return mapping
        page_number += 1


def _wait_for_eip(vpc_client: VpcClient, region_id: str, allocation_id: str, predicate, description: str):
    try:
        wait_until(
            lambda: predicate(_get_eip_by_allocation_id(vpc_client, region_id, allocation_id)),
            timeout=120,
            retry_interval=2,
        )
    except WaitUntilTimeoutError as exc:
        raise RuntimeError(f"Timeout waiting for EIP {allocation_id}: {description}") from exc


def _wait_for_eip_status(vpc_client: VpcClient, region_id: str, allocation_id: str, statuses: set[str], description: str):
    _wait_for_eip(
        vpc_client,
        region_id,
        allocation_id,
        lambda eip: eip is not None and eip.status in statuses,
        description,
    )


def ensure_common_bandwidth_package(vpc_client: VpcClient, region_id: str, cfg: InstanceConfig) -> str:
    package_name = _shared_bandwidth_name(cfg)
    package = _get_bandwidth_package(vpc_client, region_id, package_name)
    if package is not None:
        if not package.bandwidth_package_id:
            raise RuntimeError(f"Aliyun shared bandwidth package {package_name} in {region_id} has no package id")
        logger.info(f"Reuse Aliyun shared bandwidth package in {region_id}: {package_name} ({package.bandwidth_package_id})")
        return package.bandwidth_package_id

    req = CreateCommonBandwidthPackageRequest(
        region_id=region_id,
        name=package_name,
        description=package_name,
        bandwidth=cfg.aliyun_shared_bandwidth_mbps,
        isp=cfg.aliyun_shared_bandwidth_isp,
        internet_charge_type="PayByTraffic",
        tag=_shared_bandwidth_tags(cfg),
    )
    resp = vpc_client.create_common_bandwidth_package(req)
    package_id = resp.body.bandwidth_package_id
    if not package_id:
        raise RuntimeError(f"Aliyun shared bandwidth package creation returned empty package id in {region_id}")
    logger.success(f"Created Aliyun shared bandwidth package in {region_id}: {package_name} ({package_id})")
    return package_id


def _allocate_eip(vpc_client: VpcClient, region_id: str, cfg: InstanceConfig, instance_id: str) -> str:
    req = AllocateEipAddressRequest(
        region_id=region_id,
        name=_eip_name(cfg, instance_id),
        description=f"{cfg.instance_name_prefix}-{instance_id}",
        isp=cfg.aliyun_shared_bandwidth_isp,
        bandwidth=str(cfg.internet_max_bandwidth_out),
        internet_charge_type=cfg.aliyun_eip_internet_charge_type,
        instance_charge_type="PostPaid",
        tag=_eip_tags(cfg),
    )
    resp = vpc_client.allocate_eip_address(req)
    allocation_id = resp.body.allocation_id
    if not allocation_id:
        raise RuntimeError(f"Aliyun EIP allocation returned empty allocation id for {instance_id} in {region_id}")
    logger.info(f"Allocated Aliyun EIP for {instance_id} in {region_id}: {allocation_id}")
    return allocation_id


def _ensure_instance_eip(
    vpc_client: VpcClient,
    region_id: str,
    cfg: InstanceConfig,
    instance_id: str,
    *,
    current_eip=None,
    named_eip=None,
) -> str:
    current = current_eip if current_eip is not None else _get_instance_eip(vpc_client, region_id, instance_id)
    if current is not None:
        return current.allocation_id

    created_now = False
    if named_eip is not None:
        allocation_id = named_eip.allocation_id
    else:
        existing_by_name = _describe_eips(vpc_client, region_id, eip_name=_eip_name(cfg, instance_id))
        if existing_by_name:
            allocation_id = existing_by_name[0].allocation_id
        else:
            allocation_id = _allocate_eip(vpc_client, region_id, cfg, instance_id)
            created_now = True

    try:
        vpc_client.associate_eip_address(
            AssociateEipAddressRequest(
                region_id=region_id,
                allocation_id=allocation_id,
                instance_id=instance_id,
                instance_type=ECS_INSTANCE_TYPE,
            )
        )
    except Exception:
        if created_now:
            try:
                vpc_client.release_eip_address(
                    ReleaseEipAddressRequest(region_id=region_id, allocation_id=allocation_id)
                )
            except Exception as release_exc:
                logger.warning(f"Failed to release EIP {allocation_id} after associate error: {release_exc}")
        raise

    _wait_for_eip(
        vpc_client,
        region_id,
        allocation_id,
        lambda eip: eip is not None and eip.instance_id == instance_id,
        f"association to instance {instance_id}",
    )
    logger.info(f"Associated Aliyun EIP {allocation_id} with instance {instance_id} in {region_id}")
    return allocation_id


def _attach_eip_to_shared_bandwidth_package(
    vpc_client: VpcClient,
    region_id: str,
    package_id: str,
    eip,
) -> str:
    allocation_id = getattr(eip, "allocation_id", None)
    if not allocation_id:
        raise RuntimeError(f"Aliyun EIP in {region_id} is missing allocation id")

    if getattr(eip, "bandwidth_package_id", None) == package_id:
        return allocation_id

    vpc_client.add_common_bandwidth_package_ip(
        AddCommonBandwidthPackageIpRequest(
            region_id=region_id,
            bandwidth_package_id=package_id,
            ip_instance_id=allocation_id,
            ip_type="Eip",
        )
    )
    _wait_for_eip(
        vpc_client,
        region_id,
        allocation_id,
        lambda current: current is not None and current.bandwidth_package_id == package_id,
        f"join shared bandwidth package {package_id}",
    )
    logger.info(f"Added EIP {allocation_id} to shared bandwidth package {package_id} in {region_id}")
    return allocation_id


def ensure_instance_public_network(vpc_client: VpcClient, region_id: str, cfg: InstanceConfig, instance_ids: Iterable[str]) -> list[str]:
    if not cfg.use_aliyun_eip:
        return []

    unique_instance_ids = list(dict.fromkeys(instance_ids))
    if not unique_instance_ids:
        return []

    package_id = ensure_common_bandwidth_package(vpc_client, region_id, cfg)
    eips_by_instance, eips_by_name = _collect_relevant_eips(
        vpc_client,
        region_id,
        cfg,
        unique_instance_ids,
    )

    max_workers = min(MAX_PARALLEL_EIP_OPS, len(unique_instance_ids))
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        allocation_ids = list(executor.map(
            lambda instance_id: _ensure_instance_eip(
                vpc_client,
                region_id,
                cfg,
                instance_id,
                current_eip=eips_by_instance.get(instance_id),
                named_eip=eips_by_name.get(instance_id),
            ),
            unique_instance_ids,
        ))

    refreshed_eips_by_instance, _ = _collect_relevant_eips(
        vpc_client,
        region_id,
        cfg,
        unique_instance_ids,
    )
    missing_instances = [
        instance_id for instance_id in unique_instance_ids
        if instance_id not in refreshed_eips_by_instance
    ]
    if missing_instances:
        raise RuntimeError(
            f"Cannot find Aliyun EIPs after association in {region_id}: {missing_instances}"
        )

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        list(executor.map(
            lambda instance_id: _attach_eip_to_shared_bandwidth_package(
                vpc_client,
                region_id,
                package_id,
                refreshed_eips_by_instance[instance_id],
            ),
            unique_instance_ids,
        ))

    return allocation_ids


def _release_eip_resource(vpc_client: VpcClient, region_id: str, eip, context: str) -> bool:
    allocation_id = getattr(eip, "allocation_id", None)
    if not allocation_id:
        return False

    try:
        if eip.bandwidth_package_id:
            vpc_client.remove_common_bandwidth_package_ip(
                RemoveCommonBandwidthPackageIpRequest(
                    region_id=region_id,
                    bandwidth_package_id=eip.bandwidth_package_id,
                    ip_instance_id=allocation_id,
                )
            )
            _wait_for_eip(
                vpc_client,
                region_id,
                allocation_id,
                lambda current: current is not None and not current.bandwidth_package_id,
                f"leave shared bandwidth package for {context}",
            )

        if eip.instance_id:
            vpc_client.unassociate_eip_address(
                UnassociateEipAddressRequest(
                    region_id=region_id,
                    allocation_id=allocation_id,
                    instance_id=eip.instance_id,
                    instance_type=ECS_INSTANCE_TYPE,
                    force=True,
                )
            )
            _wait_for_eip(
                vpc_client,
                region_id,
                allocation_id,
                lambda current: current is not None and not current.instance_id,
                f"unassociation from {context}",
            )
        else:
            _wait_for_eip_status(
                vpc_client,
                region_id,
                allocation_id,
                {"Available"},
                f"become releasable for {context}",
            )

        vpc_client.release_eip_address(
            ReleaseEipAddressRequest(region_id=region_id, allocation_id=allocation_id)
        )
        logger.info(f"Released Aliyun EIP {allocation_id} in {region_id} for {context}")
        return True
    except Exception as exc:
        logger.warning(f"Failed to release Aliyun EIP {allocation_id} in {region_id} for {context}: {exc}")
        return False


def release_instance_public_network(vpc_client: VpcClient, region_id: str, instance_ids: Iterable[str]):
    unique_instance_ids = list(dict.fromkeys(instance_ids))
    eips_by_instance = _collect_instance_release_eips(vpc_client, region_id, unique_instance_ids)
    _release_eips_in_parallel(
        vpc_client,
        region_id,
        eips_by_instance.values(),
        lambda eip: f"instance {getattr(eip, 'instance_id', None) or getattr(eip, 'name', 'unknown')}",
    )


def cleanup_user_public_network_artifacts(vpc_client: VpcClient, region_id: str, user_prefix: str) -> tuple[int, int]:
    target_eips = [
        eip for eip in _describe_eips(vpc_client, region_id)
        if _matches_user_prefix(eip, user_prefix)
    ]
    released_eips = _release_eips_in_parallel(
        vpc_client,
        region_id,
        target_eips,
        lambda eip: f"user-prefix {user_prefix}",
    )

    target_packages = [
        package for package in _iter_all_common_bandwidth_packages(vpc_client, region_id)
        if _matches_user_prefix(package, user_prefix)
    ]
    deleted_packages = _delete_shared_bandwidth_packages_in_parallel(
        vpc_client,
        region_id,
        target_packages,
        user_prefix,
    )

    return released_eips, deleted_packages


def collect_user_tags_for_instances(vpc_client: VpcClient, region_id: str, package_names: Set[str]) -> Set[str]:
    existing = set()
    for package_name in package_names:
        package = _get_bandwidth_package(vpc_client, region_id, package_name)
        if package is not None:
            existing.add(package_name)
    return existing