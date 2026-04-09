from concurrent.futures import ThreadPoolExecutor
from typing import Iterable, Optional

from loguru import logger
from tencentcloud.common.exception.tencent_cloud_sdk_exception import TencentCloudSDKException
from tencentcloud.vpc.v20170312 import models as vpc_models
from tencentcloud.vpc.v20170312.vpc_client import VpcClient

from ..create_instances.instance_config import DEFAULT_COMMON_TAG_KEY, DEFAULT_COMMON_TAG_VALUE, DEFAULT_USER_TAG_KEY, InstanceConfig
from .retry import call_with_retry
from utils.wait_until import WaitUntilTimeoutError, wait_until

BOUND_STATUSES = {"BIND", "BIND_ENI"}
TRANSITIONAL_STATUSES = {"CREATING", "BINDING", "UNBINDING"}
UNBOUND_STATUS = "UNBIND"
MAX_PARALLEL_EIP_OPS = 8


def _chunks(values: Iterable[str], size: int):
    items = list(values)
    for index in range(0, len(items), size):
        yield items[index:index + size]


def _address_name(cfg: InstanceConfig, instance_id: str) -> str:
    return f"{cfg.instance_name_prefix}-{instance_id}-eip"


def _address_tags(cfg: InstanceConfig) -> list[vpc_models.Tag]:
    common_tag = vpc_models.Tag()
    common_tag.Key = DEFAULT_COMMON_TAG_KEY
    common_tag.Value = DEFAULT_COMMON_TAG_VALUE

    user_tag = vpc_models.Tag()
    user_tag.Key = DEFAULT_USER_TAG_KEY
    user_tag.Value = cfg.user_tag_value

    return [common_tag, user_tag]


def _make_filter(name: str, values: list[str]) -> vpc_models.Filter:
    address_filter = vpc_models.Filter()
    address_filter.Name = name
    address_filter.Values = values
    return address_filter


def _describe_addresses(
    vpc_client: VpcClient,
    *,
    filters: Optional[list[vpc_models.Filter]] = None,
    address_ids: Optional[list[str]] = None,
) -> list[vpc_models.Address]:
    results: list[vpc_models.Address] = []
    offset = 0
    limit = 100

    while True:
        req = vpc_models.DescribeAddressesRequest()
        req.Offset = offset
        req.Limit = limit
        if filters:
            req.Filters = filters
        if address_ids:
            req.AddressIds = address_ids

        resp = call_with_retry(
            lambda: vpc_client.DescribeAddresses(req),
            action="Describe Tencent EIPs",
        )
        if resp.AddressSet:
            results.extend(resp.AddressSet)

        if resp.TotalCount is None or resp.TotalCount <= offset + limit:
            return results
        offset += limit


def _describe_tagged_eips(vpc_client: VpcClient) -> list[vpc_models.Address]:
    return _describe_addresses(
        vpc_client,
        filters=[
            _make_filter("address-type", ["EIP"]),
            _make_filter(f"tag:{DEFAULT_COMMON_TAG_KEY}", [DEFAULT_COMMON_TAG_VALUE]),
        ],
    )


def _get_eip_by_id(vpc_client: VpcClient, address_id: str) -> Optional[vpc_models.Address]:
    addresses = _describe_addresses(vpc_client, address_ids=[address_id])
    return addresses[0] if addresses else None


def _collect_relevant_eips(
    vpc_client: VpcClient,
    cfg: InstanceConfig,
    instance_ids: Iterable[str],
) -> tuple[dict[str, vpc_models.Address], dict[str, vpc_models.Address]]:
    target_ids = set(instance_ids)
    if not target_ids:
        return {}, {}

    eips_by_instance: dict[str, vpc_models.Address] = {}
    for chunk in _chunks(sorted(target_ids), 100):
        for address in _describe_addresses(
            vpc_client,
            filters=[
                _make_filter("instance-id", chunk),
                _make_filter("address-type", ["EIP"]),
            ],
        ):
            if address.InstanceId and address.InstanceId not in eips_by_instance:
                eips_by_instance[address.InstanceId] = address

    target_names = {
        _address_name(cfg, instance_id): instance_id for instance_id in target_ids
    }
    eips_by_name: dict[str, vpc_models.Address] = {}
    for address in _describe_tagged_eips(vpc_client):
        address_name = address.AddressName or ""
        instance_id = target_names.get(address_name)
        if instance_id is not None and instance_id not in eips_by_name:
            eips_by_name[instance_id] = address

    return eips_by_instance, eips_by_name


def _iter_resource_tags(address: vpc_models.Address) -> list[tuple[str, str]]:
    result: list[tuple[str, str]] = []
    for tag in address.TagSet or []:
        key = getattr(tag, "Key", None)
        value = getattr(tag, "Value", None)
        if key and value is not None:
            result.append((key, value))
    return result


def _matches_user_prefix(address: vpc_models.Address, user_prefix: str) -> bool:
    tags = dict(_iter_resource_tags(address))
    if tags.get(DEFAULT_COMMON_TAG_KEY) != DEFAULT_COMMON_TAG_VALUE:
        return False
    return tags.get(DEFAULT_USER_TAG_KEY, "").startswith(user_prefix)


def _wait_for_eip(vpc_client: VpcClient, address_id: str, predicate, description: str):
    def _ready() -> bool:
        return predicate(_get_eip_by_id(vpc_client, address_id))

    try:
        wait_until(_ready, timeout=300, retry_interval=3)
    except WaitUntilTimeoutError as exc:
        raise RuntimeError(f"Timed out waiting for {description}: {address_id}") from exc


def _wait_for_eip_status(vpc_client: VpcClient, address_id: str, statuses: set[str], description: str):
    _wait_for_eip(
        vpc_client,
        address_id,
        lambda address: address is not None and (address.AddressStatus or "") in statuses,
        description,
    )


def _allocate_eip(vpc_client: VpcClient, cfg: InstanceConfig, instance_id: str) -> str:
    req = vpc_models.AllocateAddressesRequest()
    req.AddressCount = 1
    req.InternetChargeType = "TRAFFIC_POSTPAID_BY_HOUR"
    req.InternetMaxBandwidthOut = cfg.internet_max_bandwidth_out
    req.AddressName = _address_name(cfg, instance_id)
    req.Tags = _address_tags(cfg)

    resp = call_with_retry(
        lambda: vpc_client.AllocateAddresses(req),
        action=f"Allocate Tencent EIP for {instance_id}",
    )
    address_ids = resp.AddressSet or []
    if not address_ids:
        raise RuntimeError(f"AllocateAddresses returned no address for {instance_id}")

    address_id = address_ids[0]
    _wait_for_eip_status(vpc_client, address_id, {UNBOUND_STATUS}, "Tencent EIP allocation")
    return address_id


def _ensure_instance_eip(
    vpc_client: VpcClient,
    cfg: InstanceConfig,
    instance_id: str,
    eips_by_instance: dict[str, vpc_models.Address],
    eips_by_name: dict[str, vpc_models.Address],
) -> str:
    current = eips_by_instance.get(instance_id) or eips_by_name.get(instance_id)
    if current is not None and current.InstanceId and current.InstanceId != instance_id:
        current = None

    if current is None:
        address_id = _allocate_eip(vpc_client, cfg, instance_id)
    else:
        address_id = current.AddressId
        if not address_id:
            raise RuntimeError(f"Existing Tencent EIP for {instance_id} has no AddressId")

    latest = _get_eip_by_id(vpc_client, address_id)
    if latest is None:
        raise RuntimeError(f"Tencent EIP disappeared before bind: {address_id}")

    if latest.InstanceId == instance_id and (latest.AddressStatus or "") in BOUND_STATUSES and latest.AddressIp:
        return address_id

    if latest.InstanceId and latest.InstanceId != instance_id:
        address_id = _allocate_eip(vpc_client, cfg, instance_id)
        latest = _get_eip_by_id(vpc_client, address_id)
        if latest is None:
            raise RuntimeError(f"Allocated Tencent EIP disappeared before bind: {address_id}")

    if (latest.AddressStatus or "") in TRANSITIONAL_STATUSES:
        _wait_for_eip_status(vpc_client, address_id, {UNBOUND_STATUS}, "Tencent EIP stable state")
        latest = _get_eip_by_id(vpc_client, address_id)
        if latest is None:
            raise RuntimeError(f"Tencent EIP disappeared before bind: {address_id}")

    if latest.InstanceId == instance_id and (latest.AddressStatus or "") in BOUND_STATUSES:
        _wait_for_eip(
            vpc_client,
            address_id,
            lambda address: address is not None and address.InstanceId == instance_id and (address.AddressStatus or "") in BOUND_STATUSES and bool(address.AddressIp),
            "Tencent EIP bound public IP",
        )
        return address_id

    req = vpc_models.AssociateAddressRequest()
    req.AddressId = address_id
    req.InstanceId = instance_id
    call_with_retry(
        lambda: vpc_client.AssociateAddress(req),
        action=f"Associate Tencent EIP {address_id} to {instance_id}",
    )
    _wait_for_eip(
        vpc_client,
        address_id,
        lambda address: address is not None and address.InstanceId == instance_id and (address.AddressStatus or "") in BOUND_STATUSES and bool(address.AddressIp),
        f"Tencent EIP binding to {instance_id}",
    )
    return address_id


def ensure_instance_public_network(vpc_client: VpcClient, cfg: InstanceConfig, instance_ids: Iterable[str]) -> list[str]:
    target_ids = list(instance_ids)
    if not target_ids:
        return []

    eips_by_instance, eips_by_name = _collect_relevant_eips(vpc_client, cfg, target_ids)
    max_workers = min(MAX_PARALLEL_EIP_OPS, len(target_ids))
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        return list(executor.map(
            lambda instance_id: _ensure_instance_eip(vpc_client, cfg, instance_id, eips_by_instance, eips_by_name),
            target_ids,
        ))


def get_eip_public_ip_map(vpc_client: VpcClient, instance_ids: Iterable[str]) -> dict[str, str]:
    result: dict[str, str] = {}
    for chunk in _chunks(list(instance_ids), 100):
        for address in _describe_addresses(
            vpc_client,
            filters=[
                _make_filter("instance-id", chunk),
                _make_filter("address-type", ["EIP"]),
            ],
        ):
            if address.InstanceId and address.AddressIp and (address.AddressStatus or "") in BOUND_STATUSES:
                result[address.InstanceId] = address.AddressIp
    return result


def _is_not_found(exc: TencentCloudSDKException) -> bool:
    return "NotFound" in (exc.code or "")


def _release_eip_resource(vpc_client: VpcClient, address: vpc_models.Address, context: str) -> bool:
    address_id = address.AddressId
    if not address_id:
        return False

    current = _get_eip_by_id(vpc_client, address_id)
    if current is None:
        return False

    if (current.AddressStatus or "") in TRANSITIONAL_STATUSES:
        _wait_for_eip(
            vpc_client,
            address_id,
            lambda latest: latest is None or (latest.AddressStatus or "") not in TRANSITIONAL_STATUSES,
            f"{context} stable state",
        )
        current = _get_eip_by_id(vpc_client, address_id)
        if current is None:
            return True

    if (current.AddressStatus or "") == "OFFLINING":
        _wait_for_eip(vpc_client, address_id, lambda latest: latest is None, f"{context} release")
        return True

    if current.InstanceId or (current.AddressStatus or "") in BOUND_STATUSES:
        req = vpc_models.DisassociateAddressRequest()
        req.AddressId = address_id
        req.ReallocateNormalPublicIp = False
        try:
            call_with_retry(
                lambda: vpc_client.DisassociateAddress(req),
                action=f"Disassociate Tencent EIP {address_id}",
            )
        except TencentCloudSDKException as exc:
            if not _is_not_found(exc):
                raise
        _wait_for_eip_status(vpc_client, address_id, {UNBOUND_STATUS}, f"{context} unbind")
        current = _get_eip_by_id(vpc_client, address_id)
        if current is None:
            return True

    if (current.AddressStatus or "") == UNBOUND_STATUS or not current.InstanceId:
        req = vpc_models.ReleaseAddressesRequest()
        req.AddressIds = [address_id]
        try:
            call_with_retry(
                lambda: vpc_client.ReleaseAddresses(req),
                action=f"Release Tencent EIP {address_id}",
            )
        except TencentCloudSDKException as exc:
            if not _is_not_found(exc):
                raise
        _wait_for_eip(vpc_client, address_id, lambda latest: latest is None, f"{context} release")
        return True

    logger.warning(f"Skip unexpected Tencent EIP state for {context}: {current.AddressStatus}")
    return False


def release_instance_public_network(vpc_client: VpcClient, cfg: InstanceConfig, instance_ids: Iterable[str]) -> int:
    eips_by_instance, eips_by_name = _collect_relevant_eips(vpc_client, cfg, instance_ids)
    unique_eips: dict[str, vpc_models.Address] = {}
    for address in list(eips_by_instance.values()) + list(eips_by_name.values()):
        if address.AddressId:
            unique_eips[address.AddressId] = address

    targets = list(unique_eips.values())
    if not targets:
        return 0

    max_workers = min(MAX_PARALLEL_EIP_OPS, len(targets))
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        results = list(executor.map(
            lambda address: _release_eip_resource(
                vpc_client,
                address,
                f"Tencent EIP {address.AddressId} for {address.InstanceId or address.AddressName or 'unknown'}",
            ),
            targets,
        ))
    return sum(1 for released in results if released)


def cleanup_user_public_network_artifacts(vpc_client: VpcClient, user_prefix: str) -> int:
    targets = [
        address for address in _describe_tagged_eips(vpc_client)
        if _matches_user_prefix(address, user_prefix)
    ]
    if not targets:
        return 0

    max_workers = min(MAX_PARALLEL_EIP_OPS, len(targets))
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        results = list(executor.map(
            lambda address: _release_eip_resource(
                vpc_client,
                address,
                f"Tencent cleanup EIP {address.AddressId} ({address.AddressName or 'unnamed'})",
            ),
            targets,
        ))
    return sum(1 for released in results if released)