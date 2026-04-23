import hashlib
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from threading import Lock
from typing import Callable, Iterable, Optional, TypeVar

from Tea.exceptions import TeaException
from alibabacloud_vpc20160428.client import Client as VpcClient
from alibabacloud_vpc20160428.models import AllocateEipAddressRequest, AllocateEipAddressRequestTag, AssociateEipAddressRequest, DescribeEipAddressesRequest, ReleaseEipAddressRequest, UnassociateEipAddressRequest
from loguru import logger

from ..create_instances.instance_config import DEFAULT_COMMON_TAG_KEY, DEFAULT_COMMON_TAG_VALUE, DEFAULT_USER_TAG_KEY, InstanceConfig

ECS_INSTANCE_TYPE = "EcsInstance"
MAX_PARALLEL_EIP_OPS = 16
EIP_DESCRIBE_PAGE_SIZE = 100
EIP_MAX_ALLOCATION_IDS_PER_DESCRIBE = 50
EIP_WAIT_TIMEOUT_SECONDS = 120
EIP_WAIT_RETRY_INTERVAL_SECONDS = 2
EIP_REQUEST_RETRY_ATTEMPTS = 3
EIP_REQUEST_RETRY_BASE_DELAY_SECONDS = 1.0

_T = TypeVar("_T")


class _SlidingWindowRateLimiter:
    def __init__(self, max_calls: int, window_seconds: float):
        self.max_calls = max_calls
        self.window_seconds = window_seconds
        self._lock = Lock()
        self._timestamps: deque[float] = deque()

    def acquire(self):
        while True:
            now = time.monotonic()
            with self._lock:
                cutoff = now - self.window_seconds
                while self._timestamps and self._timestamps[0] <= cutoff:
                    self._timestamps.popleft()
                if len(self._timestamps) < self.max_calls:
                    self._timestamps.append(now)
                    return
                sleep_for = self._timestamps[0] + self.window_seconds - now

            if sleep_for > 0:
                time.sleep(sleep_for)


@dataclass(frozen=True)
class _EnsureInstanceEipResult:
    instance_id: str
    allocation_id: str
    association_pending: bool


_EIP_RATE_LIMITERS = {
    "describe_eip_addresses": _SlidingWindowRateLimiter(600, 60),
    "allocate_eip_address": _SlidingWindowRateLimiter(120, 60),
    "associate_eip_address": _SlidingWindowRateLimiter(120, 60),
    "release_eip_address": _SlidingWindowRateLimiter(600, 60),
    "unassociate_eip_address": _SlidingWindowRateLimiter(120, 60),
}


def _chunked(values: Iterable[str], chunk_size: int):
    chunk: list[str] = []
    for value in values:
        if not value:
            continue
        chunk.append(value)
        if len(chunk) == chunk_size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


def _eip_client_token(prefix: str, *parts: str) -> str:
    payload = "|".join(str(part) for part in parts)
    digest = hashlib.sha1(payload.encode("utf-8")).hexdigest()
    return f"{prefix}-{digest[:24]}"


def _extract_eip_status_code(exc: Exception) -> Optional[int]:
    for attr in ("statusCode", "status_code"):
        value = getattr(exc, attr, None)
        if value is not None:
            try:
                return int(value)
            except (TypeError, ValueError):
                return None

    data = getattr(exc, "data", None)
    if isinstance(data, dict):
        value = data.get("statusCode") or data.get("status_code")
        if value is not None:
            try:
                return int(value)
            except (TypeError, ValueError):
                return None

    return None


def _extract_eip_error_code(exc: Exception) -> str:
    code = getattr(exc, "code", None)
    if code is None:
        data = getattr(exc, "data", None)
        if isinstance(data, dict):
            code = data.get("Code") or data.get("code")
    return str(code or "").lower()


def _is_retryable_eip_exception(exc: Exception) -> bool:
    if isinstance(exc, (ConnectionError, TimeoutError)):
        return True

    if isinstance(exc, TeaException):
        status_code = _extract_eip_status_code(exc)
        if status_code in {429, 500, 502, 503, 504}:
            return True

    status_code = _extract_eip_status_code(exc)
    if status_code in {429, 500, 502, 503, 504}:
        return True

    code = _extract_eip_error_code(exc)
    message = str(getattr(exc, "message", None) or exc).lower()
    retry_tokens = (
        "throttl",
        "requestlimit",
        "ratelimit",
        "too many request",
        "serviceunavailable",
        "service unavailable",
        "internalerror",
        "internalservererror",
        "timeout",
        "temporar",
        "system busy",
        "try again",
    )
    return any(token in code or token in message for token in retry_tokens)


def _call_eip_api(operation: str, request: Callable[[], _T], *, context: str) -> _T:
    delay = EIP_REQUEST_RETRY_BASE_DELAY_SECONDS
    for attempt in range(1, EIP_REQUEST_RETRY_ATTEMPTS + 1):
        _EIP_RATE_LIMITERS[operation].acquire()
        try:
            return request()
        except Exception as exc:
            if attempt >= EIP_REQUEST_RETRY_ATTEMPTS or not _is_retryable_eip_exception(exc):
                raise
            logger.warning(
                f"Aliyun EIP {operation} failed for {context} on attempt {attempt}/{EIP_REQUEST_RETRY_ATTEMPTS}: {exc}; retrying in {delay:.1f}s"
            )
            time.sleep(delay)
            delay *= 2

    raise RuntimeError(f"Unreachable while calling Aliyun EIP API {operation}")


def _eip_name(cfg: InstanceConfig, instance_id: str) -> str:
    return f"{cfg.instance_name_prefix}-{instance_id}-eip"


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

    return False


def _describe_eips(vpc_client: VpcClient, region_id: str, *, allocation_id: Optional[str] = None, associated_instance_id: Optional[str] = None, eip_name: Optional[str] = None):
    page_number = 1
    results = []
    while True:
        request_kwargs = {
            "region_id": region_id,
            "page_number": page_number,
            "page_size": EIP_DESCRIBE_PAGE_SIZE,
        }
        if allocation_id is not None:
            request_kwargs["allocation_id"] = allocation_id
        if associated_instance_id is not None:
            request_kwargs["associated_instance_id"] = associated_instance_id
            request_kwargs["associated_instance_type"] = ECS_INSTANCE_TYPE
        if eip_name is not None:
            request_kwargs["eip_name"] = eip_name
        request = DescribeEipAddressesRequest(**request_kwargs)
        resp = _call_eip_api(
            "describe_eip_addresses",
            lambda request=request: vpc_client.describe_eip_addresses(request),
            context=f"describe region={region_id} allocation_id={allocation_id or '-'} instance_id={associated_instance_id or '-'} eip_name={eip_name or '-'} page={page_number}",
        )
        eip_addresses = getattr(resp.body, "eip_addresses", None)
        eips = getattr(eip_addresses, "eip_address", None) or []
        results.extend(eips)

        total_count = resp.body.total_count or 0
        if total_count <= page_number * EIP_DESCRIBE_PAGE_SIZE:
            return results
        page_number += 1


def _describe_eips_by_allocation_ids(vpc_client: VpcClient, region_id: str, allocation_ids: Iterable[str]) -> dict[str, object]:
    eips_by_allocation_id: dict[str, object] = {}
    for chunk in _chunked(dict.fromkeys(allocation_ids), EIP_MAX_ALLOCATION_IDS_PER_DESCRIBE):
        eips = _describe_eips(vpc_client, region_id, allocation_id=",".join(chunk))
        for eip in eips:
            current_allocation_id = getattr(eip, "allocation_id", None)
            if current_allocation_id:
                eips_by_allocation_id[current_allocation_id] = eip
    return eips_by_allocation_id


def _poll_eips(
    vpc_client: VpcClient,
    region_id: str,
    allocation_ids: Iterable[str],
    pending_builder: Callable[[dict[str, object]], list[str]],
) -> tuple[dict[str, object], list[str]]:
    tracked_ids = list(dict.fromkeys(allocation_ids))
    if not tracked_ids:
        return {}, []

    deadline = time.monotonic() + EIP_WAIT_TIMEOUT_SECONDS
    while True:
        current = _describe_eips_by_allocation_ids(vpc_client, region_id, tracked_ids)
        pending = pending_builder(current)
        if not pending or time.monotonic() >= deadline:
            return current, pending
        time.sleep(min(EIP_WAIT_RETRY_INTERVAL_SECONDS, max(0.0, deadline - time.monotonic())))


def _wait_for_eip_associations(vpc_client: VpcClient, region_id: str, allocation_to_instance: dict[str, str]):
    current, pending = _poll_eips(
        vpc_client,
        region_id,
        allocation_to_instance.keys(),
        lambda observed: [
            f"{allocation_id}->{instance_id}"
            for allocation_id, instance_id in allocation_to_instance.items()
            if getattr(observed.get(allocation_id), "instance_id", None) != instance_id
        ],
    )
    if pending:
        raise RuntimeError(
            f"Timeout waiting for Aliyun EIPs to associate in {region_id}: {pending}"
        )
    return current


def _eip_is_releasable(eip) -> bool:
    return (
        eip is not None
        and not getattr(eip, "instance_id", None)
        and getattr(eip, "status", None) == "Available"
    )


def _wait_for_eips_releasable(vpc_client: VpcClient, region_id: str, eips: Iterable[object]):
    allocation_ids = [
        getattr(eip, "allocation_id", None)
        for eip in eips
        if getattr(eip, "allocation_id", None)
    ]
    return _poll_eips(
        vpc_client,
        region_id,
        allocation_ids,
        lambda observed: [
            allocation_id
            for allocation_id in allocation_ids
            if not _eip_is_releasable(observed.get(allocation_id))
        ],
    )


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
        prepared = list(executor.map(
            lambda eip: _request_eip_release_preparation(
                vpc_client,
                region_id,
                eip,
                context_builder(eip),
            ),
            target_eips,
        ))

    prepared_eips = [eip for eip, ok in zip(target_eips, prepared) if ok]
    if not prepared_eips:
        return 0

    current, pending = _wait_for_eips_releasable(vpc_client, region_id, prepared_eips)
    if pending:
        logger.warning(f"Timeout waiting for Aliyun EIPs to become releasable in {region_id}: {pending}")

    releasable_ids = {
        allocation_id
        for allocation_id, eip in current.items()
        if _eip_is_releasable(eip)
    }
    releasable_eips = [
        eip for eip in prepared_eips
        if getattr(eip, "allocation_id", None) in releasable_ids
    ]
    if not releasable_eips:
        return 0

    with ThreadPoolExecutor(max_workers=min(MAX_PARALLEL_EIP_OPS, len(releasable_eips))) as executor:
        results = list(executor.map(
            lambda eip: _release_eip_resource(
                vpc_client,
                region_id,
                eip,
                context_builder(eip),
            ),
            releasable_eips,
        ))
    return sum(1 for released in results if released)


def get_eip_public_ip_map(vpc_client: VpcClient, region_id: str, instance_ids: Iterable[str]) -> dict[str, str]:
    target_ids = set(instance_ids)
    if not target_ids:
        return {}

    page_number = 1
    mapping: dict[str, str] = {}
    while True:
        request = DescribeEipAddressesRequest(
            region_id=region_id,
            page_number=page_number,
            page_size=EIP_DESCRIBE_PAGE_SIZE,
        )
        resp = _call_eip_api(
            "describe_eip_addresses",
            lambda request=request: vpc_client.describe_eip_addresses(request),
            context=f"public-ip-map region={region_id} page={page_number}",
        )
        eip_addresses = getattr(resp.body, "eip_addresses", None)
        eips = getattr(eip_addresses, "eip_address", None) or []
        for eip in eips:
            if eip.instance_id in target_ids and eip.ip_address:
                mapping[eip.instance_id] = eip.ip_address

        total_count = resp.body.total_count or 0
        if total_count <= page_number * EIP_DESCRIBE_PAGE_SIZE or len(mapping) == len(target_ids):
            return mapping
        page_number += 1


def _allocate_eip(vpc_client: VpcClient, region_id: str, cfg: InstanceConfig, instance_id: str) -> str:
    req = AllocateEipAddressRequest(
        region_id=region_id,
        name=_eip_name(cfg, instance_id),
        description=f"{cfg.instance_name_prefix}-{instance_id}",
        isp="BGP",
        bandwidth=str(cfg.internet_max_bandwidth_out),
        internet_charge_type=cfg.aliyun_eip_internet_charge_type,
        instance_charge_type="PostPaid",
        client_token=_eip_client_token("allocate", region_id, instance_id, _eip_name(cfg, instance_id)),
        tag=_eip_tags(cfg),
    )
    resp = _call_eip_api(
        "allocate_eip_address",
        lambda req=req: vpc_client.allocate_eip_address(req),
        context=f"allocate instance={instance_id} region={region_id}",
    )
    allocation_id = resp.body.allocation_id
    if not allocation_id:
        raise RuntimeError(f"Aliyun EIP allocation returned empty allocation id for {instance_id} in {region_id}")
    logger.info(f"Allocated Aliyun EIP for {instance_id} in {region_id}: {allocation_id}")
    return allocation_id


def _associate_eip(vpc_client: VpcClient, region_id: str, allocation_id: str, instance_id: str):
    request = AssociateEipAddressRequest(
        region_id=region_id,
        allocation_id=allocation_id,
        instance_id=instance_id,
        instance_type=ECS_INSTANCE_TYPE,
        client_token=_eip_client_token("associate", region_id, allocation_id, instance_id),
    )
    _call_eip_api(
        "associate_eip_address",
        lambda request=request: vpc_client.associate_eip_address(request),
        context=f"associate allocation_id={allocation_id} instance_id={instance_id} region={region_id}",
    )


def _release_eip_by_allocation_id(vpc_client: VpcClient, region_id: str, allocation_id: str, *, context: str):
    request = ReleaseEipAddressRequest(region_id=region_id, allocation_id=allocation_id)
    _call_eip_api(
        "release_eip_address",
        lambda request=request: vpc_client.release_eip_address(request),
        context=f"release allocation_id={allocation_id} region={region_id} for {context}",
    )


def _unassociate_eip(vpc_client: VpcClient, region_id: str, allocation_id: str, instance_id: str):
    request = UnassociateEipAddressRequest(
        region_id=region_id,
        allocation_id=allocation_id,
        instance_id=instance_id,
        instance_type=ECS_INSTANCE_TYPE,
        force=True,
        client_token=_eip_client_token("unassociate", region_id, allocation_id, instance_id),
    )
    _call_eip_api(
        "unassociate_eip_address",
        lambda request=request: vpc_client.unassociate_eip_address(request),
        context=f"unassociate allocation_id={allocation_id} instance_id={instance_id} region={region_id}",
    )


def _ensure_instance_eip(
    vpc_client: VpcClient,
    region_id: str,
    cfg: InstanceConfig,
    instance_id: str,
    *,
    current_eip=None,
    named_eip=None,
) -> _EnsureInstanceEipResult:
    current = current_eip
    if current is not None:
        return _EnsureInstanceEipResult(instance_id, current.allocation_id, False)

    created_now = False
    if named_eip is not None:
        allocation_id = named_eip.allocation_id
    else:
        allocation_id = _allocate_eip(vpc_client, region_id, cfg, instance_id)
        created_now = True

    try:
        _associate_eip(vpc_client, region_id, allocation_id, instance_id)
    except Exception:
        if created_now:
            try:
                _release_eip_by_allocation_id(
                    vpc_client,
                    region_id,
                    allocation_id,
                    context=f"rollback after associate failure for instance {instance_id}",
                )
            except Exception as release_exc:
                logger.warning(f"Failed to release EIP {allocation_id} after associate error: {release_exc}")
        raise

    logger.info(f"Requested Aliyun EIP association {allocation_id} -> {instance_id} in {region_id}")
    return _EnsureInstanceEipResult(instance_id, allocation_id, True)


def ensure_instance_public_network(vpc_client: VpcClient, region_id: str, cfg: InstanceConfig, instance_ids: Iterable[str]) -> list[str]:
    if not cfg.use_aliyun_eip:
        return []

    unique_instance_ids = list(dict.fromkeys(instance_ids))
    if not unique_instance_ids:
        return []

    eips_by_instance, eips_by_name = _collect_relevant_eips(
        vpc_client,
        region_id,
        cfg,
        unique_instance_ids,
    )

    max_workers = min(MAX_PARALLEL_EIP_OPS, len(unique_instance_ids))
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        results = list(executor.map(
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

    pending_associations = {
        result.allocation_id: result.instance_id
        for result in results
        if result.association_pending
    }
    if pending_associations:
        _wait_for_eip_associations(vpc_client, region_id, pending_associations)
        for allocation_id, instance_id in pending_associations.items():
            logger.info(f"Associated Aliyun EIP {allocation_id} with instance {instance_id} in {region_id}")

    return [result.allocation_id for result in results]


def _release_eip_resource(vpc_client: VpcClient, region_id: str, eip, context: str) -> bool:
    allocation_id = getattr(eip, "allocation_id", None)
    if not allocation_id:
        return False

    try:
        _release_eip_by_allocation_id(vpc_client, region_id, allocation_id, context=context)
        logger.info(f"Released Aliyun EIP {allocation_id} in {region_id} for {context}")
        return True
    except Exception as exc:
        logger.warning(f"Failed to release Aliyun EIP {allocation_id} in {region_id} for {context}: {exc}")
        return False


def _request_eip_release_preparation(vpc_client: VpcClient, region_id: str, eip, context: str) -> bool:
    allocation_id = getattr(eip, "allocation_id", None)
    if not allocation_id:
        return False

    instance_id = getattr(eip, "instance_id", None)
    if not instance_id:
        return True

    try:
        _unassociate_eip(vpc_client, region_id, allocation_id, instance_id)
        return True
    except Exception as exc:
        logger.warning(f"Failed to unassociate Aliyun EIP {allocation_id} in {region_id} for {context}: {exc}")
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


def cleanup_user_public_network_artifacts(vpc_client: VpcClient, region_id: str, user_prefix: str) -> int:
    target_eips = [
        eip for eip in _describe_eips(vpc_client, region_id)
        if _matches_user_prefix(eip, user_prefix)
    ]
    return _release_eips_in_parallel(
        vpc_client,
        region_id,
        target_eips,
        lambda eip: f"user-prefix {user_prefix}",
    )