from dataclasses import dataclass
from typing import Callable, List, TypeVar

DEFAULT_VPC_CIDR = "10.0.0.0/16"


@dataclass(frozen=True)
class InfraNames:
    provider: str
    vpc_name: str
    v_switch_name: str
    security_group_name: str
    image_name: str
    key_pair_name: str


def build_infra_names(provider: str, user_tag: str, image_name: str, key_pair_tag: str) -> InfraNames:
    infra_tag = f"conflux-massive-test-{user_tag}"
    if provider == "tencent":
        key_pair_name = f"cfx_test_{key_pair_tag}"
    else:
        key_pair_name = infra_tag

    return InfraNames(
        provider=provider,
        vpc_name=infra_tag,
        v_switch_name=infra_tag,
        security_group_name=infra_tag,
        image_name=image_name,
        key_pair_name=key_pair_name,
    )


def allocate_vacant_cidr_block(occupied_blocks: List[str], prefix: int = 24, vpc_cidr: str = DEFAULT_VPC_CIDR):
    import ipaddress

    occupied = {ipaddress.ip_network(block) for block in occupied_blocks if block}

    for subnet in ipaddress.ip_network(vpc_cidr).subnets(new_prefix=prefix):
        if all(not subnet.overlaps(used) for used in occupied):
            return str(subnet)

    raise RuntimeError(
        f"No available /{prefix} subnet found in {vpc_cidr}. "
        f"All subnets are occupied or overlapping."
    )


T = TypeVar("T")


def find_first(inputs: List[T], cond: Callable[[T], bool]):
    for item in inputs:
        if cond(item):
            return item
    return None