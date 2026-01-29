"""Aliyun ECS helpers for Conflux test automation."""
from .multi_region_runner import provision_aliyun_hosts, cleanup_targets

__all__ = [
    "provision_aliyun_hosts",
    "cleanup_targets",
    "cleanup_all_regions",
]


def __getattr__(name: str):
    if name == "cleanup_all_regions":
        from .cleanup_resources import cleanup_all_regions

        return cleanup_all_regions
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
