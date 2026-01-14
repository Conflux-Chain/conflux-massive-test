"""Naming utilities for cloud resources.

We standardize on a predictable instance name prefix so instances can be found
and cleaned up even if local state is missing.
"""

from __future__ import annotations

import re
from typing import Optional


INSTANCE_NAME_FIXED_PREFIX = "conflux-deployer"


def _sanitize_component(value: str) -> str:
    value = value.strip()
    value = re.sub(r"[^a-zA-Z0-9-]+", "-", value)
    value = re.sub(r"-+", "-", value)
    return value.strip("-")


def build_instance_name_prefix(
    *,
    deployment_id: str,
    region_id: str,
    user_prefix: Optional[str] = None,
) -> str:
    """Build the standard instance name prefix.

    Format:
        conflux-deployer-<user_prefix>-<deployment_id>-<region_id>

    All components are sanitized to contain only letters, digits, and '-'.
    """

    parts = [INSTANCE_NAME_FIXED_PREFIX]
    if user_prefix:
        parts.append(_sanitize_component(user_prefix))

    parts.append(_sanitize_component(deployment_id))
    parts.append(_sanitize_component(region_id))

    return "-".join([p for p in parts if p])
