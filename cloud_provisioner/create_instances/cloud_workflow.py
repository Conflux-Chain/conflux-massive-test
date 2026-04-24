import threading
import traceback
from typing import Optional

from loguru import logger

from ..provider_interface import IEcsClient
from .launch_workflow import create_instances_in_multi_region
from .network_infra import InfraProvider, InfraRequest
from .provision_config import CloudConfig


def ensure_network_infra(
    client: IEcsClient,
    cloud_config: CloudConfig,
    barrier: threading.Barrier,
    allow_create: bool,
) -> Optional[InfraProvider]:
    try:
        request = InfraRequest.from_config(cloud_config, allow_create=allow_create)
        infra_provider = request.ensure_infras(client)
        logger.success(f"{cloud_config.provider} infra check pass")
        barrier.wait()
    except threading.BrokenBarrierError:
        logger.debug(f"{cloud_config.provider} quit due to other cloud providers fails")
        barrier.abort()
        return None
    except Exception as exc:
        logger.error(f"Fail to build network infra: {exc}")
        barrier.abort()
        print(traceback.format_exc())
        return None

    return infra_provider


def create_instances(
    client: IEcsClient,
    cloud_config: CloudConfig,
    barrier: threading.Barrier,
    allow_create: bool,
    infra_only: bool,
    allow_backfill: bool,
):
    infra_provider = ensure_network_infra(client, cloud_config, barrier, allow_create)
    if infra_only or infra_provider is None:
        return []

    return create_instances_in_multi_region(
        client,
        cloud_config,
        infra_provider,
        allow_backfill=allow_backfill,
    )