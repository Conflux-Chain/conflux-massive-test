from concurrent.futures import ThreadPoolExecutor, as_completed
import traceback
from typing import Any, Dict, List

from loguru import logger

from .provision_config import ProvisionRegionConfig


def count_nodes(hosts) -> int:
    return sum(host.nodes_per_host for host in hosts)


def run_regions_with_config(create_in_region, regions: List[ProvisionRegionConfig]):
    max_workers = min(20, max(1, len(regions)))
    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(create_in_region, region): region for region in regions}
        for future in as_completed(futures):
            region = futures[future]
            try:
                hosts = future.result()
                results.append(
                    {
                        "region": region.name,
                        "requested_nodes": region.count,
                        "hosts": hosts,
                        "actual_nodes": count_nodes(hosts),
                        "error": None,
                        "provision_config": region,
                    }
                )
            except Exception as exc:
                logger.error(f"Region {region.name} create_instances failed: {exc}")
                logger.error(traceback.format_exc())
                results.append(
                    {
                        "region": region.name,
                        "requested_nodes": region.count,
                        "hosts": [],
                        "actual_nodes": 0,
                        "error": exc,
                        "provision_config": region,
                    }
                )
    return results


def healthy_regions_for_backfill(region_results: List[Dict[str, Any]]):
    return [
        result
        for result in region_results
        if result["error"] is None and result["actual_nodes"] >= result["requested_nodes"]
    ]


def backfill_shortfall(create_in_region, healthy_regions, shortfall: int):
    if shortfall <= 0 or not healthy_regions:
        return [], 0

    extra_hosts = []
    unresolved_shortfall = shortfall
    candidates = list(healthy_regions)

    while unresolved_shortfall > 0 and candidates:
        region_count = len(candidates)
        base = unresolved_shortfall // region_count
        rem = unresolved_shortfall % region_count
        region_requests = []
        for i, result in enumerate(candidates):
            req_nodes = base + (1 if i < rem else 0)
            if req_nodes > 0:
                cfg = result["provision_config"].model_copy(update={"count": int(req_nodes)})
                region_requests.append((result, cfg, req_nodes))

        if not region_requests:
            break

        max_workers = min(20, len(region_requests))
        progressed = 0
        failed_regions = set()

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(create_in_region, cfg): (result, req_nodes)
                for result, cfg, req_nodes in region_requests
            }
            for future in as_completed(futures):
                result, req_nodes = futures[future]
                region_id = result["region"]
                try:
                    hosts = future.result()
                    added_nodes = count_nodes(hosts)
                    if added_nodes <= 0:
                        logger.warning(f"Backfill no progress in region {region_id}, requested_nodes={req_nodes}")
                        failed_regions.add(region_id)
                        continue
                    extra_hosts.extend(hosts)
                    progressed += added_nodes
                    logger.success(f"Backfill success in region {region_id}: requested_nodes={req_nodes}, added_nodes={added_nodes}")
                except Exception as exc:
                    logger.error(f"Backfill failed in region {region_id}: {exc}")
                    logger.error(traceback.format_exc())
                    failed_regions.add(region_id)

        if progressed <= 0:
            logger.error(f"Backfill stalled, remaining_shortfall={unresolved_shortfall}")
            break

        unresolved_shortfall = max(0, unresolved_shortfall - progressed)
        if failed_regions:
            candidates = [result for result in candidates if result["region"] not in failed_regions]

    return extra_hosts, unresolved_shortfall
