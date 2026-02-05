"""Centralized topology generator.

Strategy:
- Treat groups by (provider, region) as in group-aware strategy.
- A given region (default: "ap-southeast-1") is the *central* region.

Rules:
- All nodes in the central region try to connect to peers within the central region first
  (prioritize full connectivity inside the central region). If a node still has remaining
  outgoing capacity, it connects to random peers in distinct other regions (one per region
  where possible) to spread cross-region links.

- Nodes in non-central regions first try to connect to nodes in the central region (to
  minimize diameter). If they still have remaining peers, they try intra-region peers.
  Finally, they connect to other regions (excluding central first) as a last resort.

This generator returns a NetworkTopology instance with per-edge latencies sampled from
latency_min..latency_max (ms).
"""
from __future__ import annotations

import random
from collections import defaultdict
from typing import Dict, List

from loguru import logger

from remote_simulation.network_topology import NetworkTopology
from remote_simulation.remote_node import RemoteNode


def generate_centralized_topology(
    nodes: List[RemoteNode],
    central_region: str = "ap-southeast-5",
    out_degree: int = 8,
    in_degree: int = 64,
    latency_min: int = 1,
    latency_max: int = 20,
) -> NetworkTopology:
    """Generate a centralized topology where `central_region` is the hub.

    Args:
        nodes: list of all nodes (order defines node indices used in the returned topology)
        central_region: region name treated as the central hub (e.g., "ap-southeast-1")
        out_degree: maximum number of outgoing peers per node
        in_degree: maximum number of incoming peers per node
        latency_min, latency_max: latency range sampled for links (ms)

    Returns:
        NetworkTopology with edges added and latencies set
    """
    num_nodes = len(nodes)
    topology = NetworkTopology()

    if num_nodes == 0:
        return topology

    # Group nodes by provider and region
    groups: Dict[str, List[int]] = defaultdict(list)
    region_map: Dict[int, str] = {}
    for idx, node in enumerate(nodes):
        provider = node.host_spec.provider or "<unknown-provider>"
        region = node.host_spec.region or "<unknown-region>"
        group_key = f"{provider}:{region}"
        groups[group_key].append(idx)
        region_map[idx] = region

    logger.info(f"Generating centralized topology for {num_nodes} nodes with central region '{central_region}'")

    # Collect central region nodes (may span providers if any)
    central_group_keys = [k for k in groups.keys() if k.split(":", 1)[1] == central_region]
    central_nodes = [i for k in central_group_keys for i in groups[k]]

    # Track counts
    incoming_counts = [0] * num_nodes
    outgoing_counts = [0] * num_nodes

    def try_add_connection(frm: int, to: int, latency: int) -> bool:
        if frm == to:
            return False
        if to in topology.get_peers(frm):
            return False
        if incoming_counts[to] >= in_degree:
            return False
        # add undirected link
        topology.add_connection(frm, to, latency)
        outgoing_counts[frm] += 1
        incoming_counts[to] += 1
        return True

    # 1) Central region: connect central nodes with each other in priority
    # try to make central region well-connected: for every pair try to add a connection
    for i in range(len(central_nodes)):
        a = central_nodes[i]
        for j in range(i + 1, len(central_nodes)):
            b = central_nodes[j]
            # attempt a->b and b->a if capacities allow (both directions are represented by single add)
            if outgoing_counts[a] < out_degree and incoming_counts[b] < in_degree and b not in topology.get_peers(a):
                latency = random.randint(latency_min, latency_max)
                try_add_connection(a, b, latency)
            if outgoing_counts[b] < out_degree and incoming_counts[a] < in_degree and a not in topology.get_peers(b):
                latency = random.randint(latency_min, latency_max)
                try_add_connection(b, a, latency)

    # For central nodes, if they still have capacity, connect to random peers in distinct other regions
    other_region_nodes_by_region: Dict[str, List[int]] = defaultdict(list)
    for k, idxs in groups.items():
        region = k.split(":", 1)[1]
        if region == central_region:
            continue
        other_region_nodes_by_region[region].extend(idxs)

    other_regions = list(other_region_nodes_by_region.keys())

    for node_idx in central_nodes:
        if outgoing_counts[node_idx] >= out_degree:
            continue
        used_regions = set()
        # pick at most one node per other region to diversify
        regions_shuffled = other_regions[:]
        random.shuffle(regions_shuffled)
        for region in regions_shuffled:
            if outgoing_counts[node_idx] >= out_degree:
                break
            candidates = [c for c in other_region_nodes_by_region[region] if c not in topology.get_peers(node_idx) and incoming_counts[c] < in_degree and c != node_idx]
            if not candidates:
                continue
            target = random.choice(candidates)
            latency = random.randint(latency_min, latency_max)
            if try_add_connection(node_idx, target, latency):
                used_regions.add(region)

    # 2) Non-central nodes: connect to central region first, then same region, then others
    for group_key, idxs in groups.items():
        region = group_key.split(":", 1)[1]
        # skip central region (handled)
        if region == central_region:
            continue

        for node_idx in idxs:
            if outgoing_counts[node_idx] >= out_degree:
                continue

            # a) connect to central region nodes first
            cross_candidates = [i for i in central_nodes if i not in topology.get_peers(node_idx) and incoming_counts[i] < in_degree]
            random.shuffle(cross_candidates)
            cross_candidates.sort(key=lambda x: incoming_counts[x])
            for target in cross_candidates:
                if outgoing_counts[node_idx] >= out_degree:
                    break
                latency = random.randint(latency_min, latency_max)
                try_add_connection(node_idx, target, latency)

            if outgoing_counts[node_idx] >= out_degree:
                continue

            # b) then intra-region peers
            intra_candidates = [i for i in idxs if i != node_idx and i not in topology.get_peers(node_idx) and incoming_counts[i] < in_degree]
            random.shuffle(intra_candidates)
            for target in intra_candidates:
                if outgoing_counts[node_idx] >= out_degree:
                    break
                latency = random.randint(latency_min, latency_max)
                try_add_connection(node_idx, target, latency)

            if outgoing_counts[node_idx] >= out_degree:
                continue

            # c) finally connect to nodes in other non-central regions
            other_candidates = [i for k2, idxs2 in groups.items() if k2.split(":",1)[1] != region and k2.split(":",1)[1] != central_region for i in idxs2]
            random.shuffle(other_candidates)
            other_candidates.sort(key=lambda x: incoming_counts[x])
            for target in other_candidates:
                if outgoing_counts[node_idx] >= out_degree:
                    break
                if target in topology.get_peers(node_idx):
                    continue
                if incoming_counts[target] >= in_degree:
                    continue
                latency = random.randint(latency_min, latency_max)
                try_add_connection(node_idx, target, latency)

    avg_out = sum(outgoing_counts) / num_nodes
    max_in = max(incoming_counts) if incoming_counts else 0
    logger.info(f"Centralized topology generated: avg_outgoing={avg_out:.2f}, max_incoming={max_in}")

    return topology


__all__ = ["generate_centralized_topology"]
