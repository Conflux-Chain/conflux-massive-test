"""Small-world topology generator.

Strategy:
- Group nodes by (provider, region) as before.
- In each group (provider:region), choose hubs:
  - If group size > 3, choose 3 hubs (randomly);
  - If group size <= 3, choose 1 hub.

- Hub behavior:
  - Connect hub nodes to other hub nodes first (try to make a mesh between hubs),
  - Then hubs connect to distinct other regions (one per region when possible) until out_degree.

- Non-hub behavior:
  - Each non-hub connects to 1 hub node in its group (if any)
  - Then connects to nodes in other regions (distinct regions preferred), selecting targets
    with lowest incoming_counts first to balance incoming load.

- Latencies sampled uniformly from latency_min..latency_max (ms).
"""
from __future__ import annotations

import random
from collections import defaultdict
from typing import Dict, List

from loguru import logger

from remote_simulation.network_topology import NetworkTopology
from remote_simulation.remote_node import RemoteNode


def generate_small_world_topology(
    nodes: List[RemoteNode],
    out_degree: int = 8,
    in_degree: int = 64,
    latency_min: int = 1,
    latency_max: int = 20,
) -> NetworkTopology:
    num_nodes = len(nodes)
    topology = NetworkTopology()

    if num_nodes == 0:
        return topology

    # Group by provider:region
    groups: Dict[str, List[int]] = defaultdict(list)
    for idx, node in enumerate(nodes):
        provider = node.host_spec.provider or "<unknown-provider>"
        region = node.host_spec.region or "<unknown-region>"
        group_key = f"{provider}:{region}"
        groups[group_key].append(idx)

    logger.info(f"Generating small-world topology for {num_nodes} nodes in {len(groups)} groups")

    incoming_counts = [0] * num_nodes
    outgoing_counts = [0] * num_nodes

    def try_add_connection(frm: int, to: int, latency: int) -> bool:
        if frm == to:
            return False
        if to in topology.get_peers(frm):
            return False
        if incoming_counts[to] >= in_degree:
            return False
        topology.add_connection(frm, to, latency)
        outgoing_counts[frm] += 1
        incoming_counts[to] += 1
        return True

    # select hubs in each group
    group_hubs: Dict[str, List[int]] = {}
    for gk, idxs in groups.items():
        size = len(idxs)
        if size > 3:
            hubs = random.sample(idxs, 3)
        else:
            hubs = [random.choice(idxs)]
        group_hubs[gk] = hubs

    # flatten hubs list
    all_hubs = [h for hubs in group_hubs.values() for h in hubs]

    # 1) Connect hubs with each other (try to form a mesh among hubs)
    for i in range(len(all_hubs)):
        a = all_hubs[i]
        if outgoing_counts[a] >= out_degree:
            continue
        for j in range(i + 1, len(all_hubs)):
            b = all_hubs[j]
            if outgoing_counts[a] >= out_degree:
                break
            if b in topology.get_peers(a):
                continue
            if incoming_counts[b] >= in_degree:
                continue
            latency = random.randint(latency_min, latency_max)
            try_add_connection(a, b, latency)
            # try reverse direction as well when possible
            if outgoing_counts[b] < out_degree and incoming_counts[a] < in_degree and a not in topology.get_peers(b):
                latency = random.randint(latency_min, latency_max)
                try_add_connection(b, a, latency)

    # 2) Hubs: connect to distinct other regions if capacity remains
    # Map region -> hub nodes
    region_hubs: Dict[str, List[int]] = defaultdict(list)
    for gk, hubs in group_hubs.items():
        region = gk.split(":", 1)[1]
        region_hubs[region].extend(hubs)

    regions = list(region_hubs.keys())

    for hub in all_hubs:
        if outgoing_counts[hub] >= out_degree:
            continue
        my_region = nodes[hub].host_spec.region or "<unknown-region>"
        other_regions = [r for r in regions if r != my_region]
        random.shuffle(other_regions)
        for region in other_regions:
            if outgoing_counts[hub] >= out_degree:
                break
            # prefer hub nodes in that region as cross-region targets
            candidates = [c for c in region_hubs[region] if c not in topology.get_peers(hub) and incoming_counts[c] < in_degree]
            if not candidates:
                # fallback to any node in that region
                group_keys = [k for k in groups.keys() if k.split(":",1)[1] == region]
                candidates = [c for k in group_keys for c in groups[k] if c not in topology.get_peers(hub) and incoming_counts[c] < in_degree]
            if not candidates:
                continue
            # choose least-loaded candidate (lowest incoming counts)
            candidates.sort(key=lambda x: incoming_counts[x])
            target = candidates[0]
            latency = random.randint(latency_min, latency_max)
            try_add_connection(hub, target, latency)

    # 3) Non-hubs: connect to 1 hub node in its group, then to other regions (distinct)
    for gk, idxs in groups.items():
        hubs = group_hubs[gk]
        # pick hubs for this group
        for node_idx in idxs:
            if node_idx in hubs:
                continue
            # a) connect to one hub in its group (choose least outgoing among hubs)
            available_hubs = [h for h in hubs if h not in topology.get_peers(node_idx) and incoming_counts[h] < in_degree]
            if available_hubs and outgoing_counts[node_idx] < out_degree:
                available_hubs.sort(key=lambda x: (outgoing_counts[x], incoming_counts[x]))
                target = available_hubs[0]
                latency = random.randint(latency_min, latency_max)
                try_add_connection(node_idx, target, latency)

            # b) then connect to other regions distinct regions
            if outgoing_counts[node_idx] >= out_degree:
                continue
            my_region = gk.split(":", 1)[1]
            other_regions = [r for r in regions if r != my_region]
            random.shuffle(other_regions)
            for region in other_regions:
                if outgoing_counts[node_idx] >= out_degree:
                    break
                # prefer hubs in that region
                candidates = [c for c in region_hubs[region] if c not in topology.get_peers(node_idx) and incoming_counts[c] < in_degree]
                if not candidates:
                    group_keys = [k for k in groups.keys() if k.split(":",1)[1] == region]
                    candidates = [c for k in group_keys for c in groups[k] if c not in topology.get_peers(node_idx) and incoming_counts[c] < in_degree]
                if not candidates:
                    continue
                candidates.sort(key=lambda x: incoming_counts[x])
                target = candidates[0]
                latency = random.randint(latency_min, latency_max)
                try_add_connection(node_idx, target, latency)

    # 4) Final fill: if any node still has capacity, try to add intra-region peers or any available peers
    for node_idx in range(num_nodes):
        while outgoing_counts[node_idx] < out_degree:
            # try intra-region first
            my_region = nodes[node_idx].host_spec.region or "<unknown-region>"
            intra_candidates = [c for k, idxs in groups.items() if k.split(":",1)[1] == my_region for c in idxs if c != node_idx and c not in topology.get_peers(node_idx) and incoming_counts[c] < in_degree]
            if intra_candidates:
                random.shuffle(intra_candidates)
                target = intra_candidates[0]
                latency = random.randint(latency_min, latency_max)
                if not try_add_connection(node_idx, target, latency):
                    break
                continue

            # fallback: any candidate
            candidates = [c for c in range(num_nodes) if c != node_idx and c not in topology.get_peers(node_idx) and incoming_counts[c] < in_degree]
            if not candidates:
                break
            candidates.sort(key=lambda x: incoming_counts[x])
            target = candidates[0]
            latency = random.randint(latency_min, latency_max)
            if not try_add_connection(node_idx, target, latency):
                break

    avg_out = sum(outgoing_counts) / num_nodes
    max_in = max(incoming_counts) if incoming_counts else 0
    logger.info(f"Small-world topology generated: avg_outgoing={avg_out:.2f}, max_incoming={max_in}")

    return topology


__all__ = ["generate_small_world_topology"]
