"""Group-aware topology generator.

Strategy (per user request):
- Group nodes by (provider, region).
- Each node has at most `out_degree` outgoing peers and at most `in_degree` incoming peers.
- Each node must have at least 2 outgoing peer(s) within its own group, if possible.
- For groups with exactly two nodes, the pair will be mutually connected and each will be given at least one cross-group peer when capacity allows.
- Prefer establishing outgoing links to nodes in other groups (cross-group) to reduce diameter — while respecting incoming capacity.
- Cross-group latency is sampled from the same base range.

The generator returns a `NetworkTopology` instance with per-edge latencies.
"""
from __future__ import annotations

import random
from collections import defaultdict
from typing import Dict, List

from loguru import logger

from remote_simulation.network_topology import NetworkTopology
from remote_simulation.remote_node import RemoteNode


def generate_group_aware_topology(
    nodes: List[RemoteNode],
    out_degree: int = 8,
    in_degree: int = 64,
    latency_min: int = 1,
    latency_max: int = 20,
) -> NetworkTopology:
    """Generate a topology that is conscious of cloud provider/region grouping.

    Args:
        nodes: list of all nodes (order defines node indices used in the returned topology)
        out_degree: maximum number of outgoing peers per node
        in_degree: maximum number of incoming peers per node
        latency_min, latency_max: base latency range (ms) for intra-group and cross-group links

    Returns:
        NetworkTopology with edges added and latencies set
    """
    num_nodes = len(nodes)
    topology = NetworkTopology()

    if num_nodes == 0:
        return topology

    # Group nodes by provider and region
    groups: Dict[str, List[int]] = defaultdict(list)
    for idx, node in enumerate(nodes):
        provider = node.host_spec.provider or "<unknown-provider>"
        region = node.host_spec.region or "<unknown-region>"
        group_key = f"{provider}:{region}"
        groups[group_key].append(idx)

    logger.info(f"Generating group-aware topology for {num_nodes} nodes in {len(groups)} groups")

    # Track counts
    incoming_counts = [0] * num_nodes
    outgoing_counts = [0] * num_nodes

    # Helper to add directed connection (from -> to) and update counts only when new
    def try_add_connection(frm: int, to: int, latency: int) -> bool:
        if frm == to:
            return False
        if to in topology.get_peers(frm):
            return False
        # enforce incoming capacity
        if incoming_counts[to] >= in_degree:
            return False
        # enforce outgoing capacity for frm will be checked by caller
        topology.add_connection(frm, to, latency)
        outgoing_counts[frm] += 1
        incoming_counts[to] += 1
        return True

    # 1) Ensure each node has at least 2 outgoing intra-group peers (if possible)
    for group_key, idxs in groups.items():
        size = len(idxs)
        if size <= 1:
            # singleton group cannot satisfy intra-group requirement
            continue
        for node_idx in idxs:
            required_intra = min(2, size - 1, out_degree)
            while outgoing_counts[node_idx] < required_intra:
                candidates = [
                    c
                    for c in idxs
                    if c != node_idx
                    and c not in topology.get_peers(node_idx)
                    and incoming_counts[c] < in_degree
                ]
                if not candidates:
                    break
                candidate = random.choice(candidates)
                latency = random.randint(latency_min, latency_max)
                try_add_connection(node_idx, candidate, latency)

    # 2) Prefer cross-group connections to reduce diameter and worst-case latency
    #    For each node, fill outgoing slots up to `out_degree` preferring nodes from other groups,
    #    selecting targets with lowest incoming_counts first to balance incoming load.
    for node_idx in range(num_nodes):
        if outgoing_counts[node_idx] >= out_degree:
            continue
        # prepare candidate pools
        provider = nodes[node_idx].host_spec.provider or "<unknown-provider>"
        region = nodes[node_idx].host_spec.region or "<unknown-region>"
        my_group_key = f"{provider}:{region}"

        # Make cross-group candidates ordered by increasing incoming count
        cross_candidates = [i for k, idxs in groups.items() if k != my_group_key for i in idxs]
        # shuffle to break ties and then sort by incoming load
        random.shuffle(cross_candidates)
        cross_candidates.sort(key=lambda x: incoming_counts[x])

        # Try adding cross-group connections first
        for target in cross_candidates:
            if outgoing_counts[node_idx] >= out_degree:
                break
            if target in topology.get_peers(node_idx):
                continue
            if incoming_counts[target] >= in_degree:
                continue
            latency = random.randint(latency_min, latency_max)
            try_add_connection(node_idx, target, latency)

        # If still have capacity, add intra-group peers to fill up to out_degree
        intra_candidates = [i for i in groups[my_group_key] if i != node_idx and i not in topology.get_peers(node_idx) and incoming_counts[i] < in_degree]
        random.shuffle(intra_candidates)
        for target in intra_candidates:
            if outgoing_counts[node_idx] >= out_degree:
                break
            latency = random.randint(latency_min, latency_max)
            try_add_connection(node_idx, target, latency)

    # 2b) Special-case: for groups with exactly 2 nodes, ensure mutual intra-group links exist
    # and try to give each node at least one cross-group peer when capacity allows.
    for group_key, idxs in groups.items():
        if len(idxs) != 2:
            continue
        a, b = idxs[0], idxs[1]
        # ensure mutual connection a->b and b->a if possible
        if b not in topology.get_peers(a) and outgoing_counts[a] < out_degree and incoming_counts[b] < in_degree:
            latency = random.randint(latency_min, latency_max)
            try_add_connection(a, b, latency)
        if a not in topology.get_peers(b) and outgoing_counts[b] < out_degree and incoming_counts[a] < in_degree:
            latency = random.randint(latency_min, latency_max)
            try_add_connection(b, a, latency)

        # ensure each has at least one cross-group peer if possible
        for node_idx in (a, b):
            peers = topology.get_peers(node_idx)
            has_cross = any((p not in idxs) for p in peers)
            if not has_cross and outgoing_counts[node_idx] < out_degree:
                cross_candidates = [i for k, others in groups.items() if k != group_key for i in others if i not in peers and incoming_counts[i] < in_degree]
                if not cross_candidates:
                    continue
                random.shuffle(cross_candidates)
                cross_candidates.sort(key=lambda x: incoming_counts[x])
                target = cross_candidates[0]
                latency = random.randint(latency_min, latency_max)
                try_add_connection(node_idx, target, latency)

    # Logging summary
    avg_out = sum(outgoing_counts) / num_nodes
    max_in = max(incoming_counts) if incoming_counts else 0
    logger.info(f"Topology generated: avg_outgoing={avg_out:.2f}, max_incoming={max_in}")

    return topology


__all__ = ["generate_group_aware_topology"]
