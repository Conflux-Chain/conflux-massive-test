"""Minimum-peers-first topology generator.

Strategy:
- For each node, repeatedly try to add outgoing connections until it hits `out_degree`.
- When selecting a target, prefer nodes that currently have the fewest peer connections
  (peer count is the total number of peers the node already has). Among ties, prefer
  targets in the same region as the source.
- When a connection is added, both nodes' peer counts increase immediately (the "connected
  peer also has an additional count of peer" behavior).
- Respects `out_degree` per-node outgoing limits and `in_degree` per-node incoming capacity.

Returns a `NetworkTopology` (connections are undirected in the returned topology).
"""
from __future__ import annotations

import random
from collections import defaultdict
from typing import Dict, List

from loguru import logger

from remote_simulation.network_topology import NetworkTopology
from remote_simulation.remote_node import RemoteNode


def generate_min_peer_topology(
    nodes: List[RemoteNode],
    out_degree: int = 8,
    in_degree: int = 64,
    latency_min: int = 1,
    latency_max: int = 20,
) -> NetworkTopology:
    """Generate topology preferring targets with the fewest peers.

    Args:
        nodes: list of all nodes (order defines node indices used in the returned topology)
        out_degree: maximum number of outgoing peers per node
        in_degree: maximum number of incoming peers per node
        latency_min, latency_max: latency range (ms) sampled for links
    """
    num_nodes = len(nodes)
    topology = NetworkTopology()

    if num_nodes == 0:
        return topology

    # map index -> region for same-region preference
    region_map: Dict[int, str] = {}
    for idx, node in enumerate(nodes):
        region_map[idx] = node.host_spec.region or "<unknown-region>"

    # Track counts
    outgoing_counts = [0] * num_nodes  # how many outgoing connections a node initiated
    incoming_counts = [0] * num_nodes  # how many incoming connections a node has
    peer_counts = [0] * num_nodes      # total peers count (both directions)

    def try_add_connection(frm: int, to: int, latency: int) -> bool:
        if frm == to:
            return False
        if to in topology.get_peers(frm):
            return False
        if outgoing_counts[frm] >= out_degree:
            return False
        if incoming_counts[to] >= in_degree:
            return False

        topology.add_connection(frm, to, latency)
        # update bookkeeping: source initiated an outgoing, dest received an incoming
        outgoing_counts[frm] += 1
        incoming_counts[to] += 1
        # both nodes' peer counts increase immediately
        peer_counts[frm] += 1
        peer_counts[to] += 1
        return True

    # For each node (in index order), greedily fill up to out_degree
    for node_idx in range(num_nodes):
        # keep attempting until we either fill out_degree or no viable candidates
        while outgoing_counts[node_idx] < out_degree:
            # build candidate list
            candidates = [
                j for j in range(num_nodes)
                if j != node_idx
                and j not in topology.get_peers(node_idx)
                and incoming_counts[j] < in_degree
            ]
            if not candidates:
                break

            # find minimal peer_count among candidates
            min_count = min(peer_counts[j] for j in candidates)
            candidates_min = [j for j in candidates if peer_counts[j] == min_count]

            # among ties prefer same-region targets
            same_region = [j for j in candidates_min if region_map.get(j) == region_map.get(node_idx)]
            if same_region:
                chosen_pool = same_region
            else:
                chosen_pool = candidates_min

            # pick randomly among chosen_pool to break ties
            target = random.choice(chosen_pool)
            latency = random.randint(latency_min, latency_max)

            # if try_add_connection fails (rare due to racing counts), remove target and retry
            added = try_add_connection(node_idx, target, latency)
            if not added:
                # remove target from candidate list for this iteration and continue
                try:
                    candidates.remove(target)
                except ValueError:
                    pass
                # but if no candidates left break
                if not candidates:
                    break
                continue

    avg_peers = sum(peer_counts) / num_nodes if num_nodes else 0
    max_in = max(incoming_counts) if incoming_counts else 0
    logger.info(f"Min-peers topology generated: avg_peers={avg_peers:.2f}, max_incoming={max_in}")

    return topology


__all__ = ["generate_min_peer_topology"]
