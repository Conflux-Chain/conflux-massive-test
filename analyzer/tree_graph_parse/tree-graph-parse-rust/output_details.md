# tree-graph-parse-rust Output Guide

This document explains what `analyzer/tree_graph_parse/tree-graph-parse-rust` does and how to interpret its outputs.

## 1. Project purpose

`tree-graph-parse-rust` parses Conflux block-insertion logs, rebuilds the block graph, and estimates how quickly each block can be considered safely confirmed under adversarial conditions. The workspace has two parts: core Rust logic in `tree-graph-parse-rust/`, and Python bindings in `python-wrapper/` exposed as `tg_parse_rpy`.

---

## 2. Background concepts

This section defines the Conflux-specific terms that appear throughout the rest of the document. All subsequent sections assume familiarity with these definitions.

### 2.1 DAG, tree graph, and referees

Conflux blocks form a directed acyclic graph (DAG). Each block has exactly one **parent** link and zero or more **referee** links. The parent links alone form a tree (the "tree graph"); referee links add cross-references that increase throughput but do not change the tree structure.

### 2.2 Pivot chain

The **pivot chain** is the backbone of the tree graph. Starting from the genesis block, the chain is extended at each height by choosing the child whose subtree contains the most blocks (the GHAST rule — Greedy Heaviest Adaptive SubTree). The pivot chain determines block ordering and finality.

### 2.3 Epoch

An **epoch** is the set of blocks associated with one pivot block. Specifically, a pivot block's epoch contains all blocks reachable from it (through parent or referee links) that have not already been assigned to an earlier epoch. Epoch size and epoch timing are important because confirmation time is measured per epoch.

### 2.4 Confirmation risk

In a public blockchain, a block that appears on the current pivot chain might still be reverted if an adversary with sufficient mining power constructs a competing subtree. **Confirmation risk** is the estimated probability that such a reversal can succeed at a given observation time, assuming the adversary controls a specified fraction of total mining power (`adv_percent`).

### 2.5 The adversarial model (negative binomial + random walk)

The confirmation-risk estimate combines two probabilistic components.

The first component models **how many blocks the adversary could have mined in secret**. Given that the honest network has produced `m` subtree-advantage increments for a pivot block, the number of hidden adversary blocks follows a negative binomial distribution parameterized by `m` and the adversary's mining-power fraction.

The second component models **whether the adversary can overtake the honest chain**. If the adversary has mined `k` hidden blocks while the honest network has built an advantage of `m`, the situation reduces to a one-dimensional random walk: the adversary needs to close a gap of `m - k` while each subsequent block is honest with probability `1 - adv_percent` and adversarial with probability `adv_percent`. The combined probability across all plausible `k` values gives the overall confirmation risk.

### 2.6 Key output fields: `time_offset`, `m`, `k`

`time_offset` is the elapsed time (in the block's local clock) from the block's creation until the observation point. As `time_offset` increases, more honest blocks accumulate and confirmation risk decreases.

`m` is the number of subtree-advantage increments observed for the pivot block at `time_offset` — effectively a proxy for honest mining progress.

`k` is the adversary block count at which the cumulative risk crosses the threshold, used internally during the risk calculation.

A lower `time_offset` at a given risk threshold means faster confirmation. A larger gap `m - k` implies a stronger safety margin.

### 2.7 Sequential search (threshold scan)

When computing `confirmation_risk(block, adv_percent, risk_threshold)`, the code scans increasing `time_offset` values and evaluates the combined risk at each step. This **sequential search** stops at the first `time_offset` where risk drops below the specified threshold. To avoid numerical degeneracy, risk values are floored at approximately `1e-12` before comparison.

---

## 3. Input and loading behavior

Loading starts from `Graph::load(file_or_path)` in `src/graph.rs`, which delegates path discovery to `open_conflux_log(...)` in `src/load.rs`.

The loader accepts four input shapes.

1. A direct `*.log.new_blocks` file.
2. A directory that contains exactly one `*.log.new_blocks`.
3. A direct `*.conflux.log` file, which triggers auto-generation of `<file>.new_blocks` by grepping `new block inserted into graph`.
4. A directory with exactly one `*.conflux.log`, also auto-generating `.new_blocks`.

If a directory contains multiple matching files, loading fails. Parsing also ignores unrelated lines and only consumes entries with the expected block fields (height, hash, parent, referees, timestamp, tx_count, block_size).

---

## 4. Derived computations before output

After parsing, `GraphComputer::finalize()` enriches graph state with derived fields that all output metrics rely on.

It builds parent-child links, computes `subtree_size` for every block, sorts children by subtree size, determines the pivot chain by repeatedly taking the maximum-subtree child (Section 2.2), marks epoch ownership (`epoch_block`, `epoch_set`) through referee traversal (Section 2.3), computes `past_set_size` through bitmap accumulation, and builds `subtree_adv_series` — a time series recording "best-child subtree size minus strongest-sibling subtree size" at each pivot block, which serves as the `m` sequence used in confirmation-risk estimation (Section 2.5).

---

## 5. Binary outputs

### 5.1 `compute_confirmation`

Implemented in `src/bin/compute_confirmation.rs`.

This binary currently loads one graph from a hard-coded path. For each pivot-chain block except genesis, it prints block-level fields (`height`, `subtree_size`, `past_set`, `epoch_span`, `avg_span`), where `epoch_span` is the time difference between the block and the last block in its epoch (Section 2.3), and `avg_span` is the mean epoch time up to that block. It then iterates adversary powers `{10, 15, 20, 25, 30}` percent and risk thresholds `{1e-4 .. 1e-8}` to print tuples of `(time_offset, m, k)` found by the sequential search described in Section 2.7. Finally it prints average confirmation-time summaries and total runtime.

### 5.2 `analyze_all_nodes`

Implemented in `src/bin/analyze_all_nodes.rs`.

This binary recursively scans a hard-coded root path for `conflux.log.new_blocks`, loads graphs in parallel with Rayon, and prints how many matching files were found and loaded. It also runs `avg_confirm_time(10, 1e-6)` for each graph in parallel.

Current caveat: it computes per-graph averages but does not print or aggregate those values; runtime output is primarily discovery/loading counts.

---

## 6. Rust library API outputs

The main object is `Graph`.

`pivot_chain()` returns ordered pivot blocks from genesis (Section 2.2). `epoch_span(block)` and `avg_epoch_time(block)` characterize epoch timing around the block timestamp (Section 2.3). `confirmation_risk(block, adv_percent, risk_threshold)` performs the sequential search described in Section 2.7 and returns the first point below threshold as `(time_offset, m, k, risk)`. `confirmation_risk_series(block, adv_percent)` returns the full `(time_offset, risk)` curve without threshold gating. `avg_confirm_time(adv_percent, risk_threshold)` returns a weighted average confirmation time (weighted by epoch size) and the number of blocks that participated. `export_edges(...)` writes `parent_hash,child_hash` CSV lines, and `export_indices(...)` writes `hash,index` CSV lines.

---

## 7. Python wrapper outputs (`tg_parse_rpy`)

`python-wrapper` exposes graph-level methods (`load`, `genesis_block`, `pivot_chain`, `epoch_span`, `avg_epoch_time`, `confirmation_risk`, `avg_confirm_time`) and block-level properties (`id`, `height`, `hash`, `parent_hash`, `referee_hashes`, `timestamp`, `log_timestamp`, `tx_count`, `block_size`, `children`, `epoch_block`, `epoch_set`, `past_set_size`, `subtree_size`, `epoch_size`).

Type semantics matter: hashes are exported as raw Python bytes (`PyBytes`) rather than hex strings, and `confirmation_risk(...)` returns either a tuple `(time_offset, m, k, risk)` or `None` when the threshold is never reached.

---

## 8. Interpretation checklist

When comparing blocks or runs, keep `adv_percent` and `risk_threshold` fixed to ensure comparability. Compare `time_offset` first — lower means faster confirmation. Then inspect the `m - k` gap as a safety-margin proxy (Section 2.6). Also check `epoch_size` and `avg_epoch_time`, because they affect weighted average confirmation time. Finally, verify log completeness: missing parent or referee links (Section 2.1) corrupt the graph structure and invalidate all downstream metrics.
