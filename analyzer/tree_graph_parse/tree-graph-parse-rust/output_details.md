# tree-graph-parse-rust Output Guide

This document explains what `analyzer/tree_graph_parse/tree-graph-parse-rust` does and how to interpret its outputs.

## 1. Project purpose

`tree-graph-parse-rust` parses Conflux block-insertion logs, rebuilds a DAG/tree view of blocks, computes pivot and epoch relationships, and estimates confirmation risk under adversarial mining assumptions.

The workspace has two parts: core Rust logic in `tree-graph-parse-rust/`, and Python bindings in `python-wrapper/` exposed as `tg_parse_rpy`.

---

## 2. Input and loading behavior

Loading starts from `Graph::load(file_or_path)` in `src/graph.rs`, which delegates path discovery to `open_conflux_log(...)` in `src/load.rs`.

The loader accepts four input shapes.

1. A direct `*.log.new_blocks` file.
2. A directory that contains exactly one `*.log.new_blocks`.
3. A direct `*.conflux.log` file, which triggers auto-generation of `<file>.new_blocks` by grepping `new block inserted into graph`.
4. A directory with exactly one `*.conflux.log`, also auto-generating `.new_blocks`.

If a directory contains multiple matching files, loading fails. Parsing also ignores unrelated lines and only consumes entries with the expected block fields.

---

## 3. Derived computations before output

After parsing, `GraphComputer::finalize()` enriches graph state with derived fields that all output metrics rely on.

It builds parent-child links, computes `subtree_size`, sorts children by subtree size, determines pivot chain by repeatedly taking `max_child`, marks epoch ownership (`epoch_block`, `epoch_set`) from referees, computes `past_set_size` through bitmap accumulation, and builds `subtree_adv_series` as `(best child subtree) - (best sibling subtree)` over pivot nodes.

---

## 4. Binary outputs

### 4.1 `compute_confirmation`

Implemented in `src/bin/compute_confirmation.rs`.

This binary currently loads one graph from a hard-coded path. For each pivot-chain block except genesis, it prints block-level fields (`height`, `subtree_size`, `past_set`, `epoch_span`, `avg_span`). It then iterates adversary powers `{10,15,20,25,30}` and risk thresholds `{1e-4..1e-8}` to print tuples with `time_offset`, `m`, and `k`, and later prints average confirmation-time summaries and total runtime.

Interpretation of one tuple `(time_offset, m, k)` is straightforward: lower `time_offset` means faster confirmation, while larger `m` with smaller `k` usually implies a stronger safety margin.

### 4.2 `analyze_all_nodes`

Implemented in `src/bin/analyze_all_nodes.rs`.

This binary recursively scans a hard-coded root path for `conflux.log.new_blocks`, loads graphs in parallel with Rayon, and prints how many matching files were found and loaded. It also runs `avg_confirm_time(10, 1e-6)` for each graph in parallel.

Current caveat: it computes per-graph averages but does not print or aggregate those values; runtime output is primarily discovery/loading counts.

---

## 5. Rust library outputs

The main object is `Graph`.

`pivot_chain()` returns ordered pivot blocks from genesis. `epoch_span(block)` and `avg_epoch_time(block)` characterize epoch timing around the block timestamp. `confirmation_risk(block, adv_percent, risk_threshold)` returns the first point below threshold as `(time_offset, m, k, risk)`, while `confirmation_risk_series(block, adv_percent)` returns the full `(time_offset, risk)` curve. `avg_confirm_time(adv_percent, risk_threshold)` returns a weighted average confirmation time and counted blocks. `export_edges(...)` writes `parent_hash,child_hash` CSV lines, and `export_indices(...)` writes `hash,index` CSV lines.

---

## 6. Python wrapper outputs

`python-wrapper` exposes graph-level methods (`load`, `genesis_block`, `pivot_chain`, `epoch_span`, `avg_epoch_time`, `confirmation_risk`, `avg_confirm_time`) and block-level properties (`id`, `height`, `hash`, `parent_hash`, `referee_hashes`, `timestamp`, `log_timestamp`, `tx_count`, `block_size`, `children`, `epoch_block`, `epoch_set`, `past_set_size`, `subtree_size`, `epoch_size`).

Type semantics matter: hashes are exported as raw Python bytes (`PyBytes`) rather than hex strings, and `confirmation_risk(...)` returns either a tuple `(time_offset, m, k, risk)` or `None`.

---

## 7. Mathematical meaning of confirmation risk

The risk model combines two components: hidden adversarial-block count probability (negative-binomial model) and random-walk survival probability of adversarial lead.

`normal_confirmation_risk(adv_percent, m, adv)` is therefore a tail-like failure probability estimate. During series processing, risk values are floored away from exact zero (about `1e-12`) before threshold search.

---

## 8. Interpretation checklist

When comparing blocks or runs, keep `adv_percent` and `risk_threshold` fixed, compare `time_offset` first, then inspect the `m`/`k` gap as a safety proxy. Also check `epoch_size` and `avg_epoch_time`, because they affect weighted average confirmation time, and verify log completeness because missing parent/referee links can invalidate graph structure and downstream metrics.
