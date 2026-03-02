# stat_latency_rs Table Glossary

This glossary explains every row type printed by `analyzer/stat_latency/stat_latency_rs`.

## 1) Background: Conflux block processing stages

This section defines the Conflux-specific terms used throughout the table rows. All subsequent sections assume familiarity with these concepts.

### DAG, parent, and referee

Conflux blocks form a directed acyclic graph (DAG). Each block specifies one **parent** block and zero or more **referee** (reference) blocks. The parent links alone form a tree; referee links provide additional cross-references that increase throughput without altering the tree structure.

### Pivot chain and epoch

The **pivot chain** is the main chain of the tree graph, constructed by choosing the child with the largest subtree at each height (the GHAST rule). An **epoch** is the set of blocks assigned to a single pivot block — all blocks reachable from that pivot via parent/referee links that are not yet assigned to an earlier epoch. Epoch computation is a core step in Conflux's consensus ordering.

### Sync graph and consensus graph

A Conflux node maintains two internal graph structures during block processing.

The **sync graph** receives blocks as they arrive from the network: headers are fetched first, then bodies, and the block is inserted into the sync graph once both are available.

The **consensus graph** receives blocks promoted from the sync graph and applies the full consensus protocol — computing epochs, executing transactions, and updating finality. The pipeline within a single node therefore proceeds as: header arrival → body download → sync graph insertion → consensus graph insertion → epoch computation → transaction pool notification.

The `stat_latency_rs` table tracks elapsed times at each of these stages.

---

## 2) How to read one row

Each row has a fixed set of columns: `Avg, P10, P30, P50, P80, P90, P95, P99, P999, Max, Cnt`.    

The table is built in two stages. First, for each object—usually a block, transaction or node—the analyzer computes a single value corresponding to the metric named by the row.  Next, it takes those per‑object values and produces the aggregate statistics that appear in the columns above.    

For example, the row `block broadcast latency (Receive/P95)` means: for every block, compute the node‑level P95 of `Receive`; then, across all blocks, report the average of those P95s as `Avg`, the 10‑th percentile as `P10`, … up to `Max`.  The `Cnt` column simply counts how many objects contributed to the distribution.

---

## 3) Row families

### A. Block broadcast latency

There are three closely-related rows here.  The suffix in parentheses denotes the measurement point within the block processing pipeline described in Section 1.

`block broadcast latency (Receive/<node-percentile>)` measures how long it takes a block to reach the first node(s) after being generated.  
`block broadcast latency (Sync/<node-percentile>)` records the delay between receipt and insertion into the sync graph (Section 1), and `block broadcast latency (Cons/<node-percentile>)` measures the time until the same block enters the consensus graph (Section 1).  

All three metrics are reported in **seconds**.

### B. Block event elapsed

These rows trace the internal pipeline of a block as it travels through a single node.  Each name is an increment: `HeaderReady` is the time when the header arrived, `BodyReady` when the body finished downloading, `SyncGraph` when the block reached the sync graph (Section 1), and so on.  Formally the per‑block, per‑node value is computed as follows:

```
HeaderReady = header_ready - start
BodyReady   = body_ready - header_ready
SyncGraph   = sync_graph - body_ready
ConsensusGraphStart = consensys_graph_insert - sync_graph
ConsensusGraphReady = consensys_graph_ready - consensys_graph_insert
ComputeEpoch = compute_epoch - consensys_graph_ready
NotifyTxPool = notify_tx_pool - compute_epoch
TxPoolUpdated = tx_pool_updated - notify_tx_pool
```

All eight of these are emitted in seconds.  Note that the last three appear only when the node has collected a sufficiently large fraction of peers (90 % by default); sparse sampling causes them to be dropped from the per‑block report.

### C. Custom block event elapsed

Custom rows arise when log keys do not match the standard broadcast/event/tx categories. Parser logic in `analyzer/stat_latency/stat_latency_map_reduce.py` supports two forms.

`custom_<snake_name>_<n>` is treated as a sequence of stage timestamps. The parser converts the base name to CamelCase and emits adjacent deltas as `Name0`, `Name1`, and so on. If there are checkpoints `t0..t5`, the table gets five elapsed rows: `Name0 = t1-t0` through `Name4 = t5-t4`.

`gauge_<snake_name>` is treated as a raw numeric gauge. The parser keeps the value as is, records the CamelCase name, and does not apply time conversion.

For interpretation confidence, use this rule: parser behavior and units are **verified**; mapping each stage to exact runtime functions is **inferred** unless the emitter site is found in the same source revision.

Observed custom families and meanings:

**Compute0..4**

`Compute0..4` are five elapsed slices produced from six checkpoints (`custom_compute_0..5`). The parser computes adjacent differences and divides by `1_000_000`, so these rows are always in **seconds**.

Code-informed stage mapping of the five slices aligns with the `compute_epoch` pipeline (the epoch computation step described in Section 1):

1. pre-execution preparation (`new_state`, epoch input assembly, pre-checks),
2. main transaction execution (`process_epoch_transactions`),
3. post-execution reward/PoS processing,
4. state commit plus execution commitment write,
5. post-commit hooks (including txpool notification and boundary updates).

This mapping explains why `Compute0..4` gives finer granularity than coarse rows such as `ComputeEpoch`, `NotifyTxPool`, and `TxPoolUpdated`.

**SyncToCon0..3**

`SyncToCon0..3` are four elapsed slices produced from five checkpoints (`custom_sync_to_con_0..4`). Parser behavior is the same: adjacent differences, then division by `1_000_000`, so all four rows are in **seconds**.

Code-informed stage mapping tracks the handoff from sync graph to consensus graph (Section 1):

1. sync graph marks ready and sends hash to consensus worker channel,
2. worker receives hash and waits for dependency/scheduling readiness,
3. worker bookkeeping and successor/reverse-map scheduling before dispatch,
4. consensus starts `on_new_block` processing.

**ConWorkerQueue / ConWorkerSuccessors**

These are worker-side queue/successor pressure signals. Treat them as queue-depth or dependency-count style metrics, not elapsed-time durations.

**CmptRecoverRate**

`CmptRecoverRate` is a gauge family (`gauge_cmpt_recover_rate`). Parser copies it verbatim with no scaling. Units are emitter-defined (commonly a rate-like value), so do not assume seconds.

Source-tracing note: in this workspace checkout, literal emitter strings like `custom_compute_*`, `custom_sync_to_con_*`, and `gauge_cmpt_recover_rate` were not found via text search. The family semantics above remain correct because they come from parser rules plus observed log keys; exact producer function names require the exact Conflux-Rust revision/binary that emitted the logs.


### D. Transaction latency rows

Rows in this family are `tx broadcast latency (<node-percentile>)`, `tx packed to block latency (<node-percentile>)`, `min tx packed to block latency`, `min tx to ready pool latency`, `by_block_ratio`, and `Tx wait to be packed elapsed time`.

`tx broadcast latency` measures propagation from the earliest observed receive to other node receives for fully propagated transactions. `tx packed to block latency` measures from earliest receive to packing time, while `min tx packed to block latency` picks the earliest pack minus earliest receive per transaction. `min tx to ready pool latency` is earliest ready-pool time minus earliest receive. `by_block_ratio` is the fraction of sampled transactions that arrived through the block path. `Tx wait to be packed elapsed time` is sampled packed-time minus receive-time.

All rows in this family are in **seconds** except `by_block_ratio`, which is a unitless ratio.

Millisecond caveat:

- Some printed values may effectively represent milliseconds from emitter-defined gauges or integer timer buckets, even though table formatting is generic numeric.
- Practical rule:
   - `custom_*_<n>` families in parser => converted to seconds.
   - `gauge_*` families in parser => raw numeric; verify unit from emitter docs/code for that build.

### E. Block scalar rows

Rows in this family are `block txs`, `block size`, `block referees`, and `block generation interval`.

They summarize per-block scalar properties: transaction count, serialized block size, referee count (the number of non-parent block references in the DAG, see Section 1), and adjacent generation-time intervals. Units are count, bytes, count, and seconds respectively.

### F. Node sync/cons gap rows

Rows are `node sync/cons gap (Avg)`, `node sync/cons gap (P50)`, `node sync/cons gap (P90)`, `node sync/cons gap (P99)`, and `node sync/cons gap (Max)`.

For each node, the underlying sampled quantity is `inserted_header_count(sync_graph) - inserted_block_count(consensus_graph)` — measuring the backlog between the sync graph and the consensus graph (Section 1). The table then aggregates those per-node statistics across nodes. This family is a **count** metric representing processing lag, not a time metric.

---

## 4) Practical interpretation tips

- Compare `Receive` vs `Sync` vs `Cons` tails (`P95/P99/Max`) to localize which stage in the block pipeline (Section 1) is the bottleneck.
- Large `SyncGraph` or `ConsensusGraphStart` tails suggest pipeline contention between the sync graph and consensus graph (Section 1).
- Large `ComputeEpoch`/`Compute*` tails indicate execution or epoch-compute pressure.
- High `by_block_ratio` means tx propagation is relying more on block transport.
- `CmptRecoverRate` is gauge semantics; treat it as a health/pressure indicator, not elapsed seconds unless emitter docs confirm unit.
