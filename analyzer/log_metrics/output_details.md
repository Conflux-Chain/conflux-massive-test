# log_metrics Output Guide

This document explains what `analyzer/log_metrics` outputs and how to interpret those outputs correctly.

## 1. Purpose and scope

The `analyzer/log_metrics` module processes **Conflux node runtime metrics** — periodic key-value statistics that each Conflux node writes to its `metrics.log` file during operation. These metrics cover counters (e.g., blocks processed), gauges (e.g., queue depth), and timing measurements, all emitted by instrumented modules inside the Conflux-Rust client.

The module has two output paths. The main path is figure generation from `__main__.py` plus `parse_metrics.py`. The secondary path is text-table comparison from `analysis.py` and helper printers in `parse_metrics.py`.

---

## 2. Key concepts

This section defines the terms and mechanisms referenced throughout later sections.

### 2.1 Metric naming: `module::key`

Each metric is identified by a two-part name: a **module** (the Conflux subsystem that emits it, matching `[0-9a-z_]+`) and a **key** (the specific measurement within that module). The fully qualified form is `module::key`. A bare `key` can be used only when it is unique across all modules; otherwise an ambiguity error is raised. For automation and reproducibility, always use `module::key`.

### 2.2 Two-layer percentile node selection

Because a test network may contain hundreds of nodes, plotting every node's time series would be unreadable. The module uses a **two-layer percentile** approach to select representative nodes.

In the first layer (within each node), the module computes one scalar representative value for the metric — by default the node-level 90th percentile (`node_percentile=90`).

In the second layer (across nodes), it ranks all nodes by their representative values and picks the nodes at specified global percentiles (`plot_percentiles`, default `[0, 10, 50, 90, 100]`).

Consequently, `P0` in a plot legend means "the node whose representative value is the lowest across all nodes," `P50` means "the median node," and `P100` means "the node with the highest representative value." These labels identify **which node is plotted**, not per-timestamp percentile curves.

### 2.3 Derived `.m1` metrics

Before plotting or computing statistics, the preprocessing step generates a derived metric for every original metric whose key ends in `.count`. The derived key is `<original>.m1`, and the derived value is a time-decay weighted average of the count increments — effectively a smoothed short-horizon rate or throughput signal, as opposed to the raw cumulative counter `.count`.

The computation proceeds in three steps.

1. Build an increment series: `diff[i] = value[i] - value[i-1]`, with the first point unchanged.
2. Build exponential-decay weights from elapsed time between consecutive samples (minute scale).
3. Produce a weighted value at each timestamp.

---

## 3. CLI outputs

Main entry is `python -m analyzer.log_metrics -l <log_dir> -o <output_dir> -m <metric1> <metric2> ...`.

For each metric passed to `-m`, the tool first preprocesses node logs by running `GlobalMetricsStats.preprocessing`, which creates `metrics.pq` cache files under each node directory. It then selects representative nodes using the two-layer percentile approach (Section 2.2), plots one time-series figure, and saves one PDF.

### Output files

Node cache files are written as `<log_dir>/<node_ip>/metrics.pq`. Metric figures are written as `<output_dir>/<sanitized_metric_name>.pdf`.

### Terminal output

Runtime output usually includes multiprocessing progress bars (`tqdm`) and one `paths_and_tags` print before plotting. If metric coverage is low, a warning appears: `警告: 在 <log_dir> 中仅找到 <valid>/<total> 个节点具有指标 <metric_name>`. This warning fires when fewer than 80 % of nodes contain that metric.

---

## 4. Input parsing rules

The parser accepts only lines matching `<timestamp>, <module>, Group, {k1: v1, k2: v2, ...}`.

Constraints are strict. `timestamp` is parsed as integer milliseconds, `module` must match `[0-9a-z_]+` (Section 2.1), and each metric value must parse to float. Non-matching lines are silently ignored. This means final output quality depends directly on whether `metrics.log` follows the expected line format.

---

## 5. Plot semantics

The x-axis is wall-clock time (`HH:MM`), and the y-axis is metric value with a zero lower bound. Each selected node (from the second percentile layer, Section 2.2) is one line, and optional `extra_nodes` add extra lines.

Unit handling depends on the `nano_seconds` parameter. By default values stay in raw log units. If `nano_seconds=True`, values are divided by `1e9` and labeled as seconds (`s`).

Time filtering uses `time_range="HH:MM-HH:MM"` in local time, supporting cross-midnight ranges.

---

## 6. Text-table outputs

`analysis.py` provides run-to-run comparison between two log directories.

### `compare_logs(...)`

The printed table has four columns: `指标`, `倍数`, `基准值`, and `对比值`, where `倍数 = compare / base`.

Processing logic is straightforward. It selects one global-percentile node per run (default `global_p=90`), skips metrics missing in either run or with selected value zero, sorts by ratio descending, and prints skipped metric names before the final table.

### PrettyTable helper printers

Two helper functions exist in `parse_metrics.py`: `print_node_stats_table` and `print_global_stats_table`. They format node-level percentile tables and global percentile tables respectively, but they are not called automatically by `__main__.py`.

---

## 7. Interpretation checklist

When reading one PDF, verify five points in order.

1. Is this a raw metric or a derived `.m1` rate metric (Section 2.3)?
2. Is the y-axis in raw units or nanoseconds converted to seconds?
3. Are legend labels (`P0/P50/P100`) understood as node-rank buckets, not per-timestamp percentile curves (Section 2.2)?
4. Is the metric-coverage warning (`valid/total`) acceptable?
5. If results look odd, does the original log format and module/key naming match parser expectations (Section 2.1, Section 4)?
