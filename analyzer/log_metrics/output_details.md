# log_metrics Output Guide

This document explains what `analyzer/log_metrics` outputs and how to interpret those outputs correctly.

## Scope

The module has two practical output paths. The main path is figure generation from `__main__.py` plus `parse_metrics.py`. The secondary path is text-table comparison from `analysis.py` and helper printers in `parse_metrics.py`.

---

## 1. CLI outputs

Main entry is `python -m analyzer.log_metrics -l <log_dir> -o <output_dir> -m <metric1> <metric2> ...`.

For each metric passed to `-m`, the tool first preprocesses node logs by running `GlobalMetricsStats.preprocessing`, which creates `metrics.pq` cache files under each node directory. It then selects representative nodes by percentile, plots one time series figure, and saves one PDF.

### Output files

Node cache files are written as `<log_dir>/<node_ip>/metrics.pq`. Metric figures are written as `<output_dir>/<sanitized_metric_name>.pdf`.

### Terminal output

The runtime output usually includes multiprocessing progress bars (`tqdm`) and one `paths_and_tags` print before plotting. If metric coverage is low, it prints a warning like `警告: 在 <log_dir> 中仅找到 <valid>/<total> 个节点具有指标 <metric_name>`. This warning appears when fewer than 80% of nodes contain that metric.

---

## 2. Input parsing rules

The parser accepts only lines matching `<timestamp>, <module>, Group, {k1: v1, k2: v2, ...}`.

Parsing details are strict. `timestamp` is parsed as integer milliseconds, `module` must match `[0-9a-z_]+`, and each metric value must parse to float. Non-matching lines are silently ignored. This means final output quality depends directly on whether `metrics.log` follows the expected line format.

---

## 3. Derived `.m1` metrics

Before plotting or percentile statistics, preprocessing adds derived keys for every metric ending in `.count`. The derived key is `<original>.m1`, and the derived value is a time-decay weighted average over count increments.

Computation happens in three steps.

1. Build increment series as `diff[i] = value[i] - value[i-1]`, with the first point unchanged.
2. Build exponential decay weights from timestamp distance (minute scale).
3. Produce weighted values at each timestamp.

Interpretation is important. `.count` usually represents cumulative counters, while `.count.m1` behaves like a smoothed short-horizon rate/throughput signal.

---

## 4. Meaning of plot labels (`P0/P50/P100`)

`plot_metrics_by_pecentiles` uses two percentile layers.

1. Within each node, it computes one representative value for a metric, defaulting to node-level `p90` (`node_percentile=90`).
2. Across nodes, it sorts nodes by that representative value and picks nodes at requested global percentiles (`plot_percentiles`, default `[0,10,50,90,100]`).

So `P0/P50/P100` in legend means node positions in cross-node ranking by node-level `pXX`. They are not per-timestamp percentile curves.

---

## 5. Plot semantics

The x-axis is wall-clock time (`HH:MM`), and the y-axis is metric value with a zero lower bound. Each selected node is one line, and optional `extra_nodes` add extra lines.

Unit handling depends on `nano_seconds`. By default values stay in raw log units. If `nano_seconds=True`, values are divided by `1e9` and shown as seconds (`s`).

Time filtering uses `time_range="HH:MM-HH:MM"` in local time, including cross-midnight ranges.

---

## 6. Text-table outputs

`analysis.py` provides run-to-run comparison between two log directories.

### `compare_logs(...)`

The printed table has four columns: `指标`, `倍数`, `基准值`, and `对比值`, where `倍数 = compare / base`.

Processing logic is straightforward. It selects one global node percentile per run (default `global_p=90`), skips metrics missing in either run or with selected value zero, sorts by ratio descending, and prints skipped metric names before the final table.

### PrettyTable helper printers

Two helper functions exist in `parse_metrics.py`: `print_node_stats_table` and `print_global_stats_table`. They format node-level percentile tables and global percentile tables respectively, but they are not called automatically by `__main__.py`.

---

## 7. Metric naming rules

Metrics can be queried as explicit `module::key`, or as bare `key` when the key belongs to exactly one module. If the same key exists in multiple modules, bare-key query raises an ambiguity error.

For automation and reproducibility, always use full `module::key`.

---

## 8. Interpretation checklist

When reading one PDF, verify five points in order.

1. Is this raw metric or derived `.m1`?
2. Is y-axis raw unit or nanoseconds converted to seconds?
3. Is legend interpreted as node-rank buckets (not time-point percentiles)?
4. Is metric coverage warning (`valid/total`) acceptable for representativeness?
5. If results look odd, does original log format and module/key naming match parser expectations?
