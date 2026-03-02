# tree-graph-parse-rust 输出说明

本文档说明 `analyzer/tree_graph_parse/tree-graph-parse-rust` 的功能、输出含义以及正确解读方式。

## 1. 项目作用

`tree-graph-parse-rust` 用于解析 Conflux 区块插入日志，重建区块 DAG/树结构，计算 pivot 与 epoch 关系，并在给定攻击者算力假设下估计确认风险。

工作区可以理解为两部分：`tree-graph-parse-rust/` 提供核心 Rust 逻辑与二进制程序，`python-wrapper/` 提供 PyO3 封装并以 `tg_parse_rpy` 暴露给 Python。

---

## 2. 输入与加载流程

加载入口是 `src/graph.rs` 中的 `Graph::load(file_or_path)`，路径处理由 `src/load.rs` 中的 `open_conflux_log(...)` 完成。

支持四类输入。

1. 直接传入 `*.log.new_blocks` 文件。
2. 传入目录，目录中恰好有一个 `*.log.new_blocks`。
3. 直接传入 `*.conflux.log` 文件，程序会自动 grep `new block inserted into graph` 生成 `<file>.new_blocks`。
4. 传入目录，目录中恰好有一个 `*.conflux.log`，同样会自动生成 `.new_blocks`。

目录中若出现多个候选文件会报错。解析时也只消费包含目标关键字且字段完整（height/hash/parent/referees/timestamp/tx_count/block_size）的日志行。

---

## 3. 输出前的内部派生计算

原始区块解析完成后，`GraphComputer::finalize()` 会补齐后续分析依赖的派生字段。

它会建立父子关系、计算 `subtree_size`、按子树规模对子节点排序、从创世按 `max_child` 形成 pivot 链、基于 referee 标注 epoch 归属（`epoch_block`, `epoch_set`）、通过 bitmap 计算 `past_set_size`，并构建 `subtree_adv_series`（定义为 pivot 节点上的“最优子树规模减最强兄弟子树规模”时间序列）。

---

## 4. 二进制程序输出

### 4.1 `compute_confirmation`

实现文件是 `src/bin/compute_confirmation.rs`。

当前版本从硬编码路径加载单图。对每个 pivot 链区块（除 genesis）打印区块级字段（`height`、`subtree_size`、`past_set`、`epoch_span`、`avg_span`），并在攻击者算力 `{10,15,20,25,30}` 与风险阈值 `{1e-4..1e-8}` 组合下输出 `time_offset`、`m`、`k`。最后还会打印不同参数组合下的平均确认时间和总运行耗时。

单个 `(time_offset, m, k)` 的解读是：`time_offset` 越小，达到阈值越快；通常 `m` 越大且 `k` 越小，安全边际越高。

### 4.2 `analyze_all_nodes`

实现文件是 `src/bin/analyze_all_nodes.rs`。

该程序从硬编码根目录递归查找 `conflux.log.new_blocks`，并行加载图，然后打印匹配文件数与成功加载数。之后它会并行执行 `avg_confirm_time(10, 1e-6)`。

当前限制是：它会执行计算，但不会把每个图的平均确认时间结果打印或聚合输出，因此终端结果主要是发现/加载计数。

---

## 5. Rust 库 API 输出

核心对象是 `Graph`。

`pivot_chain()` 返回从 genesis 开始的 pivot 链。`epoch_span(block)` 与 `avg_epoch_time(block)` 反映区块与其 epoch 内成员的时间关系。`confirmation_risk(block, adv_percent, risk_threshold)` 返回风险首次低于阈值时的 `(time_offset, m, k, risk)`；`confirmation_risk_series(block, adv_percent)` 返回完整 `(time_offset, risk)` 曲线。`avg_confirm_time(adv_percent, risk_threshold)` 返回按 epoch 大小加权的平均确认时间及参与统计区块数。`export_edges(...)` 输出 `parent_hash,child_hash` CSV，`export_indices(...)` 输出 `hash,index` CSV。

---

## 6. Python 包装层输出（`tg_parse_rpy`）

`python-wrapper` 暴露图级接口：`load`、`genesis_block`、`pivot_chain`、`epoch_span`、`avg_epoch_time`、`confirmation_risk`、`avg_confirm_time`。同时也暴露区块级字段：`id`、`height`、`hash`、`parent_hash`、`referee_hashes`、`timestamp`、`log_timestamp`、`tx_count`、`block_size`、`children`、`epoch_block`、`epoch_set`、`past_set_size`、`subtree_size`、`epoch_size`。

类型语义上需特别注意两点。哈希导出为 Python `bytes`（`PyBytes`）而不是十六进制字符串；`confirmation_risk(...)` 在 Python 中返回 `(time_offset, m, k, risk)` 或 `None`。

---

## 7. 确认风险的数学含义

确认风险由两部分组合而成：隐藏恶意区块数量分布（负二项分布）与对抗方领先后随机游走不被追平的概率。`normal_confirmation_risk(adv_percent, m, adv)` 可以理解为尾部失败概率估计。

在序列搜索阶段，风险值会设置下界（约 `1e-12`）以避免数值退化为绝对 0，再进行阈值比较。

---

## 8. 实际解读清单

做跨区块或跨样本比较时，建议固定同一组 `adv_percent` 和 `risk_threshold`。先比较 `time_offset`，再看 `m` 与 `k` 的差距（通常 `m-k` 越大越安全）。同时关注 `epoch_size` 与 `avg_epoch_time`，因为它们会影响加权平均确认时间。最后应优先确认日志完整性，父子/referee 关系缺失会破坏图结构并影响后续结论。
