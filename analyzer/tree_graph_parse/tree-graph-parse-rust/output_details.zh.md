# tree-graph-parse-rust 输出说明

本文档说明 `analyzer/tree_graph_parse/tree-graph-parse-rust` 的功能、输出含义以及正确解读方式。

## 1. 项目作用

`tree-graph-parse-rust` 用于解析 Conflux 区块插入日志，重建区块图结构，并估算在对抗条件下每个区块达到安全确认所需的时间。工作区分为两部分：`tree-graph-parse-rust/` 提供核心 Rust 逻辑与二进制程序，`python-wrapper/` 提供 PyO3 封装并以 `tg_parse_rpy` 暴露给 Python。

---

## 2. 背景概念

本节定义文档后续使用的 Conflux 协议术语。后续章节假设读者已熟悉这些定义。

### 2.1 DAG、树图与 referee

Conflux 区块构成有向无环图（DAG）。每个区块有且仅有一条 **parent（父）** 链接，以及零条或多条 **referee（引用）** 链接。仅由 parent 链接构成的子图是一棵树，称为"树图"（tree graph）；referee 链接提供跨分支引用以提升吞吐量，但不改变树结构。

### 2.2 Pivot 链

**Pivot 链** 是树图的主干。从 genesis（创世区块）出发，每一层选择子树规模最大的子节点（GHAST 规则 — Greedy Heaviest Adaptive SubTree），由此形成一条从创世到最新区块的链。Pivot 链决定了区块排序和最终性。

### 2.3 Epoch

**Epoch** 是与某个 pivot 区块关联的区块集合。具体地说，一个 pivot 区块的 epoch 包含所有可从该 pivot 区块经 parent 或 referee 链接到达、且尚未被分配给更早 epoch 的区块。Epoch 的大小和时间间隔是衡量确认时间的重要依据。

### 2.4 确认风险（Confirmation Risk）

在公共区块链中，当前 pivot 链上的区块仍有可能被拥有足够算力的攻击者通过构建竞争子树而推翻。**确认风险**（confirmation risk）指在给定观测时刻，假设攻击者控制总算力的指定比例（`adv_percent`）时，这种推翻成功的估计概率。

### 2.5 对抗模型（负二项分布 + 随机游走）

确认风险的估计由两个概率部分组合而成。

第一部分建模**攻击者可能秘密挖出了多少区块**。当诚实网络为某个 pivot 区块累积了 `m` 次子树优势增量后，攻击者的隐藏区块数服从以 `m` 和攻击者算力比例为参数的负二项分布。

第二部分建模**攻击者能否追上诚实链**。如果攻击者已秘密挖出 `k` 个区块而诚实网络已建立 `m` 的优势，问题退化为一维随机游走：攻击者需要追回 `m - k` 的差距，其中每个后续区块以 `1 - adv_percent` 的概率为诚实区块，以 `adv_percent` 的概率为攻击区块。将所有合理 `k` 值上的概率加总即得总确认风险。

### 2.6 核心输出字段：`time_offset`、`m`、`k`

`time_offset` 是从区块创建到观测时刻的经过时间。随着 `time_offset` 增大，诚实区块不断累积，确认风险随之降低。

`m` 是在 `time_offset` 处观测到的 pivot 区块子树优势增量次数，本质上是诚实网络挖矿进度的代理指标。

`k` 是累积风险刚好穿过阈值时对应的攻击者区块数，供计算内部使用。

在给定风险阈值下，`time_offset` 越小意味着确认越快；`m - k` 差距越大意味着安全边际越高。

### 2.7 序列搜索（阈值扫描）

调用 `confirmation_risk(block, adv_percent, risk_threshold)` 时，代码从小到大扫描递增的 `time_offset` 值，在每一步计算综合风险。这一过程称为**序列搜索**（sequential search），在风险首次低于指定阈值时停止。为了避免数值退化，风险值在比较前会被设置下界（约 `1e-12`），确保不会退化为精确的 0。

---

## 3. 输入与加载流程

加载入口是 `src/graph.rs` 中的 `Graph::load(file_or_path)`，路径处理由 `src/load.rs` 中的 `open_conflux_log(...)` 完成。

支持四类输入。

1. 直接传入 `*.log.new_blocks` 文件。
2. 传入目录，目录中恰好有一个 `*.log.new_blocks`。
3. 直接传入 `*.conflux.log` 文件，程序会自动 grep `new block inserted into graph` 生成 `<file>.new_blocks`。
4. 传入目录，目录中恰好有一个 `*.conflux.log`，同样会自动生成 `.new_blocks`。

目录中若出现多个候选文件会报错。解析时也只消费包含目标关键字且字段完整（height/hash/parent/referees/timestamp/tx_count/block_size）的日志行。

---

## 4. 输出前的内部派生计算

原始区块解析完成后，`GraphComputer::finalize()` 会补齐后续分析依赖的派生字段。

它会建立父子关系、计算每个区块的 `subtree_size`、按子树规模对子节点排序、从 genesis 沿最大子树形成 pivot 链（第 2.2 节）、基于 referee 遍历标注 epoch 归属（`epoch_block`、`epoch_set`，第 2.3 节）、通过 bitmap 计算 `past_set_size`，并构建 `subtree_adv_series` — 在每个 pivot 区块处记录"最优子节点子树规模减最强兄弟子树规模"的时间序列，该序列即为确认风险估计中使用的 `m` 序列（第 2.5 节）。

---

## 5. 二进制程序输出

### 5.1 `compute_confirmation`

实现文件是 `src/bin/compute_confirmation.rs`。

当前版本从硬编码路径加载单图。对每个 pivot 链区块（除 genesis）打印区块级字段（`height`、`subtree_size`、`past_set`、`epoch_span`、`avg_span`），其中 `epoch_span` 是该区块与其 epoch 内最后一个区块之间的时间差（第 2.3 节），`avg_span` 是截至该区块的平均 epoch 时间。接下来在攻击者算力 `{10, 15, 20, 25, 30}` 百分比与风险阈值 `{1e-4 .. 1e-8}` 的组合下，输出由第 2.7 节序列搜索得到的 `(time_offset, m, k)` 元组。最后打印不同参数组合下的平均确认时间和总运行耗时。

### 5.2 `analyze_all_nodes`

实现文件是 `src/bin/analyze_all_nodes.rs`。

该程序从硬编码根目录递归查找 `conflux.log.new_blocks`，并行加载图，然后打印匹配文件数与成功加载数。之后它会并行执行 `avg_confirm_time(10, 1e-6)`。

当前限制是：它会执行计算，但不会把每个图的平均确认时间结果打印或聚合输出，因此终端结果主要是发现/加载计数。

---

## 6. Rust 库 API 输出

核心对象是 `Graph`。

`pivot_chain()` 返回从 genesis 开始的 pivot 链（第 2.2 节）。`epoch_span(block)` 与 `avg_epoch_time(block)` 反映区块与其 epoch 内成员的时间关系（第 2.3 节）。`confirmation_risk(block, adv_percent, risk_threshold)` 执行第 2.7 节中描述的序列搜索，返回风险首次低于阈值时的 `(time_offset, m, k, risk)`。`confirmation_risk_series(block, adv_percent)` 返回完整 `(time_offset, risk)` 曲线，不受阈值截断。`avg_confirm_time(adv_percent, risk_threshold)` 返回按 epoch 大小加权的平均确认时间及参与统计区块数。`export_edges(...)` 输出 `parent_hash,child_hash` CSV，`export_indices(...)` 输出 `hash,index` CSV。

---

## 7. Python 包装层输出（`tg_parse_rpy`）

`python-wrapper` 暴露图级接口：`load`、`genesis_block`、`pivot_chain`、`epoch_span`、`avg_epoch_time`、`confirmation_risk`、`avg_confirm_time`。同时也暴露区块级字段：`id`、`height`、`hash`、`parent_hash`、`referee_hashes`、`timestamp`、`log_timestamp`、`tx_count`、`block_size`、`children`、`epoch_block`、`epoch_set`、`past_set_size`、`subtree_size`、`epoch_size`。

类型语义上需特别注意两点。哈希导出为 Python `bytes`（`PyBytes`）而不是十六进制字符串；`confirmation_risk(...)` 在 Python 中返回 `(time_offset, m, k, risk)` 元组或 `None`（当阈值永远无法达到时）。

---

## 8. 实际解读清单

做跨区块或跨样本比较时，建议固定同一组 `adv_percent` 和 `risk_threshold` 以确保可比性。先比较 `time_offset`（越低越快），再看 `m - k` 差距作为安全边际代理（第 2.6 节）。同时关注 `epoch_size` 与 `avg_epoch_time`，因为它们会影响加权平均确认时间。最后应优先确认日志完整性：parent 或 referee 链接（第 2.1 节）缺失会破坏图结构，导致所有下游指标失效。
