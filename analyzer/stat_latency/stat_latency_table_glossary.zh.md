# stat_latency_rs 表格词汇表

本词汇表解释了由 `analyzer/stat_latency/stat_latency_rs` 输出的每种行类型。

## 1) 背景：Conflux 区块处理阶段

本节定义表格各行中使用的 Conflux 协议术语。后续章节假设读者已熟悉这些概念。

### DAG、parent 与 referee

Conflux 区块构成有向无环图（DAG）。每个区块指定一个 **parent（父）** 区块和零个或多个 **referee（引用）** 区块。仅由 parent 链接构成的子图是一棵树；referee 链接提供额外的跨分支引用以提高吞吐量，但不改变树结构。

### Pivot 链与 Epoch

**Pivot 链**是树图的主链，通过在每一层选择子树规模最大的子节点（GHAST 规则）来构建。**Epoch** 是与单个 pivot 区块关联的区块集合 — 从该 pivot 区块经 parent/referee 链接可达、且尚未被分配给更早 epoch 的所有区块。Epoch 计算是 Conflux 共识排序的核心步骤。

### 同步图 (Sync Graph) 与共识图 (Consensus Graph)

Conflux 节点在处理区块时维护两个内部图结构。

**同步图**（sync graph）在区块从网络到达时接收区块：先获取区块头，再下载区块体，两者就绪后将区块插入同步图。

**共识图**（consensus graph）接收从同步图提升的区块，执行完整的共识协议 — 计算 epoch、执行交易、更新最终性。单个节点内的处理流水线因此为：区块头到达 → 区块体下载 → 同步图插入 → 共识图插入 → epoch 计算 → 交易池通知。

`stat_latency_rs` 表格追踪上述每个阶段的耗时。

---

## 2) 如何阅读单行数据

每一行都包含一组固定的统计列：`Avg, P10, P30, P50, P80, P90, P95, P99, P999, Max, Cnt`。

表格的构建分为两个阶段：
1. **单个对象计算**：分析器针对每个对象（通常是区块、交易或节点）计算对应指标的单个数值。
2. **汇总统计**：对所有对象的这些单值进行统计，生成上述分位数结果。

例如，`block broadcast latency (Receive/P95)` 行的含义是：对于每一个区块，计算其达到所有节点时的 P95 `Receive`（接收延迟）；然后，跨所有区块，报告这些 P95 的平均值为 `Avg`，第 10 百分位数为 `P10`，直到 `Max`。`Cnt` 列表示参与该分布统计的对象数量。

---

## 3) 行系列 (Row families)

### A. 区块广播延迟 (Block broadcast latency)

这里有三行紧密相关的指标，括号中的后缀表示第 1 节所述的区块处理流水线中的测量点。

`block broadcast latency (Receive/<node-percentile>)` 测量区块生成后到达第一个（或前几个）节点所需的时间。
`block broadcast latency (Sync/<node-percentile>)` 记录区块接收后到插入同步图（第 1 节）之间的延迟。
`block broadcast latency (Cons/<node-percentile>)` 测量同一区块进入共识图（第 1 节）所需的时间。

这三个指标的单位均为 **秒**。

### B. 区块事件耗时 (Block event elapsed)

这些行追踪区块在单个节点内部流程中的流转情况（参见第 1 节的处理流水线）。每个名称代表一个增量耗时。

`HeaderReady` 是区块头到达的时间。
`BodyReady` 是区块主体下载完成的时间。
`SyncGraph` 是区块到达同步图的时间。
依此类推，其计算逻辑如下（均以秒为单位）：

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

注意：最后三个指标（`ComputeEpoch` 等）仅在节点收集到足够比例的邻居（默认 90%）时才会记录；如果采样过稀疏，这些数据将从区块报告中剔除。

### C. 自定义区块事件耗时 (Custom block event elapsed)

当日志中出现不属于标准广播/事件/交易类别的 key 时，会产生自定义行。解析逻辑支持两种形式：

1. **多阶段时间戳** (`custom_<snake_name>_<n>`)：解析器将 base name 转换为大驼峰（CamelCase），并计算相邻阶段的差值。例如，若有 6 个检查点 `t0..t5`，则生成 5 个耗时行：`Name0 = t1-t0` 到 `Name4 = t5-t4`。
2. **原始数值度量** (`gauge_<snake_name>`)：解析器将其视为原始数值（Gauge），不进行单位换算，仅转换名称格式。

**置信度说明**：解析器的行为和单位转换是**已验证 (verified)** 的；而将各阶段映射到具体的运行时函数则是根据控制流**推断 (inferred)** 的。

观测到的自定义系列：

**Compute0..4**
源自 6 个检查点 (`custom_compute_0..5`)。解析器计算相邻差值并除以 `1,000,000`，因此这些行始终以 **秒** 为单位。
代码推断阶段对应 epoch 计算流水线（第 1 节所述的 epoch 计算步骤）：
1. 执行前准备（初始化 state、组装 Epoch 输入、预检）。
2. 核心交易执行（`process_epoch_transactions`）。
3. 执行后的奖励/PoS 处理。
4. 状态提交（State Commit）及执行承诺（Execution Commitment）写入。
5. 提交后钩子（包括 txpool 通知和边界更新）。

**SyncToCon0..3**
源自 5 个检查点 (`custom_sync_to_con_0..4`)。同样计算差值并除以 `1,000,000`，单位为 **秒**。
代码推断阶段追踪从同步图到共识图的交接（第 1 节）：
1. 同步图标记就绪，并将哈希发送至共识工作线程通道（Channel）。
2. 工作线程接收哈希，等待依赖项/调度就绪。
3. 工作线程进行簿记处理（Bookkeeping）及后继/反向映射调度。
4. 共识层正式开始 `on_new_block` 处理。

**ConWorkerQueue / ConWorkerSuccessors**
这些反映了共识图处理流水线中工作线程的队列或后继节点压力。应视为队列深度或依赖计数类指标，而非耗时时长。

**CmptRecoverRate**
属于 Gauge 类型 (`gauge_cmpt_recover_rate`)。解析器原样复制数值，不进行缩放。单位取决于发送端代码（通常是速率类数值），不要假定其为秒。

---

### D. 交易延迟行 (Transaction latency rows)

本系列包含：`tx broadcast latency`（交易广播延迟）、`tx packed to block latency`（交易打包进块延迟）、`min tx packed to block latency`、`min tx to ready pool latency`、`by_block_ratio`（通过区块获取交易的比例）以及 `Tx wait to be packed elapsed time`。

- `tx broadcast latency`：从最早接收到所有节点接收之间的时间。
- `tx packed to block latency`：从最早接收到被打包进块的时间。
- `by_block_ratio`：采样交易中通过区块传输路径到达的比例。

除 `by_block_ratio`（无单位比例）外，本系列其余指标单位均为 **秒**。

### E. 区块标量行 (Block scalar rows)

包括：`block txs`（块内交易数）、`block size`（区块字节大小）、`block referees`（referee 引用数，即 DAG 中的非父区块引用，第 1 节）和 `block generation interval`（区块产生间隔）。
单位分别为：计数、字节、计数、秒。

### F. 节点同步/共识差距 (Node sync/cons gap rows)

包含 `node sync/cons gap` 的各种分位数统计。
底层采样数值为：`同步图已插入区块头数 - 共识图已插入区块数`，衡量同步图与共识图之间的处理积压（第 1 节）。该指标是一个 **计数值**，表示处理落后程度，而非时间。

---

## 4) 实际解读建议

- **定位瓶颈**：对比 `Receive` vs `Sync` vs `Cons` 的尾部指标（P95/P99），可以定位第 1 节所述区块处理流水线中的瓶颈阶段。
- **流水线阻塞**：较大的 `SyncGraph` 或 `ConsensusGraphStart` 尾部数据表明同步图与共识图之间存在流水线阻塞。
- **执行压力**：较大的 `ComputeEpoch` 或 `Compute*` 尾部数据通常意味着 epoch 计算或交易执行压力大。
- **交易传播方式**：高 `by_block_ratio` 表示交易传播更多依赖区块传输而非直接中继。
- **同步滞后**：高 `node sync/cons gap` 表示共识处理能力跟不上同步获取的速度。
- **自定义单位**：对于 `gauge_*` 系列，始终根据发送端代码确定单位，解析器不保证其为时间单位。