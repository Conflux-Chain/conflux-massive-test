import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional

from analyzer.log_utils.data_utils import (
    Block,
    BlockEventRecord,
    BlockLatencyType,
    Statistics,
    Transaction,
    parse_value,
)


class NodeLogMapper:
    def __init__(self, log_file: str):
        assert os.path.exists(log_file), f"log file not found: {log_file}"
        self.log_file = log_file

        self.blocks = {}
        self.txs = {}
        self.by_block_ratio = []
        self.sync_cons_gaps = []

    @staticmethod
    def mapf(log_file: str):
        mapper = NodeLogMapper(log_file)
        mapper.map()
        return mapper

    def map(self):
        with open(self.log_file, "r", encoding="UTF-8") as file:
            for line in file:
                self.parse_log_line(line)

    def parse_log_line(self, line: str):
        if "transaction received by block" in line:
            self.by_block_ratio.append(float(parse_value(line, "ratio=", None)))

        if "new block received" in line:
            block = Block.receive(line, BlockLatencyType.Receive)
            Block.add_or_merge(self.blocks, block)

        if "new block inserted into graph" in line:
            block = Block.receive(line, BlockLatencyType.Sync)
            Block.add_or_merge(self.blocks, block)

        if "insert new block into consensus" in line:
            block = Block.receive(line, BlockLatencyType.Cons)
            Block.add_or_merge(self.blocks, block)

        if "Block events record complete" in line or "Block events record partially complete" in line:
            records = BlockEventRecord.parse(line)
            if records is not None and self.blocks.get(records.hash) is not None:
                self.blocks[records.hash].set_block_event_record(records)

        if "Statistics" in line:
            sync_len = int(parse_value(line, "SyncGraphStatistics { inserted_block_count: ", ","))
            cons_len = int(parse_value(line, "ConsensusGraphStatistics { inserted_block_count: ", ","))
            assert sync_len >= cons_len, f"invalid statistics for sync/cons gap, log line = {line}"
            self.sync_cons_gaps.append(sync_len - cons_len)

        if "Sampled transaction" in line:
            tx = Transaction.receive(line)
            Transaction.add_or_replace(self.txs, tx)


class HostLogReducer:
    def __init__(self, node_mappers: Optional[list]):
        self.node_mappers = node_mappers

        self.blocks = {}
        self.txs = {}
        self.sync_cons_gap_stats = []
        self.by_block_ratio = []

    def reduce_one(self, mapper):
        """Reduce a single NodeLogMapper into this reducer and free mapper memory."""
        self.sync_cons_gap_stats.append(Statistics(mapper.sync_cons_gaps))
        self.by_block_ratio.extend(mapper.by_block_ratio)

        for b in mapper.blocks.values():
            Block.add_or_merge(self.blocks, b)

        for tx in mapper.txs.values():
            Transaction.add_or_merge(self.txs, tx)

    def reduce(self):
        if not self.node_mappers:
            return
        for mapper in self.node_mappers:
            self.reduce_one(mapper)

    def dump(self, output_file: str):
        data = {
            "blocks": self.blocks,
            "sync_cons_gap_stats": self.sync_cons_gap_stats,
            "txs": self.txs,
            "by_block_ratio": self.by_block_ratio,
        }

        with open(output_file, "w") as fp:
            json.dump(data, fp, default=lambda o: o.__dict__)

    @staticmethod
    def load(data: dict):
        reducer = HostLogReducer(None)

        for by_block_ratio in data.get("by_block_ratio", []):
            reducer.by_block_ratio.append(by_block_ratio)

        for stat_dict in data.get("sync_cons_gap_stats", []):
            stat = Statistics([1])
            stat.__dict__ = stat_dict
            reducer.sync_cons_gap_stats.append(stat)

        for block_dict in data.get("blocks", {}).values():
            block = Block("", "", 0, 0, [])
            block.__dict__ = block_dict
            reducer.blocks[block.hash] = block

        for tx_dict in data.get("txs", {}).values():
            tx = Transaction("", 0)
            tx.__dict__ = tx_dict
            reducer.txs[tx.hash] = tx

        return reducer

    @staticmethod
    def loadf(input_file: str):
        with open(input_file, "r") as fp:
            data = json.load(fp)
            return HostLogReducer.load(data)

    @staticmethod
    def reduced(log_dir: str, executor: ThreadPoolExecutor):
        futures = []
        for (path, _, files) in os.walk(log_dir):
            for f in files:
                if f == "conflux.log":
                    log_file = os.path.join(path, f)
                    futures.append(executor.submit(NodeLogMapper.mapf, log_file))

        reducer = HostLogReducer([])
        for future in as_completed(futures):
            mapper = future.result()
            reducer.reduce_one(mapper)
            del mapper

        return reducer

