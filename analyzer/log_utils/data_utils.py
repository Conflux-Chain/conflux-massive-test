#!/usr/bin/env python3

import dateutil.parser
import enum
import re
from dataclasses import dataclass
from typing import Optional


def parse_value(log_line: str, prefix: Optional[str], suffix: Optional[str]):
    start = 0 if prefix is None else log_line.index(prefix) + len(prefix)
    end = len(log_line) if suffix is None else log_line.index(suffix, start)
    return log_line[start:end]


def parse_log_timestamp(log_line: str):
    prefix = None if log_line.find("/conflux.log:") == -1 else "/conflux.log:"
    log_time = parse_value(log_line, prefix, " ")
    return round(dateutil.parser.parse(log_time).timestamp(), 2)


class BlockLatencyType(enum.Enum):
    Receive = 0
    Sync = 1
    Cons = 2


class BlockEventRecordType(enum.Enum):
    HeaderReady = 0
    BodyReady = 1
    SyncGraph = 2
    ConsensusGraphStart = 3
    ConsensusGraphReady = 4
    ComputeEpoch = 5
    NotifyTxPool = 6
    TxPoolUpdated = 7


def only_pivot_event(t) -> bool:
    return type(t) is BlockEventRecordType and t.value >= t.ComputeEpoch.value


@dataclass(frozen=True)
class BlockCustomEventRecordType:
    type_name: str
    stage: int

    @staticmethod
    def parse(text):
        match = re.match(r"custom_([a-zA-Z0-9_]+)_([0-9]+)", text)
        if match:
            type_name = BlockCustomEventRecordType.snake_to_camel(match.group(1))  # 中间段
            stage = int(match.group(2))  # 末尾段
            return BlockCustomEventRecordType(type_name, stage)

        match = re.match(r"gauge_([a-zA-Z0-9_]+)", text)
        if match:
            type_name = BlockCustomEventRecordType.snake_to_camel(match.group(1))  # 中间段
            stage = -1
            return BlockCustomEventRecordType(type_name, stage)

        return None

    @property
    def name(self):
        if self.stage >= 0:
            return f"{self.type_name}{self.stage}"
        else:
            return self.type_name

    @staticmethod
    def snake_to_camel(snake_str):
        components = snake_str.split("_")
        return "".join(word.capitalize() for word in components)


class Transaction:
    def __init__(self, hash: str, timestamp: float, by_block=False, packed_timestamps=None, ready_pool_timstamps=None):
        self.hash = hash
        self.received_timestamps = [timestamp]
        self.by_block = by_block
        self.packed_timestamps = [packed_timestamps]
        self.ready_pool_timestamps = [ready_pool_timstamps]

    @staticmethod
    def receive(log_line: str):
        log_timestamp = parse_log_timestamp(log_line)
        tx_hash = parse_value(log_line, "Sampled transaction ", " ")
        if "in block" in log_line:
            by_block = True
            return Transaction(tx_hash, log_timestamp, by_block)
        elif "in ready pool" in log_line:
            by_block = False
            return Transaction(tx_hash, log_timestamp, by_block, None, log_timestamp)
        elif "in packing block" in log_line:
            by_block = False
            return Transaction(tx_hash, log_timestamp, by_block, log_timestamp)
        else:
            by_block = False
            return Transaction(tx_hash, log_timestamp, by_block)

    @staticmethod
    def add_or_merge(txs: dict, tx):
        if txs.get(tx.hash) is None:
            txs[tx.hash] = tx
        else:
            txs[tx.hash].merge(tx)

    @staticmethod
    def add_or_replace(txs: dict, tx):
        if txs.get(tx.hash) is None:
            txs[tx.hash] = tx
        elif tx.received_timestamps[0] < txs[tx.hash].received_timestamps[0]:
            packed_time = None
            ready_time = None
            if txs[tx.hash].packed_timestamps[0] is not None:
                packed_time = txs[tx.hash].packed_timestamps[0]
            if txs[tx.hash].ready_pool_timestamps[0] is not None:
                ready_time = txs[tx.hash].ready_pool_timestamps[0]
            txs[tx.hash] = tx
            txs[tx.hash].packed_timestamps[0] = packed_time
            txs[tx.hash].ready_pool_timestamps[0] = ready_time

        if tx.packed_timestamps[0] is not None:
            txs[tx.hash].packed_timestamps[0] = tx.packed_timestamps[0]

        if tx.ready_pool_timestamps[0] is not None:
            txs[tx.hash].ready_pool_timestamps[0] = tx.ready_pool_timestamps[0]

    def merge(self, tx):
        self.received_timestamps.extend(tx.received_timestamps)
        if tx.packed_timestamps[0] is not None:
            if self.packed_timestamps[0] is None:
                self.packed_timestamps[0] = tx.packed_timestamps[0]
            else:
                self.packed_timestamps.extend(tx.packed_timestamps)

        if tx.ready_pool_timestamps[0] is not None:
            if self.ready_pool_timestamps[0] is None:
                self.ready_pool_timestamps[0] = tx.ready_pool_timestamps[0]
            else:
                self.ready_pool_timestamps.extend(tx.ready_pool_timestamps)

    def get_latencies(self):
        min_ts = min(self.received_timestamps)
        return [ts - min_ts for ts in self.received_timestamps]

    def get_packed_to_block_latencies(self):
        min_ts = min(self.received_timestamps)
        return [ts - min_ts for ts in self.packed_timestamps if ts is not None]

    def get_min_packed_to_block_latency(self):
        packed_ts = [ts for ts in self.packed_timestamps if ts is not None]
        if not packed_ts:
            return None
        return min(packed_ts) - min(self.received_timestamps)

    def get_min_tx_to_ready_pool_latency(self):
        ready_ts = [ts for ts in self.ready_pool_timestamps if ts is not None]
        if not ready_ts:
            return None
        return min(ready_ts) - min(self.received_timestamps)

    def latency_count(self):
        return len(self.received_timestamps)


class BlockEventRecord:
    def __init__(self, records: dict):
        self.hash = records["hash"]

        BASE = 1_000_000

        self.records = dict()
        self.records[BlockEventRecordType.HeaderReady] = records["header_ready"] / BASE
        self.records[BlockEventRecordType.BodyReady] = (records["body_ready"] - records["header_ready"]) / BASE
        self.records[BlockEventRecordType.SyncGraph] = (records["sync_graph"] - records["body_ready"]) / BASE
        self.records[BlockEventRecordType.ConsensusGraphStart] = (
            records["consensys_graph_insert"] - records["sync_graph"]
        ) / BASE
        self.records[BlockEventRecordType.ConsensusGraphReady] = (
            records["consensys_graph_ready"] - records["consensys_graph_insert"]
        ) / BASE
        if "compute_epoch" in records:
            self.records[BlockEventRecordType.ComputeEpoch] = (
                records["compute_epoch"] - records["consensys_graph_ready"]
            ) / BASE
            self.records[BlockEventRecordType.NotifyTxPool] = (
                records["notify_tx_pool"] - records["compute_epoch"]
            ) / BASE
            self.records[BlockEventRecordType.TxPoolUpdated] = (
                records["tx_pool_updated"] - records["notify_tx_pool"]
            ) / BASE

        custom_records = dict()
        max_stage = 0

        for (key, value) in records.items():
            key_type = BlockCustomEventRecordType.parse(key)
            if key_type is None:
                continue
            if key_type.type_name not in custom_records:
                custom_records[key_type.type_name] = dict()

            if key_type.stage == -1:
                custom_records[key_type.type_name] = value
            else:
                custom_records[key_type.type_name][key_type.stage] = value
                max_stage = max(max_stage, key_type.stage)

        self.custom_records = dict()
        for type_name in custom_records:
            record_entry = custom_records[type_name]
            if type(record_entry) is not dict:
                t = BlockCustomEventRecordType(type_name, -1)
                self.custom_records[t] = record_entry
            else:
                for i in range(max_stage):
                    b = record_entry.get(i + 1)
                    a = record_entry.get(i)
                    if a is None or b is None:
                        break
                    t = BlockCustomEventRecordType(type_name, i)
                    self.custom_records[t] = (b - a) / BASE

    @staticmethod
    def parse(text):
        pattern = r"Block events record ([a-z\s]*)\. (.*)"
        match = re.search(pattern, text)

        if match:
            result = match.group(2).strip()
        else:
            return None

        d = dict()
        for item in result.split(", "):
            (key, value) = tuple(item.split(": "))
            if key not in ["hash", "start_timestamp"]:
                d[key] = int(value)
            else:
                d[key] = value

        return BlockEventRecord(d)


class Block:
    def __init__(self, hash: str, parent_hash: str, timestamp: float, height: int, referees: list):
        self.hash = hash
        self.parent = parent_hash
        self.timestamp = timestamp
        self.height = height
        self.referees = referees

        self.txs = 0
        self.size = 0

        self.latencies = {}
        from analyzer.log_aggregators.aggregators import default_latency_keys

        for t in default_latency_keys():
            self.latencies[t.name] = []

    @staticmethod
    def __parse_block_header__(log_line: str):
        parent_hash = parse_value(log_line, "parent_hash: ", ",")
        height = int(parse_value(log_line, "height: ", ","))
        timestamp = int(parse_value(log_line, "timestamp: ", ","))
        block_hash = parse_value(log_line, "hash: Some(", ")")
        assert len(block_hash) == 66, "invalid block hash length, line = {}".format(log_line)
        referees = []
        for ref_hash in parse_value(log_line, "referee_hashes: [", "]").split(","):
            ref_hash = ref_hash.strip()
            if len(ref_hash) > 0:
                assert len(ref_hash) == 66, "invalid block referee hash length, line = {}".format(log_line)
                referees.append(ref_hash)
        return Block(block_hash, parent_hash, timestamp, height, referees)

    @staticmethod
    def receive(log_line: str, latency_type: BlockLatencyType):
        log_timestamp = parse_log_timestamp(log_line)
        block = Block.__parse_block_header__(log_line)
        if latency_type is not BlockLatencyType.Cons:
            block.txs = int(parse_value(log_line, "tx_count=", ","))
            block.size = int(parse_value(log_line, "block_size=", None))
        block.latencies[latency_type.name].append(round(log_timestamp - block.timestamp, 2))
        return block

    @staticmethod
    def add_or_merge(blocks: dict, block):
        if blocks.get(block.hash) is None:
            blocks[block.hash] = block
        else:
            blocks[block.hash].merge(block)

    def merge(self, another):
        if self.hash != another.hash:
            return

        if self.size == 0 and another.size > 0:
            self.size = another.size

        key_union = self.latencies.keys() | another.latencies.keys()
        for k in key_union:
            if k not in another.latencies:
                continue
            elif k not in self.latencies:
                self.latencies[k] = another.latencies[k]
            else:
                self.latencies[k].extend(another.latencies[k])

    def set_block_event_record(self, record: BlockEventRecord):
        if self.hash != record.hash:
            return

        for t in BlockEventRecordType:
            if t in record.records:
                self.latencies.setdefault(t.name, []).append(record.records[t])

        for t in record.custom_records:
            self.latencies.setdefault(t.name, []).append(record.custom_records[t])

    def latency_count(self, t: BlockLatencyType):
        return len(self.latencies.get(t.name, []))

    def get_latencies(self, t: BlockLatencyType):
        return self.latencies.get(t.name, [])

    def iter_non_default_latencies(self):
        from analyzer.log_aggregators.aggregators import default_latency_keys

        default_latency_key_names = [key.name for key in default_latency_keys()]
        for t in self.latencies:
            if t not in default_latency_key_names:
                yield (t, self.latencies[t])


class Percentile(enum.Enum):
    Min = 0
    Avg = "avg"
    P10 = 0.1
    P30 = 0.3
    P50 = 0.5
    P80 = 0.8
    P90 = 0.9
    P95 = 0.95
    P99 = 0.99
    P999 = 0.999
    Max = 1
    Cnt = "cnt"

    @staticmethod
    def node_percentiles():
        for p in Percentile:
            if p != Percentile.Cnt:
                yield p


class Statistics:
    def __init__(self, data: list, avg_ndigits=2, sort=True):
        if data is None or len(data) == 0:
            return

        if sort:
            data.sort()

        data_len = len(data)

        for p in Percentile:
            if p is Percentile.Avg:
                value = sum(data) / data_len
                if avg_ndigits is not None:
                    value = round(value, avg_ndigits)
            elif p is Percentile.Cnt:
                value = data_len
            else:
                value = data[int((data_len - 1) * p.value)]

            self.__dict__[p.name] = value

    def get(self, p: Percentile, data_format: Optional[str] = None):
        result = self.__dict__[p.name]

        if data_format is not None:
            result = data_format % result

        return result
