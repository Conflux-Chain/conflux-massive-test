from abc import ABC, abstractmethod


def _default_latency_keys():
    # import lazily to avoid circular imports at module import time
    from analyzer.log_utils.data_utils import BlockLatencyType, BlockEventRecordType
    for t in BlockLatencyType:
        yield t
    for t in BlockEventRecordType:
        yield t


def default_latency_keys():
    return set(_default_latency_keys())


class LogAggregator(ABC):
    def __init__(self):
        self.blocks = {}
        self.txs = {}
        self.sync_cons_gap_stats = []

        self.block_latency_stats = dict()
        for t in default_latency_keys():
            self.block_latency_stats[t.name] = dict()

        self.tx_latency_stats = {}
        self.tx_packed_to_block_latency = {}
        self.min_tx_packed_to_block_latency = []
        self.host_by_block_ratio = []
        self.tx_wait_to_be_packed_time = []
        self.min_tx_to_ready_pool_latency = []

        self.largest_min_tx_packed_latency_hash = None
        self.largest_min_tx_packed_latency_time = None

    @abstractmethod
    def add_host(self, host_log):
        raise NotImplementedError

    @abstractmethod
    def validate(self):
        raise NotImplementedError

    @abstractmethod
    def generate_latency_stat(self, delete_after_read: bool = True):
        raise NotImplementedError

    @abstractmethod
    def stat_block_latency(self, t, p):
        raise NotImplementedError

    @abstractmethod
    def custom_block_latency_keys(self):
        raise NotImplementedError

    @abstractmethod
    def stat_tx_latency(self, p):
        raise NotImplementedError

    @abstractmethod
    def stat_tx_packed_to_block_latency(self, p):
        raise NotImplementedError

    @abstractmethod
    def stat_min_tx_packed_to_block_latency(self):
        raise NotImplementedError

    @abstractmethod
    def stat_min_tx_to_ready_pool_latency(self):
        raise NotImplementedError

    @abstractmethod
    def stat_tx_ratio(self):
        raise NotImplementedError

    @abstractmethod
    def stat_tx_wait_to_be_packed(self):
        raise NotImplementedError

    @abstractmethod
    def get_largest_min_tx_packed_latency_hash(self):
        raise NotImplementedError

    @abstractmethod
    def stat_sync_cons_gap(self, p):
        raise NotImplementedError
