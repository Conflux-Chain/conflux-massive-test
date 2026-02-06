from analyzer.log_aggregators.aggregators import LogAggregator, default_latency_keys
from analyzer.log_utils.data_utils import Statistics, BlockLatencyType, BlockEventRecordType, Percentile, only_pivot_event
from analyzer.log_utils.data_utils import Block, Transaction

class InMemoryLogAggregator(LogAggregator):
    def __init__(self):
        super().__init__()

    def add_host(self, host_log):
        # accept HostLogReducer-like object
        try:
            self.sync_cons_gap_stats.extend(host_log.sync_cons_gap_stats)
        except Exception:
            pass

        for b in getattr(host_log, 'blocks', {}).values():
            Block.add_or_merge(self.blocks, b)

        for tx in getattr(host_log, 'txs', {}).values():
            Transaction.add_or_merge(self.txs, tx)

        for tx in getattr(host_log, 'txs', {}).values():
            if tx.packed_timestamps and tx.packed_timestamps[0] is not None:
                self.tx_wait_to_be_packed_time.append(tx.packed_timestamps[0] - min(tx.received_timestamps))

        try:
            self.host_by_block_ratio.extend(host_log.by_block_ratio)
        except Exception:
            pass
    def validate(self):
        print("{} nodes in total".format(len(self.sync_cons_gap_stats)))
        print("{} blocks generated".format(len(self.blocks)))

        num_nodes = len(self.sync_cons_gap_stats)

        for block_hash in list(self.blocks.keys()):
            count_sync = self.blocks[block_hash].latency_count(BlockLatencyType.Sync)
            if count_sync != num_nodes:
                print("sync graph missed block {}: received = {}, total = {}".format(block_hash, count_sync, num_nodes))
                del self.blocks[block_hash]

        missing_tx = 0
        unpacked_tx = 0
        for tx_hash in list(self.txs.keys()):
            if self.txs[tx_hash].latency_count() != num_nodes:
                missing_tx += 1
            if self.txs[tx_hash].packed_timestamps[0] is None:
                unpacked_tx += 1

        print("Removed tx count (txs have not fully propagated)", missing_tx)
        print("Unpacked tx count", unpacked_tx)
        print("Total tx count", len(self.txs))

    def stat_sync_cons_gap(self, p:Percentile):
        data = []
        for stat in self.sync_cons_gap_stats:
            data.append(stat.get(p))
        return Statistics(data)

    def generate_latency_stat(self, delete_after_read: bool = True):
        num_nodes = len(self.sync_cons_gap_stats)

        for b in self.blocks.values():
            for t in default_latency_keys():
                latencies = b.get_latencies(t)
                if not latencies:
                    continue
                if only_pivot_event(t) and len(latencies) < int(0.9 * num_nodes):
                    continue
                self.block_latency_stats[t.name][b.hash] = Statistics(latencies)

            for (t_name, latencies) in b.iter_non_default_latencies():
                if not latencies:
                    continue
                if len(latencies) < int(0.9 * num_nodes):
                    continue
                if t_name not in self.block_latency_stats:
                    self.block_latency_stats[t_name] = dict()
                self.block_latency_stats[t_name][b.hash] = Statistics(latencies)

        for tx in self.txs.values():
            if tx.latency_count() == num_nodes:
                self.tx_latency_stats[tx.hash] = Statistics(tx.get_latencies())
            if tx.packed_timestamps and tx.packed_timestamps[0] is not None:
                self.tx_packed_to_block_latency[tx.hash] = Statistics(tx.get_packed_to_block_latencies())

                tx_latency= tx.get_min_packed_to_block_latency()
                if self.largest_min_tx_packed_latency_hash is not None:
                    if self.largest_min_tx_packed_latency_time < tx_latency:
                        self.largest_min_tx_packed_latency_hash = tx.hash
                        self.largest_min_tx_packed_latency_time = tx_latency
                else:
                    self.largest_min_tx_packed_latency_hash = tx.hash
                    self.largest_min_tx_packed_latency_time = tx_latency
                self.min_tx_packed_to_block_latency.append(tx_latency)

            if tx.ready_pool_timestamps and tx.ready_pool_timestamps[0] is not None:
                self.min_tx_to_ready_pool_latency.append(tx.get_min_tx_to_ready_pool_latency())

    # Statistics exposure methods (same interface as existing LogAggregator)
    def stat_block_latency(self, t, p:Percentile):
        data = []
        for block_stat in self.block_latency_stats[t].values():
            v = block_stat.get(p)
            if v is not None:
                data.append(v)
        return Statistics(data)

    def custom_block_latency_keys(self):
        default_latency_key_names = [k.name for k in default_latency_keys()]
        keys = [k for k in self.block_latency_stats if k not in default_latency_key_names]
        keys.sort()
        return keys

    def stat_tx_latency(self, p:Percentile):
        data = []
        for tx_stat in self.tx_latency_stats.values():
            v = tx_stat.get(p)
            if v is not None:
                data.append(v)
        return Statistics(data)

    def stat_tx_packed_to_block_latency(self, p:Percentile):
        data =[]
        for tx_stat in self.tx_packed_to_block_latency.values():
            v = tx_stat.get(p)
            if v is not None:
                data.append(v)
        return Statistics(data)

    def stat_min_tx_packed_to_block_latency(self):
        return Statistics(self.min_tx_packed_to_block_latency)

    def stat_min_tx_to_ready_pool_latency(self):
        return Statistics(self.min_tx_to_ready_pool_latency)

    def stat_tx_ratio(self):
        return Statistics(self.host_by_block_ratio)

    def stat_tx_wait_to_be_packed(self):
        return Statistics(self.tx_wait_to_be_packed_time)

    def get_largest_min_tx_packed_latency_hash(self):
        return self.largest_min_tx_packed_latency_hash
