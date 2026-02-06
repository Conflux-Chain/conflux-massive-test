from .stat_latency_map_reduce import Statistics, BlockLatencyType, BlockEventRecordType, Percentile, default_latency_keys, only_pivot_event

class DBLogAggregator:
    def __init__(self, storage):
        self.storage = storage

        self.blocks = {}
        self.txs = {}
        self.sync_cons_gap_stats = []

        self.block_latency_stats = dict()
        for t in default_latency_keys:
            self.block_latency_stats[t.name] = dict()

        self.tx_latency_stats = {}
        self.tx_packed_to_block_latency = {}
        self.min_tx_packed_to_block_latency = []
        self.host_by_block_ratio = []
        self.tx_wait_to_be_packed_time =[]
        self.min_tx_to_ready_pool_latency=[]

        self.largest_min_tx_packed_latency_hash=None
        self.largest_min_tx_packed_latency_time=None

    def validate(self):
        # similar validation as in-memory; we have no sync_cons stats when reading DB-only
        num_nodes = len(self.sync_cons_gap_stats)
        for block_hash in list(self.blocks.keys()):
            sync_stat = self.block_latency_stats.get('Sync', {}).get(block_hash, None)
            if sync_stat is None:
                count_sync = 0
            else:
                # Statistics stores Cnt in __dict__ when constructed
                try:
                    count_sync = int(getattr(sync_stat, 'Cnt', sync_stat.__dict__.get('Cnt', 0)))
                except Exception:
                    try:
                        count_sync = int(sync_stat.__dict__.get('Cnt', 0))
                    except Exception:
                        count_sync = 0

            if num_nodes and count_sync != num_nodes:
                print("sync graph missed block {}: received = {}, total = {}".format(block_hash, count_sync, num_nodes))
                del self.blocks[block_hash]

    def add_host(self, host_log):
        # capture only the per-host summary info (sync gaps, by-block ratio)
        try:
            self.sync_cons_gap_stats.extend(host_log.sync_cons_gap_stats)
        except Exception:
            pass
        try:
            self.host_by_block_ratio.extend(host_log.by_block_ratio)
        except Exception:
            pass

    def stat_sync_cons_gap(self, p:Percentile):
        data = []
        for stat in self.sync_cons_gap_stats:
            data.append(stat.get(p))
        return Statistics(data)
    def generate_latency_stat(self, delete_after_read: bool = True):
        num_nodes = len(self.sync_cons_gap_stats)

        # infer node count if not available by sampling stored blocks
        if num_nodes == 0:
            import itertools
            sample_hashes = list(itertools.islice(self.storage.iter_block_hashes(), 200))
            inferred = 0
            for bh in sample_hashes:
                lat_map_tmp = self.storage.get_block_latencies(bh)
                for t in default_latency_keys:
                    inferred = max(inferred, len(lat_map_tmp.get(t.name, [])))
            if inferred > 0:
                num_nodes = inferred

        default_names = [k.name for k in default_latency_keys]

        for block_hash in self.storage.iter_block_hashes():
            lat_map = self.storage.get_block_latencies(block_hash)

            for t in default_latency_keys:
                latencies = lat_map.get(t.name, [])
                if not latencies:
                    continue
                if only_pivot_event(t) and len(latencies) < int(0.9 * num_nodes):
                    continue
                self.block_latency_stats[t.name][block_hash] = Statistics(latencies)

            for t_name, latencies in lat_map.items():
                if t_name in default_names:
                    continue
                if not latencies:
                    continue
                if len(latencies) < int(0.9 * num_nodes):
                    continue
                if t_name not in self.block_latency_stats:
                    self.block_latency_stats[t_name] = dict()
                self.block_latency_stats[t_name][block_hash] = Statistics(latencies)

            try:
                meta = self.storage.get_block_meta(block_hash)
                if meta is not None:
                    b = type('Block', (), {})()
                    b.hash = block_hash
                    b.txs = meta.get('txs', 0)
                    b.size = meta.get('size', 0)
                    b.timestamp = meta.get('timestamp', 0)
                    b.referees = meta.get('referees', [])
                    self.blocks[block_hash] = b
            except Exception:
                pass

            if delete_after_read:
                try:
                    self.storage.delete_block_raw(block_hash)
                except Exception:
                    pass

        # transactions
        for tx_hash in self.storage.iter_tx_hashes():
            received = self.storage.get_tx_received(tx_hash)
            if not received:
                if delete_after_read:
                    try:
                        self.storage.delete_tx_raw(tx_hash)
                    except Exception:
                        pass
                continue

            min_rcv = min(received)
            recv_latencies = [ts - min_rcv for ts in received]
            if len(received) == num_nodes:
                self.tx_latency_stats[tx_hash] = Statistics(recv_latencies)

            packed = self.storage.get_tx_packed(tx_hash)
            if packed:
                packed_latencies = [ts - min_rcv for ts in packed]
                self.tx_packed_to_block_latency[tx_hash] = Statistics(packed_latencies)

                tx_latency = min(packed) - min_rcv
                if self.largest_min_tx_packed_latency_hash is not None:
                    if self.largest_min_tx_packed_latency_time < tx_latency:
                        self.largest_min_tx_packed_latency_hash = tx_hash
                        self.largest_min_tx_packed_latency_time = tx_latency
                else:
                    self.largest_min_tx_packed_latency_hash = tx_hash
                    self.largest_min_tx_packed_latency_time = tx_latency
                self.min_tx_packed_to_block_latency.append(tx_latency)

            ready = self.storage.get_tx_ready(tx_hash)
            if ready:
                self.min_tx_to_ready_pool_latency.append(min(ready) - min_rcv)

            if delete_after_read:
                try:
                    self.storage.delete_tx_raw(tx_hash)
                except Exception:
                    pass

    # same interface methods as InMemory
    def stat_block_latency(self, t, p:Percentile):
        data = []
        for block_stat in self.block_latency_stats[t].values():
            v = block_stat.get(p)
            if v is not None:
                data.append(v)
        return Statistics(data)

    def custom_block_latency_keys(self):
        default_latency_key_names = [k.name for k in default_latency_keys]
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
