import sys
import csv
from concurrent.futures import ThreadPoolExecutor
from typing import Optional
from loguru import logger
from prettytable import PrettyTable
from .stat_latency_map_reduce import BlockLatencyType, BlockEventRecordType, Percentile, Statistics, HostLogReducer, LogAggregator

class Table:
    def __init__(self, header:list):
        self.header = header
        self.rows = []

    def add_row(self, row:list):
        assert len(row) == len(self.header), "row and header length mismatch"
        self.rows.append(row)

    def pretty_print(self):
        table = PrettyTable()
        table.field_names = self.header

        for row in self.rows:
            table.add_row(row)

        print(table)

    def output_csv(self, output_file:str):
        with open(output_file, "w", newline='') as fp:
            writer = csv.writer(fp)
            writer.writerow(self.header)
            for row in self.rows:
                writer.writerow(row)

    @staticmethod
    def new_matrix(name:str):
        header = [name]

        for p in Percentile:
            if p is not Percentile.Min:
                header.append(p.name)

        return Table(header)

    def add_data(self, name:str, data_format:str, data:list):
        self.add_stat(name, data_format, Statistics(data))

    def add_stat(self, name:str, data_format:str, stat:Statistics):
        try:
            row = [name]

            for p in Percentile:
                # skip Min column (header omits it)
                if p is Percentile.Min:
                    continue

                if p in [Percentile.Avg, Percentile.Cnt]:
                    v = stat.get(p)
                else:
                    v = stat.get(p, data_format)

                # represent missing values clearly
                if v is None:
                    row.append("N/A")
                else:
                    row.append(v)

            self.add_row(row)
        except Exception as e:
            try:
                sdict = stat.__dict__
            except Exception:
                sdict = {}
            logger.warning(f"Cannot add stat for '{name}': {e}, {sdict}")

class LogAnalyzer:
    def __init__(self, stat_name:str, log_dir:str, csv_output: Optional[str]=None, storage_db: Optional[str] = None, storage_kwargs: Optional[dict] = None, preserve_db: bool = False):
        self.stat_name = stat_name
        self.log_dir = log_dir
        self.csv_output = csv_output
        self.storage_db = storage_db
        self.storage_kwargs = storage_kwargs
        self.preserve_db = preserve_db

    def analyze(self):
        self.agg = LogAggregator.load(self.log_dir, self.storage_db, storage_kwargs=self.storage_kwargs, preserve_db=self.preserve_db)

        print("{} nodes in total".format(len(self.agg.sync_cons_gap_stats)))
        print("{} blocks generated".format(len(self.agg.blocks)))

        self.agg.validate()
        self.agg.generate_latency_stat()

        table = Table.new_matrix(self.stat_name)

    def analyze_db_only(self):
        """Analyze only by reading existing storage DB (no log parsing).

        Useful for debugging and validating that stored samples match in-memory results.
        """
        if not self.storage_db:
            raise RuntimeError("--read-db requires -d/--storage-db pointing to an existing DB file")

        # import storage lazily to avoid hard dependency in other contexts
        try:
            from .storage import SqliteStorage
        except Exception:
            raise

        storage = SqliteStorage(self.storage_db, **(self.storage_kwargs or {}))
        try:
            # ensure any pending writes are flushed (if DB was recently written to)
            storage.flush()

            # quick DB counts for debugging
            block_count = sum(1 for _ in storage.iter_block_hashes())
            tx_count = sum(1 for _ in storage.iter_tx_hashes())
            print(f"DB contains {block_count} blocks and {tx_count} transactions")

            # build aggregator directly from storage and compute stats
            # select DB-backed aggregator implementation
            self.agg = LogAggregator.load_aggregator(storage)
            # do not delete raw rows when analyzing DB-only
            self.agg.generate_latency_stat(delete_after_read=False)

            print("(db-only) {} nodes in total".format(len(self.agg.sync_cons_gap_stats)))
            print("(db-only) {} blocks generated".format(block_count))

            self.agg.validate()

            table = Table.new_matrix(self.stat_name)

            for t in BlockLatencyType:
                for p in Percentile.node_percentiles():
                    name = "block broadcast latency ({}/{})".format(t.name, p.name)
                    table.add_stat(name, "%.2f", self.agg.stat_block_latency(t.name, p))

            for t in BlockEventRecordType:
                for p in Percentile.node_percentiles():
                    name = "block event elapsed ({}/{})".format(t.name, p.name)
                    table.add_stat(name, "%.2f", self.agg.stat_block_latency(t.name, p))

            for t_name in self.agg.custom_block_latency_keys():
                for p in Percentile.node_percentiles():
                    name = "custom block event elapsed ({}/{})".format(t_name, p.name)
                    table.add_stat(name, "%.2f", self.agg.stat_block_latency(t_name, p))

            if len(self.agg.tx_latency_stats) != 0:
                for p in Percentile.node_percentiles():
                    name = "tx broadcast latency ({})".format(p.name)
                    table.add_stat(name, "%.2f", self.agg.stat_tx_latency(p))

                for p in Percentile.node_percentiles():
                    name_tx_packed_to_block ="tx packed to block latency ({})".format(p.name)
                    table.add_stat(name_tx_packed_to_block, "%.2f", self.agg.stat_tx_packed_to_block_latency(p))

                table.add_stat("min tx packed to block latency", "%.2f", self.agg.stat_min_tx_packed_to_block_latency())
                table.add_stat("min tx to ready pool latency", "%.2f", self.agg.stat_min_tx_to_ready_pool_latency())
                table.add_stat("by_block_ratio", "%.2f", self.agg.stat_tx_ratio())
                table.add_stat("Tx wait to be packed elasped time", "%.2f", self.agg.stat_tx_wait_to_be_packed())

            table.pretty_print()
        finally:
            try:
                storage.close()
            except Exception:
                pass
        for t in BlockLatencyType:
            for p in Percentile.node_percentiles():
                name = "block broadcast latency ({}/{})".format(t.name, p.name)
                table.add_stat(name, "%.2f", self.agg.stat_block_latency(t.name, p))

        for t in BlockEventRecordType:
            for p in Percentile.node_percentiles():
                name = "block event elapsed ({}/{})".format(t.name, p.name)
                table.add_stat(name, "%.2f", self.agg.stat_block_latency(t.name, p))

        for t_name in self.agg.custom_block_latency_keys():
            for p in Percentile.node_percentiles():
                name = "custom block event elapsed ({}/{})".format(t_name, p.name)
                table.add_stat(name, "%.2f", self.agg.stat_block_latency(t_name, p))


        if len(self.agg.tx_latency_stats) != 0:
            #self.agg.stat_tx_latency prints: row: to propagate to P(n) number of nodes, column: Percentage of the transactions.
            for p in Percentile.node_percentiles():
                name = "tx broadcast latency ({})".format(p.name)
                table.add_stat(name, "%.2f", self.agg.stat_tx_latency(p))

            #row: the P(n) time the transaction is packed into a block. Column: Percentage of the transactions.
            for p in Percentile.node_percentiles():
                name_tx_packed_to_block ="tx packed to block latency ({})".format(p.name)
                table.add_stat(name_tx_packed_to_block, "%.2f", self.agg.stat_tx_packed_to_block_latency(p))

            #the first time a transaction is packed to the first time the transaction is geneated.
            table.add_stat("min tx packed to block latency", "%.2f", self.agg.stat_min_tx_packed_to_block_latency())

            #the time between the node receives the tx and the tx first time becomes ready
            table.add_stat("min tx to ready pool latency", "%.2f", self.agg.stat_min_tx_to_ready_pool_latency())

            #colomn: P(n) nodes: percentage of the transactions is received by block.
            table.add_stat("by_block_ratio", "%.2f", self.agg.stat_tx_ratio())

            #colomn: shows the time a transaction from receiving to packing for every node, be aware of the transactions can be packed multiple times.
            #Therefore there may be mutiple values for the same transaction.
            table.add_stat("Tx wait to be packed elasped time", "%.2f", self.agg.stat_tx_wait_to_be_packed())

        block_txs_list = []
        block_size_list = []
        block_timestamp_list = []
        referee_count_list = []
        max_time = 0
        min_time = 10 ** 40
        for block in self.agg.blocks.values():
            block_txs_list.append(block.txs)
            block_size_list.append(block.size)
            block_timestamp_list.append(block.timestamp)
            referee_count_list.append(len(block.referees))
            # Ignore the empty warm-up blocks at the start
            if block.txs > 0:
                ts = block.timestamp
                if ts < min_time:
                    min_time = ts
                if ts > max_time:
                    max_time = ts

        table.add_data("block txs", "%d", block_txs_list)
        table.add_data("block size", "%d", block_size_list)
        table.add_data("block referees", "%d", referee_count_list)

        block_timestamp_list.sort()
        intervals = []
        for i in range(1, len(block_timestamp_list)):
            intervals.append(block_timestamp_list[i] - block_timestamp_list[i-1])
        table.add_data("block generation interval", "%.2f", intervals)

        for p in [Percentile.Avg, Percentile.P50, Percentile.P90, Percentile.P99, Percentile.Max]:
            name = "node sync/cons gap ({})".format(p.name)
            if p is Percentile.Avg:
                table.add_stat(name, None, self.agg.stat_sync_cons_gap(p))
            else:
                table.add_stat(name, "%d", self.agg.stat_sync_cons_gap(p))

        tx_sum = sum(block_txs_list)
        print("{} txs generated".format(tx_sum))
        print("Throughput is {}".format(tx_sum / (max_time - min_time)))
        slowest_tx_latency = self.agg.get_largest_min_tx_packed_latency_hash()
        if slowest_tx_latency is not None:
            print("Slowest packed transaction hash: {}".format(slowest_tx_latency))
        table.pretty_print()
        if self.csv_output is not None:
            table.output_csv(self.csv_output)

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Parameter required: <stat_name> <log_dir> [<csv_output>]")
        sys.exit(1)

    csv_output = None if len(sys.argv) == 3 else sys.argv[3]

    LogAnalyzer(sys.argv[1], sys.argv[2], csv_output).analyze()