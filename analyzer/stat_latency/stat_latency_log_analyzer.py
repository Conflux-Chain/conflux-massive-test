import sys
from typing import Optional

from analyzer.log_loaders.log_loaders import LogDirectoryLoader
from analyzer.log_utils.data_utils import (
    BlockEventRecordType,
    BlockLatencyType,
    Percentile,
    Statistics,
)
from analyzer.log_utils.table import Table


class LogAnalyzer:
    def __init__(self, stat_name: str, log_dir: str, csv_output: Optional[str] = None):
        self.stat_name = stat_name
        self.log_dir = log_dir
        self.csv_output = csv_output

    def _generate_report(self):
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
                name_tx_packed_to_block = "tx packed to block latency ({})".format(p.name)
                table.add_stat(name_tx_packed_to_block, "%.2f", self.agg.stat_tx_packed_to_block_latency(p))

            table.add_stat("min tx packed to block latency", "%.2f", self.agg.stat_min_tx_packed_to_block_latency())
            table.add_stat("min tx to ready pool latency", "%.2f", self.agg.stat_min_tx_to_ready_pool_latency())
            table.add_stat("by_block_ratio", "%.2f", self.agg.stat_tx_ratio())
            table.add_stat("Tx wait to be packed elasped time", "%.2f", self.agg.stat_tx_wait_to_be_packed())

        block_txs_list = []
        block_size_list = []
        block_timestamp_list = []
        referee_count_list = []
        max_time = 0
        min_time = 10**40
        for block in self.agg.blocks.values():
            block_txs_list.append(block.txs)
            block_size_list.append(block.size)
            block_timestamp_list.append(block.timestamp)
            referee_count_list.append(len(block.referees))
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
            intervals.append(block_timestamp_list[i] - block_timestamp_list[i - 1])
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

    def analyze(self):
        self.agg = LogDirectoryLoader(self.log_dir).load()

        self.agg.validate()
        self.agg.generate_latency_stat()
        self._generate_report()


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Parameter required: <stat_name> <log_dir> [<csv_output>]")
        sys.exit(1)

    csv_output = None if len(sys.argv) == 3 else sys.argv[3]
    LogAnalyzer(sys.argv[1], sys.argv[2], csv_output).analyze()
