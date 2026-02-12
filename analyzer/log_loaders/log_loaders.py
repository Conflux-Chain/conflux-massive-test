import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional, Type

from analyzer.log_aggregators.aggregators import LogAggregator
from analyzer.stat_latency.stat_latency_map_reduce import HostLogReducer
from analyzer.log_aggregators.in_memory_aggregator import InMemoryLogAggregator


class LogDirectoryLoader:
    def __init__(
        self,
        logs_dir: str,
        max_workers: int = 8,
        aggregator_cls: Type[LogAggregator] = InMemoryLogAggregator,
    ):
        self.logs_dir = logs_dir
        self.max_workers = max_workers
        self.aggregator_cls = aggregator_cls

    def load(self, aggregator_cls: Optional[Type[LogAggregator]] = None) -> LogAggregator:
        agg_cls = aggregator_cls or self.aggregator_cls
        agg = agg_cls()
        executor = ThreadPoolExecutor(max_workers=self.max_workers)

        futures = []
        for (path, _, files) in os.walk(self.logs_dir):
            for f in files:
                if f == "blocks.log":
                    log_file = os.path.join(path, f)
                    futures.append(executor.submit(HostLogReducer.loadf, log_file))

        # process host reducers as they complete to reduce peak memory
        for future in as_completed(futures):
            host = future.result()
            agg.add_host(host)
            del host

        # leave validation and stat generation to caller to avoid duplicate work
        executor.shutdown()

        return agg

