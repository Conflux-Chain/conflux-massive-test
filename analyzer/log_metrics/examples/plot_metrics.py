import argparse
from ..log_data_manage import GlobalMetricsStats
from ..utils import sanitize_metric_name
from ..parse_metrics import plot_metrics_by_pecentiles


if __name__ == "__main__":    
    parser = argparse.ArgumentParser(description="绘制节点指标图表")
    
    parser.add_argument(
        "-l", "--log-path",
        type=str,
        required=True,
        help="日志根目录路径"
    )
    
    parser.add_argument(
        "-e", "--extra-nodes",
        type=str,
        nargs="*",
        default=[],
        help="额外绘制的节点名称（可指定多个）"
    )
    
    args = parser.parse_args()
    
    path = args.log_path
    extra_nodes = args.extra_nodes

    GlobalMetricsStats.preprocessing(path)
    ns = GlobalMetricsStats.collect_metric_ns(path)
    
    def _plot_metrics(metric: str, **kwargs):
        sanitized_metric = sanitize_metric_name(metric)
        # 更多细节参见函数注释
        plot_metrics_by_pecentiles(path, metric, figsize=(12, 6), extra_nodes=extra_nodes, save_fig = f"{sanitized_metric}.pdf", **kwargs)
    
    # TPS
    _plot_metrics(ns.good_tps_m1)
    # 交易池未打包交易数量，交易池满了会有一系列问题，持续增长会有问题
    _plot_metrics(ns.txpool__stat_unpacked_txs)
    # 交易池未打包交易数量，交易池满了会有一系列问题
    _plot_metrics(ns.txpool__stat_ready_accounts)
    # 一个区块的交易打包时间
    _plot_metrics(ns.blockgen__pack_transaction_histo_mean)
    
    # 其他也许有用的参数
    # _plot_metrics(ns.get_block_headers_response_receive_bytes_m1)
    # _plot_metrics(ns.get_block_headers_response_queue_wait_time_p99, nano_seconds=True)
    # _plot_metrics(ns.get_block_headers_send_bytes_m1)
    # _plot_metrics(ns.get_block_headers_queue_wait_time_p99, nano_seconds=True)
    # _plot_metrics(ns.network_system_data__write_m1)
    # _plot_metrics(ns.network_system_data__writable_counter_m1)
    # _plot_metrics(ns.network_system_data__send_queue_size)
