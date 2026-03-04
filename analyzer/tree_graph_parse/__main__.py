import argparse

import numpy as np
from .tg_parse_rpy import RustGraph
from .analyze_rust_graph import load_network_result, median_graph, worst_graph, describe_blocks, best_graph
from .plot import plot_percentiles

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="绘制节点指标图表")
    
    parser.add_argument(
        "-l", "--log-path",
        type=str,
        required=True,
        help="日志根目录路径"
    )
    
    parser.add_argument(
        "-o", "--output-path",
        type=str,
        required=True,
        help="Figure 输出目录路径"
    )
    
    args = parser.parse_args()
    
    path = args.log_path
    output_path = args.output_path

    
    net_info = load_network_result(path)
    pathes, confirm_times, blocks = net_info
    print(f"Network avg time {confirm_times.mean(): .1f} median {np.median(confirm_times): .1f} with {blocks[2].mean(): .1f} blocks confirmed")
    

    
    print("\n==== The best node ====")
    print(describe_blocks(best_graph(net_info)))
    
    print("\n==== The median node ====")
    print(describe_blocks(median_graph(net_info)))
    
    print("\n==== The worst node ====")
    print(describe_blocks(worst_graph(net_info)))
    

    
    plot_percentiles(confirm_times, save_fig=f"{output_path}/confirm_delay.pdf")
    plot_percentiles(confirm_times, max_percentile = 95, save_fig=f"{output_path}/confirm_delay_no_tail.pdf")
