import argparse
from pathlib import Path
from .log_data_manage import GlobalMetricsStats
from .utils import sanitize_metric_name
from .parse_metrics import plot_metrics_by_pecentiles


if __name__ == "__main__":    
    parser = argparse.ArgumentParser(description="绘制节点指标图表")
    
    parser.add_argument(
        "-l", "--log-path",
        type=str,
        required=True,
        help="日志节点目录路径"
    )
    
    parser.add_argument(
        "-o", "--output-path",
        type=str,
        required=True,
        help="输出目录路径"
    )
    
    parser.add_argument(
        "-m", "--metrics",
        type=str,
        nargs="*",
        default=[],
        help="绘制指标列表"
    )
    
    args = parser.parse_args()
    
    
    path = args.log_path
    output_path = args.output_path
    
    Path(output_path).mkdir(parents=True, exist_ok=True)
    GlobalMetricsStats.preprocessing(path)
            
    # TPS
    for metric in args.metrics:
        sanitized_metric = sanitize_metric_name(metric)
        plot_metrics_by_pecentiles(path, metric, figsize=(12, 6), save_fig = f"{output_path}/{sanitized_metric}.pdf")
