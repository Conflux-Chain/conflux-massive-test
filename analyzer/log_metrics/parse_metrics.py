from pathlib import Path
from prettytable import PrettyTable
import locale
from matplotlib import pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime
from typing import List, Optional

from .log_data_manage import SingleNodeMetrics, GlobalMetricsStats, NodeMetricsStats
from .utils import create_time_mask
import matplotlib.ticker as ticker



def print_node_stats_table(node_stats, sort_lines=None):
    """
    使用PrettyTable打印节点统计表格，并添加千分位逗号

    参数:
    - node_stats: 每个节点的百分位数统计
    """
    if not node_stats:
        print("没有节点统计数据可显示")
        return

    # 获取所有节点的百分位数键（如p0, p10, p20...）
    sample_node = next(iter(node_stats.values()))
    try:
        p_keys = sorted(sample_node.keys(), key=lambda x: int(
            x[1:])) if sample_node else []
    except:
        p_keys = list(sample_node.keys())

    if not p_keys:
        print("没有百分位数据可显示")
        return

    # 创建表格
    table = PrettyTable()

    # 添加列名
    table.field_names = ["节点"] + p_keys

    # 设置对齐方式
    table.align["节点"] = "l"
    for p_key in p_keys:
        table.align[p_key] = "r"

    # 添加数据行
    node_stat_list = list(node_stats.items())
    if sort_lines is not None:
        node_stat_list.sort(key=sort_lines)

    for node_ip, stats in node_stat_list:
        row = [node_ip]
        for p in p_keys:
            # 添加千分位逗号格式化
            value = stats.get(p, 0)
            formatted_value = locale.format_string(
                "%.2f", value, grouping=True)
            row.append(formatted_value)
        table.add_row(row)

    # 打印表格
    print("\n===== 节点百分位数统计 =====")
    print(table)


def print_global_stats_table(global_stats):
    """
    使用PrettyTable打印全局统计表格，并添加千分位逗号

    参数:
    - global_stats: 每个节点百分位数的全局统计
    """
    if not global_stats:
        print("没有全局统计数据可显示")
        return

    # 获取所有全局百分位数键（如p0, p10, p20...）
    sample_stat = next(iter(global_stats.values()))
    global_p_keys = sorted(sample_stat.keys(), key=lambda x: int(
        x[1:])) if sample_stat else []

    if not global_p_keys:
        print("没有百分位数据可显示")
        return

    # 创建表格
    table = PrettyTable()

    # 添加列名
    table.field_names = ["百分位数"] + global_p_keys

    # 设置对齐方式
    table.align["百分位数"] = "l"
    for p_key in global_p_keys:
        table.align[p_key] = "r"

    # 添加数据行
    for p_key, stats in global_stats.items():
        row = [p_key]
        for p in global_p_keys:
            # 添加千分位逗号格式化
            value = stats.get(p, 0)
            formatted_value = locale.format_string(
                "%.2f", value, grouping=True)
            row.append(formatted_value)
        table.add_row(row)

    # 打印表格
    print("\n===== 全局百分位数统计 =====")
    print(table)


def ms_to_datetime(ms):
    return datetime.fromtimestamp(ms / 1000)


def _plot_metrics_core(metric_name: str,
                       paths_and_tags: list[tuple[str, str]],
                       figsize: tuple[int, int] = (40, 10),
                       time_range: Optional[str] = None,
                       nano_seconds: bool = False,
                       title = None,
                       y_label = None,
                       legend_loc = None,
                       save_fig = None,
                       ):
    """
    绘制指标的核心函数（内部使用）
    
    Args:
        paths_and_tags: [(path, tag), ...] 路径和标签的元组列表
    """
    # 创建图表
    plt.figure(figsize=figsize)
    
    data = []

    # 为每个节点绘制折线图
    for i, (path, tag) in enumerate(paths_and_tags):
        node_metrics = SingleNodeMetrics.load(path)
        timestamps, values = node_metrics.query_metric(metric_name)
        
        if nano_seconds:
            values = values / 1e9
        
        if time_range is not None:
            mask = create_time_mask(time_range, timestamps)
            timestamps = timestamps[mask]
            values = values[mask]

        dates = [ms_to_datetime(ts) for ts in timestamps]

        # 绘制折线图
        data.append(values)
        plt.plot(dates, values, linewidth=2, label=tag)

    # 设置图表标题和轴标签
    if title is None:
        title = f'Compare of {metric_name}'
    plt.title(title, fontsize=24)
    plt.xlabel('Time', fontsize=18)
    if y_label is None:
        y_label = 'Value'
    plt.ylabel(y_label, fontsize=18)

    # 设置y轴从0开始
    plt.ylim(bottom=0)

    # 配置x轴格式使其更可读
    ax = plt.gca()
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
    ax.xaxis.set_major_locator(mdates.MinuteLocator(interval=1))
    if not nano_seconds:
        ax.yaxis.set_major_formatter(ticker.EngFormatter())
    else:
        ax.yaxis.set_major_formatter(ticker.EngFormatter(unit="s"))
    
    plt.xticks(fontsize=14)
    plt.yticks(fontsize=14)

    # 添加网格线
    plt.grid(True, linestyle='--', alpha=0.7)

    # 添加图例
    if legend_loc is None:
        legend_loc = 'upper right'
    plt.legend(fontsize=18, loc=legend_loc)

    # 调整布局
    plt.tight_layout()
    
    if save_fig is not None:
        plt.savefig(save_fig)
        
    # 显示图表
    plt.show()
    
    return data


def plot_metrics_by_pecentiles(log_dir: str,
                               metric_name: str,
                               *,
                               extra_nodes: Optional[List[str]] = None,
                               plot_percentiles: list[int] = list((0, 10, 50, 90, 100)),
                               node_percentile: int = 90,
                               figsize: tuple[int, int] = (40, 10),
                               time_range: Optional[str] = None,
                               nano_seconds: bool = False,
                               title = None,
                               y_label = None,
                               legend_loc = None,
                               save_fig = None,
                               ):
    """
    绘制不同性能水平节点的指标时序图
    
    工作流程：
    1. 计算每个节点的代表值（基于 node_percentile）
    2. 根据代表值排序，选出 plot_percentiles 对应的节点
    3. 绘制选中节点的完整时序数据
    
    参数说明：
        log_dir: 节点日志根目录
        metric_name: 指标名称
            指标名称比较复杂，可以通过 GlobalMetricsStats.collect_metric_ns(log_dir) 取得所有合法值
        
        plot_percentiles: 选择哪些百分位的节点绘制
            例如 [100, 50, 0] 表示最大、中位、最小节点
            
        node_percentile: 节点代表值的计算方式
            90 表示取 P90 值
            
        extra_nodes: 额外指定的节点列表，与节点文件夹名称相同
        time_range: 时间范围过滤，格式 "HH:MM-HH:MM"（本地时间）
        nano_seconds: 是否为纳秒单位（影响显示格式）
        
        # Matplotlib 参数
        figsize: 图表尺寸
        title: 标题
        y_label: Y 轴标签
        legend_loc: 图例位置
        save_fig: 保存路径
    
    示例：
        # 绘制最好、中等、最差三个节点
        plot_metrics_by_percentiles(
            "logs/20260203", 
            "block_latency",
            plot_percentiles=[0, 50, 100]
        )
        
        # 查看指定时段 + 额外节点
        plot_metrics_by_percentiles(
            "logs/20260203",
            "tx_latency", 
            time_range="08:00-10:00",
            extra_nodes=["node_123"]
        )
    """
    
    global_stats = GlobalMetricsStats.load_percentiles(log_dir, node_percentile)
    selected_nodes = global_stats.query_node_stat_at_percentiles(
        metric_name, f"p{node_percentile}", plot_percentiles
    )

    if extra_nodes is None:
        extra_paths_and_tags = list()
    else:
        extra_paths_and_tags = [(Path(f"{log_dir}/{name}").absolute(), name) for name in extra_nodes]

    if selected_nodes is None:
        return


    selected_nodes.reverse()
    tags = [f"P{p}" for p in plot_percentiles]
    tags.reverse()
    
    paths_and_tags = [(path, tag) for tag, (path, _) in zip(tags, selected_nodes)]
    paths_and_tags.extend(extra_paths_and_tags)
    
    return _plot_metrics_core(
        metric_name=metric_name,
        paths_and_tags=paths_and_tags,
        figsize=figsize,
        time_range=time_range,
        nano_seconds=nano_seconds,
        title=title,
        y_label=y_label,
        legend_loc=legend_loc,
        save_fig=save_fig,
    )


def plot_metrics_by_paths(paths: list[str],
                          tags: list[str],
                          metric_name: str,
                          figsize: tuple[int, int] = (40, 10),
                          time_range: Optional[str] = None,
                          nano_seconds: bool = False,
                        #   title = None,
                        #   y_label = None,
                        #   legend_loc = None,
                        #   save_fig = None,
                          ):
    """
    绘制指定路径列表的节点指标图表
    
    Args:
        paths: SingleNodeMetrics 的路径列表
        tags: 对应的标签列表
        metric_name: 指标名称
    """
    if len(paths) != len(tags):
        raise ValueError("paths 和 tags 长度必须一致")
    
    paths_and_tags = list(zip(paths, tags))
    
    return _plot_metrics_core(
        metric_name=metric_name,
        paths_and_tags=paths_and_tags,
        figsize=figsize,
        time_range=time_range,
        nano_seconds=nano_seconds,
        # title=title,
        # y_label=y_label,
        # legend_loc=legend_loc,
        # save_fig=save_fig,
    )


# def plot_metrics_dual_axis(ip, path, metric_name1, metric_name2, figsize=(40, 8)):
#     """
#     为同一IP的不同指标绘制双Y轴折线图

#     参数:
#     ip: IP地址
#     path: 数据文件路径
#     metric_names: 要提取的两个指标名称的列表或元组
#     figsize: 图表大小，默认(40, 10)
#     """
#     metric_names = [metric_name1, metric_name2]

#     # 收集数据
#     data = []
#     for metric in metric_names:
#         data.append(extract_metrics_from_file(ip, path, metric))

#     # 创建图表和两个Y轴
#     fig, ax1 = plt.subplots(figsize=figsize)
#     ax2 = ax1.twinx()

#     # 绘制第一个指标 (左Y轴)
#     _, values1, timestamps1 = data[0]
#     dates1 = [ms_to_datetime(ts) for ts in timestamps1]
#     line1, = ax1.plot(dates1, values1, color='#1f77b4',
#                       linewidth=2, label=f"{metric_names[0]}")

#     # 设置左Y轴
#     ax1.set_xlabel('Time', fontsize=18)
#     ax1.set_ylabel(metric_names[0], color='#1f77b4', fontsize=18)
#     ax1.tick_params(axis='y', labelcolor='#1f77b4', labelsize=14)
#     ax1.set_ylim(bottom=0)  # 从0开始

#     # 绘制第二个指标 (右Y轴)
#     _, values2, timestamps2 = data[1]
#     dates2 = [ms_to_datetime(ts) for ts in timestamps2]
#     line2, = ax2.plot(dates2, values2, color='#ff7f0e',
#                       linewidth=2, label=f"{metric_names[1]}")

#     # 设置右Y轴
#     ax2.set_ylabel(metric_names[1], color='#ff7f0e', fontsize=18)
#     ax2.tick_params(axis='y', labelcolor='#ff7f0e', labelsize=14)
#     ax2.set_ylim(bottom=0)  # 从0开始

#     # 设置图表标题
#     plt.title(f'{metric_names[0]} vs {metric_names[1]}', fontsize=24)

#     # 配置x轴格式
#     ax1.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
#     ax1.xaxis.set_major_locator(mdates.MinuteLocator(interval=1))
#     plt.xticks(rotation=45, fontsize=14)

#     # 添加网格线
#     ax1.grid(True, linestyle='--', alpha=0.7)

#     # 合并图例
#     lines = [line1, line2]
#     labels = [l.get_label() for l in lines]
#     ax1.legend(lines, labels, loc='upper right', fontsize=18)

#     # 调整布局
#     fig.tight_layout()

#     # 显示图表
#     plt.show()
