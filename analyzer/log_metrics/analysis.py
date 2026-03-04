
from log_data_manage import GlobalMetricsStats
import locale
from prettytable import PrettyTable

locale.setlocale(locale.LC_ALL, '')

def task_percentile_stats(log_dir1, log_dir2, node_p=99):
    net_info1 = GlobalMetricsStats.load_percentiles(log_dir1, node_p)
    net_info2 = GlobalMetricsStats.load_percentiles(log_dir2, node_p)
    return net_info1, net_info2, f"p{node_p}"


def task_time_window_avg(log_dir1, log_dir2, time1, time2):
    net_info1 = GlobalMetricsStats.load_time_slice(log_dir1, time1)
    net_info2 = GlobalMetricsStats.load_time_slice(log_dir2, time2)
    return net_info1, net_info2, "time"


def compare_logs(net1: GlobalMetricsStats, net2: GlobalMetricsStats, stat_name: str, global_p=90):
    answer = []
    skipped = []

    all_metrics = set()
    all_metrics.update(net1.all_metric_names())
    all_metrics.update(net2.all_metric_names())
    selected_metrics = [
        m for m in all_metrics if "." not in m or m[-3:] == ".m1" or m[-4:-2] == ".p"]

    for metric_name in selected_metrics:
        res1 = net1.query_node_stat_at_percentiles(
            metric_name, stat_name, (global_p,))
        res2 = net2.query_node_stat_at_percentiles(
            metric_name, stat_name, (global_p,))

        if res1 is None or res2 is None:
            skipped.append(metric_name)
            continue

        v1, v2 = res1[0][1], res2[0][1]

        if float(v1) == 0.0 or float(v2) == 0.0:
            skipped.append(metric_name)
            continue

        answer.append((metric_name, v1, v2, v2/v1))

    # 按比例排序
    sorted_by_ratio = sorted(answer, key=lambda x: -x[3])

    print("\n跳过指标")
    for s in skipped:
        print(s)

    print("\n========  比例降序  =======")
    print_compare_table(sorted_by_ratio)
        
def print_compare_table(compare_result: list[tuple[str, float, float, float]]):

    # 创建表格
    table = PrettyTable()

    # 添加列名
    table.field_names = ["指标", "倍数", "基准值", "对比值"]

    # 设置对齐方式
    table.align["指标"] = "l"
    table.align["倍数"] = "c"
    for p_key in ["基准值", "对比值"]:
        table.align[p_key] = "r"


    for metric_name, base, compare, ratio in compare_result:
        row = (
            metric_name,
            locale.format_string("%.2f", ratio, grouping=True),
            locale.format_string("%.2f", base, grouping=True),
            locale.format_string("%.2f", compare, grouping=True),
        )
        table.add_row(row)

    # 打印表格
    print(table)