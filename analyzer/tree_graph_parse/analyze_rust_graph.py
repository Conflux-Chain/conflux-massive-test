import os
import glob
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import List, Optional
from .tg_parse_rpy import RustGraph
from tqdm import tqdm
import numpy as np
from functools import partial
from prettytable import PrettyTable
from numpy.typing import NDArray


def find_files(root_path: str, pattern: str) -> List[str]:
    """
    查找指定路径下所有匹配pattern的文件

    Args:
        root_path: 根路径
        pattern: 文件名模式，例如"*.graph"

    Returns:
        文件路径列表
    """
    matching_files = []

    # 使用glob模式匹配文件
    search_pattern = os.path.join(root_path, "**", pattern)
    matching_files = glob.glob(search_pattern, recursive=True)

    return matching_files


def load_all_graphs(file_paths: List[str], max_workers: Optional[int] = None) -> List[RustGraph]:
    """
    多线程加载所有图文件

    Args:
        file_paths: 文件路径列表
        max_workers: 最大线程数，None表示使用默认值

    Returns:
        加载的RustGraph对象列表
    """
    graphs = []

    # 使用ThreadPoolExecutor进行多线程加载
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # 提交所有加载任务
        future_to_path = {executor.submit(
            RustGraph.load, path): path for path in file_paths}

        # 收集结果
        for future in future_to_path:
            graph = future.result()
            if graph is not None:
                graphs.append(graph)

    return graphs


def _process_file(path, adv_percent, risk):
    return (path, *RustGraph.load(path).avg_confirm_time(adv_percent, risk))


def load_network_result(path, adv_percent: int = 10, risk: float = 1e-6):
    matching_files = find_files(path, "conflux.log.new_blocks")
    func = partial(_process_file, adv_percent=adv_percent, risk=risk)

    # 使用线程池处理文件
    with ThreadPoolExecutor() as executor:
        pathes, confirm_times, confirm_blocks = tuple(
            zip(*executor.map(func, matching_files)))

    return pathes, np.array(confirm_times), np.array(confirm_blocks)


def graph_by_pencentile(network_result, percentile) -> RustGraph:
    pathes, confirm_time, blocks = network_result
    index = percentile_to_index(confirm_time, percentile)
    return RustGraph.load(pathes[index])


def worst_graph(network_result) -> RustGraph:
    return graph_by_pencentile(network_result, 100)


def best_graph(network_result) -> RustGraph:
    return graph_by_pencentile(network_result, 0)


def median_graph(network_result) -> RustGraph:
    return graph_by_pencentile(network_result, 50)


def describe_blocks(graph: RustGraph, adv_percentage=10, risk=1e-6) -> PrettyTable:
    table = PrettyTable()
    table.field_names = ["height", "epoch_size", "total_confirm",
                         "pivot_confirm", "avg_epoch_time", "epoch_span", "subtree_size"]

    confirm_time, confirm_blocks = graph.avg_confirm_time(adv_percentage, risk)
    print(f"Confirm time {confirm_time} with {confirm_blocks}")

    # First part: Detailed block analysis,
    for block in graph.pivot_chain:
        if block.height == 0:
            continue

        confirmation_risk = graph.confirmation_risk(
            block, adv_percentage, risk)
        if confirmation_risk is None:
            break

        pivot_confirm_time, m, k, _ = confirmation_risk
        avg_epoch_time = graph.avg_epoch_time(block)

        table.add_row([
            block.height,
            block.epoch_size,
            f"{pivot_confirm_time + avg_epoch_time: .1f}",
            f"{pivot_confirm_time: .1f}",
            f"{avg_epoch_time: .1f}",
            graph.epoch_span(block),
            block.subtree_size
        ])

    return table


def confirm_time_list(graph: RustGraph, adv_percentage=10, risk=1e-6) -> List[float]:
    res = []
    for block in graph.pivot_chain:
        if block.height == 0:
            continue

        confirmation_risk = graph.confirmation_risk(
            block, adv_percentage, risk)
        if confirmation_risk is None:
            break

        pivot_confirm_time, m, k, _ = confirmation_risk
        avg_epoch_time = graph.avg_epoch_time(block)

        res.append(pivot_confirm_time + avg_epoch_time)
    return res


# def percentile_to_index(data:, percentile: float) -> int:
#     """
#     Given a percentile (0-100), return the corresponding index in the data array.

#     Args:
#         data: numpy array of values
#         percentile: percentile value (0-100)

#     Returns:
#         index: the index corresponding to the percentile
#     """
#     # Calculate the value at the given percentile
#     percentile_value = np.percentile(data, percentile)

#     # Sort the data to find the index
#     data_sorted = np.sort(data)

#     # Find the index where this value would be inserted
#     # This gives us the index in the sorted array
#     index = np.searchsorted(data_sorted, percentile_value, side='left')

#     return index  # pyright: ignore[reportReturnType]

def percentile_to_index(data: NDArray[np.float64], percentile: float) -> int:
    """
    Given a percentile (0-100), return the index in the original array.
    
    Args:
        data: numpy array of values
        percentile: percentile value (0-100)
    
    Returns:
        index: the index in the original (unsorted) array
    """
    # 获取排序后的索引（argsort 返回的是原始数组的索引）
    sorted_indices = np.argsort(data)
    
    # 计算百分位对应的位置
    n = len(data)
    position = int(np.round(percentile / 100 * (n - 1)))
    
    # 返回原始数组中的索引
    return sorted_indices[position]
