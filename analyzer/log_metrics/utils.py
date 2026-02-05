import os
import re
import functools
import numpy as np
from numpy.typing import NDArray
from typing import Optional, Generator
from types import SimpleNamespace
import re
from datetime import datetime, time


def iter_log_file_items(log_file: str) -> Generator[tuple[np.int64, str, str, float], None, None]:
    """
    从日志文件中提取指标数据并生成时间戳、模块名、指标名和指标值的元组。

    Args:
        log_file: 日志文件路径

    Yields:
        tuple: 包含(时间戳, 模块名, 指标名, 指标值)的元组
    """
    # 提取指标的正则表达式，匹配格式为: "timestamp, module_name, Group, {metrics}"
    metric_pattern = re.compile(r'(\d+), ([0-9a-z_]+), Group, \{([^}]+)\}')

    with open(log_file, 'r') as f:
        for line in f:
            match = metric_pattern.search(line)
            if not match:
                continue

            timestamp = np.int64(match.group(1))
            module_name = match.group(2)
            metrics_part = match.group(3)
            metrics_items = metrics_part.split(', ')

            for metric_item in metrics_items:
                key_value = metric_item.split(': ')
                if len(key_value) == 2:
                    metric_key, metric_value = key_value
                    metric_key = metric_key.strip()
                    try:
                        metric_value = float(metric_value)
                        yield (timestamp, module_name, metric_key, metric_value)
                    except ValueError:
                        continue  # 跳过无法转换为浮点数的值


@functools.lru_cache(maxsize=4096)
def parse_metric_name(metric_name: str) -> tuple[Optional[str], str]:
    """
    解析指标名称，提取目标模块和目标键。

    Args:
        metric_name: 指标名称，格式可能为 "module::key" 或仅 "key"

    Returns:
        tuple: 包含(目标模块, 目标键)的元组，如果未指定模块则模块为None
    """
    if '::' in metric_name:
        target_module, target_key = metric_name.split('::', 1)
    else:
        target_module, target_key = None, metric_name
    return target_module, target_key


def node_paths(log_dir: str) -> list[str]:
    """
    获取日志目录下所有节点的完整路径列表。

    Args:
        log_dir: 日志目录路径

    Returns:
        list: 节点目录的完整路径列表
    """
    all_node_ips = []
    for node_ip in os.listdir(log_dir):
        node_path = os.path.join(log_dir, node_ip)

        # 检查是否为IP地址目录
        if os.path.isdir(node_path) and re.match(r'\d+\.\d+\.\d+\.\d+', node_ip):
            all_node_ips.append(node_ip)

    return [os.path.join(log_dir, node_ip) for node_ip in all_node_ips]


def time_decay_weighted_average(timestamps: NDArray[np.int64],
                                values: NDArray[np.float64]) -> NDArray[np.float64]:
    """
    计算基于时间衰减的加权平均值。

    使用指数时间衰减为每个值分配权重，时间越近的数据权重越大。

    Args:
        timestamps: 时间戳数组（毫秒）
        values: 对应的值数组

    Returns:
        NDArray: 时间衰减加权平均后的值数组
    """
    timestamps = timestamps
    values = values
    N = len(timestamps)

    # 计算差分序列
    value_differences = np.zeros_like(values)
    value_differences[0] = values[0]  # 第一个元素保持不变
    value_differences[1:] = values[1:] - values[:-1]  # 其余元素为差值

    # 计算时间差矩阵，单位分钟
    time_diff_matrix = (timestamps - timestamps[:, np.newaxis]) / 60_000

    # 创建下三角掩码（包括对角线）
    lower_triangle_mask = np.tril(np.ones((N, N)), 0).astype(bool)

    # 将不需要计算的上三角部分设置为一个很小的负数
    # 这样 exp 计算时会接近于 0，避免不必要的精确计算
    filtered_time_matrix = time_diff_matrix.copy()
    filtered_time_matrix[~lower_triangle_mask] = -10000  # 足够小的负数使 exp 结果接近于 0

    # 计算指数权重矩阵
    decay_weights = np.exp(filtered_time_matrix)

    # 对每行求和以进行归一化
    weight_row_sums = np.sum(decay_weights, axis=1, keepdims=True)

    # 归一化权重矩阵
    normalized_weights = decay_weights / weight_row_sums

    # 矩阵乘法：归一化的权重矩阵 × 差值向量
    weighted_result = np.dot(normalized_weights, value_differences)

    return weighted_result


def trim_time_window(timestamps: NDArray[np.int64],
                     values: NDArray[np.float64], 
                     prefix_minutes: int = 0,
                     suffix_minutes: int = 0) -> tuple[NDArray[np.int64], NDArray[np.float64]]:
    """
    移除时间序列开头和结尾的数据点。

    Args:
        timestamps: 对应的时间戳数组（毫秒）
        values: 数据值数组
        prefix_minutes: 要移除的开头时间段（分钟），默认为0分钟
        suffix_minutes: 要移除的结尾时间段（分钟），默认为0分钟

    Returns:
        tuple: 包含(过滤后的值数组, 过滤后的时间戳数组)的元组
    """
    if len(timestamps) == 0:
        return timestamps, values

    if prefix_minutes == 0 and suffix_minutes == 0:
        return timestamps, values

    time_mask = np.ones(len(timestamps), dtype=bool)

    # 处理前缀（开头部分）
    if prefix_minutes > 0:
        min_timestamp = np.min(timestamps)
        prefix_cutoff = min_timestamp + prefix_minutes * 60000  # 转换为毫秒
        prefix_mask = timestamps >= prefix_cutoff
        time_mask = time_mask & prefix_mask

    # 处理后缀（结尾部分）
    if suffix_minutes > 0:
        max_timestamp = np.max(timestamps)
        suffix_cutoff = max_timestamp - suffix_minutes * 60000  # 转换为毫秒
        suffix_mask = timestamps <= suffix_cutoff
        time_mask = time_mask & suffix_mask

    # 应用掩码筛选数据
    filtered_values = values[time_mask]
    filtered_timestamps = timestamps[time_mask]

    return filtered_timestamps, filtered_values


def create_time_mask(time_range: str, timestamps: NDArray[np.int64]):
    """
    创建一个布尔掩码，标识哪些时间戳落在给定的时间区间内
    
    参数:
    time_range (str): 时间区间，格式为 "HH:MM-HH:MM"，例如 "18:34-19:25"
    timestamps (numpy.ndarray): 时间戳数组，格式为 numpy 数组的 datetime64 对象
    
    返回:
    numpy.ndarray: 布尔掩码，True 表示时间戳在区间内，False 表示不在区间内
    """
    # 解析时间区间
    start_str, end_str = time_range.split('-')
    start_hour, start_minute = map(int, start_str.split(':'))
    end_hour, end_minute = map(int, end_str.split(':'))
    
    start_time = time(start_hour, start_minute)
    end_time = time(end_hour, end_minute)
    
    # 将时间戳转换为 datetime 对象
    dt_timestamps = np.array([datetime.fromtimestamp(ts/1000) for ts in timestamps])
    
    # 创建一个函数来检查时间是否在区间内
    def is_in_range(dt) -> bool:
        t = dt.time()
        if start_time <= end_time:
            return start_time <= t <= end_time
        else:  # 跨越午夜的情况
            return t >= start_time or t <= end_time
    
    # 应用函数到每个时间戳
    mask = np.array([is_in_range(dt) for dt in dt_timestamps])
    
    return mask

def sanitize_metric_name(key):
    # 替换非字母数字字符为下划线
    sanitized = re.sub(r'[^a-zA-Z0-9]', '_', key)
    # 确保不以数字开头
    if sanitized and sanitized[0].isdigit():
        sanitized = '_' + sanitized
    # 处理空字符串情况
    if not sanitized:
        sanitized = '_'
    return sanitized

def create_namespace_from_string_set(string_set: set[str]) -> SimpleNamespace:
    # 创建字典，键是清理后的变量名，值是原始字符串
    attr_dict = {sanitize_metric_name(s): s for s in string_set}
    
    # 创建并返回SimpleNamespace
    return SimpleNamespace(**attr_dict)
