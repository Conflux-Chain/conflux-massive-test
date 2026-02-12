from typing import List, Tuple, Callable, Optional, Set, TypeVar
import pathlib
import pandas as pd
import numpy as np
import functools
from functools import partial
import multiprocessing as mp
from tqdm.auto import tqdm
import time
from typing import Optional, Set, Tuple, Self
from types import SimpleNamespace
import numpy.typing as npt
from .utils import iter_log_file_items, time_decay_weighted_average, node_paths, parse_metric_name, create_namespace_from_string_set


class SingleNodeMetrics:
    """
    用于存储和查询单个节点时序指标数据的类。
    从日志文件加载数据，并将其优化保存为parquet格式，使用时从parquet格式加载
    """

    def __init__(self, path: pathlib.Path, df: pd.DataFrame):
        """
        初始化SingleNodeMetrics实例

        Args:
            path: 指标数据目录路径
            df: 包含指标数据的DataFrame
        """
        self.df = df
        self.path = path

    @classmethod
    def load(cls, directory_path: str) -> Self:
        """
        从指定目录加载指标数据到内存

        Args:
            directory_path: 指标数据所在的目录路径

        Returns:
            SingleNodeMetrics实例

        Raises:
            FileNotFoundError: 当指定目录中既没有metrics.pq也没有metrics.log文件时
        """
        path = pathlib.Path(directory_path)
        metrics_pq_path = path / "metrics.pq"
        metrics_log_path = path / "metrics.log"

        if metrics_pq_path.exists():
            return cls(path, pd.read_parquet(metrics_pq_path))
        elif metrics_log_path.exists():
            return cls(path, cls.preprocess_log_file(metrics_log_path, metrics_pq_path))
        else:
            raise FileNotFoundError(f"日志文件不存在: {directory_path}")

    @classmethod
    def preprocess_log_file(cls, log_file: pathlib.Path, pq_file: pathlib.Path) -> pd.DataFrame:
        """
        预处理日志文件并存储优化后的parquet格式

        Args:
            log_file: 日志文件路径
            pq_file: 要保存的parquet文件路径

        Returns:
            处理后的DataFrame
        """
        # 创建DataFrame
        df = pd.DataFrame(iter_log_file_items(log_file), columns=[
                          'timestamp', 'module', 'key', 'value'])

        # 优化数据类型
        df['timestamp'] = df['timestamp'].astype(np.int64)
        df['module'] = df['module'].astype('category')
        df['key'] = df['key'].astype('category')
        df['value'] = df['value'].astype(np.float64)

        # 为所有以.count结尾的指标添加.count.m1派生指标
        df = cls._add_count_m1_metrics(df)

        # 创建多级索引以加速查询
        df = df.set_index(['module', 'key']).sort_index()

        # 保存为parquet
        df.to_parquet(
            pq_file,
            engine='pyarrow',
            compression='snappy',  # 平衡压缩率和速度
            index=True
        )

        return df

    @classmethod
    def _add_count_m1_metrics(cls, df: pd.DataFrame) -> pd.DataFrame:
        """
        为所有以.count结尾的指标添加使用时间衰减加权平均的.m1派生指标

        Args:
            df: 原始指标DataFrame

        Returns:
            添加了.m1派生指标的DataFrame
        """
        # 查找所有以.count结尾的key
        count_keys = df[df['key'].str.endswith('.count')]

        # 新行的列表
        new_rows = []

        # 对每个count指标分组处理
        for (module, key), group in count_keys.groupby(['module', 'key'], observed=True):
            # 排序确保时间戳顺序
            group = group.sort_values('timestamp')

            # 获取时间戳和值
            timestamps = group['timestamp'].to_numpy()
            values = group['value'].to_numpy()

            # 计算新值
            new_values = time_decay_weighted_average(timestamps, values)

            # 创建新行
            for ts, val in zip(timestamps, new_values):
                new_rows.append({
                    'timestamp': ts,
                    'module': module,
                    'key': f"{key}.m1",  # 新的key是原key加.m1
                    'value': val
                })

        # 如果有新行，添加到数据框
        if new_rows:
            new_df = pd.DataFrame(new_rows)
            # 优化数据类型
            new_df['timestamp'] = new_df['timestamp'].astype(np.int64)
            new_df['module'] = new_df['module'].astype('category')
            new_df['key'] = new_df['key'].astype('category')
            new_df['value'] = new_df['value'].astype(np.float64)

            # 合并回原数据框
            df = pd.concat([df, new_df], ignore_index=True)

        return df

    def query_metric(self, metric_name: str) -> Tuple[npt.NDArray[np.int64], npt.NDArray[np.float64]]:
        """
        查询指标并返回时间戳和值的NumPy数组

        Args:
            metric_name: 指标名称，格式为'module::key'

        Returns:
            包含时间戳和值的NumPy数组元组
            如果找不到指标，则返回空数组
        """
        metric_df = query_dataframe(self.df, metric_name)
        if metric_df is None:
            return np.array([], dtype=np.int64), np.array([], dtype=np.float64)
        else:
            return metric_df['timestamp'].to_numpy(dtype=np.int64), metric_df['value'].to_numpy(dtype=np.float64)

    def get_all_metric_names(self) -> Set[str]:
        """
        返回所有可用的metric名称

        Returns:
            包含所有指标名称的集合，每个名称格式为'module::key'
        """
        return list_metric_names(self.df)
    
    @classmethod
    def collect_metric_names(cls, path: str) -> List[str]:
        """
        返回文件所有可用的指标名称列表。
        """
        node_metrics = SingleNodeMetrics.load(path)
        return node_metrics.get_all_metric_names()


class NodeMetricsStats:
    """
    处理节点指标百分位数等统计信息的计算、缓存和查询。

    支持的统计信息包括：单个或多个百分位数的值、特定时间片段（指定一分钟）的平均值
    """

    def __init__(self, path: str, df: pd.DataFrame):
        self.df = df
        self.path = path

    @classmethod
    @functools.lru_cache(maxsize=1024)
    def load_percentiles_from_path(cls, path: str, percentile: int) -> Self:
        """
        从指定路径加载单一百分位数的指标数据。

        Args:
            path: 指标数据的存储路径
            percentile: 需要计算的百分位数

        Returns:
            包含请求百分位数数据的NodeMetricPercentiles实例
        """
        node_metrics = SingleNodeMetrics.load(path)
        return cls.load_percentiles(node_metrics, percentiles=(percentile,))

    @classmethod
    def load_percentiles(cls,
                         node_metrics: SingleNodeMetrics,
                         percentiles: Tuple[int, ...] = (50, 95, 99)) -> Self:
        """
        从指标存储中加载多个百分位数的数据。

        Args:
            node_metrics: 包含原始指标数据的SingleNodeMetrics实例
            percentiles: 需要计算的百分位数元组，默认为(50, 95, 99)

        Returns:
            包含请求百分位数数据的NodeMetricPercentiles实例
        """
        # 获取原始DataFrame
        original_df = node_metrics.df.reset_index()

        # 按模块和指标键分组，计算每个组的指定百分位数
        result_df = original_df.groupby(['module', 'key'], observed=True)['value'].agg([
            lambda x: np.percentile(x, p) for p in percentiles
        ])

        # 为结果列命名，使用pXX格式（如p50, p95, p99）
        result_df.columns = [f'p{p}' for p in percentiles]

        return cls(node_metrics.path, result_df)

    @classmethod
    def load_time_slice(cls, path: str, minute_str: str) -> Self:
        """
        加载特定时间片段的指标数据的平均值。

        Args:
            path: 指标数据的存储路径
            minute_str: 时间字符串格式为"HH:MM"，表示需要分析的时间片段

        Returns:
            包含指定时间片段平均值的NodeMetricPercentiles实例
        """
        node_metrics = SingleNodeMetrics.load(path)

        # 获取原始DataFrame
        original_df = node_metrics.df.reset_index()

        # 解析目标时间
        q_hour, q_minute = map(int, minute_str.split(':'))

        # 获取本地时区的偏移量（小时）
        local_timezone_offset = -time.timezone // 3600
        if time.localtime().tm_isdst:
            local_timezone_offset += 1  # 考虑夏令时

        # 将本地时间转换为UTC时间
        utc_hour = (q_hour - local_timezone_offset) % 24
        utc_minute = q_minute

        def mean_in_minute(df: pd.DataFrame) -> float:
            """
            计算特定分钟内数据的平均值，utc_hour 和 utc_minute 从上层函数捕获
            """
            df_datetime = pd.to_datetime(df['timestamp'], unit='ms')

            # 筛选指定小时和分钟的数据
            mask = (df_datetime.dt.hour == utc_hour) & (
                df_datetime.dt.minute == utc_minute)
            filtered_data = df[mask]

            return filtered_data['value'].mean()

        # 对每个模块和指标计算指定时间的平均值
        result_df = original_df.groupby(['module', 'key'], observed=True).apply(
            mean_in_minute
        ).to_frame("time")

        result_df = result_df.dropna()

        return cls(node_metrics.path, result_df)

    def query_metric(self, metric_name: str, stat_name: str) -> Optional[float]:
        """
        查询特定指标的百分位数值。

        Args:
            metric_name: 指标名称，通常格式为'module::key'
            stat_name: 查询的列，如：p50, time

        Returns:
            指标的百分位数值，如果指标不存在则返回None
        """

        if stat_name not in self.df.columns:
            raise ValueError(f"Statistics name {stat_name} not found")

        metric_series = query_dataframe(self.df, metric_name)
        if metric_series is None:
            return None
        else:
            return metric_series[stat_name]

    def all_metric_names(self) -> List[str]:
        """
        返回所有可用的指标名称列表。

        Returns:
            所有可用的指标名称列表，格式为'module::key'
        """
        return list_metric_names(self.df)


class GlobalMetricsStats:
    """
    管理和聚合区块链系统中多个节点的指标数据。
    
    该类提供功能来加载、处理和查询来自多个节点的指标统计信息，
    支持并行处理以提高效率。
    """
    T = TypeVar('T')

    def __init__(self, log_dir: str, node_stats_list: List[NodeMetricsStats]):
        self.node_stats_list = node_stats_list
        self.log_dir = log_dir
        
    @staticmethod
    def for_each_node_parallel(log_dir: str, func: Callable[[str], T], 
                              desc: str = "Process nodes metrics log") -> List[T]:
        """
        使用多进程并行地为每个节点日志执行函数。
        
        参数:
            log_dir: 包含所有节点日志文件的目录（目录组织方式为 <node ip>/metrics.log）
            func: 应用于每个节点路径的函数
            desc: 进度条的描述
            
        返回:
            对每个节点应用函数后的结果列表
        """
        with mp.Pool(processes=mp.cpu_count()) as pool:
            paths: List[str] = node_paths(log_dir)
            return list(tqdm(pool.imap(func, paths), total=len(paths), desc=desc))
        # return [func(path) for path in node_paths(log_dir)]

    @classmethod
    def preprocessing(cls, log_dir: str):
        """
        预处理节点日志指标
        """
        GlobalMetricsStats.for_each_node_parallel(log_dir, SingleNodeMetrics.load, "Pre-process nodes metrics log ")

    @classmethod
    def load_percentiles(cls, log_dir: str, percentile: int) -> Self:
        """
        加载指定百分位数的节点指标统计信息。
        """
    
        return GlobalMetricsStats(log_dir, GlobalMetricsStats._load_percentiles(log_dir, percentile))
    
    @staticmethod
    @functools.lru_cache(maxsize=10)
    def _load_percentiles(log_dir: str, percentile: int) -> Self:
        func = partial(NodeMetricsStats.load_percentiles_from_path, percentile=percentile)
        return GlobalMetricsStats.for_each_node_parallel(log_dir, func)

    @classmethod
    def load_time_slice(cls, log_dir: str, minute_str: str) -> Self:
        """
        加载指定时间片段均值的节点指标统计信息。
        """
    
        return GlobalMetricsStats(log_dir, GlobalMetricsStats._load_time_slice(log_dir, minute_str))

    @staticmethod
    @functools.lru_cache(maxsize=3)
    def _load_time_slice(log_dir: str, minute_str: str) -> List[NodeMetricsStats]:
        func = partial(NodeMetricsStats.load_time_slice, minute_str=minute_str)
        return  GlobalMetricsStats.for_each_node_parallel(log_dir, func)

    def query_node_stat_at_percentiles(self, metric_name: str, stat_name: str, 
                                      global_percentiles: List[float | int]) -> Optional[List[Tuple[str, float]]]:
        """
        查询指定指标的统计信息的全局百分位数
        
        参数:
            metric_name: 指标名称
            stat_name: 统计项名称
            net_percentiles: 要查询的全局百分位数列表
            
        返回:
            选定百分位数对应的节点路径和值的元组列表，如果没有有效节点则返回 None
        """
        result_list: List[Tuple[str, float]] = []

        for node in self.node_stats_list:
            val = node.query_metric(metric_name, stat_name)
            if val is None:
                continue
            result_list.append((node.path, val))

        result_list.sort(key=lambda x: x[1])

        valid_nodes = len(result_list)
        total_nodes = len(self.node_stats_list)
        
        if valid_nodes / total_nodes < 0.8:
            print(f"警告: 在 {self.log_dir} 中仅找到 {valid_nodes}/{total_nodes} 个节点具有指标 {metric_name}")

        if valid_nodes == 0:
            return None

        # 选择对应百分位数的节点
        selected_nodes: List[Tuple[str, float]] = []

        for percentile in global_percentiles:
            # 计算百分位数对应的索引
            index = min(int((percentile / 100) * valid_nodes), valid_nodes - 1)
            # 获取该百分位数对应的单个节点
            selected_nodes.append(result_list[index])

        return selected_nodes

    def all_metric_names(self) -> Set[str]:
        """
        返回所有可用的指标名称集合。
        
        返回:
            格式为'module::key'的指标名称集合
        """
        names: Set[str] = set()
        for node in self.node_stats_list:
            names.update(node.all_metric_names())

        return names
    
    @classmethod
    def collect_metric_ns(cls, log_dir: str) -> SimpleNamespace:
        """
        返回指定路径下所有可用指标的集合
        """
        names: Set[str] = set()
        for names in GlobalMetricsStats.for_each_node_parallel(log_dir, SingleNodeMetrics.collect_metric_names):
            names.update(names)
        
        key_names: Set[str] = set()
        for name in names:
            _module, key = parse_metric_name(name)
            key_names.add(key)
            
        names.update(key_names)
        return create_namespace_from_string_set(names)
        


def query_dataframe(df: pd.DataFrame, metric_name: str) -> Optional[pd.Series]:
    """
    通用的DataFrame查询函数

    参数:
        df: 带有多级索引的DataFrame
        metric_name: 指标名称，格式为"module::key"或仅"key"

    返回:
        查询结果的DataFrame切片, 如果没找到则返回None
    """

    # 解析查询参数
    module, key = parse_metric_name(metric_name)

    # 如果没有指定module，尝试根据key找到唯一的module
    if module is None:
        try:
            # 根据key查找所有匹配的行
            matching_rows = df.xs(key, level='key', drop_level=False)

            # 找出所有唯一的module
            unique_modules = matching_rows.index.get_level_values(
                'module').unique()

            if len(unique_modules) == 0:
                return None
            elif len(unique_modules) > 1:
                raise ValueError(
                    f"Key '{key}' 存在于多个模块中: {list(unique_modules)}")

            # 只有一个module时，赋值给module变量
            module = unique_modules[0]
        except KeyError:
            return None

    # 现在我们有了module和key，使用统一的方式查询
    try:
        result = df.loc[(module, key)]
        return result
    except KeyError:
        return None


def list_metric_names(df: pd.DataFrame) -> set[str]:
    """
    返回所有可用的metric名称，格式为'module::key'的集合
    """

    # 获取多级索引
    index = df.index

    # 创建一个集合存储所有的module::key组合
    metric_names = set()

    # 遍历索引中的所有(module, key)对
    for module, key in index:
        metric_names.add(f"{module}::{key}")

    return metric_names
