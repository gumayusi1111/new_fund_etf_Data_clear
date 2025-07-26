"""
VMA工具函数
===========

提供VMA系统的通用工具函数
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Any, Tuple
import logging
import time
from functools import wraps

class VMAUtils:
    """VMA工具函数集合"""

    @staticmethod
    def format_number(value: float, decimal_places: int = 2) -> float:
        """
        格式化数字到指定小数位

        Args:
            value: 数值
            decimal_places: 小数位数

        Returns:
            格式化后的数值
        """
        if pd.isna(value) or np.isinf(value):
            return np.nan
        return round(float(value), decimal_places)

    @staticmethod
    def safe_divide(numerator: float, denominator: float,
                   default: float = np.nan) -> float:
        """
        安全除法，处理除零和无穷大情况

        Args:
            numerator: 分子
            denominator: 分母
            default: 默认值

        Returns:
            除法结果或默认值
        """
        if pd.isna(numerator) or pd.isna(denominator) or denominator == 0:
            return default

        result = numerator / denominator

        if np.isinf(result) or np.isnan(result):
            return default

        return result

    @staticmethod
    def calculate_percentile_rank(value: float, series: pd.Series) -> float:
        """
        计算数值在序列中的百分位排名

        Args:
            value: 目标数值
            series: 数据序列

        Returns:
            百分位排名(0-100)
        """
        if pd.isna(value) or len(series) == 0:
            return np.nan

        # 移除NaN值
        clean_series = series.dropna()

        if len(clean_series) == 0:
            return np.nan

        # 计算小于等于目标值的数量
        count_le = (clean_series <= value).sum()

        # 计算百分位
        percentile = (count_le / len(clean_series)) * 100

        return round(percentile, 1)

    @staticmethod
    def detect_outliers(series: pd.Series, method: str = 'iqr',
                       threshold: float = 1.5) -> pd.Series:
        """
        检测异常值

        Args:
            series: 数据序列
            method: 检测方法 ('iqr', 'zscore')
            threshold: 阈值

        Returns:
            布尔序列，True表示异常值
        """
        if method == 'iqr':
            Q1 = series.quantile(0.25)
            Q3 = series.quantile(0.75)
            IQR = Q3 - Q1

            lower_bound = Q1 - threshold * IQR
            upper_bound = Q3 + threshold * IQR

            return (series < lower_bound) | (series > upper_bound)

        elif method == 'zscore':
            z_scores = np.abs((series - series.mean()) / series.std())
            return z_scores > threshold

        else:
            raise ValueError(f"不支持的异常值检测方法: {method}")

    @staticmethod
    def sliding_window_apply(series: pd.Series, window: int,
                           func: callable) -> pd.Series:
        """
        滑动窗口应用函数

        Args:
            series: 数据序列
            window: 窗口大小
            func: 应用的函数

        Returns:
            结果序列
        """
        return series.rolling(window=window, min_periods=window).apply(func, raw=False)

    @staticmethod
    def format_timestamp(dt: datetime = None) -> str:
        """
        格式化时间戳

        Args:
            dt: 日期时间对象，None则使用当前时间

        Returns:
            格式化的时间字符串
        """
        if dt is None:
            dt = datetime.now()
        return dt.strftime('%Y-%m-%d %H:%M:%S')

    @staticmethod
    def parse_date_range(start_date: str, end_date: str) -> Tuple[datetime, datetime]:
        """
        解析日期范围

        Args:
            start_date: 开始日期字符串
            end_date: 结束日期字符串

        Returns:
            日期范围元组
        """
        start_dt = pd.to_datetime(start_date)
        end_dt = pd.to_datetime(end_date)
        return start_dt, end_dt

    @staticmethod
    def get_trading_days_count(start_date: datetime, end_date: datetime) -> int:
        """
        计算交易日数量(简化版，不考虑节假日)

        Args:
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            交易日数量
        """
        # 简化计算，实际应该排除节假日
        total_days = (end_date - start_date).days + 1
        weekdays = 0

        current_date = start_date
        while current_date <= end_date:
            if current_date.weekday() < 5:  # 周一到周五
                weekdays += 1
            current_date += timedelta(days=1)

        return weekdays

    @staticmethod
    def memory_usage_mb(df: pd.DataFrame) -> float:
        """
        计算DataFrame内存使用量(MB)

        Args:
            df: DataFrame

        Returns:
            内存使用量(MB)
        """
        return df.memory_usage(deep=True).sum() / 1024 / 1024

    @staticmethod
    def optimize_dtypes(df: pd.DataFrame) -> pd.DataFrame:
        """
        优化DataFrame数据类型以节省内存

        Args:
            df: 原始DataFrame

        Returns:
            优化后的DataFrame
        """
        df_optimized = df.copy()

        # 优化数值列
        for col in df_optimized.select_dtypes(include=[np.number]).columns:
            col_min = df_optimized[col].min()
            col_max = df_optimized[col].max()

            if str(df_optimized[col].dtype).startswith('int'):
                if col_min >= np.iinfo(np.int8).min and col_max <= np.iinfo(np.int8).max:
                    df_optimized[col] = df_optimized[col].astype(np.int8)
                elif col_min >= np.iinfo(np.int16).min and col_max <= np.iinfo(np.int16).max:
                    df_optimized[col] = df_optimized[col].astype(np.int16)
                elif col_min >= np.iinfo(np.int32).min and col_max <= np.iinfo(np.int32).max:
                    df_optimized[col] = df_optimized[col].astype(np.int32)

            elif str(df_optimized[col].dtype).startswith('float'):
                if col_min >= np.finfo(np.float32).min and col_max <= np.finfo(np.float32).max:
                    df_optimized[col] = df_optimized[col].astype(np.float32)

        return df_optimized

    @staticmethod
    def performance_timer(func):
        """
        性能计时装饰器

        Args:
            func: 被装饰的函数

        Returns:
            装饰后的函数
        """
        @wraps(func)
        def wrapper(*args, **kwargs):
            start_time = time.time()
            result = func(*args, **kwargs)
            end_time = time.time()

            logger = logging.getLogger(func.__module__)
            logger.debug(f"{func.__name__} 执行时间: {end_time - start_time:.3f}秒")

            return result
        return wrapper

    @staticmethod
    def validate_data_consistency(df1: pd.DataFrame, df2: pd.DataFrame,
                                key_columns: List[str]) -> Dict[str, Any]:
        """
        验证两个DataFrame数据一致性

        Args:
            df1: 第一个DataFrame
            df2: 第二个DataFrame
            key_columns: 关键列列表

        Returns:
            一致性检查结果
        """
        result = {
            'consistent': True,
            'row_count_match': len(df1) == len(df2),
            'column_match': set(df1.columns) == set(df2.columns),
            'key_differences': []
        }

        # 检查关键列数据一致性
        for col in key_columns:
            if col in df1.columns and col in df2.columns:
                # 合并数据进行比较
                merged = df1.merge(df2, on=['date'], suffixes=('_1', '_2'), how='outer')

                col1 = f"{col}_1"
                col2 = f"{col}_2"

                if col1 in merged.columns and col2 in merged.columns:
                    # 计算差异
                    diff_mask = ~np.isclose(merged[col1], merged[col2],
                                          equal_nan=True, rtol=1e-5)
                    diff_count = diff_mask.sum()

                    if diff_count > 0:
                        result['consistent'] = False
                        result['key_differences'].append({
                            'column': col,
                            'diff_count': int(diff_count),
                            'total_rows': len(merged)
                        })

        return result

    @staticmethod
    def create_summary_stats(df: pd.DataFrame, numeric_columns: List[str]) -> Dict[str, Dict]:
        """
        创建数值列的统计摘要

        Args:
            df: DataFrame
            numeric_columns: 数值列列表

        Returns:
            统计摘要字典
        """
        summary = {}

        for col in numeric_columns:
            if col in df.columns:
                series = df[col].dropna()

                if len(series) > 0:
                    summary[col] = {
                        'count': len(series),
                        'mean': round(series.mean(), 4),
                        'std': round(series.std(), 4),
                        'min': round(series.min(), 4),
                        'max': round(series.max(), 4),
                        'median': round(series.median(), 4),
                        'q25': round(series.quantile(0.25), 4),
                        'q75': round(series.quantile(0.75), 4),
                        'null_count': df[col].isnull().sum()
                    }

        return summary