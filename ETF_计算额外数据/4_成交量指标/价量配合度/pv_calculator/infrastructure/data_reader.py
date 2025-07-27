"""
价量配合度数据读取器
=================

负责从各种数据源读取ETF数据，包括CSV文件、缓存文件等
专为价量配合度计算系统优化
"""

import pandas as pd
import numpy as np
from pathlib import Path
from typing import Optional, Dict, List, Tuple
import logging
from datetime import datetime

from .config import PVConfig

class PVDataReader:
    """价量配合度数据读取器"""

    def __init__(self, config: PVConfig):
        self.config = config
        self.logger = logging.getLogger(__name__)

        # 定义列名映射
        self.column_mapping = {
            'price_columns': ['收盘价', '收盘价(元)', 'close', 'Close', '收盘'],
            'volume_columns': ['成交量(手数)', '成交量', 'volume', 'Volume'],
            'date_columns': ['日期', '交易日期', 'date', 'Date', '时间'],
            'code_columns': ['代码', '股票代码', 'code', 'Code', 'symbol']
        }

    def read_etf_data(self, etf_code: str, threshold: str) -> Optional[pd.DataFrame]:
        """
        读取ETF原始数据

        Args:
            etf_code: ETF代码
            threshold: 门槛类型

        Returns:
            ETF数据DataFrame或None
        """
        try:
            file_path = self.config.get_source_path(threshold, etf_code)

            if not file_path.exists():
                self.logger.warning(f"数据文件不存在: {file_path}")
                return None

            # 读取CSV文件
            df = pd.read_csv(file_path, encoding='utf-8')

            if df.empty:
                self.logger.warning(f"数据文件为空: {file_path}")
                return None

            # 标准化列名
            df = self._standardize_columns(df)

            # 验证必要列
            required_columns = ['收盘价(元)', '成交量(手数)', '日期']
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                self.logger.error(f"缺少必要列 {etf_code}: {missing_columns}")
                return None

            # 数据清洗和预处理
            df = self._preprocess_data(df, etf_code)

            # 添加代码列（如果不存在）
            if '代码' not in df.columns:
                df['代码'] = etf_code

            self.logger.debug(f"成功读取 {etf_code} 数据: {len(df)}行")
            return df

        except Exception as e:
            self.logger.error(f"读取数据失败 {etf_code}: {str(e)}")
            return None

    def read_cache_data(self, etf_code: str, threshold: str) -> Optional[pd.DataFrame]:
        """
        读取缓存数据

        Args:
            etf_code: ETF代码
            threshold: 门槛类型

        Returns:
            缓存数据DataFrame或None
        """
        try:
            cache_path = self.config.get_cache_path(threshold, etf_code)

            if not cache_path.exists():
                return None

            # 读取缓存CSV文件
            df = pd.read_csv(cache_path, encoding='utf-8')

            if df.empty:
                return None

            # 验证缓存数据格式
            expected_columns = [
                'code', 'date', 'pv_corr_10', 'pv_corr_20', 'pv_corr_30',
                'vpt', 'vpt_momentum', 'vpt_ratio',
                'volume_quality', 'volume_consistency',
                'pv_strength', 'pv_divergence', 'calc_time'
            ]

            missing_columns = [col for col in expected_columns if col not in df.columns]
            if missing_columns:
                self.logger.warning(f"缓存数据格式不完整 {etf_code}: 缺少 {missing_columns}")
                return None

            # 确保日期列是datetime类型
            df['date'] = pd.to_datetime(df['date'])

            self.logger.debug(f"成功读取 {etf_code} 缓存数据: {len(df)}行")
            return df

        except Exception as e:
            self.logger.warning(f"读取缓存数据失败 {etf_code}: {str(e)}")
            return None

    def get_data_date_range(self, etf_code: str, threshold: str) -> Optional[Tuple[datetime, datetime]]:
        """
        获取数据的日期范围

        Args:
            etf_code: ETF代码
            threshold: 门槛类型

        Returns:
            (最早日期, 最晚日期) 或 None
        """
        try:
            data = self.read_etf_data(etf_code, threshold)
            if data is None or data.empty:
                return None

            date_series = pd.to_datetime(data['日期'])
            return (date_series.min(), date_series.max())

        except Exception as e:
            self.logger.error(f"获取数据日期范围失败 {etf_code}: {str(e)}")
            return None

    def _standardize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        标准化列名

        Args:
            df: 原始DataFrame

        Returns:
            标准化后的DataFrame
        """
        df_copy = df.copy()

        # 标准化价格列
        for price_col in self.column_mapping['price_columns']:
            if price_col in df_copy.columns:
                df_copy = df_copy.rename(columns={price_col: '收盘价(元)'})
                break

        # 标准化成交量列
        for volume_col in self.column_mapping['volume_columns']:
            if volume_col in df_copy.columns:
                df_copy = df_copy.rename(columns={volume_col: '成交量(手数)'})
                break

        # 标准化日期列
        for date_col in self.column_mapping['date_columns']:
            if date_col in df_copy.columns:
                df_copy = df_copy.rename(columns={date_col: '日期'})
                break

        # 标准化代码列
        for code_col in self.column_mapping['code_columns']:
            if code_col in df_copy.columns:
                df_copy = df_copy.rename(columns={code_col: '代码'})
                break

        return df_copy

    def _preprocess_data(self, df: pd.DataFrame, etf_code: str) -> pd.DataFrame:
        """
        数据预处理

        Args:
            df: 原始DataFrame
            etf_code: ETF代码

        Returns:
            预处理后的DataFrame
        """
        df_copy = df.copy()

        try:
            # 1. 处理日期列 - 支持YYYYMMDD格式
            df_copy['日期'] = pd.to_datetime(df_copy['日期'], format='%Y%m%d', errors='coerce')

            # 2. 处理价格列
            if '收盘价(元)' in df_copy.columns:
                df_copy['收盘价(元)'] = pd.to_numeric(df_copy['收盘价(元)'], errors='coerce')
                # 移除价格为0或负数的记录
                invalid_price_mask = (df_copy['收盘价(元)'] <= 0) | df_copy['收盘价(元)'].isna()
                if invalid_price_mask.any():
                    invalid_count = invalid_price_mask.sum()
                    self.logger.warning(f"{etf_code}: 移除{invalid_count}条无效价格记录")
                    df_copy = df_copy[~invalid_price_mask]

            # 3. 处理成交量列
            if '成交量(手数)' in df_copy.columns:
                df_copy['成交量(手数)'] = pd.to_numeric(df_copy['成交量(手数)'], errors='coerce')
                # 成交量为负数设为0
                negative_volume_mask = df_copy['成交量(手数)'] < 0
                if negative_volume_mask.any():
                    negative_count = negative_volume_mask.sum()
                    self.logger.warning(f"{etf_code}: 修正{negative_count}条负成交量记录")
                    df_copy.loc[negative_volume_mask, '成交量(手数)'] = 0

                # 成交量NaN设为0
                nan_volume_mask = df_copy['成交量(手数)'].isna()
                if nan_volume_mask.any():
                    nan_count = nan_volume_mask.sum()
                    self.logger.warning(f"{etf_code}: 修正{nan_count}条NaN成交量记录")
                    df_copy.loc[nan_volume_mask, '成交量(手数)'] = 0

            # 4. 移除日期无效的记录
            invalid_date_mask = df_copy['日期'].isna()
            if invalid_date_mask.any():
                invalid_count = invalid_date_mask.sum()
                self.logger.warning(f"{etf_code}: 移除{invalid_count}条无效日期记录")
                df_copy = df_copy[~invalid_date_mask]

            # 5. 按日期排序并去重
            df_copy = df_copy.sort_values('日期', ascending=True).drop_duplicates(subset=['日期'])

            # 6. 重置索引
            df_copy = df_copy.reset_index(drop=True)

            # 检查数据量是否足够
            min_data_points = self.config.min_data_points
            if len(df_copy) < min_data_points:
                self.logger.warning(f"{etf_code}: 数据量不足，需要至少{min_data_points}条记录，实际{len(df_copy)}条")

            return df_copy

        except Exception as e:
            self.logger.error(f"数据预处理失败 {etf_code}: {str(e)}")
            return df_copy

    def get_data_summary(self, etf_code: str, threshold: str) -> Dict[str, any]:
        """
        获取数据摘要信息

        Args:
            etf_code: ETF代码
            threshold: 门槛类型

        Returns:
            数据摘要字典
        """
        try:
            data = self.read_etf_data(etf_code, threshold)
            if data is None or data.empty:
                return {'error': '无法读取数据'}

            price_col = '收盘价(元)'
            volume_col = '成交量(手数)'

            summary = {
                'etf_code': etf_code,
                'threshold': threshold,
                'total_records': len(data),
                'date_range': {
                    'start': data['日期'].min().strftime('%Y-%m-%d'),
                    'end': data['日期'].max().strftime('%Y-%m-%d')
                },
                'price_stats': {
                    'min': float(data[price_col].min()),
                    'max': float(data[price_col].max()),
                    'mean': float(data[price_col].mean()),
                    'std': float(data[price_col].std())
                },
                'volume_stats': {
                    'min': float(data[volume_col].min()),
                    'max': float(data[volume_col].max()),
                    'mean': float(data[volume_col].mean()),
                    'std': float(data[volume_col].std())
                },
                'data_quality': {
                    'price_null_count': int(data[price_col].isnull().sum()),
                    'volume_null_count': int(data[volume_col].isnull().sum()),
                    'price_zero_count': int((data[price_col] == 0).sum()),
                    'volume_zero_count': int((data[volume_col] == 0).sum())
                }
            }

            return summary

        except Exception as e:
            return {'error': f'获取数据摘要失败: {str(e)}'}

    def batch_read_etf_data(self, etf_codes: List[str], threshold: str) -> Dict[str, Optional[pd.DataFrame]]:
        """
        批量读取ETF数据

        Args:
            etf_codes: ETF代码列表
            threshold: 门槛类型

        Returns:
            ETF代码到DataFrame的映射
        """
        results = {}
        successful_count = 0

        for etf_code in etf_codes:
            data = self.read_etf_data(etf_code, threshold)
            results[etf_code] = data
            if data is not None:
                successful_count += 1

        self.logger.info(f"批量读取数据完成: {successful_count}/{len(etf_codes)} 成功")
        return results