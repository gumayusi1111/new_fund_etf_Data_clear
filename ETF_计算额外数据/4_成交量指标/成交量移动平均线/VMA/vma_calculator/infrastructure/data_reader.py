"""
VMA数据读取器
============

负责从各种数据源读取ETF数据，包括CSV文件、缓存文件等
"""

import pandas as pd
import numpy as np
from pathlib import Path
from typing import Optional, Dict, List, Tuple
import logging
from datetime import datetime

from .config import VMAConfig

class VMADataReader:
    """VMA数据读取器"""

    def __init__(self, config: VMAConfig):
        self.config = config
        self.logger = logging.getLogger(__name__)

        # 定义列名映射
        self.column_mapping = {
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
                self.logger.warning(f"文件为空: {file_path}")
                return None

            # 标准化列名
            df = self._standardize_columns(df)

            # 数据清洗
            df = self._clean_data(df, etf_code)

            # 数据验证
            if not self._validate_data(df, etf_code):
                return None

            self.logger.info(f"成功读取{etf_code}数据: {len(df)}条记录")
            return df

        except Exception as e:
            self.logger.error(f"读取{etf_code}数据失败: {str(e)}")
            return None

    def read_cache_data(self, etf_code: str, threshold: str) -> Optional[pd.DataFrame]:
        """
        读取缓存的VMA计算结果

        Args:
            etf_code: ETF代码
            threshold: 门槛类型

        Returns:
            缓存的VMA结果DataFrame或None
        """
        try:
            cache_path = self.config.get_cache_path(threshold, etf_code)

            if not cache_path.exists():
                return None

            # 检查缓存是否过期
            if self._is_cache_expired(cache_path):
                self.logger.info(f"缓存已过期: {etf_code}")
                return None

            df = pd.read_csv(cache_path)

            if df.empty:
                return None

            self.logger.info(f"读取到{etf_code}缓存数据: {len(df)}条记录")
            return df

        except Exception as e:
            self.logger.error(f"读取{etf_code}缓存失败: {str(e)}")
            return None

    def batch_read_etf_data(self, etf_codes: List[str],
                           threshold: str) -> Dict[str, pd.DataFrame]:
        """
        批量读取ETF数据

        Args:
            etf_codes: ETF代码列表
            threshold: 门槛类型

        Returns:
            ETF代码到DataFrame的映射
        """
        results = {}

        for etf_code in etf_codes:
            data = self.read_etf_data(etf_code, threshold)
            if data is not None:
                results[etf_code] = data

        self.logger.info(f"批量读取完成: {len(results)}/{len(etf_codes)}个ETF")
        return results

    def _standardize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """标准化列名"""
        df = df.copy()

        # 找到成交量列
        volume_col = None
        for col in df.columns:
            if any(vol_name in col for vol_name in self.column_mapping['volume_columns']):
                volume_col = col
                break

        if volume_col:
            df['成交量(手数)'] = df[volume_col]

        # 找到日期列
        date_col = None
        for col in df.columns:
            if any(date_name in col for date_name in self.column_mapping['date_columns']):
                date_col = col
                break

        if date_col:
            df['日期'] = df[date_col]

        # 找到代码列
        code_col = None
        for col in df.columns:
            if any(code_name in col for code_name in self.column_mapping['code_columns']):
                code_col = col
                break

        if code_col:
            df['代码'] = df[code_col]

        return df

    def _clean_data(self, df: pd.DataFrame, etf_code: str) -> pd.DataFrame:
        """数据清洗"""
        df = df.copy()

        # 1. 处理日期列 - 修复日期格式问题
        if '日期' in df.columns:
            # 源数据日期格式为YYYYMMDD字符串，需要正确转换
            df['日期'] = pd.to_datetime(df['日期'].astype(str), format='%Y%m%d', errors='coerce')
            # 移除日期为空的行
            df = df.dropna(subset=['日期'])
            # 转换为标准日期格式字符串以便输出
            df['日期'] = df['日期'].dt.strftime('%Y-%m-%d')

        # 2. 处理成交量列
        if '成交量(手数)' in df.columns:
            # 转换为数值类型
            df['成交量(手数)'] = pd.to_numeric(df['成交量(手数)'], errors='coerce')

            # 移除负值和异常大的值
            df = df[df['成交量(手数)'] >= 0]

            # 处理异常值(使用3σ原则)
            volume_mean = df['成交量(手数)'].mean()
            volume_std = df['成交量(手数)'].std()
            threshold = volume_mean + 3 * volume_std

            outlier_count = (df['成交量(手数)'] > threshold).sum()
            if outlier_count > 0:
                self.logger.warning(f"{etf_code}检测到{outlier_count}个成交量异常值")
                # 不直接删除，而是记录

        # 3. 按日期排序 - 降序排列（最新数据在顶部）
        if '日期' in df.columns:
            df = df.sort_values('日期', ascending=False).reset_index(drop=True)

        # 4. 添加代码列
        if '代码' not in df.columns:
            df['代码'] = etf_code

        return df

    def _validate_data(self, df: pd.DataFrame, etf_code: str) -> bool:
        """验证数据质量"""
        if df.empty:
            self.logger.error(f"{etf_code}: 数据为空")
            return False

        # 检查必要列
        required_cols = ['成交量(手数)', '日期']
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            self.logger.error(f"{etf_code}: 缺少必要列 {missing_cols}")
            return False

        # 检查数据量
        if len(df) < self.config.quality_config['min_trading_days']:
            self.logger.warning(
                f"{etf_code}: 数据量不足({len(df)}条)，"
                f"最少需要{self.config.quality_config['min_trading_days']}条"
            )
            return False

        # 检查成交量数据完整性
        volume_null_ratio = df['成交量(手数)'].isnull().sum() / len(df) * 100
        if volume_null_ratio > (100 - self.config.quality_config['min_completeness']):
            self.logger.error(f"{etf_code}: 成交量数据缺失比例过高({volume_null_ratio:.1f}%)")
            return False

        # 检查零值比例
        volume_zero_ratio = (df['成交量(手数)'] == 0).sum() / len(df) * 100
        if volume_zero_ratio > self.config.quality_config['max_zero_ratio']:
            self.logger.warning(f"{etf_code}: 成交量零值比例较高({volume_zero_ratio:.1f}%)")

        return True

    def _is_cache_expired(self, cache_path: Path) -> bool:
        """检查缓存是否过期"""
        try:
            from datetime import datetime, timedelta

            # 获取文件修改时间
            mtime = datetime.fromtimestamp(cache_path.stat().st_mtime)

            # 检查是否超过有效期
            validity_hours = self.config.cache_config['cache_validity_hours']
            expiry_time = mtime + timedelta(hours=validity_hours)

            return datetime.now() > expiry_time

        except Exception:
            return True  # 出错时视为过期

    def get_data_info(self, etf_code: str, threshold: str) -> Dict:
        """获取数据信息摘要"""
        try:
            data = self.read_etf_data(etf_code, threshold)
            if data is None:
                return {"status": "error", "message": "无法读取数据"}

            volume_col = '成交量(手数)'
            info = {
                "status": "success",
                "total_records": len(data),
                "date_range": {
                    "start": data['日期'].min().strftime('%Y-%m-%d'),
                    "end": data['日期'].max().strftime('%Y-%m-%d')
                },
                "volume_stats": {
                    "min": int(data[volume_col].min()),
                    "max": int(data[volume_col].max()),
                    "mean": int(data[volume_col].mean()),
                    "median": int(data[volume_col].median())
                },
                "null_count": int(data[volume_col].isnull().sum()),
                "zero_count": int((data[volume_col] == 0).sum())
            }

            return info

        except Exception as e:
            return {"status": "error", "message": str(e)}