"""
VMA核心计算引擎
================

实现成交量移动平均线的核心计算逻辑，包含10个核心字段的计算：
- 基础均线: vma_5, vma_10, vma_20
- 核心比率: volume_ratio_5, volume_ratio_10, volume_ratio_20
- 趋势分析: volume_trend_short, volume_trend_medium
- 变化分析: volume_change_rate, volume_activity_score
"""

import pandas as pd
import numpy as np
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import logging
from ..infrastructure.config import VMAConfig

class VMAEngine:
    """VMA核心计算引擎"""

    def __init__(self, config: VMAConfig = None):
        self.logger = logging.getLogger(__name__)
        self.config = config or VMAConfig()

    def calculate_vma_indicators(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        计算VMA的10个核心指标

        Args:
            data: 包含成交量数据的DataFrame，必须包含'成交量(手数)'和'日期'列

        Returns:
            包含10个VMA指标的DataFrame
        """
        if data.empty:
            return pd.DataFrame()

        # 确保数据按日期排序 - 升序(历史到最新，用于计算)
        data = data.sort_values('日期', ascending=True).copy()

        # 验证必要列
        required_columns = ['成交量(手数)', '日期']
        for col in required_columns:
            if col not in data.columns:
                raise ValueError(f"缺少必要列: {col}")

        # 计算结果DataFrame
        result = pd.DataFrame()
        result['code'] = data.get('代码', '')
        result['date'] = data['日期']

        try:
            # 1. 基础均线计算
            result['vma_5'] = self._calculate_sma(data['成交量(手数)'], 5)
            result['vma_10'] = self._calculate_sma(data['成交量(手数)'], 10)
            result['vma_20'] = self._calculate_sma(data['成交量(手数)'], 20)

            # 2. 核心比率计算
            result['volume_ratio_5'] = self._calculate_volume_ratio(
                data['成交量(手数)'], result['vma_5']
            )
            result['volume_ratio_10'] = self._calculate_volume_ratio(
                data['成交量(手数)'], result['vma_10']
            )
            result['volume_ratio_20'] = self._calculate_volume_ratio(
                data['成交量(手数)'], result['vma_20']
            )

            # 3. 趋势分析计算
            result['volume_trend_short'] = self._calculate_trend_ratio(
                result['vma_5'], result['vma_10']
            )
            result['volume_trend_medium'] = self._calculate_trend_ratio(
                result['vma_10'], result['vma_20']
            )

            # 4. 变化分析计算
            result['volume_change_rate'] = self._calculate_change_rate(
                data['成交量(手数)']
            )
            result['volume_activity_score'] = self._calculate_activity_score(
                data['成交量(手数)']
            )

            # 5. 添加计算时间戳
            result['calc_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            # 移除预热期数据（确保指标稳定性）
            warmup_period = self.config.get_warmup_period()
            if len(result) > warmup_period:
                result = result.iloc[warmup_period:].reset_index(drop=True)
                self.logger.debug(f"移除预热期数据: {warmup_period}行，保留{len(result)}行有效数据")
            else:
                self.logger.warning(f"数据量不足，需要至少{warmup_period + 1}行数据，当前{len(result)}行")
                result = pd.DataFrame()  # 数据不足，返回空DataFrame

            # 按日期降序排列（最新数据在顶部）
            if not result.empty and 'date' in result.columns:
                result = result.sort_values('date', ascending=False).reset_index(drop=True)

            self.logger.info(f"VMA计算完成，输出{len(result)}条记录")
            return result

        except Exception as e:
            self.logger.error(f"VMA计算错误: {str(e)}")
            raise

    def _calculate_sma(self, data: pd.Series, window: int) -> pd.Series:
        """计算简单移动平均线 - 向量化优化"""
        precision = self.config.get_precision_digits()
        return data.rolling(window=window, min_periods=window).mean().round(precision)

    def _calculate_volume_ratio(self, volume: pd.Series, vma: pd.Series) -> pd.Series:
        """计算成交量比率 - 向量化优化"""
        precision = self.config.get_precision_digits()
        with np.errstate(divide='ignore', invalid='ignore'):
            ratio = np.divide(volume.values, vma.values, out=np.full_like(volume.values, np.nan, dtype=float), where=(vma.values != 0))
            return pd.Series(ratio, index=volume.index).round(precision)

    def _calculate_trend_ratio(self, short_vma: pd.Series, long_vma: pd.Series) -> pd.Series:
        """计算趋势比率 - 向量化优化"""
        precision = self.config.get_precision_digits()
        with np.errstate(divide='ignore', invalid='ignore'):
            ratio = np.divide(short_vma.values, long_vma.values, out=np.full_like(short_vma.values, np.nan, dtype=float), where=(long_vma.values != 0))
            return pd.Series(ratio, index=short_vma.index).round(precision)

    def _calculate_change_rate(self, volume: pd.Series) -> pd.Series:
        """计算日变化率 - 向量化优化"""
        precision = self.config.get_precision_digits()
        return volume.pct_change().round(precision)

    def _calculate_activity_score(self, volume: pd.Series, window: int = None) -> pd.Series:
        """
        计算相对活跃度得分 - 向量化优化
        基于过去N日成交量排名的百分比
        """
        if window is None:
            window = self.config.activity_window
        precision = self.config.get_precision_digits()
        # 使用更高效的rolling rank方法
        rolling_rank = volume.rolling(window=window).rank(pct=True) * 100
        return rolling_rank.round(precision)

    def validate_data_quality(self, data: pd.DataFrame) -> Dict[str, any]:
        """
        验证数据质量

        Returns:
            数据质量报告字典
        """
        if data.empty:
            return {"valid": False, "error": "数据为空"}

        volume_col = '成交量(手数)'

        # 检查必要列
        if volume_col not in data.columns:
            return {"valid": False, "error": f"缺少{volume_col}列"}

        # 统计信息
        total_rows = len(data)
        null_count = data[volume_col].isnull().sum()
        zero_count = (data[volume_col] == 0).sum()
        negative_count = (data[volume_col] < 0).sum()

        completeness = (total_rows - null_count) / total_rows * 100

        min_completeness = self.config.get_min_data_completeness() * 100  # 转换为百分比

        quality_report = {
            "valid": completeness >= min_completeness,  # 使用配置的数据完整性要求
            "total_rows": total_rows,
            "null_count": null_count,
            "zero_count": zero_count,
            "negative_count": negative_count,
            "completeness": round(completeness, 2),
            "min_volume": data[volume_col].min(),
            "max_volume": data[volume_col].max(),
            "mean_volume": round(data[volume_col].mean(), 2),
            "required_completeness": min_completeness
        }

        return quality_report

    def calculate_incremental(self, existing_data: pd.DataFrame,
                            new_data: pd.DataFrame) -> pd.DataFrame:
        """
        增量计算VMA指标 - 优化版本

        Args:
            existing_data: 已有的VMA计算结果
            new_data: 新的原始数据

        Returns:
            更新后的VMA结果
        """
        if existing_data.empty:
            return self.calculate_vma_indicators(new_data)

        # 获取最后一个计算日期
        last_date = existing_data['date'].max() if not existing_data.empty else None

        # 筛选出新增的数据
        if last_date is not None:
            new_rows = new_data[new_data['日期'] > last_date]
            if new_rows.empty:
                # 没有新数据，返回现有数据
                return existing_data

            # 需要重新计算的窗口数据（包含足够的历史数据以计算移动平均）
            recent_data = new_data[new_data['日期'] >= pd.to_datetime(last_date) - pd.Timedelta(days=60)]

            # 计算新的指标
            new_indicators = self.calculate_vma_indicators(recent_data)

            # 合并结果 - 只保留新计算的部分
            if not new_indicators.empty:
                combined_result = pd.concat([
                    existing_data[existing_data['date'] < new_indicators['date'].min()],
                    new_indicators
                ], ignore_index=True)

                # 确保按日期降序排列
                return combined_result.sort_values('date', ascending=False).reset_index(drop=True)

        return self.calculate_vma_indicators(new_data)

    def get_calculation_config(self) -> Dict[str, any]:
        """
        获取当前计算配置信息，用于调试和验证

        Returns:
            包含所有计算相关配置的字典
        """
        return {
            "vma_periods": self.config.vma_periods,
            "activity_window": self.config.activity_window,
            "warmup_period": self.config.get_warmup_period(),  # 动态计算的值
            "precision_digits": self.config.get_precision_digits(),
            "min_data_completeness": self.config.get_min_data_completeness(),
            "min_data_points": self.config.min_data_points,
            "calculation_explanation": {
                "warmup_period_reason": f"基于最长VMA周期({max(self.config.vma_periods)})、活跃度窗口({self.config.activity_window})和稳定性缓冲(10)计算得出",
                "precision_reason": f"所有数值结果保留{self.config.get_precision_digits()}位小数以确保计算精度",
                "completeness_reason": f"要求{self.config.get_min_data_completeness()*100}%以上数据完整性以保证指标质量"
            }
        }