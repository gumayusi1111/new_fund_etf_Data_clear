"""
价量配合度核心计算引擎
===================

实现价量配合度的核心计算逻辑，包含10个核心字段的计算：
- 价量相关性: pv_corr_10, pv_corr_20, pv_corr_30
- 量价趋势: vpt, vpt_momentum, vpt_ratio
- 成交量分析: volume_quality, volume_consistency
- 价量强度: pv_strength, pv_divergence

基于中国A股市场特征优化的专业计算系统
"""

import pandas as pd
import numpy as np
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import logging
from ..infrastructure.config import PVConfig

class PVEngine:
    """价量配合度核心计算引擎"""

    def __init__(self, config: PVConfig = None):
        self.logger = logging.getLogger(__name__)
        self.config = config or PVConfig()

    def calculate_pv_indicators(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        计算价量配合度的10个核心指标

        Args:
            data: 包含价格和成交量数据的DataFrame，必须包含'收盘价(元)'和'成交量(手数)'列

        Returns:
            包含10个价量配合度指标的DataFrame
        """
        if data.empty:
            return pd.DataFrame()

        # 确保数据按日期排序 - 升序(历史到最新，用于计算)
        data = data.sort_values('日期', ascending=True).copy()

        # 验证必要列
        required_columns = ['收盘价(元)', '成交量(手数)', '日期']
        for col in required_columns:
            if col not in data.columns:
                raise ValueError(f"缺少必要列: {col}")

        # 计算结果DataFrame
        result = pd.DataFrame()
        result['code'] = data.get('代码', '')
        result['date'] = data['日期']

        try:
            # 1. 价量相关性指标组计算
            result['pv_corr_10'] = self._calculate_pv_correlation(
                data['收盘价(元)'], data['成交量(手数)'], 10
            )
            result['pv_corr_20'] = self._calculate_pv_correlation(
                data['收盘价(元)'], data['成交量(手数)'], 20
            )
            result['pv_corr_30'] = self._calculate_pv_correlation(
                data['收盘价(元)'], data['成交量(手数)'], 30
            )

            # 2. 量价趋势指标组计算
            result['vpt'] = self._calculate_vpt(
                data['收盘价(元)'], data['成交量(手数)']
            )
            result['vpt_momentum'] = self._calculate_vpt_momentum(result['vpt'])
            result['vpt_ratio'] = self._calculate_vpt_ratio(result['vpt'])

            # 3. 成交量分析指标组计算  
            result['volume_quality'] = self._calculate_volume_quality(
                data['成交量(手数)']
            )
            result['volume_consistency'] = self._calculate_volume_consistency(
                data['成交量(手数)']
            )

            # 4. 价量强度指标组计算
            result['pv_strength'] = self._calculate_pv_strength(
                data['收盘价(元)'], data['成交量(手数)']
            )
            result['pv_divergence'] = self._calculate_pv_divergence(
                data['收盘价(元)'], result['vpt']
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
                result = pd.DataFrame()

            # 按日期降序排列（最新数据在顶部）
            if not result.empty and 'date' in result.columns:
                result = result.sort_values('date', ascending=False).reset_index(drop=True)

            self.logger.info(f"价量配合度计算完成，输出{len(result)}条记录")
            return result

        except Exception as e:
            self.logger.error(f"价量配合度计算错误: {str(e)}")
            raise

    def _calculate_pv_correlation(self, price: pd.Series, volume: pd.Series, window: int) -> pd.Series:
        """
        计算价量相关系数 - 向量化优化版本
        
        Args:
            price: 价格序列
            volume: 成交量序列  
            window: 计算窗口
            
        Returns:
            价量相关系数序列
        """
        precision = self.config.get_precision_digits()
        
        # 计算价格变化率和成交量变化率
        price_change = price.pct_change().fillna(0)
        volume_change = volume.pct_change().fillna(0)
        
        # 使用pandas的滚动相关系数计算 - 向量化操作
        correlation = price_change.rolling(window=window, min_periods=int(window * 0.8)).corr(volume_change)
        
        return correlation.round(precision)

    def _calculate_vpt(self, price: pd.Series, volume: pd.Series) -> pd.Series:
        """
        计算累积量价趋势值(Volume Price Trend) - 向量化优化版本
        
        VPT[i] = VPT[i-1] + 成交量[i] × (收盘价[i] - 收盘价[i-1]) / 收盘价[i-1]
        
        Args:
            price: 价格序列
            volume: 成交量序列
            
        Returns:
            VPT序列
        """
        precision = self.config.get_precision_digits()
        
        # 向量化计算价格变化率
        price_change_rate = price.pct_change().fillna(0)
        
        # 向量化计算VPT增量
        vpt_increments = volume * price_change_rate
        
        # 使用cumsum进行累积求和 - 向量化操作
        vpt = vpt_increments.cumsum()
        
        return vpt.round(precision)

    def _calculate_vpt_momentum(self, vpt: pd.Series) -> pd.Series:
        """
        计算VPT动量指标
        
        VPT_MOMENTUM = VPT[i] - VPT[i-1]
        
        Args:
            vpt: VPT序列
            
        Returns:
            VPT动量序列
        """
        precision = self.config.get_precision_digits()
        return vpt.diff().round(precision)

    def _calculate_vpt_ratio(self, vpt: pd.Series, window: int = 20) -> pd.Series:
        """
        计算VPT相对比率
        
        VPT_RATIO = VPT[i] / mean(VPT[-20:])
        
        Args:
            vpt: VPT序列
            window: 均值计算窗口
            
        Returns:
            VPT比率序列
        """
        precision = self.config.get_precision_digits()
        
        # 计算滚动均值
        vpt_mean = vpt.rolling(window=window, min_periods=window).mean()
        
        # 计算比率，避免除零
        with np.errstate(divide='ignore', invalid='ignore'):
            ratio = np.divide(vpt.values, vpt_mean.values, 
                            out=np.full_like(vpt.values, np.nan, dtype=float), 
                            where=(vpt_mean.values != 0))
        
        return pd.Series(ratio, index=vpt.index).round(precision)

    def _calculate_volume_quality(self, volume: pd.Series, window: int = None) -> pd.Series:
        """
        计算成交量质量综合评分 - 向量化优化版本
        
        VOLUME_QUALITY = min(100, (vol_stability × 0.6 + vol_relative × 0.4) × 50)
        
        Args:
            volume: 成交量序列
            window: 计算窗口
            
        Returns:
            成交量质量评分序列(0-100)
        """
        if window is None:
            window = self.config.volume_quality_window
            
        precision = self.config.get_precision_digits()
        
        # 向量化计算滚动标准差和稳定性
        vol_std = volume.rolling(window=window, min_periods=window).std()
        vol_stability = 1 / (vol_std + 1e-8)
        
        # 向量化计算相对水平
        vol_mean_60 = volume.rolling(window=60, min_periods=30).mean()
        vol_relative = volume / (vol_mean_60 + 1e-8)
        
        # 向量化综合评分计算
        quality_score = (vol_stability * 0.6 + vol_relative * 0.4) * 50
        quality_score = np.minimum(quality_score, 100)
        
        return quality_score.round(precision)

    def _calculate_volume_consistency(self, volume: pd.Series, window: int = None) -> pd.Series:
        """
        计算成交量一致性指标 - 向量化优化版本
        
        VOLUME_CONSISTENCY = max(0, 100 - cv × 100)
        其中 cv = std / mean (变异系数)
        
        Args:
            volume: 成交量序列
            window: 计算窗口
            
        Returns:
            成交量一致性指标序列(0-100)
        """
        if window is None:
            window = self.config.volume_quality_window
            
        precision = self.config.get_precision_digits()
        
        # 向量化计算滚动统计量
        vol_mean = volume.rolling(window=window, min_periods=window).mean()
        vol_std = volume.rolling(window=window, min_periods=window).std()
        
        # 向量化计算变异系数
        cv = vol_std / (vol_mean + 1e-8)
        
        # 向量化一致性指标计算
        consistency = np.maximum(0, 100 - cv * 100)
        
        return consistency.round(precision)

    def _calculate_pv_strength(self, price: pd.Series, volume: pd.Series, window: int = None) -> pd.Series:
        """
        计算价量强度综合指标 - 向量化优化版本
        
        PV_STRENGTH = price_momentum / (volume_momentum + 1e-8)
        
        Args:
            price: 价格序列
            volume: 成交量序列
            window: 动量计算窗口
            
        Returns:
            价量强度指标序列
        """
        if window is None:
            window = self.config.pv_strength_window
            
        precision = self.config.get_precision_digits()
        
        # 向量化计算价格和成交量动量
        price_momentum = np.abs(price.pct_change(periods=window))
        volume_momentum = np.abs(volume.pct_change(periods=window))
        
        # 向量化价量强度计算
        pv_strength = price_momentum / (volume_momentum + 1e-8)
        
        return pv_strength.round(precision)

    def _calculate_pv_divergence(self, price: pd.Series, vpt: pd.Series, window: int = None) -> pd.Series:
        """
        计算价量背离程度
        
        PV_DIVERGENCE = abs(price_trend - vpt_trend) × 100
        
        Args:
            price: 价格序列  
            vpt: VPT序列
            window: 趋势计算窗口
            
        Returns:
            价量背离程度序列(0-100+)
        """
        if window is None:
            window = self.config.pv_divergence_window
            
        precision = self.config.get_precision_digits()
        
        # 计算价格趋势 (n日价格变化率)
        price_trend = price.pct_change(periods=window)
        
        # 计算VPT趋势 (n日VPT变化率)
        vpt_trend = vpt.pct_change(periods=window)
        
        # 背离程度 = |价格趋势 - VPT趋势| × 100
        divergence = np.abs(price_trend - vpt_trend) * 100
        
        return pd.Series(divergence, index=price.index).round(precision)

    def validate_data_quality(self, data: pd.DataFrame) -> Dict[str, any]:
        """
        验证数据质量
        
        Returns:
            数据质量报告字典
        """
        if data.empty:
            return {"valid": False, "error": "数据为空"}

        price_col = '收盘价(元)'
        volume_col = '成交量(手数)'

        # 检查必要列
        for col in [price_col, volume_col]:
            if col not in data.columns:
                return {"valid": False, "error": f"缺少{col}列"}

        # 统计信息
        total_rows = len(data)
        price_null_count = data[price_col].isnull().sum()
        volume_null_count = data[volume_col].isnull().sum()
        price_zero_count = (data[price_col] == 0).sum()
        volume_zero_count = (data[volume_col] == 0).sum()
        price_negative_count = (data[price_col] < 0).sum()
        volume_negative_count = (data[volume_col] < 0).sum()

        # 数据完整性
        max_null_count = max(price_null_count, volume_null_count)
        completeness = (total_rows - max_null_count) / total_rows * 100

        min_completeness = self.config.get_min_data_completeness() * 100

        quality_report = {
            "valid": completeness >= min_completeness,
            "total_rows": total_rows,
            "price_null_count": price_null_count,
            "volume_null_count": volume_null_count,
            "price_zero_count": price_zero_count,
            "volume_zero_count": volume_zero_count,
            "price_negative_count": price_negative_count,
            "volume_negative_count": volume_negative_count,
            "completeness": round(completeness, 2),
            "min_price": data[price_col].min(),
            "max_price": data[price_col].max(),
            "mean_price": round(data[price_col].mean(), 4),
            "min_volume": data[volume_col].min(),
            "max_volume": data[volume_col].max(),
            "mean_volume": round(data[volume_col].mean(), 2),
            "required_completeness": min_completeness
        }

        return quality_report

    def calculate_incremental(self, existing_data: pd.DataFrame,
                            new_data: pd.DataFrame) -> pd.DataFrame:
        """
        增量计算价量配合度指标
        
        Args:
            existing_data: 已有的价量配合度计算结果
            new_data: 新的原始数据
            
        Returns:
            更新后的价量配合度结果
        """
        if existing_data.empty:
            return self.calculate_pv_indicators(new_data)

        # 获取最后一个计算日期
        last_date = existing_data['date'].max() if not existing_data.empty else None

        # 筛选出新增的数据
        if last_date is not None:
            new_rows = new_data[new_data['日期'] > last_date]
            if new_rows.empty:
                return existing_data

            # 需要重新计算的窗口数据（包含足够的历史数据）
            recent_data = new_data[new_data['日期'] >= pd.to_datetime(last_date) - pd.Timedelta(days=90)]

            # 计算新的指标
            new_indicators = self.calculate_pv_indicators(recent_data)

            # 合并结果
            if not new_indicators.empty:
                combined_result = pd.concat([
                    existing_data[existing_data['date'] < new_indicators['date'].min()],
                    new_indicators
                ], ignore_index=True)

                return combined_result.sort_values('date', ascending=False).reset_index(drop=True)

        return self.calculate_pv_indicators(new_data)

    def get_calculation_config(self) -> Dict[str, any]:
        """
        获取当前计算配置信息
        
        Returns:
            包含所有计算相关配置的字典
        """
        return {
            "correlation_periods": self.config.correlation_periods,
            "vpt_periods": self.config.vpt_periods,
            "volume_quality_window": self.config.volume_quality_window,
            "pv_strength_window": self.config.pv_strength_window,
            "pv_divergence_window": self.config.pv_divergence_window,
            "warmup_period": self.config.get_warmup_period(),
            "precision_digits": self.config.get_precision_digits(),
            "min_data_completeness": self.config.get_min_data_completeness(),
            "min_data_points": self.config.min_data_points,
            "calculation_explanation": {
                "warmup_period_reason": f"基于最长相关性周期({max(self.config.correlation_periods)})和稳定性缓冲计算得出",
                "precision_reason": f"所有数值结果保留{self.config.get_precision_digits()}位小数以确保计算精度",
                "completeness_reason": f"要求{self.config.get_min_data_completeness()*100}%以上数据完整性以保证指标质量"
            }
        }