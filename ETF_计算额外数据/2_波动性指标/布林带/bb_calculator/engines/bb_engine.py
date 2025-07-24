#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
布林带计算引擎
============

科学准确的布林带指标计算核心引擎
基于John Bollinger原始算法实现
支持向量化计算和多种布林带衍生指标
"""

import pandas as pd
import numpy as np
from typing import Dict, Optional, Tuple
import warnings
warnings.filterwarnings('ignore')
from ..infrastructure.config import BBConfig


class BollingerBandsEngine:
    """布林带计算引擎 - 科学算法实现"""
    
    def __init__(self, config: BBConfig):
        """初始化布林带计算引擎"""
        self.config = config
        self.period = config.get_bb_period()
        self.std_multiplier = config.get_bb_std_multiplier()
        self.precision = config.get_precision()
    
    def calculate_bollinger_bands(self, df: pd.DataFrame) -> Dict[str, Optional[float]]:
        """
        计算布林带指标 - 科学标准算法
        
        算法来源: John Bollinger (1983)
        标准参数: 20日周期，2倍标准差
        
        Args:
            df: ETF数据DataFrame，需包含'收盘价'列
            
        Returns:
            Dict[str, Optional[float]]: 布林带计算结果
        """
        if df.empty or '收盘价' not in df.columns:
            return self._get_empty_result()
        
        # 数据验证：确保有足够的数据点
        if len(df) < self.period:
            return self._get_empty_result()
        
        try:
            # 获取收盘价序列并清理数据
            prices = df['收盘价'].copy()
            prices = prices.dropna()
            
            if len(prices) < self.period:
                return self._get_empty_result()
            
            # 科学布林带计算
            middle_band = self._calculate_sma(prices, self.period)
            rolling_std = self._calculate_rolling_std(prices, self.period)
            
            # 计算上下轨 - 标准Bollinger算法
            upper_band = middle_band + (self.std_multiplier * rolling_std)
            lower_band = middle_band - (self.std_multiplier * rolling_std)
            
            # 获取最新有效值
            latest_price = float(prices.iloc[-1])
            latest_middle = self._get_latest_valid_value(middle_band)
            latest_upper = self._get_latest_valid_value(upper_band)
            latest_lower = self._get_latest_valid_value(lower_band)
            
            # 数据有效性验证
            if None in [latest_middle, latest_upper, latest_lower]:
                return self._get_empty_result()
            
            # 计算布林带衍生指标
            bb_width = self._calculate_bb_width(latest_upper, latest_lower, latest_middle)
            bb_position = self._calculate_bb_position(latest_price, latest_upper, latest_lower)
            bb_percent_b = self._calculate_percent_b(latest_price, latest_upper, latest_lower)
            
            # 应用精度控制 - 统一8位精度
            return {
                'bb_middle': round(latest_middle, self.precision),
                'bb_upper': round(latest_upper, self.precision),
                'bb_lower': round(latest_lower, self.precision),
                'bb_width': round(bb_width, self.precision),
                'bb_position': round(bb_position, self.precision),
                'bb_percent_b': round(bb_percent_b, self.precision)
            }
            
        except Exception as e:
            return self._get_empty_result()
    
    def calculate_full_history(self, df: pd.DataFrame) -> Optional[pd.DataFrame]:
        """
        计算完整历史布林带数据 - 向量化优化版本
        
        Args:
            df: 价格数据DataFrame
            
        Returns:
            Optional[pd.DataFrame]: 完整历史布林带数据
        """
        if df.empty or '收盘价' not in df.columns:
            return None
        
        if len(df) < self.period:
            return None
        
        try:
            # 确保数据按日期升序排序（为计算滚动指标）
            if '日期' in df.columns:
                df_sorted = df.sort_values('日期', ascending=True).copy()
            else:
                df_sorted = df.copy()
            
            prices = df_sorted['收盘价'].copy()
            
            # 向量化计算布林带
            middle_band = prices.rolling(window=self.period, min_periods=self.period).mean()
            rolling_std = prices.rolling(window=self.period, min_periods=self.period).std()
            
            upper_band = middle_band + (self.std_multiplier * rolling_std)
            lower_band = middle_band - (self.std_multiplier * rolling_std)
            
            # 计算衍生指标
            bb_width = (upper_band - lower_band) / middle_band * 100
            bb_position = (prices - lower_band) / (upper_band - lower_band) * 100
            bb_percent_b = (prices - lower_band) / (upper_band - lower_band)
            
            # 创建结果DataFrame
            result_df = pd.DataFrame({
                'bb_middle': middle_band.round(self.precision),
                'bb_upper': upper_band.round(self.precision),
                'bb_lower': lower_band.round(self.precision),
                'bb_width': bb_width.round(self.precision),
                'bb_position': bb_position.round(self.precision),
                'bb_percent_b': bb_percent_b.round(self.precision)
            })
            
            # 复制日期列
            if '日期' in df_sorted.columns:
                result_df['日期'] = df_sorted['日期'].values
            
            # 按日期降序排列（最新的在前面）
            if '日期' in result_df.columns:
                result_df = result_df.sort_values('日期', ascending=False).reset_index(drop=True)
            
            return result_df
            
        except Exception as e:
            return None
    
    def _calculate_sma(self, prices: pd.Series, period: int) -> pd.Series:
        """计算简单移动平均"""
        return prices.rolling(window=period, min_periods=period).mean()
    
    def _calculate_rolling_std(self, prices: pd.Series, period: int) -> pd.Series:
        """计算滚动标准差"""
        return prices.rolling(window=period, min_periods=period).std()
    
    def _get_latest_valid_value(self, series: pd.Series) -> Optional[float]:
        """获取最新的有效值"""
        try:
            # 获取非空值
            valid_values = series.dropna()
            if len(valid_values) == 0:
                return None
            return float(valid_values.iloc[-1])
        except Exception:
            return None
    
    def _calculate_bb_width(self, upper: float, lower: float, middle: float) -> float:
        """计算布林带宽度百分比"""
        if middle == 0:
            return 0.0
        return (upper - lower) / middle * 100
    
    def _calculate_bb_position(self, price: float, upper: float, lower: float) -> float:
        """计算价格在布林带中的相对位置"""
        if upper == lower:
            return 50.0
        return (price - lower) / (upper - lower) * 100
    
    def _calculate_percent_b(self, price: float, upper: float, lower: float) -> float:
        """计算%B指标"""
        if upper == lower:
            return 0.5
        return (price - lower) / (upper - lower)
    
    def _get_empty_result(self) -> Dict[str, Optional[float]]:
        """返回空结果"""
        return {
            'bb_middle': None,
            'bb_upper': None,
            'bb_lower': None,
            'bb_width': None,
            'bb_position': None,
            'bb_percent_b': None
        }
    
    def calculate_incremental_update(self, cached_data: pd.DataFrame, 
                                   new_data: pd.DataFrame) -> Optional[pd.DataFrame]:
        """
        增量计算布林带数据
        
        Args:
            cached_data: 已缓存的历史数据
            new_data: 新的原始数据
            
        Returns:
            Optional[pd.DataFrame]: 更新后的完整数据
        """
        try:
            # 合并数据
            if cached_data.empty:
                return self.calculate_full_history(new_data)
            
            # 重新计算足够的历史数据以确保准确性
            # 为了保证滚动计算的准确性，需要足够的重叠数据
            overlap_period = self.period * 2
            
            if len(new_data) >= overlap_period:
                # 使用新数据的最后overlap_period行进行计算
                calculation_data = new_data.tail(len(new_data)).copy()
            else:
                # 合并缓存数据和新数据
                calculation_data = pd.concat([cached_data.tail(overlap_period), new_data], 
                                           ignore_index=True)
            
            # 重新计算
            full_result = self.calculate_full_history(calculation_data)
            
            if full_result is None:
                return cached_data
            
            return full_result
            
        except Exception as e:
            # 发生错误时返回缓存数据
            return cached_data
    
    def validate_calculation_result(self, result: Dict[str, Optional[float]]) -> bool:
        """验证计算结果的有效性"""
        if not result:
            return False
        
        # 检查关键指标是否存在
        required_keys = ['bb_middle', 'bb_upper', 'bb_lower']
        for key in required_keys:
            if key not in result or result[key] is None:
                return False
        
        # 检查数值逻辑关系
        middle = result['bb_middle']
        upper = result['bb_upper'] 
        lower = result['bb_lower']
        
        # 布林带的基本约束：lower <= middle <= upper
        if not (lower <= middle <= upper):
            return False
        
        # 检查数值范围（价格应该为正）
        if any(val <= 0 for val in [middle, upper, lower]):
            return False
        
        return True