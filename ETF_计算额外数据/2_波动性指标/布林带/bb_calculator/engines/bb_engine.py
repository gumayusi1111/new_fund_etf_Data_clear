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


class BollingerBandsEngine:
    """布林带计算引擎 - 科学算法实现"""
    
    def __init__(self, config):
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
            
            # 计算衍生指标
            bb_width = self._calculate_bb_width(latest_upper, latest_lower, latest_middle)
            bb_position = self._calculate_bb_position(latest_price, latest_upper, latest_lower)
            bb_percent_b = self._calculate_percent_b(latest_price, latest_upper, latest_lower)
            
            return {
                'bb_middle': self._round_value(latest_middle),
                'bb_upper': self._round_value(latest_upper),
                'bb_lower': self._round_value(latest_lower),
                'bb_width': self._round_value(bb_width),
                'bb_position': self._round_value(bb_position),
                'bb_percent_b': self._round_value(bb_percent_b)
            }
            
        except Exception as e:
            return self._get_empty_result()
    
    def _calculate_sma(self, prices: pd.Series, period: int) -> pd.Series:
        """计算简单移动平均线 - 标准算法"""
        return prices.rolling(window=period, min_periods=period).mean()
    
    def _calculate_rolling_std(self, prices: pd.Series, period: int) -> pd.Series:
        """
        计算滚动标准差 - 标准统计算法
        使用样本标准差 (ddof=1)，这是Bollinger Bands的标准做法
        """
        return prices.rolling(window=period, min_periods=period).std(ddof=1)
    
    def _get_latest_valid_value(self, series: pd.Series) -> Optional[float]:
        """获取序列中最新的有效值"""
        valid_values = series.dropna()
        if valid_values.empty:
            return None
        return float(valid_values.iloc[-1])
    
    def _calculate_bb_width(self, upper: Optional[float], lower: Optional[float], 
                           middle: Optional[float]) -> Optional[float]:
        """
        计算布林带宽度 - 标准公式
        公式: (上轨 - 下轨) / 中轨 × 100
        """
        if None in [upper, lower, middle] or abs(middle) < 1e-10:
            return None
        
        try:
            width = ((upper - lower) / middle) * 100
            return width
        except (ZeroDivisionError, Exception):
            return None
    
    def _calculate_bb_position(self, price: float, upper: Optional[float], 
                              lower: Optional[float]) -> Optional[float]:
        """
        计算价格在布林带中的相对位置
        公式: (收盘价 - 下轨) / (上轨 - 下轨) × 100
        """
        if None in [upper, lower] or abs(upper - lower) < 1e-10:
            return None
        
        try:
            position = ((price - lower) / (upper - lower)) * 100
            # 允许突破，但限制在合理范围内
            position = max(-50, min(150, position))
            return position
        except (ZeroDivisionError, Exception):
            return None
    
    def _calculate_percent_b(self, price: float, upper: Optional[float], 
                            lower: Optional[float]) -> Optional[float]:
        """
        计算%B指标 - Bollinger标准公式
        公式: (收盘价 - 下轨) / (上轨 - 下轨)
        
        %B含义:
        - %B > 1.0: 价格在上轨之上
        - %B < 0.0: 价格在下轨之下
        - %B = 0.5: 价格在中轨位置
        """
        if None in [upper, lower] or abs(upper - lower) < 1e-10:
            return None
        
        try:
            percent_b = (price - lower) / (upper - lower)
            return percent_b
        except (ZeroDivisionError, Exception):
            return None
    
    def _round_value(self, value: Optional[float]) -> Optional[float]:
        """按配置精度四舍五入"""
        if value is None:
            return None
        return round(value, self.precision)
    
    def _get_empty_result(self) -> Dict[str, Optional[float]]:
        """获取空结果"""
        return {
            'bb_middle': None,
            'bb_upper': None,
            'bb_lower': None,
            'bb_width': None,
            'bb_position': None,
            'bb_percent_b': None
        }
    
    def calculate_historical_bollinger_bands(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        计算历史布林带数据（向量化高性能版本）
        
        Args:
            df: ETF数据DataFrame
            
        Returns:
            pd.DataFrame: 包含历史布林带数据的DataFrame
        """
        if df.empty or '收盘价' not in df.columns:
            return pd.DataFrame()
        
        if len(df) < self.period:
            return pd.DataFrame()
        
        try:
            result_df = df.copy()
            prices = result_df['收盘价']
            
            # 向量化计算所有指标
            result_df['bb_middle'] = self._calculate_sma(prices, self.period)
            rolling_std = self._calculate_rolling_std(prices, self.period)
            
            result_df['bb_upper'] = result_df['bb_middle'] + (self.std_multiplier * rolling_std)
            result_df['bb_lower'] = result_df['bb_middle'] - (self.std_multiplier * rolling_std)
            
            # 计算衍生指标 - 向量化
            result_df['bb_width'] = self._calculate_bb_width_vectorized(
                result_df['bb_upper'], result_df['bb_lower'], result_df['bb_middle']
            )
            
            result_df['bb_position'] = self._calculate_bb_position_vectorized(
                prices, result_df['bb_upper'], result_df['bb_lower']
            )
            
            result_df['bb_percent_b'] = self._calculate_percent_b_vectorized(
                prices, result_df['bb_upper'], result_df['bb_lower']
            )
            
            # 四舍五入到指定精度
            bb_columns = ['bb_middle', 'bb_upper', 'bb_lower', 'bb_width', 'bb_position', 'bb_percent_b']
            for col in bb_columns:
                if col in result_df.columns:
                    result_df[col] = result_df[col].round(self.precision)
            
            return result_df
            
        except Exception as e:
            return pd.DataFrame()
    
    def _calculate_bb_width_vectorized(self, upper: pd.Series, lower: pd.Series, 
                                      middle: pd.Series) -> pd.Series:
        """向量化计算布林带宽度"""
        return ((upper - lower) / middle * 100).replace([np.inf, -np.inf], np.nan)
    
    def _calculate_bb_position_vectorized(self, prices: pd.Series, upper: pd.Series, 
                                         lower: pd.Series) -> pd.Series:
        """向量化计算价格相对位置"""
        position = (prices - lower) / (upper - lower) * 100
        position = position.replace([np.inf, -np.inf], np.nan)
        # 限制在合理范围内
        return position.clip(-50, 150)
    
    def _calculate_percent_b_vectorized(self, prices: pd.Series, upper: pd.Series, 
                                       lower: pd.Series) -> pd.Series:
        """向量化计算%B指标"""
        percent_b = (prices - lower) / (upper - lower)
        return percent_b.replace([np.inf, -np.inf], np.nan)
    
    def verify_calculation(self, df: pd.DataFrame, calculated_results: Dict) -> Tuple[bool, Dict]:
        """
        验证计算结果的准确性 - 独立算法验证
        
        Args:
            df: 原始数据
            calculated_results: 计算结果
            
        Returns:
            Tuple[bool, Dict]: (是否正确, 验证信息)
        """
        try:
            if len(df) < self.period:
                return False, {'error': '数据不足'}
            
            # 独立计算验证
            prices = df['收盘价'].tail(self.period)
            
            # 验证中轨计算 - 使用独立算法
            expected_middle = prices.mean()
            calculated_middle = calculated_results.get('bb_middle')
            
            if calculated_middle is None:
                return False, {'error': '中轨计算失败'}
            
            # 允许微小的浮点误差
            tolerance = 1e-6
            middle_diff = abs(calculated_middle - expected_middle)
            
            if middle_diff > tolerance:
                return False, {
                    'error': '中轨计算不准确',
                    'expected': expected_middle,
                    'calculated': calculated_middle,
                    'difference': middle_diff
                }
            
            # 验证标准差计算
            expected_std = prices.std(ddof=1)  # 样本标准差
            upper_calculated = calculated_results.get('bb_upper')
            lower_calculated = calculated_results.get('bb_lower')
            
            if None in [upper_calculated, lower_calculated]:
                return False, {'error': '上下轨计算失败'}
            
            expected_upper = expected_middle + (self.std_multiplier * expected_std)
            expected_lower = expected_middle - (self.std_multiplier * expected_std)
            
            upper_diff = abs(upper_calculated - expected_upper)
            lower_diff = abs(lower_calculated - expected_lower)
            
            if upper_diff > tolerance or lower_diff > tolerance:
                return False, {
                    'error': '上下轨计算不准确',
                    'upper_diff': upper_diff,
                    'lower_diff': lower_diff,
                    'expected_std': expected_std
                }
            
            return True, {
                'middle_verified': True,
                'bands_verified': True,
                'std_verified': True,
                'tolerance_used': tolerance
            }
            
        except Exception as e:
            return False, {'error': f'验证过程异常: {str(e)}'}
    
    def get_calculation_summary(self, bb_results: Dict[str, Optional[float]]) -> Dict:
        """获取计算汇总信息"""
        total_indicators = len(bb_results)
        successful_calcs = sum(1 for v in bb_results.values() if v is not None)
        
        success_rate = (successful_calcs / total_indicators) * 100 if total_indicators > 0 else 0
        
        return {
            'total_indicators': total_indicators,
            'successful_calculations': successful_calcs,
            'success_rate': round(success_rate, 2),
            'bb_period': self.period,
            'std_multiplier': self.std_multiplier,
            'precision': self.precision,
            'algorithm_version': 'John Bollinger Standard (1983)'
        }