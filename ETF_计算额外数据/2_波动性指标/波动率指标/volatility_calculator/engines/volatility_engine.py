#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
波动率计算引擎 - 模仿布林带完善实现
============================

高效的波动率指标计算核心引擎
支持向量化计算和多种波动率衍生指标
完全模仿布林带系统的稳健架构
"""

import pandas as pd
import numpy as np
from typing import Dict, Optional, List, Tuple
from ..infrastructure.config import VolatilityConfig


class VolatilityEngine:
    """波动率计算引擎 - 模仿布林带完善实现"""
    
    def __init__(self, config: VolatilityConfig):
        """初始化波动率计算引擎（模仿布林带架构）"""
        self.config = config
        self.volatility_periods = config.volatility_periods
        self.annualized = config.annualized
        self.trading_days_per_year = config.trading_days_per_year
        self.precision = getattr(config, 'precision', 8)
    
    def _calculate_returns(self, prices: pd.Series) -> pd.Series:
        """计算对数收益率（模仿布林带的稳健数据处理）"""
        return np.log(prices / prices.shift(1)).replace([np.inf, -np.inf], np.nan)
    
    def _calculate_rolling_volatility(self, prices: pd.Series, period: int) -> pd.Series:
        """计算滚动波动率（完全模仿布林带的滚动标准差计算）"""
        returns = self._calculate_returns(prices)
        # 标准的滚动窗口计算，但允许部分数据
        volatility = returns.rolling(window=period, min_periods=period//2).std()
        
        # 年化处理（如果启用）
        if self.annualized:
            volatility = volatility * np.sqrt(self.trading_days_per_year)
            
        return volatility
    
    def _calculate_price_range(self, high: pd.Series, low: pd.Series, close: pd.Series) -> pd.Series:
        """计算价格振幅（模仿布林带的向量化计算）"""
        prev_close = close.shift(1)
        price_range = ((high - low) / prev_close * 100).replace([np.inf, -np.inf], np.nan)
        return price_range
    
    def _calculate_volatility_ratios(self, vol_20: Optional[float], vol_30: Optional[float]) -> Tuple[Optional[float], Optional[str]]:
        """计算波动率比率和状态（模仿布林带的衍生指标计算）"""
        if None in [vol_20, vol_30] or vol_30 == 0:
            return None, None
        
        try:
            vol_ratio = vol_20 / vol_30
            
            # 波动率状态判断
            if vol_ratio > 1.5:
                vol_state = "HIGH"
            elif vol_ratio > 1.2:
                vol_state = "MEDIUM"
            elif vol_ratio > 0.8:
                vol_state = "NORMAL"
            else:
                vol_state = "LOW"
                
            return vol_ratio, vol_state
        except (ZeroDivisionError, Exception):
            return None, None
    
    def _calculate_volatility_level(self, vol_10: Optional[float]) -> Optional[str]:
        """计算波动率水平（模仿布林带的分级方法）"""
        if vol_10 is None:
            return None
            
        try:
            if self.annualized:
                if vol_10 > 0.4:  # 40%
                    return "EXTREME_HIGH"
                elif vol_10 > 0.25:  # 25%
                    return "HIGH" 
                elif vol_10 > 0.15:  # 15%
                    return "MEDIUM"
                else:
                    return "LOW"
            else:
                if vol_10 > 0.025:  # 2.5%
                    return "EXTREME_HIGH"
                elif vol_10 > 0.016:  # 1.6%
                    return "HIGH"
                elif vol_10 > 0.009:  # 0.9%
                    return "MEDIUM"
                else:
                    return "LOW"
        except Exception:
            return None
    
    def _round_value(self, value: Optional[float]) -> Optional[float]:
        """按配置精度四舍五入（完全模仿布林带）"""
        if value is None:
            return None
        return round(value, self.precision)
    
    def _get_empty_result(self) -> Dict[str, Optional[float]]:
        """获取空结果（模仿布林带）"""
        result = {}
        
        # 历史波动率字段
        for period in self.volatility_periods:
            result[f'vol_{period}'] = None
            
        # 滚动波动率字段
        for period in [10, 30]:
            result[f'rolling_vol_{period}'] = None
            
        # 其他指标
        result.update({
            'price_range': None,
            'vol_ratio_20_30': None,
            'vol_state': None,
            'vol_level': None
        })
        
        return result
    
    def calculate_volatility_indicators(self, df: pd.DataFrame) -> Dict[str, Optional[float]]:
        """
        计算波动率指标 - 完全模仿布林带的计算方法
        
        Args:
            df: 包含价格数据的DataFrame
            
        Returns:
            Dict[str, Optional[float]]: 波动率指标结果字典
        """
        if df.empty or '收盘价' not in df.columns:
            return self._get_empty_result()
        
        # 检查数据量是否足够（模仿布林带的数据验证）
        max_period = max(self.volatility_periods) if self.volatility_periods else 60
        if len(df) < max_period:
            return self._get_empty_result()
        
        try:
            # 获取价格数据
            close_prices = df['收盘价'].copy()
            high_prices = df.get('最高价', close_prices)
            low_prices = df.get('最低价', close_prices)
            
            # 计算价格振幅（添加安全检查）
            price_range = self._calculate_price_range(high_prices, low_prices, close_prices)
            if len(price_range) > 0 and not price_range.empty:
                latest_value = price_range.iloc[-1]
                latest_price_range = float(latest_value) if not pd.isna(latest_value) else None
            else:
                latest_price_range = None
            
            # 计算各周期历史波动率
            vol_results = {}
            for period in self.volatility_periods:
                if period <= len(df):
                    volatility = self._calculate_rolling_volatility(close_prices, period)
                    if len(volatility) > 0 and not volatility.empty:
                        latest_value = volatility.iloc[-1]
                        latest_vol = float(latest_value) if not pd.isna(latest_value) else None
                    else:
                        latest_vol = None
                    vol_results[f'vol_{period}'] = self._round_value(latest_vol)
                else:
                    vol_results[f'vol_{period}'] = None
            
            # 计算滚动波动率（添加安全检查）
            for period in [10, 30]:
                if period <= len(df):
                    rolling_vol = self._calculate_rolling_volatility(close_prices, period)
                    if len(rolling_vol) > 0 and not rolling_vol.empty:
                        latest_value = rolling_vol.iloc[-1]
                        latest_rolling = float(latest_value) if not pd.isna(latest_value) else None
                    else:
                        latest_rolling = None
                    vol_results[f'rolling_vol_{period}'] = self._round_value(latest_rolling)
            
            # 计算衍生指标
            vol_20 = vol_results.get('vol_20')
            vol_30 = vol_results.get('vol_30')
            vol_10 = vol_results.get('vol_10')
            
            vol_ratio, vol_state = self._calculate_volatility_ratios(vol_20, vol_30)
            vol_level = self._calculate_volatility_level(vol_10)
            
            # 组合所有结果
            result = vol_results.copy()
            result.update({
                'price_range': self._round_value(latest_price_range),
                'vol_ratio_20_30': self._round_value(vol_ratio),
                'vol_state': vol_state,
                'vol_level': vol_level
            })
            
            return result
            
        except Exception as e:
            return self._get_empty_result()
    
    def calculate_historical_volatility_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        计算历史波动率数据（向量化） - 修复时序逻辑错误
        
        Args:
            df: ETF数据DataFrame
            
        Returns:
            pd.DataFrame: 包含历史波动率数据的DataFrame
        """
        if df.empty or '收盘价' not in df.columns:
            return pd.DataFrame()
        
        max_period = max(self.volatility_periods) if self.volatility_periods else 60
        if len(df) < max_period:
            return pd.DataFrame()
        
        try:
            # 先确保数据按日期正序排列（最早在前）用于计算
            calc_df = df.copy()
            if '日期' in calc_df.columns:
                calc_df = calc_df.sort_values('日期', ascending=True).reset_index(drop=True)
            
            close_prices = calc_df['收盘价']
            high_prices = calc_df.get('最高价', close_prices)
            low_prices = calc_df.get('最低价', close_prices)
            
            # 向量化计算价格振幅（使用正序数据）
            calc_df['price_range'] = self._calculate_price_range(high_prices, low_prices, close_prices)
            
            # 向量化计算各周期波动率（使用正序数据）
            for period in self.volatility_periods:
                if period <= len(calc_df):
                    calc_df[f'vol_{period}'] = self._calculate_rolling_volatility(close_prices, period)
                else:
                    calc_df[f'vol_{period}'] = np.nan
            
            # 向量化计算滚动波动率（使用正序数据）
            for period in [10, 30]:
                if period <= len(calc_df):
                    calc_df[f'rolling_vol_{period}'] = self._calculate_rolling_volatility(close_prices, period)
                else:
                    calc_df[f'rolling_vol_{period}'] = np.nan
            
            # 向量化计算衍生指标
            self._calculate_vectorized_indicators(calc_df)
            
            # 计算完成后，保持日期降序排列用于输出（最新在前）
            # 重要：必须保持计算时的数据对应关系，不能简单排序
            result_df = calc_df.sort_values('日期', ascending=False).reset_index(drop=True)
            
            # 四舍五入到指定精度（模仿布林带的精度处理）
            vol_columns = [f'vol_{p}' for p in self.volatility_periods] + \
                         [f'rolling_vol_{p}' for p in [10, 30]] + \
                         ['price_range', 'vol_ratio_20_30']
            
            for col in vol_columns:
                if col in result_df.columns:
                    result_df[col] = result_df[col].round(self.precision)
            
            return result_df
            
        except Exception as e:
            return pd.DataFrame()
    
    def _calculate_vectorized_indicators(self, df: pd.DataFrame) -> None:
        """向量化计算波动率衍生指标（模仿布林带的向量化方法）"""
        try:
            # 计算波动率比率（向量化）
            if 'vol_20' in df.columns and 'vol_30' in df.columns:
                vol_20 = df['vol_20']
                vol_30 = df['vol_30']
                
                # 避免除零（模仿布林带的安全除法）
                vol_ratio = np.where(vol_30 != 0, vol_20 / vol_30, np.nan)
                df['vol_ratio_20_30'] = vol_ratio
                
                # 向量化波动率状态判断
                vol_state = np.select(
                    [vol_ratio > 1.5, vol_ratio > 1.2, vol_ratio > 0.8],
                    ['HIGH', 'MEDIUM', 'NORMAL'],
                    default='LOW'
                )
                df['vol_state'] = vol_state
            
            # 计算波动率水平（向量化）
            if 'vol_10' in df.columns:
                vol_10 = df['vol_10']
                
                if self.annualized:
                    vol_level = np.select(
                        [vol_10 > 0.4, vol_10 > 0.25, vol_10 > 0.15],
                        ['EXTREME_HIGH', 'HIGH', 'MEDIUM'],
                        default='LOW'
                    )
                else:
                    vol_level = np.select(
                        [vol_10 > 0.025, vol_10 > 0.016, vol_10 > 0.009],
                        ['EXTREME_HIGH', 'HIGH', 'MEDIUM'],
                        default='LOW'
                    )
                
                df['vol_level'] = vol_level
                
        except Exception as e:
            pass  # 静默处理异常，确保主流程不受影响
    
    def verify_calculation(self, df: pd.DataFrame, calculated_results: Dict) -> Tuple[bool, Dict]:
        """
        验证计算结果的准确性 - 完全模仿布林带的验证方法
        
        Args:
            df: 原始数据
            calculated_results: 计算结果
            
        Returns:
            Tuple[bool, Dict]: (是否正确, 验证信息)
        """
        try:
            max_period = max(self.volatility_periods) if self.volatility_periods else 60
            if len(df) < max_period:
                return False, {'error': '数据不足'}
            
            # 独立计算验证（模仿布林带的独立验证算法）
            close_prices = df['收盘价'].copy()
            
            # 验证最新的波动率计算
            for period in self.volatility_periods:
                if period <= len(df):
                    # 独立计算
                    returns = self._calculate_returns(close_prices)
                    if len(returns) >= period:
                        recent_returns = returns.tail(period)
                        expected_vol = recent_returns.std()
                        
                        if self.annualized:
                            expected_vol = expected_vol * np.sqrt(self.trading_days_per_year)
                        
                        calculated_vol = calculated_results.get(f'vol_{period}')
                        
                        if calculated_vol is None:
                            return False, {'error': f'vol_{period} 计算失败'}
                        
                        # 允许微小的浮点误差（模仿布林带的容错机制）
                        tolerance = 1e-6
                        vol_diff = abs(calculated_vol - expected_vol)
                        
                        if vol_diff > tolerance:
                            return False, {
                                'error': f'vol_{period} 计算不准确',
                                'expected': round(expected_vol, 8),
                                'calculated': calculated_vol,
                                'difference': vol_diff
                            }
            
            return True, {
                'volatility_verified': True,
                'tolerance_used': 1e-6,
                'periods_checked': self.volatility_periods
            }
            
        except Exception as e:
            return False, {'error': f'验证过程异常: {str(e)}'}
    
    def get_calculation_summary(self, vol_results: Dict[str, Optional[float]]) -> Dict:
        """获取计算汇总信息（模仿布林带的汇总方法）"""
        total_indicators = len(vol_results)
        successful_calcs = sum(1 for v in vol_results.values() if v is not None)
        
        success_rate = (successful_calcs / total_indicators) * 100 if total_indicators > 0 else 0
        
        return {
            'total_indicators': total_indicators,
            'successful_calculations': successful_calcs,
            'success_rate': round(success_rate, 2),
            'volatility_periods': self.volatility_periods,
            'annualized': self.annualized,
            'precision': self.precision
        }