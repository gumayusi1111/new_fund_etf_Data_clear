#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SMA计算引擎 (重构版)
=================

高效的简单移动平均线计算核心引擎
"""

import pandas as pd
import numpy as np
from typing import Dict, Optional, List, Tuple
from ..infrastructure.config import SMAConfig


class SMAEngine:
    """SMA计算引擎 - 重构版"""
    
    def __init__(self, config: SMAConfig):
        """初始化SMA计算引擎"""
        self.config = config
        # 向后兼容性属性
        self.periods = config.sma_periods
    
    def calculate_single_sma(self, prices: pd.Series, period: int) -> pd.Series:
        """
        计算单个周期的简单移动平均线
        
        Args:
            prices: 价格序列
            period: SMA周期
            
        Returns:
            pd.Series: SMA值序列
        """
        if len(prices) < period:
            return pd.Series([np.nan] * len(prices), index=prices.index)
        
        return prices.rolling(window=period, min_periods=period).mean()
    
    def calculate_all_sma(self, df: pd.DataFrame) -> Dict[str, Optional[float]]:
        """
        计算所有SMA指标
        
        Args:
            df: ETF数据DataFrame
            
        Returns:
            Dict[str, Optional[float]]: SMA计算结果
        """
        if df.empty or '收盘价' not in df.columns:
            return {}
        
        # 使用所有可用数据
        work_df = df.copy()
        prices = work_df['收盘价']
        sma_results = {}
        
        # 计算所有SMA周期
        for period in self.config.sma_periods:
            if period > len(prices):
                sma_results[f'SMA_{period}'] = None
                continue
            
            try:
                sma_values = self.calculate_single_sma(prices, period)
                valid_sma_values = sma_values.dropna()
                
                if not valid_sma_values.empty:
                    latest_sma = float(valid_sma_values.iloc[-1])
                    sma_results[f'SMA_{period}'] = round(latest_sma, 8)
                else:
                    sma_results[f'SMA_{period}'] = None
            except Exception:
                sma_results[f'SMA_{period}'] = None
        
        # 计算SMA差值指标
        smadiff_results = self._calculate_sma_diff(sma_results)
        sma_results.update(smadiff_results)
        
        return sma_results
    
    def _calculate_sma_diff(self, sma_results: Dict[str, Optional[float]]) -> Dict[str, Optional[float]]:
        """计算SMA差值指标"""
        smadiff_results = {}
        
        # 差值计算配置
        diff_pairs = [
            (5, 20, "SMA_DIFF_5_20"),    # 核心趋势指标
            (5, 10, "SMA_DIFF_5_10"),    # 短期动量指标
        ]
        
        for short_period, long_period, diff_key in diff_pairs:
            short_sma = sma_results.get(f'SMA_{short_period}')
            long_sma = sma_results.get(f'SMA_{long_period}')
            
            if short_sma is not None and long_sma is not None:
                diff_value = short_sma - long_sma
                smadiff_results[diff_key] = round(diff_value, 8)
            else:
                smadiff_results[diff_key] = None
        
        # 计算相对差值百分比（安全除法）
        diff_5_20 = smadiff_results.get('SMA_DIFF_5_20')
        sma_20 = sma_results.get('SMA_20')
        
        if (diff_5_20 is not None and sma_20 is not None and 
            abs(sma_20) > 1e-10):  # 使用更严格的非零检查
            
            try:
                relative_diff_pct = (diff_5_20 / sma_20) * 100
                smadiff_results['SMA_DIFF_5_20_PCT'] = round(relative_diff_pct, 8)
            except (ZeroDivisionError, Exception):
                smadiff_results['SMA_DIFF_5_20_PCT'] = None
        else:
            smadiff_results['SMA_DIFF_5_20_PCT'] = None
        
        return smadiff_results
    
    def verify_sma_calculation(self, prices: pd.Series, period: int, 
                              calculated_value: float) -> Tuple[bool, float]:
        """
        验证SMA计算结果的准确性
        
        Args:
            prices: 价格序列
            period: SMA周期
            calculated_value: 计算得到的SMA值
            
        Returns:
            Tuple[bool, float]: (是否正确, 独立计算值)
        """
        try:
            if len(prices) < period:
                return False, np.nan
            
            # 独立计算：取最近n天的平均值
            recent_prices = prices.tail(period)
            independent_sma = recent_prices.mean()
            
            # 允许微小的浮点误差
            difference = abs(calculated_value - independent_sma)
            tolerance = 1e-6
            
            is_correct = difference < tolerance
            return is_correct, independent_sma
            
        except Exception:
            return False, np.nan
    
    def get_calculation_summary(self, sma_results: Dict[str, Optional[float]]) -> Dict:
        """获取计算汇总信息"""
        total_periods = len(self.config.sma_periods)
        successful_calcs = sum(1 for k, v in sma_results.items() 
                              if k.startswith('SMA_') and v is not None)
        
        success_rate = (successful_calcs / total_periods) * 100 if total_periods > 0 else 0
        
        return {
            'total_periods': total_periods,
            'successful_calculations': successful_calcs,
            'success_rate': round(success_rate, 2),
            'calculated_indicators': len([k for k in sma_results.keys() if sma_results[k] is not None])
        } 