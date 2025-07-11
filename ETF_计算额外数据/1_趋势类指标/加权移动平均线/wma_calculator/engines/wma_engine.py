#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
WMA计算引擎模块 - 重构版
=======================

从原有wma_engine.py完全迁移，保持所有算法和计算逻辑不变
确保WMA计算结果完全一致
"""

import pandas as pd
import numpy as np
from typing import Dict, Optional, List, Tuple
from ..infrastructure.config import WMAConfig


class WMAEngine:
    """WMA计算引擎 - 重构版（算法完全一致）"""
    
    def __init__(self, config: WMAConfig):
        """
        初始化WMA计算引擎 - 保持原有初始化逻辑
        
        Args:
            config: WMA配置对象
        """
        self.config = config
        
        print("🔬 WMA计算引擎初始化完成 (科学严谨版)")
        print(f"   🎯 支持周期: {self.config.wma_periods}")
        print(f"   📊 数据策略: 使用所有可用数据，不限制行数")
        print(f"   🔬 算法标准: 严格按照标准WMA公式计算")
    
    def calculate_single_wma(self, prices: pd.Series, period: int) -> pd.Series:
        """
        计算单个周期的加权移动平均线 - 保持原有算法完全一致
        
        🔬 科学公式: WMA = Σ(Price_i × Weight_i) / Σ(Weight_i)
        其中 Weight_i = i (i = 1, 2, ..., n)，最新价格权重最大
        
        Args:
            prices: 价格序列
            period: WMA周期
            
        Returns:
            pd.Series: WMA值序列
        """
        # 科学验证：检查输入数据 - 保持原有验证逻辑
        if len(prices) < period:
            print(f"⚠️  科学警告: 数据长度({len(prices)})小于周期({period})")
            return pd.Series([np.nan] * len(prices), index=prices.index)
        
        # 标准WMA权重计算：线性递增权重 - 保持原有权重计算
        weights = np.arange(1, period + 1, dtype=np.float64)
        weights_sum = weights.sum()
        
        def calculate_wma_point(price_window):
            """
            计算单个WMA点的严格算法 - 保持原有算法完全一致
            
            🔬 科学公式: WMA = Σ(Price_i × i) / Σ(i), i=1,2,...,n
            """
            if len(price_window) < period:
                return np.nan
            
            # 确保数据类型为float64，提高计算精度
            price_array = np.array(price_window, dtype=np.float64)
            
            # 严格按照WMA公式计算 - 保持原有公式
            weighted_sum = np.dot(price_array, weights)
            wma_value = weighted_sum / weights_sum
            
            return wma_value
        
        # 使用滑动窗口计算WMA - 保持原有滑动窗口逻辑
        wma_values = prices.rolling(window=period, min_periods=period).apply(
            calculate_wma_point, raw=True
        )
        
        return wma_values
    
    def calculate_all_wma(self, df: pd.DataFrame) -> Dict[str, Optional[float]]:
        """
        计算所有周期的WMA指标 - 保持原有完整逻辑
        
        Args:
            df: 包含价格数据的DataFrame
            
        Returns:
            Dict[str, Optional[float]]: WMA结果字典
        """
        print("🔬 开始科学WMA计算...")
        wma_results = {}
        
        # 科学验证：数据完整性检查 - 保持原有验证
        if df.empty:
            print("❌ 科学错误: 输入数据为空")
            return wma_results
            
        if '收盘价' not in df.columns:
            print("❌ 科学错误: 缺少收盘价字段")
            return wma_results
        
        # 数据验证：检查是否有足够数据 - 改进的验证逻辑
        max_period = max(self.config.wma_periods) if self.config.wma_periods else 20
        available_periods = [p for p in self.config.wma_periods if p <= len(df)]
        
        if len(available_periods) == 0:
            min_period = min(self.config.wma_periods) if self.config.wma_periods else 3
            print(f"❌ 数据不足: 数据行数({len(df)})小于最小周期({min_period})")
            return wma_results
        
        if len(available_periods) < len(self.config.wma_periods):
            unavailable_periods = [p for p in self.config.wma_periods if p > len(df)]
            print(f"⚠️ 部分周期将跳过: {unavailable_periods} (数据不足)")
        
        print(f"📊 数据概况: {len(df)}行历史数据，支持周期: {available_periods}")
        
        prices = df['收盘价'].copy()
        
        # 科学验证：价格数据检查 - 保持原有验证
        if prices.isnull().any():
            print(f"⚠️  科学警告: 检测到{prices.isnull().sum()}个缺失价格值")
            prices = prices.ffill()
        
        # 计算各周期WMA - 只计算可用周期
        for period in available_periods:
            try:
                
                wma_values = self.calculate_single_wma(prices, period)
                
                # 获取最新的有效值 - 保持原有获取逻辑
                valid_wma_values = wma_values.dropna()
                
                if not valid_wma_values.empty:
                    latest_wma = valid_wma_values.iloc[-1]
                    # 科学精度：保留6位小数 - 保持原有精度
                    latest_wma = round(float(latest_wma), 6)
                    wma_results[f'WMA_{period}'] = latest_wma
                    
                    valid_count = len(valid_wma_values)
                    efficiency = ((len(prices) - period + 1) / len(prices)) * 100
                    
                    print(f"  ✅ WMA_{period}: {valid_count} 个有效值 → 最新: {latest_wma:.6f} (效率: {efficiency:.1f}%)")
                else:
                    print(f"  ❌ WMA_{period}: 无有效数据")
                    wma_results[f'WMA_{period}'] = None
                    
            except Exception as e:
                print(f"  ❌ WMA_{period} 计算异常: {str(e)}")
                wma_results[f'WMA_{period}'] = None
        
        # 计算WMA差值 - 保持原有差值计算
        wmadiff_results = self.calculate_wma_diff(wma_results)
        wma_results.update(wmadiff_results)
        
        # 科学统计：计算成功率 - 保持原有统计
        total_periods = len(self.config.wma_periods)
        successful_calcs = sum(1 for k, v in wma_results.items() if k.startswith('WMA_') and 'DIFF' not in k and v is not None)
        success_rate = (successful_calcs / total_periods) * 100
        
        print(f"🔬 WMA计算完成: {successful_calcs}/{total_periods} 成功 (成功率: {success_rate:.1f}%)")
        
        return wma_results
    
    def calculate_wma_diff(self, wma_results: Dict[str, Optional[float]]) -> Dict[str, Optional[float]]:
        """
        计算WMA差值指标 - 统一使用下划线格式
        
        Args:
            wma_results: WMA计算结果
            
        Returns:
            Dict[str, Optional[float]]: WMA差值结果
        """
        print("🔬 开始计算WMA差值指标...")
        wmadiff_results = {}
        
        # 科学配置：差值组合 - 统一使用下划线格式
        diff_pairs = [
            (5, 20, "WMA_DIFF_5_20"),    # 核心趋势指标
            (3, 5, "WMA_DIFF_3_5"),      # 超短期动量指标
        ]
        
        for short_period, long_period, diff_key in diff_pairs:
            try:
                short_wma = wma_results.get(f'WMA_{short_period}')
                long_wma = wma_results.get(f'WMA_{long_period}')
                
                if short_wma is not None and long_wma is not None:
                    # 科学计算：短期WMA - 长期WMA - 保持原有公式
                    diff_value = short_wma - long_wma
                    
                    # 科学精度：保留6位小数 - 保持原有精度
                    diff_value = round(diff_value, 6)
                    wmadiff_results[diff_key] = diff_value
                    
                    # 科学解释 - 保持原有解释逻辑
                    trend_strength = abs(diff_value)
                    if diff_value > 0:
                        trend_desc = f"上升趋势 (强度: {trend_strength:.6f})"
                    elif diff_value < 0:
                        trend_desc = f"下降趋势 (强度: {trend_strength:.6f})"
                    else:
                        trend_desc = "平衡状态"
                    
                    print(f"  ✅ {diff_key}: {diff_value:.6f} → {trend_desc}")
                else:
                    wmadiff_results[diff_key] = None
                    print(f"  ❌ {diff_key}: 缺少必要的WMA数据 (WMA_{short_period}: {short_wma}, WMA_{long_period}: {long_wma})")
                    
            except Exception as e:
                print(f"  ❌ {diff_key} 计算异常: {str(e)}")
                wmadiff_results[diff_key] = None
        
        # 计算相对差值百分比 - 保持原有相对差值计算
        self._calculate_relative_wmadiff(wma_results, wmadiff_results)
        
        return wmadiff_results
    
    def _calculate_relative_wmadiff(self, wma_results: Dict, wmadiff_results: Dict):
        """
        计算相对WMA差值百分比 - 统一使用下划线格式
        
        Args:
            wma_results: WMA原始结果
            wmadiff_results: WMA差值结果 (会被修改)
        """
        try:
            # 计算WMA5-20的相对差值百分比 - 统一使用下划线格式
            if wmadiff_results.get('WMA_DIFF_5_20') is not None and wma_results.get('WMA_20') is not None:
                diff_abs = wmadiff_results['WMA_DIFF_5_20']
                wma_20 = wma_results['WMA_20']
                
                if wma_20 != 0:
                    relative_diff_pct = (diff_abs / wma_20) * 100
                    wmadiff_results['WMA_DIFF_5_20_PCT'] = round(relative_diff_pct, 4)
                    print(f"  ✅ WMA_DIFF_5_20_PCT: {relative_diff_pct:.4f}% (相对差值)")
                else:
                    wmadiff_results['WMA_DIFF_5_20_PCT'] = None
                    
        except Exception as e:
            print(f"  ⚠️  相对差值计算警告: {str(e)}")
            wmadiff_results['WMA_DIFF_5_20_PCT'] = None
    
    def verify_wma_calculation(self, prices: pd.Series, period: int, expected_wma: float) -> Tuple[bool, float]:
        """
        验证WMA计算的正确性 - 保持原有验证逻辑
        
        Args:
            prices: 价格序列
            period: WMA周期
            expected_wma: 期望的WMA值
            
        Returns:
            Tuple[bool, float]: (是否正确, 实际计算值)
        """
        if len(prices) < period:
            return False, np.nan
        
        # 获取最近period个价格 - 保持原有逻辑
        recent_prices = prices.tail(period).values
        
        # 独立算法：手工计算WMA - 保持原有验证算法
        weights = np.arange(1, period + 1, dtype=np.float64)
        weighted_sum = np.sum(recent_prices * weights)
        weights_sum = np.sum(weights)
        independent_wma = weighted_sum / weights_sum
        
        # 精度比较 - 保持原有精度
        tolerance = 1e-6
        is_correct = abs(independent_wma - expected_wma) < tolerance
        
        return is_correct, round(independent_wma, 6) 