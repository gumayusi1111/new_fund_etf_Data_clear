#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
EMA计算引擎 - 中短期专版
======================

实现科学严谨的指数移动平均线计算
专注于EMA12和EMA26，支持MACD基础指标
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional
from .config import EMAConfig


class EMAEngine:
    """EMA计算引擎 - 中短期专版"""
    
    def __init__(self, config: EMAConfig):
        """
        初始化EMA计算引擎
        
        Args:
            config: EMA配置对象
        """
        self.config = config
        print("⚙️  EMA计算引擎初始化完成")
        print(f"   📊 EMA周期: {self.config.ema_periods}")
        
        # 预计算平滑因子
        self.smoothing_factors = {}
        for period in self.config.ema_periods:
            alpha = self.config.get_smoothing_factor(period)
            self.smoothing_factors[period] = alpha
            print(f"   📈 EMA{period}: α = {alpha:.6f}")
    
    def calculate_ema_values(self, df: pd.DataFrame) -> Dict:
        """
        计算所有EMA指标值 - 科学严谨版
        
        Args:
            df: ETF价格数据 (按时间升序排列)
            
        Returns:
            Dict: EMA计算结果
        """
        try:
            if df.empty:
                return {}
            
            print(f"🔢 开始EMA计算，数据量: {len(df)}行")
            
            # 工作副本，保护原始数据
            work_df = df.copy()
            
            results = {}
            
            # 计算各周期EMA
            ema_values = {}
            for period in self.config.ema_periods:
                ema_column = f'EMA{period}'
                ema_values[period] = self._calculate_single_ema(work_df['收盘价'], period)
                work_df[ema_column] = ema_values[period]
                
                # 获取最新EMA值
                latest_ema = ema_values[period].iloc[-1]
                results[f'ema_{period}'] = round(float(latest_ema), 6)
                print(f"   ✅ EMA{period}: {results[f'ema_{period}']}")
            
            # 计算EMA差值指标（核心MACD组件）
            if 12 in self.config.ema_periods and 26 in self.config.ema_periods:
                ema_diff = ema_values[12].iloc[-1] - ema_values[26].iloc[-1]
                results['ema_diff_12_26'] = round(float(ema_diff), 6)
                
                # 相对差值百分比
                if ema_values[26].iloc[-1] != 0:
                    ema_diff_pct = (ema_diff / ema_values[26].iloc[-1]) * 100
                    results['ema_diff_12_26_pct'] = round(float(ema_diff_pct), 3)
                else:
                    results['ema_diff_12_26_pct'] = 0.0
                
                print(f"   📊 EMA差值(12-26): {results['ema_diff_12_26']}")
                print(f"   📊 EMA差值百分比: {results['ema_diff_12_26_pct']}%")
            
            # 计算短期EMA动量（EMA12相对于前一日）
            if 12 in self.config.ema_periods and len(ema_values[12]) >= 2:
                current_ema12 = ema_values[12].iloc[-1]
                prev_ema12 = ema_values[12].iloc[-2]
                ema12_momentum = current_ema12 - prev_ema12
                results['ema12_momentum'] = round(float(ema12_momentum), 6)
                print(f"   🔄 EMA12动量: {results['ema12_momentum']}")
            
            print(f"✅ EMA计算完成，共{len(results)}个指标")
            return results
            
        except Exception as e:
            print(f"❌ EMA计算失败: {str(e)}")
            return {}
    
    def _calculate_single_ema(self, prices: pd.Series, period: int) -> pd.Series:
        """
        计算单个周期的EMA - 科学严谨实现
        
        EMA公式: EMA(today) = α × Price(today) + (1-α) × EMA(yesterday)
        其中: α = 2/(period+1) (平滑因子)
        
        Args:
            prices: 价格序列
            period: EMA周期
            
        Returns:
            pd.Series: EMA序列
        """
        try:
            alpha = self.smoothing_factors[period]
            
            # 🔬 科学实现：使用pandas的ewm方法，确保计算精度
            # adjust=False：使用标准EMA公式
            # alpha=alpha：使用预计算的平滑因子
            ema_series = prices.ewm(alpha=alpha, adjust=False).mean()
            
            return ema_series
            
        except Exception as e:
            print(f"❌ EMA{period}计算失败: {str(e)}")
            return pd.Series(dtype=float)
    
    def calculate_ema_signals(self, df: pd.DataFrame, ema_values: Dict = None) -> Dict:
        """
        🚫 已简化：仅计算基础EMA数据，移除主观判断
        
        Args:
            df: ETF数据
            ema_values: 预计算的EMA值（避免重复计算）
            
        Returns:
            Dict: 基础EMA数据结果（无主观判断）
        """
        try:
            if df.empty or len(df) < max(self.config.ema_periods):
                return {'status': '数据不足'}
            
            # 使用预计算的EMA值或重新计算
            if ema_values is None:
                ema_results = self.calculate_ema_values(df)
            else:
                ema_results = ema_values
            if not ema_results:
                return {'status': '计算失败'}
            
            # 🚫 已移除所有主观判断代码 - 只返回基础数据
            basic_info = {
                'status': 'success',
                'ema_count': len([k for k in ema_results.keys() if k.startswith('ema_')]),
                'has_diff': 'ema_diff_12_26' in ema_results
            }
            
            # 合并EMA计算结果
            basic_info.update(ema_results)
            
            print(f"✅ EMA基础数据计算完成，共{basic_info['ema_count']}个EMA指标")
            return basic_info
            
        except Exception as e:
            print(f"❌ EMA数据计算失败: {str(e)}")
            return {'status': '计算错误'}
    
    def get_trend_direction_icon(self, signal_data: Dict) -> str:
        """
        🚫 已简化：获取趋势方向图标（仅基于客观数据）
        
        Args:
            signal_data: 信号数据
            
        Returns:
            str: 趋势图标
        """
        try:
            # 🚫 已移除主观判断 - 只基于客观差值数据
            diff = signal_data.get('ema_diff_12_26', 0)
            
            if diff > 0:
                return '📈'
            elif diff < 0:
                return '📉'
            else:
                return '➡️'
                
        except Exception:
            return '❓'
    
    def validate_ema_calculation(self, df: pd.DataFrame, ema_values: Dict = None) -> bool:
        """
        验证EMA计算结果的科学性
        
        Args:
            df: ETF数据
            ema_values: 预计算的EMA值（避免重复计算）
            
        Returns:
            bool: 计算是否有效
        """
        try:
            if df.empty or len(df) < max(self.config.ema_periods):
                return False
            
            # 使用预计算的EMA值或重新计算
            if ema_values is None:
                ema_results = self.calculate_ema_values(df)
            else:
                ema_results = ema_values
            
            # 基础验证
            for period in self.config.ema_periods:
                ema_key = f'ema_{period}'
                if ema_key not in ema_results:
                    return False
                
                ema_value = ema_results[ema_key]
                
                # 检查EMA值是否为正数
                if ema_value <= 0:
                    print(f"❌ EMA{period}值异常: {ema_value}")
                    return False
                
                # 检查EMA值是否在合理范围内（相对于当前价格）
                current_price = float(df['收盘价'].iloc[-1])
                ratio = ema_value / current_price
                
                if ratio < 0.5 or ratio > 2.0:  # EMA值应该接近当前价格
                    print(f"❌ EMA{period}值偏离过大: {ema_value} vs 价格{current_price}")
                    return False
            
            print("✅ EMA计算验证通过")
            return True
            
        except Exception as e:
            print(f"❌ EMA验证失败: {str(e)}")
            return False 