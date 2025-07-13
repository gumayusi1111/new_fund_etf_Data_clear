#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
波动率计算引擎
============

实现多种波动率指标的计算，包括：
1. 历史波动率 (Historical Volatility)
2. 价格振幅 (Price Range)
3. 滚动波动率 (Rolling Volatility)
"""

import pandas as pd
import numpy as np
from typing import Dict, Optional, List, Tuple
from ..infrastructure.config import VolatilityConfig


class VolatilityEngine:
    """波动率计算引擎"""
    
    def __init__(self, config: VolatilityConfig):
        """
        初始化波动率计算引擎
        
        Args:
            config: 波动率配置对象
        """
        self.config = config
        
        print("📊 波动率计算引擎初始化完成")
        print(f"   🎯 支持周期: {self.config.volatility_periods}")
        print(f"   📊 年化计算: {'启用' if self.config.annualized else '禁用'}")
        print(f"   📈 算法标准: 严格按照标准波动率公式计算")
    
    def calculate_returns(self, prices: pd.Series) -> pd.Series:
        """
        计算收益率序列
        
        Args:
            prices: 价格序列
            
        Returns:
            pd.Series: 收益率序列
        """
        # 计算对数收益率
        returns = np.log(prices / prices.shift(1))
        return returns.dropna()
    
    def calculate_simple_returns(self, prices: pd.Series) -> pd.Series:
        """
        计算简单收益率序列
        
        Args:
            prices: 价格序列
            
        Returns:
            pd.Series: 简单收益率序列
        """
        # 计算简单收益率
        returns = (prices / prices.shift(1) - 1)
        return returns.dropna()
    
    def calculate_historical_volatility(self, prices: pd.Series, period: int) -> pd.Series:
        """
        计算历史波动率
        
        📊 公式: 
        - 收益率 = ln(P_t / P_{t-1})
        - 波动率 = std(收益率, period)
        - 年化波动率 = 波动率 × √252 (如果启用年化)
        
        Args:
            prices: 价格序列
            period: 计算周期
            
        Returns:
            pd.Series: 历史波动率序列
        """
        if len(prices) < period + 1:
            print(f"⚠️ 历史波动率计算: 数据长度({len(prices)})小于所需周期({period+1})")
            return pd.Series([np.nan] * len(prices), index=prices.index)
        
        # 计算收益率
        returns = self.calculate_returns(prices)
        
        # 计算滚动标准差
        volatility = returns.rolling(window=period, min_periods=period).std()
        
        # 年化处理
        if self.config.annualized:
            volatility = volatility * np.sqrt(self.config.trading_days_per_year)
        
        # 调整索引以匹配原始价格序列
        volatility = volatility.reindex(prices.index)
        
        return volatility
    
    def calculate_price_range(self, high: pd.Series, low: pd.Series, close: pd.Series) -> pd.Series:
        """
        计算价格振幅
        
        📊 公式: 价格振幅 = (最高价 - 最低价) / 昨收盘价 × 100%
        
        Args:
            high: 最高价序列
            low: 最低价序列
            close: 收盘价序列
            
        Returns:
            pd.Series: 价格振幅序列（百分比）
        """
        # 获取前一日收盘价
        prev_close = close.shift(1)
        
        # 计算价格振幅百分比
        price_range = ((high - low) / prev_close * 100).fillna(0)
        
        return price_range
    
    def calculate_rolling_volatility(self, prices: pd.Series, period: int, 
                                   method: str = 'std') -> pd.Series:
        """
        计算滚动波动率
        
        Args:
            prices: 价格序列
            period: 滚动窗口大小
            method: 计算方法 ('std' 或 'parkinson')
            
        Returns:
            pd.Series: 滚动波动率序列
        """
        if method == 'std':
            return self.calculate_historical_volatility(prices, period)
        elif method == 'parkinson':
            # Parkinson方法需要高低价数据，这里简化使用标准方法
            return self.calculate_historical_volatility(prices, period)
        else:
            raise ValueError(f"不支持的计算方法: {method}")
    
    def calculate_volatility_indicators(self, df: pd.DataFrame) -> Dict[str, Optional[float]]:
        """
        计算所有波动率指标
        
        Args:
            df: 包含价格数据的DataFrame
            
        Returns:
            Dict[str, Optional[float]]: 波动率指标结果字典
        """
        print("📊 开始波动率指标计算...")
        volatility_results = {}
        
        # 数据验证
        if df.empty:
            print("❌ 输入数据为空")
            return volatility_results
        
        required_columns = ['最高价', '最低价', '收盘价']
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            print(f"❌ 缺少必需字段: {missing_columns}")
            return volatility_results
        
        # 获取价格数据
        high_prices = df['最高价'].copy()
        low_prices = df['最低价'].copy()
        close_prices = df['收盘价'].copy()
        
        # 数据完整性检查
        if close_prices.isnull().any():
            print(f"⚠️ 检测到{close_prices.isnull().sum()}个缺失收盘价值")
            close_prices = close_prices.ffill()
        
        # 1. 计算价格振幅 - 按第一大类标准使用英文字段名
        try:
            price_range = self.calculate_price_range(high_prices, low_prices, close_prices)
            if not price_range.empty:
                latest_range = price_range.iloc[-1] if len(price_range) > 0 else None  # 最新数据在最后
                volatility_results['PRICE_RANGE'] = round(float(latest_range), 8) if latest_range is not None else None
                
                if latest_range is not None:
                    print(f"  ✅ PRICE_RANGE: {latest_range:.4f}%")
                else:
                    print(f"  ❌ PRICE_RANGE: 计算失败")
            else:
                volatility_results['PRICE_RANGE'] = None
                print(f"  ❌ PRICE_RANGE: 无有效数据")
                
        except Exception as e:
            print(f"  ❌ PRICE_RANGE 计算异常: {str(e)}")
            volatility_results['PRICE_RANGE'] = None
        
        # 2. 计算各周期历史波动率
        available_periods = [p for p in self.config.volatility_periods if p <= len(df)]
        
        if len(available_periods) == 0:
            min_period = min(self.config.volatility_periods) if self.config.volatility_periods else 10
            print(f"❌ 数据不足: 数据行数({len(df)})小于最小周期({min_period})")
            return volatility_results
        
        if len(available_periods) < len(self.config.volatility_periods):
            unavailable_periods = [p for p in self.config.volatility_periods if p > len(df)]
            print(f"⚠️ 部分周期将跳过: {unavailable_periods} (数据不足)")
        
        print(f"📊 数据概况: {len(df)}行历史数据，支持周期: {available_periods}")
        
        for period in available_periods:
            try:
                # 计算历史波动率
                historical_vol = self.calculate_historical_volatility(close_prices, period)
                
                # 获取最新的有效值
                valid_vol_values = historical_vol.dropna()
                
                if not valid_vol_values.empty:
                    latest_vol = valid_vol_values.iloc[-1]  # 最新数据在最后
                    latest_vol = round(float(latest_vol), 8)
                    volatility_results[f'VOL_{period}'] = latest_vol  # 按第一大类标准使用英文字段名
                    
                    valid_count = len(valid_vol_values)
                    efficiency = ((len(close_prices) - period) / len(close_prices)) * 100
                    
                    unit = "(年化)" if self.config.annualized else "(日)"
                    print(f"  ✅ VOL_{period}: {valid_count} 个有效值 → 最新: {latest_vol:.6f} {unit} (效率: {efficiency:.1f}%)")
                else:
                    print(f"  ❌ VOL_{period}: 无有效数据")
                    volatility_results[f'VOL_{period}'] = None
                    
            except Exception as e:
                print(f"  ❌ Volatility_{period} 计算异常: {str(e)}")
                volatility_results[f'Volatility_{period}'] = None
        
        # 3. 计算滚动波动率（使用不同周期）
        rolling_periods = [10, 30]  # 固定使用10日和30日滚动
        
        for period in rolling_periods:
            if period <= len(df):
                try:
                    rolling_vol = self.calculate_rolling_volatility(close_prices, period)
                    
                    valid_rolling_values = rolling_vol.dropna()
                    
                    if not valid_rolling_values.empty:
                        latest_rolling = valid_rolling_values.iloc[-1]  # 最新数据在最后
                        latest_rolling = round(float(latest_rolling), 8)
                        volatility_results[f'ROLLING_VOL_{period}'] = latest_rolling  # 按第一大类标准使用英文字段名
                        
                        unit = "(年化)" if self.config.annualized else "(日)"
                        print(f"  ✅ ROLLING_VOL_{period}: {latest_rolling:.6f} {unit}")
                    else:
                        volatility_results[f'ROLLING_VOL_{period}'] = None
                        print(f"  ❌ ROLLING_VOL_{period}: 无有效数据")
                        
                except Exception as e:
                    print(f"  ❌ Rolling_Vol_{period} 计算异常: {str(e)}")
                    volatility_results[f'Rolling_Vol_{period}'] = None
        
        # 4. 计算波动率比率和状态指标
        try:
            self._calculate_volatility_ratios(volatility_results)
        except Exception as e:
            print(f"  ⚠️ 波动率比率计算异常: {str(e)}")
        
        # 计算成功率统计
        total_indicators = len(self.config.volatility_periods) + 2 + len(rolling_periods) + 1  # 历史波动率 + 滚动波动率 + 价格振幅
        successful_calcs = sum(1 for v in volatility_results.values() if v is not None)
        success_rate = (successful_calcs / total_indicators) * 100
        
        print(f"📊 波动率计算完成: {successful_calcs}/{total_indicators} 成功 (成功率: {success_rate:.1f}%)")
        
        return volatility_results
    
    def _calculate_volatility_ratios(self, volatility_results: Dict[str, Optional[float]]) -> None:
        """
        计算波动率比率和衍生指标
        
        Args:
            volatility_results: 波动率结果字典（会被修改）
        """
        try:
            # 计算短期/长期波动率比率 - 按第一大类标准使用英文字段名
            vol_20 = volatility_results.get('VOL_20')
            vol_60 = volatility_results.get('VOL_60')
            
            if vol_20 is not None and vol_60 is not None and vol_60 != 0:
                vol_ratio = vol_20 / vol_60
                volatility_results['VOL_RATIO_20_60'] = round(vol_ratio, 8)  # 保持8位小数精度
                
                # 波动率状态判断
                if vol_ratio > 1.5:
                    vol_state = "HIGH"
                elif vol_ratio > 1.2:
                    vol_state = "MEDIUM"
                elif vol_ratio > 0.8:
                    vol_state = "NORMAL"
                else:
                    vol_state = "LOW"
                
                volatility_results['VOL_STATE'] = vol_state
                print(f"  ✅ VOL_RATIO_20_60: {vol_ratio:.4f} → {vol_state}")
            
            # 计算波动率分位数（需要历史数据，这里简化处理）
            vol_10 = volatility_results.get('VOL_10')
            if vol_10 is not None:
                # 简化的波动率水平判断
                if self.config.annualized:
                    if vol_10 > 0.4:  # 40%
                        vol_level = "EXTREME_HIGH"
                    elif vol_10 > 0.25:  # 25%
                        vol_level = "HIGH"
                    elif vol_10 > 0.15:  # 15%
                        vol_level = "MEDIUM"
                    else:
                        vol_level = "LOW"
                else:
                    if vol_10 > 0.025:  # 2.5%
                        vol_level = "EXTREME_HIGH"
                    elif vol_10 > 0.016:  # 1.6%
                        vol_level = "HIGH"
                    elif vol_10 > 0.009:  # 0.9%
                        vol_level = "MEDIUM"
                    else:
                        vol_level = "LOW"
                
                volatility_results['VOL_LEVEL'] = vol_level
                print(f"  ✅ VOL_LEVEL: {vol_level}")
                
        except Exception as e:
            print(f"  ⚠️ 波动率比率计算警告: {str(e)}")
    
    def verify_volatility_calculation(self, prices: pd.Series, period: int, 
                                    expected_vol: float) -> Tuple[bool, float]:
        """
        验证波动率计算的正确性
        
        Args:
            prices: 价格序列
            period: 波动率周期
            expected_vol: 期望的波动率值
            
        Returns:
            Tuple[bool, float]: (是否正确, 实际计算值)
        """
        if len(prices) < period + 1:
            return False, np.nan
        
        # 独立算法：手工计算波动率
        returns = np.log(prices / prices.shift(1)).dropna()
        
        if len(returns) < period:
            return False, np.nan
        
        recent_returns = returns.tail(period)
        independent_vol = recent_returns.std()
        
        if self.config.annualized:
            independent_vol = independent_vol * np.sqrt(self.config.trading_days_per_year)
        
        # 精度比较
        tolerance = 1e-6
        is_correct = abs(independent_vol - expected_vol) < tolerance
        
        return is_correct, round(independent_vol, 6)