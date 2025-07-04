#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MACD计算引擎
============

实现MACD指标的核心计算逻辑:
- DIF线 = EMA(Close, 12) - EMA(Close, 26)
- DEA线 = EMA(DIF, 9) 
- MACD柱 = (DIF - DEA) × 2

核心算法基于指数移动平均线，提供准确的MACD指标计算
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple, Union
from .config import MACDConfig


class MACDEngine:
    """MACD计算引擎 - 完整技术指标实现"""
    
    def __init__(self, config: MACDConfig):
        """
        初始化MACD计算引擎
        
        Args:
            config: MACD配置对象
        """
        self.config = config
        self.fast_period, self.slow_period, self.signal_period = config.get_macd_periods()
        
        print(f"🎯 MACD引擎初始化完成")
        print(f"⚙️ 参数设置: EMA({self.fast_period}, {self.slow_period}, {self.signal_period})")
    
    def calculate_ema(self, data: pd.Series, period: int) -> pd.Series:
        """
        计算指数移动平均线
        
        Args:
            data: 价格数据序列
            period: EMA周期
        
        Returns:
            EMA序列
        """
        return data.ewm(span=period, adjust=False).mean()
    
    def calculate_macd_components(self, close_prices: pd.Series) -> Dict[str, pd.Series]:
        """
        计算MACD的所有组成部分
        
        Args:
            close_prices: 收盘价序列
            
        Returns:
            包含DIF、DEA、MACD、EMA12、EMA26的字典
        """
        # 计算快慢EMA
        ema_fast = self.calculate_ema(close_prices, self.fast_period)
        ema_slow = self.calculate_ema(close_prices, self.slow_period)
        
        # 计算DIF线 (快线 - 慢线)
        dif = ema_fast - ema_slow
        
        # 计算DEA线 (DIF的信号线)
        dea = self.calculate_ema(dif, self.signal_period)
        
        # 计算MACD柱状图
        macd_histogram = (dif - dea) * 2
        
        return {
            'EMA_Fast': ema_fast,
            'EMA_Slow': ema_slow,
            'DIF': dif,
            'DEA': dea,
            'MACD': macd_histogram
        }
    
    def calculate_single_macd(self, close_prices: List[float]) -> Optional[Dict[str, float]]:
        """
        计算单个时间点的MACD值
        
        Args:
            close_prices: 收盘价列表 (至少需要slow_period + signal_period个数据点)
            
        Returns:
            MACD各组件的当前值，如果数据不足则返回None
        """
        if len(close_prices) < self.slow_period + self.signal_period:
            return None
        
        # 转换为pandas Series
        price_series = pd.Series(close_prices)
        
        # 计算所有组件
        components = self.calculate_macd_components(price_series)
        
        # 返回最新值
        return {
            'EMA_Fast': components['EMA_Fast'].iloc[-1],
            'EMA_Slow': components['EMA_Slow'].iloc[-1],
            'DIF': components['DIF'].iloc[-1],
            'DEA': components['DEA'].iloc[-1],
            'MACD': components['MACD'].iloc[-1]
        }
    
    def calculate_batch_macd(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        批量计算历史数据的MACD
        
        Args:
            df: 包含Close列的DataFrame
            
        Returns:
            添加了MACD指标列的DataFrame
        """
        if 'Close' not in df.columns:
            raise ValueError("输入DataFrame必须包含'Close'列")
        
        # 计算MACD组件
        components = self.calculate_macd_components(df['Close'])
        
        # 添加到原DataFrame
        result_df = df.copy()
        for name, values in components.items():
            result_df[name] = values
        
        return result_df
    
    def get_signal_status(self, dif: float, dea: float, prev_dif: Optional[float] = None, 
                         prev_dea: Optional[float] = None) -> Dict[str, Union[bool, str]]:
        """
        获取MACD信号状态
        
        Args:
            dif: 当前DIF值
            dea: 当前DEA值
            prev_dif: 前一个DIF值
            prev_dea: 前一个DEA值
            
        Returns:
            信号状态字典
        """
        signal_status = {
            'dif_above_dea': dif > dea,
            'dif_above_zero': dif > 0,
            'dea_above_zero': dea > 0,
            'macd_above_zero': (dif - dea) > 0,
            'signal_type': 'hold'
        }
        
        # 金叉死叉判断 (需要前一个值)
        if prev_dif is not None and prev_dea is not None:
            # 金叉: DIF从下方穿越DEA
            if dif > dea and prev_dif <= prev_dea:
                if dif > 0:
                    signal_status['signal_type'] = 'golden_cross_above_zero'
                else:
                    signal_status['signal_type'] = 'golden_cross_below_zero'
                signal_status['is_golden_cross'] = True
            
            # 死叉: DIF从上方穿越DEA
            elif dif < dea and prev_dif >= prev_dea:
                if dif > 0:
                    signal_status['signal_type'] = 'death_cross_above_zero'
                else:
                    signal_status['signal_type'] = 'death_cross_below_zero'
                signal_status['is_death_cross'] = True
            
            # DIF穿越零轴
            elif dif > 0 and prev_dif <= 0:
                signal_status['signal_type'] = 'dif_cross_zero_up'
                signal_status['is_zero_cross_up'] = True
            elif dif < 0 and prev_dif >= 0:
                signal_status['signal_type'] = 'dif_cross_zero_down'
                signal_status['is_zero_cross_down'] = True
        
        return signal_status
    
    def validate_macd_data(self, macd_results: Dict[str, float]) -> bool:
        """
        验证MACD计算结果的有效性
        
        Args:
            macd_results: MACD计算结果
            
        Returns:
            是否有效
        """
        required_keys = ['EMA_Fast', 'EMA_Slow', 'DIF', 'DEA', 'MACD']
        
        # 检查必要字段
        if not all(key in macd_results for key in required_keys):
            return False
        
        # 检查数值有效性
        for key, value in macd_results.items():
            if pd.isna(value) or np.isinf(value):
                return False
        
        # 检查逻辑关系
        fast_ema = macd_results['EMA_Fast']
        slow_ema = macd_results['EMA_Slow']
        dif = macd_results['DIF']
        dea = macd_results['DEA']
        macd = macd_results['MACD']
        
        # DIF应该等于快线减慢线
        expected_dif = fast_ema - slow_ema
        if abs(dif - expected_dif) > 0.0001:
            return False
        
        # MACD应该等于(DIF - DEA) * 2
        expected_macd = (dif - dea) * 2
        if abs(macd - expected_macd) > 0.0001:
            return False
        
        return True
    
    def get_engine_info(self) -> Dict:
        """获取引擎信息"""
        return {
            'engine_name': 'MACDEngine',
            'version': '1.0.0',
            'fast_period': self.fast_period,
            'slow_period': self.slow_period,
            'signal_period': self.signal_period,
            'parameter_set': self.config.parameter_set
        } 