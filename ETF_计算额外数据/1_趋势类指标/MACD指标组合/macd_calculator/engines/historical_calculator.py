#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MACD历史数据计算器 - 重构版
===========================

处理MACD的历史数据计算逻辑
"""

import pandas as pd
from typing import Dict, List, Optional, Any
from ..infrastructure.config import MACDConfig
from .macd_engine import MACDEngine


class MACDHistoricalCalculator:
    """MACD历史数据计算器 - 重构版"""
    
    def __init__(self, config: MACDConfig, macd_engine: MACDEngine):
        """
        初始化历史数据计算器
        
        Args:
            config: MACD配置对象
            macd_engine: MACD计算引擎
        """
        self.config = config
        self.macd_engine = macd_engine
        
        print("📊 MACD历史数据计算器初始化完成")
        print(f"   🔧 支持参数: EMA{config.get_macd_periods()}")
    
    def calculate_historical_macd(self, df: pd.DataFrame, etf_code: str) -> pd.DataFrame:
        """
        计算历史MACD数据
        
        Args:
            df: 历史价格数据
            etf_code: ETF代码
            
        Returns:
            包含历史MACD数据的DataFrame
        """
        try:
            if df is None or df.empty:
                return pd.DataFrame()
            
            # 使用标准MACD引擎计算
            result_df = self.macd_engine.calculate_macd_for_etf(df)
            
            return result_df
            
        except Exception as e:
            print(f"❌ 历史MACD计算失败 {etf_code}: {str(e)}")
            return pd.DataFrame()
    
    def get_supported_periods(self) -> Dict[str, int]:
        """
        获取支持的周期参数
        
        Returns:
            周期参数字典
        """
        fast, slow, signal = self.config.get_macd_periods()
        return {
            'fast_period': fast,
            'slow_period': slow,
            'signal_period': signal
        }