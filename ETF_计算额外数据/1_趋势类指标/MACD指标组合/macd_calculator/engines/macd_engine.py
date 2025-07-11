#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MACD计算引擎 - 重构版
====================

实现MACD指标的核心计算逻辑:
- DIF线 = EMA(Close, 12) - EMA(Close, 26)
- DEA线 = EMA(DIF, 9) 
- MACD柱 = (DIF - DEA) × 2
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple, Union
from ..infrastructure.config import MACDConfig


class MACDEngine:
    """MACD计算引擎 - 重构版"""
    
    def __init__(self, config: MACDConfig):
        """
        初始化MACD计算引擎
        
        Args:
            config: MACD配置对象
        """
        self.config = config
        self.fast_period, self.slow_period, self.signal_period = config.get_macd_periods()
        
        print("⚙️ MACD计算引擎初始化完成")
        print(f"   📊 MACD参数: EMA({self.fast_period}, {self.slow_period}, {self.signal_period})")
        print(f"   📈 快线周期: {self.fast_period}")
        print(f"   📉 慢线周期: {self.slow_period}")
        print(f"   📊 信号线周期: {self.signal_period}")
    
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
        
        # 计算DEA线 (DIF的信号线EMA)
        dea = self.calculate_ema(dif, self.signal_period)
        
        # 计算MACD柱 (DIF - DEA) × 2
        macd_histogram = (dif - dea) * 2
        
        return {
            'EMA_FAST': ema_fast,
            'EMA_SLOW': ema_slow,
            'DIF': dif,
            'DEA': dea,
            'MACD': macd_histogram
        }
    
    def calculate_macd_for_etf(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        为单个ETF计算MACD指标
        
        Args:
            df: ETF数据DataFrame，必须包含'收盘价'列
            
        Returns:
            包含MACD指标的DataFrame
        """
        try:
            if df is None or df.empty:
                raise ValueError("输入数据为空")
            
            if '收盘价' not in df.columns:
                raise ValueError("数据中缺少'收盘价'列")
            
            # 确保数据按日期正序排序（与原系统一致）
            if '日期' in df.columns:
                df = df.sort_values('日期').reset_index(drop=True)
            
            # 将中文列名映射为英文（与原系统保持一致）
            df_processed = df.copy()
            df_processed['Date'] = pd.to_datetime(df['日期'])
            df_processed['Close'] = df['收盘价']
            
            # 使用原系统的计算方式
            macd_components = self.calculate_macd_components(df_processed['Close'])
            
            # 创建中间结果DataFrame（与原系统流程一致）
            temp_df = df_processed.copy()
            temp_df['EMA_Fast'] = macd_components['EMA_FAST']
            temp_df['EMA_Slow'] = macd_components['EMA_SLOW'] 
            temp_df['DIF'] = macd_components['DIF']
            temp_df['DEA'] = macd_components['DEA']
            temp_df['MACD'] = macd_components['MACD']
            
            # 格式化结果（遵循原系统的格式化逻辑）
            result_df = pd.DataFrame({
                'date': temp_df['Date'].dt.strftime('%Y-%m-%d'),  # 日期格式化
                'code': df['代码'],  # ETF代码
                'ema_fast': temp_df['EMA_Fast'].round(6),  # 快线EMA
                'ema_slow': temp_df['EMA_Slow'].round(6),  # 慢线EMA
                'dif': temp_df['DIF'].round(6),  # ema_fast - ema_slow
                'dea': temp_df['DEA'].round(6),  # DIF的信号线EMA
                'macd_bar': temp_df['MACD'].round(6),  # (dif - dea) * 2
            })
            
            # 添加计算时间戳 - 与原系统保持一致
            from datetime import datetime
            calc_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            result_df['calc_time'] = calc_time
            
            # 按日期倒序排列（最新日期在前）- 与原系统一致
            result_df = result_df.sort_values('date', ascending=False).reset_index(drop=True)
            
            # 移除无效数据行
            result_df = result_df.dropna().reset_index(drop=True)
            
            return result_df
            
        except Exception as e:
            print(f"❌ MACD计算失败: {str(e)}")
            return pd.DataFrame()
    
    def validate_calculation_requirements(self, df: pd.DataFrame) -> bool:
        """
        验证MACD计算要求
        
        Args:
            df: ETF数据DataFrame
            
        Returns:
            验证结果
        """
        try:
            if df is None or df.empty:
                return False
            
            # MACD需要足够的数据点
            min_required = max(self.slow_period, self.signal_period) + 10
            if len(df) < min_required:
                print(f"⚠️ 数据点不足: 需要至少{min_required}个数据点，当前{len(df)}个")
                return False
            
            # 检查必要列
            if '收盘价' not in df.columns:
                print("❌ 缺少'收盘价'列")
                return False
            
            # 检查价格数据有效性
            if df['收盘价'].isna().any() or (df['收盘价'] <= 0).any():
                print("❌ 包含无效价格数据")
                return False
            
            return True
            
        except Exception as e:
            print(f"❌ 验证失败: {str(e)}")
            return False
    
    def get_calculation_info(self) -> Dict:
        """
        获取计算引擎信息
        
        Returns:
            引擎信息字典
        """
        return {
            'engine_name': 'MACD Engine',
            'version': '2.0.0',
            'parameters': {
                'fast_period': self.fast_period,
                'slow_period': self.slow_period,
                'signal_period': self.signal_period
            },
            'output_indicators': ['DIF', 'DEA', 'MACD', 'EMA_FAST', 'EMA_SLOW'],
            'auxiliary_indicators': ['DIF_DEA_DIFF', 'DIF_MOMENTUM', 'DEA_MOMENTUM'],
            'min_data_points': max(self.slow_period, self.signal_period) + 10
        }