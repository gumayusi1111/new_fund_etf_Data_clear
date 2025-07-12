#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MACD CSV处理器 - 重构版
======================

处理MACD结果的CSV输出
"""

import pandas as pd
import os
from typing import Dict, List, Optional, Any


class MACDCSVHandler:
    """MACD CSV处理器 - 重构版"""
    
    def __init__(self, config=None):
        """
        初始化CSV处理器
        
        Args:
            config: MACD配置对象（可选）
        """
        self.config = config
    
    def save_to_csv(self, df: pd.DataFrame, file_path: str) -> bool:
        """
        保存DataFrame到CSV文件
        
        Args:
            df: MACD结果DataFrame
            file_path: 输出文件路径
            
        Returns:
            保存是否成功
        """
        try:
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            df.to_csv(file_path, index=False, encoding='utf-8')
            return True
        except Exception as e:
            print(f"❌ CSV保存失败: {str(e)}")
            return False
    
    def read_from_csv(self, file_path: str) -> Optional[pd.DataFrame]:
        """
        从CSV文件读取MACD数据
        
        Args:
            file_path: CSV文件路径
            
        Returns:
            MACD数据DataFrame
        """
        try:
            if not os.path.exists(file_path):
                return None
            
            df = pd.read_csv(file_path, encoding='utf-8')
            return df
        except Exception as e:
            print(f"❌ CSV读取失败: {str(e)}")
            return None
    
    def format_macd_output(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        格式化MACD输出数据
        
        Args:
            df: 原始MACD计算结果
            
        Returns:
            格式化后的DataFrame
        """
        try:
            # 选择输出列 - 使用标准化英文字段名
            output_columns = ['code', 'date', 'MACD', 'SIGNAL', 'HISTOGRAM']
            
            # 添加可选列
            optional_columns = ['EMA_FAST', 'EMA_SLOW', 'MACD_SIGNAL_DIFF', 
                              'MACD_MOMENTUM', 'SIGNAL_MOMENTUM']
            
            for col in optional_columns:
                if col in df.columns:
                    output_columns.append(col)
            
            # 过滤存在的列
            available_columns = [col for col in output_columns if col in df.columns]
            result_df = df[available_columns].copy()
            
            # 格式化数值精度 - 使用标准化字段名
            numeric_columns = ['MACD', 'SIGNAL', 'HISTOGRAM', 'EMA_FAST', 'EMA_SLOW', 
                             'MACD_SIGNAL_DIFF', 'MACD_MOMENTUM', 'SIGNAL_MOMENTUM']
            
            for col in numeric_columns:
                if col in result_df.columns:
                    result_df[col] = result_df[col].round(8)
            
            return result_df
            
        except Exception as e:
            print(f"❌ 格式化失败: {str(e)}")
            return df