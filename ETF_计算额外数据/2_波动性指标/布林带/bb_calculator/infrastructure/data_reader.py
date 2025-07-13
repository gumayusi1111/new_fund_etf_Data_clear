#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
布林带数据读取器
==============

负责ETF数据的读取、预处理和验证
参照趋势类指标的数据读取模式
"""

import pandas as pd
import os
from typing import Optional, Dict, List
from .utils import BBUtils


class BBDataReader:
    """布林带数据读取器"""
    
    def __init__(self, config):
        """初始化数据读取器"""
        self.config = config
        self.utils = BBUtils()
    
    def read_etf_data(self, etf_code: str) -> Optional[pd.DataFrame]:
        """
        读取单个ETF的数据
        
        Args:
            etf_code: ETF代码
            
        Returns:
            Optional[pd.DataFrame]: ETF数据，如果读取失败返回None
        """
        file_path = self.config.get_etf_file_path(etf_code)
        
        if not os.path.exists(file_path):
            return None
        
        try:
            # 读取CSV文件
            df = pd.read_csv(file_path, encoding='utf-8')
            
            # 数据预处理
            processed_df = self._preprocess_data(df, etf_code)
            
            if processed_df is None or processed_df.empty:
                return None
            
            # 数据验证
            if not self._validate_data(processed_df):
                return None
            
            return processed_df
            
        except Exception as e:
            return None
    
    def _preprocess_data(self, df: pd.DataFrame, etf_code: str) -> Optional[pd.DataFrame]:
        """
        数据预处理
        
        Args:
            df: 原始数据
            etf_code: ETF代码
            
        Returns:
            Optional[pd.DataFrame]: 预处理后的数据
        """
        if df.empty:
            return None
        
        try:
            # 复制数据避免修改原始数据
            processed_df = df.copy()
            
            # 标准化列名（处理中英文混用问题）
            processed_df = self._standardize_columns(processed_df)
            
            # 检查必需列
            required_columns = ['日期', '收盘价']
            for col in required_columns:
                if col not in processed_df.columns:
                    return None
            
            # 处理日期列
            processed_df = self._process_date_column(processed_df)
            
            # 处理价格列
            processed_df = self._process_price_columns(processed_df)
            
            # 按日期排序
            processed_df = processed_df.sort_values('日期').reset_index(drop=True)
            
            # 去除无效数据
            processed_df = self._clean_invalid_data(processed_df)
            
            # 确保有足够的数据进行布林带计算
            min_required = self.config.get_min_periods()
            if len(processed_df) < min_required:
                return None
            
            return processed_df
            
        except Exception as e:
            return None
    
    def _standardize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """标准化列名"""
        # 列名映射表（处理中英文混用）
        column_mapping = {
            'date': '日期',
            'Date': '日期',
            '日期': '日期',
            'close': '收盘价',
            'Close': '收盘价',
            '收盘价': '收盘价',
            'closing_price': '收盘价',
            'high': '最高价',
            'High': '最高价', 
            '最高价': '最高价',
            'low': '最低价',
            'Low': '最低价',
            '最低价': '最低价',
            'open': '开盘价',
            'Open': '开盘价',
            '开盘价': '开盘价',
            'volume': '成交量',
            'Volume': '成交量',
            '成交量': '成交量'
        }
        
        # 重命名列
        df_renamed = df.rename(columns=column_mapping)
        
        return df_renamed
    
    def _process_date_column(self, df: pd.DataFrame) -> pd.DataFrame:
        """处理日期列"""
        try:
            # 转换日期格式
            df['日期'] = pd.to_datetime(df['日期'], errors='coerce')
            
            # 移除无效日期
            df = df.dropna(subset=['日期'])
            
            return df
            
        except Exception:
            return df
    
    def _process_price_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """处理价格列"""
        price_columns = ['收盘价', '开盘价', '最高价', '最低价']
        
        for col in price_columns:
            if col in df.columns:
                try:
                    # 转换为数值类型
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                    
                    # 移除负值和零值（价格不应该为负或零）
                    if col in ['收盘价', '开盘价', '最高价', '最低价']:
                        df = df[df[col] > 0]
                        
                except Exception:
                    continue
        
        return df
    
    def _clean_invalid_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """清理无效数据"""
        # 移除收盘价为空的行
        df = df.dropna(subset=['收盘价'])
        
        # 移除重复日期（保留最后一个）
        df = df.drop_duplicates(subset=['日期'], keep='last')
        
        return df
    
    def _validate_data(self, df: pd.DataFrame) -> bool:
        """验证数据质量"""
        validation_result = self.utils.validate_etf_data_structure(df)
        
        # 基本验证
        if not validation_result['is_valid']:
            return False
        
        # 检查数据量
        min_required = self.config.get_min_periods()
        if validation_result['row_count'] < min_required:
            return False
        
        # 检查价格范围
        price_range = validation_result.get('price_range')
        if not price_range:
            return False
        
        # 价格应该在合理范围内
        if price_range['min'] <= 0 or price_range['max'] > 10000:  # ETF价格通常不会超过1万
            return False
        
        return True