#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
波动率数据读取器
=============

基于第一大类标准的波动率数据读取系统
- 支持从ETF_初筛结果读取筛选ETF列表
- 统一字段格式：输入中文，输出英文
- 统一日期格式：YYYYMMDD → YYYY-MM-DD
"""

import os
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple, Any
from .config import VolatilityConfig


class VolatilityDataReader:
    """波动率数据读取器"""
    
    def __init__(self, config: VolatilityConfig):
        """
        初始化数据读取器
        
        Args:
            config: 波动率配置对象
        """
        self.config = config
        
        if not config.performance_mode:
            print("📊 波动率数据读取器初始化完成")
            print(f"   📁 数据源: {config.data_path}")
            print(f"   📈 复权类型: {config.adj_type}")
    
    def get_screening_etf_codes(self, threshold: str) -> List[str]:
        """
        获取筛选结果的ETF代码列表 - 按第一大类标准
        
        Args:
            threshold: 门槛类型
            
        Returns:
            List[str]: ETF代码列表
        """
        try:
            # 按第一大类标准，从ETF_初筛目录读取筛选结果
            screening_file_path = os.path.join(
                self.config.base_data_path, 
                "..", "ETF_初筛", "data", threshold, "通过筛选ETF.txt"
            )
            
            # 标准化路径
            screening_file_path = os.path.normpath(screening_file_path)
            
            if not os.path.exists(screening_file_path):
                print(f"❌ 筛选文件不存在: {screening_file_path}")
                # 如果筛选文件不存在，返回所有可用ETF
                return self.get_available_etfs()
            
            # 读取筛选文件
            with open(screening_file_path, 'r', encoding='utf-8') as f:
                lines = f.readlines()
            
            etf_codes = []
            for line in lines:
                line = line.strip()
                if line and len(line) == 6 and line.isdigit():
                    # 标准化ETF代码
                    standardized_code = self._standardize_etf_code(line)
                    etf_codes.append(standardized_code)
            
            print(f"📊 {threshold}: 读取到 {len(etf_codes)} 个筛选通过的ETF")
            return etf_codes
            
        except Exception as e:
            print(f"❌ 读取筛选结果异常 {threshold}: {str(e)}")
            # 出错时返回所有可用ETF
            return self.get_available_etfs()
    
    def _standardize_etf_code(self, code: str) -> str:
        """
        标准化ETF代码（添加交易所后缀）- 按第一大类标准
        
        Args:
            code: 6位数字代码
            
        Returns:
            str: 标准化后的ETF代码
        """
        if len(code) == 6 and code.isdigit():
            # 基于代码前缀判断交易所
            if code.startswith(('50', '51', '52', '56', '58')):
                return f"{code}.SH"  # 上海
            elif code.startswith(('15', '16', '18')):
                return f"{code}.SZ"  # 深圳
            else:
                return f"{code}.SH"  # 默认上海
        return code
    
    def get_available_etfs(self) -> List[str]:
        """
        获取可用的ETF代码列表
        
        Returns:
            List[str]: 可用的ETF代码列表
        """
        if not os.path.exists(self.config.data_path):
            print(f"❌ 数据路径不存在: {self.config.data_path}")
            return []
        
        csv_files = [f for f in os.listdir(self.config.data_path) if f.endswith('.csv')]
        etf_codes = []
        
        for file in csv_files:
            etf_code = file.replace('.csv', '')
            standardized_code = self._standardize_etf_code(etf_code)
            etf_codes.append(standardized_code)
        
        return sorted(etf_codes)
    
    def read_etf_data(self, etf_code: str) -> Optional[Tuple[pd.DataFrame, Dict[str, Any]]]:
        """
        读取ETF数据 - 按第一大类标准处理字段和日期格式
        
        Args:
            etf_code: ETF代码
            
        Returns:
            Tuple[pd.DataFrame, Dict]: (数据框, 元数据) 或 None
        """
        file_path = self.config.get_file_path(etf_code)
        
        if not os.path.exists(file_path):
            if not self.config.performance_mode:
                print(f"❌ 文件不存在: {file_path}")
            return None
        
        try:
            # 读取CSV文件
            df = pd.read_csv(file_path, encoding='utf-8')
            
            # 数据清洗和验证
            df = self._clean_and_validate_data(df, etf_code)
            
            if df is None or df.empty:
                return None
            
            # 生成元数据
            metadata = self._generate_metadata(df, etf_code, file_path)
            
            return df, metadata
            
        except Exception as e:
            if not self.config.performance_mode:
                print(f"❌ 读取文件异常 {etf_code}: {str(e)}")
            return None
    
    def _clean_and_validate_data(self, df: pd.DataFrame, etf_code: str) -> Optional[pd.DataFrame]:
        """
        清洗和验证数据 - 按第一大类标准
        
        Args:
            df: 原始数据框
            etf_code: ETF代码
            
        Returns:
            pd.DataFrame: 清洗后的数据框或None
        """
        try:
            if df.empty:
                return None
            
            # 检查必需字段（输入使用中文字段名）
            required_columns = ['日期', '开盘价', '最高价', '最低价', '收盘价']
            
            # 处理成交量字段的变体
            volume_variants = ['成交量', '成交量(手数)', '成交量（手数）']
            volume_field = None
            for variant in volume_variants:
                if variant in df.columns:
                    volume_field = variant
                    break
            
            if volume_field:
                # 将成交量字段重命名为标准名称
                if volume_field != '成交量':
                    df = df.rename(columns={volume_field: '成交量'})
                required_columns.append('成交量')
            
            missing_columns = [col for col in required_columns if col not in df.columns]
            
            if missing_columns:
                if not self.config.performance_mode:
                    print(f"❌ {etf_code}: 缺少必需字段: {missing_columns}")
                return None
            
            # 数据类型转换
            df = df.copy()
            
            # 处理日期 - 按第一大类标准（YYYYMMDD数字格式 → datetime）
            if '日期' in df.columns:
                df['日期'] = pd.to_datetime(df['日期'], format='%Y%m%d', errors='coerce')
                df = df.dropna(subset=['日期'])
                df = df.sort_values('日期', ascending=True)  # 按时间正序排序（最早在前）
                df = df.reset_index(drop=True)
            
            # 处理价格字段
            price_columns = ['开盘价', '最高价', '最低价', '收盘价']
            for col in price_columns:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # 处理成交量
            if '成交量' in df.columns:
                df['成交量'] = pd.to_numeric(df['成交量'], errors='coerce')
            
            # 检查数据有效性
            df = df.dropna(subset=price_columns)
            
            if len(df) < self.config.min_data_points:
                if not self.config.performance_mode:
                    print(f"❌ {etf_code}: 有效数据不足 ({len(df)} < {self.config.min_data_points})")
                return None
            
            # 数据合理性检查
            for col in price_columns:
                if (df[col] <= 0).any():
                    if not self.config.performance_mode:
                        print(f"⚠️ {etf_code}: 检测到异常价格数据，正在清理...")
                    df = df[df[col] > 0]
            
            # 过滤后重新检查数据量
            if len(df) < self.config.min_data_points:
                if not self.config.performance_mode:
                    print(f"❌ {etf_code}: 清理异常数据后有效数据不足 ({len(df)} < {self.config.min_data_points})")
                return None
            
            if df.empty:
                return None
            
            return df
            
        except Exception as e:
            if not self.config.performance_mode:
                print(f"❌ {etf_code}: 数据清洗异常: {str(e)}")
            return None
    
    def _generate_metadata(self, df: pd.DataFrame, etf_code: str, file_path: str) -> Dict[str, Any]:
        """
        生成数据元数据
        
        Args:
            df: 数据框
            etf_code: ETF代码
            file_path: 文件路径
            
        Returns:
            Dict: 元数据
        """
        try:
            file_stats = os.stat(file_path)
            
            metadata = {
                'etf_code': etf_code,
                'file_path': file_path,
                'data_rows': len(df),
                'date_range': {
                    'start': df['日期'].min().strftime('%Y-%m-%d') if not df.empty else None,
                    'end': df['日期'].max().strftime('%Y-%m-%d') if not df.empty else None
                },
                'price_range': {
                    'min_close': float(df['收盘价'].min()) if not df.empty else None,
                    'max_close': float(df['收盘价'].max()) if not df.empty else None,
                    'latest_close': float(df['收盘价'].iloc[-1]) if not df.empty else None  # 最新在最后
                },
                'file_info': {
                    'size_bytes': file_stats.st_size,
                    'modified_time': file_stats.st_mtime
                },
                'config': {
                    'adj_type': self.config.adj_type,
                    'periods': self.config.volatility_periods,
                    'annualized': self.config.annualized
                }
            }
            
            return metadata
            
        except Exception as e:
            return {
                'etf_code': etf_code,
                'error': f'元数据生成失败: {str(e)}'
            }
    
    def get_etf_file_path(self, etf_code: str) -> Optional[str]:
        """
        获取ETF文件路径
        
        Args:
            etf_code: ETF代码
            
        Returns:
            str: 文件路径或None
        """
        file_path = self.config.get_file_path(etf_code)
        return file_path if os.path.exists(file_path) else None
    
    def batch_read_etf_data(self, etf_codes: List[str]) -> Dict[str, Tuple[pd.DataFrame, Dict]]:
        """
        批量读取ETF数据
        
        Args:
            etf_codes: ETF代码列表
            
        Returns:
            Dict: ETF代码到数据的映射
        """
        results = {}
        
        for etf_code in etf_codes:
            data_result = self.read_etf_data(etf_code)
            if data_result:
                results[etf_code] = data_result
        
        return results