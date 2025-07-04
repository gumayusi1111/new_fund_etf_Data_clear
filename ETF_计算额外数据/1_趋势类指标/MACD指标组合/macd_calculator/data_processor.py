#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MACD数据处理器
==============

负责ETF数据的读取、清洗、验证和筛选
🔍 功能: 数据源管理、数据质量检查、ETF筛选
📊 支持: 前复权/后复权/除权数据，批量处理

"""

import os
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple
from .config import MACDConfig


class MACDDataProcessor:
    """MACD数据处理器 - 专业数据管理版"""
    
    def __init__(self, config: MACDConfig):
        """
        初始化数据处理器
        
        Args:
            config: MACD配置对象
        """
        self.config = config
        self.data_source_path = config.get_data_source_path()
        
        print("📊 MACD数据处理器初始化完成")
        print(f"📁 数据源路径: {self.data_source_path}")
    
    def get_available_etf_files(self) -> List[str]:
        """
        获取可用的ETF文件列表
        
        Returns:
            ETF代码列表
        """
        if not os.path.exists(self.data_source_path):
            print(f"⚠️ 数据源路径不存在: {self.data_source_path}")
            return []
        
        etf_files = []
        for file in os.listdir(self.data_source_path):
            if file.endswith('.csv'):
                etf_code = file.replace('.csv', '').replace('.SZ', '').replace('.SH', '')
                etf_files.append(etf_code)
        
        print(f"📈 发现 {len(etf_files)} 个ETF数据文件")
        return sorted(etf_files)
    
    def load_etf_data(self, etf_code: str) -> Optional[pd.DataFrame]:
        """
        加载单个ETF的数据
        
        Args:
            etf_code: ETF代码
            
        Returns:
            ETF数据DataFrame，如果加载失败返回None
        """
        # 尝试不同的文件名格式
        possible_filenames = [
            f"{etf_code}.csv",
            f"{etf_code}.SZ.csv",
            f"{etf_code}.SH.csv"
        ]
        
        for filename in possible_filenames:
            file_path = os.path.join(self.data_source_path, filename)
            if os.path.exists(file_path):
                try:
                    df = pd.read_csv(file_path)
                    return self._clean_and_validate_data(df, etf_code)
                except Exception as e:
                    print(f"❌ 读取文件失败 {filename}: {e}")
                    continue
        
        print(f"❌ 未找到ETF数据文件: {etf_code}")
        return None
    
    def _clean_and_validate_data(self, df: pd.DataFrame, etf_code: str) -> Optional[pd.DataFrame]:
        """
        清洗和验证数据
        
        Args:
            df: 原始数据DataFrame
            etf_code: ETF代码
            
        Returns:
            清洗后的DataFrame
        """
        try:
            # 中文列名映射到英文
            column_mapping = {
                '日期': 'Date',
                '开盘价': 'Open', 
                '最高价': 'High',
                '最低价': 'Low',
                '收盘价': 'Close',
                '成交量(手数)': 'Volume',
                '成交额(千元)': 'Amount'
            }
            
            # 重命名列
            df = df.rename(columns=column_mapping)
            
            # 检查必要列
            required_columns = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume']
            missing_columns = [col for col in required_columns if col not in df.columns]
            
            if missing_columns:
                print(f"❌ {etf_code} 缺少必要列: {missing_columns}")
                print(f"   实际列名: {list(df.columns)}")
                return None
            
            # 数据类型转换
            # 处理日期格式：20250627 -> 2025-06-27
            df['Date'] = pd.to_datetime(df['Date'], format='%Y%m%d', errors='coerce')
            
            for col in ['Open', 'High', 'Low', 'Close']:
                df[col] = pd.to_numeric(df[col], errors='coerce')
            df['Volume'] = pd.to_numeric(df['Volume'], errors='coerce')
            
            # 移除无效数据行
            df = df.dropna(subset=['Date', 'Close'])
            
            # 按日期排序
            df = df.sort_values('Date').reset_index(drop=True)
            
            # 数据质量检查
            if len(df) < 100:  # 至少需要100个交易日
                print(f"⚠️ {etf_code} 数据量不足 ({len(df)} 行)")
                return None
            
            # 价格合理性检查
            if df['Close'].min() <= 0:
                print(f"⚠️ {etf_code} 存在非正价格")
                df = df[df['Close'] > 0].reset_index(drop=True)
            
            # 检查价格连续性
            price_changes = df['Close'].pct_change().abs()
            extreme_changes = price_changes > 0.5  # 50%以上的单日变动
            if extreme_changes.sum() > 5:
                print(f"⚠️ {etf_code} 存在 {extreme_changes.sum()} 个异常价格变动")
            
            print(f"✅ {etf_code} 数据清洗完成，有效数据 {len(df)} 行")
            return df
            
        except Exception as e:
            print(f"❌ {etf_code} 数据清洗失败: {e}")
            return None
    
    def filter_qualified_etfs(self, threshold_type: str = "3000万门槛") -> List[str]:
        """
        筛选符合条件的ETF
        
        Args:
            threshold_type: 门槛类型 ("3000万门槛" 或 "5000万门槛")
            
        Returns:
            符合条件的ETF代码列表
        """
        # 修复筛选文件路径计算 - 找到项目根目录
        project_root = self.config.base_path
        while not os.path.basename(project_root) == 'data_clear':
            parent = os.path.dirname(project_root)
            if parent == project_root:  # 已经到达根目录
                break
            project_root = parent
        
        # 读取筛选后的ETF列表
        filter_file_path = os.path.join(
            project_root,
            "ETF_初筛", "data", threshold_type, "通过筛选ETF.txt"
        )
        
        if not os.path.exists(filter_file_path):
            print(f"⚠️ 筛选文件不存在: {filter_file_path}")
            # 返回所有可用的ETF文件
            return self.get_available_etf_files()
        
        try:
            qualified_etfs = []
            with open(filter_file_path, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    # 跳过空行和注释行（以#开头）
                    if line and not line.startswith('#'):
                        qualified_etfs.append(line)
            
            print(f"📋 {threshold_type}: 发现 {len(qualified_etfs)} 个符合条件的ETF")
            return qualified_etfs
            
        except Exception as e:
            print(f"❌ 读取筛选文件失败: {e}")
            return self.get_available_etf_files()
    
    def batch_load_etf_data(self, etf_codes: List[str]) -> Dict[str, pd.DataFrame]:
        """
        批量加载ETF数据
        
        Args:
            etf_codes: ETF代码列表
            
        Returns:
            ETF代码到数据DataFrame的映射字典
        """
        etf_data = {}
        
        print(f"🔄 开始批量加载 {len(etf_codes)} 个ETF的数据...")
        
        success_count = 0
        for i, etf_code in enumerate(etf_codes, 1):
            print(f"📊 [{i}/{len(etf_codes)}] 处理 {etf_code}...", end=" ")
            
            df = self.load_etf_data(etf_code)
            if df is not None:
                etf_data[etf_code] = df
                success_count += 1
                print("✅")
            else:
                print("❌")
        
        print(f"🎯 批量加载完成: {success_count}/{len(etf_codes)} 个ETF加载成功")
        return etf_data
    
    def check_data_requirements_for_macd(self, df: pd.DataFrame) -> bool:
        """
        检查数据是否满足MACD计算要求
        
        Args:
            df: ETF数据DataFrame
            
        Returns:
            是否满足要求
        """
        fast_period, slow_period, signal_period = self.config.get_macd_periods()
        min_required = slow_period + signal_period + 10  # 额外的10个数据点确保稳定
        
        if len(df) < min_required:
            return False
        
        # 检查是否有足够的非空收盘价
        valid_closes = df['Close'].dropna()
        if len(valid_closes) < min_required:
            return False
        
        return True
    
    def get_latest_trading_date(self, etf_code: str) -> Optional[str]:
        """
        获取ETF的最新交易日期
        
        Args:
            etf_code: ETF代码
            
        Returns:
            最新交易日期字符串，格式为YYYY-MM-DD
        """
        df = self.load_etf_data(etf_code)
        if df is not None and len(df) > 0:
            latest_date = df['Date'].max()
            return latest_date.strftime('%Y-%m-%d')
        return None
    
    def get_data_summary(self, etf_codes: List[str]) -> Dict:
        """
        获取数据概要统计
        
        Args:
            etf_codes: ETF代码列表
            
        Returns:
            数据概要字典
        """
        summary = {
            'total_etfs': len(etf_codes),
            'valid_etfs': 0,
            'invalid_etfs': 0,
            'macd_ready_etfs': 0,
            'average_data_length': 0,
            'date_range': {'start': None, 'end': None},
            'failed_etfs': []
        }
        
        valid_lengths = []
        all_start_dates = []
        all_end_dates = []
        
        for etf_code in etf_codes:
            df = self.load_etf_data(etf_code)
            if df is not None:
                summary['valid_etfs'] += 1
                valid_lengths.append(len(df))
                
                all_start_dates.append(df['Date'].min())
                all_end_dates.append(df['Date'].max())
                
                if self.check_data_requirements_for_macd(df):
                    summary['macd_ready_etfs'] += 1
            else:
                summary['invalid_etfs'] += 1
                summary['failed_etfs'].append(etf_code)
        
        if valid_lengths:
            summary['average_data_length'] = int(np.mean(valid_lengths))
        
        if all_start_dates and all_end_dates:
            summary['date_range']['start'] = min(all_start_dates).strftime('%Y-%m-%d')
            summary['date_range']['end'] = max(all_end_dates).strftime('%Y-%m-%d')
        
        return summary
    
    def validate_etf_for_processing(self, etf_code: str) -> Tuple[bool, str]:
        """
        验证ETF是否可以进行MACD处理
        
        Args:
            etf_code: ETF代码
            
        Returns:
            (是否可以处理, 原因说明)
        """
        df = self.load_etf_data(etf_code)
        
        if df is None:
            return False, "数据加载失败"
        
        if not self.check_data_requirements_for_macd(df):
            fast_period, slow_period, signal_period = self.config.get_macd_periods()
            min_required = slow_period + signal_period + 10
            return False, f"数据量不足，需要至少{min_required}个交易日，当前{len(df)}个"
        
        # 检查最近数据的完整性
        recent_data = df.tail(50)  # 检查最近50个交易日
        missing_closes = recent_data['Close'].isna().sum()
        if missing_closes > 5:
            return False, f"最近数据质量差，{missing_closes}个缺失收盘价"
        
        return True, "数据验证通过" 