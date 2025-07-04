#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ETF数据加载器
负责从日更数据中只读加载ETF数据，严格不修改源数据
"""

import pandas as pd
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import os
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import multiprocessing as mp

from .utils.config import get_config
from .utils.logger import get_logger


class ETFDataLoader:
    """
    ETF数据加载器
    严格遵循只读原则，不修改日更数据
    """
    
    def __init__(self):
        self.config = get_config()
        self.logger = get_logger()
        self.daily_source = self.config.get_daily_data_source()
        
        # 验证数据源
        if not self.daily_source.exists():
            raise FileNotFoundError(f"日更数据源不存在: {self.daily_source}")
    
    def get_available_etf_codes(self, fuquan_type: str = "0_ETF日K(前复权)") -> List[str]:
        """
        获取可用的ETF代码列表
        
        Args:
            fuquan_type: 复权类型目录名
        
        Returns:
            ETF代码列表
        """
        try:
            fuquan_dir = self.daily_source / fuquan_type
            if not fuquan_dir.exists():
                self.logger.warning(f"复权目录不存在: {fuquan_dir}")
                return []
            
            # 获取所有CSV文件
            csv_files = list(fuquan_dir.glob("*.csv"))
            
            # 提取ETF代码（文件名去掉.csv后缀）
            etf_codes = []
            for csv_file in csv_files:
                code = csv_file.stem  # 文件名不含扩展名
                if self._is_valid_etf_code(code):
                    etf_codes.append(code)
            
            etf_codes.sort()
            self.logger.info(f"📊 从 {fuquan_type} 发现 {len(etf_codes)} 个ETF")
            return etf_codes
            
        except Exception as e:
            self.logger.error(f"获取ETF代码失败: {e}")
            return []
    
    def _is_valid_etf_code(self, code: str) -> bool:
        """
        验证ETF代码有效性
        
        Args:
            code: ETF代码（可能包含.SZ或.SH后缀）
        
        Returns:
            是否有效
        """
        # 移除可能的交易所后缀
        clean_code = code
        if '.' in code:
            clean_code = code.split('.')[0]
        
        # 基本格式检查（6位数字）
        if not clean_code.isdigit() or len(clean_code) != 6:
            return False
        
        # ETF代码通常以1、5开头
        if not clean_code.startswith(('1', '5')):
            return False
            
        return True
    
    def load_etf_data(self, etf_code: str, fuquan_type: str, 
                     days_back: int = None) -> Optional[pd.DataFrame]:
        """
        加载单个ETF的数据
        
        Args:
            etf_code: ETF代码
            fuquan_type: 复权类型
            days_back: 加载最近N天的数据，None表示加载全部
        
        Returns:
            ETF数据DataFrame，失败返回None
        """
        try:
            fuquan_dir = self.daily_source / fuquan_type
            csv_file = fuquan_dir / f"{etf_code}.csv"
            
            if not csv_file.exists():
                self.logger.warning(f"ETF数据文件不存在: {csv_file}")
                return None
            
            # 只读方式加载数据
            df = pd.read_csv(csv_file, encoding='utf-8')
            
            # 验证数据格式
            if not self._validate_dataframe(df):
                self.logger.warning(f"ETF数据格式无效: {etf_code}")
                return None
            
            # 数据预处理
            df = self._preprocess_dataframe(df)
            
            # 按日期限制数据
            if days_back is not None:
                df = self._limit_recent_days(df, days_back)
            
            self.logger.debug(f"✅ 加载ETF数据: {etf_code}, 共 {len(df)} 行")
            return df
            
        except Exception as e:
            self.logger.error(f"加载ETF数据失败 {etf_code}: {e}")
            return None
    
    def _validate_dataframe(self, df: pd.DataFrame) -> bool:
        """
        验证DataFrame格式
        
        Args:
            df: 数据DataFrame
        
        Returns:
            是否有效
        """
        # 期望的11个字段
        expected_columns = [
            '代码', '日期', '开盘价', '最高价', '最低价', '收盘价',
            '上日收盘', '涨跌', '涨幅%', '成交量(手数)', '成交额(千元)'
        ]
        
        # 检查必要字段
        missing_columns = [col for col in expected_columns if col not in df.columns]
        if missing_columns:
            self.logger.warning(f"缺少必要字段: {missing_columns}")
            return False
        
        # 检查数据非空
        if df.empty:
            self.logger.warning("数据为空")
            return False
        
        return True
    
    def _preprocess_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        预处理DataFrame（不修改原始数据）
        
        Args:
            df: 原始数据
        
        Returns:
            处理后的数据副本
        """
        # 创建副本避免修改原始数据
        df_processed = df.copy()
        
        # 日期处理
        if '日期' in df_processed.columns:
            df_processed['日期'] = pd.to_datetime(df_processed['日期'], errors='coerce')
            # 按日期排序
            df_processed = df_processed.sort_values('日期').reset_index(drop=True)
        
        # 数值类型转换
        numeric_columns = [
            '开盘价', '最高价', '最低价', '收盘价', '上日收盘', '涨跌', 
            '涨幅%', '成交量(手数)', '成交额(千元)'
        ]
        
        for col in numeric_columns:
            if col in df_processed.columns:
                df_processed[col] = pd.to_numeric(df_processed[col], errors='coerce')
        
        # 删除无效行
        df_processed = df_processed.dropna(subset=['日期', '收盘价'])
        
        return df_processed
    
    def _limit_recent_days(self, df: pd.DataFrame, days_back: int) -> pd.DataFrame:
        """
        限制为最近N天的数据
        
        Args:
            df: 数据DataFrame
            days_back: 天数
        
        Returns:
            限制后的数据
        """
        if df.empty or '日期' not in df.columns:
            return df
        
        # 获取最新日期
        latest_date = df['日期'].max()
        start_date = latest_date - timedelta(days=days_back)
        
        # 过滤数据
        recent_df = df[df['日期'] >= start_date].copy()
        
        return recent_df
    
    def load_multiple_etfs(self, etf_codes: List[str], fuquan_type: str,
                          days_back: int = None, max_workers: int = None) -> Dict[str, pd.DataFrame]:
        """
        批量加载多个ETF的数据（并行优化版）
        
        Args:
            etf_codes: ETF代码列表
            fuquan_type: 复权类型
            days_back: 加载最近N天的数据
            max_workers: 最大并行工作数，None表示自动设置
        
        Returns:
            ETF代码到DataFrame的字典
        """
        if not etf_codes:
            return {}
        
        # 自动设置并行数
        if max_workers is None:
            max_workers = min(32, (os.cpu_count() or 1) * 4)  # 限制最大32个线程
        
        etf_data = {}
        
        # 使用ThreadPoolExecutor并行加载
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # 提交所有任务
            future_to_code = {
                executor.submit(self.load_etf_data, etf_code, fuquan_type, days_back): etf_code
                for etf_code in etf_codes
            }
            
            # 收集结果
            for future in as_completed(future_to_code):
                etf_code = future_to_code[future]
                try:
                    df = future.result()
                    if df is not None and not df.empty:
                        etf_data[etf_code] = df
                except Exception as e:
                    self.logger.error(f"并行加载ETF数据失败 {etf_code}: {e}")
        
        self.logger.info(f"📊 成功加载 {len(etf_data)}/{len(etf_codes)} 个ETF数据")
        return etf_data
    
    def load_multiple_etfs_batch(self, etf_codes: List[str], fuquan_type: str,
                                days_back: int = None, batch_size: int = 100) -> Dict[str, pd.DataFrame]:
        """
        批量加载多个ETF的数据（分批处理版）
        
        Args:
            etf_codes: ETF代码列表
            fuquan_type: 复权类型
            days_back: 加载最近N天的数据
            batch_size: 批处理大小
        
        Returns:
            ETF代码到DataFrame的字典
        """
        if not etf_codes:
            return {}
        
        etf_data = {}
        total_codes = len(etf_codes)
        
        # 分批处理
        for i in range(0, total_codes, batch_size):
            batch_codes = etf_codes[i:i + batch_size]
            batch_data = self.load_multiple_etfs(batch_codes, fuquan_type, days_back)
            etf_data.update(batch_data)
            
            self.logger.info(f"📊 批处理进度: {min(i + batch_size, total_codes)}/{total_codes}")
        
        return etf_data
    
    def get_data_summary(self, fuquan_type: str = None) -> Dict[str, any]:
        """
        获取数据源摘要信息
        
        Args:
            fuquan_type: 复权类型，None表示所有类型
        
        Returns:
            摘要信息字典
        """
        summary = {
            "数据源路径": str(self.daily_source),
            "复权类型统计": {}
        }
        
        fuquan_type_list = [fuquan_type] if fuquan_type else self.config.get_fuquan_types()
        
        for fuquan in fuquan_type_list:
            fuquan_dir = self.daily_source / fuquan
            if fuquan_dir.exists():
                csv_files = list(fuquan_dir.glob("*.csv"))
                etf_count = len([f for f in csv_files if self._is_valid_etf_code(f.stem)])
                
                summary["复权类型统计"][fuquan] = {
                    "ETF数量": etf_count,
                    "目录存在": True
                }
            else:
                summary["复权类型统计"][fuquan] = {
                    "ETF数量": 0,
                    "目录存在": False
                }
        
        return summary
    
    def validate_data_source(self) -> bool:
        """
        验证数据源完整性
        
        Returns:
            数据源是否有效
        """
        try:
            summary = self.get_data_summary()
            
            self.logger.info("🔍 数据源验证结果:")
            self.logger.info(f"  数据源路径: {summary['数据源路径']}")
            
            total_etfs = 0
            for fuquan_type, stats in summary["复权类型统计"].items():
                etf_count = stats["ETF数量"]
                exists = stats["目录存在"]
                status = "✅" if exists and etf_count > 0 else "❌"
                self.logger.info(f"  {status} {fuquan_type}: {etf_count} 个ETF")
                
                if exists and etf_count > 0:
                    total_etfs = max(total_etfs, etf_count)
            
            if total_etfs == 0:
                self.logger.error("❌ 未发现任何有效的ETF数据")
                return False
            
            self.logger.info(f"✅ 数据源验证通过，发现 {total_etfs} 个ETF")
            return True
            
        except Exception as e:
            self.logger.error(f"数据源验证失败: {e}")
            return False 