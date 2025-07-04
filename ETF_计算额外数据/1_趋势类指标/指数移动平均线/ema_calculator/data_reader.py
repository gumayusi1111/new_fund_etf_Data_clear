#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ETF数据读取器模块 - EMA专版
=========================

专门负责为EMA计算读取和预处理ETF数据
支持智能路径检测、数据验证和格式标准化
"""

import pandas as pd
import os
from typing import Optional, Tuple, List, Dict
from .config import EMAConfig


class ETFDataReader:
    """ETF数据读取器 - EMA专版"""
    
    def __init__(self, config: EMAConfig):
        """
        初始化数据读取器
        
        Args:
            config: EMA配置对象
        """
        self.config = config
        print("📖 ETF数据读取器初始化完成 (EMA专版)")
        print(f"   📂 数据目录: {self.config.data_dir}")
    
    def get_available_etfs(self) -> List[str]:
        """
        获取可用的ETF代码列表
        
        Returns:
            List[str]: 可用的ETF代码列表
        """
        try:
            if not os.path.exists(self.config.data_dir):
                print(f"❌ 数据目录不存在: {self.config.data_dir}")
                return []
            
            csv_files = [f for f in os.listdir(self.config.data_dir) if f.endswith('.csv')]
            etf_codes = [f.replace('.csv', '') for f in csv_files]
            
            print(f"📊 发现 {len(etf_codes)} 个ETF数据文件")
            return sorted(etf_codes)
            
        except Exception as e:
            print(f"❌ 获取ETF列表失败: {str(e)}")
            return []
    
    def read_etf_data(self, etf_code: str) -> Optional[Tuple[pd.DataFrame, int]]:
        """
        读取单个ETF的数据 - EMA优化版
        
        Args:
            etf_code: ETF代码
            
        Returns:
            Tuple[pd.DataFrame, int]: (数据DataFrame, 总行数) 或 None
        """
        try:
            file_path = self.config.get_etf_file_path(etf_code)
            
            if not os.path.exists(file_path):
                print(f"❌ {etf_code} 文件不存在: {file_path}")
                return None
            
            # ⚡ 性能优化：指定数据类型和必要列，减少读取时间（不影响原始文件）
            dtype_dict = {
                '收盘价': 'float32',  # 使用float32减少内存占用
                '开盘价': 'float32',
                '最高价': 'float32', 
                '最低价': 'float32'
            }
            # 只读取EMA计算需要的列，提升读取速度
            usecols = ['日期', '收盘价']
            
            df = pd.read_csv(file_path, encoding='utf-8', dtype=dtype_dict, usecols=usecols)
            total_rows = len(df)
            
            # 数据验证
            if df.empty:
                print(f"❌ {etf_code} 数据为空")
                return None
            
            # 检查必要列
            required_columns = ['日期', '收盘价']
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                print(f"❌ {etf_code} 缺少必要列: {missing_columns}")
                return None
            
            # 数据预处理
            df = self._preprocess_data(df, etf_code)
            if df is None:
                return None
            
            # 数据策略：用户要求使用完整历史数据
            # 原数据是什么就是什么，不要人为限制行数
            if self.config.required_rows is not None and len(df) > self.config.required_rows:
                df = df.tail(self.config.required_rows)
            
            actual_rows = len(df)
            print(f"📊 {etf_code}: {actual_rows}/{total_rows} 行数据 ({df['日期'].iloc[0]} 到 {df['日期'].iloc[-1]})")
            
            return df, total_rows
            
        except Exception as e:
            print(f"❌ {etf_code} 数据读取失败: {str(e)}")
            return None
    
    def _preprocess_data(self, df: pd.DataFrame, etf_code: str) -> Optional[pd.DataFrame]:
        """
        数据预处理
        
        Args:
            df: 原始数据
            etf_code: ETF代码
            
        Returns:
            pd.DataFrame: 预处理后的数据或None
        """
        try:
            # 复制数据，避免修改原始数据
            df = df.copy()
            
            # 1. 日期处理 - 修复：正确处理YYYYMMDD格式
            # 原始数据格式：20250627 -> 转换为 2025-06-27
            df['日期'] = pd.to_datetime(df['日期'], format='%Y%m%d')
            
            # 2. 按日期排序（升序）
            df = df.sort_values('日期', ascending=True).reset_index(drop=True)
            
            # 3. 收盘价处理 - 已经在读取时指定为float32，跳过转换
            
            # 4. 数据质量检查
            # 检查收盘价是否有缺失值
            invalid_prices = df['收盘价'].isna().sum()
            if invalid_prices > 0:
                print(f"⚠️  {etf_code}: {invalid_prices} 个无效收盘价，将自动清理")
                df = df.dropna(subset=['收盘价'])
            
            # 检查收盘价是否为正数
            non_positive_prices = (df['收盘价'] <= 0).sum()
            if non_positive_prices > 0:
                print(f"⚠️  {etf_code}: {non_positive_prices} 个非正数收盘价，将自动清理")
                df = df[df['收盘价'] > 0]
            
            # 5. 最终验证 - EMA需要更多数据
            if len(df) < self.config.max_period:
                print(f"❌ {etf_code}: 有效数据({len(df)}行)不足以计算EMA{self.config.max_period}")
                return None
            
            print(f"✅ {etf_code}: 数据预处理完成，{len(df)}行有效数据")
            return df
            
        except Exception as e:
            print(f"❌ {etf_code} 数据预处理失败: {str(e)}")
            return None
    
    def get_latest_price_info(self, df: pd.DataFrame) -> Dict:
        """
        获取最新价格信息
        
        Args:
            df: ETF数据
            
        Returns:
            Dict: 最新价格信息
        """
        try:
            if df.empty:
                return {}
            
            latest_row = df.iloc[-1]
            
            # 计算涨跌幅
            change_pct = 0.0
            if len(df) >= 2:
                prev_close = df.iloc[-2]['收盘价']
                current_close = latest_row['收盘价']
                if prev_close > 0:
                    change_pct = ((current_close - prev_close) / prev_close) * 100
            
            return {
                'date': latest_row['日期'].strftime('%Y-%m-%d'),
                'close': round(float(latest_row['收盘价']), 3),
                'change_pct': round(change_pct, 3)
            }
            
        except Exception as e:
            print(f"⚠️  获取最新价格信息失败: {str(e)}")
            return {}
    
    def get_date_range(self, df: pd.DataFrame) -> Dict:
        """
        获取数据日期范围
        
        Args:
            df: ETF数据
            
        Returns:
            Dict: 日期范围信息
        """
        try:
            if df.empty:
                return {}
            
            start_date = df['日期'].iloc[0]
            end_date = df['日期'].iloc[-1]
            total_days = (end_date - start_date).days
            
            return {
                'start_date': start_date.strftime('%Y-%m-%d'),
                'end_date': end_date.strftime('%Y-%m-%d'),
                'total_days': total_days,
                'data_points': len(df)
            }
            
        except Exception as e:
            print(f"⚠️  获取日期范围失败: {str(e)}")
            return {}
    
    def validate_etf_code(self, etf_code: str) -> bool:
        """
        验证ETF代码是否有效
        
        Args:
            etf_code: ETF代码
            
        Returns:
            bool: 是否有效
        """
        try:
            file_path = self.config.get_etf_file_path(etf_code)
            exists = os.path.exists(file_path)
            
            if not exists:
                print(f"❌ ETF代码无效: {etf_code} (文件不存在)")
            
            return exists
            
        except Exception as e:
            print(f"❌ 验证ETF代码失败: {str(e)}")
            return False
    
    def get_screening_etf_codes(self, threshold: str) -> List[str]:
        """
        从ETF筛选结果获取ETF代码列表
        
        Args:
            threshold: 门槛类型 ("3000万门槛" 或 "5000万门槛")
            
        Returns:
            List[str]: ETF代码列表
        """
        try:
            # 智能路径计算
            current_dir = os.getcwd()
            
            # 检测项目根目录
            if "ETF_计算额外数据" in current_dir:
                project_root = current_dir.split("ETF_计算额外数据")[0]
            else:
                project_root = current_dir
            
            # 构建筛选结果文件路径
            screening_file = os.path.join(
                project_root, 
                "ETF_初筛", 
                "data", 
                threshold, 
                "通过筛选ETF.txt"
            )
            
            print(f"🔍 查找筛选结果: {screening_file}")
            
            if not os.path.exists(screening_file):
                print(f"❌ 筛选结果文件不存在: {screening_file}")
                return []
            
            # 读取ETF代码
            with open(screening_file, 'r', encoding='utf-8') as f:
                etf_codes = [line.strip() for line in f.readlines() if line.strip()]
            
            # 标准化ETF代码（添加交易所后缀）
            standardized_codes = []
            for code in etf_codes:
                if not code.endswith(('.SH', '.SZ')):
                    # 根据代码规律添加后缀
                    if code.startswith('5'):
                        code += '.SH'  # 上交所
                    elif code.startswith('1'):
                        code += '.SZ'  # 深交所
                    else:
                        print(f"⚠️  无法识别交易所: {code}")
                        continue
                standardized_codes.append(code)
            
            print(f"✅ 成功读取{threshold}筛选结果: {len(standardized_codes)}个ETF")
            return standardized_codes
            
        except Exception as e:
            print(f"❌ 读取筛选结果失败: {str(e)}")
            return [] 