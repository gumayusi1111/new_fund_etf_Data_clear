#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
EMA数据读取器 - 重构版
==================

参照WMA/SMA系统的数据读取架构
统一数据读取接口，支持ETF数据和筛选结果读取
"""

import os
import pandas as pd
from typing import List, Optional, Tuple, Dict
from .config import EMAConfig


class EMADataReader:
    """EMA数据读取器 - 重构版（与WMA/SMA保持一致）"""
    
    def __init__(self, config: EMAConfig):
        """
        初始化数据读取器
        
        Args:
            config: EMA配置对象
        """
        self.config = config
        
        if not config.performance_mode:
            print("📖 EMA数据读取器初始化完成")
            print(f"   📁 数据路径: {config.data_dir}")
            print(f"   📊 复权类型: {config.adj_type}")
    
    def get_available_etfs(self) -> List[str]:
        """
        获取可用的ETF代码列表
        
        Returns:
            List[str]: 可用的ETF代码列表（包含.SH/.SZ后缀）
        """
        try:
            if not os.path.exists(self.config.data_dir):
                return []
            
            csv_files = [f for f in os.listdir(self.config.data_dir) if f.endswith('.csv')]
            
            # 标准化ETF代码格式
            etf_codes = []
            for csv_file in csv_files:
                etf_code = csv_file.replace('.csv', '')
                
                # 使用统一的ETF代码标准化方法
                etf_code = self.config.normalize_etf_code(etf_code)
                etf_codes.append(etf_code)
            
            # 排序并返回
            return sorted(etf_codes)
            
        except Exception as e:
            print(f"❌ 获取可用ETF失败: {str(e)}")
            return []
    
    def validate_etf_code(self, etf_code: str) -> bool:
        """
        验证ETF代码是否有效
        
        Args:
            etf_code: ETF代码
            
        Returns:
            bool: 是否有效
        """
        try:
            file_path = self.get_etf_file_path(etf_code)
            return os.path.exists(file_path)
        except Exception:
            return False
    
    def get_etf_file_path(self, etf_code: str) -> str:
        """
        获取ETF数据文件路径
        
        Args:
            etf_code: ETF代码
            
        Returns:
            str: 文件路径
        """
        # 使用统一的清理方法
        clean_code = self.config.clean_etf_code(etf_code)
        filename = f"{clean_code}.csv"
        return os.path.join(self.config.data_dir, filename)
    
    def read_etf_data(self, etf_code: str) -> Optional[Tuple[pd.DataFrame, int]]:
        """
        读取ETF数据
        
        Args:
            etf_code: ETF代码
            
        Returns:
            Optional[Tuple[pd.DataFrame, int]]: (数据DataFrame, 总行数) 或 None
        """
        try:
            file_path = self.get_etf_file_path(etf_code)
            
            if not os.path.exists(file_path):
                if not self.config.performance_mode:
                    print(f"❌ 文件不存在: {file_path}")
                return None
            
            # 读取CSV数据
            df = pd.read_csv(file_path, encoding='utf-8')
            
            if df.empty:
                if not self.config.performance_mode:
                    print(f"❌ {etf_code}: 数据文件为空")
                return None
            
            # 验证必要列 - 支持多种列名格式
            required_columns = ['日期', '开盘价', '最高价', '最低价', '收盘价']
            volume_columns = ['成交量', '成交量(手数)', '成交量（手数）']
            
            # 检查基础列
            missing_columns = [col for col in required_columns if col not in df.columns]
            
            # 检查成交量列（支持多种格式）
            volume_col_found = False
            volume_col_name = None
            for vol_col in volume_columns:
                if vol_col in df.columns:
                    volume_col_found = True
                    volume_col_name = vol_col
                    break
            
            if not volume_col_found:
                missing_columns.append('成交量')
            
            if missing_columns:
                if not self.config.performance_mode:
                    print(f"❌ {etf_code}: 缺少必要列 {missing_columns}")
                return None
            
            # 标准化成交量列名
            if volume_col_name and volume_col_name != '成交量':
                df = df.rename(columns={volume_col_name: '成交量'})
            
            # 数据预处理 - 确保按时间升序排列（用于EMA计算）
            if '日期' in df.columns:
                # 转换日期格式
                df['日期'] = pd.to_datetime(df['日期'], format='%Y%m%d', errors='coerce')
                
                # 移除无效日期行
                df = df.dropna(subset=['日期'])
                
                if df.empty:
                    if not self.config.performance_mode:
                        print(f"❌ {etf_code}: 日期转换后数据为空")
                    return None
                
                # 按时间升序排序（EMA计算需要）
                df = df.sort_values('日期', ascending=True).reset_index(drop=True)
            
            # 数据类型转换
            numeric_columns = ['开盘价', '最高价', '最低价', '收盘价', '成交量']
            for col in numeric_columns:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # 移除价格为空或零的行
            price_columns = ['开盘价', '最高价', '最低价', '收盘价']
            for col in price_columns:
                if col in df.columns:
                    df = df[df[col] > 0]
            
            if df.empty:
                if not self.config.performance_mode:
                    print(f"❌ {etf_code}: 价格清理后数据为空")
                return None
            
            total_rows = len(df)
            
            # 检查数据量是否足够
            if total_rows < self.config.max_period:
                if not self.config.performance_mode:
                    print(f"⚠️ {etf_code}: 数据量不足 ({total_rows}行 < {self.config.max_period}行)")
                # 不返回None，让调用者决定是否继续
            
            if not self.config.performance_mode:
                print(f"📊 {etf_code}: 数据读取成功 ({total_rows}行)")
            
            return df, total_rows
            
        except Exception as e:
            if not self.config.performance_mode:
                print(f"❌ {etf_code} 数据读取失败: {str(e)}")
            return None
    
    def get_latest_price_info(self, df: pd.DataFrame) -> Dict:
        """
        获取最新价格信息
        
        Args:
            df: ETF数据DataFrame
            
        Returns:
            Dict: 最新价格信息
        """
        try:
            if df.empty:
                return {}
            
            # 确保数据按日期排序，获取真正的最新数据
            if '日期' in df.columns:
                # 按日期降序排列，确保最新数据在第一行
                df_sorted = df.sort_values('日期', ascending=False)
                latest_row = df_sorted.iloc[0]
                prev_row = df_sorted.iloc[1] if len(df_sorted) >= 2 else latest_row
            else:
                # 如果没有日期列，假设数据已经按时间排序（最新在最后）
                latest_row = df.iloc[-1]
                prev_row = df.iloc[-2] if len(df) >= 2 else latest_row
            
            latest_price = float(latest_row['收盘价'])
            prev_price = float(prev_row['收盘价'])
            
            # 计算涨跌
            price_change = latest_price - prev_price
            price_change_pct = (price_change / prev_price * 100) if prev_price != 0 else 0
            
            return {
                'latest_date': latest_row['日期'].strftime('%Y-%m-%d') if hasattr(latest_row['日期'], 'strftime') else str(latest_row['日期']),
                'latest_price': round(latest_price, 3),
                'price_change': round(price_change, 3),
                'price_change_pct': round(price_change_pct, 2),
                'volume': int(latest_row['成交量']) if pd.notna(latest_row['成交量']) else 0,
                'high': round(float(latest_row['最高价']), 3),
                'low': round(float(latest_row['最低价']), 3),
                'open': round(float(latest_row['开盘价']), 3)
            }
            
        except Exception as e:
            print(f"❌ 获取价格信息失败: {str(e)}")
            return {}
    
    def get_screening_etf_codes(self, threshold: str) -> List[str]:
        """
        获取筛选结果中的ETF代码列表
        
        Args:
            threshold: 门槛类型（如"3000万门槛"）
            
        Returns:
            List[str]: ETF代码列表
        """
        try:
            # 构建筛选结果文件路径
            project_root = self.config.data_dir.split("ETF日更")[0]
            screening_path = os.path.join(project_root, "ETF_初筛", "data")
            
            if not os.path.exists(screening_path):
                if not self.config.performance_mode:
                    print(f"❌ 筛选结果目录不存在: {screening_path}")
                return []
            
            # 查找筛选结果文件 - 支持.txt格式
            threshold_dir = os.path.join(screening_path, threshold)
            
            if not os.path.exists(threshold_dir):
                if not self.config.performance_mode:
                    print(f"❌ 未找到{threshold}目录: {threshold_dir}")
                return []
            
            # 查找通过筛选ETF.txt文件
            screening_file = os.path.join(threshold_dir, "通过筛选ETF.txt")
            
            if not os.path.exists(screening_file):
                if not self.config.performance_mode:
                    print(f"❌ 未找到{threshold}的筛选结果文件")
                return []
            
            # 读取筛选结果文件
            etf_codes = []
            with open(screening_file, 'r', encoding='utf-8') as f:
                lines = f.readlines()
            
            # 解析ETF代码（跳过注释行）
            for line in lines:
                line = line.strip()
                if line and not line.startswith('#'):
                    code_str = line.strip()
                    
                    # 使用统一的ETF代码标准化方法
                    code_str = self.config.normalize_etf_code(code_str)
                    etf_codes.append(code_str)
            
            if not self.config.performance_mode:
                print(f"📊 {threshold}: 找到 {len(etf_codes)} 个筛选ETF")
            
            return etf_codes
            
        except Exception as e:
            if not self.config.performance_mode:
                print(f"❌ 读取筛选结果失败: {str(e)}")
            return []