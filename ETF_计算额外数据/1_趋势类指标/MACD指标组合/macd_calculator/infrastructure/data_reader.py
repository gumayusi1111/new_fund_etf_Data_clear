#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MACD数据读取器 - 重构版
======================

提供统一的数据读取接口
"""

import os
import pandas as pd
import glob
from typing import List, Optional, Dict, Any
from .config import MACDConfig


class MACDDataReader:
    """MACD数据读取器 - 重构版"""
    
    def __init__(self, config: MACDConfig):
        """
        初始化数据读取器
        
        Args:
            config: MACD配置对象
        """
        self.config = config
        self.data_path = config.get_data_source_path()
        self.adj_type = config.adj_type
        
        print("📖 MACD数据读取器初始化完成")
        print(f"   📁 数据路径: {self.data_path}")
        print(f"   📊 复权类型: {self.adj_type}")
    
    def get_etf_file_path(self, etf_code: str) -> Optional[str]:
        """
        获取ETF数据文件的完整路径
        
        Args:
            etf_code: ETF代码
            
        Returns:
            文件路径，如果文件不存在返回None
        """
        # 标准化ETF代码格式
        clean_code = etf_code.replace('.SH', '').replace('.SZ', '')
        
        # 构建文件模式并查找匹配的文件
        file_pattern = os.path.join(self.data_path, f"{clean_code}.*")
        matching_files = glob.glob(file_pattern)
        
        if matching_files:
            return matching_files[0]  # 返回第一个匹配的文件
        
        return None
    
    def read_etf_data(self, etf_code: str) -> Optional[pd.DataFrame]:
        """
        读取单个ETF数据
        
        Args:
            etf_code: ETF代码
            
        Returns:
            ETF数据DataFrame，如果失败返回None
        """
        try:
            # 标准化ETF代码格式
            clean_code = etf_code.replace('.SH', '').replace('.SZ', '')
            
            # 构建文件路径
            file_pattern = os.path.join(self.data_path, f"{clean_code}.*")
            matching_files = glob.glob(file_pattern)
            
            if not matching_files:
                print(f"❌ 未找到ETF数据文件: {etf_code}")
                return None
            
            file_path = matching_files[0]
            
            # 读取CSV数据
            df = pd.read_csv(file_path, encoding='utf-8')
            
            # 数据验证和清理
            if df.empty:
                print(f"❌ ETF数据文件为空: {etf_code}")
                return None
            
            # 确保必要的列存在
            required_columns = ['日期', '收盘价']
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                print(f"❌ 缺少必要的列: {missing_columns} in {etf_code}")
                return None
            
            # 日期处理 - 确保与其他系统一致
            # 处理数字格式的日期（如20250710）
            df['日期'] = pd.to_datetime(df['日期'], format='%Y%m%d', errors='coerce')
            # 如果上面失败，尝试标准格式
            if df['日期'].isna().any():
                df['日期'] = pd.to_datetime(df['日期'], errors='coerce')
            df = df.sort_values('日期').reset_index(drop=True)
            
            # 添加ETF代码列 - 清理格式保持一致
            df['代码'] = clean_code
            
            return df
            
        except Exception as e:
            print(f"❌ 读取ETF数据失败 {etf_code}: {str(e)}")
            return None
    
    def get_available_etfs(self, threshold: str = "3000万门槛") -> List[str]:
        """
        获取可用的ETF代码列表 - 从ETF_初筛结果读取
        
        Args:
            threshold: 门槛类型 ("3000万门槛" 或 "5000万门槛")
            
        Returns:
            ETF代码列表
        """
        try:
            # 构建ETF_初筛结果文件路径
            project_root = self.data_path
            while not os.path.basename(project_root) == 'data_clear':
                parent = os.path.dirname(project_root)
                if parent == project_root:
                    break
                project_root = parent
            
            filter_file = os.path.join(
                project_root,
                "ETF_初筛",
                "data", 
                threshold,
                "通过筛选ETF.txt"
            )
            
            if not os.path.exists(filter_file):
                print(f"❌ 初筛结果文件不存在: {filter_file}")
                print("⚠️ 回退到读取所有原数据ETF...")
                return self._get_all_etfs_fallback()
            
            # 读取筛选后的ETF列表
            etf_codes = []
            with open(filter_file, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#'):
                        code = line.strip()
                        # 添加交易所后缀
                        if code.startswith(('51', '52', '56', '58')):
                            etf_codes.append(f"{code}.SH")
                        elif code.startswith(('15', '16')):
                            etf_codes.append(f"{code}.SZ")
                        else:
                            etf_codes.append(f"{code}.SH")  # 默认上交所
            
            print(f"📊 从初筛结果读取到 {len(etf_codes)} 个ETF ({threshold})")
            return sorted(etf_codes)
            
        except Exception as e:
            print(f"❌ 读取初筛结果失败: {str(e)}")
            print("⚠️ 回退到读取所有原数据ETF...")
            return self._get_all_etfs_fallback()
    
    def _get_all_etfs_fallback(self) -> List[str]:
        """回退方案：从原数据目录读取所有ETF"""
        try:
            if not os.path.exists(self.data_path):
                print(f"❌ 数据目录不存在: {self.data_path}")
                return []
            
            # 获取所有CSV文件
            csv_files = glob.glob(os.path.join(self.data_path, "*.csv"))
            
            # 提取ETF代码
            etf_codes = []
            for file_path in csv_files:
                filename = os.path.basename(file_path)
                code = filename.replace('.csv', '')
                
                # 添加交易所后缀
                if code.startswith(('51', '52', '56', '58')):
                    etf_codes.append(f"{code}.SH")
                elif code.startswith(('15', '16')):
                    etf_codes.append(f"{code}.SZ")
                else:
                    etf_codes.append(f"{code}.SH")  # 默认上交所
            
            print(f"📊 从原数据目录读取到 {len(etf_codes)} 个ETF (回退模式)")
            return sorted(etf_codes)
            
        except Exception as e:
            print(f"❌ 获取ETF列表失败: {str(e)}")
            return []
    
    def validate_etf_data(self, df: pd.DataFrame, etf_code: str) -> bool:
        """
        验证ETF数据有效性
        
        Args:
            df: ETF数据DataFrame
            etf_code: ETF代码
            
        Returns:
            验证结果
        """
        try:
            if df is None or df.empty:
                return False
            
            # 检查必要列
            required_columns = ['日期', '收盘价', '代码']
            if not all(col in df.columns for col in required_columns):
                return False
            
            # 检查数据量
            if len(df) < 30:  # MACD需要足够的历史数据
                print(f"⚠️ {etf_code} 数据量不足 ({len(df)}行)，MACD计算需要至少30个数据点")
                return False
            
            # 检查价格数据
            if df['收盘价'].isna().any() or (df['收盘价'] <= 0).any():
                print(f"⚠️ {etf_code} 包含无效价格数据")
                return False
            
            return True
            
        except Exception as e:
            print(f"❌ 验证ETF数据失败 {etf_code}: {str(e)}")
            return False
    
    def get_data_info(self, etf_code: str) -> Dict[str, Any]:
        """
        获取ETF数据信息
        
        Args:
            etf_code: ETF代码
            
        Returns:
            数据信息字典
        """
        df = self.read_etf_data(etf_code)
        
        if df is None:
            return {'error': 'Failed to read data'}
        
        return {
            'etf_code': etf_code,
            'total_records': len(df),
            'date_range': {
                'start': df['日期'].min().strftime('%Y-%m-%d'),
                'end': df['日期'].max().strftime('%Y-%m-%d')
            },
            'price_range': {
                'min': float(df['收盘价'].min()),
                'max': float(df['收盘价'].max())
            },
            'data_quality': 'valid' if self.validate_etf_data(df, etf_code) else 'invalid'
        }