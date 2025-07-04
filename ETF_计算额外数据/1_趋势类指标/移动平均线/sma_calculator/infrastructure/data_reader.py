#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ETF数据读取器 (重构版)
===================

精简而强大的ETF数据读取和预处理功能
"""

import pandas as pd
import os
from typing import Optional, Tuple, List, Dict
from .config import SMAConfig


class ETFDataReader:
    """ETF数据读取器 - 重构版"""
    
    def __init__(self, config: SMAConfig):
        """初始化数据读取器"""
        self.config = config
        
        # 向后兼容性属性
        self.data_dir = config.data_dir
        self.adj_type_map = {
            '前复权': '0_ETF日K(前复权)',
            '后复权': '0_ETF日K(后复权)', 
            '除权': '0_ETF日K(除权)'
        }
    
    def get_available_etfs(self) -> List[str]:
        """获取可用的ETF代码列表"""
        try:
            if not os.path.exists(self.config.data_dir):
                return []
            
            csv_files = [f for f in os.listdir(self.config.data_dir) if f.endswith('.csv')]
            return sorted([f.replace('.csv', '') for f in csv_files])
            
        except Exception:
            return []
    
    def read_etf_data(self, etf_code: str) -> Optional[Tuple[pd.DataFrame, int]]:
        """
        读取单个ETF的数据
        
        Args:
            etf_code: ETF代码
            
        Returns:
            Tuple[pd.DataFrame, int]: (数据DataFrame, 总行数) 或 None
        """
        try:
            file_path = self.config.get_etf_file_path(etf_code)
            print(f"   🔍 尝试读取文件: {file_path}")
            
            if not os.path.exists(file_path):
                print(f"   ❌ 文件不存在: {file_path}")
                return None
            
            print(f"   ✅ 文件存在，大小: {os.path.getsize(file_path)} 字节")
            
            try:
                # 优化读取：只读取必要列
                print(f"   🔍 尝试读取CSV文件，列: ['日期', '收盘价']")
                df = pd.read_csv(
                    file_path, 
                    encoding='utf-8',
                    usecols=['日期', '收盘价'],
                    dtype={'收盘价': 'float32'}
                )
                
                total_rows = len(df)
                print(f"   ✅ 成功读取CSV，总行数: {total_rows}")
                
                if df.empty:
                    print(f"   ❌ 读取的DataFrame为空")
                    return None
                
                # 数据预处理
                print(f"   🔍 开始数据预处理")
                df = self._preprocess_data(df)
                if df is None:
                    print(f"   ❌ 预处理后DataFrame为None")
                    return None
                
                if len(df) < self.config.max_period:
                    print(f"   ❌ 数据行数 {len(df)} 小于所需最小行数 {self.config.max_period}")
                    return None
                
                print(f"   ✅ 预处理成功，处理后行数: {len(df)}")
                return df, total_rows
                
            except KeyError as e:
                print(f"   ❌ 列名错误: {str(e)}")
                # 尝试读取全部列以查看实际列名
                try:
                    all_df = pd.read_csv(file_path, encoding='utf-8', nrows=1)
                    print(f"   📊 实际列名: {all_df.columns.tolist()}")
                except Exception as inner_e:
                    print(f"   ❌ 尝试读取全部列失败: {str(inner_e)}")
                return None
                
        except Exception as e:
            print(f"   ❌ 读取ETF数据异常: {str(e)}")
            import traceback
            print(f"   📋 详细错误: {traceback.format_exc()}")
            return None
    
    def _preprocess_data(self, df: pd.DataFrame) -> Optional[pd.DataFrame]:
        """数据预处理"""
        try:
            df = df.copy()
            
            # 日期处理：YYYYMMDD -> datetime
            df['日期'] = pd.to_datetime(df['日期'], format='%Y%m%d')
            
            # 按日期排序（升序）
            df = df.sort_values('日期', ascending=True).reset_index(drop=True)
            
            # 收盘价处理
            df['收盘价'] = pd.to_numeric(df['收盘价'], errors='coerce')
            
            # 数据清理：移除无效价格
            df = df.dropna(subset=['收盘价'])
            df = df[df['收盘价'] > 0]
            
            return df if len(df) > 0 else None
            
        except Exception:
            return None
    
    def get_latest_price_info(self, df: pd.DataFrame) -> Dict:
        """获取最新价格信息"""
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
            
        except Exception:
            return {}
    
    def get_date_range(self, df: pd.DataFrame) -> Dict:
        """获取数据日期范围"""
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
            
        except Exception:
            return {}
    
    def validate_etf_code(self, etf_code: str) -> bool:
        """验证ETF代码是否有效"""
        try:
            file_path = self.config.get_etf_file_path(etf_code)
            return os.path.exists(file_path)
        except Exception:
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
            # 智能检测项目根目录
            current_dir = os.getcwd()
            if "ETF_计算额外数据" in current_dir:
                project_root = current_dir.split("ETF_计算额外数据")[0]
            else:
                project_root = current_dir
            
            # 构建筛选结果文件路径
            screening_file = os.path.join(
                project_root, "ETF_初筛", "data", threshold, "通过筛选ETF.txt"
            )
            
            if not os.path.exists(screening_file):
                return []
            
            # 读取ETF代码（过滤注释行）
            with open(screening_file, 'r', encoding='utf-8') as f:
                etf_codes = [line.strip() for line in f.readlines() 
                           if line.strip() and not line.strip().startswith('#')]
            
            # ETF代码标准化
            return self._standardize_etf_codes(etf_codes)
            
        except Exception:
            return []
    
    def _standardize_etf_codes(self, etf_codes: List[str]) -> List[str]:
        """标准化ETF代码格式"""
        standardized_codes = []
        
        for code in etf_codes:
            if code.endswith(('.SH', '.SZ')):
                standardized_codes.append(code)
                continue
            
            if len(code) == 6 and code.isdigit():
                # 基于规则判断交易所
                if code.startswith(('50', '51', '52', '56', '58')):
                    standardized_codes.append(code + '.SH')
                elif code.startswith(('15', '16', '18')):
                    standardized_codes.append(code + '.SZ')
                else:
                    # 通过文件存在性判断
                    for suffix in ['.SH', '.SZ']:
                        test_code = code + suffix
                        if os.path.exists(self.config.get_etf_file_path(test_code)):
                            standardized_codes.append(test_code)
                            break
        
        return standardized_codes 