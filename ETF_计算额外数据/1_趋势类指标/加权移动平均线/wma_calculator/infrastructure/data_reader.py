#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
WMA数据读取器模块 - 重构版
=========================

从原有data_reader.py完全迁移，保持所有功能不变
"""

import pandas as pd
import os
from typing import List, Optional, Dict, Tuple
from .config import WMAConfig


class WMADataReader:
    """WMA数据读取器 - 重构版（功能完全一致）"""
    
    def __init__(self, config: WMAConfig):
        """
        初始化数据读取器 - 保持原有初始化逻辑
        
        Args:
            config: WMA配置对象
        """
        self.config = config
        
        print("📊 WMA数据读取器初始化完成")
        print(f"   📁 数据路径: {self.config.data_path}")
        print(f"   📈 复权类型: {self.config.adj_type}")
    
    def get_available_etfs(self) -> List[str]:
        """
        获取可用的ETF代码列表 - 保持原有逻辑
        
        Returns:
            List[str]: 可用的ETF代码列表
        """
        if not os.path.exists(self.config.data_path):
            print(f"❌ 数据目录不存在: {self.config.data_path}")
            return []
        
        etf_files = [f for f in os.listdir(self.config.data_path) if f.endswith('.csv')]
        etf_codes = [f.replace('.csv', '') for f in etf_files]
        
        return sorted(etf_codes)
    
    def read_etf_data(self, etf_code: str) -> Optional[Tuple[pd.DataFrame, int]]:
        """
        读取ETF数据 - 保持原有读取逻辑
        
        Args:
            etf_code: ETF代码
            
        Returns:
            Optional[Tuple[pd.DataFrame, int]]: (数据DataFrame, 原始总行数) 或 None
        """
        file_path = self.config.get_file_path(etf_code)
        
        if not os.path.exists(file_path):
            print(f"❌ 数据文件不存在: {file_path}")
            return None
        
        try:
            # 读取CSV文件 - 保持原有读取方式
            df = pd.read_csv(file_path, encoding='utf-8')
            total_rows = len(df)
            
            # 数据验证 - 保持原有验证逻辑
            if df.empty:
                print(f"❌ {etf_code}: 数据文件为空")
                return None
            
            # 验证必要字段 - 保持原有字段验证
            required_columns = ['代码', '日期', '收盘价']
            missing_columns = [col for col in required_columns if col not in df.columns]
            
            if missing_columns:
                print(f"❌ {etf_code}: 缺少必要字段: {missing_columns}")
                return None
            
            # 数据清洗 - 保持原有清洗逻辑
            df = self._clean_data(df, etf_code)
            
            if df is None or df.empty:
                print(f"❌ {etf_code}: 数据清洗后为空")
                return None
            
            print(f"📊 {etf_code}: 成功读取 {len(df)} 行数据 (原始: {total_rows} 行)")
            return df, total_rows
            
        except FileNotFoundError:
            print(f"❌ {etf_code}: 数据文件不存在: {file_path}")
            return None
        except pd.errors.EmptyDataError:
            print(f"❌ {etf_code}: 数据文件为空")
            return None
        except UnicodeDecodeError as e:
            print(f"❌ {etf_code}: 文件编码错误: {str(e)}")
            return None
        except Exception as e:
            print(f"❌ {etf_code}: 数据读取异常: {str(e)}")
            return None
    
    def _clean_data(self, df: pd.DataFrame, etf_code: str) -> Optional[pd.DataFrame]:
        """
        数据清洗 - 保持原有清洗逻辑
        
        Args:
            df: 原始数据
            etf_code: ETF代码
            
        Returns:
            Optional[pd.DataFrame]: 清洗后的数据或None
        """
        try:
            # 复制数据，避免修改原始数据
            df_cleaned = df.copy()
            
            # 日期格式处理 - 修正：正确处理YYYYMMDD格式，转换为YYYY-MM-DD字符串格式
            df_cleaned['日期'] = pd.to_datetime(df_cleaned['日期'], format='%Y%m%d', errors='coerce')
            
            # 移除日期无效的行
            df_cleaned = df_cleaned.dropna(subset=['日期'])
            
            # 转换为YYYY-MM-DD字符串格式，与SMA系统保持一致
            df_cleaned['日期'] = df_cleaned['日期'].dt.strftime('%Y-%m-%d')
            
            # 按日期排序 - 保持原有排序方式（字符串格式的日期排序）
            df_cleaned = df_cleaned.sort_values('日期')
            
            # 价格字段数值化处理 - 保持原有处理方式
            price_columns = ['收盘价', '开盘价', '最高价', '最低价']
            for col in price_columns:
                if col in df_cleaned.columns:
                    df_cleaned[col] = pd.to_numeric(df_cleaned[col], errors='coerce')
            
            # 移除收盘价无效的行
            df_cleaned = df_cleaned.dropna(subset=['收盘价'])
            
            # 移除收盘价为0或负数的行
            df_cleaned = df_cleaned[df_cleaned['收盘价'] > 0]
            
            # 重置索引
            df_cleaned = df_cleaned.reset_index(drop=True)
            
            if len(df_cleaned) == 0:
                print(f"⚠️ {etf_code}: 数据清洗后没有有效数据")
                return None
            
            return df_cleaned
            
        except ValueError as e:
            print(f"❌ {etf_code}: 数据格式错误: {str(e)}")
            return None
        except pd.errors.ParserError as e:
            print(f"❌ {etf_code}: 日期解析错误: {str(e)}")
            return None
        except Exception as e:
            print(f"❌ {etf_code}: 数据清洗异常: {str(e)}")
            return None
    
    def get_latest_price_info(self, df: pd.DataFrame) -> Dict:
        """
        获取最新价格信息 - 保持原有获取逻辑
        
        Args:
            df: ETF数据
            
        Returns:
            Dict: 最新价格信息
        """
        if df.empty:
            return {}
        
        latest_row = df.iloc[-1]
        
        price_info = {
            'date': str(latest_row['日期']),  # 日期已经是YYYY-MM-DD字符串格式
            'close': round(float(latest_row['收盘价']), 6),
            'code': latest_row.get('代码', '')
        }
        
        # 计算涨跌幅 - 保持原有计算方式
        if len(df) >= 2:
            prev_close = df.iloc[-2]['收盘价']
            change = latest_row['收盘价'] - prev_close
            change_pct = (change / prev_close) * 100 if prev_close > 0 else 0
            
            price_info.update({
                'change': round(float(change), 6),
                'change_pct': round(float(change_pct), 4)
            })
        else:
            price_info.update({
                'change': 0.0,
                'change_pct': 0.0
            })
        
        return price_info
    
    def get_date_range(self, df: pd.DataFrame) -> Dict:
        """
        获取数据日期范围 - 保持原有获取逻辑
        
        Args:
            df: ETF数据
            
        Returns:
            Dict: 日期范围信息
        """
        if df.empty:
            return {}
        
        return {
            'start_date': str(df['日期'].iloc[0]),  # 日期已经是YYYY-MM-DD字符串格式
            'end_date': str(df['日期'].iloc[-1]),   # 日期已经是YYYY-MM-DD字符串格式
            'total_days': len(df)
        }
    
    def cleanup_memory(self, df: pd.DataFrame) -> None:
        """
        清理内存 - 保持原有清理方式
        
        Args:
            df: 要清理的DataFrame
        """
        if df is not None:
            del df
    
    def get_screening_etf_codes(self, threshold: str) -> List[str]:
        """
        获取筛选结果的ETF代码列表 - 保持原有获取逻辑
        
        Args:
            threshold: 门槛类型 (如: "3000万门槛", "5000万门槛")
            
        Returns:
            List[str]: ETF代码列表
        """
        try:
            # 智能检测项目根目录 - 与SMA系统保持一致
            current_dir = os.getcwd()
            if "ETF_计算额外数据" in current_dir:
                project_root = current_dir.split("ETF_计算额外数据")[0]
            else:
                project_root = current_dir
            
            # 构建筛选结果文件路径 - 与SMA系统保持一致（不包含复权类型文件夹）
            screening_file = os.path.join(
                project_root, "ETF_初筛", "data", threshold, "通过筛选ETF.txt"
            )
            
            if not os.path.exists(screening_file):
                print(f"❌ 筛选文件不存在: {screening_file}")
                return []
            
            # 读取ETF代码（过滤注释行） - 与SMA系统保持一致
            with open(screening_file, 'r', encoding='utf-8') as f:
                etf_codes = [line.strip() for line in f.readlines() 
                           if line.strip() and not line.strip().startswith('#')]
            
            # ETF代码标准化 - 与SMA系统保持一致
            return self._standardize_etf_codes(etf_codes)
            
        except FileNotFoundError:
            print(f"❌ 筛选文件不存在: {screening_file}")
            return []
        except UnicodeDecodeError as e:
            print(f"❌ 筛选文件编码错误: {str(e)}")
            return []
        except Exception as e:
            print(f"❌ 读取筛选结果异常: {str(e)}")
            return []
    
    def _standardize_etf_codes(self, etf_codes: List[str]) -> List[str]:
        """
        标准化ETF代码格式 - 与SMA系统保持一致
        
        Args:
            etf_codes: 原始ETF代码列表
            
        Returns:
            List[str]: 标准化后的ETF代码列表
        """
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
                        if os.path.exists(self.config.get_file_path(test_code)):
                            standardized_codes.append(test_code)
                            break
        
        return standardized_codes
    
    def get_etf_file_path(self, etf_code: str) -> str:
        """
        获取ETF数据文件路径 - 保持原有获取逻辑
        
        Args:
            etf_code: ETF代码
            
        Returns:
            str: 文件路径
        """
        return self.config.get_file_path(etf_code) 