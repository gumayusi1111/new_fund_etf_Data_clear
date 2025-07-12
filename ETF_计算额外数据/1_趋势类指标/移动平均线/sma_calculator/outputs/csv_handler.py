#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CSV输出处理器 (重构版)
===================

专门负责CSV格式的数据输出
"""

import pandas as pd
import io
from typing import Dict, Any, List
from ..interfaces.output_interface import ICSVHandler, OutputResult, OutputStatus, OutputFormat


class CSVOutputHandler(ICSVHandler):
    """CSV输出处理器 - 重构版"""
    
    def write(self, data: Any, output_path: str, options: Dict = None) -> OutputResult:
        """写入数据"""
        if isinstance(data, pd.DataFrame):
            return self.write_csv(data, output_path)
        return OutputResult(status=OutputStatus.FAILED, format=OutputFormat.CSV)
    
    def write_csv(self, data: pd.DataFrame, output_path: str, 
                  encoding: str = 'utf-8', index: bool = False) -> OutputResult:
        """写入CSV文件"""
        try:
            data.to_csv(output_path, index=index, encoding=encoding)
            return OutputResult(
                status=OutputStatus.SUCCESS,
                format=OutputFormat.CSV,
                output_path=output_path,
                records_count=len(data)
            )
        except Exception as e:
            return OutputResult(
                status=OutputStatus.FAILED,
                format=OutputFormat.CSV,
                error_message=str(e)
            )
    
    def append_csv(self, data: pd.DataFrame, output_path: str) -> OutputResult:
        """追加数据到CSV文件"""
        try:
            data.to_csv(output_path, mode='a', header=False, index=False, encoding='utf-8')
            return OutputResult(
                status=OutputStatus.SUCCESS,
                format=OutputFormat.CSV,
                output_path=output_path,
                records_count=len(data)
            )
        except Exception as e:
            return OutputResult(
                status=OutputStatus.FAILED,
                format=OutputFormat.CSV,
                error_message=str(e)
            )
    
    def format_single_etf(self, etf_result: Dict) -> str:
        """将单个ETF的历史数据格式化为CSV字符串"""
        try:
            # 提取数据
            etf_code = etf_result['etf_code']
            historical_data = etf_result.get('historical_data')  # 这应该是完整的历史数据DataFrame
            
            if historical_data is None or historical_data.empty:
                return f"Error: No historical data for ETF {etf_code}"
            
            # 创建CSV内容 - 与原系统格式保持一致
            csv_buffer = io.StringIO()
            
            # 处理ETF代码格式（移除后缀）
            clean_etf_code = etf_code.split('.')[0] if '.' in etf_code else etf_code
            
            # 写入CSV头部
            csv_buffer.write("code,date,SMA_5,SMA_10,SMA_20,SMA_60,SMA_DIFF_5_20,SMA_DIFF_5_20_PCT,SMA_DIFF_5_10\n")
            
            # 按日期倒序排列（最新的在前）
            # 检查列名是否存在
            date_col = '日期' if '日期' in historical_data.columns else 'date'
            if date_col not in historical_data.columns:
                return f"Error: No date column found for ETF {etf_code}"
                
            df_sorted = historical_data.sort_values(date_col, ascending=False)
            
            # 逐行写入数据
            for _, row in df_sorted.iterrows():
                date_str = row[date_col]
                ma5 = row.get('MA5', 0)
                ma10 = row.get('MA10', 0)
                ma20 = row.get('MA20', 0)
                ma60 = row.get('MA60', 0)
                
                # 计算差值（安全除法）
                diff_5_20 = ma5 - ma20 if ma5 and ma20 else 0
                diff_5_20_pct = (diff_5_20 / ma20 * 100) if ma20 and abs(ma20) > 1e-10 else 0
                diff_5_10 = ma5 - ma10 if ma5 and ma10 else 0
                
                # 写入数据行
                # 转换日期格式为YYYY-MM-DD
                if len(str(date_str)) == 8:  # YYYYMMDD格式
                    formatted_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
                else:
                    formatted_date = str(date_str)
                csv_buffer.write(f"{clean_etf_code},{formatted_date},{ma5:.8f},{ma10:.8f},{ma20:.8f},{ma60:.8f},{diff_5_20:+.8f},{diff_5_20_pct:+.8f},{diff_5_10:+.8f}\n")
            
            # 获取CSV内容
            csv_content = csv_buffer.getvalue()
            csv_buffer.close()
            
            return csv_content
            
        except Exception as e:
            # 出错时返回错误信息
            return f"Error formatting ETF {etf_result.get('etf_code', 'unknown')}: {str(e)}"
    
    def format_historical_data(self, df: pd.DataFrame, etf_code: str) -> str:
        """直接格式化DataFrame为历史数据CSV"""
        try:
            csv_buffer = io.StringIO()
            
            # 处理ETF代码格式
            clean_etf_code = etf_code.split('.')[0] if '.' in etf_code else etf_code
            
            # 写入CSV头部
            csv_buffer.write("code,date,SMA_5,SMA_10,SMA_20,SMA_60,SMA_DIFF_5_20,SMA_DIFF_5_20_PCT,SMA_DIFF_5_10\n")
            
            # 按日期倒序排列
            # 检查列名是否存在
            date_col = '日期' if '日期' in df.columns else 'date'
            if date_col not in df.columns:
                return f"Error: No date column found for {etf_code}"
                
            df_sorted = df.sort_values(date_col, ascending=False)
            
            # 逐行写入数据
            for _, row in df_sorted.iterrows():
                date_str = row[date_col]
                ma5 = row.get('MA5', 0)
                ma10 = row.get('MA10', 0)
                ma20 = row.get('MA20', 0)
                ma60 = row.get('MA60', 0)
                
                # 计算差值（安全除法）
                diff_5_20 = ma5 - ma20 if ma5 and ma20 else 0
                diff_5_20_pct = (diff_5_20 / ma20 * 100) if ma20 and abs(ma20) > 1e-10 else 0
                diff_5_10 = ma5 - ma10 if ma5 and ma10 else 0
                
                # 写入数据行
                # 转换日期格式为YYYY-MM-DD
                if len(str(date_str)) == 8:  # YYYYMMDD格式
                    formatted_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
                else:
                    formatted_date = str(date_str)
                csv_buffer.write(f"{clean_etf_code},{formatted_date},{ma5:.8f},{ma10:.8f},{ma20:.8f},{ma60:.8f},{diff_5_20:+.8f},{diff_5_20_pct:+.8f},{diff_5_10:+.8f}\n")
            
            csv_content = csv_buffer.getvalue()
            csv_buffer.close()
            
            return csv_content
            
        except Exception as e:
            return f"Error formatting historical data for {etf_code}: {str(e)}"
    
    def get_supported_formats(self):
        return [OutputFormat.CSV]
    
    def validate_output_path(self, output_path: str) -> bool:
        return output_path.endswith('.csv') 