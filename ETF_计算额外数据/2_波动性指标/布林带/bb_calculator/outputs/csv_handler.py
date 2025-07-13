#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
布林带CSV处理器
==============

负责布林带数据的CSV格式化和输出处理
"""

import pandas as pd
from typing import Dict, List, Optional, Any
from ..infrastructure.config import BBConfig
from ..infrastructure.utils import BBUtils


class BBCSVHandler:
    """布林带CSV处理器"""
    
    def __init__(self, config: BBConfig):
        """初始化CSV处理器"""
        self.config = config
        self.utils = BBUtils()
        self.output_fields = config.get_bb_output_fields()
    
    def format_bb_result_to_dataframe(self, etf_code: str, raw_data: pd.DataFrame,
                                     bb_results: Dict[str, Optional[float]]) -> pd.DataFrame:
        """
        将布林带计算结果格式化为DataFrame
        
        Args:
            etf_code: ETF代码
            raw_data: 原始ETF数据
            bb_results: 布林带计算结果
            
        Returns:
            pd.DataFrame: 格式化后的DataFrame
        """
        if raw_data.empty or not bb_results:
            return pd.DataFrame()
        
        try:
            # 格式化ETF代码（去除.SH/.SZ后缀）
            clean_etf_code = self.utils.format_etf_code(etf_code)
            
            # 获取最新日期
            latest_date = raw_data['日期'].max()
            if pd.isna(latest_date):
                return pd.DataFrame()
            
            # 格式化日期为YYYY-MM-DD格式
            formatted_date = latest_date.strftime('%Y-%m-%d')
            
            # 构建结果字典
            result_row = {
                'code': clean_etf_code,
                'date': formatted_date,
                'bb_middle': bb_results.get('bb_middle'),
                'bb_upper': bb_results.get('bb_upper'),
                'bb_lower': bb_results.get('bb_lower'),
                'bb_width': bb_results.get('bb_width'),
                'bb_position': bb_results.get('bb_position'),
                'bb_percent_b': bb_results.get('bb_percent_b')
            }
            
            # 创建DataFrame
            result_df = pd.DataFrame([result_row])
            
            return result_df
            
        except Exception:
            return pd.DataFrame()
    
    def format_historical_bb_to_dataframe(self, etf_code: str, 
                                         historical_df: pd.DataFrame) -> pd.DataFrame:
        """
        将历史布林带数据格式化为标准输出格式
        
        Args:
            etf_code: ETF代码
            historical_df: 包含布林带计算结果的历史数据
            
        Returns:
            pd.DataFrame: 格式化后的DataFrame
        """
        if historical_df.empty:
            return pd.DataFrame()
        
        try:
            # 格式化ETF代码
            clean_etf_code = self.utils.format_etf_code(etf_code)
            
            # 选择需要的列
            result_df = historical_df.copy()
            
            # 添加code列
            result_df['code'] = clean_etf_code
            
            # 格式化日期列
            if '日期' in result_df.columns:
                result_df['date'] = pd.to_datetime(result_df['日期']).dt.strftime('%Y-%m-%d')
            else:
                return pd.DataFrame()
            
            # 选择输出字段，按照配置顺序
            output_columns = self.output_fields.copy()
            
            # 确保所有必需列都存在
            for col in output_columns:
                if col not in result_df.columns:
                    result_df[col] = None
            
            # 选择并重新排列列
            result_df = result_df[output_columns]
            
            # 去除所有布林带值都为空的行
            bb_value_columns = ['bb_middle', 'bb_upper', 'bb_lower']
            result_df = result_df.dropna(subset=bb_value_columns, how='all')
            
            return result_df
            
        except Exception:
            return pd.DataFrame()
    
    def save_bb_data_to_csv(self, etf_code: str, data: pd.DataFrame, 
                           output_file_path: str) -> Dict[str, Any]:
        """
        保存布林带数据到CSV文件
        
        Args:
            etf_code: ETF代码
            data: 要保存的数据
            output_file_path: 输出文件路径
            
        Returns:
            Dict[str, Any]: 保存结果
        """
        result = {
            'success': False,
            'file_path': output_file_path,
            'row_count': 0,
            'file_size_mb': 0,
            'error': None
        }
        
        if data.empty:
            result['error'] = '数据为空'
            return result
        
        try:
            # 确保目录存在
            import os
            output_dir = os.path.dirname(output_file_path)
            self.utils.ensure_directory_exists(output_dir)
            
            # 保存数据
            data.to_csv(output_file_path, index=False, encoding='utf-8')
            
            # 获取文件信息
            result['row_count'] = len(data)
            result['file_size_mb'] = self.utils.get_file_size_mb(output_file_path) or 0
            result['success'] = True
            
        except Exception as e:
            result['error'] = str(e)
        
        return result
    
    def load_bb_data_from_csv(self, file_path: str) -> Optional[pd.DataFrame]:
        """
        从CSV文件加载布林带数据
        
        Args:
            file_path: CSV文件路径
            
        Returns:
            Optional[pd.DataFrame]: 加载的数据，失败返回None
        """
        try:
            df = pd.read_csv(file_path, encoding='utf-8')
            
            # 验证数据格式
            if self._validate_csv_format(df):
                return df
            else:
                return None
                
        except Exception:
            return None
    
    def _validate_csv_format(self, df: pd.DataFrame) -> bool:
        """验证CSV格式是否正确"""
        if df.empty:
            return False
        
        # 检查必需列
        required_columns = ['code', 'date', 'bb_middle', 'bb_upper', 'bb_lower']
        for col in required_columns:
            if col not in df.columns:
                return False
        
        return True
    
    def merge_bb_data(self, dataframes: List[pd.DataFrame]) -> pd.DataFrame:
        """
        合并多个布林带数据DataFrame
        
        Args:
            dataframes: DataFrame列表
            
        Returns:
            pd.DataFrame: 合并后的数据
        """
        if not dataframes:
            return pd.DataFrame()
        
        try:
            # 过滤空的DataFrame
            valid_dfs = [df for df in dataframes if not df.empty]
            
            if not valid_dfs:
                return pd.DataFrame()
            
            # 合并数据
            merged_df = pd.concat(valid_dfs, ignore_index=True)
            
            # 按code和date排序
            if 'code' in merged_df.columns and 'date' in merged_df.columns:
                merged_df = merged_df.sort_values(['code', 'date']).reset_index(drop=True)
            
            return merged_df
            
        except Exception:
            return pd.DataFrame()
    
    def split_bb_data_by_etf(self, merged_df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        """
        按ETF代码拆分合并的布林带数据
        
        Args:
            merged_df: 合并的数据
            
        Returns:
            Dict[str, pd.DataFrame]: ETF代码到数据的映射
        """
        result = {}
        
        if merged_df.empty or 'code' not in merged_df.columns:
            return result
        
        try:
            # 按ETF代码分组
            for etf_code, group_df in merged_df.groupby('code'):
                result[str(etf_code)] = group_df.reset_index(drop=True)
            
            return result
            
        except Exception:
            return result
    
    def create_summary_report(self, etf_data_dict: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
        """
        创建布林带数据汇总报告
        
        Args:
            etf_data_dict: ETF代码到数据的映射
            
        Returns:
            Dict[str, Any]: 汇总报告
        """
        report = {
            'total_etfs': len(etf_data_dict),
            'total_records': 0,
            'etf_details': {},
            'data_quality': {
                'complete_records': 0,
                'incomplete_records': 0,
                'empty_etfs': 0
            },
            'date_range': {
                'earliest_date': None,
                'latest_date': None
            }
        }
        
        all_dates = []
        
        for etf_code, df in etf_data_dict.items():
            etf_detail = {
                'row_count': len(df) if not df.empty else 0,
                'date_range': None,
                'complete_indicators': 0,
                'incomplete_indicators': 0
            }
            
            if df.empty:
                report['data_quality']['empty_etfs'] += 1
            else:
                report['total_records'] += len(df)
                
                # 日期范围
                if 'date' in df.columns:
                    dates = pd.to_datetime(df['date'], errors='coerce').dropna()
                    if not dates.empty:
                        etf_detail['date_range'] = {
                            'start': dates.min().strftime('%Y-%m-%d'),
                            'end': dates.max().strftime('%Y-%m-%d')
                        }
                        all_dates.extend(dates)
                
                # 数据完整性
                bb_columns = ['bb_middle', 'bb_upper', 'bb_lower']
                for _, row in df.iterrows():
                    if all(pd.notna(row[col]) for col in bb_columns if col in df.columns):
                        etf_detail['complete_indicators'] += 1
                        report['data_quality']['complete_records'] += 1
                    else:
                        etf_detail['incomplete_indicators'] += 1
                        report['data_quality']['incomplete_records'] += 1
            
            report['etf_details'][etf_code] = etf_detail
        
        # 全局日期范围
        if all_dates:
            all_dates_series = pd.Series(all_dates)
            report['date_range']['earliest_date'] = all_dates_series.min().strftime('%Y-%m-%d')
            report['date_range']['latest_date'] = all_dates_series.max().strftime('%Y-%m-%d')
        
        return report
    
    def export_summary_to_csv(self, summary_report: Dict[str, Any], 
                             output_file_path: str) -> Dict[str, Any]:
        """
        导出汇总报告到CSV
        
        Args:
            summary_report: 汇总报告
            output_file_path: 输出文件路径
            
        Returns:
            Dict[str, Any]: 导出结果
        """
        result = {
            'success': False,
            'file_path': output_file_path,
            'error': None
        }
        
        try:
            # 准备汇总数据
            summary_data = []
            
            for etf_code, details in summary_report.get('etf_details', {}).items():
                summary_data.append({
                    'etf_code': etf_code,
                    'record_count': details.get('row_count', 0),
                    'complete_indicators': details.get('complete_indicators', 0),
                    'incomplete_indicators': details.get('incomplete_indicators', 0),
                    'date_start': details.get('date_range', {}).get('start', ''),
                    'date_end': details.get('date_range', {}).get('end', '')
                })
            
            # 创建DataFrame并保存
            summary_df = pd.DataFrame(summary_data)
            
            # 确保目录存在
            import os
            output_dir = os.path.dirname(output_file_path)
            self.utils.ensure_directory_exists(output_dir)
            
            summary_df.to_csv(output_file_path, index=False, encoding='utf-8')
            
            result['success'] = True
            
        except Exception as e:
            result['error'] = str(e)
        
        return result