#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
布林带CSV处理器
==============

专门处理布林带结果的CSV输出格式化和批量处理
参照趋势类指标的CSV处理模式
"""

import os
import pandas as pd
from typing import Dict, List, Optional, Any
from datetime import datetime
from ..infrastructure.utils import BBUtils


class BBCSVHandler:
    """布林带CSV处理器"""
    
    def __init__(self, config):
        """初始化CSV处理器"""
        self.config = config
        self.utils = BBUtils()
    
    def format_single_result(self, etf_code: str, bb_results: Dict, 
                           raw_data: pd.DataFrame) -> Optional[Dict]:
        """
        格式化单个ETF的布林带结果为标准输出格式
        
        Args:
            etf_code: ETF代码
            bb_results: 布林带计算结果
            raw_data: 原始ETF数据
            
        Returns:
            Optional[Dict]: 格式化后的结果字典
        """
        try:
            if not bb_results or raw_data.empty:
                return None
            
            # 格式化ETF代码
            clean_etf_code = self.utils.format_etf_code(etf_code)
            
            # 获取最新日期
            latest_date = raw_data['日期'].max()
            if pd.isna(latest_date):
                return None
            
            # 格式化日期为YYYY-MM-DD
            formatted_date = latest_date.strftime('%Y-%m-%d')
            
            # 构建标准输出格式
            formatted_result = {
                'code': clean_etf_code,
                'date': formatted_date,
                'bb_middle': bb_results.get('bb_middle'),
                'bb_upper': bb_results.get('bb_upper'),  
                'bb_lower': bb_results.get('bb_lower'),
                'bb_width': bb_results.get('bb_width'),
                'bb_position': bb_results.get('bb_position'),
                'bb_percent_b': bb_results.get('bb_percent_b')
            }
            
            # 数据有效性检查
            if all(v is None for k, v in formatted_result.items() if k not in ['code', 'date']):
                return None
            
            return formatted_result
            
        except Exception:
            return None
    
    def format_bb_result_to_dataframe(self, etf_code: str, etf_data: pd.DataFrame, 
                                     bb_results: Dict) -> pd.DataFrame:
        """
        将布林带结果格式化为DataFrame
        
        Args:
            etf_code: ETF代码
            etf_data: 原始ETF数据
            bb_results: 布林带计算结果
            
        Returns:
            pd.DataFrame: 格式化后的DataFrame
        """
        try:
            if not bb_results or etf_data.empty:
                return pd.DataFrame()
            
            # 创建结果DataFrame
            result_df = etf_data.copy()
            
            # 添加布林带指标字段（统一使用小写字段名）
            for field, values in bb_results.items():
                if values is not None:
                    # 统一字段名为小写格式
                    standardized_field = field.lower()
                    result_df[standardized_field] = values
            
            # 清理ETF代码
            clean_etf_code = self.utils.format_etf_code(etf_code)
            result_df['code'] = clean_etf_code
            
            # 确保日期格式
            if '日期' in result_df.columns:
                result_df['date'] = pd.to_datetime(result_df['日期']).dt.strftime('%Y-%m-%d')
            
            # 选择输出字段（按第一大类标准）
            output_fields = self.config.get_bb_output_fields()
            available_fields = [field for field in output_fields if field in result_df.columns]
            
            if available_fields:
                result_df = result_df[available_fields]
            
            return result_df
            
        except Exception as e:
            return pd.DataFrame()
    
    def format_bb_full_history_to_dataframe(self, etf_code: str, etf_data: pd.DataFrame, 
                                           bb_results_df: pd.DataFrame) -> pd.DataFrame:
        """
        将布林带完整历史结果格式化为DataFrame
        
        Args:
            etf_code: ETF代码
            etf_data: 原始ETF数据
            bb_results_df: 布林带计算结果DataFrame
            
        Returns:
            pd.DataFrame: 格式化后的DataFrame
        """
        try:
            if bb_results_df.empty or etf_data.empty:
                return pd.DataFrame()
            
            # 清理ETF代码
            clean_etf_code = self.utils.format_etf_code(etf_code)
            
            # 合并日期信息
            result_df = bb_results_df.copy()
            
            # 如果bb_results_df没有日期列，从etf_data中获取
            if '日期' not in result_df.columns and '日期' in etf_data.columns:
                # 确保两个DataFrame长度匹配
                etf_dates = etf_data['日期'].tail(len(result_df))
                result_df['日期'] = etf_dates.values
            
            # 添加ETF代码
            result_df['code'] = clean_etf_code
            
            # 确保日期格式
            if '日期' in result_df.columns:
                result_df['date'] = pd.to_datetime(result_df['日期']).dt.strftime('%Y-%m-%d')
            
            # 选择输出字段（按第一大类标准）
            output_fields = self.config.get_bb_output_fields()
            available_fields = [field for field in output_fields if field in result_df.columns]
            
            if available_fields:
                result_df = result_df[available_fields]
            
            # 移除NaN行
            result_df = result_df.dropna(subset=['bb_middle', 'bb_upper', 'bb_lower'])
            
            return result_df
            
        except Exception as e:
            return pd.DataFrame()
    
    def create_batch_csv(self, results_dict: Dict[str, Any], threshold: str, 
                        param_set: str = None, output_filename: str = None) -> bool:
        """
        为每个ETF创建独立的CSV文件（支持参数集分层）
        
        Args:
            results_dict: 批量计算结果字典
            threshold: 门槛类型
            param_set: 参数集名称
            output_filename: 输出文件名（可选）
            
        Returns:
            bool: 创建是否成功
        """
        try:
            # 确定参数集
            if not param_set:
                param_set = self.config.get_current_param_set_name()
            
            # 确保目录存在 - 使用参数集分层目录
            output_dir = os.path.join(self.config.default_output_dir, threshold, param_set)
            self.utils.ensure_directory_exists(output_dir)
            
            # 为每个ETF生成独立的CSV文件
            saved_count = 0
            
            # 获取ETF列表
            etf_list = results_dict.get('etf_list', [])
            
            for etf_code in etf_list:
                try:
                    # 从缓存中读取参数集特定的数据
                    clean_etf_code = self.utils.format_etf_code(etf_code)
                    cache_file = self.config.get_cache_file_path(threshold, clean_etf_code, param_set)
                    
                    if os.path.exists(cache_file):
                        # 直接复制缓存中的数据到输出目录
                        import pandas as pd
                        cached_data = pd.read_csv(cache_file)
                        
                        output_file = os.path.join(output_dir, f"{clean_etf_code}.csv")
                        cached_data.to_csv(output_file, index=False, encoding='utf-8-sig', float_format='%.8f')
                        saved_count += 1
                        
                except Exception:
                    continue
            
            return saved_count > 0
            
        except Exception:
            return False
    
    def save_bb_data_to_csv(self, etf_code: str, data: pd.DataFrame, output_path: str) -> Dict[str, Any]:
        """
        保存布林带数据到CSV文件
        
        Args:
            etf_code: ETF代码
            data: 数据DataFrame
            output_path: 输出路径
            
        Returns:
            Dict[str, Any]: 保存结果
        """
        result = {
            'success': False,
            'output_path': output_path,
            'error': None
        }
        
        try:
            if data.empty:
                result['error'] = '数据为空'
                return result
            
            # 确保目录存在
            output_dir = os.path.dirname(output_path)
            self.utils.ensure_directory_exists(output_dir)
            
            # 保存数据
            data.to_csv(output_path, index=False, encoding='utf-8-sig', float_format='%.8f')
            
            result['success'] = True
            return result
            
        except Exception as e:
            result['error'] = str(e)
            return result
    
    def load_bb_data_from_csv(self, csv_path: str) -> Optional[pd.DataFrame]:
        """
        从CSV文件加载布林带数据
        
        Args:
            csv_path: CSV文件路径
            
        Returns:
            Optional[pd.DataFrame]: 加载的数据，失败返回None
        """
        try:
            if not os.path.exists(csv_path):
                return None
            
            data = pd.read_csv(csv_path, encoding='utf-8-sig')
            return data if not data.empty else None
            
        except Exception:
            return None
    
    def create_summary_csv(self, batch_results: Dict[str, Any]) -> bool:
        """
        创建处理结果汇总CSV
        
        Args:
            batch_results: 批量处理结果
            
        Returns:
            bool: 创建是否成功
        """
        try:
            summary_data = []
            
            for threshold, details in batch_results.get('threshold_details', {}).items():
                etf_list = details.get('etf_list', [])
                successful_etfs = details.get('successful_etfs', 0)
                failed_etfs = details.get('failed_etfs', 0)
                
                summary_row = {
                    'threshold': threshold,
                    'total_etfs': len(etf_list),
                    'successful_calculations': successful_etfs,
                    'failed_calculations': failed_etfs,
                    'success_rate_percent': round((successful_etfs / max(len(etf_list), 1)) * 100, 2),
                    'bb_config': f"BB({self.config.get_bb_period()},{self.config.get_bb_std_multiplier()})",
                    'adj_type': self.config.adj_type,
                    'processing_date': datetime.now().strftime('%Y-%m-%d'),
                    'processing_time': datetime.now().strftime('%H:%M:%S')
                }
                
                summary_data.append(summary_row)
            
            if not summary_data:
                return False
            
            # 转换为DataFrame
            df = pd.DataFrame(summary_data)
            
            # 保存汇总文件
            output_dir = self.config.default_output_dir
            self.utils.ensure_directory_exists(output_dir)
            
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            summary_file = os.path.join(output_dir, f"bb_processing_summary_{timestamp}.csv")
            
            df.to_csv(summary_file, index=False, encoding='utf-8-sig', float_format='%.8f')
            
            return True
            
        except Exception:
            return False
    
    def validate_csv_output(self, csv_file_path: str) -> Dict[str, Any]:
        """
        验证CSV输出文件的格式和内容
        
        Args:
            csv_file_path: CSV文件路径
            
        Returns:
            Dict[str, Any]: 验证结果
        """
        validation_result = {
            'is_valid': False,
            'file_exists': False,
            'row_count': 0,
            'column_count': 0,
            'expected_columns': self.config.get_bb_output_fields(),
            'missing_columns': [],
            'extra_columns': [],
            'data_quality_issues': []
        }
        
        try:
            if not os.path.exists(csv_file_path):
                return validation_result
            
            validation_result['file_exists'] = True
            
            # 读取CSV文件
            df = pd.read_csv(csv_file_path, encoding='utf-8-sig')
            
            validation_result['row_count'] = len(df)
            validation_result['column_count'] = len(df.columns)
            
            # 检查列结构
            expected_columns = set(validation_result['expected_columns'])
            actual_columns = set(df.columns)
            
            validation_result['missing_columns'] = list(expected_columns - actual_columns)
            validation_result['extra_columns'] = list(actual_columns - expected_columns)
            
            # 数据质量检查
            if 'code' in df.columns:
                empty_codes = df['code'].isna().sum()
                if empty_codes > 0:
                    validation_result['data_quality_issues'].append(f"空ETF代码: {empty_codes}行")
            
            if 'date' in df.columns:
                invalid_dates = df['date'].isna().sum()
                if invalid_dates > 0:
                    validation_result['data_quality_issues'].append(f"无效日期: {invalid_dates}行")
            
            # 检查布林带数值
            bb_columns = ['bb_middle', 'bb_upper', 'bb_lower']
            for col in bb_columns:
                if col in df.columns:
                    null_values = df[col].isna().sum()
                    if null_values > 0:
                        validation_result['data_quality_issues'].append(f"{col}空值: {null_values}行")
            
            # 逻辑关系检查（上轨 > 中轨 > 下轨）
            if all(col in df.columns for col in bb_columns):
                valid_rows = df[(df['bb_upper'] >= df['bb_middle']) & 
                              (df['bb_middle'] >= df['bb_lower'])]
                invalid_count = len(df) - len(valid_rows)
                if invalid_count > 0:
                    validation_result['data_quality_issues'].append(
                        f"布林带逻辑错误: {invalid_count}行（应满足上轨≥中轨≥下轨）"
                    )
            
            # 判断整体有效性
            validation_result['is_valid'] = (
                len(validation_result['missing_columns']) == 0 and
                len(validation_result['data_quality_issues']) == 0 and
                validation_result['row_count'] > 0
            )
            
            return validation_result
            
        except Exception as e:
            validation_result['data_quality_issues'].append(f"验证异常: {str(e)}")
            return validation_result
    
    def merge_multiple_csv_files(self, csv_files: List[str], output_file: str) -> bool:
        """
        合并多个CSV文件
        
        Args:
            csv_files: CSV文件路径列表
            output_file: 输出合并文件路径
            
        Returns:
            bool: 合并是否成功
        """
        try:
            all_dataframes = []
            
            for csv_file in csv_files:
                if os.path.exists(csv_file):
                    df = pd.read_csv(csv_file, encoding='utf-8-sig')
                    if not df.empty:
                        all_dataframes.append(df)
            
            if not all_dataframes:
                return False
            
            # 合并所有数据
            merged_df = pd.concat(all_dataframes, ignore_index=True)
            
            # 去重（基于code列）
            merged_df = merged_df.drop_duplicates(subset=['code'], keep='last')
            
            # 按code排序
            merged_df = merged_df.sort_values('code').reset_index(drop=True)
            
            # 确保列顺序正确
            column_order = self.config.get_bb_output_fields()
            merged_df = merged_df.reindex(columns=column_order)
            
            # 保存合并结果
            self.utils.ensure_directory_exists(os.path.dirname(output_file))
            merged_df.to_csv(output_file, index=False, encoding='utf-8-sig', float_format='%.8f')
            
            return True
            
        except Exception:
            return False
    
    def get_csv_statistics(self, csv_file_path: str) -> Dict[str, Any]:
        """
        获取CSV文件统计信息
        
        Args:
            csv_file_path: CSV文件路径
            
        Returns:
            Dict[str, Any]: 统计信息
        """
        stats = {
            'file_path': csv_file_path,
            'file_exists': False,
            'file_size_mb': 0,
            'row_count': 0,
            'column_count': 0,
            'bb_statistics': {}
        }
        
        try:
            if not os.path.exists(csv_file_path):
                return stats
            
            stats['file_exists'] = True
            stats['file_size_mb'] = self.utils.get_file_size_mb(csv_file_path)
            
            # 读取数据
            df = pd.read_csv(csv_file_path, encoding='utf-8-sig')
            
            stats['row_count'] = len(df)
            stats['column_count'] = len(df.columns)
            
            # 布林带数值统计
            bb_columns = ['bb_middle', 'bb_upper', 'bb_lower', 'bb_width', 'bb_position', 'bb_percent_b']
            
            for col in bb_columns:
                if col in df.columns:
                    col_data = df[col].dropna()
                    if not col_data.empty:
                        stats['bb_statistics'][col] = {
                            'count': len(col_data),
                            'mean': round(col_data.mean(), 4),
                            'std': round(col_data.std(), 4),
                            'min': round(col_data.min(), 4),
                            'max': round(col_data.max(), 4),
                            'null_count': df[col].isna().sum()
                        }
            
            return stats
            
        except Exception:
            return stats