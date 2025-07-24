#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
布林带文件管理器
==============

负责输出文件的管理、组织和操作
"""

import os
import shutil
import pandas as pd
from typing import Dict, List, Optional, Any, Tuple
from .config import BBConfig
from .utils import BBUtils


class BBFileManager:
    """布林带文件管理器"""
    
    def __init__(self, config: BBConfig):
        """初始化文件管理器"""
        self.config = config
        self.utils = BBUtils()
        self.output_dir = config.default_output_dir
        
        # 确保输出目录存在
        self._ensure_output_directories()
    
    def _ensure_output_directories(self) -> None:
        """确保输出目录存在"""
        directories = [
            self.output_dir,
            os.path.join(self.output_dir, "3000万门槛"),
            os.path.join(self.output_dir, "5000万门槛")
        ]
        
        for directory in directories:
            self.utils.ensure_directory_exists(directory)
    
    def get_output_file_path(self, threshold: str, etf_code: str) -> str:
        """获取输出文件路径"""
        clean_etf_code = self.utils.format_etf_code(etf_code)
        return os.path.join(self.output_dir, threshold, f"{clean_etf_code}.csv")
    
    def save_etf_data(self, threshold: str, etf_code: str, data: pd.DataFrame) -> Dict[str, Any]:
        """
        保存ETF计算结果
        
        Args:
            threshold: 门槛名称
            etf_code: ETF代码
            data: 计算结果数据
            
        Returns:
            Dict[str, Any]: 保存结果
        """
        result = {
            'success': False,
            'file_path': '',
            'file_size_mb': 0,
            'row_count': 0,
            'error': None
        }
        
        if data.empty:
            result['error'] = '数据为空'
            return result
        
        try:
            output_file = self.get_output_file_path(threshold, etf_code)
            result['file_path'] = output_file
            
            # 确保目录存在
            output_dir = os.path.dirname(output_file)
            self.utils.ensure_directory_exists(output_dir)
            
            # 保存数据
            data.to_csv(output_file, index=False, encoding='utf-8', float_format='%.8f')
            
            # 获取文件信息
            result['file_size_mb'] = self.utils.get_file_size_mb(output_file) or 0
            result['row_count'] = len(data)
            result['success'] = True
            
        except Exception as e:
            result['error'] = str(e)
        
        return result
    
    def load_etf_data(self, threshold: str, etf_code: str) -> Optional[pd.DataFrame]:
        """
        加载ETF数据
        
        Args:
            threshold: 门槛名称
            etf_code: ETF代码
            
        Returns:
            Optional[pd.DataFrame]: 数据，如果不存在则返回None
        """
        output_file = self.get_output_file_path(threshold, etf_code)
        
        if not os.path.exists(output_file):
            return None
        
        try:
            return pd.read_csv(output_file, encoding='utf-8')
        except Exception:
            return None
    
    def delete_etf_data(self, threshold: str, etf_code: str) -> bool:
        """
        删除ETF数据文件
        
        Args:
            threshold: 门槛名称
            etf_code: ETF代码
            
        Returns:
            bool: 是否删除成功
        """
        output_file = self.get_output_file_path(threshold, etf_code)
        
        try:
            if os.path.exists(output_file):
                os.remove(output_file)
            return True
        except Exception:
            return False
    
    def batch_save_etf_data(self, threshold: str, etf_data_dict: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
        """
        批量保存ETF数据
        
        Args:
            threshold: 门槛名称
            etf_data_dict: ETF代码到数据的映射
            
        Returns:
            Dict[str, Any]: 批量保存结果
        """
        result = {
            'success': False,
            'total_files': len(etf_data_dict),
            'successful_saves': 0,
            'failed_saves': 0,
            'total_size_mb': 0,
            'total_rows': 0,
            'save_details': {},
            'errors': []
        }
        
        for etf_code, data in etf_data_dict.items():
            try:
                save_result = self.save_etf_data(threshold, etf_code, data)
                
                if save_result['success']:
                    result['successful_saves'] += 1
                    result['total_size_mb'] += save_result['file_size_mb']
                    result['total_rows'] += save_result['row_count']
                else:
                    result['failed_saves'] += 1
                    result['errors'].append(f"{etf_code}: {save_result['error']}")
                
                result['save_details'][etf_code] = save_result
                
            except Exception as e:
                result['failed_saves'] += 1
                result['errors'].append(f"{etf_code}: {str(e)}")
        
        result['success'] = result['failed_saves'] == 0
        result['total_size_mb'] = round(result['total_size_mb'], 2)
        
        return result
    
    def get_threshold_statistics(self, threshold: str) -> Dict[str, Any]:
        """
        获取门槛目录统计信息
        
        Args:
            threshold: 门槛名称
            
        Returns:
            Dict[str, Any]: 统计信息
        """
        stats = {
            'threshold': threshold,
            'directory_path': os.path.join(self.output_dir, threshold),
            'directory_exists': False,
            'file_count': 0,
            'total_size_mb': 0,
            'files': []
        }
        
        threshold_dir = os.path.join(self.output_dir, threshold)
        stats['directory_exists'] = os.path.exists(threshold_dir)
        
        if not stats['directory_exists']:
            return stats
        
        try:
            csv_files = [f for f in os.listdir(threshold_dir) if f.endswith('.csv')]
            stats['file_count'] = len(csv_files)
            
            total_size = 0
            for filename in csv_files:
                file_path = os.path.join(threshold_dir, filename)
                file_size = self.utils.get_file_size_mb(file_path)
                
                if file_size:
                    total_size += file_size
                
                file_info = {
                    'filename': filename,
                    'etf_code': filename.replace('.csv', ''),
                    'size_mb': file_size,
                    'modification_time': self.utils.get_file_modification_time(file_path)
                }
                
                # 尝试获取数据行数
                try:
                    df = pd.read_csv(file_path)
                    file_info['row_count'] = len(df)
                except Exception:
                    file_info['row_count'] = None
                
                stats['files'].append(file_info)
            
            stats['total_size_mb'] = round(total_size, 2)
            
        except Exception:
            pass
        
        return stats
    
    def get_all_statistics(self) -> Dict[str, Any]:
        """获取所有输出目录的统计信息"""
        all_stats = {
            'output_directory': self.output_dir,
            'thresholds': {},
            'summary': {
                'total_thresholds': 0,
                'total_files': 0,
                'total_size_mb': 0
            }
        }
        
        thresholds = ["3000万门槛", "5000万门槛"]
        
        for threshold in thresholds:
            threshold_stats = self.get_threshold_statistics(threshold)
            all_stats['thresholds'][threshold] = threshold_stats
            
            if threshold_stats['directory_exists']:
                all_stats['summary']['total_thresholds'] += 1
                all_stats['summary']['total_files'] += threshold_stats['file_count']
                all_stats['summary']['total_size_mb'] += threshold_stats['total_size_mb']
        
        all_stats['summary']['total_size_mb'] = round(all_stats['summary']['total_size_mb'], 2)
        
        return all_stats
    
    def backup_threshold_data(self, threshold: str, backup_dir: str) -> Dict[str, Any]:
        """
        备份门槛数据
        
        Args:
            threshold: 门槛名称
            backup_dir: 备份目录
            
        Returns:
            Dict[str, Any]: 备份结果
        """
        result = {
            'success': False,
            'source_dir': os.path.join(self.output_dir, threshold),
            'backup_dir': backup_dir,
            'copied_files': 0,
            'total_size_mb': 0,
            'error': None
        }
        
        source_dir = result['source_dir']
        
        if not os.path.exists(source_dir):
            result['error'] = '源目录不存在'
            return result
        
        try:
            # 创建备份目录
            self.utils.ensure_directory_exists(backup_dir)
            
            # 复制文件
            csv_files = [f for f in os.listdir(source_dir) if f.endswith('.csv')]
            
            total_size = 0
            for filename in csv_files:
                source_file = os.path.join(source_dir, filename)
                backup_file = os.path.join(backup_dir, filename)
                
                shutil.copy2(source_file, backup_file)
                
                file_size = self.utils.get_file_size_mb(backup_file)
                if file_size:
                    total_size += file_size
                
                result['copied_files'] += 1
            
            result['total_size_mb'] = round(total_size, 2)
            result['success'] = True
            
        except Exception as e:
            result['error'] = str(e)
        
        return result
    
    def clean_threshold_data(self, threshold: str) -> Dict[str, Any]:
        """
        清理门槛数据
        
        Args:
            threshold: 门槛名称
            
        Returns:
            Dict[str, Any]: 清理结果
        """
        result = {
            'success': False,
            'threshold': threshold,
            'directory': os.path.join(self.output_dir, threshold),
            'deleted_files': 0,
            'errors': []
        }
        
        threshold_dir = result['directory']
        
        if not os.path.exists(threshold_dir):
            result['success'] = True
            return result
        
        try:
            csv_files = [f for f in os.listdir(threshold_dir) if f.endswith('.csv')]
            
            for filename in csv_files:
                file_path = os.path.join(threshold_dir, filename)
                try:
                    os.remove(file_path)
                    result['deleted_files'] += 1
                except Exception as e:
                    result['errors'].append(f"删除{filename}失败: {str(e)}")
            
            result['success'] = len(result['errors']) == 0
            
        except Exception as e:
            result['errors'].append(f"清理目录异常: {str(e)}")
        
        return result
    
    def verify_data_integrity(self, threshold: str) -> Dict[str, Any]:
        """
        验证数据完整性
        
        Args:
            threshold: 门槛名称
            
        Returns:
            Dict[str, Any]: 验证结果
        """
        result = {
            'success': False,
            'threshold': threshold,
            'total_files': 0,
            'valid_files': 0,
            'invalid_files': 0,
            'file_details': {},
            'errors': []
        }
        
        threshold_dir = os.path.join(self.output_dir, threshold)
        
        if not os.path.exists(threshold_dir):
            result['errors'].append('目录不存在')
            return result
        
        try:
            csv_files = [f for f in os.listdir(threshold_dir) if f.endswith('.csv')]
            result['total_files'] = len(csv_files)
            
            for filename in csv_files:
                file_path = os.path.join(threshold_dir, filename)
                etf_code = filename.replace('.csv', '')
                
                file_detail = {
                    'filename': filename,
                    'etf_code': etf_code,
                    'is_valid': False,
                    'row_count': 0,
                    'column_count': 0,
                    'required_columns_present': False,
                    'error': None
                }
                
                try:
                    # 读取文件
                    df = pd.read_csv(file_path)
                    file_detail['row_count'] = len(df)
                    file_detail['column_count'] = len(df.columns)
                    
                    # 检查必需列
                    required_columns = self.config.get_bb_output_fields()
                    missing_columns = [col for col in required_columns if col not in df.columns]
                    
                    if not missing_columns:
                        file_detail['required_columns_present'] = True
                        file_detail['is_valid'] = True
                        result['valid_files'] += 1
                    else:
                        file_detail['error'] = f"缺少列: {missing_columns}"
                        result['invalid_files'] += 1
                    
                except Exception as e:
                    file_detail['error'] = str(e)
                    result['invalid_files'] += 1
                
                result['file_details'][etf_code] = file_detail
            
            result['success'] = result['invalid_files'] == 0
            
        except Exception as e:
            result['errors'].append(f"验证过程异常: {str(e)}")
        
        return result
    
    def optimize_storage(self, threshold: str) -> Dict[str, Any]:
        """
        优化存储（压缩重复数据等）
        
        Args:
            threshold: 门槛名称
            
        Returns:
            Dict[str, Any]: 优化结果
        """
        result = {
            'success': False,
            'threshold': threshold,
            'original_size_mb': 0,
            'optimized_size_mb': 0,
            'space_saved_mb': 0,
            'space_saved_percent': 0,
            'optimized_files': 0,
            'errors': []
        }
        
        threshold_dir = os.path.join(self.output_dir, threshold)
        
        if not os.path.exists(threshold_dir):
            result['success'] = True
            return result
        
        try:
            csv_files = [f for f in os.listdir(threshold_dir) if f.endswith('.csv')]
            original_total_size = 0
            optimized_total_size = 0
            
            for filename in csv_files:
                file_path = os.path.join(threshold_dir, filename)
                
                try:
                    # 记录原始大小
                    original_size = self.utils.get_file_size_mb(file_path) or 0
                    original_total_size += original_size
                    
                    # 读取并优化数据
                    df = pd.read_csv(file_path)
                    
                    # 优化：去除重复行、优化数据类型等
                    optimized_df = self._optimize_dataframe(df)
                    
                    # 保存优化后的数据
                    optimized_df.to_csv(file_path, index=False, encoding='utf-8')
                    
                    # 记录优化后大小
                    optimized_size = self.utils.get_file_size_mb(file_path) or 0
                    optimized_total_size += optimized_size
                    
                    result['optimized_files'] += 1
                    
                except Exception as e:
                    result['errors'].append(f"优化{filename}失败: {str(e)}")
            
            result['original_size_mb'] = round(original_total_size, 2)
            result['optimized_size_mb'] = round(optimized_total_size, 2)
            result['space_saved_mb'] = round(original_total_size - optimized_total_size, 2)
            
            if original_total_size > 0:
                result['space_saved_percent'] = round(
                    (result['space_saved_mb'] / original_total_size) * 100, 2
                )
            
            result['success'] = len(result['errors']) == 0
            
        except Exception as e:
            result['errors'].append(f"存储优化异常: {str(e)}")
        
        return result
    
    def _optimize_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """优化DataFrame"""
        # 去除重复行
        optimized_df = df.drop_duplicates()
        
        # 优化数据类型（可选）
        # 例如：将float64转换为float32以节省空间
        for col in optimized_df.columns:
            if optimized_df[col].dtype == 'float64':
                # 检查是否可以安全转换为float32
                try:
                    optimized_df[col] = pd.to_numeric(optimized_df[col], downcast='float')
                except Exception:
                    pass
        
        return optimized_df