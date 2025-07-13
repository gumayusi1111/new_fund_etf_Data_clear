#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
布林带文件管理器
==============

负责文件的读写、路径管理和数据格式转换
参照趋势类指标的文件管理模式
"""

import os
import pandas as pd
from typing import Dict, List, Optional, Any
from datetime import datetime
from ..infrastructure.utils import BBUtils


class BBFileManager:
    """布林带文件管理器"""
    
    def __init__(self, config):
        """初始化文件管理器"""
        self.config = config
        self.utils = BBUtils()
        
        # 确保目录存在
        self.config.ensure_directories_exist()
    
    def save_bb_results(self, results: Dict[str, Any], threshold: str, 
                       file_format: str = 'csv') -> bool:
        """
        保存布林带计算结果
        
        Args:
            results: 计算结果字典
            threshold: 门槛类型
            file_format: 文件格式 ('csv', 'json', 'excel')
            
        Returns:
            bool: 保存是否成功
        """
        try:
            if not results or not results.get('success'):
                return False
            
            etf_code = results.get('etf_code', '')
            bb_results = results.get('bb_results', {})
            formatted_data = results.get('formatted_data', {})
            
            if not formatted_data:
                return False
            
            # 构建输出文件路径
            output_dir = os.path.join(self.config.default_output_dir, threshold)
            self.utils.ensure_directory_exists(output_dir)
            
            clean_etf_code = self.utils.format_etf_code(etf_code)
            
            if file_format.lower() == 'csv':
                return self._save_as_csv(formatted_data, output_dir, clean_etf_code)
            elif file_format.lower() == 'json':
                return self._save_as_json(results, output_dir, clean_etf_code)
            elif file_format.lower() == 'excel':
                return self._save_as_excel(formatted_data, output_dir, clean_etf_code)
            else:
                return False
                
        except Exception:
            return False
    
    def _save_as_csv(self, formatted_data: Dict, output_dir: str, etf_code: str) -> bool:
        """保存为CSV格式"""
        try:
            # 转换为DataFrame
            df = pd.DataFrame([formatted_data])
            
            # 设置列顺序
            column_order = self.config.get_bb_output_fields()
            df = df.reindex(columns=column_order)
            
            # 保存文件
            output_file = os.path.join(output_dir, f"{etf_code}.csv")
            df.to_csv(output_file, index=False, encoding='utf-8')
            
            return True
            
        except Exception:
            return False
    
    def _save_as_json(self, results: Dict, output_dir: str, etf_code: str) -> bool:
        """保存为JSON格式"""
        try:
            import json
            
            output_file = os.path.join(output_dir, f"{etf_code}.json")
            
            # 构建JSON数据
            json_data = {
                'etf_code': etf_code,
                'calculation_timestamp': self.utils.get_current_timestamp(),
                'bb_config': {
                    'period': self.config.get_bb_period(),
                    'std_multiplier': self.config.get_bb_std_multiplier(),
                    'adj_type': self.config.adj_type
                },
                'results': results.get('formatted_data', {}),
                'validation': results.get('validation_result', {}),
                'processing_time': results.get('processing_time', 0)
            }
            
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(json_data, f, ensure_ascii=False, indent=2)
            
            return True
            
        except Exception:
            return False
    
    def _save_as_excel(self, formatted_data: Dict, output_dir: str, etf_code: str) -> bool:
        """保存为Excel格式"""
        try:
            # 转换为DataFrame
            df = pd.DataFrame([formatted_data])
            
            # 设置列顺序
            column_order = self.config.get_bb_output_fields()
            df = df.reindex(columns=column_order)
            
            # 保存文件
            output_file = os.path.join(output_dir, f"{etf_code}.xlsx")
            df.to_excel(output_file, index=False, engine='openpyxl')
            
            return True
            
        except Exception:
            return False
    
    def batch_save_results(self, batch_results: Dict[str, Any], threshold: str) -> Dict[str, Any]:
        """
        批量保存结果
        
        Args:
            batch_results: 批量计算结果
            threshold: 门槛类型
            
        Returns:
            Dict[str, Any]: 保存统计信息
        """
        save_stats = {
            'total_etfs': 0,
            'successful_saves': 0,
            'failed_saves': 0,
            'save_errors': []
        }
        
        try:
            successful_results = batch_results.get('threshold_details', {}).get(threshold, {}).get('results', {})
            
            save_stats['total_etfs'] = len(successful_results)
            
            for etf_code, result in successful_results.items():
                try:
                    if self.save_bb_results(result, threshold):
                        save_stats['successful_saves'] += 1
                    else:
                        save_stats['failed_saves'] += 1
                        save_stats['save_errors'].append(f"{etf_code}: 保存失败")
                        
                except Exception as e:
                    save_stats['failed_saves'] += 1
                    save_stats['save_errors'].append(f"{etf_code}: {str(e)}")
            
            return save_stats
            
        except Exception as e:
            save_stats['save_errors'].append(f"批量保存异常: {str(e)}")
            return save_stats
    
    def create_summary_report(self, batch_results: Dict[str, Any], output_dir: str) -> bool:
        """创建汇总报告"""
        try:
            summary_data = {
                'generation_time': self.utils.get_current_timestamp(),
                'bb_config': self.config.to_dict(),
                'processing_summary': {
                    'total_etfs_processed': batch_results.get('total_etfs_processed', 0),
                    'successful_etfs': batch_results.get('successful_etfs', 0),
                    'failed_etfs': batch_results.get('failed_etfs', 0),
                    'processing_time_seconds': batch_results.get('processing_time_seconds', 0)
                },
                'threshold_details': {}
            }
            
            # 添加门槛详情
            for threshold, details in batch_results.get('threshold_details', {}).items():
                summary_data['threshold_details'][threshold] = {
                    'etf_count': len(details.get('etf_list', [])),
                    'successful_calculations': details.get('successful_etfs', 0),
                    'failed_calculations': details.get('failed_etfs', 0),
                    'success_rate': (details.get('successful_etfs', 0) / 
                                   max(len(details.get('etf_list', [])), 1)) * 100
                }
            
            # 保存为JSON报告
            import json
            report_file = os.path.join(output_dir, f"bb_summary_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
            
            with open(report_file, 'w', encoding='utf-8') as f:
                json.dump(summary_data, f, ensure_ascii=False, indent=2)
            
            return True
            
        except Exception:
            return False
    
    def read_existing_results(self, etf_code: str, threshold: str) -> Optional[Dict]:
        """读取已存在的计算结果"""
        try:
            clean_etf_code = self.utils.format_etf_code(etf_code)
            result_file = os.path.join(self.config.default_output_dir, threshold, f"{clean_etf_code}.csv")
            
            if not os.path.exists(result_file):
                return None
            
            df = pd.read_csv(result_file, encoding='utf-8')
            
            if df.empty:
                return None
            
            # 转换为字典格式
            result_dict = df.iloc[0].to_dict()
            
            return result_dict
            
        except Exception:
            return None
    
    def get_output_file_info(self, threshold: str) -> Dict[str, Any]:
        """获取输出文件信息"""
        info = {
            'threshold': threshold,
            'output_directory': os.path.join(self.config.default_output_dir, threshold),
            'file_count': 0,
            'total_size_mb': 0,
            'file_list': []
        }
        
        try:
            output_dir = os.path.join(self.config.default_output_dir, threshold)
            
            if not os.path.exists(output_dir):
                return info
            
            total_size = 0
            file_list = []
            
            for file_name in os.listdir(output_dir):
                if file_name.endswith(('.csv', '.json', '.xlsx')):
                    file_path = os.path.join(output_dir, file_name)
                    file_size = os.path.getsize(file_path)
                    total_size += file_size
                    
                    file_list.append({
                        'name': file_name,
                        'size_bytes': file_size,
                        'modification_time': datetime.fromtimestamp(
                            os.path.getmtime(file_path)
                        ).isoformat()
                    })
            
            info['file_count'] = len(file_list)
            info['total_size_mb'] = round(total_size / (1024 * 1024), 2)
            info['file_list'] = sorted(file_list, key=lambda x: x['modification_time'], reverse=True)
            
            return info
            
        except Exception:
            return info
    
    def cleanup_old_results(self, threshold: str, days_old: int = 7) -> int:
        """清理旧的结果文件"""
        cleaned_count = 0
        
        try:
            output_dir = os.path.join(self.config.default_output_dir, threshold)
            
            if not os.path.exists(output_dir):
                return 0
            
            cutoff_time = datetime.now().timestamp() - (days_old * 24 * 60 * 60)
            
            for file_name in os.listdir(output_dir):
                file_path = os.path.join(output_dir, file_name)
                
                if os.path.getmtime(file_path) < cutoff_time:
                    os.remove(file_path)
                    cleaned_count += 1
            
            return cleaned_count
            
        except Exception:
            return 0