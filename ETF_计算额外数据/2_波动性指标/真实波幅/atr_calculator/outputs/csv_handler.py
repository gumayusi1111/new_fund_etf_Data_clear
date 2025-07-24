#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ATR CSV输出处理模块
==================

负责将ATR计算结果保存为CSV格式，支持：
- 标准化CSV格式输出
- 门槛分类存储
- 文件命名规范
- 批量保存操作
- 数据完整性验证

输出格式：
- data/3000万门槛/ETF代码.csv
- data/5000万门槛/ETF代码.csv
"""

import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
import logging
from datetime import datetime


class ATRCSVHandler:
    """ATR CSV处理器"""
    
    def __init__(self, config=None):
        """初始化CSV处理器"""
        if config:
            self.data_dir = config.data_dir
            self.cache_dir = config.cache_dir
            self.thresholds = config.thresholds
            self.output_fields = config.output_fields
            self.precision = config.precision
        else:
            # 默认配置
            base_dir = Path(__file__).parent.parent.parent
            self.data_dir = base_dir / "data"
            self.cache_dir = base_dir / "cache"
            self.thresholds = ["3000万门槛", "5000万门槛"]
            self.output_fields = [
                'tr', 'atr_10', 'atr_percent', 'atr_change_rate',
                'atr_ratio_hl', 'stop_loss', 'volatility_level'
            ]
            self.precision = {
                'tr': 4, 'atr_10': 4, 'atr_percent': 2,
                'atr_change_rate': 2, 'atr_ratio_hl': 2, 'stop_loss': 2
            }
        
        # 设置日志
        self.logger = logging.getLogger(__name__)
        
        # 确保输出目录存在
        self._ensure_output_directories()
    
    def _ensure_output_directories(self):
        """确保输出目录存在"""
        try:
            self.data_dir.mkdir(parents=True, exist_ok=True)
            
            for threshold in self.thresholds:
                threshold_dir = self.data_dir / threshold
                threshold_dir.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            self.logger.error(f"创建输出目录失败: {e}")
    
    def _format_output_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """格式化输出数据 - 只输出ATR计算字段"""
        try:
            # 只保留日期和ATR计算字段
            required_columns = ['日期'] + self.output_fields
            
            # 检查所有必需列是否存在
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                self.logger.warning(f"缺少必需的ATR计算列: {missing_columns}")
                # 为缺失的列添加NaN值
                for col in missing_columns:
                    if col != '日期':
                        df[col] = np.nan
            
            # 只选择需要的列
            output_df = df[required_columns].copy()
            
            # 应用数值精度
            for field, precision in self.precision.items():
                if field in output_df.columns and precision is not None:
                    if pd.api.types.is_numeric_dtype(output_df[field]):
                        output_df[field] = output_df[field].round(precision)
            
            # 格式化日期列并排序
            if '日期' in output_df.columns:
                try:
                    # 先按日期降序排序（最新日期在最上面）
                    output_df['日期'] = pd.to_datetime(output_df['日期'])
                    output_df = output_df.sort_values('日期', ascending=False)
                    # 再格式化为字符串
                    output_df['日期'] = output_df['日期'].dt.strftime('%Y-%m-%d')
                except Exception as e:
                    self.logger.warning(f"日期格式化失败: {e}")
            
            return output_df
            
        except Exception as e:
            self.logger.error(f"格式化输出数据失败: {e}")
            return df
    
    def _generate_output_filename(self, etf_code: str) -> str:
        """生成输出文件名"""
        # 清理ETF代码，移除特殊字符
        clean_code = etf_code.replace('.', '').replace('-', '').replace(' ', '')
        return f"{clean_code}.csv"
    
    def save_single_etf(self, etf_code: str, threshold: str, 
                       data: pd.DataFrame) -> Tuple[bool, Dict[str, Any]]:
        """保存单个ETF的ATR计算结果"""
        try:
            # 获取输出路径
            threshold_dir = self.data_dir / threshold
            output_filename = self._generate_output_filename(etf_code)
            output_path = threshold_dir / output_filename
            
            # 格式化数据
            formatted_data = self._format_output_data(data)
            
            # 数据验证
            if formatted_data.empty:
                return False, {
                    'error': '数据为空',
                    'etf_code': etf_code,
                    'threshold': threshold
                }
            
            # 检查ATR字段完整性
            atr_fields_present = [field for field in self.output_fields 
                                if field in formatted_data.columns]
            if len(atr_fields_present) == 0:
                return False, {
                    'error': '没有ATR计算字段',
                    'etf_code': etf_code,
                    'threshold': threshold
                }
            
            # 保存CSV文件
            formatted_data.to_csv(
                output_path,
                index=False,
                encoding='utf-8',  # 避免UTF-8 BOM问题
                float_format='%.8f'
            )
            
            # 计算统计信息
            file_size = output_path.stat().st_size
            valid_atr_records = formatted_data['atr_10'].notna().sum() if 'atr_10' in formatted_data.columns else 0
            
            result_info = {
                'success': True,
                'etf_code': etf_code,
                'threshold': threshold,
                'output_path': str(output_path),
                'file_size_bytes': file_size,
                'file_size_kb': file_size / 1024,
                'total_records': len(formatted_data),
                'valid_atr_records': valid_atr_records,
                'atr_coverage': valid_atr_records / len(formatted_data) * 100 if len(formatted_data) > 0 else 0,
                'columns_saved': list(formatted_data.columns),
                'atr_fields_saved': atr_fields_present,
                'save_time': datetime.now().isoformat()
            }
            
            self.logger.debug(f"保存成功: {etf_code} -> {output_path}")
            return True, result_info
            
        except Exception as e:
            error_info = {
                'success': False,
                'error': str(e),
                'etf_code': etf_code,
                'threshold': threshold,
                'save_time': datetime.now().isoformat()
            }
            self.logger.error(f"保存ETF数据失败 {etf_code}-{threshold}: {e}")
            return False, error_info
    
    def save_batch_results(self, batch_results: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
        """批量保存ATR计算结果"""
        try:
            save_statistics = {
                'total_etfs': 0,
                'successful_saves': 0,
                'failed_saves': 0,
                'thresholds': {},
                'save_details': [],
                'total_file_size_kb': 0,
                'save_time': datetime.now().isoformat()
            }
            
            # 按门槛组织数据
            for threshold in self.thresholds:
                save_statistics['thresholds'][threshold] = {
                    'total_etfs': 0,
                    'successful_saves': 0,
                    'failed_saves': 0,
                    'total_file_size_kb': 0,
                    'etf_details': []
                }
            
            # 处理每个ETF的结果
            for etf_code, etf_results in batch_results.items():
                save_statistics['total_etfs'] += 1
                
                for threshold in self.thresholds:
                    if threshold in etf_results:
                        threshold_result = etf_results[threshold]
                        save_statistics['thresholds'][threshold]['total_etfs'] += 1
                        
                        if threshold_result.get('success', False) and 'data' in threshold_result:
                            # 保存数据
                            success, save_info = self.save_single_etf(
                                etf_code, threshold, threshold_result['data']
                            )
                            
                            if success:
                                save_statistics['successful_saves'] += 1
                                save_statistics['thresholds'][threshold]['successful_saves'] += 1
                                save_statistics['thresholds'][threshold]['total_file_size_kb'] += save_info['file_size_kb']
                                save_statistics['total_file_size_kb'] += save_info['file_size_kb']
                                
                                save_statistics['thresholds'][threshold]['etf_details'].append({
                                    'etf_code': etf_code,
                                    'success': True,
                                    'file_size_kb': save_info['file_size_kb'],
                                    'records': save_info['total_records'],
                                    'atr_coverage': save_info['atr_coverage']
                                })
                            else:
                                save_statistics['failed_saves'] += 1
                                save_statistics['thresholds'][threshold]['failed_saves'] += 1
                                
                                save_statistics['thresholds'][threshold]['etf_details'].append({
                                    'etf_code': etf_code,
                                    'success': False,
                                    'error': save_info.get('error', '未知错误')
                                })
                        else:
                            save_statistics['failed_saves'] += 1
                            save_statistics['thresholds'][threshold]['failed_saves'] += 1
                            
                            save_statistics['thresholds'][threshold]['etf_details'].append({
                                'etf_code': etf_code,
                                'success': False,
                                'error': threshold_result.get('error', '计算失败')
                            })
            
            # 计算成功率
            for threshold in self.thresholds:
                threshold_stats = save_statistics['thresholds'][threshold]
                if threshold_stats['total_etfs'] > 0:
                    threshold_stats['success_rate'] = (
                        threshold_stats['successful_saves'] / threshold_stats['total_etfs'] * 100
                    )
                else:
                    threshold_stats['success_rate'] = 0
            
            if save_statistics['total_etfs'] > 0:
                save_statistics['overall_success_rate'] = (
                    save_statistics['successful_saves'] / save_statistics['total_etfs'] * 100
                )
            else:
                save_statistics['overall_success_rate'] = 0
            
            self.logger.info(f"批量保存完成: {save_statistics['successful_saves']}/{save_statistics['total_etfs']} ETFs")
            return save_statistics
            
        except Exception as e:
            self.logger.error(f"批量保存失败: {e}")
            return {
                'error': str(e),
                'total_etfs': 0,
                'successful_saves': 0,
                'failed_saves': 0,
                'save_time': datetime.now().isoformat()
            }
    
    def create_summary_report(self, save_statistics: Dict[str, Any], 
                            output_filename: str = "atr_calculation_summary.csv") -> bool:
        """创建汇总报告"""
        try:
            summary_data = []
            
            for threshold, stats in save_statistics.get('thresholds', {}).items():
                for etf_detail in stats.get('etf_details', []):
                    summary_row = {
                        '门槛类型': threshold,
                        'ETF代码': etf_detail['etf_code'],
                        '保存状态': '成功' if etf_detail['success'] else '失败',
                        '文件大小(KB)': etf_detail.get('file_size_kb', 0),
                        '数据记录数': etf_detail.get('records', 0),
                        'ATR覆盖率(%)': etf_detail.get('atr_coverage', 0),
                        '错误信息': etf_detail.get('error', '')
                    }
                    summary_data.append(summary_row)
            
            if summary_data:
                summary_df = pd.DataFrame(summary_data)
                summary_path = self.data_dir / output_filename
                
                summary_df.to_csv(summary_path, index=False, encoding='utf-8', float_format='%.8f')
                self.logger.info(f"汇总报告已保存: {summary_path}")
                return True
            else:
                self.logger.warning("没有数据生成汇总报告")
                return False
                
        except Exception as e:
            self.logger.error(f"创建汇总报告失败: {e}")
            return False
    
    def get_output_file_info(self, etf_code: str, threshold: str) -> Dict[str, Any]:
        """获取输出文件信息"""
        try:
            threshold_dir = self.data_dir / threshold
            output_filename = self._generate_output_filename(etf_code)
            output_path = threshold_dir / output_filename
            
            if output_path.exists():
                file_stat = output_path.stat()
                
                # 尝试读取文件获取更多信息
                try:
                    df = pd.read_csv(output_path)
                    data_info = {
                        'total_records': len(df),
                        'columns': list(df.columns),
                        'atr_fields': [col for col in self.output_fields if col in df.columns],
                        'date_range': {
                            'start': str(df['日期'].min()) if '日期' in df.columns else None,
                            'end': str(df['日期'].max()) if '日期' in df.columns else None
                        } if '日期' in df.columns else None
                    }
                except Exception:
                    data_info = {'total_records': 0, 'columns': [], 'atr_fields': []}
                
                return {
                    'exists': True,
                    'file_path': str(output_path),
                    'file_size_bytes': file_stat.st_size,
                    'file_size_kb': file_stat.st_size / 1024,
                    'modification_time': datetime.fromtimestamp(file_stat.st_mtime).isoformat(),
                    **data_info
                }
            else:
                return {
                    'exists': False,
                    'file_path': str(output_path),
                    'total_records': 0
                }
                
        except Exception as e:
            return {
                'exists': False,
                'error': str(e),
                'total_records': 0
            }