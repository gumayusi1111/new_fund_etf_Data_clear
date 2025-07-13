#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
布林带主控制器
============

协调各个组件，提供统一的对外接口
参照趋势类指标的控制器模式
"""

import time
import pandas as pd
from typing import Dict, List, Optional, Any
from ..infrastructure.config import BBConfig
from ..infrastructure.data_reader import BBDataReader
from ..infrastructure.utils import BBUtils
from ..infrastructure.cache_manager import BBCacheManager
from ..engines.bb_engine import BollingerBandsEngine
from ..outputs.csv_handler import BBCSVHandler


class BBMainController:
    """布林带主控制器"""
    
    def __init__(self, adj_type: str = "前复权"):
        """初始化主控制器"""
        self.config = BBConfig(adj_type=adj_type)
        self.data_reader = BBDataReader(self.config)
        self.utils = BBUtils()
        self.bb_engine = BollingerBandsEngine(self.config)
        self.cache_manager = BBCacheManager(self.config)
        self.csv_handler = BBCSVHandler(self.config)
        
        # 确保目录存在
        self.config.ensure_directories_exist()
    
    def update_config(self, new_config):
        """更新配置并重新初始化相关组件"""
        self.config = new_config
        self.data_reader = BBDataReader(self.config)
        self.bb_engine = BollingerBandsEngine(self.config)  # 重新创建引擎
        self.cache_manager = BBCacheManager(self.config)
        self.csv_handler = BBCSVHandler(self.config)
    
    def get_system_status(self) -> Dict[str, Any]:
        """获取系统状态"""
        status = {
            'system_name': 'BollingerBands Calculator',
            'version': '1.0.0',
            'timestamp': self.utils.get_current_timestamp(),
            'config': {
                'adj_type': self.config.adj_type,
                'bb_period': self.config.get_bb_period(),
                'std_multiplier': self.config.get_bb_std_multiplier(),
                'precision': self.config.get_precision()
            },
            'paths': {
                'data_dir': self.config.data_dir,
                'cache_dir': self.config.cache_dir,
                'output_dir': self.config.default_output_dir,
                'data_dir_exists': self.config.validate_data_path()
            },
            'algorithm': {
                'name': 'John Bollinger Standard Algorithm',
                'year': '1983',
                'parameters': {
                    'period': self.config.get_bb_period(),
                    'std_multiplier': self.config.get_bb_std_multiplier(),
                    'precision': self.config.get_precision()
                }
            }
        }
        
        return status
    
    def process_single_etf(self, etf_code: str, save_output: bool = True) -> Dict[str, Any]:
        """
        处理单个ETF的布林带计算
        
        Args:
            etf_code: ETF代码
            save_output: 是否保存输出文件
            
        Returns:
            Dict[str, Any]: 处理结果
        """
        result = {
            'success': False,
            'etf_code': etf_code,
            'bb_results': {},
            'processing_time': 0,
            'validation_result': {},
            'error': None
        }
        
        start_time = time.time()
        
        try:
            # 先检查缓存
            cached_result = self.cache_manager.get_cached_result(etf_code)
            if cached_result and cached_result.get('success'):
                result.update(cached_result)
                result['processing_time'] = time.time() - start_time
                result['from_cache'] = True
                return result
            
            # 读取ETF数据
            etf_data = self.data_reader.read_etf_data(etf_code)
            if etf_data is None or etf_data.empty:
                result['error'] = f'无法读取ETF数据: {etf_code}'
                return result
            
            # 计算布林带指标
            bb_results = self.bb_engine.calculate_bollinger_bands(etf_data)
            
            if not bb_results or all(v is None for v in bb_results.values()):
                result['error'] = f'布林带计算失败: {etf_code}'
                return result
            
            # 验证计算结果
            validation_result = self.bb_engine.verify_calculation(etf_data, bb_results)
            result['validation_result'] = validation_result[1]
            
            if not validation_result[0]:
                result['error'] = f'计算验证失败: {validation_result[1].get("error", "未知错误")}'
                return result
            
            # 生成完整历史数据用于缓存和输出
            full_history_bb = self.bb_engine.calculate_full_history(etf_data)
            if full_history_bb is not None:
                # 格式化完整历史数据
                formatted_rows = []
                for idx, row in etf_data.iterrows():
                    if idx < len(full_history_bb['bb_middle']):
                        date_str = row['日期'].strftime('%Y-%m-%d') if pd.notna(row['日期']) else ''
                        formatted_rows.append({
                            'date': date_str,
                            'code': etf_code,
                            'bb_middle': full_history_bb['bb_middle'].iloc[idx] if not pd.isna(full_history_bb['bb_middle'].iloc[idx]) else '',
                            'bb_upper': full_history_bb['bb_upper'].iloc[idx] if not pd.isna(full_history_bb['bb_upper'].iloc[idx]) else '',
                            'bb_lower': full_history_bb['bb_lower'].iloc[idx] if not pd.isna(full_history_bb['bb_lower'].iloc[idx]) else '',
                            'bb_width': full_history_bb['bb_width'].iloc[idx] if not pd.isna(full_history_bb['bb_width'].iloc[idx]) else '',
                            'bb_position': full_history_bb['bb_position'].iloc[idx] if not pd.isna(full_history_bb['bb_position'].iloc[idx]) else '',
                            'bb_percent_b': full_history_bb['bb_percent_b'].iloc[idx] if not pd.isna(full_history_bb['bb_percent_b'].iloc[idx]) else ''
                        })
                
                # 按日期倒序排列（最新日期在前）
                full_history_df = pd.DataFrame(formatted_rows)
                if not full_history_df.empty and 'date' in full_history_df.columns:
                    full_history_df['date_sort'] = pd.to_datetime(full_history_df['date'], errors='coerce')
                    full_history_df = full_history_df.sort_values('date_sort', ascending=False).drop('date_sort', axis=1)
                
                result['full_history_data'] = full_history_df
            
            # 格式化输出数据
            if save_output:
                formatted_data = self._format_bb_result(etf_code, etf_data, bb_results)
                if formatted_data:
                    result['formatted_data'] = formatted_data
            
            result['success'] = True
            result['bb_results'] = bb_results
            result['processing_time'] = time.time() - start_time
            result['from_cache'] = False
            
            # 单个ETF处理不保存到缓存，避免根目录混乱
            # 缓存只在批量处理时保存到门槛目录
            
        except Exception as e:
            result['error'] = str(e)
        
        return result
    
    def _format_bb_result(self, etf_code: str, raw_data, bb_results: Dict) -> Optional[Dict]:
        """格式化布林带结果"""
        try:
            # 格式化ETF代码
            clean_etf_code = self.utils.format_etf_code(etf_code)
            
            # 获取最新日期
            latest_date = raw_data['日期'].max()
            if pd.isna(latest_date):
                return None
            
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
            
            return result_row
            
        except Exception:
            return None
    
    def calculate_screening_results(self, thresholds: List[str]) -> Dict[str, Any]:
        """
        基于ETF初筛结果进行批量计算
        
        Args:
            thresholds: 门槛列表
            
        Returns:
            Dict[str, Any]: 批量处理结果
        """
        result = {
            'success': False,
            'total_etfs_processed': 0,
            'successful_etfs': 0,
            'failed_etfs': 0,
            'thresholds_processed': len(thresholds),
            'processing_time_seconds': 0,
            'threshold_details': {},
            'errors': []
        }
        
        start_time = time.time()
        
        try:
            total_successful = 0
            total_failed = 0
            
            for threshold in thresholds:
                threshold_result = self._process_threshold(threshold)
                result['threshold_details'][threshold] = threshold_result
                
                if threshold_result['success']:
                    total_successful += threshold_result['successful_etfs']
                    total_failed += threshold_result['failed_etfs']
                else:
                    result['errors'].append(f"{threshold}: {threshold_result.get('error', '未知错误')}")
            
            result['total_etfs_processed'] = total_successful + total_failed
            result['successful_etfs'] = total_successful
            result['failed_etfs'] = total_failed
            result['processing_time_seconds'] = time.time() - start_time
            
            result['success'] = len(result['errors']) == 0
            
        except Exception as e:
            result['error'] = str(e)
        
        return result
    
    def _process_threshold(self, threshold: str) -> Dict[str, Any]:
        """处理单个门槛的ETF列表"""
        threshold_result = {
            'success': False,
            'threshold': threshold,
            'etf_list': [],
            'successful_etfs': 0,
            'failed_etfs': 0,
            'results': {},
            'error': None
        }
        
        try:
            # 读取ETF筛选列表
            etf_list = self.utils.read_etf_screening_list(threshold)
            if not etf_list:
                threshold_result['error'] = f'无法读取{threshold}的ETF筛选列表'
                return threshold_result
            
            threshold_result['etf_list'] = etf_list
            
            # 批量处理ETF
            successful_results = {}
            failed_etfs = []
            
            for etf_code in etf_list:
                try:
                    process_result = self.process_single_etf(etf_code, save_output=True)
                    
                    if process_result['success']:
                        successful_results[etf_code] = process_result
                        threshold_result['successful_etfs'] += 1
                        
                        # 保存到门槛缓存目录（不保存到根目录）
                        try:
                            clean_etf_code = self.utils.format_etf_code(etf_code)
                            self.cache_manager.save_to_cache(clean_etf_code, process_result, threshold)
                        except Exception:
                            pass
                    else:
                        failed_etfs.append({
                            'etf_code': etf_code,
                            'error': process_result.get('error', '未知错误')
                        })
                        threshold_result['failed_etfs'] += 1
                        
                except Exception as e:
                    failed_etfs.append({
                        'etf_code': etf_code,
                        'error': str(e)
                    })
                    threshold_result['failed_etfs'] += 1
            
            threshold_result['results'] = successful_results
            threshold_result['failed_details'] = failed_etfs
            threshold_result['success'] = True
            
            # 保存批量结果到CSV
            if successful_results:
                try:
                    csv_saved = self.csv_handler.create_batch_csv(threshold_result, threshold)
                    threshold_result['csv_saved'] = csv_saved
                except Exception as e:
                    threshold_result['csv_error'] = str(e)
            
            # 创建或更新meta文件
            self._update_meta_file(threshold, threshold_result)
            
        except Exception as e:
            threshold_result['error'] = str(e)
        
        return threshold_result
    
    def _update_meta_file(self, threshold: str, threshold_result: Dict) -> None:
        """更新meta文件，参考MACD格式"""
        try:
            import json
            import os
            from datetime import datetime
            
            meta_file = os.path.join(self.config.cache_dir, "meta", f"{threshold}_meta.json")
            
            # 创建meta数据
            meta_data = {
                "threshold": threshold,
                "last_update": datetime.now().isoformat(),
                "total_etfs": threshold_result.get('successful_etfs', 0) + threshold_result.get('failed_etfs', 0),
                "bb_config": {
                    "cached_etfs_count": threshold_result.get('successful_etfs', 0),
                    "bb_params": {
                        "period": self.config.get_bb_period(),
                        "std_multiplier": self.config.get_bb_std_multiplier(),
                        "adj_type": self.config.adj_type
                    },
                    "last_calculation": datetime.now().isoformat()
                },
                "cache_created": datetime.now().isoformat(),
                "processing_stats": {
                    "total_cache_hits": 0,
                    "total_new_calculations": threshold_result.get('successful_etfs', 0),
                    "total_failed_count": threshold_result.get('failed_etfs', 0),
                    "success_rate": (threshold_result.get('successful_etfs', 0) / max(threshold_result.get('successful_etfs', 0) + threshold_result.get('failed_etfs', 0), 1)) * 100
                }
            }
            
            # 确保meta目录存在
            os.makedirs(os.path.dirname(meta_file), exist_ok=True)
            
            # 保存meta文件
            with open(meta_file, 'w', encoding='utf-8') as f:
                json.dump(meta_data, f, ensure_ascii=False, indent=2)
                
        except Exception:
            pass