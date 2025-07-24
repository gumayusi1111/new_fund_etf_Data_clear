#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
布林带主控制器
============

协调各个组件，提供统一的对外接口
参照趋势类指标的控制器模式
"""

import time
from typing import Dict, List, Optional, Any
from ..infrastructure.config import BBConfig
from ..infrastructure.data_reader import BBDataReader
from ..infrastructure.cache_manager import BBCacheManager
from ..infrastructure.file_manager import BBFileManager
from ..infrastructure.utils import BBUtils
from ..engines.bb_engine import BollingerBandsEngine
from ..outputs.csv_handler import BBCSVHandler


class BBMainController:
    """布林带主控制器"""
    
    def __init__(self, config: BBConfig = None):
        """初始化主控制器"""
        self.config = config or BBConfig()
        self.data_reader = BBDataReader(self.config)
        self.cache_manager = BBCacheManager(self.config)
        self.file_manager = BBFileManager(self.config)
        self.utils = BBUtils()
        self.bb_engine = BollingerBandsEngine(self.config)
        self.csv_handler = BBCSVHandler(self.config)
        
        # 确保目录存在
        self.config.ensure_directories_exist()
    
    def update_config(self, new_config: BBConfig):
        """更新配置并重新初始化相关组件"""
        self.config = new_config
        self.data_reader = BBDataReader(self.config)
        self.cache_manager = BBCacheManager(self.config)
        self.file_manager = BBFileManager(self.config)
        self.bb_engine = BollingerBandsEngine(self.config)
        self.csv_handler = BBCSVHandler(self.config)
        
        # 确保目录存在
        self.config.ensure_directories_exist()
    
    def get_system_status(self) -> Dict[str, Any]:
        """获取系统状态"""
        # 获取筛选后的ETF列表
        available_etfs = []
        try:
            # 获取各门槛的筛选ETF列表
            etf_3000 = self.utils.read_etf_screening_list('3000万门槛')
            etf_5000 = self.utils.read_etf_screening_list('5000万门槛')
            # 合并去重
            available_etfs = list(set(etf_3000 + etf_5000))
        except Exception:
            available_etfs = []
        
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
            'available_etfs': available_etfs,
            'cache_stats': self.cache_manager.get_cache_statistics(),
            'output_stats': self.file_manager.get_all_statistics()
        }
        
        return status
    
    def calculate_single_etf(self, etf_code: str, threshold: str) -> Dict[str, Any]:
        """
        计算单个ETF的布林带指标（统一接口）
        
        Args:
            etf_code: ETF代码
            threshold: 门槛类型
            
        Returns:
            Dict[str, Any]: 计算结果
        """
        return self.process_single_etf(etf_code, threshold, use_cache=True)
    
    def process_single_etf(self, etf_code: str, threshold: Optional[str] = None, 
                          use_cache: bool = True) -> Dict[str, Any]:
        """
        处理单个ETF的布林带计算
        
        Args:
            etf_code: ETF代码
            threshold: 门槛(用于缓存和输出路径)
            use_cache: 是否使用缓存
            
        Returns:
            Dict[str, Any]: 处理结果
        """
        result = {
            'success': False,
            'etf_code': etf_code,
            'threshold': threshold,
            'bb_results': {},
            'cache_used': False,
            'processing_time': 0,
            'error': None
        }
        
        start_time = time.time()
        
        try:
            # 读取ETF数据
            etf_data = self.data_reader.read_etf_data(etf_code)
            if etf_data is None or etf_data.empty:
                result['error'] = f'无法读取ETF数据: {etf_code}'
                return result
            
            # 检查缓存
            if use_cache and threshold:
                cached_data = self.cache_manager.load_cache(threshold, etf_code)
                if cached_data is not None:
                    result['success'] = True
                    result['cache_used'] = True
                    result['bb_results'] = cached_data.to_dict('records')
                    return result
            
            # 计算布林带完整历史数据
            bb_results_df = self.bb_engine.calculate_full_history(etf_data)
            
            if bb_results_df is None or bb_results_df.empty:
                result['error'] = f'布林带计算失败: {etf_code}'
                return result
            
            # 格式化输出数据
            formatted_df = self.csv_handler.format_bb_full_history_to_dataframe(
                etf_code, etf_data, bb_results_df
            )
            
            if formatted_df.empty:
                result['error'] = f'数据格式化失败: {etf_code}'
                return result
            
            # 保存缓存
            if threshold and use_cache:
                self.cache_manager.save_cache(threshold, etf_code, formatted_df)
            
            # 保存输出文件
            if threshold:
                output_path = self.config.get_output_file_path(threshold, etf_code)
                save_result = self.csv_handler.save_bb_data_to_csv(
                    etf_code, formatted_df, output_path
                )
                
                if not save_result['success']:
                    result['error'] = f'保存文件失败: {save_result["error"]}'
                    return result
            
            result['success'] = True
            result['bb_results'] = formatted_df.to_dict('records')
            result['processing_time'] = time.time() - start_time
            
        except Exception as e:
            result['error'] = str(e)
        
        return result
    
    def calculate_and_save_screening_results(self, thresholds: List[str], 
                                           use_cache: bool = True) -> Dict[str, Any]:
        """
        基于ETF初筛结果进行批量计算和保存
        
        Args:
            thresholds: 门槛列表
            use_cache: 是否使用缓存
            
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
            'output_directory': self.config.default_output_dir,
            'save_statistics': {},
            'cache_statistics': {},
            'threshold_details': {},
            'errors': []
        }
        
        start_time = time.time()
        
        try:
            total_successful = 0
            total_failed = 0
            all_save_results = []
            
            for threshold in thresholds:
                threshold_result = self._process_threshold(threshold, use_cache)
                result['threshold_details'][threshold] = threshold_result
                
                if threshold_result['success']:
                    total_successful += threshold_result['successful_etfs']
                    total_failed += threshold_result['failed_etfs']
                    
                    if 'save_statistics' in threshold_result:
                        all_save_results.append(threshold_result['save_statistics'])
                else:
                    result['errors'].append(f"{threshold}: {threshold_result.get('error', '未知错误')}")
            
            result['total_etfs_processed'] = total_successful + total_failed
            result['successful_etfs'] = total_successful
            result['failed_etfs'] = total_failed
            result['processing_time_seconds'] = time.time() - start_time
            
            # 汇总保存统计
            if all_save_results:
                result['save_statistics'] = self._summarize_save_stats(all_save_results)
            
            # 获取缓存统计
            result['cache_statistics'] = self.cache_manager.get_cache_statistics()
            
            # 生成统一标准的meta文件
            threshold_statistics = {}
            for threshold in thresholds:
                if threshold in result['threshold_details']:
                    threshold_details = result['threshold_details'][threshold]
                    threshold_statistics[threshold] = {
                        'successful_etfs': threshold_details.get('successful_etfs', 0),
                        'failed_etfs': threshold_details.get('failed_etfs', 0)
                    }
                    # 为每个门槛生成meta文件
                    self.cache_manager.create_threshold_meta(threshold, threshold_statistics[threshold])
            
            # 生成系统级meta文件
            if threshold_statistics:
                self.cache_manager.create_system_meta(threshold_statistics)
            
            result['success'] = len(result['errors']) == 0
            
        except Exception as e:
            result['error'] = str(e)
        
        return result
    
    def _process_threshold(self, threshold: str, use_cache: bool = True) -> Dict[str, Any]:
        """处理单个门槛的ETF列表"""
        threshold_result = {
            'success': False,
            'threshold': threshold,
            'etf_list': [],
            'successful_etfs': 0,
            'failed_etfs': 0,
            'save_statistics': {},
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
            successful_data = {}
            failed_etfs = []
            
            for etf_code in etf_list:
                try:
                    process_result = self.process_single_etf(etf_code, threshold, use_cache)
                    
                    if process_result['success']:
                        # 读取保存的数据
                        output_path = self.config.get_output_file_path(threshold, etf_code)
                        saved_data = self.csv_handler.load_bb_data_from_csv(output_path)
                        if saved_data is not None:
                            successful_data[etf_code] = saved_data
                        threshold_result['successful_etfs'] += 1
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
            
            # 生成保存统计
            threshold_result['save_statistics'] = self._generate_save_statistics(
                threshold, successful_data, failed_etfs
            )
            
            threshold_result['success'] = True
            
        except Exception as e:
            threshold_result['error'] = str(e)
        
        return threshold_result
    
    def _generate_save_statistics(self, threshold: str, successful_data: Dict, 
                                failed_etfs: List) -> Dict[str, Any]:
        """生成保存统计信息"""
        stats = {
            'threshold': threshold,
            'total_files_saved': len(successful_data),
            'total_failed_files': len(failed_etfs),
            'total_size_bytes': 0,
            'total_records': 0,
            'failed_details': failed_etfs
        }
        
        # 计算文件大小和记录数
        for etf_code, data in successful_data.items():
            if not data.empty:
                stats['total_records'] += len(data)
                
                # 估算文件大小
                output_path = self.config.get_output_file_path(threshold, etf_code)
                file_size = self.utils.get_file_size_mb(output_path)
                if file_size:
                    stats['total_size_bytes'] += file_size * 1024 * 1024
        
        return stats
    
    def _summarize_save_stats(self, save_stats_list: List[Dict]) -> Dict[str, Any]:
        """汇总多个门槛的保存统计"""
        summary = {
            'total_files_saved': 0,
            'total_failed_files': 0,
            'total_size_bytes': 0,
            'total_records': 0,
            'threshold_breakdown': {}
        }
        
        for stats in save_stats_list:
            summary['total_files_saved'] += stats.get('total_files_saved', 0)
            summary['total_failed_files'] += stats.get('total_failed_files', 0)
            summary['total_size_bytes'] += stats.get('total_size_bytes', 0)
            summary['total_records'] += stats.get('total_records', 0)
            
            threshold = stats.get('threshold', 'unknown')
            summary['threshold_breakdown'][threshold] = stats
        
        return summary
    
    def calculate_historical_batch(self, thresholds: List[str], 
                                  verbose: bool = False) -> Dict[str, Any]:
        """
        超高性能批量历史计算
        
        Args:
            thresholds: 门槛列表
            verbose: 是否详细输出
            
        Returns:
            Dict[str, Any]: 批量计算结果
        """
        result = {
            'success': False,
            'total_etfs_processed': 0,
            'successful_etfs': 0,
            'failed_etfs': 0,
            'thresholds_processed': len(thresholds),
            'processing_time_seconds': 0,
            'etfs_per_second': 0,
            'output_directory': self.config.default_output_dir,
            'save_statistics': {},
            'threshold_details': {},
            'errors': []
        }
        
        start_time = time.time()
        
        try:
            # 使用普通的批量处理方法
            batch_result = self.calculate_and_save_screening_results(thresholds, use_cache=True)
            
            # 复制结果
            result.update(batch_result)
            
            # 计算处理速度
            processing_time = time.time() - start_time
            result['processing_time_seconds'] = processing_time
            
            if processing_time > 0 and result['total_etfs_processed'] > 0:
                result['etfs_per_second'] = round(result['total_etfs_processed'] / processing_time, 2)
            
        except Exception as e:
            result['error'] = str(e)
        
        return result