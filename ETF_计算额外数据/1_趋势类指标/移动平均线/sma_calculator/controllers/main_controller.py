#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SMA主控制器
=========

负责协调所有组件，处理主要业务流程
"""

import os
from datetime import datetime
from typing import Dict, List, Optional, Any
from .etf_processor import ETFProcessor
from .batch_processor import BatchProcessor
from ..infrastructure.config import SMAConfig
from ..infrastructure.data_reader import ETFDataReader
from ..infrastructure.cache_manager import SMACacheManager
from ..engines.sma_engine import SMAEngine
from ..outputs.csv_handler import CSVOutputHandler
from ..outputs.display_formatter import DisplayFormatter


class SMAMainController:
    """SMA主控制器 - 协调所有组件的业务逻辑"""
    
    def __init__(self, adj_type: str = "前复权", sma_periods: Optional[List[int]] = None, 
                 output_dir: Optional[str] = None, enable_cache: bool = True):
        """
        初始化主控制器
        
        Args:
            adj_type: 复权类型
            sma_periods: SMA周期列表
            output_dir: 输出目录
            enable_cache: 是否启用缓存
        """
        # 初始化配置
        self.config = SMAConfig(
            adj_type=adj_type,
            sma_periods=sma_periods or [5, 10, 20, 60]
        )
        
        # 设置输出目录
        module_dir = os.path.dirname(__file__)
        self.output_dir = output_dir or os.path.join(module_dir, "..", "..", "data")
        
        # 初始化核心组件
        self.data_reader = ETFDataReader(config=self.config)
        self.sma_engine = SMAEngine(config=self.config)
        self.csv_handler = CSVOutputHandler()
        self.display_formatter = DisplayFormatter()
        
        # 初始化缓存组件
        self.enable_cache = enable_cache
        self.cache_manager = None
        if enable_cache:
            self.cache_manager = SMACacheManager()
        
        # 初始化处理器
        self.etf_processor = ETFProcessor(
            data_reader=self.data_reader,
            sma_engine=self.sma_engine,
            config=self.config
        )
        
        self.batch_processor = BatchProcessor(
            etf_processor=self.etf_processor,
            cache_manager=self.cache_manager,
            csv_handler=self.csv_handler,
            enable_cache=enable_cache
        )
        
        print("🗂️ 智能缓存系统已启用" if enable_cache else "⚠️ 缓存系统已禁用")
        print("✅ 所有组件初始化完成")
        print("=" * 60)
    
    def process_single_etf(self, etf_code: str, include_advanced_analysis: bool = False) -> Optional[Dict]:
        """
        处理单个ETF
        
        Args:
            etf_code: ETF代码
            include_advanced_analysis: 是否包含高级分析
            
        Returns:
            Optional[Dict]: 处理结果
        """
        print(f"🔄 开始处理ETF: {etf_code}")
        
        result = self.etf_processor.process_single_etf(etf_code, include_advanced_analysis)
        
        if result:
            # 显示结果
            self.display_formatter.display_single_etf_result(result)
            return result
        else:
            print(f"❌ ETF {etf_code} 处理失败")
            return None
    
    def process_screening_results(self, thresholds: List[str] = None, 
                                include_advanced_analysis: bool = False):
        """
        处理ETF筛选结果
        
        Args:
            thresholds: 门槛列表
            include_advanced_analysis: 是否包含高级分析
            
        Returns:
            tuple: (按门槛组织的处理结果, 运行时缓存统计)
        """
        if thresholds is None:
            thresholds = ["3000万门槛", "5000万门槛"]
        
        print(f"📊 处理门槛: {', '.join(thresholds)}")
        
        all_results = {}
        runtime_cache_stats = {
            'total_etfs_processed': 0,
            'total_cache_hits': 0,
            'total_incremental_updates': 0,
            'total_new_calculations': 0,
            'total_failed': 0,
            'thresholds_stats': {}
        }
        
        for threshold in thresholds:
            print(f"\n📊 {threshold}: 读取筛选结果...")
            
            # 读取筛选结果
            etf_codes = self._load_screening_results(threshold)
            
            if not etf_codes:
                print(f"❌ {threshold}: 未找到筛选结果")
                continue
            
            print(f"📊 {threshold}: 找到 {len(etf_codes)} 个通过筛选的ETF")
            
            # 记录处理前的状态，以便计算本次运行的统计
            threshold_start_etfs = len(etf_codes)
            
            # 批量处理ETF
            results = self.batch_processor.process_etf_list(
                etf_codes=etf_codes,
                threshold=threshold,
                include_advanced_analysis=include_advanced_analysis
            )
            
            all_results[threshold] = results
            
            # 从门槛Meta中获取最新的处理统计
            if self.enable_cache and self.cache_manager:
                threshold_meta = self.cache_manager.load_meta(threshold)
                if threshold_meta.get('update_history'):
                    latest_update = threshold_meta['update_history'][-1]
                    processing_stats = latest_update.get('processing_stats', {})
                    
                    # 累计统计信息
                    cache_hits = processing_stats.get('cache_hits', 0)
                    incremental_updates = processing_stats.get('incremental_updates', 0) 
                    new_calculations = processing_stats.get('new_calculations', 0)
                    failed_count = processing_stats.get('failed_count', 0)
                    
                    runtime_cache_stats['total_etfs_processed'] += threshold_start_etfs
                    runtime_cache_stats['total_cache_hits'] += cache_hits
                    runtime_cache_stats['total_incremental_updates'] += incremental_updates
                    runtime_cache_stats['total_new_calculations'] += new_calculations
                    runtime_cache_stats['total_failed'] += failed_count
                    
                    # 保存每个门槛的统计
                    runtime_cache_stats['thresholds_stats'][threshold] = {
                        'etfs_processed': threshold_start_etfs,
                        'cache_hits': cache_hits,
                        'cache_hit_rate': processing_stats.get('cache_hit_rate', 0),
                        'incremental_updates': incremental_updates,
                        'new_calculations': new_calculations,
                        'failed_count': failed_count
                    }
        
        # 计算总体缓存命中率（安全除法）
        total_processed = runtime_cache_stats['total_etfs_processed']
        total_hits = runtime_cache_stats['total_cache_hits']
        
        if total_processed > 0 and total_hits >= 0:
            runtime_cache_stats['overall_cache_hit_rate'] = total_hits / total_processed
        else:
            runtime_cache_stats['overall_cache_hit_rate'] = 0.0
        
        return all_results, runtime_cache_stats
    
    def calculate_and_save_screening_results(self, thresholds: List[str] = None, 
                                           output_dir: Optional[str] = None,
                                           include_advanced_analysis: bool = False) -> Dict[str, Any]:
        """
        计算并保存基于筛选结果的SMA数据 - 完整流程
        
        Args:
            thresholds: 门槛列表
            output_dir: 输出目录
            include_advanced_analysis: 是否包含高级分析
            
        Returns:
            Dict[str, Any]: 处理结果统计
        """
        print("🚀 开始基于ETF筛选结果的SMA批量计算...")
        start_time = datetime.now()
        
        try:
            # 步骤1: 处理筛选结果
            screening_results, runtime_cache_stats = self.process_screening_results(
                thresholds=thresholds,
                include_advanced_analysis=include_advanced_analysis
            )
            
            if not screening_results:
                return {
                    'success': False,
                    'message': '没有找到有效的筛选结果'
                }
            
            # 步骤2: 保存结果
            save_stats = self._save_threshold_results(screening_results, output_dir or self.output_dir)
            
            # 步骤3: 更新全局Meta
            if self.enable_cache and self.cache_manager:
                self._update_global_cache_meta()
            
            # 构建返回结果
            end_time = datetime.now()
            processing_time = (end_time - start_time).total_seconds()
            
            total_etfs = sum(len(results) for results in screening_results.values())
            
            result_summary = {
                'success': True,
                'total_etfs_processed': total_etfs,
                'thresholds_processed': len(screening_results),
                'output_directory': output_dir or self.output_dir,
                'save_statistics': save_stats,
                'processing_time_seconds': processing_time,
                'timestamp': end_time.isoformat()
            }
            
            # 添加运行时缓存统计（修复的部分）
            if self.enable_cache and self.cache_manager:
                result_summary['cache_statistics'] = {
                    'cache_hit_rate': runtime_cache_stats['overall_cache_hit_rate'] * 100,  # 转换为百分比
                    'total_cache_hits': runtime_cache_stats['total_cache_hits'],
                    'total_incremental_updates': runtime_cache_stats['total_incremental_updates'],
                    'total_new_calculations': runtime_cache_stats['total_new_calculations'],
                    'total_failed': runtime_cache_stats['total_failed'],
                    'threshold_details': runtime_cache_stats['thresholds_stats']
                }
            
            return result_summary
            
        except Exception as e:
            return {
                'success': False,
                'message': f'处理失败: {str(e)}'
            }
    
    def _load_screening_results(self, threshold: str) -> List[str]:
        """读取筛选结果文件"""
        try:
            # 使用数据读取器的方法来获取筛选结果
            etf_codes = self.data_reader.get_screening_etf_codes(threshold)
            
            if not etf_codes:
                print(f"⚠️ {threshold}: 未找到筛选结果")
            else:
                print(f"📊 {threshold}: 找到 {len(etf_codes)} 个通过筛选的ETF")
            
            return etf_codes
            
        except Exception as e:
            print(f"❌ 读取筛选结果失败: {str(e)}")
            return []
    
    def _save_threshold_results(self, results: Dict[str, List[Dict]], output_base_dir: str) -> Dict:
        """保存门槛结果到data目录"""
        save_stats = {
            'total_files_saved': 0,
            'total_size_bytes': 0,
            'thresholds': {}
        }
        
        for threshold, results_list in results.items():
            if not results_list:
                continue
            
            # 使用批量处理器保存结果
            threshold_stats = self.batch_processor.save_results_to_files(
                results_list, output_base_dir, threshold
            )
            
            save_stats['thresholds'][threshold] = threshold_stats
            save_stats['total_files_saved'] += threshold_stats['files_saved']
            save_stats['total_size_bytes'] += threshold_stats['total_size']
            
            print(f"✅ {threshold}: 成功保存 {threshold_stats['files_saved']} 个历史文件")
            if threshold_stats['failed_saves'] > 0:
                print(f"⚠️  {threshold}: {threshold_stats['failed_saves']} 个文件保存失败")
        
        return save_stats
    
    def _update_global_cache_meta(self):
        """更新全局缓存Meta信息"""
        try:
            if self.cache_manager:
                # 更新全局Meta
                global_meta = self.cache_manager.load_meta(None)
                global_meta["last_global_update"] = datetime.now().isoformat()
                
                # 统计缓存总大小
                total_size = 0
                total_etfs = 0
                
                for threshold in ["3000万门槛", "5000万门槛"]:
                    cache_dir = self.cache_manager.get_cache_dir(threshold)
                    if os.path.exists(cache_dir):
                        for file in os.listdir(cache_dir):
                            if file.endswith('.csv'):
                                file_path = os.path.join(cache_dir, file)
                                total_size += os.path.getsize(file_path)
                                total_etfs += 1
                
                global_meta["total_cache_size_mb"] = round(total_size / 1024 / 1024, 2)
                global_meta["total_cached_etfs"] = total_etfs
                
                # 保存全局Meta
                self.cache_manager.save_meta(global_meta, None)
                
        except Exception as e:
            print(f"❌ 更新全局Meta失败: {str(e)}")
    
    def get_system_status(self) -> Dict:
        """获取系统状态"""
        try:
            status = {
                'system_info': {
                    'version': '2.0.0',
                    'config': f'SMA系统 - {self.config.adj_type}',
                    'data_path': 'ETF数据路径',
                    'output_path': self.output_dir
                },
                'data_status': {
                    'available_etfs_count': len(self.data_reader.get_available_etfs()),
                    'data_path_valid': True,
                    'sample_etfs': self.data_reader.get_available_etfs()[:5]
                },
                'components': {
                    'Data Reader': 'Ready',
                    'SMA Engine': 'Ready',
                    'CSV Handler': 'Ready',
                    'Cache Manager': 'Ready' if self.enable_cache else 'Disabled'
                }
            }
            
            return status
            
        except Exception as e:
            return {'error': f'系统状态检查失败: {str(e)}'} 