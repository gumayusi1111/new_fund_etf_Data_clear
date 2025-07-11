#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
EMA主控制器 - 重构版
==================

参照WMA/SMA系统的主控制器架构
提供统一的EMA计算接口和业务流程协调
"""

import os
import pandas as pd
import numpy as np
from datetime import datetime
from typing import Dict, List, Optional, Any
from ..infrastructure.config import EMAConfig
from ..infrastructure.data_reader import EMADataReader
from ..infrastructure.cache_manager import EMACacheManager
from ..infrastructure.file_manager import EMAFileManager
from ..engines.ema_engine import EMAEngine
from ..engines.historical_calculator import EMAHistoricalCalculator
from .etf_processor import EMAETFProcessor
from .batch_processor import EMABatchProcessor


class EMAMainController:
    """EMA主控制器 - 重构版（与WMA/SMA保持一致的架构）"""
    
    def __init__(self, adj_type: str = "前复权", ema_periods: Optional[List[int]] = None, 
                 output_dir: Optional[str] = None, enable_cache: bool = True, performance_mode: bool = False):
        """
        初始化EMA主控制器 - 重构版本
        
        Args:
            adj_type: 复权类型，默认"前复权"
            ema_periods: EMA周期列表，默认[12, 26]  
            output_dir: 输出目录，默认None（使用配置中的默认目录）
            enable_cache: 是否启用缓存，默认True
            performance_mode: 是否启用性能模式（关闭调试输出）
        """
        print("=" * 60)
        print("🚀 EMA主控制器启动 - 重构版本")
        print("=" * 60)
        
        # 初始化配置
        self.config = EMAConfig(adj_type=adj_type, ema_periods=ema_periods, 
                               enable_cache=enable_cache, performance_mode=performance_mode)
        self.output_dir = output_dir or self.config.default_output_dir
        self.enable_cache = enable_cache
        self.performance_mode = performance_mode
        
        # 初始化核心组件
        self.data_reader = EMADataReader(self.config)
        self.ema_engine = EMAEngine(self.config)
        self.historical_calculator = EMAHistoricalCalculator(self.config)
        self.file_manager = EMAFileManager(self.config)
        
        # 初始化缓存管理器
        self.cache_manager = None
        if enable_cache:
            self.cache_manager = EMACacheManager(self.config)
        
        # 初始化处理器
        self.etf_processor = EMAETFProcessor(
            data_reader=self.data_reader,
            ema_engine=self.ema_engine,
            config=self.config
        )
        
        self.batch_processor = EMABatchProcessor(
            etf_processor=self.etf_processor,
            cache_manager=self.cache_manager,
            config=self.config,
            enable_cache=enable_cache
        )
        
        print("🗂️ 智能缓存系统已启用" if enable_cache else "⚠️ 缓存系统已禁用")
        print("✅ 所有组件初始化完成")
        print("=" * 60)
        
        if not performance_mode:
            print("🚀 EMA主控制器初始化完成")
            print(f"   🔧 复权类型: {adj_type}")
            print(f"   📊 EMA周期: {ema_periods or [12, 26]}")
            print(f"   🗂️ 缓存: {'启用' if enable_cache else '禁用'}")
            print(f"   🚀 性能模式: {'启用' if performance_mode else '禁用'}")
    
    def get_available_etfs(self) -> List[str]:
        """
        获取可用的ETF代码列表
        
        Returns:
            List[str]: 可用的ETF代码列表
        """
        return self.data_reader.get_available_etfs()
    
    def process_single_etf(self, etf_code: str, include_advanced_analysis: bool = False) -> Optional[Dict]:
        """
        处理单个ETF的EMA计算
        
        Args:
            etf_code: ETF代码
            include_advanced_analysis: 是否包含高级分析
            
        Returns:
            Dict: 计算结果或None
        """
        if not self.performance_mode:
            print(f"🔄 开始处理: {etf_code}")
        
        try:
            # 使用ETF处理器
            result = self.etf_processor.process_single_etf(etf_code, include_advanced_analysis)
            
            if result:
                if not self.performance_mode:
                    print(f"✅ {etf_code} 处理完成")
                return result
            else:
                if not self.performance_mode:
                    print(f"❌ {etf_code} 处理失败")
                return None
                
        except Exception as e:
            if not self.performance_mode:
                print(f"❌ {etf_code} 处理异常: {str(e)}")
            return None
    
    def process_multiple_etfs(self, etf_codes: List[str], 
                            include_advanced_analysis: bool = False, max_workers: int = 4) -> List[Dict]:
        """
        处理多个ETF的EMA计算（支持并行）
        
        Args:
            etf_codes: ETF代码列表
            include_advanced_analysis: 是否包含高级分析
            max_workers: 最大并行工作线程数
            
        Returns:
            List[Dict]: 计算结果列表
        """
        if not self.performance_mode:
            print(f"📊 开始批量处理 {len(etf_codes)} 个ETF...")
        
        # 使用批量处理器（支持并行）
        return self.batch_processor.process_etf_list(etf_codes, None, include_advanced_analysis, max_workers)
    
    def calculate_and_save_screening_results(self, thresholds: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        计算并保存筛选结果的EMA数据 - 完整流程
        
        Args:
            thresholds: 门槛列表，默认["3000万门槛", "5000万门槛"]
            
        Returns:
            Dict[str, Any]: 处理结果统计
        """
        thresholds = thresholds or ["3000万门槛", "5000万门槛"]
        
        print("🚀 开始基于ETF筛选结果的EMA批量计算...")
        start_time = datetime.now()
        
        try:
            # 步骤1: 处理筛选结果
            screening_results, runtime_cache_stats = self.process_screening_results(
                thresholds=thresholds,
                include_advanced_analysis=False
            )
            
            if not screening_results:
                return {
                    'success': False,
                    'message': '没有找到有效的筛选结果'
                }
            
            # 步骤2: 保存结果
            save_stats = self._save_threshold_results(screening_results, self.output_dir)
            
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
                'output_directory': self.output_dir,
                'save_statistics': save_stats,
                'processing_time_seconds': processing_time,
                'timestamp': end_time.isoformat()
            }
            
            # 添加运行时缓存统计
            if self.enable_cache and self.cache_manager:
                result_summary['cache_statistics'] = {
                    'cache_hit_rate': runtime_cache_stats['overall_cache_hit_rate'] * 100,
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
                    
                    runtime_cache_stats['total_etfs_processed'] += len(etf_codes)
                    runtime_cache_stats['total_cache_hits'] += cache_hits
                    runtime_cache_stats['total_incremental_updates'] += incremental_updates
                    runtime_cache_stats['total_new_calculations'] += new_calculations
                    runtime_cache_stats['total_failed'] += failed_count
                    
                    # 保存每个门槛的统计
                    runtime_cache_stats['thresholds_stats'][threshold] = {
                        'etfs_processed': len(etf_codes),
                        'cache_hits': cache_hits,
                        'cache_hit_rate': processing_stats.get('cache_hit_rate', 0),
                        'incremental_updates': incremental_updates,
                        'new_calculations': new_calculations,
                        'failed_count': failed_count
                    }
        
        # 计算总体缓存命中率
        total_processed = runtime_cache_stats['total_etfs_processed']
        total_hits = runtime_cache_stats['total_cache_hits']
        
        if total_processed > 0 and total_hits >= 0:
            runtime_cache_stats['overall_cache_hit_rate'] = total_hits / total_processed
        else:
            runtime_cache_stats['overall_cache_hit_rate'] = 0.0
        
        return all_results, runtime_cache_stats
    
    def _load_screening_results(self, threshold: str) -> List[str]:
        """读取筛选结果文件"""
        try:
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
    
    def quick_analysis(self, etf_code: str, include_historical: bool = False) -> Optional[Dict]:
        """
        快速分析单个ETF
        
        Args:
            etf_code: ETF代码
            include_historical: 是否包含历史数据分析
            
        Returns:
            Dict: 分析结果或None
        """
        if not self.performance_mode:
            print(f"🔍 快速分析: {etf_code}")
        
        # 处理单个ETF
        result = self.etf_processor.process_single_etf(etf_code, include_advanced_analysis=True)
        
        if not result:
            if not self.performance_mode:
                print(f"❌ {etf_code}: 分析失败")
            return None
        
        # 如果需要历史数据分析
        if include_historical:
            if not self.performance_mode:
                print(f"\n🚀 {etf_code}: 开始历史数据分析...")
            
            # 读取完整数据
            data_result = self.data_reader.read_etf_data(etf_code)
            if data_result is not None:
                df, _ = data_result
                if not df.empty:
                    # 计算完整历史EMA
                    historical_df = self.historical_calculator.calculate_full_historical_ema_optimized(df, etf_code)
                    
                    if historical_df is not None:
                        # 添加历史数据到结果
                        result['historical_analysis'] = {
                            'total_history_days': len(historical_df),
                            'valid_ema_days': historical_df[f'EMA{max(self.config.ema_periods)}'].notna().sum(),
                            'earliest_date': historical_df['日期'].min(),
                            'latest_date': historical_df['日期'].max(),
                            'historical_trend_summary': self._analyze_historical_trend(historical_df)
                        }
                        
                        if not self.performance_mode:
                            print(f"✅ {etf_code}: 历史数据分析完成")
                            print(f"   📊 历史数据: {result['historical_analysis']['total_history_days']}天")
                            print(f"   📈 有效EMA: {result['historical_analysis']['valid_ema_days']}天")
        
        return result
    
    def _analyze_historical_trend(self, historical_df: pd.DataFrame) -> Dict:
        """分析历史趋势"""
        try:
            # 获取最近30天的数据
            recent_data = historical_df.head(30)
            
            if len(recent_data) < 10:
                return {'analysis': '数据不足，无法进行趋势分析'}
            
            # 使用EMA差值字段
            diff_col = 'EMA_DIFF_12_26'
            
            if diff_col in recent_data.columns:
                diff_values = recent_data[diff_col].dropna()
                if len(diff_values) >= 5:
                    positive_count = (diff_values > 0).sum()
                    trend_strength = positive_count / len(diff_values)
                    
                    if trend_strength >= 0.7:
                        trend_desc = "强上升趋势"
                    elif trend_strength >= 0.3:
                        trend_desc = "震荡趋势"
                    else:
                        trend_desc = "下降趋势"
                    
                    return {
                        'recent_trend': trend_desc,
                        'trend_strength': f"{trend_strength*100:.1f}%",
                        'recent_days_analyzed': len(diff_values),
                        'latest_diff': float(diff_values.iloc[0]) if len(diff_values) > 0 else None,
                        'column_used': diff_col
                    }
            
            return {'analysis': '趋势分析数据不完整'}
            
        except Exception as e:
            return {'analysis': f'趋势分析失败: {str(e)}'}
    
    def get_system_status(self) -> Dict:
        """获取系统状态"""
        try:
            status = {
                'system_info': {
                    'version': '2.0.0',
                    'config': f'EMA系统 - {self.config.adj_type}',
                    'data_path': self.config.data_path,
                    'output_path': self.output_dir
                },
                'data_status': {
                    'available_etfs_count': len(self.data_reader.get_available_etfs()),
                    'data_path_valid': True,
                    'sample_etfs': self.data_reader.get_available_etfs()[:5]
                },
                'components': {
                    'Data Reader': 'Ready',
                    'EMA Engine': 'Ready',
                    'ETF Processor': 'Ready',
                    'Batch Processor': 'Ready',
                    'Cache Manager': 'Ready' if self.enable_cache else 'Disabled'
                }
            }
            
            return status
            
        except Exception as e:
            return {'error': f'系统状态检查失败: {str(e)}'}
    
    def show_system_status(self) -> None:
        """显示系统状态"""
        try:
            status = self.get_system_status()
            
            if 'error' in status:
                print(f"❌ 系统状态检查失败: {status['error']}")
                return
            
            print("\n🔧 EMA系统状态")
            print("=" * 40)
            
            # 系统信息
            system_info = status.get('system_info', {})
            print(f"📊 版本: {system_info.get('version', 'Unknown')}")
            print(f"🔧 配置: {system_info.get('config', 'Unknown')}")
            print(f"📁 数据路径: {system_info.get('data_path', 'Unknown')}")
            print(f"📂 输出路径: {system_info.get('output_path', 'Unknown')}")
            
            # 数据状态
            data_status = status.get('data_status', {})
            print(f"\n📁 数据状态:")
            print(f"   可用ETF: {data_status.get('available_etfs_count', 0)} 个")
            print(f"   数据路径: {'✅ 有效' if data_status.get('data_path_valid', False) else '❌ 无效'}")
            
            # 样本ETF
            sample_etfs = data_status.get('sample_etfs', [])
            if sample_etfs:
                print(f"   样本ETF: {', '.join(sample_etfs)}")
            
            # 组件状态
            components = status.get('components', {})
            print(f"\n🔧 组件状态:")
            for component, state in components.items():
                status_icon = "✅" if state == "Ready" else "⚠️" if state == "Disabled" else "❌"
                print(f"   {component}: {status_icon} {state}")
                
        except Exception as e:
            print(f"❌ 系统状态显示失败: {str(e)}")
    
    def calculate_single_etf(self, etf_code: str, save_result: bool = True, 
                           threshold: str = "3000万门槛", verbose: bool = False) -> Optional[Dict]:
        """
        计算单个ETF的EMA指标
        
        Args:
            etf_code: ETF代码
            save_result: 是否保存结果
            threshold: 门槛类型
            verbose: 是否显示详细信息
            
        Returns:
            Dict: 计算结果或None
        """
        try:
            if verbose:
                print(f"🔄 开始计算 {etf_code} 的EMA指标...")
            
            # 使用ETF处理器计算
            result = self.etf_processor.process_single_etf(etf_code, include_advanced_analysis=True)
            
            if result and save_result:
                # 保存结果到文件
                results = [result]
                save_stats = self.batch_processor.save_results_to_files(
                    results, self.output_dir, threshold
                )
                
                if verbose:
                    print(f"📁 结果已保存: {save_stats.get('files_saved', 0)} 个文件")
            
            return result
            
        except Exception as e:
            if verbose:
                print(f"❌ {etf_code} 计算失败: {str(e)}")
            return None
    
    def calculate_screening_results(self, threshold: str = "3000万门槛", 
                                  max_etfs: Optional[int] = None, 
                                  verbose: bool = False) -> Dict:
        """
        计算筛选结果的EMA指标
        
        Args:
            threshold: 门槛类型
            max_etfs: 最大ETF数量限制
            verbose: 是否显示详细信息
            
        Returns:
            Dict: 处理结果统计
        """
        try:
            if verbose:
                print(f"🔍 开始批量计算 {threshold} 的EMA指标...")
            
            # 获取筛选结果
            etf_codes = self._load_screening_results(threshold)
            
            if not etf_codes:
                return {
                    'success': False,
                    'error': f'未找到{threshold}的筛选结果',
                    'processed_count': 0,
                    'success_count': 0
                }
            
            # 限制ETF数量
            if max_etfs and len(etf_codes) > max_etfs:
                etf_codes = etf_codes[:max_etfs]
                if verbose:
                    print(f"⚠️  限制处理数量为 {max_etfs} 个ETF")
            
            # 批量处理
            results = self.batch_processor.process_etf_list(etf_codes, threshold, include_advanced_analysis=True)
            
            # 保存结果
            if results:
                save_stats = self.batch_processor.save_results_to_files(
                    results, self.output_dir, threshold
                )
                
                success_count = len([r for r in results if r.get('success', False)])
                
                return {
                    'success': True,
                    'processed_count': len(results),
                    'success_count': success_count,
                    'failed_count': len(results) - success_count,
                    'files_saved': save_stats.get('files_saved', 0),
                    'threshold': threshold
                }
            else:
                return {
                    'success': False,
                    'error': '批量处理失败',
                    'processed_count': 0,
                    'success_count': 0
                }
                
        except Exception as e:
            return {
                'success': False,
                'error': f'批量计算失败: {str(e)}',
                'processed_count': 0,
                'success_count': 0
            }
    
    def validate_ema_calculation(self, etf_code: str, verbose: bool = False) -> bool:
        """
        验证EMA计算的正确性
        
        Args:
            etf_code: ETF代码
            verbose: 是否显示详细信息
            
        Returns:
            bool: 验证是否通过
        """
        try:
            if verbose:
                print(f"🔍 验证 {etf_code} 的EMA计算...")
            
            # 计算EMA
            result = self.etf_processor.process_single_etf(etf_code, include_advanced_analysis=True)
            
            if not result:
                if verbose:
                    print(f"❌ {etf_code}: 计算失败")
                return False
            
            # 验证结果完整性
            if not result.get('success', False):
                if verbose:
                    print(f"❌ {etf_code}: 计算未成功")
                return False
            
            # 验证EMA值
            ema_values = result.get('ema_values', {})
            if not ema_values:
                if verbose:
                    print(f"❌ {etf_code}: EMA值为空")
                return False
            
            # 检查每个周期的EMA值
            for period in self.config.ema_periods:
                ema_key = f'ema_{period}'
                if ema_key not in ema_values:
                    if verbose:
                        print(f"❌ {etf_code}: 缺少 {ema_key}")
                    return False
                
                ema_value = ema_values[ema_key]
                if ema_value <= 0:
                    if verbose:
                        print(f"❌ {etf_code}: {ema_key} 值无效: {ema_value}")
                    return False
            
            if verbose:
                print(f"✅ {etf_code}: EMA计算验证通过")
                for period in self.config.ema_periods:
                    ema_key = f'ema_{period}'
                    ema_value = ema_values[ema_key]
                    print(f"   {ema_key}: {ema_value:.6f}")
            
            return True
            
        except Exception as e:
            if verbose:
                print(f"❌ {etf_code} 验证失败: {str(e)}")
            return False