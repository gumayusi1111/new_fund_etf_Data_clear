#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
WMA主控制器模块 - 重构版
=====================

从原有controller.py完全迁移核心功能，保持算法和功能完全一致
"""

import os
import pandas as pd
import numpy as np
from datetime import datetime
from typing import Dict, List, Optional, Any
from ..infrastructure.config import WMAConfig
from ..infrastructure.data_reader import WMADataReader
from ..infrastructure.cache_manager import WMACacheManager
from ..engines.wma_engine import WMAEngine
from ..engines.historical_calculator import WMAHistoricalCalculator
from ..outputs.result_processor import WMAResultProcessor
from .etf_processor import WMAETFProcessor
from .batch_processor import WMABatchProcessor


class WMAMainController:
    """WMA主控制器 - 重构版（功能完全一致）"""
    
    def __init__(self, adj_type: str = "前复权", wma_periods: Optional[List[int]] = None, 
                 output_dir: Optional[str] = None, enable_cache: bool = True, performance_mode: bool = True):
        """
        初始化WMA主控制器 - 完全重构版本
        
        Args:
            adj_type: 复权类型，默认"前复权"
            wma_periods: WMA周期列表，默认[3, 5, 10, 20]  
            output_dir: 输出目录，默认None（使用配置中的默认目录）
            enable_cache: 是否启用缓存，默认True
            performance_mode: 是否启用性能模式（关闭调试输出）
        """
        print("=" * 60)
        print("🚀 WMA主控制器启动 - 完全重构版本")
        print("=" * 60)
        
        # 初始化配置
        self.config = WMAConfig(adj_type=adj_type, wma_periods=wma_periods, enable_cache=enable_cache, performance_mode=performance_mode)
        self.output_dir = output_dir or self.config.default_output_dir
        self.enable_cache = enable_cache
        self.performance_mode = performance_mode
        
        # 初始化核心组件
        self.data_reader = WMADataReader(self.config)
        self.wma_engine = WMAEngine(self.config)
        self.historical_calculator = WMAHistoricalCalculator(self.config)
        self.result_processor = WMAResultProcessor(self.config)
        
        # 初始化缓存管理器
        self.cache_manager = None
        if enable_cache:
            self.cache_manager = WMACacheManager(self.config)
        
        # 初始化处理器 - 新的分层处理器
        self.etf_processor = WMAETFProcessor(
            data_reader=self.data_reader,
            wma_engine=self.wma_engine,
            config=self.config
        )
        
        self.batch_processor = WMABatchProcessor(
            etf_processor=self.etf_processor,
            cache_manager=self.cache_manager,
            config=self.config,
            enable_cache=enable_cache
        )
        
        print("🗂️ 智能缓存系统已启用" if enable_cache else "⚠️ 缓存系统已禁用")
        print("✅ 所有组件初始化完成")
        print("=" * 60)
        
        if not performance_mode:
            print("🚀 WMA主控制器初始化完成")
            print(f"   🔧 复权类型: {adj_type}")
            print(f"   📊 WMA周期: {wma_periods or [3, 5, 10, 20]}")
            print(f"   🗂️ 缓存: {'启用' if enable_cache else '禁用'}")
            print(f"   🚀 性能模式: {'启用' if performance_mode else '禁用'}")
    
    def get_available_etfs(self) -> List[str]:
        """
        获取可用的ETF代码列表 - 保持原有功能
        
        Returns:
            List[str]: 可用的ETF代码列表
        """
        return self.data_reader.get_available_etfs()
    
    def process_single_etf(self, etf_code: str, include_advanced_analysis: bool = False) -> Optional[Dict]:
        """
        处理单个ETF的WMA计算 - 保持原有功能和算法完全一致
        
        Args:
            etf_code: ETF代码
            include_advanced_analysis: 是否包含高级分析
            
        Returns:
            Dict: 计算结果或None
        """
        print(f"🔄 开始处理: {etf_code}")
        
        try:
            # 使用新的ETF处理器 - 保持原有处理逻辑
            result = self.etf_processor.process_single_etf(etf_code, include_advanced_analysis)
            
            if result:
                print(f"✅ {etf_code} 处理完成")
                return result
            else:
                print(f"❌ {etf_code} 处理失败")
                return None
                
        except Exception as e:
            print(f"❌ {etf_code} 处理异常: {str(e)}")
            return None
    
    def process_multiple_etfs(self, etf_codes: List[str], 
                            include_advanced_analysis: bool = False) -> List[Dict]:
        """
        处理多个ETF的WMA计算 - 保持原有功能
        
        Args:
            etf_codes: ETF代码列表
            include_advanced_analysis: 是否包含高级分析
            
        Returns:
            List[Dict]: 计算结果列表
        """
        print(f"📊 开始批量处理 {len(etf_codes)} 个ETF...")
        
        # 使用新的批量处理器 - 保持原有批量处理逻辑
        return self.batch_processor.process_etf_list(etf_codes, None, include_advanced_analysis)
    
    def calculate_and_save(self, etf_codes: List[str], output_dir: Optional[str] = None,
                          include_advanced_analysis: bool = False) -> Dict[str, Any]:
        """
        计算并保存结果的完整流程 - 保持原有功能完全一致
        
        Args:
            etf_codes: ETF代码列表
            output_dir: 输出目录（可选）
            include_advanced_analysis: 是否包含高级分析
            
        Returns:
            Dict[str, Any]: 处理结果摘要
        """
        # 处理ETF - 保持原有处理逻辑
        results = self.process_multiple_etfs(etf_codes, include_advanced_analysis)
        
        if not results:
            print("❌ 没有成功处理的ETF")
            return {'success': False, 'message': '没有成功处理的ETF'}
        
        # 保存结果 - 使用新的批量处理器
        save_stats = self.batch_processor.save_results_to_files(
            results, output_dir or self.output_dir, None
        )
        
        # 获取结果统计 - 保持原有统计逻辑
        return {
            'success': True,
            'processed_etfs': len(results),
            'total_etfs': len(etf_codes),
            'success_rate': len(results) / len(etf_codes) * 100,
            'output_directory': output_dir or self.output_dir,
            'saved_files': save_stats['files_saved'],
            'statistics': self._get_result_stats(results)
        }
    
    def calculate_and_save_screening_results(self, thresholds: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        计算并保存筛选结果的WMA数据 - 包含data文件保存功能
        
        Args:
            thresholds: 门槛列表，默认["3000万门槛", "5000万门槛"]
            
        Returns:
            Dict[str, Any]: 处理结果统计
        """
        thresholds = thresholds or ["3000万门槛", "5000万门槛"]
        
        print(f"🚀 开始筛选结果WMA计算和保存...")
        print(f"📊 门槛设置: {thresholds}")
        
        all_results = {}
        
        for threshold in thresholds:
            print(f"\n📈 处理门槛: {threshold}")
            
            # 使用批量处理器进行增量更新计算
            results = self.batch_processor.process_screening_results(threshold)
            
            if results:
                all_results[threshold] = results
                print(f"✅ {threshold}: {len(results)}个ETF增量更新完成")
                
                # 显示增量更新统计
                cache_hits = len([r for r in results if r.get('data_source') == 'cache'])
                incremental_updates = len([r for r in results if r.get('data_source') == 'incremental_update'])
                full_calculations = len([r for r in results if r.get('data_source') not in ['cache', 'incremental_update']])
                
                print(f"   💾 缓存命中: {cache_hits}")
                print(f"   ⚡ 增量更新: {incremental_updates}")
                print(f"   🔄 全量计算: {full_calculations}")
            else:
                all_results[threshold] = []
                print(f"❌ {threshold}: 无可用结果")
        
        # 保存筛选结果到data目录
        if all_results:
            print(f"\n💾 保存筛选结果到data目录...")
            save_stats = self.result_processor.save_screening_batch_results(all_results, self.output_dir)
            print(f"✅ data文件保存完成")
        
        # 显示结果预览
        for threshold, results in all_results.items():
            if results:
                print(f"\n📊 {threshold} 结果预览:")
                self.result_processor.display_results(results[:3])  # 显示前3个
        
        return {
            'calculation_results': all_results,
            'total_etfs': sum(len(results) for results in all_results.values()),
            'mode': 'complete_with_data_save'  # 标记为完整保存模式
        }
    
    def calculate_historical_batch(self, etf_codes: Optional[List[str]] = None, 
                                 thresholds: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        🚀 超高性能历史批量计算 - 使用batch_processor确保缓存集成
        
        Args:
            etf_codes: ETF代码列表，None则处理所有可用ETF
            thresholds: 门槛列表，默认["3000万门槛", "5000万门槛"]
            
        Returns:
            Dict[str, Any]: 处理结果统计
        """
        print("🚀 开始超高性能历史批量计算...")
        print("⚡ 使用智能缓存模式，支持增量更新")
        start_time = datetime.now()
        
        thresholds = thresholds or ["3000万门槛", "5000万门槛"]
        
        total_etfs_processed = 0
        total_files_saved = 0
        total_size_bytes = 0
        processing_stats = {}
        
        try:
            for threshold in thresholds:
                print(f"\n📊 {threshold}: 读取筛选结果...")
                
                # 获取该门槛的ETF列表
                if etf_codes is None:
                    threshold_etf_codes = self.data_reader.get_screening_etf_codes(threshold)
                    print(f"📊 {threshold}: 找到 {len(threshold_etf_codes)} 个通过筛选的ETF")
                else:
                    threshold_etf_codes = etf_codes
                
                if not threshold_etf_codes:
                    print(f"❌ {threshold}: 未找到筛选结果")
                    continue
                
                print(f"🚀 开始超高性能批量计算（智能缓存模式）...")
                
                # 使用现有的批量处理器（包含缓存逻辑）进行处理
                calculation_start = datetime.now()
                results = self.batch_processor.process_etf_list(
                    etf_codes=threshold_etf_codes,
                    threshold=threshold,
                    include_advanced_analysis=False
                )
                calculation_end = datetime.now()
                calculation_time = (calculation_end - calculation_start).total_seconds()
                
                # 转换结果格式以适配统计逻辑
                results_dict = {}
                results_for_save = []
                for result in results:
                    if result and 'etf_code' in result:
                        etf_code = result['etf_code']
                        if 'wma_values' in result:
                            results_dict[etf_code] = result['wma_values']
                        # 为保存准备结果格式
                        if 'historical_data' in result:
                            results_for_save.append(result)
                
                # 保存结果到data目录
                save_start = datetime.now()
                save_stats = self.batch_processor.save_results_to_files(
                    results_for_save, self.output_dir, threshold
                )
                save_end = datetime.now()
                save_time = (save_end - save_start).total_seconds()
                
                # 统计信息
                threshold_etfs = len(results_dict)
                total_etfs_processed += threshold_etfs
                total_files_saved += save_stats.get('files_saved', 0)
                
                # 计算文件大小
                threshold_dir = os.path.join(self.output_dir, threshold)
                threshold_size = 0
                if os.path.exists(threshold_dir):
                    for file in os.listdir(threshold_dir):
                        if file.endswith('.csv'):
                            file_path = os.path.join(threshold_dir, file)
                            threshold_size += os.path.getsize(file_path)
                total_size_bytes += threshold_size
                
                processing_stats[threshold] = {
                    'etfs_processed': threshold_etfs,
                    'files_saved': threshold_etfs,
                    'calculation_time': calculation_time,
                    'save_time': save_time,
                    'etfs_per_second': threshold_etfs / calculation_time if calculation_time > 0 else 0
                }
                
                print(f"✅ {threshold}: 历史数据计算和保存完成")
                print(f"   ⚡ 计算时间: {calculation_time:.2f}秒")
                print(f"   💾 保存时间: {save_time:.2f}秒") 
                print(f"   🚀 计算速度: {processing_stats[threshold]['etfs_per_second']:.1f} ETF/秒")
            
            end_time = datetime.now()
            total_time = (end_time - start_time).total_seconds()
            
            # 构建返回结果
            result = {
                'success': True,
                'total_etfs_processed': total_etfs_processed,
                'thresholds_processed': len([t for t in thresholds if t in processing_stats]),
                'processing_time_seconds': total_time,
                'etfs_per_second': total_etfs_processed / total_time if total_time > 0 else 0,
                'output_directory': self.output_dir,
                'save_statistics': {
                    'total_files_saved': total_files_saved,
                    'total_size_bytes': total_size_bytes
                },
                'processing_details': processing_stats
            }
            
            print(f"\n🎉 超高性能历史批量计算完成！")
            print(f"📊 总体统计:")
            print(f"   📁 处理ETF数: {total_etfs_processed}")
            print(f"   📁 处理门槛数: {result['thresholds_processed']}")
            print(f"   ⏱️  总处理时间: {total_time:.2f}秒")
            print(f"   🚀 平均处理速度: {result['etfs_per_second']:.1f} ETF/秒")
            print(f"   💾 保存文件: {total_files_saved}")
            print(f"   📊 文件大小: {total_size_bytes/1024/1024:.2f}MB")
            
            return result
            
        except Exception as e:
            error_message = f"超高性能历史批量计算失败: {str(e)}"
            print(f"❌ {error_message}")
            
            return {
                'success': False,
                'message': error_message,
                'total_etfs_processed': 0
            }
    
    def calculate_and_save_historical_wma(self, etf_codes: Optional[List[str]] = None, 
                                        thresholds: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        计算并保存完整历史WMA数据 - 兼容旧版本接口，使用缓存集成
        
        Args:
            etf_codes: ETF代码列表，None则处理所有可用ETF
            thresholds: 门槛列表，默认["3000万门槛", "5000万门槛"]
            
        Returns:
            Dict[str, Any]: 处理结果统计
        """
        # 重定向到新的缓存集成版本
        return self.calculate_historical_batch(etf_codes, thresholds)
    
    def quick_analysis(self, etf_code: str, include_historical: bool = False) -> Optional[Dict]:
        """
        快速分析单个ETF - 增强版本
        
        Args:
            etf_code: ETF代码（如"510050.SH"或"510050"）
            include_historical: 是否包含历史数据分析
            
        Returns:
            Dict: 分析结果或None
        """
        print(f"🔍 快速分析: {etf_code}")
        
        # 处理单个ETF
        result = self.etf_processor.process_single_etf(etf_code, include_advanced_analysis=True)
        
        if not result:
            print(f"❌ {etf_code}: 分析失败")
            return None
        
        # 显示基础结果
        self.result_processor.display_results([result])
        
        # 如果需要历史数据分析
        if include_historical:
            print(f"\n🚀 {etf_code}: 开始历史数据分析...")
            
            # 读取完整数据
            data_result = self.data_reader.read_etf_data(etf_code)
            if data_result is not None:
                df, _ = data_result
                if not df.empty:
                    # 计算完整历史WMA
                    historical_df = self.historical_calculator.calculate_full_historical_wma_optimized(df, etf_code)
                    
                    if historical_df is not None:
                        # 添加历史数据到结果
                        result['historical_analysis'] = {
                            'total_history_days': len(historical_df),
                            'valid_wma_days': historical_df[f'WMA{max(self.config.wma_periods)}'].notna().sum(),
                            'earliest_date': historical_df['date'].min(),
                            'latest_date': historical_df['date'].max(),
                            'historical_trend_summary': self._analyze_historical_trend(historical_df)
                        }
                        
                        print(f"✅ {etf_code}: 历史数据分析完成")
                        print(f"   📊 历史数据: {result['historical_analysis']['total_history_days']}天")
                        print(f"   📈 有效WMA: {result['historical_analysis']['valid_wma_days']}天")
                        print(f"   📅 日期范围: {result['historical_analysis']['earliest_date']} 至 {result['historical_analysis']['latest_date']}")
        
        return result
    
    def _analyze_historical_trend(self, historical_df: pd.DataFrame) -> Dict:
        """
        分析历史趋势 - 统一使用下划线字段格式
        
        Args:
            historical_df: 历史WMA数据
            
        Returns:
            Dict: 趋势分析结果
        """
        try:
            # 获取最近30天的数据（如果有的话）
            recent_data = historical_df.head(30)  # 按时间倒序，head是最新的
            
            if len(recent_data) < 10:
                return {'analysis': '数据不足，无法进行趋势分析'}
            
            # 统一使用下划线格式的差值字段
            diff_col = 'WMA_DIFF_5_20'
            
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
            
            return {'analysis': '趋势分析数据不完整，找不到WMA_DIFF_5_20列'}
            
        except Exception as e:
            return {'analysis': f'趋势分析失败: {str(e)}', 'error': str(e)}
    
    def _get_result_stats(self, results: List[Dict]) -> Dict:
        """获取结果统计 - 保持原有统计逻辑"""
        if not results:
            return {}
        
        return {
            'total_results': len(results),
            'successful_calculations': len([r for r in results if r.get('wma_values')]),
            'data_sources': {
                'cache_hits': len([r for r in results if r.get('data_source') == 'cache_hit']),
                'fresh_calculations': len([r for r in results if r.get('data_source') == 'fresh_calculation'])
            }
        }
    
    def get_system_status(self) -> Dict:
        """获取系统状态 - 保持原有功能"""
        try:
            status = {
                'system_info': {
                    'version': '2.0.0',
                    'config': f'WMA系统 - {self.config.adj_type}',
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
                    'WMA Engine': 'Ready',
                    'ETF Processor': 'Ready',
                    'Batch Processor': 'Ready',
                    'Cache Manager': 'Ready' if self.enable_cache else 'Disabled'
                }
            }
            
            return status
            
        except Exception as e:
            return {'error': f'系统状态检查失败: {str(e)}'} 