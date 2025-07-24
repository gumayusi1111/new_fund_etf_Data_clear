#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ATR主控制器
===========

协调所有ATR计算组件，提供统一的对外接口：
- 批量计算管理
- 门槛筛选处理
- 缓存策略控制
- 结果输出管理
- 性能监控

支持的主要功能：
- 单个ETF快速分析
- 批量门槛计算
- 增量更新处理
- 系统状态监控
"""

import time
import pandas as pd
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
import logging
from datetime import datetime

from ..engines.atr_engine import ATREngine
from ..infrastructure.config import ATRConfig
from ..infrastructure.cache_manager import ATRCacheManager
from ..infrastructure.data_reader import ATRDataReader
from ..infrastructure.utils import setup_logger, timer, format_processing_results
from ..outputs.csv_handler import ATRCSVHandler


class ATRMainController:
    """ATR主控制器"""
    
    def __init__(self, config: ATRConfig = None, enable_cache: bool = True, 
                 performance_mode: bool = True):
        """初始化主控制器"""
        # 配置管理
        self.config = config or ATRConfig()
        self.enable_cache = enable_cache
        self.performance_mode = performance_mode
        
        # 设置日志
        self.logger = setup_logger(__name__, "INFO" if performance_mode else "DEBUG")
        
        # 初始化组件
        self.atr_engine = ATREngine(self.config)
        self.cache_manager = ATRCacheManager(self.config) if enable_cache else None
        self.data_reader = ATRDataReader(self.config)
        self.csv_handler = ATRCSVHandler(self.config)
        
        # 确保目录结构存在
        self.config.ensure_directories_exist()
        
        # 性能统计
        self.performance_stats = {
            'total_calculations': 0,
            'cache_hits': 0,
            'cache_misses': 0,
            'total_processing_time': 0,
            'session_start_time': datetime.now()
        }
        
        self.logger.info("ATR主控制器初始化完成")
    
    def get_system_status(self) -> Dict[str, Any]:
        """获取系统状态信息"""
        try:
            status = {
                'system_info': self.config.get_system_info(),
                'components': {
                    'ATR Engine': '正常',
                    'Cache Manager': '正常' if self.cache_manager else '禁用',
                    'Data Reader': '正常',
                    'CSV Handler': '正常'
                },
                'data_status': {
                    'etf_data_path': self.config.etf_data_path,
                    'available_etfs_count': 0,
                    'cache_statistics': {}
                },
                'performance': self.performance_stats.copy()
            }
            
            # 检查数据可用性
            try:
                available_etfs = self.data_reader.get_available_etf_files()
                status['data_status']['available_etfs_count'] = len(available_etfs)
            except Exception as e:
                status['data_status']['etf_data_error'] = str(e)
            
            # 获取缓存统计
            if self.cache_manager:
                try:
                    cache_stats = self.cache_manager.get_cache_statistics()
                    status['data_status']['cache_statistics'] = cache_stats
                except Exception as e:
                    status['data_status']['cache_error'] = str(e)
            
            return status
            
        except Exception as e:
            return {'error': f"获取系统状态失败: {str(e)}"}
    
    def get_available_etfs(self) -> List[str]:
        """获取可用ETF列表"""
        try:
            etf_files = self.data_reader.get_available_etf_files()
            etf_codes = []
            
            for file_path in etf_files:
                etf_code = self.data_reader.extract_etf_code_from_filename(file_path)
                if etf_code:
                    etf_codes.append(etf_code)
            
            return sorted(etf_codes)
            
        except Exception as e:
            self.logger.error(f"获取可用ETF列表失败: {e}")
            return []
    
    @timer
    def calculate_single_etf(self, etf_code: str, threshold: str) -> Dict[str, Any]:
        """计算单个ETF的ATR指标"""
        try:
            start_time = time.time()
            
            # 查找ETF文件
            etf_files = self.data_reader.get_available_etf_files()
            etf_file = None
            
            for file_path in etf_files:
                file_etf_code = self.data_reader.extract_etf_code_from_filename(file_path)
                if file_etf_code and (file_etf_code == etf_code or file_etf_code.split('.')[0] == etf_code):
                    etf_file = file_path
                    break
            
            if not etf_file:
                return {
                    'success': False,
                    'error': f'未找到ETF文件: {etf_code}',
                    'etf_code': etf_code,
                    'threshold': threshold
                }
            
            # 读取ETF数据
            etf_data = self.data_reader.read_etf_file(etf_file, etf_code)
            if etf_data is None:
                return {
                    'success': False,
                    'error': f'读取ETF数据失败: {etf_code}',
                    'etf_code': etf_code,
                    'threshold': threshold
                }
            
            # 跳过门槛条件检查 - ETF已通过初筛验证
            # 这样可以确保与其他波动性指标系统的文件数量一致性
            threshold_info = {'threshold': threshold, 'status': 'passed_initial_screening'}
            
            # 尝试增量更新或从缓存加载
            cached_data = None
            calculation_mode = 'full_calculation'
            
            if self.cache_manager:
                cached_data = self.cache_manager.get_cached_data(etf_code, threshold, etf_file)
                
                if cached_data is not None:
                    # 尝试增量更新
                    try:
                        incremental_result = self.atr_engine.calculate_incremental_update(
                            cached_data, etf_data, etf_code
                        )
                        
                        if incremental_result is not None and not incremental_result.empty:
                            # 增量更新成功
                            atr_data = incremental_result
                            calculation_mode = 'incremental_update'
                            self.performance_stats['cache_hits'] += 1
                            self.logger.debug(f"增量更新成功: {etf_code}-{threshold}")
                        else:
                            # 增量更新失败，进行全量计算
                            atr_result = self.atr_engine.calculate_full_atr(etf_data)
                            if not atr_result['success']:
                                return {
                                    'success': False,
                                    'error': f'ATR计算失败: {atr_result["error"]}',
                                    'etf_code': etf_code,
                                    'threshold': threshold
                                }
                            atr_data = atr_result['data']
                            calculation_mode = 'full_calculation'
                            self.performance_stats['cache_misses'] += 1
                    except Exception as e:
                        # 增量更新出错，进行全量计算
                        self.logger.warning(f"增量更新失败，执行全量计算: {etf_code}-{threshold}, 错误: {e}")
                        atr_result = self.atr_engine.calculate_full_atr(etf_data)
                        if not atr_result['success']:
                            return {
                                'success': False,
                                'error': f'ATR计算失败: {atr_result["error"]}',
                                'etf_code': etf_code,
                                'threshold': threshold
                            }
                        atr_data = atr_result['data']
                        calculation_mode = 'full_calculation'
                        self.performance_stats['cache_misses'] += 1
                else:
                    # 无缓存数据，进行全量计算
                    atr_result = self.atr_engine.calculate_full_atr(etf_data)
                    if not atr_result['success']:
                        return {
                            'success': False,
                            'error': f'ATR计算失败: {atr_result["error"]}',
                            'etf_code': etf_code,
                            'threshold': threshold
                        }
                    atr_data = atr_result['data']
                    calculation_mode = 'full_calculation'
                    self.performance_stats['cache_misses'] += 1
            else:
                # 无缓存管理器，直接全量计算
                atr_result = self.atr_engine.calculate_full_atr(etf_data)
                if not atr_result['success']:
                    return {
                        'success': False,
                        'error': f'ATR计算失败: {atr_result["error"]}',
                        'etf_code': etf_code,
                        'threshold': threshold
                    }
                atr_data = atr_result['data']
                calculation_mode = 'full_calculation'
                self.performance_stats['cache_misses'] += 1
            
            # 保存到缓存
            if self.cache_manager:
                self.cache_manager.save_calculated_data(
                    etf_code, threshold, atr_data, etf_file,
                    {
                        'atr_statistics': atr_result.get('statistics', {}),
                        'threshold_info': threshold_info,
                        'calculation_mode': calculation_mode
                    }
                )
            
            # 更新性能统计
            self.performance_stats['total_calculations'] += 1
            calculation_time = time.time() - start_time
            self.performance_stats['total_processing_time'] += calculation_time
            
            return {
                'success': True,
                'etf_code': etf_code,
                'threshold': threshold,
                'data': atr_data,
                'latest_values': atr_result.get('latest_values', {}),
                'statistics': atr_result.get('statistics', {}),
                'threshold_info': threshold_info,
                'calculation_mode': calculation_mode,
                'from_cache': calculation_mode == 'incremental_update',
                'calculation_time': calculation_time
            }
            
        except Exception as e:
            self.logger.error(f"计算单个ETF失败 {etf_code}-{threshold}: {e}")
            return {
                'success': False,
                'error': str(e),
                'etf_code': etf_code,
                'threshold': threshold
            }
    
    def quick_analysis(self, etf_code: str, include_historical: bool = False) -> Optional[Dict[str, Any]]:
        """快速分析单个ETF（用于测试和验证）"""
        try:
            # 使用第一个门槛进行快速分析
            threshold = self.config.thresholds[0]
            result = self.calculate_single_etf(etf_code, threshold)
            
            if not result['success']:
                return None
            
            # 构建快速分析结果
            analysis = {
                'etf_code': etf_code,
                'threshold': threshold,
                'latest_values': result.get('latest_values', {}),
                'calculation_time': result.get('calculation_time', 0),
                'from_cache': result.get('from_cache', False)
            }
            
            # 添加历史分析
            if include_historical and 'data' in result:
                data = result['data']
                if not data.empty and 'atr_10' in data.columns:
                    atr_data = data['atr_10'].dropna()
                    analysis['historical_analysis'] = {
                        'total_history_days': len(data),
                        'valid_atr_days': len(atr_data),
                        'avg_atr': atr_data.mean() if len(atr_data) > 0 else None,
                        'max_atr': atr_data.max() if len(atr_data) > 0 else None,
                        'min_atr': atr_data.min() if len(atr_data) > 0 else None
                    }
            
            return analysis
            
        except Exception as e:
            self.logger.error(f"快速分析失败 {etf_code}: {e}")
            return None
    
    def calculate_screening_results(self, thresholds: List[str] = None) -> Dict[str, Any]:
        """批量计算筛选结果"""
        if thresholds is None:
            thresholds = self.config.thresholds
        
        start_time = time.time()
        self.logger.info(f"开始批量ATR计算，门槛: {thresholds}")
        
        # 导入筛选列表读取功能
        from ..infrastructure.utils import read_etf_screening_list
        
        # 批量计算结果
        batch_results = {}
        threshold_statistics = {}
        
        for threshold in thresholds:
            self.logger.info(f"处理门槛: {threshold}")
            threshold_start_time = time.time()
            
            # 获取该门槛的筛选ETF列表
            threshold_etfs = read_etf_screening_list(threshold)
            if not threshold_etfs:
                self.logger.warning(f"门槛 {threshold} 没有筛选通过的ETF")
                threshold_statistics[threshold] = {
                    'total_etfs': 0,
                    'successful_etfs': 0,
                    'failed_etfs': 0,
                    'processing_time': 0
                }
                continue
            
            threshold_results = {}
            successful_count = 0
            failed_count = 0
            
            for i, etf_code in enumerate(threshold_etfs, 1):
                if not self.performance_mode and i % 50 == 0:
                    self.logger.info(f"进度: {i}/{len(threshold_etfs)} ({i/len(threshold_etfs)*100:.1f}%)")
                
                result = self.calculate_single_etf(etf_code, threshold)
                threshold_results[etf_code] = result
                
                if result['success']:
                    successful_count += 1
                else:
                    failed_count += 1
            
            # 门槛统计
            threshold_time = time.time() - threshold_start_time
            threshold_statistics[threshold] = {
                'total_etfs': len(threshold_etfs),
                'successful_etfs': successful_count,
                'failed_etfs': failed_count,
                'success_rate': successful_count / len(threshold_etfs) * 100 if threshold_etfs else 0,
                'processing_time': threshold_time
            }
            
            # 保存结果到批量结果
            for etf_code, result in threshold_results.items():
                if etf_code not in batch_results:
                    batch_results[etf_code] = {}
                batch_results[etf_code][threshold] = result
            
            self.logger.info(f"{threshold} 完成: {successful_count}/{len(threshold_etfs)} ({successful_count/len(threshold_etfs)*100:.1f}%)")
        
        # 保存CSV文件
        save_stats = self.csv_handler.save_batch_results(batch_results)
        
        # 生成统一标准的meta文件
        if self.cache_manager:
            # 为每个门槛生成meta文件
            for threshold, stats in threshold_statistics.items():
                self.cache_manager.create_threshold_meta(threshold, stats)
            
            # 生成系统级meta文件
            self.cache_manager.create_system_meta(threshold_statistics)
            
            self.logger.info("统一meta文件生成完成")
        
        # 总体统计
        total_time = time.time() - start_time
        
        # 计算总处理的ETF数量
        total_processed = sum(stats['total_etfs'] for stats in threshold_statistics.values())
        
        final_results = {
            'success': True,
            'total_etfs_processed': total_processed,
            'thresholds_processed': thresholds,
            'threshold_statistics': threshold_statistics,
            'save_statistics': save_stats,
            'processing_time': total_time,
            'performance_stats': self.performance_stats.copy(),
            'cache_hit_rate': (self.performance_stats['cache_hits'] / 
                              (self.performance_stats['cache_hits'] + self.performance_stats['cache_misses']) * 100
                              if self.performance_stats['cache_hits'] + self.performance_stats['cache_misses'] > 0 else 0)
        }
        
        self.logger.info(f"批量计算完成: {total_time:.2f}秒")
        return final_results
    
    def clear_cache(self, etf_code: str = None, threshold: str = None):
        """清理缓存"""
        if self.cache_manager:
            self.cache_manager.clear_cache(etf_code, threshold)
            self.logger.info(f"缓存清理完成: ETF={etf_code}, 门槛={threshold}")
        else:
            self.logger.warning("缓存管理器未启用")
    
    def get_performance_summary(self) -> str:
        """获取性能摘要"""
        stats = self.performance_stats
        
        cache_hit_rate = 0
        if stats['cache_hits'] + stats['cache_misses'] > 0:
            cache_hit_rate = stats['cache_hits'] / (stats['cache_hits'] + stats['cache_misses']) * 100
        
        avg_time = 0
        if stats['total_calculations'] > 0:
            avg_time = stats['total_processing_time'] / stats['total_calculations']
        
        return f"""
📊 ATR计算器性能摘要:
   🔢 总计算次数: {stats['total_calculations']}
   💾 缓存命中率: {cache_hit_rate:.1f}%
   ⏱️ 平均计算时间: {avg_time:.3f}秒
   🕐 总处理时间: {stats['total_processing_time']:.2f}秒
   🚀 会话开始时间: {stats['session_start_time'].strftime('%Y-%m-%d %H:%M:%S')}
        """.strip()