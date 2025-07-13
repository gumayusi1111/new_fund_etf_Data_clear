#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
波动率主控制器
=============

基于第一大类标准的波动率主控制器
统一字段格式、缓存管理和批量处理
"""

import os
import pandas as pd
import numpy as np
from datetime import datetime
from typing import Dict, List, Optional, Any
from ..infrastructure.config import VolatilityConfig
from ..infrastructure.data_reader import VolatilityDataReader
from ..infrastructure.cache_manager import VolatilityCacheManager
from ..engines.volatility_engine import VolatilityEngine
from ..engines.historical_calculator import VolatilityHistoricalCalculator
from ..outputs.result_processor import VolatilityResultProcessor
from .etf_processor import VolatilityETFProcessor
from .batch_processor import VolatilityBatchProcessor


class VolatilityMainController:
    """波动率主控制器"""
    
    def __init__(self, adj_type: str = "前复权", volatility_periods: Optional[List[int]] = None, 
                 output_dir: Optional[str] = None, enable_cache: bool = True, 
                 performance_mode: bool = True, annualized: bool = True):
        """
        初始化波动率主控制器
        
        Args:
            adj_type: 复权类型，默认"前复权"
            volatility_periods: 波动率周期列表，默认[10, 20, 30, 60]
            output_dir: 输出目录，默认None（使用配置中的默认目录）
            enable_cache: 是否启用缓存，默认True
            performance_mode: 是否启用性能模式（关闭调试输出）
            annualized: 是否计算年化波动率
        """
        print("=" * 60)
        print("📊 波动率主控制器启动 - 基于第一大类标准")
        print("=" * 60)
        
        # 初始化配置
        self.config = VolatilityConfig(
            adj_type=adj_type, 
            volatility_periods=volatility_periods, 
            enable_cache=enable_cache, 
            performance_mode=performance_mode,
            annualized=annualized
        )
        self.output_dir = output_dir or self.config.default_output_dir
        self.enable_cache = enable_cache
        self.performance_mode = performance_mode
        
        # 初始化缓存管理器
        self.cache_manager = None
        if enable_cache:
            self.cache_manager = VolatilityCacheManager(self.config)
        
        # 初始化核心组件
        self.data_reader = VolatilityDataReader(self.config)
        self.volatility_engine = VolatilityEngine(self.config)
        self.historical_calculator = VolatilityHistoricalCalculator(self.config, self.cache_manager)
        self.result_processor = VolatilityResultProcessor(self.config)
        
        # 初始化处理器
        self.etf_processor = VolatilityETFProcessor(
            data_reader=self.data_reader,
            volatility_engine=self.volatility_engine,
            config=self.config
        )
        
        self.batch_processor = VolatilityBatchProcessor(
            etf_processor=self.etf_processor,
            cache_manager=self.cache_manager,
            config=self.config,
            enable_cache=enable_cache
        )
        
        print("🗂️ 智能缓存系统已启用" if enable_cache else "⚠️ 缓存系统已禁用")
        print("✅ 所有组件初始化完成")
        print("=" * 60)
    
    def get_available_etfs(self) -> List[str]:
        """获取可用的ETF代码列表"""
        return self.data_reader.get_available_etfs()
    
    def process_single_etf(self, etf_code: str, include_advanced_analysis: bool = False) -> Optional[Dict]:
        """
        处理单个ETF的波动率计算
        
        Args:
            etf_code: ETF代码
            include_advanced_analysis: 是否包含高级分析
            
        Returns:
            Dict: 计算结果或None
        """
        print(f"🔄 开始处理: {etf_code}")
        
        try:
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
        处理多个ETF的波动率计算
        
        Args:
            etf_codes: ETF代码列表
            include_advanced_analysis: 是否包含高级分析
            
        Returns:
            List[Dict]: 计算结果列表
        """
        print(f"📊 开始批量处理 {len(etf_codes)} 个ETF...")
        
        return self.batch_processor.process_etf_list(etf_codes, None, include_advanced_analysis)
    
    def calculate_and_save_screening_results(self, thresholds: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        计算并保存筛选结果的波动率数据 - 按第一大类标准
        
        Args:
            thresholds: 门槛列表，默认["3000万门槛", "5000万门槛"]
            
        Returns:
            Dict[str, Any]: 处理结果统计
        """
        thresholds = thresholds or ["3000万门槛", "5000万门槛"]
        
        print(f"📊 开始筛选结果波动率计算和保存...")
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
            'mode': 'complete_with_data_save'
        }
    
    def calculate_historical_batch(self, etf_codes: Optional[List[str]] = None, 
                                 thresholds: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        超高性能历史批量计算 - 按第一大类标准
        
        Args:
            etf_codes: ETF代码列表，None则处理所有可用ETF
            thresholds: 门槛列表，默认["3000万门槛", "5000万门槛"]
            
        Returns:
            Dict[str, Any]: 处理结果统计
        """
        thresholds = thresholds or ["3000万门槛", "5000万门槛"]
        
        print(f"🚀 开始历史波动率数据计算和保存...")
        print(f"📊 门槛设置: {thresholds}")
        
        all_stats = {}
        
        # 为每个门槛分别处理
        for threshold in thresholds:
            print(f"\n📈 计算门槛: {threshold}")
            
            # 获取该门槛的ETF列表
            if etf_codes is None:
                threshold_etf_codes = self.data_reader.get_screening_etf_codes(threshold)
                print(f"📊 {threshold}: 读取筛选结果...")
                print(f"📊 {threshold}: 找到 {len(threshold_etf_codes)} 个通过筛选的ETF")
            else:
                threshold_etf_codes = etf_codes
            
            print(f"📈 {threshold} 待处理ETF数量: {len(threshold_etf_codes)}")
            
            # 获取该门槛的ETF文件路径字典
            etf_files_dict = {}
            for etf_code in threshold_etf_codes:
                file_path = self.data_reader.get_etf_file_path(etf_code)
                if file_path and os.path.exists(file_path):
                    etf_files_dict[etf_code] = file_path
            
            print(f"📁 {threshold} 有效ETF文件数量: {len(etf_files_dict)}")
            
            # 批量计算历史波动率（支持缓存）
            results = self.historical_calculator.batch_calculate_historical_volatility(
                etf_files_dict, list(etf_files_dict.keys()), threshold
            )
            
            if results:
                # 保存历史数据文件
                save_stats = self.historical_calculator.save_historical_results(
                    results, self.output_dir, threshold
                )
                all_stats[threshold] = save_stats
                
                print(f"✅ {threshold}: 历史数据计算和保存完成")
            else:
                print(f"❌ {threshold}: 历史数据计算失败")
                all_stats[threshold] = {}
        
        # 计算总处理ETF数量
        total_etfs = sum(len(stats.get('etf_codes', [])) for stats in all_stats.values() if stats)
        
        return {
            'processing_statistics': all_stats,
            'total_etfs_processed': total_etfs,
            'thresholds_processed': thresholds
        }
    
    def quick_analysis(self, etf_code: str, include_historical: bool = False) -> Optional[Dict]:
        """
        快速分析单个ETF
        
        Args:
            etf_code: ETF代码
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
                    # 计算完整历史波动率
                    historical_df = self.historical_calculator.calculate_full_historical_volatility_optimized(df, etf_code)
                    
                    if historical_df is not None:
                        # 添加历史数据到结果
                        result['historical_analysis'] = {
                            'total_history_days': len(historical_df),
                            'valid_vol_days': historical_df[f'VOL_{max(self.config.volatility_periods)}'].notna().sum(),
                            'earliest_date': historical_df['date'].min() if 'date' in historical_df.columns else None,
                            'latest_date': historical_df['date'].max() if 'date' in historical_df.columns else None,
                            'historical_trend_summary': self._analyze_historical_trend(historical_df)
                        }
                        
                        print(f"✅ {etf_code}: 历史数据分析完成")
                        print(f"   📊 历史数据: {result['historical_analysis']['total_history_days']}天")
                        print(f"   📈 有效波动率: {result['historical_analysis']['valid_vol_days']}天")
        
        return result
    
    def _analyze_historical_trend(self, historical_df: pd.DataFrame) -> Dict:
        """分析历史趋势"""
        try:
            # 获取最近30天的数据
            recent_data = historical_df.tail(30)  # 最新在最后
            
            if len(recent_data) < 10:
                return {'analysis': '数据不足，无法进行趋势分析'}
            
            # 使用英文字段名（大写）
            ratio_col = 'VOL_RATIO_20_30'
            
            if ratio_col in recent_data.columns:
                ratio_values = recent_data[ratio_col].dropna()
                if len(ratio_values) >= 5:
                    high_vol_count = (ratio_values > 1.2).sum()
                    trend_strength = high_vol_count / len(ratio_values)
                    
                    if trend_strength >= 0.7:
                        trend_desc = "高波动趋势"
                    elif trend_strength >= 0.3:
                        trend_desc = "波动震荡"
                    else:
                        trend_desc = "低波动趋势"
                    
                    return {
                        'recent_trend': trend_desc,
                        'trend_strength': f"{trend_strength*100:.1f}%",
                        'recent_days_analyzed': len(ratio_values),
                        'latest_ratio': float(ratio_values.iloc[-1]) if len(ratio_values) > 0 else None,
                        'column_used': ratio_col
                    }
            
            return {'analysis': '趋势分析数据不完整'}
            
        except Exception as e:
            return {'analysis': f'趋势分析失败: {str(e)}', 'error': str(e)}
    
    def get_system_status(self) -> Dict:
        """获取系统状态"""
        try:
            status = {
                'system_info': {
                    'version': '1.0.0',
                    'config': f'波动率系统 - {self.config.adj_type}',
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
                    'Volatility Engine': 'Ready',
                    'ETF Processor': 'Ready',
                    'Batch Processor': 'Ready',
                    'Cache Manager': 'Ready' if self.enable_cache else 'Disabled'
                }
            }
            
            return status
            
        except Exception as e:
            return {'error': f'系统状态检查失败: {str(e)}'}