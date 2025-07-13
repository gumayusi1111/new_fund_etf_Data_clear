#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
波动率批处理器
=============

处理批量ETF的波动率计算，集成缓存管理和增量更新
"""

import os
import pandas as pd
from datetime import datetime
from typing import Dict, List, Optional, Any
from ..infrastructure.config import VolatilityConfig
from ..infrastructure.cache_manager import VolatilityCacheManager
from .etf_processor import VolatilityETFProcessor


class VolatilityBatchProcessor:
    """波动率批处理器"""
    
    def __init__(self, etf_processor: VolatilityETFProcessor, 
                 cache_manager: Optional[VolatilityCacheManager],
                 config: VolatilityConfig, enable_cache: bool = True):
        """
        初始化批处理器
        
        Args:
            etf_processor: ETF处理器
            cache_manager: 缓存管理器
            config: 配置对象
            enable_cache: 是否启用缓存
        """
        self.etf_processor = etf_processor
        self.cache_manager = cache_manager
        self.config = config
        self.enable_cache = enable_cache
        
        if not config.performance_mode:
            print("📊 波动率批处理器初始化完成")
            print(f"   🗂️ 缓存: {'启用' if enable_cache else '禁用'}")
    
    def process_etf_list(self, etf_codes: List[str], threshold: Optional[str] = None,
                        include_advanced_analysis: bool = False) -> List[Dict]:
        """
        处理ETF列表
        
        Args:
            etf_codes: ETF代码列表
            threshold: 门槛类型
            include_advanced_analysis: 是否包含高级分析
            
        Returns:
            List[Dict]: 处理结果列表
        """
        print(f"📊 开始批量处理 {len(etf_codes)} 个ETF...")
        if threshold:
            print(f"   🎯 门槛: {threshold}")
        
        results = []
        cache_hits = 0
        fresh_calculations = 0
        incremental_updates = 0
        
        for i, etf_code in enumerate(etf_codes, 1):
            try:
                if not self.config.performance_mode:
                    print(f"[{i}/{len(etf_codes)}] 处理: {etf_code}")
                
                # 尝试从缓存加载
                result = None
                if self.enable_cache and self.cache_manager:
                    result = self._try_load_from_cache(etf_code, threshold)
                    if result:
                        result['data_source'] = 'cache'
                        cache_hits += 1
                        if not self.config.performance_mode:
                            print(f"   💾 缓存命中")
                
                # 如果缓存未命中，进行计算
                if result is None:
                    result = self.etf_processor.process_single_etf(etf_code, include_advanced_analysis)
                    if result:
                        result['data_source'] = 'fresh_calculation'
                        fresh_calculations += 1
                        
                        # 保存到缓存
                        if self.enable_cache and self.cache_manager:
                            self._save_to_cache(result, threshold)
                
                if result:
                    results.append(result)
                    if not self.config.performance_mode:
                        print(f"   ✅ 处理成功")
                else:
                    if not self.config.performance_mode:
                        print(f"   ❌ 处理失败")
                
            except Exception as e:
                print(f"   ❌ 处理异常 {etf_code}: {str(e)}")
                continue
        
        # 统计信息
        total_processed = len(results)
        success_rate = (total_processed / len(etf_codes)) * 100
        
        print(f"\n📊 批量处理完成:")
        print(f"   ✅ 成功: {total_processed}/{len(etf_codes)} ({success_rate:.1f}%)")
        print(f"   💾 缓存命中: {cache_hits}")
        print(f"   🔄 全新计算: {fresh_calculations}")
        print(f"   ⚡ 增量更新: {incremental_updates}")
        
        return results
    
    def process_screening_results(self, threshold: str) -> List[Dict]:
        """
        处理筛选结果的ETF，支持增量更新
        
        Args:
            threshold: 门槛类型
            
        Returns:
            List[Dict]: 处理结果列表
        """
        print(f"📊 处理筛选结果: {threshold}")
        
        # 获取筛选的ETF列表
        etf_codes = self.etf_processor.data_reader.get_screening_etf_codes(threshold)
        
        if not etf_codes:
            print(f"   ❌ 未找到 {threshold} 的筛选结果")
            return []
        
        print(f"   📈 找到 {len(etf_codes)} 个ETF")
        
        # 批量处理（支持缓存和增量更新）
        return self.process_etf_list(etf_codes, threshold, include_advanced_analysis=False)
    
    def _try_load_from_cache(self, etf_code: str, threshold: Optional[str] = None) -> Optional[Dict]:
        """
        尝试从缓存加载结果
        
        Args:
            etf_code: ETF代码
            threshold: 门槛类型
            
        Returns:
            Dict: 缓存结果或None
        """
        try:
            if not self.cache_manager:
                return None
            
            # 获取源文件路径
            source_file_path = self.etf_processor.data_reader.get_etf_file_path(etf_code)
            if not source_file_path:
                return None
            
            # 检查缓存有效性
            is_valid, meta_data = self.cache_manager.check_cache_validity(
                etf_code, source_file_path, threshold
            )
            
            if not is_valid:
                return None
            
            # 加载缓存数据
            cached_df = self.cache_manager.load_cache(etf_code, threshold)
            if cached_df is None:
                return None
            
            # 检查是否需要增量更新
            if self._needs_incremental_update(cached_df, source_file_path):
                return self._perform_incremental_update(etf_code, cached_df, threshold)
            
            # 从缓存元数据构建结果
            if meta_data and 'volatility_values' in meta_data:
                return {
                    'etf_code': etf_code,
                    'volatility_values': meta_data['volatility_values'],
                    'metadata': meta_data.get('metadata', {}),
                    'calculation_date': meta_data.get('cache_created_time'),
                    'data_source': 'cache'
                }
            
            return None
            
        except Exception as e:
            if not self.config.performance_mode:
                print(f"   ⚠️ 缓存加载异常 {etf_code}: {str(e)}")
            return None
    
    def _needs_incremental_update(self, cached_df: pd.DataFrame, source_file_path: str) -> bool:
        """
        检查是否需要增量更新
        
        Args:
            cached_df: 缓存数据
            source_file_path: 源文件路径
            
        Returns:
            bool: 是否需要增量更新
        """
        try:
            # 读取源文件的最新数据
            source_df = pd.read_csv(source_file_path, encoding='utf-8')
            
            if source_df.empty:
                return False
            
            # 确保日期列格式一致
            if 'date' in cached_df.columns:
                cached_df['date'] = pd.to_datetime(cached_df['date'])
            if '日期' in source_df.columns:
                source_df['日期'] = pd.to_datetime(source_df['日期'])
            
            # 比较最新日期
            cached_latest = cached_df['date'].max() if 'date' in cached_df.columns else pd.Timestamp.min
            source_latest = source_df['日期'].max() if '日期' in source_df.columns else pd.Timestamp.min
            
            return source_latest > cached_latest
            
        except Exception:
            return False
    
    def _perform_incremental_update(self, etf_code: str, cached_df: pd.DataFrame,
                                   threshold: Optional[str] = None) -> Optional[Dict]:
        """
        执行增量更新
        
        Args:
            etf_code: ETF代码
            cached_df: 缓存数据
            threshold: 门槛类型
            
        Returns:
            Dict: 更新后的结果或None
        """
        try:
            # 读取新数据
            data_result = self.etf_processor.data_reader.read_etf_data(etf_code)
            if data_result is None:
                return None
            
            new_df, metadata = data_result
            
            # 执行增量更新计算
            from ..engines.historical_calculator import VolatilityHistoricalCalculator
            historical_calculator = VolatilityHistoricalCalculator(self.config)
            
            updated_df = historical_calculator.calculate_incremental_update(
                cached_df, new_df, etf_code
            )
            
            if updated_df is None:
                return None
            
            # 获取最新的波动率值
            latest_volatility_values = self._extract_latest_volatility_values(updated_df)
            
            # 构建结果
            result = {
                'etf_code': etf_code,
                'volatility_values': latest_volatility_values,
                'metadata': metadata,
                'calculation_date': datetime.now().isoformat(),
                'data_source': 'incremental_update'
            }
            
            # 更新缓存
            if self.cache_manager:
                source_file_path = self.etf_processor.data_reader.get_etf_file_path(etf_code)
                additional_meta = {'volatility_values': latest_volatility_values}
                
                self.cache_manager.save_cache(
                    etf_code, updated_df, source_file_path, threshold, additional_meta
                )
            
            return result
            
        except Exception as e:
            if not self.config.performance_mode:
                print(f"   ❌ 增量更新异常 {etf_code}: {str(e)}")
            return None
    
    def _extract_latest_volatility_values(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        从历史数据中提取最新的波动率值
        
        Args:
            df: 历史数据
            
        Returns:
            Dict: 最新波动率值
        """
        try:
            if df.empty:
                return {}
            
            latest_row = df.iloc[0]  # 最新数据在第一行
            volatility_values = {}
            
            # 提取波动率指标
            volatility_columns = [col for col in df.columns if any(
                keyword in col for keyword in ['Volatility_', 'Rolling_Vol_', 'Price_Range', 'Vol_']
            )]
            
            for col in volatility_columns:
                if col in latest_row and pd.notna(latest_row[col]):
                    volatility_values[col] = float(latest_row[col])
            
            return volatility_values
            
        except Exception as e:
            print(f"   ⚠️ 提取波动率值异常: {str(e)}")
            return {}
    
    def _save_to_cache(self, result: Dict, threshold: Optional[str] = None) -> bool:
        """
        保存结果到缓存
        
        Args:
            result: 计算结果
            threshold: 门槛类型
            
        Returns:
            bool: 是否保存成功
        """
        try:
            if not self.cache_manager:
                return False
            
            etf_code = result.get('etf_code')
            volatility_values = result.get('volatility_values', {})
            
            # 创建简化的缓存数据（只保存必要信息）
            cache_data = {
                'etf_code': etf_code,
                'calculation_date': result.get('calculation_date'),
                'volatility_values': volatility_values
            }
            
            cache_df = pd.DataFrame([cache_data])
            
            # 获取源文件路径
            source_file_path = self.etf_processor.data_reader.get_etf_file_path(etf_code)
            
            # 保存缓存
            additional_meta = {
                'volatility_values': volatility_values,
                'metadata': result.get('metadata', {})
            }
            
            return self.cache_manager.save_cache(
                etf_code, cache_df, source_file_path, threshold, additional_meta
            )
            
        except Exception as e:
            if not self.config.performance_mode:
                print(f"   ⚠️ 缓存保存异常: {str(e)}")
            return False
    
    def save_results_to_files(self, results: List[Dict], output_dir: str,
                            threshold: Optional[str] = None) -> Dict[str, Any]:
        """
        保存结果到文件
        
        Args:
            results: 结果列表
            output_dir: 输出目录
            threshold: 门槛类型
            
        Returns:
            Dict: 保存统计信息
        """
        try:
            if not results:
                return {'files_saved': 0, 'error': '无可保存的结果'}
            
            # 创建输出目录
            if threshold:
                final_output_dir = os.path.join(output_dir, threshold)
            else:
                final_output_dir = output_dir
            
            os.makedirs(final_output_dir, exist_ok=True)
            
            saved_count = 0
            total_size = 0
            
            for result in results:
                etf_code = result.get('etf_code', '')
                clean_code = etf_code.replace('.SH', '').replace('.SZ', '')
                
                output_file = os.path.join(final_output_dir, f"{clean_code}.csv")
                
                # 准备保存数据
                save_data = {
                    'ETF_Code': etf_code,
                    'Calculation_Date': result.get('calculation_date', ''),
                    'Data_Source': result.get('data_source', ''),
                    **result.get('volatility_values', {})
                }
                
                # 保存文件
                save_df = pd.DataFrame([save_data])
                save_df.to_csv(output_file, index=False, encoding='utf-8')
                
                # 统计信息
                file_size = os.path.getsize(output_file)
                total_size += file_size
                saved_count += 1
            
            return {
                'files_saved': saved_count,
                'total_files': len(results),
                'success_rate': (saved_count / len(results)) * 100,
                'total_size_kb': total_size / 1024,
                'output_directory': final_output_dir
            }
            
        except Exception as e:
            return {'files_saved': 0, 'error': f'保存异常: {str(e)}'}