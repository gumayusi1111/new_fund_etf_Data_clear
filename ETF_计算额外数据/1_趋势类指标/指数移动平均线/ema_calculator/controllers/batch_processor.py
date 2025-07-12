#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
EMA批量处理器 - 重构版
====================

参照WMA/SMA系统的批量处理架构
支持智能缓存、增量更新和批量结果保存
"""

import os
from datetime import datetime
from typing import Dict, List, Optional, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
from ..infrastructure.config import EMAConfig
from ..infrastructure.cache_manager import EMACacheManager
from .etf_processor import EMAETFProcessor


class EMABatchProcessor:
    """EMA批量处理器 - 重构版（与WMA/SMA保持一致）"""
    
    def __init__(self, etf_processor: EMAETFProcessor, cache_manager: Optional[EMACacheManager], 
                 config: EMAConfig, enable_cache: bool = True):
        """
        初始化批量处理器
        
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
            print("📊 EMA批量处理器初始化完成")
            print(f"   🗂️ 缓存状态: {'启用' if enable_cache else '禁用'}")
    
    def process_etf_list(self, etf_codes: List[str], threshold: Optional[str] = None, 
                        include_advanced_analysis: bool = False, max_workers: int = 4) -> List[Dict]:
        """
        批量处理ETF列表
        
        Args:
            etf_codes: ETF代码列表
            threshold: 门槛类型（用于缓存管理）
            include_advanced_analysis: 是否包含高级分析
            max_workers: 最大并行工作线程数
            
        Returns:
            List[Dict]: 处理结果列表
        """
        try:
            if not self.config.performance_mode:
                print(f"📊 开始批量处理 {len(etf_codes)} 个ETF...")
            
            results = []
            processing_stats = {
                'cache_hits': 0,
                'incremental_updates': 0,
                'new_calculations': 0,
                'failed_count': 0,
                'cache_hit_rate': 0.0
            }
            
            # 如果启用缓存且有门槛信息，进行智能处理
            if self.enable_cache and self.cache_manager and threshold:
                results, processing_stats = self._process_with_cache(
                    etf_codes, threshold, include_advanced_analysis
                )
            else:
                # 不使用缓存的直接处理（支持并行）
                results = self._process_without_cache(etf_codes, include_advanced_analysis, max_workers)
                processing_stats['new_calculations'] = len([r for r in results if r.get('success', False)])
                processing_stats['failed_count'] = len([r for r in results if not r.get('success', False)])
            
            # 计算缓存命中率
            total_processed = len(etf_codes)
            if total_processed > 0:
                processing_stats['cache_hit_rate'] = processing_stats['cache_hits'] / total_processed
            
            # 更新处理统计
            if self.enable_cache and self.cache_manager and threshold:
                self.cache_manager.update_processing_stats(threshold, processing_stats)
            
            if not self.config.performance_mode:
                print(f"✅ 批量处理完成: {len(results)} 个结果")
                if self.enable_cache:
                    print(f"   💾 缓存命中: {processing_stats['cache_hits']}")
                    print(f"   ⚡ 增量更新: {processing_stats['incremental_updates']}")
                    print(f"   🔄 新计算: {processing_stats['new_calculations']}")
                    print(f"   📊 命中率: {processing_stats['cache_hit_rate']:.1%}")
            
            return results
            
        except Exception as e:
            print(f"❌ 批量处理失败: {str(e)}")
            return []
    
    def _process_with_cache(self, etf_codes: List[str], threshold: str, 
                          include_advanced_analysis: bool) -> tuple[List[Dict], Dict]:
        """
        使用缓存的批量处理
        
        Args:
            etf_codes: ETF代码列表
            threshold: 门槛类型
            include_advanced_analysis: 是否包含高级分析
            
        Returns:
            tuple: (处理结果列表, 处理统计)
        """
        results = []
        processing_stats = {
            'cache_hits': 0,
            'incremental_updates': 0,
            'new_calculations': 0,
            'failed_count': 0
        }
        
        # 分析ETF变化情况
        analysis = self.cache_manager.analyze_etf_changes(etf_codes, threshold)
        
        # 处理相同的ETF（检查缓存有效性）
        for etf_code in analysis['same_etfs']:
            if self._is_cache_valid(etf_code, threshold):
                # 缓存命中
                cached_result = self._load_from_cache(etf_code, threshold)
                if cached_result:
                    results.append(cached_result)
                    processing_stats['cache_hits'] += 1
                    continue
            
            # 缓存失效，重新计算
            result = self.etf_processor.process_single_etf(etf_code, include_advanced_analysis)
            if result:
                if result.get('success', False):
                    # 保存到缓存
                    self._save_to_cache(etf_code, result, threshold)
                    processing_stats['incremental_updates'] += 1
                else:
                    processing_stats['failed_count'] += 1
                results.append(result)
        
        # 处理新增的ETF（全量计算）
        for etf_code in analysis['new_etfs']:
            result = self.etf_processor.process_single_etf(etf_code, include_advanced_analysis)
            if result:
                if result.get('success', False):
                    # 保存到缓存
                    self._save_to_cache(etf_code, result, threshold)
                    processing_stats['new_calculations'] += 1
                else:
                    processing_stats['failed_count'] += 1
                results.append(result)
        
        # 更新处理统计信息
        if self.cache_manager and threshold:
            self.cache_manager.update_processing_stats(threshold, processing_stats)
        
        return results, processing_stats
    
    def _process_without_cache(self, etf_codes: List[str], 
                             include_advanced_analysis: bool, max_workers: int = 4) -> List[Dict]:
        """
        不使用缓存的批量处理（并行优化版）
        
        Args:
            etf_codes: ETF代码列表
            include_advanced_analysis: 是否包含高级分析
            max_workers: 最大并行工作线程数
            
        Returns:
            List[Dict]: 处理结果列表
        """
        results = []
        
        # 如果ETF数量较少或禁用性能模式，使用串行处理
        if len(etf_codes) < 10 or not self.config.performance_mode:
            for i, etf_code in enumerate(etf_codes, 1):
                if not self.config.performance_mode:
                    print(f"📊 进度: {i}/{len(etf_codes)} - {etf_code}")
                
                result = self.etf_processor.process_single_etf(etf_code, include_advanced_analysis)
                if result:
                    results.append(result)
            return results
        
        # 并行处理（适用于大批量数据）
        if not self.config.performance_mode:
            print(f"🚀 启用并行处理: {max_workers} 个工作线程")
        
        completed_count = 0
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # 提交所有任务
            future_to_etf = {
                executor.submit(self._process_single_etf_safe, etf_code, include_advanced_analysis): etf_code 
                for etf_code in etf_codes
            }
            
            # 收集结果
            for future in as_completed(future_to_etf):
                etf_code = future_to_etf[future]
                completed_count += 1
                
                try:
                    result = future.result()
                    if result:
                        results.append(result)
                    
                    if not self.config.performance_mode and completed_count % 10 == 0:
                        print(f"⚡ 并行进度: {completed_count}/{len(etf_codes)}")
                        
                except Exception as e:
                    if not self.config.performance_mode:
                        print(f"❌ {etf_code} 并行处理失败: {str(e)}")
        
        if not self.config.performance_mode:
            print(f"✅ 并行处理完成: {len(results)}/{len(etf_codes)}")
        
        return results
    
    def _process_single_etf_safe(self, etf_code: str, include_advanced_analysis: bool) -> Optional[Dict]:
        """
        安全的单ETF处理（用于并行调用）
        
        Args:
            etf_code: ETF代码
            include_advanced_analysis: 是否包含高级分析
            
        Returns:
            Optional[Dict]: 处理结果
        """
        try:
            return self.etf_processor.process_single_etf(etf_code, include_advanced_analysis)
        except Exception as e:
            return {
                'etf_code': etf_code,
                'success': False,
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }
    
    def _is_cache_valid(self, etf_code: str, threshold: str) -> bool:
        """
        检查缓存是否有效
        
        Args:
            etf_code: ETF代码
            threshold: 门槛类型
            
        Returns:
            bool: 缓存是否有效
        """
        try:
            if not self.cache_manager:
                return False
            
            # 获取源文件路径
            source_file_path = self.config.get_etf_file_path(etf_code)
            
            # 使用缓存管理器的验证方法
            return self.cache_manager.is_cache_valid(etf_code, threshold, source_file_path)
            
        except Exception as e:
            if not self.config.performance_mode:
                print(f"⚠️ {etf_code} 缓存有效性检查失败: {str(e)}")
            return False
    
    def _load_from_cache(self, etf_code: str, threshold: str) -> Optional[Dict]:
        """
        从缓存加载ETF结果
        
        Args:
            etf_code: ETF代码
            threshold: 门槛类型
            
        Returns:
            Optional[Dict]: 缓存的结果或None
        """
        try:
            if not self.cache_manager:
                return None
            
            cached_df = self.cache_manager.load_cached_etf_data(etf_code, threshold)
            if cached_df is None or cached_df.empty:
                return None
            
            # 从缓存数据重构结果
            latest_row = cached_df.iloc[0]  # 缓存数据按时间倒序，第一行是最新的
            
            # 提取EMA值
            ema_values = {}
            for period in self.config.ema_periods:
                ema_col = f'EMA{period}'
                if ema_col in cached_df.columns:
                    ema_values[f'ema_{period}'] = float(latest_row[ema_col])
            
            # 从源文件获取价格信息（缓存文件只包含EMA计算字段）
            try:
                source_data = self.etf_processor.data_reader.read_etf_data(etf_code)
                if source_data:
                    source_df, _ = source_data
                    latest_source_row = source_df.iloc[0]  # 最新数据
                    price_info = {
                        'latest_date': str(latest_row['date']),
                        'latest_price': float(latest_source_row['收盘价']),
                        'volume': int(latest_source_row['成交量']) if '成交量' in source_df.columns else 0,
                        'high': float(latest_source_row['最高价']) if '最高价' in source_df.columns else 0,
                        'low': float(latest_source_row['最低价']) if '最低价' in source_df.columns else 0,
                        'open': float(latest_source_row['开盘价']) if '开盘价' in source_df.columns else 0
                    }
                else:
                    # 如果无法读取源文件，使用默认值
                    price_info = {
                        'latest_date': str(latest_row['date']),
                        'latest_price': 0.0,
                        'volume': 0,
                        'high': 0.0,
                        'low': 0.0,
                        'open': 0.0
                    }
            except Exception as e:
                # 如果出错，使用默认值
                price_info = {
                    'latest_date': str(latest_row['date']),
                    'latest_price': 0.0,
                    'volume': 0,
                    'high': 0.0,
                    'low': 0.0,
                    'open': 0.0
                }
            
            # 构建结果
            result = {
                'etf_code': etf_code,
                'success': True,
                'price_info': price_info,
                'ema_values': ema_values,
                'signals': {'status': 'cached'},
                'total_rows': len(cached_df),
                'data_source': 'cache'
            }
            
            if not self.config.performance_mode:
                print(f"💾 {etf_code}: 从缓存加载")
            
            return result
            
        except Exception as e:
            if not self.config.performance_mode:
                print(f"❌ {etf_code} 缓存加载失败: {str(e)}")
            return None
    
    def _save_to_cache(self, etf_code: str, result: Dict, threshold: str) -> bool:
        """
        保存结果到缓存
        
        Args:
            etf_code: ETF代码
            result: 处理结果
            threshold: 门槛类型
            
        Returns:
            bool: 是否保存成功
        """
        try:
            if not self.cache_manager or not result.get('success', False):
                return False
            
            # 读取完整历史数据并计算EMA
            data_result = self.etf_processor.data_reader.read_etf_data(etf_code)
            if not data_result:
                return False
            
            df, _ = data_result
            
            # 使用EMA引擎计算完整历史数据
            full_ema_df = self.etf_processor.ema_engine.calculate_full_historical_ema(df, etf_code)
            if full_ema_df is None:
                return False
            
            # 保存到缓存
            return self.cache_manager.save_etf_cache(etf_code, full_ema_df, threshold)
            
        except Exception as e:
            if not self.config.performance_mode:
                print(f"❌ {etf_code} 缓存保存失败: {str(e)}")
            return False
    
    def save_results_to_files(self, results: List[Dict], output_base_dir: str, 
                            threshold: Optional[str] = None) -> Dict[str, Any]:
        """
        保存批量处理结果到文件
        
        Args:
            results: 处理结果列表
            output_base_dir: 输出基础目录
            threshold: 门槛类型
            
        Returns:
            Dict[str, Any]: 保存统计信息
        """
        try:
            if not results or not threshold:
                return {
                    'files_saved': 0,
                    'total_size': 0,
                    'failed_saves': 0
                }
            
            # 创建输出目录
            output_dir = os.path.join(output_base_dir, threshold)
            os.makedirs(output_dir, exist_ok=True)
            
            save_stats = {
                'files_saved': 0,
                'total_size': 0,
                'failed_saves': 0,
                'saved_files': []
            }
            
            for result in results:
                if not result.get('success', False):
                    continue
                
                etf_code = result['etf_code']
                
                try:
                    # 生成完整历史文件
                    saved_file = self._save_historical_file(etf_code, result, output_dir)
                    
                    if saved_file:
                        file_size = os.path.getsize(saved_file)
                        save_stats['files_saved'] += 1
                        save_stats['total_size'] += file_size
                        save_stats['saved_files'].append(os.path.basename(saved_file))
                        
                        if not self.config.performance_mode:
                            print(f"💾 {etf_code}: 历史文件已保存")
                    else:
                        save_stats['failed_saves'] += 1
                        
                except Exception as e:
                    save_stats['failed_saves'] += 1
                    if not self.config.performance_mode:
                        print(f"❌ {etf_code}: 文件保存失败 - {str(e)}")
            
            return save_stats
            
        except Exception as e:
            print(f"❌ 批量文件保存失败: {str(e)}")
            return {
                'files_saved': 0,
                'total_size': 0,
                'failed_saves': len(results),
                'error': str(e)
            }
    
    def _save_historical_file(self, etf_code: str, result: Dict, output_dir: str) -> Optional[str]:
        """
        保存单个ETF的历史文件
        
        Args:
            etf_code: ETF代码
            result: 处理结果
            output_dir: 输出目录
            
        Returns:
            Optional[str]: 保存的文件路径或None
        """
        try:
            # 读取完整历史数据
            data_result = self.etf_processor.data_reader.read_etf_data(etf_code)
            if not data_result:
                return None
            
            df, _ = data_result
            
            # 计算完整历史EMA
            full_ema_df = self.etf_processor.ema_engine.calculate_full_historical_ema(df, etf_code)
            if full_ema_df is None:
                return None
            
            # 保存文件
            clean_etf_code = etf_code.replace('.SH', '').replace('.SZ', '')
            filename = f"{clean_etf_code}.csv"
            file_path = os.path.join(output_dir, filename)
            
            full_ema_df.to_csv(file_path, index=False, encoding='utf-8')
            
            return file_path
            
        except Exception as e:
            if not self.config.performance_mode:
                print(f"❌ {etf_code} 历史文件生成失败: {str(e)}")
            return None