#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
批量处理器
=========

负责ETF批量处理和智能缓存逻辑
"""

import os
import pandas as pd
from datetime import datetime
from typing import Dict, List, Optional, Set
from .etf_processor import ETFProcessor
from ..infrastructure.cache_manager import SMACacheManager
from ..outputs.csv_handler import CSVOutputHandler


class BatchProcessor:
    """批量处理器 - 负责ETF批量处理和缓存管理"""
    
    def __init__(self, etf_processor: ETFProcessor, cache_manager: Optional[SMACacheManager] = None, 
                 csv_handler: Optional[CSVOutputHandler] = None, enable_cache: bool = True):
        """
        初始化批量处理器
        
        Args:
            etf_processor: ETF处理器
            cache_manager: 缓存管理器
            csv_handler: CSV处理器
            enable_cache: 是否启用缓存
        """
        self.etf_processor = etf_processor
        self.cache_manager = cache_manager
        self.csv_handler = csv_handler
        self.enable_cache = enable_cache
    
    def process_etf_list(self, etf_codes: List[str], threshold: str, 
                        include_advanced_analysis: bool = False) -> List[Dict]:
        """
        批量处理ETF列表
        
        Args:
            etf_codes: ETF代码列表
            threshold: 门槛类型
            include_advanced_analysis: 是否包含高级分析
            
        Returns:
            List[Dict]: 处理结果列表
        """
        if self.enable_cache and self.cache_manager:
            return self._process_with_cache(etf_codes, threshold, include_advanced_analysis)
        else:
            return self._process_without_cache(etf_codes, include_advanced_analysis)
    
    def _process_with_cache(self, etf_codes: List[str], threshold: str, 
                           include_advanced_analysis: bool) -> List[Dict]:
        """缓存模式处理"""
        print(f"\n============================================================")
        print(f"🔄 处理{threshold}筛选结果 (智能缓存模式)")
        print(f"============================================================")
        
        # 分析ETF变化
        analysis = self.cache_manager.analyze_etf_changes(etf_codes, threshold)
        
        results = []
        processing_stats = {
            'total_processed': len(etf_codes),
            'cache_hits': 0,
            'incremental_updates': 0,
            'new_calculations': 0,
            'success_count': 0,
            'failed_count': 0,
            'cache_hit_rate': 0
        }
        
        # 处理相同ETF（增量计算）
        if analysis['same_etfs']:
            print(f"\n🔄 增量处理 {len(analysis['same_etfs'])} 个相同ETF...")
            for etf_code in analysis['same_etfs']:
                result = self._process_cached_etf(etf_code, threshold)
                if result:
                    results.append(result)
                    processing_stats['success_count'] += 1
                    if result.get('data_source') == 'cache':
                        processing_stats['cache_hits'] += 1
                    else:
                        processing_stats['incremental_updates'] += 1
                else:
                    processing_stats['failed_count'] += 1
        
        # 处理新增ETF（全量计算）
        if analysis['new_etfs']:
            print(f"\n🆕 全量处理 {len(analysis['new_etfs'])} 个新增ETF...")
            for etf_code in analysis['new_etfs']:
                result = self._process_new_etf(etf_code, threshold, include_advanced_analysis)
                if result:
                    results.append(result)
                    processing_stats['success_count'] += 1
                    processing_stats['new_calculations'] += 1
                else:
                    processing_stats['failed_count'] += 1
        
        # 计算缓存命中率（安全除法）
        total_processed = processing_stats['total_processed']
        cache_hits = processing_stats['cache_hits']
        
        if total_processed > 0 and cache_hits >= 0:
            processing_stats['cache_hit_rate'] = cache_hits / total_processed
        else:
            processing_stats['cache_hit_rate'] = 0.0
        
        # 更新门槛Meta
        self.cache_manager.update_threshold_meta(threshold, analysis, processing_stats)
        
        print(f"\n📊 处理统计:")
        print(f"   💾 缓存命中: {processing_stats['cache_hits']} 个")
        print(f"   ⚡ 增量更新: {processing_stats['incremental_updates']} 个") 
        print(f"   🆕 新增计算: {processing_stats['new_calculations']} 个")
        print(f"   ✅ 成功: {processing_stats['success_count']} 个")
        print(f"   ❌ 失败: {processing_stats['failed_count']} 个")
        print(f"   📈 缓存命中率: {processing_stats['cache_hit_rate']:.1%}")
        
        return results
    
    def _process_without_cache(self, etf_codes: List[str], include_advanced_analysis: bool) -> List[Dict]:
        """无缓存模式处理"""
        print(f"\n🔄 处理 {len(etf_codes)} 个ETF (无缓存模式)...")
        
        results = []
        for i, etf_code in enumerate(etf_codes, 1):
            print(f"   [{i}/{len(etf_codes)}] 🔄 {etf_code}: 计算中...")
            
            result = self.etf_processor.process_single_etf(etf_code, include_advanced_analysis)
            if result:
                results.append(result)
                print(f"   [{i}/{len(etf_codes)}] ✅ {etf_code}: 计算完成")
            else:
                print(f"   [{i}/{len(etf_codes)}] ❌ {etf_code}: 计算失败")
        
        return results
    
    def _process_cached_etf(self, etf_code: str, threshold: str) -> Optional[Dict]:
        """处理缓存中的ETF"""
        try:
            # 尝试从缓存加载
            cached_result = self._load_from_cache(etf_code, threshold)
            if cached_result:
                latest_date = self.cache_manager.get_cached_etf_latest_date(etf_code, threshold)
                print(f"   💾 {etf_code}: 使用缓存 (最新: {latest_date})")
                return cached_result
            
            # 缓存失效，进行增量更新
            result = self._process_incremental_etf(etf_code, threshold)
            if result:
                print(f"   ⚡ {etf_code}: 增量更新完成")
                return result
            
            return None
            
        except Exception as e:
            print(f"   ❌ {etf_code}: 缓存处理失败: {str(e)}")
            return None
    
    def _process_new_etf(self, etf_code: str, threshold: str, include_advanced_analysis: bool) -> Optional[Dict]:
        """处理新ETF"""
        try:
            # 全量计算
            result = self.etf_processor.process_single_etf(etf_code, include_advanced_analysis)
            
            if result and result.get('historical_data') is not None:
                # 保存到缓存
                historical_data = result['historical_data']
                success = self.cache_manager.save_etf_cache(etf_code, historical_data, threshold)
                if not success:
                    print(f"   ⚠️ {etf_code}: 缓存保存失败")
            
            return result
            
        except Exception as e:
            print(f"   ❌ {etf_code}: 新ETF处理失败: {str(e)}")
            return None
    
    def _process_incremental_etf(self, etf_code: str, threshold: str) -> Optional[Dict]:
        """增量更新ETF"""
        try:
            # 正常计算SMA
            result = self.etf_processor.process_single_etf(etf_code)
            
            if result and result.get('historical_data') is not None:
                # 增量更新缓存
                historical_data = result['historical_data']
                success = self.cache_manager.save_etf_cache(etf_code, historical_data, threshold)
                if not success:
                    print(f"   ⚠️ {etf_code}: 缓存更新失败")
            
            return result
            
        except Exception as e:
            print(f"   ❌ {etf_code}: 增量更新失败: {str(e)}")
            return None
    
    def _load_from_cache(self, etf_code: str, threshold: str) -> Optional[Dict]:
        """从缓存加载ETF结果"""
        try:
            cached_df = self.cache_manager.load_cached_etf_data(etf_code, threshold)
            
            if cached_df is None or cached_df.empty:
                return None
            
            # 从缓存数据构建结果对象（安全访问）
            if len(cached_df) == 0:
                return None
                
            latest_row = cached_df.iloc[0]  # 第一行是最新数据
            
            # 构建最新价格信息
            latest_price = {
                'date': str(latest_row['date']),
                'close': float(latest_row['SMA_5']) if pd.notna(latest_row['SMA_5']) else 0.0,
                'change_pct': 0.0  # 从缓存暂时无法计算涨跌幅
            }
            
            # 构建SMA值
            sma_values = {}
            for period in self.etf_processor.config.sma_periods:
                sma_col = f'SMA_{period}'
                if sma_col in cached_df.columns:
                    sma_val = latest_row[sma_col]
                    if pd.notna(sma_val):
                        sma_values[f'SMA_{period}'] = float(sma_val)
            
            # 差值指标
            diff_cols = {
                'SMA_DIFF_5_20': 'SMA_DIFF_5_20',
                'SMA_DIFF_5_10': 'SMA_DIFF_5_10', 
                'SMA_DIFF_5_20_PCT': 'SMA_DIFF_5_20_PCT'
            }
            
            for cache_col, result_key in diff_cols.items():
                if cache_col in cached_df.columns:
                    diff_val = latest_row[cache_col]
                    if pd.notna(diff_val):
                        sma_values[result_key] = float(diff_val)
            
            # 构建结果对象
            result = {
                'etf_code': etf_code,
                'adj_type': self.etf_processor.config.adj_type,
                'latest_price': latest_price,
                'sma_values': sma_values,
                'signals': {'status': 'from_cache'},
                'processing_time': datetime.now().isoformat(),
                'data_source': 'cache',
                'historical_data': cached_df
            }
            
            return result
            
        except Exception as e:
            print(f"   ❌ {etf_code}: 缓存加载失败: {str(e)}")
            return None
    
    def save_results_to_files(self, results: List[Dict], output_dir: str, threshold: str) -> Dict:
        """保存结果到文件"""
        save_stats = {
            'files_saved': 0,
            'total_size': 0,
            'failed_saves': 0
        }
        
        print(f"\n📁 保存{threshold}结果...")
        
        # 创建输出目录
        threshold_dir = os.path.join(output_dir, threshold)
        os.makedirs(threshold_dir, exist_ok=True)
        
        # 为每个ETF保存历史数据文件
        for result in results:
            etf_code = result['etf_code']
            
            # 检查是否有历史数据
            if 'historical_data' in result and result['historical_data'] is not None:
                historical_data = result['historical_data']
                
                # 生成CSV文件
                clean_etf_code = etf_code.replace('.SH', '').replace('.SZ', '')
                output_file = os.path.join(threshold_dir, f"{clean_etf_code}.csv")
                
                try:
                    # 保存历史数据文件
                    historical_data.to_csv(output_file, index=False, encoding='utf-8-sig')
                    
                    file_size = os.path.getsize(output_file)
                    save_stats['files_saved'] += 1
                    save_stats['total_size'] += file_size
                    
                    rows_count = len(historical_data)
                    print(f"   💾 {etf_code}: {clean_etf_code}.csv ({rows_count}行, {file_size} 字节)")
                    
                except Exception as e:
                    print(f"   ❌ {etf_code}: 保存失败 - {str(e)}")
                    save_stats['failed_saves'] += 1
            else:
                print(f"   ❌ {etf_code}: 无历史数据")
                save_stats['failed_saves'] += 1
        
        return save_stats 