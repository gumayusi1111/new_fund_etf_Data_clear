#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
WMA批量处理器模块 - 重构版
=========================

参考SMA项目的批量处理架构，支持智能缓存和高性能批量处理
"""

import os
import pandas as pd
from datetime import datetime
from typing import Dict, List, Optional, Any
from ..infrastructure.config import WMAConfig
from ..infrastructure.cache_manager import WMACacheManager
from .etf_processor import WMAETFProcessor


class WMABatchProcessor:
    """WMA批量处理器 - 支持智能缓存的高性能批量处理"""
    
    def __init__(self, etf_processor: WMAETFProcessor, cache_manager: Optional[WMACacheManager],
                 config: WMAConfig, enable_cache: bool = True):
        """
        初始化批量处理器
        
        Args:
            etf_processor: ETF处理器
            cache_manager: 缓存管理器
            config: WMA配置
            enable_cache: 是否启用缓存
        """
        self.etf_processor = etf_processor
        self.cache_manager = cache_manager
        self.config = config
        self.enable_cache = enable_cache
    
    def process_etf_list(self, etf_codes: List[str], threshold: Optional[str] = None,
                        include_advanced_analysis: bool = False) -> List[Dict]:
        """
        批量处理ETF列表 - 支持智能缓存
        
        Args:
            etf_codes: ETF代码列表
            threshold: 门槛类型（用于缓存）
            include_advanced_analysis: 是否包含高级分析
            
        Returns:
            List[Dict]: 处理结果列表
        """
        results = []
        success_count = 0
        cache_hit_count = 0
        new_calculation_count = 0
        
        print(f"📊 开始批量处理 {len(etf_codes)} 个ETF...")
        
        for i, etf_code in enumerate(etf_codes, 1):
            print(f"\n{'='*60}")
            print(f"🔄 处理进度: {i}/{len(etf_codes)} - {etf_code}")
            print(f"{'='*60}")
            
            # 智能处理：优先增量更新，回退到缓存或全量计算
            result = None
            if self.enable_cache and self.cache_manager and threshold:
                # 尝试增量更新或缓存加载
                result = self._process_cached_etf(etf_code, threshold)
                if result:
                    if result.get('data_source') == 'cache':
                        cache_hit_count += 1
                        if not (self.config and self.config.performance_mode):
                            print(f"💾 {etf_code}: 使用缓存")
                    elif result.get('data_source') == 'incremental_update':
                        new_calculation_count += 1
                        if not (self.config and self.config.performance_mode):
                            print(f"⚡ {etf_code}: 增量更新 ({result.get('incremental_rows', 0)}行)")
                    else:
                        new_calculation_count += 1
            
            if not result:
                # 回退到全量计算
                result = self.etf_processor.process_single_etf(etf_code, include_advanced_analysis)
                if result:
                    new_calculation_count += 1
                    if not (self.config and self.config.performance_mode):
                        print(f"🔄 {etf_code}: 全量计算")
                    
                    # 保存到缓存（如果启用缓存且有门槛）
                    if self.enable_cache and self.cache_manager and threshold:
                        self._try_save_to_cache(etf_code, result, threshold)
            
            if result:
                results.append(result)
                success_count += 1
            else:
                print(f"❌ {etf_code} 处理失败，跳过...")
        
        # 更新缓存统计（如果启用缓存且有门槛）
        if self.enable_cache and self.cache_manager and threshold:
            self._update_cache_statistics(threshold, {
                'cache_hits': cache_hit_count,
                'new_calculations': new_calculation_count,
                'total_processed': len(etf_codes),
                'cache_hit_rate': (cache_hit_count / len(etf_codes)) * 100 if etf_codes else 0
            })
        
        print(f"\n✅ 批量处理完成! 成功处理 {success_count}/{len(etf_codes)} 个ETF")
        if self.enable_cache and threshold:
            print(f"🗂️ 缓存命中: {cache_hit_count}, 新计算: {new_calculation_count}")
        
        return results
    
    def _try_get_cached_result(self, etf_code: str, threshold: str) -> Optional[Dict]:
        """
        尝试从缓存获取结果
        
        Args:
            etf_code: ETF代码
            threshold: 门槛类型
            
        Returns:
            Optional[Dict]: 缓存结果或None
        """
        try:
            # 检查缓存是否有效
            source_file_path = self.config.get_file_path(etf_code)
            if not self.cache_manager.is_cache_valid(etf_code, threshold, source_file_path):
                return None
            
            # 获取缓存结果
            cached_df = self.cache_manager.load_cached_etf_data(etf_code, threshold)
            if cached_df is None or cached_df.empty:
                return None
            
            # 确保缓存数据按时间倒序排列，第一行是最新数据
            cached_df = cached_df.sort_values('date', ascending=False)
            latest_row = cached_df.iloc[0]  # 第一行是最新数据（按时间倒序）
            
            # 提取WMA值
            wma_values = {}
            for col in cached_df.columns:
                if col.startswith('WMA'):
                    value = latest_row[col]
                    if pd.notna(value):
                        wma_values[col] = round(float(value), 8)
                    else:
                        wma_values[col] = None
            
            # 构建结果
            result = {
                'etf_code': etf_code,
                'wma_values': wma_values,
                'latest_price': {
                    'date': str(latest_row.get('date', '')),
                    'close': round(float(latest_row.get('收盘价', 0)), 8),
                    'change_pct': round(float(latest_row.get('涨幅%', 0)), 8) if '涨幅%' in latest_row else 0.0
                },
                'date_range': {
                    'start_date': str(cached_df.iloc[-1].get('date', '')),
                    'end_date': str(cached_df.iloc[0].get('date', '')),
                    'total_days': len(cached_df)
                },
                'data_source': 'cache_hit',
                'cache_timestamp': datetime.fromtimestamp(
                    os.path.getmtime(self.cache_manager.get_cache_file_path(etf_code, threshold))
                ).isoformat()
            }
            
            return result
            
        except Exception as e:
            print(f"⚠️ 缓存读取失败: {etf_code} - {str(e)}")
            return None
    
    def _try_save_to_cache(self, etf_code: str, result: Dict, threshold: str) -> bool:
        """
        尝试保存结果到缓存 - 统一缓存格式
        
        Args:
            etf_code: ETF代码
            result: 计算结果
            threshold: 门槛类型
            
        Returns:
            bool: 保存是否成功
        """
        try:
            # 统一缓存格式：直接保存historical_data（与SMA项目一致）
            if result and result.get('historical_data') is not None:
                historical_data = result['historical_data']
                return self.cache_manager.save_etf_cache(etf_code, historical_data, threshold)
            
            return False
            
        except Exception as e:
            print(f"⚠️ 缓存保存失败: {etf_code} - {str(e)}")
            return False
    
    def _update_cache_statistics(self, threshold: str, stats: Dict) -> None:
        """
        更新缓存统计信息
        
        Args:
            threshold: 门槛类型
            stats: 统计信息
        """
        try:
            if self.cache_manager:
                self.cache_manager.update_processing_stats(threshold, stats)
        except Exception as e:
            print(f"⚠️ 更新缓存统计失败: {str(e)}")
    
    def _add_wma_to_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        将WMA数据添加到DataFrame
        
        Args:
            df: 原始数据DataFrame
            
        Returns:
            pd.DataFrame: 包含WMA数据的DataFrame
        """
        df_with_wma = df.copy()
        
        # 计算各周期WMA，统一使用下划线格式
        for period in self.config.wma_periods:
            wma_series = self.etf_processor.wma_engine.calculate_single_wma(df['收盘价'], period)
            df_with_wma[f'WMA_{period}'] = wma_series
        
        # 计算WMA差值
        if 'WMA_5' in df_with_wma.columns and 'WMA_20' in df_with_wma.columns:
            diff_5_20 = df_with_wma['WMA_5'] - df_with_wma['WMA_20']
            df_with_wma['WMA_DIFF_5_20'] = diff_5_20
        
        if 'WMA_3' in df_with_wma.columns and 'WMA_5' in df_with_wma.columns:
            diff_3_5 = df_with_wma['WMA_3'] - df_with_wma['WMA_5']
            df_with_wma['WMA_DIFF_3_5'] = diff_3_5
        
        # 计算相对差值百分比
        if 'WMA_DIFF_5_20' in df_with_wma.columns and 'WMA_20' in df_with_wma.columns:
            diff_series = df_with_wma['WMA_DIFF_5_20']
            wma20_series = df_with_wma['WMA_20']
            # 避免除以零错误
            valid_mask = wma20_series != 0
            pct_series = pd.Series([0.0] * len(df_with_wma), index=df_with_wma.index)
            pct_series[valid_mask] = (diff_series[valid_mask] / wma20_series[valid_mask] * 100).round(8)
            df_with_wma['WMA_DIFF_5_20_PCT'] = pct_series
        
        return df_with_wma
    
    def save_results_to_files(self, results: List[Dict], output_dir: str, 
                            threshold: Optional[str] = None) -> Dict:
        """
        保存结果到文件
        
        Args:
            results: 处理结果列表
            output_dir: 输出目录
            threshold: 门槛类型
            
        Returns:
            Dict: 保存统计信息
        """
        if not results:
            return {'files_saved': 0, 'total_size': 0, 'failed_saves': 0}
        
        # 确定输出目录
        if threshold:
            final_output_dir = os.path.join(output_dir, threshold)
        else:
            final_output_dir = output_dir
        
        # 确保目录存在
        try:
            os.makedirs(final_output_dir, exist_ok=True)
        except Exception as e:
            print(f"❌ 创建目录失败 {final_output_dir}: {str(e)}")
            print("💡 尝试使用当前目录...")
            final_output_dir = "."
        
        files_saved = 0
        total_size = 0
        failed_saves = 0
        
        print(f"💾 开始保存 {len(results)} 个ETF的历史数据文件...")
        
        for result in results:
            try:
                etf_code = result['etf_code']
                
                # 重新读取完整数据
                data_result = self.etf_processor.data_reader.read_etf_data(etf_code)
                if data_result is None:
                    failed_saves += 1
                    continue
                
                df, _ = data_result
                
                # 计算并添加WMA数据
                df_with_wma = self._add_wma_to_dataframe(df)
                
                # 保存文件
                clean_etf_code = etf_code.replace('.SH', '').replace('.SZ', '')
                output_file = os.path.join(final_output_dir, f"{clean_etf_code}.csv")
                
                # 确保输出文件的父目录存在
                try:
                    os.makedirs(os.path.dirname(output_file), exist_ok=True)
                except Exception as dir_e:
                    print(f"⚠️ {etf_code}: 创建目录失败 - {str(dir_e)}")
                
                # 按时间倒序保存（与原有逻辑一致）
                df_sorted = df_with_wma.sort_values('date', ascending=False)
                df_sorted.to_csv(output_file, index=False, encoding='utf-8')
                
                file_size = os.path.getsize(output_file)
                total_size += file_size
                files_saved += 1
                
                print(f"💾 {etf_code}: 已保存 ({len(df_sorted)}行, {file_size}字节)")
                
            except Exception as e:
                print(f"❌ {result.get('etf_code', 'Unknown')}: 保存失败 - {str(e)}")
                failed_saves += 1
        
        print(f"✅ 文件保存完成: {files_saved}成功, {failed_saves}失败, 总大小{total_size/1024/1024:.1f}MB")
        
        return {
            'files_saved': files_saved,
            'total_size': total_size,
            'failed_saves': failed_saves
        }
    
    def process_screening_results(self, threshold: str) -> List[Dict]:
        """
        处理筛选结果的ETF列表
        
        Args:
            threshold: 门槛类型 (如: "3000万门槛", "5000万门槛")
            
        Returns:
            List[Dict]: 处理结果列表
        """
        print(f"📊 读取{threshold}筛选结果...")
        
        # 使用数据读取器获取筛选结果
        etf_codes = self.etf_processor.data_reader.get_screening_etf_codes(threshold)
        
        if not etf_codes:
            print(f"❌ {threshold}: 未找到筛选结果")
            return []
        
        print(f"📊 {threshold}: 找到 {len(etf_codes)} 个通过筛选的ETF")
        
        # 使用现有的process_etf_list方法处理
        results = self.process_etf_list(etf_codes, threshold, include_advanced_analysis=False)
        
        print(f"✅ {threshold}: 处理完成，成功处理 {len(results)} 个ETF")
        
        return results
    
    def _process_incremental_etf(self, etf_code: str, threshold: str) -> Optional[Dict]:
        """增量更新ETF - 只计算新增数据"""
        try:
            # 1. 获取缓存的最新日期
            cached_latest_date = self.cache_manager.get_cached_etf_latest_date(etf_code, threshold)
            if not cached_latest_date:
                # 没有缓存，进行全量计算
                return self._process_new_etf(etf_code, threshold, False)
            
            # 2. 读取原始ETF数据
            source_file_path = self.config.get_file_path(etf_code)
            etf_df = pd.read_csv(source_file_path, encoding='utf-8')
            
            if etf_df.empty:
                return None
            
            # 确保日期格式统一为YYYY-MM-DD
            etf_df['date'] = pd.to_datetime(etf_df['date']).dt.strftime('%Y-%m-%d')
            etf_df = etf_df.sort_values('date', ascending=False).reset_index(drop=True)
            
            # 3. 检查是否有新数据
            latest_source_date = etf_df.iloc[0]['date']
            if str(latest_source_date) <= str(cached_latest_date):
                # 没有新数据，直接返回缓存结果
                if not (self.config and self.config.performance_mode):
                    print(f"   💾 {etf_code}: 使用缓存 (已是最新)")
                return self._load_from_cache(etf_code, threshold)
            
            # 4. 加载缓存数据
            cached_df = self.cache_manager.load_cached_etf_data(etf_code, threshold)
            if cached_df is None:
                # 缓存加载失败，进行全量计算
                return self._process_new_etf(etf_code, threshold, False)
            
            # 5. 找出新增的数据行
            new_data_df = etf_df[etf_df['date'] > cached_latest_date].copy()
            
            if new_data_df.empty:
                # 没有新数据
                return self._load_from_cache(etf_code, threshold)
            
            new_rows_count = len(new_data_df)
            if not (self.config and self.config.performance_mode):
                print(f"   ⚡ {etf_code}: 增量计算 {new_rows_count} 行新数据")
            
            # 6. 获取足够的历史数据用于计算WMA
            max_period = max(self.config.wma_periods)
            # 需要额外的历史数据来计算新数据的WMA
            history_needed = max_period + new_rows_count
            
            # 从原始数据获取足够的历史记录
            calc_df = etf_df.head(history_needed).copy()
            
            # 7. 计算新数据的WMA
            from ..engines.historical_calculator import WMAHistoricalCalculator
            calculator = WMAHistoricalCalculator(self.config)
            
            # 计算包含新数据的WMA
            new_wma_df = calculator.calculate_historical_wma(etf_code, calc_df)
            
            if new_wma_df is None:
                return None
            
            # 8. 只保留新增数据的WMA结果
            new_wma_df = new_wma_df.head(new_rows_count)
            
            # 9. 合并新旧数据
            # 移除缓存中与新数据日期重复的行（如果有）
            cached_df = cached_df[~cached_df['date'].isin(new_wma_df['date'])]
            
            # 合并数据（新数据在前）
            updated_df = pd.concat([new_wma_df, cached_df], ignore_index=True)
            
            # 确保按日期倒序排列
            updated_df = updated_df.sort_values('date', ascending=False).reset_index(drop=True)
            
            # 10. 更新缓存
            success = self.cache_manager.save_etf_cache(etf_code, updated_df, threshold)
            if not success:
                print(f"   ⚠️ {etf_code}: 缓存更新失败")
            
            # 11. 构建返回结果
            latest_row = updated_df.iloc[0]
            
            # 构建最新价格信息
            latest_price = {
                'date': str(latest_row['date']),
                'close': float(etf_df.iloc[0]['收盘价']) if '收盘价' in etf_df.columns else 0.0,
                'change_pct': float(etf_df.iloc[0]['涨幅%']) if '涨幅%' in etf_df.columns else 0.0
            }
            
            # 构建WMA值 - 统一使用下划线格式
            wma_values = {}
            for period in self.config.wma_periods:
                wma_col = f'WMA_{period}'
                if wma_col in latest_row:
                    wma_val = latest_row[wma_col]
                    if pd.notna(wma_val):
                        wma_values[f'WMA_{period}'] = float(wma_val)
            
            # 差值指标 - 统一使用下划线格式
            diff_fields = ['WMA_DIFF_5_20', 'WMA_DIFF_5_20_PCT', 'WMA_DIFF_3_5']
            for field in diff_fields:
                if field in latest_row:
                    val = latest_row[field]
                    if pd.notna(val):
                        wma_values[field] = float(val)
            
            result = {
                'etf_code': etf_code,
                'adj_type': self.config.adj_type,
                'latest_price': latest_price,
                'wma_values': wma_values,
                'processing_time': datetime.now().isoformat(),
                'data_source': 'incremental_update',
                'historical_data': updated_df,
                'success': True,
                'incremental_rows': new_rows_count
            }
            
            return result
            
        except Exception as e:
            print(f"   ❌ {etf_code}: 增量更新失败: {str(e)}")
            # 失败时尝试全量计算
            return self._process_new_etf(etf_code, threshold, False)
    
    def _process_cached_etf(self, etf_code: str, threshold: str) -> Optional[Dict]:
        """处理缓存中的ETF - 优先使用缓存，只在必要时增量更新"""
        try:
            # 1. 获取缓存的最新日期
            cached_latest_date = self.cache_manager.get_cached_etf_latest_date(etf_code, threshold)
            if not cached_latest_date:
                # 没有缓存，进行全量计算
                if not (self.config and self.config.performance_mode):
                    print(f"   🆕 {etf_code}: 无缓存，全量计算")
                return self._process_new_etf(etf_code, threshold, False)
            
            # 2. 快速检查源文件是否有更新
            source_file_path = self.config.get_file_path(etf_code)
            
            # 读取源文件的第一行来获取最新日期
            try:
                # 只读取前几行以提高性能
                source_df = pd.read_csv(source_file_path, encoding='utf-8', nrows=2)
                if source_df.empty:
                    return None
                
                # 🔧 修复：统一日期格式处理
                # 源文件日期格式通常是YYYYMMDD，需要转换为YYYY-MM-DD进行比较
                source_date_raw = str(source_df.iloc[0]['date'])
                if len(source_date_raw) == 8 and source_date_raw.isdigit():
                    # YYYYMMDD格式，转换为YYYY-MM-DD
                    latest_source_date = f"{source_date_raw[:4]}-{source_date_raw[4:6]}-{source_date_raw[6:8]}"
                else:
                    # 已经是YYYY-MM-DD格式或其他格式
                    latest_source_date = str(pd.to_datetime(source_date_raw).strftime('%Y-%m-%d'))
                
                # 确保缓存日期也是YYYY-MM-DD格式
                cached_date_str = str(pd.to_datetime(cached_latest_date).strftime('%Y-%m-%d'))
                
                # 3. 如果源数据没有更新，直接使用缓存
                if latest_source_date <= cached_date_str:
                    if not (self.config and self.config.performance_mode):
                        print(f"   💾 {etf_code}: 使用缓存 (最新: {cached_date_str})")
                    return self._load_from_cache(etf_code, threshold)
                
                # 4. 源数据有更新，进行增量计算
                if not (self.config and self.config.performance_mode):
                    print(f"   ⚡ {etf_code}: 检测到更新 ({cached_date_str} -> {latest_source_date})")
                return self._process_incremental_etf(etf_code, threshold)
                
            except Exception as e:
                # 读取源文件失败，尝试使用缓存
                if not (self.config and self.config.performance_mode):
                    print(f"   ⚠️ {etf_code}: 源文件读取失败，使用缓存")
                return self._load_from_cache(etf_code, threshold)
            
        except Exception as e:
            if not (self.config and self.config.performance_mode):
                print(f"   ❌ {etf_code}: 缓存处理失败: {str(e)}")
            return None
    
    def _process_new_etf(self, etf_code: str, threshold: str, include_advanced_analysis: bool = False) -> Optional[Dict]:
        """处理新ETF - 全量计算"""
        try:
            # 使用ETF处理器进行全量计算
            result = self.etf_processor.process_single_etf(etf_code, include_advanced_analysis)
            if result:
                result['data_source'] = 'full_calculation'
                
                # 保存到缓存
                if self.enable_cache and self.cache_manager and threshold:
                    self._try_save_to_cache(etf_code, result, threshold)
            
            return result
            
        except Exception as e:
            if not (self.config and self.config.performance_mode):
                print(f"   ❌ {etf_code}: 全量计算失败: {str(e)}")
            return None
    
    def _load_from_cache(self, etf_code: str, threshold: str) -> Optional[Dict]:
        """从缓存加载ETF结果 - 统一缓存格式"""
        try:
            cached_df = self.cache_manager.load_cached_etf_data(etf_code, threshold)
            
            if cached_df is None or cached_df.empty:
                return None
            
            # 确保缓存数据按时间倒序排列
            cached_df = cached_df.sort_values('date', ascending=False).reset_index(drop=True)
            latest_row = cached_df.iloc[0]  # 第一行是最新数据
            
            # 构建最新价格信息
            latest_price = {
                'date': str(latest_row['date']),
                'close': 0.0,
                'change_pct': 0.0
            }
            
            # 尝试从源文件获取最新价格信息
            try:
                source_file_path = self.config.get_file_path(etf_code)
                source_df = pd.read_csv(source_file_path, encoding='utf-8', nrows=1)
                if not source_df.empty:
                    latest_price['close'] = float(source_df.iloc[0]['收盘价']) if '收盘价' in source_df.columns else 0.0
                    latest_price['change_pct'] = float(source_df.iloc[0]['涨幅%']) if '涨幅%' in source_df.columns else 0.0
            except:
                pass  # 如果读取失败，使用默认值
            
            # 构建WMA值 - 统一使用下划线格式
            wma_values = {}
            for period in self.config.wma_periods:
                wma_col = f'WMA_{period}'
                if wma_col in cached_df.columns:
                    wma_val = latest_row[wma_col]
                    if pd.notna(wma_val):
                        wma_values[f'WMA_{period}'] = float(wma_val)
            
            # 差值指标 - 统一使用下划线格式
            diff_fields = ['WMA_DIFF_5_20', 'WMA_DIFF_5_20_PCT', 'WMA_DIFF_3_5']
            for field in diff_fields:
                if field in cached_df.columns:
                    val = latest_row[field]
                    if pd.notna(val):
                        wma_values[field] = float(val)
            
            # 构建结果对象
            result = {
                'etf_code': etf_code,
                'adj_type': self.config.adj_type,
                'latest_price': latest_price,
                'wma_values': wma_values,
                'processing_time': datetime.now().isoformat(),
                'data_source': 'cache',
                'historical_data': cached_df,
                'success': True
            }
            
            return result
            
        except Exception as e:
            if not (self.config and self.config.performance_mode):
                print(f"   ❌ {etf_code}: 缓存加载失败: {str(e)}")
            return None 