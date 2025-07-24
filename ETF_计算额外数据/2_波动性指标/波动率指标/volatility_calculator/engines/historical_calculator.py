#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
波动率历史计算器
=============

实现波动率指标的历史数据计算、增量更新和向量化批量处理
基于WMA历史计算器架构设计
"""

import os
import gc
import pandas as pd
import numpy as np
from datetime import datetime
from typing import Dict, List, Optional, Any, Tuple
from ..infrastructure.config import VolatilityConfig
from .volatility_engine import VolatilityEngine


class VolatilityHistoricalCalculator:
    """波动率历史计算器"""
    
    def __init__(self, config: VolatilityConfig, cache_manager=None):
        """
        初始化历史计算器
        
        Args:
            config: 波动率配置对象
            cache_manager: 缓存管理器（可选）
        """
        self.config = config
        self.volatility_engine = VolatilityEngine(config)
        self.cache_manager = cache_manager
        
        print("🚀 波动率历史计算器初始化完成")
        print("   📊 支持向量化批量计算")
        print("   ⚡ 支持增量更新")
        print("   🗂️ 支持智能缓存")
    
    def calculate_full_historical_volatility_optimized(self, df: pd.DataFrame, 
                                                     etf_code: str) -> Optional[pd.DataFrame]:
        """
        计算完整历史波动率数据 - 完全模仿布林带的向量化优化版本
        
        Args:
            df: 价格数据DataFrame
            etf_code: ETF代码
            
        Returns:
            pd.DataFrame: 包含历史波动率数据的DataFrame
        """
        try:
            if df.empty or '收盘价' not in df.columns:
                print(f"❌ {etf_code}: 数据为空或缺少必需字段")
                return None
            
            # 检查数据量是否足够（模仿布林带的数据验证）
            max_period = max(self.config.volatility_periods) if self.config.volatility_periods else 60
            if len(df) < max_period:
                print(f"❌ {etf_code}: 数据不足 ({len(df)} < {max_period})")
                return None
            
            print(f"🔬 {etf_code}: 开始向量化历史波动率计算...")
            
            # 准备结果DataFrame（模仿布林带的数据结构处理）
            result_df = df.copy()
            # 确保按日期正序排列，用于计算
            result_df = result_df.sort_values('日期', ascending=True).reset_index(drop=True)
            
            # 字段名转换：中文 → 英文（按第一大类标准）
            column_mapping = {
                '日期': 'date',
                '开盘价': 'open', 
                '最高价': 'high',
                '最低价': 'low',
                '收盘价': 'close',
                '成交量': 'volume'
            }
            
            for chinese_col, english_col in column_mapping.items():
                if chinese_col in result_df.columns:
                    result_df[english_col] = result_df[chinese_col]
            
            # 日期格式标准化 - 处理数字格式日期（如20250711）
            if '日期' in result_df.columns:
                # 将数字格式日期转换为标准日期格式
                result_df['date'] = pd.to_datetime(result_df['日期'], format='%Y%m%d', errors='coerce').dt.strftime('%Y-%m-%d')
            elif 'date' in result_df.columns:
                try:
                    result_df['date'] = pd.to_datetime(result_df['date'], errors='coerce').dt.strftime('%Y-%m-%d')
                except:
                    result_df['date'] = result_df['date']
            
            # 添加ETF代码列（按README.md规范：纯数字，无交易所后缀）
            clean_code = etf_code.replace('.SH', '').replace('.SZ', '')
            result_df['code'] = clean_code
            
            # 使用波动率引擎进行向量化计算（模仿布林带调用引擎的方式）
            historical_df = self.volatility_engine.calculate_historical_volatility_indicators(df)
            
            if historical_df.empty:
                print(f"❌ {etf_code}: 波动率计算失败")
                return None
            
            # 将计算结果合并到结果DataFrame（确保日期对应关系正确）
            # 先确保historical_df也按日期正序排列，与result_df一致
            if '日期' in historical_df.columns:
                historical_df = historical_df.sort_values('日期', ascending=True).reset_index(drop=True)
            
            vol_columns = [col for col in historical_df.columns if col.startswith(('vol_', 'rolling_vol_', 'price_range'))]
            vol_columns.extend(['vol_ratio_20_30', 'vol_state', 'vol_level'])
            
            # 确保两个DataFrame长度一致且索引对应
            if len(result_df) == len(historical_df):
                for col in vol_columns:
                    if col in historical_df.columns:
                        result_df[col] = historical_df[col].values  # 使用.values确保索引对齐
            else:
                print(f"⚠️ {etf_code}: DataFrame长度不匹配 result_df={len(result_df)}, historical_df={len(historical_df)}")
                # 回退到更安全的方法：重新计算
                return None
            
            # 添加计算元数据
            result_df['calc_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            # 按照新标准规定的输出格式选择字段（统一小写）
            # 格式: code,date,vol_10,vol_20,vol_30,rolling_vol_10,rolling_vol_30,price_range,vol_ratio_20_30,vol_state,vol_level,calc_time
            output_columns = ['code', 'date']
            
            # 添加波动率指标字段（小写字段名）
            for period in self.config.volatility_periods:
                col_name = f'vol_{period}'
                if col_name in result_df.columns:
                    output_columns.append(col_name)
                else:
                    # 如果字段不存在，添加空值占位
                    result_df[col_name] = np.nan
                    output_columns.append(col_name)
            
            # 添加滚动波动率字段（小写字段名）
            for period in [10, 30]:
                col_name = f'rolling_vol_{period}'
                if col_name in result_df.columns:
                    output_columns.append(col_name)
                else:
                    # 如果字段不存在，添加空值占位
                    result_df[col_name] = np.nan
                    output_columns.append(col_name)
            
            # 添加其他指标字段（小写字段名）
            other_fields = ['price_range', 'vol_ratio_20_30', 'vol_state', 'vol_level']
            
            for field in other_fields:
                if field in result_df.columns:
                    output_columns.append(field)
                else:
                    # 如果字段不存在，添加空值占位
                    result_df[field] = np.nan if field != 'vol_state' and field != 'vol_level' else 'UNKNOWN'
                    output_columns.append(field)
            
            # 添加计算时间
            if 'calc_time' in result_df.columns:
                output_columns.append('calc_time')
            
            # 最终结果筛选
            final_df = result_df[output_columns].copy()
            
            # 最终排序：按日期降序排列，确保最新日期在最上面
            if 'date' in final_df.columns:
                final_df['date'] = pd.to_datetime(final_df['date'], errors='coerce')
                final_df = final_df.sort_values('date', ascending=False).reset_index(drop=True)
                # 转回字符串格式
                final_df['date'] = final_df['date'].dt.strftime('%Y-%m-%d')
            
            print(f"✅ {etf_code}: 向量化历史计算完成 ({len(final_df)}行)")
            
            return final_df
            
        except Exception as e:
            print(f"❌ {etf_code}: 向量化历史计算异常: {str(e)}")
            return None
    
    def _calculate_vectorized_volatility_indicators(self, df: pd.DataFrame) -> None:
        """
        向量化计算波动率衍生指标
        
        Args:
            df: 数据框（会被修改）
        """
        try:
            # 计算波动率比率（向量化）- 按第一大类标准使用小写字段名
            if 'vol_20' in df.columns and 'vol_30' in df.columns:
                vol_20 = df['vol_20']
                vol_30 = df['vol_30']
                
                # 避免除零
                vol_ratio = np.where(vol_30 != 0, vol_20 / vol_30, np.nan)
                df['vol_ratio_20_30'] = vol_ratio
                
                # 向量化波动率状态判断 - 使用英文状态值
                vol_state = np.select(
                    [vol_ratio > 1.5, vol_ratio > 1.2, vol_ratio > 0.8],
                    ['HIGH', 'MEDIUM', 'NORMAL'],
                    default='LOW'
                )
                df['vol_state'] = vol_state
            
            # 计算波动率水平（向量化）- 按第一大类标准使用小写字段名
            if 'vol_10' in df.columns:
                vol_10 = df['vol_10']
                
                if self.config.annualized:
                    vol_level = np.select(
                        [vol_10 > 0.4, vol_10 > 0.25, vol_10 > 0.15],
                        ['EXTREME_HIGH', 'HIGH', 'MEDIUM'],
                        default='LOW'
                    )
                else:
                    vol_level = np.select(
                        [vol_10 > 0.025, vol_10 > 0.016, vol_10 > 0.009],
                        ['EXTREME_HIGH', 'HIGH', 'MEDIUM'],
                        default='LOW'
                    )
                
                df['vol_level'] = vol_level
            
        except Exception as e:
            print(f"  ⚠️ 向量化波动率指标计算警告: {str(e)}")
    
    def calculate_incremental_update(self, cached_df: pd.DataFrame, new_df: pd.DataFrame,
                                   etf_code: str) -> Optional[pd.DataFrame]:
        """
        计算增量更新
        
        Args:
            cached_df: 缓存的历史数据
            new_df: 新的数据
            etf_code: ETF代码
            
        Returns:
            pd.DataFrame: 更新后的完整数据
        """
        try:
            print(f"⚡ {etf_code}: 开始增量更新计算...")
            
            # 确保日期列格式一致
            if 'date' not in cached_df.columns:
                cached_df = cached_df.rename(columns={'日期': 'date'})
            if '日期' in new_df.columns:
                new_df = new_df.rename(columns={'日期': 'date'})
            
            cached_df['date'] = pd.to_datetime(cached_df['date'])
            new_df['date'] = pd.to_datetime(new_df['date'])
            
            # 找出新增的数据行
            cached_dates = set(cached_df['date'])
            new_dates = set(new_df['date'])
            
            # 新增日期
            incremental_dates = new_dates - cached_dates
            
            if not incremental_dates:
                print(f"   📊 {etf_code}: 无新增数据，返回缓存数据")
                return cached_df
            
            print(f"   📈 {etf_code}: 发现 {len(incremental_dates)} 个新增交易日")
            
            # 优化：使用更高效的数据合并方式
            incremental_df = new_df[new_df['date'].isin(incremental_dates)].copy()
            
            # 按日期排序后合并（避免多次排序）
            incremental_df = incremental_df.sort_values('date', ascending=False).reset_index(drop=True)
            cached_df_sorted = cached_df.sort_values('date', ascending=False).reset_index(drop=True)
            
            # 使用numpy数组合并提高效率
            combined_df = pd.concat([incremental_df, cached_df_sorted], ignore_index=True)
            
            # 重新计算所有波动率指标（因为滚动计算需要考虑新数据）
            max_period = max(self.config.volatility_periods) if self.config.volatility_periods else 60
            recalc_rows = min(len(combined_df), max_period * 2)  # 重算影响范围
            
            print(f"   🔄 {etf_code}: 重新计算前 {recalc_rows} 行数据...")
            
            # 只重算受影响的部分（优化内存使用）
            if recalc_rows < len(combined_df):
                recalc_df = combined_df.iloc[:recalc_rows].copy()
                unchanged_df = combined_df.iloc[recalc_rows:].copy()
            else:
                recalc_df = combined_df.copy()
                unchanged_df = pd.DataFrame()
            
            # 重新计算波动率指标
            updated_df = self.calculate_full_historical_volatility_optimized(
                recalc_df.rename(columns={'date': '日期'}), etf_code
            )
            
            if updated_df is None:
                print(f"❌ {etf_code}: 增量计算失败")
                return cached_df
            
            # 合并重算数据和未变化数据
            if not unchanged_df.empty:
                # 确保列结构一致
                for col in updated_df.columns:
                    if col not in unchanged_df.columns:
                        unchanged_df[col] = np.nan
                
                final_df = pd.concat([updated_df, unchanged_df], ignore_index=True)
            else:
                final_df = updated_df
            
            print(f"✅ {etf_code}: 增量更新完成 (总计 {len(final_df)} 行)")
            
            # 内存清理
            del combined_df, recalc_df
            if not unchanged_df.empty:
                del unchanged_df
            gc.collect()
            
            return final_df
            
        except Exception as e:
            print(f"❌ {etf_code}: 增量更新异常: {str(e)}")
            return cached_df  # 返回原始缓存数据
    
    def batch_calculate_historical_volatility(self, etf_files_dict: Dict[str, str],
                                            etf_codes: List[str], threshold: Optional[str] = None) -> Dict[str, pd.DataFrame]:
        """
        批量计算历史波动率数据 - 超高性能向量化版本（支持缓存）
        
        Args:
            etf_files_dict: ETF代码到文件路径的映射
            etf_codes: ETF代码列表
            threshold: 门槛类型（用于缓存）
            
        Returns:
            Dict[str, pd.DataFrame]: ETF代码到历史数据的映射
        """
        print(f"🚀 开始批量历史波动率计算 ({len(etf_codes)}个ETF)...")
        results = {}
        
        # 添加内存管理：批量处理
        batch_size = 50  # 每批处理50个ETF
        processed_count = 0
        cache_hits = 0
        fresh_calculations = 0
        
        for i, etf_code in enumerate(etf_codes, 1):
            try:
                file_path = etf_files_dict.get(etf_code)
                if not file_path or not os.path.exists(file_path):
                    print(f"⚠️ [{i}/{len(etf_codes)}] {etf_code}: 文件不存在")
                    continue
                
                print(f"📊 [{i}/{len(etf_codes)}] 处理: {etf_code}")
                
                historical_df = None
                
                # 尝试从缓存加载
                if self.cache_manager:
                    is_valid, meta_data = self.cache_manager.check_cache_validity(etf_code, file_path, threshold)
                    if is_valid:
                        cached_df = self.cache_manager.load_cache(etf_code, threshold)
                        if cached_df is not None and not cached_df.empty:
                            historical_df = cached_df
                            cache_hits += 1
                            print(f"   💾 {etf_code}: 缓存命中 ({len(historical_df)}行)")
                
                # 如果缓存未命中，进行全量计算
                if historical_df is None:
                    # 读取数据
                    df = pd.read_csv(file_path, encoding='utf-8')
                    
                    if df.empty:
                        print(f"   ❌ {etf_code}: 数据为空")
                        continue
                    
                    # 数据预处理
                    required_columns = ['日期', '开盘价', '最高价', '最低价', '收盘价']
                    if not all(col in df.columns for col in required_columns):
                        print(f"   ❌ {etf_code}: 缺少必需字段")
                        continue
                    
                    # 向量化计算
                    historical_df = self.calculate_full_historical_volatility_optimized(df, etf_code)
                    
                    if historical_df is not None:
                        fresh_calculations += 1
                        print(f"✅ {etf_code}: 向量化历史计算完成 ({len(historical_df)}行)")
                        
                        # 保存到缓存
                        if self.cache_manager:
                            self.cache_manager.save_cache(etf_code, historical_df, file_path, threshold)
                    else:
                        print(f"   ❌ {etf_code}: 计算失败")
                        continue
                    
                    # 清理临时变量
                    del df
                
                # 添加到结果
                if historical_df is not None:
                    results[etf_code] = historical_df
                    print(f"   ✅ {etf_code}: 完成 ({len(historical_df)}行)")
                    processed_count += 1
                
                # 内存管理：每处理一批ETF后清理内存
                if processed_count % batch_size == 0:
                    gc.collect()
                    print(f"   🧹 内存清理完成 (已处理 {processed_count}/{len(etf_codes)})")
                
                # 清理临时变量
                if 'historical_df' in locals():
                    del historical_df
                
            except Exception as e:
                print(f"   ❌ {etf_code}: 批量计算异常: {str(e)}")
                continue
        
        success_rate = (len(results) / len(etf_codes)) * 100
        print(f"\n🎉 批量历史计算完成!")
        print(f"   📊 成功: {len(results)}/{len(etf_codes)} ({success_rate:.1f}%)")
        print(f"   💾 缓存命中: {cache_hits}")
        print(f"   🔄 全新计算: {fresh_calculations}")
        
        return results
    
    def save_historical_results(self, results: Dict[str, pd.DataFrame], 
                              output_dir: str, threshold: str) -> Dict[str, Any]:
        """
        保存历史计算结果
        
        Args:
            results: 计算结果
            output_dir: 输出目录
            threshold: 门槛类型
            
        Returns:
            Dict: 保存统计信息
        """
        try:
            # 创建输出目录
            threshold_output_dir = os.path.join(output_dir, threshold)
            os.makedirs(threshold_output_dir, exist_ok=True)
            
            saved_count = 0
            total_size = 0
            saved_etfs = []
            
            print(f"💾 保存历史数据到: {threshold_output_dir}")
            
            for etf_code, df in results.items():
                try:
                    clean_code = etf_code.replace('.SH', '').replace('.SZ', '')
                    output_file = os.path.join(threshold_output_dir, f"{clean_code}.csv")
                    
                    # 保存文件
                    df.to_csv(output_file, index=False, encoding='utf-8', float_format='%.8f')
                    
                    # 统计信息
                    file_size = os.path.getsize(output_file)
                    total_size += file_size
                    saved_count += 1
                    saved_etfs.append(etf_code)
                    
                    if not self.config.performance_mode:
                        print(f"   💾 {etf_code}: {len(df)}行 → {file_size/1024:.1f}KB")
                    
                except Exception as e:
                    print(f"   ❌ 保存失败 {etf_code}: {str(e)}")
            
            stats = {
                'threshold': threshold,
                'saved_count': saved_count,
                'total_files': len(results),
                'success_rate': (saved_count / len(results)) * 100 if results else 0,
                'total_size_kb': total_size / 1024,
                'output_directory': threshold_output_dir,
                'etf_codes': saved_etfs
            }
            
            print(f"✅ {threshold}: 保存完成")
            print(f"   📊 文件: {saved_count}/{len(results)}")
            print(f"   💾 大小: {stats['total_size_kb']:.1f}KB")
            
            return stats
            
        except Exception as e:
            print(f"❌ 历史结果保存异常: {str(e)}")
            return {'error': str(e)}