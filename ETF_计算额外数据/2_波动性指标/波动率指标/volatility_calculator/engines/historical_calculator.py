#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
波动率历史计算器
=============

实现波动率指标的历史数据计算、增量更新和向量化批量处理
基于WMA历史计算器架构设计
"""

import os
import pandas as pd
import numpy as np
from datetime import datetime
from typing import Dict, List, Optional, Any, Tuple
from ..infrastructure.config import VolatilityConfig
from .volatility_engine import VolatilityEngine


class VolatilityHistoricalCalculator:
    """波动率历史计算器"""
    
    def __init__(self, config: VolatilityConfig):
        """
        初始化历史计算器
        
        Args:
            config: 波动率配置对象
        """
        self.config = config
        self.volatility_engine = VolatilityEngine(config)
        
        print("🚀 波动率历史计算器初始化完成")
        print("   📊 支持向量化批量计算")
        print("   ⚡ 支持增量更新")
        print("   🗂️ 支持智能缓存")
    
    def calculate_full_historical_volatility_optimized(self, df: pd.DataFrame, 
                                                     etf_code: str) -> Optional[pd.DataFrame]:
        """
        计算完整历史波动率数据 - 向量化优化版本
        
        Args:
            df: 价格数据DataFrame
            etf_code: ETF代码
            
        Returns:
            pd.DataFrame: 包含历史波动率数据的DataFrame
        """
        try:
            if df.empty:
                print(f"❌ {etf_code}: 数据为空")
                return None
            
            print(f"🔬 {etf_code}: 开始向量化历史波动率计算...")
            
            # 准备结果DataFrame - 按第一大类标准处理字段和排序
            result_df = df[['日期', '开盘价', '最高价', '最低价', '收盘价', '成交量']].copy()
            result_df = result_df.sort_values('日期', ascending=True).reset_index(drop=True)  # 按时间正序
            
            # 字段名转换：中文 → 英文（按第一大类标准）
            result_df = result_df.rename(columns={
                '日期': 'date',
                '开盘价': 'open', 
                '最高价': 'high',
                '最低价': 'low',
                '收盘价': 'close',
                '成交量': 'volume'
            })
            
            # 日期格式标准化：datetime → YYYY-MM-DD字符串
            result_df['date'] = result_df['date'].dt.strftime('%Y-%m-%d')
            
            # 添加ETF代码列（按第一大类标准）
            clean_code = etf_code.replace('.SH', '').replace('.SZ', '')
            result_df['code'] = clean_code
            
            # 获取价格数据（使用英文字段名）
            high_prices = result_df['high']
            low_prices = result_df['low']
            close_prices = result_df['close']
            
            # 向量化计算价格振幅 - 按第一大类标准使用英文字段名
            print(f"   📊 计算价格振幅...")
            prev_close = close_prices.shift(1)
            price_range = ((high_prices - low_prices) / prev_close * 100).fillna(0)
            result_df['PRICE_RANGE'] = price_range
            
            # 向量化计算收益率
            returns = np.log(close_prices / close_prices.shift(1))
            
            # 向量化计算各周期历史波动率 - 按第一大类标准使用英文字段名
            for period in self.config.volatility_periods:
                if period <= len(df):
                    print(f"   📈 计算 VOL_{period}...")
                    
                    # 计算滚动标准差
                    rolling_std = returns.rolling(window=period, min_periods=period).std()
                    
                    # 年化处理
                    if self.config.annualized:
                        rolling_std = rolling_std * np.sqrt(self.config.trading_days_per_year)
                    
                    result_df[f'VOL_{period}'] = rolling_std
                else:
                    print(f"   ⚠️ 跳过 VOL_{period}: 数据不足")
                    result_df[f'VOL_{period}'] = np.nan
            
            # 向量化计算滚动波动率 - 按第一大类标准使用英文字段名
            for period in [10, 30]:
                if period <= len(df):
                    print(f"   📈 计算 ROLLING_VOL_{period}...")
                    rolling_vol = returns.rolling(window=period, min_periods=period).std()
                    
                    if self.config.annualized:
                        rolling_vol = rolling_vol * np.sqrt(self.config.trading_days_per_year)
                    
                    result_df[f'ROLLING_VOL_{period}'] = rolling_vol
            
            # 计算波动率比率和状态（向量化）
            self._calculate_vectorized_volatility_indicators(result_df)
            
            # 添加计算元数据 - 按第一大类标准
            result_df['calc_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            print(f"✅ {etf_code}: 向量化历史计算完成 ({len(result_df)}行)")
            
            return result_df
            
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
            # 计算波动率比率（向量化）- 按第一大类标准使用英文字段名
            if 'VOL_20' in df.columns and 'VOL_60' in df.columns:
                vol_20 = df['VOL_20']
                vol_60 = df['VOL_60']
                
                # 避免除零
                vol_ratio = np.where(vol_60 != 0, vol_20 / vol_60, np.nan)
                df['VOL_RATIO_20_60'] = vol_ratio
                
                # 向量化波动率状态判断 - 使用英文状态值
                vol_state = np.select(
                    [vol_ratio > 1.5, vol_ratio > 1.2, vol_ratio > 0.8],
                    ['HIGH', 'MEDIUM', 'NORMAL'],
                    default='LOW'
                )
                df['VOL_STATE'] = vol_state
            
            # 计算波动率水平（向量化）- 按第一大类标准使用英文字段名
            if 'VOL_10' in df.columns:
                vol_10 = df['VOL_10']
                
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
                
                df['VOL_LEVEL'] = vol_level
            
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
            
            # 合并数据（新数据在前）
            combined_df = pd.concat([
                new_df[new_df['date'].isin(incremental_dates)],
                cached_df
            ], ignore_index=True)
            
            # 按日期排序（最新在前）
            combined_df = combined_df.sort_values('date', ascending=False).reset_index(drop=True)
            
            # 重新计算所有波动率指标（因为滚动计算需要考虑新数据）
            max_period = max(self.config.volatility_periods) if self.config.volatility_periods else 60
            recalc_rows = min(len(combined_df), max_period * 2)  # 重算影响范围
            
            print(f"   🔄 {etf_code}: 重新计算前 {recalc_rows} 行数据...")
            
            # 只重算受影响的部分
            recalc_df = combined_df.head(recalc_rows).copy()
            unchanged_df = combined_df.iloc[recalc_rows:].copy() if recalc_rows < len(combined_df) else pd.DataFrame()
            
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
            
            return final_df
            
        except Exception as e:
            print(f"❌ {etf_code}: 增量更新异常: {str(e)}")
            return cached_df  # 返回原始缓存数据
    
    def batch_calculate_historical_volatility(self, etf_files_dict: Dict[str, str],
                                            etf_codes: List[str]) -> Dict[str, pd.DataFrame]:
        """
        批量计算历史波动率数据 - 超高性能向量化版本
        
        Args:
            etf_files_dict: ETF代码到文件路径的映射
            etf_codes: ETF代码列表
            
        Returns:
            Dict[str, pd.DataFrame]: ETF代码到历史数据的映射
        """
        print(f"🚀 开始批量历史波动率计算 ({len(etf_codes)}个ETF)...")
        results = {}
        
        for i, etf_code in enumerate(etf_codes, 1):
            try:
                file_path = etf_files_dict.get(etf_code)
                if not file_path or not os.path.exists(file_path):
                    print(f"⚠️ [{i}/{len(etf_codes)}] {etf_code}: 文件不存在")
                    continue
                
                print(f"📊 [{i}/{len(etf_codes)}] 处理: {etf_code}")
                
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
                    results[etf_code] = historical_df
                    print(f"   ✅ {etf_code}: 完成 ({len(historical_df)}行)")
                else:
                    print(f"   ❌ {etf_code}: 计算失败")
                
            except Exception as e:
                print(f"   ❌ {etf_code}: 批量计算异常: {str(e)}")
                continue
        
        success_rate = (len(results) / len(etf_codes)) * 100
        print(f"\n🎉 批量历史计算完成!")
        print(f"   📊 成功: {len(results)}/{len(etf_codes)} ({success_rate:.1f}%)")
        
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
                    df.to_csv(output_file, index=False, encoding='utf-8')
                    
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