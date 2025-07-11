#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
EMA历史数据计算器 - 重构版
========================

参照WMA/SMA系统的历史数据处理架构
提供高性能的批量历史数据计算功能
"""

import os
import pandas as pd
import numpy as np
from datetime import datetime
from typing import Dict, List, Optional, Any
from ..infrastructure.config import EMAConfig
from .ema_engine import EMAEngine


class EMAHistoricalCalculator:
    """EMA历史数据计算器 - 重构版（与WMA/SMA保持一致）"""
    
    def __init__(self, config: EMAConfig):
        """
        初始化历史数据计算器
        
        Args:
            config: EMA配置对象
        """
        self.config = config
        self.ema_engine = EMAEngine(config)
        
        if not config.performance_mode:
            print("📊 EMA历史数据计算器初始化完成")
            print(f"   🔧 支持周期: {config.ema_periods}")
    
    def batch_calculate_historical_ema(self, etf_files_dict: Dict[str, str], 
                                     etf_codes: List[str]) -> Dict[str, pd.DataFrame]:
        """
        批量计算多个ETF的历史EMA数据
        
        Args:
            etf_files_dict: ETF代码到文件路径的映射
            etf_codes: 要处理的ETF代码列表
            
        Returns:
            Dict[str, pd.DataFrame]: ETF代码到历史数据的映射
        """
        try:
            if not self.config.performance_mode:
                print(f"🚀 开始批量计算{len(etf_codes)}个ETF的历史EMA数据...")
            
            results = {}
            success_count = 0
            
            for i, etf_code in enumerate(etf_codes, 1):
                if not self.config.performance_mode:
                    print(f"📊 进度: {i}/{len(etf_codes)} - {etf_code}")
                
                file_path = etf_files_dict.get(etf_code)
                if not file_path or not os.path.exists(file_path):
                    if not self.config.performance_mode:
                        print(f"❌ {etf_code}: 文件不存在")
                    continue
                
                # 计算单个ETF的历史数据
                historical_df = self.calculate_single_etf_historical(etf_code, file_path)
                
                if historical_df is not None:
                    results[etf_code] = historical_df
                    success_count += 1
                    if not self.config.performance_mode:
                        print(f"✅ {etf_code}: 历史计算完成 ({len(historical_df)}行)")
                else:
                    if not self.config.performance_mode:
                        print(f"❌ {etf_code}: 历史计算失败")
            
            if not self.config.performance_mode:
                print(f"🎉 批量历史计算完成: {success_count}/{len(etf_codes)}")
            
            return results
            
        except Exception as e:
            print(f"❌ 批量历史计算失败: {str(e)}")
            return {}
    
    def calculate_single_etf_historical(self, etf_code: str, file_path: str) -> Optional[pd.DataFrame]:
        """
        计算单个ETF的完整历史EMA数据
        
        Args:
            etf_code: ETF代码
            file_path: ETF数据文件路径
            
        Returns:
            Optional[pd.DataFrame]: 包含EMA计算结果的历史数据
        """
        try:
            # 读取完整历史数据
            df = pd.read_csv(file_path, encoding='utf-8')
            
            if df.empty:
                return None
            
            # 验证必要列
            required_columns = ['日期', '开盘价', '最高价', '最低价', '收盘价', '成交量']
            missing_columns = [col for col in required_columns if col not in df.columns]
            
            if missing_columns:
                if not self.config.performance_mode:
                    print(f"❌ {etf_code}: 缺少必要列 {missing_columns}")
                return None
            
            # 数据预处理
            df = self._preprocess_data(df)
            
            if df.empty:
                return None
            
            # 检查数据量是否足够
            if len(df) < self.config.max_period:
                if not self.config.performance_mode:
                    print(f"⚠️ {etf_code}: 数据量不足 ({len(df)}行 < {self.config.max_period}行)")
                # 继续处理，但可能精度不够
            
            # 使用EMA引擎计算完整历史数据
            result_df = self.ema_engine.calculate_full_historical_ema(df, etf_code)
            
            return result_df
            
        except Exception as e:
            if not self.config.performance_mode:
                print(f"❌ {etf_code} 历史计算失败: {str(e)}")
            return None
    
    def _preprocess_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        数据预处理 - 与WMA/SMA保持一致
        
        Args:
            df: 原始数据DataFrame
            
        Returns:
            pd.DataFrame: 预处理后的数据
        """
        try:
            # 复制数据，避免修改原始数据
            work_df = df.copy()
            
            # 日期处理
            if '日期' in work_df.columns:
                work_df['日期'] = pd.to_datetime(work_df['日期'], format='%Y%m%d', errors='coerce')
                work_df = work_df.dropna(subset=['日期'])
                
                if work_df.empty:
                    return work_df
                
                # 按时间升序排序（EMA计算需要）
                work_df = work_df.sort_values('日期', ascending=True).reset_index(drop=True)
            
            # 数值列处理
            numeric_columns = ['开盘价', '最高价', '最低价', '收盘价', '成交量']
            for col in numeric_columns:
                if col in work_df.columns:
                    work_df[col] = pd.to_numeric(work_df[col], errors='coerce')
            
            # 移除价格为空或零的行
            price_columns = ['开盘价', '最高价', '最低价', '收盘价']
            for col in price_columns:
                if col in work_df.columns:
                    work_df = work_df[work_df[col] > 0]
            
            # 移除成交量为负的行
            if '成交量' in work_df.columns:
                work_df = work_df[work_df['成交量'] >= 0]
            
            return work_df
            
        except Exception as e:
            print(f"❌ 数据预处理失败: {str(e)}")
            return pd.DataFrame()
    
    def save_historical_results(self, historical_results: Dict[str, pd.DataFrame], 
                              output_base_dir: str, threshold: str) -> Dict[str, Any]:
        """
        保存历史计算结果到文件
        
        Args:
            historical_results: 历史计算结果
            output_base_dir: 输出基础目录
            threshold: 门槛类型
            
        Returns:
            Dict[str, Any]: 保存统计信息
        """
        try:
            # 创建输出目录
            output_dir = os.path.join(output_base_dir, threshold)
            os.makedirs(output_dir, exist_ok=True)
            
            save_stats = {
                'files_saved': 0,
                'total_size': 0,
                'failed_saves': 0,
                'saved_files': []
            }
            
            for etf_code, historical_df in historical_results.items():
                try:
                    # 清理ETF代码
                    clean_etf_code = etf_code.replace('.SH', '').replace('.SZ', '')
                    filename = f"{clean_etf_code}.csv"
                    file_path = os.path.join(output_dir, filename)
                    
                    # 保存文件
                    historical_df.to_csv(file_path, index=False, encoding='utf-8')
                    
                    # 统计
                    file_size = os.path.getsize(file_path)
                    save_stats['files_saved'] += 1
                    save_stats['total_size'] += file_size
                    save_stats['saved_files'].append(filename)
                    
                    if not self.config.performance_mode:
                        print(f"💾 {etf_code}: 历史文件已保存 ({len(historical_df)}行, {file_size}字节)")
                    
                except Exception as e:
                    save_stats['failed_saves'] += 1
                    if not self.config.performance_mode:
                        print(f"❌ {etf_code}: 历史文件保存失败 - {str(e)}")
            
            if not self.config.performance_mode:
                print(f"📁 {threshold}历史文件保存统计:")
                print(f"   ✅ 成功保存: {save_stats['files_saved']} 个文件")
                print(f"   💿 总大小: {save_stats['total_size'] / 1024 / 1024:.2f} MB")
                print(f"   ❌ 保存失败: {save_stats['failed_saves']} 个文件")
            
            return save_stats
            
        except Exception as e:
            print(f"❌ 历史结果保存失败: {str(e)}")
            return {
                'files_saved': 0,
                'total_size': 0,
                'failed_saves': len(historical_results),
                'error': str(e)
            }
    
    def calculate_full_historical_ema_optimized(self, df: pd.DataFrame, etf_code: str) -> Optional[pd.DataFrame]:
        """
        优化版完整历史EMA计算（用于单个ETF快速分析）
        
        Args:
            df: ETF数据DataFrame
            etf_code: ETF代码
            
        Returns:
            Optional[pd.DataFrame]: 包含EMA计算结果的DataFrame
        """
        try:
            # 直接使用EMA引擎的方法
            return self.ema_engine.calculate_full_historical_ema(df, etf_code)
            
        except Exception as e:
            print(f"❌ {etf_code} 优化历史计算失败: {str(e)}")
            return None