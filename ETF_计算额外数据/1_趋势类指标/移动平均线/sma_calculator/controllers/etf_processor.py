#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ETF处理器
========

负责单个ETF的SMA计算和处理逻辑
"""

import pandas as pd
import numpy as np
from datetime import datetime
from typing import Dict, Optional, List
from ..infrastructure.data_reader import ETFDataReader
from ..engines.sma_engine import SMAEngine


class ETFProcessor:
    """ETF处理器 - 负责单个ETF的SMA计算"""
    
    def __init__(self, data_reader: ETFDataReader, sma_engine: SMAEngine, config):
        """
        初始化ETF处理器
        
        Args:
            data_reader: 数据读取器
            sma_engine: SMA计算引擎
            config: 配置对象
        """
        self.data_reader = data_reader
        self.sma_engine = sma_engine
        self.config = config
    
    def process_single_etf(self, etf_code: str, include_advanced_analysis: bool = False) -> Optional[Dict]:
        """
        处理单个ETF的SMA计算
        
        Args:
            etf_code: ETF代码
            include_advanced_analysis: 是否包含高级分析
            
        Returns:
            Optional[Dict]: 处理结果或None
        """
        try:
            # 读取ETF数据
            data_result = self.data_reader.read_etf_data(etf_code)
            if data_result is None:
                print(f"❌ {etf_code}: 数据读取失败")
                return None
            
            df, total_rows = data_result
            
            if df.empty:
                print(f"❌ {etf_code}: 数据为空")
                return None
            
            # 计算SMA指标
            sma_values = self.sma_engine.calculate_all_sma(df)
            
            if not sma_values:
                print(f"❌ {etf_code}: SMA计算失败 - 无有效SMA值")
                return None
            
            # 获取最新价格信息
            latest_price = self._get_latest_price_info(df)
            
            # 生成完整历史数据
            historical_data = self._generate_historical_data(etf_code, df)
            
            # 构建结果
            result = {
                'etf_code': etf_code,
                'adj_type': self.config.adj_type,
                'latest_price': latest_price,
                'sma_values': sma_values,
                'signals': {'status': 'calculated'},
                'processing_time': datetime.now().isoformat(),
                'data_source': 'fresh_calculation',
                'historical_data': historical_data
            }
            
            if include_advanced_analysis:
                result['advanced_analysis'] = self._perform_advanced_analysis(sma_values)
            
            return result
            
        except Exception as e:
            print(f"❌ {etf_code}: 处理异常 - {str(e)}")
            return None
    
    def _get_latest_price_info(self, df: pd.DataFrame) -> Dict:
        """获取最新价格信息"""
        try:
            # 数据读取器返回的是按日期升序排列的数据，所以取最后一行
            latest_row = df.iloc[-1]
            
            # 计算涨跌幅
            change_pct = 0.0
            if len(df) >= 2:
                prev_close = df.iloc[-2]['收盘价']
                current_close = latest_row['收盘价']
                if prev_close > 0:
                    change_pct = ((current_close - prev_close) / prev_close) * 100
            
            return {
                'date': latest_row['日期'].strftime('%Y-%m-%d') if hasattr(latest_row['日期'], 'strftime') else str(latest_row['日期']),
                'close': float(latest_row['收盘价']),
                'change_pct': round(change_pct, 3)
            }
        except Exception:
            return {
                'date': '',
                'close': 0.0,
                'change_pct': 0.0
            }
    
    def _generate_historical_data(self, etf_code: str, df: pd.DataFrame) -> Optional[pd.DataFrame]:
        """生成ETF的完整历史数据（包含所有SMA计算）- 使用向量化引擎"""
        try:
            # 使用超高性能向量化引擎
            from ..engines.sma_historical_calculator import SMAHistoricalCalculator
            historical_calculator = SMAHistoricalCalculator(self.config)
            
            # 向量化计算
            result_df = historical_calculator.calculate_full_historical_sma_optimized(df, etf_code)
            
            if result_df is not None:
                print(f"   🚀 {etf_code}: 超高性能历史数据生成完成 - {len(result_df)}行")
                return result_df
            else:
                print(f"   ❌ {etf_code}: 超高性能历史数据生成失败，回退到传统方法")
                # 回退到传统方法
                return self._generate_historical_data_traditional(etf_code, df)
                
        except Exception as e:
            print(f"   ❌ {etf_code}: 超高性能历史数据生成异常: {str(e)}，回退到传统方法")
            # 回退到传统方法
            return self._generate_historical_data_traditional(etf_code, df)
    
    def _generate_historical_data_traditional(self, etf_code: str, df: pd.DataFrame) -> Optional[pd.DataFrame]:
        """传统方法生成ETF的完整历史数据（备用方法）"""
        try:
            # 列名映射：中文转英文
            column_mapping = {
                '日期': 'date',
                '收盘价': 'close'
            }
            df_calc = df.rename(columns=column_mapping)
            
            # 确保数据按日期升序排列进行SMA计算
            df_calc = df_calc.sort_values('date').reset_index(drop=True)
            
            # 计算所有SMA指标（安全的类型转换）
            try:
                prices = df_calc['close'].astype(float)
                # 移除NaN值
                prices = prices.dropna()
                if prices.empty:
                    print(f"   ❌ {etf_code}: 价格数据清理后为空")
                    return None
            except (ValueError, TypeError) as e:
                print(f"   ❌ {etf_code}: 价格数据类型转换失败: {str(e)}")
                return None
            
            # 创建结果DataFrame
            result_df = pd.DataFrame({
                '代码': etf_code.replace('.SH', '').replace('.SZ', ''),
                '日期': df_calc['date']
            })
            
            # 批量计算所有SMA
            for period in self.config.sma_periods:
                sma_series = prices.rolling(window=period, min_periods=period).mean()
                result_df[f'MA{period}'] = sma_series.round(6)
            
            # 批量计算SMA差值
            self._calculate_sma_differences(result_df)
            
            # 确保按时间倒序排列（最新在顶部）
            result_df = result_df.sort_values('日期', ascending=False).reset_index(drop=True)
            
            print(f"   📊 {etf_code}: 传统方法历史数据生成完成 - {len(result_df)}行")
            return result_df
            
        except Exception as e:
            print(f"   ❌ {etf_code}: 传统方法历史数据生成失败: {str(e)}")
            return None
    
    def _calculate_sma_differences(self, result_df: pd.DataFrame):
        """计算SMA差值指标"""
        try:
            # SMA差值5-20
            if 'SMA_5' in result_df.columns and 'SMA_20' in result_df.columns:
                ma5 = result_df['SMA_5']
                ma20 = result_df['SMA_20']
                
                result_df['SMA_DIFF_5_20'] = np.where(
                    (ma5.notna()) & (ma20.notna()),
                    (ma5 - ma20).round(6),
                    ''
                )
                
                # 安全的百分比计算，避免除零风险
                ma20_safe = ma20.replace(0, np.nan)  # 将0替换为NaN
                result_df['SMA_DIFF_5_20_PCT'] = np.where(
                    (ma5.notna()) & (ma20_safe.notna()),
                    ((ma5 - ma20_safe) / ma20_safe * 100).round(4),
                    ''
                )
            
            # SMA差值5-10
            if 'SMA_5' in result_df.columns and 'SMA_10' in result_df.columns:
                ma5 = result_df['SMA_5']
                ma10 = result_df['SMA_10']
                
                result_df['SMA_DIFF_5_10'] = np.where(
                    (ma5.notna()) & (ma10.notna()),
                    (ma5 - ma10).round(6),
                    ''
                )
                
        except Exception as e:
            print(f"   ⚠️ SMA差值计算失败: {str(e)}")
    
    def _perform_advanced_analysis(self, sma_values: Dict) -> Dict:
        """执行高级分析"""
        # 这里可以添加更复杂的技术分析逻辑
        return {
            'trend_analysis': 'advanced_analysis_placeholder',
            'support_resistance': 'advanced_analysis_placeholder',
            'sma_count': len([k for k, v in sma_values.items() if v is not None])
        } 