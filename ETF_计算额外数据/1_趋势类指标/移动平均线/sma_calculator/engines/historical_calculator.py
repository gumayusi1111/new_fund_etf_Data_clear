#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
历史数据计算器 (重构版)
====================

专门负责历史数据分析和处理的计算引擎
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta


class HistoricalCalculator:
    """历史数据计算器 - 重构版"""
    
    def __init__(self):
        """初始化历史数据计算器"""
        pass
    
    def build_full_dataframe(self, df: pd.DataFrame, sma_results: Dict) -> pd.DataFrame:
        """
        构建包含SMA指标的完整DataFrame
        
        Args:
            df: 原始ETF数据
            sma_results: SMA计算结果
            
        Returns:
            pd.DataFrame: 包含SMA指标的完整数据
        """
        if df.empty:
            return pd.DataFrame()
        
        try:
            # 复制原始数据
            result_df = df.copy()
            
            # 重新计算所有SMA序列
            sma_columns = self._calculate_sma_series(df['收盘价'], sma_results)
            
            # 添加SMA列到DataFrame
            for col_name, sma_series in sma_columns.items():
                result_df[col_name] = sma_series
            
            # 计算差值序列
            diff_columns = self._calculate_diff_series(result_df)
            for col_name, diff_series in diff_columns.items():
                result_df[col_name] = diff_series
            
            # 按时间倒序排列
            result_df = result_df.sort_values('日期', ascending=False).reset_index(drop=True)
            
            return result_df
            
        except Exception as e:
            print(f"构建完整DataFrame失败: {str(e)}")
            return df.copy()
    
    def _calculate_sma_series(self, prices: pd.Series, sma_results: Dict) -> Dict[str, pd.Series]:
        """计算SMA时间序列"""
        sma_columns = {}
        
        # 默认SMA周期
        periods = [5, 10, 20, 60]
        
        for period in periods:
            col_name = f'MA{period}'
            if len(prices) >= period:
                sma_series = prices.rolling(window=period, min_periods=period).mean()
                sma_columns[col_name] = sma_series.round(6)
            else:
                sma_columns[col_name] = pd.Series([np.nan] * len(prices), index=prices.index)
        
        return sma_columns
    
    def _calculate_diff_series(self, df: pd.DataFrame) -> Dict[str, pd.Series]:
        """计算差值序列"""
        diff_columns = {}
        
        # 定义差值计算
        diff_pairs = [
            ('MA5', 'MA20', 'MA5_MA20_DIFF'),
            ('MA5', 'MA10', 'MA5_MA10_DIFF'),
        ]
        
        for short_col, long_col, diff_col in diff_pairs:
            if short_col in df.columns and long_col in df.columns:
                diff_series = df[short_col] - df[long_col]
                diff_columns[diff_col] = diff_series.round(6)
        
        # 计算相对差值百分比（安全除法）
        if 'MA5' in df.columns and 'MA20' in df.columns:
            ma5 = df['MA5']
            ma20 = df['MA20']
            # 使用向量化的安全除法
            ma20_safe = ma20.replace(0, np.nan)  # 将0替换为NaN
            ma5_ma20_pct = np.where(
                (ma5.notna()) & (ma20_safe.notna()),
                ((ma5 - ma20_safe) / ma20_safe * 100).round(4),
                np.nan
            )
            diff_columns['MA5_MA20_DIFF_PCT'] = pd.Series(ma5_ma20_pct, index=df.index)
        
        return diff_columns
    
    def calculate_period_statistics(self, df: pd.DataFrame, days: int = 30) -> Dict:
        """
        计算指定周期的统计信息
        
        Args:
            df: 数据DataFrame
            days: 统计周期天数
            
        Returns:
            Dict: 统计结果
        """
        try:
            if df.empty or len(df) < days:
                return {}
            
            # 取最近的数据
            recent_data = df.head(days)  # 数据已按时间倒序
            
            stats = {
                'period_days': days,
                'data_points': len(recent_data),
                'price_statistics': self._calculate_price_stats(recent_data),
                'sma_statistics': self._calculate_sma_stats(recent_data),
                'trend_analysis': self._analyze_trend(recent_data)
            }
            
            return stats
            
        except Exception as e:
            print(f"计算周期统计失败: {str(e)}")
            return {}
    
    def _calculate_price_stats(self, df: pd.DataFrame) -> Dict:
        """计算价格统计信息"""
        if '收盘价' not in df.columns:
            return {}
        
        prices = df['收盘价'].dropna()
        if prices.empty:
            return {}
        
        last_price = prices.iloc[-1]
        price_change_pct = 0
        if len(prices) > 1 and abs(last_price) > 1e-10:
            price_change_pct = round(float((prices.iloc[0] - last_price) / last_price * 100), 2)
        
        return {
            'max_price': round(float(prices.max()), 6),
            'min_price': round(float(prices.min()), 6),
            'avg_price': round(float(prices.mean()), 6),
            'price_volatility': round(float(prices.std()), 6),
            'price_change_pct': price_change_pct
        }
    
    def _calculate_sma_stats(self, df: pd.DataFrame) -> Dict:
        """计算SMA统计信息"""
        sma_stats = {}
        
        sma_columns = ['MA5', 'MA10', 'MA20', 'MA60']
        for col in sma_columns:
            if col in df.columns:
                sma_data = df[col].dropna()
                if not sma_data.empty:
                    sma_stats[col] = {
                        'latest': round(float(sma_data.iloc[0]), 6),
                        'average': round(float(sma_data.mean()), 6),
                        'volatility': round(float(sma_data.std()), 6)
                    }
        
        return sma_stats
    
    def _analyze_trend(self, df: pd.DataFrame) -> Dict:
        """分析趋势信息"""
        trend_analysis = {}
        
        # MA5-MA20差值趋势
        if 'MA5_MA20_DIFF' in df.columns:
            diff_data = df['MA5_MA20_DIFF'].dropna()
            if not diff_data.empty:
                latest_diff = float(diff_data.iloc[0])
                avg_diff = float(diff_data.mean())
                
                trend_analysis['ma5_ma20_trend'] = {
                    'latest_diff': round(latest_diff, 6),
                    'average_diff': round(avg_diff, 6),
                    'trend_direction': 'bullish' if latest_diff > 0 else 'bearish' if latest_diff < 0 else 'neutral',
                    'trend_strength': 'strong' if abs(latest_diff) > abs(avg_diff) * 1.5 else 'moderate'
                }
        
        return trend_analysis
    
    def format_historical_data(self, df: pd.DataFrame, max_rows: int = 100) -> str:
        """
        格式化历史数据为CSV字符串
        
        Args:
            df: 数据DataFrame
            max_rows: 最大行数
            
        Returns:
            str: CSV格式字符串
        """
        try:
            if df.empty:
                return ""
            
            # 限制行数
            display_df = df.head(max_rows) if len(df) > max_rows else df
            
            # 转换为CSV字符串
            csv_string = display_df.to_csv(index=False, encoding='utf-8', float_format='%.6f')
            
            return csv_string
            
        except Exception as e:
            print(f"格式化历史数据失败: {str(e)}")
            return ""
    
    def get_data_quality_metrics(self, df: pd.DataFrame) -> Dict:
        """获取数据质量指标"""
        try:
            if df.empty:
                return {'quality_score': 0, 'issues': ['数据为空']}
            
            issues = []
            quality_score = 100
            
            # 检查数据完整性
            if '收盘价' in df.columns:
                price_na_count = df['收盘价'].isna().sum()
                if price_na_count > 0:
                    issues.append(f'收盘价缺失{price_na_count}个')
                    quality_score -= min(price_na_count * 5, 30)
            
            # 检查日期连续性
            if '日期' in df.columns and len(df) > 1:
                date_gaps = self._check_date_continuity(df['日期'])
                if date_gaps > 0:
                    issues.append(f'日期间隔{date_gaps}处')
                    quality_score -= min(date_gaps * 2, 20)
            
            # 检查SMA计算完整性
            sma_columns = ['MA5', 'MA10', 'MA20', 'MA60']
            missing_sma = [col for col in sma_columns if col not in df.columns]
            if missing_sma:
                issues.append(f'缺失SMA指标: {missing_sma}')
                quality_score -= len(missing_sma) * 10
            
            quality_level = ('优秀' if quality_score >= 90 else 
                           '良好' if quality_score >= 80 else 
                           '一般' if quality_score >= 70 else '较差')
            
            return {
                'quality_score': max(0, quality_score),
                'quality_level': quality_level,
                'total_records': len(df),
                'issues': issues if issues else ['无明显问题']
            }
            
        except Exception as e:
            return {'quality_score': 0, 'issues': [f'质量检查失败: {str(e)}']}
    
    def _check_date_continuity(self, dates: pd.Series) -> int:
        """检查日期连续性"""
        try:
            sorted_dates = dates.sort_values()
            gaps = 0
            
            for i in range(1, len(sorted_dates)):
                current_date = pd.to_datetime(sorted_dates.iloc[i])
                prev_date = pd.to_datetime(sorted_dates.iloc[i-1])
                
                # 检查是否有异常大的间隔（超过10天）
                if (current_date - prev_date).days > 10:
                    gaps += 1
            
            return gaps
            
        except Exception:
            return 0 