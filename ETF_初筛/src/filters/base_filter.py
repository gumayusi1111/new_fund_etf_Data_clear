#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
基础筛选器
定义ETF筛选器的基本接口和通用方法
"""

import pandas as pd
from abc import ABC, abstractmethod
from typing import Dict, Any, List, Tuple, Optional
from dataclasses import dataclass
from datetime import datetime

from ..utils.logger import get_logger


@dataclass
class FilterResult:
    """筛选结果数据类"""
    etf_code: str
    passed: bool
    score: float  # 筛选得分 0-100
    reason: str   # 筛选原因/结果说明
    metrics: Dict[str, Any]  # 计算的指标值
    
    def __str__(self):
        status = "✅ 通过" if self.passed else "❌ 未通过"
        return f"{self.etf_code}: {status} (得分: {self.score:.1f}) - {self.reason}"


class BaseFilter(ABC):
    """
    基础筛选器抽象类
    所有具体筛选器都应继承此类
    """
    
    def __init__(self, name: str, config: Dict[str, Any] = None):
        """
        初始化筛选器
        
        Args:
            name: 筛选器名称
            config: 筛选器配置参数
        """
        self.name = name
        self.config = config or {}
        self.logger = get_logger()
    
    @abstractmethod
    def filter_single_etf(self, etf_code: str, df: pd.DataFrame) -> FilterResult:
        """
        筛选单个ETF
        
        Args:
            etf_code: ETF代码
            df: ETF数据DataFrame
        
        Returns:
            筛选结果
        """
        pass
    
    def filter_multiple_etfs(self, etf_data: Dict[str, pd.DataFrame]) -> Dict[str, FilterResult]:
        """
        批量筛选多个ETF
        
        Args:
            etf_data: ETF代码到DataFrame的字典
        
        Returns:
            ETF代码到筛选结果的字典
        """
        results = {}
        
        self.logger.info(f"🔍 开始执行筛选器: {self.name}")
        
        for etf_code, df in etf_data.items():
            try:
                result = self.filter_single_etf(etf_code, df)
                results[etf_code] = result
                
                # 记录详细结果
                self.logger.debug(f"  {result}")
                
            except Exception as e:
                self.logger.error(f"筛选ETF失败 {etf_code}: {e}")
                results[etf_code] = FilterResult(
                    etf_code=etf_code,
                    passed=False,
                    score=0.0,
                    reason=f"筛选异常: {str(e)}",
                    metrics={}
                )
        
        # 统计结果
        passed_count = sum(1 for r in results.values() if r.passed)
        total_count = len(results)
        pass_rate = (passed_count / total_count * 100) if total_count > 0 else 0
        
        self.logger.info(f"📊 {self.name} 筛选完成: {passed_count}/{total_count} 通过 ({pass_rate:.1f}%)")
        
        return results
    
    def get_summary_stats(self, results: Dict[str, FilterResult]) -> Dict[str, Any]:
        """
        获取筛选结果统计
        
        Args:
            results: 筛选结果字典
        
        Returns:
            统计信息
        """
        if not results:
            return {}
        
        passed_results = [r for r in results.values() if r.passed]
        failed_results = [r for r in results.values() if not r.passed]
        
        # 增强除零保护
        total_count = len(results)
        passed_count = len(passed_results)
        
        stats = {
            "筛选器名称": self.name,
            "总ETF数": total_count,
            "通过数": passed_count,
            "未通过数": len(failed_results),
            "通过率": (passed_count / total_count * 100) if total_count > 0 else 0,
            "平均得分": (sum(r.score for r in results.values()) / total_count) if total_count > 0 else 0,
            "通过ETF得分": {
                "平均": (sum(r.score for r in passed_results) / passed_count) if passed_count > 0 else 0,
                "最高": max(r.score for r in passed_results) if passed_results else 0,
                "最低": min(r.score for r in passed_results) if passed_results else 0
            }
        }
        
        return stats
    
    # 通用计算方法
    
    def calculate_basic_metrics(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        计算基础指标
        
        Args:
            df: ETF数据
        
        Returns:
            基础指标字典
        """
        if df.empty:
            return {}
        
        try:
            metrics = {
                "交易天数": len(df),
                "最新收盘价": df['收盘价'].iloc[-1] if '收盘价' in df.columns else 0,
                "平均成交量": df['成交量(手数)'].mean() if '成交量(手数)' in df.columns else 0,
                "平均成交额": df['成交额(千元)'].mean() if '成交额(千元)' in df.columns else 0,
                "平均涨幅": df['涨幅%'].mean() if '涨幅%' in df.columns else 0,
                "涨幅标准差": df['涨幅%'].std() if '涨幅%' in df.columns else 0,
                "最大单日涨幅": df['涨幅%'].max() if '涨幅%' in df.columns else 0,
                "最大单日跌幅": df['涨幅%'].min() if '涨幅%' in df.columns else 0,
                "价格区间": {
                    "最高": df['最高价'].max() if '最高价' in df.columns else 0,
                    "最低": df['最低价'].min() if '最低价' in df.columns else 0
                }
            }
            
            return metrics
        except Exception as e:
            self.logger.error(f"计算基础指标失败: {e}")
            return {}
    
    def calculate_trend_metrics(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        计算趋势指标
        
        Args:
            df: ETF数据
        
        Returns:
            趋势指标字典
        """
        if df.empty or len(df) < 2:
            return {}
        
        try:
            close_prices = df['收盘价']
            
            # 计算累计收益率（增加除零保护）
            if close_prices.iloc[0] > 0:
                total_return = (close_prices.iloc[-1] / close_prices.iloc[0] - 1) * 100
            else:
                total_return = 0.0
            
            # 计算最大回撤
            cumulative_nav = (1 + df['涨幅%'] / 100).cumprod()
            peak_values = cumulative_nav.cummax()
            drawdowns = (cumulative_nav / peak_values - 1) * 100
            max_drawdown = drawdowns.min()
            
            # 计算夏普比率（简化版，假设无风险利率为3%）
            annual_return = total_return * (252 / len(df))  # 假设252个交易日
            annual_volatility = df['涨幅%'].std() * (252 ** 0.5)
            sharpe_ratio = (annual_return - 3) / annual_volatility if annual_volatility > 0 else 0
            
            # 胜率（上涨天数比例）
            win_rate = (df['涨幅%'] > 0).mean() * 100
            
            metrics = {
                "总收益率": total_return,
                "年化收益率": annual_return,
                "年化波动率": annual_volatility,
                "最大回撤": abs(max_drawdown),
                "夏普比率": sharpe_ratio,
                "胜率": win_rate,
                "趋势方向": "上涨" if total_return > 0 else "下跌"
            }
            
            return metrics
        except Exception as e:
            self.logger.error(f"计算趋势指标失败: {e}")
            return {}
    
    def calculate_liquidity_metrics(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        计算流动性指标
        
        Args:
            df: ETF数据
        
        Returns:
            流动性指标字典
        """
        if df.empty:
            return {}
        
        try:
            # 成交量稳定性（变异系数）
            volume_stability = df['成交量(手数)'].std() / df['成交量(手数)'].mean() if df['成交量(手数)'].mean() > 0 else float('inf')
            
            # 零成交量天数
            zero_volume_days = (df['成交量(手数)'] == 0).sum()
            zero_volume_ratio = zero_volume_days / len(df) * 100
            
            # 平均换手率（简化计算，假设流通股本未知）
            # 这里用成交额/市值的近似值
            
            metrics = {
                "平均成交量": df['成交量(手数)'].mean(),
                "成交量稳定性": volume_stability,
                "零成交量天数": zero_volume_days,
                "零成交量比例": zero_volume_ratio,
                "成交量中位数": df['成交量(手数)'].median(),
                "成交额中位数": df['成交额(千元)'].median()
            }
            
            return metrics
        except Exception as e:
            self.logger.error(f"计算流动性指标失败: {e}")
            return {}
    
    def is_valid_data(self, df: pd.DataFrame, min_days: int = 30) -> bool:
        """
        验证数据有效性
        
        Args:
            df: ETF数据
            min_days: 最小交易天数
        
        Returns:
            数据是否有效
        """
        if df.empty or len(df) < min_days:
            return False
        
        # 检查必要字段
        required_columns = ['收盘价', '成交量(手数)', '涨幅%']
        for col in required_columns:
            if col not in df.columns or df[col].isna().all():
                return False
        
        return True 