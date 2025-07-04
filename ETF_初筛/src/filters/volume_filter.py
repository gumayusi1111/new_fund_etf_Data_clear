#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
流动性门槛筛选器
实现request.md中定义的流动性门槛筛选逻辑
严格基于11个基础字段
"""

import pandas as pd
import numpy as np
from typing import Dict, Any, Tuple

from .base_filter import BaseFilter, FilterResult


class VolumeFilter(BaseFilter):
    """流动性门槛筛选器 - 基于request.md设计"""
    
    def __init__(self, config: Dict[str, Any] = None, threshold_name: str = "5000万门槛"):
        super().__init__(f"VolumeFilter({threshold_name})", config)
        
        self.threshold_name = threshold_name
        
        # 从配置获取参数
        liquidity_config = self.config.get("流动性门槛", {})
        filter_config = self.config.get("筛选配置", {})
        
        # 获取指定门槛的配置
        threshold_config = liquidity_config.get(threshold_name, {})
        
        # 如果找不到指定门槛配置，使用默认配置（兼容旧格式）
        if not threshold_config:
            if "日均成交额基准_万元" in liquidity_config:
                threshold_config = liquidity_config
            else:
                threshold_config = liquidity_config.get("5000万门槛", {})
        
        self.observation_days = filter_config.get("观察期_天数", 30)
        self.min_history_days = filter_config.get("最小历史数据天数", 60)
        self.daily_volume_base = threshold_config.get("日均成交额基准_万元", 5000) * 10  # 转为千元
        self.zero_volume_days_limit = threshold_config.get("零成交量天数限制", 3)
        self.consecutive_zero_days_limit = threshold_config.get("连续零成交天数限制", 2)
        self.fake_liquidity_multiplier = threshold_config.get("虚假流动性倍数", 5)
        self.price_match_error = threshold_config.get("成交价格匹配误差", 0.05)
        self.dynamic_threshold_quantile = threshold_config.get("动态阈值_分位数", 0.3)
    
    def filter_single_etf(self, etf_code: str, df: pd.DataFrame) -> FilterResult:
        """筛选单个ETF的流动性"""
        
        # 数据有效性检查 - 优先检查最小历史数据天数
        if not self.is_valid_data(df, self.min_history_days):
            return FilterResult(
                etf_code=etf_code,
                passed=False,
                score=0.0,
                reason=f"历史数据不足{self.min_history_days}天(实际{len(df)}天)",
                metrics={"交易天数": len(df), "要求天数": self.min_history_days}
            )
        
        # 计算流动性指标
        liquidity_metrics = self._calculate_liquidity_metrics(df)
        
        # 执行流动性检查
        check_result, reason = self._check_liquidity_requirements(liquidity_metrics)
        
        return FilterResult(
            etf_code=etf_code,
            passed=check_result,
            score=100.0 if check_result else 0.0,
            reason=reason,
            metrics=liquidity_metrics
        )
    
    def _calculate_liquidity_metrics(self, df: pd.DataFrame) -> Dict[str, Any]:
        """计算流动性指标"""
        try:
            metrics = {}
            
            # 取最近观察期的数据
            recent_df = df.tail(self.observation_days).copy()
            
            # 1. 基础流动性指标
            volume_amount = recent_df['成交额(千元)'] if '成交额(千元)' in recent_df.columns else pd.Series()
            volume_shares = recent_df['成交量(手数)'] if '成交量(手数)' in recent_df.columns else pd.Series()
            
            if not volume_amount.empty:
                metrics["日均成交额"] = volume_amount.mean()
                metrics["单日最大成交额"] = volume_amount.max()
                
                # 零成交量统计
                zero_volume_days = (volume_shares == 0).sum() if not volume_shares.empty else 0
                metrics["零成交量天数"] = zero_volume_days
                
                # 连续零成交量统计
                consecutive_zero_days = self._get_max_consecutive_zero_volume(volume_shares)
                metrics["连续零成交天数"] = consecutive_zero_days
                
                # 成交量对应的平均价格
                if not volume_shares.empty and '收盘价' in recent_df.columns:
                    close_prices = recent_df['收盘价']
                    valid_data = (volume_shares > 0) & (volume_amount > 0) & (close_prices > 0)
                    
                    if valid_data.any():
                        # 计算成交价格匹配性（增加除零保护）
                        volume_shares_safe = volume_shares[valid_data]
                        # 再次检查以防除零（双重保护）
                        non_zero_shares = volume_shares_safe > 0
                        if non_zero_shares.any():
                            trade_prices = (volume_amount[valid_data] * 1000) / (volume_shares_safe * 100)  # 千元转元，手数转股数
                            price_diff = abs(trade_prices - close_prices[valid_data]) / close_prices[valid_data]
                            metrics["平均价格匹配误差"] = price_diff.mean()
                            metrics["最大价格匹配误差"] = price_diff.max()
                        else:
                            # 如果所有成交量都为零，设置默认值
                            metrics["平均价格匹配误差"] = 0.0
                            metrics["最大价格匹配误差"] = 0.0
            
            return metrics
            
        except Exception as e:
            self.logger.error(f"计算流动性指标失败: {e}")
            return {}
    
    def _get_max_consecutive_zero_volume(self, volume_series: pd.Series) -> int:
        """计算最大连续零成交量天数"""
        if volume_series.empty:
            return 0
        
        max_consecutive = 0
        current_consecutive = 0
        
        for volume in volume_series:
            if volume == 0:
                current_consecutive += 1
                max_consecutive = max(max_consecutive, current_consecutive)
            else:
                current_consecutive = 0
        
        return max_consecutive
    
    def _check_liquidity_requirements(self, metrics: Dict[str, Any]) -> Tuple[bool, str]:
        """检查流动性要求"""
        issues = []
        
        # 1. 日均成交额基础要求
        daily_avg_amount = metrics.get("日均成交额", 0)
        if daily_avg_amount < self.daily_volume_base:
            issues.append(f"日均成交额不足({daily_avg_amount:.0f}千元<{self.daily_volume_base}千元)")
        
        # 2. 零成交量天数检查
        zero_volume_days = metrics.get("零成交量天数", 999)
        if zero_volume_days > self.zero_volume_days_limit:
            issues.append(f"零成交量天数过多({zero_volume_days}>{self.zero_volume_days_limit}天)")
        
        # 3. 连续零成交量检查
        consecutive_zero_days = metrics.get("连续零成交天数", 999)
        if consecutive_zero_days > self.consecutive_zero_days_limit:
            issues.append(f"连续零成交量天数过多({consecutive_zero_days}>{self.consecutive_zero_days_limit}天)")
        
        # 4. 虚假流动性检测
        max_daily_amount = metrics.get("单日最大成交额", 0)
        if daily_avg_amount > 0 and max_daily_amount > daily_avg_amount * self.fake_liquidity_multiplier:
            issues.append(f"可能存在虚假流动性(单日最大{max_daily_amount:.0f}千元 > 日均{daily_avg_amount:.0f}千元×{self.fake_liquidity_multiplier})")
        
        # 5. 成交价格匹配性检查
        avg_price_match_error = metrics.get("平均价格匹配误差", 0)
        if avg_price_match_error > self.price_match_error:
            issues.append(f"成交价格匹配异常(误差{avg_price_match_error:.1%}>{self.price_match_error:.1%})")
        
        # 综合判断
        if len(issues) == 0:
            return True, "流动性门槛检查通过"
        else:
            return False, "; ".join(issues)
    
    def get_filter_description(self) -> Dict[str, Any]:
        """获取筛选器说明"""
        return {
            "筛选器名称": self.name,
            "设计依据": "request.md - 流动性门槛筛选",
            "检查项目": {
                "历史数据要求": f"至少{self.min_history_days}天交易数据",
                "日均成交额基准": f"{self.daily_volume_base}千元",
                "零成交量天数限制": f"≤{self.zero_volume_days_limit}天",
                "连续零成交天数限制": f"≤{self.consecutive_zero_days_limit}天",
                "虚假流动性检测": f"单日峰值≤日均×{self.fake_liquidity_multiplier}",
                "成交价格匹配": f"误差≤{self.price_match_error:.1%}"
            },
            "使用字段": ["成交额(千元)", "成交量(手数)", "收盘价"],
            "判断逻辑": "硬性门槛，任一项不满足即剔除"
        } 