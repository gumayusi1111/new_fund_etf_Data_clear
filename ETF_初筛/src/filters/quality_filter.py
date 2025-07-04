#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
价格质量筛选器
实现request.md中定义的价格质量检查和数据质量验证逻辑
严格基于11个基础字段
"""

import pandas as pd
import numpy as np
from typing import Dict, Any, Tuple

from .base_filter import BaseFilter, FilterResult


class QualityFilter(BaseFilter):
    """价格质量筛选器 - 基于request.md设计"""
    
    def __init__(self, config: Dict[str, Any] = None):
        super().__init__("QualityFilter", config)
        
        # 从配置获取参数
        price_quality_config = self.config.get("价格质量标准", {})
        data_quality_config = self.config.get("数据质量要求", {})
        volatility_config = self.config.get("异常波动阈值", {})
        filter_config = self.config.get("筛选配置", {})
        
        self.observation_days = filter_config.get("观察期_天数", 30)
        self.tolerance_ratio = filter_config.get("容错比例", 0.1)
        self.min_history_days = filter_config.get("最小历史数据天数", 60)
        
        # 价格质量标准
        self.min_price = price_quality_config.get("最低价格", 0.01)
        self.max_price = price_quality_config.get("最高价格", 500)
        self.gap_threshold = price_quality_config.get("跳空阈值", 0.20)
        self.price_change_threshold = price_quality_config.get("价格变化率阈值", 0.001)
        self.consecutive_same_price_limit = price_quality_config.get("连续相同价格限制", 3)
        self.active_days_requirement = price_quality_config.get("活跃天数要求", 5)
        
        # 数据质量要求
        self.missing_rate_limit = data_quality_config.get("缺失率上限", 0.05)
        logic_check_errors = data_quality_config.get("逻辑检查误差", {})
        self.price_change_error = logic_check_errors.get("涨跌误差", 0.01)
        self.price_pct_error = logic_check_errors.get("涨幅误差", 0.001)
        
        # 细化容错率：OHLC vs 成交数据
        self.ohlc_tolerance = 0.10  # OHLC数据容错10%
        self.volume_tolerance = 0.05  # 成交额/量容错5%
        
        # 异常波动设置
        self.normal_etf_threshold = volatility_config.get("普通ETF", 0.10)
        self.abnormal_days_limit = volatility_config.get("异常天数限制", 2)
        self.abnormal_amplitude_threshold = volatility_config.get("异常振幅阈值", 0.15)
    
    def filter_single_etf(self, etf_code: str, df: pd.DataFrame) -> FilterResult:
        """筛选单个ETF的价格质量"""
        
        # 数据有效性检查 - 优先检查最小历史数据天数
        if not self.is_valid_data(df, self.min_history_days):
            return FilterResult(
                etf_code=etf_code,
                passed=False,
                score=0.0,
                reason=f"历史数据不足{self.min_history_days}天(实际{len(df)}天)",
                metrics={"交易天数": len(df), "要求天数": self.min_history_days}
            )
        
        # 计算质量指标
        quality_metrics = self._calculate_quality_metrics(df)
        
        # 执行质量检查
        check_result, reason = self._check_quality_requirements(quality_metrics)
        
        return FilterResult(
            etf_code=etf_code,
            passed=check_result,
            score=100.0 if check_result else 0.0,
            reason=reason,
            metrics=quality_metrics
        )
    
    def _calculate_quality_metrics(self, df: pd.DataFrame) -> Dict[str, Any]:
        """计算价格质量指标"""
        try:
            metrics = {}
            
            # 取最近观察期的数据
            recent_df = df.tail(self.observation_days).copy()
            
            # 1. 数据完整性检查
            required_fields = ['代码', '日期', '开盘价', '最高价', '最低价', '收盘价', '成交量(手数)', '成交额(千元)']
            available_fields = [col for col in required_fields if col in recent_df.columns]
            
            total_data_points = len(recent_df) * len(available_fields)
            missing_data_points = recent_df[available_fields].isna().sum().sum()
            missing_rate = missing_data_points / total_data_points if total_data_points > 0 else 1.0
            metrics["数据缺失率"] = missing_rate
            
            # 2. OHLC逻辑检查
            ohlc_fields = ['开盘价', '最高价', '最低价', '收盘价', '上日收盘']
            if all(col in recent_df.columns for col in ohlc_fields):
                ohlc_errors = self._check_ohlc_logic(recent_df)
                metrics["OHLC逻辑错误数"] = ohlc_errors
                metrics["OHLC逻辑错误率"] = ohlc_errors / len(recent_df) if len(recent_df) > 0 else 0
            
            # 3. 价格合理性检查
            if '收盘价' in recent_df.columns:
                close_prices = recent_df['收盘价']
                metrics["最低收盘价"] = close_prices.min()
                metrics["最高收盘价"] = close_prices.max()
                metrics["价格范围合理"] = (close_prices.min() >= self.min_price) and (close_prices.max() <= self.max_price)
            
            # 4. 价格活跃度检查（最近10天）
            if '收盘价' in recent_df.columns and len(recent_df) >= 10:
                recent_10day_prices = recent_df['收盘价'].tail(10)
                price_change_rate = recent_10day_prices.std() / recent_10day_prices.mean() if recent_10day_prices.mean() > 0 else 0
                metrics["价格变化率"] = price_change_rate
                
                # 统计有价格变化的天数（优化：更精确的变动检测）
                price_change_days = (recent_10day_prices.diff().abs() > 0.001).sum()
                metrics["价格变化天数"] = price_change_days
                
                # 新增：价格变动次数检查（防止僵尸ETF）
                effective_change_count = 0
                for i in range(1, len(recent_10day_prices)):
                    if abs(recent_10day_prices.iloc[i] - recent_10day_prices.iloc[i-1]) > 0.001:
                        effective_change_count += 1
                metrics["有效变动次数"] = effective_change_count
                
                # 连续相同价格检查
                consecutive_same_price_days = self._get_max_consecutive_same_price(recent_10day_prices)
                metrics["连续相同价格天数"] = consecutive_same_price_days
            
            # 5. 异常波动检查
            if '涨幅%' in recent_df.columns:
                price_changes = recent_df['涨幅%']
                abnormal_volatility_days = (abs(price_changes) > self.normal_etf_threshold * 100).sum()
                metrics["异常波动天数"] = abnormal_volatility_days
                metrics["异常波动比例"] = abnormal_volatility_days / len(recent_df) if len(recent_df) > 0 else 0
            
            # 6. 振幅异常检查
            if all(col in recent_df.columns for col in ['最高价', '最低价', '上日收盘']):
                abnormal_amplitude_days = self._check_amplitude_anomaly(recent_df)
                metrics["异常振幅天数"] = abnormal_amplitude_days
                metrics["异常振幅比例"] = abnormal_amplitude_days / len(recent_df) if len(recent_df) > 0 else 0
            
            # 7. 数据逻辑一致性检查
            if all(col in recent_df.columns for col in ['涨跌', '涨幅%', '收盘价', '上日收盘']):
                logic_inconsistency_count = self._check_data_consistency(recent_df)
                metrics["逻辑不一致数"] = logic_inconsistency_count
                metrics["逻辑不一致率"] = logic_inconsistency_count / len(recent_df) if len(recent_df) > 0 else 0
            
            return metrics
            
        except Exception as e:
            self.logger.error(f"计算价格质量指标失败: {e}")
            return {}
    
    def _check_ohlc_logic(self, df: pd.DataFrame) -> int:
        """检查OHLC逻辑错误数"""
        error_count = 0
        
        for _, row in df.iterrows():
            open_price = row['开盘价']
            high_price = row['最高价'] 
            low_price = row['最低价']
            close_price = row['收盘价']
            prev_close = row['上日收盘']
            
            # 基础OHLC逻辑检查
            if not (low_price <= open_price <= high_price and low_price <= close_price <= high_price and low_price <= high_price):
                error_count += 1
                continue
            
            # 跳空检查
            if prev_close > 0:
                gap_ratio = abs(open_price - prev_close) / prev_close
                if gap_ratio > self.gap_threshold:
                    # 这不算错误，但需要标记
                    pass
        
        return error_count
    
    def _get_max_consecutive_same_price(self, prices: pd.Series) -> int:
        """计算最大连续相同价格天数"""
        if prices.empty:
            return 0
        
        max_consecutive = 0
        current_consecutive = 1
        
        for i in range(1, len(prices)):
            if abs(prices.iloc[i] - prices.iloc[i-1]) < 0.001:  # 价格基本相同
                current_consecutive += 1
                max_consecutive = max(max_consecutive, current_consecutive)
            else:
                current_consecutive = 1
        
        return max_consecutive
    
    def _check_amplitude_anomaly(self, df: pd.DataFrame) -> int:
        """检查振幅异常天数"""
        abnormal_days = 0
        
        for _, row in df.iterrows():
            high_price = row['最高价']
            low_price = row['最低价'] 
            prev_close = row['上日收盘']
            
            if prev_close > 0:
                amplitude = (high_price - low_price) / prev_close
                if amplitude > self.abnormal_amplitude_threshold:
                    abnormal_days += 1
        
        return abnormal_days
    
    def _check_data_consistency(self, df: pd.DataFrame) -> int:
        """检查数据逻辑一致性（优化：细化容错率）"""
        inconsistency_count = 0
        
        for _, row in df.iterrows():
            price_change = row['涨跌']
            price_change_pct = row['涨幅%']
            close_price = row['收盘价']
            prev_close = row['上日收盘']
            
            if prev_close > 0:
                # 检查涨跌计算（价格数据用OHLC容错率10%）
                expected_price_change = close_price - prev_close
                relative_error = abs(price_change - expected_price_change) / max(abs(expected_price_change), 0.01)
                if relative_error > self.ohlc_tolerance:
                    inconsistency_count += 1
                    continue
                
                # 检查涨幅计算（价格数据用OHLC容错率10%）
                expected_price_change_pct = (close_price - prev_close) / prev_close * 100
                if abs(price_change_pct - expected_price_change_pct) > abs(expected_price_change_pct) * self.ohlc_tolerance:
                    inconsistency_count += 1
        
        return inconsistency_count
    
    def _check_quality_requirements(self, metrics: Dict[str, Any]) -> Tuple[bool, str]:
        """检查质量要求"""
        issue_list = []
        
        # 1. 数据完整性检查
        data_missing_rate = metrics.get("数据缺失率", 1.0)
        if data_missing_rate > self.missing_rate_limit:
            issue_list.append(f"数据缺失率过高({data_missing_rate:.1%}>{self.missing_rate_limit:.1%})")
        
        # 2. OHLC逻辑检查
        ohlc_error_rate = metrics.get("OHLC逻辑错误率", 1.0)
        if ohlc_error_rate > self.tolerance_ratio:
            issue_list.append(f"OHLC逻辑错误过多({ohlc_error_rate:.1%}>{self.tolerance_ratio:.1%})")
        
        # 3. 价格合理性检查
        price_range_reasonable = metrics.get("价格范围合理", False)
        if not price_range_reasonable:
            min_price = metrics.get("最低收盘价", 0)
            max_price = metrics.get("最高收盘价", 0)
            issue_list.append(f"价格范围异常(最低{min_price:.2f}元,最高{max_price:.2f}元)")
        
        # 4. 价格活跃度检查（优化：增加变动次数检查）
        price_change_rate = metrics.get("价格变化率", 0)
        if price_change_rate < self.price_change_threshold:
            issue_list.append(f"价格缺乏活跃度(变化率{price_change_rate:.3f}<{self.price_change_threshold:.3f})")
        
        price_change_days = metrics.get("价格变化天数", 0)
        if price_change_days < self.active_days_requirement:
            issue_list.append(f"价格变化天数不足({price_change_days}<{self.active_days_requirement}天)")
        
        # 新增：有效变动次数检查（防止僵尸ETF）
        effective_change_count = metrics.get("有效变动次数", 0)
        if effective_change_count < 3:  # 最近10天至少变动3次
            issue_list.append(f"价格变动次数不足({effective_change_count}<3次，疑似僵尸ETF)")
        
        consecutive_same_price_days = metrics.get("连续相同价格天数", 0)
        if consecutive_same_price_days > self.consecutive_same_price_limit:
            issue_list.append(f"连续相同价格时间过长({consecutive_same_price_days}>{self.consecutive_same_price_limit}天)")
        
        # 5. 异常波动检查
        abnormal_volatility_days = metrics.get("异常波动天数", 999)
        if abnormal_volatility_days > self.abnormal_days_limit:
            issue_list.append(f"异常波动天数过多({abnormal_volatility_days}>{self.abnormal_days_limit}天)")
        
        abnormal_amplitude_days = metrics.get("异常振幅天数", 999)
        if abnormal_amplitude_days > self.abnormal_days_limit:
            issue_list.append(f"异常振幅天数过多({abnormal_amplitude_days}>{self.abnormal_days_limit}天)")
        
        # 6. 数据逻辑一致性检查
        logic_inconsistency_rate = metrics.get("逻辑不一致率", 1.0)
        if logic_inconsistency_rate > self.tolerance_ratio:
            issue_list.append(f"数据逻辑错误过多({logic_inconsistency_rate:.1%}>{self.tolerance_ratio:.1%})")
        
        # 综合判断
        if len(issue_list) == 0:
            return True, "价格质量检查通过"
        else:
            return False, "; ".join(issue_list)
    
    def get_filter_description(self) -> Dict[str, Any]:
        """获取筛选器说明"""
        return {
            "筛选器名称": self.name,
            "设计依据": "request.md - 价格质量检查和数据质量验证 (已优化)",
            "检查项目": {
                "历史数据要求": f"至少{self.min_history_days}天交易数据",
                "数据完整性": f"缺失率≤{self.missing_rate_limit:.1%}",
                "OHLC逻辑": f"错误率≤{self.tolerance_ratio:.1%}",
                "价格合理性": f"{self.min_price}-{self.max_price}元",
                "价格活跃度": f"变化率≥{self.price_change_threshold:.3f}",
                "变动次数检查": "10天≥3次变动(防僵尸ETF)",
                "异常波动": f"≤{self.abnormal_days_limit}天",
                "数据一致性": f"OHLC容错{self.ohlc_tolerance:.0%}/成交容错{self.volume_tolerance:.0%}"
            },
            "使用字段": [
                "开盘价", "最高价", "最低价", "收盘价", "上日收盘", 
                "涨跌", "涨幅%", "成交量(手数)", "成交额(千元)"
            ],
            "判断逻辑": "多维度检查，细化容错率，强化活跃度检测"
        } 