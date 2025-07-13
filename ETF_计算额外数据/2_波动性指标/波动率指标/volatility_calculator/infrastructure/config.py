#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
波动率指标配置管理模块
==================

基于第一大类标准的波动率指标配置系统
"""

import os
from typing import List, Dict, Optional
from dataclasses import dataclass, field


@dataclass
class VolatilitySystemParams:
    """波动率系统参数配置"""
    name: str = 'Volatility'
    sensitivity_level: str = 'HIGH'
    base_threshold: float = 0.15
    tolerance_ratio: float = 0.20
    volume_factor: float = 1.15
    signal_decay: float = 0.10
    quality_bonus_threshold: float = 2.0
    noise_filter: float = 0.12
    description: str = '波动率指标对市场风险敏感，用于评估价格变动的不确定性'


class VolatilityConfig:
    """波动率配置管理器"""
    
    # 复权类型映射 - 与第一大类保持一致
    ADJ_TYPES = {
        "前复权": "0_ETF日K(前复权)",
        "后复权": "0_ETF日K(后复权)",
        "除权": "0_ETF日K(除权)"
    }
    
    # 科学复权评估
    ADJ_TYPE_SCIENTIFIC_EVALUATION = {
        "前复权": {
            "scientific_score": 95,
            "recommendation": "强烈推荐",
            "pros": ["价格连续性好", "适合波动率计算", "历史数据稳定"],
            "cons": ["历史价格非实际价格"],
            "use_cases": ["波动率分析", "风险评估", "历史波动率计算"]
        },
        "后复权": {
            "scientific_score": 60,
            "recommendation": "谨慎使用",
            "pros": ["基于当前价格", "便于理解收益"],
            "cons": ["历史价格会变化", "影响波动率指标", "不利于波动率分析"],
            "use_cases": ["收益计算", "资产配置"]
        },
        "除权": {
            "scientific_score": 30,
            "recommendation": "不推荐",
            "pros": ["实际交易价格"],
            "cons": ["价格跳跃严重", "破坏波动率连续性", "影响波动率准确性"],
            "use_cases": ["查看实际价格"]
        }
    }
    
    # 默认波动率计算周期
    DEFAULT_VOLATILITY_PERIODS = [10, 20, 30, 60]
    
    def __init__(self, adj_type: str = "前复权", volatility_periods: Optional[List[int]] = None, 
                 enable_cache: bool = True, performance_mode: bool = True, 
                 annualized: bool = True):
        """
        初始化波动率配置
        
        Args:
            adj_type: 复权类型
            volatility_periods: 波动率周期列表
            enable_cache: 是否启用缓存
            performance_mode: 是否启用性能模式
            annualized: 是否计算年化波动率
        """
        self.adj_type = self._validate_and_recommend_adj_type(adj_type)
        self.volatility_periods = self._validate_volatility_periods(
            volatility_periods or self.DEFAULT_VOLATILITY_PERIODS.copy()
        )
        self.max_period = max(self.volatility_periods)
        self.enable_cache = enable_cache
        self.performance_mode = performance_mode
        self.annualized = annualized
        
        # 波动率系统专属参数
        self.system_params = VolatilitySystemParams()
        
        # 数据策略：使用所有可用数据
        self.required_rows = None
        
        # 波动率计算参数
        self.trading_days_per_year = 252  # 年化波动率计算基础
        self.min_data_points = 10  # 最少数据点要求
        
        # 数据路径配置 - 按第一大类标准
        self._setup_paths()
        
        # 输出初始化信息
        self._print_init_info()
    
    def _setup_paths(self) -> None:
        """配置系统路径 - 按第一大类标准"""
        current_dir = os.getcwd()
        
        if "波动率指标" in current_dir:
            self.base_data_path = "../../../ETF日更"
            self.volatility_script_dir = "."
        elif "data_clear" in current_dir and current_dir.endswith("data_clear"):
            self.base_data_path = "./ETF日更"
            self.volatility_script_dir = "./ETF_计算额外数据/2_波动性指标/波动率指标"
        else:
            self.base_data_path = "./ETF日更"
            self.volatility_script_dir = "."
        
        self.data_path = os.path.join(self.base_data_path, self.ADJ_TYPES[self.adj_type])
        self.default_output_dir = os.path.join(self.volatility_script_dir, "data")
    
    def _print_init_info(self) -> None:
        """打印初始化信息"""
        if not self.performance_mode:
            print(f"📊 波动率配置初始化完成:")
            print(f"   📈 复权类型: {self.adj_type} (科学评分: {self.get_scientific_score()}/100)")
            print(f"   🎯 波动率周期: {self.volatility_periods}")
            print(f"   📊 年化计算: {'启用' if self.annualized else '禁用'}")
            print(f"   ⚙️ 系统特性: {self.system_params.description}")
            print(f"   📁 数据路径: {self.data_path}")
            print(f"   🗂️ 缓存启用: {self.enable_cache}")
    
    def get_scientific_score(self) -> int:
        """获取当前复权类型的科学评分"""
        return self.ADJ_TYPE_SCIENTIFIC_EVALUATION[self.adj_type]["scientific_score"]
    
    def _validate_volatility_periods(self, periods: List[int]) -> List[int]:
        """验证波动率周期参数"""
        if not periods:
            return self.DEFAULT_VOLATILITY_PERIODS.copy()
        
        valid_periods = []
        for period in periods:
            if isinstance(period, int) and period >= 3 and period <= 500:
                valid_periods.append(period)
        
        if not valid_periods:
            return self.DEFAULT_VOLATILITY_PERIODS.copy()
        
        return sorted(set(valid_periods))
    
    def _validate_and_recommend_adj_type(self, adj_type: str) -> str:
        """验证并推荐复权类型"""
        if adj_type not in self.ADJ_TYPES:
            print(f"❌ 不支持的复权类型: {adj_type}")
            adj_type = "前复权"
        return adj_type
    
    def get_file_path(self, etf_code: str) -> str:
        """获取ETF数据文件的完整路径"""
        clean_etf_code = etf_code.replace('.SH', '').replace('.SZ', '')
        return os.path.join(self.data_path, f"{clean_etf_code}.csv")
    
    def validate_threshold(self, threshold: str) -> bool:
        """验证门槛参数的有效性"""
        valid_thresholds = ["3000万门槛", "5000万门槛"]
        return threshold in valid_thresholds