#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SMA配置管理模块 (重构版)
=====================

精简而强大的SMA配置管理，专注于核心功能
"""

import os
from typing import List, Optional, Dict
from dataclasses import dataclass, field


@dataclass
class SMASystemParams:
    """SMA系统参数配置"""
    name: str = 'SMA'
    sensitivity_level: str = 'STANDARD'
    base_threshold: float = 0.25
    tolerance_ratio: float = 0.33
    volume_factor: float = 1.15
    signal_decay: float = 0.25
    quality_bonus_threshold: float = 2.0
    noise_filter: float = 0.20
    description: str = 'SMA最平滑稳定，具有很好的噪音过滤，适合作为标准参数基准'


class SMAConfig:
    """SMA配置管理类 - 重构版"""
    
    # 复权类型映射
    ADJ_TYPE_MAPPING = {
        "前复权": "0_ETF日K(前复权)",
        "后复权": "0_ETF日K(后复权)", 
        "除权": "0_ETF日K(除权)"
    }
    
    # 默认SMA周期（中短线专版）
    DEFAULT_SMA_PERIODS = [5, 10, 20, 60]
    
    def __init__(self, adj_type: str = "前复权", sma_periods: Optional[List[int]] = None):
        """
        初始化SMA配置
        
        Args:
            adj_type: 复权类型
            sma_periods: SMA周期列表
        """
        self.adj_type = adj_type
        self.sma_periods = sma_periods or self.DEFAULT_SMA_PERIODS.copy()
        self.system_params = SMASystemParams()
        self.required_rows = None  # 使用所有可用数据
        
        # 初始化路径
        self._setup_paths()
        
    def _setup_paths(self) -> None:
        """配置系统路径"""
        current_dir = os.getcwd()
        
        # 智能检测项目根目录
        if "ETF_计算额外数据" in current_dir:
            project_root = current_dir.split("ETF_计算额外数据")[0]
        else:
            project_root = current_dir
            
        # 数据目录
        adj_dir = self.ADJ_TYPE_MAPPING[self.adj_type]
        self.data_dir = os.path.join(project_root, "ETF日更", adj_dir)
        
        # 输出目录
        self.default_output_dir = os.path.join(
            os.path.dirname(__file__), "..", "..", "data"
        )
        
    def get_system_thresholds(self) -> Dict[str, float]:
        """获取系统阈值参数"""
        base = self.system_params.base_threshold
        return {
            'minimal': base,
            'moderate': base * 3.2,
            'strong': base * 6,
            'noise_filter': self.system_params.noise_filter
        }
    
    def get_system_score_weights(self) -> Dict[str, float]:
        """获取系统评分权重"""
        return {
            '强势': 1.2,
            '中等': 0.8,
            '温和': 0.4,
            '微弱': 0.1
        }
    
    def get_volume_threshold(self) -> float:
        """获取量能确认阈值"""
        return self.system_params.volume_factor
    
    def get_tolerance_ratio(self) -> float:
        """获取容错比例"""
        return self.system_params.tolerance_ratio
        
    def validate_data_path(self) -> bool:
        """验证数据路径"""
        if not os.path.exists(self.data_dir):
            return False
            
        csv_files = [f for f in os.listdir(self.data_dir) if f.endswith('.csv')]
        return len(csv_files) > 0
    
    def get_etf_file_path(self, etf_code: str) -> str:
        """获取ETF数据文件路径"""
        # 智能添加交易所后缀
        if not etf_code.endswith(('.SH', '.SZ')):
            if etf_code.startswith('5'):
                etf_code += '.SH'
            elif etf_code.startswith('1'):
                etf_code += '.SZ'
        
        return os.path.join(self.data_dir, f"{etf_code}.csv")
    
    def get_sma_display_info(self) -> str:
        """获取配置显示信息"""
        periods = ", ".join([f"MA{p}" for p in self.sma_periods])
        return f"SMA配置 ({self.adj_type}): {periods}"
        
    @property
    def max_period(self) -> int:
        """获取最大周期"""
        return max(self.sma_periods) if self.sma_periods else 60
        
    def to_dict(self) -> Dict:
        """转换为字典格式"""
        return {
            'adj_type': self.adj_type,
            'sma_periods': self.sma_periods,
            'max_period': self.max_period,
            'required_rows': self.required_rows,
            'data_dir': self.data_dir,
            'system_thresholds': self.get_system_thresholds(),
            'system_score_weights': self.get_system_score_weights(),
            'volume_threshold': self.get_volume_threshold(),
            'tolerance_ratio': self.get_tolerance_ratio()
        } 