#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ATR配置管理模块
==============

负责ATR计算器的所有配置参数管理，包括:
- 计算参数配置
- 门槛设置
- 文件路径管理
- 中国市场优化参数
"""

import os
from pathlib import Path
from typing import Dict, List, Any


class ATRConfig:
    """ATR配置管理器"""
    
    def __init__(self):
        # 基础路径配置
        self.base_dir = Path(__file__).parent.parent.parent
        self.calculator_dir = self.base_dir / "atr_calculator"
        
        # 数据路径配置
        self.data_dir = self.base_dir / "data"
        self.cache_dir = self.base_dir / "cache"
        self.etf_data_path = self._get_etf_data_path()
        
        # ATR计算参数
        self.atr_params = {
            'period': 10,                    # ATR计算周期
            'stop_loss_multiplier': 2.2,     # 止损倍数(中国市场保守设定)
            'limit_adjustment': 1.2,          # 涨跌停修正系数
            'limit_threshold': 9.8,           # 涨跌停判定阈值(%)
        }
        
        # 波动性分级标准
        self.volatility_thresholds = {
            'high': 3.0,      # 高波动阈值 (ATR_Percent > 3.0%)
            'low': 1.5,       # 低波动阈值 (ATR_Percent < 1.5%)
        }
        
        # 门槛配置
        self.thresholds = ["3000万门槛", "5000万门槛"]
        
        # 输出字段配置 (统一小写字段名)
        self.output_fields = [
            'tr',                    # 真实波幅
            'atr_10',               # 10日ATR
            'atr_percent',          # ATR百分比
            'atr_change_rate',      # ATR变化率
            'atr_ratio_hl',         # ATR占区间比
            'stop_loss',            # 止损位
            'volatility_level'      # 波动水平
        ]
        
        # 数值精度配置 (统一8位精度与其他波动性指标一致)
        self.precision = {
            'tr': 8,                    # 8位小数统一精度
            'atr_10': 8,               # 8位小数统一精度
            'atr_percent': 8,          # 8位小数显示微小波动
            'atr_change_rate': 8,      # 8位小数统一精度
            'atr_ratio_hl': 8,         # 8位小数统一精度
            'stop_loss': 8,            # 8位小数统一精度
            'volatility_level': None   # 字符串类型
        }
        
        # 性能配置
        self.performance = {
            'enable_cache': True,
            'batch_size': 1000,
            'vector_operations': True,
            'parallel_processing': False  # 可根据需要开启
        }
    
    def _get_etf_data_path(self) -> str:
        """获取ETF数据路径"""
        # 向上查找ETF数据目录，使用日更数据
        current = self.base_dir
        for _ in range(5):  # 最多向上查找5层
            current = current.parent
            # 优先使用日更数据
            etf_daily_path = current / "ETF日更" / "0_ETF日K(前复权)"
            if etf_daily_path.exists():
                return str(etf_daily_path)
            # 备用周更数据
            etf_weekly_path = current / "ETF周更" / "0_ETF日K(前复权)"
            if etf_weekly_path.exists():
                return str(etf_weekly_path)
        
        # 备用路径（优先日更）
        daily_path = "/Users/wenbai/Desktop/金融/data_clear/ETF日更/0_ETF日K(前复权)"
        weekly_path = "/Users/wenbai/Desktop/金融/data_clear/ETF周更/0_ETF日K(前复权)"
        return daily_path if Path(daily_path).exists() else weekly_path
    
    def get_cache_path(self, threshold: str) -> Path:
        """获取缓存路径"""
        return self.cache_dir / threshold
    
    def get_data_path(self, threshold: str) -> Path:
        """获取数据输出路径"""
        return self.data_dir / threshold
    
    def ensure_directories_exist(self):
        """确保所有必要目录存在"""
        directories = [
            self.data_dir,
            self.cache_dir,
        ]
        
        # 为每个门槛创建目录
        for threshold in self.thresholds:
            directories.extend([
                self.get_cache_path(threshold),
                self.get_data_path(threshold)
            ])
        
        for directory in directories:
            directory.mkdir(parents=True, exist_ok=True)
    
    def get_atr_display_info(self) -> str:
        """获取ATR配置显示信息"""
        return f"ATR({self.atr_params['period']}) | 止损倍数: {self.atr_params['stop_loss_multiplier']}"
    
    def get_volatility_level(self, atr_percent: float) -> str:
        """根据ATR百分比确定波动水平"""
        if atr_percent > self.volatility_thresholds['high']:
            return '高'
        elif atr_percent < self.volatility_thresholds['low']:
            return '低'
        else:
            return '中'
    
    def should_apply_limit_adjustment(self, price_change_pct: float) -> bool:
        """判断是否需要涨跌停修正"""
        return abs(price_change_pct) >= self.volatility_thresholds['limit_threshold']
    
    def get_system_info(self) -> Dict[str, Any]:
        """获取系统信息"""
        return {
            'version': '1.0.0',
            'calculator_name': 'ATR真实波幅计算器',
            'base_dir': str(self.base_dir),
            'etf_data_path': self.etf_data_path,
            'supported_thresholds': self.thresholds,
            'output_fields': self.output_fields,
            'atr_period': self.atr_params['period'],
            'stop_loss_multiplier': self.atr_params['stop_loss_multiplier']
        }