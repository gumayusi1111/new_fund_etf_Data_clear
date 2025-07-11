#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MACD配置管理模块 - 重构版
=========================

模块化架构的MACD配置管理器
与其他趋势类指标系统保持一致的架构风格
"""

import os
from typing import Dict, List, Optional, Tuple


class MACDConfig:
    """MACD配置管理器 - 重构版"""
    
    # MACD系统基础参数设置
    MACD_SYSTEM_PARAMS = {
        'name': 'MACD',
        'description': 'Moving Average Convergence Divergence',
        'category': '趋势类指标',
        'type': 'technical_indicator',
        'components': ['DIF', 'DEA', 'MACD'],
        'primary_use': '技术指标计算'
    }
    
    # MACD参数组合设置
    PARAMETER_SETS = {
        'standard': {
            'fast_period': 12,
            'slow_period': 26,
            'signal_period': 9,
            'description': '标准参数，EMA(12,26,9)'
        },
        'sensitive': {
            'fast_period': 8,
            'slow_period': 17,
            'signal_period': 9,
            'description': '敏感参数，EMA(8,17,9)'
        },
        'smooth': {
            'fast_period': 19,
            'slow_period': 39,
            'signal_period': 9,
            'description': '平滑参数，EMA(19,39,9)'
        }
    }
    
    # 复权类型映射
    ADJ_TYPES = {
        "前复权": "0_ETF日K(前复权)",
        "后复权": "0_ETF日K(后复权)",
        "除权": "0_ETF日K(除权)"
    }
    
    def __init__(self, parameter_set: str = 'standard', adj_type: str = "前复权", 
                 enable_cache: bool = True, performance_mode: bool = False):
        """
        初始化MACD配置 - 重构版
        
        Args:
            parameter_set: 参数组合类型 ('standard', 'sensitive', 'smooth')
            adj_type: 复权类型，默认"前复权"
            enable_cache: 是否启用缓存，默认True
            performance_mode: 是否启用性能模式
        """
        print("⚙️ MACD配置初始化 (重构版)...")
        
        # 基础参数
        self.parameter_set = parameter_set
        self.current_params = self.PARAMETER_SETS[parameter_set]
        self.adj_type = adj_type
        self.enable_cache = enable_cache
        self.performance_mode = performance_mode
        
        # 路径设置
        self.base_path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        
        # 找到项目根目录 (data_clear)
        project_root = self.base_path
        while not os.path.basename(project_root) == 'data_clear':
            parent = os.path.dirname(project_root)
            if parent == project_root:
                break
            project_root = parent
        
        # 设置数据源路径
        self.data_source_path = os.path.join(
            project_root, 
            "ETF日更", 
            self.ADJ_TYPES[self.adj_type]
        )
        self.default_output_dir = os.path.join(self.base_path, "data")
        
        # 打印配置信息
        print(f"   🔍 项目根目录: {project_root}")
        print(f"   📂 数据目录: {self.data_source_path}")
        print(f"   ✅ 复权类型: {self.adj_type}")
        print(f"   📊 MACD参数: {parameter_set} - {self.current_params['description']}")
        print(f"   🗂️ 缓存: {'启用' if self.enable_cache else '禁用'}")
        print(f"   🚀 性能模式: {'启用' if self.performance_mode else '禁用'}")
        print(f"   📁 数据目录: {self.data_source_path}")
        print(f"   📄 数据策略: 使用所有可用历史数据")
    
    def get_macd_periods(self) -> Tuple[int, int, int]:
        """获取MACD三个核心周期参数"""
        return (
            self.current_params['fast_period'],
            self.current_params['slow_period'], 
            self.current_params['signal_period']
        )
    
    def get_data_source_path(self) -> str:
        """获取数据源路径"""
        return self.data_source_path
    
    def get_output_base_dir(self) -> str:
        """获取输出基础目录"""
        return self.default_output_dir
    
    def switch_parameter_set(self, parameter_set: str):
        """切换参数组合"""
        if parameter_set in self.PARAMETER_SETS:
            self.parameter_set = parameter_set
            self.current_params = self.PARAMETER_SETS[parameter_set]
            print(f"🔄 已切换到参数组合: {parameter_set}")
        else:
            raise ValueError(f"不支持的参数组合: {parameter_set}")
    
    def get_system_info(self) -> Dict:
        """获取系统信息 - 重构版"""
        return {
            'system_name': self.MACD_SYSTEM_PARAMS['name'],
            'version': '2.0.0 - 重构版',
            'parameter_set': self.parameter_set,
            'current_params': self.current_params,
            'adj_type': self.adj_type,
            'components': self.MACD_SYSTEM_PARAMS['components'],
            'architecture': 'modular',
            'cache_enabled': self.enable_cache,
            'performance_mode': self.performance_mode
        }