#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MACD配置管理模块 - 客观数据专版
============================

🚫 已简化：只保留客观数据计算，移除主观判断
管理MACD指标计算的基础配置参数
🎯 核心特性: DIF+DEA+MACD三线组合计算
📊 经典参数: EMA(12,26,9) - 最广泛使用的标准配置
🚫 已移除: 信号权重、阈值评估等主观判断配置

"""

import os
from typing import Dict, List, Optional, Tuple

class MACDConfig:
    """MACD配置管理器 - 客观数据专版"""
    
    # 🎯 MACD系统基础参数设置
    MACD_SYSTEM_PARAMS = {
        'name': 'MACD',
        'description': 'Moving Average Convergence Divergence',
        'category': '趋势类指标',
        'type': 'technical_indicator',  # 技术指标
        'components': ['DIF', 'DEA', 'MACD'],  # 🚫 已移除Signal组件
        'primary_use': '技术指标计算'  # 🚫 已移除趋势确认和动量分析
    }
    
    # 📊 MACD参数组合设置（基础计算参数）
    PARAMETER_SETS = {
        'standard': {
            'fast_period': 12,      # 快线EMA周期
            'slow_period': 26,      # 慢线EMA周期  
            'signal_period': 9,     # 信号线EMA周期
            'description': '标准参数，EMA(12,26,9)',
            # 🚫 已移除敏感性和稳定性评估
            # 'sensitivity': 'medium',
            # 'stability': 'high'
        },
        'sensitive': {
            'fast_period': 8,       # 更敏感的参数
            'slow_period': 17,
            'signal_period': 9,
            'description': '敏感参数，EMA(8,17,9)',
            # 🚫 已移除敏感性和稳定性评估
            # 'sensitivity': 'high',
            # 'stability': 'medium'
        },
        'smooth': {
            'fast_period': 19,      # 更平滑的参数
            'slow_period': 39,
            'signal_period': 9,
            'description': '平滑参数，EMA(19,39,9)',
            # 🚫 已移除敏感性和稳定性评估
            # 'sensitivity': 'low',
            # 'stability': 'very_high'
        }
    }
    
    # 🚫 已移除主观判断配置
    # # 🔄 信号强度评估阈值
    # SIGNAL_THRESHOLDS = {
    #     'zero_line': 0.0,           # 零轴位置
    #     'weak_signal': 0.001,       # 微弱信号阈值
    #     'moderate_signal': 0.005,   # 中等信号阈值  
    #     'strong_signal': 0.010,     # 强势信号阈值
    #     'extreme_signal': 0.020,    # 极端信号阈值
    #     'dif_dea_min_gap': 0.0001   # DIF/DEA最小有效差距
    # }
    # 
    # # 📈 信号评分权重系统
    # SIGNAL_WEIGHTS = {
    #     'golden_cross_above_zero': 1.0,    # 零轴上方金叉 - 最强买入
    #     'golden_cross_below_zero': 0.6,    # 零轴下方金叉 - 较弱买入
    #     'death_cross_above_zero': -0.6,    # 零轴上方死叉 - 较弱卖出
    #     'death_cross_below_zero': -1.0,    # 零轴下方死叉 - 最强卖出
    #     'dif_cross_zero_up': 0.8,          # DIF上穿零轴
    #     'dif_cross_zero_down': -0.8,       # DIF下穿零轴
    #     'histogram_expanding': 0.3,        # 柱状图扩大
    #     'histogram_contracting': -0.3,     # 柱状图收缩
    #     'divergence_bullish': 0.9,         # 牛市背离
    #     'divergence_bearish': -0.9         # 熊市背离
    # }
    
    # 复权类型映射
    ADJ_TYPES = {
        "前复权": "0_ETF日K(前复权)",    # 🔬 推荐: 技术分析最优选择
        "后复权": "0_ETF日K(后复权)",    # ⚠️ 谨慎: 价格会变动
        "除权": "0_ETF日K(除权)"        # ❌ 不推荐: 价格跳跃
    }
    
    def __init__(self, parameter_set: str = 'standard'):
        """
        初始化MACD配置
        
        Args:
            parameter_set: 参数组合类型 ('standard', 'sensitive', 'smooth')
        """
        self.parameter_set = parameter_set
        self.current_params = self.PARAMETER_SETS[parameter_set]
        self.adj_type = "前复权"  # 默认使用前复权
        
        # 设置基础路径 - 修复路径计算
        self.base_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        
        # 找到项目根目录 (data_clear)
        project_root = self.base_path
        while not os.path.basename(project_root) == 'data_clear':
            parent = os.path.dirname(project_root)
            if parent == project_root:  # 已经到达根目录
                break
            project_root = parent
        
        # 设置数据源路径指向项目根目录下的ETF日更
        self.data_source_path = os.path.join(
            project_root, 
            "ETF日更", 
            self.ADJ_TYPES[self.adj_type]
        )
        self.default_output_dir = os.path.join(self.base_path, "data")
        
        print(f"📊 MACD配置初始化完成 (客观数据专版)")
        print(f"🎯 参数组合: {parameter_set} {self.current_params['description']}")
        print(f"⚙️ 核心参数: EMA({self.current_params['fast_period']}, "
              f"{self.current_params['slow_period']}, {self.current_params['signal_period']})")
        print("🚫 已移除: 信号权重、阈值评估等主观判断配置")
    
    def get_macd_periods(self) -> Tuple[int, int, int]:
        """获取MACD三个核心周期参数"""
        return (
            self.current_params['fast_period'],
            self.current_params['slow_period'], 
            self.current_params['signal_period']
        )
    
    # 🚫 已移除主观判断方法
    # def get_signal_thresholds(self) -> Dict[str, float]:
    #     """获取信号判断阈值"""
    #     return self.SIGNAL_THRESHOLDS.copy()
    # 
    # def get_signal_weights(self) -> Dict[str, float]:
    #     """获取信号评分权重"""
    #     return self.SIGNAL_WEIGHTS.copy()
    
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
        """获取系统信息 - 客观数据专版"""
        return {
            'system_name': self.MACD_SYSTEM_PARAMS['name'],
            'version': '1.0.0 - 客观数据专版',
            'parameter_set': self.parameter_set,
            'current_params': self.current_params,
            'adj_type': self.adj_type,
            'components': self.MACD_SYSTEM_PARAMS['components'],
            'note': '已移除信号分析、交易建议等主观判断功能'
        } 