#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
布林带配置管理模块
================

布林带系统的配置管理，统一格式规范
参照趋势类指标的配置模式
"""

import os
from typing import List, Optional, Dict
from dataclasses import dataclass


@dataclass
class BBSystemParams:
    """布林带系统参数配置"""
    name: str = 'BollingerBands'
    sensitivity_level: str = 'STANDARD'
    period: int = 20                    # 标准周期
    std_dev_multiplier: float = 2.0     # 标准差倍数
    min_periods: int = 20               # 最小计算周期
    precision: int = 8                  # 计算精度
    description: str = '布林带指标，通过动态轨道识别超买超卖和波动性变化'


class BBConfig:
    """布林带配置管理类"""
    
    # 复权类型映射
    ADJ_TYPE_MAPPING = {
        "前复权": "0_ETF日K(前复权)",
        "后复权": "0_ETF日K(后复权)", 
        "除权": "0_ETF日K(除权)"
    }
    
    # 布林带参数配置 - 基于中国ETF市场优化的科学参数
    BB_PARAMS_SETS = {
        '短周期': {
            'period': 10,                   # 短周期10日，适合快速响应
            'std_multiplier': 2.0,          # 标准2倍标准差
            'std_multiplier_upper': 2.0,    # 上轨标准差倍数
            'std_multiplier_lower': 2.0,    # 下轨标准差倍数
            'min_periods_required': 10      # 最少数据要求
        },
        '标准': {
            'period': 20,                   # 标准20日周期(John Bollinger经典设置)
            'std_multiplier': 2.0,          # 标准2倍标准差
            'std_multiplier_upper': 2.0,    # 上轨标准差倍数
            'std_multiplier_lower': 2.0,    # 下轨标准差倍数
            'min_periods_required': 20      # 最少数据要求
        }
    }
    
    # 默认使用标准参数(20,2) - 经学术验证最适合中国ETF市场
    DEFAULT_BB_PARAMS = BB_PARAMS_SETS['标准']
    
    def __init__(self, adj_type: str = "前复权", bb_params: Optional[Dict] = None):
        """
        初始化布林带配置
        
        Args:
            adj_type: 复权类型
            bb_params: 布林带参数配置
        """
        self.adj_type = adj_type
        self.bb_params = bb_params or self.DEFAULT_BB_PARAMS.copy()
        self.system_params = BBSystemParams()
        
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
        
        # 缓存目录  
        self.cache_dir = os.path.join(
            os.path.dirname(__file__), "..", "..", "cache"
        )
        
    def get_bb_period(self) -> int:
        """获取布林带计算周期"""
        return self.bb_params.get('period', 20)
        
    def get_bb_std_multiplier(self) -> float:
        """获取标准差倍数"""
        return self.bb_params.get('std_multiplier', 2.0)
        
    def get_upper_std_multiplier(self) -> float:
        """获取上轨标准差倍数"""
        return self.bb_params.get('std_multiplier_upper', 2.0)
        
    def get_lower_std_multiplier(self) -> float:
        """获取下轨标准差倍数"""
        return self.bb_params.get('std_multiplier_lower', 2.0)
        
    def get_min_periods(self) -> int:
        """获取最小计算周期"""
        return max(self.get_bb_period(), self.system_params.min_periods)
        
    def get_precision(self) -> int:
        """获取计算精度"""
        return self.system_params.precision
        
    def get_bb_output_fields(self) -> List[str]:
        """获取布林带输出字段列表（统一英文格式）"""
        return [
            'code',           # ETF代码
            'date',           # 日期
            'bb_middle',      # 中轨(20日SMA)
            'bb_upper',       # 上轨
            'bb_lower',       # 下轨
            'bb_width',       # 带宽百分比
            'bb_position',    # 价格相对位置
            'bb_percent_b'    # %B指标
        ]
        
    def validate_data_path(self) -> bool:
        """验证数据路径"""
        if not os.path.exists(self.data_dir):
            return False
            
        csv_files = [f for f in os.listdir(self.data_dir) if f.endswith('.csv')]
        return len(csv_files) > 0
    
    def get_etf_file_path(self, etf_code: str) -> str:
        """获取ETF数据文件路径"""
        # 清理ETF代码，去除交易所后缀
        clean_etf_code = etf_code.replace('.SH', '').replace('.SZ', '')
        return os.path.join(self.data_dir, f"{clean_etf_code}.csv")
    
    def get_bb_display_info(self) -> str:
        """获取配置显示信息"""
        period = self.get_bb_period()
        std_mult = self.get_bb_std_multiplier()
        param_set_name = self.get_current_param_set_name()
        return f"布林带配置 ({self.adj_type}): BB({period},{std_mult}) [{param_set_name}]"
    
    def get_available_param_sets(self) -> List[str]:
        """获取可用的参数集列表"""
        return list(self.BB_PARAMS_SETS.keys())
    
    def set_param_set(self, param_set_name: str) -> bool:
        """设置参数集
        
        Args:
            param_set_name: 参数集名称 ('短周期', '标准')
            
        Returns:
            bool: 设置是否成功
        """
        if param_set_name in self.BB_PARAMS_SETS:
            self.bb_params = self.BB_PARAMS_SETS[param_set_name].copy()
            return True
        return False
    
    def get_current_param_set_name(self) -> str:
        """获取当前参数集名称"""
        current_period = self.get_bb_period()
        for name, params in self.BB_PARAMS_SETS.items():
            if params['period'] == current_period:
                return name
        return '自定义'
        
    def get_cache_file_path(self, threshold: str, etf_code: str) -> str:
        """获取缓存文件路径"""
        clean_etf_code = etf_code.replace('.SH', '').replace('.SZ', '')
        return os.path.join(self.cache_dir, threshold, f"{clean_etf_code}.csv")
        
    def get_output_file_path(self, threshold: str, etf_code: str) -> str:
        """获取输出文件路径"""
        clean_etf_code = etf_code.replace('.SH', '').replace('.SZ', '')
        return os.path.join(self.default_output_dir, threshold, f"{clean_etf_code}.csv")
        
    def ensure_directories_exist(self) -> None:
        """确保必要目录存在"""
        directories = [
            self.default_output_dir,
            self.cache_dir,
            os.path.join(self.cache_dir, "meta"),
            os.path.join(self.cache_dir, "3000万门槛"),
            os.path.join(self.cache_dir, "5000万门槛"),
            os.path.join(self.default_output_dir, "3000万门槛"),
            os.path.join(self.default_output_dir, "5000万门槛")
        ]
        
        for directory in directories:
            os.makedirs(directory, exist_ok=True)
            
    def to_dict(self) -> Dict:
        """转换为字典格式"""
        return {
            'adj_type': self.adj_type,
            'bb_params': self.bb_params,
            'bb_period': self.get_bb_period(),
            'bb_std_multiplier': self.get_bb_std_multiplier(),
            'min_periods': self.get_min_periods(),
            'precision': self.get_precision(),
            'data_dir': self.data_dir,
            'output_dir': self.default_output_dir,
            'cache_dir': self.cache_dir,
            'output_fields': self.get_bb_output_fields()
        }