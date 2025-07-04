#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
EMA配置管理模块 - 中短期专版 + 系统差异化
==========================

专门管理指数移动平均线的配置参数
🎯 系统特性: EMA快速响应，平衡敏感性和稳定性，参数介于WMA和SMA之间
专注于中短期交易指标：EMA12, EMA26
"""

import os
from typing import List, Optional, Dict


class EMAConfig:
    """EMA配置管理类 - 中短期专版 + 系统差异化"""
    
    # 🎯 EMA系统专属参数设置（快速响应系统）
    EMA_SYSTEM_PARAMS = {
        'name': 'EMA',
        'sensitivity_level': 'FAST_RESPONSE',  # 快速响应敏感度
        'base_threshold': 0.22,               # 介于WMA(0.20)和SMA(0.25)之间
        'tolerance_ratio': 0.30,              # 适中容错：允许30%次要均线反向
        'volume_factor': 1.18,                # 适中量能确认：5日均量/20日均量 > 1.18
        'signal_decay': 0.20,                 # 适中信号衰减：介于WMA(0.15)和SMA(0.25)之间
        'quality_bonus_threshold': 2.2,       # 适中质量信号奖励阈值：差距>2.2%
        'noise_filter': 0.18,                 # 适中噪音过滤：<0.18%视为噪音
        'description': 'EMA快速响应价格变化，平衡敏感性和稳定性，适合捕捉趋势转折'
    }
    
    def __init__(self, adj_type: str = "前复权", ema_periods: Optional[List[int]] = None):
        """
        初始化EMA配置 - 系统差异化版
        
        Args:
            adj_type: 复权类型 ("前复权", "后复权", "除权")
            ema_periods: EMA周期列表，None时使用默认中短期配置
        """
        print("⚙️  EMA配置初始化 (系统差异化版)...")
        
        # 复权类型配置
        self.adj_type = adj_type
        self.adj_type_mapping = {
            "前复权": "0_ETF日K(前复权)",
            "后复权": "0_ETF日K(后复权)", 
            "除权": "0_ETF日K(除权)"
        }
        
        # 🎯 中短期EMA周期配置（专注MACD基础）
        if ema_periods is None:
            self.ema_periods = [12, 26]  # 专注中短期，MACD标准周期
        else:
            self.ema_periods = ema_periods
        
        # 🎯 EMA系统专属参数
        self.system_params = self.EMA_SYSTEM_PARAMS.copy()
        
        # 数据要求 - EMA需要更多数据来稳定
        self.required_rows = None  # 使用所有可用历史数据，不限制行数
        
        # 路径配置
        self._setup_paths()
        
        print(f"   ✅ 复权类型: {self.adj_type}")
        print(f"   📊 EMA周期: {self.ema_periods} (中短期专版)")
        print(f"   ⚙️ 系统特性: {self.system_params['description']}")
        print(f"   📊 系统参数: 基准阈值={self.system_params['base_threshold']}%, 容错率={self.system_params['tolerance_ratio']}")
        print(f"   📁 数据目录: {self.data_dir}")
        if self.required_rows is not None:
            print(f"   📄 数据要求: {self.required_rows}行")
        else:
            print(f"   📄 数据策略: 使用所有可用历史数据")
        
    def _setup_paths(self):
        """智能路径配置"""
        # 获取当前脚本的基础目录
        current_dir = os.getcwd()
        
        # 智能检测项目根目录
        if "ETF_计算额外数据" in current_dir:
            # 当前在项目内部
            project_root = current_dir.split("ETF_计算额外数据")[0]
        else:
            # 假设当前目录就是项目根目录
            project_root = current_dir
            
        # 构建数据目录路径
        self.data_dir = os.path.join(project_root, "ETF日更", self.adj_type_mapping[self.adj_type])
        
        # 🔬 智能输出目录配置 - 模仿SMA结构
        # 基础输出目录，具体门槛目录在运行时确定
        self.default_output_dir = os.path.join(
            os.path.dirname(__file__), 
            "..", 
            "data"
        )
        
        print(f"   🔍 项目根目录: {project_root}")
        print(f"   📂 数据目录: {self.data_dir}")
        
    def get_system_thresholds(self) -> Dict[str, float]:
        """
        获取EMA系统专属的阈值参数（快速响应）
        
        Returns:
            Dict: 系统阈值配置
        """
        return {
            'minimal': self.system_params['base_threshold'],     # 0.22% - 介于WMA和SMA之间
            'moderate': self.system_params['base_threshold'] * 3.6, # 0.79% - 适中比例
            'strong': self.system_params['base_threshold'] * 6.8,   # 1.50% - 适中比例  
            'noise_filter': self.system_params['noise_filter']   # 0.18% - 适中噪音过滤
        }
    
    def get_system_score_weights(self) -> Dict[str, float]:
        """
        获取EMA系统专属的评分权重（快速响应）
        
        Returns:
            Dict: 系统评分权重
        """
        # EMA快速响应，权重介于WMA和SMA之间
        return {
            '强势': 1.1,    # 介于WMA(1.0)和SMA(1.2)之间
            '中等': 0.7,    # 介于WMA(0.6)和SMA(0.8)之间
            '温和': 0.35,   # 介于WMA(0.3)和SMA(0.4)之间
            '微弱': 0.075   # 介于WMA(0.05)和SMA(0.1)之间
        }
    
    def get_volume_threshold(self) -> float:
        """获取EMA系统的量能确认阈值"""
        return self.system_params['volume_factor']
    
    def get_tolerance_ratio(self) -> float:
        """获取EMA系统的容错比例"""
        return self.system_params['tolerance_ratio']
        
    def validate_data_path(self) -> bool:
        """
        验证数据路径是否存在
        
        Returns:
            bool: 路径是否有效
        """
        if os.path.exists(self.data_dir):
            file_count = len([f for f in os.listdir(self.data_dir) if f.endswith('.csv')])
            print(f"   ✅ 数据路径验证成功，找到 {file_count} 个CSV文件")
            return True
        else:
            print(f"   ❌ 数据路径不存在: {self.data_dir}")
            return False
    
    def get_etf_file_path(self, etf_code: str) -> str:
        """
        获取ETF数据文件路径
        
        Args:
            etf_code: ETF代码
            
        Returns:
            str: 文件路径
        """
        # 标准化ETF代码格式
        if not etf_code.endswith(('.SH', '.SZ')):
            # 如果没有后缀，需要智能判断
            if etf_code.startswith('5'):
                etf_code += '.SH'
            elif etf_code.startswith('1'):
                etf_code += '.SZ'
        
        filename = f"{etf_code}.csv"
        return os.path.join(self.data_dir, filename)
    
    def get_ema_display_info(self) -> str:
        """
        获取EMA配置的显示信息
        
        Returns:
            str: 配置描述
        """
        period_desc = ", ".join([f"EMA{p}" for p in self.ema_periods])
        return f"EMA配置 ({self.adj_type}): {period_desc}"
        
    def get_smoothing_factor(self, period: int) -> float:
        """
        获取EMA平滑因子
        
        Args:
            period: EMA周期
            
        Returns:
            float: 平滑因子 α = 2/(period+1)
        """
        return 2.0 / (period + 1)
        
    @property
    def max_period(self) -> int:
        """获取最大周期"""
        return max(self.ema_periods) if self.ema_periods else 26
        
    def to_dict(self) -> Dict:
        """将配置转换为字典格式"""
        return {
            'adj_type': self.adj_type,
            'ema_periods': self.ema_periods,
            'max_period': self.max_period,
            'required_rows': self.required_rows,
            'data_dir': self.data_dir,
            'system_params': self.system_params,
            'system_thresholds': self.get_system_thresholds(),
            'system_score_weights': self.get_system_score_weights(),
            'optimization': f'EMA系统专属参数：快速响应控制 (基准{self.system_params["base_threshold"]}%)'
        } 