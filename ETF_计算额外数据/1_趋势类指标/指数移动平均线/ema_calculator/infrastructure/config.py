#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
EMA配置管理模块 - 重构版
=====================

参照WMA/SMA系统的配置管理架构
统一配置管理，支持缓存和性能优化
"""

import os
from typing import List, Optional, Dict


class EMAConfig:
    
    @staticmethod
    def normalize_etf_code(etf_code: str) -> str:
        """
        统一的ETF代码标准化处理
        
        Args:
            etf_code: 原始ETF代码
            
        Returns:
            str: 标准化后的ETF代码（包含交易所后缀）
        """
        if not etf_code:
            return etf_code
            
        etf_code = str(etf_code).strip().upper()
        
        # 如果已经有后缀，直接返回
        if etf_code.endswith(('.SH', '.SZ')):
            return etf_code
            
        # 根据代码开头添加后缀
        if etf_code.startswith('5'):
            return f"{etf_code}.SH"
        elif etf_code.startswith('1'):
            return f"{etf_code}.SZ"
        else:
            return etf_code
    
    @staticmethod 
    def clean_etf_code(etf_code: str) -> str:
        """
        移除ETF代码的交易所后缀
        
        Args:
            etf_code: 包含后缀的ETF代码
            
        Returns:
            str: 清理后的ETF代码
        """
        if not etf_code:
            return etf_code
            
        return str(etf_code).replace('.SH', '').replace('.SZ', '')
    """EMA配置管理类 - 重构版（与WMA/SMA保持一致）"""
    
    # EMA系统专属参数设置
    EMA_SYSTEM_PARAMS = {
        'name': 'EMA',
        'sensitivity_level': 'FAST_RESPONSE',
        'base_threshold': 0.22,
        'tolerance_ratio': 0.30,
        'volume_factor': 1.18,
        'signal_decay': 0.20,
        'quality_bonus_threshold': 2.2,
        'noise_filter': 0.18,
        'description': 'EMA快速响应价格变化，平衡敏感性和稳定性，适合捕捉趋势转折'
    }
    
    def __init__(self, adj_type: str = "前复权", ema_periods: Optional[List[int]] = None, 
                 enable_cache: bool = True, performance_mode: bool = False):
        """
        初始化EMA配置 - 重构版
        
        Args:
            adj_type: 复权类型 ("前复权", "后复权", "除权")
            ema_periods: EMA周期列表，None时使用默认[12, 26]
            enable_cache: 是否启用缓存
            performance_mode: 是否启用性能模式（关闭调试输出）
        """
        if not performance_mode:
            print("⚙️  EMA配置初始化 (重构版)...")
        
        # 复权类型配置
        self.adj_type = adj_type
        self.adj_type_mapping = {
            "前复权": "0_ETF日K(前复权)",
            "后复权": "0_ETF日K(后复权)", 
            "除权": "0_ETF日K(除权)"
        }
        
        # EMA周期配置（专注中短期）
        if ema_periods is None:
            self.ema_periods = [12, 26]  # MACD标准周期
        else:
            self.ema_periods = ema_periods
        
        # 系统配置
        self.enable_cache = enable_cache
        self.performance_mode = performance_mode
        self.system_params = self.EMA_SYSTEM_PARAMS.copy()
        
        # 数据要求 - 使用所有可用历史数据
        self.required_rows = None
        
        # 路径配置
        self._setup_paths()
        
        if not performance_mode:
            print(f"   ✅ 复权类型: {self.adj_type}")
            print(f"   📊 EMA周期: {self.ema_periods}")
            print(f"   🗂️ 缓存: {'启用' if enable_cache else '禁用'}")
            print(f"   🚀 性能模式: {'启用' if performance_mode else '禁用'}")
            print(f"   📁 数据目录: {self.data_dir}")
            print(f"   📄 数据策略: 使用所有可用历史数据")
        
    def _setup_paths(self):
        """智能路径配置 - 与WMA/SMA保持一致"""
        # 获取当前脚本的基础目录
        current_dir = os.getcwd()
        
        # 智能检测项目根目录
        if "ETF_计算额外数据" in current_dir:
            project_root = current_dir.split("ETF_计算额外数据")[0]
        else:
            project_root = current_dir
            
        # 构建数据目录路径 - 指向ETF日更数据
        self.data_dir = os.path.join(project_root, "ETF日更", self.adj_type_mapping[self.adj_type])
        
        # 输出目录配置 - 与WMA/SMA保持一致
        self.default_output_dir = os.path.join(
            os.path.dirname(__file__), 
            "..", 
            "..", 
            "data"
        )
        
        # 数据路径 - 项目根目录下的ETF数据
        self.data_path = self.data_dir
        
        if not self.performance_mode:
            print(f"   🔍 项目根目录: {project_root}")
            print(f"   📂 数据目录: {self.data_dir}")
    
    def get_system_thresholds(self) -> Dict[str, float]:
        """获取EMA系统专属的阈值参数"""
        return {
            'minimal': self.system_params['base_threshold'],
            'moderate': self.system_params['base_threshold'] * 3.6,
            'strong': self.system_params['base_threshold'] * 6.8,
            'noise_filter': self.system_params['noise_filter']
        }
    
    def get_system_score_weights(self) -> Dict[str, float]:
        """获取EMA系统专属的评分权重"""
        return {
            '强势': 1.1,
            '中等': 0.7,
            '温和': 0.35,
            '微弱': 0.075
        }
    
    def get_volume_threshold(self) -> float:
        """获取EMA系统的量能确认阈值"""
        return self.system_params['volume_factor']
    
    def get_tolerance_ratio(self) -> float:
        """获取EMA系统的容错比例"""
        return self.system_params['tolerance_ratio']
        
    def validate_data_path(self) -> bool:
        """验证数据路径是否存在"""
        if os.path.exists(self.data_dir):
            file_count = len([f for f in os.listdir(self.data_dir) if f.endswith('.csv')])
            if not self.performance_mode:
                print(f"   ✅ 数据路径验证成功，找到 {file_count} 个CSV文件")
            return True
        else:
            if not self.performance_mode:
                print(f"   ❌ 数据路径不存在: {self.data_dir}")
            return False
    
    def get_etf_file_path(self, etf_code: str) -> str:
        """获取ETF数据文件路径"""
        # 清理ETF代码，移除交易所后缀，因为实际文件名不包含后缀
        clean_etf_code = etf_code.replace('.SH', '').replace('.SZ', '')
        
        filename = f"{clean_etf_code}.csv"
        return os.path.join(self.data_dir, filename)
    
    def get_ema_display_info(self) -> str:
        """获取EMA配置的显示信息"""
        period_desc = ", ".join([f"EMA{p}" for p in self.ema_periods])
        return f"EMA配置 ({self.adj_type}): {period_desc}"
        
    def get_smoothing_factor(self, period: int) -> float:
        """获取EMA平滑因子 α = 2/(period+1)"""
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
            'enable_cache': self.enable_cache,
            'performance_mode': self.performance_mode,
            'system_params': self.system_params,
            'system_thresholds': self.get_system_thresholds(),
            'system_score_weights': self.get_system_score_weights(),
            'optimization': f'EMA系统专属参数：快速响应控制 (基准{self.system_params["base_threshold"]}%)'
        }