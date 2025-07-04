#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
WMA配置管理模块 - 重构版
=====================

从原有config.py完全迁移，保持所有功能和参数不变
"""

import os
from typing import List, Dict, Optional
from dataclasses import dataclass, field


@dataclass
class WMASystemParams:
    """WMA系统参数配置 - 保持原有参数"""
    name: str = 'WMA'
    sensitivity_level: str = 'HIGHEST'
    base_threshold: float = 0.20
    tolerance_ratio: float = 0.25
    volume_factor: float = 1.20
    signal_decay: float = 0.15
    quality_bonus_threshold: float = 2.5
    noise_filter: float = 0.15
    description: str = 'WMA对近期价格最敏感，变化最快，需要最严格的参数控制假信号'


class WMAConfig:
    """WMA配置管理器 - 重构版（功能完全一致）"""
    
    # 复权类型映射 - 保持原有映射
    ADJ_TYPES = {
        "前复权": "0_ETF日K(前复权)",
        "后复权": "0_ETF日K(后复权)",
        "除权": "0_ETF日K(除权)"
    }
    
    # 科学复权评估 - 保持原有评估
    ADJ_TYPE_SCIENTIFIC_EVALUATION = {
        "前复权": {
            "scientific_score": 95,
            "recommendation": "强烈推荐",
            "pros": ["价格连续性好", "适合技术指标", "历史数据稳定"],
            "cons": ["历史价格非实际价格"],
            "use_cases": ["技术分析", "WMA计算", "趋势判断"]
        },
        "后复权": {
            "scientific_score": 60,
            "recommendation": "谨慎使用",
            "pros": ["基于当前价格", "便于理解收益"],
            "cons": ["历史价格会变化", "影响技术指标", "不利于回测"],
            "use_cases": ["收益计算", "资产配置"]
        },
        "除权": {
            "scientific_score": 30,
            "recommendation": "不推荐",
            "pros": ["实际交易价格"],
            "cons": ["价格跳跃严重", "破坏指标连续性", "影响WMA准确性"],
            "use_cases": ["查看实际价格"]
        }
    }
    
    # 默认WMA周期 - 保持原有周期
    DEFAULT_WMA_PERIODS = [3, 5, 10, 20]
    
    # 默认ETF代码 - 保持原有代码
    DEFAULT_ETF_CODE = "510050.SH"
    
    def __init__(self, adj_type: str = "前复权", wma_periods: Optional[List[int]] = None, 
                 enable_cache: bool = True, performance_mode: bool = True):
        """
        初始化配置 - 保持原有初始化逻辑
        
        Args:
            adj_type: 复权类型
            wma_periods: WMA周期列表
            enable_cache: 是否启用缓存
            performance_mode: 是否启用性能模式（关闭调试输出）
        """
        self.adj_type = self._validate_and_recommend_adj_type(adj_type)
        self.wma_periods = self._validate_wma_periods(wma_periods or self.DEFAULT_WMA_PERIODS.copy())
        self.max_period = max(self.wma_periods)
        self.enable_cache = enable_cache
        self.performance_mode = performance_mode
        
        # WMA系统专属参数 - 保持原有参数
        self.system_params = WMASystemParams()
        
        # 数据策略 - 保持原有策略：使用所有可用数据
        self.required_rows = None
        
        # 数据路径配置 - 保持原有路径逻辑
        self._setup_paths()
        
        # 输出初始化信息 - 保持原有输出
        self._print_init_info()
    
    def _setup_paths(self) -> None:
        """配置系统路径 - 保持原有路径逻辑"""
        current_dir = os.getcwd()
        
        if "加权移动平均线" in current_dir:
            self.base_data_path = "../../../ETF日更"
            self.wma_script_dir = "."
        elif "data_clear" in current_dir and current_dir.endswith("data_clear"):
            self.base_data_path = "./ETF日更"
            self.wma_script_dir = "./ETF_计算额外数据/1_趋势类指标/加权移动平均线"
        else:
            self.base_data_path = "./ETF日更"
            self.wma_script_dir = "."
        
        self.data_path = os.path.join(self.base_data_path, self.ADJ_TYPES[self.adj_type])
        self.default_output_dir = os.path.join(self.wma_script_dir, "data")
    
    def _print_init_info(self) -> None:
        """打印初始化信息 - 保持原有输出格式"""
        if not self.performance_mode:
            print(f"🔬 WMA配置初始化完成 (科学严谨版 + 系统差异化):")
            print(f"   📈 复权类型: {self.adj_type} (科学评分: {self.get_scientific_score()}/100)")
            print(f"   🎯 WMA周期: {self.wma_periods}")
            print(f"   ⚙️ 系统特性: {self.system_params.description}")
            print(f"   📊 系统参数: 基准阈值={self.system_params.base_threshold}%, 容错率={self.system_params.tolerance_ratio}")
            print(f"   📊 数据策略: 使用所有可用数据，不限制行数")
            print(f"   📁 数据路径: {self.data_path}")
            print(f"   🗂️ 缓存启用: {self.enable_cache}")
            print(f"   🚀 性能模式: {self.performance_mode}")
            
            # 科学建议 - 保持原有建议逻辑
            self._provide_scientific_recommendation()
    
    def get_system_thresholds(self) -> Dict[str, float]:
        """获取WMA系统专属的阈值参数 - 保持原有逻辑"""
        return {
            'minimal': self.system_params.base_threshold,
            'moderate': self.system_params.base_threshold * 3,
            'strong': self.system_params.base_threshold * 6,
            'noise_filter': self.system_params.noise_filter
        }
    
    def get_system_score_weights(self) -> Dict[str, float]:
        """获取WMA系统专属的评分权重 - 保持原有权重"""
        return {
            '强势': 1.0,
            '中等': 0.6,
            '温和': 0.3,
            '微弱': 0.05
        }
    
    def get_volume_threshold(self) -> float:
        """获取WMA系统的量能确认阈值 - 保持原有阈值"""
        return self.system_params.volume_factor
    
    def get_tolerance_ratio(self) -> float:
        """获取WMA系统的容错比例 - 保持原有比例"""
        return self.system_params.tolerance_ratio
    
    def _validate_wma_periods(self, periods: List[int]) -> List[int]:
        """
        验证WMA周期参数
        
        Args:
            periods: WMA周期列表
            
        Returns:
            List[int]: 验证后的周期列表
        """
        if not periods:
            print("❌ WMA周期列表不能为空，使用默认周期")
            return self.DEFAULT_WMA_PERIODS.copy()
        
        valid_periods = []
        for period in periods:
            if not isinstance(period, int):
                if not self.performance_mode:
                    print(f"❌ 跳过非整数周期: {period}")
                continue
            
            if period < 1:
                if not self.performance_mode:
                    print(f"❌ 跳过无效周期: {period} (必须≥1)")
                continue
            
            if period > 250:
                if not self.performance_mode:
                    print(f"⚠️ 周期 {period} 过大（>250天），可能导致计算效率低下")
            
            valid_periods.append(period)
        
        if not valid_periods:
            print("❌ 没有有效的WMA周期，使用默认周期")
            return self.DEFAULT_WMA_PERIODS.copy()
        
        # 去重并排序
        valid_periods = sorted(set(valid_periods))
        
        if len(valid_periods) != len(periods):
            print(f"🔧 WMA周期已验证和去重: {valid_periods}")
        
        return valid_periods
    
    def _validate_and_recommend_adj_type(self, adj_type: str) -> str:
        """验证并推荐复权类型 - 保持原有验证逻辑"""
        if adj_type not in self.ADJ_TYPES:
            print(f"❌ 科学错误: 不支持的复权类型: {adj_type}")
            print(f"💡 支持的类型: {list(self.ADJ_TYPES.keys())}")
            adj_type = "前复权"
            print(f"🔬 已自动使用科学推荐类型: {adj_type}")
        
        return adj_type
    
    def validate_threshold(self, threshold: str) -> bool:
        """
        验证门槛参数的有效性
        
        Args:
            threshold: 门槛类型
            
        Returns:
            bool: 是否为有效门槛
        """
        valid_thresholds = ["3000万门槛", "5000万门槛"]
        return threshold in valid_thresholds
    
    def validate_file_path(self, file_path: str) -> bool:
        """
        验证文件路径的存在性
        
        Args:
            file_path: 文件路径
            
        Returns:
            bool: 文件是否存在
        """
        return os.path.exists(file_path)
    
    def get_scientific_score(self) -> int:
        """获取当前复权类型的科学评分 - 保持原有评分"""
        return self.ADJ_TYPE_SCIENTIFIC_EVALUATION[self.adj_type]["scientific_score"]
    
    def _provide_scientific_recommendation(self):
        """提供科学建议 - 保持原有建议逻辑"""
        evaluation = self.ADJ_TYPE_SCIENTIFIC_EVALUATION[self.adj_type]
        
        if evaluation["scientific_score"] < 70:
            print(f"⚠️  科学建议: 当前复权类型'{self.adj_type}'科学评分较低")
            print(f"🔬 推荐使用: '前复权' (科学评分: 95/100)")
            print(f"💡 理由: 前复权最适合WMA等技术指标计算")
    
    def get_scientific_evaluation(self) -> Dict:
        """获取复权类型的科学评估 - 保持原有评估"""
        return self.ADJ_TYPE_SCIENTIFIC_EVALUATION[self.adj_type]
    
    def validate_data_path(self) -> bool:
        """验证数据路径是否存在 - 保持原有验证逻辑"""
        if os.path.exists(self.data_path):
            print(f"🔍 数据路径验证: {self.data_path} ✅")
            return True
        else:
            print(f"❌ 数据路径不存在: {self.data_path}")
            print("💡 提示: 请检查脚本运行位置或数据文件路径")
            return False
    
    def get_file_path(self, etf_code: str) -> str:
        """获取ETF数据文件的完整路径 - 修正后的路径逻辑"""
        # 去掉交易所后缀，因为数据文件只使用6位数字代码
        clean_etf_code = etf_code.replace('.SH', '').replace('.SZ', '')
        return os.path.join(self.data_path, f"{clean_etf_code}.csv")
    
    def get_adj_folder_name(self) -> str:
        """获取复权文件夹名称 - 保持原有逻辑"""
        return self.ADJ_TYPES[self.adj_type]
    
    def to_dict(self) -> Dict:
        """将配置转换为字典格式 - 保持原有转换逻辑"""
        return {
            'adj_type': self.adj_type,
            'scientific_score': self.get_scientific_score(),
            'scientific_evaluation': self.get_scientific_evaluation(),
            'wma_periods': self.wma_periods,
            'max_period': self.max_period,
            'required_rows': self.required_rows,
            'data_path': self.data_path,
            'system_params': self.system_params.__dict__,
            'system_thresholds': self.get_system_thresholds(),
            'system_score_weights': self.get_system_score_weights(),
            'optimization': f'WMA系统专属参数：最严格控制 (基准{self.system_params.base_threshold}%)',
            'enable_cache': self.enable_cache,
            'performance_mode': self.performance_mode
        } 