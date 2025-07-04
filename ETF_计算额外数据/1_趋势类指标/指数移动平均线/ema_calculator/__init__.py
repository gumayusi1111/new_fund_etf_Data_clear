#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
EMA计算器模块化包 - 中短期专版
==============================

📦 模块架构:
- config: 配置管理
- data_reader: 数据读取器
- ema_engine: EMA计算引擎  
- signal_analyzer: 信号分析器
- result_processor: 结果处理器
- file_manager: 文件管理器
- controller: 主控制器

🛡️ 设计原则:
- 高内聚低耦合
- 专注中短期指标 (EMA12, EMA26)
- 简洁高效
"""

__version__ = "1.0.0"
__author__ = "ETF数据处理系统"

# 导入主要组件
from .config import EMAConfig
from .data_reader import ETFDataReader
from .ema_engine import EMAEngine
# from .signal_analyzer import SignalAnalyzer  # 🚫 已移除复杂分析
from .result_processor import ResultProcessor
from .file_manager import FileManager
from .controller import EMAController

# 导出主要接口
__all__ = [
    'EMAConfig',
    'ETFDataReader', 
    'EMAEngine',
    # 'SignalAnalyzer',  # 🚫 已移除复杂分析
    'ResultProcessor',
    'FileManager',
    'EMAController'
] 