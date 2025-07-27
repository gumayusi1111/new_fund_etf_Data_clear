"""
OBV指标计算引擎模块
==================

包含OBV指标的核心计算逻辑:
- OBV主计算引擎 (obv_engine.py)
- 历史数据计算器 (historical_calculator.py)
- 实时计算引擎 (realtime_calculator.py)

提供高性能的向量化OBV指标计算能力
"""

__version__ = "1.0.0"
__author__ = "ETF量化分析系统"

# 导入核心计算引擎
from .obv_engine import OBVEngine

# 公开接口
__all__ = [
    'OBVEngine'
]