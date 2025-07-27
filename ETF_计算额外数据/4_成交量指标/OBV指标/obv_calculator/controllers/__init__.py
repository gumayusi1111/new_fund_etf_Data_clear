"""
OBV指标控制器模块
=================

包含系统控制和协调逻辑:
- 主控制器 (main_controller.py)
- ETF处理器 (etf_processor.py) 
- 批处理器 (batch_processor.py)

负责协调各个组件，提供统一的系统接口
"""

__version__ = "1.0.0"
__author__ = "ETF量化分析系统"

# 导入核心控制器
from .main_controller import OBVController

# 公开接口
__all__ = [
    'OBVController'
]