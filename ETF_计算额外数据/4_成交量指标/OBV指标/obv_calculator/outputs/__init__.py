"""
OBV指标输出模块
===============

包含数据输出和结果展示组件:
- CSV处理器 (csv_handler.py)
- 显示格式化器 (display_formatter.py)
- 结果处理器 (result_processor.py)

提供标准化的数据输出和用户友好的结果展示
"""

__version__ = "1.0.0"
__author__ = "ETF量化分析系统"

# 导入核心输出组件
from .csv_handler import OBVCSVHandler
from .display_formatter import OBVDisplayFormatter

# 公开接口
__all__ = [
    'OBVCSVHandler',
    'OBVDisplayFormatter'
]