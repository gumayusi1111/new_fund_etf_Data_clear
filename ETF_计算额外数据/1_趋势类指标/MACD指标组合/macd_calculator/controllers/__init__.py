"""
MACD控制器层
===========

提供MACD系统的控制器组件
"""

from .main_controller import MACDMainController
from .etf_processor import MACDETFProcessor
from .batch_processor import MACDBatchProcessor

__all__ = [
    'MACDMainController',
    'MACDETFProcessor', 
    'MACDBatchProcessor'
]