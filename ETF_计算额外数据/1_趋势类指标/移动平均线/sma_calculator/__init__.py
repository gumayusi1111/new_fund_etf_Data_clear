"""
SMA计算器 - 重构版
=================

模块化的简单移动平均线计算器
- 分层架构，职责清晰
- 智能缓存，高性能
- 功能完全兼容原版
"""

from .controllers import SMAMainController
from .engines import SMAEngine
from .infrastructure import SMAConfig
from .outputs import CSVOutputHandler, DisplayFormatter

__version__ = "2.0.0"
__author__ = "SMA Calculator Team"
__description__ = "重构版SMA计算器，分层架构，高性能缓存"

__all__ = [
    'SMAMainController',
    'SMAEngine', 
    'SMAConfig',
    'CSVOutputHandler',
    'DisplayFormatter'
]