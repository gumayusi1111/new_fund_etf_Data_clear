"""
WMA计算器 - 重构版
=================

模块化的加权移动平均线计算器
- 分层架构，职责清晰
- 智能缓存，高性能
- 功能完全兼容原版
"""

from .controllers import WMAMainController
from .engines import WMAEngine
from .infrastructure import WMAConfig
from .outputs import WMACSVHandler, WMADisplayFormatter

__version__ = "2.0.0"
__author__ = "WMA Calculator Team"
__description__ = "重构版WMA计算器，分层架构，高性能缓存"

__all__ = [
    'WMAMainController',
    'WMAEngine', 
    'WMAConfig',
    'WMACSVHandler',
    'WMADisplayFormatter'
] 