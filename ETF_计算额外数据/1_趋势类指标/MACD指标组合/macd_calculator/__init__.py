#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MACD计算器 - 重构版
==================

模块化的MACD指标计算系统
- 分层架构，职责清晰
- 智能缓存，高性能
- 与其他趋势类指标系统保持一致的架构
"""

from .controllers import MACDMainController
from .engines import MACDEngine
from .infrastructure import MACDConfig
from .outputs import MACDCSVHandler, MACDDisplayFormatter, MACDResultProcessor

__version__ = "2.0.0"
__author__ = "MACD Calculator Team"
__description__ = "重构版MACD计算器，分层架构，高性能计算"

__all__ = [
    'MACDMainController',
    'MACDEngine', 
    'MACDConfig',
    'MACDCSVHandler',
    'MACDDisplayFormatter',
    'MACDResultProcessor'
] 