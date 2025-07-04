#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ETF初筛系统
用于从日更数据中筛选出优质ETF的模块化系统
"""

__version__ = "1.0.0"
__author__ = "ETF Data Team"

# 导出主要组件
from .data_loader import ETFDataLoader
from .processors.data_processor import ETFDataProcessor
from .processors.output_manager import OutputManager

__all__ = [
    'ETFDataLoader',
    'ETFDataProcessor', 
    'OutputManager'
] 