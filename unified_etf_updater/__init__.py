#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
统一ETF更新器模块
提供模块化的ETF数据更新和管理功能
"""

from .core import UnifiedETFUpdater
from .database import DatabaseManager
from .git_manager import GitManager
from .updaters import ETFUpdaters
from .validator import WeeklyDailyValidator

__version__ = "1.0.0"
__author__ = "ETF Data System"

__all__ = [
    'UnifiedETFUpdater',
    'DatabaseManager', 
    'GitManager',
    'ETFUpdaters',
    'WeeklyDailyValidator'
] 