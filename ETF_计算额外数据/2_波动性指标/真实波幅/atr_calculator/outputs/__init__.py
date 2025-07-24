#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ATR输出处理模块
==============

负责ATR计算结果的各种输出格式处理：
- CSV文件输出
- 数据格式化
- 批量保存操作
- 汇总报告生成
"""

from .csv_handler import ATRCSVHandler

__all__ = [
    'ATRCSVHandler',
]