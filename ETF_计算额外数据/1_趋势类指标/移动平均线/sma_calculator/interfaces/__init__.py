#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SMA接口定义层
============

定义系统中各层组件的标准接口，确保松耦合和高内聚的架构设计
"""

from .output_interface import (
    IOutputHandler, ICSVHandler, IDisplayFormatter, IStatisticsCalculator, IResultBuilder,
    OutputResult, OutputStatus, OutputFormat
)

__all__ = [
    # 输出接口
    'IOutputHandler', 'ICSVHandler', 'IDisplayFormatter', 'IStatisticsCalculator', 'IResultBuilder',
    'OutputResult', 'OutputStatus', 'OutputFormat'
] 