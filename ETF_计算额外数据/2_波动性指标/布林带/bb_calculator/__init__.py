#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
布林带计算器包
==============

布林带(Bollinger Bands)技术指标计算系统
采用六层模块化架构，支持智能缓存和增量计算
"""

__version__ = "1.0.0"
__author__ = "ETF技术指标系统"
__description__ = "布林带计算器 - 六层架构模块化系统"

# 版本信息
VERSION_INFO = {
    'version': __version__,
    'name': 'BollingerBands Calculator',
    'description': __description__,
    'architecture': '六层模块化架构',
    'features': [
        '智能缓存系统',
        '增量计算优化', 
        '向量化批量处理',
        '高性能计算引擎',
        '多门槛支持',
        '科学算法验证'
    ]
}

def get_version_info():
    """获取版本信息"""
    return VERSION_INFO