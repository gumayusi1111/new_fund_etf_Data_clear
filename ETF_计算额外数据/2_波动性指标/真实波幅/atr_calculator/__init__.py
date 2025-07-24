#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ATR (Average True Range) 真实波幅计算器
====================================

真实波幅(ATR)是衡量价格波动性的核心指标，广泛用于风险管理和仓位控制。
本模块提供完整的ATR计算体系，包含7个核心字段的计算和分析。

核心功能:
- TR (True Range): 真实波幅计算
- ATR_10: 10日平均真实波幅  
- ATR_Percent: ATR百分比(标准化)
- ATR_Change_Rate: ATR变化率
- ATR_Ratio_HL: ATR占区间比
- Stop_Loss: 建议止损位
- Volatility_Level: 波动水平分级

技术特性:
- 🚀 向量化计算引擎
- 💾 智能缓存系统
- 📊 双门槛支持(3000万/5000万)
- 🔄 增量更新机制
- 📈 中国市场优化

版本: 1.0.0
作者: ETF量化团队
更新: 2025-07-13
"""

__version__ = "1.0.0"
__author__ = "ETF量化团队"

# 导出主要组件
from .engines.atr_engine import ATREngine
from .controllers.main_controller import ATRMainController
from .infrastructure.config import ATRConfig

__all__ = [
    'ATREngine',
    'ATRMainController', 
    'ATRConfig',
]