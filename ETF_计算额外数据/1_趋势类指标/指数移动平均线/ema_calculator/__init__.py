#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
EMA计算器模块 - 重构版
====================

模块化的指数移动平均线计算系统
参照WMA/SMA系统的成功架构，提供统一的EMA计算接口

📦 新架构组件:
- controllers: 控制器层 (主控制器、ETF处理器、批量处理器)
- engines: 计算引擎层 (EMA引擎、历史数据计算器)
- infrastructure: 基础设施层 (配置、数据读取、缓存管理、文件管理)
- outputs: 输出处理层 (结果处理、CSV处理、显示格式化)

🛡️ 设计原则:
- 分层架构，高内聚低耦合
- 智能缓存，增量更新
- 与WMA/SMA系统保持一致
- 专注中短期指标 (EMA12, EMA26)
"""

__version__ = "2.0.0"
__author__ = "EMA计算器重构团队"

# 主要接口导入 - 新架构
from .controllers.main_controller import EMAMainController
from .infrastructure.config import EMAConfig

# 向后兼容的别名 (与原系统保持一致)
EMAController = EMAMainController

# 导出主要接口
__all__ = [
    'EMAMainController',
    'EMAController',  # 向后兼容别名
    'EMAConfig'
] 