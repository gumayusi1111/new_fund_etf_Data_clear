"""
📊 动量振荡器计算模块 (Momentum Oscillators Calculator Module)
==========================================================

基于"动量先于价格"理论的技术指标计算系统
涵盖13个核心动量指标，为ETF深度筛选提供客观数据支持

模块架构:
- engines/: 核心计算引擎
- infrastructure/: 基础设施（配置、缓存、数据读取）
- controllers/: 控制器（业务逻辑协调）
- outputs/: 输出处理（CSV、格式化显示）
- interfaces/: 接口定义

作者: Claude Code Assistant
版本: 2.0.0 - 模块化重构版
创建时间: 2025-07-30
"""

__version__ = "2.0.0"
__author__ = "Claude Code Assistant"

# 导入核心组件
from .engines.momentum_engine import MomentumEngine
from .controllers.main_controller import MomentumController
from .infrastructure.config import MomentumConfig, momentum_config
from .infrastructure.data_reader import MomentumDataReader
from .infrastructure.cache_manager import MomentumCacheManager
from .outputs.csv_handler import MomentumCSVHandler
from .outputs.display_formatter import MomentumDisplayFormatter

__all__ = [
    "MomentumEngine",
    "MomentumController", 
    "MomentumConfig",
    "momentum_config",
    "MomentumDataReader",
    "MomentumCacheManager",
    "MomentumCSVHandler",
    "MomentumDisplayFormatter"
]