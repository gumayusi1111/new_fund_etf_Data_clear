"""
OBV指标基础设施模块
==================

包含系统核心基础设施组件:
- 配置管理 (config.py)
- 缓存管理 (cache_manager.py) 
- 数据读取 (data_reader.py)
- 工具函数 (utils.py)

这些模块为整个OBV系统提供底层支持和服务
"""

__version__ = "1.0.0"
__author__ = "ETF量化分析系统"

# 导入核心基础设施组件
from .config import OBVConfig, obv_config
from .cache_manager import OBVCacheManager, CacheMetadata
from .data_reader import OBVDataReader

# 公开接口
__all__ = [
    'OBVConfig',
    'obv_config', 
    'OBVCacheManager',
    'CacheMetadata',
    'OBVDataReader'
]