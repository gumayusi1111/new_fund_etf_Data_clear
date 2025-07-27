"""
OBV指标系统配置模块
==================

配置管理、常量定义和系统参数设置
"""

import os
from pathlib import Path
from typing import Dict, List, Any
from datetime import timedelta

class OBVConfig:
    """OBV指标系统配置类"""
    
    # ==================== 基础路径配置 ====================
    
    # 获取当前文件所在目录的父目录的父目录 (OBV指标根目录)
    BASE_DIR = Path(__file__).parent.parent.parent
    
    # 数据目录
    DATA_DIR = BASE_DIR / "data"
    CACHE_DIR = BASE_DIR / "cache" 
    LOGS_DIR = BASE_DIR / "logs"
    TESTS_DIR = BASE_DIR / "tests"
    
    # 门槛目录
    THRESHOLD_3000_DATA = DATA_DIR / "3000万门槛"
    THRESHOLD_5000_DATA = DATA_DIR / "5000万门槛"
    THRESHOLD_3000_CACHE = CACHE_DIR / "3000万门槛"
    THRESHOLD_5000_CACHE = CACHE_DIR / "5000万门槛"
    
    # 缓存元数据目录
    CACHE_META_DIR = CACHE_DIR / "meta"
    
    # 源数据目录 (ETF数据来源 - 日更数据)
    ETF_SOURCE_DIR = BASE_DIR.parent.parent.parent / "ETF日更" / "0_ETF日K(前复权)"
    
    # ==================== OBV计算参数 ====================
    
    # OBV核心参数
    OBV_CONFIG = {
        'ma_period': 10,           # OBV移动平均周期
        'change_periods': [5, 20], # 变化率计算周期
        'min_data_points': 21,     # 最少数据点(支持20日变化率)
        'precision': 8,            # 计算精度(小数位)
        'initial_obv': 'volume'    # 初始OBV值:'volume'或'zero'
    }
    
    # 异常值处理
    ANOMALY_CONFIG = {
        'volume_zero_threshold': 1e-6,    # 成交量零值阈值
        'volume_max_multiplier': 50,      # 成交量异常倍数
        'price_change_threshold': 0.15,   # 价格变化异常阈值(15%)
        'handle_missing_data': True,      # 是否处理缺失数据
        'interpolation_method': 'forward' # 插值方法:'forward','backward','linear'
    }
    
    # ==================== 门槛筛选配置 ====================
    
    # 成交额门槛(万元)
    THRESHOLD_CONFIG = {
        '3000万门槛': {
            'min_amount': 3000,  # 万元
            'description': '中等规模ETF专用门槛',
            'cache_suffix': '3000万门槛'
        },
        '5000万门槛': {
            'min_amount': 5000,  # 万元  
            'description': '大规模ETF专用门槛',
            'cache_suffix': '5000万门槛'
        }
    }
    
    # ==================== 缓存系统配置 ====================
    
    CACHE_CONFIG = {
        'enabled': True,
        'expire_days': 30,              # 缓存过期天数
        'max_cache_size_mb': 2048,      # 最大缓存大小(MB)
        'cache_file_extension': '.csv', # 缓存文件扩展名
        'meta_file_extension': '.json', # 元数据文件扩展名
        'cleanup_on_startup': False,    # 启动时清理过期缓存
        'compression': False,           # 是否压缩缓存文件
    }
    
    # ==================== 性能优化配置 ====================
    
    PERFORMANCE_CONFIG = {
        'batch_size': 100,              # 批处理大小
        'max_workers': 4,               # 最大并行数
        'memory_limit_mb': 1024,        # 内存限制(MB)
        'chunk_size': 10000,            # 数据块大小
        'use_vectorization': True,      # 使用向量化计算
        'enable_jit': False,            # 启用JIT编译(需numba)
    }
    
    # ==================== 日志配置 ====================
    
    LOGGING_CONFIG = {
        'level': 'INFO',
        'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        'file_name': 'obv_system.log',
        'max_file_size_mb': 50,
        'backup_count': 5,
        'console_output': True
    }
    
    # ==================== 数据字段配置 ====================
    
    # 输入数据字段映射
    INPUT_FIELDS = {
        'code': '代码',
        'date': '日期', 
        'close': '收盘价',
        'volume': '成交量(手数)',
        'amount': '成交额(千元)'
    }
    
    # 输出数据字段
    OUTPUT_FIELDS = [
        'code',           # ETF代码
        'date',           # 计算日期
        'obv',            # OBV指标值
        'obv_ma10',       # OBV 10日移动平均
        'obv_change_5',   # OBV 5日变化率(%)
        'obv_change_20',  # OBV 20日变化率(%)
        'calc_time'       # 计算时间戳
    ]
    
    # ==================== 验证规则配置 ====================
    
    VALIDATION_CONFIG = {
        'required_fields': ['代码', '日期', '收盘价', '成交量(手数)'],
        'numeric_fields': ['收盘价', '成交量(手数)', '成交额(千元)'],
        'date_format': '%Y-%m-%d',
        'code_pattern': r'^\d{6}$',        # ETF代码格式
        'min_records': 21,                 # 最少记录数
        'max_missing_ratio': 0.05,         # 最大缺失比例(5%)
    }
    
    # ==================== 系统常量 ====================
    
    CONSTANTS = {
        'EPSILON': 1e-8,                   # 数值计算精度
        'MAX_OBV_VALUE': 1e15,             # OBV最大值
        'MIN_OBV_VALUE': -1e15,            # OBV最小值
        'DEFAULT_FILL_VALUE': 0.0,         # 默认填充值
        'TRADING_DAYS_PER_YEAR': 252,      # 年交易日数
        'BUSINESS_HOURS': (9.5, 15),       # 交易时间(小时)
    }
    
    # ==================== 工具方法 ====================
    
    @classmethod
    def init_directories(cls):
        """初始化目录结构"""
        directories = [
            cls.DATA_DIR,
            cls.CACHE_DIR,
            cls.LOGS_DIR, 
            cls.TESTS_DIR,
            cls.THRESHOLD_3000_DATA,
            cls.THRESHOLD_5000_DATA,
            cls.THRESHOLD_3000_CACHE,
            cls.THRESHOLD_5000_CACHE,
            cls.CACHE_META_DIR
        ]
        
        for directory in directories:
            directory.mkdir(parents=True, exist_ok=True)
    
    @classmethod
    def get_threshold_config(cls, threshold: str) -> Dict[str, Any]:
        """获取门槛配置"""
        return cls.THRESHOLD_CONFIG.get(threshold, {})
    
    @classmethod
    def get_data_dir(cls, threshold: str) -> Path:
        """获取门槛对应的数据目录"""
        if threshold == '3000万门槛':
            return cls.THRESHOLD_3000_DATA
        elif threshold == '5000万门槛':
            return cls.THRESHOLD_5000_DATA
        else:
            raise ValueError(f"不支持的门槛类型: {threshold}")
    
    @classmethod 
    def get_cache_dir(cls, threshold: str) -> Path:
        """获取门槛对应的缓存目录"""
        if threshold == '3000万门槛':
            return cls.THRESHOLD_3000_CACHE
        elif threshold == '5000万门槛':
            return cls.THRESHOLD_5000_CACHE
        else:
            raise ValueError(f"不支持的门槛类型: {threshold}")
    
    @classmethod
    def get_supported_thresholds(cls) -> List[str]:
        """获取支持的门槛类型列表"""
        return list(cls.THRESHOLD_CONFIG.keys())
    
    @classmethod
    def validate_threshold(cls, threshold: str) -> bool:
        """验证门槛类型是否有效"""
        return threshold in cls.THRESHOLD_CONFIG
    
    @classmethod
    def get_system_info(cls) -> Dict[str, Any]:
        """获取系统信息"""
        return {
            'name': 'OBV指标计算系统',
            'version': '1.0.0',
            'base_dir': str(cls.BASE_DIR),
            'supported_thresholds': cls.get_supported_thresholds(),
            'output_fields': cls.OUTPUT_FIELDS,
            'cache_enabled': cls.CACHE_CONFIG['enabled'],
            'performance_mode': cls.PERFORMANCE_CONFIG['use_vectorization']
        }

# 全局配置实例
obv_config = OBVConfig()

# 初始化目录
obv_config.init_directories()