"""
动量振荡器指标系统配置模块
============================

配置管理、常量定义和系统参数设置
"""

import os
from pathlib import Path
from typing import Dict, List, Any
from datetime import timedelta

class MomentumConfig:
    """动量振荡器指标系统配置类"""
    
    # ==================== 基础路径配置 ====================
    
    # 获取当前文件所在目录的父目录的父目录 (动量振荡器根目录)
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
    
    # 初筛数据目录
    ETF_FILTER_DIR = BASE_DIR.parent.parent / "ETF_初筛" / "data"
    
    # ==================== 动量指标计算参数 ====================
    
    # 动量核心参数
    MOMENTUM_CONFIG = {
        'momentum_periods': [10, 20],      # 基础动量周期
        'roc_periods': [5, 12, 25],        # ROC变动率周期
        'pmo_config': {                    # PMO参数
            'roc_period': 10,              # PMO内部ROC周期
            'ema1_period': 35,             # 第一次EMA平滑
            'ema2_period': 20,             # 第二次EMA平滑(PMO主线)
            'signal_period': 9,            # PMO信号线周期
            'scale_factor': 1000           # PMO放大倍数
        },
        'williams_period': 14,             # 威廉指标周期
        'composite_config': {              # 综合指标参数
            'trend_threshold': 2,          # 趋势判断阈值(%)
            'divergence_window': 30,       # 背离检测窗口 (优化: 20→30天)
            'volatility_window': 10        # 波动率计算窗口
        },
        'min_data_points': 30,             # 最少数据点
        'precision': 8,                    # 计算精度(小数位)
    }
    
    # 异常值处理
    ANOMALY_CONFIG = {
        'price_zero_threshold': 1e-6,      # 价格零值阈值
        'limit_move_threshold': 0.195,     # 涨跌停阈值(19.5%)
        'price_change_max': 0.5,           # 价格变化最大值(50%)
        'handle_missing_data': True,       # 是否处理缺失数据
        'interpolation_method': 'forward', # 插值方法
        'remove_suspended': True,          # 移除停牌数据
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
        'file_name': 'momentum_oscillator.log',
        'max_file_size_mb': 50,
        'backup_count': 5,
        'console_output': True
    }
    
    # ==================== 数据字段配置 ====================
    
    # 输入数据字段映射 (中文名 -> 英文名)
    INPUT_FIELDS = {
        '代码': 'code',
        '日期': 'date', 
        '开盘价': 'open',
        '最高价': 'high',
        '最低价': 'low',
        '收盘价': 'close',
        '上日收盘': 'prev_close',
        '涨跌': 'change',
        '涨幅%': 'change_pct',
        '成交量(手数)': 'volume',
        '成交额(千元)': 'amount'
    }
    
    # 输出数据字段 (纯客观数据)
    OUTPUT_FIELDS = [
        'code',                    # ETF代码
        'date',                    # 计算日期
        'momentum_10',             # 10日价格动量
        'momentum_20',             # 20日价格动量
        'roc_5',                   # 5日变动率(%)
        'roc_12',                  # 12日变动率(%)
        'roc_25',                  # 25日变动率(%)
        'pmo',                     # 价格动量振荡器主线
        'pmo_signal',              # PMO信号线
        'williams_r',              # 威廉指标(-100~0)
        'momentum_strength',       # 动量强度 |ROC_20|
        'momentum_acceleration',   # 动量加速度
        'momentum_volatility',     # 动量波动率
        'calc_time'                # 计算时间戳
    ]
    
    # ==================== 验证规则配置 ====================
    
    VALIDATION_CONFIG = {
        'required_fields': ['代码', '日期', '收盘价', '最高价', '最低价'],
        'numeric_fields': ['开盘价', '最高价', '最低价', '收盘价', '成交量(手数)', '成交额(千元)'],
        'date_format': '%Y-%m-%d',
        'code_pattern': r'^\d{6}$',        # ETF代码格式
        'min_records': 30,                 # 最少记录数
        'max_missing_ratio': 0.05,         # 最大缺失比例(5%)
    }
    
    # ==================== 系统常量 ====================
    
    CONSTANTS = {
        'EPSILON': 1e-8,                   # 数值计算精度
        'MAX_MOMENTUM_VALUE': 1e10,        # 动量最大值
        'MIN_MOMENTUM_VALUE': -1e10,       # 动量最小值
        'DEFAULT_FILL_VALUE': 0.0,         # 默认填充值
        'TRADING_DAYS_PER_YEAR': 252,      # 年交易日数
        'BUSINESS_HOURS': (9.5, 15),       # 交易时间(小时)
        'WILLIAMS_R_MIN': -100,            # 威廉指标最小值
        'WILLIAMS_R_MAX': 0,               # 威廉指标最大值
    }
    
    # ==================== 字段描述 ====================
    
    # 纯客观数据字段描述
    FIELD_DESCRIPTIONS = {
        "momentum_10": "10日价格动量 (Close - Close[t-10])",
        "momentum_20": "20日价格动量 (Close - Close[t-20])",
        "roc_5": "5日变动率 (百分比)",
        "roc_12": "12日变动率 (百分比)",
        "roc_25": "25日变动率 (百分比)",
        "pmo": "价格动量振荡器主线 (双重EMA平滑)",
        "pmo_signal": "PMO信号线 (9日EMA)",
        "williams_r": "14日威廉指标 (-100~0)",
        "momentum_strength": "动量强度 |ROC_20| (绝对值)",
        "momentum_acceleration": "动量加速度 (momentum_10 - momentum_20)",
        "momentum_volatility": "动量波动率 (ROC_20的10日标准差)"
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
    def get_filter_file_path(cls, threshold: str) -> Path:
        """获取初筛文件路径"""
        return cls.ETF_FILTER_DIR / threshold / "通过筛选ETF.txt"
    
    @classmethod
    def get_system_info(cls) -> Dict[str, Any]:
        """获取系统信息"""
        return {
            'name': '动量振荡器指标计算系统',
            'version': '2.0.0',
            'base_dir': str(cls.BASE_DIR),
            'supported_thresholds': cls.get_supported_thresholds(),
            'output_fields': cls.OUTPUT_FIELDS,
            'cache_enabled': cls.CACHE_CONFIG['enabled'],
            'performance_mode': cls.PERFORMANCE_CONFIG['use_vectorization']
        }

# 全局配置实例
momentum_config = MomentumConfig()

# 初始化目录
momentum_config.init_directories()