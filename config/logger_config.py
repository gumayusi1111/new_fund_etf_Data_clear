#!/usr/bin/env python3
"""
日志配置模块
为ETF数据同步系统提供统一的日志配置
"""

import os
import json
import logging
import logging.handlers
from typing import Dict, Any
from pathlib import Path


def load_config() -> Dict[str, Any]:
    """加载配置文件"""
    config_path = os.path.join(os.path.dirname(__file__), 'config.json')
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"警告：无法加载配置文件 {config_path}: {e}")
        return get_default_config()


def get_default_config() -> Dict[str, Any]:
    """获取默认配置"""
    return {
        "logging": {
            "level": "INFO",
            "file_path": "./logs/etf_sync.log",
            "max_size_mb": 10,
            "backup_count": 5,
            "console_output": True,
            "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        }
    }


def setup_logger(name, log_file, level=logging.INFO):
    """设置日志记录器"""
    # 确保日志目录存在
    log_dir = Path(log_file).parent
    log_dir.mkdir(parents=True, exist_ok=True)
    
    # 创建日志记录器
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    # 避免重复添加handler
    if logger.handlers:
        return logger
    
    # 创建文件处理器
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setLevel(level)
    
    # 创建控制台处理器
    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)
    
    # 创建格式器
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    
    # 添加处理器到日志记录器
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger


def get_project_root():
    """获取项目根目录"""
    current_file = Path(__file__)
    # logger_config.py 在 config/ 目录下，需要向上一级找到项目根目录
    project_root = current_file.parent.parent
    return project_root


def get_logger_paths():
    """获取日志文件路径"""
    project_root = get_project_root()
    logs_dir = project_root / "logs"
    
    return {
        'system': logs_dir / "system" / "etf_sync.log",              # 统一系统日志
    }





def setup_system_logger():
    """设置统一系统日志"""
    paths = get_logger_paths()
    return setup_logger('etf_system', paths['system'])

# 别名函数，保持向后兼容
def setup_daily_logger():
    """日更专用日志（实际使用统一日志）"""
    return setup_system_logger()

def setup_weekly_logger():
    """周更专用日志（实际使用统一日志）"""
    return setup_system_logger()





def get_all_loggers():
    """获取所有日志记录器（现在都指向同一个日志）"""
    system_logger = setup_system_logger()
    return {
        'system': system_logger,
        'daily': system_logger,   # 指向同一个logger
        'weekly': system_logger,  # 指向同一个logger
    }


def log_system_info(logger: logging.Logger):
    """记录系统信息"""
    try:
        import platform
        import sys
        
        logger.info("=" * 50)
        logger.info("系统信息:")
        logger.info(f"  操作系统: {platform.system()} {platform.release()}")
        logger.info(f"  Python版本: {sys.version}")
        logger.info(f"  工作目录: {os.getcwd()}")
        logger.info("=" * 50)
        
    except Exception as e:
        logger.warning(f"无法获取系统信息: {e}")


# 向后兼容的函数
def setup_logger_old(name: str = "etf_sync", log_type: str = "general") -> logging.Logger:
    """向后兼容的日志设置函数"""
    logger_map = {
        "general": setup_system_logger,
        "daily": setup_daily_logger,
        "weekly": setup_weekly_logger
    }
    
    setup_func = logger_map.get(log_type, setup_system_logger)
    return setup_func()


if __name__ == "__main__":
    # 测试日志配置
    test_logger = setup_logger("test")
    log_system_info(test_logger)
    test_logger.info("这是一条测试日志信息")
    test_logger.warning("这是一条警告信息")
    test_logger.error("这是一条错误信息")
    print("日志测试完成，请检查logs目录中的日志文件") 