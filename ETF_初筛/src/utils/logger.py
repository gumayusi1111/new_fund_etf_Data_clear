#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
日志管理器
为ETF初筛系统提供统一的日志记录功能
"""

import logging
import logging.handlers
import os
from pathlib import Path
from datetime import datetime
from typing import Optional

from .config import get_config


class ETFFilterLogger:
    """ETF初筛专用日志器"""
    
    def __init__(self, name: str = "etf_filter"):
        self.name = name
        self.logger = logging.getLogger(name)
        self._setup_logger()
    
    def _setup_logger(self):
        """设置日志器"""
        if self.logger.handlers:
            return  # 避免重复设置
            
        config = get_config()
        log_settings = config.get_log_settings()
        
        # 设置日志级别
        level = getattr(logging, log_settings.get("级别", "INFO"))
        self.logger.setLevel(level)
        
        # 创建日志目录
        log_dir = config.get_log_dir()
        log_dir.mkdir(parents=True, exist_ok=True)
        
        # 创建格式器
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        # 文件处理器（带轮转）
        log_file = log_dir / f"{self.name}.log"
        file_handler = logging.handlers.RotatingFileHandler(
            log_file,
            maxBytes=log_settings.get("最大文件大小_MB", 10) * 1024 * 1024,
            backupCount=log_settings.get("备份数量", 5),
            encoding='utf-8'
        )
        file_handler.setFormatter(formatter)
        self.logger.addHandler(file_handler)
        
        # 控制台处理器
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)
    
    def info(self, message: str):
        """记录信息日志"""
        self.logger.info(message)
    
    def warning(self, message: str):
        """记录警告日志"""
        self.logger.warning(message)
    
    def error(self, message: str):
        """记录错误日志"""
        self.logger.error(message)
    
    def debug(self, message: str):
        """记录调试日志"""
        self.logger.debug(message)
    
    def start_process(self, process_name: str):
        """开始处理流程"""
        self.info("=" * 60)
        self.info(f"🚀 开始执行: {process_name}")
        self.info("=" * 60)
    
    def end_process(self, process_name: str, success: bool = True):
        """结束处理流程"""
        status = "✅ 成功完成" if success else "❌ 执行失败"
        self.info(f"{status}: {process_name}")
        self.info("=" * 60)
    
    def log_stats(self, title: str, stats: dict):
        """记录统计信息"""
        self.info(f"📊 {title}")
        for key, value in stats.items():
            self.info(f"  {key}: {value}")


# 全局日志器实例
_global_logger = None


def setup_logger(name: str = "etf_filter") -> ETFFilterLogger:
    """
    设置并获取日志器
    
    Args:
        name: 日志器名称
    
    Returns:
        ETFFilterLogger实例
    """
    return ETFFilterLogger(name)


def get_logger() -> ETFFilterLogger:
    """获取全局日志器实例"""
    global _global_logger
    if _global_logger is None:
        _global_logger = setup_logger()
    return _global_logger


class ProcessTimer:
    """处理过程计时器"""
    
    def __init__(self, process_name: str, logger: ETFFilterLogger = None):
        self.process_name = process_name
        self.logger = logger or get_logger()
        self.start_time = None
    
    def __enter__(self):
        self.start_time = datetime.now()
        self.logger.start_process(self.process_name)
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        end_time = datetime.now()
        duration = end_time - self.start_time
        
        if exc_type is None:
            self.logger.info(f"⏱️  处理耗时: {duration}")
            self.logger.end_process(self.process_name, success=True)
        else:
            self.logger.error(f"❌ 处理异常: {exc_val}")
            self.logger.end_process(self.process_name, success=False)
        
        return False  # 不抑制异常 