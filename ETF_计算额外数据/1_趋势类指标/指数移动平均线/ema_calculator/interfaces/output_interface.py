#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
EMA输出接口定义模块 - 重构版
===========================

定义输出处理的接口
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Any


class EMAOutputInterface(ABC):
    """EMA输出接口 - 定义输出处理的标准接口"""
    
    @abstractmethod
    def save_results(self, results: List[Dict], output_dir: str) -> Dict[str, Any]:
        """
        保存结果到文件
        
        Args:
            results: 计算结果列表
            output_dir: 输出目录
            
        Returns:
            保存操作的结果信息
        """
        pass
    
    @abstractmethod
    def format_display(self, results: List[Dict]) -> str:
        """
        格式化显示结果
        
        Args:
            results: 计算结果列表
            
        Returns:
            格式化后的显示字符串
        """
        pass
    
    @abstractmethod
    def validate_results(self, results: List[Dict]) -> bool:
        """
        验证结果有效性
        
        Args:
            results: 计算结果列表
            
        Returns:
            验证结果
        """
        pass