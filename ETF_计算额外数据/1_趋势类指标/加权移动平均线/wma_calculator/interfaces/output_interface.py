#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
WMA输出接口定义模块 - 重构版
===========================

定义输出处理的接口
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Any


class WMAOutputInterface(ABC):
    """WMA输出接口 - 定义输出处理的标准接口"""
    
    @abstractmethod
    def save_results(self, results: List[Dict], output_dir: str) -> Dict[str, Any]:
        """
        保存结果到文件
        
        Args:
            results: 结果列表
            output_dir: 输出目录
            
        Returns:
            Dict[str, Any]: 保存结果统计
        """
        pass
    
    @abstractmethod
    def display_results(self, results: List[Dict]) -> None:
        """
        显示结果
        
        Args:
            results: 结果列表
        """
        pass 