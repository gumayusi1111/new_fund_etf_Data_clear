#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
WMA文件管理器模块 - 重构版
=========================

基础的文件管理功能
"""

import os
from typing import Optional


class WMAFileManager:
    """WMA文件管理器 - 基础版本"""
    
    def __init__(self, output_dir: str):
        """
        初始化文件管理器
        
        Args:
            output_dir: 输出目录
        """
        self.output_dir = output_dir
    
    def create_output_directory(self, output_dir: str) -> str:
        """
        创建输出目录
        
        Args:
            output_dir: 输出目录路径
            
        Returns:
            str: 创建的目录路径
        """
        os.makedirs(output_dir, exist_ok=True)
        return output_dir
    
    def show_output_summary(self, output_dir: str) -> None:
        """
        显示输出摘要
        
        Args:
            output_dir: 输出目录
        """
        if os.path.exists(output_dir):
            files = [f for f in os.listdir(output_dir) if f.endswith('.csv')]
            print(f"📁 输出目录: {output_dir}")
            print(f"📄 生成文件: {len(files)} 个CSV文件")
        else:
            print(f"📁 输出目录不存在: {output_dir}") 