#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MACD文件管理器 - 重构版
======================

处理MACD系统的文件操作
"""

import os
import pandas as pd
from typing import Dict, Optional, Any
from .config import MACDConfig


class MACDFileManager:
    """MACD文件管理器 - 重构版"""
    
    def __init__(self, config: MACDConfig):
        """
        初始化文件管理器
        
        Args:
            config: MACD配置对象
        """
        self.config = config
        self.output_dir = config.get_output_base_dir()
        
        print("📁 MACD文件管理器初始化完成")
        print(f"   📂 输出目录: {self.output_dir}")
    
    def ensure_output_dir(self, sub_dir: str = "") -> str:
        """
        确保输出目录存在
        
        Args:
            sub_dir: 子目录名称
            
        Returns:
            完整的输出目录路径
        """
        full_path = os.path.join(self.output_dir, sub_dir) if sub_dir else self.output_dir
        os.makedirs(full_path, exist_ok=True)
        return full_path
    
    def save_result(self, df: pd.DataFrame, etf_code: str, sub_dir: str = "") -> str:
        """
        保存MACD计算结果
        
        Args:
            df: MACD结果DataFrame
            etf_code: ETF代码
            sub_dir: 子目录
            
        Returns:
            保存的文件路径
        """
        try:
            output_path = self.ensure_output_dir(sub_dir)
            clean_code = etf_code.replace('.SH', '').replace('.SZ', '')
            file_path = os.path.join(output_path, f"{clean_code}.csv")
            
            df.to_csv(file_path, index=False, encoding='utf-8')
            return file_path
            
        except Exception as e:
            print(f"❌ 保存文件失败: {str(e)}")
            return ""
    
    def file_exists(self, etf_code: str, sub_dir: str = "") -> bool:
        """
        检查文件是否存在
        
        Args:
            etf_code: ETF代码
            sub_dir: 子目录
            
        Returns:
            文件是否存在
        """
        clean_code = etf_code.replace('.SH', '').replace('.SZ', '')
        file_path = os.path.join(self.output_dir, sub_dir, f"{clean_code}.csv")
        return os.path.exists(file_path)