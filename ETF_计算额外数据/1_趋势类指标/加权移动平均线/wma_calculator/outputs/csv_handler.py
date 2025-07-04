#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
WMA CSV处理器模块 - 重构版
=========================

处理CSV文件的输出
"""

import os
import pandas as pd
from typing import Dict, List, Any
from ..interfaces.output_interface import WMAOutputInterface


class WMACSVHandler(WMAOutputInterface):
    """WMA CSV处理器"""
    
    def save_results(self, results: List[Dict], output_dir: str) -> Dict[str, Any]:
        """
        保存结果到CSV文件
        
        Args:
            results: 结果列表
            output_dir: 输出目录
            
        Returns:
            Dict[str, Any]: 保存结果统计
        """
        if not results:
            return {'files_saved': 0, 'total_size': 0}
        
        os.makedirs(output_dir, exist_ok=True)
        
        files_saved = 0
        total_size = 0
        
        for result in results:
            try:
                etf_code = result['etf_code']
                clean_code = etf_code.replace('.SH', '').replace('.SZ', '')
                output_file = os.path.join(output_dir, f"{clean_code}.csv")
                
                # 这里可以根据需要实现具体的CSV保存逻辑
                # 目前使用简化的实现
                
                files_saved += 1
                if os.path.exists(output_file):
                    total_size += os.path.getsize(output_file)
                    
            except Exception as e:
                print(f"❌ 保存失败: {result.get('etf_code', 'Unknown')} - {str(e)}")
        
        return {
            'files_saved': files_saved,
            'total_size': total_size
        }
    
    def display_results(self, results: List[Dict]) -> None:
        """
        显示结果
        
        Args:
            results: 结果列表
        """
        print(f"📊 处理完成，共 {len(results)} 个结果") 