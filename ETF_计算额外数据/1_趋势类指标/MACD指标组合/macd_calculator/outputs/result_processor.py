#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MACD结果处理器 - 重构版
======================

处理MACD计算结果的输出和格式化
"""

import pandas as pd
import os
from typing import Dict, List, Optional, Any
from ..interfaces.output_interface import MACDOutputInterface


class MACDResultProcessor(MACDOutputInterface):
    """MACD结果处理器 - 重构版"""
    
    def __init__(self, config=None):
        """
        初始化结果处理器
        
        Args:
            config: MACD配置对象（可选）
        """
        self.config = config
    
    def save_results(self, results: List[Dict], output_dir: str) -> Dict[str, Any]:
        """
        保存结果到文件
        
        Args:
            results: 计算结果列表
            output_dir: 输出目录
            
        Returns:
            保存操作的结果信息
        """
        try:
            os.makedirs(output_dir, exist_ok=True)
            saved_files = []
            
            for result in results:
                if 'result_df' in result and not result['result_df'].empty:
                    etf_code = result['etf_code'].replace('.SH', '').replace('.SZ', '')
                    filename = f"{etf_code}.csv"
                    file_path = os.path.join(output_dir, filename)
                    
                    result['result_df'].to_csv(file_path, index=False, encoding='utf-8')
                    saved_files.append(file_path)
            
            return {
                'success': True,
                'saved_files': saved_files,
                'total_files': len(saved_files)
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'saved_files': [],
                'total_files': 0
            }
    
    def format_display(self, results: List[Dict]) -> str:
        """
        格式化显示结果
        
        Args:
            results: 计算结果列表
            
        Returns:
            格式化后的显示字符串
        """
        if not results:
            return "无结果数据"
        
        display_lines = []
        display_lines.append("MACD计算结果摘要")
        display_lines.append("=" * 40)
        
        for result in results:
            if result.get('success', False):
                etf_code = result['etf_code']
                data_points = result.get('data_points', 0)
                display_lines.append(f"✅ {etf_code}: {data_points} 个数据点")
            else:
                etf_code = result.get('etf_code', 'Unknown')
                error = result.get('error', 'Unknown error')
                display_lines.append(f"❌ {etf_code}: {error}")
        
        return '\n'.join(display_lines)
    
    def validate_results(self, results: List[Dict]) -> bool:
        """
        验证结果有效性
        
        Args:
            results: 计算结果列表
            
        Returns:
            验证结果
        """
        if not results:
            return False
        
        for result in results:
            if not result.get('success', False):
                continue
                
            if 'result_df' not in result:
                return False
                
            df = result['result_df']
            if df.empty:
                return False
                
            # 检查必要的MACD列
            required_columns = ['DIF', 'DEA', 'MACD']
            if not all(col in df.columns for col in required_columns):
                return False
        
        return True
    
    def process_single_result(self, result_df: pd.DataFrame, etf_code: str) -> Dict[str, Any]:
        """
        处理单个ETF的MACD结果
        
        Args:
            result_df: MACD计算结果DataFrame
            etf_code: ETF代码
            
        Returns:
            处理后的结果字典
        """
        try:
            if result_df.empty:
                return {'success': False, 'error': 'Empty result'}
            
            # 基础统计
            latest = result_df.iloc[-1]
            summary = {
                'etf_code': etf_code,
                'total_records': len(result_df),
                'latest_values': {
                    'DIF': float(latest['DIF']),
                    'DEA': float(latest['DEA']),
                    'MACD': float(latest['MACD'])
                },
                'statistics': {
                    'DIF_mean': float(result_df['DIF'].mean()),
                    'DEA_mean': float(result_df['DEA'].mean()),
                    'MACD_mean': float(result_df['MACD'].mean())
                }
            }
            
            return {'success': True, 'summary': summary}
            
        except Exception as e:
            return {'success': False, 'error': str(e)}