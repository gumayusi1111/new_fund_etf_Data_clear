#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
EMA CSV处理器 - 重构版
===================

参照WMA/SMA系统的CSV处理架构
提供CSV格式化和输出功能
"""

from typing import Dict, List
from ..infrastructure.config import EMAConfig


class EMACSVHandler:
    """EMA CSV处理器 - 重构版（与WMA/SMA保持一致）"""
    
    def __init__(self, config: EMAConfig):
        """
        初始化CSV处理器
        
        Args:
            config: EMA配置对象
        """
        self.config = config
        
        if not config.performance_mode:
            print("📄 EMA CSV处理器初始化完成")
    
    def generate_csv_header(self) -> str:
        """
        生成CSV文件头
        
        Returns:
            str: CSV头部行
        """
        headers = [
            'code', 'date'
        ]
        
        # 添加EMA列
        for period in sorted(self.config.ema_periods):
            headers.append(f'EMA_{period}')
        
        # 添加EMA差值列
        if 12 in self.config.ema_periods and 26 in self.config.ema_periods:
            headers.extend(['EMA_DIFF_12_26', 'EMA_DIFF_12_26_PCT', 'EMA12_MOMENTUM'])
        
        return ','.join(headers)
    
    def format_result_row(self, etf_code: str, price_info: Dict, 
                         ema_values: Dict, signals: Dict) -> str:
        """
        格式化单个结果为CSV行
        
        Args:
            etf_code: ETF代码
            price_info: 价格信息
            ema_values: EMA计算结果
            signals: 信号数据
            
        Returns:
            str: CSV数据行
        """
        try:
            row_data = []
            
            # 基础数据
            clean_code = etf_code.replace('.SH', '').replace('.SZ', '')
            row_data.append(clean_code)
            
            # 格式化日期为ISO标准格式
            date_str = str(price_info.get('latest_date', ''))
            if len(date_str) == 8 and date_str.isdigit():
                formatted_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
            else:
                formatted_date = date_str
            row_data.append(formatted_date)
            
            # EMA值
            for period in sorted(self.config.ema_periods):
                ema_key = f'ema_{period}'
                ema_value = ema_values.get(ema_key, 0)
                row_data.append(f"{ema_value:.8f}")
            
            # EMA差值和动量
            if 12 in self.config.ema_periods and 26 in self.config.ema_periods:
                diff_value = ema_values.get('ema_diff_12_26', 0)
                diff_pct = ema_values.get('ema_diff_12_26_pct', 0)
                momentum = ema_values.get('ema12_momentum', 0)
                row_data.append(f"{diff_value:.8f}")
                row_data.append(f"{diff_pct:.8f}")
                row_data.append(f"{momentum:.8f}")
            
            return ','.join(row_data)
            
        except Exception as e:
            return f"# 错误: {str(e)}"
    
    def generate_batch_csv(self, results: List[Dict]) -> str:
        """
        生成批量结果的完整CSV内容
        
        Args:
            results: 处理结果列表
            
        Returns:
            str: 完整的CSV内容
        """
        try:
            lines = []
            
            # 添加头部
            lines.append(self.generate_csv_header())
            
            # 添加数据行
            for result in results:
                if result.get('success', False):
                    row = self.format_result_row(
                        result['etf_code'],
                        result.get('price_info', {}),
                        result.get('ema_values', {}),
                        result.get('signals', {})
                    )
                    lines.append(row)
            
            return '\n'.join(lines)
            
        except Exception as e:
            return f"# CSV生成失败: {str(e)}"