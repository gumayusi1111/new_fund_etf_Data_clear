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
            'ETF代码', '日期', '收盘价', '涨跌', '涨跌幅%', '成交量'
        ]
        
        # 添加EMA列
        for period in sorted(self.config.ema_periods):
            headers.append(f'EMA{period}')
        
        # 添加EMA差值列
        if 12 in self.config.ema_periods and 26 in self.config.ema_periods:
            headers.extend(['EMA差值12-26', 'EMA差值百分比%'])
        
        # 添加EMA动量列
        if 12 in self.config.ema_periods:
            headers.append('EMA12动量')
        
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
            row_data.append(str(price_info.get('latest_date', '')))
            row_data.append(f"{price_info.get('latest_price', 0):.3f}")
            row_data.append(f"{price_info.get('price_change', 0):+.3f}")
            row_data.append(f"{price_info.get('price_change_pct', 0):+.2f}")
            row_data.append(str(price_info.get('volume', 0)))
            
            # EMA值
            for period in sorted(self.config.ema_periods):
                ema_key = f'ema_{period}'
                ema_value = ema_values.get(ema_key, 0)
                row_data.append(f"{ema_value:.6f}")
            
            # EMA差值
            if 12 in self.config.ema_periods and 26 in self.config.ema_periods:
                diff_value = ema_values.get('ema_diff_12_26', 0)
                diff_pct = ema_values.get('ema_diff_12_26_pct', 0)
                row_data.append(f"{diff_value:.6f}")
                row_data.append(f"{diff_pct:.3f}")
            
            # EMA动量
            if 12 in self.config.ema_periods:
                momentum = ema_values.get('ema12_momentum', 0)
                row_data.append(f"{momentum:.6f}")
            
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