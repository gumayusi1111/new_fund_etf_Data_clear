#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""显示格式化器"""

from typing import Dict, Any
from ..interfaces.output_interface import IDisplayFormatter


class DisplayFormatter(IDisplayFormatter):
    def format_summary(self, data: Dict[str, Any]) -> str:
        return f"汇总: {data}"
    
    def format_progress(self, current: int, total: int, message: str = "") -> str:
        return f"进度: {current}/{total} {message}"
    
    def format_table(self, data, headers=None) -> str:
        return "表格显示"
    
    def format_cache_statistics(self, stats: Dict[str, Any]) -> str:
        return f"缓存统计: {stats}"
    
    def display_single_etf_result(self, result: Dict[str, Any]) -> None:
        """显示单个ETF的计算结果"""
        if not result:
            return
        
        etf_code = result.get('etf_code', 'Unknown')
        
        print(f"\n📊 {etf_code} SMA计算结果:")
        print("=" * 50)
        
        # 显示基本信息
        latest_price = result.get('latest_price', {})
        if latest_price:
            print(f"📅 最新日期: {latest_price.get('date', 'N/A')}")
            print(f"💰 最新价格: {latest_price.get('close', 'N/A')}")
        
        # 显示SMA值
        sma_values = result.get('sma_values', {})
        if sma_values:
            print(f"\n📈 SMA指标:")
            for key, value in sma_values.items():
                if value is not None:
                    if 'SMA_' in key:
                        period = key.replace('SMA_', '')
                        print(f"   MA{period}: {value}")
                    elif 'DIFF' in key:
                        print(f"   {key}: {value}")
        
        # 显示数据源
        data_source = result.get('data_source', 'unknown')
        print(f"\n🔍 数据源: {data_source}")
        
        # 显示历史数据信息
        historical_data = result.get('historical_data')
        if historical_data is not None and hasattr(historical_data, '__len__'):
            print(f"📊 历史数据行数: {len(historical_data)}")
        
        print("=" * 50) 