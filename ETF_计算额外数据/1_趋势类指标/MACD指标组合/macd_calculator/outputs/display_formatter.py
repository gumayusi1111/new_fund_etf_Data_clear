#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MACD显示格式化器 - 重构版
========================

处理MACD结果的显示格式化
"""

import pandas as pd
from typing import Dict, List, Optional, Any
from datetime import datetime


class MACDDisplayFormatter:
    """MACD显示格式化器 - 重构版"""
    
    def __init__(self, config=None):
        """
        初始化显示格式化器
        
        Args:
            config: MACD配置对象（可选）
        """
        self.config = config
    
    def format_result_summary(self, results: List[Dict]) -> str:
        """
        格式化结果摘要
        
        Args:
            results: 计算结果列表
            
        Returns:
            格式化的摘要字符串
        """
        if not results:
            return "📊 MACD计算结果: 无数据"
        
        success_count = sum(1 for r in results if r.get('success', False))
        total_count = len(results)
        
        lines = []
        lines.append("📊 MACD计算结果摘要")
        lines.append("=" * 50)
        lines.append(f"✅ 成功: {success_count}/{total_count}")
        lines.append("")
        
        # 显示详细结果
        for result in results[:10]:  # 只显示前10个
            if result.get('success', False):
                etf_code = result['etf_code']
                data_points = result.get('data_points', 0)
                lines.append(f"  ✅ {etf_code}: {data_points} 个数据点")
            else:
                etf_code = result.get('etf_code', 'Unknown')
                error = result.get('error', 'Unknown')
                lines.append(f"  ❌ {etf_code}: {error}")
        
        if len(results) > 10:
            lines.append(f"  ... 还有 {len(results) - 10} 个结果")
        
        return '\n'.join(lines)
    
    def format_single_result(self, result_df: pd.DataFrame, etf_code: str) -> str:
        """
        格式化单个ETF的MACD结果
        
        Args:
            result_df: MACD结果DataFrame
            etf_code: ETF代码
            
        Returns:
            格式化的结果字符串
        """
        if result_df.empty:
            return f"❌ {etf_code}: 无MACD数据"
        
        latest = result_df.iloc[-1]
        lines = []
        lines.append(f"📊 {etf_code} MACD指标")
        lines.append("=" * 40)
        lines.append(f"📅 最新日期: {latest['日期'].strftime('%Y-%m-%d') if pd.notna(latest['日期']) else 'N/A'}")
        lines.append(f"📈 DIF: {latest['DIF']:.6f}")
        lines.append(f"📉 DEA: {latest['DEA']:.6f}")
        lines.append(f"📊 MACD: {latest['MACD']:.6f}")
        lines.append(f"📋 数据点数: {len(result_df)}")
        
        # 统计信息
        lines.append("")
        lines.append("📊 统计信息:")
        lines.append(f"  DIF均值: {result_df['DIF'].mean():.6f}")
        lines.append(f"  DEA均值: {result_df['DEA'].mean():.6f}")
        lines.append(f"  MACD均值: {result_df['MACD'].mean():.6f}")
        
        return '\n'.join(lines)
    
    def format_system_status(self, status_info: Dict) -> str:
        """
        格式化系统状态信息
        
        Args:
            status_info: 系统状态信息字典
            
        Returns:
            格式化的状态字符串
        """
        lines = []
        lines.append("🔧 MACD系统状态")
        lines.append("=" * 40)
        
        if 'version' in status_info:
            lines.append(f"📊 版本: {status_info['version']}")
        
        if 'config' in status_info:
            lines.append(f"🔧 配置: {status_info['config']}")
        
        if 'data_path' in status_info:
            lines.append(f"📁 数据路径: {status_info['data_path']}")
        
        if 'available_etfs' in status_info:
            lines.append(f"📋 可用ETF: {status_info['available_etfs']} 个")
        
        if 'components' in status_info:
            lines.append("🔧 组件状态:")
            for component, status in status_info['components'].items():
                status_emoji = "✅" if status else "❌"
                lines.append(f"   {component}: {status_emoji}")
        
        return '\n'.join(lines)
    
    def format_calculation_progress(self, current: int, total: int, etf_code: str = "") -> str:
        """
        格式化计算进度
        
        Args:
            current: 当前进度
            total: 总数
            etf_code: 当前处理的ETF代码
            
        Returns:
            格式化的进度字符串
        """
        percentage = (current / total * 100) if total > 0 else 0
        progress_bar = "█" * int(percentage // 5) + "░" * (20 - int(percentage // 5))
        
        status_line = f"📊 MACD计算进度: [{progress_bar}] {percentage:.1f}% ({current}/{total})"
        if etf_code:
            status_line += f" - 当前: {etf_code}"
        
        return status_line