#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
WMA显示格式化器模块 - 重构版
===========================

格式化显示输出结果
"""

from typing import Dict, List


class WMADisplayFormatter:
    """WMA显示格式化器"""
    
    def display_single_etf_result(self, result: Dict) -> None:
        """
        显示单个ETF的结果
        
        Args:
            result: ETF处理结果
        """
        etf_code = result.get('etf_code', 'Unknown')
        wma_values = result.get('wma_values', {})
        latest_price = result.get('latest_price', {})
        
        print(f"\n📊 {etf_code} WMA计算结果:")
        print("=" * 50)
        print(f"📅 最新日期: {latest_price.get('date', '')}")
        print(f"💰 最新价格: {latest_price.get('close', 0):.3f}")
        
        print(f"\n📈 WMA指标:")
        for key, value in wma_values.items():
            if value is not None:
                print(f"   {key}: {value:.6f}")
        
        print(f"\n🔍 数据源: {result.get('data_source', 'unknown')}")
        print("=" * 50)
    
    def display_batch_results(self, results: List[Dict]) -> None:
        """
        显示批量结果摘要
        
        Args:
            results: 结果列表
        """
        if not results:
            print("📊 没有结果可显示")
            return
        
        print(f"\n📊 批量处理结果摘要")
        print("=" * 60)
        print(f"📁 处理ETF数量: {len(results)}")
        
        # 统计数据源
        cache_hits = len([r for r in results if r.get('data_source') == 'cache_hit'])
        fresh_calcs = len([r for r in results if r.get('data_source') == 'fresh_calculation'])
        
        if cache_hits > 0 or fresh_calcs > 0:
            print(f"🗂️ 缓存命中: {cache_hits}")
            print(f"🔄 新计算: {fresh_calcs}")
        
        print("=" * 60) 