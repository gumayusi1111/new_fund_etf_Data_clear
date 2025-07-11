#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MACD批量处理器 - 重构版
======================

处理批量ETF的MACD计算
"""

from typing import Dict, List, Optional, Any
from ..infrastructure.config import MACDConfig
from .etf_processor import MACDETFProcessor


class MACDBatchProcessor:
    """MACD批量处理器 - 重构版"""
    
    def __init__(self, config: MACDConfig, etf_processor: MACDETFProcessor):
        """
        初始化批量处理器
        
        Args:
            config: MACD配置对象
            etf_processor: ETF处理器
        """
        self.config = config
        self.etf_processor = etf_processor
        
        print("📊 MACD批量处理器初始化完成")
        print(f"   🗂️ 缓存状态: {'启用' if config.enable_cache else '禁用'}")
    
    def process_etf_list(self, etf_codes: List[str], max_etfs: Optional[int] = None, 
                        verbose: bool = False) -> Dict[str, Any]:
        """
        批量处理ETF列表
        
        Args:
            etf_codes: ETF代码列表
            max_etfs: 最大处理数量限制
            verbose: 是否显示详细信息
            
        Returns:
            批量处理结果
        """
        try:
            # 限制处理数量
            if max_etfs:
                etf_codes = etf_codes[:max_etfs]
            
            results = []
            success_count = 0
            
            for i, etf_code in enumerate(etf_codes, 1):
                if verbose:
                    print(f"📊 处理 {etf_code} ({i}/{len(etf_codes)})...")
                
                result = self.etf_processor.process_etf(etf_code, save_result=True)
                results.append(result)
                
                if result.get('success', False):
                    success_count += 1
                    if verbose:
                        print(f"✅ {etf_code} 处理完成")
                else:
                    if verbose:
                        print(f"❌ {etf_code} 处理失败: {result.get('error', 'Unknown')}")
            
            return {
                'success': True,
                'processed_count': len(etf_codes),
                'success_count': success_count,
                'results': results
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'processed_count': 0,
                'success_count': 0,
                'results': []
            }