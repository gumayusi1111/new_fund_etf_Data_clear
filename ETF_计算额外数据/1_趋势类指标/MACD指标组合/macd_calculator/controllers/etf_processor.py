#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MACD ETF处理器 - 重构版
======================

处理单个ETF的MACD计算逻辑
"""

from typing import Dict, Optional, Any
from ..infrastructure.config import MACDConfig
from ..infrastructure.data_reader import MACDDataReader
from ..engines.macd_engine import MACDEngine
from ..outputs.result_processor import MACDResultProcessor


class MACDETFProcessor:
    """MACD ETF处理器 - 重构版"""
    
    def __init__(self, config: MACDConfig, data_reader: MACDDataReader, 
                 macd_engine: MACDEngine, result_processor: MACDResultProcessor):
        """
        初始化ETF处理器
        
        Args:
            config: MACD配置对象
            data_reader: 数据读取器
            macd_engine: MACD计算引擎
            result_processor: 结果处理器
        """
        self.config = config
        self.data_reader = data_reader
        self.macd_engine = macd_engine
        self.result_processor = result_processor
        
        print("🔄 MACD ETF处理器初始化完成")
    
    def process_etf(self, etf_code: str, save_result: bool = True) -> Dict[str, Any]:
        """
        处理单个ETF的MACD计算
        
        Args:
            etf_code: ETF代码
            save_result: 是否保存结果
            
        Returns:
            处理结果字典
        """
        try:
            # 读取数据
            df = self.data_reader.read_etf_data(etf_code)
            if df is None:
                return {'success': False, 'error': 'Failed to read data', 'etf_code': etf_code}
            
            # 验证数据
            if not self.macd_engine.validate_calculation_requirements(df):
                return {'success': False, 'error': 'Data validation failed', 'etf_code': etf_code}
            
            # 计算MACD
            result_df = self.macd_engine.calculate_macd_for_etf(df)
            if result_df.empty:
                return {'success': False, 'error': 'MACD calculation failed', 'etf_code': etf_code}
            
            # 处理结果
            process_result = self.result_processor.process_single_result(result_df, etf_code)
            
            return {
                'success': True,
                'etf_code': etf_code,
                'data_points': len(result_df),
                'result_df': result_df,
                'summary': process_result.get('summary', {})
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'etf_code': etf_code
            }