#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
EMA ETF处理器 - 重构版
====================

参照WMA/SMA系统的ETF处理架构
负责单个ETF的EMA计算和结果处理
"""

from typing import Dict, Optional
from ..infrastructure.config import EMAConfig
from ..infrastructure.data_reader import EMADataReader
from ..engines.ema_engine import EMAEngine


class EMAETFProcessor:
    """EMA ETF处理器 - 重构版（与WMA/SMA保持一致）"""
    
    def __init__(self, data_reader: EMADataReader, ema_engine: EMAEngine, config: EMAConfig):
        """
        初始化ETF处理器
        
        Args:
            data_reader: 数据读取器
            ema_engine: EMA计算引擎
            config: 配置对象
        """
        self.data_reader = data_reader
        self.ema_engine = ema_engine
        self.config = config
        
        if not config.performance_mode:
            print("🔄 EMA ETF处理器初始化完成")
    
    def process_single_etf(self, etf_code: str, include_advanced_analysis: bool = False) -> Optional[Dict]:
        """
        处理单个ETF的EMA计算
        
        Args:
            etf_code: ETF代码
            include_advanced_analysis: 是否包含高级分析
            
        Returns:
            Optional[Dict]: 处理结果或None
        """
        try:
            if not self.config.performance_mode:
                print(f"🔢 开始处理ETF: {etf_code}")
            
            # 1. 验证ETF代码
            if not self.data_reader.validate_etf_code(etf_code):
                if not self.config.performance_mode:
                    print(f"❌ ETF代码无效: {etf_code}")
                return None
            
            # 2. 读取数据
            data_result = self.data_reader.read_etf_data(etf_code)
            if not data_result:
                if not self.config.performance_mode:
                    print(f"❌ 数据读取失败: {etf_code}")
                return None
            
            df, total_rows = data_result
            
            # 3. 计算EMA值
            ema_values = self.ema_engine.calculate_ema_values(df)
            if not ema_values:
                if not self.config.performance_mode:
                    print(f"❌ EMA计算失败: {etf_code}")
                return None
            
            # 4. 获取价格信息
            price_info = self.data_reader.get_latest_price_info(df)
            
            # 5. 简化信号分析（移除复杂判断）
            signals = self.ema_engine.calculate_ema_signals(df, ema_values)
            
            # 6. 验证结果
            if not self._validate_result_data(etf_code, price_info, ema_values, signals):
                if not self.config.performance_mode:
                    print(f"❌ 结果验证失败: {etf_code}")
                return None
            
            # 7. 构建返回结果
            result = {
                'etf_code': etf_code,
                'success': True,
                'price_info': price_info,
                'ema_values': ema_values,
                'signals': signals,
                'total_rows': total_rows,
                'data_source': 'fresh_calculation'  # 标记数据来源
            }
            
            if not self.config.performance_mode:
                print(f"✅ {etf_code} EMA处理完成")
            
            return result
            
        except Exception as e:
            if not self.config.performance_mode:
                print(f"❌ {etf_code} 处理失败: {str(e)}")
            return {
                'etf_code': etf_code,
                'success': False,
                'error': str(e)
            }
    
    def _validate_result_data(self, etf_code: str, price_info: Dict, 
                            ema_values: Dict, signals: Dict) -> bool:
        """
        验证处理结果数据的完整性
        
        Args:
            etf_code: ETF代码
            price_info: 价格信息
            ema_values: EMA计算结果
            signals: 信号数据
            
        Returns:
            bool: 数据是否有效
        """
        try:
            # 检查价格信息
            if not price_info or 'latest_price' not in price_info:
                return False
            
            # 检查EMA值
            if not ema_values:
                return False
            
            # 检查必要的EMA指标
            for period in self.config.ema_periods:
                ema_key = f'ema_{period}'
                if ema_key not in ema_values:
                    return False
                
                # 检查EMA值是否合理
                ema_value = ema_values[ema_key]
                if ema_value <= 0:
                    return False
            
            # 检查信号数据
            if not signals or signals.get('status') == '计算错误':
                return False
            
            return True
            
        except Exception as e:
            if not self.config.performance_mode:
                print(f"❌ {etf_code} 结果验证异常: {str(e)}")
            return False
    
    def quick_analysis(self, etf_code: str) -> Optional[Dict]:
        """
        快速分析模式（简化版处理）
        
        Args:
            etf_code: ETF代码
            
        Returns:
            Optional[Dict]: 分析结果或None
        """
        try:
            if not self.config.performance_mode:
                print(f"⚡ 快速分析: {etf_code}")
            
            result = self.process_single_etf(etf_code, include_advanced_analysis=False)
            
            if result and result.get('success', False):
                # 添加快速分析标记
                result['analysis_mode'] = 'quick'
                return result
            else:
                return None
                
        except Exception as e:
            if not self.config.performance_mode:
                print(f"❌ {etf_code} 快速分析失败: {str(e)}")
            return None