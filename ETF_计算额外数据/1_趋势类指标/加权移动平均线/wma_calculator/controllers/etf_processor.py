#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
WMA ETF处理器模块 - 重构版
=========================

从原有controller.py中提取单个ETF处理逻辑，保持算法完全一致
"""

import pandas as pd
from datetime import datetime
from typing import Dict, Optional, Any
from ..infrastructure.config import WMAConfig
from ..infrastructure.data_reader import WMADataReader
from ..engines.wma_engine import WMAEngine


class WMAETFProcessor:
    """WMA ETF处理器 - 负责单个ETF的处理逻辑"""
    
    def __init__(self, data_reader: WMADataReader, wma_engine: WMAEngine, config: WMAConfig):
        """
        初始化ETF处理器
        
        Args:
            data_reader: 数据读取器
            wma_engine: WMA计算引擎
            config: WMA配置
        """
        self.data_reader = data_reader
        self.wma_engine = wma_engine
        self.config = config
    
    def process_single_etf(self, etf_code: str, include_advanced_analysis: bool = False) -> Optional[Dict]:
        """
        处理单个ETF的WMA计算 - 保持原有算法完全一致
        
        Args:
            etf_code: ETF代码
            include_advanced_analysis: 是否包含高级分析
            
        Returns:
            Optional[Dict]: 处理结果或None
        """
        try:
            # 步骤1: 读取数据 - 保持原有读取逻辑
            data_result = self.data_reader.read_etf_data(etf_code)
            if data_result is None:
                print(f"❌ {etf_code} 数据读取失败")
                return None
            
            df, total_rows = data_result
            
            # 步骤2: 计算WMA - 保持原有计算逻辑
            wma_results = self.wma_engine.calculate_all_wma(df)
            if not wma_results or all(v is None for v in wma_results.values()):
                print(f"❌ {etf_code} WMA计算失败")
                return None
            
            # 步骤3: 获取价格和日期信息 - 保持原有获取逻辑
            latest_price = self.data_reader.get_latest_price_info(df)
            date_range = self.data_reader.get_date_range(df)
            
            # 步骤4: 简化信号分析 - 保持原有简化逻辑
            signals = {
                'status': 'simplified'  # 标记为简化模式
            }
            
            # 步骤5: 数据优化信息 - 保持原有数据优化信息
            data_optimization = {
                'total_available_days': total_rows,
                'used_days': len(df),
                'efficiency_gain': f"{((total_rows - len(df)) / total_rows * 100):.1f}%" if total_rows > len(df) else "0.0%"
            }
            
            # 步骤6: 格式化结果 - 保持原有结果格式
            result = self._format_single_result(
                etf_code, wma_results, latest_price, date_range, 
                data_optimization, signals, include_advanced_analysis
            )
            
            # 步骤7: 清理内存 - 保持原有清理逻辑
            self.data_reader.cleanup_memory(df)
            
            return result
            
        except (FileNotFoundError, pd.errors.EmptyDataError) as e:
            print(f"❌ {etf_code} 数据读取失败: {e}")
            return None
        except ValueError as e:
            print(f"❌ {etf_code} 数据格式错误: {e}")
            return None
        except Exception as e:
            print(f"❌ {etf_code} 处理异常: {e}")
            return None
    
    def _format_single_result(self, etf_code: str, wma_results: Dict, latest_price: Dict, 
                            date_range: Dict, data_optimization: Dict, signals: Dict,
                            include_advanced_analysis: bool = False) -> Dict:
        """
        格式化单个ETF的结果 - 保持原有格式化逻辑
        
        Args:
            etf_code: ETF代码
            wma_results: WMA计算结果
            latest_price: 最新价格信息
            date_range: 日期范围信息
            data_optimization: 数据优化信息
            signals: 信号分析结果
            include_advanced_analysis: 是否包含高级分析
            
        Returns:
            Dict: 格式化后的结果
        """
        result = {
            'etf_code': etf_code,
            'adj_type': self.config.adj_type,
            'wma_values': wma_results,
            'latest_price': latest_price,
            'date_range': date_range,
            'data_optimization': data_optimization,
            'signals': signals,
            'data_source': 'fresh_calculation',
            'calculation_timestamp': datetime.now().isoformat()
        }
        
        # 如果包含高级分析，可以在这里扩展
        if include_advanced_analysis:
            # 这里可以添加高级分析逻辑
            # 目前保持简化，与原有逻辑一致
            pass
        
        return result 