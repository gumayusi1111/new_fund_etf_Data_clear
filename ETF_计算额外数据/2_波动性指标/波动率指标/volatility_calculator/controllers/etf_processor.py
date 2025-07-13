#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
波动率ETF处理器
=============

处理单个ETF的波动率计算，集成缓存和增量更新功能
"""

import os
from datetime import datetime
from typing import Dict, Optional, Any
from ..infrastructure.config import VolatilityConfig
from ..infrastructure.data_reader import VolatilityDataReader
from ..engines.volatility_engine import VolatilityEngine


class VolatilityETFProcessor:
    """波动率ETF处理器"""
    
    def __init__(self, data_reader: VolatilityDataReader, volatility_engine: VolatilityEngine, 
                 config: VolatilityConfig):
        """
        初始化ETF处理器
        
        Args:
            data_reader: 数据读取器
            volatility_engine: 波动率计算引擎
            config: 配置对象
        """
        self.data_reader = data_reader
        self.volatility_engine = volatility_engine
        self.config = config
        
        if not config.performance_mode:
            print("📊 波动率ETF处理器初始化完成")
    
    def process_single_etf(self, etf_code: str, include_advanced_analysis: bool = False) -> Optional[Dict]:
        """
        处理单个ETF的波动率计算
        
        Args:
            etf_code: ETF代码
            include_advanced_analysis: 是否包含高级分析
            
        Returns:
            Dict: 处理结果或None
        """
        try:
            if not self.config.performance_mode:
                print(f"📊 开始处理ETF: {etf_code}")
            
            # 读取ETF数据
            data_result = self.data_reader.read_etf_data(etf_code)
            if data_result is None:
                if not self.config.performance_mode:
                    print(f"❌ {etf_code}: 数据读取失败")
                return None
            
            df, metadata = data_result
            
            if df.empty:
                if not self.config.performance_mode:
                    print(f"❌ {etf_code}: 数据为空")
                return None
            
            # 计算波动率指标
            volatility_values = self.volatility_engine.calculate_volatility_indicators(df)
            
            if not volatility_values:
                if not self.config.performance_mode:
                    print(f"❌ {etf_code}: 波动率计算失败")
                return None
            
            # 构建结果
            result = {
                'etf_code': etf_code,
                'volatility_values': volatility_values,
                'metadata': metadata,
                'calculation_date': datetime.now().isoformat(),
                'data_source': 'fresh_calculation',
                'config': {
                    'adj_type': self.config.adj_type,
                    'volatility_periods': self.config.volatility_periods,
                    'annualized': self.config.annualized
                }
            }
            
            # 高级分析
            if include_advanced_analysis:
                advanced_analysis = self._perform_advanced_analysis(df, volatility_values)
                result['advanced_analysis'] = advanced_analysis
            
            if not self.config.performance_mode:
                print(f"✅ {etf_code}: 处理完成")
            
            return result
            
        except Exception as e:
            if not self.config.performance_mode:
                print(f"❌ {etf_code}: 处理异常: {str(e)}")
            return None
    
    def _perform_advanced_analysis(self, df, volatility_values: Dict) -> Dict[str, Any]:
        """
        执行高级分析
        
        Args:
            df: 价格数据
            volatility_values: 波动率值
            
        Returns:
            Dict: 高级分析结果
        """
        try:
            analysis = {}
            
            # 数据质量分析
            analysis['data_quality'] = {
                'total_days': len(df),
                'data_completeness': (1 - df.isnull().sum().sum() / (len(df) * len(df.columns))) * 100,
                'date_range_days': (df['日期'].max() - df['日期'].min()).days if '日期' in df.columns else 0
            }
            
            # 波动率趋势分析
            if len(df) >= 60:  # 至少60天数据
                recent_30_days = df.head(30)['收盘价']
                earlier_30_days = df.iloc[30:60]['收盘价']
                
                if len(recent_30_days) >= 10 and len(earlier_30_days) >= 10:
                    recent_vol = self.volatility_engine.calculate_historical_volatility(recent_30_days, 10).mean()
                    earlier_vol = self.volatility_engine.calculate_historical_volatility(earlier_30_days, 10).mean()
                    
                    if not (pd.isna(recent_vol) or pd.isna(earlier_vol)):
                        vol_change = (recent_vol - earlier_vol) / earlier_vol * 100 if earlier_vol != 0 else 0
                        
                        analysis['volatility_trend'] = {
                            'recent_30d_avg_vol': float(recent_vol) if not pd.isna(recent_vol) else None,
                            'earlier_30d_avg_vol': float(earlier_vol) if not pd.isna(earlier_vol) else None,
                            'volatility_change_pct': float(vol_change) if not pd.isna(vol_change) else None,
                            'trend_direction': 'increasing' if vol_change > 5 else 'decreasing' if vol_change < -5 else 'stable'
                        }
            
            # 价格振幅分析
            price_range = volatility_values.get('Price_Range')
            if price_range is not None:
                if price_range > 5.0:
                    range_level = '高振幅'
                elif price_range > 2.0:
                    range_level = '中等振幅'
                else:
                    range_level = '低振幅'
                
                analysis['price_range_analysis'] = {
                    'current_range_pct': price_range,
                    'range_level': range_level
                }
            
            # 波动率特征分析
            vol_10 = volatility_values.get('Volatility_10')
            vol_20 = volatility_values.get('Volatility_20')
            
            if vol_10 is not None and vol_20 is not None:
                analysis['volatility_characteristics'] = {
                    'short_term_vol': vol_10,
                    'medium_term_vol': vol_20,
                    'vol_consistency': abs(vol_10 - vol_20) / vol_20 * 100 if vol_20 != 0 else 0
                }
            
            return analysis
            
        except Exception as e:
            return {'error': f'高级分析异常: {str(e)}'}
    
    def validate_etf_data(self, etf_code: str) -> Dict[str, Any]:
        """
        验证ETF数据的有效性
        
        Args:
            etf_code: ETF代码
            
        Returns:
            Dict: 验证结果
        """
        try:
            validation_result = {
                'etf_code': etf_code,
                'is_valid': False,
                'issues': [],
                'recommendations': []
            }
            
            # 检查文件存在性
            file_path = self.data_reader.get_etf_file_path(etf_code)
            if not file_path:
                validation_result['issues'].append('文件不存在')
                validation_result['recommendations'].append('检查ETF代码是否正确')
                return validation_result
            
            # 读取并验证数据
            data_result = self.data_reader.read_etf_data(etf_code)
            if data_result is None:
                validation_result['issues'].append('数据读取失败')
                validation_result['recommendations'].append('检查文件格式和编码')
                return validation_result
            
            df, metadata = data_result
            
            # 数据量检查
            if len(df) < self.config.min_data_points:
                validation_result['issues'].append(f'数据量不足({len(df)} < {self.config.min_data_points})')
                validation_result['recommendations'].append('需要更多历史数据')
            
            # 数据质量检查
            missing_ratio = df.isnull().sum().sum() / (len(df) * len(df.columns))
            if missing_ratio > 0.1:  # 超过10%缺失
                validation_result['issues'].append(f'数据缺失率过高({missing_ratio:.1%})')
                validation_result['recommendations'].append('检查数据源质量')
            
            # 价格合理性检查
            if '收盘价' in df.columns:
                if (df['收盘价'] <= 0).any():
                    validation_result['issues'].append('存在非正价格数据')
                    validation_result['recommendations'].append('清理异常价格数据')
            
            # 日期连续性检查（简化）
            if '日期' in df.columns and len(df) > 1:
                date_gaps = df['日期'].diff().dt.days.dropna()
                large_gaps = (date_gaps > 7).sum()  # 超过7天的间隔
                
                if large_gaps > len(df) * 0.1:  # 超过10%的大间隔
                    validation_result['issues'].append('存在较多日期间隔')
                    validation_result['recommendations'].append('检查数据连续性')
            
            # 综合评估
            validation_result['is_valid'] = len(validation_result['issues']) == 0
            validation_result['data_summary'] = {
                'total_rows': len(df),
                'date_range': {
                    'start': metadata.get('date_range', {}).get('start'),
                    'end': metadata.get('date_range', {}).get('end')
                },
                'missing_ratio': missing_ratio
            }
            
            return validation_result
            
        except Exception as e:
            return {
                'etf_code': etf_code,
                'is_valid': False,
                'error': f'验证过程异常: {str(e)}'
            }