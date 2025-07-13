#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
波动率结果处理器
=============

处理波动率计算结果的显示、保存和统计分析
"""

import os
import pandas as pd
import numpy as np
from datetime import datetime
from typing import Dict, List, Optional, Any
from ..infrastructure.config import VolatilityConfig


class VolatilityResultProcessor:
    """波动率结果处理器"""
    
    def __init__(self, config: VolatilityConfig):
        """
        初始化结果处理器
        
        Args:
            config: 波动率配置对象
        """
        self.config = config
        
        if not config.performance_mode:
            print("📊 波动率结果处理器初始化完成")
    
    def display_results(self, results: List[Dict]) -> None:
        """
        显示计算结果
        
        Args:
            results: 结果列表
        """
        if not results:
            print("❌ 无可显示的结果")
            return
        
        print(f"\n📊 波动率指标结果 (共{len(results)}个ETF)")
        print("=" * 80)
        
        for i, result in enumerate(results, 1):
            etf_code = result.get('etf_code', 'Unknown')
            volatility_values = result.get('volatility_values', {})
            
            print(f"\n{i}. {etf_code}")
            print("-" * 40)
            
            # 显示价格振幅
            price_range = volatility_values.get('Price_Range')
            if price_range is not None:
                print(f"   💥 价格振幅: {price_range:.4f}%")
            
            # 显示历史波动率
            for period in self.config.volatility_periods:
                vol_key = f'Volatility_{period}'
                vol_value = volatility_values.get(vol_key)
                if vol_value is not None:
                    unit = "(年化)" if self.config.annualized else "(日)"
                    print(f"   📈 {vol_key}: {vol_value:.6f} {unit}")
            
            # 显示滚动波动率
            for period in [10, 30]:
                rolling_key = f'Rolling_Vol_{period}'
                rolling_value = volatility_values.get(rolling_key)
                if rolling_value is not None:
                    unit = "(年化)" if self.config.annualized else "(日)"
                    print(f"   🔄 {rolling_key}: {rolling_value:.6f} {unit}")
            
            # 显示波动率状态
            vol_state = volatility_values.get('Vol_State')
            if vol_state:
                print(f"   🎯 波动率状态: {vol_state}")
            
            vol_level = volatility_values.get('Vol_Level')
            if vol_level:
                print(f"   📊 波动率水平: {vol_level}")
            
            # 显示数据来源
            data_source = result.get('data_source', 'unknown')
            print(f"   💾 数据来源: {data_source}")
    
    def save_results_to_csv(self, results: List[Dict], output_file: str) -> bool:
        """
        保存结果到CSV文件
        
        Args:
            results: 结果列表
            output_file: 输出文件路径
            
        Returns:
            bool: 是否保存成功
        """
        try:
            if not results:
                print("❌ 无可保存的结果")
                return False
            
            # 准备数据
            rows = []
            for result in results:
                etf_code = result.get('etf_code', '')
                volatility_values = result.get('volatility_values', {})
                
                row = {
                    'ETF_Code': etf_code,
                    'Calculation_Date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'Data_Source': result.get('data_source', ''),
                    'Config_Type': self.config.adj_type
                }
                
                # 添加波动率指标
                row.update(volatility_values)
                rows.append(row)
            
            # 创建DataFrame并保存
            df = pd.DataFrame(rows)
            
            # 确保输出目录存在
            os.makedirs(os.path.dirname(output_file), exist_ok=True)
            
            df.to_csv(output_file, index=False, encoding='utf-8')
            
            print(f"✅ 结果已保存: {output_file}")
            print(f"   📊 记录数: {len(df)}")
            print(f"   📁 文件大小: {os.path.getsize(output_file)/1024:.1f}KB")
            
            return True
            
        except Exception as e:
            print(f"❌ 保存结果异常: {str(e)}")
            return False
    
    def save_screening_batch_results(self, all_results: Dict[str, List[Dict]], 
                                   output_dir: str) -> Dict[str, Any]:
        """
        保存筛选批量结果到data目录
        
        Args:
            all_results: 所有门槛的结果
            output_dir: 输出目录
            
        Returns:
            Dict: 保存统计信息
        """
        try:
            save_stats = {
                'saved_files': [],
                'total_records': 0,
                'total_size_kb': 0
            }
            
            for threshold, results in all_results.items():
                if not results:
                    continue
                
                # 创建门槛目录
                threshold_dir = os.path.join(output_dir, threshold)
                os.makedirs(threshold_dir, exist_ok=True)
                
                # 保存每个ETF的结果
                for result in results:
                    etf_code = result.get('etf_code', '')
                    clean_code = etf_code.replace('.SH', '').replace('.SZ', '')
                    
                    output_file = os.path.join(threshold_dir, f"{clean_code}.csv")
                    
                    # 将结果转换为单行DataFrame
                    volatility_values = result.get('volatility_values', {})
                    row_data = {
                        'ETF_Code': etf_code,
                        'Calculation_Date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                        'Data_Source': result.get('data_source', ''),
                        **volatility_values
                    }
                    
                    df = pd.DataFrame([row_data])
                    df.to_csv(output_file, index=False, encoding='utf-8')
                    
                    # 统计信息
                    file_size = os.path.getsize(output_file)
                    save_stats['saved_files'].append(output_file)
                    save_stats['total_records'] += 1
                    save_stats['total_size_kb'] += file_size / 1024
                
                print(f"✅ {threshold}: 保存 {len(results)} 个文件")
            
            save_stats['total_size_kb'] = round(save_stats['total_size_kb'], 2)
            
            return save_stats
            
        except Exception as e:
            print(f"❌ 批量结果保存异常: {str(e)}")
            return {'error': str(e)}
    
    def generate_summary_statistics(self, results: List[Dict]) -> Dict[str, Any]:
        """
        生成汇总统计信息
        
        Args:
            results: 结果列表
            
        Returns:
            Dict: 统计信息
        """
        try:
            if not results:
                return {'error': '无可统计的结果'}
            
            stats = {
                'total_etfs': len(results),
                'successful_calculations': 0,
                'data_sources': {},
                'volatility_statistics': {}
            }
            
            # 收集所有波动率值
            all_volatility_values = {}
            
            for result in results:
                volatility_values = result.get('volatility_values', {})
                data_source = result.get('data_source', 'unknown')
                
                # 统计数据来源
                stats['data_sources'][data_source] = stats['data_sources'].get(data_source, 0) + 1
                
                if volatility_values:
                    stats['successful_calculations'] += 1
                    
                    # 收集波动率值
                    for key, value in volatility_values.items():
                        if value is not None and isinstance(value, (int, float)):
                            if key not in all_volatility_values:
                                all_volatility_values[key] = []
                            all_volatility_values[key].append(value)
            
            # 计算波动率统计
            for key, values in all_volatility_values.items():
                if values:
                    stats['volatility_statistics'][key] = {
                        'count': len(values),
                        'mean': round(np.mean(values), 6),
                        'median': round(np.median(values), 6),
                        'std': round(np.std(values), 6),
                        'min': round(np.min(values), 6),
                        'max': round(np.max(values), 6)
                    }
            
            # 计算成功率
            stats['success_rate'] = (stats['successful_calculations'] / stats['total_etfs']) * 100
            
            return stats
            
        except Exception as e:
            return {'error': f'统计生成异常: {str(e)}'}
    
    def display_summary_statistics(self, stats: Dict[str, Any]) -> None:
        """
        显示汇总统计信息
        
        Args:
            stats: 统计信息
        """
        if 'error' in stats:
            print(f"❌ 统计信息错误: {stats['error']}")
            return
        
        print(f"\n📊 波动率指标汇总统计")
        print("=" * 60)
        
        print(f"📈 总ETF数量: {stats['total_etfs']}")
        print(f"✅ 成功计算: {stats['successful_calculations']}")
        print(f"📊 成功率: {stats['success_rate']:.1f}%")
        
        # 数据来源统计
        print(f"\n💾 数据来源分布:")
        for source, count in stats['data_sources'].items():
            print(f"   {source}: {count}")
        
        # 波动率统计
        if stats['volatility_statistics']:
            print(f"\n📊 波动率指标统计 (前5个):")
            sorted_indicators = sorted(stats['volatility_statistics'].items(),
                                     key=lambda x: x[1]['count'], reverse=True)[:5]
            
            for indicator, indicator_stats in sorted_indicators:
                print(f"   {indicator}:")
                print(f"     计数: {indicator_stats['count']}")
                print(f"     均值: {indicator_stats['mean']}")
                print(f"     中位数: {indicator_stats['median']}")
                print(f"     范围: [{indicator_stats['min']}, {indicator_stats['max']}]")