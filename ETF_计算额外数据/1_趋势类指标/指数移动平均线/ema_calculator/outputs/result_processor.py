#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
EMA结果处理器 - 重构版
====================

参照WMA/SMA系统的结果处理架构
提供结果验证、格式化和输出功能
"""

import os
import pandas as pd
from typing import Dict, List, Optional
from ..infrastructure.config import EMAConfig


class EMAResultProcessor:
    """EMA结果处理器 - 重构版（与WMA/SMA保持一致）"""
    
    def __init__(self, config: EMAConfig):
        """
        初始化结果处理器
        
        Args:
            config: EMA配置对象
        """
        self.config = config
        
        if not config.performance_mode:
            print("📋 EMA结果处理器初始化完成")
    
    def validate_result_data(self, etf_code: str, price_info: Dict, 
                           ema_values: Dict, signals: Dict) -> bool:
        """
        验证结果数据的完整性
        
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
    
    def format_console_output(self, etf_code: str, price_info: Dict, 
                            ema_values: Dict, signals: Dict) -> str:
        """
        格式化控制台输出
        
        Args:
            etf_code: ETF代码
            price_info: 价格信息
            ema_values: EMA计算结果
            signals: 信号数据
            
        Returns:
            str: 格式化的输出文本
        """
        try:
            output_lines = []
            output_lines.append(f"📊 {etf_code} EMA分析结果")
            output_lines.append("=" * 40)
            
            # 价格信息
            latest_price = price_info.get('latest_price', 0)
            price_change = price_info.get('price_change', 0)
            price_change_pct = price_info.get('price_change_pct', 0)
            
            change_icon = "📈" if price_change >= 0 else "📉"
            output_lines.append(f"💰 最新价格: {latest_price} ({change_icon} {price_change:+.3f}, {price_change_pct:+.2f}%)")
            
            # EMA值
            output_lines.append("\n📈 EMA指标:")
            for period in sorted(self.config.ema_periods):
                ema_key = f'ema_{period}'
                if ema_key in ema_values:
                    ema_value = ema_values[ema_key]
                    output_lines.append(f"   EMA{period}: {ema_value:.6f}")
            
            # EMA差值（如果有）
            if 'ema_diff_12_26' in ema_values:
                diff_value = ema_values['ema_diff_12_26']
                diff_pct = ema_values.get('ema_diff_12_26_pct', 0)
                trend_icon = "📈" if diff_value > 0 else "📉" if diff_value < 0 else "➡️"
                output_lines.append(f"\n🔄 EMA差值(12-26): {diff_value:.6f} ({trend_icon} {diff_pct:.3f}%)")
            
            # EMA动量（如果有）
            if 'ema12_momentum' in ema_values:
                momentum = ema_values['ema12_momentum']
                momentum_icon = "⬆️" if momentum > 0 else "⬇️" if momentum < 0 else "➡️"
                output_lines.append(f"🔄 EMA12动量: {momentum:.6f} {momentum_icon}")
            
            return "\n".join(output_lines)
            
        except Exception as e:
            return f"❌ 输出格式化失败: {str(e)}"
    
    def get_csv_header(self) -> str:
        """
        获取CSV文件头
        
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
    
    def format_ema_result_row(self, etf_code: str, price_info: Dict, 
                            ema_values: Dict, signals: Dict) -> str:
        """
        格式化EMA结果为CSV行
        
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
    
    def save_historical_results(self, etf_code: str, df: pd.DataFrame, 
                              ema_values: Dict, threshold: str, 
                              arrangement: str, output_base_dir: str) -> Optional[str]:
        """
        保存ETF的完整历史EMA数据
        
        Args:
            etf_code: ETF代码
            df: 历史数据DataFrame
            ema_values: EMA计算结果
            threshold: 门槛类型
            arrangement: 排列信息
            output_base_dir: 输出基础目录
            
        Returns:
            Optional[str]: 保存的文件路径或None
        """
        try:
            # 创建输出目录
            output_dir = os.path.join(output_base_dir, threshold)
            os.makedirs(output_dir, exist_ok=True)
            
            # 计算完整历史EMA
            result_df = df.copy()
            
            # 使用EMA引擎计算历史数据（需要从配置获取引擎）
            # 这里简化处理，实际应该从主控制器传入引擎
            from ..engines.ema_engine import EMAEngine
            ema_engine = EMAEngine(self.config)
            
            full_ema_df = ema_engine.calculate_full_historical_ema(result_df, etf_code)
            if full_ema_df is None:
                return None
            
            # 保存文件
            clean_etf_code = etf_code.replace('.SH', '').replace('.SZ', '')
            filename = f"{clean_etf_code}.csv"
            file_path = os.path.join(output_dir, filename)
            
            full_ema_df.to_csv(file_path, index=False, encoding='utf-8')
            
            if not self.config.performance_mode:
                print(f"💾 {etf_code}: 历史EMA数据已保存到 {file_path}")
            
            return file_path
            
        except Exception as e:
            if not self.config.performance_mode:
                print(f"❌ {etf_code} 历史数据保存失败: {str(e)}")
            return None
    
    def create_summary_stats(self, results: List[Dict]) -> Dict:
        """
        创建批量处理的统计摘要
        
        Args:
            results: 处理结果列表
            
        Returns:
            Dict: 统计摘要
        """
        try:
            total_count = len(results)
            success_count = len([r for r in results if r.get('success', False)])
            failed_count = total_count - success_count
            
            # EMA值统计
            ema_stats = {}
            for period in self.config.ema_periods:
                ema_key = f'ema_{period}'
                values = [r.get('ema_values', {}).get(ema_key, 0) 
                         for r in results if r.get('success', False)]
                
                if values:
                    ema_stats[f'ema_{period}'] = {
                        'min': min(values),
                        'max': max(values),
                        'avg': sum(values) / len(values)
                    }
            
            return {
                'total_processed': total_count,
                'success_count': success_count,
                'failed_count': failed_count,
                'success_rate': success_count / total_count if total_count > 0 else 0,
                'ema_statistics': ema_stats
            }
            
        except Exception as e:
            return {
                'total_processed': len(results),
                'success_count': 0,
                'failed_count': len(results),
                'success_rate': 0.0,
                'error': str(e)
            }
    
    def format_summary_display(self, stats: Dict) -> str:
        """
        格式化统计摘要显示
        
        Args:
            stats: 统计数据
            
        Returns:
            str: 格式化的摘要文本
        """
        try:
            lines = []
            lines.append("\n📊 EMA批量处理统计摘要")
            lines.append("=" * 40)
            
            lines.append(f"📈 总处理数: {stats.get('total_processed', 0)}")
            lines.append(f"✅ 成功数: {stats.get('success_count', 0)}")
            lines.append(f"❌ 失败数: {stats.get('failed_count', 0)}")
            lines.append(f"📊 成功率: {stats.get('success_rate', 0):.1%}")
            
            # EMA统计
            ema_stats = stats.get('ema_statistics', {})
            if ema_stats:
                lines.append("\n📈 EMA值统计:")
                for period in sorted(self.config.ema_periods):
                    ema_key = f'ema_{period}'
                    if ema_key in ema_stats:
                        stat = ema_stats[ema_key]
                        lines.append(f"   EMA{period}: 最小{stat['min']:.3f}, 最大{stat['max']:.3f}, 平均{stat['avg']:.3f}")
            
            return "\n".join(lines)
            
        except Exception as e:
            return f"❌ 摘要格式化失败: {str(e)}"