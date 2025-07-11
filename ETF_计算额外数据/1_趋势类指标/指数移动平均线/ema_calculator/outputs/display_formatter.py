#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
EMA显示格式化器 - 重构版
======================

参照WMA/SMA系统的显示格式化架构
提供控制台和统计显示功能
"""

from typing import Dict, List
from ..infrastructure.config import EMAConfig


class EMADisplayFormatter:
    """EMA显示格式化器 - 重构版（与WMA/SMA保持一致）"""
    
    def __init__(self, config: EMAConfig):
        """
        初始化显示格式化器
        
        Args:
            config: EMA配置对象
        """
        self.config = config
        
        if not config.performance_mode:
            print("🖥️ EMA显示格式化器初始化完成")
    
    def display_single_etf_result(self, result: Dict) -> None:
        """
        显示单个ETF的结果
        
        Args:
            result: ETF处理结果
        """
        try:
            if not result.get('success', False):
                print(f"❌ {result.get('etf_code', 'Unknown')}: 处理失败")
                return
            
            etf_code = result['etf_code']
            price_info = result.get('price_info', {})
            ema_values = result.get('ema_values', {})
            
            output = self.format_single_result(etf_code, price_info, ema_values)
            print(output)
            
        except Exception as e:
            print(f"❌ 显示结果失败: {str(e)}")
    
    def format_single_result(self, etf_code: str, price_info: Dict, ema_values: Dict) -> str:
        """
        格式化单个ETF结果
        
        Args:
            etf_code: ETF代码
            price_info: 价格信息
            ema_values: EMA计算结果
            
        Returns:
            str: 格式化的显示文本
        """
        try:
            lines = []
            lines.append(f"📊 {etf_code} EMA分析结果")
            lines.append("=" * 40)
            
            # 价格信息
            latest_price = price_info.get('latest_price', 0)
            price_change = price_info.get('price_change', 0)
            price_change_pct = price_info.get('price_change_pct', 0)
            
            change_icon = "📈" if price_change >= 0 else "📉"
            lines.append(f"💰 最新价格: {latest_price} ({change_icon} {price_change:+.3f}, {price_change_pct:+.2f}%)")
            
            # EMA值
            lines.append("\n📈 EMA指标:")
            for period in sorted(self.config.ema_periods):
                ema_key = f'ema_{period}'
                if ema_key in ema_values:
                    ema_value = ema_values[ema_key]
                    lines.append(f"   EMA{period}: {ema_value:.6f}")
            
            # EMA差值（如果有）
            if 'ema_diff_12_26' in ema_values:
                diff_value = ema_values['ema_diff_12_26']
                diff_pct = ema_values.get('ema_diff_12_26_pct', 0)
                trend_icon = "📈" if diff_value > 0 else "📉" if diff_value < 0 else "➡️"
                lines.append(f"\n🔄 EMA差值(12-26): {diff_value:.6f} ({trend_icon} {diff_pct:.3f}%)")
            
            # EMA动量（如果有）
            if 'ema12_momentum' in ema_values:
                momentum = ema_values['ema12_momentum']
                momentum_icon = "⬆️" if momentum > 0 else "⬇️" if momentum < 0 else "➡️"
                lines.append(f"🔄 EMA12动量: {momentum:.6f} {momentum_icon}")
            
            return "\n".join(lines)
            
        except Exception as e:
            return f"❌ 格式化失败: {str(e)}"
    
    def display_batch_summary(self, results: List[Dict]) -> None:
        """
        显示批量处理摘要
        
        Args:
            results: 处理结果列表
        """
        try:
            summary = self.format_batch_summary(results)
            print(summary)
            
        except Exception as e:
            print(f"❌ 显示摘要失败: {str(e)}")
    
    def format_batch_summary(self, results: List[Dict]) -> str:
        """
        格式化批量处理摘要
        
        Args:
            results: 处理结果列表
            
        Returns:
            str: 格式化的摘要文本
        """
        try:
            total_count = len(results)
            success_count = len([r for r in results if r.get('success', False)])
            failed_count = total_count - success_count
            
            lines = []
            lines.append("\n📊 EMA批量处理摘要")
            lines.append("=" * 40)
            
            lines.append(f"📈 总处理数: {total_count}")
            lines.append(f"✅ 成功数: {success_count}")
            lines.append(f"❌ 失败数: {failed_count}")
            lines.append(f"📊 成功率: {success_count / total_count:.1%}" if total_count > 0 else "📊 成功率: 0%")
            
            # EMA值统计
            if success_count > 0:
                lines.append("\n📈 EMA值范围:")
                
                for period in sorted(self.config.ema_periods):
                    ema_key = f'ema_{period}'
                    values = [r.get('ema_values', {}).get(ema_key, 0) 
                             for r in results if r.get('success', False) and ema_key in r.get('ema_values', {})]
                    
                    if values:
                        min_val = min(values)
                        max_val = max(values)
                        avg_val = sum(values) / len(values)
                        lines.append(f"   EMA{period}: {min_val:.3f} ~ {max_val:.3f} (平均: {avg_val:.3f})")
            
            return "\n".join(lines)
            
        except Exception as e:
            return f"❌ 摘要格式化失败: {str(e)}"
    
    def display_system_status(self, status: Dict) -> None:
        """
        显示系统状态
        
        Args:
            status: 系统状态信息
        """
        try:
            if 'error' in status:
                print(f"❌ 系统状态检查失败: {status['error']}")
                return
            
            lines = []
            lines.append("🔧 EMA系统状态")
            lines.append("=" * 40)
            
            # 系统信息
            system_info = status.get('system_info', {})
            lines.append(f"📊 版本: {system_info.get('version', 'Unknown')}")
            lines.append(f"🔧 配置: {system_info.get('config', 'Unknown')}")
            
            # 数据状态
            data_status = status.get('data_status', {})
            lines.append(f"\n📁 数据状态:")
            lines.append(f"   可用ETF: {data_status.get('available_etfs_count', 0)} 个")
            lines.append(f"   数据路径: {'✅ 有效' if data_status.get('data_path_valid', False) else '❌ 无效'}")
            
            # 组件状态
            components = status.get('components', {})
            lines.append(f"\n🔧 组件状态:")
            for component, state in components.items():
                status_icon = "✅" if state == "Ready" else "⚠️" if state == "Disabled" else "❌"
                lines.append(f"   {component}: {status_icon} {state}")
            
            print("\n".join(lines))
            
        except Exception as e:
            print(f"❌ 系统状态显示失败: {str(e)}")
    
    def display_progress(self, current: int, total: int, etf_code: str) -> None:
        """
        显示处理进度
        
        Args:
            current: 当前进度
            total: 总数
            etf_code: 当前处理的ETF代码
        """
        if not self.config.performance_mode:
            percentage = (current / total * 100) if total > 0 else 0
            print(f"📊 进度: {current}/{total} ({percentage:.1f}%) - {etf_code}")
    
    def display_cache_stats(self, stats: Dict) -> None:
        """
        显示缓存统计信息
        
        Args:
            stats: 缓存统计数据
        """
        try:
            lines = []
            lines.append("🗂️ 缓存统计")
            lines.append("=" * 30)
            
            lines.append(f"💾 缓存命中: {stats.get('cache_hits', 0)}")
            lines.append(f"⚡ 增量更新: {stats.get('incremental_updates', 0)}")
            lines.append(f"🔄 新计算: {stats.get('new_calculations', 0)}")
            lines.append(f"❌ 失败数: {stats.get('failed_count', 0)}")
            lines.append(f"📊 命中率: {stats.get('cache_hit_rate', 0):.1%}")
            
            print("\n".join(lines))
            
        except Exception as e:
            print(f"❌ 缓存统计显示失败: {str(e)}")