"""
显示格式化模块
==============

负责动量振荡器数据的格式化显示
提供美观的控制台输出格式
"""

import pandas as pd
from typing import Dict, List, Optional, Any
import logging

from ..infrastructure.config import MomentumConfig

class MomentumDisplayFormatter:
    """动量振荡器显示格式化器"""
    
    def __init__(self):
        """初始化显示格式化器"""
        self.logger = logging.getLogger(self.__class__.__name__)
        self.field_descriptions = MomentumConfig.FIELD_DESCRIPTIONS
        
    def format_statistics(self, stats: Dict[str, Any]) -> str:
        """
        格式化统计信息
        
        Args:
            stats: 统计信息字典
            
        Returns:
            格式化的统计信息字符串
        """
        try:
            lines = []
            lines.append("📊 处理统计:")
            lines.append(f"   ✅ 成功: {stats.get('success', 0)}")
            lines.append(f"   ❌ 失败: {stats.get('failed', 0)}")
            lines.append(f"   ⏭️ 跳过: {stats.get('skipped', 0)}")
            
            return "\n".join(lines)
            
        except Exception as e:
            self.logger.error(f"格式化统计信息失败: {str(e)}")
            return "统计信息格式化失败"
    
    def format_momentum_summary(self, data: pd.DataFrame, etf_code: str) -> str:
        """
        格式化动量指标摘要
        
        Args:
            data: 动量指标数据
            etf_code: ETF代码
            
        Returns:
            格式化的摘要字符串
        """
        try:
            if data.empty:
                return f"📈 {etf_code}: 暂无数据"
            
            latest = data.iloc[0]  # 最新数据
            
            lines = []
            lines.append(f"📈 {etf_code} 动量指标摘要:")
            lines.append(f"   📅 最新日期: {latest.get('date', 'N/A')}")
            lines.append(f"   📊 数据条数: {len(data)}")
            
            # 关键指标展示
            if 'roc_25' in latest:
                lines.append(f"   🚀 25日ROC: {latest['roc_25']:.2f}%")
            if 'momentum_strength' in latest:
                lines.append(f"   💪 动量强度: {latest['momentum_strength']:.2f}")
            if 'williams_r' in latest:
                lines.append(f"   📉 威廉指标: {latest['williams_r']:.1f}")
            
            return "\n".join(lines)
            
        except Exception as e:
            self.logger.error(f"格式化动量摘要失败 {etf_code}: {str(e)}")
            return f"📈 {etf_code}: 摘要格式化失败"
    
    def format_system_info(self) -> str:
        """
        格式化系统信息
        
        Returns:
            格式化的系统信息字符串
        """
        try:
            system_info = MomentumConfig.get_system_info()
            
            lines = []
            lines.append("🚀 动量振荡器主程序启动")
            lines.append("=" * 50)
            lines.append(f"📌 系统: {system_info['name']}")
            lines.append(f"🔢 版本: {system_info['version']}")
            lines.append(f"📊 支持门槛: {', '.join(system_info['supported_thresholds'])}")
            lines.append(f"📈 指标数量: {len(system_info['output_fields'])}个")
            
            return "\n".join(lines)
            
        except Exception as e:
            self.logger.error(f"格式化系统信息失败: {str(e)}")
            return "系统信息格式化失败"