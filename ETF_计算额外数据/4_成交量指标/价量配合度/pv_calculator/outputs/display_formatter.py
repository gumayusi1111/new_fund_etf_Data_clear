"""
PV显示格式化器
===============

负责PV结果的控制台显示格式化
"""

import pandas as pd
from typing import Dict, List, Any, Optional
import logging
from datetime import datetime

class PVDisplayFormatter:
    """PV显示格式化器"""

    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def format_single_result(self, result: pd.DataFrame, etf_code: str) -> str:
        """
        格式化单个ETF的PV结果显示

        Args:
            result: PV计算结果
            etf_code: ETF代码

        Returns:
            格式化的显示字符串
        """
        try:
            if result.empty:
                return f"ETF {etf_code}: 无数据"

            # 基本信息
            lines = [
                f"📊 ETF {etf_code} 价量协调指标计算结果",
                f"{'='*60}",
                f"数据记录数: {len(result)}",
                f"日期范围: {result['date'].min()} ~ {result['date'].max()}",
                ""
            ]

            # 最新数据展示
            if len(result) > 0:
                latest = result.iloc[-1]
                lines.extend([
                    "📈 最新价量协调数据:",
                    f"  日期: {latest['date']}",
                    "",
                    "🔗 价量相关性指标:",
                    f"  10日相关性: {latest.get('pv_corr_10', 'N/A'):>8.4f}",
                    f"  20日相关性: {latest.get('pv_corr_20', 'N/A'):>8.4f}",
                    f"  30日相关性: {latest.get('pv_corr_30', 'N/A'):>8.4f}",
                    "",
                    "📊 成交量价格趋势(VPT):",
                    f"  VPT指标:    {latest.get('vpt', 'N/A'):>12.2f}",
                    f"  VPT动量:    {latest.get('vpt_momentum', 'N/A'):>12.4f}",
                    f"  VPT比率:    {latest.get('vpt_ratio', 'N/A'):>12.4f}",
                    "",
                    "🎯 成交量质量评估:",
                    f"  质量评分:   {latest.get('volume_quality', 'N/A'):>8.4f}",
                    f"  一致性评分: {latest.get('volume_consistency', 'N/A'):>8.4f}",
                    "",
                    "⚡ 价量协调强度:",
                    f"  协调强度:   {latest.get('pv_strength', 'N/A'):>8.1f}%",
                    f"  背离程度:   {latest.get('pv_divergence', 'N/A'):>8.4f}",
                ])

                # 添加价量协调强度解读
                pv_strength = latest.get('pv_strength', 0)
                if pv_strength is not None and not pd.isna(pv_strength):
                    strength_level = self._interpret_pv_strength(pv_strength)
                    lines.extend([
                        "",
                        f"💡 协调强度解读: {strength_level}"
                    ])

                # 添加价量背离程度解读
                pv_divergence = latest.get('pv_divergence', 0)
                if pv_divergence is not None and not pd.isna(pv_divergence):
                    divergence_level = self._interpret_pv_divergence(pv_divergence)
                    lines.extend([
                        f"💡 背离程度解读: {divergence_level}"
                    ])

            return '\n'.join(lines)

        except Exception as e:
            return f"ETF {etf_code} 显示格式化失败: {str(e)}"

    def format_batch_summary(self, batch_result: Dict[str, Any]) -> str:
        """
        格式化批量处理结果摘要

        Args:
            batch_result: 批量处理结果

        Returns:
            格式化的摘要字符串
        """
        try:
            lines = [
                "🚀 PV价量协调指标批量计算结果摘要",
                "="*60,
                ""
            ]

            # 基本统计
            total_count = batch_result.get('total_count', 0)
            processed_count = batch_result.get('processed_count', 0)
            failed_count = batch_result.get('failed_count', 0)
            skipped_count = batch_result.get('skipped_count', 0)
            success_rate = batch_result.get('success_rate', 0)
            total_time = batch_result.get('total_time', 0)

            lines.extend([
                f"📊 处理统计:",
                f"  总数量:   {total_count:>6}",
                f"  成功:     {processed_count:>6} ({success_rate:>5.1f}%)",
                f"  失败:     {failed_count:>6}",
                f"  跳过:     {skipped_count:>6}",
                f"  总耗时:   {total_time:>6.2f}秒",
                ""
            ])

            # 性能统计
            summary = batch_result.get('summary', {})
            performance = summary.get('performance', {})

            if performance:
                lines.extend([
                    f"⚡ 性能统计:",
                    f"  平均耗时: {performance.get('avg_processing_time', 0):>6.3f}秒",
                    f"  最大耗时: {performance.get('max_processing_time', 0):>6.3f}秒",
                    f"  最小耗时: {performance.get('min_processing_time', 0):>6.3f}秒",
                    ""
                ])

            # 缓存统计
            cache_stats = summary.get('cache_stats', {})
            if cache_stats:
                cache_hits = cache_stats.get('cache_hits', 0)
                cache_hit_rate = cache_stats.get('cache_hit_rate', 0)
                lines.extend([
                    f"💾 缓存统计:",
                    f"  缓存命中: {cache_hits:>6}次",
                    f"  命中率:   {cache_hit_rate:>6.1f}%",
                    ""
                ])

            # 数据统计
            data_stats = summary.get('data_stats', {})
            if data_stats:
                lines.extend([
                    f"📈 数据统计:",
                    f"  平均记录数: {data_stats.get('avg_record_count', 0):>6}条",
                    f"  总记录数:   {data_stats.get('total_records', 0):>6}条",
                    ""
                ])

            # 失败详情
            failed_etfs = batch_result.get('failed_etfs', [])
            if failed_etfs and len(failed_etfs) <= 10:  # 只显示前10个失败
                lines.extend([
                    "❌ 失败ETF详情:",
                ])
                for failed in failed_etfs[:10]:
                    etf_code = failed.get('etf_code', 'Unknown')
                    error = failed.get('error', 'Unknown error')[:50]
                    lines.append(f"  {etf_code}: {error}")

                if len(failed_etfs) > 10:
                    lines.append(f"  ... 还有{len(failed_etfs) - 10}个失败")
                lines.append("")

            return '\n'.join(lines)

        except Exception as e:
            return f"批量摘要格式化失败: {str(e)}"

    def format_system_status(self, status: Dict[str, Any]) -> str:
        """
        格式化系统状态显示

        Args:
            status: 系统状态信息

        Returns:
            格式化的状态字符串
        """
        try:
            lines = [
                "🖥️  PV价量协调系统状态",
                "="*50,
                f"系统时间: {status.get('system_time', 'Unknown')}",
                ""
            ]

            # ETF数量统计和筛选信息
            etf_counts = status.get('etf_counts', {})
            config = status.get('config', {})
            etf_filter_stats = config.get('etf_filter_stats', {})

            if etf_counts:
                lines.extend([
                    "📈 ETF数量统计 (使用ETF_初筛系统筛选后结果):",
                ])
                for threshold, count in etf_counts.items():
                    filter_stats = etf_filter_stats.get(threshold, {})
                    total_count = filter_stats.get('total_count', 0)
                    filter_rate = filter_stats.get('filter_rate', 0)
                    filter_exists = filter_stats.get('filter_list_exists', False)

                    if filter_exists:
                        lines.append(f"  {threshold}: {count:>6}个 (筛选自{total_count}个, 筛选率{filter_rate}%)")
                    else:
                        lines.append(f"  {threshold}: {count:>6}个 (未筛选)")
                lines.append("")

            # 缓存统计
            cache_stats = status.get('cache_stats', {})
            if cache_stats:
                lines.extend([
                    "💾 缓存状态:",
                    f"  缓存条目: {cache_stats.get('total_entries', 0):>6}个",
                    f"  缓存大小: {cache_stats.get('total_size_mb', 0):>6.1f}MB",
                    f"  命中率:   {cache_stats.get('hit_rate', 0):>6.1f}%",
                    ""
                ])

            # 存储使用情况
            storage_usage = status.get('storage_usage', {})
            if storage_usage:
                lines.extend([
                    "💽 存储使用:",
                ])
                for category, usage in storage_usage.items():
                    size_mb = usage.get('size_mb', 0)
                    file_count = usage.get('file_count', 0)
                    lines.append(f"  {category}: {size_mb:>6.1f}MB ({file_count}个文件)")
                lines.append("")

            return '\n'.join(lines)

        except Exception as e:
            return f"系统状态格式化失败: {str(e)}"

    def format_test_results(self, test_results: Dict[str, Any]) -> str:
        """
        格式化测试结果显示

        Args:
            test_results: 测试结果

        Returns:
            格式化的测试结果字符串
        """
        try:
            overall_success = test_results.get('success', False)
            status_icon = "✅" if overall_success else "❌"

            lines = [
                f"{status_icon} PV价量协调系统测试结果",
                "="*50,
                f"测试时间: {test_results.get('test_time', 'Unknown')}",
                f"总体状态: {'通过' if overall_success else '失败'}",
                ""
            ]

            # 各项测试详情
            tests = test_results.get('tests', {})
            for test_name, test_result in tests.items():
                passed = test_result.get('passed', False)
                message = test_result.get('message', '')
                icon = "✅" if passed else "❌"

                lines.extend([
                    f"{icon} {test_name}:",
                    f"  {message}",
                ])

                # 详细信息
                details = test_result.get('details', {})
                if details:
                    for key, value in details.items():
                        lines.append(f"    {key}: {value}")

                lines.append("")

            return '\n'.join(lines)

        except Exception as e:
            return f"测试结果格式化失败: {str(e)}"

    def format_table(self, data: pd.DataFrame, max_rows: int = 10) -> str:
        """
        格式化DataFrame为表格显示

        Args:
            data: 要显示的数据
            max_rows: 最大显示行数

        Returns:
            格式化的表格字符串
        """
        try:
            if data.empty:
                return "📄 无数据"

            # 限制显示行数
            display_data = data.head(max_rows)

            # 使用pandas的字符串表示
            table_str = display_data.to_string(index=False, float_format='%.4f')

            # 如果数据被截断，添加说明
            if len(data) > max_rows:
                table_str += f"\n... 还有{len(data) - max_rows}行数据"

            return table_str

        except Exception as e:
            return f"表格格式化失败: {str(e)}"

    def format_pv_analysis_summary(self, result: pd.DataFrame, etf_code: str) -> str:
        """
        格式化PV指标分析摘要

        Args:
            result: PV计算结果
            etf_code: ETF代码

        Returns:
            分析摘要字符串
        """
        try:
            if result.empty:
                return f"ETF {etf_code}: 无分析数据"

            lines = [
                f"🔍 ETF {etf_code} 价量协调分析摘要",
                "="*50,
                ""
            ]

            # 相关性分析
            corr_cols = ['pv_corr_10', 'pv_corr_20', 'pv_corr_30']
            corr_stats = {}
            for col in corr_cols:
                if col in result.columns:
                    series = result[col].dropna()
                    if len(series) > 0:
                        corr_stats[col] = {
                            'mean': series.mean(),
                            'positive_ratio': (series > 0.1).sum() / len(series) * 100,
                            'negative_ratio': (series < -0.1).sum() / len(series) * 100
                        }

            if corr_stats:
                lines.extend([
                    "🔗 价量相关性分析:",
                ])
                for col, stats in corr_stats.items():
                    period = col.split('_')[-1]
                    lines.extend([
                        f"  {period}日相关性:",
                        f"    平均值: {stats['mean']:>8.4f}",
                        f"    正相关: {stats['positive_ratio']:>6.1f}%",
                        f"    负相关: {stats['negative_ratio']:>6.1f}%",
                    ])
                lines.append("")

            # VPT趋势分析
            if 'vpt_momentum' in result.columns:
                vpt_momentum = result['vpt_momentum'].dropna()
                if len(vpt_momentum) > 0:
                    positive_momentum = (vpt_momentum > 0).sum() / len(vpt_momentum) * 100
                    lines.extend([
                        "📊 VPT趋势分析:",
                        f"  正向动量比例: {positive_momentum:>6.1f}%",
                        f"  平均动量值:   {vpt_momentum.mean():>8.4f}",
                        ""
                    ])

            # 协调强度分析
            if 'pv_strength' in result.columns:
                pv_strength = result['pv_strength'].dropna()
                if len(pv_strength) > 0:
                    high_strength = (pv_strength > 70).sum() / len(pv_strength) * 100
                    low_strength = (pv_strength < 30).sum() / len(pv_strength) * 100
                    lines.extend([
                        "⚡ 协调强度分析:",
                        f"  平均强度:   {pv_strength.mean():>6.1f}%",
                        f"  高强度比例: {high_strength:>6.1f}%",
                        f"  低强度比例: {low_strength:>6.1f}%",
                        ""
                    ])

            return '\n'.join(lines)

        except Exception as e:
            return f"分析摘要格式化失败: {str(e)}"

    def _interpret_pv_strength(self, strength: float) -> str:
        """解读价量协调强度"""
        if strength >= 80:
            return "极强协调 🔥"
        elif strength >= 60:
            return "强协调 💪"
        elif strength >= 40:
            return "中等协调 👍"
        elif strength >= 20:
            return "弱协调 👎"
        else:
            return "极弱协调 ❄️"

    def _interpret_pv_divergence(self, divergence: float) -> str:
        """解读价量背离程度"""
        abs_div = abs(divergence)
        if abs_div >= 0.8:
            return "严重背离 ⚠️"
        elif abs_div >= 0.6:
            return "明显背离 📉"
        elif abs_div >= 0.4:
            return "轻微背离 📊"
        elif abs_div >= 0.2:
            return "基本协调 ✅"
        else:
            return "高度协调 🎯"