"""
OBV指标显示格式化器 - 结果展示美化
===============================

专门用于格式化和美化OBV指标计算结果的展示
提供统一的输出格式和用户友好的界面

核心功能:
- 测试结果格式化
- 系统状态美化显示
- 批量计算摘要格式化
- 错误信息友好化
- 统计数据可视化

技术特点:
- 彩色输出支持
- 表格化数据展示
- 进度指示和状态图标
- 多语言友好
"""

from typing import Dict, List, Optional, Any
import json
from datetime import datetime


class OBVDisplayFormatter:
    """OBV指标显示格式化器"""
    
    def __init__(self):
        """初始化格式化器"""
        # 状态图标映射
        self.status_icons = {
            'success': '✅',
            'error': '❌', 
            'warning': '⚠️',
            'info': 'ℹ️',
            'running': '🔄',
            'completed': '✅',
            'failed': '❌',
            'active': '🟢',
            'inactive': '🔴',
            'pending': '🟡'
        }
        
        # 颜色代码 (如果支持)
        self.colors = {
            'red': '\033[91m',
            'green': '\033[92m',
            'yellow': '\033[93m',
            'blue': '\033[94m',
            'purple': '\033[95m',
            'cyan': '\033[96m',
            'white': '\033[97m',
            'reset': '\033[0m',
            'bold': '\033[1m'
        }
    
    def format_test_results(self, test_results: Dict[str, Any]) -> str:
        """
        格式化测试结果
        
        Args:
            test_results: 测试结果字典
            
        Returns:
            格式化后的测试结果字符串
        """
        try:
            if 'error' in test_results:
                return f"❌ 测试执行失败: {test_results['error']}"
            
            output = []
            output.append("📋 OBV系统功能测试结果")
            output.append("-" * 50)
            
            # 测试摘要
            summary = test_results.get('summary', {})
            total_tests = summary.get('total_tests', 0)
            passed_tests = summary.get('passed_tests', 0)
            failed_tests = summary.get('failed_tests', 0)
            success_rate = summary.get('success_rate', 0)
            overall_status = summary.get('overall_status', 'UNKNOWN')
            
            status_icon = self.status_icons['success'] if overall_status == 'PASS' else self.status_icons['error']
            
            output.append(f"\n📊 测试摘要:")
            output.append(f"  总测试数: {total_tests}")
            output.append(f"  通过数量: {passed_tests}")
            output.append(f"  失败数量: {failed_tests}")
            output.append(f"  成功率: {success_rate:.1f}%")
            output.append(f"  整体状态: {status_icon} {overall_status}")
            
            # 各项测试详情
            tests = test_results.get('tests', {})
            if tests:
                output.append(f"\n🔍 测试详情:")
                
                test_descriptions = {
                    'component_init': '组件初始化测试',
                    'data_reading': '数据读取测试',
                    'calculation_engine': '计算引擎测试',
                    'cache_system': '缓存系统测试',
                    'output_system': '输出系统测试'
                }
                
                for test_name, test_result in tests.items():
                    test_desc = test_descriptions.get(test_name, test_name)
                    passed = test_result.get('passed', False)
                    status_icon = self.status_icons['success'] if passed else self.status_icons['error']
                    
                    output.append(f"  {status_icon} {test_desc}")
                    
                    # 显示测试详情
                    if 'details' in test_result:
                        details = test_result['details']
                        if isinstance(details, dict):
                            for key, value in details.items():
                                if isinstance(value, (int, float)):
                                    output.append(f"    {key}: {value}")
                                elif isinstance(value, bool):
                                    output.append(f"    {key}: {'是' if value else '否'}")
                                else:
                                    output.append(f"    {key}: {value}")
                    
                    # 显示错误信息
                    if 'error' in test_result:
                        output.append(f"    错误: {test_result['error']}")
            
            return "\n".join(output)
            
        except Exception as e:
            return f"❌ 格式化测试结果失败: {str(e)}"
    
    def format_system_status(self, status: Dict[str, Any]) -> str:
        """
        格式化系统状态
        
        Args:
            status: 系统状态字典
            
        Returns:
            格式化后的状态字符串
        """
        try:
            if 'error' in status:
                return f"❌ 获取系统状态失败: {status['error']}"
            
            output = []
            output.append("🖥️  OBV系统状态监控")
            output.append("-" * 50)
            
            # 系统基础信息
            system_info = status.get('system_info', {})
            if system_info:
                output.append(f"\n📋 系统信息:")
                output.append(f"  系统名称: {system_info.get('name', 'N/A')}")
                output.append(f"  版本号: {system_info.get('version', 'N/A')}")
                output.append(f"  运行状态: {self._get_status_icon(system_info.get('status', ''))} {system_info.get('status', 'N/A')}")
                
                uptime_seconds = system_info.get('uptime_seconds', 0)
                uptime_str = self._format_duration(uptime_seconds)
                output.append(f"  运行时长: {uptime_str}")
                
                start_time = system_info.get('start_time', '')
                if start_time:
                    try:
                        start_dt = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
                        output.append(f"  启动时间: {start_dt.strftime('%Y-%m-%d %H:%M:%S')}")
                    except:
                        output.append(f"  启动时间: {start_time}")
            
            # 性能统计
            performance = status.get('performance', {})
            if performance:
                output.append(f"\n📈 性能统计:")
                output.append(f"  总计算次数: {performance.get('total_calculations', 0)}")
                output.append(f"  缓存命中率: {performance.get('cache_hit_rate', 0):.1f}%")
                output.append(f"  平均处理时间: {performance.get('average_processing_time', 0):.3f}秒")
                output.append(f"  错误率: {performance.get('error_rate', 0):.1f}%")
            
            # 组件状态
            components = status.get('components', {})
            if components:
                output.append(f"\n🔧 组件状态:")
                
                for comp_name, comp_info in components.items():
                    if comp_name == 'cache_managers':
                        output.append(f"  缓存管理器:")
                        if isinstance(comp_info, dict):
                            for threshold, cache_stats in comp_info.items():
                                if isinstance(cache_stats, dict):
                                    storage = cache_stats.get('storage', {})
                                    performance = cache_stats.get('performance', {})
                                    
                                    output.append(f"    {threshold}:")
                                    output.append(f"      缓存文件: {storage.get('cache_files', 0)}个")
                                    output.append(f"      使用空间: {storage.get('total_size_mb', 0):.1f}MB")
                                    output.append(f"      命中率: {performance.get('hit_rate', 0):.1f}%")
                    else:
                        comp_status = comp_info.get('status', 'UNKNOWN') if isinstance(comp_info, dict) else str(comp_info)
                        status_icon = self._get_status_icon(comp_status)
                        output.append(f"  {comp_name}: {status_icon} {comp_status}")
                        
                        # 显示额外信息
                        if isinstance(comp_info, dict):
                            for key, value in comp_info.items():
                                if key != 'status':
                                    if isinstance(value, bool):
                                        value_str = '是' if value else '否'
                                    else:
                                        value_str = str(value)
                                    output.append(f"    {key}: {value_str}")
            
            # 存储状态
            storage = status.get('storage', {})
            if storage:
                output.append(f"\n💾 存储状态:")
                for threshold, storage_info in storage.items():
                    if isinstance(storage_info, dict):
                        data_files = storage_info.get('data_files', 0)
                        cache_files = storage_info.get('cache_files', 0)
                        
                        output.append(f"  {threshold}:")
                        output.append(f"    数据文件: {data_files}个")
                        output.append(f"    缓存文件: {cache_files}个")
            
            return "\n".join(output)
            
        except Exception as e:
            return f"❌ 格式化系统状态失败: {str(e)}"
    
    def format_batch_summary(self, batch_result: Dict[str, Any]) -> str:
        """
        格式化批量计算摘要
        
        Args:
            batch_result: 批量计算结果
            
        Returns:
            格式化后的摘要字符串
        """
        try:
            if 'error' in batch_result:
                return f"❌ 批量计算失败: {batch_result['error']}"
            
            output = []
            
            # 基本统计
            threshold = batch_result.get('threshold', 'N/A')
            total_count = batch_result.get('total_count', 0)
            success_count = batch_result.get('success_count', 0)
            error_count = batch_result.get('error_count', 0)
            success_rate = batch_result.get('success_rate', 0)
            total_time = batch_result.get('total_time', 0)
            
            overall_success = batch_result.get('success', False)
            status_icon = self.status_icons['success'] if overall_success else self.status_icons['error']
            
            output.append(f"📊 批量计算结果摘要 - {threshold}")
            output.append("-" * 50)
            output.append(f"\n{status_icon} 执行结果:")
            output.append(f"  总ETF数量: {total_count}")
            output.append(f"  成功计算: {success_count}")
            output.append(f"  失败数量: {error_count}")
            output.append(f"  成功率: {success_rate:.1f}%")
            output.append(f"  总耗时: {total_time:.2f}秒")
            
            # 性能统计
            avg_time = batch_result.get('avg_time_per_etf', 0)
            total_data_points = batch_result.get('total_data_points', 0)
            cache_hit_rate = batch_result.get('cache_hit_rate', 0)
            max_workers = batch_result.get('max_workers', 1)
            
            output.append(f"\n⚡ 性能统计:")
            output.append(f"  平均处理时间: {avg_time:.3f}秒/ETF")
            output.append(f"  总数据点数: {total_data_points:,}")
            output.append(f"  缓存命中率: {cache_hit_rate:.1f}%")
            output.append(f"  并行线程数: {max_workers}")
            
            # 处理模式
            force_recalculate = batch_result.get('force_recalculate', False)
            output.append(f"  强制重算: {'是' if force_recalculate else '否'}")
            
            # 错误详情(如果有)
            if error_count > 0 and 'errors' in batch_result:
                errors = batch_result['errors']
                total_errors = batch_result.get('total_errors', len(errors))
                
                output.append(f"\n❌ 错误详情 (显示前{min(len(errors), 5)}个):")
                for i, error in enumerate(errors[:5]):
                    etf_code = error.get('etf_code', 'N/A')
                    error_msg = error.get('error', 'N/A')
                    output.append(f"  {i+1}. {etf_code}: {error_msg}")
                
                if total_errors > len(errors):
                    output.append(f"  ... 还有 {total_errors - len(errors)} 个错误")
            
            # 建议和提示
            output.append(f"\n💡 性能建议:")
            if success_rate < 80:
                output.append(f"  • 成功率较低，建议检查数据源和配置")
            if cache_hit_rate < 50:
                output.append(f"  • 缓存命中率较低，建议优化缓存策略")
            if avg_time > 1.0:
                output.append(f"  • 处理速度较慢，建议增加并行线程数")
            
            if success_rate >= 95 and cache_hit_rate >= 80:
                output.append(f"  • 系统运行良好，性能优秀 {self.status_icons['success']}")
            
            return "\n".join(output)
            
        except Exception as e:
            return f"❌ 格式化批量摘要失败: {str(e)}"
    
    def format_obv_analysis_summary(self, analysis: Dict[str, Any]) -> str:
        """
        格式化OBV分析摘要
        
        Args:
            analysis: OBV分析结果
            
        Returns:
            格式化后的分析字符串
        """
        try:
            output = []
            
            # OBV趋势分析
            if 'trend' in analysis:
                trend = analysis['trend']
                trend_direction = trend.get('direction', 'N/A')
                trend_strength = trend.get('strength', 0)
                
                direction_icon = {
                    'up': '📈',
                    'down': '📉', 
                    'sideways': '➡️'
                }.get(trend_direction.lower(), '❓')
                
                output.append(f"📊 OBV趋势分析:")
                output.append(f"  趋势方向: {direction_icon} {trend_direction}")
                output.append(f"  趋势强度: {trend_strength:.1f}")
            
            # 资金流向分析
            if 'money_flow' in analysis:
                flow = analysis['money_flow']
                flow_direction = flow.get('direction', 'N/A')
                flow_intensity = flow.get('intensity', 0)
                
                flow_icon = {
                    'inflow': '💰',
                    'outflow': '💸',
                    'neutral': '🔄'
                }.get(flow_direction.lower(), '❓')
                
                output.append(f"\n💰 资金流向:")
                output.append(f"  流向: {flow_icon} {flow_direction}")
                output.append(f"  强度: {flow_intensity:.1f}")
            
            # 变化率分析
            if 'change_rates' in analysis:
                rates = analysis['change_rates']
                change_5d = rates.get('5d', 0)
                change_20d = rates.get('20d', 0)
                
                output.append(f"\n📈 变化率分析:")
                output.append(f"  5日变化率: {change_5d:+.2f}%")
                output.append(f"  20日变化率: {change_20d:+.2f}%")
            
            # 信号强度
            if 'signal_strength' in analysis:
                signal = analysis['signal_strength']
                strength = signal.get('overall', 0)
                confidence = signal.get('confidence', 0)
                
                strength_icon = '🔥' if strength > 70 else '⚡' if strength > 40 else '💭'
                
                output.append(f"\n📡 信号强度:")
                output.append(f"  综合强度: {strength_icon} {strength:.1f}")
                output.append(f"  置信度: {confidence:.1f}%")
            
            return "\n".join(output) if output else "📊 暂无OBV分析数据"
            
        except Exception as e:
            return f"❌ 格式化OBV分析失败: {str(e)}"
    
    def format_detailed_analysis(self, analysis: Dict[str, Any]) -> str:
        """
        格式化详细分析结果
        
        Args:
            analysis: 详细分析结果
            
        Returns:
            格式化后的分析字符串
        """
        try:
            output = []
            output.append("🔍 OBV深度分析报告")
            output.append("=" * 50)
            
            # 基础信息
            if 'basic_info' in analysis:
                info = analysis['basic_info']
                output.append(f"\n📋 基础信息:")
                output.append(f"  ETF代码: {info.get('etf_code', 'N/A')}")
                output.append(f"  门槛类型: {info.get('threshold', 'N/A')}")
                output.append(f"  分析日期: {info.get('analysis_date', 'N/A')}")
                output.append(f"  数据周期: {info.get('data_period', 'N/A')}")
            
            # OBV统计
            if 'obv_statistics' in analysis:
                stats = analysis['obv_statistics']
                output.append(f"\n📊 OBV统计:")
                output.append(f"  当前值: {stats.get('current_value', 0):,.2f}")
                output.append(f"  最高值: {stats.get('max_value', 0):,.2f}")
                output.append(f"  最低值: {stats.get('min_value', 0):,.2f}")
                output.append(f"  平均值: {stats.get('mean_value', 0):,.2f}")
                output.append(f"  标准差: {stats.get('std_value', 0):,.2f}")
            
            # 趋势分析
            if 'trend_analysis' in analysis:
                trend = analysis['trend_analysis']
                output.append(f"\n📈 趋势分析:")
                
                # 短期趋势
                short_term = trend.get('short_term', {})
                output.append(f"  短期趋势(5日): {short_term.get('direction', 'N/A')} "
                          f"({short_term.get('strength', 0):.1f})")
                
                # 中期趋势
                medium_term = trend.get('medium_term', {})
                output.append(f"  中期趋势(20日): {medium_term.get('direction', 'N/A')} "
                          f"({medium_term.get('strength', 0):.1f})")
                
                # 趋势一致性
                consistency = trend.get('consistency', 0)
                output.append(f"  趋势一致性: {consistency:.1f}%")
            
            # 背离分析
            if 'divergence_analysis' in analysis:
                div = analysis['divergence_analysis']
                output.append(f"\n🔄 背离分析:")
                output.append(f"  价格OBV背离: {div.get('price_obv_divergence', 'N/A')}")
                output.append(f"  背离强度: {div.get('divergence_strength', 0):.1f}")
                output.append(f"  背离持续天数: {div.get('divergence_days', 0)}")
            
            # 支撑阻力
            if 'support_resistance' in analysis:
                sr = analysis['support_resistance']
                output.append(f"\n🎯 支撑阻力:")
                output.append(f"  支撑位: {sr.get('support_level', 0):,.2f}")
                output.append(f"  阻力位: {sr.get('resistance_level', 0):,.2f}")
                output.append(f"  距支撑位: {sr.get('distance_to_support', 0):+.1f}%")
                output.append(f"  距阻力位: {sr.get('distance_to_resistance', 0):+.1f}%")
            
            return "\n".join(output)
            
        except Exception as e:
            return f"❌ 格式化详细分析失败: {str(e)}"
    
    def _get_status_icon(self, status: str) -> str:
        """获取状态对应的图标"""
        status_lower = status.lower()
        
        if status_lower in ['running', 'active', 'ok', 'pass', 'success']:
            return self.status_icons['active']
        elif status_lower in ['error', 'failed', 'fail']:
            return self.status_icons['inactive']
        elif status_lower in ['warning', 'pending']:
            return self.status_icons['pending']
        else:
            return self.status_icons['info']
    
    def _format_duration(self, seconds: int) -> str:
        """格式化时长显示"""
        if seconds < 60:
            return f"{seconds}秒"
        elif seconds < 3600:
            minutes = seconds // 60
            remaining_seconds = seconds % 60
            return f"{minutes}分{remaining_seconds}秒"
        else:
            hours = seconds // 3600
            remaining_minutes = (seconds % 3600) // 60
            return f"{hours}小时{remaining_minutes}分钟"
    
    def format_progress_bar(self, current: int, total: int, width: int = 30) -> str:
        """
        格式化进度条
        
        Args:
            current: 当前进度
            total: 总数
            width: 进度条宽度
            
        Returns:
            进度条字符串
        """
        if total == 0:
            return f"[{'=' * width}] 0%"
        
        progress = current / total
        filled_width = int(width * progress)
        
        bar = '=' * filled_width + '-' * (width - filled_width)
        percentage = progress * 100
        
        return f"[{bar}] {percentage:.1f}% ({current}/{total})"
    
    def format_table(self, headers: List[str], rows: List[List[str]], 
                    title: Optional[str] = None) -> str:
        """
        格式化表格显示
        
        Args:
            headers: 表头列表
            rows: 数据行列表
            title: 表格标题
            
        Returns:
            格式化后的表格字符串
        """
        try:
            if not headers or not rows:
                return "📊 暂无数据"
            
            # 计算列宽
            col_widths = [len(header) for header in headers]
            for row in rows:
                for i, cell in enumerate(row):
                    if i < len(col_widths):
                        col_widths[i] = max(col_widths[i], len(str(cell)))
            
            output = []
            
            # 标题
            if title:
                output.append(title)
                output.append("=" * len(title))
                output.append("")
            
            # 表头
            header_row = " | ".join(header.ljust(col_widths[i]) 
                                  for i, header in enumerate(headers))
            output.append(header_row)
            
            # 分隔线
            separator = "-+-".join("-" * width for width in col_widths)
            output.append(separator)
            
            # 数据行
            for row in rows:
                data_row = " | ".join(str(cell).ljust(col_widths[i]) 
                                    for i, cell in enumerate(row))
                output.append(data_row)
            
            return "\n".join(output)
            
        except Exception as e:
            return f"❌ 格式化表格失败: {str(e)}"