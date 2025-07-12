#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
WMA结果处理器模块 - 完整版本
===========================

专门负责WMA计算结果的格式化、保存和展示
完全移植原版系统的结果处理功能
"""

import os
import json
import csv
import numpy as np
import pandas as pd
from datetime import datetime
from typing import List, Dict, Any, Optional
from ..infrastructure.config import WMAConfig


def convert_numpy_types(obj):
    """
    转换numpy类型为Python原生类型，用于JSON序列化
    
    🔬 科学序列化: 处理所有numpy类型，确保JSON兼容性
    """
    if isinstance(obj, (np.bool_, bool)):
        return bool(obj)
    elif isinstance(obj, np.integer):
        return int(obj)
    elif isinstance(obj, np.floating):
        return float(obj)
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    elif isinstance(obj, dict):
        return {key: convert_numpy_types(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [convert_numpy_types(item) for item in obj]
    elif isinstance(obj, tuple):
        return tuple(convert_numpy_types(item) for item in obj)
    return obj


class WMAResultProcessor:
    """WMA结果处理器 - 完整版本"""
    
    def __init__(self, config: WMAConfig):
        """
        初始化结果处理器
        
        Args:
            config: WMA配置对象
        """
        self.config = config
        print("💾 WMA结果处理器初始化完成")
    
    def format_single_result(self, etf_code: str, wma_results: Dict, latest_price: Dict, 
                           date_range: Dict, data_optimization: Dict, signals: Dict,
                           wma_statistics: Dict = None, quality_metrics: Dict = None) -> Dict:
        """
        格式化单个ETF的计算结果 - 保持原有格式化逻辑
        
        Args:
            etf_code: ETF代码
            wma_results: WMA计算结果
            latest_price: 最新价格信息
            date_range: 日期范围
            data_optimization: 数据优化信息
            signals: 信号分析结果
            wma_statistics: WMA统计信息（可选）
            quality_metrics: 质量指标（可选）
            
        Returns:
            Dict: 格式化后的结果
        """
        result = {
            'etf_code': etf_code,
            'adj_type': self.config.adj_type,
            'calculation_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'data_optimization': data_optimization,
            'data_range': date_range,
            'latest_price': latest_price,
            'wma_values': wma_results,
            'signals': signals
        }
        
        # 添加可选的统计信息
        if wma_statistics:
            result['wma_statistics'] = wma_statistics
        
        if quality_metrics:
            result['quality_metrics'] = quality_metrics
        
        return result
    
    def save_results(self, results_list: List[Dict], output_dir: str = "data") -> Dict[str, str]:
        """
        保存精简计算结果 - 只输出CSV格式
        
        Args:
            results_list: 计算结果列表
            output_dir: 输出目录
            
        Returns:
            Dict[str, str]: 保存的文件路径
        """
        if not results_list:
            print("❌ 没有有效结果可保存")
            return {}
        
        # 创建输出目录
        os.makedirs(output_dir, exist_ok=True)
        
        # 生成时间戳
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # 保存CSV结果文件 (表格化数据)
        csv_file = os.path.join(output_dir, f"WMA_Results_{timestamp}.csv")
        self._create_csv_file(results_list, csv_file)
        
        # 计算文件大小
        csv_size = os.path.getsize(csv_file)
        
        print(f"💾 结果文件已保存:")
        print(f"   📈 CSV数据: {os.path.basename(csv_file)} ({csv_size} 字节)")
        print(f"   💿 总大小: {csv_size / 1024:.1f} KB")
        
        return {
            'csv_file': csv_file
        }
    
    def _create_csv_file(self, results_list: List[Dict], csv_file: str):
        """
        创建CSV文件 - 科学数据表格
        
        Args:
            results_list: 结果列表
            csv_file: CSV文件路径
        """
        try:
            # 准备CSV数据
            csv_data = []
            
            for result in results_list:
                try:
                    # 精简CSV - 只保留最重要的核心字段
                    # 修复adj_type字段访问问题
                    adj_type = result.get('adj_type', self.config.adj_type)
                    
                    row = {
                        'code': result['etf_code'],
                        'adj_type': adj_type,
                        'date': result['latest_price']['date'],
                        'close': result['latest_price']['close'],
                        'change_pct': result['latest_price']['change_pct'],
                    }
                    
                    # WMA核心指标
                    wma_values = result['wma_values']
                    for period in self.config.wma_periods:
                        # 修复字段命名不一致问题
                        wma_key = f'WMA{period}'  # 统一使用无下划线格式
                        wma_val = wma_values.get(wma_key)
                        if wma_val is None:
                            # 尝试备用字段名
                            wma_val = wma_values.get(f'WMA_{period}')
                        row[f'WMA_{period}'] = round(wma_val, 8) if wma_val is not None else ''
                    
                    # WMA差值指标 - 统一使用下划线格式
                    diff_indicators = [
                        ('WMA_DIFF_5_20', 'WMA_DIFF_5_20'),
                        ('WMA_DIFF_3_5', 'WMA_DIFF_3_5'),
                        ('WMA_DIFF_5_20_PCT', 'WMA_DIFF_5_20_PCT')
                    ]
                    
                    for possible_keys, csv_column_name in diff_indicators:
                        diff_val = None
                        # 尝试多个可能的字段名
                        for key in possible_keys:
                            if key in wma_values:
                                diff_val = wma_values[key]
                                break
                        
                        if diff_val is not None:
                            # 统一精度标准：8位小数
                            row[csv_column_name] = round(diff_val, 8)
                        else:
                            row[csv_column_name] = ''
                    
                    csv_data.append(row)
                    
                except (KeyError, TypeError) as e:
                    print(f"⚠️ 跳过ETF {result.get('etf_code', 'Unknown')}: 数据格式错误 - {e}")
                    continue
                except Exception as e:
                    print(f"⚠️ 跳过ETF {result.get('etf_code', 'Unknown')}: 处理失败 - {e}")
                    continue
            
            # 确保输出目录存在
            output_dir = os.path.dirname(csv_file)
            if output_dir:
                os.makedirs(output_dir, exist_ok=True)
            
            # 写入CSV文件
            if csv_data:
                with open(csv_file, 'w', newline='', encoding='utf-8-sig') as f:
                    writer = csv.DictWriter(f, fieldnames=csv_data[0].keys())
                    writer.writeheader()
                    writer.writerows(csv_data)
                
                print(f"   📈 简化CSV结构: {len(csv_data)}行 × {len(csv_data[0])}列")
                print(f"   ✅ 成功保存 {len(csv_data)}/{len(results_list)} 个ETF数据")
            else:
                print("⚠️ 没有有效数据可保存到CSV文件")
            
        except FileNotFoundError as e:
            print(f"❌ CSV文件路径错误: {e}")
        except PermissionError as e:
            print(f"❌ CSV文件权限不足: {e}")
        except Exception as e:
            print(f"❌ CSV文件创建失败: {e}")
            # 添加详细错误信息帮助调试
            print(f"   📁 目标文件: {csv_file}")
            print(f"   📊 数据条数: {len(results_list) if results_list else 0}")
    
    def display_results(self, results_list: List[Dict]):
        """显示计算结果摘要 - 保持原有显示逻辑"""
        if not results_list:
            print("❌ 没有有效结果可显示")
            return
        
        print(f"\n📊 WMA计算结果摘要 ({len(results_list)}个ETF)")
        print("=" * 80)
        
        try:
            for i, result in enumerate(results_list, 1):
                try:
                    # 修复adj_type字段访问问题
                    adj_type = result.get('adj_type', self.config.adj_type)
                    
                    print(f"\n{i}. 📈 {result['etf_code']} ({adj_type})")
                    print(f"   📅 日期: {result['latest_price']['date']}")
                    print(f"   💰 价格: {result['latest_price']['close']:.3f} ({result['latest_price']['change_pct']:+.3f}%)")
                    
                    print(f"   🎯 WMA值:", end="")
                    for period in self.config.wma_periods:
                        # 尝试多种可能的字段名
                        wma_val = None
                        for key_format in [f'WMA{period}', f'WMA_{period}']:
                            wma_val = result['wma_values'].get(key_format)
                            if wma_val is not None:
                                break
                        
                        if wma_val:
                            print(f" WMA{period}:{wma_val:.3f}", end="")
                    print()
                    
                    # 显示WMA差值信息 - 统一使用下划线格式
                    wma_values = result['wma_values']
                    
                    # 尝试多种可能的差值字段名
                    wmadiff_5_20 = wma_values.get('WMA_DIFF_5_20')
                    wmadiff_5_20_pct = wma_values.get('WMA_DIFF_5_20_PCT')
                    
                    if wmadiff_5_20 is not None:
                        # 计算相对差值百分比（如果没有现成的）
                        if wmadiff_5_20_pct is None:
                            wma_20 = wma_values.get('WMA_20')
                            if wma_20 and wma_20 != 0:
                                wmadiff_5_20_pct = (wmadiff_5_20 / wma_20) * 100
                        
                        if wmadiff_5_20_pct is not None:
                            trend_indicator = "↗️" if wmadiff_5_20 > 0 else "↘️" if wmadiff_5_20 < 0 else "➡️"
                            print(f"   📊 WMA差值: {wmadiff_5_20:+.6f} ({wmadiff_5_20_pct:+.2f}%) {trend_indicator}")
                        else:
                            print(f"   📊 WMA差值: {wmadiff_5_20:+.6f}")
                    else:
                        print("   📊 WMA差值: 数据不足")
                
                except KeyError as e:
                    print(f"\n{i}. ❌ 无法显示ETF {result.get('etf_code', 'Unknown')}: 缺少关键字段 {str(e)}")
                except Exception as e:
                    print(f"\n{i}. ❌ 无法显示ETF {result.get('etf_code', 'Unknown')}: {str(e)}")
        
        except Exception as e:
            print(f"❌ 显示结果失败: {str(e)}")
    
    def get_result_stats(self, results_list: List[Dict]) -> Dict:
        """获取结果统计信息 - 保持原有统计逻辑"""
        if not results_list:
            return {}
        
        return {
            'total_etfs': len(results_list),
            'successful_calculations': len(results_list)
        }
    
    def create_summary_data(self, results_list: List[Dict]) -> Dict:
        """创建汇总数据 - 保持原有汇总逻辑"""
        return {
            'calculation_summary': {
                'total_etfs': len(results_list),
                'calculation_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'adj_type': self.config.adj_type,
                'wma_periods': self.config.wma_periods,
                'optimization': f'使用所有可用数据，不限制行数',
                'data_source': f'ETF日更/{self.config.ADJ_TYPES[self.config.adj_type]}'
            },
            'results': results_list
        }
    
    def save_screening_batch_results(self, screening_results: Dict, output_dir: str = "data") -> Dict[str, Any]:
        """
        保存筛选批量处理结果 - 保持原有筛选保存逻辑
        
        Args:
            screening_results: 筛选结果字典
            output_dir: 输出目录
            
        Returns:
            Dict[str, Any]: 保存结果统计
        """
        if not screening_results:
            print("❌ 没有筛选结果可保存")
            return {}
        
        # 创建输出目录
        os.makedirs(output_dir, exist_ok=True)
        
        # 生成时间戳
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        saved_files = {}
        total_size = 0
        
        print(f"💾 保存筛选批量处理结果...")
        
        # 为每个门槛保存ETF历史数据文件（与SMA格式一致）
        for threshold, results_list in screening_results.items():
            if not results_list:
                continue
                
            print(f"\n📁 处理{threshold}结果...")
            
            threshold_stats = {
                'files_saved': 0,
                'total_size': 0,
                'failed_saves': 0
            }
            
            # 创建门槛目录
            threshold_dir = os.path.join(output_dir, threshold)
            os.makedirs(threshold_dir, exist_ok=True)
            
            # 为每个ETF保存完整历史数据文件
            for result in results_list:
                etf_code = result['etf_code']
                
                try:
                    # 📊 读取完整历史数据
                    from ..infrastructure.data_reader import WMADataReader
                    data_reader = WMADataReader(self.config)
                    data_result = data_reader.read_etf_data(etf_code)
                    
                    if data_result is None:
                        print(f"   ❌ {etf_code}: 无法读取原始数据")
                        threshold_stats['failed_saves'] += 1
                        continue
                    
                    full_df, _ = data_result
                    
                    # 🚀 使用历史保存方法生成完整WMA历史数据
                    saved_file = self.save_historical_results(
                        etf_code, full_df, result['wma_values'], threshold, output_dir
                    )
                    
                    if saved_file:
                        file_size = os.path.getsize(saved_file)
                        threshold_stats['files_saved'] += 1
                        threshold_stats['total_size'] += file_size
                    else:
                        threshold_stats['failed_saves'] += 1
                        
                except Exception as e:
                    threshold_stats['failed_saves'] += 1
                    print(f"   ❌ {etf_code}: 保存失败 - {e}")
            
            saved_files[f'{threshold}_files'] = threshold_stats['files_saved']
            total_size += threshold_stats['total_size']
            
            print(f"✅ {threshold}: 成功保存 {threshold_stats['files_saved']} 个完整历史文件")
            if threshold_stats['failed_saves'] > 0:
                print(f"⚠️ {threshold}: 失败 {threshold_stats['failed_saves']} 个")
        
        print(f"\n💾 筛选结果保存完成:")
        print(f"   📁 总文件数: {sum(v for k, v in saved_files.items() if '_files' in k)}")
        print(f"   💿 总大小: {total_size / 1024 / 1024:.1f} MB")
        
        return {
            'saved_files': saved_files,
            'total_size_mb': total_size / 1024 / 1024,
            'total_etfs': sum(len(results) for results in screening_results.values())
        }
    
    def _create_screening_summary(self, screening_results: Dict, summary_file: str):
        """创建筛选结果摘要文件"""
        with open(summary_file, 'w', encoding='utf-8') as f:
            f.write("🚀 WMA筛选结果汇总\n")
            f.write("=" * 60 + "\n\n")
            
            f.write(f"📊 筛选汇总:\n")
            f.write(f"   复权类型: {self.config.adj_type}\n")
            f.write(f"   WMA周期: {self.config.wma_periods}\n")
            f.write(f"   计算时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            
            # 各门槛统计
            for threshold, results in screening_results.items():
                f.write(f"📈 {threshold}:\n")
                f.write(f"   ETF数量: {len(results)}\n")
                
                if results:
                    # 趋势统计
                    upward_count = sum(1 for r in results 
                                     if r.get('wma_values', {}).get('WMA_DIFF_5_20', 0) > 0)
                    downward_count = len(results) - upward_count
                    
                    f.write(f"   上升趋势: {upward_count} ({upward_count/len(results)*100:.1f}%)\n")
                    f.write(f"   下降趋势: {downward_count} ({downward_count/len(results)*100:.1f}%)\n")
                
                f.write("\n")
    
    def save_historical_results(self, etf_code: str, full_df: pd.DataFrame, 
                              latest_wma_results: Dict, threshold: str, 
                              output_base_dir: str = "data") -> Optional[str]:
        """
        保存单个ETF的完整历史WMA数据文件 - 保持原有历史保存逻辑
        
        Args:
            etf_code: ETF代码
            full_df: 完整历史数据
            latest_wma_results: 最新WMA计算结果（用于验证）
            threshold: 门槛类型 ("3000万门槛" 或 "5000万门槛")
            output_base_dir: 输出基础目录
            
        Returns:
            Optional[str]: 保存的文件路径 或 None
        """
        try:
            # ---------- 缓存检查：如果历史文件已存在且最新 ----------
            # 生成文件名：直接使用ETF代码（去掉交易所后缀）
            clean_etf_code = etf_code.replace('.SH', '').replace('.SZ', '')
            threshold_dir = os.path.join(output_base_dir, threshold)
            os.makedirs(threshold_dir, exist_ok=True)
            output_file = os.path.join(threshold_dir, f"{clean_etf_code}.csv")

            # 若文件已存在且源数据未更新，则直接返回，避免重复计算
            if os.path.exists(output_file):
                try:
                    # 获取缓存文件更新时间
                    cache_mtime = os.path.getmtime(output_file)
                    # 获取源CSV文件更新时间 (使用DataReader)
                    from ..infrastructure.data_reader import WMADataReader
                    data_reader = WMADataReader(self.config)
                    source_file_path = data_reader.get_etf_file_path(etf_code)
                    if source_file_path and os.path.exists(source_file_path):
                        source_mtime = os.path.getmtime(source_file_path)
                        if cache_mtime >= source_mtime:
                            # 缓存有效，直接返回
                            if not (self.config and self.config.performance_mode):
                                print(f"   💾 {etf_code}: 历史文件已存在且最新，跳过保存")
                            return output_file
                except Exception:
                    # 如果检查失败，继续重新计算保存
                    pass

            # 导入历史数据计算器
            from ..engines.historical_calculator import WMAHistoricalCalculator
            
            # 使用超高性能版本计算完整历史WMA
            historical_calculator = WMAHistoricalCalculator(self.config)
            enhanced_df = historical_calculator.calculate_full_historical_wma_optimized(full_df, etf_code)
            
            if enhanced_df is None or enhanced_df.empty:
                print(f"   ❌ {etf_code}: WMA计算失败")
                return None
            
            # 重新生成 output_file 变量（与缓存检查使用同一文件名）
            output_file = os.path.join(threshold_dir, f"{clean_etf_code}.csv")
            
            # 保存完整历史数据
            enhanced_df.to_csv(output_file, index=False, encoding='utf-8-sig')
            
            file_size = os.path.getsize(output_file)
            rows_count = len(enhanced_df)
            print(f"   💾 {etf_code}: {clean_etf_code}.csv ({rows_count}行, {file_size} 字节)")
            
            return output_file
            
        except Exception as e:
            print(f"   ❌ {etf_code}: 保存完整历史文件失败 - {e}")
            return None 