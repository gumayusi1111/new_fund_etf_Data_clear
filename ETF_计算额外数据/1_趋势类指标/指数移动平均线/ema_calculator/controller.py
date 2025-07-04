#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
EMA主控制器 - 中短期专版
======================

协调所有EMA计算模块的主控制器
提供统一的计算接口和批量处理功能
"""

import pandas as pd
from typing import Dict, List, Optional, Tuple
from .config import EMAConfig
from .data_reader import ETFDataReader
from .ema_engine import EMAEngine
# from .signal_analyzer import SignalAnalyzer  # 🚫 已移除复杂分析
from .result_processor import ResultProcessor
from .file_manager import FileManager


class EMAController:
    """EMA主控制器 - 中短期专版"""
    
    def __init__(self, adj_type: str = "前复权", ema_periods: Optional[List[int]] = None):
        """
        初始化EMA控制器
        
        Args:
            adj_type: 复权类型
            ema_periods: EMA周期列表
        """
        print("🚀 EMA控制器启动中...")
        
        # 初始化配置
        self.config = EMAConfig(adj_type, ema_periods)
        
        # 初始化各个模块
        self.data_reader = ETFDataReader(self.config)
        self.ema_engine = EMAEngine(self.config)
        # self.signal_analyzer = SignalAnalyzer(self.config)  # 🚫 已移除复杂分析
        self.result_processor = ResultProcessor(self.config)
        self.file_manager = FileManager(self.config)
        
        print("✅ EMA控制器初始化完成")
        print(f"   📊 {self.config.get_ema_display_info()}")
    
    def calculate_single_etf(self, etf_code: str, save_result: bool = True, 
                           threshold: str = "3000万门槛", verbose: bool = False) -> Optional[Dict]:
        """
        计算单个ETF的EMA指标
        
        Args:
            etf_code: ETF代码
            save_result: 是否保存结果到文件
            threshold: 门槛类型（用于文件输出目录）
            verbose: 是否显示详细输出
            
        Returns:
            Dict: 计算结果或None
        """
        try:
            print(f"\n🔢 开始计算 {etf_code} 的EMA指标...")
            
            # 1. 验证ETF代码
            if not self.data_reader.validate_etf_code(etf_code):
                print(f"❌ ETF代码无效: {etf_code}")
                return None
            
            # 2. 读取数据
            data_result = self.data_reader.read_etf_data(etf_code)
            if not data_result:
                print(f"❌ 数据读取失败: {etf_code}")
                return None
            
            df, total_rows = data_result
            
            # 3. 计算EMA值（只计算一次）
            ema_values = self.ema_engine.calculate_ema_values(df)
            if not ema_values:
                print(f"❌ EMA计算失败: {etf_code}")
                return None
            
            # 4. 获取价格信息
            price_info = self.data_reader.get_latest_price_info(df)
            
            # 5. 🚫 简化信号分析 - 只保留基础数据
            signals = {
                'status': 'simplified'  # 标记为简化模式
            }
            
            # 6. 验证结果（传入预计算的EMA值）
            if not self.result_processor.validate_result_data(etf_code, price_info, ema_values, signals):
                print(f"❌ 结果验证失败: {etf_code}")
                return None
            
            # 7. 格式化输出
            console_output = self.result_processor.format_console_output(
                etf_code, price_info, ema_values, signals
            )
            
            # 8. 保存结果
            csv_content = None
            if save_result:
                csv_header = self.result_processor.get_csv_header()
                csv_row = self.result_processor.format_ema_result_row(
                    etf_code, price_info, ema_values, signals
                )
                csv_content = f"{csv_header}\n{csv_row}"
                
                # 保存到文件
                success = self.file_manager.save_etf_result(etf_code, csv_content, threshold)
                if not success:
                    print(f"⚠️  文件保存失败: {etf_code}")
            
            # 9. 显示结果
            if verbose:
                print(console_output)
            else:
                # 🚫 简化输出 - 只显示基础信息
                print(f"✅ {etf_code}: EMA计算完成")
            
            # 10. 构建返回结果
            result = {
                'etf_code': etf_code,
                'success': True,
                'price_info': price_info,
                'ema_values': ema_values,
                'signals': signals,
                'console_output': console_output,
                'csv_content': csv_content,
                'total_rows': total_rows
            }
            
            print(f"✅ {etf_code} EMA计算完成")
            return result
            
        except Exception as e:
            print(f"❌ {etf_code} 计算失败: {str(e)}")
            return {
                'etf_code': etf_code,
                'success': False,
                'error': str(e)
            }
    
    def calculate_screening_results(self, threshold: str = "3000万门槛", 
                                  max_etfs: Optional[int] = None, verbose: bool = False) -> Dict:
        """
        计算筛选结果中的所有ETF（生成完整历史数据文件）
        
        Args:
            threshold: 门槛类型
            max_etfs: 最大处理ETF数量（测试用）
            verbose: 是否显示详细输出
            
        Returns:
            Dict: 批量计算结果
        """
        try:
            print(f"\n🎯 开始批量计算 {threshold} 的EMA指标...")
            
            # 1. 获取筛选结果
            etf_codes = self.data_reader.get_screening_etf_codes(threshold)
            if not etf_codes:
                print(f"❌ 无法获取{threshold}的筛选结果")
                return {'success': False, 'error': '无筛选结果'}
            
            # 限制数量（测试用）
            if max_etfs and len(etf_codes) > max_etfs:
                etf_codes = etf_codes[:max_etfs]
                print(f"⚠️  限制处理数量为 {max_etfs} 个ETF")
            
            print(f"📋 共需处理 {len(etf_codes)} 个ETF")
            
            # 2. 批量计算（不保存单行文件，只收集结果）
            results = []
            success_count = 0
            
            for i, etf_code in enumerate(etf_codes, 1):
                print(f"\n📊 进度: {i}/{len(etf_codes)} - {etf_code}")
                
                result = self.calculate_single_etf(
                    etf_code, 
                    save_result=False,  # 不保存单行文件
                    threshold=threshold, 
                    verbose=verbose
                )
                
                if result:
                    results.append(result)
                    if result.get('success', False):
                        success_count += 1
            
            # 3. 📊 生成完整历史数据文件（模仿SMA/WMA）
            print(f"\n💾 开始生成完整历史数据文件...")
            
            save_stats = {
                'total_files_saved': 0,
                'total_size_bytes': 0,
                'thresholds': {threshold: {'files_saved': 0, 'total_size': 0, 'failed_saves': 0}}
            }
            
            # 为每个成功的ETF生成完整历史文件
            for result in results:
                if result.get('success', False):
                    etf_code = result['etf_code']
                    print(f"   📊 处理 {etf_code} 的完整历史数据...")
                    
                    # 重新读取完整历史数据（不限制行数）
                    data_result = self.data_reader.read_etf_data(etf_code)
                    if data_result:
                        full_df, _ = data_result
                        
                        # 保存完整历史EMA文件
                        saved_file = self.result_processor.save_historical_results(
                            etf_code, 
                            full_df, 
                            result['ema_values'], 
                            threshold,
                            result['signals'].get('arrangement', {}).get('arrangement', ''),
                            self.config.default_output_dir
                        )
                        
                        if saved_file:
                            import os
                            file_size = os.path.getsize(saved_file)
                            save_stats['total_files_saved'] += 1
                            save_stats['total_size_bytes'] += file_size
                            save_stats['thresholds'][threshold]['files_saved'] += 1
                            save_stats['thresholds'][threshold]['total_size'] += file_size
                        else:
                            save_stats['thresholds'][threshold]['failed_saves'] += 1
                    else:
                        save_stats['thresholds'][threshold]['failed_saves'] += 1
                        print(f"   ❌ {etf_code}: 无法读取完整历史数据")
            
            # 4. 生成统计
            stats = self.result_processor.create_summary_stats(results)
            
            # 5. 显示摘要
            summary_display = self.result_processor.format_summary_display(stats)
            print(summary_display)
            
            # 6. 显示保存统计
            if save_stats:
                print(f"\n📁 文件保存统计:")
                print(f"   ✅ 成功文件: {save_stats['total_files_saved']} 个")
                print(f"   💿 总大小: {save_stats['total_size_bytes'] / 1024 / 1024:.1f} MB")
                print(f"   📊 文件类型: 完整历史EMA数据")
            
            return {
                'success': True,
                'threshold': threshold,
                'results': results,
                'stats': stats,
                'save_stats': save_stats,
                'processed_count': len(etf_codes),
                'success_count': success_count
            }
            
        except Exception as e:
            print(f"❌ 批量计算失败: {str(e)}")
            return {'success': False, 'error': str(e)}
    
    def quick_analysis(self, etf_code: str) -> Optional[str]:
        """
        快速分析模式（不保存文件）
        
        Args:
            etf_code: ETF代码
            
        Returns:
            str: 分析结果文本或None
        """
        try:
            print(f"⚡ 快速分析 {etf_code}...")
            
            result = self.calculate_single_etf(
                etf_code, 
                save_result=False, 
                verbose=True
            )
            
            if result and result.get('success', False):
                return result.get('console_output', '')
            else:
                return None
                
        except Exception as e:
            print(f"❌ 快速分析失败: {str(e)}")
            return None
    
    def get_system_status(self) -> Dict:
        """
        获取系统状态
        
        Returns:
            Dict: 系统状态信息
        """
        try:
            # 验证数据路径
            data_path_valid = self.config.validate_data_path()
            
            # 获取可用ETF数量
            available_etfs = len(self.data_reader.get_available_etfs())
            
            # 检查输出目录状态
            dir_stats_3000 = self.file_manager.get_directory_stats("3000万门槛")
            dir_stats_5000 = self.file_manager.get_directory_stats("5000万门槛")
            
            return {
                'config': {
                    'adj_type': self.config.adj_type,
                    'ema_periods': self.config.ema_periods,
                    'required_rows': self.config.required_rows
                },
                'data_status': {
                    'path_valid': data_path_valid,
                    'available_etfs': available_etfs,
                    'data_dir': self.config.data_dir
                },
                'output_status': {
                    '3000万门槛': dir_stats_3000,
                    '5000万门槛': dir_stats_5000
                }
            }
            
        except Exception as e:
            print(f"⚠️  获取系统状态失败: {str(e)}")
            return {'error': str(e)}
    
    def show_system_status(self) -> None:
        """显示系统状态"""
        try:
            status = self.get_system_status()
            
            print(f"""
🔧 EMA系统状态:
   📊 配置信息:
      复权类型: {status['config']['adj_type']}
      EMA周期: {status['config']['ema_periods']}
      数据要求: {status['config']['required_rows']}行
      
   📁 数据状态:
      数据路径: {'✅ 有效' if status['data_status']['path_valid'] else '❌ 无效'}
      可用ETF: {status['data_status']['available_etfs']} 个
      数据目录: {status['data_status']['data_dir']}
      
   📂 输出状态:""")
            
            for threshold, stats in status['output_status'].items():
                if stats.get('exists', False):
                    print(f"      {threshold}: {stats['file_count']} 个文件 ({stats['total_size_mb']}MB)")
                else:
                    print(f"      {threshold}: 目录不存在")
            
        except Exception as e:
            print(f"⚠️  显示状态失败: {str(e)}")
    
    def validate_ema_calculation(self, etf_code: str) -> bool:
        """
        验证EMA计算的正确性
        
        Args:
            etf_code: ETF代码
            
        Returns:
            bool: 计算是否正确
        """
        try:
            print(f"🔬 验证 {etf_code} 的EMA计算...")
            
            # 读取数据
            data_result = self.data_reader.read_etf_data(etf_code)
            if not data_result:
                return False
            
            df, _ = data_result
            
            # 计算EMA值并验证
            ema_values = self.ema_engine.calculate_ema_values(df)
            return self.ema_engine.validate_ema_calculation(df, ema_values)
            
        except Exception as e:
            print(f"❌ 验证失败: {str(e)}")
            return False 