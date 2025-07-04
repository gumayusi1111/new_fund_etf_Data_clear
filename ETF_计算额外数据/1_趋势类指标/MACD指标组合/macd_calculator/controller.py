#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MACD控制器 - 客观数据专版
=======================

🚫 已简化：只保留客观数据计算，移除主观判断
MACD系统的核心控制器，整合数据计算模块
🎯 功能: 流程控制、数据计算、进度管理、错误处理
📊 接口: 单ETF处理、批量处理、配置管理
🚫 已移除: 信号分析、交易建议等主观判断

"""

import os
import time
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from .config import MACDConfig
from .macd_engine import MACDEngine
# from .signal_analyzer import MACDSignalAnalyzer  # 🚫 已删除主观分析
from .data_processor import MACDDataProcessor
from .result_processor import MACDResultProcessor


class MACDController:
    """MACD控制器 - 客观数据专版"""
    
    def __init__(self, parameter_set: str = 'standard'):
        """
        初始化MACD控制器
        
        Args:
            parameter_set: MACD参数组合 ('standard', 'sensitive', 'smooth')
        """
        self.start_time = time.time()
        
        # 初始化配置
        self.config = MACDConfig(parameter_set)
        
        # 初始化各组件 - 🚫 已移除主观分析组件
        self.macd_engine = MACDEngine(self.config)
        # self.signal_analyzer = MACDSignalAnalyzer(self.config)  # 🚫 已删除主观分析
        self.data_processor = MACDDataProcessor(self.config)
        self.result_processor = MACDResultProcessor(self.config)
        
        print("🎯 MACD控制器初始化完成 (客观数据专版)")
        print(f"⚙️ 参数配置: {parameter_set}")
        print("🚫 已移除: 信号分析、交易建议等主观判断")
        print("=" * 60)
    
    def process_single_etf(self, etf_code: str, threshold_type: str = "3000万门槛") -> Tuple[bool, str]:
        """
        处理单个ETF的MACD计算 - 客观数据专版
        
        Args:
            etf_code: ETF代码
            threshold_type: 门槛类型
            
        Returns:
            (是否成功, 状态信息)
        """
        try:
            print(f"🔄 开始处理 {etf_code}...")
            
            # 1. 数据验证
            is_valid, reason = self.data_processor.validate_etf_for_processing(etf_code)
            if not is_valid:
                return False, f"数据验证失败: {reason}"
            
            # 2. 加载数据
            df = self.data_processor.load_etf_data(etf_code)
            if df is None:
                return False, "数据加载失败"
            
            # 3. MACD计算
            df_with_macd = self.macd_engine.calculate_batch_macd(df)
            if df_with_macd is None or len(df_with_macd) == 0:
                return False, "MACD计算失败"
            
            # 🚫 已移除信号分析步骤 - 只保留客观数据
            # 4. 信号分析
            # df_with_signals = self.signal_analyzer.batch_analyze_historical_data(df_with_macd)
            # if df_with_signals is None or len(df_with_signals) == 0:
            #     return False, "信号分析失败"
            
            # 4. 结果格式化（只包含客观数据）
            formatted_df = self.result_processor.format_macd_results(df_with_macd, etf_code)
            if formatted_df is None or len(formatted_df) == 0:
                return False, "结果格式化失败"
            
            # 5. 格式化并保存结果
            save_success = self.result_processor.save_single_etf_result(
                formatted_df, etf_code, threshold_type
            )
            if not save_success:
                return False, "结果保存失败"
            
            print(f"✅ {etf_code} 处理完成")
            return True, "处理成功"
            
        except Exception as e:
            error_msg = f"处理异常: {str(e)}"
            print(f"❌ {etf_code} {error_msg}")
            return False, error_msg
    
    def process_batch_etfs(self, etf_codes: List[str], threshold_type: str = "3000万门槛") -> Dict:
        """
        批量处理ETF的MACD计算
        
        Args:
            etf_codes: ETF代码列表
            threshold_type: 门槛类型
            
        Returns:
            处理结果统计字典
        """
        print(f"🚀 开始批量处理 {len(etf_codes)} 个ETF...")
        print(f"📊 门槛类型: {threshold_type}")
        print(f"⚙️ MACD参数: EMA{self.config.get_macd_periods()}")
        print("=" * 60)
        
        results_data = {}
        save_status = {}
        batch_start_time = time.time()
        
        for i, etf_code in enumerate(etf_codes, 1):
            print(f"📊 [{i}/{len(etf_codes)}] 处理 {etf_code}...")
            
            success, message = self.process_single_etf(etf_code, threshold_type)
            if success:
                save_status[etf_code] = "成功"
            else:
                save_status[etf_code] = "失败"
                print(f"❌ {etf_code}: {message}")
        
        # 统计结果
        results = {
            'total_etfs': len(etf_codes),
            'successful_etfs': sum(1 for s in save_status.values() if s == "成功"),
            'failed_etfs': sum(1 for s in save_status.values() if s == "失败"),
            'processing_time': time.time() - batch_start_time,
            'save_status': save_status
        }
        
        print("\n" + "=" * 60)
        print("📊 MACD批量处理完成")
        print("=" * 60)
        print(f"📈 总ETF数量: {results['total_etfs']}")
        print(f"✅ 成功处理: {results['successful_etfs']}")
        print(f"❌ 失败处理: {results['failed_etfs']}")
        print(f"⏱️ 处理耗时: {results['processing_time']:.2f} 秒")
        print(f"📊 成功率: {results['successful_etfs']/results['total_etfs']*100:.1f}%")
        
        return results
    
    def process_by_threshold(self, threshold_type: str = "3000万门槛") -> Dict:
        """
        按门槛类型处理ETF
        
        Args:
            threshold_type: 门槛类型 ("3000万门槛" 或 "5000万门槛")
            
        Returns:
            处理结果字典
        """
        print(f"🎯 开始处理 {threshold_type} 的ETF...")
        
        # 获取符合门槛的ETF列表
        qualified_etfs = self.data_processor.filter_qualified_etfs(threshold_type)
        
        if not qualified_etfs:
            print(f"⚠️ 未找到符合 {threshold_type} 的ETF")
            return {'error': '未找到符合条件的ETF'}
        
        # 执行批量处理
        return self.process_batch_etfs(qualified_etfs, threshold_type)
    
    def get_system_status(self) -> Dict:
        """获取系统状态信息"""
        current_time = time.time()
        runtime = current_time - self.start_time
        
        status = {
            'system_name': 'MACD指标计算系统',
            'version': '1.0.0',
            'start_time': datetime.fromtimestamp(self.start_time).strftime('%Y-%m-%d %H:%M:%S'),
            'runtime_seconds': runtime,
            'config_info': self.config.get_system_info(),
            'engine_info': self.macd_engine.get_engine_info(),
            'data_source': self.config.get_data_source_path(),
            'output_directory': self.config.get_output_base_dir()
        }
        
        return status
    
    def test_single_etf(self, etf_code: str = "159696") -> Dict:
        """
        测试单个ETF的处理流程
        
        Args:
            etf_code: 测试用的ETF代码
            
        Returns:
            测试结果
        """
        print(f"🧪 开始测试ETF: {etf_code}")
        
        test_results = {
            'etf_code': etf_code,
            'test_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'steps': {}
        }
        
        # 步骤1: 数据验证
        print("🔍 测试步骤1: 数据验证...")
        is_valid, reason = self.data_processor.validate_etf_for_processing(etf_code)
        test_results['steps']['validation'] = {'success': is_valid, 'message': reason}
        
        if not is_valid:
            return test_results
        
        # 步骤2: 数据加载
        print("📊 测试步骤2: 数据加载...")
        df = self.data_processor.load_etf_data(etf_code)
        test_results['steps']['data_loading'] = {
            'success': df is not None,
            'data_length': len(df) if df is not None else 0
        }
        
        if df is None:
            return test_results
        
        # 步骤3: MACD计算
        print("⚙️ 测试步骤3: MACD计算...")
        try:
            df_with_macd = self.macd_engine.calculate_batch_macd(df)
            test_results['steps']['macd_calculation'] = {
                'success': df_with_macd is not None,
                'has_dif': 'DIF' in df_with_macd.columns if df_with_macd is not None else False,
                'has_dea': 'DEA' in df_with_macd.columns if df_with_macd is not None else False,
                'has_macd': 'MACD' in df_with_macd.columns if df_with_macd is not None else False
            }
        except Exception as e:
            test_results['steps']['macd_calculation'] = {'success': False, 'error': str(e)}
            return test_results
        
        print(f"✅ {etf_code} 测试完成")
        return test_results 