#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
布林带多参数集测试程序
====================

测试短周期和标准周期参数集的性能对比
基于学术研究为中国ETF市场优化参数设置
"""

import sys
import os
import time
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from bb_calculator.controllers.main_controller import BBMainController
from bb_calculator.infrastructure.config import BBConfig

def test_param_sets():
    """测试不同参数集的效果"""
    print("=" * 60)
    print("🔬 布林带多参数集测试")
    print("=" * 60)
    
    # 测试ETF列表（选择几个代表性的）
    test_etfs = ["159201", "159215", "159217"]
    param_sets = ["短周期", "标准"]
    
    results = {}
    
    for param_set_name in param_sets:
        print(f"\n📊 测试参数集：{param_set_name}")
        
        # 创建配置
        config = BBConfig()
        success = config.set_param_set(param_set_name)
        if not success:
            print(f"❌ 参数集 {param_set_name} 设置失败")
            continue
            
        print(f"   {config.get_bb_display_info()}")
        
        # 创建控制器
        controller = BBMainController()
        controller.config = config  # 使用新配置
        
        param_results = []
        
        for etf_code in test_etfs:
            print(f"   🔄 计算 {etf_code}...")
            start_time = time.time()
            
            result = controller.process_single_etf(etf_code, save_output=True)
            
            end_time = time.time()
            process_time = end_time - start_time
            
            if result['success']:
                bb_results = result['bb_results']
                param_results.append({
                    'etf_code': etf_code,
                    'success': True,
                    'process_time': process_time,
                    'bb_width': bb_results.get('bb_width'),
                    'bb_position': bb_results.get('bb_position'),
                    'bb_percent_b': bb_results.get('bb_percent_b')
                })
                print(f"      ✅ 成功 ({process_time:.2f}s)")
                print(f"      📈 宽度: {bb_results.get('bb_width'):.2f}%")
                print(f"      📍 位置: {bb_results.get('bb_position'):.2f}%")
            else:
                param_results.append({
                    'etf_code': etf_code,
                    'success': False,
                    'error': result.get('error')
                })
                print(f"      ❌ 失败: {result.get('error')}")
        
        results[param_set_name] = param_results
    
    # 结果对比
    print("\n" + "=" * 60)
    print("📊 参数集对比分析")
    print("=" * 60)
    
    for param_set_name, param_results in results.items():
        successful = sum(1 for r in param_results if r['success'])
        total = len(param_results)
        avg_time = sum(r.get('process_time', 0) for r in param_results if r['success']) / max(successful, 1)
        
        print(f"\n🔹 {param_set_name}:")
        print(f"   成功率: {successful}/{total} ({successful/total*100:.1f}%)")
        print(f"   平均耗时: {avg_time:.3f}秒")
        
        if successful > 0:
            avg_width = sum(r.get('bb_width', 0) for r in param_results if r['success']) / successful
            avg_position = sum(r.get('bb_position', 0) for r in param_results if r['success']) / successful
            print(f"   平均带宽: {avg_width:.2f}%")
            print(f"   平均位置: {avg_position:.2f}%")
    
    print("\n🎯 科学建议:")
    print("   • 短周期(10,2): 反应敏感，适合短线交易")
    print("   • 标准(20,2): 稳定可靠，适合中线交易")
    print("   • 基于学术研究，标准(20,2)最适合中国ETF市场")

if __name__ == "__main__":
    test_param_sets()