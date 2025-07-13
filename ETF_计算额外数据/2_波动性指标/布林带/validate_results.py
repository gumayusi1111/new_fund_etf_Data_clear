#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
布林带结果验证脚本
================

手动验证布林带计算结果的准确性
"""

import sys
import os
import pandas as pd
import numpy as np

# 添加路径到系统路径
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)

from bb_calculator.controllers.main_controller import BBMainController


def manual_bollinger_calculation(prices, period=20, std_multiplier=2.0):
    """
    手动计算布林带（独立验证算法）
    """
    if len(prices) < period:
        return None
    
    # 使用最后20个价格
    recent_prices = prices.tail(period)
    
    # 中轨：简单移动平均
    middle = recent_prices.mean()
    
    # 标准差（样本标准差，ddof=1）
    std_dev = recent_prices.std(ddof=1)
    
    # 上下轨
    upper = middle + (std_multiplier * std_dev)
    lower = middle - (std_multiplier * std_dev)
    
    # 当前价格（最后一个）
    current_price = float(prices.iloc[-1])
    
    # 带宽
    width = ((upper - lower) / middle) * 100
    
    # 价格位置
    position = ((current_price - lower) / (upper - lower)) * 100
    
    # %B指标
    percent_b = (current_price - lower) / (upper - lower)
    
    return {
        'middle': middle,
        'upper': upper,
        'lower': lower,
        'width': width,
        'position': position,
        'percent_b': percent_b,
        'current_price': current_price,
        'std_dev': std_dev
    }


def verify_calculation(etf_code='159201'):
    """验证布林带计算结果"""
    print(f"🔍 验证ETF {etf_code} 的布林带计算...")
    
    # 使用系统计算
    controller = BBMainController()
    
    # 读取原始数据
    etf_data = controller.data_reader.read_etf_data(etf_code)
    if etf_data is None or etf_data.empty:
        print(f"❌ 无法读取 {etf_code} 数据")
        return False
    
    print(f"📊 数据概览:")
    print(f"   总行数: {len(etf_data)}")
    print(f"   最新价格: {etf_data['收盘价'].iloc[-1]}")
    print(f"   最新日期: {etf_data['日期'].iloc[-1]}")
    
    # 系统计算结果
    system_result = controller.process_single_etf(etf_code, save_output=False)
    if not system_result['success']:
        print(f"❌ 系统计算失败: {system_result.get('error')}")
        return False
    
    system_bb = system_result['bb_results']
    print(f"\n🤖 系统计算结果:")
    print(f"   中轨: {system_bb.get('bb_middle')}")
    print(f"   上轨: {system_bb.get('bb_upper')}")
    print(f"   下轨: {system_bb.get('bb_lower')}")
    print(f"   带宽: {system_bb.get('bb_width')}%")
    print(f"   位置: {system_bb.get('bb_position')}%")
    print(f"   %B值: {system_bb.get('bb_percent_b')}")
    
    # 手动验证计算
    manual_result = manual_bollinger_calculation(etf_data['收盘价'])
    if manual_result is None:
        print("❌ 手动计算失败")
        return False
    
    print(f"\n✋ 手动计算结果:")
    print(f"   中轨: {manual_result['middle']:.8f}")
    print(f"   上轨: {manual_result['upper']:.8f}")
    print(f"   下轨: {manual_result['lower']:.8f}")
    print(f"   带宽: {manual_result['width']:.8f}%")
    print(f"   位置: {manual_result['position']:.8f}%")
    print(f"   %B值: {manual_result['percent_b']:.8f}")
    print(f"   标准差: {manual_result['std_dev']:.8f}")
    
    # 对比差异
    print(f"\n📊 计算差异分析:")
    
    tolerance = 1e-6
    errors = []
    
    # 中轨差异
    middle_diff = abs(system_bb['bb_middle'] - manual_result['middle'])
    print(f"   中轨差异: {middle_diff:.10f}")
    if middle_diff > tolerance:
        errors.append(f"中轨差异过大: {middle_diff}")
    
    # 上轨差异
    upper_diff = abs(system_bb['bb_upper'] - manual_result['upper'])
    print(f"   上轨差异: {upper_diff:.10f}")
    if upper_diff > tolerance:
        errors.append(f"上轨差异过大: {upper_diff}")
    
    # 下轨差异
    lower_diff = abs(system_bb['bb_lower'] - manual_result['lower'])
    print(f"   下轨差异: {lower_diff:.10f}")
    if lower_diff > tolerance:
        errors.append(f"下轨差异过大: {lower_diff}")
    
    # 带宽差异
    width_diff = abs(system_bb['bb_width'] - manual_result['width'])
    print(f"   带宽差异: {width_diff:.10f}%")
    if width_diff > tolerance:
        errors.append(f"带宽差异过大: {width_diff}%")
    
    # 位置差异
    position_diff = abs(system_bb['bb_position'] - manual_result['position'])
    print(f"   位置差异: {position_diff:.10f}%")
    if position_diff > tolerance:
        errors.append(f"位置差异过大: {position_diff}%")
    
    # %B差异
    percent_b_diff = abs(system_bb['bb_percent_b'] - manual_result['percent_b'])
    print(f"   %B差异: {percent_b_diff:.10f}")
    if percent_b_diff > tolerance:
        errors.append(f"%B差异过大: {percent_b_diff}")
    
    # 结果判断
    if not errors:
        print(f"\n✅ 验证通过！所有计算结果在容差范围内（{tolerance}）")
        print("🎯 布林带算法实现正确，计算精度符合要求")
        return True
    else:
        print(f"\n❌ 验证失败！发现以下问题:")
        for error in errors:
            print(f"   - {error}")
        return False


def test_multiple_etfs():
    """测试多个ETF的计算准确性"""
    test_etfs = ['159201', '510050', '512000']
    
    print("🚀 开始批量验证布林带计算...")
    print("=" * 60)
    
    passed = 0
    total = len(test_etfs)
    
    for etf_code in test_etfs:
        print(f"\n{'='*20} {etf_code} {'='*20}")
        try:
            if verify_calculation(etf_code):
                passed += 1
                print(f"✅ {etf_code} 验证通过")
            else:
                print(f"❌ {etf_code} 验证失败")
        except Exception as e:
            print(f"❌ {etf_code} 验证异常: {e}")
    
    print(f"\n{'='*60}")
    print(f"📊 验证汇总: {passed}/{total} 通过")
    print(f"📈 成功率: {(passed/total)*100:.1f}%")
    
    if passed == total:
        print("🎉 所有ETF验证通过！布林带计算系统完全正确")
        return True
    else:
        print("⚠️  部分ETF验证失败，需要检查算法实现")
        return False


if __name__ == "__main__":
    success = test_multiple_etfs()
    sys.exit(0 if success else 1)