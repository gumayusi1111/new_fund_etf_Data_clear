#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
布林带测试脚本
============

测试布林带计算功能是否正常工作
"""

import sys
import os
import pandas as pd

# 添加路径到系统路径
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)

from bb_calculator.controllers.main_controller import BBMainController


def test_single_etf():
    """测试单个ETF的布林带计算"""
    print("🧪 开始测试布林带计算功能...")
    
    # 初始化控制器
    try:
        controller = BBMainController()
        print("✅ 控制器初始化成功")
    except Exception as e:
        print(f"❌ 控制器初始化失败: {e}")
        return False
    
    # 获取系统状态
    try:
        status = controller.get_system_status()
        print(f"📊 系统状态: {status['system_name']} v{status['version']}")
        print(f"📁 数据目录: {status['paths']['data_dir']}")
        print(f"📁 数据目录存在: {status['paths']['data_dir_exists']}")
        print(f"⚙️  配置: BB({status['config']['bb_period']}, {status['config']['std_multiplier']})")
    except Exception as e:
        print(f"❌ 获取系统状态失败: {e}")
        return False
    
    # 测试ETF代码
    test_etf_codes = ['159201', '510050', '512000', '588000']
    
    for etf_code in test_etf_codes:
        print(f"\n🔍 测试ETF: {etf_code}")
        try:
            result = controller.process_single_etf(etf_code, save_output=False)
            
            if result['success']:
                print(f"✅ {etf_code} 计算成功")
                print(f"⏱️  处理时间: {result['processing_time']:.3f}秒")
                
                # 显示计算结果
                bb_results = result['bb_results']
                print(f"📈 布林带结果:")
                print(f"   中轨: {bb_results.get('bb_middle')}")
                print(f"   上轨: {bb_results.get('bb_upper')}")
                print(f"   下轨: {bb_results.get('bb_lower')}")
                print(f"   带宽: {bb_results.get('bb_width')}%")
                print(f"   位置: {bb_results.get('bb_position')}%")
                print(f"   %B值: {bb_results.get('bb_percent_b')}")
                
                # 验证结果
                validation = result.get('validation_result', {})
                if validation.get('middle_verified'):
                    print("✅ 计算验证通过")
                else:
                    print("⚠️  计算验证失败")
                
                return True
                
            else:
                print(f"❌ {etf_code} 计算失败: {result.get('error', '未知错误')}")
                
        except Exception as e:
            print(f"❌ {etf_code} 处理异常: {e}")
    
    return False


def test_data_reading():
    """测试数据读取功能"""
    print("\n📁 测试数据读取功能...")
    
    try:
        controller = BBMainController()
        
        # 测试读取一个ETF数据
        test_code = '159201'
        etf_data = controller.data_reader.read_etf_data(test_code)
        
        if etf_data is not None and not etf_data.empty:
            print(f"✅ 成功读取 {test_code} 数据")
            print(f"📊 数据行数: {len(etf_data)}")
            print(f"📅 日期范围: {etf_data['日期'].min()} 到 {etf_data['日期'].max()}")
            print(f"💰 价格范围: {etf_data['收盘价'].min():.3f} 到 {etf_data['收盘价'].max():.3f}")
            
            # 显示前几行数据
            print("\n📋 数据样本:")
            print(etf_data[['日期', '收盘价']].head().to_string(index=False))
            
            return True
        else:
            print(f"❌ 无法读取 {test_code} 数据")
            return False
            
    except Exception as e:
        print(f"❌ 数据读取测试失败: {e}")
        return False


def main():
    """主测试函数"""
    print("🚀 布林带系统测试开始")
    print("=" * 50)
    
    # 测试数据读取
    data_test_passed = test_data_reading()
    
    if data_test_passed:
        # 测试布林带计算
        calc_test_passed = test_single_etf()
        
        if calc_test_passed:
            print("\n🎉 所有测试通过！布林带系统运行正常")
            return True
        else:
            print("\n❌ 布林带计算测试失败")
            return False
    else:
        print("\n❌ 数据读取测试失败，无法继续")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)