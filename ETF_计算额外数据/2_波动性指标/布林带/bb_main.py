#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
布林带多参数集批量计算主程序
==========================

支持短周期(10,2)和标准(20,2)两套参数分别生成完整数据
支持参数集分层目录结构，匹配MACD模式
"""

import sys
import os
import time
from datetime import datetime

# 添加当前目录到sys.path
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, current_dir)

from bb_calculator.controllers.main_controller import BBMainController
from bb_calculator.infrastructure.config import BBConfig

def main():
    """多参数集批量计算主函数"""
    
    # 检查是否是状态查询
    if len(sys.argv) > 1 and sys.argv[1] == '--status':
        config = BBConfig()
        controller = BBMainController()
        status = controller.get_system_status()
        
        print("📊 布林带指标计算器 - 基于波动率指标标准")
        print("📈 支持多参数集(短周期、标准)，智能缓存")
        print("🗂️ 参数分层目录结构，兼容MACD模式")
        print("=" * 60)
        print(f"📊 系统状态信息:")
        print(f"   🔧 系统版本: {status['version']}")
        print(f"   📁 数据路径: {status['config']['adj_type']}")
        print(f"   📊 可用ETF: {len(status.get('available_etfs', []))}个")
        print(f"   🗂️ 缓存状态: Ready")
        print(f"   🎯 参数集: {', '.join(config.get_available_param_sets())}")
        return
    
    print("=" * 60)
    print("🚀 布林带多参数集批量计算系统启动")
    print("=" * 60)
    
    # 获取可用参数集
    config = BBConfig()
    param_sets = config.get_available_param_sets()
    thresholds = ["3000万门槛", "5000万门槛"]
    
    print(f"📊 参数集: {', '.join(param_sets)}")
    print(f"🎯 门槛: {', '.join(thresholds)}")
    print("📁 输出结构: cache/门槛/参数集/ETF文件.csv")
    
    all_results = {}
    total_start_time = time.time()
    
    # 遍历每个参数集
    for param_set_name in param_sets:
        print(f"\n🔄 开始处理参数集: {param_set_name}")
        param_start_time = time.time()
        
        # 创建配置并设置参数集
        param_config = BBConfig()
        param_config.set_param_set(param_set_name)
        
        print(f"   {param_config.get_bb_display_info()}")
        
        # 确保目录结构存在
        param_config.ensure_directories_exist()
        
        # 创建控制器并更新配置
        controller = BBMainController()
        controller.update_config(param_config)
        
        param_results = {}
        
        # 处理每个门槛
        for threshold in thresholds:
            print(f"\n   📈 计算 {threshold}...")
            
            # 执行批量计算
            result = controller.calculate_and_save_screening_results([threshold])
            param_results[threshold] = result
            
            if result.get("success"):
                threshold_detail = result.get("threshold_details", {}).get(threshold, {})
                if threshold_detail.get("success"):
                    successful = threshold_detail.get("successful_etfs", 0)
                    failed = threshold_detail.get("failed_etfs", 0)
                    total = successful + failed
                    
                    print(f"      ✅ 成功: {successful}/{total} ({successful/total*100:.1f}%)")
                    
                    # 保存到参数集专属目录
                    try:
                        csv_saved = controller.csv_handler.create_batch_csv(
                            threshold_detail, threshold, param_set_name
                        )
                        if csv_saved:
                            print(f"      💾 数据已保存到: data/{threshold}/{param_set_name}/")
                    except Exception as e:
                        print(f"      ⚠️ 保存警告: {str(e)}")
                else:
                    print(f"      ❌ {threshold} 计算失败")
            else:
                print(f"      ❌ {threshold} 批量处理失败")
        
        param_end_time = time.time()
        param_duration = param_end_time - param_start_time
        
        all_results[param_set_name] = param_results
        print(f"\n✅ {param_set_name} 完成 (耗时: {param_duration:.1f}秒)")
    
    # 最终汇总
    total_end_time = time.time()
    total_duration = total_end_time - total_start_time
    
    print("\n" + "=" * 60)
    print("🎉 多参数集批量计算完成！")
    print("=" * 60)
    
    for param_set_name, param_results in all_results.items():
        print(f"\n📊 {param_set_name} 汇总:")
        total_success = 0
        total_count = 0
        
        for threshold, result in param_results.items():
            if result.get("success"):
                threshold_detail = result.get("threshold_details", {}).get(threshold, {})
                if threshold_detail.get("success"):
                    successful = threshold_detail.get("successful_etfs", 0)
                    failed = threshold_detail.get("failed_etfs", 0)
                    total_success += successful
                    total_count += successful + failed
                    print(f"   {threshold}: {successful}/{successful + failed}")
        
        if total_count > 0:
            print(f"   总成功率: {total_success}/{total_count} ({total_success/total_count*100:.1f}%)")
    
    print(f"\n⏱️ 总耗时: {total_duration:.1f}秒")
    print("📁 数据位置:")
    print("   • cache/门槛/参数集/ETF文件.csv (缓存)")
    print("   • data/门槛/参数集/ETF文件.csv (输出)")
    
    print("\n" + "=" * 60)
    print("✅ 系统运行完成")
    print("=" * 60)

if __name__ == "__main__":
    main()
