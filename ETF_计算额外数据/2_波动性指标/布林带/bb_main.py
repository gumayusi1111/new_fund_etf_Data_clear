#\!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
布林带主程序
============

布林带指标计算的主程序入口
支持批量计算、单个ETF分析、系统状态查看等功能
"""

import argparse
import sys
import os
import time
import pandas as pd
from datetime import datetime

# 添加当前目录到sys.path
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, current_dir)

from bb_calculator.controllers.main_controller import BBMainController
from bb_calculator.infrastructure.utils import BBUtils


def main():
    """主函数"""
    print("=" * 60)
    print("🚀 布林带计算器启动")
    print("=" * 60)
    
    # 初始化控制器
    controller = BBMainController()
    
    print("🚀 默认模式：批量计算布林带指标...")
    print("   📊 指标配置：BB(20,2) - 标准布林带")
    print("   📈 包含指标：中轨、上轨、下轨、带宽、位置、%B")
    print("   🗂️ 输出目录：data/{threshold}/")
    
    # 处理所有门槛
    thresholds = ["3000万门槛", "5000万门槛"]
    all_results = {}
    total_etfs = 0
    total_success = 0
    
    for threshold in thresholds:
        print(f"\n🔄 处理 {threshold}...")
        
        # 使用控制器的批量处理方法
        result = controller.calculate_screening_results([threshold])
        all_results[threshold] = result
        
        if result["success"]:
            threshold_detail = result["threshold_details"].get(threshold, {})
            if threshold_detail.get("success"):
                total_etfs += threshold_detail["successful_etfs"] + threshold_detail["failed_etfs"]
                total_success += threshold_detail["successful_etfs"]
                
                print(f"✅ {threshold} 处理完成")
                print(f"   📊 总数: {threshold_detail['successful_etfs'] + threshold_detail['failed_etfs']}")
                print(f"   ✅ 成功: {threshold_detail['successful_etfs']}")
                print(f"   ❌ 失败: {threshold_detail['failed_etfs']}")
    
    # 显示汇总结果
    print(f"\n🎉 批量计算完成！")
    print(f"📊 总计处理: {total_etfs} 个ETF")
    print(f"✅ 成功计算: {total_success} 个")
    if total_etfs > 0:
        print(f"📈 整体成功率: {(total_success/total_etfs*100):.1f}%")
    
    print("\n" + "=" * 60)
    print("✅ 程序执行完成")
    print("=" * 60)


if __name__ == "__main__":
    main()
