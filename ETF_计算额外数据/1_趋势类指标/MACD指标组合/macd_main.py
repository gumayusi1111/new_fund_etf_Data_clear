#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MACD指标组合系统 - 主程序 (客观数据专版)
=====================================

🚫 已简化：只保留客观数据计算，移除主观判断
专业的MACD技术指标计算系统
🎯 功能: DIF+DEA+MACD三线组合计算
📊 输出: 完整的MACD指标数据（纯客观数值）
⚙️ 参数: 支持标准(12,26,9)、敏感(8,17,9)、平滑(19,39,9)配置
🚫 已移除: 金叉死叉识别、零轴分析、交易信号等主观判断

"""

import sys
import os
from datetime import datetime

# 添加模块路径
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, current_dir)

from macd_calculator.controller import MACDController


def print_welcome_banner():
    """打印欢迎信息"""
    print("=" * 70)
    print(" " * 10 + "MACD指标组合计算系统 (客观数据专版)")
    print("=" * 70)
    print("🎯 Moving Average Convergence Divergence Analysis")
    print("📊 专业技术指标: DIF + DEA + MACD (纯客观数据)")
    print("⚙️ 三种参数配置: 标准(12,26,9) | 敏感(8,17,9) | 平滑(19,39,9)")
    print("🚫 已移除主观判断: 信号分析、交易建议、金叉死叉等")
    print("=" * 70)
    print()


def print_menu():
    """打印菜单选项"""
    print("📋 功能菜单:")
    print("0️⃣  批量生成所有配置数据 🔥 (推荐)")
    print("1️⃣  处理3000万门槛ETF (标准参数)")
    print("2️⃣  处理5000万门槛ETF (标准参数)")
    print("3️⃣  处理3000万门槛ETF (敏感参数)")
    print("4️⃣  处理5000万门槛ETF (敏感参数)")
    print("5️⃣  处理3000万门槛ETF (平滑参数)")
    print("6️⃣  处理5000万门槛ETF (平滑参数)")
    print("7️⃣  测试单个ETF")
    print("8️⃣  系统状态检查")
    print("9️⃣  退出程序")
    print("-" * 50)


def main():
    """主程序入口"""
    print_welcome_banner()
    
    while True:
        print_menu()
        choice = input("请选择功能 (0-9): ").strip()
        
        try:
            if choice == '0':
                # 批量生成所有配置数据
                batch_generate_all_configs()
                
            elif choice == '1':
                # 3000万门槛 - 标准参数
                print("🚀 开始处理3000万门槛ETF (标准参数)...")
                controller = MACDController('standard')
                result = controller.process_by_threshold("3000万门槛")
                
            elif choice == '2':
                # 5000万门槛 - 标准参数
                print("🚀 开始处理5000万门槛ETF (标准参数)...")
                controller = MACDController('standard')
                result = controller.process_by_threshold("5000万门槛")
                
            elif choice == '3':
                # 3000万门槛 - 敏感参数
                print("🚀 开始处理3000万门槛ETF (敏感参数)...")
                controller = MACDController('sensitive')
                result = controller.process_by_threshold("3000万门槛")
                
            elif choice == '4':
                # 5000万门槛 - 敏感参数
                print("🚀 开始处理5000万门槛ETF (敏感参数)...")
                controller = MACDController('sensitive')
                result = controller.process_by_threshold("5000万门槛")
                
            elif choice == '5':
                # 3000万门槛 - 平滑参数
                print("🚀 开始处理3000万门槛ETF (平滑参数)...")
                controller = MACDController('smooth')
                result = controller.process_by_threshold("3000万门槛")
                
            elif choice == '6':
                # 5000万门槛 - 平滑参数
                print("🚀 开始处理5000万门槛ETF (平滑参数)...")
                controller = MACDController('smooth')
                result = controller.process_by_threshold("5000万门槛")
                
            elif choice == '7':
                # 测试单个ETF
                controller = MACDController('standard')
                
                etf_code = input("请输入ETF代码 (默认159696): ").strip()
                if not etf_code:
                    etf_code = "159696"
                
                print(f"🧪 开始测试ETF: {etf_code}")
                test_result = controller.test_single_etf(etf_code)
                
                print("\n📊 测试结果:")
                for step, details in test_result['steps'].items():
                    print(f"  {step}: {details}")
                
            elif choice == '8':
                # 系统状态检查
                controller = MACDController('standard')
                status = controller.get_system_status()
                
                print("\n📊 系统状态信息:")
                print(f"  系统名称: {status['system_name']}")
                print(f"  版本号: {status['version']}")
                print(f"  启动时间: {status['start_time']}")
                print(f"  运行时长: {status['runtime_seconds']:.2f} 秒")
                print(f"  数据源: {status['data_source']}")
                print(f"  输出目录: {status['output_directory']}")
                
            elif choice == '9':
                # 退出程序
                print("👋 感谢使用MACD指标计算系统，再见！")
                break
                
            else:
                print("❌ 无效选择，请输入0-9之间的数字")
                continue
                
        except KeyboardInterrupt:
            print("\n\n⚠️ 用户中断程序执行")
            break
            
        except Exception as e:
            print(f"❌ 程序执行出错: {e}")
            print("请检查系统状态或联系技术支持")
        
        # 询问是否继续
        print("\n" + "=" * 50)
        continue_choice = input("是否继续使用? (y/n): ").strip().lower()
        if continue_choice in ['n', 'no', '否']:
            print("👋 感谢使用MACD指标计算系统，再见！")
            break
        print()


def quick_test():
    """快速测试模式"""
    print("🧪 MACD系统快速测试模式")
    print("=" * 50)
    
    try:
        # 初始化控制器
        controller = MACDController('standard')
        
        # 测试ETF
        test_etf = "159696"
        print(f"📊 测试ETF: {test_etf}")
        
        test_result = controller.test_single_etf(test_etf)
        
        print("✅ 测试完成")
        print(f"📊 测试结果: {test_result}")
        
    except Exception as e:
        print(f"❌ 快速测试失败: {e}")


def batch_generate_all_configs():
    """批量生成所有配置的MACD数据"""
    print("🚀 开始批量生成所有配置的MACD数据...")
    print("=" * 70)
    
    # 配置参数组合
    configs = [
        ('standard', '标准参数(12,26,9)'),
        ('sensitive', '敏感参数(8,17,9)'),
        ('smooth', '平滑参数(19,39,9)')
    ]
    
    thresholds = ['3000万门槛', '5000万门槛']
    
    total_tasks = len(configs) * len(thresholds)
    current_task = 0
    
    print(f"📊 总共需要处理 {total_tasks} 个任务")
    print("=" * 70)
    
    for config_name, config_desc in configs:
        for threshold in thresholds:
            current_task += 1
            print(f"\n🔄 [{current_task}/{total_tasks}] 处理 {threshold} - {config_desc}")
            print("-" * 60)
            
            try:
                controller = MACDController(config_name)
                result = controller.process_by_threshold(threshold)
                
                if result.get('error'):
                    print(f"❌ 失败: {result['error']}")
                else:
                    success_count = result.get('successful_etfs', 0)
                    total_count = result.get('total_etfs', 0)
                    print(f"✅ 完成: {success_count}/{total_count} 个ETF处理成功")
                    
            except Exception as e:
                print(f"❌ 处理异常: {e}")
            
            print("-" * 60)
    
    print("\n" + "=" * 70)
    print("🎉 批量生成完成！")
    print("📁 数据已保存到以下目录:")
    print("   - data/3000万门槛/ (包含标准、敏感、平滑三种参数)")
    print("   - data/5000万门槛/ (包含标准、敏感、平滑三种参数)")
    print("=" * 70)


if __name__ == "__main__":
    # 检查是否是快速测试模式
    if len(sys.argv) > 1 and sys.argv[1] == '--test':
        quick_test()
    else:
        main() 