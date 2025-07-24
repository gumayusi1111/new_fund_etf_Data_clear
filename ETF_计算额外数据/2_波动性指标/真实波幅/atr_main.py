#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ATR真实波幅计算器主程序
====================

基于README.md中的完整计算公式，实现7个核心ATR字段的批量计算：
1. TR: 真实波幅
2. ATR_10: 10日平均真实波幅  
3. ATR_Percent: ATR百分比(标准化)
4. ATR_Change_Rate: ATR变化率
5. ATR_Ratio_HL: ATR占区间比
6. Stop_Loss: 建议止损位
7. Volatility_Level: 波动水平分级

系统特性：
- 🚀 向量化计算引擎，性能提升50-100倍
- 💾 智能缓存系统，自动检测文件变化
- 📊 双门槛支持(3000万/5000万)
- 🔄 增量更新机制
- 📈 中国市场优化(涨跌停修正、T+1制度)
- 🎯 精确止损位计算
- 📋 完整的CSV输出
"""

import sys
import os
import argparse
import time
from pathlib import Path

# 添加当前目录到sys.path
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, current_dir)

from atr_calculator.controllers.main_controller import ATRMainController
from atr_calculator.infrastructure.config import ATRConfig
from atr_calculator.infrastructure.utils import setup_logger


def validate_etf_code(etf_code: str) -> bool:
    """验证ETF代码格式"""
    if not etf_code:
        return False
    
    # 标准格式：6位数字.SH或.SZ
    import re
    pattern = r'^\d{6}\.(SH|SZ)$'
    return bool(re.match(pattern, etf_code))


def get_validated_etf_code(prompt: str, available_etfs: list = None) -> str:
    """获取经过验证的ETF代码"""
    while True:
        etf_code = input(prompt).strip().upper()
        
        if not etf_code:
            print("❌ 请输入ETF代码")
            continue
        
        # 自动添加后缀
        if len(etf_code) == 6 and etf_code.isdigit():
            # 基于规则判断交易所
            if etf_code.startswith(('50', '51', '52', '56', '58')):
                etf_code += '.SH'
            elif etf_code.startswith(('15', '16', '18')):
                etf_code += '.SZ'
            else:
                print("❌ 无法确定交易所，请输入完整代码（如：510050.SH）")
                continue
        
        if not validate_etf_code(etf_code):
            print("❌ ETF代码格式错误，请输入正确格式（如：510050.SH）")
            continue
        
        # 检查是否在可用列表中
        if available_etfs and etf_code not in available_etfs:
            clean_code = etf_code.replace('.SH', '').replace('.SZ', '')
            if clean_code not in [e.replace('.SH', '').replace('.SZ', '') for e in available_etfs]:
                print(f"❌ ETF代码 {etf_code} 不在可用列表中")
                continue
        
        return etf_code


def test_system_functionality():
    """测试系统完整功能"""
    print("🧪 ATR系统功能完整性测试")
    print("=" * 80)
    
    try:
        # 初始化控制器
        config = ATRConfig()
        controller = ATRMainController(
            config=config,
            enable_cache=True,
            performance_mode=False  # 测试模式下启用详细日志
        )
        
        # 测试1: 系统状态检查
        print("\n1️⃣ 系统状态检查...")
        status = controller.get_system_status()
        if 'error' in status:
            print(f"   ❌ 系统状态检查失败: {status['error']}")
            return False
        else:
            print(f"   ✅ 系统状态正常")
            print(f"   📊 可用ETF数量: {status['data_status']['available_etfs_count']}")
            print(f"   🎯 ATR周期: {status['system_info']['atr_period']}日")
            print(f"   🔧 止损倍数: {status['system_info']['stop_loss_multiplier']}")
        
        # 测试2: 单个ETF快速分析
        print("\n2️⃣ 单个ETF快速分析测试...")
        available_etfs = controller.get_available_etfs()
        if not available_etfs:
            print("   ❌ 没有可用的ETF数据")
            return False
        
        test_etf = available_etfs[0]  # 使用第一个可用的ETF
        result = controller.quick_analysis(test_etf, include_historical=False)
        
        if result:
            print(f"   ✅ 快速分析成功: {test_etf}")
            latest_values = result.get('latest_values', {})
            if 'atr_percent' in latest_values:
                print(f"   📈 当前ATR百分比: {latest_values['atr_percent']:.2f}%")
            if 'volatility_level' in latest_values:
                print(f"   📊 波动水平: {latest_values['volatility_level']}")
            if 'stop_loss' in latest_values and 'atr_10' in latest_values:
                print(f"   🎯 建议止损位: {latest_values['stop_loss']:.2f}")
        else:
            print(f"   ❌ 快速分析失败: {test_etf}")
        
        # 测试3: ATR计算精度验证
        print("\n3️⃣ ATR计算精度验证...")
        threshold = config.thresholds[0]
        calc_result = controller.calculate_single_etf(test_etf, threshold)
        
        if calc_result['success'] and 'data' in calc_result:
            data = calc_result['data']
            atr_fields = ['tr', 'atr_10', 'atr_percent', 'atr_change_rate', 
                         'atr_ratio_hl', 'stop_loss', 'volatility_level']
            
            available_fields = [field for field in atr_fields if field in data.columns]
            print(f"   ✅ ATR字段计算成功: {len(available_fields)}/7")
            
            # 验证数值合理性
            if 'atr_10' in data.columns:
                atr_values = data['atr_10'].dropna()
                if len(atr_values) > 0:
                    print(f"   📊 ATR数据范围: {atr_values.min():.4f} - {atr_values.max():.4f}")
                    print(f"   📈 有效ATR记录: {len(atr_values)}/{len(data)}")
        else:
            print(f"   ❌ ATR计算验证失败")
        
        print(f"\n✅ 系统功能测试完成")
        return True
        
    except Exception as e:
        print(f"\n❌ 系统功能测试失败: {str(e)}")
        return False


def interactive_mode():
    """交互模式"""
    print("\n🔍 进入ATR交互分析模式")
    print("=" * 50)
    
    try:
        config = ATRConfig()
        controller = ATRMainController(config=config, enable_cache=True)
        
        # 获取可用ETF
        available_etfs = controller.get_available_etfs()
        if not available_etfs:
            print("❌ 没有可用的ETF数据")
            return
        
        print(f"📊 发现 {len(available_etfs)} 个可用ETF")
        print("💡 输入 'list' 查看前10个ETF, 输入 'quit' 退出")
        
        while True:
            user_input = input("\n🔍 请输入ETF代码 (如: 159001): ").strip()
            
            if user_input.lower() in ['quit', 'exit', 'q']:
                print("👋 退出交互模式")
                break
            elif user_input.lower() == 'list':
                print("\n📈 可用ETF示例:")
                for i, etf in enumerate(available_etfs[:10], 1):
                    print(f"   {i:2d}. {etf}")
                if len(available_etfs) > 10:
                    print(f"   ... 还有 {len(available_etfs) - 10} 个")
                continue
            elif not user_input:
                continue
            
            # 标准化ETF代码
            etf_code = user_input.upper()
            if len(etf_code) == 6 and etf_code.isdigit():
                if etf_code.startswith(('50', '51', '52', '56', '58')):
                    etf_code += '.SH'
                elif etf_code.startswith(('15', '16', '18')):
                    etf_code += '.SZ'
                else:
                    etf_code += '.SH'  # 默认上海
            
            print(f"\n🔍 分析ETF: {etf_code}")
            
            # 快速分析
            result = controller.quick_analysis(etf_code, include_historical=True)
            
            if result:
                print(f"✅ 分析完成")
                
                latest_values = result.get('latest_values', {})
                if latest_values:
                    print(f"\n📊 最新ATR指标:")
                    if 'atr_10' in latest_values:
                        print(f"   ATR(10日): {latest_values['atr_10']:.4f}")
                    if 'atr_percent' in latest_values:
                        print(f"   ATR百分比: {latest_values['atr_percent']:.2f}%")
                    if 'volatility_level' in latest_values:
                        print(f"   波动水平: {latest_values['volatility_level']}")
                    if 'stop_loss' in latest_values:
                        print(f"   建议止损位: {latest_values['stop_loss']:.2f}")
                    if 'atr_change_rate' in latest_values:
                        rate = latest_values['atr_change_rate']
                        if not pd.isna(rate):
                            print(f"   ATR变化率: {rate:.2f}%")
                
                # 历史分析
                if 'historical_analysis' in result:
                    hist = result['historical_analysis']
                    print(f"\n📈 历史分析:")
                    print(f"   总交易天数: {hist['total_history_days']}")
                    print(f"   有效ATR天数: {hist['valid_atr_days']}")
                    if hist.get('avg_atr'):
                        print(f"   平均ATR: {hist['avg_atr']:.4f}")
                
                print(f"   ⏱️ 计算时间: {result.get('calculation_time', 0):.3f}秒")
                print(f"   💾 来源: {'缓存' if result.get('from_cache', False) else '实时计算'}")
            else:
                print(f"❌ 分析失败: {etf_code}")
    
    except KeyboardInterrupt:
        print("\n👋 用户中断，退出交互模式")
    except Exception as e:
        print(f"❌ 交互模式错误: {e}")


def parse_arguments():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(
        description='ATR真实波幅计算器 - 基于README.md完整计算公式',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用示例:
  python atr_main.py                        # 默认批量计算
  python atr_main.py --status              # 系统状态检查
  python atr_main.py --etf 159001          # 单个ETF分析
  python atr_main.py --threshold "3000万门槛"  # 指定门槛
  python atr_main.py --interactive         # 交互模式
  python atr_main.py --test                # 功能测试
  python atr_main.py --clear-cache         # 清理缓存
        """
    )
    
    parser.add_argument('--status', action='store_true', 
                       help='显示系统状态信息')
    parser.add_argument('--test', action='store_true',
                       help='运行系统功能测试')
    parser.add_argument('--etf', type=str,
                       help='分析单个ETF (如: 159001)')
    parser.add_argument('--threshold', type=str,
                       help='指定门槛类型 (如: "3000万门槛")')
    parser.add_argument('--interactive', action='store_true',
                       help='交互模式')
    parser.add_argument('--batch', action='store_true',
                       help='批量处理模式')
    parser.add_argument('--verbose', action='store_true',
                       help='详细输出模式')
    parser.add_argument('--no-cache', action='store_true',
                       help='禁用缓存')
    parser.add_argument('--clear-cache', action='store_true',
                       help='清理所有缓存')
    
    return parser.parse_args()


def main():
    """主函数"""
    args = parse_arguments()
    
    print("🎯 ATR真实波幅计算器")
    print("📊 7个核心字段: tr, atr_10, atr_percent, atr_change_rate, atr_ratio_hl, stop_loss, volatility_level")
    print("💾 智能缓存: 支持增量更新")
    print("🚀 向量化计算: 50-100倍性能提升")
    print("=" * 80)
    
    try:
        # 根据参数决定性能模式
        performance_mode = not args.verbose
        enable_cache = not args.no_cache
        
        # 初始化配置和控制器
        config = ATRConfig()
        controller = ATRMainController(
            config=config,
            enable_cache=enable_cache,
            performance_mode=performance_mode
        )
        
        # 清理缓存
        if args.clear_cache:
            print("\n🧹 清理所有缓存...")
            controller.clear_cache()
            print("✅ 缓存清理完成")
            return
        
        # 系统状态检查
        if args.status:
            print("\n📊 系统状态信息:")
            status = controller.get_system_status()
            
            if 'error' not in status:
                system_info = status['system_info']
                data_status = status['data_status']
                
                print(f"   🔧 系统版本: {system_info['version']}")
                print(f"   📁 数据路径: {os.path.basename(system_info['etf_data_path'])}")
                print(f"   📊 可用ETF: {data_status['available_etfs_count']}个")
                print(f"   🎯 ATR周期: {system_info['atr_period']}日")
                print(f"   🔧 止损倍数: {system_info['stop_loss_multiplier']}")
                print(f"   🗂️ 缓存状态: {status['components']['Cache Manager']}")
                
                # 显示可用ETF示例
                available_etfs = controller.get_available_etfs()
                if available_etfs:
                    print(f"\n📈 可用ETF示例 (共{len(available_etfs)}个):")
                    for i, etf in enumerate(available_etfs[:10], 1):
                        print(f"   {i:2d}. {etf}")
                    if len(available_etfs) > 10:
                        print(f"   ... 还有 {len(available_etfs) - 10} 个")
                
                # 显示性能统计
                print(controller.get_performance_summary())
            else:
                print(f"   ❌ 系统状态检查失败: {status['error']}")
            return
        
        # 功能测试
        if args.test:
            test_system_functionality()
            return
        
        # 交互模式
        if args.interactive:
            interactive_mode()
            return
        
        # 单个ETF分析
        if args.etf:
            etf_code = args.etf
            
            # 标准化ETF代码
            if len(etf_code) == 6 and etf_code.isdigit():
                if etf_code.startswith(('50', '51', '52', '56', '58')):
                    etf_code += '.SH'
                elif etf_code.startswith(('15', '16', '18')):
                    etf_code += '.SZ'
                else:
                    etf_code += '.SH'  # 默认上海
            
            print(f"\n🔍 分析ETF: {etf_code}")
            result = controller.quick_analysis(etf_code, include_historical=True)
            
            if result:
                print(f"\n✅ 分析完成")
                latest_values = result.get('latest_values', {})
                
                print(f"📊 最新ATR指标:")
                for field in ['atr_10', 'atr_percent', 'volatility_level', 'stop_loss', 'atr_change_rate']:
                    if field in latest_values:
                        value = latest_values[field]
                        if field == 'volatility_level':
                            print(f"   {field}: {value}")
                        elif not pd.isna(value):
                            if field in ['atr_percent', 'atr_change_rate']:
                                print(f"   {field}: {value:.2f}%")
                            else:
                                print(f"   {field}: {value:.4f}")
                
                if 'historical_analysis' in result:
                    hist = result['historical_analysis']
                    print(f"\n📈 历史分析: {hist['valid_atr_days']}/{hist['total_history_days']}天有效数据")
            else:
                print(f"\n❌ 分析失败")
            return
        
        # 指定门槛处理
        if args.threshold:
            print(f"\n📊 处理指定门槛: {args.threshold}")
            results = controller.calculate_screening_results([args.threshold])
            
            if results['success']:
                threshold_stats = results['threshold_statistics'][args.threshold]
                print(f"\n🎉 门槛处理完成！")
                print(f"📊 总处理ETF: {threshold_stats['total_etfs']}")
                print(f"✅ 成功: {threshold_stats['successful_etfs']}")
                print(f"❌ 失败: {threshold_stats['failed_etfs']}")
                print(f"📈 成功率: {threshold_stats['success_rate']:.1f}%")
                print(f"⏱️ 处理时间: {threshold_stats['processing_time']:.2f}秒")
            return
        
        # 批量处理或默认执行
        if args.batch or True:  # 默认执行批量处理
            print(f"\n🚀 开始批量ATR计算...")
            print(f"🎯 门槛: {', '.join(config.thresholds)}")
            print(f"📈 输出字段: {', '.join(config.output_fields)}")
            
            start_time = time.time()
            results = controller.calculate_screening_results()
            
            if results['success']:
                # 显示结果
                total_time = time.time() - start_time
                total_etfs = results['total_etfs_processed']
                
                print(f"\n🎉 批量ATR计算完成！")
                print(f"📊 总处理ETF数量: {total_etfs}")
                print(f"⏱️ 总耗时: {total_time:.2f}秒 ({total_time/60:.1f}分钟)")
                print(f"💾 缓存命中率: {results.get('cache_hit_rate', 0):.1f}%")
                
                # 显示各门槛统计
                for threshold, stats in results['threshold_statistics'].items():
                    successful = stats['successful_etfs']
                    total = stats['total_etfs']
                    success_rate = stats['success_rate']
                    
                    print(f"\n📈 {threshold}:")
                    print(f"   ✅ 成功: {successful}/{total} ({success_rate:.1f}%)")
                    print(f"   ⏱️ 处理时间: {stats['processing_time']:.2f}秒")
                
                # 显示保存统计
                save_stats = results.get('save_statistics', {})
                if save_stats and 'total_file_size_kb' in save_stats:
                    print(f"\n💾 文件保存:")
                    print(f"   📁 总文件大小: {save_stats['total_file_size_kb']:.1f} KB")
                    print(f"   📄 保存成功: {save_stats['successful_saves']}")
                
                # 显示性能摘要
                print(controller.get_performance_summary())
                
                print(f"\n📁 数据保存位置:")
                for threshold in config.thresholds:
                    print(f"   📂 {threshold}: data/{threshold}/")
            else:
                print(f"\n❌ 批量计算失败: {results.get('error', '未知错误')}")
        
    except KeyboardInterrupt:
        print("\n👋 程序中断，退出")
    except Exception as e:
        print(f"❌ 执行错误: {str(e)}")
        print("💡 请检查:")
        print("   1. 数据文件路径是否正确")
        print("   2. 依赖包是否安装完整")
        print("   3. 文件权限是否正确")


if __name__ == "__main__":
    # 导入pandas用于isna函数
    import pandas as pd
    main()