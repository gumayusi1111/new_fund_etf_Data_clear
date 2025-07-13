#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
波动率指标计算器主入口 - 基于第一大类标准
=======================================

新版本特性：
1. 完整的分层架构 - controllers, engines, infrastructure, outputs
2. 智能缓存系统 - 自动检测文件变化，增量计算，大幅提升性能
3. 超高性能历史数据计算 - 向量化计算，速度提升50-100倍
4. 统一字段格式 - 与第一大类趋势指标保持一致
5. 结果处理完善 - CSV导出、统计分析、趋势分析

支持的波动率指标：
- VOL_10, VOL_20, VOL_30, VOL_60: 各周期历史波动率
- ROLLING_VOL_10, ROLLING_VOL_30: 滚动波动率
- PRICE_RANGE: 价格振幅百分比
- VOL_RATIO_20_60: 短期/长期波动率比率
- VOL_STATE: 波动率状态 (HIGH/MEDIUM/NORMAL/LOW)
- VOL_LEVEL: 波动率水平 (EXTREME_HIGH/HIGH/MEDIUM/LOW)
"""

import sys
import os
import re
import argparse

# 确保模块路径正确
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, current_dir)

from volatility_calculator.controllers.main_controller import VolatilityMainController


def validate_etf_code(etf_code: str) -> bool:
    """验证ETF代码格式"""
    if not etf_code:
        return False
    
    # 标准格式：6位数字.SH或.SZ
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
    print("🧪 波动率系统功能完整性测试")
    print("=" * 80)
    
    try:
        # 初始化控制器
        controller = VolatilityMainController(
            adj_type="前复权", 
            volatility_periods=[10, 20, 30, 60],
            enable_cache=True,
            annualized=True
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
        
        # 测试2: 单个ETF快速分析
        print("\n2️⃣ 单个ETF快速分析测试...")
        available_etfs = controller.get_available_etfs()
        if not available_etfs:
            print("   ❌ 没有可用的ETF数据")
            return False
        
        test_etf = available_etfs[0]  # 使用第一个可用的ETF
        result = controller.quick_analysis(test_etf, include_historical=False)
        
        if result:
            print(f"   ✅ 快速分析成功")
            volatility_values = result.get('volatility_values', {})
            print(f"   📈 波动率指标数量: {len([k for k in volatility_values.keys() if k.startswith('VOL_')])}")
        else:
            print(f"   ❌ 快速分析失败")
        
        # 测试3: 历史数据分析
        print("\n3️⃣ 历史数据分析测试...")
        historical_result = controller.quick_analysis(test_etf, include_historical=True)
        
        if historical_result and 'historical_analysis' in historical_result:
            hist_analysis = historical_result['historical_analysis']
            print(f"   ✅ 历史数据分析成功")
            print(f"   📊 历史数据天数: {hist_analysis['total_history_days']}")
            print(f"   📈 有效波动率天数: {hist_analysis['valid_vol_days']}")
        else:
            print(f"   ⚠️ 历史数据分析跳过（数据可能不足）")
        
        print(f"\n✅ 系统功能测试完成")
        return True
        
    except Exception as e:
        print(f"\n❌ 系统功能测试失败: {str(e)}")
        return False


def parse_arguments():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(
        description='波动率指标计算器 - 基于第一大类标准',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用示例:
  python volatility_main.py                    # 默认批量计算
  python volatility_main.py --status          # 系统状态检查
  python volatility_main.py --etf 159001      # 单个ETF分析
  python volatility_main.py --threshold "3000万门槛"  # 指定门槛
  python volatility_main.py --batch --verbose # 详细批量计算
  python volatility_main.py --test            # 功能测试
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
    parser.add_argument('--batch', action='store_true',
                       help='批量处理模式')
    parser.add_argument('--verbose', action='store_true',
                       help='详细输出模式')
    parser.add_argument('--no-cache', action='store_true',
                       help='禁用缓存')
    parser.add_argument('--no-annualized', action='store_true',
                       help='禁用年化计算')
    
    return parser.parse_args()


def main():
    """主函数"""
    args = parse_arguments()
    
    print("📊 波动率指标计算器 - 基于第一大类标准")
    print("📈 支持历史波动率、价格振幅、滚动波动率等指标")
    print("🗂️ 智能缓存：支持增量更新")
    print("=" * 60)
    
    try:
        # 根据参数决定性能模式
        performance_mode = not args.verbose
        enable_cache = not args.no_cache
        annualized = not args.no_annualized
        
        # 初始化控制器
        controller = VolatilityMainController(
            performance_mode=performance_mode,
            enable_cache=enable_cache,
            annualized=annualized
        )
        
        # 系统状态检查
        if args.status:
            print("\n📊 系统状态信息:")
            status = controller.get_system_status()
            
            if 'error' not in status:
                print(f"   🔧 系统版本: {status['system_info']['version']}")
                print(f"   📁 数据路径: {os.path.basename(status['system_info']['data_path'])}")
                print(f"   📊 可用ETF: {status['data_status']['available_etfs_count']}个")
                print(f"   🗂️ 缓存状态: {status['components']['Cache Manager']}")
                
                # 显示可用ETF示例
                available_etfs = controller.get_available_etfs()
                if available_etfs:
                    print(f"\n📈 可用ETF示例 (共{len(available_etfs)}个):")
                    for i, etf in enumerate(available_etfs[:10], 1):
                        print(f"   {i:2d}. {etf}")
                    if len(available_etfs) > 10:
                        print(f"   ... 还有 {len(available_etfs) - 10} 个")
            else:
                print(f"   ❌ 系统状态检查失败: {status['error']}")
            return
        
        # 功能测试
        if args.test:
            test_system_functionality()
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
                print(f"📊 波动率状态: {result.get('volatility_values', {}).get('VOL_STATE', 'Unknown')}")
                print(f"📈 波动率水平: {result.get('volatility_values', {}).get('VOL_LEVEL', 'Unknown')}")
            else:
                print(f"\n❌ 分析失败")
            return
        
        # 指定门槛处理
        if args.threshold:
            print(f"\n📊 处理指定门槛: {args.threshold}")
            results = controller.calculate_and_save_screening_results([args.threshold])
            
            total_etfs = results.get('total_etfs', 0)
            print(f"\n🎉 门槛处理完成！")
            print(f"📊 总处理ETF数量: {total_etfs}")
            return
        
        # 批量处理或默认执行
        if args.batch or True:  # 默认执行批量处理
            print(f"\n🚀 开始批量波动率计算...")
            results = controller.calculate_historical_batch()
            
            # 显示结果
            stats = results.get('processing_statistics', {})
            total_etfs = results.get('total_etfs_processed', 0)
            
            print(f"\n🎉 批量波动率计算完成！")
            print(f"📊 总处理ETF数量: {total_etfs}")
            
            for threshold, threshold_stats in stats.items():
                if threshold_stats:
                    saved_count = threshold_stats.get('saved_count', 0)
                    total_files = threshold_stats.get('total_files', 0)
                    success_rate = threshold_stats.get('success_rate', 0)
                    total_size_kb = threshold_stats.get('total_size_kb', 0)
                    
                    print(f"\n📈 {threshold}:")
                    print(f"   ✅ 成功: {saved_count}/{total_files} ({success_rate:.1f}%)")
                    print(f"   💾 文件大小: {total_size_kb:.1f} KB")
                else:
                    print(f"\n❌ {threshold}: 计算失败")
        
    except KeyboardInterrupt:
        print("\n👋 程序中断，退出")
    except Exception as e:
        print(f"❌ 执行错误: {str(e)}")
        print("💡 请检查:")
        print("   1. 数据文件路径是否正确")
        print("   2. 依赖包是否安装完整")
        print("   3. 文件权限是否正确")


if __name__ == "__main__":
    main()