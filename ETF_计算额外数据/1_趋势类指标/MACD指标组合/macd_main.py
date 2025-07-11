#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MACD主程序 - 重构版
==================

MACD指标计算的主程序入口
使用重构后的模块化架构，与其他趋势类指标系统保持一致

使用示例:
    python macd_main_new.py                                   # 默认：标准MACD参数计算
    python macd_main_new.py --etf 510050.SH                  # 计算单个ETF
    python macd_main_new.py --parameter-set sensitive        # 使用敏感参数
    python macd_main_new.py --quick 510050.SH                # 快速分析
    python macd_main_new.py --status                         # 查看系统状态
    python macd_main_new.py --validate 510050.SH            # 验证计算正确性
    
🚀 默认运行：使用标准MACD参数(12,26,9)计算所有可用ETF
"""

import argparse
import sys
import os

# 添加当前目录到sys.path
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, current_dir)

from macd_calculator import MACDMainController


def parse_arguments():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(
        description="MACD指标计算器 - 重构版",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用示例:
  %(prog)s                                    # 默认：标准MACD参数计算 🚀
  %(prog)s --etf 510050.SH                    # 计算单个ETF
  %(prog)s --etf 510050.SH --verbose          # 详细模式
  %(prog)s --parameter-set sensitive          # 使用敏感参数(8,17,9)
  %(prog)s --parameter-set smooth             # 使用平滑参数(19,39,9)
  %(prog)s --quick 510050.SH                  # 快速分析（不保存文件）
  %(prog)s --status                           # 查看系统状态
  %(prog)s --validate 510050.SH              # 验证计算正确性
  %(prog)s --list                            # 列出可用ETF

参数说明:
  standard: EMA(12,26,9) - 标准参数
  sensitive: EMA(8,17,9) - 敏感参数  
  smooth: EMA(19,39,9) - 平滑参数
        """
    )
    
    # 基础操作组
    operation_group = parser.add_mutually_exclusive_group(required=False)
    operation_group.add_argument('--etf', type=str, help='计算指定ETF的MACD指标')
    operation_group.add_argument('--quick', type=str, help='快速分析模式（不保存文件）')
    operation_group.add_argument('--status', action='store_true', help='显示系统状态')
    operation_group.add_argument('--validate', type=str, help='验证MACD计算正确性')
    operation_group.add_argument('--list', action='store_true', help='列出可用ETF代码')
    
    # 配置选项
    parser.add_argument('--parameter-set', type=str, default='standard',
                       choices=['standard', 'sensitive', 'smooth'],
                       help='MACD参数组合 (默认: standard)')
    
    parser.add_argument('--adj-type', type=str, default='前复权',
                       choices=['前复权', '后复权', '除权'],
                       help='复权类型 (默认: 前复权)')
    
    parser.add_argument('--max-etfs', type=int,
                       help='限制处理的ETF数量（测试用）')
    
    parser.add_argument('--verbose', action='store_true',
                       help='显示详细输出')
    
    return parser.parse_args()


def main():
    """主函数"""
    try:
        # 解析参数
        args = parse_arguments()
        
        print("=" * 60)
        print("🚀 MACD计算器启动 - 重构版")
        print("=" * 60)
        
        # 初始化控制器
        controller = MACDMainController(
            parameter_set=args.parameter_set,
            adj_type=args.adj_type
        )
        
        # 🚀 默认模式：计算可用ETF的MACD指标 - 所有参数组合
        if not any([args.etf, args.quick, args.status, args.validate, args.list]):
            print("🔍 默认模式：MACD批量计算 - 三种参数组合 × 两种门槛类型...")
            
            # 定义三种MACD参数组合
            parameter_sets = ['standard', 'sensitive', 'smooth']
            parameter_names = {'standard': '标准', 'sensitive': '敏感', 'smooth': '平滑'}
            
            # 处理两种门槛类型
            thresholds = ["3000万门槛", "5000万门槛"]
            total_success = 0
            total_processed = 0
            
            for threshold in thresholds:
                print(f"\n{'='*80}")
                print(f"🎯 处理 {threshold} ETF - 所有参数组合...")
                print(f"{'='*80}")
                
                # 获取当前门槛的ETF列表
                available_etfs = controller.get_available_etfs(threshold)
                if not available_etfs:
                    print(f"❌ 未找到 {threshold} 的ETF数据")
                    continue
                
                # 限制处理数量
                if args.max_etfs:
                    available_etfs = available_etfs[:args.max_etfs]
                    print(f"📊 限制处理数量: {args.max_etfs}")
                
                print(f"📈 找到 {len(available_etfs)} 个ETF ({threshold})")
                
                # 处理每种参数组合
                for param_set in parameter_sets:
                    param_name = parameter_names[param_set]
                    print(f"\n{'='*60}")
                    print(f"🔧 参数组合: {param_name} ({param_set})")
                    print(f"{'='*60}")
                    
                    # 创建该参数组合的控制器
                    param_controller = MACDMainController(
                        parameter_set=param_set,
                        adj_type=args.adj_type
                    )
                    
                    success_count = 0
                    for i, etf_code in enumerate(available_etfs, 1):
                        if args.verbose:
                            print(f"📊 处理 {etf_code} ({i}/{len(available_etfs)}) - {param_name}")
                        
                        result = param_controller.calculate_single_etf(
                            etf_code=etf_code,
                            save_result=True,
                            threshold=threshold,
                            parameter_folder=param_name,
                            verbose=args.verbose
                        )
                        
                        if result and result.get('success', False):
                            success_count += 1
                            if args.verbose:
                                print(f"✅ {etf_code} - {param_name} 处理完成")
                        else:
                            if args.verbose:
                                error = result.get('error', 'Unknown') if result else 'Unknown'
                                print(f"❌ {etf_code} - {param_name} 处理失败: {error}")
                    
                    print(f"🎉 {threshold} - {param_name} 处理完成！成功: {success_count}/{len(available_etfs)}")
                    total_success += success_count
                    total_processed += len(available_etfs)
            
            print(f"\n{'='*80}")
            print(f"🏆 全部批量处理完成！总成功: {total_success}/{total_processed}")
            return
        
        # 根据参数执行不同操作
        elif args.etf:
            # 单个ETF计算
            result = controller.calculate_single_etf(
                etf_code=args.etf,
                save_result=True,
                verbose=args.verbose
            )
            
            if result and result.get('success', False):
                print(f"\n✅ {args.etf} MACD计算成功完成")
                print(f"📁 结果已保存")
            else:
                error = result.get('error', 'Unknown') if result else 'Unknown'
                print(f"\n❌ {args.etf} MACD计算失败: {error}")
                sys.exit(1)
        
        elif args.quick:
            # 快速分析
            result = controller.quick_analysis(args.quick)
            
            if result:
                print(f"\n⚡ {args.quick} 快速分析完成")
            else:
                print(f"\n❌ {args.quick} 快速分析失败")
                sys.exit(1)
        
        elif args.status:
            # 显示系统状态
            controller.show_system_status()
        
        elif args.validate:
            # 验证计算正确性
            is_valid = controller.validate_macd_calculation(args.validate)
            
            if is_valid:
                print(f"\n✅ {args.validate} MACD计算验证通过")
            else:
                print(f"\n❌ {args.validate} MACD计算验证失败")
                sys.exit(1)
        
        elif args.list:
            # 列出可用ETF
            etf_codes = controller.get_available_etfs()
            
            if etf_codes:
                print(f"\n📋 可用ETF代码 ({len(etf_codes)} 个):")
                
                # 分组显示
                sh_codes = [code for code in etf_codes if code.endswith('.SH')]
                sz_codes = [code for code in etf_codes if code.endswith('.SZ')]
                
                if sh_codes:
                    print(f"\n🏛️  上交所 ({len(sh_codes)} 个):")
                    for i, code in enumerate(sh_codes[:20], 1):  # 只显示前20个
                        print(f"   {code}", end='  ')
                        if i % 5 == 0:
                            print()
                    if len(sh_codes) > 20:
                        print(f"\n   ... 还有 {len(sh_codes) - 20} 个")
                    else:
                        print()
                
                if sz_codes:
                    print(f"\n🏢 深交所 ({len(sz_codes)} 个):")
                    for i, code in enumerate(sz_codes[:20], 1):  # 只显示前20个
                        print(f"   {code}", end='  ')
                        if i % 5 == 0:
                            print()
                    if len(sz_codes) > 20:
                        print(f"\n   ... 还有 {len(sz_codes) - 20} 个")
                    else:
                        print()
            else:
                print("\n❌ 未找到可用ETF代码")
                sys.exit(1)
        
        print("\n" + "=" * 60)
        print("✅ 程序执行完成")
        print("=" * 60)
        
    except KeyboardInterrupt:
        print("\n\n⚠️  用户中断程序")
        sys.exit(0)
    except Exception as e:
        print(f"\n❌ 程序执行失败: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()