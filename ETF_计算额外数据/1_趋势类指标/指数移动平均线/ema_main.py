#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
EMA主程序 - 重构版
=================

指数移动平均线计算的主程序入口
使用重构后的模块化架构，与WMA/SMA系统保持一致

使用示例:
    python ema_main.py                                     # 默认：批量计算所有门槛（3000万+5000万）
    python ema_main.py --etf 510050.SH                    # 计算单个ETF
    python ema_main.py --screening --threshold 3000万门槛   # 处理指定门槛
    python ema_main.py --quick 510050.SH                  # 快速分析
    python ema_main.py --status                           # 查看系统状态
    python ema_main.py --validate 510050.SH              # 验证计算正确性
    
🚀 默认运行：直接运行即可计算所有ETF的EMA指标
"""

import argparse
import sys
import os

# 添加当前目录到sys.path
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, current_dir)

from ema_calculator import EMAMainController, EMAController


def parse_arguments():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(
        description="EMA指数移动平均线计算器 - 中短期专版",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用示例:
  %(prog)s                                    # 默认：批量计算所有门槛（3000万+5000万）🚀
  %(prog)s --etf 510050.SH                    # 计算单个ETF
  %(prog)s --etf 510050.SH --verbose          # 详细模式
  %(prog)s --screening --threshold 3000万门槛   # 处理指定门槛
  %(prog)s --quick 510050.SH                  # 快速分析（不保存文件）
  %(prog)s --status                           # 查看系统状态
  %(prog)s --validate 510050.SH              # 验证计算正确性
  %(prog)s --list                            # 列出可用ETF

配置选项:
  %(prog)s --etf 510050.SH --adj-type 后复权   # 指定复权类型
  %(prog)s --etf 510050.SH --periods 5 10 20  # 自定义EMA周期
  
🎯 默认模式特点:
  - 自动处理3000万和5000万门槛
  - 使用完整历史数据（不限行数）
  - 与SMA/WMA系统保持一致
  - 直接运行无需参数
        """
    )
    
    # 基础操作组（允许默认操作）
    operation_group = parser.add_mutually_exclusive_group(required=False)
    operation_group.add_argument('--etf', type=str, help='计算指定ETF的EMA指标')
    operation_group.add_argument('--screening', action='store_true', help='批量处理筛选结果')
    operation_group.add_argument('--quick', type=str, help='快速分析模式（不保存文件）')
    operation_group.add_argument('--status', action='store_true', help='显示系统状态')
    operation_group.add_argument('--validate', type=str, help='验证EMA计算正确性')
    operation_group.add_argument('--list', action='store_true', help='列出可用ETF代码')
    
    # 配置选项
    parser.add_argument('--adj-type', type=str, default='前复权',
                       choices=['前复权', '后复权', '除权'],
                       help='复权类型 (默认: 前复权)')
    
    parser.add_argument('--periods', type=int, nargs='+', 
                       help='自定义EMA周期 (默认: 12 26)')
    
    parser.add_argument('--threshold', type=str, default='3000万门槛',
                       choices=['3000万门槛', '5000万门槛'],
                       help='筛选门槛类型 (默认: 3000万门槛)')
    
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
        print("🚀 EMA计算器启动 - 中短期专版")
        print("=" * 60)
        
        # 初始化控制器
        controller = EMAController(
            adj_type=args.adj_type,
            ema_periods=args.periods
        )
        
        # 🚀 默认模式：批量处理所有门槛（模仿SMA/WMA）
        if not any([args.etf, args.screening, args.quick, args.status, args.validate, args.list]):
            print("🔍 默认模式：EMA批量计算所有门槛...")
            
            # 处理3000万和5000万门槛
            thresholds = ["3000万门槛", "5000万门槛"]
            print(f"📊 处理门槛: {', '.join(thresholds)}")
            
            total_success = 0
            total_processed = 0
            
            for threshold in thresholds:
                print(f"\n📈 开始处理 {threshold}...")
                result = controller.calculate_screening_results(
                    threshold=threshold,
                    max_etfs=args.max_etfs,
                    verbose=args.verbose
                )
                
                if result.get('success', False):
                    print(f"✅ {threshold} 处理完成: {result['success_count']}/{result['processed_count']}")
                    total_success += result['success_count']
                    total_processed += result['processed_count']
                else:
                    print(f"❌ {threshold} 处理失败: {result.get('error', '未知错误')}")
            
            print(f"\n🎉 批量处理完成！总计: {total_success}/{total_processed}")
            return
        
        # 根据参数执行不同操作
        elif args.etf:
            # 单个ETF计算
            result = controller.calculate_single_etf(
                etf_code=args.etf,
                save_result=True,
                threshold=args.threshold,
                verbose=args.verbose
            )
            
            if result and result.get('success', False):
                print(f"\n✅ {args.etf} EMA计算成功完成")
                print(f"📁 结果已保存到 {args.threshold} 目录")
            else:
                print(f"\n❌ {args.etf} EMA计算失败")
                sys.exit(1)
        
        elif args.screening:
            # 批量处理筛选结果
            result = controller.calculate_screening_results(
                threshold=args.threshold,
                max_etfs=args.max_etfs,
                verbose=args.verbose
            )
            
            if result.get('success', False):
                print(f"\n✅ 批量处理完成")
                print(f"📊 成功: {result['success_count']}/{result['processed_count']}")
                print(f"📁 结果已保存到 {args.threshold} 目录")
            else:
                print(f"\n❌ 批量处理失败: {result.get('error', '未知错误')}")
                sys.exit(1)
        
        elif args.quick:
            # 快速分析
            result = controller.quick_analysis(args.quick)
            
            if result:
                print(f"\n⚡ {args.quick} 快速分析完成")
                print(result)
            else:
                print(f"\n❌ {args.quick} 快速分析失败")
                sys.exit(1)
        
        elif args.status:
            # 显示系统状态
            controller.show_system_status()
        
        elif args.validate:
            # 验证计算正确性
            is_valid = controller.validate_ema_calculation(args.validate)
            
            if is_valid:
                print(f"\n✅ {args.validate} EMA计算验证通过")
            else:
                print(f"\n❌ {args.validate} EMA计算验证失败")
                sys.exit(1)
        
        elif args.list:
            # 列出可用ETF
            etf_codes = controller.data_reader.get_available_etfs()
            
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