#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SMA系统主程序 - 精简版 (重构版)
===============================

使用重构后的控制器架构的主程序入口
调用 controllers/ 目录下的模块化组件
"""

import argparse
import sys
import os

# 添加当前目录到sys.path
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, current_dir)

def main():
    """主程序入口"""
    parser = argparse.ArgumentParser(
        description='SMA系统 - 移动平均线计算系统 (重构版)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例用法:
  系统状态:       python sma_main.py --status
  单个ETF:        python sma_main.py --etf 159001
  单个门槛:       python sma_main.py --threshold "3000万门槛"
  默认双门槛:     python sma_main.py --run-default
  全部门槛:       python sma_main.py --all-thresholds
  详细模式:       python sma_main.py --run-default --verbose
        """
    )
    
    # 主要功能参数
    parser.add_argument('--status', action='store_true',
                       help='显示系统状态和组件信息')
    
    parser.add_argument('--etf', type=str, metavar='ETF_CODE',
                       help='计算单个ETF的SMA指标 (例: 510050.SH)')
    
    parser.add_argument('--threshold', type=str, metavar='THRESHOLD',
                       help='处理指定门槛的ETF筛选结果 (例: "3000万门槛")')
    
    parser.add_argument('--run-default', action='store_true',
                       help='运行默认配置：计算3000万和5000万两个门槛 (推荐)')
    
    parser.add_argument('--all-thresholds', action='store_true',
                       help='处理所有可用门槛的ETF筛选结果')
    
    parser.add_argument('--verbose', action='store_true',
                       help='详细输出模式')
    
    args = parser.parse_args()
    
    try:
        print("🚀 SMA系统启动 (重构版)...")
        print("=" * 60)
        
        # 使用重构后的控制器
        from sma_calculator.controllers.main_controller import SMAMainController
        controller = SMAMainController()
        
        # 处理不同的命令
        if args.status:
            return handle_status_command(controller)
        elif args.etf:
            return handle_single_etf_command(controller, args)
        elif args.threshold:
            return handle_threshold_command(controller, args)
        elif args.run_default:
            return handle_default_run_command(controller, args)
        elif args.all_thresholds:
            return handle_all_thresholds_command(controller, args)
        else:
            # 默认行为：如果没有指定参数，运行默认的双门槛计算
            print("🎯 未指定参数，运行默认配置...")
            args.run_default = True
            return handle_default_run_command(controller, args)
    
    except KeyboardInterrupt:
        print("\n⚠️ 用户中断操作")
        return 1
    except Exception as e:
        print(f"\n❌ 程序异常: {str(e)}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        return 1


def handle_status_command(controller):
    """处理状态命令"""
    print("📊 系统状态检查...")
    
    try:
        status = controller.get_system_status()
        
        if 'error' in status:
            print(f"❌ {status['error']}")
            return 1
        
        print("\n✅ 系统状态正常")
        print("=" * 60)
        
        return 0
        
    except Exception as e:
        print(f"❌ 状态检查失败: {str(e)}")
        return 1


def handle_single_etf_command(controller, args):
    """处理单个ETF命令"""
    etf_code = args.etf.upper()
    
    print(f"🔄 单个ETF计算: {etf_code}")
    print("=" * 60)
    
    try:
        result = controller.process_single_etf(etf_code=etf_code)
        
        if result:
            print(f"\n✅ {etf_code} 计算完成")
            return 0
        else:
            print(f"\n❌ {etf_code} 计算失败")
            return 1
            
    except Exception as e:
        print(f"\n❌ {etf_code} 处理异常: {str(e)}")
        return 1


def handle_threshold_command(controller, args):
    """处理单个门槛命令"""
    threshold = args.threshold
    
    print(f"🔄 单门槛计算: {threshold}")
    print("=" * 60)
    
    try:
        result = controller.calculate_and_save_screening_results(thresholds=[threshold])
        
        if result.get('success'):
            print(f"\n✅ {threshold} 处理完成")
            return 0
        else:
            print(f"\n❌ {threshold} 处理失败")
            return 1
            
    except Exception as e:
        print(f"\n❌ {threshold} 处理异常: {str(e)}")
        return 1


def get_default_thresholds():
    """获取默认门槛列表"""
    return ["3000万门槛", "5000万门槛"]

def handle_default_run_command(controller, args):
    """处理默认运行命令：计算3000万和5000万门槛"""
    thresholds = get_default_thresholds()
    
    print("🎯 默认双门槛计算模式")
    print("📊 将计算: 3000万门槛 + 5000万门槛")
    print("=" * 60)
    
    try:
        result = controller.calculate_and_save_screening_results(
            thresholds=thresholds,
            include_advanced_analysis=False
        )
        
        if result.get('success'):
            print(f"\n🎉 默认双门槛计算完成！")
            
            # 显示统计信息
            if 'total_etfs_processed' in result:
                print("📊 处理统计:")
                print(f"   📁 处理ETF数: {result.get('total_etfs_processed', 0)}")
                print(f"   📁 处理门槛数: {result.get('thresholds_processed', 0)}")
                print(f"   📁 输出目录: {result.get('output_directory', '')}")
                print(f"   ⏱️  总处理时间: {result.get('processing_time_seconds', 0):.2f}秒")
                
                save_stats = result.get('save_statistics', {})
                if save_stats:
                    print(f"   💾 保存文件: {save_stats.get('total_files_saved', 0)}")
                    size_mb = save_stats.get('total_size_bytes', 0) / 1024 / 1024
                    print(f"   📊 文件大小: {size_mb:.2f}MB")
                
                # 显示缓存统计
                cache_stats = result.get('cache_statistics', {})
                if cache_stats:
                    print(f"   🗂️ 缓存命中率: {cache_stats.get('cache_hit_rate', 0):.1f}%")
            
            return 0
        else:
            print(f"\n❌ 默认双门槛计算失败: {result.get('message', '未知错误')}")
            return 1
            
    except Exception as e:
        print(f"\n❌ 默认双门槛计算异常: {str(e)}")
        return 1


def handle_all_thresholds_command(controller, args):
    """处理所有门槛命令"""
    thresholds = get_default_thresholds()
    
    print("🔄 全部门槛计算")
    print("=" * 60)
    
    try:
        result = controller.calculate_and_save_screening_results(
            thresholds=thresholds,
            include_advanced_analysis=False
        )
        
        if result.get('success'):
            print(f"\n✅ 全部门槛处理完成")
            
            # 显示统计信息
            print("📊 总体统计:")
            print(f"   📁 处理ETF数: {result.get('total_etfs_processed', 0)}")
            print(f"   📁 处理门槛数: {result.get('thresholds_processed', 0)}")
            print(f"   📁 输出目录: {result.get('output_directory', '')}")
            print(f"   ⏱️  总处理时间: {result.get('processing_time_seconds', 0):.2f}秒")
            
            save_stats = result.get('save_statistics', {})
            if save_stats:
                print(f"   💾 保存文件: {save_stats.get('total_files_saved', 0)}")
                size_mb = save_stats.get('total_size_bytes', 0) / 1024 / 1024
                print(f"   📊 文件大小: {size_mb:.2f}MB")
            
            return 0
        else:
            print(f"\n❌ 全部门槛处理失败: {result.get('message', '未知错误')}")
            return 1
            
    except Exception as e:
        print(f"\n❌ 全部门槛处理异常: {str(e)}")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code) 