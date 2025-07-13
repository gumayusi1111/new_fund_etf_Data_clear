#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
布林带系统主程序
==============

布林带指标计算系统的主程序入口
采用六层模块化架构，支持智能缓存和增量计算
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
        description='布林带系统 - Bollinger Bands计算系统',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例用法:
  默认运行:       python bb_main.py (🚀 批量计算所有门槛)
  系统状态:       python bb_main.py --status
  单个ETF:        python bb_main.py --etf 159001
  单个门槛:       python bb_main.py --threshold "3000万门槛"
  详细模式:       python bb_main.py --verbose
  
  🚀 高性能批量计算:
  批量计算:       python bb_main.py --batch
  指定门槛:       python bb_main.py --threshold "3000万门槛" --batch
        """
    )
    
    # 主要功能参数
    parser.add_argument('--status', action='store_true',
                       help='显示系统状态和组件信息')
    
    parser.add_argument('--etf', type=str, metavar='ETF_CODE',
                       help='计算单个ETF的布林带指标 (例: 159001)')
    
    parser.add_argument('--threshold', type=str, metavar='THRESHOLD',
                       help='处理指定门槛的ETF筛选结果 (例: "3000万门槛")')
    
    parser.add_argument('--batch', action='store_true',
                       help='批量计算模式')
    
    parser.add_argument('--verbose', action='store_true',
                       help='详细输出模式')
    
    parser.add_argument('--no-cache', action='store_true',
                       help='禁用缓存')
    
    args = parser.parse_args()
    
    try:
        print("🚀 布林带系统启动...")
        print("=" * 60)
        
        # 使用布林带主控制器
        from bb_calculator.controllers.main_controller import BBMainController
        controller = BBMainController()
        
        # 处理不同的命令
        if args.status:
            return handle_status_command(controller)
        elif args.etf:
            return handle_single_etf_command(controller, args)
        elif args.threshold and not args.batch:
            return handle_threshold_command(controller, args)
        elif args.batch:
            return handle_batch_command(controller, args)
        else:
            # 默认行为：批量计算所有门槛
            print("🚀 默认模式：批量计算所有门槛...")
            args.batch = True
            args.verbose = True
            return handle_batch_command(controller, args)
    
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
        
        print("\n✅ 系统状态正常")
        print("=" * 60)
        print(f"📋 系统名称: {status['system_name']}")
        print(f"📅 检查时间: {status['timestamp']}")
        
        # 配置信息
        config = status['config']
        print(f"\n⚙️  配置信息:")
        print(f"   📊 复权类型: {config['adj_type']}")
        print(f"   📊 布林带周期: {config['bb_period']}")
        print(f"   📊 标准差倍数: {config['std_multiplier']}")
        print(f"   📊 计算精度: {config['precision']}")
        
        # 路径信息
        paths = status['paths']
        print(f"\n📁 路径信息:")
        print(f"   📂 数据目录: {paths['data_dir']}")
        print(f"   📂 缓存目录: {paths['cache_dir']}")
        print(f"   📂 输出目录: {paths['output_dir']}")
        print(f"   ✅ 数据目录有效: {paths['data_dir_exists']}")
        
        # 缓存统计
        cache_stats = status.get('cache_statistics', {})
        if cache_stats:
            print(f"\n🗄️ 缓存统计:")
            operations = cache_stats.get('cache_operations', {})
            print(f"   📊 缓存命中率: {cache_stats.get('cache_hit_rate', 0)}%")
            print(f"   📊 命中次数: {operations.get('hits', 0)}")
            print(f"   📊 未命中次数: {operations.get('misses', 0)}")
        
        return 0
        
    except Exception as e:
        print(f"❌ 状态检查失败: {str(e)}")
        return 1


def handle_single_etf_command(controller, args):
    """处理单个ETF命令"""
    etf_code = args.etf.upper()
    
    print(f"🔄 单个ETF布林带计算: {etf_code}")
    print("=" * 60)
    
    try:
        use_cache = not args.no_cache
        result = controller.process_single_etf(
            etf_code=etf_code, 
            threshold=args.threshold,
            use_cache=use_cache
        )
        
        if result['success']:
            print(f"\n✅ {etf_code} 计算完成")
            
            if args.verbose and result['bb_results']:
                print("\n📊 布林带指标结果:")
                bb_results = result['bb_results']
                print(f"   📈 中轨(bb_middle): {bb_results.get('bb_middle', 'N/A')}")
                print(f"   📈 上轨(bb_upper): {bb_results.get('bb_upper', 'N/A')}")
                print(f"   📈 下轨(bb_lower): {bb_results.get('bb_lower', 'N/A')}")
                print(f"   📈 带宽(bb_width): {bb_results.get('bb_width', 'N/A')}%")
                print(f"   📈 位置(bb_position): {bb_results.get('bb_position', 'N/A')}%")
                print(f"   📈 %B指标: {bb_results.get('bb_percent_b', 'N/A')}")
                
                if result.get('cache_used'):
                    print("   🗄️  使用缓存数据")
                
                print(f"   ⏱️  处理时间: {result.get('processing_time', 0):.2f}秒")
            
            return 0
        else:
            print(f"\n❌ {etf_code} 计算失败: {result.get('error', '未知错误')}")
            return 1
            
    except Exception as e:
        print(f"\n❌ {etf_code} 处理异常: {str(e)}")
        return 1


def handle_threshold_command(controller, args):
    """处理单个门槛命令"""
    threshold = args.threshold
    
    print(f"🔄 单门槛布林带计算: {threshold}")
    print("=" * 60)
    
    try:
        use_cache = not args.no_cache
        result = controller.calculate_and_save_screening_results(
            thresholds=[threshold]
        )
        
        if result['success']:
            print(f"\n✅ {threshold} 处理完成")
            
            if args.verbose:
                print("📊 处理统计:")
                print(f"   📁 处理ETF数: {result.get('total_etfs_processed', 0)}")
                print(f"   ✅ 成功ETF数: {result.get('successful_etfs', 0)}")
                print(f"   ❌ 失败ETF数: {result.get('failed_etfs', 0)}")
                print(f"   ⏱️  总处理时间: {result.get('processing_time_seconds', 0):.2f}秒")
                
                save_stats = result.get('save_statistics', {})
                if save_stats:
                    print(f"   💾 保存文件: {save_stats.get('total_files_saved', 0)}")
                    size_mb = save_stats.get('total_size_bytes', 0) / 1024 / 1024
                    print(f"   📊 文件大小: {size_mb:.2f}MB")
            
            return 0
        else:
            error_msg = result.get('error', '未知错误')
            errors = result.get('errors', [])
            if errors:
                error_msg = '; '.join(errors)
            print(f"\n❌ {threshold} 处理失败: {error_msg}")
            return 1
            
    except Exception as e:
        print(f"\n❌ {threshold} 处理异常: {str(e)}")
        return 1


def handle_batch_command(controller, args):
    """处理批量计算命令"""
    if args.threshold:
        thresholds = [args.threshold]
        print(f"🚀 批量计算指定门槛: {args.threshold}")
    else:
        thresholds = ["3000万门槛", "5000万门槛"]
        print("🚀 批量计算所有门槛: 3000万门槛 + 5000万门槛")
    
    print("⚡ 预期性能: 高效缓存 + 增量计算")
    print("=" * 60)
    
    try:
        result = controller.calculate_historical_batch(
            thresholds=thresholds,
            verbose=args.verbose
        )
        
        if result['success']:
            print(f"\n🎉 批量布林带计算完成！")
            
            # 显示统计信息
            print("📊 处理统计:")
            print(f"   📁 处理ETF数: {result.get('total_etfs_processed', 0)}")
            print(f"   ✅ 成功ETF数: {result.get('successful_etfs', 0)}")
            print(f"   ❌ 失败ETF数: {result.get('failed_etfs', 0)}")
            print(f"   📁 处理门槛数: {result.get('thresholds_processed', 0)}")
            print(f"   📁 输出目录: {result.get('output_directory', '')}")
            print(f"   ⏱️  总处理时间: {result.get('processing_time_seconds', 0):.2f}秒")
            print(f"   🚀 平均处理速度: {result.get('etfs_per_second', 0):.1f} ETF/秒")
            
            save_stats = result.get('save_statistics', {})
            if save_stats:
                print(f"   💾 保存文件: {save_stats.get('total_files_saved', 0)}")
                size_mb = save_stats.get('total_size_bytes', 0) / 1024 / 1024
                print(f"   📊 文件大小: {size_mb:.2f}MB")
            
            # 显示门槛详情
            if args.verbose:
                threshold_details = result.get('threshold_details', {})
                for threshold, details in threshold_details.items():
                    print(f"\n📋 {threshold} 详情:")
                    print(f"   📁 ETF列表长度: {len(details.get('etf_list', []))}")
                    print(f"   ✅ 成功: {details.get('successful_etfs', 0)}")
                    print(f"   ❌ 失败: {details.get('failed_etfs', 0)}")
            
            return 0
        else:
            error_msg = result.get('error', '未知错误')
            errors = result.get('errors', [])
            if errors:
                error_msg = '; '.join(errors)
            print(f"\n❌ 批量计算失败: {error_msg}")
            return 1
            
    except Exception as e:
        print(f"\n❌ 批量计算异常: {str(e)}")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)