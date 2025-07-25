"""
RSI指标优化主启动器
基于威廉指标的主启动器架构

功能特性：
1. RSI指标批量计算和单个计算
2. 支持双门槛处理（3000万/5000万）
3. 优化的计算引擎和缓存系统
4. 完整的错误处理和日志记录
"""

import os
import sys
import argparse
from datetime import datetime
import traceback

# 添加项目路径
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, current_dir)

try:
    from rsi_calculator.controllers.main_controller_optimized import RSIMainControllerOptimized
    from rsi_calculator.infrastructure.config import RSIConfig
except ImportError as e:
    print(f"❌ 导入RSI模块失败: {str(e)}")
    print("🔧 请确保所有必要的模块文件已正确创建")
    sys.exit(1)


def run_single_etf_calculation(etf_code, threshold, adj_type="前复权"):
    """
    运行单个ETF的RSI计算
    
    Args:
        etf_code: ETF代码
        threshold: 门槛值
        adj_type: 复权类型
    """
    try:
        print(f"🚀 开始单个ETF RSI计算: {etf_code}")
        print(f"📊 门槛值: {threshold}")
        print(f"📈 复权类型: {adj_type}")
        
        # 初始化控制器
        controller = RSIMainControllerOptimized(adj_type=adj_type)
        
        # 执行计算
        result = controller.calculate_single_etf_optimized(etf_code, threshold)
        
        if result["success"]:
            print(f"✅ RSI计算成功: {etf_code}")
            print(f"📊 数据行数: {result['record_count']}")
            print(f"⏱️ 计算时间: {result['calculation_time_ms']:.2f}ms")
            print(f"💾 数据源: {result['data_source']}")
            
            # 显示统计信息
            if 'statistics' in result and result['statistics']:
                print("📈 RSI统计摘要:")
                for indicator, stats in result['statistics'].items():
                    if isinstance(stats, dict) and 'current' in stats:
                        print(f"   • {indicator}: {stats['current']}")
        else:
            print(f"❌ RSI计算失败: {etf_code}")
            print(f"错误信息: {result.get('error', '未知错误')}")
        
        # 打印优化摘要
        controller.print_optimization_summary()
        
        return result
        
    except Exception as e:
        print(f"❌ 单个ETF计算过程发生异常: {str(e)}")
        print(f"🔍 异常详情: {traceback.format_exc()}")
        return {"success": False, "error": str(e)}


def run_batch_calculation(threshold, adj_type="前复权", max_etfs=None):
    """
    运行批量RSI计算
    
    Args:
        threshold: 门槛值
        adj_type: 复权类型
        max_etfs: 最大处理ETF数量，None表示处理所有
    """
    try:
        print(f"🚀 开始批量RSI计算")
        print(f"📊 门槛值: {threshold}")
        print(f"📈 复权类型: {adj_type}")
        
        # 初始化控制器
        controller = RSIMainControllerOptimized(adj_type=adj_type)
        
        # 获取ETF列表（从初筛结果）
        etf_codes = controller.data_reader.get_etf_file_list(threshold)
        
        if not etf_codes:
            print("❌ 未找到任何ETF数据文件")
            return {"success": False, "error": "无ETF数据"}
        
        # 限制处理数量（用于测试）
        if max_etfs and max_etfs > 0:
            etf_codes = etf_codes[:max_etfs]
            print(f"🔧 限制处理ETF数量: {len(etf_codes)}")
        
        print(f"📊 准备处理{len(etf_codes)}个ETF")
        
        # 执行批量计算
        batch_results = controller.calculate_batch_etfs(etf_codes, threshold)
        
        # 显示最终结果
        success_rate = (batch_results["success_count"] / batch_results["total_count"] * 100) if batch_results["total_count"] > 0 else 0
        
        print(f"\n🎉 批量RSI计算完成!")
        print(f"📊 总体成功率: {success_rate:.1f}%")
        print(f"⏱️ 总耗时: {batch_results['duration_seconds']:.1f}秒")
        
        # 打印优化摘要
        controller.print_optimization_summary()
        
        return batch_results
        
    except Exception as e:
        print(f"❌ 批量计算过程发生异常: {str(e)}")
        print(f"🔍 异常详情: {traceback.format_exc()}")
        return {"success": False, "error": str(e)}


def run_system_test(adj_type="前复权"):
    """
    运行系统测试
    
    Args:
        adj_type: 复权类型
    """
    try:
        print("🧪 开始RSI系统测试")
        
        # 1. 配置测试
        print("\n1️⃣ 配置系统测试:")
        config = RSIConfig(adj_type=adj_type)
        config.print_config_summary()
        
        validation = config.validate_config()
        if validation['is_valid']:
            print("✅ 配置验证通过")
        else:
            print("❌ 配置验证失败:")
            for error in validation['errors']:
                print(f"   - {error}")
            return False
        
        # 2. 控制器测试
        print("\n2️⃣ 控制器初始化测试:")
        controller = RSIMainControllerOptimized(adj_type=adj_type)
        print("✅ 控制器初始化成功")
        
        # 3. 数据源测试
        print("\n3️⃣ 数据源验证测试:")
        validation_result = controller.data_reader.validate_data_source()
        if validation_result['is_valid']:
            print(f"✅ 数据源验证通过: {validation_result['total_files']}个文件")
        else:
            print("❌ 数据源验证失败:")
            for error in validation_result['error_details']:
                print(f"   - {error}")
        
        # 4. ETF列表测试
        print("\n4️⃣ ETF列表获取测试:")
        etf_codes = controller.data_reader.get_etf_file_list("3000万门槛")
        if etf_codes:
            print(f"✅ 成功获取{len(etf_codes)}个ETF")
            print(f"   前5个ETF: {etf_codes[:5]}")
        else:
            print("❌ 未获取到ETF列表")
            return False
        
        # 5. 单个ETF测试
        print("\n5️⃣ 单个ETF计算测试:")
        test_etf = etf_codes[0]
        result = controller.calculate_single_etf_optimized(test_etf, "3000万门槛")
        if result["success"]:
            print(f"✅ 测试计算成功: {test_etf}")
        else:
            print(f"❌ 测试计算失败: {test_etf}")
        
        # 6. 性能摘要
        print("\n6️⃣ 系统性能摘要:")
        controller.print_optimization_summary()
        controller.data_reader.print_performance_summary()
        controller.cache_manager.print_cache_summary()
        
        print("🎉 系统测试完成!")
        return True
        
    except Exception as e:
        print(f"❌ 系统测试失败: {str(e)}")
        print(f"🔍 异常详情: {traceback.format_exc()}")
        return False


def main():
    """主函数"""
    parser = argparse.ArgumentParser(
        description="RSI指标优化计算系统",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用示例:
  python rsi_main_optimized.py --mode test                    # 系统测试
  python rsi_main_optimized.py --mode single --etf 159001    # 单个ETF计算
  python rsi_main_optimized.py --mode batch                   # 批量计算
  python rsi_main_optimized.py --mode batch --max-etfs 10    # 限制批量计算数量
  python rsi_main_optimized.py --threshold "5000万门槛"       # 指定门槛值
  python rsi_main_optimized.py --adj-type "后复权"            # 指定复权类型
        """
    )
    
    parser.add_argument(
        '--mode',
        choices=['test', 'single', 'batch'],
        default='test',
        help='运行模式: test(系统测试), single(单个ETF), batch(批量计算)'
    )
    
    parser.add_argument(
        '--etf',
        type=str,
        help='ETF代码(单个计算模式必需)'
    )
    
    parser.add_argument(
        '--threshold',
        choices=['3000万门槛', '5000万门槛'],
        default='3000万门槛',
        help='门槛值选择'
    )
    
    parser.add_argument(
        '--adj-type',
        choices=['前复权', '后复权', '除权'],
        default='前复权',
        help='复权类型选择'
    )
    
    parser.add_argument(
        '--max-etfs',
        type=int,
        help='批量计算时的最大ETF数量(用于测试)'
    )
    
    args = parser.parse_args()
    
    try:
        print("=" * 80)
        print("🚀 RSI指标优化计算系统")
        print("=" * 80)
        print(f"🕐 启动时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"🔧 运行模式: {args.mode}")
        print(f"📊 门槛值: {args.threshold}")
        print(f"📈 复权类型: {args.adj_type}")
        print("=" * 80)
        
        # 根据模式执行相应操作
        if args.mode == 'test':
            success = run_system_test(args.adj_type)
            sys.exit(0 if success else 1)
            
        elif args.mode == 'single':
            if not args.etf:
                print("❌ 单个ETF计算模式需要指定--etf参数")
                sys.exit(1)
            
            result = run_single_etf_calculation(args.etf, args.threshold, args.adj_type)
            sys.exit(0 if result["success"] else 1)
            
        elif args.mode == 'batch':
            result = run_batch_calculation(args.threshold, args.adj_type, args.max_etfs)
            if isinstance(result, dict) and "success_count" in result:
                # 批量计算成功，根据成功率决定退出码
                success_rate = result["success_count"] / result["total_count"] if result["total_count"] > 0 else 0
                sys.exit(0 if success_rate > 0.5 else 1)  # 成功率超过50%认为成功
            else:
                sys.exit(1)
        
    except KeyboardInterrupt:
        print("\n⚠️ 用户中断操作")
        sys.exit(130)
    except Exception as e:
        print(f"❌ 程序执行异常: {str(e)}")
        print(f"🔍 异常详情: {traceback.format_exc()}")
        sys.exit(1)
    finally:
        print(f"\n🕐 结束时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 80)


if __name__ == "__main__":
    main()