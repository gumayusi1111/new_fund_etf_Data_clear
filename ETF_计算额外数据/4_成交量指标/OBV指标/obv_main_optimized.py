#!/usr/bin/env python3
"""
OBV指标计算系统 - 主启动器
========================

OBV (On-Balance Volume) 能量潮指标专业计算系统
基于约瑟夫·格兰维尔经典理论，专为中国ETF市场优化

使用示例:
    python obv_main_optimized.py --mode test
    python obv_main_optimized.py --mode single --etf 159001 --threshold "3000万门槛"
    python obv_main_optimized.py --mode batch --threshold "3000万门槛"
    python obv_main_optimized.py --mode all
    python obv_main_optimized.py --mode incremental --etf 159001 --threshold "3000万门槛"

核心特性:
- 4个核心OBV字段: obv, obv_ma10, obv_change_5, obv_change_20
- 向量化计算，8位小数精度
- 96%+缓存命中率，智能增量更新
- 双门槛处理: 3000万/5000万门槛

版本: 1.0.0
创建: 2025-07-27
"""

import argparse
import sys
import logging
from pathlib import Path
from datetime import datetime

# 添加当前目录到Python路径
current_dir = Path(__file__).parent
sys.path.insert(0, str(current_dir))

from obv_calculator.controllers.main_controller import OBVController
from obv_calculator.outputs.display_formatter import OBVDisplayFormatter
from obv_calculator.infrastructure.config import OBVConfig

class OBVMainOptimized:
    """OBV指标系统主启动器"""

    def __init__(self):
        """初始化主启动器"""
        self.config = OBVConfig()
        self.controller = OBVController(self.config)
        self.formatter = OBVDisplayFormatter()
        self.logger = logging.getLogger('OBVMain')

    def run_test_mode(self, sample_size: int = 5) -> bool:
        """
        运行测试模式
        
        Args:
            sample_size: 测试样本数量
            
        Returns:
            测试是否通过
        """
        print("🚀 启动OBV指标系统测试...")
        print("="*60)

        try:
            # 执行系统测试
            test_results = self.controller.test_system(sample_size)

            # 显示测试结果
            formatted_results = self.formatter.format_test_results(test_results)
            print(formatted_results)

            # 显示系统状态
            print("\n" + "="*60)
            status = self.controller.get_system_status()
            formatted_status = self.formatter.format_system_status(status)
            print(formatted_status)

            return test_results.get('success', False)

        except Exception as e:
            print(f"❌ 测试执行失败: {str(e)}")
            return False

    def run_single_mode(self, etf_code: str, threshold: str,
                       force_recalculate: bool = False) -> bool:
        """
        运行单ETF计算模式
        
        Args:
            etf_code: ETF代码
            threshold: 门槛类型
            force_recalculate: 是否强制重新计算
            
        Returns:
            计算是否成功
        """
        print(f"📊 计算单个ETF的OBV指标: {etf_code} ({threshold})")
        print("="*60)

        try:
            # 执行单ETF计算
            result = self.controller.calculate_single_etf(
                etf_code, threshold, force_recalculate
            )

            if result['success']:
                print(f"✅ ETF {etf_code} OBV指标计算成功!")

                # 显示结果摘要
                print(f"\n📄 计算结果:")
                print(f"  ETF代码: {result.get('etf_code', 'N/A')}")
                print(f"  门槛类型: {result.get('threshold', 'N/A')}")
                print(f"  数据点数: {result.get('data_points', 0)}")
                print(f"  输出路径: {result.get('output_path', 'N/A')}")
                print(f"  处理时间: {result.get('processing_time', 0):.3f}秒")
                print(f"  缓存命中: {'是' if result.get('cache_hit', False) else '否'}")

                # 显示计算统计
                if 'calculation_stats' in result:
                    calc_stats = result['calculation_stats']
                    print(f"\n📈 OBV计算统计:")
                    print(f"  数据质量:")
                    quality = calc_stats.get('data_quality', {})
                    print(f"    OBV有效率: {quality.get('obv_valid_ratio', 0):.1%}")
                    print(f"    MA10有效率: {quality.get('ma_valid_ratio', 0):.1%}")
                    print(f"    5日变化率有效率: {quality.get('change_5_valid_ratio', 0):.1%}")
                    print(f"    20日变化率有效率: {quality.get('change_20_valid_ratio', 0):.1%}")

                return True
            else:
                print(f"❌ ETF {etf_code} OBV计算失败: {result.get('error', '未知错误')}")
                return False

        except Exception as e:
            print(f"❌ 单ETF计算异常: {str(e)}")
            return False

    def run_batch_mode(self, threshold: str, etf_codes: list = None,
                      force_recalculate: bool = False,
                      max_workers: int = 4) -> bool:
        """
        运行批量计算模式
        
        Args:
            threshold: 门槛类型
            etf_codes: ETF代码列表，None则计算所有
            force_recalculate: 是否强制重新计算
            max_workers: 最大并行线程数
            
        Returns:
            计算是否成功
        """
        print(f"🚀 批量计算OBV指标: {threshold}")
        print("="*60)

        try:
            # 执行批量计算
            result = self.controller.calculate_batch_etfs(
                threshold, etf_codes, force_recalculate, max_workers
            )

            # 显示批量结果摘要
            formatted_summary = self.formatter.format_batch_summary(result)
            print(formatted_summary)

            return result.get('success', False)

        except Exception as e:
            print(f"❌ 批量计算异常: {str(e)}")
            return False

    def run_all_mode(self, force_recalculate: bool = False,
                    max_workers: int = 4) -> bool:
        """
        运行全量计算模式(所有门槛)
        
        Args:
            force_recalculate: 是否强制重新计算
            max_workers: 最大并行线程数
            
        Returns:
            计算是否成功
        """
        print("🚀 全量计算OBV指标(所有门槛)")
        print("="*60)

        try:
            # 执行全量计算
            overall_success = True

            for threshold in ['3000万门槛', '5000万门槛']:
                print(f"\n📊 开始处理 {threshold}...")

                result = self.controller.calculate_batch_etfs(
                    threshold, None, force_recalculate, max_workers
                )

                if result.get('success', False):
                    success_rate = result.get('success_rate', 0)
                    success_count = result.get('success_count', 0)
                    total_count = result.get('total_count', 0)
                    total_time = result.get('total_time', 0)

                    print(f"✅ {threshold} 完成: {success_count}/{total_count} "
                          f"成功 ({success_rate:.1f}%), 耗时 {total_time:.2f}秒")
                else:
                    print(f"❌ {threshold} 失败: {result.get('error', '未知错误')}")
                    overall_success = False

            return overall_success

        except Exception as e:
            print(f"❌ 全量计算异常: {str(e)}")
            return False

    def run_incremental_mode(self, etf_code: str, threshold: str) -> bool:
        """
        运行增量更新模式
        
        Args:
            etf_code: ETF代码
            threshold: 门槛类型
            
        Returns:
            更新是否成功
        """
        print(f"🔄 增量更新OBV指标: {etf_code} ({threshold})")
        print("="*60)

        try:
            # 执行增量更新
            result = self.controller.calculate_incremental_update(etf_code, threshold)

            if result['success']:
                if result.get('incremental', False):
                    if 'message' in result:
                        print(f"ℹ️ {result['message']}")
                    else:
                        new_points = result.get('new_data_points', 0)
                        print(f"✅ ETF {etf_code} 增量更新成功!")
                        print(f"  新增数据点: {new_points}")
                        print(f"  输出路径: {result.get('output_path', 'N/A')}")
                        print(f"  处理时间: {result.get('processing_time', 0):.3f}秒")
                else:
                    print(f"✅ ETF {etf_code} 全量计算完成!")
                    print(f"  数据点数: {result.get('data_points', 0)}")

                return True
            else:
                print(f"❌ ETF {etf_code} 增量更新失败: {result.get('error', '未知错误')}")
                return False

        except Exception as e:
            print(f"❌ 增量更新异常: {str(e)}")
            return False

    def run_status_mode(self) -> bool:
        """运行状态查看模式"""
        print("🖥️  OBV指标系统状态")
        print("="*60)

        try:
            # 获取系统状态
            status = self.controller.get_system_status()

            # 显示格式化状态
            formatted_status = self.formatter.format_system_status(status)
            print(formatted_status)

            return True

        except Exception as e:
            print(f"❌ 获取系统状态失败: {str(e)}")
            return False

    def run_cleanup_mode(self, force: bool = False) -> bool:
        """运行缓存清理模式"""
        print(f"🧹 清理OBV系统缓存 {'(强制)' if force else ''}")
        print("="*60)

        try:
            # 执行系统清理
            cleanup_stats = self.controller.cleanup_system(force)

            if 'error' not in cleanup_stats:
                print(f"✅ 系统清理完成:")
                summary = cleanup_stats.get('summary', {})
                print(f"  删除文件: {summary.get('total_files_removed', 0)}个")
                print(f"  释放空间: {summary.get('total_space_freed_mb', 0):.1f}MB")
                print(f"  强制清理: {'是' if summary.get('force_cleanup', False) else '否'}")

                return True
            else:
                print(f"❌ 系统清理失败: {cleanup_stats['error']}")
                return False

        except Exception as e:
            print(f"❌ 系统清理异常: {str(e)}")
            return False

def create_argument_parser():
    """创建命令行参数解析器"""
    parser = argparse.ArgumentParser(
        description='OBV指标计算系统 - 专业的能量潮指标计算',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
使用示例:
  python obv_main_optimized.py --mode test                           # 系统测试
  python obv_main_optimized.py --mode single --etf 159001 --threshold "3000万门槛"  # 单ETF计算
  python obv_main_optimized.py --mode batch --threshold "3000万门槛"   # 批量计算
  python obv_main_optimized.py --mode all                            # 全量计算
  python obv_main_optimized.py --mode incremental --etf 159001 --threshold "3000万门槛"  # 增量更新
  python obv_main_optimized.py --mode status                         # 系统状态
  python obv_main_optimized.py --mode cleanup --force                # 强制清理缓存

OBV指标说明:
  - obv: 累积能量潮指标值
  - obv_ma10: OBV的10日移动平均
  - obv_change_5: OBV的5日变化率(%)
  - obv_change_20: OBV的20日变化率(%)
        '''
    )

    # 基本参数
    parser.add_argument(
        '--mode',
        choices=['test', 'single', 'batch', 'all', 'incremental', 'status', 'cleanup'],
        required=True,
        help='运行模式'
    )

    # ETF相关参数
    parser.add_argument('--etf', type=str, help='ETF代码(single/incremental模式必需)')
    parser.add_argument(
        '--threshold',
        choices=['3000万门槛', '5000万门槛'],
        help='门槛类型(single/batch/incremental模式必需)'
    )
    parser.add_argument(
        '--etf-list',
        nargs='+',
        help='ETF代码列表(batch模式可选)'
    )

    # 控制参数
    parser.add_argument(
        '--force-recalculate',
        action='store_true',
        help='强制重新计算(忽略缓存)'
    )
    parser.add_argument(
        '--max-workers',
        type=int,
        default=4,
        help='最大并行线程数(默认4)'
    )
    parser.add_argument(
        '--sample-size',
        type=int,
        default=5,
        help='测试样本数量(test模式,默认5)'
    )
    parser.add_argument(
        '--force',
        action='store_true',
        help='强制执行(cleanup模式)'
    )

    # 日志控制
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='详细输出'
    )
    parser.add_argument(
        '--quiet',
        action='store_true',
        help='静默模式'
    )

    return parser

def main():
    """主函数"""
    parser = create_argument_parser()
    args = parser.parse_args()

    # 设置日志级别
    if args.quiet:
        logging.basicConfig(level=logging.ERROR)
    elif args.verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    # 初始化主程序
    try:
        obv_main = OBVMainOptimized()

        print(f"OBV指标计算系统 v1.0.0")
        print(f"基于约瑟夫·格兰维尔理论 | 专为中国ETF市场优化")
        print(f"启动时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print()

        # 根据模式执行相应操作
        success = False

        if args.mode == 'test':
            success = obv_main.run_test_mode(args.sample_size)

        elif args.mode == 'single':
            if not args.etf:
                print("❌ single模式需要指定--etf参数")
                sys.exit(1)
            if not args.threshold:
                print("❌ single模式需要指定--threshold参数")
                sys.exit(1)

            success = obv_main.run_single_mode(
                args.etf, args.threshold, args.force_recalculate
            )

        elif args.mode == 'batch':
            if not args.threshold:
                print("❌ batch模式需要指定--threshold参数")
                sys.exit(1)

            success = obv_main.run_batch_mode(
                args.threshold, args.etf_list,
                args.force_recalculate, args.max_workers
            )

        elif args.mode == 'all':
            success = obv_main.run_all_mode(
                args.force_recalculate, args.max_workers
            )

        elif args.mode == 'incremental':
            if not args.etf:
                print("❌ incremental模式需要指定--etf参数")
                sys.exit(1)
            if not args.threshold:
                print("❌ incremental模式需要指定--threshold参数")
                sys.exit(1)

            success = obv_main.run_incremental_mode(args.etf, args.threshold)

        elif args.mode == 'status':
            success = obv_main.run_status_mode()

        elif args.mode == 'cleanup':
            success = obv_main.run_cleanup_mode(args.force)

        # 输出最终结果
        print("\n" + "="*60)
        if success:
            print("✅ 操作完成成功!")
            print("\n💡 OBV指标说明:")
            print("  • OBV: 累积成交量，反映资金流向趋势")
            print("  • OBV_MA10: 10日移动平均，平滑短期波动")
            print("  • OBV_CHANGE_5: 5日变化率，短期动量指标")
            print("  • OBV_CHANGE_20: 20日变化率，中期趋势指标")
            sys.exit(0)
        else:
            print("❌ 操作执行失败!")
            sys.exit(1)

    except KeyboardInterrupt:
        print("\n⚠️  操作被用户中断")
        sys.exit(1)
    except Exception as e:
        print(f"❌ 程序异常: {str(e)}")
        sys.exit(1)

if __name__ == '__main__':
    main()