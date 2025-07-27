#!/usr/bin/env python3
"""
价量配合度计算系统 - 主启动器
=========================

基于10个核心字段的专业价量配合度计算系统
适用于中国A股ETF深度筛选分析

使用示例:
    python pv_main_optimized.py --mode test
    python pv_main_optimized.py --mode single --etf 159001 --threshold "3000万门槛"
    python pv_main_optimized.py --mode batch --threshold "3000万门槛"
    python pv_main_optimized.py --mode all

版本: 1.0.0
作者: ETF量化分析系统
日期: 2025-07-26
"""

import argparse
import sys
import logging
from pathlib import Path
from datetime import datetime

# 添加当前目录到Python路径
current_dir = Path(__file__).parent
sys.path.insert(0, str(current_dir))

from pv_calculator.controllers.main_controller import PVController
from pv_calculator.outputs.display_formatter import PVDisplayFormatter

class PVMainOptimized:
    """价量配合度系统主启动器"""

    def __init__(self):
        self.controller = PVController()
        self.formatter = PVDisplayFormatter()
        self.logger = logging.getLogger('PVMain')

    def run_test_mode(self, sample_size: int = 5) -> bool:
        """
        运行测试模式

        Args:
            sample_size: 测试样本数量

        Returns:
            测试是否通过
        """
        print("🚀 启动价量配合度系统测试...")
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
        print(f"📊 计算单个ETF价量配合度指标: {etf_code} ({threshold})")
        print("="*60)

        try:
            # 执行单ETF计算
            result = self.controller.calculate_single_etf(
                etf_code, threshold, force_recalculate
            )

            if result['success']:
                print(f"✅ ETF {etf_code} 计算成功!")

                # 显示结果摘要
                if 'output_info' in result:
                    output_info = result['output_info']
                    print(f"\n📄 输出信息:")
                    print(f"  文件路径: {output_info.get('file_path', 'N/A')}")
                    print(f"  记录数量: {output_info.get('record_count', 0)}")
                    print(f"  文件大小: {output_info.get('file_size_kb', 0):.1f}KB")

                # 显示计算详情
                if 'calculation_details' in result:
                    calc_details = result['calculation_details']
                    print(f"\n📈 计算详情:")
                    print(f"  数据来源: {calc_details.get('source', 'N/A')}")
                    print(f"  处理时间: {result.get('processing_time', 0):.3f}秒")
                    print(f"  缓存命中: {'是' if result.get('cache_hit', False) else '否'}")

                # 显示价量配合度分析
                if 'pv_analysis' in result:
                    pv_analysis = result['pv_analysis']
                    print(f"\n📊 价量配合度分析:")
                    formatted_analysis = self.formatter.format_pv_analysis_summary(pv_analysis)
                    print(formatted_analysis)

                return True
            else:
                print(f"❌ ETF {etf_code} 计算失败: {result.get('error', '未知错误')}")
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
        print(f"🚀 批量计算价量配合度指标: {threshold}")
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
        print("🚀 全量计算价量配合度指标(所有门槛)")
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
                    processed_count = result.get('processed_count', 0)
                    total_count = result.get('total_count', 0)
                    total_time = result.get('total_time', 0)

                    print(f"✅ {threshold} 完成: {processed_count}/{total_count} "
                          f"成功 ({success_rate:.1f}%), 耗时 {total_time:.2f}秒")
                else:
                    print(f"❌ {threshold} 失败: {result.get('error', '未知错误')}")
                    overall_success = False

            return overall_success

        except Exception as e:
            print(f"❌ 全量计算异常: {str(e)}")
            return False

    def run_status_mode(self) -> bool:
        """运行状态查看模式"""
        print("🖥️  价量配合度系统状态")
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
        print(f"🧹 清理价量配合度缓存 {'(强制)' if force else ''}")
        print("="*60)

        try:
            # 执行缓存清理
            cleanup_stats = self.controller.cleanup_cache(force)

            if 'error' in cleanup_stats:
                print(f"❌ 缓存清理失败: {cleanup_stats['error']}")
                return False

            # 显示清理统计
            print(f"✅ 缓存清理完成:")
            print(f"  删除文件: {cleanup_stats.get('files_removed', 0)}个")
            print(f"  释放空间: {cleanup_stats.get('space_freed_mb', 0):.1f}MB")
            print(f"  过期缓存: {cleanup_stats.get('expired_count', 0)}个")
            print(f"  孤立文件: {cleanup_stats.get('orphaned_count', 0)}个")

            return True

        except Exception as e:
            print(f"❌ 缓存清理异常: {str(e)}")
            return False

    def run_analysis_mode(self, etf_code: str, threshold: str) -> bool:
        """运行深度分析模式"""
        print(f"🔍 深度分析ETF价量配合度: {etf_code} ({threshold})")
        print("="*60)

        try:
            # 获取计算结果
            result = self.controller.calculate_single_etf(etf_code, threshold, False)
            
            if not result['success']:
                print(f"❌ 无法获取 {etf_code} 的计算结果")
                return False

            # 执行深度分析
            analysis_result = self.controller.get_detailed_analysis(etf_code, threshold)
            
            if 'error' in analysis_result:
                print(f"❌ 深度分析失败: {analysis_result['error']}")
                return False

            # 显示分析结果
            formatted_analysis = self.formatter.format_detailed_analysis(analysis_result)
            print(formatted_analysis)

            # 显示投资建议
            if 'investment_advice' in analysis_result:
                advice = analysis_result['investment_advice']
                print(f"\n💡 投资建议:")
                print(f"  推荐等级: {advice.get('grade', 'N/A')}")
                print(f"  风险评估: {advice.get('risk_level', 'N/A')}")
                print(f"  操作建议: {advice.get('operation_advice', 'N/A')}")

            return True

        except Exception as e:
            print(f"❌ 深度分析异常: {str(e)}")
            return False

def create_argument_parser():
    """创建命令行参数解析器"""
    parser = argparse.ArgumentParser(
        description='价量配合度计算系统',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
使用示例:
  python pv_main_optimized.py --mode test                           # 系统测试
  python pv_main_optimized.py --mode single --etf 159001 --threshold "3000万门槛"  # 单ETF计算
  python pv_main_optimized.py --mode batch --threshold "3000万门槛"   # 批量计算
  python pv_main_optimized.py --mode all                            # 全量计算
  python pv_main_optimized.py --mode status                         # 系统状态
  python pv_main_optimized.py --mode cleanup --force                # 强制清理缓存
  python pv_main_optimized.py --mode analysis --etf 159001 --threshold "3000万门槛"  # 深度分析
        '''
    )

    # 基本参数
    parser.add_argument(
        '--mode',
        choices=['test', 'single', 'batch', 'all', 'status', 'cleanup', 'analysis'],
        required=True,
        help='运行模式'
    )

    # ETF相关参数
    parser.add_argument('--etf', type=str, help='ETF代码(single/analysis模式必需)')
    parser.add_argument(
        '--threshold',
        choices=['3000万门槛', '5000万门槛'],
        help='门槛类型(single/batch/analysis模式必需)'
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
        pv_main = PVMainOptimized()

        print(f"价量配合度计算系统 v1.0.0")
        print(f"启动时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print()

        # 根据模式执行相应操作
        success = False

        if args.mode == 'test':
            success = pv_main.run_test_mode(args.sample_size)

        elif args.mode == 'single':
            if not args.etf:
                print("❌ single模式需要指定--etf参数")
                sys.exit(1)
            if not args.threshold:
                print("❌ single模式需要指定--threshold参数")
                sys.exit(1)

            success = pv_main.run_single_mode(
                args.etf, args.threshold, args.force_recalculate
            )

        elif args.mode == 'batch':
            if not args.threshold:
                print("❌ batch模式需要指定--threshold参数")
                sys.exit(1)

            success = pv_main.run_batch_mode(
                args.threshold, args.etf_list,
                args.force_recalculate, args.max_workers
            )

        elif args.mode == 'all':
            success = pv_main.run_all_mode(
                args.force_recalculate, args.max_workers
            )

        elif args.mode == 'status':
            success = pv_main.run_status_mode()

        elif args.mode == 'cleanup':
            success = pv_main.run_cleanup_mode(args.force)

        elif args.mode == 'analysis':
            if not args.etf:
                print("❌ analysis模式需要指定--etf参数")
                sys.exit(1)
            if not args.threshold:
                print("❌ analysis模式需要指定--threshold参数")
                sys.exit(1)

            success = pv_main.run_analysis_mode(args.etf, args.threshold)

        # 输出最终结果
        print("\n" + "="*60)
        if success:
            print("✅ 操作完成成功!")
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