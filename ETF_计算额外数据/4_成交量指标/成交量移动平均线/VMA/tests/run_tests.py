#!/usr/bin/env python3
"""
VMA系统测试运行器
================

统一的测试运行脚本，支持不同类型的测试和报告生成
"""

import sys
import unittest
import argparse
import time
import os
from pathlib import Path
from io import StringIO
import json

# 添加项目路径
sys.path.insert(0, str(Path(__file__).parent.parent))

# 测试发现和运行
class VMATestRunner:
    """VMA测试运行器"""

    def __init__(self):
        self.test_root = Path(__file__).parent
        self.results = {
            'total_tests': 0,
            'passed_tests': 0,
            'failed_tests': 0,
            'error_tests': 0,
            'skipped_tests': 0,
            'execution_time': 0,
            'test_details': []
        }

    def discover_tests(self, test_type: str = 'all') -> unittest.TestSuite:
        """发现测试用例"""
        loader = unittest.TestLoader()
        suite = unittest.TestSuite()

        test_patterns = {
            'unit': 'unit/test_*.py',
            'integration': 'integration/test_*.py',
            'performance': 'performance/test_*.py',
            'all': 'test_*.py'
        }

        if test_type not in test_patterns:
            raise ValueError(f"不支持的测试类型: {test_type}")

        if test_type == 'all':
            # 运行所有测试
            for test_dir in ['unit', 'integration', 'performance']:
                test_path = self.test_root / test_dir
                if test_path.exists():
                    discovered = loader.discover(str(test_path), pattern='test_*.py')
                    suite.addTest(discovered)
        else:
            # 运行特定类型的测试
            test_path = self.test_root / test_type
            if test_path.exists():
                discovered = loader.discover(str(test_path), pattern='test_*.py')
                suite.addTest(discovered)

        return suite

    def run_tests(self, test_type: str = 'all', verbosity: int = 2) -> dict:
        """运行测试"""
        print(f"开始运行{test_type}测试...")
        print("=" * 60)

        # 发现测试用例
        suite = self.discover_tests(test_type)

        if suite.countTestCases() == 0:
            print(f"未找到{test_type}测试用例")
            return self.results

        # 创建测试运行器
        stream = StringIO()
        runner = unittest.TextTestRunner(
            stream=stream,
            verbosity=verbosity,
            buffer=True
        )

        # 运行测试
        start_time = time.time()
        result = runner.run(suite)
        end_time = time.time()

        # 收集结果
        self.results.update({
            'total_tests': result.testsRun,
            'passed_tests': result.testsRun - len(result.failures) - len(result.errors) - len(result.skipped),
            'failed_tests': len(result.failures),
            'error_tests': len(result.errors),
            'skipped_tests': len(result.skipped),
            'execution_time': end_time - start_time
        })

        # 收集详细信息
        for test, traceback in result.failures:
            self.results['test_details'].append({
                'test': str(test),
                'status': 'FAILED',
                'message': traceback
            })

        for test, traceback in result.errors:
            self.results['test_details'].append({
                'test': str(test),
                'status': 'ERROR',
                'message': traceback
            })

        for test, reason in result.skipped:
            self.results['test_details'].append({
                'test': str(test),
                'status': 'SKIPPED',
                'message': reason
            })

        # 输出测试结果
        print(stream.getvalue())
        self._print_summary()

        return self.results

    def _print_summary(self):
        """打印测试摘要"""
        print("\n" + "=" * 60)
        print("测试执行摘要")
        print("=" * 60)

        total = self.results['total_tests']
        passed = self.results['passed_tests']
        failed = self.results['failed_tests']
        errors = self.results['error_tests']
        skipped = self.results['skipped_tests']

        print(f"总测试数: {total}")
        print(f"通过: {passed} ({passed/total*100:.1f}%)" if total > 0 else "通过: 0")
        print(f"失败: {failed}")
        print(f"错误: {errors}")
        print(f"跳过: {skipped}")
        print(f"执行时间: {self.results['execution_time']:.2f}秒")

        # 计算成功率
        if total > 0:
            success_rate = passed / total * 100
            print(f"成功率: {success_rate:.1f}%")

            if success_rate >= 90:
                print("✅ 测试结果: 优秀")
            elif success_rate >= 80:
                print("⚠️  测试结果: 良好")
            elif success_rate >= 70:
                print("⚠️  测试结果: 一般")
            else:
                print("❌ 测试结果: 需要改进")

        # 如果有失败或错误，显示详情
        if failed > 0 or errors > 0:
            print("\n失败和错误详情:")
            print("-" * 40)
            for detail in self.results['test_details']:
                if detail['status'] in ['FAILED', 'ERROR']:
                    print(f"\n{detail['status']}: {detail['test']}")
                    print(detail['message'][:200] + "..." if len(detail['message']) > 200 else detail['message'])

    def save_results(self, output_file: str = None):
        """保存测试结果到文件"""
        if output_file is None:
            timestamp = time.strftime("%Y%m%d_%H%M%S")
            output_file = f"test_results_{timestamp}.json"

        output_path = self.test_root / output_file

        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(self.results, f, ensure_ascii=False, indent=2)

        print(f"\n测试结果已保存到: {output_path}")


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='VMA系统测试运行器')
    parser.add_argument(
        '--type', '-t',
        choices=['unit', 'integration', 'performance', 'all'],
        default='all',
        help='测试类型 (默认: all)'
    )
    parser.add_argument(
        '--verbosity', '-v',
        type=int,
        choices=[0, 1, 2],
        default=2,
        help='输出详细程度 (默认: 2)'
    )
    parser.add_argument(
        '--output', '-o',
        type=str,
        help='结果输出文件路径'
    )
    parser.add_argument(
        '--quiet', '-q',
        action='store_true',
        help='静默模式，只显示摘要'
    )

    args = parser.parse_args()

    # 调整输出详细程度
    if args.quiet:
        args.verbosity = 0

    # 创建测试运行器
    runner = VMATestRunner()

    try:
        # 运行测试
        results = runner.run_tests(args.type, args.verbosity)

        # 保存结果
        if args.output:
            runner.save_results(args.output)

        # 根据测试结果设置退出码
        if results['failed_tests'] > 0 or results['error_tests'] > 0:
            sys.exit(1)
        else:
            sys.exit(0)

    except Exception as e:
        print(f"测试运行失败: {e}")
        sys.exit(2)


if __name__ == '__main__':
    main()