#!/usr/bin/env python3
"""
价量配合度系统测试运行器
======================

统一的测试运行脚本，支持不同类型的测试和报告生成
基于价量配合度计算系统的专业测试框架
"""

import sys
import unittest
import argparse
import time
import os
from pathlib import Path
from io import StringIO
import json
import pandas as pd
import numpy as np

# 添加项目路径
sys.path.insert(0, str(Path(__file__).parent.parent))

from pv_calculator.engines.pv_engine import PVEngine
from pv_calculator.infrastructure.config import PVConfig
from pv_calculator.infrastructure.data_reader import PVDataReader
from pv_calculator.controllers.main_controller import PVController

class PVTestRunner:
    """价量配合度测试运行器"""

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
        print(f"开始运行价量配合度{test_type}测试...")
        print("=" * 60)

        # 发现测试用例
        suite = self.discover_tests(test_type)

        if suite.countTestCases() == 0:
            print(f"未找到{test_type}测试用例，运行内置基础测试")
            return self.run_builtin_tests()

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
            'execution_time': end_time - start_time,
            'test_details': self._format_test_details(result)
        })

        # 输出结果
        self._print_test_summary()
        
        return self.results

    def run_builtin_tests(self) -> dict:
        """运行内置基础测试"""
        print("运行内置价量配合度基础测试...")
        
        start_time = time.time()
        test_results = []
        
        # 测试1: 配置系统测试
        test_results.append(self._test_config_system())
        
        # 测试2: 数据引擎测试
        test_results.append(self._test_pv_engine())
        
        # 测试3: 指标计算测试
        test_results.append(self._test_pv_indicators())
        
        # 测试4: 数据质量验证测试
        test_results.append(self._test_data_quality())
        
        # 测试5: 系统集成测试
        test_results.append(self._test_system_integration())
        
        end_time = time.time()
        
        # 统计结果
        total_tests = len(test_results)
        passed_tests = sum(1 for result in test_results if result['passed'])
        failed_tests = total_tests - passed_tests
        
        self.results.update({
            'total_tests': total_tests,
            'passed_tests': passed_tests,
            'failed_tests': failed_tests,
            'error_tests': 0,
            'skipped_tests': 0,
            'execution_time': end_time - start_time,
            'test_details': test_results
        })
        
        self._print_test_summary()
        return self.results

    def _test_config_system(self) -> dict:
        """测试配置系统"""
        test_name = "价量配合度配置系统测试"
        print(f"  🧪 {test_name}...")
        
        try:
            config = PVConfig()
            
            # 验证关键配置
            assert len(config.correlation_periods) == 3
            assert config.correlation_periods == [10, 20, 30]
            assert len(config.vpt_periods) == 3
            assert config.volume_quality_window == 20
            assert config.get_warmup_period() > 0
            assert config.get_precision_digits() == 8
            
            # 验证筛选阈值
            assert 'grade_a' in config.screening_thresholds
            assert 'grade_b' in config.screening_thresholds
            assert 'grade_c' in config.screening_thresholds
            assert 'grade_d' in config.screening_thresholds
            
            print(f"    ✅ {test_name} - 通过")
            return {'test_name': test_name, 'passed': True, 'message': '配置系统正常'}
            
        except Exception as e:
            print(f"    ❌ {test_name} - 失败: {str(e)}")
            return {'test_name': test_name, 'passed': False, 'message': str(e)}

    def _test_pv_engine(self) -> dict:
        """测试价量配合度计算引擎"""
        test_name = "价量配合度计算引擎测试"
        print(f"  🧪 {test_name}...")
        
        try:
            engine = PVEngine()
            
            # 创建测试数据
            test_data = self._create_test_data()
            
            # 执行计算
            result = engine.calculate_pv_indicators(test_data)
            
            # 验证结果
            assert not result.empty, "计算结果不能为空"
            
            expected_columns = [
                'pv_corr_10', 'pv_corr_20', 'pv_corr_30',
                'vpt', 'vpt_momentum', 'vpt_ratio',
                'volume_quality', 'volume_consistency',
                'pv_strength', 'pv_divergence'
            ]
            
            for col in expected_columns:
                assert col in result.columns, f"缺少必要列: {col}"
            
            # 验证数据类型和范围
            assert result['pv_corr_10'].dtype in [np.float64, float], "相关系数数据类型错误"
            assert result['volume_quality'].min() >= 0, "成交量质量不能为负"
            assert result['volume_quality'].max() <= 100, "成交量质量不能超过100"
            
            print(f"    ✅ {test_name} - 通过")
            return {'test_name': test_name, 'passed': True, 'message': f'成功计算{len(result)}条记录'}
            
        except Exception as e:
            print(f"    ❌ {test_name} - 失败: {str(e)}")
            return {'test_name': test_name, 'passed': False, 'message': str(e)}

    def _test_pv_indicators(self) -> dict:
        """测试价量配合度指标计算"""
        test_name = "价量配合度指标计算测试"
        print(f"  🧪 {test_name}...")
        
        try:
            engine = PVEngine()
            test_data = self._create_test_data()
            result = engine.calculate_pv_indicators(test_data)
            
            if result.empty:
                raise ValueError("计算结果为空")
            
            # 检查各项指标的合理性
            latest_row = result.iloc[0]  # 最新数据
            
            # 相关系数应该在[-1, 1]范围内
            for col in ['pv_corr_10', 'pv_corr_20', 'pv_corr_30']:
                value = latest_row[col]
                if not pd.isna(value):
                    assert -1 <= value <= 1, f"{col}值超出合理范围: {value}"
            
            # 成交量质量应该在[0, 100]范围内
            vol_quality = latest_row['volume_quality']
            if not pd.isna(vol_quality):
                assert 0 <= vol_quality <= 100, f"成交量质量超出范围: {vol_quality}"
            
            # VPT应该是数值类型
            vpt = latest_row['vpt']
            assert pd.notna(vpt) or isinstance(vpt, (int, float)), "VPT值应该是数值类型"
            
            print(f"    ✅ {test_name} - 通过")
            return {'test_name': test_name, 'passed': True, 'message': '指标计算合理性验证通过'}
            
        except Exception as e:
            print(f"    ❌ {test_name} - 失败: {str(e)}")
            return {'test_name': test_name, 'passed': False, 'message': str(e)}

    def _test_data_quality(self) -> dict:
        """测试数据质量验证"""
        test_name = "数据质量验证测试"
        print(f"  🧪 {test_name}...")
        
        try:
            engine = PVEngine()
            
            # 测试正常数据
            normal_data = self._create_test_data()
            quality_report = engine.validate_data_quality(normal_data)
            assert quality_report['valid'] == True, "正常数据应该通过质量验证"
            
            # 测试异常数据 - 空数据
            empty_data = pd.DataFrame()
            quality_report_empty = engine.validate_data_quality(empty_data)
            assert quality_report_empty['valid'] == False, "空数据应该无法通过质量验证"
            
            # 测试异常数据 - 缺少必要列
            incomplete_data = pd.DataFrame({
                '收盘价(元)': [10, 11, 12],
                '日期': ['2025-01-01', '2025-01-02', '2025-01-03']
                # 缺少成交量列
            })
            quality_report_incomplete = engine.validate_data_quality(incomplete_data)
            assert quality_report_incomplete['valid'] == False, "不完整数据应该无法通过质量验证"
            
            print(f"    ✅ {test_name} - 通过")
            return {'test_name': test_name, 'passed': True, 'message': '数据质量验证功能正常'}
            
        except Exception as e:
            print(f"    ❌ {test_name} - 失败: {str(e)}")
            return {'test_name': test_name, 'passed': False, 'message': str(e)}

    def _test_system_integration(self) -> dict:
        """测试系统集成"""
        test_name = "系统集成测试"
        print(f"  🧪 {test_name}...")
        
        try:
            # 初始化系统组件
            config = PVConfig()
            engine = PVEngine(config)
            
            # 测试配置获取
            calc_config = engine.get_calculation_config()
            assert 'correlation_periods' in calc_config, "配置信息不完整"
            assert 'warmup_period' in calc_config, "缺少预热期配置"
            
            # 测试增量计算
            test_data = self._create_test_data()
            
            # 首次计算
            result1 = engine.calculate_pv_indicators(test_data)
            
            # 增量计算（相同数据）
            result2 = engine.calculate_incremental(result1, test_data)
            
            # 验证增量计算结果
            assert not result2.empty, "增量计算结果不能为空"
            assert len(result2.columns) == len(result1.columns), "增量计算列数应该一致"
            
            print(f"    ✅ {test_name} - 通过")
            return {'test_name': test_name, 'passed': True, 'message': '系统集成功能正常'}
            
        except Exception as e:
            print(f"    ❌ {test_name} - 失败: {str(e)}")
            return {'test_name': test_name, 'passed': False, 'message': str(e)}

    def _create_test_data(self, rows: int = 100) -> pd.DataFrame:
        """创建测试数据"""
        np.random.seed(42)  # 固定随机种子以确保测试可重复
        
        dates = pd.date_range('2024-01-01', periods=rows, freq='D')
        
        # 生成模拟的价格和成交量数据
        base_price = 10.0
        prices = []
        volumes = []
        
        for i in range(rows):
            # 价格随机游走
            if i == 0:
                price = base_price
            else:
                price_change = np.random.normal(0, 0.02)  # 2%标准差
                price = prices[-1] * (1 + price_change)
                price = max(price, 1.0)  # 价格不能低于1元
            
            prices.append(price)
            
            # 成交量与价格变动有一定相关性
            if i == 0:
                volume = 10000
            else:
                price_change_rate = (price - prices[-2]) / prices[-2]
                volume_base = 10000 + abs(price_change_rate) * 50000  # 价格变动越大，成交量越大
                volume = max(int(volume_base + np.random.normal(0, 5000)), 100)
            
            volumes.append(volume)
        
        return pd.DataFrame({
            '日期': dates,
            '收盘价(元)': prices,
            '成交量(手数)': volumes,
            '代码': ['TEST001'] * rows
        })

    def _format_test_details(self, result) -> list:
        """格式化测试详情"""
        details = []
        
        # 处理失败的测试
        for test, traceback in result.failures:
            details.append({
                'test_name': str(test),
                'status': 'failed',
                'message': traceback
            })
        
        # 处理错误的测试
        for test, traceback in result.errors:
            details.append({
                'test_name': str(test),
                'status': 'error',
                'message': traceback
            })
        
        # 处理跳过的测试
        for test, reason in result.skipped:
            details.append({
                'test_name': str(test),
                'status': 'skipped',
                'message': reason
            })
        
        return details

    def _print_test_summary(self):
        """打印测试摘要"""
        print("\n" + "=" * 60)
        print("📊 价量配合度测试摘要")
        print("=" * 60)
        print(f"总测试数: {self.results['total_tests']}")
        print(f"通过: {self.results['passed_tests']} ✅")
        print(f"失败: {self.results['failed_tests']} ❌")
        print(f"错误: {self.results['error_tests']} 💥")
        print(f"跳过: {self.results['skipped_tests']} ⏭️")
        print(f"执行时间: {self.results['execution_time']:.2f}秒")
        
        # 计算成功率
        if self.results['total_tests'] > 0:
            success_rate = (self.results['passed_tests'] / self.results['total_tests']) * 100
            print(f"成功率: {success_rate:.1f}%")
            
            if success_rate >= 90:
                print("🎉 测试状态: 优秀")
            elif success_rate >= 80:
                print("👍 测试状态: 良好")
            elif success_rate >= 70:
                print("⚠️  测试状态: 需要改进")
            else:
                print("❌ 测试状态: 存在严重问题")
        
        print("=" * 60)

    def generate_report(self, output_file: str = None):
        """生成测试报告"""
        if output_file is None:
            output_file = f"pv_test_report_{int(time.time())}.json"
        
        report = {
            'test_suite': 'Price-Volume Coordination System',
            'timestamp': time.strftime('%Y-%m-%d %H:%M:%S'),
            'results': self.results
        }
        
        try:
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(report, f, ensure_ascii=False, indent=2)
            print(f"📄 测试报告已保存到: {output_file}")
        except Exception as e:
            print(f"❌ 保存测试报告失败: {str(e)}")

def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='价量配合度系统测试运行器')
    parser.add_argument(
        '--type',
        choices=['unit', 'integration', 'performance', 'all'],
        default='all',
        help='测试类型'
    )
    parser.add_argument(
        '--verbosity',
        type=int,
        choices=[0, 1, 2],
        default=2,
        help='输出详细程度'
    )
    parser.add_argument(
        '--report',
        type=str,
        help='测试报告输出文件'
    )
    
    args = parser.parse_args()
    
    # 运行测试
    runner = PVTestRunner()
    results = runner.run_tests(args.type, args.verbosity)
    
    # 生成报告
    if args.report:
        runner.generate_report(args.report)
    
    # 返回适当的退出码
    if results['failed_tests'] == 0 and results['error_tests'] == 0:
        sys.exit(0)
    else:
        sys.exit(1)

if __name__ == '__main__':
    main()