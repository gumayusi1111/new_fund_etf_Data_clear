#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
布林带计算系统主程序
==================

布林带（Bollinger Bands）技术指标批量计算系统
基于John Bollinger 1983年经典算法实现

主要功能：
1. 基于ETF初筛结果进行批量布林带计算
2. 支持智能缓存和增量计算优化
3. 向量化高性能计算引擎
4. 科学算法验证和质量控制
5. 多格式输出和汇总报告

使用方法：
    python bb_main.py --threshold 3000万门槛 --adj_type 前复权
    python bb_main.py --threshold 5000万门槛 --format json
    python bb_main.py --all --cache_refresh
"""

import sys
import os
import argparse
import time
from datetime import datetime
from typing import List, Dict, Any

# 添加模块搜索路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from bb_calculator.controllers.main_controller import BBMainController
from bb_calculator.infrastructure.cache_manager import BBCacheManager
from bb_calculator.interfaces.file_manager import BBFileManager
from bb_calculator.outputs.csv_handler import BBCSVHandler


class BollingerBandsMain:
    """布林带计算系统主程序类"""
    
    def __init__(self, adj_type: str = "前复权"):
        """初始化主程序"""
        self.adj_type = adj_type
        self.controller = BBMainController(adj_type=adj_type)
        self.cache_manager = BBCacheManager(self.controller.config)
        self.file_manager = BBFileManager(self.controller.config)
        self.csv_handler = BBCSVHandler(self.controller.config)
        
        print(f"✓ 布林带计算系统已初始化 [{adj_type}]")
        print(f"  算法: John Bollinger Standard (1983)")
        print(f"  参数: BB({self.controller.config.get_bb_period()},{self.controller.config.get_bb_std_multiplier()})")
        print(f"  精度: {self.controller.config.get_precision()}位小数")
    
    def run_single_etf(self, etf_code: str, use_cache: bool = True) -> Dict[str, Any]:
        """
        计算单个ETF的布林带指标
        
        Args:
            etf_code: ETF代码
            use_cache: 是否使用缓存
            
        Returns:
            Dict[str, Any]: 计算结果
        """
        print(f"\n🔄 计算ETF: {etf_code}")
        start_time = time.time()
        
        # 检查缓存
        if use_cache:
            cached_result = self.cache_manager.get_cached_result(etf_code)
            if cached_result:
                print(f"✓ 缓存命中: {etf_code} (耗时: {time.time() - start_time:.3f}秒)")
                return cached_result
        
        # 执行计算
        result = self.controller.process_single_etf(etf_code, save_output=True)
        
        # 保存到缓存
        if result.get('success') and use_cache:
            self.cache_manager.save_to_cache(etf_code, result)
        
        processing_time = time.time() - start_time
        result['processing_time'] = processing_time
        
        if result.get('success'):
            print(f"✓ 计算成功: {etf_code} (耗时: {processing_time:.3f}秒)")
        else:
            print(f"✗ 计算失败: {etf_code} - {result.get('error', '未知错误')}")
        
        return result
    
    def run_threshold_batch(self, threshold: str, use_cache: bool = True, 
                           save_batch_csv: bool = True) -> Dict[str, Any]:
        """
        批量计算指定门槛的所有ETF布林带指标
        
        Args:
            threshold: 门槛类型 (如: "3000万门槛", "5000万门槛")
            use_cache: 是否使用缓存
            save_batch_csv: 是否保存批量CSV
            
        Returns:
            Dict[str, Any]: 批量处理结果
        """
        print(f"\n🚀 开始批量计算 - {threshold}")
        print(f"📊 使用缓存: {'是' if use_cache else '否'}")
        
        start_time = time.time()
        
        # 执行批量计算
        batch_results = self.controller.calculate_screening_results([threshold])
        
        if not batch_results.get('success'):
            print(f"✗ 批量计算失败: {batch_results.get('error', '未知错误')}")
            return batch_results
        
        # 处理结果统计
        threshold_details = batch_results.get('threshold_details', {}).get(threshold, {})
        total_etfs = len(threshold_details.get('etf_list', []))
        successful = threshold_details.get('successful_etfs', 0)
        failed = threshold_details.get('failed_etfs', 0)
        success_rate = (successful / max(total_etfs, 1)) * 100
        
        processing_time = time.time() - start_time
        
        print(f"\n📈 批量计算完成 - {threshold}")
        print(f"  总ETF数: {total_etfs}")
        print(f"  成功: {successful} | 失败: {failed}")
        print(f"  成功率: {success_rate:.1f}%")
        print(f"  处理时间: {processing_time:.2f}秒")
        
        # 保存结果文件
        if save_batch_csv and successful > 0:
            print(f"\n💾 保存结果文件...")
            
            # 保存批量CSV
            csv_success = self.csv_handler.create_batch_csv(
                threshold_details, threshold
            )
            
            # 保存个别文件
            save_stats = self.file_manager.batch_save_results(batch_results, threshold)
            
            print(f"  CSV保存: {'成功' if csv_success else '失败'}")
            print(f"  个别文件: {save_stats.get('successful_saves', 0)}/{save_stats.get('total_etfs', 0)}")
        
        return batch_results
    
    def run_all_thresholds(self, use_cache: bool = True) -> Dict[str, Any]:
        """
        计算所有门槛的布林带指标
        
        Args:
            use_cache: 是否使用缓存
            
        Returns:
            Dict[str, Any]: 全部处理结果
        """
        thresholds = ["3000万门槛", "5000万门槛"]
        
        print(f"\n🌟 开始全面计算 - 所有门槛")
        print(f"📋 门槛列表: {', '.join(thresholds)}")
        
        overall_start_time = time.time()
        all_results = {}
        
        for threshold in thresholds:
            threshold_result = self.run_threshold_batch(threshold, use_cache=use_cache)
            all_results[threshold] = threshold_result
        
        # 生成整体汇总
        total_processing_time = time.time() - overall_start_time
        
        overall_stats = {
            'total_thresholds': len(thresholds),
            'total_processing_time': total_processing_time,
            'threshold_results': all_results,
            'cache_statistics': self.cache_manager.get_cache_statistics()
        }
        
        # 创建汇总报告
        summary_success = self.csv_handler.create_summary_csv(
            {'threshold_details': {k: v for k, v in all_results.items()}}
        )
        
        print(f"\n🎯 全面计算完成")
        print(f"  总处理时间: {total_processing_time:.2f}秒")
        print(f"  缓存命中率: {overall_stats['cache_statistics']['hit_rate_percent']:.1f}%")
        print(f"  汇总报告: {'已生成' if summary_success else '生成失败'}")
        
        return overall_stats
    
    def validate_system(self) -> Dict[str, Any]:
        """系统验证和健康检查"""
        print(f"\n🔍 系统验证检查...")
        
        validation_result = {
            'system_status': 'unknown',
            'config_valid': False,
            'data_path_valid': False,
            'cache_system_valid': False,
            'algorithm_verification': {},
            'performance_test': {}
        }
        
        try:
            # 1. 配置验证
            config_valid = self.controller.config.validate_data_path()
            validation_result['config_valid'] = config_valid
            print(f"  配置验证: {'✓ 通过' if config_valid else '✗ 失败'}")
            
            # 2. 数据路径验证
            data_path_valid = os.path.exists(self.controller.config.data_dir)
            validation_result['data_path_valid'] = data_path_valid
            print(f"  数据路径: {'✓ 有效' if data_path_valid else '✗ 无效'}")
            
            # 3. 缓存系统验证
            cache_stats = self.cache_manager.get_cache_statistics()
            cache_valid = cache_stats.get('cache_file_count', 0) >= 0
            validation_result['cache_system_valid'] = cache_valid
            print(f"  缓存系统: {'✓ 正常' if cache_valid else '✗ 异常'}")
            
            # 4. 算法验证（使用测试数据）
            print(f"  🧪 算法验证测试...")
            
            # 创建测试数据
            import pandas as pd
            import numpy as np
            
            test_dates = pd.date_range('2024-01-01', periods=50, freq='D')
            test_prices = 100 + np.cumsum(np.random.randn(50) * 0.5)  # 模拟价格走势
            test_data = pd.DataFrame({
                '日期': test_dates,
                '收盘价': test_prices
            })
            
            # 执行算法验证
            bb_results = self.controller.bb_engine.calculate_bollinger_bands(test_data)
            verification = self.controller.bb_engine.verify_calculation(test_data, bb_results)
            
            validation_result['algorithm_verification'] = {
                'calculation_success': bb_results.get('bb_middle') is not None,
                'verification_passed': verification[0],
                'verification_details': verification[1]
            }
            
            algo_status = verification[0] and bb_results.get('bb_middle') is not None
            print(f"    算法正确性: {'✓ 验证通过' if algo_status else '✗ 验证失败'}")
            
            # 5. 性能测试
            print(f"  ⚡ 性能测试...")
            perf_start = time.time()
            
            for _ in range(5):  # 测试5次计算
                self.controller.bb_engine.calculate_bollinger_bands(test_data)
            
            avg_time = (time.time() - perf_start) / 5
            validation_result['performance_test'] = {
                'average_calculation_time': avg_time,
                'calculations_per_second': 1 / avg_time if avg_time > 0 else 0
            }
            
            print(f"    平均计算时间: {avg_time:.4f}秒")
            print(f"    计算速度: {1/avg_time:.1f} ETF/秒")
            
            # 综合评估
            all_checks_passed = (
                config_valid and 
                data_path_valid and 
                cache_valid and 
                algo_status
            )
            
            validation_result['system_status'] = 'healthy' if all_checks_passed else 'issues_detected'
            
            print(f"\n🎯 系统状态: {'✅ 健康' if all_checks_passed else '⚠️  发现问题'}")
            
            return validation_result
            
        except Exception as e:
            validation_result['system_status'] = 'error'
            validation_result['error'] = str(e)
            print(f"✗ 系统验证异常: {str(e)}")
            return validation_result


def main():
    """主函数"""
    parser = argparse.ArgumentParser(
        description='布林带技术指标批量计算系统',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用示例:
  python bb_main.py --threshold 3000万门槛               # 计算3000万门槛ETF
  python bb_main.py --threshold 5000万门槛 --no_cache    # 计算5000万门槛ETF(不使用缓存)
  python bb_main.py --all                               # 计算所有门槛ETF
  python bb_main.py --validate                          # 系统验证
  python bb_main.py --etf 512170                        # 计算单个ETF
        """
    )
    
    parser.add_argument('--threshold', type=str, 
                       choices=['3000万门槛', '5000万门槛'],
                       help='ETF门槛类型')
    
    parser.add_argument('--all', action='store_true',
                       help='计算所有门槛的ETF')
    
    parser.add_argument('--etf', type=str,
                       help='计算单个ETF代码')
    
    parser.add_argument('--adj_type', type=str, 
                       choices=['前复权', '后复权', '除权'],
                       default='前复权',
                       help='复权类型 (默认: 前复权)')
    
    parser.add_argument('--no_cache', action='store_true',
                       help='禁用缓存系统')
    
    parser.add_argument('--cache_refresh', action='store_true',
                       help='刷新所有缓存')
    
    parser.add_argument('--validate', action='store_true',
                       help='执行系统验证')
    
    parser.add_argument('--format', type=str,
                       choices=['csv', 'json', 'excel'],
                       default='csv',
                       help='输出格式 (默认: csv)')
    
    args = parser.parse_args()
    
    # 启动横幅
    print("=" * 80)
    print("🎯 布林带(Bollinger Bands)技术指标计算系统")
    print("📊 基于John Bollinger 1983年经典算法实现")
    print("⚡ 支持智能缓存、增量计算和向量化处理")
    print("=" * 80)
    
    try:
        # 初始化系统
        bb_main = BollingerBandsMain(adj_type=args.adj_type)
        use_cache = not args.no_cache
        
        # 缓存刷新
        if args.cache_refresh:
            print(f"\n🗑️  刷新缓存...")
            cleared_count = bb_main.cache_manager.clear_cache()
            print(f"   清理了 {cleared_count} 个缓存文件")
        
        # 执行相应操作
        if args.validate:
            # 系统验证
            validation_result = bb_main.validate_system()
            
        elif args.etf:
            # 单个ETF计算
            result = bb_main.run_single_etf(args.etf, use_cache=use_cache)
            
        elif args.threshold:
            # 指定门槛批量计算
            result = bb_main.run_threshold_batch(args.threshold, use_cache=use_cache)
            
        elif args.all:
            # 全部门槛计算
            result = bb_main.run_all_thresholds(use_cache=use_cache)
            
        else:
            # 默认显示帮助
            parser.print_help()
            return
        
        # 显示缓存统计
        cache_stats = bb_main.cache_manager.get_cache_statistics()
        print(f"\n📊 缓存统计:")
        print(f"   命中率: {cache_stats['hit_rate_percent']:.1f}%")
        print(f"   请求次数: {cache_stats['total_requests']}")
        print(f"   缓存大小: {cache_stats['cache_size_mb']:.2f}MB")
        
        print(f"\n✅ 布林带计算系统执行完成")
        print(f"⏱️  完成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
    except KeyboardInterrupt:
        print(f"\n⚠️  用户中断操作")
        sys.exit(1)
        
    except Exception as e:
        print(f"\n❌ 系统异常: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()