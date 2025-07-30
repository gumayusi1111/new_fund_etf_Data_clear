#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
📊 动量振荡器主启动器 (Momentum Oscillators Main Launcher)
======================================================

基于"动量先于价格"理论的技术指标计算系统
涵盖13个核心动量指标，为ETF深度筛选提供客观数据支持

动量指标包括:
- 基础动量: momentum_10, momentum_20 (价格动量)
- 变动率: roc_5, roc_12, roc_25 (变化率百分比)
- PMO指标: pmo, pmo_signal (价格动量振荡器)
- 威廉指标: williams_r (威廉%R)
- 复合指标: momentum_strength, momentum_acceleration, 
           momentum_trend, momentum_divergence, momentum_volatility

版本: 2.0.0 - 模块化重构版
作者: Claude Code Assistant
创建时间: 2025-07-30
"""

import sys
import os
import argparse
import logging
import time
from pathlib import Path
from typing import List, Optional, Dict, Any

# 添加模块路径
current_dir = Path(__file__).parent
sys.path.insert(0, str(current_dir))

try:
    from mom_calculator import (
        MomentumController, 
        MomentumDisplayFormatter,
        MomentumConfig,
        momentum_config
    )
except ImportError as e:
    print(f"❌ 导入模块失败: {e}")
    print("📝 请检查 mom_calculator 模块是否完整")
    sys.exit(1)

# 配置日志
def setup_logging(level: str = "INFO") -> None:
    """设置日志配置"""
    log_config = MomentumConfig.LOGGING_CONFIG
    log_file = MomentumConfig.LOGS_DIR / log_config['file_name']
    
    # 确保日志目录存在
    MomentumConfig.LOGS_DIR.mkdir(parents=True, exist_ok=True)
    
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format=log_config['format'],
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8', mode='w'),  # 每次运行清空日志文件
            logging.StreamHandler() if log_config['console_output'] else logging.NullHandler()
        ]
    )

def load_pre_screened_etfs(threshold: str) -> List[str]:
    """
    加载初筛后的ETF列表
    
    Args:
        threshold: 门槛类型 ("3000万门槛" 或 "5000万门槛")
        
    Returns:
        ETF代码列表
    """
    etf_list_file = MomentumConfig.get_filter_file_path(threshold)
    
    if not etf_list_file.exists():
        print(f"⚠️ 初筛列表文件不存在: {etf_list_file}")
        return []
    
    try:
        with open(etf_list_file, 'r', encoding='utf-8') as f:
            etf_codes = [line.strip() for line in f if line.strip()]
        
        print(f"📋 已加载 {threshold} 初筛ETF列表: {len(etf_codes)}个")
        return etf_codes
        
    except Exception as e:
        print(f"❌ 读取初筛列表失败: {e}")
        return []


def process_batch_etfs(controller: MomentumController,
                      source_path: str,
                      threshold: Optional[str] = None,
                      max_etfs: Optional[int] = None) -> Dict[str, Any]:
    """
    批量处理ETF
    
    Args:
        controller: 动量控制器
        source_path: 源数据路径
        threshold: 指定门槛类型，None则处理两个门槛
        max_etfs: 最大处理数量限制
        
    Returns:
        批量处理结果统计
    """
    print(f"\n🚀 开始批量处理ETF动量指标")
    print(f"📂 源数据路径: {source_path}")
    
    if threshold:
        print(f"🎯 指定门槛: {threshold}")
        # 使用初筛列表
        etf_codes = load_pre_screened_etfs(threshold)
        if not etf_codes:
            return {'success': False, 'error': '无法加载初筛ETF列表'}
        
        if max_etfs:
            etf_codes = etf_codes[:max_etfs]
            print(f"⚠️ 限制处理数量: {max_etfs}个ETF")
        
        stats = controller.batch_process_etfs(source_path, etf_codes=etf_codes, use_filtered_list=False, threshold_override=threshold)
    else:
        print(f"🎯 处理模式: 双门槛模式")
        # 处理两个门槛
        all_stats = {}
        
        for thresh in ["3000万门槛", "5000万门槛"]:
            print(f"\n--- 处理 {thresh} ---")
            etf_codes = load_pre_screened_etfs(thresh)
            if not etf_codes:
                continue
                
            if max_etfs:
                etf_codes = etf_codes[:max_etfs]
                
            thresh_stats = controller.batch_process_etfs(source_path, etf_codes=etf_codes, use_filtered_list=False, threshold_override=thresh)
            all_stats[thresh] = thresh_stats
        
        stats = all_stats
    
    return stats

def cleanup_cache(controller: MomentumController, force: bool = False) -> None:
    """清理缓存"""
    print(f"\n🧹 开始清理缓存...")
    
    cleanup_stats = controller.cache_manager.cleanup_expired_cache(force=force)
    
    print(f"✅ 缓存清理完成:")
    print(f"   🗑️ 删除文件: {cleanup_stats.get('files_removed', 0)}个")
    print(f"   💾 释放空间: {cleanup_stats.get('space_freed_mb', 0):.1f}MB")

def show_system_status(controller: MomentumController, 
                      formatter: MomentumDisplayFormatter) -> None:
    """显示系统状态"""
    print(f"\n🎯 系统详细状态:")
    
    # 系统信息
    print(formatter.format_system_info())
    
    # 获取状态
    status = controller.get_system_status()
    
    # 缓存状态
    cache_stats = status.get('cache_status', {})
    if cache_stats:
        print(f"\n💾 缓存系统状态:")
        print(f"   📁 缓存目录: {cache_stats.get('cache_dir', 'N/A')}")
        print(f"   📊 缓存总大小: {cache_stats.get('total_size_mb', 0):.1f}MB")
        print(f"   📄 缓存文件数: {cache_stats.get('file_count', 0)}个")
        
        if 'performance' in cache_stats:
            perf = cache_stats['performance']
            print(f"   🎯 命中率: {perf.get('hit_rate', 0):.1f}%")
            print(f"   📈 请求总数: {perf.get('total_requests', 0)}次")
            print(f"   ✅ 命中次数: {perf.get('hits', 0)}次")
            print(f"   ❌ 未命中次数: {perf.get('misses', 0)}次")
    
    # 数据统计
    data_stats = status.get('data_status', {})
    if data_stats:
        print(f"\n📊 数据处理统计:")
        for threshold, stats in data_stats.items():
            if isinstance(stats, dict):
                print(f"   {threshold}:")
                print(f"     ✅ 成功: {stats.get('success', 0)}个")
                print(f"     ❌ 失败: {stats.get('failed', 0)}个")
                print(f"     ⏭️ 跳过: {stats.get('skipped', 0)}个")

def create_parser() -> argparse.ArgumentParser:
    """创建命令行参数解析器"""
    parser = argparse.ArgumentParser(
        description='动量振荡器指标计算系统',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例用法:
  python momentum_main.py                          # 双门槛批量处理
  python momentum_main.py -t 5000万门槛            # 处理5000万门槛
  python momentum_main.py -s 159915               # 处理单个ETF
  python momentum_main.py --status                # 显示系统状态
  python momentum_main.py --cleanup               # 清理过期缓存
  python momentum_main.py -t 3000万门槛 -n 10     # 限制处理10个ETF
        """
    )
    
    # 处理模式
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument(
        '-s', '--single',
        metavar='ETF_CODE',
        help='处理单个ETF'
    )
    mode_group.add_argument(
        '-b', '--batch',
        action='store_true',
        default=True,
        help='批量处理ETF (默认模式)'
    )
    
    # 门槛选择
    parser.add_argument(
        '-t', '--threshold',
        choices=['3000万门槛', '5000万门槛'],
        help='指定门槛类型，不指定则处理两个门槛'
    )
    
    # 数量限制
    parser.add_argument(
        '-n', '--max-etfs',
        type=int,
        metavar='N',
        help='最大处理ETF数量'
    )
    
    # 源数据路径
    parser.add_argument(
        '--source-path',
        default="/Users/wenbai/Desktop/金融/data_clear/ETF日更/0_ETF日K(前复权)",
        help='ETF源数据路径'
    )
    
    # 系统操作
    parser.add_argument(
        '--status',
        action='store_true',
        help='显示系统状态'
    )
    
    parser.add_argument(
        '--cleanup',
        action='store_true',
        help='清理过期缓存'
    )
    
    parser.add_argument(
        '--force-cleanup',
        action='store_true',
        help='强制清理所有缓存'
    )
    
    # 日志级别
    parser.add_argument(
        '--log-level',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        default='INFO',
        help='日志级别'
    )
    
    # 版本信息
    parser.add_argument(
        '--version',
        action='version',
        version=f'动量振荡器系统 v{momentum_config.get_system_info()["version"]}'
    )
    
    return parser

def main():
    """主程序入口"""
    # 解析命令行参数
    parser = create_parser()
    args = parser.parse_args()
    
    # 设置日志
    setup_logging(args.log_level)
    
    # 初始化组件
    try:
        print("🚀 初始化动量振荡器系统...")
        controller = MomentumController()
        formatter = MomentumDisplayFormatter()
        print("✅ 系统初始化完成")
        
    except Exception as e:
        print(f"❌ 系统初始化失败: {e}")
        logging.exception("系统初始化异常")
        return 1
    
    # 检查源数据路径
    if not args.status and not args.cleanup and not args.force_cleanup:
        if not os.path.exists(args.source_path):
            print(f"❌ 源数据路径不存在: {args.source_path}")
            return 1
    
    try:
        # 系统状态
        if args.status:
            show_system_status(controller, formatter)
            return 0
        
        # 缓存清理
        if args.cleanup or args.force_cleanup:
            cleanup_cache(controller, force=args.force_cleanup)
            return 0
        
        # 单个ETF处理
        if args.single:
            print(f"\n🔄 开始处理单个ETF: {args.single}")
            success = controller.process_single_etf(args.single, threshold_override=args.threshold)
            if success:
                print(f"✅ {args.single}: 处理成功")
                return 0
            else:
                print(f"❌ {args.single}: 处理失败")
                return 1
        
        # 批量处理
        else:
            stats = process_batch_etfs(
                controller, 
                args.source_path,
                threshold=args.threshold,
                max_etfs=args.max_etfs
            )
            
            print("\n" + "="*60)
            print("📊 处理结果统计:")
            
            if args.threshold:
                # 单门槛统计
                print(formatter.format_statistics(stats))
            else:
                # 双门槛统计
                for threshold, thresh_stats in stats.items():
                    print(f"\n--- {threshold} ---")
                    print(formatter.format_statistics(thresh_stats))
            
            # 最终状态
            print("\n🎯 最终系统状态:")
            final_status = controller.get_system_status()
            cache_stats = final_status.get('cache_status', {})
            if 'performance' in cache_stats:
                perf = cache_stats['performance']
                print(f"   💾 缓存命中率: {perf.get('hit_rate', 0):.1f}%")
                print(f"   📊 缓存请求: {perf.get('total_requests', 0)}次")
            
            print("\n✅ 批量处理完成!")
            return 0
            
    except KeyboardInterrupt:
        print("\n⚠️ 用户中断程序")
        return 130
    except Exception as e:
        print(f"\n❌ 程序执行异常: {e}")
        logging.exception("程序执行异常")
        return 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)