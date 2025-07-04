#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ETF数据系统主启动器
统一ETF更新器的简单启动入口
"""

import sys
import argparse
from pathlib import Path

# 确保可以导入项目模块
PROJECT_ROOT = Path(__file__).parent
sys.path.insert(0, str(PROJECT_ROOT))

from unified_etf_updater import UnifiedETFUpdater


def main():
    """主函数"""
    parser = argparse.ArgumentParser(
        description='ETF数据系统统一更新器',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用示例:
  python main.py                    # 执行完整数据更新
  python main.py --mode test        # 系统状态测试
  python main.py --mode validate    # 手动数据校验
  python main.py --no-git          # 禁用Git自动提交
  python main.py --no-push         # 禁用Git推送（仅本地提交）
  python main.py --no-screening    # 禁用ETF初筛
  python main.py --no-validation   # 禁用数据校验
        """
    )
    
    parser.add_argument(
        '--mode', 
        choices=['update', 'test', 'validate'], 
        default='update',
        help='运行模式: update(数据更新), test(系统测试), validate(数据校验)'
    )
    
    parser.add_argument(
        '--no-git', 
        action='store_true',
        help='禁用Git自动提交功能'
    )
    
    parser.add_argument(
        '--no-push', 
        action='store_true',
        help='禁用Git自动推送功能（仅本地提交）'
    )
    
    parser.add_argument(
        '--no-screening', 
        action='store_true',
        help='禁用ETF自动初筛功能'
    )
    
    parser.add_argument(
        '--no-validation', 
        action='store_true',
        help='禁用周更日更数据校验功能'
    )
    
    args = parser.parse_args()
    
    try:
        # 初始化统一更新器
        updater = UnifiedETFUpdater(PROJECT_ROOT)
        
        # 根据命令行参数调整配置
        if args.no_git:
            updater.set_git_enabled(False)
        
        if args.no_push:
            updater.set_git_push_enabled(False)
        
        if args.no_screening:
            updater.set_screening_enabled(False)
        
        if args.no_validation:
            updater.set_validation_enabled(False)
        
        # 执行相应操作
        if args.mode == 'test':
            # 测试模式
            print("🔍 开始系统状态测试...")
            updater.test_system_status()
            print("✅ 系统状态测试完成")
        elif args.mode == 'validate':
            # 数据校验模式
            print("🔍 开始周更日更数据校验...")
            result = updater.run_weekly_daily_validation()
            
            if result['needs_attention']:
                print(f"⚠️ 发现数据不一致: {result['message']}")
                print("📋 建议检查三个复权类型的数据一致性！")
            else:
                print(f"✅ 数据校验通过: {result['message']}")
        else:
            # 正常更新模式
            print("🚀 开始执行ETF数据更新...")
            results = updater.run_full_update()
            
            # 输出简要结果
            success_count = sum(results.values())
            total_count = len(results)
            
            if success_count > 0:
                print(f"✅ 更新完成！成功更新 {success_count}/{total_count} 个模块")
            else:
                print("ℹ️ 更新完成，但没有新数据需要处理")
    
    except KeyboardInterrupt:
        print("\n⚠️ 用户中断操作")
        sys.exit(1)
    except Exception as e:
        print(f"❌ 执行过程中发生错误: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main() 