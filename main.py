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
        print("🔧 正在初始化ETF数据系统...")
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
                sys.exit(2)  # 使用不同的退出码表示需要用户关注
            else:
                print(f"✅ 数据校验通过: {result['message']}")
        else:
            # 正常更新模式
            print("🚀 开始执行ETF数据更新...")
            results = updater.run_full_update()
            
            # 改进的结果统计和报告
            print("\n" + "="*60)
            print("📊 更新结果详细报告")
            print("="*60)
            
            if not results:
                print("❌ 未获取到更新结果")
                sys.exit(1)
            
            success_count = 0
            failed_modules = []
            
            # 模块状态映射
            module_names = {
                'daily': '日更',
                'weekly': '周更',
                'market_status': '市场状况',
                'etf_screening': 'ETF初筛'
            }
            
            for module, success in results.items():
                module_name = module_names.get(module, module)
                if success:
                    print(f"✅ {module_name}: 成功更新")
                    success_count += 1
                else:
                    print(f"❌ {module_name}: 跳过/失败")
                    failed_modules.append(module_name)
            
            total_count = len(results)
            success_rate = (success_count / total_count) * 100 if total_count > 0 else 0
            
            print(f"\n📈 总体统计:")
            print(f"   成功模块: {success_count}/{total_count}")
            print(f"   成功率: {success_rate:.1f}%")
            
            if failed_modules:
                print(f"   失败/跳过模块: {', '.join(failed_modules)}")
            
            # 根据结果给出不同的退出状态和建议
            if success_count == 0:
                print("\n⚠️ 没有模块成功更新，可能的原因:")
                print("   • 今日无新数据")
                print("   • 网络连接问题")
                print("   • 脚本配置问题")
                print("💡 建议运行 'python main.py --mode test' 检查系统状态")
            elif success_count < total_count:
                print(f"\n⚠️ 部分模块未成功更新")
                print("💡 这通常是正常的（例如：今日无新数据时日更会跳过）")
            else:
                print(f"\n🎉 所有模块都成功更新！")
    
    except KeyboardInterrupt:
        print("\n⚠️ 用户中断操作")
        sys.exit(130)  # 标准的键盘中断退出码
    except ImportError as e:
        print(f"❌ 模块导入错误: {e}")
        print("💡 请检查项目依赖是否正确安装")
        sys.exit(1)
    except FileNotFoundError as e:
        print(f"❌ 文件未找到: {e}")
        print("💡 请检查配置文件和脚本是否存在")
        sys.exit(1)
    except PermissionError as e:
        print(f"❌ 权限错误: {e}")
        print("💡 请检查文件和目录的读写权限")
        sys.exit(1)
    except Exception as e:
        print(f"❌ 执行过程中发生未预期的错误: {e}")
        print(f"🔍 错误类型: {type(e).__name__}")
        print("💡 请查看详细日志信息，或运行 'python main.py --mode test' 检查系统状态")
        # 在调试模式下显示完整的堆栈跟踪
        import os
        if os.getenv('DEBUG', '').lower() in ('1', 'true', 'yes'):
            import traceback
            traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main() 