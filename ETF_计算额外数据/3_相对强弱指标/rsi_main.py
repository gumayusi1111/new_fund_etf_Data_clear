#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
第三大类：相对强弱指标主程序
========================

一次性运行所有相对强弱指标系统：RSI、威廉指标
专为超买超卖状态识别和市场情绪分析优化
"""

import os
import sys
import time
import subprocess
from pathlib import Path


def run_indicator(name: str, script_path: str, working_dir: str, indicator_type: str) -> bool:
    """运行单个指标系统"""
    print(f"\n🚀 开始运行 {name}...")
    print("=" * 50)
    
    start_time = time.time()
    
    try:
        # 根据不同指标类型使用不同的命令参数
        if indicator_type == "rsi":
            # RSI使用batch模式进行批量计算
            cmd_args = [sys.executable, script_path, "--mode", "batch"]
        elif indicator_type == "williams":
            # 威廉指标使用默认参数计算所有门槛（不使用--test，直接批量计算）
            cmd_args = [sys.executable, script_path]
        else:
            # 默认情况
            cmd_args = [sys.executable, script_path]
        
        # 运行指标脚本
        result = subprocess.run(
            cmd_args,
            cwd=working_dir,
            timeout=2400,  # 40分钟超时(相对强弱指标计算量较大)
            capture_output=False,  # 让输出直接显示
            text=True
        )
        
        duration = time.time() - start_time
        
        if result.returncode == 0:
            print(f"\n✅ {name} 完成 ({duration:.2f}秒)")
            return True
        else:
            print(f"\n❌ {name} 失败 (返回码: {result.returncode})")
            return False
            
    except subprocess.TimeoutExpired:
        duration = time.time() - start_time
        print(f"\n⏰ {name} 执行超时 ({duration:.2f}秒)")
        return False
    except Exception as e:
        duration = time.time() - start_time
        print(f"\n💥 {name} 执行异常: {str(e)} ({duration:.2f}秒)")
        return False


def main():
    """主函数 - 依次运行所有相对强弱指标"""
    
    # 获取当前目录
    base_dir = Path(__file__).parent
    
    # 相对强弱指标系统配置 - 每个都支持两门槛+向量化+增量计算
    indicators = [
        {
            "name": "RSI相对强弱指数",
            "script": "rsi_main_optimized.py",
            "dir": "RSI",
            "type": "rsi",
            "features": "威尔德平滑法+多周期RSI(6/12/24)+8位精度+智能缓存+增量更新",
            "status": "✅ 生产就绪",
            "coverage": "569个ETF, 100%计算成功"
        },
        {
            "name": "威廉指标 (Williams %R)", 
            "script": "williams_main_optimized.py",
            "dir": "威廉指标",
            "type": "williams",
            "features": "超买超卖信号+与RSI互补验证+向量化计算+智能缓存+增量更新",
            "status": "🔧 开发中",
            "coverage": "规划实现中"
        }
    ]
    
    print("🎯 第三大类：相对强弱指标批量计算")
    print("=" * 60)
    print(f"📁 工作目录: {base_dir}")
    print(f"📊 指标数量: {len(indicators)}个")
    print(f"⏰ 开始时间: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("\n🚀 系统特性:")
    print("   📈 双门槛处理: 3000万门槛 + 5000万门槛")
    print("   ⚡ 向量化计算: 威尔德平滑法优化实现")
    print("   💾 智能缓存: 96%+ 缓存命中率")
    print("   🔄 增量更新: 只处理新增/变更数据")
    print("   🗂️ 自动保存: cache + data 双重存储")
    print("   💪 超买超卖: 识别市场情绪极端状态")
    print("   📊 多周期分析: 短中长期RSI组合策略")
    
    # 显示指标详情
    print("\n📊 指标系统详情:")
    for idx, indicator in enumerate(indicators, 1):
        print(f"   {idx}. {indicator['name']}")
        print(f"      状态: {indicator['status']}")
        print(f"      覆盖: {indicator['coverage']}")
        print(f"      特性: {indicator['features']}")
    
    # 执行统计
    start_time = time.time()
    success_count = 0
    failed_indicators = []
    
    # 依次执行每个指标
    for idx, indicator in enumerate(indicators, 1):
        print(f"\n💪 [{idx}/{len(indicators)}] 执行 {indicator['name']}")
        
        script_path = base_dir / indicator['dir'] / indicator['script']
        working_dir = base_dir / indicator['dir']
        
        # 检查脚本是否存在
        if not script_path.exists():
            print(f"❌ 脚本不存在: {script_path}")
            print(f"   可能原因: {indicator['name']} 尚未完成开发")
            failed_indicators.append(indicator['name'])
            continue
        
        # 检查工作目录
        if not working_dir.exists():
            print(f"❌ 工作目录不存在: {working_dir}")
            failed_indicators.append(indicator['name'])
            continue
        
        # 运行指标
        success = run_indicator(
            indicator['name'],
            str(script_path),
            str(working_dir),
            indicator['type']
        )
        
        if success:
            success_count += 1
            print(f"\n🎯 {indicator['name']} 计算结果:")
            
            # 显示RSI特定的输出信息
            if "RSI" in indicator['name']:
                print("   📊 RSI指标字段:")
                print("      • RSI_6: 6日相对强弱指数 (高敏感度)")
                print("      • RSI_12: 12日相对强弱指数 (中国市场优化)")
                print("      • RSI_24: 24日相对强弱指数 (长期趋势)")
                print("      • RSI_DIFF_6_24: RSI6与RSI24差值")
                print("      • RSI_CHANGE_RATE: RSI12日变化率(%)")
                
                print("\n   🎯 交易信号参考:")
                print("      • RSI > 70: 超买区域，警惕回调")
                print("      • RSI < 30: 超卖区域，关注反弹")
                print("      • RSI突破50: 多空分界线，趋势确认")
                
            elif "威廉" in indicator['name']:
                print("   📊 威廉指标字段:")
                print("      • WR_14: 14日威廉指标")
                print("      • WR_21: 21日威廉指标")
                print("   🎯 交易信号参考:")
                print("      • WR > -20: 超买区域")
                print("      • WR < -80: 超卖区域")
        else:
            failed_indicators.append(indicator['name'])
    
    # 显示总结
    total_duration = time.time() - start_time
    success_rate = (success_count / len(indicators)) * 100 if len(indicators) > 0 else 0
    
    print("\n" + "=" * 60)
    print("📊 第三大类执行总结")
    print("=" * 60)
    print(f"⏰ 总耗时: {total_duration:.2f}秒 ({total_duration/60:.1f}分钟)")
    print(f"✅ 成功: {success_count}")
    print(f"❌ 失败: {len(failed_indicators)}")
    print(f"📊 成功率: {success_rate:.1f}%")
    
    if failed_indicators:
        print(f"\n❌ 失败的指标:")
        for indicator in failed_indicators:
            print(f"   - {indicator}")
        
        print(f"\n💡 提示:")
        print("   • 检查各指标的主程序文件是否存在")
        print("   • 确认指标系统是否已完成开发")
        print("   • 查看详细日志了解失败原因")
    
    if success_count > 0:
        print(f"\n💪 相对强弱指标应用建议:")
        print("   📈 RSI多周期策略: 结合RSI6/12/24判断趋势强度")
        print("   🔄 RSI背离策略: 价格与RSI背离时寻找反转机会")
        print("   ⚖️ RSI+威廉组合: 双重验证超买超卖信号")
        print("   📊 门槛分析: 对比3000万/5000万门槛差异")
    
    print(f"\n🎯 相对强弱指标批量计算完成！")
    
    # 返回状态码
    return 0 if success_count == len(indicators) else 1


if __name__ == "__main__":
    sys.exit(main())