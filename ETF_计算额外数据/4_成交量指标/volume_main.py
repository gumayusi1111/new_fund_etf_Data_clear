#!/usr/bin/env python3
"""
第4大类：成交量指标统一主启动器
================================

统一管理三个成交量相关指标系统:
1. OBV指标 - On-Balance Volume 能量潮指标
2. 价量配合度 - Price-Volume Coordination 价量关系分析
3. VMA - Volume Moving Average 成交量移动平均线

使用示例:
    # 测试所有指标
    python volume_main.py --mode test
    
    # 单个ETF计算所有指标
    python volume_main.py --mode single --etf 159001 --threshold 3000万门槛
    
    # 批量计算所有指标
    python volume_main.py --mode batch --threshold 3000万门槛
    
    # 全量计算所有指标
    python volume_main.py --mode all
    
    # 运行特定指标
    python volume_main.py --mode single --etf 159001 --threshold 3000万门槛 --indicators obv,pv
    
版本: 1.0.0
作者: ETF量化分析系统  
日期: 2025-07-27
"""

import argparse
import sys
import subprocess
import logging
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

class VolumeIndicatorLauncher:
    """成交量指标统一启动器"""
    
    def __init__(self):
        """初始化启动器"""
        self.base_path = Path(__file__).parent
        
        # 定义指标配置
        self.indicators = {
            'obv': {
                'name': 'OBV指标',
                'description': 'On-Balance Volume 能量潮指标',
                'path': self.base_path / 'OBV指标',
                'script': 'obv_main_optimized.py',
                'color': '🟡'
            },
            'pv': {
                'name': '价量配合度',
                'description': 'Price-Volume Coordination 价量关系分析',
                'path': self.base_path / '价量配合度',
                'script': 'pv_main_optimized.py',
                'color': '🟢'
            },
            'vma': {
                'name': 'VMA成交量移动平均线',
                'description': 'Volume Moving Average 成交量移动平均线',
                'path': self.base_path / '成交量移动平均线' / 'VMA',
                'script': 'vma_main_optimized.py',
                'color': '🔵'
            }
        }
        
        # 设置日志
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger('VolumeMain')
        
    def print_header(self):
        """打印启动横幅"""
        print("=" * 70)
        print("🚀 ETF成交量指标统一计算系统 v1.0.0")
        print("   基于格兰维尔OBV理论和现代价量分析技术")
        print("=" * 70)
        print(f"⏰ 启动时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print()
        
    def print_indicators_info(self):
        """打印指标信息"""
        print("📊 支持的成交量指标:")
        print("-" * 50)
        for key, info in self.indicators.items():
            print(f"{info['color']} {info['name']} ({key})")
            print(f"   {info['description']}")
        print()
        
    def validate_indicator_paths(self) -> bool:
        """验证所有指标路径和脚本是否存在"""
        all_valid = True
        for key, info in self.indicators.items():
            script_path = info['path'] / info['script']
            if not script_path.exists():
                self.logger.error(f"指标脚本不存在: {script_path}")
                all_valid = False
                
        return all_valid
        
    def run_indicator(self, indicator: str, mode: str, etf: Optional[str] = None, 
                     threshold: Optional[str] = None, **kwargs) -> Dict:
        """
        运行单个指标
        
        Args:
            indicator: 指标代码
            mode: 运行模式
            etf: ETF代码 (单个模式时需要)
            threshold: 门槛类型
            **kwargs: 其他参数
            
        Returns:
            运行结果字典
        """
        if indicator not in self.indicators:
            return {
                'indicator': indicator,
                'success': False,
                'error': f'未知指标: {indicator}',
                'runtime': 0
            }
            
        info = self.indicators[indicator]
        script_path = info['path'] / info['script']
        
        # 构建命令
        cmd = [sys.executable, str(script_path), '--mode', mode]
        
        if etf:
            cmd.extend(['--etf', etf])
        if threshold:
            cmd.extend(['--threshold', threshold])
            
        # 添加其他参数 - 只在相关模式下添加
        for key, value in kwargs.items():
            if value is not None:
                # 转换参数名：下划线转换为连字符
                param_name = key.replace('_', '-')
                # 特殊处理sample_size参数，只在test模式下添加
                if key == 'sample_size' and mode == 'test':
                    cmd.extend([f'--{param_name}', str(value)])
                elif key != 'sample_size':  # 其他参数正常添加
                    cmd.extend([f'--{param_name}', str(value)])
                
        start_time = time.time()
        
        try:
            self.logger.info(f"启动 {info['name']} - 模式: {mode}")
            
            result = subprocess.run(
                cmd,
                cwd=info['path'],
                capture_output=True,
                text=True,
                timeout=1800  # 30分钟超时
            )
            
            runtime = time.time() - start_time
            
            if result.returncode == 0:
                return {
                    'indicator': indicator,
                    'name': info['name'],
                    'success': True,
                    'runtime': runtime,
                    'output': result.stdout,
                    'color': info['color']
                }
            else:
                return {
                    'indicator': indicator,
                    'name': info['name'],
                    'success': False,
                    'runtime': runtime,
                    'error': result.stderr or result.stdout,
                    'color': info['color']
                }
                
        except subprocess.TimeoutExpired:
            return {
                'indicator': indicator,
                'name': info['name'],
                'success': False,
                'runtime': time.time() - start_time,
                'error': '运行超时 (30分钟)',
                'color': info['color']
            }
        except Exception as e:
            return {
                'indicator': indicator,
                'name': info['name'],
                'success': False,
                'runtime': time.time() - start_time,
                'error': f'执行异常: {str(e)}',
                'color': info['color']
            }
            
    def run_parallel(self, indicators: List[str], mode: str, etf: Optional[str] = None,
                    threshold: Optional[str] = None, max_workers: int = 3, **kwargs) -> List[Dict]:
        """
        并行运行多个指标
        
        Args:
            indicators: 指标列表
            mode: 运行模式
            etf: ETF代码
            threshold: 门槛类型
            max_workers: 最大并发数
            **kwargs: 其他参数
            
        Returns:
            运行结果列表
        """
        results = []
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # 提交任务
            future_to_indicator = {
                executor.submit(
                    self.run_indicator, indicator, mode, etf, threshold, **kwargs
                ): indicator for indicator in indicators
            }
            
            # 收集结果
            for future in as_completed(future_to_indicator):
                result = future.result()
                results.append(result)
                
                # 实时显示结果
                if result['success']:
                    print(f"✅ {result['color']} {result['name']} 完成 "
                          f"({result['runtime']:.1f}秒)")
                else:
                    print(f"❌ {result['color']} {result['name']} 失败 "
                          f"({result['runtime']:.1f}秒)")
                    if result.get('error'):
                        # 清理错误信息，只显示关键部分
                        error_msg = result['error'].strip()
                        if len(error_msg) > 200:
                            # 尝试找到真正的错误信息
                            error_lines = error_msg.split('\n')
                            for line in error_lines:
                                if any(keyword in line.lower() for keyword in ['error', 'exception', 'failed', '错误', '失败', 'not found']):
                                    error_msg = line.strip()
                                    break
                            else:
                                error_msg = error_lines[-1].strip() if error_lines else error_msg[:200]
                        print(f"   💡 {error_msg[:150]}{'...' if len(error_msg) > 150 else ''}")
                        
        return sorted(results, key=lambda x: x['indicator'])
        
    def print_summary(self, results: List[Dict], mode: str, etf: Optional[str] = None,
                     threshold: Optional[str] = None):
        """
        打印运行总结
        
        Args:
            results: 运行结果列表
            mode: 运行模式
            etf: ETF代码
            threshold: 门槛类型
        """
        print()
        print("=" * 70)
        print("📈 运行总结")
        print("=" * 70)
        
        success_count = sum(1 for r in results if r['success'])
        total_count = len(results)
        total_runtime = sum(r['runtime'] for r in results)
        
        print(f"📊 运行模式: {mode}")
        if etf:
            print(f"📋 ETF代码: {etf}")
        if threshold:
            print(f"💰 门槛类型: {threshold}")
        print(f"⏱️  总用时: {total_runtime:.1f}秒")
        print(f"✅ 成功率: {success_count}/{total_count} ({success_count/total_count*100:.1f}%)")
        print()
        
        print("📝 详细结果:")
        print("-" * 50)
        for result in results:
            if result['success']:
                print(f"✅ {result['color']} {result['name']}: 成功 ({result['runtime']:.1f}秒)")
            else:
                print(f"❌ {result['color']} {result['name']}: 失败 ({result['runtime']:.1f}秒)")
                if result.get('error'):
                    # 清理并简化错误信息
                    error_msg = result['error'].strip()
                    if len(error_msg) > 200:
                        error_lines = error_msg.split('\n')
                        for line in error_lines:
                            if any(keyword in line.lower() for keyword in ['error', 'exception', 'failed', '错误', '失败', 'not found']):
                                error_msg = line.strip()
                                break
                        else:
                            error_msg = error_lines[-1].strip() if error_lines else error_msg[:200]
                    print(f"   💡 错误信息: {error_msg[:150]}{'...' if len(error_msg) > 150 else ''}")
                    
        print()
        if success_count == total_count:
            print("🎉 所有指标计算完成！")
        else:
            print(f"⚠️  {total_count - success_count} 个指标运行失败，请检查日志")
            
    def run(self, mode: str, indicators: Optional[List[str]] = None, 
            etf: Optional[str] = None, threshold: Optional[str] = None,
            parallel: bool = True, **kwargs) -> bool:
        """
        运行成交量指标计算
        
        Args:
            mode: 运行模式
            indicators: 指标列表，None表示运行所有指标
            etf: ETF代码
            threshold: 门槛类型  
            parallel: 是否并行运行
            **kwargs: 其他参数
            
        Returns:
            是否全部成功
        """
        self.print_header()
        
        # 验证路径
        if not self.validate_indicator_paths():
            print("❌ 指标脚本验证失败，请检查文件路径")
            return False
            
        # 确定要运行的指标
        if indicators is None:
            indicators = list(self.indicators.keys())
        else:
            # 验证指标代码
            invalid_indicators = [ind for ind in indicators if ind not in self.indicators]
            if invalid_indicators:
                print(f"❌ 无效的指标代码: {invalid_indicators}")
                print(f"💡 支持的指标: {list(self.indicators.keys())}")
                return False
                
        self.print_indicators_info()
        
        print(f"🚦 开始运行 {len(indicators)} 个指标 (并行: {'是' if parallel else '否'})")
        print(f"📝 指标列表: {', '.join(indicators)}")
        print()
        
        start_time = time.time()
        
        if parallel and len(indicators) > 1:
            results = self.run_parallel(indicators, mode, etf, threshold, **kwargs)
        else:
            results = []
            for indicator in indicators:
                result = self.run_indicator(indicator, mode, etf, threshold, **kwargs)
                results.append(result)
                
                if result['success']:
                    print(f"✅ {result['color']} {result['name']} 完成 "
                          f"({result['runtime']:.1f}秒)")
                else:
                    print(f"❌ {result['color']} {result['name']} 失败 "
                          f"({result['runtime']:.1f}秒)")
                          
        total_runtime = time.time() - start_time
        
        # 更新总运行时间
        for result in results:
            if 'runtime' not in result:
                result['runtime'] = 0
                
        self.print_summary(results, mode, etf, threshold)
        
        success_count = sum(1 for r in results if r['success'])
        return success_count == len(results)


def main():
    """主函数"""
    parser = argparse.ArgumentParser(
        description='ETF成交量指标统一计算系统',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用示例:
  测试模式:
    python volume_main.py --mode test
    
  单个ETF:
    python volume_main.py --mode single --etf 159001 --threshold 3000万门槛
    
  批量处理:
    python volume_main.py --mode batch --threshold 3000万门槛
    
  指定指标:
    python volume_main.py --mode test --indicators obv,pv
        """
    )
    
    parser.add_argument('--mode', required=True,
                       choices=['test', 'single', 'batch', 'all'],
                       help='运行模式')
    
    parser.add_argument('--indicators', type=str,
                       help='指标列表，逗号分隔 (可选: obv,pv,vma)')
    
    parser.add_argument('--etf', type=str,
                       help='ETF代码 (单个模式时必需)')
    
    parser.add_argument('--threshold', type=str,
                       choices=['3000万门槛', '5000万门槛'],
                       help='门槛类型')
    
    parser.add_argument('--parallel', action='store_true', default=True,
                       help='并行运行 (默认开启)')
    
    parser.add_argument('--sequential', action='store_true',
                       help='顺序运行 (关闭并行)')
    
    parser.add_argument('--sample-size', type=int, default=5,
                       help='测试模式样本数量')
    
    parser.add_argument('--max-workers', type=int, default=3,
                       help='最大并发数')
    
    parser.add_argument('--verbose', action='store_true',
                       help='详细输出')
    
    args = parser.parse_args()
    
    # 处理参数
    indicators = None
    if args.indicators:
        indicators = [ind.strip() for ind in args.indicators.split(',')]
        
    parallel = args.parallel and not args.sequential
    
    # 验证参数
    if args.mode == 'single' and not args.etf:
        parser.error("单个模式需要指定 --etf 参数")
        
    if args.mode in ['single', 'batch'] and not args.threshold:
        parser.error(f"{args.mode} 模式需要指定 --threshold 参数")
        
    # 设置日志级别
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
        
    # 运行启动器
    launcher = VolumeIndicatorLauncher()
    
    try:
        success = launcher.run(
            mode=args.mode,
            indicators=indicators,
            etf=args.etf,
            threshold=args.threshold,
            parallel=parallel,
            sample_size=args.sample_size,
            max_workers=args.max_workers
        )
        
        sys.exit(0 if success else 1)
        
    except KeyboardInterrupt:
        print("\n⚠️  用户中断执行")
        sys.exit(130)
    except Exception as e:
        print(f"\n❌ 执行异常: {str(e)}")
        logging.exception("执行异常")
        sys.exit(1)


if __name__ == '__main__':
    main()