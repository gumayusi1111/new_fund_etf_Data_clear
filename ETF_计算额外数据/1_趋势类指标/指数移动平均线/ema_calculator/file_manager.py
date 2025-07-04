#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
EMA文件管理器 - 中短期专版
========================

负责管理EMA计算结果的文件输出
包括目录创建、文件写入、路径管理等功能
"""

import os
import pandas as pd
from typing import Dict, List, Optional
from .config import EMAConfig


class FileManager:
    """EMA文件管理器 - 中短期专版"""
    
    def __init__(self, config: EMAConfig):
        """
        初始化文件管理器
        
        Args:
            config: EMA配置对象
        """
        self.config = config
        self.output_summary = []  # 输出文件摘要
        print("📁 EMA文件管理器初始化完成")
    
    def create_output_directory(self, threshold: str) -> str:
        """
        创建输出目录 - 模仿SMA结构
        
        Args:
            threshold: 门槛类型 ("3000万门槛" 或 "5000万门槛")
            
        Returns:
            str: 输出目录路径
        """
        try:
            # 构建输出目录路径
            output_dir = os.path.join(
                self.config.default_output_dir,
                threshold
            )
            
            # 创建目录
            os.makedirs(output_dir, exist_ok=True)
            
            print(f"📂 创建输出目录: {output_dir}")
            return output_dir
            
        except Exception as e:
            print(f"❌ 创建输出目录失败: {str(e)}")
            return ""
    
    def save_etf_result(self, etf_code: str, csv_content: str, threshold: str) -> bool:
        """
        保存单个ETF的计算结果
        
        Args:
            etf_code: ETF代码
            csv_content: CSV格式的内容
            threshold: 门槛类型
            
        Returns:
            bool: 是否保存成功
        """
        try:
            # 创建输出目录
            output_dir = self.create_output_directory(threshold)
            if not output_dir:
                return False
            
            # 构建文件路径
            filename = f"{etf_code}.csv"
            file_path = os.path.join(output_dir, filename)
            
            # 写入文件
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(csv_content)
            
            # 记录到摘要
            self.output_summary.append({
                'etf_code': etf_code,
                'file_path': file_path,
                'threshold': threshold,
                'success': True
            })
            
            print(f"✅ 保存成功: {file_path}")
            return True
            
        except Exception as e:
            print(f"❌ 保存 {etf_code} 失败: {str(e)}")
            # 记录失败
            self.output_summary.append({
                'etf_code': etf_code,
                'error': str(e),
                'threshold': threshold,
                'success': False
            })
            return False
    
    def batch_save_results(self, results: List[Dict], threshold: str) -> Dict:
        """
        批量保存结果 - 高性能版
        
        Args:
            results: 结果列表
            threshold: 门槛类型
            
        Returns:
            Dict: 批量保存统计
        """
        try:
            if not results:
                return {'total': 0, 'success': 0, 'failed': 0}
            
            print(f"💾 开始高速批量保存 {len(results)} 个ETF结果到 {threshold}...")
            
            # 创建输出目录
            output_dir = self.create_output_directory(threshold)
            if not output_dir:
                return {'total': len(results), 'success': 0, 'failed': len(results)}
            
            success_count = 0
            failed_count = 0
            
            # 🚀 批量写入优化：预处理所有文件路径和内容
            batch_operations = []
            for result in results:
                try:
                    etf_code = result.get('etf_code', '')
                    csv_content = result.get('csv_content', '')
                    
                    if not etf_code or not csv_content:
                        failed_count += 1
                        continue
                    
                    file_path = os.path.join(output_dir, f"{etf_code}.csv")
                    batch_operations.append((etf_code, file_path, csv_content))
                    
                except Exception as e:
                    failed_count += 1
            
            # 🚀 批量执行文件写入
            for etf_code, file_path, csv_content in batch_operations:
                try:
                    with open(file_path, 'w', encoding='utf-8') as f:
                        f.write(csv_content)
                    
                    # 记录到摘要
                    self.output_summary.append({
                        'etf_code': etf_code,
                        'file_path': file_path,
                        'threshold': threshold,
                        'success': True
                    })
                    success_count += 1
                    
                except Exception as e:
                    self.output_summary.append({
                        'etf_code': etf_code,
                        'error': str(e),
                        'threshold': threshold,
                        'success': False
                    })
                    failed_count += 1
            
            print(f"⚡ 高速批量保存完成: {success_count} 成功, {failed_count} 失败")
            
            return {
                'total': len(results),
                'success': success_count,
                'failed': failed_count,
                'output_dir': output_dir
            }
            
        except Exception as e:
            print(f"❌ 批量保存失败: {str(e)}")
            return {'total': len(results), 'success': 0, 'failed': len(results)}
    
    def get_output_file_path(self, etf_code: str, threshold: str) -> str:
        """
        获取输出文件路径
        
        Args:
            etf_code: ETF代码
            threshold: 门槛类型
            
        Returns:
            str: 文件路径
        """
        try:
            output_dir = os.path.join(
                self.config.default_output_dir,
                threshold
            )
            filename = f"{etf_code}.csv"
            return os.path.join(output_dir, filename)
            
        except Exception as e:
            print(f"⚠️  获取输出路径失败: {str(e)}")
            return ""
    
    def check_existing_files(self, threshold: str) -> List[str]:
        """
        检查已存在的输出文件
        
        Args:
            threshold: 门槛类型
            
        Returns:
            List[str]: 已存在的ETF代码列表
        """
        try:
            output_dir = os.path.join(
                self.config.default_output_dir,
                threshold
            )
            
            if not os.path.exists(output_dir):
                return []
            
            csv_files = [f for f in os.listdir(output_dir) if f.endswith('.csv')]
            etf_codes = [f.replace('.csv', '') for f in csv_files]
            
            print(f"📋 发现 {len(etf_codes)} 个已存在的EMA文件")
            return etf_codes
            
        except Exception as e:
            print(f"⚠️  检查已存在文件失败: {str(e)}")
            return []
    
    def clean_output_directory(self, threshold: str, confirm: bool = False) -> bool:
        """
        清理输出目录
        
        Args:
            threshold: 门槛类型
            confirm: 是否确认清理
            
        Returns:
            bool: 是否清理成功
        """
        try:
            if not confirm:
                print("⚠️  清理操作需要确认参数 confirm=True")
                return False
            
            output_dir = os.path.join(
                self.config.default_output_dir,
                threshold
            )
            
            if not os.path.exists(output_dir):
                print(f"📂 目录不存在，无需清理: {output_dir}")
                return True
            
            # 删除所有CSV文件
            csv_files = [f for f in os.listdir(output_dir) if f.endswith('.csv')]
            
            for filename in csv_files:
                file_path = os.path.join(output_dir, filename)
                os.remove(file_path)
            
            print(f"🧹 清理完成，删除了 {len(csv_files)} 个文件")
            return True
            
        except Exception as e:
            print(f"❌ 清理目录失败: {str(e)}")
            return False
    
    def show_output_summary(self) -> None:
        """
        显示输出摘要
        """
        try:
            if not self.output_summary:
                print("📋 暂无输出文件记录")
                return
            
            total_files = len(self.output_summary)
            success_files = sum(1 for item in self.output_summary if item.get('success', False))
            failed_files = total_files - success_files
            
            print(f"""
📊 EMA文件输出摘要:
   📁 总文件数: {total_files}
   ✅ 成功: {success_files}
   ❌ 失败: {failed_files}
""")
            
            # 按门槛分组显示
            threshold_groups = {}
            for item in self.output_summary:
                threshold = item.get('threshold', '未知')
                if threshold not in threshold_groups:
                    threshold_groups[threshold] = {'success': 0, 'failed': 0}
                
                if item.get('success', False):
                    threshold_groups[threshold]['success'] += 1
                else:
                    threshold_groups[threshold]['failed'] += 1
            
            for threshold, stats in threshold_groups.items():
                print(f"   📂 {threshold}: {stats['success']} 成功, {stats['failed']} 失败")
            
            # 显示失败的文件
            failed_items = [item for item in self.output_summary if not item.get('success', False)]
            if failed_items:
                print("\n❌ 失败文件详情:")
                for item in failed_items[:5]:  # 只显示前5个
                    etf_code = item.get('etf_code', '')
                    error = item.get('error', '未知错误')
                    print(f"   {etf_code}: {error}")
                
                if len(failed_items) > 5:
                    print(f"   ... 还有 {len(failed_items) - 5} 个失败文件")
            
        except Exception as e:
            print(f"⚠️  显示输出摘要失败: {str(e)}")
    
    def get_directory_stats(self, threshold: str) -> Dict:
        """
        获取目录统计信息
        
        Args:
            threshold: 门槛类型
            
        Returns:
            Dict: 目录统计
        """
        try:
            output_dir = os.path.join(
                self.config.default_output_dir,
                threshold
            )
            
            if not os.path.exists(output_dir):
                return {
                    'exists': False,
                    'file_count': 0,
                    'total_size': 0
                }
            
            csv_files = [f for f in os.listdir(output_dir) if f.endswith('.csv')]
            
            total_size = 0
            for filename in csv_files:
                file_path = os.path.join(output_dir, filename)
                total_size += os.path.getsize(file_path)
            
            return {
                'exists': True,
                'file_count': len(csv_files),
                'total_size': total_size,
                'total_size_mb': round(total_size / (1024 * 1024), 2),
                'directory': output_dir
            }
            
        except Exception as e:
            print(f"⚠️  获取目录统计失败: {str(e)}")
            return {'exists': False, 'error': str(e)}
    
    def create_directory_readme(self, threshold: str) -> bool:
        """
        创建目录说明文件
        
        Args:
            threshold: 门槛类型
            
        Returns:
            bool: 是否创建成功
        """
        try:
            output_dir = os.path.join(
                self.config.default_output_dir,
                threshold
            )
            
            if not os.path.exists(output_dir):
                return False
            
            readme_path = os.path.join(output_dir, "README.md")
            
            readme_content = f"""# EMA计算结果 - {threshold}

## 概述
此目录包含{threshold}的EMA指标计算结果。

## 配置信息
- **复权类型**: {self.config.adj_type}
- **EMA周期**: {self.config.ema_periods}
- **数据要求**: {self.config.required_rows}行

## 文件格式
每个ETF一个CSV文件，格式如下：
```
ETF代码,复权类型,最新日期,最新价格,涨跌幅(%),EMA12,EMA26,EMA差值(12-26),EMA差值(%),EMA排列
```

## 生成时间
{pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}

## 注意事项
- 所有EMA值保留6位小数
- 差值指标为EMA12-EMA26
- 排列分析基于价格与EMA的关系
"""
            
            with open(readme_path, 'w', encoding='utf-8') as f:
                f.write(readme_content)
            
            print(f"📝 创建说明文件: {readme_path}")
            return True
            
        except Exception as e:
            print(f"⚠️  创建说明文件失败: {str(e)}")
            return False 