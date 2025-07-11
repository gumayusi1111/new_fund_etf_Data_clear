#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
EMA文件管理器 - 重构版
==================

参照WMA/SMA系统的文件管理架构
统一文件操作接口，支持结果保存和目录管理
"""

import os
from typing import Dict, Optional
from .config import EMAConfig


class EMAFileManager:
    """EMA文件管理器 - 重构版（与WMA/SMA保持一致）"""
    
    def __init__(self, config: EMAConfig):
        """
        初始化文件管理器
        
        Args:
            config: EMA配置对象
        """
        self.config = config
        
        if not config.performance_mode:
            print("📁 EMA文件管理器初始化完成")
            print(f"   📂 输出目录: {config.default_output_dir}")
    
    def ensure_output_directory(self, threshold: str) -> str:
        """
        确保输出目录存在
        
        Args:
            threshold: 门槛类型
            
        Returns:
            str: 输出目录路径
        """
        output_dir = os.path.join(self.config.default_output_dir, threshold)
        os.makedirs(output_dir, exist_ok=True)
        return output_dir
    
    def save_etf_result(self, etf_code: str, csv_content: str, threshold: str) -> bool:
        """
        保存单个ETF的结果到文件
        
        Args:
            etf_code: ETF代码
            csv_content: CSV内容
            threshold: 门槛类型
            
        Returns:
            bool: 是否保存成功
        """
        try:
            output_dir = self.ensure_output_directory(threshold)
            
            # 清理ETF代码，移除交易所后缀
            clean_etf_code = etf_code.replace('.SH', '').replace('.SZ', '')
            filename = f"{clean_etf_code}.csv"
            file_path = os.path.join(output_dir, filename)
            
            # 保存文件
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(csv_content)
            
            file_size = os.path.getsize(file_path)
            
            if not self.config.performance_mode:
                print(f"💾 {etf_code}: 结果已保存 ({file_size} 字节)")
            
            return True
            
        except Exception as e:
            if not self.config.performance_mode:
                print(f"❌ 保存{etf_code}结果失败: {str(e)}")
            return False
    
    def save_historical_result(self, etf_code: str, csv_content: str, 
                             threshold: str, output_dir: Optional[str] = None) -> Optional[str]:
        """
        保存ETF的历史数据结果
        
        Args:
            etf_code: ETF代码
            csv_content: CSV内容
            threshold: 门槛类型
            output_dir: 自定义输出目录
            
        Returns:
            Optional[str]: 保存的文件路径，失败返回None
        """
        try:
            if output_dir:
                base_dir = output_dir
            else:
                base_dir = self.config.default_output_dir
            
            threshold_dir = os.path.join(base_dir, threshold)
            os.makedirs(threshold_dir, exist_ok=True)
            
            # 清理ETF代码，移除交易所后缀
            clean_etf_code = etf_code.replace('.SH', '').replace('.SZ', '')
            filename = f"{clean_etf_code}.csv"
            file_path = os.path.join(threshold_dir, filename)
            
            # 保存文件
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(csv_content)
            
            file_size = os.path.getsize(file_path)
            
            if not self.config.performance_mode:
                print(f"💾 {etf_code}: 历史数据已保存 ({file_size} 字节)")
            
            return file_path
            
        except Exception as e:
            if not self.config.performance_mode:
                print(f"❌ 保存{etf_code}历史数据失败: {str(e)}")
            return None
    
    def get_directory_stats(self, threshold: str) -> Dict:
        """
        获取指定门槛目录的统计信息
        
        Args:
            threshold: 门槛类型
            
        Returns:
            Dict: 目录统计信息
        """
        try:
            output_dir = os.path.join(self.config.default_output_dir, threshold)
            
            if not os.path.exists(output_dir):
                return {
                    'exists': False,
                    'file_count': 0,
                    'total_size_mb': 0.0
                }
            
            # 统计CSV文件
            csv_files = [f for f in os.listdir(output_dir) if f.endswith('.csv')]
            total_size = 0
            
            for csv_file in csv_files:
                file_path = os.path.join(output_dir, csv_file)
                total_size += os.path.getsize(file_path)
            
            return {
                'exists': True,
                'file_count': len(csv_files),
                'total_size_mb': round(total_size / 1024 / 1024, 2),
                'directory': output_dir
            }
            
        except Exception as e:
            print(f"❌ 获取目录统计失败: {str(e)}")
            return {
                'exists': False,
                'file_count': 0,
                'total_size_mb': 0.0,
                'error': str(e)
            }
    
    def clean_directory(self, threshold: str) -> bool:
        """
        清理指定门槛目录（谨慎使用）
        
        Args:
            threshold: 门槛类型
            
        Returns:
            bool: 是否清理成功
        """
        try:
            output_dir = os.path.join(self.config.default_output_dir, threshold)
            
            if not os.path.exists(output_dir):
                return True
            
            # 删除目录中的所有CSV文件
            csv_files = [f for f in os.listdir(output_dir) if f.endswith('.csv')]
            
            for csv_file in csv_files:
                file_path = os.path.join(output_dir, csv_file)
                os.remove(file_path)
            
            if not self.config.performance_mode:
                print(f"🗑️ {threshold}: 清理了 {len(csv_files)} 个文件")
            
            return True
            
        except Exception as e:
            if not self.config.performance_mode:
                print(f"❌ 清理目录失败: {str(e)}")
            return False