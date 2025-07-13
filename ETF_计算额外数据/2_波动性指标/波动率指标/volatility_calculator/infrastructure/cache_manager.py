#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
波动率缓存管理器
=============

基于第一大类标准的波动率缓存管理系统
"""

import os
import json
import pandas as pd
import numpy as np
from datetime import datetime
from typing import Dict, List, Optional, Any, Tuple
from .config import VolatilityConfig


class VolatilityCacheManager:
    """波动率缓存管理器"""
    
    def __init__(self, config: VolatilityConfig):
        """
        初始化缓存管理器
        
        Args:
            config: 波动率配置对象
        """
        self.config = config
        self.cache_base_dir = os.path.join(config.volatility_script_dir, "cache")
        self.meta_dir = os.path.join(self.cache_base_dir, "meta")
        
        # 确保缓存目录存在
        os.makedirs(self.cache_base_dir, exist_ok=True)
        os.makedirs(self.meta_dir, exist_ok=True)
        
        if not config.performance_mode:
            print("🗂️ 波动率缓存管理器初始化完成")
            print(f"   📁 缓存目录: {self.cache_base_dir}")
    
    def get_cache_file_path(self, etf_code: str, threshold: Optional[str] = None) -> str:
        """获取缓存文件路径"""
        if threshold:
            threshold_dir = os.path.join(self.cache_base_dir, threshold)
            os.makedirs(threshold_dir, exist_ok=True)
            clean_code = etf_code.replace('.SH', '').replace('.SZ', '')
            return os.path.join(threshold_dir, f"{clean_code}.csv")
        else:
            clean_code = etf_code.replace('.SH', '').replace('.SZ', '')
            return os.path.join(self.cache_base_dir, f"{clean_code}.csv")
    
    def get_meta_file_path(self, etf_code: str, threshold: Optional[str] = None) -> str:
        """获取元数据文件路径"""
        clean_code = etf_code.replace('.SH', '').replace('.SZ', '')
        if threshold:
            return os.path.join(self.meta_dir, f"{threshold}_{clean_code}_meta.json")
        else:
            return os.path.join(self.meta_dir, f"{clean_code}_meta.json")
    
    def check_cache_validity(self, etf_code: str, source_file_path: str,
                           threshold: Optional[str] = None) -> Tuple[bool, Optional[Dict]]:
        """检查缓存有效性"""
        try:
            cache_file = self.get_cache_file_path(etf_code, threshold)
            meta_file = self.get_meta_file_path(etf_code, threshold)
            
            # 检查缓存文件是否存在
            if not os.path.exists(cache_file) or not os.path.exists(meta_file):
                return False, None
            
            # 读取元数据
            with open(meta_file, 'r', encoding='utf-8') as f:
                meta_data = json.load(f)
            
            # 检查源文件修改时间
            if not os.path.exists(source_file_path):
                return False, None
            
            source_mtime = os.path.getmtime(source_file_path)
            cached_mtime = meta_data.get('source_file_mtime', 0)
            
            # 检查配置是否变化
            cached_config = meta_data.get('config', {})
            current_config = {
                'adj_type': self.config.adj_type,
                'volatility_periods': self.config.volatility_periods,
                'annualized': self.config.annualized
            }
            
            config_changed = cached_config != current_config
            file_changed = abs(source_mtime - cached_mtime) > 5  # 5秒容差，避免文件系统时间精度问题
            
            is_valid = not (config_changed or file_changed)
            
            return is_valid, meta_data
            
        except Exception as e:
            if not self.config.performance_mode:
                print(f"❌ 缓存检查异常 {etf_code}: {str(e)}")
            return False, None
    
    def load_cache(self, etf_code: str, threshold: Optional[str] = None) -> Optional[pd.DataFrame]:
        """加载缓存数据"""
        try:
            cache_file = self.get_cache_file_path(etf_code, threshold)
            
            if not os.path.exists(cache_file):
                return None
            
            df = pd.read_csv(cache_file, encoding='utf-8')
            
            # 确保日期列正确解析
            if 'date' in df.columns:
                df['date'] = pd.to_datetime(df['date'])
            
            return df
            
        except Exception as e:
            if not self.config.performance_mode:
                print(f"❌ 缓存加载异常 {etf_code}: {str(e)}")
            return None
    
    def save_cache(self, etf_code: str, df: pd.DataFrame, source_file_path: str,
                   threshold: Optional[str] = None, additional_meta: Optional[Dict] = None) -> bool:
        """保存缓存数据"""
        try:
            cache_file = self.get_cache_file_path(etf_code, threshold)
            meta_file = self.get_meta_file_path(etf_code, threshold)
            
            # 保存数据
            df.to_csv(cache_file, index=False, encoding='utf-8')
            
            # 生成元数据
            meta_data = {
                'etf_code': etf_code,
                'threshold': threshold,
                'cache_created_time': datetime.now().isoformat(),
                'source_file_path': source_file_path,
                'source_file_mtime': os.path.getmtime(source_file_path) if os.path.exists(source_file_path) else 0,
                'config': {
                    'adj_type': self.config.adj_type,
                    'volatility_periods': self.config.volatility_periods,
                    'annualized': self.config.annualized
                },
                'data_info': {
                    'rows': len(df),
                    'columns': list(df.columns),
                    'date_range': {
                        'start': df['date'].min().isoformat() if 'date' in df.columns and not df.empty else None,
                        'end': df['date'].max().isoformat() if 'date' in df.columns and not df.empty else None
                    }
                }
            }
            
            # 添加额外元数据
            if additional_meta:
                meta_data.update(additional_meta)
            
            # 保存元数据
            with open(meta_file, 'w', encoding='utf-8') as f:
                json.dump(meta_data, f, ensure_ascii=False, indent=2)
            
            return True
            
        except Exception as e:
            if not self.config.performance_mode:
                print(f"❌ 缓存保存异常 {etf_code}: {str(e)}")
            return False
    
    def invalidate_cache(self, etf_code: str, threshold: Optional[str] = None) -> bool:
        """使缓存失效"""
        try:
            cache_file = self.get_cache_file_path(etf_code, threshold)
            meta_file = self.get_meta_file_path(etf_code, threshold)
            
            # 删除缓存文件
            if os.path.exists(cache_file):
                os.remove(cache_file)
            
            # 删除元数据文件
            if os.path.exists(meta_file):
                os.remove(meta_file)
            
            return True
            
        except Exception as e:
            if not self.config.performance_mode:
                print(f"❌ 缓存失效异常 {etf_code}: {str(e)}")
            return False
    
    def get_cache_info(self, threshold: Optional[str] = None) -> Dict[str, Any]:
        """获取缓存信息"""
        try:
            if threshold:
                cache_dir = os.path.join(self.cache_base_dir, threshold)
            else:
                cache_dir = self.cache_base_dir
            
            if not os.path.exists(cache_dir):
                return {'cached_files': 0, 'total_size_kb': 0}
            
            cached_files = []
            total_size = 0
            
            for file in os.listdir(cache_dir):
                if file.endswith('.csv'):
                    file_path = os.path.join(cache_dir, file)
                    file_size = os.path.getsize(file_path)
                    total_size += file_size
                    cached_files.append(file.replace('.csv', ''))
            
            return {
                'cached_files': len(cached_files),
                'total_size_kb': round(total_size / 1024, 2),
                'etf_codes': cached_files[:10]  # 只返回前10个
            }
            
        except Exception as e:
            return {'error': f'获取缓存信息失败: {str(e)}'}
    
    def clear_cache(self, threshold: Optional[str] = None) -> int:
        """清除缓存"""
        try:
            cleared_count = 0
            
            if threshold:
                # 清除特定threshold的缓存
                cache_dir = os.path.join(self.cache_base_dir, threshold)
                if os.path.exists(cache_dir):
                    for file in os.listdir(cache_dir):
                        if file.endswith('.csv'):
                            file_path = os.path.join(cache_dir, file)
                            os.remove(file_path)
                            cleared_count += 1
                            
                            # 删除对应的元数据
                            etf_code = file.replace('.csv', '')
                            meta_file = self.get_meta_file_path(etf_code, threshold)
                            if os.path.exists(meta_file):
                                os.remove(meta_file)
            else:
                # 清除所有缓存（包括所有threshold子目录）
                if os.path.exists(self.cache_base_dir):
                    # 清除根目录下的csv文件
                    for file in os.listdir(self.cache_base_dir):
                        if file.endswith('.csv'):
                            file_path = os.path.join(self.cache_base_dir, file)
                            os.remove(file_path)
                            cleared_count += 1
                            
                            # 删除对应的元数据
                            etf_code = file.replace('.csv', '')
                            meta_file = self.get_meta_file_path(etf_code, None)
                            if os.path.exists(meta_file):
                                os.remove(meta_file)
                    
                    # 清除所有threshold子目录
                    for item in os.listdir(self.cache_base_dir):
                        item_path = os.path.join(self.cache_base_dir, item)
                        if os.path.isdir(item_path) and item != "meta":
                            for file in os.listdir(item_path):
                                if file.endswith('.csv'):
                                    file_path = os.path.join(item_path, file)
                                    os.remove(file_path)
                                    cleared_count += 1
                                    
                                    # 删除对应的元数据
                                    etf_code = file.replace('.csv', '')
                                    meta_file = self.get_meta_file_path(etf_code, item)
                                    if os.path.exists(meta_file):
                                        os.remove(meta_file)
            
            return cleared_count
            
        except Exception as e:
            if not self.config.performance_mode:
                print(f"❌ 清除缓存异常: {str(e)}")
            return 0