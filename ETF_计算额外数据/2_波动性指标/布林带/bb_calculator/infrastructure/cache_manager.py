#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
布林带缓存管理器
==============

智能缓存系统，提高计算效率和数据管理
参照趋势类指标的缓存模式
"""

import os
import json
import pickle
import hashlib
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
import pandas as pd
from .utils import BBUtils


class BBCacheManager:
    """布林带智能缓存管理器"""
    
    def __init__(self, config):
        """初始化缓存管理器"""
        self.config = config
        self.utils = BBUtils()
        self.cache_dir = config.cache_dir
        self.meta_cache_dir = os.path.join(self.cache_dir, "meta")
        
        # 确保缓存目录存在
        self._ensure_cache_directories()
        
        # 缓存统计
        self.cache_stats = {
            'hits': 0,
            'misses': 0,
            'total_requests': 0
        }
    
    def _ensure_cache_directories(self) -> None:
        """确保缓存目录存在"""
        directories = [
            self.cache_dir,
            self.meta_cache_dir,
            os.path.join(self.cache_dir, "3000万门槛"),
            os.path.join(self.cache_dir, "5000万门槛")
        ]
        
        for directory in directories:
            self.utils.ensure_directory_exists(directory)
    
    def get_cache_key(self, etf_code: str, threshold: str = "") -> str:
        """生成缓存键值"""
        # 基于ETF代码、复权类型和配置参数生成唯一键值
        key_components = [
            self.utils.format_etf_code(etf_code),
            self.config.adj_type,
            str(self.config.get_bb_period()),
            str(self.config.get_bb_std_multiplier()),
            threshold
        ]
        
        key_string = "|".join(key_components)
        return hashlib.md5(key_string.encode()).hexdigest()
    
    def is_cache_valid(self, etf_code: str, threshold: str = "") -> bool:
        """检查缓存是否有效"""
        self.cache_stats['total_requests'] += 1
        
        try:
            cache_key = self.get_cache_key(etf_code, threshold)
            meta_file = os.path.join(self.meta_cache_dir, f"{cache_key}.json")
            
            if not os.path.exists(meta_file):
                self.cache_stats['misses'] += 1
                return False
            
            # 检查元数据
            with open(meta_file, 'r', encoding='utf-8') as f:
                meta_data = json.load(f)
            
            # 检查源文件是否被修改
            source_file = self.config.get_etf_file_path(etf_code)
            if not os.path.exists(source_file):
                self.cache_stats['misses'] += 1
                return False
            
            source_mtime = self.utils.get_file_modification_time(source_file)
            cached_mtime = meta_data.get('source_file_mtime')
            
            if source_mtime != cached_mtime:
                self.cache_stats['misses'] += 1
                return False
            
            # 检查缓存时间（24小时有效期）
            cache_time = datetime.fromisoformat(meta_data.get('cache_timestamp'))
            if datetime.now() - cache_time > timedelta(hours=24):
                self.cache_stats['misses'] += 1
                return False
            
            # 检查配置参数是否匹配
            cached_config = meta_data.get('bb_config', {})
            current_config = {
                'period': self.config.get_bb_period(),
                'std_multiplier': self.config.get_bb_std_multiplier(),
                'adj_type': self.config.adj_type
            }
            
            if cached_config != current_config:
                self.cache_stats['misses'] += 1
                return False
            
            self.cache_stats['hits'] += 1
            return True
            
        except Exception:
            self.cache_stats['misses'] += 1
            return False
    
    def get_cached_result(self, etf_code: str, threshold: str = "") -> Optional[Dict]:
        """获取缓存结果"""
        if not self.is_cache_valid(etf_code, threshold):
            return None
        
        try:
            cache_key = self.get_cache_key(etf_code, threshold)
            
            if threshold:
                cache_file = os.path.join(self.cache_dir, threshold, f"{cache_key}.pkl")
            else:
                cache_file = os.path.join(self.cache_dir, f"{cache_key}.pkl")
            
            if not os.path.exists(cache_file):
                return None
            
            with open(cache_file, 'rb') as f:
                cached_data = pickle.load(f)
            
            return cached_data
            
        except Exception:
            return None
    
    def save_to_cache(self, etf_code: str, calculation_result: Dict, 
                     threshold: str = "") -> bool:
        """保存计算结果到缓存"""
        try:
            cache_key = self.get_cache_key(etf_code, threshold)
            
            # 保存计算结果
            if threshold:
                cache_file = os.path.join(self.cache_dir, threshold, f"{cache_key}.pkl")
            else:
                cache_file = os.path.join(self.cache_dir, f"{cache_key}.pkl")
            
            with open(cache_file, 'wb') as f:
                pickle.dump(calculation_result, f)
            
            # 保存元数据
            source_file = self.config.get_etf_file_path(etf_code)
            meta_data = {
                'etf_code': etf_code,
                'cache_key': cache_key,
                'cache_timestamp': datetime.now().isoformat(),
                'source_file_path': source_file,
                'source_file_mtime': self.utils.get_file_modification_time(source_file),
                'source_file_hash': self.utils.calculate_file_hash(source_file),
                'bb_config': {
                    'period': self.config.get_bb_period(),
                    'std_multiplier': self.config.get_bb_std_multiplier(),
                    'adj_type': self.config.adj_type
                },
                'threshold': threshold,
                'calculation_success': calculation_result.get('success', False)
            }
            
            meta_file = os.path.join(self.meta_cache_dir, f"{cache_key}.json")
            with open(meta_file, 'w', encoding='utf-8') as f:
                json.dump(meta_data, f, ensure_ascii=False, indent=2)
            
            return True
            
        except Exception:
            return False
    
    def clear_cache(self, etf_code: Optional[str] = None, threshold: Optional[str] = None) -> int:
        """清理缓存"""
        cleared_count = 0
        
        try:
            if etf_code:
                # 清理特定ETF的缓存
                cache_key = self.get_cache_key(etf_code, threshold or "")
                
                # 删除缓存文件
                if threshold:
                    cache_file = os.path.join(self.cache_dir, threshold, f"{cache_key}.pkl")
                else:
                    cache_file = os.path.join(self.cache_dir, f"{cache_key}.pkl")
                
                if os.path.exists(cache_file):
                    os.remove(cache_file)
                    cleared_count += 1
                
                # 删除元数据文件
                meta_file = os.path.join(self.meta_cache_dir, f"{cache_key}.json")
                if os.path.exists(meta_file):
                    os.remove(meta_file)
                    cleared_count += 1
            else:
                # 清理所有缓存
                for root, dirs, files in os.walk(self.cache_dir):
                    for file in files:
                        file_path = os.path.join(root, file)
                        os.remove(file_path)
                        cleared_count += 1
            
            return cleared_count
            
        except Exception:
            return 0
    
    def get_cache_statistics(self) -> Dict[str, Any]:
        """获取缓存统计信息"""
        total_requests = self.cache_stats['total_requests']
        hits = self.cache_stats['hits']
        
        hit_rate = (hits / total_requests * 100) if total_requests > 0 else 0
        
        # 计算缓存目录大小
        cache_size = 0
        cache_file_count = 0
        
        try:
            for root, dirs, files in os.walk(self.cache_dir):
                for file in files:
                    file_path = os.path.join(root, file)
                    cache_size += os.path.getsize(file_path)
                    cache_file_count += 1
        except Exception:
            pass
        
        return {
            'hit_rate_percent': round(hit_rate, 2),
            'total_requests': total_requests,
            'cache_hits': hits,
            'cache_misses': self.cache_stats['misses'],
            'cache_size_mb': round(cache_size / (1024 * 1024), 2),
            'cache_file_count': cache_file_count,
            'cache_directory': self.cache_dir,
            'last_updated': self.utils.get_current_timestamp()
        }
    
    def cleanup_expired_cache(self, max_age_hours: int = 24) -> int:
        """清理过期缓存"""
        cleared_count = 0
        cutoff_time = datetime.now() - timedelta(hours=max_age_hours)
        
        try:
            # 遍历元数据文件
            for meta_file in os.listdir(self.meta_cache_dir):
                if not meta_file.endswith('.json'):
                    continue
                
                meta_path = os.path.join(self.meta_cache_dir, meta_file)
                
                try:
                    with open(meta_path, 'r', encoding='utf-8') as f:
                        meta_data = json.load(f)
                    
                    cache_time = datetime.fromisoformat(meta_data.get('cache_timestamp'))
                    
                    if cache_time < cutoff_time:
                        # 删除过期缓存
                        cache_key = meta_data.get('cache_key')
                        threshold = meta_data.get('threshold', '')
                        
                        if threshold:
                            cache_file = os.path.join(self.cache_dir, threshold, f"{cache_key}.pkl")
                        else:
                            cache_file = os.path.join(self.cache_dir, f"{cache_key}.pkl")
                        
                        if os.path.exists(cache_file):
                            os.remove(cache_file)
                            cleared_count += 1
                        
                        # 删除元数据文件
                        os.remove(meta_path)
                        cleared_count += 1
                        
                except Exception:
                    continue
            
            return cleared_count
            
        except Exception:
            return 0