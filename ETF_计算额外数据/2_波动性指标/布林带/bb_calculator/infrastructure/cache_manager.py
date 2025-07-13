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
                     threshold: str = "", param_set: str = None) -> bool:
        """保存计算结果到缓存（支持参数集分层）"""
        try:
            # 直接保存为CSV格式，每个ETF一个文件
            if threshold:
                if param_set:
                    cache_file = os.path.join(self.cache_dir, threshold, param_set, f"{etf_code}.csv")
                else:
                    # 使用当前参数集
                    current_param_set = self.config.get_current_param_set_name()
                    cache_file = os.path.join(self.cache_dir, threshold, current_param_set, f"{etf_code}.csv")
            else:
                cache_file = os.path.join(self.cache_dir, f"{etf_code}.csv")
            
            # 确保目录存在
            os.makedirs(os.path.dirname(cache_file), exist_ok=True)
            
            # 创建缓存数据格式 - 直接保存计算结果中的完整历史数据
            if calculation_result.get('success') and calculation_result.get('full_history_data') is not None:
                # 直接使用计算结果中的完整历史数据
                full_data = calculation_result.get('full_history_data')
                full_data.to_csv(cache_file, index=False, encoding='utf-8')
                
                # 创建元数据文件
                self._create_cache_metadata(etf_code, threshold)
            
            return True
            
        except Exception:
            return False
    
    def _create_cache_metadata(self, etf_code: str, threshold: str = "") -> bool:
        """创建缓存元数据文件"""
        try:
            cache_key = self.get_cache_key(etf_code, threshold)
            meta_file = os.path.join(self.meta_cache_dir, f"{cache_key}.json")
            
            # 获取源文件信息
            source_file = self.config.get_etf_file_path(etf_code)
            source_mtime = self.utils.get_file_modification_time(source_file) if os.path.exists(source_file) else None
            
            # 创建元数据
            meta_data = {
                'etf_code': etf_code,
                'threshold': threshold,
                'cache_timestamp': datetime.now().isoformat(),
                'source_file_mtime': source_mtime,
                'bb_config': {
                    'period': self.config.get_bb_period(),
                    'std_multiplier': self.config.get_bb_std_multiplier(),
                    'adj_type': self.config.adj_type
                },
                'param_set': self.config.get_current_param_set_name()
            }
            
            # 保存元数据文件
            with open(meta_file, 'w', encoding='utf-8') as f:
                json.dump(meta_data, f, ensure_ascii=False, indent=2)
            
            return True
            
        except Exception:
            return False
    
    def _generate_full_history_data(self, etf_code: str, calculation_result: Dict) -> Optional[pd.DataFrame]:
        """生成完整历史数据用于缓存"""
        try:
            from .data_reader import BBDataReader
            
            # 重新读取原始数据
            data_reader = BBDataReader(self.config)
            raw_data = data_reader.read_etf_data(etf_code)
            
            if raw_data is None or raw_data.empty:
                return None
            
            # 重新计算完整的布林带历史数据
            from ..engines.bb_engine import BollingerBandsEngine
            bb_engine = BollingerBandsEngine(self.config)
            
            # 这里需要修改引擎以返回完整历史数据
            full_bb_data = bb_engine.calculate_full_history(raw_data)
            
            if full_bb_data is None:
                return None
                
            # 格式化完整数据
            formatted_rows = []
            for idx, row in raw_data.iterrows():
                if idx < len(full_bb_data['bb_middle']):
                    date_str = row['日期'].strftime('%Y-%m-%d') if pd.notna(row['日期']) else ''
                    formatted_rows.append({
                        'date': date_str,
                        'code': etf_code,
                        'bb_middle': full_bb_data['bb_middle'][idx] if not pd.isna(full_bb_data['bb_middle'][idx]) else '',
                        'bb_upper': full_bb_data['bb_upper'][idx] if not pd.isna(full_bb_data['bb_upper'][idx]) else '',
                        'bb_lower': full_bb_data['bb_lower'][idx] if not pd.isna(full_bb_data['bb_lower'][idx]) else '',
                        'bb_width': full_bb_data['bb_width'][idx] if not pd.isna(full_bb_data['bb_width'][idx]) else '',
                        'bb_position': full_bb_data['bb_position'][idx] if not pd.isna(full_bb_data['bb_position'][idx]) else '',
                        'bb_percent_b': full_bb_data['bb_percent_b'][idx] if not pd.isna(full_bb_data['bb_percent_b'][idx]) else ''
                    })
            
            # 按日期倒序排列（最新日期在前）
            df = pd.DataFrame(formatted_rows)
            if not df.empty and 'date' in df.columns:
                df['date_sort'] = pd.to_datetime(df['date'], errors='coerce')
                df = df.sort_values('date_sort', ascending=False).drop('date_sort', axis=1)
            
            return df
            
        except Exception:
            return None
    
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