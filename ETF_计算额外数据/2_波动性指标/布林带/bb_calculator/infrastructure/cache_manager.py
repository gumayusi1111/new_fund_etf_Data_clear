#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
布林带缓存管理器
==============

智能缓存系统，支持增量计算和高效的数据管理
"""

import os
import json
import pandas as pd
from typing import Dict, List, Optional, Tuple, Any
from .config import BBConfig
from .utils import BBUtils


class BBCacheManager:
    """布林带缓存管理器"""
    
    def __init__(self, config: BBConfig):
        """初始化缓存管理器"""
        self.config = config
        self.utils = BBUtils()
        self.cache_dir = config.cache_dir
        self.meta_dir = os.path.join(self.cache_dir, "meta")
        
        # 确保缓存目录存在
        self._ensure_cache_directories()
        
        # 缓存统计
        self.cache_stats = {
            'hits': 0,
            'misses': 0,
            'creates': 0,
            'updates': 0,
            'deletes': 0
        }
    
    def _ensure_cache_directories(self) -> None:
        """确保缓存目录存在"""
        directories = [
            self.cache_dir,
            self.meta_dir,
            os.path.join(self.cache_dir, "3000万门槛"),
            os.path.join(self.cache_dir, "5000万门槛")
        ]
        
        for directory in directories:
            self.utils.ensure_directory_exists(directory)
    
    def get_cache_file_path(self, threshold: str, etf_code: str) -> str:
        """获取缓存文件路径"""
        clean_etf_code = self.utils.format_etf_code(etf_code)
        return os.path.join(self.cache_dir, threshold, f"{clean_etf_code}.csv")
    
    def get_meta_file_path(self, threshold: str) -> str:
        """获取元数据文件路径"""
        return os.path.join(self.meta_dir, f"{threshold}_meta.json")
    
    def get_global_meta_file_path(self) -> str:
        """获取全局元数据文件路径"""
        return os.path.join(self.meta_dir, "cache_global_meta.json")
    
    def load_cache(self, threshold: str, etf_code: str) -> Optional[pd.DataFrame]:
        """
        加载缓存数据
        
        Args:
            threshold: 门槛名称
            etf_code: ETF代码
            
        Returns:
            Optional[pd.DataFrame]: 缓存数据，如果不存在或无效则返回None
        """
        cache_file = self.get_cache_file_path(threshold, etf_code)
        
        if not os.path.exists(cache_file):
            self.cache_stats['misses'] += 1
            return None
        
        try:
            # 读取缓存文件
            cached_df = pd.read_csv(cache_file)
            
            # 验证缓存数据格式
            if self._validate_cache_data(cached_df):
                self.cache_stats['hits'] += 1
                return cached_df
            else:
                # 缓存数据无效，删除
                self.delete_cache(threshold, etf_code)
                self.cache_stats['misses'] += 1
                return None
                
        except Exception:
            # 读取失败，删除损坏的缓存
            self.delete_cache(threshold, etf_code)
            self.cache_stats['misses'] += 1
            return None
    
    def save_cache(self, threshold: str, etf_code: str, data: pd.DataFrame) -> bool:
        """
        保存缓存数据
        
        Args:
            threshold: 门槛名称
            etf_code: ETF代码
            data: 要缓存的数据
            
        Returns:
            bool: 是否保存成功
        """
        if data.empty:
            return False
        
        cache_file = self.get_cache_file_path(threshold, etf_code)
        
        try:
            # 确保目录存在
            cache_dir = os.path.dirname(cache_file)
            self.utils.ensure_directory_exists(cache_dir)
            
            # 保存数据
            data.to_csv(cache_file, index=False, encoding='utf-8')
            
            # 更新元数据
            self._update_cache_metadata(threshold, etf_code, cache_file)
            
            self.cache_stats['creates'] += 1
            return True
            
        except Exception:
            return False
    
    def update_cache(self, threshold: str, etf_code: str, data: pd.DataFrame) -> bool:
        """
        更新缓存数据
        
        Args:
            threshold: 门槛名称
            etf_code: ETF代码
            data: 新数据
            
        Returns:
            bool: 是否更新成功
        """
        result = self.save_cache(threshold, etf_code, data)
        if result:
            self.cache_stats['updates'] += 1
            self.cache_stats['creates'] -= 1  # 减去create计数
        return result
    
    def delete_cache(self, threshold: str, etf_code: str) -> bool:
        """
        删除缓存数据
        
        Args:
            threshold: 门槛名称
            etf_code: ETF代码
            
        Returns:
            bool: 是否删除成功
        """
        cache_file = self.get_cache_file_path(threshold, etf_code)
        
        try:
            if os.path.exists(cache_file):
                os.remove(cache_file)
                self.cache_stats['deletes'] += 1
                
                # 更新元数据
                self._remove_from_cache_metadata(threshold, etf_code)
                
            return True
            
        except Exception:
            return False
    
    def is_cache_valid(self, threshold: str, etf_code: str, source_file_path: str) -> bool:
        """
        检查缓存是否有效
        
        Args:
            threshold: 门槛名称
            etf_code: ETF代码
            source_file_path: 源数据文件路径
            
        Returns:
            bool: 缓存是否有效
        """
        cache_file = self.get_cache_file_path(threshold, etf_code)
        
        # 缓存文件不存在
        if not os.path.exists(cache_file):
            return False
        
        # 源文件不存在
        if not os.path.exists(source_file_path):
            return False
        
        try:
            # 比较文件修改时间
            cache_mtime = self.utils.get_file_modification_time(cache_file)
            source_mtime = self.utils.get_file_modification_time(source_file_path)
            
            if cache_mtime is None or source_mtime is None:
                return False
            
            # 如果源文件比缓存新，缓存无效
            if source_mtime > cache_mtime:
                return False
            
            # 检查缓存数据格式
            cached_df = self.load_cache(threshold, etf_code)
            if cached_df is None:
                return False
            
            return self._validate_cache_data(cached_df)
            
        except Exception:
            return False
    
    def _validate_cache_data(self, data: pd.DataFrame) -> bool:
        """验证缓存数据格式"""
        if data.empty:
            return False
        
        # 检查必需列
        required_columns = ['code', 'date', 'bb_middle', 'bb_upper', 'bb_lower']
        for col in required_columns:
            if col not in data.columns:
                return False
        
        # 检查数据类型
        try:
            # 确保日期列可以转换
            pd.to_datetime(data['date'], errors='coerce')
            
            # 确保数值列是数值类型
            numeric_columns = ['bb_middle', 'bb_upper', 'bb_lower', 'bb_width', 'bb_position', 'bb_percent_b']
            for col in numeric_columns:
                if col in data.columns:
                    pd.to_numeric(data[col], errors='coerce')
            
            return True
            
        except Exception:
            return False
    
    def _update_cache_metadata(self, threshold: str, etf_code: str, cache_file: str) -> None:
        """更新缓存元数据"""
        try:
            meta_file = self.get_meta_file_path(threshold)
            
            # 加载现有元数据
            metadata = {}
            if os.path.exists(meta_file):
                try:
                    with open(meta_file, 'r', encoding='utf-8') as f:
                        metadata = json.load(f)
                except Exception:
                    metadata = {}
            
            # 更新ETF元数据
            clean_etf_code = self.utils.format_etf_code(etf_code)
            metadata[clean_etf_code] = {
                'cache_file': cache_file,
                'last_updated': self.utils.get_current_timestamp(),
                'file_size_mb': self.utils.get_file_size_mb(cache_file),
                'modification_time': self.utils.get_file_modification_time(cache_file)
            }
            
            # 保存元数据
            with open(meta_file, 'w', encoding='utf-8') as f:
                json.dump(metadata, f, ensure_ascii=False, indent=2)
                
        except Exception:
            pass
    
    def _remove_from_cache_metadata(self, threshold: str, etf_code: str) -> None:
        """从缓存元数据中移除ETF"""
        try:
            meta_file = self.get_meta_file_path(threshold)
            
            if not os.path.exists(meta_file):
                return
            
            # 加载元数据
            with open(meta_file, 'r', encoding='utf-8') as f:
                metadata = json.load(f)
            
            # 移除ETF
            clean_etf_code = self.utils.format_etf_code(etf_code)
            if clean_etf_code in metadata:
                del metadata[clean_etf_code]
            
            # 保存元数据
            with open(meta_file, 'w', encoding='utf-8') as f:
                json.dump(metadata, f, ensure_ascii=False, indent=2)
                
        except Exception:
            pass
    
    def get_cache_statistics(self, threshold: Optional[str] = None) -> Dict[str, Any]:
        """
        获取缓存统计信息
        
        Args:
            threshold: 指定门槛，如果为None则获取全部统计
            
        Returns:
            Dict[str, Any]: 缓存统计信息
        """
        stats = {
            'cache_operations': self.cache_stats.copy(),
            'cache_hit_rate': self._calculate_hit_rate(),
            'threshold_statistics': {}
        }
        
        thresholds = [threshold] if threshold else ["3000万门槛", "5000万门槛"]
        
        for thresh in thresholds:
            thresh_stats = self._get_threshold_cache_stats(thresh)
            stats['threshold_statistics'][thresh] = thresh_stats
        
        return stats
    
    def _calculate_hit_rate(self) -> float:
        """计算缓存命中率"""
        total_requests = self.cache_stats['hits'] + self.cache_stats['misses']
        if total_requests == 0:
            return 0.0
        
        return round((self.cache_stats['hits'] / total_requests) * 100, 2)
    
    def _get_threshold_cache_stats(self, threshold: str) -> Dict[str, Any]:
        """获取特定门槛的缓存统计"""
        cache_dir = os.path.join(self.cache_dir, threshold)
        
        stats = {
            'cached_files_count': 0,
            'total_cache_size_mb': 0,
            'cache_directory': cache_dir,
            'cache_files': []
        }
        
        if not os.path.exists(cache_dir):
            return stats
        
        try:
            cache_files = [f for f in os.listdir(cache_dir) if f.endswith('.csv')]
            stats['cached_files_count'] = len(cache_files)
            
            total_size = 0
            for filename in cache_files:
                file_path = os.path.join(cache_dir, filename)
                file_size = self.utils.get_file_size_mb(file_path)
                if file_size:
                    total_size += file_size
                    
                stats['cache_files'].append({
                    'filename': filename,
                    'size_mb': file_size,
                    'modification_time': self.utils.get_file_modification_time(file_path)
                })
            
            stats['total_cache_size_mb'] = round(total_size, 2)
            
        except Exception:
            pass
        
        return stats
    
    def clear_cache(self, threshold: Optional[str] = None) -> Dict[str, Any]:
        """
        清空缓存
        
        Args:
            threshold: 指定门槛，如果为None则清空所有缓存
            
        Returns:
            Dict[str, Any]: 清理结果
        """
        result = {
            'success': False,
            'cleared_thresholds': [],
            'cleared_files_count': 0,
            'errors': []
        }
        
        thresholds = [threshold] if threshold else ["3000万门槛", "5000万门槛"]
        
        for thresh in thresholds:
            try:
                cache_dir = os.path.join(self.cache_dir, thresh)
                if os.path.exists(cache_dir):
                    files_before = self.utils.get_directory_file_count(cache_dir)
                    
                    if self.utils.clean_directory(cache_dir):
                        result['cleared_thresholds'].append(thresh)
                        result['cleared_files_count'] += files_before
                    else:
                        result['errors'].append(f"清理{thresh}缓存失败")
                        
            except Exception as e:
                result['errors'].append(f"清理{thresh}缓存异常: {str(e)}")
        
        result['success'] = len(result['errors']) == 0
        
        # 重置统计信息
        if result['success']:
            self.cache_stats = {
                'hits': 0,
                'misses': 0,
                'creates': 0,
                'updates': 0,
                'deletes': 0
            }
        
        return result
    
    def optimize_cache(self, threshold: str, max_age_days: int = 30) -> Dict[str, Any]:
        """
        优化缓存，删除过期的缓存文件
        
        Args:
            threshold: 门槛名称
            max_age_days: 最大年龄（天）
            
        Returns:
            Dict[str, Any]: 优化结果
        """
        result = {
            'success': False,
            'removed_files': [],
            'removed_count': 0,
            'errors': []
        }
        
        cache_dir = os.path.join(self.cache_dir, threshold)
        
        if not os.path.exists(cache_dir):
            result['success'] = True
            return result
        
        try:
            import time
            current_time = time.time()
            max_age_seconds = max_age_days * 24 * 3600
            
            cache_files = [f for f in os.listdir(cache_dir) if f.endswith('.csv')]
            
            for filename in cache_files:
                file_path = os.path.join(cache_dir, filename)
                
                try:
                    file_mtime = self.utils.get_file_modification_time(file_path)
                    if file_mtime and (current_time - file_mtime) > max_age_seconds:
                        os.remove(file_path)
                        result['removed_files'].append(filename)
                        result['removed_count'] += 1
                        
                except Exception as e:
                    result['errors'].append(f"删除文件{filename}失败: {str(e)}")
            
            result['success'] = True
            
        except Exception as e:
            result['errors'].append(f"缓存优化异常: {str(e)}")
        
        return result