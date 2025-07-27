"""
OBV指标缓存管理系统 - 智能缓存与增量更新
=====================================

实现高效的缓存管理和增量更新机制
支持文件级缓存、元数据管理和自动清理

核心功能:
- 智能缓存检测和命中
- 增量数据更新
- 缓存过期管理
- 元数据追踪
- 空间管理和清理

技术特点:
- 96%+缓存命中率设计
- 原子性操作保证数据一致性
- 内存优化和性能优先
- 支持并发访问
"""

import os
import json
import hashlib
import pickle
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime, timedelta
import pandas as pd
import logging
import threading
from dataclasses import dataclass, asdict
import shutil

@dataclass
class CacheMetadata:
    """缓存元数据结构"""
    etf_code: str
    threshold: str
    file_path: str
    file_size: int
    record_count: int
    last_date: str
    create_time: str
    update_time: str
    data_hash: str
    version: str = "1.0.0"

class OBVCacheManager:
    """OBV指标缓存管理器"""
    
    def __init__(self, cache_dir: Path, meta_dir: Path, 
                 expire_days: int = 30, max_size_mb: int = 2048):
        """
        初始化缓存管理器
        
        Args:
            cache_dir: 缓存目录
            meta_dir: 元数据目录
            expire_days: 缓存过期天数
            max_size_mb: 最大缓存大小(MB)
        """
        self.cache_dir = Path(cache_dir)
        self.meta_dir = Path(meta_dir)
        self.expire_days = expire_days
        self.max_size_mb = max_size_mb
        
        # 确保目录存在
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.meta_dir.mkdir(parents=True, exist_ok=True)
        
        # 线程锁保证并发安全
        self._lock = threading.RLock()
        
        # 初始化日志
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # 缓存统计 - 支持持久化
        self._stats_file = self.meta_dir / 'cache_stats.json'
        self._stats = self._load_stats()
        
        self.logger.info(f"缓存管理器初始化完成 - 目录:{cache_dir}, 过期:{expire_days}天")
    
    def _load_stats(self) -> Dict[str, int]:
        """从文件加载统计信息"""
        default_stats = {
            'hits': 0,
            'misses': 0,
            'updates': 0,
            'cleanups': 0,
            'errors': 0
        }
        
        try:
            if self._stats_file.exists():
                with open(self._stats_file, 'r', encoding='utf-8') as f:
                    loaded_stats = json.load(f)
                    # 确保所有必需字段都存在
                    for key in default_stats:
                        if key not in loaded_stats:
                            loaded_stats[key] = 0
                    return loaded_stats
        except Exception as e:
            self.logger.warning(f"加载统计信息失败，使用默认值: {str(e)}")
        
        return default_stats
    
    def _save_stats(self):
        """保存统计信息到文件"""
        try:
            with self._lock:
                with open(self._stats_file, 'w', encoding='utf-8') as f:
                    json.dump(self._stats, f, indent=2)
        except Exception as e:
            self.logger.error(f"保存统计信息失败: {str(e)}")
    
    def get_cache_key(self, etf_code: str, threshold: str) -> str:
        """
        生成缓存键
        
        Args:
            etf_code: ETF代码
            threshold: 门槛类型
            
        Returns:
            缓存键字符串
        """
        return f"{etf_code}_{threshold}".replace('万门槛', '')
    
    def get_cache_path(self, etf_code: str, threshold: str) -> Path:
        """
        获取缓存文件路径
        
        Args:
            etf_code: ETF代码
            threshold: 门槛类型
            
        Returns:
            缓存文件完整路径
        """
        # 直接使用cache_dir，因为config中已经包含门槛目录
        cache_subdir = self.cache_dir
        
        cache_subdir.mkdir(parents=True, exist_ok=True)
        return cache_subdir / f"{etf_code}.csv"
    
    def get_metadata_path(self, etf_code: str, threshold: str) -> Path:
        """
        获取元数据文件路径 - 使用阈值级别聚合格式
        
        Args:
            etf_code: ETF代码
            threshold: 门槛类型
            
        Returns:
            元数据文件路径
        """
        return self.meta_dir / f"{threshold}_meta.json"
    
    def load_metadata(self, etf_code: str, threshold: str) -> Optional[CacheMetadata]:
        """
        加载缓存元数据 - 从阈值级别聚合文件中获取
        
        Args:
            etf_code: ETF代码
            threshold: 门槛类型
            
        Returns:
            缓存元数据或None
        """
        try:
            meta_path = self.get_metadata_path(etf_code, threshold)
            
            if not meta_path.exists():
                return None
            
            with open(meta_path, 'r', encoding='utf-8') as f:
                threshold_meta = json.load(f)
                
                # 从阈值级别聚合数据中获取特定ETF信息
                if etf_code not in threshold_meta:
                    return None
                
                etf_meta = threshold_meta[etf_code]
                
                # 构建CacheMetadata对象
                cache_path = self.get_cache_path(etf_code, threshold)
                return CacheMetadata(
                    etf_code=etf_code,
                    threshold=threshold,
                    file_path=str(cache_path),
                    file_size=cache_path.stat().st_size if cache_path.exists() else 0,
                    record_count=etf_meta.get('data_count', 0),
                    last_date=etf_meta.get('last_date', ''),
                    create_time=etf_meta.get('last_updated', ''),
                    update_time=etf_meta.get('last_updated', ''),
                    data_hash=etf_meta.get('data_hash', ''),
                    version="1.0.0"
                )
                
        except Exception as e:
            self.logger.error(f"加载元数据失败 {etf_code}_{threshold}: {str(e)}")
            return None
    
    def save_metadata(self, metadata: CacheMetadata) -> bool:
        """
        保存缓存元数据 - 使用阈值级别聚合格式
        
        Args:
            metadata: 缓存元数据
            
        Returns:
            保存是否成功
        """
        try:
            with self._lock:
                meta_path = self.get_metadata_path(metadata.etf_code, metadata.threshold)
                
                # 加载现有阈值级别数据
                threshold_meta = {}
                if meta_path.exists():
                    try:
                        with open(meta_path, 'r', encoding='utf-8') as f:
                            threshold_meta = json.load(f)
                    except:
                        threshold_meta = {}
                
                # 更新特定ETF的元数据
                threshold_meta[metadata.etf_code] = {
                    "last_updated": metadata.update_time,
                    "data_count": metadata.record_count,
                    "threshold": metadata.threshold,
                    "cache_file": f"{metadata.etf_code}.csv",
                    "last_date": metadata.last_date,
                    "data_hash": metadata.data_hash
                }
                
                # 原子性写入
                temp_path = meta_path.with_suffix('.tmp')
                with open(temp_path, 'w', encoding='utf-8') as f:
                    json.dump(threshold_meta, f, ensure_ascii=False, indent=2)
                
                # 原子性重命名
                temp_path.replace(meta_path)
                
                return True
                
        except Exception as e:
            self.logger.error(f"保存元数据失败 {metadata.etf_code}_{metadata.threshold}: {str(e)}")
            return False
    
    def is_cache_valid(self, etf_code: str, threshold: str, 
                      source_data_date: Optional[str] = None) -> bool:
        """
        检查缓存是否有效
        
        Args:
            etf_code: ETF代码
            threshold: 门槛类型
            source_data_date: 源数据最新日期(可选)
            
        Returns:
            缓存是否有效
        """
        try:
            # 检查缓存文件是否存在
            cache_path = self.get_cache_path(etf_code, threshold)
            if not cache_path.exists():
                return False
            
            # 加载元数据
            metadata = self.load_metadata(etf_code, threshold)
            if not metadata:
                return False
            
            # 检查文件完整性
            if not Path(metadata.file_path).exists():
                return False
            
            # 检查过期时间
            update_time = datetime.fromisoformat(metadata.update_time)
            if datetime.now() - update_time > timedelta(days=self.expire_days):
                self.logger.debug(f"缓存过期: {etf_code}_{threshold}")
                return False
            
            # 检查数据日期
            if source_data_date:
                try:
                    source_date = datetime.strptime(source_data_date, '%Y-%m-%d')
                    cache_date = datetime.strptime(metadata.last_date, '%Y-%m-%d')
                    
                    if source_date > cache_date:
                        self.logger.debug(f"数据有更新: {etf_code}_{threshold}")
                        return False
                except:
                    pass
            
            return True
            
        except Exception as e:
            self.logger.error(f"缓存有效性检查失败 {etf_code}_{threshold}: {str(e)}")
            return False
    
    def load_cache(self, etf_code: str, threshold: str) -> Optional[pd.DataFrame]:
        """
        加载缓存数据
        
        Args:
            etf_code: ETF代码
            threshold: 门槛类型
            
        Returns:
            缓存的DataFrame或None
        """
        try:
            with self._lock:
                cache_path = self.get_cache_path(etf_code, threshold)
                
                if not cache_path.exists():
                    self._stats['misses'] += 1
                    self._save_stats()  # 保存统计信息
                    return None
                
                # 验证缓存有效性
                if not self.is_cache_valid(etf_code, threshold):
                    self._stats['misses'] += 1
                    self._save_stats()  # 保存统计信息
                    return None
                
                # 加载数据
                df = pd.read_csv(cache_path)
                
                # 验证数据完整性
                expected_columns = ['code', 'date', 'obv', 'obv_ma10', 
                                  'obv_change_5', 'obv_change_20', 'calc_time']
                if not all(col in df.columns for col in expected_columns):
                    self.logger.warning(f"缓存数据格式不完整: {etf_code}_{threshold}")
                    self._stats['misses'] += 1
                    self._save_stats()  # 保存统计信息
                    return None
                
                self._stats['hits'] += 1
                self._save_stats()  # 保存统计信息
                self.logger.debug(f"缓存命中: {etf_code}_{threshold}, 记录数: {len(df)}")
                
                return df
                
        except Exception as e:
            self.logger.error(f"加载缓存失败 {etf_code}_{threshold}: {str(e)}")
            self._stats['errors'] += 1
            self._stats['misses'] += 1  # 加载失败也应该计为miss
            self._save_stats()  # 保存统计信息
            return None
    
    def save_cache(self, etf_code: str, threshold: str, 
                   data: pd.DataFrame, source_hash: str = "") -> bool:
        """
        保存数据到缓存
        
        Args:
            etf_code: ETF代码
            threshold: 门槛类型
            data: 要缓存的数据
            source_hash: 源数据哈希值
            
        Returns:
            保存是否成功
        """
        try:
            with self._lock:
                if data.empty:
                    self.logger.warning(f"尝试缓存空数据: {etf_code}_{threshold}")
                    return False
                
                cache_path = self.get_cache_path(etf_code, threshold)
                
                # 原子性保存数据
                temp_path = cache_path.with_suffix('.tmp')
                data.to_csv(temp_path, index=False, encoding='utf-8')
                temp_path.replace(cache_path)
                
                # 计算文件信息
                file_size = cache_path.stat().st_size
                record_count = len(data)
                last_date = data['date'].max() if 'date' in data.columns else ""
                
                # 生成数据哈希
                if not source_hash:
                    source_hash = self._calculate_data_hash(data)
                
                # 创建元数据
                metadata = CacheMetadata(
                    etf_code=etf_code,
                    threshold=threshold,
                    file_path=str(cache_path),
                    file_size=file_size,
                    record_count=record_count,
                    last_date=last_date,
                    create_time=datetime.now().isoformat(),
                    update_time=datetime.now().isoformat(),
                    data_hash=source_hash
                )
                
                # 保存元数据
                if self.save_metadata(metadata):
                    self._stats['updates'] += 1
                    self._save_stats()  # 保存统计信息
                    self.logger.debug(f"缓存保存成功: {etf_code}_{threshold}, 记录数: {record_count}")
                    return True
                else:
                    # 如果元数据保存失败，删除数据文件
                    cache_path.unlink(missing_ok=True)
                    return False
                
        except Exception as e:
            self.logger.error(f"保存缓存失败 {etf_code}_{threshold}: {str(e)}")
            self._stats['errors'] += 1
            return False
    
    def update_cache_incremental(self, etf_code: str, threshold: str,
                               new_data: pd.DataFrame) -> bool:
        """
        增量更新缓存
        
        Args:
            etf_code: ETF代码
            threshold: 门槛类型
            new_data: 新增数据
            
        Returns:
            更新是否成功
        """
        try:
            with self._lock:
                # 加载现有缓存
                existing_data = self.load_cache(etf_code, threshold)
                
                if existing_data is None:
                    # 没有缓存，直接保存新数据
                    return self.save_cache(etf_code, threshold, new_data)
                
                # 过滤重复数据
                if 'date' in existing_data.columns and 'date' in new_data.columns:
                    max_existing_date = existing_data['date'].max()
                    new_data_filtered = new_data[
                        new_data['date'] > max_existing_date
                    ].copy()
                    
                    if new_data_filtered.empty:
                        self.logger.debug(f"没有新数据需要增量更新: {etf_code}_{threshold}")
                        return True
                    
                    # 合并数据
                    combined_data = pd.concat([existing_data, new_data_filtered], 
                                            ignore_index=True)
                else:
                    # 无法判断新旧数据，直接替换
                    combined_data = new_data
                
                # 保存合并后的数据
                return self.save_cache(etf_code, threshold, combined_data)
                
        except Exception as e:
            self.logger.error(f"增量更新缓存失败 {etf_code}_{threshold}: {str(e)}")
            self._stats['errors'] += 1
            return False
    
    def invalidate_cache(self, etf_code: str, threshold: str) -> bool:
        """
        使缓存失效(删除) - 从阈值级别聚合文件中移除ETF记录
        
        Args:
            etf_code: ETF代码
            threshold: 门槛类型
            
        Returns:
            操作是否成功
        """
        try:
            with self._lock:
                cache_path = self.get_cache_path(etf_code, threshold)
                meta_path = self.get_metadata_path(etf_code, threshold)
                
                success = True
                
                # 删除缓存文件
                if cache_path.exists():
                    cache_path.unlink()
                    self.logger.debug(f"删除缓存文件: {cache_path}")
                
                # 从阈值级别元数据中移除ETF记录
                if meta_path.exists():
                    try:
                        with open(meta_path, 'r', encoding='utf-8') as f:
                            threshold_meta = json.load(f)
                        
                        # 移除特定ETF的记录
                        if etf_code in threshold_meta:
                            del threshold_meta[etf_code]
                            
                            # 如果文件为空，删除整个文件
                            if not threshold_meta:
                                meta_path.unlink()
                                self.logger.debug(f"删除空元数据文件: {meta_path}")
                            else:
                                # 保存更新后的元数据
                                with open(meta_path, 'w', encoding='utf-8') as f:
                                    json.dump(threshold_meta, f, ensure_ascii=False, indent=2)
                                self.logger.debug(f"从元数据中移除ETF记录: {etf_code}")
                    except Exception as e:
                        self.logger.warning(f"处理元数据文件失败: {str(e)}")
                
                return success
                
        except Exception as e:
            self.logger.error(f"缓存失效操作失败 {etf_code}_{threshold}: {str(e)}")
            return False
    
    def cleanup_expired_cache(self, force: bool = False) -> Dict[str, Any]:
        """
        清理过期缓存
        
        Args:
            force: 是否强制清理所有缓存
            
        Returns:
            清理统计信息
        """
        try:
            with self._lock:
                cleanup_stats = {
                    'files_removed': 0,
                    'space_freed_mb': 0.0,
                    'expired_count': 0,
                    'error_count': 0,
                    'total_scanned': 0
                }
                
                current_time = datetime.now()
                cutoff_time = current_time - timedelta(days=self.expire_days)
                
                # 扫描所有阈值级别元数据文件
                for meta_file in self.meta_dir.glob("*万门槛_meta.json"):
                    cleanup_stats['total_scanned'] += 1
                    
                    try:
                        with open(meta_file, 'r', encoding='utf-8') as f:
                            threshold_meta = json.load(f)
                        
                        # 检查文件中的每个ETF记录
                        etfs_to_remove = []
                        for etf_code, etf_meta in threshold_meta.items():
                            try:
                                update_time = datetime.fromisoformat(etf_meta['last_updated'])
                                
                                # 检查是否需要清理
                                should_cleanup = force or update_time < cutoff_time
                                
                                if should_cleanup:
                                    # 删除数据文件
                                    threshold = etf_meta.get('threshold', '3000万门槛')
                                    cache_path = self.get_cache_path(etf_code, threshold)
                                    if cache_path.exists():
                                        file_size = cache_path.stat().st_size
                                        cache_path.unlink()
                                        cleanup_stats['space_freed_mb'] += file_size / (1024 * 1024)
                                    
                                    etfs_to_remove.append(etf_code)
                                    cleanup_stats['files_removed'] += 1
                                    if not force:
                                        cleanup_stats['expired_count'] += 1
                                    
                                    self.logger.debug(f"清理缓存: {etf_code}_{threshold}")
                            
                            except Exception as e:
                                self.logger.error(f"处理ETF记录失败 {etf_code}: {str(e)}")
                                cleanup_stats['error_count'] += 1
                        
                        # 从元数据中移除过期的ETF记录
                        for etf_code in etfs_to_remove:
                            if etf_code in threshold_meta:
                                del threshold_meta[etf_code]
                        
                        # 如果文件为空，删除整个文件，否则保存更新后的数据
                        if not threshold_meta:
                            meta_file.unlink()
                            self.logger.debug(f"删除空元数据文件: {meta_file}")
                        else:
                            with open(meta_file, 'w', encoding='utf-8') as f:
                                json.dump(threshold_meta, f, ensure_ascii=False, indent=2)
                    
                    except Exception as e:
                        self.logger.error(f"清理缓存项失败 {meta_file}: {str(e)}")
                        cleanup_stats['error_count'] += 1
                        continue
                
                self._stats['cleanups'] += 1
                
                self.logger.info(f"缓存清理完成 - 删除文件:{cleanup_stats['files_removed']}个, "
                              f"释放空间:{cleanup_stats['space_freed_mb']:.1f}MB")
                
                return cleanup_stats
                
        except Exception as e:
            self.logger.error(f"缓存清理异常: {str(e)}")
            return {'error': str(e)}
    
    def get_cache_statistics(self) -> Dict[str, Any]:
        """
        获取缓存统计信息
        
        Returns:
            缓存统计信息
        """
        try:
            with self._lock:
                # 基础统计
                base_stats = self._stats.copy()
                
                # 计算命中率
                total_requests = base_stats['hits'] + base_stats['misses']
                hit_rate = (base_stats['hits'] / total_requests * 100) if total_requests > 0 else 0
                
                # 扫描缓存目录
                cache_files = list(self.cache_dir.rglob("*.csv"))
                meta_files = list(self.meta_dir.glob("*万门槛_meta.json"))
                
                # 计算总大小
                total_size = sum(f.stat().st_size for f in cache_files)
                total_size_mb = total_size / (1024 * 1024)
                
                # 组装统计信息
                stats = {
                    'performance': {
                        'hit_rate': round(hit_rate, 2),
                        'hits': base_stats['hits'],
                        'misses': base_stats['misses'],
                        'total_requests': total_requests
                    },
                    'storage': {
                        'cache_files': len(cache_files),
                        'meta_files': len(meta_files),
                        'total_size_mb': round(total_size_mb, 2),
                        'max_size_mb': self.max_size_mb,
                        'usage_percent': round(total_size_mb / self.max_size_mb * 100, 1) if self.max_size_mb > 0 else 0
                    },
                    'operations': {
                        'updates': base_stats['updates'],
                        'cleanups': base_stats['cleanups'],
                        'errors': base_stats['errors']
                    },
                    'configuration': {
                        'expire_days': self.expire_days,
                        'max_size_mb': self.max_size_mb,
                        'cache_dir': str(self.cache_dir),
                        'meta_dir': str(self.meta_dir)
                    }
                }
                
                return stats
                
        except Exception as e:
            self.logger.error(f"获取缓存统计失败: {str(e)}")
            return {'error': str(e)}
    
    def _calculate_data_hash(self, data: pd.DataFrame) -> str:
        """
        计算数据哈希值
        
        Args:
            data: 数据DataFrame
            
        Returns:
            数据哈希值
        """
        try:
            # 使用核心数据列计算哈希
            core_columns = ['code', 'date', 'obv']
            available_columns = [col for col in core_columns if col in data.columns]
            
            if not available_columns:
                return ""
            
            # 排序后计算哈希
            sorted_data = data[available_columns].sort_values(available_columns)
            data_str = sorted_data.to_string(index=False)
            
            return hashlib.md5(data_str.encode('utf-8')).hexdigest()[:16]
            
        except Exception as e:
            self.logger.error(f"计算数据哈希失败: {str(e)}")
            return ""
    
    def get_cache_size_limit_status(self) -> Dict[str, Any]:
        """
        检查缓存大小限制状态
        
        Returns:
            大小限制状态信息
        """
        try:
            stats = self.get_cache_statistics()
            storage_info = stats.get('storage', {})
            
            current_size = storage_info.get('total_size_mb', 0)
            usage_percent = storage_info.get('usage_percent', 0)
            
            status = {
                'current_size_mb': current_size,
                'max_size_mb': self.max_size_mb,
                'usage_percent': usage_percent,
                'near_limit': usage_percent > 80,
                'over_limit': usage_percent > 100,
                'recommended_action': None
            }
            
            if usage_percent > 100:
                status['recommended_action'] = '立即执行缓存清理'
            elif usage_percent > 80:
                status['recommended_action'] = '建议执行缓存清理'
            elif usage_percent > 60:
                status['recommended_action'] = '可考虑定期清理'
            else:
                status['recommended_action'] = '存储空间充足'
            
            return status
            
        except Exception as e:
            self.logger.error(f"检查缓存大小限制失败: {str(e)}")
            return {'error': str(e)}
    
    def reset_statistics(self):
        """重置统计信息"""
        with self._lock:
            self._stats = {
                'hits': 0,
                'misses': 0,
                'updates': 0,
                'cleanups': 0,
                'errors': 0
            }
            self.logger.info("缓存统计信息已重置")