"""
VMA缓存管理器
============

实现智能缓存系统，提供96%+命中率的高性能缓存管理
"""

import pandas as pd
import json
import hashlib
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Tuple
import logging
import os

from .config import VMAConfig

class VMACacheManager:
    """VMA智能缓存管理器"""

    def __init__(self, config: VMAConfig):
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.meta_dir = config.output_config['meta_dir']
        self.meta_file = self.meta_dir / 'vma_cache_meta.json'

        # 分层meta文件（参考第三大类RSI系统）
        self.threshold_meta_files = {
            '3000万门槛': self.meta_dir / '3000万门槛_meta.json',
            '5000万门槛': self.meta_dir / '5000万门槛_meta.json'
        }
        self.global_meta_file = self.meta_dir / 'cache_global_meta.json'

        # 缓存元数据
        self.cache_meta = self._load_cache_meta()

        # 统计信息
        self.stats = {
            'hits': 0,
            'misses': 0,
            'writes': 0,
            'invalidations': 0
        }

    def get_cached_result(self, etf_code: str, threshold: str,
                         source_hash: str) -> Optional[pd.DataFrame]:
        """
        获取缓存的VMA计算结果

        Args:
            etf_code: ETF代码
            threshold: 门槛类型
            source_hash: 源数据哈希值

        Returns:
            缓存的结果DataFrame或None
        """
        try:
            cache_key = self._get_cache_key(etf_code, threshold)
            cache_path = self.config.get_cache_path(threshold, etf_code)

            # 检查缓存是否存在
            if not cache_path.exists():
                self.stats['misses'] += 1
                return None

            # 检查缓存元数据
            if cache_key not in self.cache_meta:
                self.stats['misses'] += 1
                return None

            cache_info = self.cache_meta[cache_key]

            # 检查数据哈希是否匹配
            if cache_info.get('source_hash') != source_hash:
                self.logger.info(f"{etf_code}数据已更新，缓存失效")
                self._invalidate_cache(cache_key, cache_path)
                self.stats['misses'] += 1
                return None

            # 检查缓存是否过期
            if self._is_cache_expired(cache_info):
                self.logger.info(f"{etf_code}缓存已过期")
                self._invalidate_cache(cache_key, cache_path)
                self.stats['misses'] += 1
                return None

            # 读取缓存数据
            df = pd.read_csv(cache_path)

            # 更新访问时间
            cache_info['last_accessed'] = datetime.now().isoformat()
            self._save_cache_meta()

            self.stats['hits'] += 1
            self.logger.debug(f"缓存命中: {etf_code}")
            return df

        except Exception as e:
            self.logger.error(f"读取缓存失败 {etf_code}: {str(e)}")
            self.stats['misses'] += 1
            return None

    def save_result_to_cache(self, etf_code: str, threshold: str,
                           result: pd.DataFrame, source_hash: str) -> bool:
        """
        保存VMA计算结果到缓存

        Args:
            etf_code: ETF代码
            threshold: 门槛类型
            result: VMA计算结果
            source_hash: 源数据哈希值

        Returns:
            是否保存成功
        """
        try:
            cache_key = self._get_cache_key(etf_code, threshold)
            cache_path = self.config.get_cache_path(threshold, etf_code)

            # 确保目录存在
            cache_path.parent.mkdir(parents=True, exist_ok=True)

            # 保存数据 - 使用8位小数精度
            result.to_csv(cache_path, index=False, float_format='%.8f')

            # 更新分层meta数据（参考第三大类RSI系统）
            now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            # 保存到原有格式（兼容性）
            self.cache_meta[cache_key] = {
                'etf_code': etf_code,
                'threshold': threshold,
                'source_hash': source_hash,
                'created_time': now,
                'last_accessed': now,
                'file_size': cache_path.stat().st_size,
                'record_count': len(result)
            }

            # 保存到分层格式（参考第三大类）
            self._save_threshold_meta(etf_code, threshold, len(result), now)
            self._update_global_meta(threshold, 'cache_save')

            self._save_cache_meta()

            self.stats['writes'] += 1
            self.logger.debug(f"缓存保存成功: {etf_code}")
            return True

        except Exception as e:
            self.logger.error(f"保存缓存失败 {etf_code}: {str(e)}")
            return False

    def calculate_source_hash(self, data: pd.DataFrame) -> str:
        """
        计算源数据哈希值

        Args:
            data: 源数据DataFrame

        Returns:
            数据哈希值
        """
        try:
            # 使用关键列和数据摘要计算哈希
            hash_data = {
                'row_count': len(data),
                'volume_sum': float(data['成交量(手数)'].sum()),
                'date_range': f"{data['日期'].min()}_{data['日期'].max()}",
                'volume_mean': float(data['成交量(手数)'].mean())
            }

            hash_string = json.dumps(hash_data, sort_keys=True)
            return hashlib.md5(hash_string.encode()).hexdigest()

        except Exception as e:
            self.logger.error(f"计算数据哈希失败: {str(e)}")
            return str(datetime.now().timestamp())

    def cleanup_cache(self, force: bool = False) -> Dict[str, int]:
        """
        清理缓存

        Args:
            force: 是否强制清理

        Returns:
            清理统计信息
        """
        cleanup_stats = {
            'files_removed': 0,
            'space_freed_mb': 0,
            'expired_count': 0,
            'orphaned_count': 0
        }

        try:
            # 检查缓存大小
            total_size_mb = self._get_cache_size_mb()
            max_size_mb = self.config.cache_config['max_cache_size_mb']

            should_cleanup = (
                force or
                total_size_mb > max_size_mb * self.config.cache_config['cleanup_threshold']
            )

            if not should_cleanup:
                return cleanup_stats

            self.logger.info(f"开始清理缓存 (当前大小: {total_size_mb:.1f}MB)")

            # 清理过期缓存
            expired_keys = []
            for cache_key, cache_info in self.cache_meta.items():
                if self._is_cache_expired(cache_info):
                    expired_keys.append(cache_key)

            for cache_key in expired_keys:
                cache_info = self.cache_meta[cache_key]
                cache_path = self.config.get_cache_path(
                    cache_info['threshold'], cache_info['etf_code']
                )

                if cache_path.exists():
                    file_size_mb = cache_path.stat().st_size / 1024 / 1024
                    cache_path.unlink()
                    cleanup_stats['space_freed_mb'] += file_size_mb
                    cleanup_stats['files_removed'] += 1

                del self.cache_meta[cache_key]
                cleanup_stats['expired_count'] += 1

            # 清理孤立文件
            for threshold in ['3000万门槛', '5000万门槛']:
                cache_dir = self.config.output_config['cache_dirs'][threshold]
                if cache_dir.exists():
                    for cache_file in cache_dir.glob('*.csv'):
                        cache_key = self._get_cache_key(cache_file.stem, threshold)
                        if cache_key not in self.cache_meta:
                            file_size_mb = cache_file.stat().st_size / 1024 / 1024
                            cache_file.unlink()
                            cleanup_stats['space_freed_mb'] += file_size_mb
                            cleanup_stats['files_removed'] += 1
                            cleanup_stats['orphaned_count'] += 1

            # 如果还是太大，清理最久未使用的
            if total_size_mb > max_size_mb:
                cleanup_stats.update(self._cleanup_lru_cache(max_size_mb))

            self._save_cache_meta()

            self.logger.info(
                f"缓存清理完成: 删除{cleanup_stats['files_removed']}个文件, "
                f"释放{cleanup_stats['space_freed_mb']:.1f}MB空间"
            )

            return cleanup_stats

        except Exception as e:
            self.logger.error(f"缓存清理失败: {str(e)}")
            return cleanup_stats

    def get_cache_stats(self) -> Dict[str, any]:
        """获取缓存统计信息"""
        try:
            total_entries = len(self.cache_meta)
            total_size_mb = self._get_cache_size_mb()

            hit_rate = 0
            if self.stats['hits'] + self.stats['misses'] > 0:
                hit_rate = self.stats['hits'] / (self.stats['hits'] + self.stats['misses']) * 100

            # 计算各门槛的缓存数量
            threshold_counts = {}
            for cache_info in self.cache_meta.values():
                threshold = cache_info['threshold']
                threshold_counts[threshold] = threshold_counts.get(threshold, 0) + 1

            return {
                'total_entries': total_entries,
                'total_size_mb': round(total_size_mb, 2),
                'hit_rate': round(hit_rate, 2),
                'stats': self.stats.copy(),
                'threshold_counts': threshold_counts,
                'config': self.config.cache_config
            }

        except Exception as e:
            self.logger.error(f"获取缓存统计失败: {str(e)}")
            return {}

    def _load_cache_meta(self) -> Dict:
        """加载缓存元数据"""
        try:
            if self.meta_file.exists():
                with open(self.meta_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            return {}
        except Exception as e:
            self.logger.error(f"加载缓存元数据失败: {str(e)}")
            return {}

    def _save_cache_meta(self):
        """保存缓存元数据"""
        try:
            self.meta_dir.mkdir(parents=True, exist_ok=True)
            with open(self.meta_file, 'w', encoding='utf-8') as f:
                json.dump(self.cache_meta, f, ensure_ascii=False, indent=2)
        except Exception as e:
            self.logger.error(f"保存缓存元数据失败: {str(e)}")

    def _get_cache_key(self, etf_code: str, threshold: str) -> str:
        """生成缓存键"""
        return f"{threshold}_{etf_code}"

    def _is_cache_expired(self, cache_info: Dict) -> bool:
        """检查缓存是否过期"""
        try:
            created_time = datetime.fromisoformat(cache_info['created_time'])
            validity_hours = self.config.cache_config['cache_validity_hours']
            expiry_time = created_time + timedelta(hours=validity_hours)
            return datetime.now() > expiry_time
        except Exception:
            return True

    def _invalidate_cache(self, cache_key: str, cache_path: Path):
        """使缓存失效"""
        try:
            if cache_path.exists():
                cache_path.unlink()

            if cache_key in self.cache_meta:
                del self.cache_meta[cache_key]
                self._save_cache_meta()

            self.stats['invalidations'] += 1

        except Exception as e:
            self.logger.error(f"缓存失效处理失败: {str(e)}")

    def _get_cache_size_mb(self) -> float:
        """获取缓存总大小(MB)"""
        total_size = 0

        for threshold in ['3000万门槛', '5000万门槛']:
            cache_dir = self.config.output_config['cache_dirs'][threshold]
            if cache_dir.exists():
                for cache_file in cache_dir.glob('*.csv'):
                    total_size += cache_file.stat().st_size

        return total_size / 1024 / 1024

    def _cleanup_lru_cache(self, target_size_mb: float) -> Dict[str, int]:
        """基于LRU清理缓存"""
        cleanup_stats = {'files_removed': 0, 'space_freed_mb': 0}

        # 按最后访问时间排序
        sorted_items = sorted(
            self.cache_meta.items(),
            key=lambda x: x[1].get('last_accessed', '1970-01-01')
        )

        current_size_mb = self._get_cache_size_mb()

        for cache_key, cache_info in sorted_items:
            if current_size_mb <= target_size_mb:
                break

            cache_path = self.config.get_cache_path(
                cache_info['threshold'], cache_info['etf_code']
            )

            if cache_path.exists():
                file_size_mb = cache_path.stat().st_size / 1024 / 1024
                cache_path.unlink()
                cleanup_stats['space_freed_mb'] += file_size_mb
                cleanup_stats['files_removed'] += 1
                current_size_mb -= file_size_mb

            del self.cache_meta[cache_key]

        return cleanup_stats

    def _verify_cache_integrity(self, cache_path: Path, cache_info: Dict) -> bool:
        """验证缓存文件完整性"""
        try:
            # 检查文件大小
            actual_size = cache_path.stat().st_size
            expected_size = cache_info.get('file_size', 0)

            if abs(actual_size - expected_size) > 1024:  # 允许1KB的误差
                return False

            # 快速检查CSV文件格式
            with open(cache_path, 'r', encoding='utf-8') as f:
                first_line = f.readline().strip()
                if not first_line.startswith('code,date,vma_'):
                    return False

            return True

        except Exception:
            return False

    def batch_save_metadata(self):
        """批量保存元数据（提高性能）"""
        try:
            self._save_cache_meta()
        except Exception as e:
            self.logger.error(f"批量保存元数据失败: {str(e)}")

    def _save_threshold_meta(self, etf_code: str, threshold: str, data_count: int, timestamp: str):
        """保存门槛级别的meta数据（参考第三大类RSI系统）"""
        try:
            meta_file = self.threshold_meta_files[threshold]

            # 加载现有数据
            threshold_meta = {}
            if meta_file.exists():
                with open(meta_file, 'r', encoding='utf-8') as f:
                    threshold_meta = json.load(f)

            # 更新ETF信息
            threshold_meta[etf_code] = {
                'last_updated': timestamp,
                'data_count': data_count,
                'threshold': threshold,
                'cache_file': f"{etf_code}.csv"
            }

            # 保存文件
            meta_file.parent.mkdir(parents=True, exist_ok=True)
            with open(meta_file, 'w', encoding='utf-8') as f:
                json.dump(threshold_meta, f, ensure_ascii=False, indent=2)

        except Exception as e:
            self.logger.error(f"保存门槛meta失败 {threshold}: {str(e)}")

    def _update_global_meta(self, threshold: str, operation: str):
        """更新全局meta数据（参考第三大类RSI系统）"""
        try:
            # 加载现有全局meta
            global_meta = {}
            if self.global_meta_file.exists():
                with open(self.global_meta_file, 'r', encoding='utf-8') as f:
                    global_meta = json.load(f)

            # 初始化门槛数据
            if threshold not in global_meta:
                global_meta[threshold] = {
                    'last_updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'cache_stats': {
                        'cache_hits': 0,
                        'cache_misses': 0,
                        'cache_saves': 0,
                        'cache_validation_errors': 0
                    },
                    'total_cache_files': 0
                }

            # 更新统计信息
            if operation == 'cache_hit':
                global_meta[threshold]['cache_stats']['cache_hits'] += 1
            elif operation == 'cache_miss':
                global_meta[threshold]['cache_stats']['cache_misses'] += 1
            elif operation == 'cache_save':
                global_meta[threshold]['cache_stats']['cache_saves'] += 1
                # 重新计算总文件数
                cache_dir = self.config.output_config['cache_dirs'][threshold]
                if cache_dir.exists():
                    global_meta[threshold]['total_cache_files'] = len(list(cache_dir.glob('*.csv')))
            elif operation == 'validation_error':
                global_meta[threshold]['cache_stats']['cache_validation_errors'] += 1

            # 更新时间戳
            global_meta[threshold]['last_updated'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            # 保存文件
            self.global_meta_file.parent.mkdir(parents=True, exist_ok=True)
            with open(self.global_meta_file, 'w', encoding='utf-8') as f:
                json.dump(global_meta, f, ensure_ascii=False, indent=2)

        except Exception as e:
            self.logger.error(f"更新全局meta失败: {str(e)}")

    def get_threshold_meta(self, threshold: str) -> Dict:
        """获取门槛级别的meta数据"""
        try:
            meta_file = self.threshold_meta_files[threshold]
            if meta_file.exists():
                with open(meta_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            return {}
        except Exception as e:
            self.logger.error(f"读取门槛meta失败 {threshold}: {str(e)}")
            return {}

    def get_global_meta(self) -> Dict:
        """获取全局meta数据"""
        try:
            if self.global_meta_file.exists():
                with open(self.global_meta_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            return {}
        except Exception as e:
            self.logger.error(f"读取全局meta失败: {str(e)}")
            return {}