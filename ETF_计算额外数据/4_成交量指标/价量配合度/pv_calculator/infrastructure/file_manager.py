"""
PV价量配合度系统文件管理器
=========================

负责PV系统的文件操作，包括目录管理、文件路径处理、批量操作等
"""

import pandas as pd
from pathlib import Path
from typing import List, Dict, Optional, Tuple
import logging
import os
import shutil
from datetime import datetime

from .config import PVConfig

class PVFileManager:
    """PV价量配合度系统文件管理器"""

    def __init__(self, config: PVConfig):
        self.config = config
        self.logger = logging.getLogger(__name__)

    def ensure_directories(self):
        """确保所有必要目录存在"""
        try:
            directories = []

            # 数据输出目录
            for threshold_dir in self.config.output_config['data_dirs'].values():
                directories.append(threshold_dir)

            # 缓存目录
            for cache_dir in self.config.output_config['cache_dirs'].values():
                directories.append(cache_dir)

            # Meta目录
            directories.append(self.config.output_config['meta_dir'])

            # 日志目录
            log_dir = self.config.logging_config['file_path'].parent
            directories.append(log_dir)

            # 创建所有目录
            for directory in directories:
                directory.mkdir(parents=True, exist_ok=True)
                self.logger.debug(f"确保目录存在: {directory}")

            self.logger.info(f"目录检查完成，共检查{len(directories)}个目录")

        except Exception as e:
            self.logger.error(f"创建目录失败: {str(e)}")
            raise

    def save_pv_result(self, etf_code: str, threshold: str,
                       result: pd.DataFrame) -> bool:
        """
        保存PV价量配合度计算结果

        Args:
            etf_code: ETF代码
            threshold: 门槛类型
            result: PV计算结果

        Returns:
            是否保存成功
        """
        try:
            output_path = self.config.get_output_path(threshold, etf_code)

            # 确保目录存在
            output_path.parent.mkdir(parents=True, exist_ok=True)

            # 保存CSV文件
            result.to_csv(output_path, index=False, encoding='utf-8')

            self.logger.debug(f"PV结果保存成功: {output_path}")
            return True

        except Exception as e:
            self.logger.error(f"保存PV结果失败 {etf_code}: {str(e)}")
            return False

    def load_pv_result(self, etf_code: str, threshold: str) -> Optional[pd.DataFrame]:
        """
        加载已有的PV价量配合度计算结果

        Args:
            etf_code: ETF代码
            threshold: 门槛类型

        Returns:
            PV结果DataFrame或None
        """
        try:
            output_path = self.config.get_output_path(threshold, etf_code)

            if not output_path.exists():
                return None

            df = pd.read_csv(output_path)
            self.logger.debug(f"PV结果加载成功: {etf_code}")
            return df

        except Exception as e:
            self.logger.error(f"加载PV结果失败 {etf_code}: {str(e)}")
            return None

    def get_etf_file_info(self, threshold: str) -> List[Dict]:
        """
        获取ETF文件信息列表

        Args:
            threshold: 门槛类型

        Returns:
            文件信息列表
        """
        try:
            data_dir = self.config.data_source['data_dirs'][threshold]

            if not data_dir.exists():
                self.logger.warning(f"数据目录不存在: {data_dir}")
                return []

            file_info_list = []

            for csv_file in data_dir.glob("*.csv"):
                try:
                    # 获取文件基本信息
                    stat = csv_file.stat()
                    etf_code = csv_file.stem

                    file_info = {
                        'etf_code': etf_code,
                        'file_path': str(csv_file),
                        'file_size_mb': round(stat.st_size / 1024 / 1024, 2),
                        'modified_time': datetime.fromtimestamp(stat.st_mtime).strftime('%Y-%m-%d %H:%M:%S'),
                        'threshold': threshold
                    }

                    # 检查是否已有PV结果
                    output_path = self.config.get_output_path(threshold, etf_code)
                    file_info['has_pv_result'] = output_path.exists()

                    # 检查是否有缓存
                    cache_path = self.config.get_cache_path(threshold, etf_code)
                    file_info['has_cache'] = cache_path.exists()

                    file_info_list.append(file_info)

                except Exception as e:
                    self.logger.warning(f"获取文件信息失败 {csv_file}: {str(e)}")
                    continue

            # 按ETF代码排序
            file_info_list.sort(key=lambda x: x['etf_code'])

            self.logger.info(f"获取到{len(file_info_list)}个ETF文件信息({threshold})")
            return file_info_list

        except Exception as e:
            self.logger.error(f"获取ETF文件信息失败: {str(e)}")
            return []

    def backup_results(self, threshold: str, backup_dir: Optional[Path] = None) -> bool:
        """
        备份PV价量配合度计算结果

        Args:
            threshold: 门槛类型
            backup_dir: 备份目录，None则使用默认

        Returns:
            是否备份成功
        """
        try:
            if backup_dir is None:
                backup_dir = self.config.output_config['base_dir'] / f"backup/{threshold}"

            source_dir = self.config.output_config['data_dirs'][threshold]

            if not source_dir.exists():
                self.logger.warning(f"源目录不存在: {source_dir}")
                return False

            # 创建备份目录
            backup_dir.mkdir(parents=True, exist_ok=True)

            # 复制文件
            file_count = 0
            for csv_file in source_dir.glob("*.csv"):
                backup_file = backup_dir / csv_file.name
                shutil.copy2(csv_file, backup_file)
                file_count += 1

            self.logger.info(f"备份完成: {file_count}个文件到 {backup_dir}")
            return True

        except Exception as e:
            self.logger.error(f"备份失败: {str(e)}")
            return False

    def clean_old_results(self, threshold: str, days_old: int = 30) -> Dict[str, int]:
        """
        清理旧的PV结果文件

        Args:
            threshold: 门槛类型
            days_old: 多少天前的文件

        Returns:
            清理统计信息
        """
        stats = {'files_removed': 0, 'space_freed_mb': 0}

        try:
            output_dir = self.config.output_config['data_dirs'][threshold]

            if not output_dir.exists():
                return stats

            cutoff_time = datetime.now().timestamp() - (days_old * 24 * 3600)

            for csv_file in output_dir.glob("*.csv"):
                if csv_file.stat().st_mtime < cutoff_time:
                    file_size_mb = csv_file.stat().st_size / 1024 / 1024
                    csv_file.unlink()
                    stats['files_removed'] += 1
                    stats['space_freed_mb'] += file_size_mb

            self.logger.info(
                f"清理旧文件完成: 删除{stats['files_removed']}个文件, "
                f"释放{stats['space_freed_mb']:.1f}MB空间"
            )

            return stats

        except Exception as e:
            self.logger.error(f"清理旧文件失败: {str(e)}")
            return stats

    def get_storage_usage(self) -> Dict[str, Dict[str, float]]:
        """
        获取存储使用情况

        Returns:
            各目录的存储使用情况(MB)
        """
        usage = {}

        try:
            # 数据目录使用情况
            for threshold, data_dir in self.config.output_config['data_dirs'].items():
                if data_dir.exists():
                    total_size = sum(f.stat().st_size for f in data_dir.glob("*.csv"))
                    file_count = len(list(data_dir.glob("*.csv")))
                    usage[f'data_{threshold}'] = {
                        'size_mb': round(total_size / 1024 / 1024, 2),
                        'file_count': file_count
                    }

            # 缓存目录使用情况
            for threshold, cache_dir in self.config.output_config['cache_dirs'].items():
                if cache_dir.exists():
                    total_size = sum(f.stat().st_size for f in cache_dir.glob("*.csv"))
                    file_count = len(list(cache_dir.glob("*.csv")))
                    usage[f'cache_{threshold}'] = {
                        'size_mb': round(total_size / 1024 / 1024, 2),
                        'file_count': file_count
                    }

            return usage

        except Exception as e:
            self.logger.error(f"获取存储使用情况失败: {str(e)}")
            return {}

    def validate_file_integrity(self, etf_code: str, threshold: str) -> Dict[str, bool]:
        """
        验证文件完整性

        Args:
            etf_code: ETF代码
            threshold: 门槛类型

        Returns:
            验证结果
        """
        results = {
            'source_exists': False,
            'source_readable': False,
            'output_exists': False,
            'output_readable': False,
            'cache_exists': False,
            'cache_readable': False
        }

        try:
            # 检查源文件
            source_path = self.config.get_source_path(threshold, etf_code)
            results['source_exists'] = source_path.exists()

            if results['source_exists']:
                try:
                    pd.read_csv(source_path, nrows=1)
                    results['source_readable'] = True
                except Exception:
                    results['source_readable'] = False

            # 检查输出文件
            output_path = self.config.get_output_path(threshold, etf_code)
            results['output_exists'] = output_path.exists()

            if results['output_exists']:
                try:
                    pd.read_csv(output_path, nrows=1)
                    results['output_readable'] = True
                except Exception:
                    results['output_readable'] = False

            # 检查缓存文件
            cache_path = self.config.get_cache_path(threshold, etf_code)
            results['cache_exists'] = cache_path.exists()

            if results['cache_exists']:
                try:
                    pd.read_csv(cache_path, nrows=1)
                    results['cache_readable'] = True
                except Exception:
                    results['cache_readable'] = False

            return results

        except Exception as e:
            self.logger.error(f"验证文件完整性失败 {etf_code}: {str(e)}")
            return results