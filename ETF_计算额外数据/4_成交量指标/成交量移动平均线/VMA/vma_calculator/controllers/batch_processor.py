"""
VMA批处理器
===========

负责批量处理多个ETF的VMA计算
"""

import logging
from typing import Dict, List, Any, Optional
from datetime import datetime
import time
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor

from ..engines.vma_engine import VMAEngine
from ..infrastructure.config import VMAConfig
from ..infrastructure.data_reader import VMADataReader
from ..infrastructure.cache_manager import VMACacheManager
from ..infrastructure.file_manager import VMAFileManager
# Resource monitor and retry manager removed for simplification
from ..outputs.result_processor import VMAResultProcessor

class VMABatchProcessor:
    """VMA批处理器"""

    def __init__(self, config: VMAConfig, engine: VMAEngine,
                 data_reader: VMADataReader, cache_manager: VMACacheManager,
                 file_manager: VMAFileManager, result_processor: VMAResultProcessor):
        self.config = config
        self.engine = engine
        self.data_reader = data_reader
        self.cache_manager = cache_manager
        self.file_manager = file_manager
        self.result_processor = result_processor
        self.logger = logging.getLogger(__name__)

        # Resource management simplified

        # 性能优化配置
        self.performance_config = {
            'adaptive_batch_size': True,      # 自适应批次大小
            'memory_threshold_mb': 1000,      # 内存阈值(MB)
            'cpu_threshold_percent': 75,      # CPU阈值(%)
            'dynamic_worker_scaling': True,   # 动态工作线程缩放
            'prefetch_enabled': True,         # 预取优化
            'result_streaming': True          # 结果流式处理
        }

    def process_batch(self, etf_codes: List[str], threshold: str,
                     force_recalculate: bool = False,
                     max_workers: int = None) -> Dict[str, Any]:
        """
        批量处理ETF的VMA计算

        Args:
            etf_codes: ETF代码列表
            threshold: 门槛类型
            force_recalculate: 是否强制重新计算
            max_workers: 最大并行工作线程数

        Returns:
            批处理结果
        """
        start_time = time.time()

        # Resource monitoring disabled

        try:
            # 资源准备和优化
            total_estimated_size = len(etf_codes) * 1000  # 估算每个ETF 1000行数据
            # Resource preparation disabled

            # 动态调整工作线程数
            if max_workers is None:
                max_workers = self._calculate_optimal_workers(len(etf_codes))

            self.logger.info(f"开始批量处理 {len(etf_codes)} 个ETF ({threshold}), 使用{max_workers}个工作线程")

            # 初始化结果统计
            results = {
                'success': True,
                'threshold': threshold,
                'total_count': len(etf_codes),
                'processed_count': 0,
                'failed_count': 0,
                'skipped_count': 0,
                'start_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'failed_etfs': [],
                'processing_details': []
            }

            # 使用智能批处理策略
            return self._process_with_optimization(etf_codes, threshold, force_recalculate, max_workers, results)

        finally:
            # Resource monitoring disabled
            pass

    def _process_with_optimization(self, etf_codes: List[str], threshold: str,
                                 force_recalculate: bool, max_workers: int,
                                 results: Dict[str, Any]) -> Dict[str, Any]:
        """使用优化策略处理批次"""

        try:
            # 使用标准批处理方法 (自适应批处理功能暂时禁用)
            return self._process_standard_batch(etf_codes, threshold, force_recalculate, max_workers, results)

        except Exception as e:
            error_msg = f"批量处理优化异常: {str(e)}"
            self.logger.error(error_msg)
            results['success'] = False
            results['error'] = error_msg
            return results

    def _process_standard_batch(self, etf_codes: List[str], threshold: str,
                              force_recalculate: bool, max_workers: int,
                              results: Dict[str, Any]) -> Dict[str, Any]:
        """标准批处理方法"""

        batch_start_time = time.time()

        try:
            # 使用线程池并行处理
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # 提交所有任务
                future_to_etf = {
                    executor.submit(
                        self._process_single_etf_worker,
                        etf_code, threshold, force_recalculate
                    ): etf_code
                    for etf_code in etf_codes
                }

                # 收集结果
                for future in concurrent.futures.as_completed(future_to_etf):
                    etf_code = future_to_etf[future]

                    try:
                        timeout_seconds = self.config.get_batch_timeout()
                        result = future.result(timeout=timeout_seconds)

                        if result['success']:
                            results['processed_count'] += 1
                            self.logger.debug(f"✓ {etf_code} 处理成功")
                        elif result.get('skipped', False):
                            results['skipped_count'] += 1
                            self.logger.debug(f"- {etf_code} 已跳过(缓存有效)")
                        else:
                            results['failed_count'] += 1
                            results['failed_etfs'].append({
                                'etf_code': etf_code,
                                'error': result.get('error', '未知错误')
                            })
                            self.logger.warning(f"✗ {etf_code} 处理失败: {result.get('error', '未知错误')}")

                        # 记录处理详情
                        results['processing_details'].append({
                            'etf_code': etf_code,
                            'success': result['success'],
                            'processing_time': result.get('processing_time', 0),
                            'record_count': result.get('record_count', 0),
                            'cache_hit': result.get('cache_hit', False)
                        })

                    except concurrent.futures.TimeoutError:
                        results['failed_count'] += 1
                        results['failed_etfs'].append({
                            'etf_code': etf_code,
                            'error': '处理超时'
                        })
                        self.logger.error(f"✗ {etf_code} 处理超时")

                    except Exception as e:
                        results['failed_count'] += 1
                        results['failed_etfs'].append({
                            'etf_code': etf_code,
                            'error': f'未知错误: {str(e)}'
                        })
                        self.logger.error(f"✗ {etf_code} 处理异常: {str(e)}")

            # 计算总体统计
            batch_end_time = time.time()
            results['total_time'] = round(batch_end_time - batch_start_time, 2)
            results['success_rate'] = round(
                results['processed_count'] / results['total_count'] * 100, 1
            ) if results['total_count'] > 0 else 0

            # 生成批处理报告
            results['summary'] = self._generate_batch_summary(results)

            # 判断整体是否成功(使用配置的成功率阈值)
            success_threshold = self.config.get_success_rate_threshold() * 100  # 转换为百分比
            results['success'] = results['success_rate'] >= success_threshold

            self.logger.info(
                f"批量处理完成: {results['processed_count']}/{results['total_count']} "
                f"成功 ({results['success_rate']}%), "
                f"耗时 {results['total_time']}秒"
            )

            return results

        except Exception as e:
            error_msg = f"批量处理异常: {str(e)}"
            self.logger.error(error_msg)
            results['success'] = False
            results['error'] = error_msg
            return results

    def _process_single_etf_worker(self, etf_code: str, threshold: str,
                                  force_recalculate: bool) -> Dict[str, Any]:
        """
        单个ETF处理工作函数(用于并行处理)

        Args:
            etf_code: ETF代码
            threshold: 门槛类型
            force_recalculate: 是否强制重新计算

        Returns:
            处理结果
        """
        start_time = time.time()

        try:
            # 1. 读取源数据
            source_data = self.data_reader.read_etf_data(etf_code, threshold)

            if source_data is None or source_data.empty:
                return {
                    'success': False,
                    'error': '无法读取源数据或数据为空',
                    'etf_code': etf_code,
                    'processing_time': time.time() - start_time
                }

            # 2. 计算数据哈希
            source_hash = self.cache_manager.calculate_source_hash(source_data)

            # 3. 检查缓存(如果不强制重算)
            cached_result = None
            if not force_recalculate:
                cached_result = self.cache_manager.get_cached_result(
                    etf_code, threshold, source_hash
                )

            if cached_result is not None and not cached_result.empty:
                # 使用缓存结果
                self.file_manager.save_vma_result(etf_code, threshold, cached_result)

                return {
                    'success': True,
                    'skipped': True,
                    'cache_hit': True,
                    'etf_code': etf_code,
                    'record_count': len(cached_result),
                    'processing_time': time.time() - start_time
                }

            # 4. 计算VMA指标
            vma_result = self.engine.calculate_vma_indicators(source_data)

            if vma_result is None or vma_result.empty:
                return {
                    'success': False,
                    'error': 'VMA计算结果为空',
                    'etf_code': etf_code,
                    'processing_time': time.time() - start_time
                }

            # 5. 保存结果
            save_success = self.file_manager.save_vma_result(
                etf_code, threshold, vma_result
            )

            if not save_success:
                return {
                    'success': False,
                    'error': '保存VMA结果失败',
                    'etf_code': etf_code,
                    'processing_time': time.time() - start_time
                }

            # 6. 保存到缓存
            self.cache_manager.save_result_to_cache(
                etf_code, threshold, vma_result, source_hash
            )

            return {
                'success': True,
                'cache_hit': False,
                'etf_code': etf_code,
                'record_count': len(vma_result),
                'processing_time': time.time() - start_time
            }

        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'etf_code': etf_code,
                'processing_time': time.time() - start_time
            }

    def _generate_batch_summary(self, results: Dict[str, Any]) -> Dict[str, Any]:
        """生成批处理摘要"""
        try:
            processing_details = results.get('processing_details', [])

            if not processing_details:
                return {}

            # 计算性能统计
            processing_times = [detail['processing_time'] for detail in processing_details]
            record_counts = [detail['record_count'] for detail in processing_details if detail['record_count'] > 0]
            cache_hits = sum(1 for detail in processing_details if detail.get('cache_hit', False))

            summary = {
                'performance': {
                    'avg_processing_time': round(sum(processing_times) / len(processing_times), 3),
                    'max_processing_time': round(max(processing_times), 3),
                    'min_processing_time': round(min(processing_times), 3),
                    'total_processing_time': round(sum(processing_times), 2)
                },
                'data_stats': {
                    'avg_record_count': int(sum(record_counts) / len(record_counts)) if record_counts else 0,
                    'max_record_count': max(record_counts) if record_counts else 0,
                    'min_record_count': min(record_counts) if record_counts else 0,
                    'total_records': sum(record_counts)
                },
                'cache_stats': {
                    'cache_hits': cache_hits,
                    'cache_hit_rate': round(cache_hits / len(processing_details) * 100, 1) if processing_details else 0
                }
            }

            return summary

        except Exception as e:
            self.logger.error(f"生成批处理摘要失败: {str(e)}")
            return {}

    def process_by_threshold(self, threshold: str, force_recalculate: bool = False,
                           max_workers: int = 4) -> Dict[str, Any]:
        """
        按门槛处理所有ETF

        Args:
            threshold: 门槛类型
            force_recalculate: 是否强制重新计算
            max_workers: 最大并行工作线程数

        Returns:
            处理结果
        """
        try:
            # 获取ETF列表
            etf_codes = self.config.get_etf_list(threshold)

            if not etf_codes:
                return {
                    'success': False,
                    'error': f'没有找到{threshold}的ETF数据',
                    'threshold': threshold
                }

            self.logger.info(f"开始处理{threshold}的所有ETF: {len(etf_codes)}个")

            # 执行批量处理
            result = self.process_batch(etf_codes, threshold, force_recalculate, max_workers)

            return result

        except Exception as e:
            error_msg = f"按门槛处理失败 {threshold}: {str(e)}"
            self.logger.error(error_msg)
            return {
                'success': False,
                'error': error_msg,
                'threshold': threshold
            }

    def process_all_thresholds(self, force_recalculate: bool = False,
                             max_workers: int = 4) -> Dict[str, Any]:
        """
        处理所有门槛的ETF

        Args:
            force_recalculate: 是否强制重新计算
            max_workers: 最大并行工作线程数

        Returns:
            总体处理结果
        """
        start_time = time.time()

        overall_results = {
            'success': True,
            'start_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'thresholds': {},
            'total_summary': {
                'total_etfs': 0,
                'processed_etfs': 0,
                'failed_etfs': 0
            }
        }

        try:
            # 处理各个门槛
            for threshold in ['3000万门槛', '5000万门槛']:
                self.logger.info(f"开始处理 {threshold}")

                threshold_result = self.process_by_threshold(
                    threshold, force_recalculate, max_workers
                )

                overall_results['thresholds'][threshold] = threshold_result

                # 累计统计
                if threshold_result.get('success', False):
                    overall_results['total_summary']['total_etfs'] += threshold_result.get('total_count', 0)
                    overall_results['total_summary']['processed_etfs'] += threshold_result.get('processed_count', 0)
                    overall_results['total_summary']['failed_etfs'] += threshold_result.get('failed_count', 0)
                else:
                    overall_results['success'] = False

                self.logger.info(f"{threshold} 处理完成")

            # 计算总体时间
            overall_results['total_time'] = round(time.time() - start_time, 2)

            # 计算总体成功率
            total_etfs = overall_results['total_summary']['total_etfs']
            processed_etfs = overall_results['total_summary']['processed_etfs']

            overall_results['total_summary']['success_rate'] = round(
                processed_etfs / total_etfs * 100, 1
            ) if total_etfs > 0 else 0

            self.logger.info(
                f"全部门槛处理完成: {processed_etfs}/{total_etfs} 成功, "
                f"耗时 {overall_results['total_time']}秒"
            )

            return overall_results

        except Exception as e:
            error_msg = f"处理所有门槛失败: {str(e)}"
            self.logger.error(error_msg)
            overall_results['success'] = False
            overall_results['error'] = error_msg
            return overall_results

    def _calculate_optimal_workers(self, etf_count: int) -> int:
        """
        计算最优工作线程数

        Args:
            etf_count: ETF总数

        Returns:
            最优工作线程数
        """
        try:
            # 如果没有性能配置，使用默认值
            if not hasattr(self, 'performance_config'):
                return self.config.get_max_workers()

            import psutil

            # 获取系统信息
            cpu_count = psutil.cpu_count()
            memory_gb = psutil.virtual_memory().total / (1024**3)

            # 基于CPU核心数的基础线程数
            base_workers = min(cpu_count, self.config.get_max_workers())

            # 根据内存调整
            if memory_gb < 4:
                memory_factor = 0.5
            elif memory_gb < 8:
                memory_factor = 0.75
            else:
                memory_factor = 1.0

            # 根据ETF数量调整
            if etf_count < 10:
                etf_factor = 0.5
            elif etf_count < 50:
                etf_factor = 1.0
            else:
                etf_factor = 1.2

            # 检查当前系统负载
            cpu_percent = psutil.cpu_percent(interval=1)
            if cpu_percent > self.performance_config.get('cpu_threshold_percent', 75):
                load_factor = 0.7
            else:
                load_factor = 1.0

            optimal_workers = int(base_workers * memory_factor * etf_factor * load_factor)

            # 确保在合理范围内
            optimal_workers = max(1, min(optimal_workers, self.config.get_max_workers(), etf_count))

            self.logger.debug(f"工作线程数优化: CPU核心={cpu_count}, 内存={memory_gb:.1f}GB, "
                             f"ETF数量={etf_count}, 最优线程数={optimal_workers}")

            return optimal_workers

        except ImportError:
            # 如果psutil不可用，使用配置的默认值
            self.logger.warning("psutil不可用，使用配置的默认工作线程数")
            return self.config.get_max_workers()
        except Exception as e:
            self.logger.error(f"计算最优工作线程数失败: {e}")
            return self.config.get_max_workers()

    def get_performance_statistics(self) -> Dict[str, Any]:
        """获取性能统计信息"""
        try:
            stats = {
                'performance_config': getattr(self, 'performance_config', {}),
                'optimization_enabled': {
                    'resource_monitoring': False,
                    'retry_mechanism': hasattr(self, 'retry_manager'),
                    'adaptive_batching': getattr(self, 'performance_config', {}).get('adaptive_batch_size', False),
                    'dynamic_scaling': getattr(self, 'performance_config', {}).get('dynamic_worker_scaling', False)
                }
            }

            # Resource monitoring disabled
            stats['resource_usage'] = {}

            # 添加重试统计（如果可用）
            if hasattr(self, 'retry_manager'):
                try:
                    retry_stats = self.retry_manager.get_retry_statistics()
                    stats['retry_statistics'] = retry_stats
                except Exception as e:
                    self.logger.warning(f"获取重试统计失败: {e}")

            return stats

        except Exception as e:
            self.logger.error(f"获取性能统计失败: {e}")
            return {
                'error': str(e),
                'optimization_enabled': {
                    'resource_monitoring': False,
                    'retry_mechanism': False,
                    'adaptive_batching': False,
                    'dynamic_scaling': False
                }
            }
