"""
OBV指标主控制器 - 系统协调中心
============================

统一管理OBV指标计算的所有核心功能
协调计算引擎、缓存管理、数据读取等模块

核心功能:
- 单ETF和批量ETF计算
- 智能缓存管理和增量更新
- 系统状态监控和统计
- 错误处理和恢复
- 性能优化和并发控制

技术特点:
- 高内聚低耦合的架构设计
- 线程安全的并发处理
- 全面的异常处理和日志记录
- 可配置的性能参数
"""

import os
import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd

from ..engines.obv_engine import OBVEngine
from ..infrastructure.cache_manager import OBVCacheManager
from ..infrastructure.data_reader import OBVDataReader
from ..infrastructure.config import OBVConfig
from ..outputs.csv_handler import OBVCSVHandler

class OBVController:
    """OBV指标主控制器"""
    
    def __init__(self, config: Optional[OBVConfig] = None):
        """
        初始化主控制器
        
        Args:
            config: 配置对象，None则使用默认配置
        """
        self.config = config or OBVConfig()
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # 初始化核心组件
        self._init_components()
        
        # 并发控制
        self._thread_lock = threading.RLock()
        self._executor = None
        
        # 系统统计
        self._stats = {
            'calculations': 0,
            'cache_hits': 0,
            'cache_misses': 0,
            'errors': 0,
            'total_processing_time': 0.0,
            'start_time': datetime.now()
        }
        
        self.logger.info("OBV主控制器初始化完成")
    
    def _init_components(self):
        """初始化核心组件"""
        try:
            # 计算引擎
            self.engine = OBVEngine(precision=self.config.OBV_CONFIG['precision'])
            
            # 数据读取器
            self.data_reader = OBVDataReader(
                source_dir=self.config.ETF_SOURCE_DIR,
                encoding='utf-8'
            )
            
            # 缓存管理器(按门槛分别初始化)
            self.cache_managers = {}
            for threshold in self.config.get_supported_thresholds():
                cache_dir = self.config.get_cache_dir(threshold)
                self.cache_managers[threshold] = OBVCacheManager(
                    cache_dir=cache_dir,
                    meta_dir=self.config.CACHE_META_DIR,
                    expire_days=self.config.CACHE_CONFIG['expire_days'],
                    max_size_mb=self.config.CACHE_CONFIG['max_cache_size_mb']
                )
            
            # CSV输出处理器(按门槛分别初始化)
            self.csv_handlers = {}
            for threshold in self.config.get_supported_thresholds():
                data_dir = self.config.get_data_dir(threshold)
                self.csv_handlers[threshold] = OBVCSVHandler(
                    output_dir=data_dir,
                    precision=self.config.OBV_CONFIG['precision']
                )
            
            self.logger.info("核心组件初始化完成")
            
        except Exception as e:
            self.logger.error(f"组件初始化失败: {str(e)}")
            raise
    
    def calculate_single_etf(self, etf_code: str, threshold: str,
                           force_recalculate: bool = False) -> Dict[str, Any]:
        """
        计算单个ETF的OBV指标
        
        Args:
            etf_code: ETF代码
            threshold: 门槛类型
            force_recalculate: 是否强制重新计算
            
        Returns:
            计算结果字典
        """
        try:
            start_time = datetime.now()
            
            # 验证参数
            if not self._validate_parameters(etf_code, threshold):
                return {'success': False, 'error': '参数验证失败'}
            
            cache_manager = self.cache_managers[threshold]
            csv_handler = self.csv_handlers[threshold]
            
            # 检查缓存(除非强制重新计算)
            if not force_recalculate:
                cached_data = cache_manager.load_cache(etf_code, threshold)
                if cached_data is not None:
                    self._stats['cache_hits'] += 1
                    
                    # 保存到输出目录
                    output_path = csv_handler.save_etf_data(etf_code, cached_data)
                    
                    processing_time = (datetime.now() - start_time).total_seconds()
                    
                    return {
                        'success': True,
                        'etf_code': etf_code,
                        'threshold': threshold,
                        'cache_hit': True,
                        'data_points': len(cached_data),
                        'output_path': str(output_path),
                        'processing_time': processing_time
                    }
            
            self._stats['cache_misses'] += 1
            
            # 读取源数据
            source_data = self.data_reader.read_etf_data(etf_code)
            if source_data is None or source_data.empty:
                return {'success': False, 'error': f'无法读取ETF数据: {etf_code}'}
            
            # 应用门槛筛选
            if threshold in self.config.THRESHOLD_CONFIG:
                filtered_data = self._apply_threshold_filter(source_data, threshold)
                if filtered_data.empty:
                    return {'success': False, 'error': f'数据不满足{threshold}要求'}
                source_data = filtered_data
            
            # 执行OBV计算
            calculation_result = self.engine.calculate_obv_batch(source_data)
            
            if not calculation_result['success']:
                self._stats['errors'] += 1
                return {
                    'success': False,
                    'error': f"OBV计算失败: {calculation_result.get('error', '未知错误')}"
                }
            
            calculated_data = calculation_result['data']
            
            # 保存到缓存
            cache_manager.save_cache(etf_code, threshold, calculated_data)
            
            # 保存到输出目录
            output_path = csv_handler.save_etf_data(etf_code, calculated_data)
            
            # 更新统计
            processing_time = (datetime.now() - start_time).total_seconds()
            self._stats['calculations'] += 1
            self._stats['total_processing_time'] += processing_time
            
            return {
                'success': True,
                'etf_code': etf_code,
                'threshold': threshold,
                'cache_hit': False,
                'data_points': len(calculated_data),
                'output_path': str(output_path),
                'processing_time': processing_time,
                'calculation_stats': calculation_result.get('statistics', {})
            }
            
        except Exception as e:
            self.logger.error(f"单ETF计算异常 {etf_code}_{threshold}: {str(e)}")
            self._stats['errors'] += 1
            return {'success': False, 'error': str(e)}
    
    def calculate_batch_etfs(self, threshold: str, 
                           etf_codes: Optional[List[str]] = None,
                           force_recalculate: bool = False,
                           max_workers: int = 4) -> Dict[str, Any]:
        """
        批量计算ETF的OBV指标
        
        Args:
            threshold: 门槛类型
            etf_codes: ETF代码列表，None则计算所有可用ETF
            force_recalculate: 是否强制重新计算
            max_workers: 最大并行线程数
            
        Returns:
            批量计算结果
        """
        try:
            start_time = datetime.now()
            
            # 确定要处理的ETF列表
            if etf_codes is None:
                # 发现所有可用ETF
                availability_check = self.data_reader.check_data_availability(
                    self.data_reader._discover_available_etfs()
                )
                target_etfs = availability_check['available_codes']
            else:
                target_etfs = etf_codes
            
            if not target_etfs:
                return {'success': False, 'error': '没有可处理的ETF数据'}
            
            self.logger.info(f"开始批量计算 {threshold} - ETF数量: {len(target_etfs)}")
            
            # 并行处理
            results = []
            errors = []
            
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # 提交所有任务
                future_to_etf = {
                    executor.submit(
                        self.calculate_single_etf, 
                        etf_code, 
                        threshold, 
                        force_recalculate
                    ): etf_code
                    for etf_code in target_etfs
                }
                
                # 收集结果
                for future in as_completed(future_to_etf):
                    etf_code = future_to_etf[future]
                    try:
                        result = future.result(timeout=60)  # 60秒超时
                        if result['success']:
                            results.append(result)
                        else:
                            errors.append({
                                'etf_code': etf_code,
                                'error': result.get('error', '未知错误')
                            })
                    except Exception as e:
                        errors.append({
                            'etf_code': etf_code,
                            'error': f'计算异常: {str(e)}'
                        })
            
            # 统计结果
            total_time = (datetime.now() - start_time).total_seconds()
            success_count = len(results)
            error_count = len(errors)
            total_count = len(target_etfs)
            success_rate = (success_count / total_count * 100) if total_count > 0 else 0
            
            # 计算汇总统计
            total_data_points = sum(r.get('data_points', 0) for r in results)
            cache_hit_count = sum(1 for r in results if r.get('cache_hit', False))
            cache_hit_rate = (cache_hit_count / success_count * 100) if success_count > 0 else 0
            
            batch_result = {
                'success': success_count > 0,
                'threshold': threshold,
                'total_count': total_count,
                'success_count': success_count,
                'error_count': error_count,
                'success_rate': success_rate,
                'total_time': total_time,
                'avg_time_per_etf': total_time / total_count if total_count > 0 else 0,
                'total_data_points': total_data_points,
                'cache_hit_rate': cache_hit_rate,
                'force_recalculate': force_recalculate,
                'max_workers': max_workers
            }
            
            if errors:
                batch_result['errors'] = errors[:10]  # 最多返回10个错误示例
                batch_result['total_errors'] = len(errors)
            
            self.logger.info(f"批量计算完成 {threshold} - 成功: {success_count}/{total_count} "
                          f"({success_rate:.1f}%), 耗时: {total_time:.2f}秒")
            
            return batch_result
            
        except Exception as e:
            self.logger.error(f"批量计算异常 {threshold}: {str(e)}")
            return {'success': False, 'error': str(e)}
    
    def calculate_incremental_update(self, etf_code: str, threshold: str) -> Dict[str, Any]:
        """
        执行增量更新计算
        
        Args:
            etf_code: ETF代码
            threshold: 门槛类型
            
        Returns:
            增量更新结果
        """
        try:
            start_time = datetime.now()
            
            cache_manager = self.cache_managers[threshold]
            
            # 加载现有缓存
            existing_data = cache_manager.load_cache(etf_code, threshold)
            if existing_data is None or existing_data.empty:
                # 没有缓存，执行全量计算
                return self.calculate_single_etf(etf_code, threshold, False)
            
            # 获取最后更新日期
            last_date = existing_data['date'].max()
            
            # 获取增量数据
            incremental_source = self.data_reader.get_incremental_data(etf_code, last_date)
            if incremental_source is None or incremental_source.empty:
                return {
                    'success': True,
                    'etf_code': etf_code,
                    'threshold': threshold,
                    'incremental': True,
                    'message': '没有新数据需要更新',
                    'processing_time': (datetime.now() - start_time).total_seconds()
                }
            
            # 应用门槛筛选
            if threshold in self.config.THRESHOLD_CONFIG:
                filtered_incremental = self._apply_threshold_filter(incremental_source, threshold)
                if filtered_incremental.empty:
                    return {
                        'success': True,
                        'etf_code': etf_code,
                        'threshold': threshold,
                        'incremental': True,
                        'message': f'增量数据不满足{threshold}要求',
                        'processing_time': (datetime.now() - start_time).total_seconds()
                    }
                incremental_source = filtered_incremental
            
            # 执行增量OBV计算
            incremental_result = self.engine.calculate_obv_incremental(
                existing_data, incremental_source
            )
            
            if not incremental_result['success']:
                return {
                    'success': False,
                    'error': f"增量计算失败: {incremental_result.get('error', '未知错误')}"
                }
            
            new_data = incremental_result['data']
            
            # 更新缓存
            cache_manager.update_cache_incremental(etf_code, threshold, new_data)
            
            # 更新输出文件
            csv_handler = self.csv_handlers[threshold]
            output_path = csv_handler.update_etf_data_incremental(etf_code, new_data)
            
            processing_time = (datetime.now() - start_time).total_seconds()
            
            return {
                'success': True,
                'etf_code': etf_code,
                'threshold': threshold,
                'incremental': True,
                'new_data_points': len(new_data),
                'output_path': str(output_path),
                'processing_time': processing_time
            }
            
        except Exception as e:
            self.logger.error(f"增量更新异常 {etf_code}_{threshold}: {str(e)}")
            return {'success': False, 'error': str(e)}
    
    def test_system(self, sample_size: int = 5) -> Dict[str, Any]:
        """
        系统功能测试
        
        Args:
            sample_size: 测试样本数量
            
        Returns:
            测试结果
        """
        try:
            test_results = {
                'success': True,
                'tests': {},
                'summary': {},
                'errors': []
            }
            
            # 测试1: 组件初始化测试
            test_results['tests']['component_init'] = self._test_component_initialization()
            
            # 测试2: 数据读取测试
            test_results['tests']['data_reading'] = self._test_data_reading(sample_size)
            
            # 测试3: 计算引擎测试
            test_results['tests']['calculation_engine'] = self._test_calculation_engine(sample_size)
            
            # 测试4: 缓存系统测试
            test_results['tests']['cache_system'] = self._test_cache_system(sample_size)
            
            # 测试5: 输出系统测试
            test_results['tests']['output_system'] = self._test_output_system(sample_size)
            
            # 汇总测试结果
            passed_tests = sum(1 for test in test_results['tests'].values() if test.get('passed', False))
            total_tests = len(test_results['tests'])
            
            test_results['summary'] = {
                'total_tests': total_tests,
                'passed_tests': passed_tests,
                'failed_tests': total_tests - passed_tests,
                'success_rate': (passed_tests / total_tests * 100) if total_tests > 0 else 0,
                'overall_status': 'PASS' if passed_tests == total_tests else 'FAIL'
            }
            
            test_results['success'] = (passed_tests == total_tests)
            
            return test_results
            
        except Exception as e:
            self.logger.error(f"系统测试异常: {str(e)}")
            return {'success': False, 'error': str(e)}
    
    def get_system_status(self) -> Dict[str, Any]:
        """
        获取系统状态
        
        Returns:
            系统状态信息
        """
        try:
            # 基础系统信息
            uptime = datetime.now() - self._stats['start_time']
            
            status = {
                'system_info': {
                    'name': 'OBV指标计算系统',
                    'version': '1.0.0',
                    'status': 'RUNNING',
                    'uptime_seconds': int(uptime.total_seconds()),
                    'start_time': self._stats['start_time'].isoformat()
                },
                'performance': {
                    'total_calculations': self._stats['calculations'],
                    'cache_hit_rate': (self._stats['cache_hits'] / 
                                     (self._stats['cache_hits'] + self._stats['cache_misses']) * 100) 
                                     if (self._stats['cache_hits'] + self._stats['cache_misses']) > 0 else 0,
                    'average_processing_time': (self._stats['total_processing_time'] / 
                                              self._stats['calculations']) 
                                              if self._stats['calculations'] > 0 else 0,
                    'error_rate': (self._stats['errors'] / 
                                 (self._stats['calculations'] + self._stats['errors']) * 100) 
                                 if (self._stats['calculations'] + self._stats['errors']) > 0 else 0
                },
                'components': {},
                'storage': {}
            }
            
            # 组件状态
            status['components']['engine'] = {
                'status': 'ACTIVE',
                'precision': self.engine.precision
            }
            
            status['components']['data_reader'] = {
                'status': 'ACTIVE',
                'source_dir': str(self.data_reader.source_dir),
                'source_exists': self.data_reader.source_dir.exists()
            }
            
            # 缓存系统状态
            cache_stats = {}
            for threshold, cache_manager in self.cache_managers.items():
                cache_stats[threshold] = cache_manager.get_cache_statistics()
            status['components']['cache_managers'] = cache_stats
            
            # 存储状态
            storage_info = {}
            for threshold in self.config.get_supported_thresholds():
                data_dir = self.config.get_data_dir(threshold)
                cache_dir = self.config.get_cache_dir(threshold)
                
                storage_info[threshold] = {
                    'data_dir': str(data_dir),
                    'cache_dir': str(cache_dir),
                    'data_files': len(list(data_dir.glob("*.csv"))) if data_dir.exists() else 0,
                    'cache_files': len(list(cache_dir.glob("*.csv"))) if cache_dir.exists() else 0
                }
            
            status['storage'] = storage_info
            
            return status
            
        except Exception as e:
            self.logger.error(f"获取系统状态异常: {str(e)}")
            return {'error': str(e)}
    
    def cleanup_system(self, force: bool = False) -> Dict[str, Any]:
        """
        系统清理操作
        
        Args:
            force: 是否强制清理
            
        Returns:
            清理结果
        """
        try:
            cleanup_results = {
                'success': True,
                'operations': {},
                'summary': {}
            }
            
            total_files_removed = 0
            total_space_freed = 0.0
            
            # 清理所有门槛的缓存
            for threshold, cache_manager in self.cache_managers.items():
                cache_cleanup = cache_manager.cleanup_expired_cache(force)
                
                cleanup_results['operations'][f'{threshold}_cache'] = cache_cleanup
                
                if 'files_removed' in cache_cleanup:
                    total_files_removed += cache_cleanup['files_removed']
                    total_space_freed += cache_cleanup.get('space_freed_mb', 0)
            
            # 重置统计信息
            if force:
                self._stats = {
                    'calculations': 0,
                    'cache_hits': 0,
                    'cache_misses': 0,
                    'errors': 0,
                    'total_processing_time': 0.0,
                    'start_time': datetime.now()
                }
                cleanup_results['operations']['stats_reset'] = {'success': True}
            
            cleanup_results['summary'] = {
                'total_files_removed': total_files_removed,
                'total_space_freed_mb': total_space_freed,
                'force_cleanup': force
            }
            
            self.logger.info(f"系统清理完成 - 删除文件: {total_files_removed}, "
                          f"释放空间: {total_space_freed:.1f}MB")
            
            return cleanup_results
            
        except Exception as e:
            self.logger.error(f"系统清理异常: {str(e)}")
            return {'success': False, 'error': str(e)}
    
    def _validate_parameters(self, etf_code: str, threshold: str) -> bool:
        """验证输入参数"""
        if not etf_code or not isinstance(etf_code, str):
            self.logger.error("ETF代码无效")
            return False
        
        if not self.config.validate_threshold(threshold):
            self.logger.error(f"不支持的门槛类型: {threshold}")
            return False
        
        return True
    
    def _apply_threshold_filter(self, data: pd.DataFrame, threshold: str) -> pd.DataFrame:
        """应用成交额门槛筛选"""
        return self.data_reader._apply_threshold_filter(data, threshold)
    
    def _test_component_initialization(self) -> Dict[str, Any]:
        """测试组件初始化"""
        try:
            test_result = {'passed': True, 'details': {}}
            
            # 测试各组件是否正常初始化
            test_result['details']['engine'] = self.engine is not None
            test_result['details']['data_reader'] = self.data_reader is not None
            test_result['details']['cache_managers'] = len(self.cache_managers) > 0
            test_result['details']['csv_handlers'] = len(self.csv_handlers) > 0
            
            test_result['passed'] = all(test_result['details'].values())
            
            return test_result
            
        except Exception as e:
            return {'passed': False, 'error': str(e)}
    
    def _test_data_reading(self, sample_size: int) -> Dict[str, Any]:
        """测试数据读取功能"""
        try:
            # 获取可用ETF列表
            available_etfs = self.data_reader._discover_available_etfs()
            
            if not available_etfs:
                return {'passed': False, 'error': '没有可用的ETF数据'}
            
            # 随机选择样本进行测试
            import random
            test_etfs = random.sample(available_etfs, min(sample_size, len(available_etfs)))
            
            success_count = 0
            for etf_code in test_etfs:
                data = self.data_reader.read_etf_data(etf_code)
                if data is not None and not data.empty:
                    success_count += 1
            
            success_rate = success_count / len(test_etfs) * 100
            
            return {
                'passed': success_rate >= 80,  # 80%以上成功率算通过
                'details': {
                    'tested_etfs': len(test_etfs),
                    'successful_reads': success_count,
                    'success_rate': success_rate
                }
            }
            
        except Exception as e:
            return {'passed': False, 'error': str(e)}
    
    def _test_calculation_engine(self, sample_size: int) -> Dict[str, Any]:
        """测试计算引擎功能"""
        try:
            # 使用一个ETF的数据测试计算
            available_etfs = self.data_reader._discover_available_etfs()
            if not available_etfs:
                return {'passed': False, 'error': '没有可用的ETF数据进行测试'}
            
            test_etf = available_etfs[0]
            test_data = self.data_reader.read_etf_data(test_etf)
            
            if test_data is None or test_data.empty:
                return {'passed': False, 'error': '无法获取测试数据'}
            
            # 执行计算测试
            result = self.engine.calculate_obv_batch(test_data)
            
            return {
                'passed': result['success'],
                'details': {
                    'test_etf': test_etf,
                    'input_records': len(test_data),
                    'output_records': len(result.get('data', [])) if result.get('success') else 0,
                    'processing_time': result.get('processing_time', 0)
                }
            }
            
        except Exception as e:
            return {'passed': False, 'error': str(e)}
    
    def _test_cache_system(self, sample_size: int) -> Dict[str, Any]:
        """测试缓存系统功能"""
        try:
            # 测试第一个门槛的缓存管理器
            threshold = list(self.cache_managers.keys())[0]
            cache_manager = self.cache_managers[threshold]
            
            # 获取缓存统计
            stats = cache_manager.get_cache_statistics()
            
            return {
                'passed': 'error' not in stats,
                'details': {
                    'threshold_tested': threshold,
                    'cache_files': stats.get('storage', {}).get('cache_files', 0),
                    'hit_rate': stats.get('performance', {}).get('hit_rate', 0)
                }
            }
            
        except Exception as e:
            return {'passed': False, 'error': str(e)}
    
    def _test_output_system(self, sample_size: int) -> Dict[str, Any]:
        """测试输出系统功能"""
        try:
            # 测试CSV输出处理器
            threshold = list(self.csv_handlers.keys())[0]
            csv_handler = self.csv_handlers[threshold]
            
            # 创建测试数据
            test_data = pd.DataFrame({
                'code': ['159001'],
                'date': ['2025-07-27'],
                'obv': [1000000.12345678],
                'obv_ma10': [950000.12345678],
                'obv_change_5': [5.67],
                'obv_change_20': [-2.34],
                'calc_time': ['2025-07-27 12:00:00']
            })
            
            # 测试保存功能
            output_path = csv_handler.save_etf_data('TEST001', test_data)
            
            # 验证文件是否创建
            file_exists = output_path.exists() if output_path else False
            
            # 清理测试文件
            if file_exists and output_path:
                try:
                    output_path.unlink()
                except:
                    pass
            
            return {
                'passed': file_exists,
                'details': {
                    'threshold_tested': threshold,
                    'test_file_created': file_exists,
                    'output_path': str(output_path) if output_path else None
                }
            }
            
        except Exception as e:
            return {'passed': False, 'error': str(e)}
    
    def __del__(self):
        """析构函数，清理资源"""
        try:
            if hasattr(self, '_executor') and self._executor:
                self._executor.shutdown(wait=False)
        except:
            pass