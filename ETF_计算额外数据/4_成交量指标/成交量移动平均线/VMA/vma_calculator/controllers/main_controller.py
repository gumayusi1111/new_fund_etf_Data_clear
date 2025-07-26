"""
VMA主控制器
===========

VMA系统的主要控制器，协调各个模块的工作
"""

import logging
from typing import Dict, List, Optional, Any
from datetime import datetime

from ..engines.vma_engine import VMAEngine
from ..infrastructure.config import VMAConfig
from ..infrastructure.data_reader import VMADataReader
from ..infrastructure.cache_manager import VMACacheManager
from ..infrastructure.file_manager import VMAFileManager
from ..outputs.result_processor import VMAResultProcessor
from .batch_processor import VMABatchProcessor
from .etf_processor import VMAETFProcessor

class VMAController:
    """VMA系统主控制器"""

    def __init__(self):
        # 初始化配置
        self.config = VMAConfig()

        # 设置日志
        self.logger = self._setup_logging()

        # 初始化各个组件
        self.engine = VMAEngine(self.config)
        self.data_reader = VMADataReader(self.config)
        self.cache_manager = VMACacheManager(self.config)
        self.file_manager = VMAFileManager(self.config)
        self.result_processor = VMAResultProcessor(self.config)

        # 初始化处理器
        self.batch_processor = VMABatchProcessor(
            self.config, self.engine, self.data_reader,
            self.cache_manager, self.file_manager, self.result_processor
        )
        self.etf_processor = VMAETFProcessor(
            self.config, self.engine, self.data_reader,
            self.cache_manager, self.file_manager, self.result_processor
        )

        # 确保目录存在
        self.file_manager.ensure_directories()

        self.logger.info("VMA系统初始化完成")

    def calculate_single_etf(self, etf_code: str, threshold: str,
                           force_recalculate: bool = False) -> Dict[str, Any]:
        """
        计算单个ETF的VMA指标

        Args:
            etf_code: ETF代码
            threshold: 门槛类型
            force_recalculate: 是否强制重新计算

        Returns:
            计算结果字典
        """
        try:
            self.logger.info(f"开始计算单个ETF VMA指标: {etf_code} ({threshold})")

            result = self.etf_processor.process_single_etf(
                etf_code, threshold, force_recalculate
            )

            if result['success']:
                self.logger.info(f"ETF {etf_code} VMA计算成功")
            else:
                self.logger.error(f"ETF {etf_code} VMA计算失败: {result.get('error', '未知错误')}")

            return result

        except Exception as e:
            error_msg = f"计算单个ETF失败 {etf_code}: {str(e)}"
            self.logger.error(error_msg)
            return {
                'success': False,
                'error': error_msg,
                'etf_code': etf_code,
                'threshold': threshold
            }

    def calculate_batch_etfs(self, threshold: str, etf_codes: Optional[List[str]] = None,
                           force_recalculate: bool = False,
                           max_workers: int = 4) -> Dict[str, Any]:
        """
        批量计算ETF的VMA指标

        Args:
            threshold: 门槛类型
            etf_codes: ETF代码列表，None则计算所有
            force_recalculate: 是否强制重新计算
            max_workers: 最大并行工作线程数

        Returns:
            批量计算结果
        """
        try:
            self.logger.info(f"开始批量计算VMA指标: {threshold}")

            if etf_codes is None:
                etf_codes = self.config.get_etf_list(threshold)

            self.logger.info(f"待计算ETF数量: {len(etf_codes)}")

            result = self.batch_processor.process_batch(
                etf_codes, threshold, force_recalculate, max_workers
            )

            # 输出批量处理摘要
            self._log_batch_summary(result)

            return result

        except Exception as e:
            error_msg = f"批量计算失败 {threshold}: {str(e)}"
            self.logger.error(error_msg)
            return {
                'success': False,
                'error': error_msg,
                'threshold': threshold,
                'processed_count': 0,
                'failed_count': 0
            }

    def test_system(self, sample_size: int = 5) -> Dict[str, Any]:
        """
        测试VMA系统

        Args:
            sample_size: 测试样本数量

        Returns:
            测试结果
        """
        try:
            self.logger.info(f"开始VMA系统测试 (样本数: {sample_size})")

            test_results = {
                'success': True,
                'test_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'tests': {}
            }

            # 测试配置
            test_results['tests']['config'] = self._test_config()

            # 测试数据读取
            test_results['tests']['data_reader'] = self._test_data_reader(sample_size)

            # 测试计算引擎
            test_results['tests']['engine'] = self._test_engine(sample_size)

            # 测试缓存系统
            test_results['tests']['cache'] = self._test_cache_system()

            # 测试文件管理
            test_results['tests']['file_manager'] = self._test_file_manager()

            # 综合评估
            all_passed = all(
                test['passed'] for test in test_results['tests'].values()
            )
            test_results['success'] = all_passed

            status = "通过" if all_passed else "失败"
            self.logger.info(f"VMA系统测试完成: {status}")

            return test_results

        except Exception as e:
            error_msg = f"系统测试失败: {str(e)}"
            self.logger.error(error_msg)
            return {
                'success': False,
                'error': error_msg,
                'test_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }

    def get_system_status(self) -> Dict[str, Any]:
        """获取系统状态信息"""
        try:
            status = {
                'system_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'config': self.config.to_dict(),
                'cache_stats': self.cache_manager.get_cache_stats(),
                'storage_usage': self.file_manager.get_storage_usage(),
                'etf_counts': {}
            }

            # 获取各门槛ETF数量
            for threshold in ['3000万门槛', '5000万门槛']:
                etf_list = self.config.get_etf_list(threshold)
                status['etf_counts'][threshold] = len(etf_list)

            return status

        except Exception as e:
            self.logger.error(f"获取系统状态失败: {str(e)}")
            return {'error': str(e)}

    def cleanup_cache(self, force: bool = False) -> Dict[str, Any]:
        """清理缓存"""
        try:
            self.logger.info("开始清理VMA缓存")
            cleanup_stats = self.cache_manager.cleanup_cache(force)
            self.logger.info("VMA缓存清理完成")
            return cleanup_stats
        except Exception as e:
            error_msg = f"缓存清理失败: {str(e)}"
            self.logger.error(error_msg)
            return {'error': error_msg}

    def _setup_logging(self) -> logging.Logger:
        """设置日志系统"""
        logger = logging.getLogger('VMAController')

        if not logger.handlers:
            # 创建日志目录
            log_file = self.config.logging_config['file_path']
            log_file.parent.mkdir(parents=True, exist_ok=True)

            # 配置日志处理器
            handler = logging.FileHandler(log_file, encoding='utf-8')
            formatter = logging.Formatter(self.config.logging_config['format'])
            handler.setFormatter(formatter)

            # 添加控制台输出
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(formatter)

            logger.addHandler(handler)
            logger.addHandler(console_handler)
            logger.setLevel(self.config.logging_config['level'])

        return logger

    def _log_batch_summary(self, result: Dict[str, Any]):
        """记录批量处理摘要"""
        if result.get('success', False):
            self.logger.info(
                f"批量计算完成 - 成功: {result.get('processed_count', 0)}个, "
                f"失败: {result.get('failed_count', 0)}个, "
                f"耗时: {result.get('total_time', 0):.2f}秒"
            )
        else:
            self.logger.error(f"批量计算失败: {result.get('error', '未知错误')}")

    def _test_config(self) -> Dict[str, Any]:
        """测试配置"""
        try:
            # 检查关键配置
            assert len(self.config.vma_periods) > 0
            assert len(self.config.ratio_periods) > 0
            assert self.config.activity_window > 0

            return {
                'passed': True,
                'message': '配置测试通过',
                'details': {
                    'periods_count': len(self.config.vma_periods),
                    'ratios_count': len(self.config.ratio_periods)
                }
            }
        except Exception as e:
            return {
                'passed': False,
                'message': f'配置测试失败: {str(e)}'
            }

    def _test_data_reader(self, sample_size: int) -> Dict[str, Any]:
        """测试数据读取"""
        try:
            # 获取测试样本
            etf_list = self.config.get_etf_list('3000万门槛')[:sample_size]

            success_count = 0
            for etf_code in etf_list:
                data = self.data_reader.read_etf_data(etf_code, '3000万门槛')
                if data is not None and not data.empty:
                    success_count += 1

            success_rate = success_count / len(etf_list) * 100 if etf_list else 0

            return {
                'passed': success_rate >= 80,  # 80%成功率通过
                'message': f'数据读取测试: {success_rate:.1f}%成功率',
                'details': {
                    'tested_count': len(etf_list),
                    'success_count': success_count,
                    'success_rate': success_rate
                }
            }
        except Exception as e:
            return {
                'passed': False,
                'message': f'数据读取测试失败: {str(e)}'
            }

    def _test_engine(self, sample_size: int) -> Dict[str, Any]:
        """测试计算引擎"""
        try:
            # 获取测试数据
            etf_list = self.config.get_etf_list('3000万门槛')[:sample_size]

            if not etf_list:
                return {
                    'passed': False,
                    'message': '没有可用的测试数据'
                }

            test_etf = etf_list[0]
            data = self.data_reader.read_etf_data(test_etf, '3000万门槛')

            if data is None or data.empty:
                return {
                    'passed': False,
                    'message': '无法读取测试数据'
                }

            # 测试VMA计算
            result = self.engine.calculate_vma_indicators(data)

            # 验证结果
            expected_columns = [
                'vma_5', 'vma_10', 'vma_20',
                'volume_ratio_5', 'volume_ratio_10', 'volume_ratio_20',
                'volume_trend_short', 'volume_trend_medium',
                'volume_change_rate', 'volume_activity_score'
            ]

            missing_columns = [col for col in expected_columns if col not in result.columns]
            has_data = len(result) > 0

            return {
                'passed': len(missing_columns) == 0 and has_data,
                'message': f'计算引擎测试: {"通过" if len(missing_columns) == 0 and has_data else "失败"}',
                'details': {
                    'output_columns': len(result.columns),
                    'output_rows': len(result),
                    'missing_columns': missing_columns
                }
            }
        except Exception as e:
            return {
                'passed': False,
                'message': f'计算引擎测试失败: {str(e)}'
            }

    def _test_cache_system(self) -> Dict[str, Any]:
        """测试缓存系统"""
        try:
            # 获取缓存统计
            cache_stats = self.cache_manager.get_cache_stats()

            return {
                'passed': True,
                'message': '缓存系统正常',
                'details': cache_stats
            }
        except Exception as e:
            return {
                'passed': False,
                'message': f'缓存系统测试失败: {str(e)}'
            }

    def _test_file_manager(self) -> Dict[str, Any]:
        """测试文件管理"""
        try:
            # 测试目录创建
            self.file_manager.ensure_directories()

            # 测试存储使用情况
            storage_usage = self.file_manager.get_storage_usage()

            return {
                'passed': True,
                'message': '文件管理系统正常',
                'details': storage_usage
            }
        except Exception as e:
            return {
                'passed': False,
                'message': f'文件管理测试失败: {str(e)}'
            }