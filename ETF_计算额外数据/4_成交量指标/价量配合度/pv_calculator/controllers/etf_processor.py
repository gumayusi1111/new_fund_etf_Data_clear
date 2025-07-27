"""
PV单ETF处理器
==============

负责单个ETF的PV计算处理逻辑
"""

import logging
import time
from typing import Dict, Any, Optional
from datetime import datetime

from ..engines.pv_engine import PVEngine
from ..infrastructure.config import PVConfig
from ..infrastructure.data_reader import PVDataReader
from ..infrastructure.cache_manager import PVCacheManager
from ..infrastructure.file_manager import PVFileManager
from ..outputs.result_processor import PVResultProcessor

class PVETFProcessor:
    """PV单ETF处理器"""

    def __init__(self, config: PVConfig, engine: PVEngine,
                 data_reader: PVDataReader, cache_manager: PVCacheManager,
                 file_manager: PVFileManager, result_processor: PVResultProcessor):
        self.config = config
        self.engine = engine
        self.data_reader = data_reader
        self.cache_manager = cache_manager
        self.file_manager = file_manager
        self.result_processor = result_processor
        self.logger = logging.getLogger(__name__)

    def process_single_etf(self, etf_code: str, threshold: str,
                          force_recalculate: bool = False) -> Dict[str, Any]:
        """
        处理单个ETF的PV计算

        Args:
            etf_code: ETF代码
            threshold: 门槛类型
            force_recalculate: 是否强制重新计算

        Returns:
            处理结果字典
        """
        start_time = time.time()

        result = {
            'success': False,
            'etf_code': etf_code,
            'threshold': threshold,
            'start_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'cache_hit': False,
            'data_quality': {},
            'calculation_details': {},
            'output_info': {}
        }

        try:
            self.logger.info(f"开始处理ETF: {etf_code} ({threshold})")

            # 步骤1: 数据读取和验证
            source_data = self._load_and_validate_data(etf_code, threshold)
            if source_data is None:
                result['error'] = '数据读取失败或数据质量不合格'
                return result

            result['data_quality'] = self.engine.validate_data_quality(source_data)

            # 步骤2: 缓存检查
            if not force_recalculate:
                cached_result = self._check_cache(etf_code, threshold, source_data)
                if cached_result is not None:
                    result.update(self._handle_cache_hit(etf_code, threshold, cached_result))
                    result['processing_time'] = time.time() - start_time
                    return result

            # 步骤3: PV计算
            pv_result = self._calculate_pv(etf_code, source_data)
            if pv_result is None:
                result['error'] = 'PV计算失败'
                return result

            result['calculation_details'] = self._get_calculation_details(pv_result)

            # 步骤4: 结果保存
            save_success = self._save_results(etf_code, threshold, pv_result, source_data)
            if not save_success:
                result['error'] = '结果保存失败'
                return result

            # 步骤5: 输出信息整理
            result['output_info'] = self._get_output_info(etf_code, threshold, pv_result)

            # 完成处理
            result['success'] = True
            result['processing_time'] = time.time() - start_time

            self.logger.info(
                f"ETF {etf_code} 处理完成: {len(pv_result)}条记录, "
                f"耗时 {result['processing_time']:.3f}秒"
            )

            return result

        except Exception as e:
            error_msg = f"处理ETF {etf_code} 时发生异常: {str(e)}"
            self.logger.error(error_msg)
            result['error'] = error_msg
            result['processing_time'] = time.time() - start_time
            return result

    def _load_and_validate_data(self, etf_code: str, threshold: str) -> Optional[Any]:
        """加载和验证数据"""
        try:
            # 读取源数据
            source_data = self.data_reader.read_etf_data(etf_code, threshold)

            if source_data is None or source_data.empty:
                self.logger.warning(f"ETF {etf_code} 源数据为空")
                return None

            # 数据质量验证
            quality_report = self.engine.validate_data_quality(source_data)

            if not quality_report.get('valid', False):
                self.logger.warning(
                    f"ETF {etf_code} 数据质量不合格: {quality_report.get('error', '未知错误')}"
                )
                return None

            self.logger.debug(f"ETF {etf_code} 数据验证通过: {len(source_data)}条记录")
            return source_data

        except Exception as e:
            self.logger.error(f"加载验证数据失败 {etf_code}: {str(e)}")
            return None

    def _check_cache(self, etf_code: str, threshold: str, source_data: Any) -> Optional[Any]:
        """检查缓存"""
        try:
            # 计算源数据哈希
            source_hash = self.cache_manager.calculate_source_hash(source_data)

            # 获取缓存结果
            cached_result = self.cache_manager.get_cached_result(
                etf_code, threshold, source_hash
            )

            if cached_result is not None and not cached_result.empty:
                self.logger.debug(f"ETF {etf_code} 缓存命中")
                return cached_result

            return None

        except Exception as e:
            self.logger.error(f"检查缓存失败 {etf_code}: {str(e)}")
            return None

    def _handle_cache_hit(self, etf_code: str, threshold: str, cached_result: Any) -> Dict[str, Any]:
        """处理缓存命中情况"""
        try:
            # 保存到输出目录
            save_success = self.file_manager.save_pv_result(
                etf_code, threshold, cached_result
            )

            return {
                'success': save_success,
                'cache_hit': True,
                'calculation_details': {
                    'source': 'cache',
                    'record_count': len(cached_result)
                },
                'output_info': self._get_output_info(etf_code, threshold, cached_result),
                'error': '保存缓存结果失败' if not save_success else None
            }

        except Exception as e:
            return {
                'success': False,
                'cache_hit': True,
                'error': f'处理缓存结果失败: {str(e)}'
            }

    def _calculate_pv(self, etf_code: str, source_data: Any) -> Optional[Any]:
        """计算PV指标"""
        try:
            self.logger.debug(f"开始计算ETF {etf_code} PV指标")

            pv_result = self.engine.calculate_pv_indicators(source_data)

            if pv_result is None or pv_result.empty:
                self.logger.error(f"ETF {etf_code} PV计算结果为空")
                return None

            # 验证输出字段
            expected_columns = [
                'pv_corr_10', 'pv_corr_20', 'pv_corr_30',
                'vpt', 'vpt_momentum', 'vpt_ratio',
                'volume_quality', 'volume_consistency',
                'pv_strength', 'pv_divergence'
            ]

            missing_columns = [col for col in expected_columns if col not in pv_result.columns]
            if missing_columns:
                self.logger.warning(f"ETF {etf_code} 缺少输出字段: {missing_columns}")

            self.logger.debug(f"ETF {etf_code} PV计算完成: {len(pv_result)}条记录")
            return pv_result

        except Exception as e:
            self.logger.error(f"计算PV失败 {etf_code}: {str(e)}")
            return None

    def _save_results(self, etf_code: str, threshold: str, pv_result: Any,
                     source_data: Any) -> bool:
        """保存计算结果"""
        try:
            # 保存PV结果到输出目录
            save_success = self.file_manager.save_pv_result(
                etf_code, threshold, pv_result
            )

            if not save_success:
                self.logger.error(f"保存PV结果失败: {etf_code}")
                return False

            # 保存到缓存
            source_hash = self.cache_manager.calculate_source_hash(source_data)
            cache_success = self.cache_manager.save_result_to_cache(
                etf_code, threshold, pv_result, source_hash
            )

            if cache_success:
                self.logger.debug(f"ETF {etf_code} 结果已保存到缓存")
            else:
                self.logger.warning(f"ETF {etf_code} 缓存保存失败")

            return True

        except Exception as e:
            self.logger.error(f"保存结果失败 {etf_code}: {str(e)}")
            return False

    def _get_calculation_details(self, pv_result: Any) -> Dict[str, Any]:
        """获取计算详情"""
        try:
            # 基本统计信息
            details = {
                'source': 'calculation',
                'record_count': len(pv_result),
                'date_range': {
                    'start': pv_result['date'].min(),  # 已经是字符串格式
                    'end': pv_result['date'].max()     # 已经是字符串格式
                },
                'columns_count': len(pv_result.columns)
            }

            # PV相关性指标统计
            corr_columns = ['pv_corr_10', 'pv_corr_20', 'pv_corr_30']
            for col in corr_columns:
                if col in pv_result.columns:
                    series = pv_result[col].dropna()
                    if len(series) > 0:
                        details[f'{col}_stats'] = {
                            'mean': round(series.mean(), 4),
                            'max': round(series.max(), 4),
                            'min': round(series.min(), 4),
                            'std': round(series.std(), 4)
                        }

            # VPT指标统计
            vpt_columns = ['vpt', 'vpt_momentum', 'vpt_ratio']
            for col in vpt_columns:
                if col in pv_result.columns:
                    series = pv_result[col].dropna()
                    if len(series) > 0:
                        details[f'{col}_stats'] = {
                            'mean': round(series.mean(), 2),
                            'max': round(series.max(), 2),
                            'min': round(series.min(), 2),
                            'std': round(series.std(), 2)
                        }

            # 成交量质量指标统计
            quality_columns = ['volume_quality', 'volume_consistency']
            for col in quality_columns:
                if col in pv_result.columns:
                    series = pv_result[col].dropna()
                    if len(series) > 0:
                        details[f'{col}_stats'] = {
                            'mean': round(series.mean(), 4),
                            'max': round(series.max(), 4),
                            'min': round(series.min(), 4)
                        }

            return details

        except Exception as e:
            self.logger.error(f"获取计算详情失败: {str(e)}")
            return {'error': str(e)}

    def _get_output_info(self, etf_code: str, threshold: str, pv_result: Any) -> Dict[str, Any]:
        """获取输出信息"""
        try:
            output_path = self.config.get_output_path(threshold, etf_code)

            info = {
                'file_path': str(output_path),
                'file_exists': output_path.exists(),
                'record_count': len(pv_result),
                'columns': list(pv_result.columns),
                'threshold': threshold
            }

            # 文件大小信息
            if output_path.exists():
                file_size = output_path.stat().st_size
                info['file_size_kb'] = round(file_size / 1024, 2)
                info['modified_time'] = datetime.fromtimestamp(
                    output_path.stat().st_mtime
                ).strftime('%Y-%m-%d %H:%M:%S')

            return info

        except Exception as e:
            self.logger.error(f"获取输出信息失败 {etf_code}: {str(e)}")
            return {'error': str(e)}

    def get_etf_status(self, etf_code: str, threshold: str) -> Dict[str, Any]:
        """
        获取ETF的处理状态信息

        Args:
            etf_code: ETF代码
            threshold: 门槛类型

        Returns:
            状态信息字典
        """
        try:
            status = {
                'etf_code': etf_code,
                'threshold': threshold,
                'check_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }

            # 检查文件完整性
            file_integrity = self.file_manager.validate_file_integrity(etf_code, threshold)
            status['file_integrity'] = file_integrity

            # 检查数据信息
            if file_integrity.get('source_readable', False):
                data_info = self.data_reader.get_data_info(etf_code, threshold)
                status['data_info'] = data_info

            # 检查PV结果
            pv_result = self.file_manager.load_pv_result(etf_code, threshold)
            if pv_result is not None:
                status['pv_result'] = {
                    'exists': True,
                    'record_count': len(pv_result),
                    'columns': list(pv_result.columns),
                    'latest_date': pv_result['date'].max().strftime('%Y-%m-%d') if 'date' in pv_result.columns else None
                }
            else:
                status['pv_result'] = {'exists': False}

            return status

        except Exception as e:
            return {
                'etf_code': etf_code,
                'threshold': threshold,
                'error': str(e),
                'check_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }