"""
VMA结果处理器
=============

负责VMA计算结果的后处理、格式化和验证
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional
import logging
from datetime import datetime

from ..infrastructure.config import VMAConfig
from ..infrastructure.utils import VMAUtils

class VMAResultProcessor:
    """VMA结果处理器"""

    def __init__(self, config: VMAConfig):
        self.config = config
        self.logger = logging.getLogger(__name__)

        # 定义输出字段和精度 - 统一8位小数
        self.output_schema = {
            'vma_5': {'type': 'float', 'precision': 8},
            'vma_10': {'type': 'float', 'precision': 8},
            'vma_20': {'type': 'float', 'precision': 8},
            'volume_ratio_5': {'type': 'float', 'precision': 8},
            'volume_ratio_10': {'type': 'float', 'precision': 8},
            'volume_ratio_20': {'type': 'float', 'precision': 8},
            'volume_trend_short': {'type': 'float', 'precision': 8},
            'volume_trend_medium': {'type': 'float', 'precision': 8},
            'volume_change_rate': {'type': 'float', 'precision': 8},
            'volume_activity_score': {'type': 'float', 'precision': 8}
        }

    def process_result(self, raw_result: pd.DataFrame, etf_code: str) -> pd.DataFrame:
        """
        处理原始VMA计算结果

        Args:
            raw_result: 原始计算结果
            etf_code: ETF代码

        Returns:
            处理后的结果DataFrame
        """
        try:
            if raw_result is None or raw_result.empty:
                self.logger.warning(f"ETF {etf_code} 原始结果为空")
                return pd.DataFrame()

            # 复制数据避免修改原始数据
            result = raw_result.copy()

            # 1. 标准化数据格式
            result = self._standardize_format(result, etf_code)

            # 2. 应用精度格式化
            result = self._apply_precision(result)

            # 3. 数据质量检查和清理
            result = self._quality_check_and_clean(result, etf_code)

            # 4. 添加元数据
            result = self._add_metadata(result, etf_code)

            # 5. 排序和索引重置
            result = self._finalize_result(result)

            self.logger.debug(f"ETF {etf_code} 结果处理完成: {len(result)}条记录")
            return result

        except Exception as e:
            self.logger.error(f"处理结果失败 {etf_code}: {str(e)}")
            return pd.DataFrame()

    def _standardize_format(self, df: pd.DataFrame, etf_code: str) -> pd.DataFrame:
        """标准化数据格式"""
        try:
            # 确保必要列存在
            if 'code' not in df.columns:
                df['code'] = etf_code

            if 'date' not in df.columns:
                self.logger.error(f"ETF {etf_code} 缺少date列")
                return df

            # 标准化日期格式
            df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')

            # 确保code列为字符串
            df['code'] = df['code'].astype(str)

            return df

        except Exception as e:
            self.logger.error(f"标准化格式失败 {etf_code}: {str(e)}")
            return df

    def _apply_precision(self, df: pd.DataFrame) -> pd.DataFrame:
        """应用精度格式化"""
        try:
            for column, schema in self.output_schema.items():
                if column in df.columns:
                    precision = schema['precision']

                    # 处理数值列
                    df[column] = df[column].apply(
                        lambda x: VMAUtils.format_number(x, precision)
                    )

            return df

        except Exception as e:
            self.logger.error(f"应用精度格式化失败: {str(e)}")
            return df

    def _quality_check_and_clean(self, df: pd.DataFrame, etf_code: str) -> pd.DataFrame:
        """数据质量检查和清理"""
        try:
            original_count = len(df)

            # 1. 移除全部为NaN的行
            df = df.dropna(how='all', subset=list(self.output_schema.keys()))

            # 2. 检查VMA值的合理性(非负数)
            vma_columns = ['vma_5', 'vma_10', 'vma_20']
            for col in vma_columns:
                if col in df.columns:
                    # 移除负数(VMA不应该为负)
                    negative_mask = df[col] < 0
                    if negative_mask.any():
                        self.logger.warning(f"ETF {etf_code} {col}列发现{negative_mask.sum()}个负值")
                        df.loc[negative_mask, col] = np.nan

            # 3. 检查比率值的合理性
            ratio_columns = ['volume_ratio_5', 'volume_ratio_10', 'volume_ratio_20']
            for col in ratio_columns:
                if col in df.columns:
                    # 移除异常大的比率值(>100倍)
                    extreme_mask = df[col] > 100
                    if extreme_mask.any():
                        self.logger.warning(f"ETF {etf_code} {col}列发现{extreme_mask.sum()}个极端值")
                        df.loc[extreme_mask, col] = np.nan

            # 4. 检查趋势比率合理性
            trend_columns = ['volume_trend_short', 'volume_trend_medium']
            for col in trend_columns:
                if col in df.columns:
                    # 移除异常的趋势比率(>10倍或<0.1倍)
                    extreme_mask = (df[col] > 10) | (df[col] < 0.1)
                    if extreme_mask.any():
                        self.logger.warning(f"ETF {etf_code} {col}列发现{extreme_mask.sum()}个极端趋势比率")
                        df.loc[extreme_mask, col] = np.nan

            # 5. 检查活跃度得分合理性(0-100)
            if 'volume_activity_score' in df.columns:
                invalid_mask = (df['volume_activity_score'] < 0) | (df['volume_activity_score'] > 100)
                if invalid_mask.any():
                    self.logger.warning(f"ETF {etf_code} volume_activity_score发现{invalid_mask.sum()}个无效值")
                    df.loc[invalid_mask, 'volume_activity_score'] = np.nan

            cleaned_count = len(df)
            if original_count != cleaned_count:
                self.logger.info(f"ETF {etf_code} 数据清理: {original_count} -> {cleaned_count}")

            return df

        except Exception as e:
            self.logger.error(f"数据质量检查失败 {etf_code}: {str(e)}")
            return df

    def _add_metadata(self, df: pd.DataFrame, etf_code: str) -> pd.DataFrame:
        """添加元数据"""
        try:
            # 添加计算时间戳
            if 'calc_time' not in df.columns:
                df['calc_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            return df

        except Exception as e:
            self.logger.error(f"添加元数据失败 {etf_code}: {str(e)}")
            return df

    def _finalize_result(self, df: pd.DataFrame) -> pd.DataFrame:
        """最终化结果"""
        try:
            # 按日期排序
            if 'date' in df.columns:
                df = df.sort_values('date').reset_index(drop=True)

            # 确保列顺序
            column_order = ['code', 'date'] + list(self.output_schema.keys()) + ['calc_time']

            # 只保留存在的列
            available_columns = [col for col in column_order if col in df.columns]
            df = df[available_columns]

            return df

        except Exception as e:
            self.logger.error(f"最终化结果失败: {str(e)}")
            return df

    def validate_result(self, result: pd.DataFrame, etf_code: str) -> Dict[str, Any]:
        """
        验证结果质量

        Args:
            result: VMA计算结果
            etf_code: ETF代码

        Returns:
            验证报告
        """
        try:
            report = {
                'valid': True,
                'etf_code': etf_code,
                'record_count': len(result),
                'column_count': len(result.columns),
                'issues': []
            }

            if result.empty:
                report['valid'] = False
                report['issues'].append('结果为空')
                return report

            # 检查必要列
            required_columns = ['code', 'date'] + list(self.output_schema.keys())
            missing_columns = [col for col in required_columns if col not in result.columns]

            if missing_columns:
                report['valid'] = False
                report['issues'].append(f'缺少必要列: {missing_columns}')

            # 检查数据完整性
            for column in self.output_schema.keys():
                if column in result.columns:
                    null_count = result[column].isnull().sum()
                    null_ratio = null_count / len(result) * 100

                    if null_ratio > 50:  # 超过50%为空视为异常
                        report['issues'].append(f'{column}列空值比例过高: {null_ratio:.1f}%')

                    # 检查数据范围
                    if column.startswith('vma_'):
                        negative_count = (result[column] < 0).sum()
                        if negative_count > 0:
                            report['issues'].append(f'{column}列存在{negative_count}个负值')

                    elif column.startswith('volume_ratio_'):
                        extreme_count = (result[column] > 10).sum()
                        if extreme_count > len(result) * 0.1:  # 超过10%的数据异常大
                            report['issues'].append(f'{column}列存在过多极端值({extreme_count}个)')

            # 检查日期连续性
            if 'date' in result.columns:
                dates = pd.to_datetime(result['date']).sort_values()
                date_gaps = dates.diff().dt.days
                large_gaps = (date_gaps > 7).sum()  # 超过7天的间隔

                if large_gaps > 0:
                    report['issues'].append(f'存在{large_gaps}个较大的日期间隔')

            # 综合评估
            if len(report['issues']) > 3:
                report['valid'] = False

            return report

        except Exception as e:
            return {
                'valid': False,
                'etf_code': etf_code,
                'error': str(e)
            }

    def generate_summary_stats(self, result: pd.DataFrame, etf_code: str) -> Dict[str, Any]:
        """
        生成结果统计摘要

        Args:
            result: VMA计算结果
            etf_code: ETF代码

        Returns:
            统计摘要
        """
        try:
            if result.empty:
                return {'etf_code': etf_code, 'empty': True}

            summary = {
                'etf_code': etf_code,
                'record_count': len(result),
                'date_range': {
                    'start': result['date'].min(),
                    'end': result['date'].max()
                },
                'indicators': {}
            }

            # 生成各指标统计
            numeric_columns = list(self.output_schema.keys())
            summary['indicators'] = VMAUtils.create_summary_stats(result, numeric_columns)

            return summary

        except Exception as e:
            return {
                'etf_code': etf_code,
                'error': str(e)
            }

    def compare_results(self, result1: pd.DataFrame, result2: pd.DataFrame,
                       etf_code: str) -> Dict[str, Any]:
        """
        比较两个VMA结果

        Args:
            result1: 第一个结果
            result2: 第二个结果
            etf_code: ETF代码

        Returns:
            比较报告
        """
        try:
            comparison = {
                'etf_code': etf_code,
                'consistent': True,
                'differences': []
            }

            # 基本信息比较
            if len(result1) != len(result2):
                comparison['consistent'] = False
                comparison['differences'].append({
                    'type': 'record_count',
                    'result1': len(result1),
                    'result2': len(result2)
                })

            # 列比较
            cols1 = set(result1.columns)
            cols2 = set(result2.columns)

            if cols1 != cols2:
                comparison['consistent'] = False
                comparison['differences'].append({
                    'type': 'columns',
                    'missing_in_result1': list(cols2 - cols1),
                    'missing_in_result2': list(cols1 - cols2)
                })

            # 数据一致性比较
            if not result1.empty and not result2.empty:
                common_columns = list(cols1 & cols2)
                if 'date' in common_columns:
                    consistency_report = VMAUtils.validate_data_consistency(
                        result1, result2,
                        [col for col in common_columns if col in self.output_schema]
                    )

                    if not consistency_report['consistent']:
                        comparison['consistent'] = False
                        comparison['differences'].append({
                            'type': 'data_values',
                            'details': consistency_report['key_differences']
                        })

            return comparison

        except Exception as e:
            return {
                'etf_code': etf_code,
                'error': str(e),
                'consistent': False
            }