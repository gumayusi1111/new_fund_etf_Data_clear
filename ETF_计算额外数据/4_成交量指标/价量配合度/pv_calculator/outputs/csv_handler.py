"""
PV CSV处理器
=============

专门处理PV结果的CSV文件读写操作
"""

import pandas as pd
from pathlib import Path
from typing import Dict, List, Any, Optional
import logging

class PVCSVHandler:
    """PV CSV文件处理器"""

    def __init__(self):
        self.logger = logging.getLogger(__name__)

        # CSV格式配置 - 修复为8位小数精度
        self.csv_config = {
            'encoding': 'utf-8',
            'index': False,
            'float_format': '%.8f'  # 固定8位小数精度
        }

    def save_csv(self, data: pd.DataFrame, file_path: Path,
                header_comment: Optional[str] = None) -> bool:
        """
        保存DataFrame到CSV文件

        Args:
            data: 要保存的数据
            file_path: 文件路径
            header_comment: 可选的头部注释

        Returns:
            是否保存成功
        """
        try:
            # 确保目录存在
            file_path.parent.mkdir(parents=True, exist_ok=True)

            # 保存CSV
            data.to_csv(file_path, **self.csv_config)

            # 如果有头部注释，添加到文件开头
            if header_comment:
                self._add_header_comment(file_path, header_comment)

            self.logger.debug(f"CSV保存成功: {file_path}")
            return True

        except Exception as e:
            self.logger.error(f"CSV保存失败 {file_path}: {str(e)}")
            return False

    def load_csv(self, file_path: Path) -> Optional[pd.DataFrame]:
        """
        从CSV文件加载DataFrame

        Args:
            file_path: 文件路径

        Returns:
            加载的DataFrame或None
        """
        try:
            if not file_path.exists():
                return None

            # 读取CSV
            df = pd.read_csv(file_path, encoding=self.csv_config['encoding'])

            self.logger.debug(f"CSV加载成功: {file_path} ({len(df)}条记录)")
            return df

        except Exception as e:
            self.logger.error(f"CSV加载失败 {file_path}: {str(e)}")
            return None

    def _add_header_comment(self, file_path: Path, comment: str):
        """在CSV文件开头添加注释"""
        try:
            # 读取现有内容
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()

            # 添加注释并写回
            comment_lines = [f"# {line}" for line in comment.split('\n')]
            header = '\n'.join(comment_lines) + '\n'

            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(header + content)

        except Exception as e:
            self.logger.error(f"添加头部注释失败: {str(e)}")

    def batch_save_csv(self, data_dict: Dict[str, pd.DataFrame],
                      base_dir: Path, file_suffix: str = '') -> Dict[str, bool]:
        """
        批量保存CSV文件

        Args:
            data_dict: ETF代码到DataFrame的映射
            base_dir: 基础目录
            file_suffix: 文件后缀

        Returns:
            保存结果映射
        """
        results = {}

        try:
            for etf_code, data in data_dict.items():
                file_name = f"{etf_code}{file_suffix}.csv"
                file_path = base_dir / file_name

                success = self.save_csv(data, file_path)
                results[etf_code] = success

            success_count = sum(1 for success in results.values() if success)
            self.logger.info(f"批量保存完成: {success_count}/{len(data_dict)} 成功")

            return results

        except Exception as e:
            self.logger.error(f"批量保存失败: {str(e)}")
            return results

    def merge_csv_files(self, file_paths: List[Path],
                       output_path: Path) -> bool:
        """
        合并多个CSV文件

        Args:
            file_paths: CSV文件路径列表
            output_path: 输出文件路径

        Returns:
            是否合并成功
        """
        try:
            dfs = []

            for file_path in file_paths:
                df = self.load_csv(file_path)
                if df is not None and not df.empty:
                    dfs.append(df)

            if not dfs:
                self.logger.warning("没有有效的CSV文件可以合并")
                return False

            # 合并所有DataFrame
            merged_df = pd.concat(dfs, ignore_index=True)

            # 按code和date排序
            if 'code' in merged_df.columns and 'date' in merged_df.columns:
                merged_df = merged_df.sort_values(['code', 'date']).reset_index(drop=True)

            # 保存合并结果
            success = self.save_csv(merged_df, output_path)

            if success:
                self.logger.info(f"CSV合并成功: {len(dfs)}个文件 -> {output_path}")

            return success

        except Exception as e:
            self.logger.error(f"CSV合并失败: {str(e)}")
            return False

    def save_pv_indicators_csv(self, data: pd.DataFrame, file_path: Path,
                              etf_code: str = None) -> bool:
        """
        保存PV指标到CSV文件（专门处理10个PV指标字段）

        Args:
            data: PV指标数据
            file_path: 文件路径
            etf_code: ETF代码（用于生成注释）

        Returns:
            是否保存成功
        """
        try:
            # 验证PV指标字段
            expected_pv_columns = [
                'pv_corr_10', 'pv_corr_20', 'pv_corr_30',
                'vpt', 'vpt_momentum', 'vpt_ratio',
                'volume_quality', 'volume_consistency',
                'pv_strength', 'pv_divergence'
            ]

            # 检查必要的PV指标列
            missing_columns = [col for col in expected_pv_columns if col not in data.columns]
            if missing_columns:
                self.logger.warning(f"缺少PV指标列: {missing_columns}")

            # 生成PV指标专用注释
            comment = self._generate_pv_comment(etf_code, data)

            # 保存CSV
            success = self.save_csv(data, file_path, comment)

            if success:
                self.logger.info(f"PV指标CSV保存成功: {file_path} ({len(data)}条记录)")

            return success

        except Exception as e:
            self.logger.error(f"保存PV指标CSV失败 {file_path}: {str(e)}")
            return False

    def _generate_pv_comment(self, etf_code: str, data: pd.DataFrame) -> str:
        """
        生成PV指标专用的CSV注释

        Args:
            etf_code: ETF代码
            data: PV数据

        Returns:
            格式化的注释字符串
        """
        try:
            from datetime import datetime

            comment_lines = [
                f"价量协调指标计算结果 - ETF: {etf_code or 'Unknown'}",
                f"生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
                f"数据记录数: {len(data)}",
                "",
                "PV指标说明:",
                "1. pv_corr_10/20/30: 价量相关性系数(10/20/30日期)",
                "2. vpt: 成交量价格趋势指标",
                "3. vpt_momentum: VPT动量",
                "4. vpt_ratio: VPT比率",
                "5. volume_quality: 成交量质量评分",
                "6. volume_consistency: 成交量一致性评分",
                "7. pv_strength: 价量协调强度",
                "8. pv_divergence: 价量背离程度",
                "",
                "数据精度: 8位小数",
                "编码格式: UTF-8"
            ]

            if 'date' in data.columns and len(data) > 0:
                date_range = f"{data['date'].min()} ~ {data['date'].max()}"
                comment_lines.insert(3, f"日期范围: {date_range}")

            return '\n'.join(comment_lines)

        except Exception as e:
            self.logger.error(f"生成PV注释失败: {str(e)}")
            return f"PV指标计算结果 - ETF: {etf_code or 'Unknown'}"

    def validate_pv_csv_format(self, file_path: Path) -> Dict[str, Any]:
        """
        验证PV CSV文件格式

        Args:
            file_path: CSV文件路径

        Returns:
            验证结果
        """
        try:
            validation_result = {
                'valid': True,
                'file_path': str(file_path),
                'issues': []
            }

            # 检查文件是否存在
            if not file_path.exists():
                validation_result['valid'] = False
                validation_result['issues'].append('文件不存在')
                return validation_result

            # 加载数据
            df = self.load_csv(file_path)
            if df is None or df.empty:
                validation_result['valid'] = False
                validation_result['issues'].append('无法读取文件或文件为空')
                return validation_result

            # 检查必要的基础列
            required_base_columns = ['code', 'date']
            missing_base = [col for col in required_base_columns if col not in df.columns]
            if missing_base:
                validation_result['valid'] = False
                validation_result['issues'].append(f'缺少基础列: {missing_base}')

            # 检查PV指标列
            expected_pv_columns = [
                'pv_corr_10', 'pv_corr_20', 'pv_corr_30',
                'vpt', 'vpt_momentum', 'vpt_ratio',
                'volume_quality', 'volume_consistency',
                'pv_strength', 'pv_divergence'
            ]

            missing_pv = [col for col in expected_pv_columns if col not in df.columns]
            if missing_pv:
                validation_result['issues'].append(f'缺少PV指标列: {missing_pv}')
                if len(missing_pv) > 5:  # 如果缺少太多PV指标列，标记为无效
                    validation_result['valid'] = False

            # 检查数据类型
            for col in expected_pv_columns:
                if col in df.columns:
                    if not pd.api.types.is_numeric_dtype(df[col]):
                        validation_result['issues'].append(f'{col}列不是数值类型')

            # 数据范围检查
            for col in ['pv_corr_10', 'pv_corr_20', 'pv_corr_30', 'pv_divergence']:
                if col in df.columns:
                    invalid_count = ((df[col] < -1) | (df[col] > 1)).sum()
                    if invalid_count > 0:
                        validation_result['issues'].append(f'{col}列有{invalid_count}个值超出[-1,1]范围')

            for col in ['volume_quality', 'volume_consistency']:
                if col in df.columns:
                    invalid_count = ((df[col] < 0) | (df[col] > 1)).sum()
                    if invalid_count > 0:
                        validation_result['issues'].append(f'{col}列有{invalid_count}个值超出[0,1]范围')

            if 'pv_strength' in df.columns:
                invalid_count = ((df['pv_strength'] < 0) | (df['pv_strength'] > 100)).sum()
                if invalid_count > 0:
                    validation_result['issues'].append(f'pv_strength列有{invalid_count}个值超出[0,100]范围')

            validation_result['record_count'] = len(df)
            validation_result['column_count'] = len(df.columns)

            return validation_result

        except Exception as e:
            return {
                'valid': False,
                'file_path': str(file_path),
                'issues': [f'验证失败: {str(e)}']
            }