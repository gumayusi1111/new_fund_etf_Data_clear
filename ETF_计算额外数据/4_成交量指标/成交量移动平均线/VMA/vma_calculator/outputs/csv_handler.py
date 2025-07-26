"""
VMA CSV处理器
=============

专门处理VMA结果的CSV文件读写操作
"""

import pandas as pd
from pathlib import Path
from typing import Dict, List, Any, Optional
import logging

class VMACSVHandler:
    """VMA CSV文件处理器"""

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