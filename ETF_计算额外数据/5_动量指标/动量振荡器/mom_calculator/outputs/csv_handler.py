"""
CSV输出处理模块
===============

负责动量振荡器数据的CSV文件输出
提供标准化的文件保存和读取功能
"""

import pandas as pd
from pathlib import Path
from typing import Dict, List, Optional, Any
import logging
from datetime import datetime

from ..infrastructure.config import MomentumConfig

class MomentumCSVHandler:
    """动量振荡器CSV输出处理器"""
    
    def __init__(self):
        """初始化CSV处理器"""
        self.logger = logging.getLogger(self.__class__.__name__)
        self.output_fields = MomentumConfig.OUTPUT_FIELDS
        
    def save_to_csv(self, data: pd.DataFrame, etf_code: str, threshold: str) -> bool:
        """
        保存数据到CSV文件
        
        Args:
            data: 要保存的数据
            etf_code: ETF代码
            threshold: 门槛类型
            
        Returns:
            保存是否成功
        """
        try:
            if data.empty:
                self.logger.warning(f"尝试保存空数据: {etf_code}_{threshold}")
                return False
            
            # 获取输出路径
            output_path = MomentumConfig.get_data_dir(threshold) / f"{etf_code}.csv"
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            # 确保列顺序正确
            if all(col in data.columns for col in self.output_fields):
                data_to_save = data[self.output_fields]
            else:
                data_to_save = data
            
            # 保存CSV文件
            data_to_save.to_csv(output_path, index=False, encoding='utf-8')
            
            self.logger.debug(f"数据已保存: {output_path} ({len(data_to_save)}条记录)")
            return True
            
        except Exception as e:
            self.logger.error(f"保存CSV失败 {etf_code}_{threshold}: {str(e)}")
            return False
    
    def load_from_csv(self, etf_code: str, threshold: str) -> Optional[pd.DataFrame]:
        """
        从CSV文件加载数据
        
        Args:
            etf_code: ETF代码
            threshold: 门槛类型
            
        Returns:
            加载的数据或None
        """
        try:
            file_path = MomentumConfig.get_data_dir(threshold) / f"{etf_code}.csv"
            
            if not file_path.exists():
                return None
            
            df = pd.read_csv(file_path)
            self.logger.debug(f"数据已加载: {file_path} ({len(df)}条记录)")
            
            return df
            
        except Exception as e:
            self.logger.error(f"加载CSV失败 {etf_code}_{threshold}: {str(e)}")
            return None
    
    def batch_save_metadata(self, metadata: Dict[str, Any], threshold: str) -> bool:
        """
        批量保存元数据
        
        Args:
            metadata: 元数据字典
            threshold: 门槛类型
            
        Returns:
            保存是否成功
        """
        try:
            meta_file = MomentumConfig.CACHE_META_DIR / f"{threshold}_meta.json"
            meta_file.parent.mkdir(parents=True, exist_ok=True)
            
            import json
            with open(meta_file, 'w', encoding='utf-8') as f:
                json.dump(metadata, f, ensure_ascii=False, indent=2)
            
            self.logger.debug(f"元数据已保存: {meta_file}")
            return True
            
        except Exception as e:
            self.logger.error(f"保存元数据失败 {threshold}: {str(e)}")
            return False