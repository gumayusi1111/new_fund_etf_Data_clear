#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
布林带工具模块
============

通用工具函数和辅助方法
"""

import os
import hashlib
import time
from datetime import datetime
from typing import Dict, List, Optional, Any


class BBUtils:
    """布林带工具类"""
    
    @staticmethod
    def get_current_timestamp() -> str:
        """获取当前时间戳"""
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    @staticmethod
    def get_current_date() -> str:
        """获取当前日期"""
        return datetime.now().strftime("%Y-%m-%d")
    
    @staticmethod
    def format_etf_code(etf_code: str) -> str:
        """格式化ETF代码，去除交易所后缀"""
        return etf_code.replace('.SH', '').replace('.SZ', '')
    
    @staticmethod
    def calculate_file_hash(file_path: str) -> Optional[str]:
        """计算文件MD5哈希值"""
        if not os.path.exists(file_path):
            return None
        
        try:
            hash_md5 = hashlib.md5()
            with open(file_path, "rb") as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    hash_md5.update(chunk)
            return hash_md5.hexdigest()
        except Exception:
            return None
    
    @staticmethod
    def get_file_modification_time(file_path: str) -> Optional[float]:
        """获取文件修改时间"""
        if not os.path.exists(file_path):
            return None
        
        try:
            return os.path.getmtime(file_path)
        except Exception:
            return None
    
    @staticmethod
    def get_file_size_mb(file_path: str) -> Optional[float]:
        """获取文件大小（MB）"""
        if not os.path.exists(file_path):
            return None
        
        try:
            size_bytes = os.path.getsize(file_path)
            return round(size_bytes / (1024 * 1024), 2)
        except Exception:
            return None
    
    @staticmethod
    def ensure_directory_exists(directory_path: str) -> bool:
        """确保目录存在"""
        try:
            os.makedirs(directory_path, exist_ok=True)
            return True
        except Exception:
            return False
    
    @staticmethod
    def clean_directory(directory_path: str) -> bool:
        """清空目录"""
        if not os.path.exists(directory_path):
            return True
        
        try:
            for filename in os.listdir(directory_path):
                file_path = os.path.join(directory_path, filename)
                if os.path.isfile(file_path):
                    os.remove(file_path)
            return True
        except Exception:
            return False
    
    @staticmethod
    def get_directory_file_count(directory_path: str, extension: str = ".csv") -> int:
        """获取目录中指定扩展名文件的数量"""
        if not os.path.exists(directory_path):
            return 0
        
        try:
            files = [f for f in os.listdir(directory_path) 
                    if f.endswith(extension)]
            return len(files)
        except Exception:
            return 0
    
    @staticmethod
    def validate_etf_data_structure(df) -> Dict[str, Any]:
        """验证ETF数据结构"""
        validation_result = {
            'is_valid': False,
            'row_count': 0,
            'required_columns': ['日期', '收盘价'],
            'missing_columns': [],
            'data_types_valid': False,
            'date_range': None,
            'price_range': None
        }
        
        if df is None or df.empty:
            validation_result['missing_columns'] = validation_result['required_columns']
            return validation_result
        
        # 检查必需列
        missing_columns = []
        for col in validation_result['required_columns']:
            if col not in df.columns:
                missing_columns.append(col)
        
        validation_result['missing_columns'] = missing_columns
        validation_result['row_count'] = len(df)
        
        if not missing_columns:
            try:
                # 检查数据类型和范围
                if '日期' in df.columns:
                    date_col = df['日期']
                    validation_result['date_range'] = {
                        'start': str(date_col.min()),
                        'end': str(date_col.max())
                    }
                
                if '收盘价' in df.columns:
                    price_col = df['收盘价']
                    validation_result['price_range'] = {
                        'min': float(price_col.min()),
                        'max': float(price_col.max()),
                        'mean': float(price_col.mean())
                    }
                
                validation_result['data_types_valid'] = True
                validation_result['is_valid'] = True
                
            except Exception:
                validation_result['data_types_valid'] = False
        
        return validation_result
    
    @staticmethod
    def format_number(number: Optional[float], precision: int = 2) -> str:
        """格式化数字显示"""
        if number is None:
            return "N/A"
        
        if isinstance(number, (int, float)):
            return f"{number:.{precision}f}"
        
        return str(number)
    
    @staticmethod
    def format_percentage(number: Optional[float], precision: int = 2) -> str:
        """格式化百分比显示"""
        if number is None:
            return "N/A"
        
        if isinstance(number, (int, float)):
            return f"{number:.{precision}f}%"
        
        return str(number)
    
    @staticmethod
    def calculate_processing_time(start_time: float) -> Dict[str, float]:
        """计算处理时间"""
        end_time = time.time()
        elapsed_time = end_time - start_time
        
        return {
            'elapsed_seconds': round(elapsed_time, 2),
            'elapsed_minutes': round(elapsed_time / 60, 2),
            'start_time': start_time,
            'end_time': end_time
        }
    
    @staticmethod
    def get_etf_screening_file_path(threshold: str) -> str:
        """获取ETF筛选文件路径"""
        # 根据项目结构定位筛选文件
        current_dir = os.getcwd()
        
        if "ETF_计算额外数据" in current_dir:
            project_root = current_dir.split("ETF_计算额外数据")[0]
        else:
            project_root = current_dir
        
        screening_file = os.path.join(
            project_root, "ETF_初筛", "data", threshold, "通过筛选ETF.txt"
        )
        
        return screening_file
    
    @staticmethod
    def read_etf_screening_list(threshold: str) -> List[str]:
        """读取ETF筛选列表"""
        screening_file = BBUtils.get_etf_screening_file_path(threshold)
        
        if not os.path.exists(screening_file):
            return []
        
        try:
            with open(screening_file, 'r', encoding='utf-8') as f:
                etf_list = [line.strip() for line in f.readlines() if line.strip()]
            return etf_list
        except Exception:
            return []
    
    @staticmethod
    def generate_cache_key(etf_code: str, threshold: str, additional_params: Optional[Dict] = None) -> str:
        """生成缓存键"""
        key_parts = [etf_code, threshold]
        
        if additional_params:
            # 排序以确保一致性
            sorted_params = sorted(additional_params.items())
            param_str = "_".join([f"{k}={v}" for k, v in sorted_params])
            key_parts.append(param_str)
        
        return "_".join(key_parts)
    
    @staticmethod
    def safe_divide(numerator: Optional[float], denominator: Optional[float], 
                   default: Optional[float] = None) -> Optional[float]:
        """安全除法，避免除零错误"""
        if numerator is None or denominator is None:
            return default
        
        if abs(denominator) < 1e-10:  # 避免除以接近零的数
            return default
        
        try:
            return numerator / denominator
        except Exception:
            return default
    
    @staticmethod
    def create_summary_dict(success_count: int, total_count: int, 
                           processing_time: float, additional_info: Optional[Dict] = None) -> Dict:
        """创建处理汇总字典"""
        success_rate = (success_count / total_count * 100) if total_count > 0 else 0
        
        summary = {
            'success_count': success_count,
            'total_count': total_count,
            'success_rate': round(success_rate, 2),
            'processing_time_seconds': round(processing_time, 2),
            'processing_speed': round(total_count / processing_time, 2) if processing_time > 0 else 0,
            'timestamp': BBUtils.get_current_timestamp()
        }
        
        if additional_info:
            summary.update(additional_info)
        
        return summary