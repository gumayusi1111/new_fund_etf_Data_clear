#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ATR工具函数模块
==============

提供ATR计算器的通用工具函数，包括：
- 数据验证工具
- 文件操作工具
- 数学计算工具
- 格式化工具
- 性能监控工具
"""

import os
import time
import numpy as np
import pandas as pd
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union
from datetime import datetime, timedelta
import logging


def setup_logger(name: str, level: str = "INFO") -> logging.Logger:
    """设置日志记录器"""
    logger = logging.getLogger(name)
    
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    
    logger.setLevel(getattr(logging, level.upper()))
    return logger


def timer(func):
    """性能计时装饰器"""
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        
        logger = logging.getLogger(__name__)
        logger.debug(f"{func.__name__} 执行时间: {end_time - start_time:.4f}秒")
        
        return result
    return wrapper


def format_number(value: Union[int, float], decimal_places: int = 2) -> str:
    """格式化数字显示"""
    if pd.isna(value) or value is None:
        return "N/A"
    
    if isinstance(value, (int, float)):
        if abs(value) >= 1e9:
            return f"{value/1e9:.{decimal_places}f}B"
        elif abs(value) >= 1e6:
            return f"{value/1e6:.{decimal_places}f}M"
        elif abs(value) >= 1e3:
            return f"{value/1e3:.{decimal_places}f}K"
        else:
            return f"{value:.{decimal_places}f}"
    
    return str(value)


def format_percentage(value: Union[int, float], decimal_places: int = 2) -> str:
    """格式化百分比显示"""
    if pd.isna(value) or value is None:
        return "N/A"
    
    return f"{value:.{decimal_places}f}%"


def safe_divide(numerator: Union[int, float], denominator: Union[int, float], 
               default_value: float = np.nan) -> float:
    """安全除法，避免除零错误"""
    try:
        if pd.isna(numerator) or pd.isna(denominator) or denominator == 0:
            return default_value
        return numerator / denominator
    except (TypeError, ZeroDivisionError):
        return default_value


def calculate_statistics(series: pd.Series) -> Dict[str, Any]:
    """计算序列统计信息"""
    try:
        clean_series = series.dropna()
        
        if len(clean_series) == 0:
            return {
                'count': 0,
                'mean': np.nan,
                'std': np.nan,
                'min': np.nan,
                'max': np.nan,
                'median': np.nan,
                'q25': np.nan,
                'q75': np.nan
            }
        
        return {
            'count': len(clean_series),
            'mean': clean_series.mean(),
            'std': clean_series.std(),
            'min': clean_series.min(),
            'max': clean_series.max(),
            'median': clean_series.median(),
            'q25': clean_series.quantile(0.25),
            'q75': clean_series.quantile(0.75)
        }
    
    except Exception:
        return {'count': 0, 'mean': np.nan, 'std': np.nan, 'min': np.nan, 
                'max': np.nan, 'median': np.nan, 'q25': np.nan, 'q75': np.nan}


def validate_dataframe(df: pd.DataFrame, required_columns: List[str]) -> Tuple[bool, str]:
    """验证DataFrame格式和内容"""
    try:
        # 检查是否为空
        if df is None or df.empty:
            return False, "DataFrame为空"
        
        # 检查必需列
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            return False, f"缺少必需列: {missing_columns}"
        
        # 检查数据类型
        for col in required_columns:
            if col == '日期':
                continue  # 日期列单独处理
            
            if not pd.api.types.is_numeric_dtype(df[col]):
                try:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                except:
                    return False, f"列 {col} 无法转换为数值类型"
        
        # 检查数据完整性
        for col in required_columns:
            if col != '日期' and df[col].isna().all():
                return False, f"列 {col} 全部为空值"
        
        return True, "验证通过"
    
    except Exception as e:
        return False, f"验证过程出错: {str(e)}"


def clean_etf_code(etf_code: str) -> str:
    """清理和标准化ETF代码"""
    if not etf_code:
        return ""
    
    # 移除空格和特殊字符
    etf_code = etf_code.strip().upper()
    
    # 如果只有6位数字，添加交易所后缀
    if len(etf_code) == 6 and etf_code.isdigit():
        if etf_code.startswith(('50', '51', '52', '56', '58')):
            etf_code += '.SH'
        elif etf_code.startswith(('15', '16', '18')):
            etf_code += '.SZ'
        else:
            etf_code += '.SH'  # 默认上海
    
    return etf_code


def get_file_size(file_path: Union[str, Path]) -> int:
    """获取文件大小（字节）"""
    try:
        return Path(file_path).stat().st_size
    except (OSError, FileNotFoundError):
        return 0


def get_file_modification_time(file_path: Union[str, Path]) -> datetime:
    """获取文件修改时间"""
    try:
        timestamp = Path(file_path).stat().st_mtime
        return datetime.fromtimestamp(timestamp)
    except (OSError, FileNotFoundError):
        return datetime.min


def ensure_directory_exists(directory: Union[str, Path]) -> bool:
    """确保目录存在"""
    try:
        Path(directory).mkdir(parents=True, exist_ok=True)
        return True
    except Exception:
        return False


def calculate_atr_metrics_summary(atr_data: pd.DataFrame) -> Dict[str, Any]:
    """计算ATR指标汇总统计"""
    try:
        summary = {
            'data_overview': {
                'total_records': len(atr_data),
                'date_range': None,
                'valid_atr_records': 0
            },
            'atr_statistics': {},
            'volatility_analysis': {},
            'risk_metrics': {}
        }
        
        # 数据概览
        if '日期' in atr_data.columns and not atr_data['日期'].isna().all():
            dates = pd.to_datetime(atr_data['日期']).dropna()
            if len(dates) > 0:
                summary['data_overview']['date_range'] = {
                    'start': dates.min().strftime('%Y-%m-%d'),
                    'end': dates.max().strftime('%Y-%m-%d'),
                    'trading_days': len(dates)
                }
        
        # ATR统计
        atr_fields = ['tr', 'atr_10', 'atr_percent', 'atr_change_rate', 'atr_ratio_hl']
        
        for field in atr_fields:
            if field in atr_data.columns:
                summary['atr_statistics'][field] = calculate_statistics(atr_data[field])
        
        # 波动性分析
        if 'atr_percent' in atr_data.columns:
            atr_percent = atr_data['atr_percent'].dropna()
            if len(atr_percent) > 0:
                summary['volatility_analysis'] = {
                    'average_volatility': atr_percent.mean(),
                    'volatility_stability': atr_percent.std(),
                    'high_volatility_days': (atr_percent > 3.0).sum(),
                    'low_volatility_days': (atr_percent < 1.5).sum(),
                    'extreme_volatility_days': (atr_percent > 5.0).sum()
                }
        
        # 风险指标
        if 'stop_loss' in atr_data.columns and '收盘价' in atr_data.columns:
            stop_loss = atr_data['stop_loss'].dropna()
            close_price = atr_data['收盘价'].dropna()
            
            if len(stop_loss) > 0 and len(close_price) > 0:
                # 计算止损距离
                stop_distance = ((close_price - stop_loss) / close_price * 100).dropna()
                summary['risk_metrics'] = {
                    'average_stop_distance_pct': stop_distance.mean(),
                    'max_stop_distance_pct': stop_distance.max(),
                    'min_stop_distance_pct': stop_distance.min()
                }
        
        # 波动性分级统计
        if 'volatility_level' in atr_data.columns:
            vol_level_counts = atr_data['volatility_level'].value_counts()
            summary['volatility_level_distribution'] = vol_level_counts.to_dict()
        
        return summary
    
    except Exception as e:
        logger = logging.getLogger(__name__)
        logger.error(f"计算ATR指标汇总失败: {e}")
        return {
            'error': str(e),
            'data_overview': {'total_records': 0},
            'atr_statistics': {},
            'volatility_analysis': {},
            'risk_metrics': {}
        }


def format_processing_results(results: Dict[str, Any]) -> str:
    """格式化处理结果为可读文本"""
    try:
        output_lines = []
        
        # 基本信息
        if 'total_etfs_processed' in results:
            output_lines.append(f"📊 总处理ETF数量: {results['total_etfs_processed']}")
        
        # 门槛统计
        if 'threshold_statistics' in results:
            for threshold, stats in results['threshold_statistics'].items():
                if stats:
                    success_count = stats.get('successful_etfs', 0)
                    total_count = stats.get('total_etfs', 0)
                    success_rate = stats.get('success_rate', 0)
                    
                    output_lines.append(f"\n📈 {threshold}:")
                    output_lines.append(f"   ✅ 成功: {success_count}/{total_count} ({success_rate:.1f}%)")
                    
                    if 'file_size_mb' in stats:
                        output_lines.append(f"   💾 文件大小: {stats['file_size_mb']:.2f} MB")
        
        # 处理时间
        if 'processing_time' in results:
            duration = results['processing_time']
            if duration > 60:
                output_lines.append(f"\n⏱️ 处理时间: {duration/60:.1f}分钟")
            else:
                output_lines.append(f"\n⏱️ 处理时间: {duration:.1f}秒")
        
        return "\n".join(output_lines)
    
    except Exception:
        return "结果格式化失败"


def batch_process_with_progress(items: List[Any], process_func, 
                              batch_size: int = 10, 
                              show_progress: bool = True) -> List[Any]:
    """批量处理，显示进度"""
    results = []
    total_items = len(items)
    
    logger = logging.getLogger(__name__)
    
    for i in range(0, total_items, batch_size):
        batch = items[i:i + batch_size]
        batch_results = []
        
        for item in batch:
            try:
                result = process_func(item)
                batch_results.append(result)
            except Exception as e:
                logger.warning(f"处理项目失败 {item}: {e}")
                batch_results.append(None)
        
        results.extend(batch_results)
        
        if show_progress:
            processed = min(i + batch_size, total_items)
            progress = processed / total_items * 100
            logger.info(f"处理进度: {processed}/{total_items} ({progress:.1f}%)")
    
    return results


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


def read_etf_screening_list(threshold: str) -> List[str]:
    """读取ETF筛选列表"""
    screening_file = get_etf_screening_file_path(threshold)
    
    if not os.path.exists(screening_file):
        return []
    
    try:
        with open(screening_file, 'r', encoding='utf-8') as f:
            etf_list = [line.strip() for line in f.readlines() 
                       if line.strip() and not line.strip().startswith('#')]
        return etf_list
    except Exception:
        return []