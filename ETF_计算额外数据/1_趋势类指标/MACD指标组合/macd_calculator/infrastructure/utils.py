#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
统一工具函数
=============

提供各种通用的工具函数，包括日期格式化、数据验证等
"""

import pandas as pd
import numpy as np
from typing import Optional, Union


def normalize_date_format(date_val: Union[str, pd.Timestamp, None]) -> Optional[str]:
    """
    统一日期格式为YYYY-MM-DD字符串
    
    Args:
        date_val: 输入的日期值，可以是字符串、Timestamp或None
        
    Returns:
        Optional[str]: 标准化后的日期字符串(YYYY-MM-DD)，失败返回None
    """
    if pd.isna(date_val) or date_val is None:
        return None
    
    try:
        if isinstance(date_val, str):
            # 处理YYYYMMDD格式
            if len(date_val) == 8 and date_val.isdigit():
                # YYYYMMDD -> YYYY-MM-DD
                return f"{date_val[:4]}-{date_val[4:6]}-{date_val[6:8]}"
            # 处理YYYY-MM-DD格式
            elif '-' in date_val and len(date_val) == 10:
                # 验证格式正确性
                pd.to_datetime(date_val)
                return date_val
            else:
                # 其他字符串格式，尝试用pandas解析
                parsed_date = pd.to_datetime(date_val)
                return parsed_date.strftime('%Y-%m-%d')
        else:
            # pandas Timestamp或其他日期对象
            parsed_date = pd.to_datetime(date_val)
            return parsed_date.strftime('%Y-%m-%d')
    except (ValueError, TypeError):
        return None


def compare_dates_safely(date1: Union[str, pd.Timestamp, None], 
                        date2: Union[str, pd.Timestamp, None]) -> bool:
    """
    安全的日期比较，支持多种日期格式
    
    Args:
        date1: 第一个日期
        date2: 第二个日期
        
    Returns:
        bool: date1 >= date2 返回True，否则返回False
    """
    try:
        # 标准化为YYYY-MM-DD格式进行比较
        d1 = normalize_date_format(date1)
        d2 = normalize_date_format(date2)
        
        if d1 is None or d2 is None:
            return False
        
        # 转换为datetime对象进行精确比较
        dt1 = pd.to_datetime(d1)
        dt2 = pd.to_datetime(d2)
            
        return dt1 >= dt2
    except:
        return False


def validate_date_string(date_str: str) -> bool:
    """
    验证日期字符串是否有效
    
    Args:
        date_str: 日期字符串
        
    Returns:
        bool: 有效返回True，无效返回False
    """
    try:
        pd.to_datetime(date_str)
        return True
    except:
        return False