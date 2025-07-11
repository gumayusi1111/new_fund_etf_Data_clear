#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MACD缓存管理器 - 重构版
======================

参照EMA/WMA/SMA系统的智能缓存架构
支持增量更新、Meta文件管理和性能优化
"""

import os
import json
import pandas as pd
import numpy as np
from datetime import datetime
from typing import Dict, Optional, List, Any, Set
from .config import MACDConfig
from .utils import normalize_date_format, compare_dates_safely


class MACDCacheManager:
    """MACD智能缓存管理器 - 重构版（与EMA/WMA/SMA保持一致）"""
    
    def __init__(self, config: Optional[MACDConfig] = None):
        """
        初始化缓存管理器
        
        Args:
            config: MACD配置对象
        """
        self.config = config
        
        # 缓存目录设置 - 与其他系统保持一致的结构
        current_dir = os.path.dirname(__file__)
        self.cache_base_dir = os.path.join(current_dir, "..", "..", "cache")
        self.meta_dir = os.path.join(self.cache_base_dir, "meta")
        
        # 确保缓存目录存在
        self._ensure_cache_directories()
        
        if not (config and config.performance_mode):
            print("🗂️ MACD智能缓存管理器初始化完成")
            print(f"   📁 缓存目录: {self.cache_base_dir}")
            print(f"   📊 Meta目录: {self.meta_dir}")
    
    def _ensure_cache_directories(self) -> None:
        """确保缓存目录结构存在 - 支持三参数文件夹结构并自动创建meta文件"""
        thresholds = ["3000万门槛", "5000万门槛"]
        parameters = ["标准", "敏感", "平滑"]
        
        directories = [
            self.cache_base_dir,
            self.meta_dir
        ]
        
        # 为每个门槛下创建三个参数文件夹
        for threshold in thresholds:
            for parameter in parameters:
                directories.append(os.path.join(self.cache_base_dir, threshold, parameter))
        
        for directory in directories:
            if not os.path.exists(directory):
                os.makedirs(directory)
        
        # 自动创建meta文件（如果不存在）
        self._ensure_meta_files()
    
    def _ensure_meta_files(self) -> None:
        """确保meta文件存在，如果不存在则创建默认文件"""
        try:
            # 创建全局meta文件
            global_meta_file = self.get_meta_file(None)
            if not os.path.exists(global_meta_file):
                global_meta = {
                    "system_name": "MACD指标组合",
                    "version": "2.0",
                    "created_date": datetime.now().isoformat(),
                    "last_update": datetime.now().isoformat(),
                    "parameter_combinations": [
                        {
                            "name": "标准",
                            "parameters": {
                                "fast_period": 12,
                                "slow_period": 26,
                                "signal_period": 9
                            }
                        },
                        {
                            "name": "敏感",
                            "parameters": {
                                "fast_period": 8,
                                "slow_period": 17,
                                "signal_period": 9
                            }
                        },
                        {
                            "name": "平滑",
                            "parameters": {
                                "fast_period": 19,
                                "slow_period": 39,
                                "signal_period": 9
                            }
                        }
                    ],
                    "thresholds": ["3000万门槛", "5000万门槛"],
                    "total_cache_files": 0,
                    "cache_structure": {
                        "3000万门槛": {
                            "标准": 0,
                            "敏感": 0,
                            "平滑": 0
                        },
                        "5000万门槛": {
                            "标准": 0,
                            "敏感": 0,
                            "平滑": 0
                        }
                    },
                    "last_processing_stats": {
                        "total_etfs_processed": 0,
                        "total_files_generated": 0,
                        "processing_time": "",
                        "cache_hit_rate": 0.0,
                        "success_rate": 0.0
                    }
                }
                self.save_meta(global_meta, None)
            
            # 创建门槛级meta文件
            for threshold in ["3000万门槛", "5000万门槛"]:
                threshold_meta_file = self.get_meta_file(threshold)
                if not os.path.exists(threshold_meta_file):
                    threshold_meta = {
                        "threshold": threshold,
                        "last_update": datetime.now().isoformat(),
                        "total_etfs": 0,
                        "parameter_combinations": {
                            "标准": {
                                "cached_etfs_count": 0,
                                "parameter_folder": "标准",
                                "macd_params": {
                                    "fast_period": 12,
                                    "slow_period": 26,
                                    "signal_period": 9
                                },
                                "last_calculation": ""
                            },
                            "敏感": {
                                "cached_etfs_count": 0,
                                "parameter_folder": "敏感",
                                "macd_params": {
                                    "fast_period": 8,
                                    "slow_period": 17,
                                    "signal_period": 9
                                },
                                "last_calculation": ""
                            },
                            "平滑": {
                                "cached_etfs_count": 0,
                                "parameter_folder": "平滑",
                                "macd_params": {
                                    "fast_period": 19,
                                    "slow_period": 39,
                                    "signal_period": 9
                                },
                                "last_calculation": ""
                            }
                        },
                        "cache_created": datetime.now().isoformat(),
                        "update_history": [],
                        "processing_stats": {
                            "total_cache_hits": 0,
                            "total_incremental_updates": 0,
                            "total_new_calculations": 0,
                            "total_failed_count": 0,
                            "cache_hit_rate": 0.0,
                            "success_rate": 0.0
                        }
                    }
                    self.save_meta(threshold_meta, threshold)
                    
        except Exception as e:
            print(f"⚠️ 创建meta文件失败: {str(e)}")
    
    def get_cache_dir(self, threshold: str, parameter_folder: str = "标准") -> str:
        """获取指定门槛和参数的缓存目录"""
        cache_dir = os.path.join(self.cache_base_dir, threshold, parameter_folder)
        os.makedirs(cache_dir, exist_ok=True)
        return cache_dir
    
    def get_meta_file(self, threshold: Optional[str] = None) -> str:
        """获取Meta文件路径"""
        if threshold:
            return os.path.join(self.meta_dir, f"{threshold}_meta.json")
        else:
            return os.path.join(self.meta_dir, "cache_global_meta.json")
    
    def load_meta(self, threshold: Optional[str] = None) -> Dict:
        """加载Meta信息"""
        meta_file = self.get_meta_file(threshold)
        
        if os.path.exists(meta_file):
            try:
                with open(meta_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                print(f"⚠️ 加载Meta失败: {str(e)}")
        
        return self._get_default_meta(threshold)
    
    def _get_default_meta(self, threshold: Optional[str] = None) -> Dict:
        """获取默认Meta结构"""
        if threshold:
            return {
                "threshold": threshold,
                "last_update": "",
                "total_etfs": 0,
                "cached_etfs": [],
                "cache_created": datetime.now().isoformat(),
                "update_history": []
            }
        else:
            return {
                "cache_version": "2.0.0",
                "last_global_update": "",
                "total_cache_size_mb": 0,
                "total_cached_etfs": 0,
                "thresholds": ["3000万门槛", "5000万门槛"]
            }
    
    def save_meta(self, meta_data: Dict, threshold: Optional[str] = None) -> bool:
        """保存Meta信息"""
        try:
            meta_file = self.get_meta_file(threshold)
            with open(meta_file, 'w', encoding='utf-8') as f:
                json.dump(meta_data, f, ensure_ascii=False, indent=2)
            return True
        except Exception as e:
            print(f"❌ 保存Meta失败: {str(e)}")
            return False
    
    def get_cached_etfs(self, threshold: str) -> Set[str]:
        """获取已缓存的ETF代码列表"""
        cache_dir = self.get_cache_dir(threshold)
        
        if not os.path.exists(cache_dir):
            return set()
        
        csv_files = [f for f in os.listdir(cache_dir) if f.endswith('.csv')]
        etf_codes = set()
        
        for csv_file in csv_files:
            etf_code = csv_file.replace('.csv', '')
            # 智能添加交易所后缀以匹配筛选结果格式
            if not etf_code.endswith(('.SH', '.SZ')):
                if etf_code.startswith('5'):
                    etf_code += '.SH'
                elif etf_code.startswith('1'):
                    etf_code += '.SZ'
            etf_codes.add(etf_code)
        
        return etf_codes
    
    def analyze_etf_changes(self, current_etfs: List[str], threshold: str) -> Dict[str, Set[str]]:
        """分析ETF变化情况"""
        current_set = set(current_etfs)
        cached_set = self.get_cached_etfs(threshold)
        
        analysis = {
            'same_etfs': current_set & cached_set,     # 相同ETF - 增量计算
            'new_etfs': current_set - cached_set,      # 新增ETF - 全量计算
            'old_etfs': cached_set - current_set       # 旧ETF - 保持不动
        }
        
        if not (self.config and self.config.performance_mode):
            print(f"📊 {threshold} ETF变化分析:")
            print(f"   🔄 相同ETF: {len(analysis['same_etfs'])} 个 (增量计算)")
            print(f"   🆕 新增ETF: {len(analysis['new_etfs'])} 个 (全量计算)")
            print(f"   📦 保留ETF: {len(analysis['old_etfs'])} 个 (保持不动)")
        
        return analysis
    
    def save_etf_cache(self, etf_code: str, df: pd.DataFrame, threshold: str, parameter_folder: str = "标准") -> bool:
        """保存ETF缓存数据"""
        try:
            cache_dir = self.get_cache_dir(threshold, parameter_folder)
            clean_etf_code = etf_code.replace('.SH', '').replace('.SZ', '')
            cache_file = os.path.join(cache_dir, f"{clean_etf_code}.csv")
            
            # 确保数据按时间倒序排列（与data输出保持一致）
            if 'date' in df.columns:
                df = df.sort_values('date', ascending=False).reset_index(drop=True)
            elif '日期' in df.columns:
                df = df.sort_values('日期', ascending=False).reset_index(drop=True)
            
            df.to_csv(cache_file, index=False, encoding='utf-8')
            
            file_size = os.path.getsize(cache_file)
            if not (self.config and self.config.performance_mode):
                print(f"💾 {etf_code}: 缓存已保存 ({len(df)}行, {file_size} 字节)")
            
            return True
            
        except Exception as e:
            if not (self.config and self.config.performance_mode):
                print(f"❌ 保存{etf_code}缓存失败: {str(e)}")
            return False
    
    def get_cached_etf_latest_date(self, etf_code: str, threshold: str, parameter_folder: str = "标准") -> Optional[str]:
        """
        获取缓存中ETF的最新日期
        
        Args:
            etf_code: ETF代码
            threshold: 门槛类型
            parameter_folder: 参数文件夹
            
        Returns:
            Optional[str]: 最新日期字符串(YYYYMMDD格式)，未找到返回None
        """
        try:
            cache_dir = self.get_cache_dir(threshold, parameter_folder)
            clean_etf_code = etf_code.replace('.SH', '').replace('.SZ', '')
            cache_file = os.path.join(cache_dir, f"{clean_etf_code}.csv")
            
            if not os.path.exists(cache_file):
                return None
            
            # 读取缓存文件的第一行（除了header），因为是按时间倒序
            df = pd.read_csv(cache_file, encoding='utf-8', nrows=1)
            
            if df.empty:
                return None
            
            # 获取第一行的日期（最新日期）- 兼容新旧字段名
            if 'date' in df.columns:
                latest_date = df.iloc[0]['date']
            elif '日期' in df.columns:
                latest_date = df.iloc[0]['日期']
            else:
                return None
            
            # 使用统一的日期格式化函数
            return normalize_date_format(latest_date)
            
        except Exception as e:
            if not (self.config and self.config.performance_mode):
                print(f"⚠️ 获取{etf_code}最新日期失败: {str(e)}")
            return None

    def load_cached_etf_data(self, etf_code: str, threshold: str, parameter_folder: str = "标准") -> Optional[pd.DataFrame]:
        """加载ETF的缓存数据"""
        try:
            cache_dir = self.get_cache_dir(threshold, parameter_folder)
            clean_etf_code = etf_code.replace('.SH', '').replace('.SZ', '')
            cache_file = os.path.join(cache_dir, f"{clean_etf_code}.csv")
            
            if not os.path.exists(cache_file):
                return None
            
            df = pd.read_csv(cache_file, encoding='utf-8')
            if df.empty:
                return None
            
            return df
            
        except Exception as e:
            print(f"❌ 加载{etf_code}缓存数据失败: {str(e)}")
            return None
    
    def is_cache_valid_optimized(self, etf_code: str, threshold: str, source_file_path: str, parameter_folder: str = "标准") -> bool:
        """
        优化的缓存有效性检查 - 支持增量更新
        
        Args:
            etf_code: ETF代码
            threshold: 门槛类型
            source_file_path: 源文件路径
            parameter_folder: 参数文件夹
            
        Returns:
            bool: 缓存是否有效
        """
        try:
            cache_dir = self.get_cache_dir(threshold, parameter_folder)
            clean_etf_code = etf_code.replace('.SH', '').replace('.SZ', '')
            cache_file_path = os.path.join(cache_dir, f"{clean_etf_code}.csv")
            
            # 检查缓存文件是否存在
            if not os.path.exists(cache_file_path):
                return False
            
            if not os.path.exists(source_file_path):
                return False
            
            # 检查缓存数据完整性
            try:
                cache_df = pd.read_csv(cache_file_path)
                if cache_df.empty:
                    return False
                
                # 检查必要的MACD列是否存在 - 使用原系统字段名
                required_macd_columns = ['dif', 'dea', 'macd_bar']
                if not all(col in cache_df.columns for col in required_macd_columns):
                    return False
                
                # 检查缓存中的最新数据日期
                cache_latest_date = self.get_cached_etf_latest_date(etf_code, threshold, parameter_folder)
                if not cache_latest_date:
                    return False
                
                # 检查源文件的最新日期
                source_df = pd.read_csv(source_file_path, nrows=2)  # 只读取前2行提高性能
                if source_df.empty:
                    return False
                    
                # 源文件是按时间倒序排列，最新数据在第一行（除了header）
                if 'date' in source_df.columns:
                    source_latest_date = source_df.iloc[0]['date']
                elif '日期' in source_df.columns:
                    source_latest_date = source_df.iloc[0]['日期']
                else:
                    return False
                
                # 使用统一的日期比较函数
                return compare_dates_safely(cache_latest_date, source_latest_date)
                
            except Exception as e:
                if not (self.config and self.config.performance_mode):
                    print(f"⚠️ 缓存验证过程中发生错误: {etf_code} - {str(e)}")
                return False
            
        except Exception as e:
            if not (self.config and self.config.performance_mode):
                print(f"⚠️ 缓存有效性检查失败: {etf_code} - {str(e)}")
            return False

    def is_cache_valid(self, etf_code: str, threshold: str, source_file_path: str, parameter_folder: str = "标准") -> bool:
        """检查缓存是否有效 - 使用优化版本"""
        return self.is_cache_valid_optimized(etf_code, threshold, source_file_path, parameter_folder)
    
    def get_cache_file_path(self, etf_code: str, threshold: str, parameter_folder: str = "标准") -> str:
        """获取缓存文件路径"""
        cache_dir = self.get_cache_dir(threshold, parameter_folder)
        clean_etf_code = etf_code.replace('.SH', '').replace('.SZ', '')
        return os.path.join(cache_dir, f"{clean_etf_code}.csv")
    
    def update_processing_stats(self, threshold: str, stats: Dict) -> None:
        """更新处理统计信息"""
        try:
            meta = self.load_meta(threshold)
            
            if "processing_stats" not in meta:
                meta["processing_stats"] = {}
            
            meta["processing_stats"].update(stats)
            meta["last_update"] = datetime.now().isoformat()
            
            update_record = {
                "timestamp": datetime.now().isoformat(),
                "processing_stats": stats.copy()
            }
            
            if "update_history" not in meta:
                meta["update_history"] = []
            
            meta["update_history"].append(update_record)
            
            if len(meta["update_history"]) > 10:
                meta["update_history"] = meta["update_history"][-10:]
            
            self.save_meta(meta, threshold)
            
            # 自动更新全局缓存元数据
            self._update_global_cache_meta()
            
        except Exception as e:
            print(f"❌ 统计信息更新失败: {str(e)}")
    
    def _update_global_cache_meta(self) -> None:
        """自动更新全局缓存元数据"""
        try:
            # 计算总缓存大小和ETF数量
            total_size_bytes = 0
            total_etfs = 0
            
            for threshold in ["3000万门槛", "5000万门槛"]:
                cache_dir = self.get_cache_dir(threshold)
                if os.path.exists(cache_dir):
                    for file in os.listdir(cache_dir):
                        if file.endswith('.csv'):
                            file_path = os.path.join(cache_dir, file)
                            total_size_bytes += os.path.getsize(file_path)
                            total_etfs += 1
            
            # 更新全局元数据
            global_meta = {
                "cache_version": "2.0.0",
                "last_global_update": datetime.now().isoformat(),
                "total_cache_size_mb": round(total_size_bytes / 1024 / 1024, 2),
                "total_cached_etfs": total_etfs,
                "thresholds": ["3000万门槛", "5000万门槛"]
            }
            
            self.save_meta(global_meta, None)
            
        except Exception as e:
            print(f"⚠️ 全局缓存元数据更新失败: {str(e)}")

    # Legacy methods for backward compatibility
    def save_to_cache(self, etf_code: str, data: Any, threshold: str = "3000万门槛") -> bool:
        """保存数据到缓存（向后兼容方法）"""
        if isinstance(data, pd.DataFrame):
            return self.save_etf_cache(etf_code, data, threshold)
        return False
    
    def load_from_cache(self, etf_code: str, threshold: str = "3000万门槛") -> Optional[Any]:
        """从缓存加载数据（向后兼容方法）"""
        return self.load_cached_etf_data(etf_code, threshold)