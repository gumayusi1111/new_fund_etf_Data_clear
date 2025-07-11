#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SMA缓存管理器 - 智能增量缓存系统
=================================

基于原系统的完整缓存功能重构
"""

import os
import json
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Set
from .utils import normalize_date_format, compare_dates_safely


class SMACacheManager:
    """SMA缓存管理器 - 智能增量缓存系统"""
    
    def __init__(self, base_output_dir: str = "data"):
        """
        初始化缓存管理器
        
        Args:
            base_output_dir: 基础输出目录（相对于模块目录）
        """
        
        # 🗂️ 系统独立缓存目录结构设置
        module_dir = os.path.dirname(__file__)
        self.cache_base_dir = os.path.join(module_dir, "..", "..", "cache")
        self.meta_dir = os.path.join(self.cache_base_dir, "meta")
        
        # 确保缓存目录存在
        os.makedirs(self.cache_base_dir, exist_ok=True)
        os.makedirs(self.meta_dir, exist_ok=True)
        
        print("🗂️ SMA缓存管理器初始化完成")
        print(f"   📁 缓存目录: {self.cache_base_dir}")
        print(f"   📊 Meta目录: {self.meta_dir}")
    
    def get_cache_dir(self, threshold: str) -> str:
        """获取指定门槛的缓存目录"""
        cache_dir = os.path.join(self.cache_base_dir, threshold)
        os.makedirs(cache_dir, exist_ok=True)
        return cache_dir
    
    def get_meta_file(self, threshold: str) -> str:
        """获取指定门槛的Meta文件路径"""
        if threshold:
            return os.path.join(self.meta_dir, f"{threshold}_meta.json")
        else:
            return os.path.join(self.meta_dir, "cache_global_meta.json")
    
    def load_meta(self, threshold: str = None) -> Dict:
        """
        加载Meta信息
        
        Args:
            threshold: 门槛类型，None表示加载全局Meta
            
        Returns:
            Dict: Meta信息
        """
        meta_file = self.get_meta_file(threshold)
        
        if os.path.exists(meta_file):
            try:
                with open(meta_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                print(f"⚠️ 加载Meta失败: {str(e)}")
                
        # 返回默认Meta结构
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
                "cache_version": "1.0.0",
                "last_global_update": "",
                "total_cache_size_mb": 0,
                "total_cached_etfs": 0,
                "thresholds": ["3000万门槛", "5000万门槛"]
            }
    
    def save_meta(self, meta_data: Dict, threshold: str = None):
        """
        保存Meta信息
        
        Args:
            meta_data: Meta数据
            threshold: 门槛类型，None表示全局Meta
        """
        try:
            meta_file = self.get_meta_file(threshold)
            with open(meta_file, 'w', encoding='utf-8') as f:
                json.dump(meta_data, f, ensure_ascii=False, indent=2)
            
            print(f"💾 Meta已保存: {os.path.basename(meta_file)}")
            
        except Exception as e:
            print(f"❌ 保存Meta失败: {str(e)}")
    
    def get_cached_etfs(self, threshold: str) -> Set[str]:
        """
        获取已缓存的ETF代码列表
        
        Args:
            threshold: 门槛类型
            
        Returns:
            Set[str]: 已缓存的ETF代码集合
        """
        cache_dir = self.get_cache_dir(threshold)
        
        if not os.path.exists(cache_dir):
            return set()
        
        # 扫描缓存目录中的CSV文件
        csv_files = [f for f in os.listdir(cache_dir) if f.endswith('.csv')]
        etf_codes = set()
        
        for csv_file in csv_files:
            # 从文件名提取ETF代码，并添加交易所后缀
            etf_code = csv_file.replace('.csv', '')
            
            # 智能添加交易所后缀以匹配筛选结果格式
            if not etf_code.endswith(('.SH', '.SZ')):
                if etf_code.startswith('5'):
                    etf_code += '.SH'
                elif etf_code.startswith('1'):
                    etf_code += '.SZ'
                else:
                    # 默认处理其他代码
                    if len(etf_code) == 6 and etf_code.isdigit():
                        # 6位数字，根据首位判断
                        if etf_code[0] in '567':
                            etf_code += '.SH'
                        else:
                            etf_code += '.SZ'
            
            etf_codes.add(etf_code)
        
        return etf_codes
    
    def analyze_etf_changes(self, current_etfs: List[str], threshold: str) -> Dict[str, Set[str]]:
        """
        分析ETF变化情况
        
        Args:
            current_etfs: 当前筛选出的ETF列表
            threshold: 门槛类型
            
        Returns:
            Dict: 包含same_etfs, new_etfs, old_etfs的字典
        """
        current_set = set(current_etfs)
        cached_set = self.get_cached_etfs(threshold)
        
        analysis = {
            'same_etfs': current_set & cached_set,     # 相同ETF - 增量计算
            'new_etfs': current_set - cached_set,      # 新增ETF - 全量计算
            'old_etfs': cached_set - current_set       # 旧ETF - 保持不动
        }
        
        print(f"📊 {threshold} ETF变化分析:")
        print(f"   🔄 相同ETF: {len(analysis['same_etfs'])} 个 (增量计算)")
        print(f"   🆕 新增ETF: {len(analysis['new_etfs'])} 个 (全量计算)")
        print(f"   📦 保留ETF: {len(analysis['old_etfs'])} 个 (保持不动)")
        
        return analysis
    
    def get_cached_etf_latest_date(self, etf_code: str, threshold: str) -> Optional[str]:
        """
        获取缓存中ETF的最新日期
        
        Args:
            etf_code: ETF代码
            threshold: 门槛类型
            
        Returns:
            Optional[str]: 最新日期字符串(YYYYMMDD格式)，未找到返回None
        """
        try:
            cache_dir = self.get_cache_dir(threshold)
            clean_etf_code = etf_code.replace('.SH', '').replace('.SZ', '')
            cache_file = os.path.join(cache_dir, f"{clean_etf_code}.csv")
            
            if not os.path.exists(cache_file):
                return None
            
            # 读取缓存文件的第一行（除了header），因为是按时间倒序
            df = pd.read_csv(cache_file, encoding='utf-8')
            
            if df.empty:
                return None
            
            # 获取第一行的日期（最新日期）
            latest_date = df.iloc[0]['日期']
            
            # 使用统一的日期格式化函数
            return normalize_date_format(latest_date)
            
        except Exception as e:
            print(f"⚠️ 获取{etf_code}最新日期失败: {str(e)}")
            return None
    
    def load_cached_etf_data(self, etf_code: str, threshold: str) -> Optional[pd.DataFrame]:
        """
        加载ETF的缓存数据
        
        Args:
            etf_code: ETF代码
            threshold: 门槛类型
            
        Returns:
            Optional[pd.DataFrame]: 缓存数据或None
        """
        try:
            cache_dir = self.get_cache_dir(threshold)
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
    
    def save_etf_cache(self, etf_code: str, df: pd.DataFrame, threshold: str) -> bool:
        """
        保存ETF缓存数据
        
        Args:
            etf_code: ETF代码
            df: 数据DataFrame (包含SMA计算结果)
            threshold: 门槛类型
            
        Returns:
            bool: 是否保存成功
        """
        try:
            cache_dir = self.get_cache_dir(threshold)
            clean_etf_code = etf_code.replace('.SH', '').replace('.SZ', '')
            cache_file = os.path.join(cache_dir, f"{clean_etf_code}.csv")
            
            # 确保数据按时间倒序排列（与data输出保持一致）
            if '日期' in df.columns:
                df = df.sort_values('日期', ascending=False).reset_index(drop=True)
            
            # 保存到缓存文件
            df.to_csv(cache_file, index=False, encoding='utf-8')
            
            file_size = os.path.getsize(cache_file)
            print(f"💾 {etf_code}: 缓存已保存 ({len(df)}行, {file_size} 字节)")
            
            return True
            
        except Exception as e:
            print(f"❌ 保存{etf_code}缓存失败: {str(e)}")
            return False
    
    def update_threshold_meta(self, threshold: str, analysis: Dict, processing_stats: Dict):
        """
        更新门槛Meta信息
        
        Args:
            threshold: 门槛类型
            analysis: ETF变化分析结果
            processing_stats: 处理统计信息
        """
        try:
            meta = self.load_meta(threshold)
            
            # 更新基本信息
            meta["last_update"] = datetime.now().isoformat()
            meta["total_etfs"] = len(analysis['same_etfs']) + len(analysis['new_etfs'])
            meta["cached_etfs"] = list(analysis['same_etfs'] | analysis['new_etfs'])
            
            # 添加本次更新记录
            update_record = {
                "update_time": datetime.now().isoformat(),
                "same_etfs_count": len(analysis['same_etfs']),
                "new_etfs_count": len(analysis['new_etfs']),
                "old_etfs_count": len(analysis['old_etfs']),
                "processing_stats": processing_stats
            }
            
            if "update_history" not in meta:
                meta["update_history"] = []
            
            meta["update_history"].append(update_record)
            
            # 保持历史记录不超过50条
            if len(meta["update_history"]) > 50:
                meta["update_history"] = meta["update_history"][-50:]
            
            # 保存Meta
            self.save_meta(meta, threshold)
            
        except Exception as e:
            print(f"❌ 更新{threshold} Meta失败: {str(e)}")
    
    def get_cache_statistics(self) -> Dict:
        """
        获取缓存统计信息
        
        Returns:
            Dict: 缓存统计
        """
        stats = {
            "global": self.load_meta(None),
            "thresholds": {}
        }
        
        for threshold in ["3000万门槛", "5000万门槛"]:
            threshold_meta = self.load_meta(threshold)
            cache_dir = self.get_cache_dir(threshold)
            
            # 统计缓存文件数量
            cache_files = 0
            if os.path.exists(cache_dir):
                cache_files = len([f for f in os.listdir(cache_dir) if f.endswith('.csv')])
            
            stats["thresholds"][threshold] = {
                "meta": threshold_meta,
                "cache_files": cache_files
            }
        
        return stats 