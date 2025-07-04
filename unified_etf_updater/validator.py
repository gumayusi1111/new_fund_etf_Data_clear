#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
周更与日更数据同步校验器
集成到unified_etf_updater架构中
"""

import os
import pandas as pd
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Set, Optional, Tuple
from pathlib import Path


class WeeklyDailyValidator:
    """周更与日更数据同步校验器"""
    
    def __init__(self, config: dict, logger: logging.Logger, project_root: Path):
        """
        初始化校验器
        
        Args:
            config: 配置
            logger: 日志记录器
            project_root: 项目根目录
        """
        self.config = config
        self.logger = logger
        self.project_root = project_root
        
        # 设置目录
        self.weekly_dir = project_root / "ETF周更"
        self.daily_dir = project_root / "ETF日更"
        self.categories = ["0_ETF日K(前复权)", "0_ETF日K(后复权)", "0_ETF日K(除权)"]
        
        # 校验配置
        validator_config = config.get('weekly_daily_validator', {})
        self.enabled = validator_config.get('enabled', True)
        self.auto_fix = validator_config.get('auto_fix', False)
        self.tolerance = validator_config.get('tolerance', 0.0001)  # 0.01%精度容差
        
    def is_enabled(self) -> bool:
        """检查校验器是否启用"""
        return self.enabled
    
    def get_latest_date_from_etf_files(self, base_dir: Path) -> Optional[str]:
        """
        从ETF文件中获取最新的数据日期
        
        Args:
            base_dir: 基础目录路径
            
        Returns:
            最新日期字符串 YYYYMMDD，如果没有找到则返回None
        """
        sample_files = ["159001.csv", "159003.csv", "159005.csv"]
        latest_date = None
        
        for category in self.categories:
            category_dir = base_dir / category
            if not category_dir.exists():
                continue
                
            for sample_file in sample_files:
                file_path = category_dir / sample_file
                if file_path.exists():
                    try:
                        df = pd.read_csv(file_path, encoding='utf-8')
                        if not df.empty and '日期' in df.columns:
                            # 获取最新日期（第一行，因为数据按日期降序排列）
                            file_latest = df.iloc[0]['日期']
                            if latest_date is None or file_latest > latest_date:
                                latest_date = file_latest
                    except Exception as e:
                        self.logger.warning(f"读取文件失败 {file_path}: {e}")
                        continue
                    break  # 找到一个有效文件就够了
        
        return str(latest_date) if latest_date else None
    
    def get_date_range(self, start_date: str, end_date: str) -> List[str]:
        """
        获取指定日期范围内的所有日期
        
        Args:
            start_date: 开始日期 YYYYMMDD
            end_date: 结束日期 YYYYMMDD
            
        Returns:
            日期列表
        """
        start = datetime.strptime(start_date, '%Y%m%d')
        end = datetime.strptime(end_date, '%Y%m%d')
        
        dates = []
        current = start
        while current <= end:
            dates.append(current.strftime('%Y%m%d'))
            current += timedelta(days=1)
        
        return dates
    
    def load_etf_data_for_date(self, base_dir: Path, etf_code: str, target_date: str) -> Dict:
        """
        加载指定ETF在指定日期的数据
        
        Args:
            base_dir: 基础目录
            etf_code: ETF代码（如159001）
            target_date: 目标日期 YYYYMMDD
            
        Returns:
            数据字典，包含三种复权类型的数据
        """
        result = {}
        
        for category in self.categories:
            file_path = base_dir / category / f"{etf_code}.csv"
            
            if not file_path.exists():
                result[category] = None
                continue
                
            try:
                df = pd.read_csv(file_path, encoding='utf-8')
                # 转换目标日期为整数进行匹配
                target_date_int = int(target_date)
                date_data = df[df['日期'] == target_date_int]
                
                if not date_data.empty:
                    # 转换为字典，便于比较
                    row_data = date_data.iloc[0].to_dict()
                    result[category] = row_data
                else:
                    result[category] = None
                    
            except Exception as e:
                self.logger.warning(f"读取文件失败 {file_path}: {e}")
                result[category] = None
        
        return result
    
    def compare_etf_data(self, weekly_data: Dict, daily_data: Dict) -> bool:
        """
        比较周更和日更的ETF数据是否一致
        
        Args:
            weekly_data: 周更数据
            daily_data: 日更数据
            
        Returns:
            True表示一致，False表示不一致
        """
        if not weekly_data or not daily_data:
            return False
        
        # 比较所有复权类型
        for category in self.categories:
            weekly_row = weekly_data.get(category)
            daily_row = daily_data.get(category)
            
            if weekly_row is None and daily_row is None:
                continue
            if weekly_row is None or daily_row is None:
                return False
                
            # 比较关键字段（忽略可能的精度差异）
            key_fields = ['开盘价', '最高价', '最低价', '收盘价', '成交量(手数)', '成交额(千元)']
            
            for field in key_fields:
                if field in weekly_row and field in daily_row:
                    try:
                        weekly_val = float(weekly_row[field])
                        daily_val = float(daily_row[field])
                        
                        # 允许小的精度差异
                        if abs(weekly_val - daily_val) / max(abs(weekly_val), abs(daily_val), 1e-10) > self.tolerance:
                            return False
                    except (ValueError, TypeError):
                        if str(weekly_row[field]) != str(daily_row[field]):
                            return False
        
        return True
    
    def _compare_single_category(self, weekly_row: Dict, daily_row: Dict) -> bool:
        """
        比较单个复权类型的数据是否一致
        
        Args:
            weekly_row: 周更数据行
            daily_row: 日更数据行
            
        Returns:
            True表示一致，False表示不一致
        """
        if weekly_row is None or daily_row is None:
            return False
            
        # 比较关键字段（忽略可能的精度差异）
        key_fields = ['开盘价', '最高价', '最低价', '收盘价', '成交量(手数)', '成交额(千元)']
        
        for field in key_fields:
            if field in weekly_row and field in daily_row:
                try:
                    weekly_val = float(weekly_row[field])
                    daily_val = float(daily_row[field])
                    
                    # 允许小的精度差异
                    if abs(weekly_val - daily_val) / max(abs(weekly_val), abs(daily_val), 1e-10) > self.tolerance:
                        return False
                except (ValueError, TypeError):
                    if str(weekly_row[field]) != str(daily_row[field]):
                        return False
        
        return True
    
    def copy_date_data_from_weekly_to_daily(self, etf_code: str, target_date: str) -> bool:
        """
        将指定日期的数据从周更复制到日更
        
        Args:
            etf_code: ETF代码
            target_date: 目标日期
            
        Returns:
            是否成功
        """
        success_count = 0
        
        for category in self.categories:
            weekly_file = self.weekly_dir / category / f"{etf_code}.csv"
            daily_file = self.daily_dir / category / f"{etf_code}.csv"
            
            if not weekly_file.exists() or not daily_file.exists():
                continue
                
            try:
                # 读取周更数据
                weekly_df = pd.read_csv(weekly_file, encoding='utf-8')
                target_date_int = int(target_date)
                weekly_target = weekly_df[weekly_df['日期'] == target_date_int]
                
                if weekly_target.empty:
                    continue
                    
                # 读取日更数据
                daily_df = pd.read_csv(daily_file, encoding='utf-8')
                
                # 删除日更中的目标日期数据
                daily_df = daily_df[daily_df['日期'] != target_date_int]
                
                # 添加周更的目标日期数据
                daily_df = pd.concat([weekly_target, daily_df], ignore_index=True)
                
                # 按日期降序排序
                daily_df['日期'] = daily_df['日期'].astype(str)
                daily_df = daily_df.sort_values('日期', ascending=False)
                
                # 保存回日更文件
                daily_df.to_csv(daily_file, index=False, encoding='utf-8-sig')
                success_count += 1
                
            except Exception as e:
                self.logger.error(f"复制数据失败 {category}/{etf_code}.csv: {e}")
        
        return success_count == len(self.categories)
    
    def validate_overlap_period(self) -> Tuple[bool, Dict]:
        """
        校验重叠期间的数据一致性
        
        Returns:
            Tuple[是否有不一致, 详细结果]
        """
        self.logger.info("🔍 开始周更与日更数据同步校验...")
        
        # 1. 获取周更和日更的最新日期
        weekly_latest = self.get_latest_date_from_etf_files(self.weekly_dir)
        daily_latest = self.get_latest_date_from_etf_files(self.daily_dir)
        
        if not weekly_latest or not daily_latest:
            self.logger.error("❌ 无法获取数据日期，请检查文件结构")
            return False, {"error": "无法获取数据日期"}
        
        self.logger.info(f"📅 周更最新日期: {weekly_latest}")
        self.logger.info(f"📅 日更最新日期: {daily_latest}")
        
        # 2. 确定重叠期间
        if weekly_latest >= daily_latest:
            self.logger.info("✅ 周更数据已是最新，无需校验")
            return False, {"status": "周更已是最新"}
        
        overlap_dates = self.get_date_range(weekly_latest, daily_latest)
        self.logger.info(f"🔍 重叠期间: {weekly_latest} 到 {daily_latest} ({len(overlap_dates)} 天)")
        
        # 3. 获取需要校验的ETF列表（使用样本）
        sample_etfs = ["159001", "159003", "159005", "159201", "159301"]
        inconsistent_dates = set()
        total_comparisons = 0
        inconsistent_comparisons = 0
        inconsistent_details = {}
        
        # 4. 逐日比较数据
        for date in overlap_dates:
            self.logger.info(f"📊 检查 {date}...")
            date_has_inconsistency = False
            
            for etf_code in sample_etfs:
                weekly_data = self.load_etf_data_for_date(self.weekly_dir, etf_code, date)
                daily_data = self.load_etf_data_for_date(self.daily_dir, etf_code, date)
                
                # 统计每种复权类型的比较次数
                etf_comparison_count = 0
                etf_inconsistency_count = 0
                category_results = {}
                
                # 逐个复权类型进行详细比较
                for category in self.categories:
                    weekly_row = weekly_data.get(category)
                    daily_row = daily_data.get(category)
                    
                    if weekly_row is not None and daily_row is not None:
                        etf_comparison_count += 1
                        total_comparisons += 1
                        
                        # 单独比较这个复权类型
                        category_consistent = self._compare_single_category(weekly_row, daily_row)
                        category_results[category] = category_consistent
                        
                        if not category_consistent:
                            etf_inconsistency_count += 1
                            inconsistent_comparisons += 1
                
                # 记录ETF级别的结果
                etf_consistent = etf_inconsistency_count == 0
                if etf_consistent:
                    self.logger.debug(f"  ✅ {etf_code} 所有复权类型数据一致 ({etf_comparison_count}个)")
                else:
                    self.logger.warning(f"  ⚠️ {etf_code} 有{etf_inconsistency_count}个复权类型数据不一致")
                    for category, is_consistent in category_results.items():
                        if not is_consistent:
                            self.logger.warning(f"    - {category}: 不一致")
                    
                    date_has_inconsistency = True
                    
                    # 记录不一致详情
                    if date not in inconsistent_details:
                        inconsistent_details[date] = []
                    inconsistent_details[date].append({
                        'etf_code': etf_code,
                        'inconsistent_categories': [cat for cat, consistent in category_results.items() if not consistent]
                    })
            
            if date_has_inconsistency:
                inconsistent_dates.add(date)
        
        # 5. 报告结果
        result = {
            "weekly_latest": weekly_latest,
            "daily_latest": daily_latest,
            "overlap_dates": overlap_dates,
            "total_comparisons": total_comparisons,
            "inconsistent_comparisons": inconsistent_comparisons,
            "inconsistent_dates": sorted(list(inconsistent_dates)),
            "inconsistent_details": inconsistent_details
        }
        
        self.logger.info(f"📊 校验结果:")
        self.logger.info(f"   检查天数: {len(overlap_dates)} 天")
        self.logger.info(f"   检查ETF数: {len(sample_etfs)} 个")
        self.logger.info(f"   复权类型: {len(self.categories)} 种 (前复权、后复权、除权)")
        self.logger.info(f"   总比较次数: {total_comparisons} (天×ETF×复权类型)")
        self.logger.info(f"   不一致次数: {inconsistent_comparisons}")
        self.logger.info(f"   不一致日期: {len(inconsistent_dates)} 天")
        
        if inconsistent_dates:
            self.logger.warning(f"⚠️ 发现数据不一致日期: {sorted(inconsistent_dates)}")
            return True, result
        else:
            self.logger.info("🎉 所有数据一致，无需修正！")
            return False, result
    
    def auto_fix_inconsistent_data(self, inconsistent_dates: List[str]) -> bool:
        """
        自动修正不一致的数据
        
        Args:
            inconsistent_dates: 不一致的日期列表
            
        Returns:
            是否成功
        """
        if not inconsistent_dates:
            return True
        
        self.logger.info(f"🔄 开始自动修正 {len(inconsistent_dates)} 个不一致日期...")
        
        # 获取所有ETF文件列表
        weekly_category_dir = self.weekly_dir / self.categories[0]
        if not weekly_category_dir.exists():
            self.logger.error("❌ 找不到周更数据目录")
            return False
        
        etf_files = [f for f in weekly_category_dir.iterdir() if f.suffix == '.csv']
        etf_codes = [f.stem for f in etf_files]
        
        success_dates = 0
        for date in sorted(inconsistent_dates):
            self.logger.info(f"  修正 {date}...")
            date_success = 0
            
            for etf_code in etf_codes:
                if self.copy_date_data_from_weekly_to_daily(etf_code, date):
                    date_success += 1
            
            if date_success > 0:
                success_dates += 1
                self.logger.info(f"  ✅ {date} 修正完成 ({date_success}/{len(etf_codes)} 个ETF)")
            else:
                self.logger.error(f"  ❌ {date} 修正失败")
        
        if success_dates == len(inconsistent_dates):
            self.logger.info("✅ 所有不一致数据修正完成！")
            return True
        else:
            self.logger.warning(f"⚠️ 部分数据修正完成 ({success_dates}/{len(inconsistent_dates)})")
            return False
    
    def run_validation_after_weekly_update(self) -> Tuple[bool, str]:
        """
        在周更完成后运行校验
        
        Returns:
            Tuple[是否需要用户注意, 描述信息]
        """
        if not self.enabled:
            self.logger.debug("📋 周更日更校验已禁用")
            return False, "校验已禁用"
        
        # 执行校验
        has_inconsistency, result = self.validate_overlap_period()
        
        if "error" in result:
            return False, f"校验失败: {result['error']}"
        
        if "status" in result:
            return False, result['status']
        
        if not has_inconsistency:
            return False, "数据一致"
        
        inconsistent_dates = result.get('inconsistent_dates', [])
        
        # 如果启用了自动修正
        if self.auto_fix and inconsistent_dates:
            self.logger.info("🔄 已启用自动修正，开始修正数据...")
            if self.auto_fix_inconsistent_data(inconsistent_dates):
                return False, "自动修正完成"
            else:
                return True, f"自动修正部分失败，需要人工检查: {inconsistent_dates}"
        
        # 需要用户注意
        return True, f"发现数据不一致，需要人工处理: {inconsistent_dates}" 