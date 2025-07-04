#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
输出管理器
负责将筛选后的ETF数据保存到对应的复权目录结构中
"""

import pandas as pd
from pathlib import Path
from typing import Dict, List, Any
import json
from datetime import datetime

from ..utils.config import get_config
from ..utils.logger import get_logger


class OutputManager:
    """输出管理器"""
    
    def __init__(self):
        self.config = get_config()
        self.logger = get_logger()
        self.output_base = self.config.get_output_base()
        
        # 确保输出目录存在
        self.config.ensure_directories()
    
    def save_simple_results(self, processing_results: Dict[str, Any], fuquan_type: str) -> bool:
        """
        简化保存：保存两个不同门槛的筛选结果
        
        Args:
            processing_results: 处理结果
            fuquan_type: 复权类型
        
        Returns:
            保存是否成功
        """
        try:
            # 确保输出目录存在
            self.output_base.mkdir(parents=True, exist_ok=True)
            
            # 保存当前门槛的结果（5000万）
            self._save_threshold_results(processing_results, "5000万门槛")
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ 保存简化结果失败: {e}")
            return False
    
    def save_dual_threshold_results(self, results_5000w: Dict[str, Any], results_3000w: Dict[str, Any]) -> bool:
        """
        保存双门槛筛选结果：5000万和3000万
        
        Args:
            results_5000w: 5000万门槛筛选结果
            results_3000w: 3000万门槛筛选结果
        
        Returns:
            保存是否成功
        """
        try:
            # 确保输出目录存在
            self.output_base.mkdir(parents=True, exist_ok=True)
            
            # 保存5000万门槛结果
            self._save_threshold_results(results_5000w, "5000万门槛")
            
            # 保存3000万门槛结果  
            self._save_threshold_results(results_3000w, "3000万门槛")
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ 保存双门槛结果失败: {e}")
            return False
    
    def _save_threshold_results(self, processing_results: Dict[str, Any], threshold_name: str):
        """
        保存特定门槛的筛选结果
        
        Args:
            processing_results: 处理结果
            threshold_name: 门槛名称（如"5000万门槛"）
        """
        passed_etf_list = processing_results.get("通过ETF", [])
        candidate_etf_list = processing_results.get("最终结果", {}).get("候选ETF列表", [])
        
        # 创建门槛目录
        threshold_dir = self.output_base / threshold_name
        threshold_dir.mkdir(parents=True, exist_ok=True)
        
        # 保存通过筛选的ETF代码
        if passed_etf_list:
            passed_file = threshold_dir / "通过筛选ETF.txt"
            
            with open(passed_file, 'w', encoding='utf-8') as f:
                f.write(f"# 通过筛选ETF - {threshold_name} - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - 共{len(passed_etf_list)}个\n")
                for etf_code in passed_etf_list:
                    # 移除.SZ/.SH后缀，只保留6位代码
                    clean_code = etf_code.split('.')[0] if '.' in etf_code else etf_code
                    f.write(f"{clean_code}\n")
            
            self.logger.info(f"✅ 保存{threshold_name}通过筛选ETF: {passed_file} ({len(passed_etf_list)}个)")
        
        # 保存候选ETF代码
        if candidate_etf_list:
            candidate_file = threshold_dir / "候选ETF.txt"
            
            with open(candidate_file, 'w', encoding='utf-8') as f:
                f.write(f"# 候选ETF - {threshold_name} - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - 共{len(candidate_etf_list)}个\n")
                for etf_code in candidate_etf_list:
                    # 移除.SZ/.SH后缀，只保留6位代码
                    clean_code = etf_code.split('.')[0] if '.' in etf_code else etf_code
                    f.write(f"{clean_code}\n")
            
            self.logger.info(f"✅ 保存{threshold_name}候选ETF: {candidate_file} ({len(candidate_etf_list)}个)")
    
    def save_filtered_results(self, processing_results: Dict[str, Any],
                            etf_data: Dict[str, pd.DataFrame],
                            fuquan_type: str) -> bool:
        """
        保存筛选结果到对应的复权目录
        
        Args:
            processing_results: 处理结果
            etf_data: 原始ETF数据
            fuquan_type: 复权类型
        
        Returns:
            保存是否成功
        """
        try:
            passed_etf_list = processing_results.get("通过ETF", [])
            candidate_etf_list = processing_results.get("最终结果", {}).get("候选ETF列表", [])
            
            # 保存通过的ETF数据
            success_count = 0
            if passed_etf_list:
                success_count += self._save_etf_data_to_directory(
                    passed_etf_list, etf_data, fuquan_type, "通过筛选"
                )
            
            # 保存候选ETF数据（可选）
            if candidate_etf_list:
                success_count += self._save_etf_data_to_directory(
                    candidate_etf_list, etf_data, fuquan_type, "候选ETF", save_to_main=False
                )
            
            # 保存筛选结果报告
            self._save_processing_report(processing_results, fuquan_type)
            
            self.logger.info(f"✅ 成功保存 {success_count} 个ETF数据到 {fuquan_type}")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ 保存筛选结果失败: {e}")
            return False
    
    def save_all_processed_data(self, processing_results: Dict[str, Any],
                              source_data_loader) -> bool:
        """
        保存所有三种复权的筛选结果
        
        Args:
            processing_results: 处理结果
            source_data_loader: 数据加载器实例
        
        Returns:
            保存是否成功
        """
        try:
            fuquan_type_list = self.config.get_fuquan_types()
            passed_etf_list = processing_results.get("通过ETF", [])
            
            if not passed_etf_list:
                self.logger.warning("⚠️ 没有通过筛选的ETF，跳过数据保存")
                return True
            
            # 为每种复权类型保存数据
            for fuquan_type in fuquan_type_list:
                self.logger.info(f"📁 保存 {fuquan_type} 数据...")
                
                # 加载通过ETF的该复权类型数据
                etf_data = source_data_loader.load_multiple_etfs(passed_etf_list, fuquan_type)
                
                if etf_data:
                    # 保存到对应复权目录
                    self._save_etf_data_to_directory(
                        passed_etf_list, etf_data, fuquan_type, "筛选结果"
                    )
                else:
                    self.logger.warning(f"⚠️ {fuquan_type} 数据加载失败")
            
            # 保存统一的处理报告
            self._save_unified_report(processing_results)
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ 保存所有复权数据失败: {e}")
            return False
    
    def _save_etf_data_to_directory(self, etf_codes: List[str],
                                  etf_data: Dict[str, pd.DataFrame],
                                  fuquan_type: str, 
                                  description: str,
                                  save_to_main: bool = True) -> int:
        """
        保存ETF数据到指定复权目录
        
        Args:
            etf_codes: ETF代码列表
            etf_data: ETF数据字典
            fuquan_type: 复权类型
            description: 描述信息
            save_to_main: 是否保存到主目录
        
        Returns:
            成功保存的文件数
        """
        output_dir = self.output_base / fuquan_type
        output_dir.mkdir(parents=True, exist_ok=True)
        
        success_count = 0
        keep_days = self.config.get_输出设置().get("保留天数", 252)
        
        for etf_code in etf_codes:
            if etf_code in etf_data:
                try:
                    df = etf_data[etf_code]
                    
                    # 限制数据长度
                    if len(df) > keep_days:
                        df = df.tail(keep_days)
                    
                    # 确保数据按日期排序
                    if '日期' in df.columns:
                        df = df.sort_values('日期')
                    
                    # 保存到CSV文件
                    output_file = output_dir / f"{etf_code}.csv"
                    df.to_csv(output_file, index=False, encoding='utf-8')
                    
                    success_count += 1
                    self.logger.debug(f"✅ 保存ETF数据: {etf_code} -> {output_file}")
                    
                except Exception as e:
                    self.logger.error(f"❌ 保存ETF数据失败 {etf_code}: {e}")
        
        self.logger.info(f"📊 {description}: 保存 {success_count}/{len(etf_codes)} 个ETF到 {fuquan_type}")
        return success_count
    
    def _save_processing_report(self, processing_results: Dict[str, Any], 
                              fuquan_type: str):
        """
        保存处理报告
        
        Args:
            processing_results: 处理结果
            fuquan_type: 复权类型
        """
        try:
            report_dir = self.output_base / "reports"
            report_dir.mkdir(parents=True, exist_ok=True)
            
            # 生成报告文件名
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            report_file = report_dir / f"筛选报告_{fuquan_type}_{timestamp}.json"
            
            # 准备报告数据
            report_data = {
                "生成时间": datetime.now().isoformat(),
                "复权类型": fuquan_type,
                "处理摘要": processing_results.get("处理摘要", {}),
                "筛选统计": processing_results.get("最终结果", {}).get("筛选统计", {}),
                "通过ETF": processing_results.get("通过ETF", []),
                "候选ETF": processing_results.get("最终结果", {}).get("候选ETF列表", []),
                "筛选器配置": self._get_filter_config_summary()
            }
            
            # 保存报告
            with open(report_file, 'w', encoding='utf-8') as f:
                json.dump(report_data, f, ensure_ascii=False, indent=2)
            
            self.logger.info(f"📋 筛选报告已保存: {report_file}")
            
        except Exception as e:
            self.logger.error(f"❌ 保存处理报告失败: {e}")
    
    def _save_unified_report(self, processing_results: Dict[str, Any]):
        """
        保存统一处理报告
        
        Args:
            processing_results: 处理结果
        """
        try:
            # 保存最新的筛选结果摘要
            summary_file = self.output_base / "latest_filter_results.json"
            
            summary_data = {
                "更新时间": datetime.now().isoformat(),
                "筛选概要": {
                    "通过ETF数量": len(processing_results.get("通过ETF", [])),
                    "候选ETF数量": len(processing_results.get("最终结果", {}).get("候选ETF列表", [])),
                    "总处理ETF数": processing_results.get("处理摘要", {}).get("数据加载", {}).get("成功加载数", 0)
                },
                "通过ETF列表": processing_results.get("通过ETF", []),
                "复权数据可用性": {
                    fuquan_type: True for fuquan_type in self.config.get_fuquan_types()
                }
            }
            
            with open(summary_file, 'w', encoding='utf-8') as f:
                json.dump(summary_data, f, ensure_ascii=False, indent=2)
            
            self.logger.info(f"📊 统一筛选摘要已保存: {summary_file}")
            
        except Exception as e:
            self.logger.error(f"❌ 保存统一报告失败: {e}")
    
    def _get_filter_config_summary(self) -> Dict[str, Any]:
        """获取筛选器配置摘要"""
        try:
            filter_conditions = self.config.get_筛选条件()
            return {
                "筛选器类型": ["质量筛选", "成交量筛选", "趋势筛选"],
                "主要条件": {
                    "最小交易天数": filter_conditions.get("基础条件", {}).get("最小交易天数"),
                    "最小平均成交量": filter_conditions.get("基础条件", {}).get("最小平均成交量_手"),
                    "最大回撤限制": filter_conditions.get("趋势条件", {}).get("最大回撤限制"),
                    "最小夏普比率": filter_conditions.get("趋势条件", {}).get("最小夏普比率")
                }
            }
        except Exception:
            return {"error": "配置摘要生成失败"}
    
    def get_output_summary(self) -> Dict[str, Any]:
        """
        获取输出目录摘要
        
        Returns:
            输出目录状态摘要
        """
        try:
            summary = {
                "输出基础路径": str(self.output_base),
                "复权目录状态": {},
                "报告文件": []
            }
            
            # 检查各复权目录
            for fuquan_type in self.config.get_fuquan_types():
                fuquan_dir = self.output_base / fuquan_type
                if fuquan_dir.exists():
                    csv_files = list(fuquan_dir.glob("*.csv"))
                    summary["复权目录状态"][fuquan_type] = {
                        "存在": True,
                        "ETF文件数": len(csv_files),
                        "最新更新": self._get_directory_latest_mtime(fuquan_dir)
                    }
                else:
                    summary["复权目录状态"][fuquan_type] = {
                        "存在": False,
                        "ETF文件数": 0,
                        "最新更新": None
                    }
            
            # 检查报告文件
            reports_dir = self.output_base / "reports"
            if reports_dir.exists():
                report_files = list(reports_dir.glob("*.json"))
                summary["报告文件"] = [f.name for f in sorted(report_files, key=lambda x: x.stat().st_mtime, reverse=True)[:5]]
            
            return summary
            
        except Exception as e:
            self.logger.error(f"获取输出摘要失败: {e}")
            return {"error": str(e)}
    
    def _get_directory_latest_mtime(self, directory: Path) -> str:
        """获取目录中最新文件的修改时间"""
        try:
            if not directory.exists():
                return None
            
            files = list(directory.glob("*"))
            if not files:
                return None
            
            latest_file = max(files, key=lambda x: x.stat().st_mtime)
            mtime = datetime.fromtimestamp(latest_file.stat().st_mtime)
            return mtime.isoformat()
            
        except Exception:
            return None
    
    def clean_old_data(self, days_to_keep: int = 30) -> bool:
        """
        清理旧的筛选数据
        
        Args:
            days_to_keep: 保留天数
        
        Returns:
            清理是否成功
        """
        try:
            self.logger.info(f"🧹 开始清理 {days_to_keep} 天前的数据...")
            
            cutoff_time = datetime.now().timestamp() - (days_to_keep * 24 * 3600)
            cleaned_count = 0
            
            # 清理报告文件
            reports_dir = self.output_base / "reports"
            if reports_dir.exists():
                for report_file in reports_dir.glob("*.json"):
                    if report_file.stat().st_mtime < cutoff_time:
                        report_file.unlink()
                        cleaned_count += 1
            
            self.logger.info(f"✅ 清理完成，删除 {cleaned_count} 个旧文件")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ 清理旧数据失败: {e}")
            return False 