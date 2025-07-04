#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ETF数据处理器
负责协调整个筛选流程，管理筛选器链，处理筛选结果
"""

import pandas as pd
from typing import Dict, List, Any, Optional
from datetime import datetime

from ..data_loader import ETFDataLoader
from ..filters import VolumeFilter, QualityFilter, FilterResult
from ..utils.config import get_config
from ..utils.logger import get_logger, ProcessTimer


class ETFDataProcessor:
    """ETF数据处理器主类"""
    
    def __init__(self, threshold_name: str = "5000万门槛"):
        self.config = get_config()
        self.logger = get_logger()
        self.data_loader = ETFDataLoader()
        self.threshold_name = threshold_name
        
        # 初始化筛选器
        self._init_filters()
        
    def _init_filters(self):
        """初始化筛选器链"""
        # 构建新的配置结构
        filter_config_data = {
            "流动性门槛": self.config.get_liquidity_thresholds(),
            "价格质量标准": self.config.get_price_quality_standards(),
            "数据质量要求": self.config.get_data_quality_requirements(),
            "异常波动阈值": self.config.get_volatility_thresholds(),
            "筛选配置": self.config.get_filter_config()
        }
        
        self.filters = {
            "价格质量": QualityFilter(filter_config_data),
            "流动性门槛": VolumeFilter(filter_config_data, self.threshold_name)
        }
        
        self.logger.info(f"✅ 初始化 {len(self.filters)} 个筛选器")
    
    def process_all_etfs(self, fuquan_type: str = "0_ETF日K(前复权)", 
                        days_back: int = None, fast_mode: bool = False,
                        max_workers: int = None) -> Dict[str, Any]:
        """
        处理所有ETF数据的完整筛选流程
        
        Args:
            fuquan_type: 复权类型
            days_back: 加载最近N天的数据
            fast_mode: 启用快速模式（并行加载）
            max_workers: 最大并行工作数
        
        Returns:
            完整的处理结果
        """
        with ProcessTimer("ETF初筛处理", self.logger):
            # 1. 加载数据
            etf_codes = self.data_loader.get_available_etf_codes(fuquan_type)
            
            if not etf_codes:
                self.logger.error(f"❌ 未发现可用的ETF数据")
                return {"error": "无可用数据"}
            
            self.logger.info(f"📊 发现 {len(etf_codes)} 个ETF，开始加载数据...")
            
            # 根据快速模式选择加载方式
            if fast_mode:
                etf_data = self.data_loader.load_multiple_etfs(
                    etf_codes, fuquan_type, days_back, max_workers=max_workers
                )
            else:
                etf_data = self.data_loader.load_multiple_etfs(
                    etf_codes, fuquan_type, days_back, max_workers=1
                )
            
            if not etf_data:
                self.logger.error(f"❌ 数据加载失败")
                return {"error": "数据加载失败"}
            
            # 2. 执行筛选
            filter_results = self._run_filter_chain(etf_data)
            
            # 3. 生成最终结果
            final_results = self._generate_final_results(filter_results)
            
            # 4. 统计摘要
            process_summary = self._generate_process_summary(etf_codes, etf_data, filter_results, final_results)
            
            return {
                "复权类型": fuquan_type,
                "处理时间": datetime.now().isoformat(),
                "处理摘要": process_summary,
                "筛选结果": filter_results,
                "最终结果": final_results,
                "通过ETF": final_results["通过ETF列表"]
            }
    
    def _run_filter_chain(self, etf_data: Dict[str, pd.DataFrame]) -> Dict[str, Dict[str, FilterResult]]:
        """
        运行筛选器链
        
        Args:
            etf_data: ETF数据字典
        
        Returns:
            各筛选器的结果
        """
        filter_results = {}
        
        for filter_name, filter_obj in self.filters.items():
            self.logger.info(f"🔍 执行筛选器: {filter_name}")
            try:
                results = filter_obj.filter_multiple_etfs(etf_data)
                filter_results[filter_name] = results
                
                # 记录筛选器统计
                stats = filter_obj.get_summary_stats(results)
                self.logger.log_stats(f"{filter_name}统计", stats)
                
            except Exception as e:
                self.logger.error(f"❌ 筛选器 {filter_name} 执行失败: {e}")
                filter_results[filter_name] = {}
        
        return filter_results
    
    def _generate_final_results(self, filter_results: Dict[str, Dict[str, FilterResult]]) -> Dict[str, Any]:
        """
        生成最终筛选结果
        
        Args:
            filter_results: 各筛选器的结果
        
        Returns:
            最终结果字典
        """
        if not filter_results:
            return {"通过ETF列表": [], "综合评分": {}}
        
        # 获取所有ETF代码
        all_etf_codes = set()
        for results in filter_results.values():
            all_etf_codes.update(results.keys())
        
        # 计算综合评分和通过情况
        comprehensive_scores = {}
        pass_statistics = {}
        
        for etf_code in all_etf_codes:
            etf_scores = {}
            etf_passed = {}
            
            for filter_name, results in filter_results.items():
                if etf_code in results:
                    result = results[etf_code]
                    etf_scores[filter_name] = result.score
                    etf_passed[filter_name] = result.passed
                else:
                    etf_scores[filter_name] = 0.0
                    etf_passed[filter_name] = False
            
            # 计算加权综合得分
            weighted_score = self._calculate_weighted_score(etf_scores)
            passed_filter_count = sum(etf_passed.values())
            
            comprehensive_scores[etf_code] = {
                "综合得分": weighted_score,
                "各筛选器得分": etf_scores,
                "通过筛选器数": passed_filter_count,
                "总筛选器数": len(self.filters),
                "通过率": passed_filter_count / len(self.filters) * 100,
                "各筛选器通过情况": etf_passed
            }
            
            pass_statistics[etf_code] = passed_filter_count
        
        # 确定最终通过的ETF（需要通过所有筛选器）
        passed_etf_list = [
            etf_code for etf_code, count in pass_statistics.items() 
            if count == len(self.filters)
        ]
        
        # 按综合得分排序
        passed_etf_list.sort(key=lambda x: comprehensive_scores[x]["综合得分"], reverse=True)
        
        return {
            "通过ETF列表": passed_etf_list,
            "候选ETF列表": self._get_candidate_etfs(pass_statistics),
            "综合评分": comprehensive_scores,
            "筛选统计": {
                "完全通过": len(passed_etf_list),
                "部分通过": len([k for k, v in pass_statistics.items() if 0 < v < len(self.filters)]),
                "完全未通过": len([k for k, v in pass_statistics.items() if v == 0]),
                "总ETF数": len(all_etf_codes)
            }
        }
    
    def _calculate_weighted_score(self, scores: Dict[str, float]) -> float:
        """
        计算加权综合得分
        
        Args:
            scores: 各筛选器得分
        
        Returns:
            加权综合得分
        """
        # 从配置文件读取权重设置
        weights = self.config.get_filter_weights()
        
        weighted_score = 0.0
        total_weight = 0.0
        
        for filter_name, score in scores.items():
            weight = weights.get(filter_name, 0.33)  # 默认权重
            weighted_score += score * weight
            total_weight += weight
        
        return weighted_score / total_weight if total_weight > 0 else 0.0
    
    def _get_candidate_etfs(self, pass_statistics: Dict[str, int]) -> List[str]:
        """
        获取候选ETF列表（通过1个筛选器但不是全部通过）
        
        Args:
            pass_statistics: ETF通过筛选器数量统计
        
        Returns:
            候选ETF列表
        """
        candidate_etfs = [
            etf_code for etf_code, count in pass_statistics.items()
            if count >= 1 and count < len(self.filters)
        ]
        
        return sorted(candidate_etfs)
    
    def _generate_process_summary(self, all_etf_codes: List[str], 
                                etf_data: Dict[str, pd.DataFrame],
                                filter_results: Dict[str, Dict[str, FilterResult]],
                                final_results: Dict[str, Any]) -> Dict[str, Any]:
        """
        生成处理摘要
        
        Args:
            all_etf_codes: 所有ETF代码
            etf_data: 加载的ETF数据
            filter_results: 筛选结果
            final_results: 最终结果
        
        Returns:
            处理摘要
        """
        return {
            "数据加载": {
                "发现ETF总数": len(all_etf_codes),
                "成功加载数": len(etf_data),
                "加载成功率": len(etf_data) / len(all_etf_codes) * 100 if all_etf_codes else 0
            },
            "筛选器执行": {
                "筛选器总数": len(self.filters),
                "执行成功数": len(filter_results),
                "筛选器列表": list(self.filters.keys())
            },
            "筛选结果": final_results["筛选统计"],
            "数据质量": {
                "数据完整性": "良好" if len(etf_data) / len(all_etf_codes) > 0.9 else "一般",
                "数据时效性": "当日" if datetime.now().hour < 16 else "最新"
            }
        }
    
    def get_filter_descriptions(self) -> Dict[str, Any]:
        """获取所有筛选器的说明"""
        descriptions = {}
        for name, filter_obj in self.filters.items():
            if hasattr(filter_obj, 'get_filter_description'):
                descriptions[name] = filter_obj.get_filter_description()
        return descriptions
    
    def process_specific_etfs(self, etf_codes: List[str], 
                            fuquan_type: str = "0_ETF日K(前复权)",
                            days_back: int = None, fast_mode: bool = False,
                            max_workers: int = None) -> Dict[str, Any]:
        """
        处理指定的ETF列表
        
        Args:
            etf_codes: 指定的ETF代码列表
            fuquan_type: 复权类型
            days_back: 加载最近N天的数据
            fast_mode: 启用快速模式（并行加载）
            max_workers: 最大并行工作数
        
        Returns:
            处理结果
        """
        self.logger.info(f"🎯 开始处理指定的 {len(etf_codes)} 个ETF")
        
        # 根据快速模式选择加载方式
        if fast_mode:
            etf_data = self.data_loader.load_multiple_etfs(
                etf_codes, fuquan_type, days_back, max_workers=max_workers
            )
        else:
            etf_data = self.data_loader.load_multiple_etfs(
                etf_codes, fuquan_type, days_back, max_workers=1
            )
        
        if not etf_data:
            return {"error": "指定ETF数据加载失败"}
        
        # 执行筛选
        filter_results = self._run_filter_chain(etf_data)
        final_results = self._generate_final_results(filter_results)
        process_summary = self._generate_process_summary(etf_codes, etf_data, filter_results, final_results)
        
        return {
            "复权类型": fuquan_type,
            "处理时间": datetime.now().isoformat(),
            "处理摘要": process_summary,
            "筛选结果": filter_results,
            "最终结果": final_results,
            "通过ETF": final_results["通过ETF列表"]
        }
    
    def process_loaded_etfs(self, etf_data: Dict[str, pd.DataFrame], 
                           fuquan_type: str = "0_ETF日K(前复权)") -> Dict[str, Any]:
        """
        处理已加载的ETF数据（优化版，避免重复加载）
        
        Args:
            etf_data: 已加载的ETF数据字典
            fuquan_type: 复权类型（仅用于结果记录）
        
        Returns:
            完整的处理结果
        """
        with ProcessTimer("ETF初筛处理", self.logger):
            if not etf_data:
                self.logger.error(f"❌ 传入的ETF数据为空")
                return {"error": "ETF数据为空"}
            
            self.logger.info(f"📊 开始处理已加载的 {len(etf_data)} 个ETF数据...")
            
            # 1. 执行筛选
            filter_results = self._run_filter_chain(etf_data)
            
            # 2. 生成最终结果
            final_results = self._generate_final_results(filter_results)
            
            # 3. 统计摘要
            all_etf_codes = list(etf_data.keys())
            process_summary = self._generate_process_summary(all_etf_codes, etf_data, filter_results, final_results)
            
            return {
                "复权类型": fuquan_type,
                "处理时间": datetime.now().isoformat(),
                "处理摘要": process_summary,
                "筛选结果": filter_results,
                "最终结果": final_results,
                "通过ETF": final_results["通过ETF列表"]
            } 