#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
输出接口定义
===========

定义PV价量协调系统输出处理的标准接口，支持CSV输出、显示格式化和统计计算
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any, Union
from dataclasses import dataclass
from enum import Enum
import pandas as pd


class OutputFormat(Enum):
    """输出格式枚举"""
    CSV = "csv"
    JSON = "json"
    EXCEL = "excel"
    CONSOLE = "console"
    HTML = "html"


class OutputStatus(Enum):
    """输出状态枚举"""
    SUCCESS = "success"
    FAILED = "failed"
    PARTIAL = "partial"


@dataclass
class OutputResult:
    """输出结果"""
    status: OutputStatus
    format: OutputFormat
    output_path: Optional[str] = None
    records_count: Optional[int] = None
    metadata: Optional[Dict] = None
    error_message: Optional[str] = None
    
    @property
    def is_success(self) -> bool:
        """是否输出成功"""
        return self.status == OutputStatus.SUCCESS


class IOutputHandler(ABC):
    """输出处理器基础接口"""
    
    @abstractmethod
    def write(self, data: Any, output_path: str, options: Optional[Dict] = None) -> OutputResult:
        """
        写入数据
        
        Args:
            data: 要输出的数据
            output_path: 输出路径
            options: 输出选项
            
        Returns:
            OutputResult: 输出结果
        """
        pass
    
    @abstractmethod
    def get_supported_formats(self) -> List[OutputFormat]:
        """
        获取支持的输出格式
        
        Returns:
            List[OutputFormat]: 支持的格式列表
        """
        pass
    
    @abstractmethod
    def validate_output_path(self, output_path: str) -> bool:
        """
        验证输出路径是否有效
        
        Args:
            output_path: 输出路径
            
        Returns:
            bool: 是否有效
        """
        pass


class IPVCSVHandler(IOutputHandler):
    """PV价量协调CSV输出处理器接口"""
    
    @abstractmethod
    def write_csv(self, data: pd.DataFrame, output_path: str, 
                  encoding: str = 'utf-8', index: bool = False) -> OutputResult:
        """
        写入CSV文件
        
        Args:
            data: DataFrame数据
            output_path: 输出路径
            encoding: 编码格式
            index: 是否包含索引
            
        Returns:
            OutputResult: 输出结果
        """
        pass
    
    @abstractmethod
    def append_csv(self, data: pd.DataFrame, output_path: str) -> OutputResult:
        """
        追加数据到CSV文件
        
        Args:
            data: DataFrame数据
            output_path: 输出路径
            
        Returns:
            OutputResult: 输出结果
        """
        pass

    @abstractmethod
    def save_pv_indicators_csv(self, data: pd.DataFrame, output_path: str,
                              etf_code: str = None) -> OutputResult:
        """
        保存PV指标到CSV文件（包含10个PV指标字段）
        
        Args:
            data: PV指标数据
            output_path: 输出路径
            etf_code: ETF代码
            
        Returns:
            OutputResult: 输出结果
        """
        pass

    @abstractmethod
    def validate_pv_csv_format(self, file_path: str) -> Dict[str, Any]:
        """
        验证PV CSV文件格式
        
        Args:
            file_path: CSV文件路径
            
        Returns:
            Dict: 验证结果
        """
        pass


class IPVDisplayFormatter(ABC):
    """PV价量协调显示格式化器接口"""
    
    @abstractmethod
    def format_summary(self, data: Dict[str, Any]) -> str:
        """
        格式化汇总信息
        
        Args:
            data: 汇总数据
            
        Returns:
            str: 格式化后的字符串
        """
        pass
    
    @abstractmethod
    def format_progress(self, current: int, total: int, 
                       message: str = "") -> str:
        """
        格式化进度信息
        
        Args:
            current: 当前进度
            total: 总数
            message: 附加消息
            
        Returns:
            str: 格式化后的进度字符串
        """
        pass
    
    @abstractmethod
    def format_table(self, data: List[Dict], headers: Optional[List[str]] = None) -> str:
        """
        格式化表格显示
        
        Args:
            data: 表格数据
            headers: 表头列表
            
        Returns:
            str: 格式化后的表格字符串
        """
        pass
    
    @abstractmethod
    def format_cache_statistics(self, stats: Dict[str, Any]) -> str:
        """
        格式化缓存统计信息
        
        Args:
            stats: 统计数据
            
        Returns:
            str: 格式化后的统计信息
        """
        pass

    @abstractmethod
    def format_pv_analysis_summary(self, result: pd.DataFrame, etf_code: str) -> str:
        """
        格式化PV分析摘要
        
        Args:
            result: PV计算结果
            etf_code: ETF代码
            
        Returns:
            str: 格式化的分析摘要
        """
        pass


class IPVStatisticsCalculator(ABC):
    """PV价量协调统计计算器接口"""
    
    @abstractmethod
    def calculate_basic_stats(self, data: List[Dict]) -> Dict[str, Any]:
        """
        计算基础统计信息
        
        Args:
            data: 数据列表
            
        Returns:
            Dict: 统计结果
        """
        pass
    
    @abstractmethod
    def calculate_performance_stats(self, processing_results: List[Dict]) -> Dict[str, Any]:
        """
        计算性能统计信息
        
        Args:
            processing_results: 处理结果列表
            
        Returns:
            Dict: 性能统计
        """
        pass
    
    @abstractmethod
    def calculate_cache_hit_rate(self, cache_hits: int, total_requests: int) -> float:
        """
        计算缓存命中率
        
        Args:
            cache_hits: 缓存命中次数
            total_requests: 总请求次数
            
        Returns:
            float: 命中率百分比
        """
        pass

    @abstractmethod
    def calculate_pv_correlation_stats(self, pv_data: pd.DataFrame) -> Dict[str, Any]:
        """
        计算价量相关性统计
        
        Args:
            pv_data: PV数据
            
        Returns:
            Dict: 相关性统计结果
        """
        pass

    @abstractmethod
    def calculate_vpt_trend_stats(self, pv_data: pd.DataFrame) -> Dict[str, Any]:
        """
        计算VPT趋势统计
        
        Args:
            pv_data: PV数据
            
        Returns:
            Dict: VPT趋势统计结果
        """
        pass

    @abstractmethod
    def calculate_volume_quality_stats(self, pv_data: pd.DataFrame) -> Dict[str, Any]:
        """
        计算成交量质量统计
        
        Args:
            pv_data: PV数据
            
        Returns:
            Dict: 成交量质量统计结果
        """
        pass
    
    @abstractmethod
    def generate_summary_report(self, results: List[Dict], 
                               cache_stats: Optional[Dict] = None) -> Dict[str, Any]:
        """
        生成汇总报告
        
        Args:
            results: 处理结果列表
            cache_stats: 缓存统计信息
            
        Returns:
            Dict: 汇总报告
        """
        pass


class IPVResultBuilder(ABC):
    """PV价量协调结果构建器接口"""
    
    @abstractmethod
    def build_etf_result(self, etf_code: str, pv_data: pd.DataFrame, 
                        metadata: Optional[Dict] = None) -> Dict[str, Any]:
        """
        构建单ETF的PV结果
        
        Args:
            etf_code: ETF代码
            pv_data: PV数据
            metadata: 元数据
            
        Returns:
            Dict: ETF结果
        """
        pass
    
    @abstractmethod
    def build_batch_result(self, etf_results: List[Dict], 
                          summary_stats: Dict[str, Any]) -> Dict[str, Any]:
        """
        构建批量处理结果
        
        Args:
            etf_results: ETF结果列表
            summary_stats: 汇总统计
            
        Returns:
            Dict: 批量结果
        """
        pass
    
    @abstractmethod
    def build_error_result(self, error_message: str, 
                          context: Optional[Dict] = None) -> Dict[str, Any]:
        """
        构建错误结果
        
        Args:
            error_message: 错误信息
            context: 错误上下文
            
        Returns:
            Dict: 错误结果
        """
        pass

    @abstractmethod
    def build_pv_analysis_result(self, etf_code: str, pv_data: pd.DataFrame,
                                correlation_stats: Dict[str, Any],
                                vpt_stats: Dict[str, Any],
                                quality_stats: Dict[str, Any]) -> Dict[str, Any]:
        """
        构建PV分析结果
        
        Args:
            etf_code: ETF代码
            pv_data: PV数据
            correlation_stats: 相关性统计
            vpt_stats: VPT统计
            quality_stats: 质量统计
            
        Returns:
            Dict: PV分析结果
        """
        pass


class IPVValidationInterface(ABC):
    """PV价量协调验证接口"""

    @abstractmethod
    def validate_pv_indicators(self, pv_data: pd.DataFrame) -> Dict[str, Any]:
        """
        验证PV指标数据
        
        Args:
            pv_data: PV指标数据
            
        Returns:
            Dict: 验证结果
        """
        pass

    @abstractmethod
    def validate_correlation_values(self, correlation_data: pd.Series) -> Dict[str, Any]:
        """
        验证相关性数值
        
        Args:
            correlation_data: 相关性数据
            
        Returns:
            Dict: 验证结果
        """
        pass

    @abstractmethod
    def validate_vpt_values(self, vpt_data: pd.DataFrame) -> Dict[str, Any]:
        """
        验证VPT数值
        
        Args:
            vpt_data: VPT数据
            
        Returns:
            Dict: 验证结果
        """
        pass

    @abstractmethod
    def validate_volume_quality_values(self, quality_data: pd.DataFrame) -> Dict[str, Any]:
        """
        验证成交量质量数值
        
        Args:
            quality_data: 质量数据
            
        Returns:
            Dict: 验证结果
        """
        pass