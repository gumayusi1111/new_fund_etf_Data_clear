"""
VMA系统配置管理
===============

管理VMA系统的所有配置参数，包括计算参数、文件路径、缓存配置等
"""

import os
from pathlib import Path
from typing import Dict, List, Any

class VMAConfig:
    """VMA系统配置管理器"""

    def __init__(self):
        # 获取项目根目录
        self.current_dir = Path(__file__).parent.parent.parent
        self.project_root = Path("/Users/wenbai/Desktop/金融/data_clear")

        # VMA计算参数
        self.vma_periods = [5, 10, 20]
        self.ratio_periods = [5, 10, 20]
        self.activity_window = 20

        # 数据质量和预热期配置
        self.min_data_points = 30  # 最少需要30个数据点
        self.warmup_period = self._calculate_warmup_period()  # 动态计算预热期
        self.precision_digits = 8  # 计算精度位数
        self.min_data_completeness = 0.95  # 最小数据完整性要求

        # 阈值配置(基于中国A股实证标准)
        self.volume_thresholds = {
            'warm_volume_min': 1.5,     # 温和放量下限
            'warm_volume_max': 2.5,     # 温和放量上限
            'obvious_volume_min': 2.5,  # 明显放量下限
            'obvious_volume_max': 5.0,  # 明显放量上限
            'normal_min': 0.8,          # 正常成交下限
            'normal_max': 1.5,          # 正常成交上限
            'shrink_min': 0.5,          # 成交萎缩下限
            'shrink_max': 0.8,          # 成交萎缩上限
            'severe_shrink_max': 0.5    # 严重缩量上限
        }

        # 趋势判断阈值
        self.trend_thresholds = {
            'short_enhance': 1.05,      # 短期量能增强
            'short_weaken': 0.95,       # 短期量能减弱
            'medium_enhance': 1.1,      # 中期量能增强
            'medium_weaken': 0.9        # 中期量能减弱
        }

        # 基础路径（用于资源监控等）
        self.base_path = self.current_dir

        # 快捷访问属性（向后兼容）
        self.decimal_precision = self.precision_digits
        self.source_data_path = self.current_dir / "data"
        self.output_path = self.current_dir / "output"
        self.cache_path = self.current_dir / "cache"

        # 批处理快捷属性
        self.batch_timeout_seconds = 300
        self.max_workers = 4
        self.success_rate_threshold = 0.8

        # 数据源配置
        self.data_source = {
            'base_path': self.project_root / "ETF日更",
            'daily_path': self.project_root / "ETF日更",
            'data_dirs': {
                '3000万门槛': self.project_root / "ETF日更/0_ETF日K(前复权)",
                '5000万门槛': self.project_root / "ETF日更/0_ETF日K(前复权)"
            }
        }

        # ETF筛选列表配置（来自ETF_初筛系统）
        self.etf_filter_lists = {
            '3000万门槛': self.project_root / "ETF_初筛/data/3000万门槛/通过筛选ETF.txt",
            '5000万门槛': self.project_root / "ETF_初筛/data/5000万门槛/通过筛选ETF.txt"
        }

        # 输出配置
        self.output_config = {
            'base_dir': self.current_dir,
            'data_dirs': {
                '3000万门槛': self.current_dir / "data/3000万门槛",
                '5000万门槛': self.current_dir / "data/5000万门槛"
            },
            'cache_dirs': {
                '3000万门槛': self.current_dir / "cache/3000万门槛",
                '5000万门槛': self.current_dir / "cache/5000万门槛"
            },
            'meta_dir': self.current_dir / "cache/meta"
        }

        # 缓存配置
        self.cache_config = {
            'enabled': True,
            'cache_validity_hours': 24,
            'max_cache_size_mb': 500,
            'cleanup_threshold': 0.8
        }

        # 性能配置
        self.performance_config = {
            'batch_size': 100,
            'parallel_workers': 4,
            'memory_limit_mb': 1024,
            'chunk_size': 10000
        }

        # 日志配置
        self.logging_config = {
            'level': 'INFO',
            'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            'file_path': self.current_dir / 'logs/vma_system.log'
        }

        # 数据质量配置
        self.quality_config = {
            'min_completeness': 95,     # 最低数据完整性要求(%)
            'max_zero_ratio': 10,       # 最大零值比例(%)
            'outlier_threshold': 3,     # 异常值检测阈值(标准差倍数)
            'min_trading_days': 60      # 最少交易日数
        }

        # ETF门槛配置
        self.etf_thresholds = {
            '3000万门槛': {
                'min_amount': 30_000_000,
                'description': '中等规模ETF'
            },
            '5000万门槛': {
                'min_amount': 50_000_000,
                'description': '大规模ETF'
            }
        }

        # 批量处理配置
        self.batch_config = {
            'default_timeout_seconds': 300,        # 单个ETF处理超时时间
            'success_rate_threshold': 80,          # 批处理成功率阈值(%)
            'default_max_workers': 4,              # 默认并行工作线程数
            'thresholds': ['3000万门槛', '5000万门槛']  # 支持的门槛类型
        }

        # 缓存完整性配置
        self.cache_integrity_config = {
            'file_size_tolerance_bytes': 1024,     # 文件大小验证容忍度
            'csv_header_prefix': 'code,date,vma_', # CSV文件头部验证前缀
        }

        # 数据格式配置
        self.data_format_config = {
            'source_date_format': '%Y%m%d',        # 源数据日期格式
            'output_date_format': '%Y-%m-%d',      # 输出日期格式
            'outlier_detection_sigma': 3           # 异常值检测σ倍数
        }

        # 文件清理配置
        self.file_cleanup_config = {
            'default_cleanup_days': 30,           # 默认文件清理天数
            'seconds_per_day': 86400               # 每天秒数
        }

        # 系统测试配置
        self.system_test_config = {
            'success_rate_threshold': 80,         # 系统测试成功率阈值(%)
            'sample_size': 5                      # 默认测试样本数量
        }

        # 计算配置
        self.calculation_config = {
            'incremental_window_days': 60         # 增量计算时间窗口
        }

        # 确保目录存在
        self._ensure_directories()

    def _calculate_warmup_period(self) -> int:
        """
        动态计算预热期长度

        预热期需要考虑：
        1. 最长移动平均线周期 (max_vma_period)
        2. 活跃度计算窗口 (activity_window)
        3. 复合指标稳定性缓冲 (buffer)

        Returns:
            预热期长度（天数）
        """
        max_vma_period = max(self.vma_periods)  # 20
        activity_window = self.activity_window   # 20
        stability_buffer = 10  # 额外缓冲确保复合指标稳定

        # 预热期 = max(最长移动平均线周期, 活跃度窗口) + 稳定性缓冲
        warmup_period = max(max_vma_period, activity_window) + stability_buffer

        return warmup_period

    def get_warmup_period(self) -> int:
        """获取预热期长度"""
        return self.warmup_period

    def get_precision_digits(self) -> int:
        """获取计算精度位数"""
        return self.precision_digits

    def get_min_data_completeness(self) -> float:
        """获取最小数据完整性要求"""
        return self.min_data_completeness

    def get_batch_timeout(self) -> int:
        """获取批量处理超时时间"""
        return self.batch_config['default_timeout_seconds']

    def get_success_rate_threshold(self) -> float:
        """获取成功率阈值(0-1之间)"""
        return self.batch_config['success_rate_threshold'] / 100.0

    def get_max_workers(self) -> int:
        """获取默认最大工作线程数"""
        return self.batch_config['default_max_workers']

    def get_supported_thresholds(self) -> list:
        """获取支持的门槛类型列表"""
        return self.batch_config['thresholds'].copy()

    def get_file_size_tolerance(self) -> int:
        """获取文件大小验证容忍度"""
        return self.cache_integrity_config['file_size_tolerance_bytes']

    def get_csv_header_prefix(self) -> str:
        """获取CSV文件头部验证前缀"""
        return self.cache_integrity_config['csv_header_prefix']

    def get_outlier_detection_sigma(self) -> float:
        """获取异常值检测σ倍数"""
        return self.data_format_config['outlier_detection_sigma']

    def get_incremental_window_days(self) -> int:
        """获取增量计算时间窗口天数"""
        return self.calculation_config['incremental_window_days']

    def _ensure_directories(self):
        """确保所有必要目录存在"""
        dirs_to_create = []

        # 输出目录
        for threshold_dir in self.output_config['data_dirs'].values():
            dirs_to_create.append(threshold_dir)

        # 缓存目录
        for cache_dir in self.output_config['cache_dirs'].values():
            dirs_to_create.append(cache_dir)

        # Meta目录
        dirs_to_create.append(self.output_config['meta_dir'])

        # 日志目录
        dirs_to_create.append(self.logging_config['file_path'].parent)

        # 创建目录
        for dir_path in dirs_to_create:
            dir_path.mkdir(parents=True, exist_ok=True)

    def get_etf_list(self, threshold: str) -> List[str]:
        """
        获取指定门槛的ETF列表（使用ETF_初筛系统筛选后的结果）

        Args:
            threshold: '3000万门槛' 或 '5000万门槛'

        Returns:
            ETF代码列表
        """
        try:
            # 首先尝试从筛选列表读取
            filter_list_path = self.etf_filter_lists.get(threshold)
            if filter_list_path and filter_list_path.exists():
                etf_codes = []
                with open(filter_list_path, 'r', encoding='utf-8') as f:
                    for line in f:
                        line = line.strip()
                        # 跳过注释行和空行
                        if line and not line.startswith('#'):
                            etf_codes.append(line)

                # 验证数据文件是否存在
                data_dir = self.data_source['data_dirs'][threshold]
                valid_etf_codes = []
                for etf_code in etf_codes:
                    etf_file = data_dir / f"{etf_code}.csv"
                    if etf_file.exists():
                        valid_etf_codes.append(etf_code)

                if valid_etf_codes:
                    return sorted(valid_etf_codes)

            # 回退：如果筛选列表不存在，使用原来的逻辑
            data_dir = self.data_source['data_dirs'][threshold]
            if not data_dir.exists():
                return []

            etf_files = list(data_dir.glob("*.csv"))
            etf_codes = [f.stem for f in etf_files]
            return sorted(etf_codes)

        except Exception as e:
            # 发生错误时回退到原来的逻辑
            print(f"警告：读取ETF筛选列表失败 {threshold}: {e}")
            data_dir = self.data_source['data_dirs'][threshold]
            if not data_dir.exists():
                return []

            etf_files = list(data_dir.glob("*.csv"))
            etf_codes = [f.stem for f in etf_files]
            return sorted(etf_codes)

    def get_output_path(self, threshold: str, etf_code: str) -> Path:
        """获取输出文件路径"""
        output_dir = self.output_config['data_dirs'][threshold]
        return output_dir / f"{etf_code}.csv"

    def get_cache_path(self, threshold: str, etf_code: str) -> Path:
        """获取缓存文件路径"""
        cache_dir = self.output_config['cache_dirs'][threshold]
        return cache_dir / f"{etf_code}.csv"

    def get_source_path(self, threshold: str, etf_code: str) -> Path:
        """获取源数据文件路径"""
        source_dir = self.data_source['data_dirs'][threshold]
        return source_dir / f"{etf_code}.csv"

    def get_etf_filter_stats(self) -> Dict[str, Any]:
        """获取ETF筛选统计信息"""
        stats = {}

        for threshold in ['3000万门槛', '5000万门槛']:
            filter_list_path = self.etf_filter_lists.get(threshold)
            data_dir = self.data_source['data_dirs'][threshold]

            # 统计筛选后的ETF数量
            filtered_count = 0
            if filter_list_path and filter_list_path.exists():
                with open(filter_list_path, 'r', encoding='utf-8') as f:
                    for line in f:
                        line = line.strip()
                        if line and not line.startswith('#'):
                            filtered_count += 1

            # 统计所有ETF数量
            total_count = 0
            if data_dir.exists():
                total_count = len(list(data_dir.glob("*.csv")))

            stats[threshold] = {
                'filtered_count': filtered_count,
                'total_count': total_count,
                'filter_rate': round(filtered_count / total_count * 100, 1) if total_count > 0 else 0,
                'filter_list_exists': filter_list_path.exists() if filter_list_path else False
            }

        return stats

    def to_dict(self) -> Dict[str, Any]:
        """将配置转换为字典"""
        return {
            'vma_periods': self.vma_periods,
            'ratio_periods': self.ratio_periods,
            'activity_window': self.activity_window,
            'volume_thresholds': self.volume_thresholds,
            'trend_thresholds': self.trend_thresholds,
            'cache_config': self.cache_config,
            'performance_config': self.performance_config,
            'quality_config': self.quality_config,
            'etf_filter_stats': self.get_etf_filter_stats()
        }