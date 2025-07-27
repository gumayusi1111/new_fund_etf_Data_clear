"""
价量配合度系统配置管理
==================

管理价量配合度系统的所有配置参数，包括计算参数、文件路径、缓存配置等
基于中国A股市场特征调整的专业配置
"""

import os
from pathlib import Path
from typing import Dict, List, Any

class PVConfig:
    """价量配合度系统配置管理器"""

    def __init__(self):
        # 获取项目根目录
        self.current_dir = Path(__file__).parent.parent.parent
        self.project_root = Path("/Users/wenbai/Desktop/金融/data_clear")

        # 价量配合度计算参数
        self.correlation_periods = [10, 20, 30]  # 价量相关性分析周期
        self.vpt_periods = [5, 10, 20]  # VPT动量分析周期
        self.volume_quality_window = 20  # 成交量质量评估窗口
        self.pv_strength_window = 5  # 价量强度计算窗口
        self.pv_divergence_window = 10  # 价量背离检测窗口

        # 数据质量和预热期配置
        self.min_data_points = 35  # 最少需要35个数据点(适应最长30日周期)
        self.warmup_period = self._calculate_warmup_period()  # 动态计算预热期
        self.precision_digits = 8  # 计算精度位数
        self.min_data_completeness = 0.95  # 最小数据完整性要求

        # 中国市场筛选阈值(基于实证调整)
        self.screening_thresholds = {
            # A级优选标准
            'grade_a': {
                'volume_quality_min': 80,
                'pv_divergence_max': 15,
                'pv_corr_10_min': 0.5,
                'pv_corr_20_min': 0.4,
                'vpt_positive': True,
                'volume_consistency_min': 50
            },
            # B级合格标准
            'grade_b': {
                'volume_quality_min': 60,
                'pv_divergence_max': 30,
                'pv_corr_20_min': 0.3,
                'pv_strength_min': 0.6
            },
            # C级观察标准
            'grade_c': {
                'volume_quality_min': 40,
                'pv_divergence_max': 50,
                'pv_corr_10_min': 0.2
            },
            # D级排除标准
            'grade_d': {
                'volume_quality_max': 40,
                'pv_divergence_min': 50,
                'pv_corr_10_max': 0.1
            }
        }

        # 量价配合模式阈值
        self.pv_pattern_thresholds = {
            'price_volume_sync': 0.6,      # 价量同步阈值
            'price_volume_diverge': -0.3,   # 价量背离阈值
            'volume_expansion': 1.5,        # 成交量放大阈值
            'volume_contraction': 0.7,      # 成交量萎缩阈值
            'trend_confirmation': 0.5,      # 趋势确认阈值
            'reversal_warning': 0.8         # 反转预警阈值
        }

        # 基础路径
        self.base_path = self.current_dir

        # 快捷访问属性
        self.decimal_precision = self.precision_digits
        self.source_data_path = self.current_dir / "data"
        self.output_path = self.current_dir / "output"
        self.cache_path = self.current_dir / "cache"

        # 批处理配置
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

        # ETF筛选列表配置
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
            'file_path': self.current_dir / 'logs/pv_system.log'
        }

        # 数据质量配置
        self.quality_config = {
            'min_completeness': 95,         # 最低数据完整性要求(%)
            'max_zero_ratio': 10,          # 最大零值比例(%)
            'outlier_threshold': 3,         # 异常值检测阈值(标准差倍数)
            'min_trading_days': 60          # 最少交易日数
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
            'default_timeout_seconds': 300,
            'success_rate_threshold': 80,
            'default_max_workers': 4,
            'thresholds': ['3000万门槛', '5000万门槛']
        }

        # 数据格式配置
        self.data_format_config = {
            'source_date_format': '%Y%m%d',
            'output_date_format': '%Y-%m-%d',
            'outlier_detection_sigma': 3
        }

        # 计算配置
        self.calculation_config = {
            'incremental_window_days': 60
        }

        # 确保目录存在
        self._ensure_directories()

    def _calculate_warmup_period(self) -> int:
        """
        动态计算预热期长度
        
        价量配合度需要考虑：
        1. 最长相关性分析周期
        2. VPT累积计算需要的历史数据
        3. 成交量质量评估窗口
        4. 稳定性缓冲
        """
        max_correlation_period = max(self.correlation_periods)  # 30
        max_vpt_period = max(self.vpt_periods)  # 20
        volume_quality_window = self.volume_quality_window  # 20
        stability_buffer = 15  # 价量指标需要更多缓冲

        warmup_period = max(
            max_correlation_period,
            max_vpt_period,
            volume_quality_window
        ) + stability_buffer

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
        """
        try:
            # 首先尝试从筛选列表读取
            filter_list_path = self.etf_filter_lists.get(threshold)
            if filter_list_path and filter_list_path.exists():
                etf_codes = []
                with open(filter_list_path, 'r', encoding='utf-8') as f:
                    for line in f:
                        line = line.strip()
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

            # 回退到原来的逻辑
            data_dir = self.data_source['data_dirs'][threshold]
            if not data_dir.exists():
                return []

            etf_files = list(data_dir.glob("*.csv"))
            etf_codes = [f.stem for f in etf_files]
            return sorted(etf_codes)

        except Exception as e:
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

    def get_screening_grade(self, indicators: Dict[str, float]) -> str:
        """
        根据计算指标判断筛选等级
        
        Args:
            indicators: 包含所有价量配合度指标的字典
            
        Returns:
            筛选等级: 'A', 'B', 'C', 'D'
        """
        # A级优选判断
        grade_a = self.screening_thresholds['grade_a']
        if (indicators.get('volume_quality', 0) > grade_a['volume_quality_min'] and
            indicators.get('pv_divergence', 100) < grade_a['pv_divergence_max'] and
            indicators.get('pv_corr_10', 0) > grade_a['pv_corr_10_min'] and
            indicators.get('pv_corr_20', 0) > grade_a['pv_corr_20_min'] and
            indicators.get('vpt', 0) > 0 and
            indicators.get('volume_consistency', 0) > grade_a['volume_consistency_min']):
            return 'A'

        # B级合格判断
        grade_b = self.screening_thresholds['grade_b']
        if (indicators.get('volume_quality', 0) > grade_b['volume_quality_min'] and
            indicators.get('pv_divergence', 100) < grade_b['pv_divergence_max'] and
            indicators.get('pv_corr_20', 0) > grade_b['pv_corr_20_min'] and
            indicators.get('pv_strength', 0) > grade_b['pv_strength_min']):
            return 'B'

        # C级观察判断
        grade_c = self.screening_thresholds['grade_c']
        if (indicators.get('volume_quality', 0) > grade_c['volume_quality_min'] and
            indicators.get('pv_divergence', 100) < grade_c['pv_divergence_max'] and
            indicators.get('pv_corr_10', 0) > grade_c['pv_corr_10_min']):
            return 'C'

        # 其他情况为D级
        return 'D'

    def get_china_market_score(self, indicators: Dict[str, float]) -> float:
        """
        计算中国市场专用评分
        
        Args:
            indicators: 包含所有价量配合度指标的字典
            
        Returns:
            中国市场评分(0-100)
        """
        score = 0
        
        # 短期相关性权重25%
        score += indicators.get('pv_corr_10', 0) * 25
        
        # 中期相关性权重15%
        score += indicators.get('pv_corr_20', 0) * 15
        
        # 成交量质量权重35%
        score += (indicators.get('volume_quality', 0) / 100) * 35
        
        # 背离预警权重20%
        score += max(0, (100 - indicators.get('pv_divergence', 100)) / 100) * 20
        
        # 一致性权重5%
        score += (indicators.get('volume_consistency', 0) / 100) * 5
        
        return min(100, score)

    def to_dict(self) -> Dict[str, Any]:
        """将配置转换为字典"""
        return {
            'correlation_periods': self.correlation_periods,
            'vpt_periods': self.vpt_periods,
            'volume_quality_window': self.volume_quality_window,
            'pv_strength_window': self.pv_strength_window,
            'pv_divergence_window': self.pv_divergence_window,
            'screening_thresholds': self.screening_thresholds,
            'pv_pattern_thresholds': self.pv_pattern_thresholds,
            'cache_config': self.cache_config,
            'performance_config': self.performance_config,
            'quality_config': self.quality_config
        }