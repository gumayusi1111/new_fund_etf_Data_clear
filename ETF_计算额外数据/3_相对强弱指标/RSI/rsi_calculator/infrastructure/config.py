"""
RSI指标配置管理系统
基于威廉指标的统一配置架构

RSI系统配置管理类，提供：
- 复权类型配置和科学评估
- 双门槛处理配置
- RSI指标参数配置
- 智能路径检测和管理
- 系统性能参数配置
"""

import os
from datetime import datetime


class RSIConfig:
    """RSI指标系统配置管理类"""

    def __init__(self, adj_type="前复权"):
        """
        初始化RSI指标配置
        
        Args:
            adj_type: 复权类型，默认为"前复权"
        """
        self.adj_type = adj_type
        self.base_path = os.path.dirname(os.path.abspath(__file__))
        
        # 初始化路径配置
        self._setup_paths()
        
        # 初始化系统配置
        self._setup_system_config()

    # ==================== 复权类型配置 ====================
    
    ADJ_TYPES = {
        "前复权": "0_ETF日K(前复权)",
        "后复权": "0_ETF日K(后复权)",
        "除权": "0_ETF日K(除权)"
    }

    # 科学评估系统 - 基于统计学和金融理论
    ADJ_TYPE_SCIENTIFIC_EVALUATION = {
        "前复权": {
            "scientific_score": 95,
            "recommendation": "强烈推荐",
            "理论基础": "消除除权影响的连续价格序列",
            "技术指标适用性": "最适合技术分析和指标计算",
            "数据连续性": "优秀",
            "RSI指标兼容性": "完美兼容"
        },
        "后复权": {
            "scientific_score": 60,
            "recommendation": "谨慎使用",
            "理论基础": "基于当前价格的历史调整",
            "技术指标适用性": "部分适用，需要谨慎解释",
            "数据连续性": "一般",
            "RSI指标兼容性": "兼容但不推荐"
        },
        "除权": {
            "scientific_score": 30,
            "recommendation": "不推荐",
            "理论基础": "原始交易价格，包含跳跃",
            "技术指标适用性": "不适合，会产生误导信号",
            "数据连续性": "差",
            "RSI指标兼容性": "不兼容"
        }
    }

    # ==================== RSI指标参数配置 ====================
    
    # RSI指标周期参数
    RSI_PERIODS = {
        'short': 6,      # 短期参数 - 高敏感度
        'standard': 12,  # 标准参数 - 中国市场常用
        'long': 24       # 长期参数 - 平滑信号
    }
    
    # RSI指标衍生参数
    RSI_DERIVED_PARAMS = {
        'change_rate_lag': 1  # 变化率计算滞后期
    }
    
    # RSI指标阈值设置（仅供参考，不做主观判断）
    RSI_REFERENCE_LEVELS = {
        'high_level': 70,     # 高数值参考线
        'low_level': 30,      # 低数值参考线
        'middle_level': 50,   # 中间参考线
        'upper_neutral': 60,  # 上中性区参考
        'lower_neutral': 40   # 下中性区参考
    }

    # ==================== 数据处理配置 ====================
    
    # 双门槛ETF筛选标准
    THRESHOLDS = {
        "3000万门槛": 30_000_000,   # 3000万成交额门槛
        "5000万门槛": 50_000_000    # 5000万成交额门槛
    }
    
    # 数据质量要求
    DATA_QUALITY_REQUIREMENTS = {
        'min_data_points': 24,        # 最少数据点(满足RSI24计算)
        'recommended_data_points': 60, # 推荐数据点
        'decimal_precision': 8,        # 数值精度
        'date_format': '%Y-%m-%d'     # 日期格式
    }

    # ==================== 输出格式配置 ====================
    
    # RSI指标输出字段定义
    OUTPUT_FIELDS = {
        'base_fields': ['code', 'date'],
        'rsi_fields': ['rsi_6', 'rsi_12', 'rsi_24'],
        'derived_fields': ['rsi_diff_6_24', 'rsi_change_rate'],
        'meta_fields': ['calc_time']
    }
    
    # CSV输出格式配置
    CSV_CONFIG = {
        'encoding': 'utf-8',
        'decimal_places': 8,
        'date_format': '%Y-%m-%d',
        'time_format': '%Y-%m-%d %H:%M:%S'
    }

    # ==================== 性能配置 ====================
    
    # 系统性能目标
    PERFORMANCE_TARGETS = {
        'batch_processing_speed': 800,  # ETF/分钟
        'memory_limit_mb': 80,          # 内存限制
        'single_etf_time_ms': 5,        # 单ETF计算时间
        'cache_hit_rate': 0.96,         # 缓存命中率目标
        'success_rate': 0.985           # 系统成功率目标
    }
    
    # 缓存配置
    CACHE_CONFIG = {
        'time_tolerance_seconds': 5,    # 时间容差
        'auto_cleanup_days': 30,        # 自动清理天数
        'max_cache_size_mb': 400       # 最大缓存大小
    }

    def _setup_paths(self):
        """设置系统路径配置"""
        # 自动检测项目根目录
        project_root = self.base_path
        while not os.path.basename(project_root) == 'data_clear':
            parent = os.path.dirname(project_root)
            if parent == project_root:  # 到达根目录
                project_root = self.base_path
                break
            project_root = parent
        
        self.project_root = project_root
        
        # 设置数据源路径
        self.data_source_path = os.path.join(
            project_root, "ETF日更", self.ADJ_TYPES[self.adj_type]
        )
        
        # 设置RSI指标工作路径
        rsi_root = os.path.dirname(os.path.dirname(self.base_path))
        self.rsi_root = rsi_root
        
        # 设置缓存和输出路径
        self.cache_base_path = os.path.join(rsi_root, "cache")
        self.data_output_path = os.path.join(rsi_root, "data")
        
        # 确保目录存在
        self._ensure_directories_exist()

    def _setup_system_config(self):
        """设置系统配置"""
        self.system_info = {
            'name': 'RSI指标',
            'category': '相对强弱指标',
            'version': '1.0.0',
            'description': 'RSI相对强弱指数技术分析计算系统',
            'author': 'ETF技术指标计算系统',
            'created_date': datetime.now().strftime('%Y-%m-%d'),
            'supported_indicators': list(self.OUTPUT_FIELDS['rsi_fields'] + self.OUTPUT_FIELDS['derived_fields'])
        }

    def _ensure_directories_exist(self):
        """确保必要目录存在"""
        directories = [
            self.cache_base_path,
            self.data_output_path,
            os.path.join(self.cache_base_path, "3000万门槛"),
            os.path.join(self.cache_base_path, "5000万门槛"),
            os.path.join(self.cache_base_path, "meta"),
            os.path.join(self.data_output_path, "3000万门槛"),
            os.path.join(self.data_output_path, "5000万门槛")
        ]
        
        for directory in directories:
            if not os.path.exists(directory):
                os.makedirs(directory, exist_ok=True)

    # ==================== 配置获取方法 ====================
    
    def get_cache_path(self, threshold):
        """获取指定门槛的缓存路径"""
        return os.path.join(self.cache_base_path, threshold)
    
    def get_data_output_path(self, threshold):
        """获取指定门槛的数据输出路径"""
        return os.path.join(self.data_output_path, threshold)
    
    def get_meta_path(self):
        """获取元数据路径"""
        return os.path.join(self.cache_base_path, "meta")
    
    def get_rsi_periods(self):
        """获取RSI指标周期参数"""
        return self.RSI_PERIODS
    
    def get_output_fields(self):
        """获取完整输出字段列表"""
        all_fields = []
        all_fields.extend(self.OUTPUT_FIELDS['base_fields'])
        all_fields.extend(self.OUTPUT_FIELDS['rsi_fields'])
        all_fields.extend(self.OUTPUT_FIELDS['derived_fields'])
        all_fields.extend(self.OUTPUT_FIELDS['meta_fields'])
        return all_fields
    
    def get_adj_type_evaluation(self):
        """获取当前复权类型的科学评估"""
        return self.ADJ_TYPE_SCIENTIFIC_EVALUATION[self.adj_type]
    
    def is_data_source_valid(self):
        """验证数据源路径有效性"""
        return os.path.exists(self.data_source_path)
    
    def get_performance_config(self):
        """获取性能配置"""
        return {
            'targets': self.PERFORMANCE_TARGETS,
            'cache': self.CACHE_CONFIG
        }

    # ==================== 配置验证方法 ====================
    
    def validate_config(self):
        """验证配置完整性和有效性"""
        validation_results = {
            'is_valid': True,
            'errors': [],
            'warnings': []
        }
        
        # 验证数据源路径
        if not self.is_data_source_valid():
            validation_results['errors'].append(f"数据源路径不存在: {self.data_source_path}")
            validation_results['is_valid'] = False
        
        # 验证复权类型
        if self.adj_type not in self.ADJ_TYPES:
            validation_results['errors'].append(f"不支持的复权类型: {self.adj_type}")
            validation_results['is_valid'] = False
        
        # 验证科学评估建议
        evaluation = self.get_adj_type_evaluation()
        if evaluation['scientific_score'] < 70:
            validation_results['warnings'].append(
                f"当前复权类型({self.adj_type})科学评分较低: {evaluation['scientific_score']}/100, "
                f"建议: {evaluation['recommendation']}"
            )
        
        return validation_results

    def print_config_summary(self):
        """打印配置摘要"""
        print("=" * 60)
        print("🔧 RSI指标系统配置摘要")
        print("=" * 60)
        print(f"📊 系统名称: {self.system_info['name']}")
        print(f"📈 系统版本: {self.system_info['version']}")
        print(f"📅 复权类型: {self.adj_type}")
        print(f"🎯 科学评分: {self.get_adj_type_evaluation()['scientific_score']}/100")
        print(f"📁 数据源路径: {self.data_source_path}")
        print(f"💾 缓存路径: {self.cache_base_path}")
        print(f"📤 输出路径: {self.data_output_path}")
        print(f"🔢 支持指标: {len(self.system_info['supported_indicators'])}个")
        print("=" * 60)


if __name__ == "__main__":
    # 配置测试
    config = RSIConfig()
    config.print_config_summary()
    
    # 配置验证
    validation = config.validate_config()
    if validation['is_valid']:
        print("✅ 配置验证通过")
    else:
        print("❌ 配置验证失败:")
        for error in validation['errors']:
            print(f"  - {error}")
    
    if validation['warnings']:
        print("⚠️  配置警告:")
        for warning in validation['warnings']:
            print(f"  - {warning}")