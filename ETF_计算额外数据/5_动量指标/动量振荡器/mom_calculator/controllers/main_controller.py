"""
主控制器模块
============

协调各个组件，实现动量振荡器的完整业务流程
"""

import pandas as pd
from pathlib import Path
from typing import Dict, List, Optional, Any
import logging
from datetime import datetime

from ..infrastructure.config import MomentumConfig
from ..infrastructure.data_reader import MomentumDataReader
from ..infrastructure.cache_manager import MomentumCacheManager
from ..engines.momentum_engine import MomentumEngine
from ..outputs.csv_handler import MomentumCSVHandler
from ..outputs.display_formatter import MomentumDisplayFormatter

class MomentumController:
    """动量振荡器主控制器"""
    
    def __init__(self, base_path: Optional[str] = None):
        """
        初始化主控制器
        
        Args:
            base_path: 基础路径，None则使用配置默认值
        """
        # 设置基础路径
        if base_path:
            MomentumConfig.BASE_DIR = Path(base_path).parent
            MomentumConfig.init_directories()
        
        # 初始化各组件
        self.data_reader = MomentumDataReader()
        self.cache_manager = MomentumCacheManager()
        self.engine = MomentumEngine()
        self.csv_handler = MomentumCSVHandler()
        self.formatter = MomentumDisplayFormatter()
        
        # 配置日志
        self._setup_logging()
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # 统计信息
        self.stats = {"success": 0, "failed": 0, "skipped": 0}
        
        self.logger.info("🚀 动量振荡器主控制器初始化完成")
    
    def _setup_logging(self):
        """配置日志系统"""
        log_config = MomentumConfig.LOGGING_CONFIG
        log_file = MomentumConfig.LOGS_DIR / log_config['file_name']
        
        # 确保日志目录存在
        MomentumConfig.LOGS_DIR.mkdir(parents=True, exist_ok=True)
        
        # 清除现有的处理器避免重复
        root_logger = logging.getLogger()
        for handler in root_logger.handlers[:]:
            root_logger.removeHandler(handler)
        
        logging.basicConfig(
            level=getattr(logging, log_config['level']),
            format=log_config['format'],
            handlers=[
                logging.FileHandler(log_file, encoding='utf-8', mode='w'),  # 每次运行清空日志文件
                logging.StreamHandler() if log_config.get('console_output', True) else logging.NullHandler()
            ],
            force=True  # 强制重新配置
        )
    
    def process_single_etf(self, etf_code: str, force_recalculate: bool = False, threshold_override: Optional[str] = None) -> bool:
        """
        处理单个ETF的动量指标计算
        
        Args:
            etf_code: ETF代码
            force_recalculate: 是否强制重新计算
            threshold_override: 强制指定门槛类型
            
        Returns:
            处理是否成功
        """
        try:
            # 读取原始数据
            raw_data = self.data_reader.read_etf_data(etf_code)
            if raw_data is None or raw_data.empty:
                self.logger.warning(f"⚠️ {etf_code}: 无法读取数据或数据为空")
                return False
            
            # 判断成交额门槛
            threshold_category = threshold_override or self._determine_threshold(raw_data, etf_code)
            if not threshold_category:
                self.logger.info(f"⏭️ {etf_code}: 成交额不足门槛，跳过处理")
                return False
            
            # 检查缓存
            if not force_recalculate:
                cached_data = self.cache_manager.load_cache(etf_code, threshold_category)
                if cached_data is not None:
                    self.logger.info(f"💾 {etf_code}: 使用缓存数据")
                    # 保存缓存数据到输出目录
                    self.csv_handler.save_to_csv(cached_data, etf_code, threshold_category)
                    return True
            
            # 计算动量指标
            calculation_result = self.engine.calculate_momentum_indicators(raw_data, etf_code)
            
            if not calculation_result['success']:
                self.logger.error(f"❌ {etf_code}: 计算失败 - {calculation_result.get('error', 'Unknown')}")
                return False
            
            result_data = calculation_result['data']
            
            # 保存到输出目录
            if not self.csv_handler.save_to_csv(result_data, etf_code, threshold_category):
                self.logger.error(f"❌ {etf_code}: 保存失败")
                return False
            
            # 保存到缓存
            self.cache_manager.save_cache(etf_code, threshold_category, result_data)
            
            self.logger.info(f"✅ {etf_code}: 动量指标计算完成，{len(result_data)}条记录")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ {etf_code}: 处理异常 - {str(e)}")
            return False
    
    def batch_process_etfs(self, 
                          source_data_path: Optional[str] = None,
                          etf_codes: Optional[List[str]] = None,
                          use_filtered_list: bool = True,
                          force_recalculate: bool = False,
                          threshold_override: Optional[str] = None) -> Dict[str, int]:
        """
        批量处理ETF动量指标计算
        
        Args:
            source_data_path: 源数据路径，None则使用配置默认值
            etf_codes: 指定处理的ETF代码列表，None表示使用初筛列表
            use_filtered_list: 是否使用初筛列表
            force_recalculate: 是否强制重新计算
            threshold_override: 强制指定门槛类型
            
        Returns:
            处理统计信息
        """
        try:
            # 重置统计
            self.stats = {"success": 0, "failed": 0, "skipped": 0}
            
            # 确定要处理的ETF列表
            target_etfs = self._get_target_etfs(etf_codes, use_filtered_list)
            
            if not target_etfs:
                self.logger.warning("⚠️ 未找到符合条件的ETF文件")
                return self.stats
            
            self.logger.info(f"🚀 开始批量处理 {len(target_etfs)} 个ETF的动量指标")
            
            # 逐个处理ETF
            for etf_code in target_etfs:
                try:
                    success = self.process_single_etf(etf_code, force_recalculate, threshold_override)
                    
                    if success:
                        self.stats["success"] += 1
                    else:
                        self.stats["skipped"] += 1
                        
                except Exception as e:
                    self.logger.error(f"❌ {etf_code}: 批量处理异常 - {str(e)}")
                    self.stats["failed"] += 1
            
            # 保存元数据
            self._save_processing_metadata(threshold_override)
            
            self.logger.info(f"✅ 批量处理完成: 成功{self.stats['success']}, 失败{self.stats['failed']}, 跳过{self.stats['skipped']}")
            return self.stats
            
        except Exception as e:
            self.logger.error(f"❌ 批量处理异常: {str(e)}")
            return self.stats
    
    def _determine_threshold(self, data: pd.DataFrame, etf_code: str) -> Optional[str]:
        """确定成交额门槛类别 - 根据ETF所属的初筛列表确定门槛"""
        # 检查ETF属于哪个门槛列表
        for threshold in ["5000万门槛", "3000万门槛"]:  # 优先检查5000万
            filtered_etfs = self._load_filtered_etf_list(threshold)
            if etf_code in filtered_etfs:
                return threshold
        
        # 如果都不在，返回默认门槛
        return "3000万门槛"
    
    def _get_target_etfs(self, etf_codes: Optional[List[str]], use_filtered_list: bool) -> List[str]:
        """获取目标ETF列表"""
        try:
            if etf_codes:
                return etf_codes
            
            if use_filtered_list:
                # 加载初筛ETF列表
                all_etfs = []
                for threshold in ["3000万门槛", "5000万门槛"]:
                    filtered_etfs = self._load_filtered_etf_list(threshold)
                    all_etfs.extend(filtered_etfs)
                return list(set(all_etfs))  # 去重
            else:
                # 发现所有可用ETF
                return self.data_reader._discover_available_etfs()
                
        except Exception as e:
            self.logger.error(f"获取目标ETF列表失败: {str(e)}")
            return []
    
    def _load_filtered_etf_list(self, threshold_category: str) -> List[str]:
        """加载通过初筛的ETF列表"""
        try:
            filter_file = MomentumConfig.get_filter_file_path(threshold_category)
            
            if not filter_file.exists():
                return []
            
            filtered_etfs = []
            with open(filter_file, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#'):
                        etf_code = line.split('.')[0]
                        if etf_code:
                            filtered_etfs.append(etf_code)
            
            return filtered_etfs
            
        except Exception as e:
            self.logger.error(f"加载初筛ETF列表失败: {str(e)}")
            return []
    
    def _save_processing_metadata(self, target_threshold: Optional[str] = None):
        """保存处理元数据"""
        try:
            metadata = {
                "system_info": MomentumConfig.get_system_info(),
                "calculation_params": {
                    "precision": MomentumConfig.MOMENTUM_CONFIG['precision'],
                    "momentum_periods": MomentumConfig.MOMENTUM_CONFIG['momentum_periods'],
                    "roc_periods": MomentumConfig.MOMENTUM_CONFIG['roc_periods'],
                    "pmo_params": MomentumConfig.MOMENTUM_CONFIG['pmo_config'],
                    "williams_period": MomentumConfig.MOMENTUM_CONFIG['williams_period']
                },
                "fields_description": MomentumConfig.FIELD_DESCRIPTIONS,
                "processing_stats": self.stats,
                "last_update": datetime.now().isoformat()
            }
            
            # 根据target_threshold决定保存范围
            if target_threshold:
                # 单门槛处理，只保存指定门槛的元数据
                self.csv_handler.batch_save_metadata(metadata, target_threshold)
            else:
                # 双门槛处理，保存到两个门槛的meta目录
                for threshold in ["3000万门槛", "5000万门槛"]:
                    self.csv_handler.batch_save_metadata(metadata, threshold)
            
        except Exception as e:
            self.logger.error(f"保存处理元数据失败: {str(e)}")
    
    def get_system_status(self) -> Dict[str, Any]:
        """获取系统状态"""
        try:
            cache_stats = self.cache_manager.get_cache_statistics()
            
            return {
                'system_info': MomentumConfig.get_system_info(),
                'cache_status': cache_stats,
                'last_processing_stats': self.stats,
                'directories': {
                    'data': str(MomentumConfig.DATA_DIR),
                    'cache': str(MomentumConfig.CACHE_DIR),
                    'logs': str(MomentumConfig.LOGS_DIR)
                }
            }
            
        except Exception as e:
            self.logger.error(f"获取系统状态失败: {str(e)}")
            return {'error': str(e)}