#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
统一ETF更新器核心模块
主要的更新器类和系统测试功能
"""

import json
import logging
from datetime import datetime
from pathlib import Path

from .database import DatabaseManager
from .git_manager import GitManager
from .updaters import ETFUpdaters


class UnifiedETFUpdater:
    """统一ETF更新器核心类"""
    
    def __init__(self, project_root: Path = None):
        """
        初始化统一更新器
        
        Args:
            project_root: 项目根目录，默认为当前文件的父目录的父目录
        """
        # 设置项目根目录
        if project_root is None:
            self.project_root = Path(__file__).parent.parent
        else:
            self.project_root = Path(project_root)
        
        # 设置日志
        self.logger = self._setup_logger()
        
        # 加载配置
        self.config = self._load_config()
        
        # 初始化各个管理器
        self.database_manager = DatabaseManager(self.config, self.logger)
        self.git_manager = GitManager(self.config, self.logger, self.project_root)
        self.updaters = ETFUpdaters(self.config, self.logger, self.project_root)
        
        self.logger.info("统一ETF更新器初始化完成")
        self._log_status()
    
    def _setup_logger(self) -> logging.Logger:
        """设置日志记录器"""
        try:
            from config.logger_config import setup_system_logger
            return setup_system_logger()
        except ImportError:
            # 备用日志配置
            logging.basicConfig(
                level=logging.INFO,
                format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            return logging.getLogger(__name__)
    
    def _load_config(self) -> dict:
        """加载配置文件"""
        config_path = self.project_root / "config" / "config.json"
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except FileNotFoundError:
            self.logger.error(f"配置文件不存在: {config_path}")
            return {}
        except json.JSONDecodeError as e:
            self.logger.error(f"配置文件格式错误: {e}")
            return {}
    
    def _log_status(self):
        """记录各模块状态"""
        if self.database_manager.is_enabled():
            self.logger.info("✅ 数据库自动导入已启用")
        else:
            self.logger.info("ℹ️ 数据库自动导入已禁用")
        
        if self.git_manager.is_enabled():
            self.logger.info("✅ Git自动提交已启用")
        else:
            self.logger.info("ℹ️ Git自动提交已禁用")
        
        if self.updaters.auto_screening_enabled:
            self.logger.info("✅ ETF自动初筛已启用")
        else:
            self.logger.info("ℹ️ ETF自动初筛已禁用")
        
        if self.updaters.validator.is_enabled():
            self.logger.info("✅ 周更日更数据校验已启用")
        else:
            self.logger.info("ℹ️ 周更日更数据校验已禁用")
    
    def test_system_status(self):
        """测试系统状态"""
        self.logger.info("🔍 开始系统状态测试")
        
        # 检查目录结构
        required_dirs = [
            "ETF日更",
            "ETF周更", 
            "ETF市场状况",
            "ETF_初筛",
            "config",
            "logs",
            "scripts"
        ]
        
        for dir_name in required_dirs:
            dir_path = self.project_root / dir_name
            if dir_path.exists():
                self.logger.info(f"✅ 目录存在: {dir_name}")
            else:
                self.logger.error(f"❌ 目录缺失: {dir_name}")
        
        # 检查关键文件
        required_files = [
            "config/config.json",
            "config/hash_manager.py",
            "ETF日更/auto_daily_sync.py",
            "ETF周更/etf_auto_sync.py",
            "ETF市场状况/market_status_monitor.py",
            "ETF_初筛/main.py"
        ]
        
        for file_path in required_files:
            full_path = self.project_root / file_path
            if full_path.exists():
                self.logger.info(f"✅ 文件存在: {file_path}")
            else:
                self.logger.error(f"❌ 文件缺失: {file_path}")
        
        # 检查配置文件
        if self.config:
            self.logger.info(f"✅ 配置加载成功，包含 {len(self.config)} 个配置项")
        else:
            self.logger.error("❌ 配置加载失败")
        
        # 检查日志系统
        log_file = "etf_sync.log"
        log_path = self.project_root / "logs" / "system" / log_file
        
        if log_path.exists():
            self.logger.info(f"✅ 统一日志文件存在: {log_file}")
        else:
            self.logger.info(f"ℹ️  统一日志文件将自动创建: {log_file}")
        
        self.logger.info("🔍 系统状态测试完成")
    
    def run_full_update(self) -> dict:
        """
        执行完整更新流程（智能跳过无新数据的流程）
        
        Returns:
            各模块执行结果字典
        """
        start_time = datetime.now()
        self.logger.info("🚀 开始执行完整ETF数据更新流程（智能跳过无新数据）")
        
        results = {
            'daily': False,
            'weekly': False,
            'market_status': False,
            'etf_screening': False
        }
        reasons = {}
        
        # 1. 执行日更
        daily_has_new, daily_reason = self.updaters.run_daily_update()
        results['daily'] = daily_has_new
        reasons['daily'] = daily_reason
        
        # 2. 执行周更
        weekly_has_new, weekly_reason = self.updaters.run_weekly_update()
        results['weekly'] = weekly_has_new
        reasons['weekly'] = weekly_reason
        
        # 3. 市场状况依赖日更
        market_has_new, market_reason = self.updaters.run_market_status_check(daily_has_new)
        results['market_status'] = market_has_new
        reasons['market_status'] = market_reason
        
        # 4. ETF初筛依赖日更
        screening_has_new, screening_reason = self.updaters.run_etf_screening(daily_has_new)
        results['etf_screening'] = screening_has_new
        reasons['etf_screening'] = screening_reason
        
        # 5. 数据库导入（只有有新数据才导入）
        if daily_has_new:
            self.logger.info("📥 日更有新数据，导入数据库...")
            self.database_manager.import_data("daily")
        
        if weekly_has_new:
            self.logger.info("📥 周更有新数据，导入数据库...")
            self.database_manager.import_data("weekly")
        
        if market_has_new:
            self.logger.info("📥 市场状况有新数据，导入数据库...")
            self.database_manager.import_data("market_status")
        
        # 注意：ETF初筛结果是文本文件，不需要数据库导入
        
        # 6. 只有有新数据才允许Git提交
        total_success = sum(results.values())
        if total_success > 0:
            self.logger.info("")
            git_success = self.git_manager.auto_commit(results)
            if git_success:
                self.logger.info("✅ 数据更新和Git提交全部完成！")
            else:
                self.logger.warning("⚠️ 数据更新完成，但Git提交失败")
        else:
            self.logger.info("ℹ️ 没有成功的更新，跳过Git提交")
        
        # 总结报告
        self._log_summary(start_time, results, reasons, total_success)
        
        return results
    
    def _log_summary(self, start_time: datetime, results: dict, reasons: dict, total_success: int):
        """记录总结报告"""
        end_time = datetime.now()
        duration = end_time - start_time
        
        self.logger.info("=" * 60)
        self.logger.info("📊 ETF数据更新完成总结")
        self.logger.info("=" * 60)
        self.logger.info(f"开始时间: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        self.logger.info(f"结束时间: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        self.logger.info(f"总耗时: {duration}")
        self.logger.info("")
        self.logger.info("各模块执行结果:")
        for k in results:
            status = '✅ 有新数据' if results[k] else '⏭️ 跳过/无新数据'
            self.logger.info(f"  {k}: {status} ({reasons[k]})")
        self.logger.info(f"整体有新数据模块数: {total_success}/4")
    
    def set_git_enabled(self, enabled: bool):
        """设置Git自动提交是否启用"""
        self.config.setdefault('git_auto_commit', {})['enabled'] = enabled
        if enabled:
            self.logger.info("🔧 已启用Git自动提交")
        else:
            self.logger.info("🔧 已禁用Git自动提交")
    
    def set_git_push_enabled(self, enabled: bool):
        """设置Git自动推送是否启用"""
        self.config.setdefault('git_auto_commit', {})['auto_push'] = enabled
        if enabled:
            self.logger.info("🔧 已启用Git自动推送")
        else:
            self.logger.info("🔧 已禁用Git自动推送")
    
    def set_screening_enabled(self, enabled: bool):
        """设置ETF自动初筛是否启用"""
        self.updaters.set_screening_enabled(enabled)
        if enabled:
            self.logger.info("🔧 已启用ETF自动初筛")
        else:
            self.logger.info("🔧 已禁用ETF自动初筛")
    
    def set_validation_enabled(self, enabled: bool):
        """设置周更日更数据校验是否启用"""
        self.updaters.set_validation_enabled(enabled)
        if enabled:
            self.logger.info("🔧 已启用周更日更数据校验")
        else:
            self.logger.info("🔧 已禁用周更日更数据校验")
    
    def run_weekly_daily_validation(self) -> dict:
        """
        手动运行周更与日更数据校验
        
        Returns:
            校验结果字典
        """
        self.logger.info("🚀 开始手动周更日更数据校验...")
        
        needs_attention, validation_msg = self.updaters.run_weekly_daily_validation()
        
        result = {
            'needs_attention': needs_attention,
            'message': validation_msg,
            'success': True
        }
        
        if needs_attention:
            self.logger.warning("⚠️ 数据校验发现问题，需要用户关注")
        else:
            self.logger.info("✅ 数据校验通过，无需处理")
        
        return result