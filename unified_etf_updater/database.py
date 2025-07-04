#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据库管理器
处理ETF数据的数据库导入功能
"""

import logging


class DatabaseManager:
    """数据库导入管理器"""
    
    def __init__(self, config: dict, logger: logging.Logger):
        """
        初始化数据库管理器
        
        Args:
            config: 数据库配置
            logger: 日志记录器
        """
        self.config = config
        self.logger = logger
        self.db_config = config.get('database_import', {})
        self.auto_import_enabled = self.db_config.get('enabled', True)
        
        # 动态加载数据库模块
        self.database_available = self._load_database_modules()
        
    def _load_database_modules(self) -> bool:
        """动态加载数据库导入模块"""
        try:
            from ETF_database.importers.daily_importer import DailyDataImporter
            from ETF_database.importers.weekly_importer import WeeklyDataImporter
            from ETF_database.importers.market_status_importer import MarketStatusImporter
            
            self.DailyDataImporter = DailyDataImporter
            self.WeeklyDataImporter = WeeklyDataImporter
            self.MarketStatusImporter = MarketStatusImporter
            
            self.logger.info("✅ 数据库导入模块加载成功")
            return True
        except ImportError as e:
            self.logger.warning(f"⚠️ 数据库导入模块不可用: {e}")
            return False
    
    def is_enabled(self) -> bool:
        """检查数据库导入是否启用"""
        return self.database_available and self.auto_import_enabled
    
    def import_data(self, import_type: str, base_dir: str = None) -> bool:
        """
        执行数据库导入
        
        Args:
            import_type: 导入类型 (daily/weekly/market_status)
            base_dir: 数据目录
        
        Returns:
            是否导入成功
        """
        if not self.is_enabled():
            if not self.database_available:
                self.logger.warning("⚠️ 数据库导入模块不可用，跳过数据库导入")
            else:
                self.logger.info("ℹ️ 数据库自动导入已禁用，跳过")
            return False
        
        self.logger.info(f"📊 开始执行{import_type}数据库导入...")
        
        try:
            if import_type == "daily":
                return self._import_daily_data(base_dir)
            elif import_type == "weekly":
                return self._import_weekly_data(base_dir)
            elif import_type == "market_status":
                return self._import_market_status(base_dir)
            else:
                self.logger.error(f"❌ 不支持的导入类型: {import_type}")
                return False
                
        except Exception as e:
            self.logger.error(f"❌ {import_type}数据库导入失败: {str(e)}")
            return False
    
    def _import_daily_data(self, base_dir: str) -> bool:
        """导入日更数据"""
        from pathlib import Path
        
        importer = self.DailyDataImporter()
        base_dir = base_dir or str(Path(__file__).parent.parent / "ETF日更")
        
        # 只导入最近1天的数据（高性能批量导入）
        results = importer.import_latest_data_optimized(base_dir, days_back=1)
        
        success_count = sum(1 for success in results.values() if success)
        total_count = len(results)
        
        if success_count > 0:
            self.logger.info(f"✅ 日更数据库导入完成: {success_count}/{total_count}")
            return True
        else:
            self.logger.warning(f"⚠️ 日更数据库导入无更新: {success_count}/{total_count}")
            return False
    
    def _import_weekly_data(self, base_dir: str) -> bool:
        """导入周更数据"""
        from pathlib import Path
        
        importer = self.WeeklyDataImporter()
        base_dir = base_dir or str(Path(__file__).parent.parent / "ETF周更")
        
        # 只导入最近1周的数据（高性能批量导入）
        results = importer.import_latest_weekly_data_optimized(base_dir, weeks_back=1)
        
        success_count = sum(1 for success in results.values() if success)
        total_count = len(results)
        
        if success_count > 0:
            self.logger.info(f"✅ 周更数据库导入完成: {success_count}/{total_count}")
            return True
        else:
            self.logger.warning(f"⚠️ 周更数据库导入无更新: {success_count}/{total_count}")
            return False
    
    def _import_market_status(self, base_dir: str = None) -> bool:
        """导入市场状况数据"""
        from pathlib import Path
        
        importer = self.MarketStatusImporter()
        json_file = str(Path(__file__).parent.parent / "ETF市场状况" / "etf_market_status.json")
        
        results = {"market_status": importer.import_json_file(json_file)}
        success = results["market_status"]
        
        if success:
            self.logger.info("✅ 市场状况数据库导入完成")
            return True
        else:
            self.logger.warning("⚠️ 市场状况数据库导入无更新")
            return False 