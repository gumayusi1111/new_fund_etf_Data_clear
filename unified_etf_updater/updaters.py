#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ETF更新器
处理各种ETF数据更新流程
"""

import sys
import subprocess
import logging
from pathlib import Path
from typing import Tuple

from .validator import WeeklyDailyValidator


class ETFUpdaters:
    """ETF各类数据更新器"""
    
    def __init__(self, config: dict, logger: logging.Logger, project_root: Path):
        """
        初始化ETF更新器
        
        Args:
            config: 配置
            logger: 日志记录器
            project_root: 项目根目录
        """
        self.config = config
        self.logger = logger
        self.project_root = project_root
        self.screening_config = config.get('etf_screening', {})
        self.auto_screening_enabled = self.screening_config.get('enabled', True)
        
        # 初始化校验器
        self.validator = WeeklyDailyValidator(config, logger, project_root)
    
    def run_daily_update(self) -> Tuple[bool, str]:
        """
        执行日更流程（智能模式：自动检测和补漏）
        
        Returns:
            Tuple[是否成功, 原因描述]
        """
        self.logger.info("=" * 50)
        self.logger.info("开始执行ETF日更流程（智能模式）")
        self.logger.info("=" * 50)
        
        try:
            daily_script = self.project_root / "ETF日更" / "auto_daily_sync.py"
            if not daily_script.exists():
                self.logger.error(f"日更脚本不存在: {daily_script}")
                return False, "脚本不存在"
            
            daily_dir = self.project_root / "ETF日更"
            # 使用智能更新模式，自动检测最近7天的缺失数据并补漏
            cmd = [sys.executable, "auto_daily_sync.py", "--mode", "smart-update", "--days-back", "7"]
            
            result = subprocess.run(
                cmd,
                cwd=str(daily_dir),
                capture_output=True,
                text=True,
                encoding='utf-8'
            )
            
            output = result.stdout + result.stderr
            
            # 检查明确的失败情况
            if result.returncode != 0:
                self.logger.error("❌ ETF智能日更失败（退出码非0）")
                if result.stderr:
                    self.logger.error(f"错误: {result.stderr[:200]}...")
                return False, "执行失败"
            
            if "智能更新部分失败" in output or "智能更新失败" in output:
                self.logger.info("📅 今天无新数据，智能跳过日更")
                return False, "无新数据"
                
            if "没有找到今天的文件" in output or "未找到任何文件" in output:
                self.logger.info("📅 今天无新数据，智能跳过日更")
                return False, "无新数据"
                
            if "数据完整，无缺失" in output and "已是最新" in output:
                # 检查是否包含今天的数据更新
                from datetime import datetime
                today_str = datetime.now().strftime('%Y%m%d')
                if today_str in output or "今日增量更新完成" in output:
                    self.logger.info("✅ ETF日更数据已是最新，包含今日数据")
                    return True, "有今日数据"
                else:
                    self.logger.info("📅 日更数据已是最新，但无今日新数据")
                    return False, "已是最新"
                
            if "智能更新完全成功" in output or "今日增量更新完成" in output:
                self.logger.info("✅ ETF智能日更完成（有数据更新）")
                return True, "有新数据"
            else:
                # 默认情况：如果没有明确的成功标志，视为失败
                self.logger.warning("⚠️ ETF智能日更状态不明确，视为无新数据")
                return False, "状态不明确"
                
        except Exception as e:
            self.logger.error(f"执行日更时发生异常: {str(e)}")
            return False, f"异常: {str(e)}"

    def run_weekly_update(self) -> Tuple[bool, str]:
        """
        执行周更流程（智能跳过）
        
        Returns:
            Tuple[是否成功, 原因描述]
        """
        self.logger.info("=" * 50)
        self.logger.info("开始执行ETF周更流程（智能检查）")
        self.logger.info("=" * 50)
        
        try:
            weekly_script = self.project_root / "ETF周更" / "etf_auto_sync.py"
            if not weekly_script.exists():
                self.logger.error(f"周更脚本不存在: {weekly_script}")
                return False, "脚本不存在"
            
            weekly_dir = self.project_root / "ETF周更"
            cmd = [sys.executable, "etf_auto_sync.py"]
            
            result = subprocess.run(
                cmd,
                cwd=str(weekly_dir),
                capture_output=True,
                text=True,
                encoding='utf-8'
            )
            
            output = result.stdout + result.stderr
            
            if "所有文件都已是最新，无需下载" in output:
                self.logger.info("📊 周更压缩包无变化，智能跳过")
                return False, "无变化"
            
            if "未找到" in output and "月" in output:
                self.logger.info("📊 未找到当前月份压缩包，智能跳过")
                return False, "无当月数据"
            
            if result.returncode == 0 and ("数据同步完成" in output or "合并完成" in output or "下载完成" in output):
                self.logger.info("✅ ETF周更完成（有新数据）")
                
                # 周更完成后进行数据校验
                if self.validator.is_enabled():
                    self.logger.info("🔍 开始周更后数据校验...")
                    needs_attention, validation_msg = self.validator.run_validation_after_weekly_update()
                    if needs_attention:
                        self.logger.warning(f"⚠️ 数据校验发现问题: {validation_msg}")
                        self.logger.warning("📋 请检查三个复权类型的数据一致性！")
                    else:
                        self.logger.info(f"✅ 数据校验通过: {validation_msg}")
                
                return True, "有新数据"
            else:
                self.logger.error("❌ ETF周更失败")
                if result.stderr:
                    self.logger.error(f"错误: {result.stderr[:200]}...")
                return False, "执行失败"
                
        except Exception as e:
            self.logger.error(f"执行周更时发生异常: {str(e)}")
            return False, f"异常: {str(e)}"

    def run_market_status_check(self, daily_has_new_data: bool) -> Tuple[bool, str]:
        """
        执行ETF市场状况监控（依赖日更）
        
        Args:
            daily_has_new_data: 日更是否有新数据
        
        Returns:
            Tuple[是否成功, 原因描述]
        """
        self.logger.info("=" * 50)
        self.logger.info("开始执行ETF市场状况监控（智能检查）")
        self.logger.info("=" * 50)
        
        if not daily_has_new_data:
            self.logger.info("📊 日更无新数据，智能跳过市场状况检查")
            return False, "依赖日更跳过"
        
        try:
            market_script = self.project_root / "ETF市场状况" / "market_status_monitor.py"
            if not market_script.exists():
                self.logger.error(f"市场状况监控脚本不存在: {market_script}")
                return False, "脚本不存在"
            
            market_dir = self.project_root / "ETF市场状况"
            cmd = [sys.executable, "market_status_monitor.py"]
            
            result = subprocess.run(
                cmd,
                cwd=str(market_dir),
                capture_output=True,
                text=True,
                encoding='utf-8'
            )
            
            output = result.stdout + result.stderr
            
            if result.returncode == 0 and ("报告已更新" in output or "监控完成" in output):
                self.logger.info("✅ ETF市场状况监控完成（有新数据）")
                return True, "有新数据"
            else:
                self.logger.error("❌ ETF市场状况监控失败")
                if result.stderr:
                    self.logger.error(f"错误: {result.stderr[:200]}...")
                return False, "执行失败"
                
        except Exception as e:
            self.logger.error(f"执行市场状况监控时发生异常: {str(e)}")
            return False, f"异常: {str(e)}"

    def run_etf_screening(self, daily_has_new_data: bool) -> Tuple[bool, str]:
        """
        执行ETF初筛流程（依赖日更）
        
        Args:
            daily_has_new_data: 日更是否有新数据
        
        Returns:
            Tuple[是否成功, 原因描述]
        """
        self.logger.info("=" * 50)
        self.logger.info("开始执行ETF初筛流程（双门槛筛选）")
        self.logger.info("=" * 50)
        
        if not self.auto_screening_enabled:
            self.logger.info("ℹ️ ETF自动初筛已禁用，跳过")
            return False, "初筛已禁用"
        
        if not daily_has_new_data:
            self.logger.info("📊 日更无新数据，智能跳过ETF初筛")
            return False, "依赖日更跳过"
        
        try:
            screening_dir = self.project_root / "ETF_初筛"
            screening_script = screening_dir / "main.py"
            
            if not screening_script.exists():
                self.logger.error(f"ETF初筛脚本不存在: {screening_script}")
                return False, "脚本不存在"
            
            # 获取初筛配置
            fuquan_type = self.screening_config.get('fuquan_type', '0_ETF日K(后复权)')
            days_back = self.screening_config.get('days_back', None)
            
            # 构建命令
            cmd = [sys.executable, "main.py", "--mode", "dual", "--fuquan-type", fuquan_type]
            if days_back:
                cmd.extend(["--days-back", str(days_back)])
            
            self.logger.info(f"📊 运行ETF初筛: {' '.join(cmd)}")
            
            result = subprocess.run(
                cmd,
                cwd=str(screening_dir),
                capture_output=True,
                text=True,
                encoding='utf-8'
            )
            
            output = result.stdout + result.stderr
            
            # 检查执行结果
            if result.returncode == 0 and ("双门槛筛选对比结果" in output or "保存双门槛筛选结果" in output):
                self.logger.info("✅ ETF初筛完成（生成新筛选结果）")
                
                # 从输出中提取统计信息
                if "通过筛选ETF" in output:
                    lines = output.split('\n')
                    for line in lines:
                        if "5000万门槛通过筛选ETF" in line or "3000万门槛通过筛选ETF" in line:
                            self.logger.info(f"  🎯 {line.strip()}")
                
                return True, "有新筛选结果"
            else:
                self.logger.error("❌ ETF初筛失败")
                if result.stderr:
                    self.logger.error(f"错误: {result.stderr[:300]}...")
                if "no-parameter tools" in output:
                    self.logger.error("可能是工具调用问题，但筛选可能已完成")
                    # 检查是否生成了结果文件
                    data_dir = screening_dir / "data"
                    if data_dir.exists() and any(data_dir.rglob("*.txt")):
                        self.logger.info("🔍 检测到筛选结果文件，视为成功")
                        return True, "有新筛选结果"
                return False, "执行失败"
                
        except Exception as e:
            self.logger.error(f"执行ETF初筛时发生异常: {str(e)}")
            return False, f"异常: {str(e)}"
    
    def set_screening_enabled(self, enabled: bool):
        """设置ETF初筛是否启用"""
        self.auto_screening_enabled = enabled
    
    def run_weekly_daily_validation(self) -> Tuple[bool, str]:
        """
        手动运行周更与日更数据校验
        
        Returns:
            Tuple[是否需要用户注意, 描述信息]
        """
        self.logger.info("=" * 50)
        self.logger.info("手动执行周更与日更数据校验")
        self.logger.info("=" * 50)
        
        if not self.validator.is_enabled():
            self.logger.info("ℹ️ 周更日更校验已禁用")
            return False, "校验已禁用"
        
        try:
            needs_attention, validation_msg = self.validator.run_validation_after_weekly_update()
            
            if needs_attention:
                self.logger.warning(f"⚠️ 发现数据不一致: {validation_msg}")
                self.logger.warning("📋 建议检查三个复权类型的数据一致性！")
                return True, validation_msg
            else:
                self.logger.info(f"✅ 数据校验通过: {validation_msg}")
                return False, validation_msg
                
        except Exception as e:
            self.logger.error(f"执行数据校验时发生异常: {str(e)}")
            return False, f"校验异常: {str(e)}"
    
    def set_validation_enabled(self, enabled: bool):
        """设置周更日更校验是否启用"""
        # 更新配置中的校验设置
        if 'weekly_daily_validator' not in self.config:
            self.config['weekly_daily_validator'] = {}
        self.config['weekly_daily_validator']['enabled'] = enabled
        
        # 重新初始化校验器
        self.validator = WeeklyDailyValidator(self.config, self.logger, self.project_root)
        
        if enabled:
            self.logger.info("🔧 已启用周更日更数据校验")
        else:
            self.logger.info("🔧 已禁用周更日更数据校验")