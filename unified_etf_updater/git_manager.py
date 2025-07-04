#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Git管理器
处理ETF数据的Git自动提交功能
"""

import subprocess
import logging
from datetime import datetime
from pathlib import Path


class GitManager:
    """Git自动提交管理器"""
    
    def __init__(self, config: dict, logger: logging.Logger, project_root: Path):
        """
        初始化Git管理器
        
        Args:
            config: Git配置
            logger: 日志记录器
            project_root: 项目根目录
        """
        self.config = config
        self.logger = logger
        self.project_root = project_root
        self.git_config = config.get('git_auto_commit', {})
        
    def is_enabled(self) -> bool:
        """检查Git自动提交是否启用"""
        return self.git_config.get('enabled', False)
    
    def auto_commit(self, success_modules: dict) -> bool:
        """
        自动提交Git更新
        
        Args:
            success_modules: 成功的模块字典
        
        Returns:
            是否提交成功
        """
        if not self.is_enabled():
            self.logger.info("ℹ️ Git自动提交已禁用，跳过")
            return True
            
        self.logger.info("=" * 50)
        self.logger.info("开始自动Git提交")
        self.logger.info("=" * 50)
        
        try:
            # 检查是否是Git仓库
            if not self._is_git_repository():
                self.logger.warning("⚠️ 当前目录不是Git仓库，跳过自动提交")
                return False
            
            # 检查是否有变更
            if not self._has_changes():
                self.logger.info("ℹ️ 没有文件变更，跳过提交")
                return True
            
            # 显示变更的文件
            self._show_changes()
            
            # 添加数据文件
            added_files = self._add_data_files()
            
            if not added_files:
                self.logger.info("ℹ️ 没有数据文件需要提交（可能都没有变化）")
                return False
            
            # 生成提交信息并提交
            commit_msg = self._generate_commit_message(success_modules)
            
            if self._commit_changes(commit_msg):
                # 根据配置决定是否推送
                if self.git_config.get('auto_push', True):
                    return self._push_to_remote()
                else:
                    self.logger.info("ℹ️ 自动推送已禁用，仅本地提交")
                    return True
            else:
                return False
                
        except Exception as e:
            self.logger.error(f"自动Git提交时发生异常: {str(e)}")
            return False
    
    def _is_git_repository(self) -> bool:
        """检查是否是Git仓库"""
        result = subprocess.run(
            ["git", "status"],
            cwd=str(self.project_root),
            capture_output=True,
            text=True
        )
        return result.returncode == 0
    
    def _has_changes(self) -> bool:
        """检查是否有变更"""
        result = subprocess.run(
            ["git", "status", "--porcelain"],
            cwd=str(self.project_root),
            capture_output=True,
            text=True
        )
        return bool(result.stdout.strip())
    
    def _show_changes(self):
        """显示变更的文件"""
        result = subprocess.run(
            ["git", "status", "--porcelain"],
            cwd=str(self.project_root),
            capture_output=True,
            text=True
        )
        
        self.logger.info("📄 检测到以下文件变更:")
        for line in result.stdout.strip().split('\n'):
            if line.strip():
                self.logger.info(f"   {line}")
    
    def _add_data_files(self) -> list:
        """添加数据文件到Git"""
        # 添加数据文件和关键配置文件，不包含Python脚本
        data_patterns = [
            # ETF数据文件
            "ETF日更/0_ETF日K(前复权)/*.csv",
            "ETF日更/0_ETF日K(后复权)/*.csv", 
            "ETF日更/0_ETF日K(除权)/*.csv",
            "ETF周更/0_ETF日K(前复权)/*.csv",
            "ETF周更/0_ETF日K(后复权)/*.csv",
            "ETF周更/0_ETF日K(除权)/*.csv",
            # 市场状况文件
            "ETF市场状况/etf_market_status.json",
            # ETF初筛文件
            "ETF_初筛/data/5000万门槛/*.txt",
            "ETF_初筛/data/3000万门槛/*.txt",
            # 配置文件（重要：包含已处理文件的hash记录）
            "config/file_hashes.json",
            "config/config.json"
        ]
        
        added_files = []
        
        for pattern in data_patterns:
            add_result = subprocess.run(
                ["git", "add", pattern],
                cwd=str(self.project_root),
                capture_output=True,
                text=True
            )
            
            if add_result.returncode == 0:
                added_files.append(pattern)
                self.logger.info(f"✅ 已添加数据文件: {pattern}")
            else:
                # 如果文件不存在或没有变化，不报错
                if "did not match any files" not in add_result.stderr:
                    self.logger.warning(f"⚠️ 添加文件失败: {pattern} - {add_result.stderr}")
        
        return added_files
    
    def _generate_commit_message(self, success_modules: dict) -> str:
        """生成提交信息"""
        success_count = len([m for m in success_modules.values() if m])
        total_count = len(success_modules)
        
        commit_prefix = self.git_config.get('commit_message_prefix', '数据自动更新')
        commit_msg = f"{commit_prefix} - 成功率:{success_count}/{total_count}"
        
        # 添加时间戳（如果配置启用）
        if self.git_config.get('include_timestamp', True):
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            commit_msg = f"{commit_prefix} {timestamp} - 成功率:{success_count}/{total_count}"
        
        # 添加详细信息
        if success_modules.get('daily'):
            commit_msg += "\n✅ 日更数据已更新"
        if success_modules.get('weekly'):
            commit_msg += "\n✅ 周更数据已更新"
        if success_modules.get('market_status'):
            commit_msg += "\n✅ 市场状况已更新"
        if success_modules.get('etf_screening'):
            commit_msg += "\n✅ ETF初筛已完成"
        
        return commit_msg
    
    def _commit_changes(self, commit_msg: str) -> bool:
        """提交变更"""
        commit_result = subprocess.run(
            ["git", "commit", "-m", commit_msg],
            cwd=str(self.project_root),
            capture_output=True,
            text=True
        )
        
        if commit_result.returncode == 0:
            self.logger.info("✅ Git提交成功")
            self.logger.info(f"📝 提交信息: {commit_msg.split(chr(10))[0]}")
            return True
        else:
            self.logger.error(f"❌ Git提交失败: {commit_result.stderr}")
            return False
    
    def _push_to_remote(self) -> bool:
        """推送到远程仓库"""
        push_result = subprocess.run(
            ["git", "push"],
            cwd=str(self.project_root),
            capture_output=True,
            text=True
        )
        
        if push_result.returncode == 0:
            self.logger.info("✅ 推送到远程仓库成功")
            return True
        else:
            self.logger.warning("⚠️ 推送到远程仓库失败，但本地提交成功")
            self.logger.warning(f"推送错误: {push_result.stderr}")
            return False 