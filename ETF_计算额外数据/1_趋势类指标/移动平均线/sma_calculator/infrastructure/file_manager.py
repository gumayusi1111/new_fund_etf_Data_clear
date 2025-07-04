#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
文件管理器 (重构版)
================

精简而强大的文件管理功能
"""

import os
import json
from datetime import datetime
from typing import Dict, List, Optional


class SMAFileManager:
    """SMA文件管理器 - 重构版"""
    
    def __init__(self, output_dir: str):
        """初始化文件管理器"""
        self.output_dir = output_dir
        self.ensure_directory_exists(output_dir)
    
    def ensure_directory_exists(self, directory: str) -> bool:
        """确保目录存在"""
        try:
            os.makedirs(directory, exist_ok=True)
            return True
        except Exception:
            return False
    
    def create_output_directory(self, output_dir: Optional[str] = None) -> str:
        """创建输出目录"""
        target_dir = output_dir or self.output_dir
        os.makedirs(target_dir, exist_ok=True)
        return target_dir
    
    def save_json_result(self, data: Dict, filename: str, subdir: str = "") -> bool:
        """保存JSON结果"""
        try:
            save_dir = os.path.join(self.output_dir, subdir) if subdir else self.output_dir
            self.ensure_directory_exists(save_dir)
            
            file_path = os.path.join(save_dir, filename)
            
            # 添加元数据
            data_with_meta = data.copy()
            data_with_meta['_metadata'] = {
                'generated_time': datetime.now().isoformat(),
                'file_version': '1.0',
                'data_type': 'SMA_analysis'
            }
            
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(data_with_meta, f, ensure_ascii=False, indent=2)
            
            return True
        except Exception:
            return False
    
    def save_csv_result(self, csv_content: str, filename: str, subdir: str = "") -> bool:
        """保存CSV结果"""
        try:
            save_dir = os.path.join(self.output_dir, subdir) if subdir else self.output_dir
            self.ensure_directory_exists(save_dir)
            
            file_path = os.path.join(save_dir, filename)
            
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(csv_content)
            
            return True
        except Exception:
            return False
    
    def get_output_file_path(self, filename: str, subdir: str = "") -> str:
        """获取输出文件路径"""
        if subdir:
            return os.path.join(self.output_dir, subdir, filename)
        return os.path.join(self.output_dir, filename)
    
    def list_output_files(self, subdir: str = "") -> List[str]:
        """列出输出文件"""
        try:
            target_dir = os.path.join(self.output_dir, subdir) if subdir else self.output_dir
            
            if not os.path.exists(target_dir):
                return []
            
            files = [f for f in os.listdir(target_dir) 
                    if os.path.isfile(os.path.join(target_dir, f))]
            return sorted(files)
        except Exception:
            return []
    
    def clean_old_files(self, days_to_keep: int = 7) -> int:
        """清理旧文件"""
        try:
            import time
            
            current_time = time.time()
            cutoff_time = current_time - (days_to_keep * 24 * 60 * 60)
            cleaned_count = 0
            
            for root, dirs, files in os.walk(self.output_dir):
                for file in files:
                    file_path = os.path.join(root, file)
                    try:
                        if os.path.getmtime(file_path) < cutoff_time:
                            os.remove(file_path)
                            cleaned_count += 1
                    except Exception:
                        continue
            
            return cleaned_count
        except Exception:
            return 0
    
    def get_directory_summary(self) -> Dict[str, any]:
        """获取目录摘要信息"""
        try:
            if not os.path.exists(self.output_dir):
                return {'files': 0, 'total_size': 0}
            
            files_count = 0
            total_size = 0
            
            for root, dirs, files in os.walk(self.output_dir):
                for file in files:
                    file_path = os.path.join(root, file)
                    try:
                        total_size += os.path.getsize(file_path)
                        files_count += 1
                    except Exception:
                        continue
            
            return {
                'files': files_count,
                'total_size': total_size,
                'formatted_size': self._format_file_size(total_size)
            }
        except Exception:
            return {'files': 0, 'total_size': 0, 'formatted_size': '0 B'}
    
    def _format_file_size(self, size_bytes: int) -> str:
        """格式化文件大小"""
        if size_bytes < 1024:
            return f"{size_bytes} B"
        elif size_bytes < 1024 * 1024:
            return f"{size_bytes / 1024:.1f} KB"
        elif size_bytes < 1024 * 1024 * 1024:
            return f"{size_bytes / (1024 * 1024):.1f} MB"
        else:
            return f"{size_bytes / (1024 * 1024 * 1024):.1f} GB" 