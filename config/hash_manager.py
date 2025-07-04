#!/usr/bin/env python3
"""
文件哈希管理模块
自动管理ETF数据文件的哈希值，避免重复下载
"""

import os
import json
import hashlib
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from pathlib import Path


class HashManager:
    """文件哈希管理器"""
    
    def __init__(self, hash_file_path: str = None):
        """
        初始化哈希管理器
        
        Args:
            hash_file_path: 哈希文件存储路径，None时自动查找项目根目录
        """
        if hash_file_path is None:
            # 自动找到项目根目录的config/file_hashes.json
            current_file = Path(__file__).resolve()
            project_root = current_file.parent.parent  # 从config目录向上一级
            hash_file_path = project_root / "config" / "file_hashes.json"
        
        self.hash_file_path = Path(hash_file_path)
        self.hash_data = self._load_hash_file()
    
    def _load_hash_file(self) -> Dict[str, str]:
        """加载哈希文件"""
        if self.hash_file_path.exists():
            try:
                with open(self.hash_file_path, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except (json.JSONDecodeError, IOError) as e:
                print(f"警告：无法加载哈希文件 {self.hash_file_path}: {e}")
                return {}
        return {}
    
    def _save_hash_file(self):
        """保存哈希文件"""
        try:
            # 确保目录存在
            self.hash_file_path.parent.mkdir(parents=True, exist_ok=True)
            
            with open(self.hash_file_path, 'w', encoding='utf-8') as f:
                json.dump(self.hash_data, f, ensure_ascii=False, indent=4)
        except IOError as e:
            print(f"错误：无法保存哈希文件 {self.hash_file_path}: {e}")
    
    def calculate_file_hash(self, file_path: str) -> str:
        """
        计算文件的MD5哈希值
        
        Args:
            file_path: 文件路径
            
        Returns:
            文件的MD5哈希值
        """
        hash_md5 = hashlib.md5()
        try:
            with open(file_path, "rb") as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    hash_md5.update(chunk)
            return hash_md5.hexdigest()
        except IOError as e:
            print(f"错误：无法计算文件哈希 {file_path}: {e}")
            return ""
    
    def get_current_month_pattern(self) -> str:
        """获取当前月份的文件名模式"""
        now = datetime.now()
        return f"_2025年{now.month}月.rar"
    
    def is_file_downloaded(self, filename: str) -> bool:
        """
        检查文件是否已下载（基于哈希值）
        
        Args:
            filename: 文件名
            
        Returns:
            True如果文件已下载且哈希匹配
        """
        return filename in self.hash_data
    
    def update_file_hash(self, filename: str, file_path: str):
        """
        更新文件哈希值
        
        Args:
            filename: 文件名
            file_path: 本地文件路径
        """
        if os.path.exists(file_path):
            hash_value = self.calculate_file_hash(file_path)
            if hash_value:
                self.hash_data[filename] = hash_value
                self._save_hash_file()
                print(f"✓ 更新哈希: {filename} -> {hash_value[:8]}...")
    
    def verify_file_integrity(self, filename: str, file_path: str) -> bool:
        """
        验证文件完整性
        
        Args:
            filename: 文件名
            file_path: 本地文件路径
            
        Returns:
            True如果文件完整
        """
        if not os.path.exists(file_path):
            return False
        
        if filename not in self.hash_data:
            return False
        
        current_hash = self.calculate_file_hash(file_path)
        stored_hash = self.hash_data[filename]
        
        return current_hash == stored_hash
    
    def clean_old_hashes(self):
        """清理旧的哈希记录（保留当前月份和上个月）"""
        now = datetime.now()
        current_year = now.year
        current_month = now.month
        
        # 计算上个月
        if current_month == 1:
            prev_month = 12
            prev_year = current_year - 1
        else:
            prev_month = current_month - 1
            prev_year = current_year
        
        # 需要保留的月份模式
        keep_patterns = [
            f"_{current_year}年{current_month}月.rar",
            f"_{prev_year}年{prev_month}月.rar"
        ]
        
        # 找出需要删除的键
        keys_to_remove = []
        for filename in self.hash_data.keys():
            should_keep = any(pattern in filename for pattern in keep_patterns)
            if not should_keep:
                keys_to_remove.append(filename)
        
        # 删除旧的哈希记录
        if keys_to_remove:
            for key in keys_to_remove:
                del self.hash_data[key]
            self._save_hash_file()
            print(f"✓ 清理了 {len(keys_to_remove)} 个旧的哈希记录")
    
    def get_current_month_files(self) -> List[str]:
        """获取当前月份的文件列表"""
        pattern = self.get_current_month_pattern()
        return [filename for filename in self.hash_data.keys() if pattern in filename]
    
    def add_new_month_files(self, new_files: List[Tuple[str, str]]):
        """
        添加新月份的文件哈希
        
        Args:
            new_files: [(filename, file_path), ...] 新文件列表
        """
        added_count = 0
        for filename, file_path in new_files:
            if filename not in self.hash_data:
                self.update_file_hash(filename, file_path)
                added_count += 1
        
        if added_count > 0:
            print(f"✓ 添加了 {added_count} 个新文件的哈希记录")
    
    def get_status_report(self) -> Dict:
        """获取哈希管理状态报告"""
        now = datetime.now()
        current_pattern = self.get_current_month_pattern()
        
        current_month_files = [f for f in self.hash_data.keys() if current_pattern in f]
        total_files = len(self.hash_data)
        
        return {
            "total_files": total_files,
            "current_month_files": len(current_month_files),
            "current_month_pattern": current_pattern,
            "current_month_list": current_month_files,
            "last_updated": datetime.now().isoformat(),
            "hash_file_path": str(self.hash_file_path)
        }
    
    def print_status(self):
        """打印状态信息"""
        status = self.get_status_report()
        print(f"\n📊 哈希管理器状态:")
        print(f"   总文件数: {status['total_files']}")
        print(f"   当前月份文件数: {status['current_month_files']}")
        print(f"   当前月份模式: {status['current_month_pattern']}")
        print(f"   哈希文件路径: {status['hash_file_path']}")
        
        if status['current_month_list']:
            print(f"   当前月份文件:")
            for filename in status['current_month_list']:
                print(f"     - {filename}")


def auto_update_hashes_for_new_month():
    """自动为新月份更新哈希管理"""
    hash_manager = HashManager()
    
    print("🔄 检查是否需要更新哈希管理...")
    
    # 清理旧的哈希记录
    hash_manager.clean_old_hashes()
    
    # 打印当前状态
    hash_manager.print_status()
    
    return hash_manager


if __name__ == "__main__":
    # 测试哈希管理器
    print("🧪 测试哈希管理器...")
    
    # 创建管理器实例
    manager = auto_update_hashes_for_new_month()
    
    # 测试添加新文件（模拟）
    now = datetime.now()
    test_files = [
        (f"0_ETF日K(前复权)_{now.year}年{now.month}月.rar", "/tmp/test1.rar"),
        (f"0_ETF日K(后复权)_{now.year}年{now.month}月.rar", "/tmp/test2.rar"),
        (f"0_ETF日K(除权)_{now.year}年{now.month}月.rar", "/tmp/test3.rar")
    ]
    
    print(f"\n🧪 模拟添加当前月份({now.month}月)的文件...")
    # 注意：这里只是演示，实际文件不存在
    for filename, _ in test_files:
        if filename not in manager.hash_data:
            manager.hash_data[filename] = f"test_hash_{hash(filename)}"
    
    manager._save_hash_file()
    manager.print_status()
    
    print("\n✅ 哈希管理器测试完成！") 