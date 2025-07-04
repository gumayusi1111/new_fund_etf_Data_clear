#!/usr/bin/env python3
"""
ETF 周更新数据自动同步脚本
1. 从百度网盘下载新增月份 RAR 文件
2. 解压并自动合并到本地历史目录
3. 清理临时文件
4. 自动管理文件哈希，避免重复下载
5. 适用于按月份打包的大量历史数据更新
"""

import os
import sys
import shutil
import tempfile
import re
import subprocess
import json
import hashlib
from datetime import datetime
from typing import List, Tuple
from pathlib import Path

# 添加当前目录到 Python 路径以导入 etf_data_merger
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from etf_data_merger import merge_two_folders

# 添加config目录到路径
config_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'config')
sys.path.insert(0, config_dir)

try:
    import sys
    import importlib.util
    hash_manager_path = os.path.join(config_dir, 'hash_manager.py')
    spec = importlib.util.spec_from_file_location("hash_manager", hash_manager_path)
    hash_manager_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(hash_manager_module)
    HashManager = hash_manager_module.HashManager
except ImportError:
    print("警告：无法导入哈希管理器，将跳过哈希验证功能")
    HashManager = None

try:
    from bypy import ByPy
except ImportError:
    print("错误：未安装 bypy，请运行: pip install bypy")
    sys.exit(1)


# 配置项
BAIDU_REMOTE_BASE = "/ETF"  # 百度网盘中 ETF 数据根目录
LOCAL_ETF_DIR = os.path.dirname(os.path.abspath(__file__))  # 本地 ETF周更 目录
CATEGORIES = ["0_ETF日K(前复权)", "0_ETF日K(后复权)", "0_ETF日K(除权)"]


def list_remote_files_with_info(bp: ByPy, remote_path: str) -> List[Tuple[str, str, str, str]]:
    """
    列出百度网盘指定路径下的文件列表，包含详细信息
    返回: [(文件名, 大小, 修改时间, md5), ...]
    """
    try:
        import io
        import sys
        from contextlib import redirect_stdout
        
        # 捕获 list 命令的输出
        f = io.StringIO()
        with redirect_stdout(f):
            bp.list(remote_path)
        
        output = f.getvalue()
        files = []
        
        # 解析输出，查找以 'F ' 开头的行（文件）
        for line in output.split('\n'):
            line = line.strip()
            if line.startswith('F '):
                # bypy list输出格式: F 文件名 大小 修改时间 md5
                # 例如: F filename.rar 123456789 2024-06-26T15:30:45 abc123def456
                try:
                    parts = line.split()
                    if len(parts) >= 5:
                        file_name = parts[1]
                        file_size = parts[2]
                        file_time = parts[3] + "T" + parts[4] if len(parts) >= 5 else parts[3]
                        file_md5 = parts[5] if len(parts) >= 6 else ""
                        files.append((file_name, file_size, file_time, file_md5))
                    elif len(parts) >= 2:
                        # 如果格式不完整，至少获取文件名
                        file_name = parts[1]
                        files.append((file_name, "", "", ""))
                except Exception as e:
                    print(f"解析行失败: {line}, 错误: {e}")
                    continue
        
        return files
    except Exception as e:
        print(f"列出远程文件失败: {e}")
        return []


def list_remote_files(bp: ByPy, remote_path: str) -> List[str]:
    """列出百度网盘指定路径下的文件列表（保持向后兼容）"""
    files_info = list_remote_files_with_info(bp, remote_path)
    return [file_name for file_name, _, _, _ in files_info]


def check_file_needs_update(hash_manager, file_name: str, remote_size: str, remote_time: str, remote_md5: str) -> Tuple[bool, str]:
    """
    检查文件是否需要更新
    
    Args:
        hash_manager: 哈希管理器实例
        file_name: 文件名
        remote_size: 远程文件大小
        remote_time: 远程文件修改时间
        remote_md5: 远程文件MD5
        
    Returns:
        (是否需要更新, 原因说明)
    """
    if not hash_manager:
        return True, "无哈希管理器，建议下载"
    
    # 检查是否有本地记录
    if not hash_manager.is_file_downloaded(file_name):
        return True, "首次下载"
    
    # 获取本地记录的哈希值
    local_hash = hash_manager.hash_data.get(file_name, "")
    
    # 如果远程提供了MD5且与本地不同，说明文件有更新
    if remote_md5 and local_hash and remote_md5 != local_hash:
        return True, f"远程文件已更新 (MD5: {remote_md5[:8]}... vs 本地: {local_hash[:8]}...)"
    
    # 如果没有MD5但有大小和时间信息
    if remote_size or remote_time:
        # 这里可以添加更复杂的检查逻辑
        # 比如检查本地文件的修改时间和大小
        # 目前先基于MD5检查
        pass
    
    # 如果有本地记录但没有远程MD5信息，建议用户手动检查
    if not remote_md5 and local_hash:
        return False, "已有本地记录，但无法验证远程更新状态，建议手动检查"
    
    return False, "文件已是最新"


def get_current_month_files_with_info(files_info: List[Tuple[str, str, str, str]]) -> List[Tuple[str, str, int, int, str, str, str]]:
    """
    查找当前月份的 RAR 文件，包含详细信息（支持新旧两种命名格式）
    返回: [(文件名, 类别, 年份, 月份, 大小, 修改时间, MD5), ...]
    """
    # 获取当前年月日
    now = datetime.now()
    current_year = now.year
    current_month = now.month
    current_day = now.day
    
    print(f"当前时间: {current_year}年{current_month}月{current_day}日")
    
    # 旧格式：0_ETF日K(前复权)_2025年6月.rar
    old_pattern = r'(0_ETF日K\([^)]+\))_(\d{4})年(\d+)月\.rar$'
    # 新格式：0_ETF日K(前复权)_2025年_0506_0627.rar
    new_pattern = r'(0_ETF日K\([^)]+\))_(\d{4})年_(\d{2})(\d{2})_(\d{2})(\d{2})\.rar$'
    
    current_month_files = []
    
    for file_name, file_size, file_time, file_md5 in files_info:
        # 检查旧格式
        old_match = re.match(old_pattern, file_name)
        if old_match:
            category = old_match.group(1)
            year = int(old_match.group(2))
            month = int(old_match.group(3))
            
            # 只处理当前月份的文件
            if year == current_year and month == current_month:
                current_month_files.append((file_name, category, year, month, file_size, file_time, file_md5))
                print(f"✓ 发现旧格式文件: {file_name} ({year}年{month}月)")
                continue
        
        # 检查新格式（日期范围）
        new_match = re.match(new_pattern, file_name)
        if new_match:
            category = new_match.group(1)
            year = int(new_match.group(2))
            start_month = int(new_match.group(3))
            start_day = int(new_match.group(4))
            end_month = int(new_match.group(5))
            end_day = int(new_match.group(6))
            
            # 检查当前年份
            if year == current_year:
                # 检查文件的日期范围是否包含当前月份的数据
                # 只要结束月份 >= 当前月份，就认为包含当前月份的数据
                if end_month >= current_month and start_month <= current_month:
                    current_month_files.append((file_name, category, year, current_month, file_size, file_time, file_md5))
                    print(f"✓ 发现新格式文件: {file_name} ({start_month}月{start_day}日-{end_month}月{end_day}日，包含当前{current_month}月数据)")
                else:
                    print(f"  跳过日期范围文件: {file_name} ({start_month}月{start_day}日-{end_month}月{end_day}日，不包含当前{current_month}月数据)")
    
    return current_month_files


def extract_rar(rar_path: str, extract_to: str) -> bool:
    """解压 RAR 文件"""
    try:
        # 检查是否安装了 unar (macOS) 或 unrar (Linux)
        unar_available = subprocess.run(['which', 'unar'], capture_output=True, text=True).returncode == 0
        unrar_available = subprocess.run(['which', 'unrar'], capture_output=True, text=True).returncode == 0
        
        if unar_available:
            # 使用 unar (macOS 推荐)
            cmd = ['unar', '-o', extract_to, rar_path]
        elif unrar_available:
            # 使用 unrar (Linux)
            cmd = ['unrar', 'x', '-o+', rar_path, extract_to]
        else:
            print("错误：未安装解压工具")
            print("macOS: brew install unar")
            print("Linux: apt install unrar")
            return False
        
        # 解压 RAR 文件
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode == 0:
            print(f"✓ 解压成功: {os.path.basename(rar_path)}")
            return True
        else:
            print(f"✗ 解压失败: {result.stderr}")
            return False
    except Exception as e:
        print(f"解压出错: {e}")
        return False


def get_current_month_files(files: List[str]) -> List[Tuple[str, str, int, int]]:
    """
    查找当前月份的 RAR 文件（向后兼容版本，支持新旧两种命名格式）
    返回: [(文件名, 类别, 年份, 月份), ...]
    """
    # 获取当前年月日
    now = datetime.now()
    current_year = now.year
    current_month = now.month
    current_day = now.day
    
    print(f"当前时间: {current_year}年{current_month}月{current_day}日")
    
    # 旧格式：0_ETF日K(前复权)_2025年6月.rar
    old_pattern = r'(0_ETF日K\([^)]+\))_(\d{4})年(\d+)月\.rar$'
    # 新格式：0_ETF日K(前复权)_2025年_0506_0627.rar
    new_pattern = r'(0_ETF日K\([^)]+\))_(\d{4})年_(\d{2})(\d{2})_(\d{2})(\d{2})\.rar$'
    
    current_month_files = []
    
    for file_name in files:
        # 检查旧格式
        old_match = re.match(old_pattern, file_name)
        if old_match:
            category = old_match.group(1)
            year = int(old_match.group(2))
            month = int(old_match.group(3))
            
            if year == current_year and month == current_month:
                current_month_files.append((file_name, category, year, month))
                continue
        
        # 检查新格式（日期范围）
        new_match = re.match(new_pattern, file_name)
        if new_match:
            category = new_match.group(1)
            year = int(new_match.group(2))
            start_month = int(new_match.group(3))
            start_day = int(new_match.group(4))
            end_month = int(new_match.group(5))
            end_day = int(new_match.group(6))
            
            if year == current_year:
                # 检查文件的日期范围是否包含当前月份的数据
                if end_month >= current_month and start_month <= current_month:
                    current_month_files.append((file_name, category, year, current_month))
    
    return current_month_files


def sync_current_month_data():
    """同步当前月份的数据（专注于当月压缩包的周更新）"""
    now = datetime.now()
    print(f"开始同步当前月份({now.year}年{now.month}月)的 ETF 数据...")
    print("📅 只检查当前月份的压缩包更新，忽略历史数据")
    
    # 初始化哈希管理器
    hash_manager = None
    if HashManager:
        try:
            hash_manager = HashManager()
            print("✓ 哈希管理器初始化成功")
            hash_manager.print_status()
            
            # 清理旧的哈希记录
            hash_manager.clean_old_hashes()
        except Exception as e:
            print(f"⚠️ 哈希管理器初始化失败: {e}")
            hash_manager = None
    
    # 初始化 bypy
    bp = ByPy()
    
    # 获取远程文件列表
    print("获取百度网盘文件列表...")
    remote_files = list_remote_files(bp, BAIDU_REMOTE_BASE)
    if not remote_files:
        print("未找到任何文件")
        return
    
    # 查找当前月份文件
    current_month_files = get_current_month_files_with_info(list_remote_files_with_info(bp, BAIDU_REMOTE_BASE))
    if not current_month_files:
        print(f"未找到 {now.year}年{now.month}月 的 RAR 文件")
        print("可能原因：")
        print("1. 当月数据尚未上传到百度网盘")
        print("2. 文件命名格式不匹配")
        return
    
    print(f"找到当前月份的 {len(current_month_files)} 个文件:")
    for file_name, category, year, month, file_size, file_time, file_md5 in current_month_files:
        print(f"  - {file_name} ({category}) [{file_size} bytes]")
    
    # 检查哈希，过滤已下载的文件
    files_to_download = []
    files_need_manual_check = []
    
    if hash_manager:
        print("\n🔍 智能检查当前月份文件更新状态...")
        for file_name, category, year, month, file_size, file_time, file_md5 in current_month_files:
            # 只检查当前月份的文件
            now = datetime.now()
            if year != now.year or month != now.month:
                print(f"⏭️ 跳过非当前月份文件: {file_name}")
                continue
                
            needs_update, reason = check_file_needs_update(hash_manager, file_name, file_size, file_time, file_md5)
            
            if needs_update:
                files_to_download.append((file_name, category, year, month, file_size, file_time, file_md5))
                print(f"📥 需要下载: {file_name} - {reason}")
            elif "建议手动检查" in reason:
                files_need_manual_check.append((file_name, category, year, month, file_size, file_time, file_md5))
                print(f"❓ 需要确认: {file_name} - {reason}")
            else:
                print(f"✅ 文件最新: {file_name} - {reason}")
    else:
        # 没有哈希管理器时，也只处理当前月份
        now = datetime.now()
        files_to_download = [(f, c, y, m, s, t, md5) for f, c, y, m, s, t, md5 in current_month_files 
                           if y == now.year and m == now.month]

    # 如果有需要手动检查的文件，询问用户
    if files_need_manual_check:
        print(f"\n⚠️ 发现 {len(files_need_manual_check)} 个当前月份文件需要手动确认:")
        for file_name, category, year, month, file_size, file_time, file_md5 in files_need_manual_check:
            print(f"  - {file_name} ({category})")
            print(f"    大小: {file_size} bytes")
            print(f"    远程修改时间: {file_time}")
            print(f"    远程MD5: {file_md5[:16]}..." if file_md5 else "    远程MD5: 未提供")
        
        print(f"\n这些{now.month}月文件已有本地记录，但无法确定远程是否有更新。")
        response = input("是否要重新下载这些文件？(y/n/s=跳过): ").lower().strip()
        
        if response == 'y':
            files_to_download.extend(files_need_manual_check)
            print("✓ 已添加到下载列表")
        elif response == 's':
            print("✓ 跳过这些文件")
        else:
            print("✓ 不下载这些文件")

    if not files_to_download:
        now = datetime.now()
        print(f"🎉 当前月份({now.month}月)所有文件都已是最新，无需下载！")
        return

    # 检查是否有完整的三个类别
    found_categories = set(category for _, category, _, _, _, _, _ in files_to_download)
    expected_categories = set(CATEGORIES)
    missing_categories = expected_categories - found_categories
    
    if missing_categories:
        print(f"⚠️ 缺少以下类别的文件: {', '.join(missing_categories)}")
        print("将只处理已找到的文件...")
    
    # 创建临时目录
    temp_dir = tempfile.mkdtemp(prefix="etf_sync_current_")
    print(f"临时目录: {temp_dir}")
    
    try:
        success_count = 0
        # 下载并处理每个文件
        for file_name, category, year, month, file_size, file_time, file_md5 in files_to_download:
            print(f"\n处理 {file_name}...")
            
            # 下载文件
            remote_file_path = f"{BAIDU_REMOTE_BASE}/{file_name}"
            local_rar_path = os.path.join(temp_dir, file_name)
            
            print(f"下载中...")
            try:
                bp.downfile(remote_file_path, local_rar_path)
                print(f"✓ 下载完成")
                
                # 更新哈希
                if hash_manager:
                    hash_manager.update_file_hash(file_name, local_rar_path)
                    
            except Exception as e:
                print(f"✗ 下载失败: {e}")
                continue
            
            # 解压文件
            extract_dir = os.path.join(temp_dir, f"extract_{category}_{year}_{month}")
            os.makedirs(extract_dir, exist_ok=True)
            
            if not extract_rar(local_rar_path, extract_dir):
                continue
            
            # 查找解压后的目录
            extracted_dirs = [d for d in os.listdir(extract_dir) 
                            if os.path.isdir(os.path.join(extract_dir, d)) and category in d]
            
            if not extracted_dirs:
                print(f"✗ 未找到解压后的目录")
                continue
            
            extracted_data_dir = os.path.join(extract_dir, extracted_dirs[0])
            
            # 合并到对应的历史目录
            hist_dir = os.path.join(LOCAL_ETF_DIR, category)
            if os.path.isdir(hist_dir):
                print(f"合并到 {category}...")
                merge_two_folders(hist_dir, extracted_data_dir)
                print(f"✓ 合并完成")
                success_count += 1
            else:
                print(f"✗ 历史目录不存在: {hist_dir}")
        
        # 汇总结果
        now = datetime.now()
        print(f"\n🎉 {now.year}年{now.month}月数据同步完成!")
        print(f"成功处理: {success_count}/{len(files_to_download)} 个文件")
        
        if success_count > 0:
            print(f"数据已更新到: {LOCAL_ETF_DIR}")
            
        # 显示哈希管理器最终状态
        if hash_manager:
            print("\n📊 哈希管理器最终状态:")
            hash_manager.print_status()
        
    finally:
        # 清理临时目录
        print(f"清理临时目录...")
        shutil.rmtree(temp_dir, ignore_errors=True)


def test_connection():
    """测试百度网盘连接和列出文件"""
    print("测试百度网盘连接...")
    bp = ByPy()
    
    # 测试基本连接
    try:
        bp.info()
        print("✓ 连接成功")
    except Exception as e:
        print(f"✗ 连接失败: {e}")
        return False
    
    # 测试列出 ETF 目录
    print(f"\n测试列出 {BAIDU_REMOTE_BASE} 目录...")
    try:
        files = list_remote_files(bp, BAIDU_REMOTE_BASE)
        if files:
            print(f"✓ 找到 {len(files)} 个文件:")
            for file_name in files[:10]:  # 显示前10个
                print(f"  - {file_name}")
            if len(files) > 10:
                print(f"  ... 还有 {len(files) - 10} 个文件")
                
            # 查找当前月份文件
            current_files = get_current_month_files_with_info(list_remote_files_with_info(bp, BAIDU_REMOTE_BASE))
            if current_files:
                print(f"\n找到当前月份的 {len(current_files)} 个文件:")
                for file_name, category, year, month, file_size, file_time, file_md5 in current_files:
                    print(f"  - {file_name} ({category})")
                    
                # 测试哈希管理
                if HashManager:
                    hash_manager = HashManager()
                    print(f"\n📊 哈希管理器状态:")
                    hash_manager.print_status()
            else:
                now = datetime.now()
                print(f"\n未找到 {now.year}年{now.month}月 的文件")
        else:
            print("✗ 未找到任何文件")
    except Exception as e:
        print(f"✗ 列出文件失败: {e}")
    
    return True


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "test":
        test_connection()
    else:
        sync_current_month_data() 