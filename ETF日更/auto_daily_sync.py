#!/usr/bin/env python3
"""
ETF 日更新自动同步脚本 (临时处理版本)
1. 只检查今天日期的文件
2. 临时下载到内存/临时目录处理
3. 处理完成后立即删除临时文件
4. 实现真正的增量更新，不占用存储空间
"""

import os
import sys
import subprocess
import tempfile
import shutil
from datetime import datetime, timedelta
from typing import List, Set, Dict

# 添加config目录到路径
config_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'config')
sys.path.insert(0, config_dir)

try:
    import sys
    import importlib.util
    # 导入哈希管理器
    hash_manager_path = os.path.join(config_dir, 'hash_manager.py')
    spec = importlib.util.spec_from_file_location("hash_manager", hash_manager_path)
    hash_manager_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(hash_manager_module)
    HashManager = hash_manager_module.HashManager
    
    # 导入日志配置
    logger_config_path = os.path.join(config_dir, 'logger_config.py')
    spec = importlib.util.spec_from_file_location("logger_config", logger_config_path)
    logger_config_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(logger_config_module)
    setup_daily_logger = logger_config_module.setup_daily_logger
    
    # 设置日更专用日志
    logger = setup_daily_logger()
except ImportError as e:
    print(f"警告：无法导入配置模块: {e}")
    HashManager = None
    logger = None

try:
    from bypy import ByPy
except ImportError:
    print("错误：未安装 bypy，请运行: pip install bypy")
    sys.exit(1)

# 配置项
BAIDU_REMOTE_BASE = "/ETF_按日期"  # 百度网盘中按日期数据根目录
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))  # 当前脚本目录
PROCESSOR_SCRIPT = "daily_etf_processor.py"  # 数据处理脚本


def get_today_filename() -> str:
    """获取今天的文件名"""
    return datetime.now().strftime('%Y%m%d.csv')


def get_recent_filenames(days_back=15) -> List[str]:
    """获取最近N天的文件名列表"""
    filenames = []
    today = datetime.now()
    
    for i in range(days_back):
        date = today - timedelta(days=i)
        filename = date.strftime('%Y%m%d.csv')
        filenames.append(filename)
    
    return filenames


def batch_check_remote_files(bp: ByPy, filenames: List[str]) -> List[str]:
    """批量检查百度网盘中哪些文件存在"""
    print(f"🔍 检查百度网盘中最近{len(filenames)}天的文件...")
    
    try:
        import io
        from contextlib import redirect_stdout
        
        # 获取远程文件列表（一次性获取）
        f = io.StringIO()
        with redirect_stdout(f):
            bp.list(BAIDU_REMOTE_BASE)
        
        output = f.getvalue()
        
        # 解析出所有远程文件名
        remote_files = set()
        for line in output.split('\n'):
            line = line.strip()
            if line.startswith('F '):
                parts = line.split(' ', 3)
                if len(parts) >= 2 and parts[1].endswith('.csv'):
                    remote_files.add(parts[1])
        
        # 检查哪些目标文件存在
        existing_files = []
        for filename in filenames:
            if filename in remote_files:
                existing_files.append(filename)
        
        # 按日期排序（最新的在前）
        existing_files.sort(reverse=True)
        
        print(f"☁️ 远程存在 {len(existing_files)}/{len(filenames)} 个文件")
        return existing_files
        
    except Exception as e:
        print(f"❌ 批量检查远程文件失败: {e}")
        return []


def check_local_data_exists(date_str: str) -> bool:
    """检查指定日期的本地数据是否存在"""
    test_etfs = ["159001", "159003", "159005"]
    output_dirs = ["0_ETF日K(前复权)", "0_ETF日K(后复权)", "0_ETF日K(除权)"]
    
    for etf_code in test_etfs:
        for output_dir in output_dirs:
            etf_file = os.path.join(CURRENT_DIR, output_dir, f"{etf_code}.csv")
            
            if os.path.exists(etf_file):
                try:
                    # 快速检查：只读前几行
                    with open(etf_file, 'r', encoding='utf-8') as f:
                        f.readline()  # 跳过表头
                        for _ in range(5):  # 只检查前5行
                            line = f.readline().strip()
                            if line and date_str in line:
                                return True
                except Exception:
                    continue
    
    return False


def check_local_processed_status(filenames: List[str], hash_manager) -> Dict[str, bool]:
    """检查本地文件处理状态"""
    print(f"📊 检查本地处理状态...")
    
    status = {}
    
    for filename in filenames:
        date_str = filename[:8]  # YYYYMMDD
        
        # 方法1：检查hash记录
        has_hash_record = hash_manager and hash_manager.is_file_downloaded(filename)
        
        # 方法2：检查本地数据完整性
        has_local_data = check_local_data_exists(date_str)
        
        # 任一方法确认存在就认为已处理
        status[filename] = has_hash_record or has_local_data
    
    processed_count = sum(status.values())
    print(f"💾 本地已处理 {processed_count}/{len(filenames)} 个文件")
    
    return status


def check_remote_file_exists(bp: ByPy, filename: str) -> bool:
    """检查百度网盘中指定文件是否存在"""
    try:
        import io
        from contextlib import redirect_stdout
        
        # 捕获 list 命令的输出
        f = io.StringIO()
        with redirect_stdout(f):
            bp.list(BAIDU_REMOTE_BASE)
        
        output = f.getvalue()
        
        # 解析输出，查找指定文件
        for line in output.split('\n'):
            line = line.strip()
            if line.startswith('F '):
                # 格式: F 文件名 大小 日期时间 哈希
                parts = line.split(' ', 3)
                if len(parts) >= 2 and parts[1] == filename:
                    return True
        
        return False
    except Exception as e:
        print(f"检查远程文件失败: {e}")
        return False


def download_to_temp(bp: ByPy, filename: str, temp_dir: str, hash_manager=None) -> str:
    """下载文件到临时目录"""
    try:
        remote_path = f"{BAIDU_REMOTE_BASE}/{filename}"
        local_temp_path = os.path.join(temp_dir, filename)
        
        print(f"📥 下载到临时目录: {filename}...")
        bp.downfile(remote_path, local_temp_path)
        
        # 更新哈希记录
        if hash_manager:
            hash_manager.update_file_hash(filename, local_temp_path)
        
        print(f"✓ 临时下载完成: {filename}")
        return local_temp_path
    except Exception as e:
        print(f"✗ 下载失败 {filename}: {e}")
        return None


def should_update_data(filename: str, hash_manager) -> tuple[bool, str]:
    """判断是否需要更新数据"""
    if not hash_manager:
        return True, "无哈希验证"
    
    if not hash_manager.is_file_downloaded(filename):
        return True, "新文件或已更新"
    
    # 检查本地数据完整性：三个复权目录都必须有今天的数据
    today_date = filename[:8]  # YYYYMMDD
    output_dirs = [
        "0_ETF日K(前复权)",
        "0_ETF日K(后复权)", 
        "0_ETF日K(除权)"
    ]
    
    # 检查几个代表性ETF是否有今天的数据
    test_etfs = ["159001", "159003", "159005"]
    
    for etf_code in test_etfs:
        for output_dir in output_dirs:
            etf_file = os.path.join(CURRENT_DIR, output_dir, f"{etf_code}.csv")
            
            if not os.path.exists(etf_file):
                continue  # 如果文件不存在，跳过检查
            
            try:
                # 读取文件第一行数据，检查是否有今天的日期
                with open(etf_file, 'r', encoding='utf-8') as f:
                    f.readline()  # 跳过表头
                    first_data_line = f.readline().strip()
                    
                if first_data_line and today_date not in first_data_line:
                    return True, f"本地{output_dir}数据不完整，需要重新处理"
                    
            except Exception as e:
                return True, f"检查本地数据时出错，需要重新处理: {e}"
    
    return False, "已是最新"


def run_processor_with_temp_data(temp_file_path: str) -> bool:
    """使用临时数据运行处理脚本"""
    try:
        # 创建临时的源数据目录结构
        temp_source_dir = tempfile.mkdtemp(prefix="etf_temp_source_")
        temp_filename = os.path.basename(temp_file_path)
        
        # 复制文件到临时源目录
        shutil.copy2(temp_file_path, os.path.join(temp_source_dir, temp_filename))
        
        # 修改处理脚本的源目录配置（临时）
        date_str = temp_filename[:8]  # YYYYMMDD
        cmd = [
            "python", PROCESSOR_SCRIPT, 
            "--mode", "range", 
            "--start-date", date_str, 
            "--end-date", date_str,
            "--temp-source-dir", temp_source_dir  # 传递临时目录
        ]
        
        print(f"🔄 运行增量处理: {' '.join(cmd)}")
        result = subprocess.run(cmd, cwd=CURRENT_DIR, capture_output=True, text=True)
        
        # 清理临时源目录
        shutil.rmtree(temp_source_dir, ignore_errors=True)
        
        if result.returncode == 0:
            print("✓ 数据处理完成")
            # 显示处理结果摘要
            if result.stdout:
                lines = result.stdout.strip().split('\n')
                # 只显示重要的输出信息
                for line in lines[-5:]:  # 显示最后5行
                    if any(keyword in line for keyword in ['完成', '成功', '处理', '生成', '统计']):
                        print(f"  {line}")
            return True
        else:
            print(f"✗ 数据处理失败: {result.stderr}")
            return False
            
    except Exception as e:
        print(f"✗ 运行处理脚本失败: {e}")
        return False


def daily_incremental_sync():
    """每日增量同步 - 临时处理版本"""
    today_file = get_today_filename()
    print(f"🚀 ETF日更新 - 检查今天的数据: {today_file}")
    
    # 初始化 bypy
    try:
        bp = ByPy()
        print("✓ 百度网盘连接成功")
    except Exception as e:
        print(f"✗ 百度网盘连接失败: {e}")
        return False
    
    # 初始化哈希管理器
    hash_manager = None
    if HashManager:
        try:
            hash_manager = HashManager()
            print("✓ 哈希管理器初始化成功")
        except Exception as e:
            print(f"⚠️ 哈希管理器初始化失败: {e}")
            hash_manager = None
    
    # 检查是否需要更新
    need_update, reason = should_update_data(today_file, hash_manager)
    
    if not need_update:
        print(f"✅ 今天的数据已是最新 ({reason})")
        print("无需更新，保持现有数据")
        return True
    
    print(f"📋 需要更新: {today_file} ({reason})")
    
    # 检查百度网盘中是否有今天的文件
    print(f"🔍 检查百度网盘中的 {today_file}...")
    remote_exists = check_remote_file_exists(bp, today_file)
    
    if not remote_exists:
        print(f"❌ 百度网盘中没有今天的文件: {today_file}")
        print("可能原因：")
        print("1. 今天是非交易日（周末/节假日）")
        print("2. 数据尚未上传")
        return False
    
    print(f"✓ 找到远程文件: {today_file}")
    
    # 创建临时目录
    temp_dir = tempfile.mkdtemp(prefix="etf_daily_temp_")
    print(f"📁 创建临时处理目录: {temp_dir}")
    
    try:
        # 下载到临时目录
        temp_file_path = download_to_temp(bp, today_file, temp_dir, hash_manager)
        
        if not temp_file_path:
            print("⚠️ 文件下载失败")
            return False
        
        print(f"📊 临时文件大小: {os.path.getsize(temp_file_path)} 字节")
        
        # 处理数据
        print("🔄 开始增量处理...")
        if run_processor_with_temp_data(temp_file_path):
            print("🎉 今日增量更新完成！")
            return True
        else:
            print("⚠️ 数据处理失败")
            return False
            
    finally:
        # 确保清理临时目录
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir, ignore_errors=True)
            print(f"🧽 清理临时目录: {temp_dir}")


def detect_missing_data(days_back=15) -> Dict[str, List[str]]:
    """检测最近N天的缺失数据"""
    print(f"🚀 开始检测最近{days_back}天的缺失数据...")
    
    # 1. 生成要检查的文件名
    target_filenames = get_recent_filenames(days_back)
    
    # 2. 初始化百度网盘和hash管理器
    try:
        bp = ByPy()
        hash_manager = HashManager() if HashManager else None
    except Exception as e:
        print(f"❌ 初始化失败: {e}")
        return {'missing': [], 'existing': [], 'processed': []}
    
    # 3. 批量检查远程存在的文件
    remote_existing = batch_check_remote_files(bp, target_filenames)
    
    # 4. 检查本地处理状态
    local_status = check_local_processed_status(remote_existing, hash_manager)
    
    # 5. 找出缺失的文件
    missing_files = [f for f in remote_existing if not local_status.get(f, False)]
    processed_files = [f for f in remote_existing if local_status.get(f, False)]
    
    result = {
        'missing': missing_files,        # 需要下载处理的
        'existing': remote_existing,     # 远程存在的
        'processed': processed_files     # 已处理的
    }
    
    # 6. 显示结果
    show_detection_result(result, days_back)
    
    return result


def show_detection_result(result: Dict[str, List[str]], days_back: int):
    """显示检测结果"""
    missing = result['missing']
    existing = result['existing'] 
    processed = result['processed']
    
    print("\n" + "=" * 50)
    print(f"📊 最近{days_back}天数据检测结果")
    print("=" * 50)
    
    print(f"☁️ 远程文件总数: {len(existing)}")
    print(f"✅ 已处理文件数: {len(processed)}")
    print(f"❌ 缺失文件数: {len(missing)}")
    
    if missing:
        print(f"\n📋 缺失文件明细:")
        # 按日期排序显示（最新的在前）
        sorted_missing = sorted(missing, reverse=True)
        for filename in sorted_missing:
            date_str = filename[:8]
            formatted_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
            weekday = datetime.strptime(date_str, '%Y%m%d').strftime('%A')
            print(f"   📅 {filename} ({formatted_date}, {weekday})")
        
        print(f"\n💡 建议命令:")
        print(f"   python auto_daily_sync.py --mode fill-missing")
    else:
        print("\n🎉 数据完整，无缺失!")
    
    print("=" * 50)


def process_single_missing_file(filename: str) -> bool:
    """处理单个缺失文件（复用现有逻辑）"""
    # 创建临时目录
    temp_dir = tempfile.mkdtemp(prefix="etf_missing_")
    
    try:
        bp = ByPy()
        hash_manager = HashManager() if HashManager else None
        
        # 下载到临时目录
        temp_file_path = download_to_temp(bp, filename, temp_dir, hash_manager)
        
        if temp_file_path:
            # 处理数据（复用现有函数）
            return run_processor_with_temp_data(temp_file_path)
        
        return False
        
    except Exception as e:
        print(f"✗ 处理文件失败 {filename}: {e}")
        return False
        
    finally:
        # 清理临时目录
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir, ignore_errors=True)


def batch_fill_missing_files(missing_files: List[str]) -> bool:
    """批量补齐缺失文件"""
    if not missing_files:
        print("✅ 无缺失文件需要补齐")
        return True
    
    print(f"\n🔄 开始批量补齐 {len(missing_files)} 个缺失文件...")
    
    # 按日期排序，先处理最新的缺失数据
    sorted_missing = sorted(missing_files, reverse=True)
    
    success_count = 0
    for i, filename in enumerate(sorted_missing, 1):
        print(f"\n[{i}/{len(missing_files)}] 处理 {filename}...")
        
        # 使用现有的下载处理逻辑
        if process_single_missing_file(filename):
            success_count += 1
    
    print(f"\n🎉 批量补漏完成: {success_count}/{len(missing_files)}")
    return success_count > 0


def smart_daily_update(days_back=15):
    """智能日更新：先补漏，再更新今天"""
    print("🚀 智能日更新模式...")
    
    # 1. 检测缺失
    result = detect_missing_data(days_back)
    
    # 2. 补齐缺失
    missing_success = True
    if result['missing']:
        print(f"\n发现 {len(result['missing'])} 个缺失文件，开始补齐...")
        missing_success = batch_fill_missing_files(result['missing'])
    
    # 3. 执行今天的更新
    print(f"\n📅 执行今日常规更新...")
    today_success = daily_incremental_sync()
    
    # 4. 汇总结果
    if missing_success and today_success:
        print("🎉 智能更新完全成功！")
        return True
    elif today_success:
        print("✅ 今日更新成功，但缺失数据补齐可能有问题")
        return True
    else:
        print("⚠️ 智能更新部分失败")
        return False


def test_connection():
    """测试百度网盘连接"""
    print("🔧 测试百度网盘连接...")
    
    try:
        bp = ByPy()
        bp.info()
        print("✓ 连接成功")
    except Exception as e:
        print(f"✗ 连接失败: {e}")
        return False
    
    # 测试检查今天的文件
    today_file = get_today_filename()
    print(f"\n🔍 检查今天的文件: {today_file}")
    
    remote_exists = check_remote_file_exists(bp, today_file)
    if remote_exists:
        print(f"✓ 百度网盘中存在: {today_file}")
    else:
        print(f"❌ 百度网盘中不存在: {today_file}")
    
    # 测试哈希管理器
    if HashManager:
        try:
            hash_manager = HashManager()
            print(f"\n📊 哈希管理器状态:")
            hash_manager.print_status()
            
            need_update, reason = should_update_data(today_file, hash_manager)
            if need_update:
                print(f"⚠️ {today_file} 需要更新: {reason}")
            else:
                print(f"✅ {today_file} 已是最新: {reason}")
        except Exception as e:
            print(f"⚠️ 哈希管理器测试失败: {e}")
    
    return True


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='ETF日更新自动同步脚本（增强版）')
    parser.add_argument('--mode', 
                        choices=['daily', 'test', 'check-missing', 'fill-missing', 'smart-update'], 
                        default='daily',
                        help='运行模式: daily(每日更新), test(测试连接), check-missing(检查缺失), fill-missing(补齐缺失), smart-update(智能更新)')
    parser.add_argument('--days-back', type=int, default=15, 
                        help='检查最近N天的数据（默认15天）')
    
    args = parser.parse_args()
    
    if args.mode == 'test':
        success = test_connection()
        if not success:
            sys.exit(1)
    elif args.mode == 'check-missing':
        # 只检测，不处理
        detect_missing_data(args.days_back)
    elif args.mode == 'fill-missing':
        # 检测并补漏
        result = detect_missing_data(args.days_back)
        if result['missing']:
            success = batch_fill_missing_files(result['missing'])
            if not success:
                sys.exit(1)
        else:
            print("✅ 无缺失数据需要补齐")
    elif args.mode == 'smart-update':
        # 智能更新：先检测补漏，再更新今天
        success = smart_daily_update(args.days_back)
        if not success:
            sys.exit(1)
    else:  # daily
        success = daily_incremental_sync()
        if not success:
            sys.exit(1) 