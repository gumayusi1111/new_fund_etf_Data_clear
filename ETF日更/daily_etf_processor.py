#!/usr/bin/env python3
"""
ETF 日更新数据处理脚本
基于 ETF_按日期 目录的数据，生成按代码分离的三种复权数据
1. 读取 ETF_按日期 的所有 CSV 文件
2. 使用复权因子计算三种复权价格
3. 按 ETF 代码分离数据，生成三个复权目录
4. 支持增量更新和全量重建
"""

import os
import sys
import pandas as pd
import shutil
from datetime import datetime, timedelta
from typing import Dict, List, Set, Optional
from pathlib import Path
import argparse

# 配置常量
DAILY_DATA_DIR = "./按日期_源数据"  # 按日期数据目录（默认值，已废弃）
OUTPUT_BASE_DIR = "."  # 输出基础目录
CATEGORIES = ["0_ETF日K(前复权)", "0_ETF日K(后复权)", "0_ETF日K(除权)"]

# 全局变量，用于临时目录支持
TEMP_SOURCE_DIR = None

# 字段映射：从按日期格式转换为按代码格式
DATE_FORMAT_FIELDS = ["日期", "代码", "名称", "开盘价", "最高价", "最低价", "收盘价", "上日收盘", "涨跌", "涨幅%", "成交量(手数)", "成交额(千元)", "复权因子"]
CODE_FORMAT_FIELDS = ["代码", "日期", "开盘价", "最高价", "最低价", "收盘价", "上日收盘", "涨跌", "涨幅%", "成交量(手数)", "成交额(千元)"]


def ensure_output_directories():
    """确保输出目录存在"""
    for category in CATEGORIES:
        category_dir = os.path.join(OUTPUT_BASE_DIR, category)
        os.makedirs(category_dir, exist_ok=True)
        print(f"✓ 确保目录存在: {category}")


def get_daily_csv_files(start_date: Optional[str] = None, end_date: Optional[str] = None) -> List[str]:
    """
    获取指定日期范围内的CSV文件列表
    
    Args:
        start_date: 开始日期 (YYYYMMDD)，None表示从最早开始
        end_date: 结束日期 (YYYYMMDD)，None表示到最新
    
    Returns:
        排序后的CSV文件路径列表
    """
    # 使用临时目录或默认目录
    source_dir = TEMP_SOURCE_DIR if TEMP_SOURCE_DIR else DAILY_DATA_DIR
    
    if not os.path.exists(source_dir):
        print(f"错误：数据目录不存在: {source_dir}")
        return []
    
    csv_files = []
    for filename in os.listdir(source_dir):
        if filename.endswith('.csv') and len(filename) == 12:  # YYYYMMDD.csv
            date_str = filename[:8]
            
            # 日期范围过滤
            if start_date and date_str < start_date:
                continue
            if end_date and date_str > end_date:
                continue
                
            csv_files.append(os.path.join(source_dir, filename))
    
    return sorted(csv_files)


def normalize_etf_code(etf_code: str) -> str:
    """
    标准化ETF代码格式：移除.SZ/.SH后缀，统一为6位数字格式
    
    Args:
        etf_code: 原始ETF代码（如 159001.SZ 或 159001）
    
    Returns:
        标准化后的ETF代码（如 159001）
    """
    if isinstance(etf_code, str):
        # 移除.SZ、.SH等后缀
        return etf_code.split('.')[0]
    return str(etf_code)


def calculate_adjusted_prices(price: float, factor: float) -> Dict[str, float]:
    """
    根据复权因子计算三种复权价格
    
    Args:
        price: 原始价格（除权价格）
        factor: 复权因子
    
    Returns:
        包含三种复权价格的字典
    """
    return {
        'forward': price / factor,    # 前复权 = 除权价格 / 复权因子
        'backward': price * factor,   # 后复权 = 除权价格 × 复权因子
        'no_adjust': price           # 除权 = 原始价格
    }


def process_daily_file(csv_file: str) -> Dict[str, Dict[str, List]]:
    """
    处理单个日期文件，返回按代码分组的三种复权数据
    
    Args:
        csv_file: CSV文件路径
    
    Returns:
        {
            'forward': {etf_code: [row_data], ...},
            'backward': {etf_code: [row_data], ...}, 
            'no_adjust': {etf_code: [row_data], ...}
        }
    """
    try:
        # 读取CSV文件
        df = pd.read_csv(csv_file, encoding='utf-8')
        
        if df.empty:
            print(f"⚠️ 文件为空: {os.path.basename(csv_file)}")
            return {'forward': {}, 'backward': {}, 'no_adjust': {}}
        
        # 检查并处理文件内的重复数据
        before_count = len(df)
        df = df.drop_duplicates(subset=['代码', '日期'], keep='last')
        after_count = len(df)
        
        if before_count > after_count:
            print(f"🧹 {os.path.basename(csv_file)}: 文件内去重 {before_count} → {after_count} 条记录")
        
        # 验证必要字段
        required_fields = ["日期", "代码", "开盘价", "最高价", "最低价", "收盘价", "上日收盘", "涨跌", "涨幅%", "成交量(手数)", "成交额(千元)", "复权因子"]
        missing_fields = [field for field in required_fields if field not in df.columns]
        if missing_fields:
            print(f"⚠️ 缺少必要字段 {missing_fields}: {os.path.basename(csv_file)}")
            return {'forward': {}, 'backward': {}, 'no_adjust': {}}
        
        result = {'forward': {}, 'backward': {}, 'no_adjust': {}}
        
        for _, row in df.iterrows():
            etf_code = row['代码']
            factor = float(row['复权因子'])
            
            # 需要复权调整的价格字段
            price_fields = ['开盘价', '最高价', '最低价', '收盘价', '上日收盘']
            
            # 处理三种复权类型
            for adj_type in ['forward', 'backward', 'no_adjust']:
                if etf_code not in result[adj_type]:
                    result[adj_type][etf_code] = []
                
                # 构建输出行数据
                row_data = [etf_code, row['日期']]  # 代码,日期
                
                # 添加价格字段（根据复权类型调整）
                for field in price_fields:
                    original_price = float(row[field])
                    adjusted_prices = calculate_adjusted_prices(original_price, factor)
                    row_data.append(adjusted_prices[adj_type])
                
                # 添加其他字段（不需要复权调整）
                row_data.extend([
                    row['涨跌'],
                    row['涨幅%'], 
                    row['成交量(手数)'],
                    row['成交额(千元)']
                ])
                
                result[adj_type][etf_code].append(row_data)
        
        return result
        
    except Exception as e:
        print(f"✗ 处理文件失败 {os.path.basename(csv_file)}: {e}")
        return {'forward': {}, 'backward': {}, 'no_adjust': {}}


def merge_and_save_etf_data(all_data: Dict[str, Dict[str, List]], mode: str = 'incremental'):
    """
    合并并保存ETF数据到对应的文件
    
    Args:
        all_data: 所有处理后的数据
        mode: 'incremental' 增量更新, 'rebuild' 全量重建
    """
    category_map = {
        'forward': "0_ETF日K(前复权)",
        'backward': "0_ETF日K(后复权)", 
        'no_adjust': "0_ETF日K(除权)"
    }
    
    for adj_type, category in category_map.items():
        category_dir = os.path.join(OUTPUT_BASE_DIR, category)
        
        for etf_code, rows in all_data[adj_type].items():
            if not rows:
                continue
            
            # 标准化ETF代码用于文件名（移除.SZ/.SH后缀）
            normalized_code = normalize_etf_code(etf_code)
            etf_file = os.path.join(category_dir, f"{normalized_code}.csv")
            
            # 创建DataFrame
            new_df = pd.DataFrame(rows, columns=CODE_FORMAT_FIELDS)
            
            # 对新数据进行去重（防止同一天下载的数据有重复）
            before_count = len(new_df)
            new_df = new_df.drop_duplicates(subset=['代码', '日期'], keep='last')
            after_count = len(new_df)
            
            if before_count > after_count:
                print(f"🧹 {normalized_code}: 新数据去重 {before_count} → {after_count} 条记录")
            
            if mode == 'incremental' and os.path.exists(etf_file):
                # 增量模式：读取现有数据并合并
                try:
                    existing_df = pd.read_csv(etf_file, encoding='utf-8', dtype=str)
                    
                    # 确保数据类型一致
                    new_df = new_df.astype(str)
                    existing_df = existing_df.astype(str)
                    
                    # 合并数据并去重（按代码+日期组合去重，保留最新数据）
                    combined_df = pd.concat([existing_df, new_df], ignore_index=True)
                    
                    # 按代码和日期去重，保留最后一个（最新的数据覆盖旧的）
                    combined_df = combined_df.drop_duplicates(subset=['代码', '日期'], keep='last')
                    
                    # 按日期排序（降序，最新日期在前）
                    combined_df['日期'] = combined_df['日期'].astype(str)
                    combined_df = combined_df.sort_values('日期', ascending=False)
                    
                    print(f"🔄 {normalized_code}: 合并后 {len(combined_df)} 条记录（去重完成）")
                    
                except Exception as e:
                    print(f"⚠️ 读取现有文件失败 {normalized_code}.csv: {e}，使用新数据")
                    combined_df = new_df
            else:
                # 重建模式或文件不存在：直接使用新数据
                combined_df = new_df
                
                # 即使是重建模式，也要检查并去除重复数据
                before_count = len(combined_df)
                combined_df = combined_df.drop_duplicates(subset=['代码', '日期'], keep='last')
                after_count = len(combined_df)
                
                if before_count > after_count:
                    print(f"🧹 {normalized_code}: 重建模式去重 {before_count} → {after_count} 条记录")
                
                # 按日期排序（降序，最新日期在前）
                combined_df['日期'] = combined_df['日期'].astype(str)
                combined_df = combined_df.sort_values('日期', ascending=False)
            
            # 保存文件
            combined_df.to_csv(etf_file, index=False, encoding='utf-8-sig')
            
        print(f"✓ 完成 {category}: {len(all_data[adj_type])} 个ETF")


def get_latest_dates(n_days: int = 5) -> List[str]:
    """获取最近N天的日期列表（YYYYMMDD格式）"""
    dates = []
    current_date = datetime.now()
    
    for i in range(n_days):
        date_str = (current_date - timedelta(days=i)).strftime('%Y%m%d')
        dates.append(date_str)
    
    return dates


def main():
    global TEMP_SOURCE_DIR
    
    parser = argparse.ArgumentParser(description='ETF日更新数据处理脚本')
    parser.add_argument('--mode', choices=['daily', 'rebuild', 'range'], default='daily',
                        help='运行模式: daily(日更新), rebuild(全量重建), range(指定范围)')
    parser.add_argument('--start-date', type=str, help='开始日期 (YYYYMMDD)')
    parser.add_argument('--end-date', type=str, help='结束日期 (YYYYMMDD)')
    parser.add_argument('--days', type=int, default=5, help='日更新模式下处理最近几天的数据')
    parser.add_argument('--temp-source-dir', type=str, help='临时源数据目录（用于临时处理）')
    
    args = parser.parse_args()
    
    # 如果提供了临时目录，使用它
    if args.temp_source_dir:
        TEMP_SOURCE_DIR = args.temp_source_dir
        print(f"🔄 使用临时源目录: {TEMP_SOURCE_DIR}")
    
    print(f"🚀 ETF日更新数据处理开始 - 模式: {args.mode}")
    source_dir = TEMP_SOURCE_DIR if TEMP_SOURCE_DIR else DAILY_DATA_DIR
    print(f"📁 源数据目录: {source_dir}")
    print(f"📁 输出目录: {OUTPUT_BASE_DIR}")
    if TEMP_SOURCE_DIR:
        print(f"🔄 临时处理模式: 处理完成后将自动清理临时文件")
    print()
    
    # 确保输出目录存在
    ensure_output_directories()
    
    # 根据模式获取文件列表
    if args.mode == 'daily':
        # 日更新：处理最近N天的数据
        recent_dates = get_latest_dates(args.days)
        csv_files = []
        for date_str in recent_dates:
            file_path = os.path.join(source_dir, f"{date_str}.csv")
            if os.path.exists(file_path):
                csv_files.append(file_path)
        csv_files.sort()
        mode = 'incremental'
        print(f"📅 日更新模式: 处理最近 {args.days} 天的数据")
        
    elif args.mode == 'rebuild':
        # 全量重建：处理所有数据
        csv_files = get_daily_csv_files()
        mode = 'rebuild'
        print(f"🔄 全量重建模式: 处理所有历史数据")
        
    elif args.mode == 'range':
        # 指定范围：处理指定日期范围的数据
        if not args.start_date:
            print("错误：范围模式需要指定 --start-date")
            return
        csv_files = get_daily_csv_files(args.start_date, args.end_date)
        mode = 'incremental'
        print(f"📊 范围模式: {args.start_date} 到 {args.end_date or '最新'}")
    
    if not csv_files:
        print("⚠️ 未找到需要处理的CSV文件")
        return
    
    print(f"📋 找到 {len(csv_files)} 个文件需要处理")
    print()
    
    # 处理所有文件
    all_data = {'forward': {}, 'backward': {}, 'no_adjust': {}}
    
    for i, csv_file in enumerate(csv_files, 1):
        filename = os.path.basename(csv_file)
        print(f"[{i}/{len(csv_files)}] 处理 {filename}...")
        
        daily_data = process_daily_file(csv_file)
        
        # 合并数据
        for adj_type in all_data:
            for etf_code, rows in daily_data[adj_type].items():
                if etf_code not in all_data[adj_type]:
                    all_data[adj_type][etf_code] = []
                all_data[adj_type][etf_code].extend(rows)
    
    print()
    print("💾 保存数据到文件...")
    
    # 保存数据
    merge_and_save_etf_data(all_data, mode)
    
    # 统计结果
    total_etfs = len(set().union(*[data.keys() for data in all_data.values()]))
    total_records = sum(len(rows) for data in all_data.values() for rows in data.values())
    
    print()
    print("🎉 处理完成!")
    print(f"📊 统计结果:")
    print(f"   - 处理文件数: {len(csv_files)}")
    print(f"   - ETF数量: {total_etfs}")
    print(f"   - 总记录数: {total_records}")
    print(f"   - 生成目录: {', '.join(CATEGORIES)}")
    print()
    print("💡 使用说明:")
    print("   - 日更新: python daily_etf_processor.py --mode daily")
    print("   - 全量重建: python daily_etf_processor.py --mode rebuild")
    print("   - 指定范围: python daily_etf_processor.py --mode range --start-date 20250601 --end-date 20250630")


if __name__ == "__main__":
    main() 