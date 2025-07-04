#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
本地ETF按日期数据高速处理器
处理已下载的3757个历史数据文件
"""

import pandas as pd
import os
import glob
import logging
from datetime import datetime
import time
from collections import defaultdict
import json

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class LocalETFProcessor:
    def __init__(self, source_dir="ETF_按日期", batch_size=200):
        """
        初始化本地ETF处理器
        
        Args:
            source_dir: 本地数据目录
            batch_size: 批处理大小
        """
        self.source_dir = source_dir
        self.batch_size = batch_size
        
        # 输出目录
        self.output_dirs = {
            '前复权': 'ETF/merged_data/前复权',
            '后复权': 'ETF/merged_data/后复权',
            '除权': 'ETF/merged_data/除权'
        }
        
        # 创建输出目录
        for output_dir in self.output_dirs.values():
            os.makedirs(output_dir, exist_ok=True)
            
        # 进度文件
        self.progress_file = "local_processing_progress.json"
        
        # 标准列格式
        self.standard_columns = [
            '代码', '日期', '开盘价', '最高价', '最低价', '收盘价', 
            '上日收盘', '涨跌', '涨幅%', '成交量(手数)', '成交额(千元)'
        ]
        
        # ETF数据缓存
        self.etf_cache = defaultdict(lambda: defaultdict(list))
        
    def get_local_file_list(self):
        """获取本地文件列表"""
        logger.info("扫描本地文件...")
        
        pattern = os.path.join(self.source_dir, "*.csv")
        files = glob.glob(pattern)
        
        # 提取日期并排序
        date_files = []
        for file_path in files:
            filename = os.path.basename(file_path)
            if filename.endswith('.csv') and len(filename) == 12:
                date_str = filename.replace('.csv', '')
                if len(date_str) == 8 and date_str.isdigit():
                    date_files.append((date_str, file_path))
        
        date_files.sort()
        logger.info(f"发现 {len(date_files)} 个本地数据文件")
        return date_files
    
    def load_progress(self):
        """加载处理进度"""
        if os.path.exists(self.progress_file):
            try:
                with open(self.progress_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except:
                pass
        return {"processed_dates": [], "last_update": None}
    
    def save_progress(self, progress):
        """保存处理进度"""
        progress["last_update"] = datetime.now().isoformat()
        with open(self.progress_file, 'w', encoding='utf-8') as f:
            json.dump(progress, f, ensure_ascii=False, indent=2)
    
    def normalize_code(self, code):
        """标准化代码格式"""
        code = str(code).strip()
        
        # 如果已经有后缀，直接返回
        if '.SZ' in code or '.SH' in code:
            return code
        
        # 根据代码规则添加后缀
        if code.startswith(('15', '16', '17', '18')):
            return f"{code}.SZ"
        elif code.startswith(('50', '51', '52', '56', '58')):
            return f"{code}.SH"
        else:
            # 默认深交所
            return f"{code}.SZ"
    
    def process_daily_file(self, file_path, date_str):
        """处理单个日期文件"""
        try:
            # 读取数据
            df = pd.read_csv(file_path, encoding='utf-8')
            
            # 检查数据格式
            if '代码' not in df.columns:
                logger.warning(f"文件格式异常: {date_str}.csv")
                return 0
            
            # 按ETF代码分组并缓存
            etf_count = 0
            
            for _, row in df.iterrows():
                code = self.normalize_code(row['代码'])
                base_code = code.replace('.SZ', '').replace('.SH', '')
                
                # 构造数据行
                data_row = {
                    '代码': code,
                    '日期': date_str,
                    '开盘价': row.get('开盘价', ''),
                    '最高价': row.get('最高价', ''),
                    '最低价': row.get('最低价', ''),
                    '收盘价': row.get('收盘价', ''),
                    '上日收盘': row.get('上日收盘', ''),
                    '涨跌': row.get('涨跌', ''),
                    '涨幅%': row.get('涨幅%', ''),
                    '成交量(手数)': row.get('成交量(手数)', ''),
                    '成交额(千元)': row.get('成交额(千元)', '')
                }
                
                # 缓存到内存
                for fuquan_type in self.output_dirs.keys():
                    self.etf_cache[fuquan_type][base_code].append(data_row)
                
                etf_count += 1
            
            return etf_count
            
        except Exception as e:
            logger.error(f"处理文件异常: {date_str}.csv - {e}")
            return 0
    
    def flush_cache_to_files(self):
        """将缓存数据写入文件"""
        logger.info("正在将缓存数据写入文件...")
        
        total_files = 0
        
        for fuquan_type, output_dir in self.output_dirs.items():
            logger.info(f"处理{fuquan_type}数据...")
            
            for base_code, data_rows in self.etf_cache[fuquan_type].items():
                if not data_rows:
                    continue
                
                file_path = os.path.join(output_dir, f"{base_code}.csv")
                
                try:
                    # 创建DataFrame
                    df = pd.DataFrame(data_rows)
                    
                    # 按日期排序
                    df['日期'] = df['日期'].astype(str)
                    df = df.sort_values('日期')
                    df = df.reset_index(drop=True)
                    
                    # 去重（保留最后一条）
                    df = df.drop_duplicates(subset=['日期'], keep='last')
                    
                    # 只保留标准列
                    df = df[self.standard_columns]
                    
                    # 保存文件
                    df.to_csv(file_path, index=False, encoding='utf-8')
                    total_files += 1
                    
                except Exception as e:
                    logger.error(f"保存文件失败: {base_code}.csv - {e}")
        
        logger.info(f"成功保存 {total_files} 个ETF文件")
        
        # 清空缓存
        self.etf_cache.clear()
    
    def process_batch(self, file_list, start_idx, batch_size):
        """批量处理文件"""
        end_idx = min(start_idx + batch_size, len(file_list))
        batch_files = file_list[start_idx:end_idx]
        
        logger.info(f"处理批次 {start_idx+1}-{end_idx}/{len(file_list)}: {len(batch_files)} 个文件")
        
        batch_stats = {
            'processed': 0,
            'etf_count': 0,
            'failed': []
        }
        
        for i, (date_str, file_path) in enumerate(batch_files):
            try:
                # 显示进度
                if i % 50 == 0 and i > 0:
                    logger.info(f"  批次进度: {i+1}/{len(batch_files)} - {date_str}")
                
                # 处理文件
                etf_count = self.process_daily_file(file_path, date_str)
                if etf_count > 0:
                    batch_stats['processed'] += 1
                    batch_stats['etf_count'] += etf_count
                else:
                    batch_stats['failed'].append(date_str)
                    
            except Exception as e:
                logger.error(f"处理 {date_str} 异常: {e}")
                batch_stats['failed'].append(date_str)
        
        return batch_stats
    
    def process_all_local_data(self):
        """处理所有本地数据"""
        logger.info("开始处理本地ETF历史数据...")
        
        # 获取本地文件列表
        all_files = self.get_local_file_list()
        if not all_files:
            logger.error("未找到本地数据文件")
            return
        
        # 加载进度
        progress = self.load_progress()
        processed_dates = set(progress.get('processed_dates', []))
        
        # 筛选未处理的文件
        pending_files = [(date_str, file_path) for date_str, file_path in all_files 
                        if date_str not in processed_dates]
        
        logger.info(f"总文件数: {len(all_files)}")
        logger.info(f"已处理: {len(processed_dates)}")
        logger.info(f"待处理: {len(pending_files)}")
        
        if not pending_files:
            logger.info("所有文件已处理完成！")
            return
        
        # 按年份分组显示统计
        year_stats = defaultdict(int)
        for date_str, _ in pending_files:
            year = date_str[:4]
            year_stats[year] += 1
        
        logger.info("待处理文件按年份分布:")
        for year in sorted(year_stats.keys()):
            logger.info(f"  {year}年: {year_stats[year]} 个文件")
        
        # 分批处理
        total_stats = {
            'processed': 0,
            'etf_count': 0,
            'failed': []
        }
        
        start_time = datetime.now()
        
        for i in range(0, len(pending_files), self.batch_size):
            batch_stats = self.process_batch(pending_files, i, self.batch_size)
            
            # 累计统计
            for key in ['processed', 'etf_count']:
                total_stats[key] += batch_stats[key]
            total_stats['failed'].extend(batch_stats['failed'])
            
            # 更新进度
            batch_end = min(i + self.batch_size, len(pending_files))
            batch_processed = [date_str for date_str, _ in pending_files[i:batch_end]]
            progress['processed_dates'].extend(batch_processed)
            self.save_progress(progress)
            
            # 显示进度
            elapsed = datetime.now() - start_time
            remaining = len(pending_files) - batch_end
            if batch_end > 0:
                eta = elapsed * remaining / batch_end
                logger.info(f"批次完成 - 总进度: {batch_end}/{len(pending_files)} ({batch_end/len(pending_files)*100:.1f}%) ETA: {eta}")
            
            # 每处理1000个文件就写入一次
            if batch_end % 1000 == 0 or batch_end == len(pending_files):
                self.flush_cache_to_files()
        
        # 最终写入剩余缓存
        if self.etf_cache:
            self.flush_cache_to_files()
        
        # 最终统计
        elapsed = datetime.now() - start_time
        logger.info(f"处理完成！")
        logger.info(f"总耗时: {elapsed}")
        logger.info(f"处理成功: {total_stats['processed']} 个文件")
        logger.info(f"ETF数据: {total_stats['etf_count']} 条记录")
        logger.info(f"失败文件: {len(total_stats['failed'])}")
        
        if total_stats['failed']:
            logger.warning(f"失败文件: {total_stats['failed'][:10]}...")
        
        # 统计最终结果
        self.show_final_statistics()
    
    def show_final_statistics(self):
        """显示最终统计结果"""
        logger.info("=== 最终统计结果 ===")
        
        for fuquan_type, output_dir in self.output_dirs.items():
            if os.path.exists(output_dir):
                files = [f for f in os.listdir(output_dir) if f.endswith('.csv')]
                logger.info(f"{fuquan_type}: {len(files)} 个ETF文件")
                
                # 检查几个样本文件的数据量
                if files:
                    sample_file = os.path.join(output_dir, files[0])
                    try:
                        df = pd.read_csv(sample_file, encoding='utf-8')
                        logger.info(f"  样本文件 {files[0]}: {len(df)} 条历史记录")
                        logger.info(f"  时间范围: {df['日期'].min()} 到 {df['日期'].max()}")
                    except:
                        pass

def main():
    processor = LocalETFProcessor(batch_size=500)  # 本地处理可以用更大的批次
    processor.process_all_local_data()

if __name__ == "__main__":
    main() 