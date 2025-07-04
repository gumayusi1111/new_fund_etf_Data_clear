#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ETF日更数据导入器
专门处理日更新数据的数据库导入功能
"""

import os
import sys
import pandas as pd
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import List, Dict, Any

# 添加父目录到路径以导入db_connection
sys.path.insert(0, str(Path(__file__).parent.parent))
from db_connection import ETFDatabaseManager

# 导入hash管理器
from config.hash_manager import HashManager

class DailyDataImporter:
    """ETF日更数据导入器"""
    
    def __init__(self):
        """初始化导入器"""
        self.db_manager = ETFDatabaseManager()
        self.schema_map = {
            '前复权': 'basic_info_daily',
            '后复权': 'basic_info_daily', 
            '除权': 'basic_info_daily'
        }
        self.table_map = {
            '前复权': 'forward_adjusted',
            '后复权': 'backward_adjusted',
            '除权': 'ex_rights'
        }
        # 初始化hash管理器
        self.hash_manager = HashManager()
        
    def connect(self):
        """连接数据库"""
        return self.db_manager.connect()
    
    def disconnect(self):
        """断开数据库连接"""
        self.db_manager.disconnect()
    
    @property 
    def cursor(self):
        """获取数据库游标"""
        return self.db_manager.cursor
    
    def import_daily_directories(self, base_dir: str) -> Dict[str, bool]:
        """导入日更新目录下的所有数据"""
        if not os.path.exists(base_dir):
            print(f"❌ 基础目录不存在: {base_dir}")
            return {}
        
        print(f"🚀 开始导入日更数据: {base_dir}")
        
        # 日更数据目录映射
        directories = {
            '前复权': '0_ETF日K(前复权)',
            '后复权': '0_ETF日K(后复权)',
            '除权': '0_ETF日K(除权)'
        }
        
        results = {}
        
        try:
            if not self.connect():
                return results
            
            # 确保表结构存在
            self._ensure_tables_exist()
            
            for adj_type, dir_name in directories.items():
                dir_path = os.path.join(base_dir, dir_name)
                if os.path.exists(dir_path):
                    print(f"\n📂 导入{adj_type}数据: {dir_name}")
                    success = self._import_directory(dir_path, adj_type)
                    results[adj_type] = success
                else:
                    print(f"⚠️ 目录不存在: {dir_name}")
                    results[adj_type] = False
            
            # 提交所有更改
            self.db_manager.connection.commit()
            print("\n🎉 日更数据导入完成!")
            
            # 显示导入汇总
            self._show_import_summary()
            
        except Exception as e:
            print(f"❌ 日更数据导入失败: {e}")
            if hasattr(self, 'db_manager') and self.db_manager.connection:
                self.db_manager.connection.rollback()
        finally:
            self.disconnect()
        
        return results
    
    def import_latest_data_only(self, base_dir: str, days_back: int = 1) -> Dict[str, bool]:
        """只导入最近几天的新数据（增量导入）"""
        print(f"🔄 执行增量导入：最近{days_back}天的数据")
        
        if not os.path.exists(base_dir):
            print(f"❌ 基础目录不存在: {base_dir}")
            return {}
        
        directories = {
            '前复权': '0_ETF日K(前复权)',
            '后复权': '0_ETF日K(后复权)', 
            '除权': '0_ETF日K(除权)'
        }
        
        results = {}
        
        try:
            if not self.connect():
                return results
            
            # 确保表结构存在
            self._ensure_tables_exist()
            
            # 获取需要更新的日期范围
            target_dates = self._get_recent_dates(days_back)
            print(f"📅 目标日期: {target_dates}")
            
            for adj_type, dir_name in directories.items():
                dir_path = os.path.join(base_dir, dir_name)
                if os.path.exists(dir_path):
                    print(f"\n📂 增量导入{adj_type}数据")
                    success = self._import_recent_data_with_hash(dir_path, adj_type, target_dates)
                    results[adj_type] = success
                else:
                    print(f"⚠️ 目录不存在: {dir_name}")
                    results[adj_type] = False
            
            # 提交更改
            self.db_manager.connection.commit()
            print("\n✅ 增量导入完成!")
            
        except Exception as e:
            print(f"❌ 增量导入失败: {e}")
            if hasattr(self, 'db_manager') and self.db_manager.connection:
                self.db_manager.connection.rollback()
        finally:
            self.disconnect()
        
        return results
    
    def _ensure_tables_exist(self):
        """确保日更数据表存在"""
        try:
            # 创建schema
            self.cursor.execute("CREATE SCHEMA IF NOT EXISTS basic_info_daily")
            
            # 创建表的SQL模板
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS basic_info_daily.{table_name} (
                id SERIAL PRIMARY KEY,
                etf_code VARCHAR(10) NOT NULL,
                trade_date DATE NOT NULL,
                open_price NUMERIC,
                high_price NUMERIC,
                low_price NUMERIC,
                close_price NUMERIC,
                volume NUMERIC,
                amount NUMERIC,
                prev_close NUMERIC,
                change_amount NUMERIC,
                change_percent NUMERIC,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(etf_code, trade_date)
            )
            """
            
            # 创建三个复权表
            for table_name in ['forward_adjusted', 'backward_adjusted', 'ex_rights']:
                self.cursor.execute(create_table_sql.format(table_name=table_name))
            
            self.db_manager.connection.commit()
            print("✅ 日更数据表结构检查完成")
            
        except Exception as e:
            print(f"❌ 创建表结构失败: {e}")
            raise e
    
    def _import_directory(self, dir_path: str, adj_type: str) -> bool:
        """导入指定目录的所有CSV文件"""
        try:
            csv_files = list(Path(dir_path).glob("*.csv"))
            print(f"📄 找到 {len(csv_files)} 个CSV文件")
            
            schema = self.schema_map[adj_type]
            table = self.table_map[adj_type]
            table_name = f"{schema}.{table}"
            
            success_count = 0
            
            for i, csv_file in enumerate(csv_files, 1):
                if i % 100 == 0:
                    print(f"  📈 进度: {i}/{len(csv_files)}")
                
                if self._import_csv_file(str(csv_file), table_name):
                    success_count += 1
                
                # 每50个文件提交一次
                if i % 50 == 0:
                    self.db_manager.connection.commit()
            
            print(f"✅ {adj_type}数据导入完成: {success_count}/{len(csv_files)}")
            return success_count > 0
            
        except Exception as e:
            print(f"❌ 导入{adj_type}数据失败: {e}")
            return False
    
    def _import_recent_data_with_hash(self, dir_path: str, adj_type: str, target_dates: List[str]) -> bool:
        """使用hash机制优化的增量导入"""
        try:
            schema = self.schema_map[adj_type]
            table = self.table_map[adj_type]
            table_name = f"{schema}.{table}"
            
            # 获取所有CSV文件
            csv_files = list(Path(dir_path).glob("*.csv"))
            print(f"📄 找到 {len(csv_files)} 个CSV文件")
            
            # 检查有变化的文件
            changed_files = []
            unchanged_count = 0
            
            for csv_file in csv_files:
                file_path = str(csv_file)
                filename = csv_file.name
                
                # 计算当前文件hash
                current_hash = self.hash_manager.calculate_file_hash(file_path)
                
                # 检查文件是否有变化
                stored_hash_key = f"daily_{adj_type}_{filename}"
                
                if stored_hash_key in self.hash_manager.hash_data:
                    stored_hash = self.hash_manager.hash_data[stored_hash_key]
                    if current_hash == stored_hash:
                        unchanged_count += 1
                        continue  # 文件没有变化，跳过
                
                # 文件有变化或是新文件，加入处理列表
                changed_files.append((csv_file, current_hash, stored_hash_key))
            
            print(f"📊 文件变化统计: {len(changed_files)} 个有变化，{unchanged_count} 个无变化")
            
            if not changed_files:
                print(f"✅ {adj_type}数据无变化，跳过导入")
                return False  # 没有变化就返回False
            
            # 处理有变化的文件
            updated_count = 0
            
            for i, (csv_file, current_hash, hash_key) in enumerate(changed_files, 1):
                if i % 10 == 0:
                    print(f"  📈 进度: {i}/{len(changed_files)}")
                
                # 读取CSV并过滤最近的数据
                if self._import_recent_records_from_csv(str(csv_file), table_name, target_dates):
                    updated_count += 1
                    # 更新hash记录
                    self.hash_manager.hash_data[hash_key] = current_hash
            
            # 保存hash文件
            if updated_count > 0:
                self.hash_manager._save_hash_file()
                print(f"✅ {adj_type}日更增量更新: {updated_count}个文件有更新")
                return True
            else:
                print(f"ℹ️ {adj_type}日更: 虽然文件有变化，但没有目标日期范围内的新数据")
                return False
                
        except Exception as e:
            print(f"❌ {adj_type}增量导入失败: {e}")
            return False
    
    def _import_csv_file(self, csv_file_path: str, table_name: str) -> bool:
        """导入单个CSV文件"""
        try:
            df = pd.read_csv(csv_file_path, encoding='utf-8')
            if df.empty:
                return True
            
            insert_sql = f"""
                INSERT INTO {table_name} 
                (etf_code, trade_date, open_price, high_price, low_price, close_price,
                 volume, amount, prev_close, change_amount, change_percent)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (etf_code, trade_date) 
                DO UPDATE SET
                    open_price = EXCLUDED.open_price,
                    high_price = EXCLUDED.high_price,
                    low_price = EXCLUDED.low_price,
                    close_price = EXCLUDED.close_price,
                    volume = EXCLUDED.volume,
                    amount = EXCLUDED.amount,
                    prev_close = EXCLUDED.prev_close,
                    change_amount = EXCLUDED.change_amount,
                    change_percent = EXCLUDED.change_percent,
                    created_at = CURRENT_TIMESTAMP
            """
            
            for _, row in df.iterrows():
                try:
                    trade_date = datetime.strptime(str(row['日期']), '%Y%m%d').date()
                    
                    self.cursor.execute(insert_sql, (
                        row['代码'],
                        trade_date,
                        float(row['开盘价']) if pd.notnull(row['开盘价']) else None,
                        float(row['最高价']) if pd.notnull(row['最高价']) else None,
                        float(row['最低价']) if pd.notnull(row['最低价']) else None,
                        float(row['收盘价']) if pd.notnull(row['收盘价']) else None,
                        float(row['成交量(手数)']) if pd.notnull(row['成交量(手数)']) else None,
                        float(row['成交额(千元)']) if pd.notnull(row['成交额(千元)']) else None,
                        float(row['上日收盘']) if pd.notnull(row['上日收盘']) else None,
                        float(row['涨跌']) if pd.notnull(row['涨跌']) else None,
                        float(row['涨幅%']) if pd.notnull(row['涨幅%']) else None
                    ))
                except Exception as e:
                    continue  # 跳过有问题的行
            
            return True
            
        except Exception as e:
            return False
    
    def _import_recent_records_from_csv(self, csv_file_path: str, table_name: str, target_dates: List[str]) -> bool:
        """从CSV文件导入最近的记录"""
        try:
            df = pd.read_csv(csv_file_path, encoding='utf-8')
            if df.empty:
                return False
            
            # 过滤最近的数据
            df['日期'] = df['日期'].astype(str)
            recent_df = df[df['日期'].isin(target_dates)]
            
            if recent_df.empty:
                return False
            
            # 使用相同的插入逻辑
            return self._import_csv_file(csv_file_path, table_name)
            
        except Exception as e:
            return False
    
    def _get_recent_dates(self, days_back: int) -> List[str]:
        """获取最近几天的日期字符串列表"""
        from datetime import datetime, timedelta
        
        dates = []
        today = datetime.now()
        
        for i in range(days_back):
            date = today - timedelta(days=i)
            dates.append(date.strftime('%Y%m%d'))
        
        return dates
    
    def _show_import_summary(self):
        """显示导入汇总信息"""
        try:
            print(f"\n📊 日更数据导入汇总:")
            
            for adj_type, table in self.table_map.items():
                table_name = f"basic_info_daily.{table}"
                
                # 获取记录数
                self.cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                count = self.cursor.fetchone()[0]
                
                # 获取最新日期
                self.cursor.execute(f"SELECT MAX(trade_date) FROM {table_name}")
                latest_date = self.cursor.fetchone()[0]
                
                print(f"  📈 {adj_type}: {count:,} 条记录，最新日期: {latest_date}")
                
        except Exception as e:
            print(f"❌ 汇总信息获取失败: {e}")

    def _import_csv_file_batch(self, csv_file_path: str, table_name: str, batch_size: int = 1000) -> bool:
        """批量导入单个CSV文件 - 高性能版本"""
        try:
            df = pd.read_csv(csv_file_path, encoding='utf-8')
            if df.empty:
                return True
            
            # 准备批量插入的数据
            batch_data = []
            
            for _, row in df.iterrows():
                try:
                    trade_date = datetime.strptime(str(row['日期']), '%Y%m%d').date()
                    
                    record = (
                        row['代码'],
                        trade_date,
                        float(row['开盘价']) if pd.notnull(row['开盘价']) else None,
                        float(row['最高价']) if pd.notnull(row['最高价']) else None,
                        float(row['最低价']) if pd.notnull(row['最低价']) else None,
                        float(row['收盘价']) if pd.notnull(row['收盘价']) else None,
                        float(row['成交量(手数)']) if pd.notnull(row['成交量(手数)']) else None,
                        float(row['成交额(千元)']) if pd.notnull(row['成交额(千元)']) else None,
                        float(row['上日收盘']) if pd.notnull(row['上日收盘']) else None,
                        float(row['涨跌']) if pd.notnull(row['涨跌']) else None,
                        float(row['涨幅%']) if pd.notnull(row['涨幅%']) else None
                    )
                    batch_data.append(record)
                    
                    # 当达到批量大小时执行插入
                    if len(batch_data) >= batch_size:
                        self._execute_batch_insert(table_name, batch_data)
                        batch_data = []
                        
                except Exception as e:
                    continue  # 跳过有问题的行
            
            # 插入剩余的数据
            if batch_data:
                self._execute_batch_insert(table_name, batch_data)
            
            return True
            
        except Exception as e:
            print(f"❌ 批量导入CSV失败: {e}")
            return False

    def _execute_batch_insert(self, table_name: str, batch_data: List[tuple]) -> None:
        """执行批量插入"""
        if not batch_data:
            return
            
        insert_sql = f"""
            INSERT INTO {table_name} 
            (etf_code, trade_date, open_price, high_price, low_price, close_price,
             volume, amount, prev_close, change_amount, change_percent)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (etf_code, trade_date) 
            DO UPDATE SET
                open_price = EXCLUDED.open_price,
                high_price = EXCLUDED.high_price,
                low_price = EXCLUDED.low_price,
                close_price = EXCLUDED.close_price,
                volume = EXCLUDED.volume,
                amount = EXCLUDED.amount,
                prev_close = EXCLUDED.prev_close,
                change_amount = EXCLUDED.change_amount,
                change_percent = EXCLUDED.change_percent,
                created_at = CURRENT_TIMESTAMP
        """
        
        try:
            # 使用executemany进行批量插入
            self.cursor.executemany(insert_sql, batch_data)
        except Exception as e:
            print(f"❌ 批量插入失败: {e}")
            raise e

    def _import_recent_data_with_hash_optimized(self, dir_path: str, adj_type: str, target_dates: List[str]) -> bool:
        """优化版本的增量导入 - 只处理有变化的文件，批量导入"""
        try:
            schema = self.schema_map[adj_type]
            table = self.table_map[adj_type]
            table_name = f"{schema}.{table}"
            
            # 获取所有CSV文件
            csv_files = list(Path(dir_path).glob("*.csv"))
            print(f"📄 扫描 {len(csv_files)} 个CSV文件...")
            
            # 检查有变化的文件（优化版本）
            changed_files = []
            unchanged_count = 0
            
            # 批量检查hash，每100个文件显示一次进度
            for i, csv_file in enumerate(csv_files):
                if i % 100 == 0 and i > 0:
                    print(f"  🔍 检查进度: {i}/{len(csv_files)}")
                
                file_path = str(csv_file)
                filename = csv_file.name
                
                # 计算当前文件hash
                current_hash = self.hash_manager.calculate_file_hash(file_path)
                
                # 检查文件是否有变化
                stored_hash_key = f"daily_{adj_type}_{filename}"
                
                if stored_hash_key in self.hash_manager.hash_data:
                    stored_hash = self.hash_manager.hash_data[stored_hash_key]
                    if current_hash == stored_hash:
                        unchanged_count += 1
                        continue  # 文件没有变化，跳过
                
                # 文件有变化或是新文件，加入处理列表
                changed_files.append((csv_file, current_hash, stored_hash_key))
            
            print(f"📊 文件变化统计: {len(changed_files)} 个有变化，{unchanged_count} 个无变化")
            
            if not changed_files:
                print(f"✅ {adj_type}数据无变化，跳过导入")
                return False  # 没有变化就返回False
            
            # 批量处理有变化的文件
            print(f"🚀 开始批量导入 {len(changed_files)} 个变化的文件...")
            updated_count = 0
            
            for i, (csv_file, current_hash, hash_key) in enumerate(changed_files, 1):
                if i % 20 == 0:  # 每20个文件显示一次进度
                    print(f"  📈 导入进度: {i}/{len(changed_files)} ({i/len(changed_files)*100:.1f}%)")
                
                # 使用批量导入
                if self._import_recent_records_from_csv_batch(str(csv_file), table_name, target_dates):
                    updated_count += 1
                    # 更新hash记录
                    self.hash_manager.hash_data[hash_key] = current_hash
            
            # 保存hash文件
            if updated_count > 0:
                self.hash_manager._save_hash_file()
                print(f"✅ {adj_type}批量导入完成: {updated_count}个文件更新")
                return True
            else:
                print(f"ℹ️ {adj_type}: 文件有变化，但没有目标日期范围内的新数据")
                return False
                
        except Exception as e:
            print(f"❌ {adj_type}优化导入失败: {e}")
            return False

    def _import_recent_records_from_csv_batch(self, csv_file_path: str, table_name: str, target_dates: List[str]) -> bool:
        """批量导入CSV文件中的最近记录"""
        try:
            df = pd.read_csv(csv_file_path, encoding='utf-8')
            if df.empty:
                return False
            
            # 过滤最近的数据
            df['日期'] = df['日期'].astype(str)
            recent_df = df[df['日期'].isin(target_dates)]
            
            if recent_df.empty:
                return False
            
            # 准备批量插入的数据
            batch_data = []
            
            for _, row in recent_df.iterrows():
                try:
                    trade_date = datetime.strptime(str(row['日期']), '%Y%m%d').date()
                    
                    record = (
                        row['代码'],
                        trade_date,
                        float(row['开盘价']) if pd.notnull(row['开盘价']) else None,
                        float(row['最高价']) if pd.notnull(row['最高价']) else None,
                        float(row['最低价']) if pd.notnull(row['最低价']) else None,
                        float(row['收盘价']) if pd.notnull(row['收盘价']) else None,
                        float(row['成交量(手数)']) if pd.notnull(row['成交量(手数)']) else None,
                        float(row['成交额(千元)']) if pd.notnull(row['成交额(千元)']) else None,
                        float(row['上日收盘']) if pd.notnull(row['上日收盘']) else None,
                        float(row['涨跌']) if pd.notnull(row['涨跌']) else None,
                        float(row['涨幅%']) if pd.notnull(row['涨幅%']) else None
                    )
                    batch_data.append(record)
                        
                except Exception as e:
                    continue  # 跳过有问题的行
            
            # 执行批量插入
            if batch_data:
                self._execute_batch_insert(table_name, batch_data)
                return True
            
            return False
            
        except Exception as e:
            print(f"❌ 批量导入最近记录失败: {e}")
            return False

    def import_latest_data_optimized(self, base_dir: str, days_back: int = 1) -> Dict[str, bool]:
        """优化版本的增量导入 - 高性能批量处理"""
        print(f"🚀 执行高性能增量导入：最近{days_back}天的数据")
        
        if not os.path.exists(base_dir):
            print(f"❌ 基础目录不存在: {base_dir}")
            return {}
        
        directories = {
            '前复权': '0_ETF日K(前复权)',
            '后复权': '0_ETF日K(后复权)', 
            '除权': '0_ETF日K(除权)'
        }
        
        results = {}
        
        try:
            if not self.connect():
                return results
            
            # 确保表结构存在
            self._ensure_tables_exist()
            
            # 获取需要更新的日期范围
            target_dates = self._get_recent_dates(days_back)
            print(f"📅 目标日期: {target_dates}")
            
            total_start_time = datetime.now()
            
            for adj_type, dir_name in directories.items():
                dir_path = os.path.join(base_dir, dir_name)
                if os.path.exists(dir_path):
                    print(f"\n📂 高性能导入{adj_type}数据...")
                    start_time = datetime.now()
                    
                    success = self._import_recent_data_with_hash_optimized(dir_path, adj_type, target_dates)
                    
                    end_time = datetime.now()
                    duration = (end_time - start_time).total_seconds()
                    print(f"⏱️ {adj_type}导入耗时: {duration:.2f}秒")
                    
                    results[adj_type] = success
                else:
                    print(f"⚠️ 目录不存在: {dir_name}")
                    results[adj_type] = False
            
            # 提交更改
            self.db_manager.connection.commit()
            
            total_end_time = datetime.now()
            total_duration = (total_end_time - total_start_time).total_seconds()
            print(f"\n✅ 高性能增量导入完成! 总耗时: {total_duration:.2f}秒")
            
        except Exception as e:
            print(f"❌ 高性能增量导入失败: {e}")
            if hasattr(self, 'db_manager') and self.db_manager.connection:
                self.db_manager.connection.rollback()
        finally:
            self.disconnect()
        
        return results


def main():
    """独立运行测试"""
    print("🚀 ETF日更数据导入器测试")
    
    # 默认目录路径
    base_dir = "../../ETF日更"
    
    importer = DailyDataImporter()
    
    # 测试增量导入（最近1天）
    results = importer.import_latest_data_optimized(base_dir, days_back=1)
    
    print(f"\n📊 导入结果: {results}")
    
    if any(results.values()):
        print("✅ 测试成功!")
    else:
        print("❌ 测试失败!")
    
    return any(results.values())


if __name__ == "__main__":
    main() 