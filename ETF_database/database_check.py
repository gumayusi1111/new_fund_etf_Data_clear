#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ETF数据库检查工具
显示数据库连接信息、表结构、字段信息和数据样本
"""

import sys
import os
from pathlib import Path
from datetime import datetime

try:
    # 现在文件在ETF_database目录内，直接导入
    from db_connection import ETFDatabaseManager
except ImportError as e:
    print(f"❌ 无法导入数据库连接模块: {e}")
    print("请确保db_connection.py文件存在于同一目录")
    print(f"当前目录: {Path(__file__).parent}")
    sys.exit(1)

class DatabaseChecker:
    """数据库检查器"""
    
    def __init__(self):
        """初始化数据库检查器"""
        self.db_manager = ETFDatabaseManager()
        
    def show_connection_info(self):
        """显示数据库连接信息"""
        print("🔗 数据库连接信息")
        print("=" * 60)
        
        config = self.db_manager.config
        print(f"📍 服务器地址: {config['host']}")
        print(f"🔌 端口号: {config['port']}")
        print(f"🗄️  数据库名: {config['database']}")
        print(f"👤 用户名: {config['user']}")
        print(f"🔒 密码: {'*' * len(str(config['password']))}")
        print(f"📊 连接字符串: postgresql://{config['user']}:{'*' * len(str(config['password']))}@{config['host']}:{config['port']}/{config['database']}")
        print()
        
        # 测试连接
        if self.db_manager.connect():
            print("✅ 数据库连接正常")
            
            # 获取数据库版本和基本信息
            try:
                self.db_manager.cursor.execute("SELECT version();")
                version = self.db_manager.cursor.fetchone()[0]
                print(f"🐘 PostgreSQL版本: {version.split(',')[0]}")
                
                self.db_manager.cursor.execute("SELECT NOW();")
                server_time = self.db_manager.cursor.fetchone()[0]
                print(f"⏰ 服务器时间: {server_time}")
                
                self.db_manager.cursor.execute("SELECT current_database(), current_user;")
                db_name, user_name = self.db_manager.cursor.fetchone()
                print(f"📋 当前数据库: {db_name}")
                print(f"👨‍💻 当前用户: {user_name}")
                
            except Exception as e:
                print(f"⚠️ 获取数据库信息失败: {e}")
            
            self.db_manager.disconnect()
        else:
            print("❌ 数据库连接失败")
            return False
        
        return True
    
    def show_database_structure(self):
        """显示完整的数据库结构"""
        if not self.db_manager.connect():
            return False
        
        print("\n📊 数据库结构详情")
        print("=" * 60)
        
        try:
            # 获取所有schema
            self.db_manager.cursor.execute("""
                SELECT schema_name 
                FROM information_schema.schemata 
                WHERE schema_name NOT IN ('information_schema', 'pg_catalog', 'pg_toast', 'pg_temp_1')
                ORDER BY schema_name
            """)
            schemas = [row[0] for row in self.db_manager.cursor.fetchall()]
            
            print(f"📁 发现 {len(schemas)} 个Schema:")
            
            total_tables = 0
            total_records = 0
            
            for schema_idx, schema in enumerate(schemas, 1):
                print(f"\n[{schema_idx}] 📂 Schema: {schema}")
                print("-" * 50)
                
                # 获取schema下的所有表
                self.db_manager.cursor.execute("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = %s 
                    ORDER BY table_name
                """, (schema,))
                tables = [row[0] for row in self.db_manager.cursor.fetchall()]
                
                if not tables:
                    print("  📋 无表")
                    continue
                
                print(f"  📋 包含 {len(tables)} 个表:")
                total_tables += len(tables)
                
                for table_idx, table in enumerate(tables, 1):
                    print(f"\n  [{table_idx}] 📄 表名: {table}")
                    
                    # 获取表的记录数
                    try:
                        self.db_manager.cursor.execute(f'SELECT COUNT(*) FROM "{schema}"."{table}"')
                        record_count = self.db_manager.cursor.fetchone()[0]
                        total_records += record_count
                        print(f"       📊 记录数: {record_count:,} 条")
                    except Exception as e:
                        print(f"       📊 记录数: 查询失败 ({e})")
                        continue
                    
                    # 获取表结构
                    self.db_manager.cursor.execute("""
                        SELECT column_name, data_type, is_nullable, column_default, character_maximum_length
                        FROM information_schema.columns 
                        WHERE table_schema = %s AND table_name = %s 
                        ORDER BY ordinal_position
                    """, (schema, table))
                    columns = self.db_manager.cursor.fetchall()
                    
                    print(f"       🏗️  字段结构 ({len(columns)}个字段):")
                    for col_name, col_type, nullable, default, max_length in columns:
                        nullable_str = "NULL" if nullable == "YES" else "NOT NULL"
                        length_str = f"({max_length})" if max_length else ""
                        default_str = f" DEFAULT {default}" if default else ""
                        print(f"         🔹 {col_name}: {col_type}{length_str} {nullable_str}{default_str}")
                    
                    # 显示前几条数据样本
                    if record_count > 0:
                        print(f"       📝 数据样本 (前3条):")
                        try:
                            # 获取所有列名
                            column_names = [col[0] for col in columns]
                            
                            # 查询前3条记录
                            self.db_manager.cursor.execute(f'''
                                SELECT * FROM "{schema}"."{table}" 
                                ORDER BY 1 LIMIT 3
                            ''')
                            sample_records = self.db_manager.cursor.fetchall()
                            
                            for record_idx, record in enumerate(sample_records, 1):
                                print(f"         [{record_idx}] ", end="")
                                record_parts = []
                                for col_idx, (col_name, value) in enumerate(zip(column_names, record)):
                                    if col_idx < 5:  # 只显示前5个字段
                                        if isinstance(value, str) and len(value) > 20:
                                            value = value[:17] + "..."
                                        record_parts.append(f"{col_name}={value}")
                                    elif col_idx == 5:
                                        record_parts.append("...")
                                        break
                                print(" | ".join(record_parts))
                                
                        except Exception as e:
                            print(f"         数据样本获取失败: {e}")
                    
                    print()
            
            # 显示汇总信息
            print("📈 汇总统计")
            print("=" * 30)
            print(f"📁 Schema总数: {len(schemas)}")
            print(f"📋 表总数: {total_tables}")
            print(f"📊 记录总数: {total_records:,}")
            
            return True
            
        except Exception as e:
            print(f"❌ 获取数据库结构失败: {e}")
            return False
        finally:
            self.db_manager.disconnect()
    
    def show_table_detail(self, schema_name, table_name, limit=5):
        """显示指定表的详细信息"""
        if not self.db_manager.connect():
            return False
        
        print(f"\n🔍 表详细信息: {schema_name}.{table_name}")
        print("=" * 60)
        
        try:
            # 检查表是否存在
            self.db_manager.cursor.execute("""
                SELECT COUNT(*) 
                FROM information_schema.tables 
                WHERE table_schema = %s AND table_name = %s
            """, (schema_name, table_name))
            
            if self.db_manager.cursor.fetchone()[0] == 0:
                print(f"❌ 表不存在: {schema_name}.{table_name}")
                return False
            
            # 获取表基本信息
            self.db_manager.cursor.execute(f'SELECT COUNT(*) FROM "{schema_name}"."{table_name}"')
            total_count = self.db_manager.cursor.fetchone()[0]
            print(f"📊 总记录数: {total_count:,}")
            
            # 获取表结构
            self.db_manager.cursor.execute("""
                SELECT column_name, data_type, is_nullable, column_default, character_maximum_length
                FROM information_schema.columns 
                WHERE table_schema = %s AND table_name = %s 
                ORDER BY ordinal_position
            """, (schema_name, table_name))
            columns = self.db_manager.cursor.fetchall()
            
            print(f"🏗️  表结构 ({len(columns)}个字段):")
            for col_name, col_type, nullable, default, max_length in columns:
                nullable_str = "NULL" if nullable == "YES" else "NOT NULL"
                length_str = f"({max_length})" if max_length else ""
                default_str = f" DEFAULT {default}" if default else ""
                print(f"  🔹 {col_name}: {col_type}{length_str} {nullable_str}{default_str}")
            
            # 显示数据样本
            if total_count > 0:
                print(f"\n📝 数据样本 (前{limit}条):")
                column_names = [col[0] for col in columns]
                
                self.db_manager.cursor.execute(f'''
                    SELECT * FROM "{schema_name}"."{table_name}" 
                    ORDER BY 1 LIMIT %s
                ''', (limit,))
                records = self.db_manager.cursor.fetchall()
                
                # 显示表头
                print("  " + " | ".join(column_names[:8]))  # 最多显示8列
                print("  " + "-" * min(80, len(" | ".join(column_names[:8]))))
                
                # 显示数据
                for record in records:
                    record_parts = []
                    for i, value in enumerate(record[:8]):  # 最多显示8列
                        if isinstance(value, str) and len(value) > 15:
                            value = value[:12] + "..."
                        record_parts.append(str(value))
                    print("  " + " | ".join(record_parts))
            
            return True
            
        except Exception as e:
            print(f"❌ 获取表详细信息失败: {e}")
            return False
        finally:
            self.db_manager.disconnect()
    
    def run_complete_check(self):
        """运行完整的数据库检查"""
        print("🚀 ETF数据库完整检查工具")
        print("=" * 60)
        print(f"⏰ 检查时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print()
        
        # 1. 显示连接信息
        if not self.show_connection_info():
            return False
        
        # 2. 显示数据库结构
        self.show_database_structure()
        
        print("\n✅ 数据库检查完成！")
        return True


def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='ETF数据库检查工具')
    parser.add_argument('--table', nargs=2, metavar=('SCHEMA', 'TABLE'),
                        help='查看指定表的详细信息 (格式: schema table)')
    parser.add_argument('--limit', type=int, default=5,
                        help='数据样本显示条数 (默认: 5)')
    
    args = parser.parse_args()
    
    checker = DatabaseChecker()
    
    if args.table:
        # 查看指定表
        schema_name, table_name = args.table
        checker.show_connection_info()
        checker.show_table_detail(schema_name, table_name, args.limit)
    else:
        # 完整检查
        checker.run_complete_check()


if __name__ == "__main__":
    main() 