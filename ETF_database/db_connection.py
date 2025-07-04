#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ETF数据库连接管理器
提供数据库连接、配置管理和数据库初始化功能
"""

import os
import sys

try:
    import psycopg2
    from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
except ImportError:
    print("❌ 缺少psycopg2依赖，请安装: pip install psycopg2-binary")
    sys.exit(1)


class ETFDatabaseManager:
    """ETF数据库管理器"""
    
    def __init__(self):
        """初始化数据库配置"""
        self.config = {
            'host': os.getenv('DB_HOST', 'localhost'),
            'port': int(os.getenv('DB_PORT', 5432)),
            'database': os.getenv('DB_NAME', 'etf'),
            'user': os.getenv('DB_USER', 'postgres'),
            'password': os.getenv('DB_PASSWORD', 'password')
        }
        self.connection = None
        self.cursor = None
    
    def connect(self):
        """连接到数据库"""
        try:
            self.connection = psycopg2.connect(**self.config)
            self.cursor = self.connection.cursor()
            print(f"✅ 已连接到数据库: {self.config['host']}:{self.config['port']}/{self.config['database']}")
            return True
        except psycopg2.OperationalError as e:
            print(f"❌ 数据库连接失败: {e}")
            return False
    
    def disconnect(self):
        """断开数据库连接"""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        print("🔌 数据库连接已断开")
    
    def test_connection(self):
        """测试数据库连接"""
        print("🔗 测试PostgreSQL数据库连接...")
        print(f"连接信息: {self.config['host']}:{self.config['port']}/{self.config['database']}")
        print("-" * 50)
        
        if not self.connect():
            return False
        
        try:
            # 执行简单查询测试
            self.cursor.execute("SELECT version();")
            version = self.cursor.fetchone()[0]
            print(f"📊 数据库版本: {version[:50]}...")
            
            self.cursor.execute("SELECT NOW();")
            current_time = self.cursor.fetchone()[0]
            print(f"⏰ 服务器时间: {current_time}")
            
            print("\n🎉 连接测试完成！数据库工作正常。")
            return True
            
        except Exception as e:
            print(f"❌ 测试查询失败: {e}")
            return False
        finally:
            self.disconnect()
    
    def create_database(self, db_name='etf'):
        """创建数据库"""
        # 连接到默认的postgres数据库来创建新数据库
        temp_config = self.config.copy()
        temp_config['database'] = 'postgres'
        
        try:
            connection = psycopg2.connect(**temp_config)
            connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            cursor = connection.cursor()
            
            # 检查数据库是否已存在
            cursor.execute("SELECT 1 FROM pg_catalog.pg_database WHERE datname = %s", (db_name,))
            exists = cursor.fetchone()
            
            if exists:
                print(f"📋 数据库 '{db_name}' 已存在")
            else:
                cursor.execute(f'CREATE DATABASE "{db_name}"')
                print(f"✅ 数据库 '{db_name}' 创建成功")
            
            cursor.close()
            connection.close()
            return True
            
        except Exception as e:
            print(f"❌ 创建数据库失败: {e}")
            return False


    def check_database_structure(self):
        """检查ETF数据库的现有结构"""
        if not self.connect():
            return False
        
        try:
            print("🔍 检查数据库现有结构...")
            
            # 检查所有schema
            self.cursor.execute("""
                SELECT schema_name 
                FROM information_schema.schemata 
                WHERE schema_name NOT IN ('information_schema', 'pg_catalog', 'pg_toast', 'pg_temp_1', 'pg_toast_temp_1')
                ORDER BY schema_name
            """)
            schemas = [row[0] for row in self.cursor.fetchall()]
            
            print(f"\n📊 发现 {len(schemas)} 个Schema:")
            for schema in schemas:
                print(f"  📁 {schema}")
                
                # 检查每个schema下的表
                self.cursor.execute("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = %s 
                    ORDER BY table_name
                """, (schema,))
                tables = [row[0] for row in self.cursor.fetchall()]
                
                if tables:
                    print(f"    📋 表 ({len(tables)}个):")
                    for table in tables:
                        print(f"      📄 {table}")
                        
                        # 检查表字段
                        self.cursor.execute("""
                            SELECT column_name, data_type, is_nullable, column_default
                            FROM information_schema.columns 
                            WHERE table_schema = %s AND table_name = %s 
                            ORDER BY ordinal_position
                        """, (schema, table))
                        columns = self.cursor.fetchall()
                        
                        if columns:
                            for col_name, col_type, nullable, default in columns:
                                nullable_str = "NULL" if nullable == "YES" else "NOT NULL"
                                default_str = f" DEFAULT {default}" if default else ""
                                print(f"        🔹 {col_name}: {col_type} {nullable_str}{default_str}")
                        
                        # 检查表数据量
                        self.cursor.execute(f'SELECT COUNT(*) FROM "{schema}"."{table}"')
                        row_count = self.cursor.fetchone()[0]
                        print(f"        📊 数据量: {row_count:,} 行")
                        print()
                else:
                    print(f"    📋 表: 无")
                print()
            
            return True
            
        except Exception as e:
            print(f"❌ 检查数据库结构失败: {e}")
            return False
        finally:
            self.disconnect()

    def create_schema_structure(self):
        """创建ETF数据库的目录结构（表结构）- 已废弃，请使用check_database_structure()"""
        print("⚠️  此方法已废弃，请使用 check_database_structure() 来检查现有结构")
        print("⚠️  如需创建新结构，请手动确认")
        return False
    
    def update_config(self, **kwargs):
        """更新数据库配置"""
        for key, value in kwargs.items():
            if key in self.config:
                self.config[key] = value
                print(f"✅ 配置更新: {key} = {value}")
    
    def show_config(self):
        """显示当前配置"""
        print("\n📋 当前数据库配置:")
        for key, value in self.config.items():
            if key == 'password':
                print(f"  {key}: {'*' * len(str(value))}")
            else:
                print(f"  {key}: {value}")


def main():
    """主函数"""
    print("=" * 60)
    print("🚀 ETF数据库管理器")
    print("=" * 60)
    
    print("\n📋 配置说明:")
    print("可通过以下环境变量配置连接参数:")
    print("  DB_HOST     - 数据库主机 (默认: localhost)")
    print("  DB_PORT     - 数据库端口 (默认: 5432)")
    print("  DB_NAME     - 数据库名称 (默认: etf)")
    print("  DB_USER     - 用户名 (默认: postgres)")
    print("  DB_PASSWORD - 密码 (默认: password)")
    print()
    
    # 创建数据库管理器实例
    db_manager = ETFDatabaseManager()
    db_manager.show_config()
    
    # 创建数据库
    print("\n" + "=" * 60)
    print("📊 检查ETF数据库...")
    if db_manager.create_database():
        print("✅ 数据库创建/检查完成")
    else:
        print("❌ 数据库创建失败")
        return False
    
    # 测试连接
    print("\n" + "=" * 60)
    print("🔗 测试数据库连接...")
    if db_manager.test_connection():
        print("✅ 连接测试成功")
    else:
        print("❌ 连接测试失败")
        return False
    
    # 检查数据库结构
    print("\n" + "=" * 60)
    print("🔍 检查数据库结构...")
    if db_manager.check_database_structure():
        print("✅ 数据库结构检查完成")
    else:
        print("❌ 数据库结构检查失败")
        return False
    
    print("\n" + "=" * 60)
    print("🎉 ETF数据库检查完成！")
    print("=" * 60)
    return True


if __name__ == "__main__":
    success = main()
    if not success:
        sys.exit(1)