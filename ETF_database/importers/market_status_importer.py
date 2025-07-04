#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ETF市场状况数据导入器
专门处理ETF市场状况JSON数据并导入到PostgreSQL数据库
"""

import json
import os
import sys
from datetime import datetime
from typing import Dict, Any, Optional
from pathlib import Path

# 添加父目录到路径以导入db_connection
sys.path.insert(0, str(Path(__file__).parent.parent))
from db_connection import ETFDatabaseManager

class MarketStatusImporter:
    """ETF市场状况数据导入器"""
    
    def __init__(self):
        """初始化导入器"""
        self.db_manager = ETFDatabaseManager()
        
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
    
    def create_tables_if_not_exists(self):
        """创建市场状况相关表（如果不存在）"""
        try:
            # 创建schema
            self.cursor.execute("CREATE SCHEMA IF NOT EXISTS market_status")
            
            # 创建ETF状况表
            create_etf_status_sql = """
            CREATE TABLE IF NOT EXISTS market_status.etf_status (
                id SERIAL PRIMARY KEY,
                etf_code VARCHAR(20) NOT NULL,
                status VARCHAR(50) NOT NULL,
                status_code VARCHAR(20) NOT NULL,
                latest_date DATE,
                days_behind INTEGER DEFAULT 0,
                analysis TEXT,
                last_check TIMESTAMP,
                check_time_info TEXT,
                report_date DATE NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(etf_code, report_date)
            )
            """
            
            # 创建报告信息表
            create_report_info_sql = """
            CREATE TABLE IF NOT EXISTS market_status.report_info (
                id SERIAL PRIMARY KEY,
                generated_time TIMESTAMP NOT NULL,
                total_etf_count INTEGER NOT NULL,
                data_source VARCHAR(100),
                latest_trading_day DATE,
                active_count INTEGER DEFAULT 0,
                normal_count INTEGER DEFAULT 0,
                suspended_count INTEGER DEFAULT 0,
                delisted_count INTEGER DEFAULT 0,
                error_count INTEGER DEFAULT 0,
                report_date DATE NOT NULL UNIQUE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
            
            self.cursor.execute(create_etf_status_sql)
            self.cursor.execute(create_report_info_sql)
            self.db_manager.connection.commit()
            return True
            
        except Exception as e:
            print(f"❌ 创建市场状况表失败: {e}")
            self.db_manager.connection.rollback()
            return False
    
    def import_json_file(self, json_file_path: str) -> bool:
        """导入JSON文件到数据库"""
        if not os.path.exists(json_file_path):
            print(f"❌ 文件不存在: {json_file_path}")
            return False
        
        print(f"📊 开始导入市场状况数据: {json_file_path}")
        
        try:
            # 连接数据库
            if not self.connect():
                return False
            
            # 确保表存在
            if not self.create_tables_if_not_exists():
                return False
            
            # 解析JSON文件
            with open(json_file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            print(f"✅ JSON解析成功，ETF总数: {data['report_info']['total_etf_count']}")
            
            # 导入报告信息
            if not self._import_report_info(data):
                return False
            
            # 导入ETF状况
            if not self._import_etf_status(data):
                return False
            
            # 提交事务
            self.db_manager.connection.commit()
            print("🎉 市场状况数据导入完成!")
            
            # 显示导入汇总
            self._show_import_summary()
            return True
            
        except Exception as e:
            print(f"❌ 导入过程失败: {e}")
            if hasattr(self, 'db_manager') and self.db_manager.connection:
                self.db_manager.connection.rollback()
            return False
        finally:
            self.disconnect()
    
    def _import_report_info(self, data: Dict[str, Any]) -> bool:
        """导入报告信息（私有方法）"""
        try:
            report_info = data['report_info']
            status_summary = data['status_summary']
            
            generated_time = datetime.strptime(
                report_info['generated_time'], '%Y-%m-%d %H:%M:%S'
            )
            latest_trading_day = datetime.strptime(
                report_info['latest_trading_day'], '%Y-%m-%d'
            ).date()
            
            insert_sql = """
            INSERT INTO market_status.report_info 
            (generated_time, total_etf_count, data_source, latest_trading_day,
             active_count, normal_count, suspended_count, delisted_count, error_count, report_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (report_date) DO UPDATE SET
                generated_time = EXCLUDED.generated_time,
                total_etf_count = EXCLUDED.total_etf_count,
                data_source = EXCLUDED.data_source,
                latest_trading_day = EXCLUDED.latest_trading_day,
                active_count = EXCLUDED.active_count,
                normal_count = EXCLUDED.normal_count,
                suspended_count = EXCLUDED.suspended_count,
                delisted_count = EXCLUDED.delisted_count,
                error_count = EXCLUDED.error_count
            """
            
            self.cursor.execute(insert_sql, (
                generated_time,
                report_info['total_etf_count'],
                report_info['data_source'],
                latest_trading_day,
                status_summary['active_count'],
                status_summary['normal_count'],
                status_summary['suspended_count'],
                status_summary['delisted_count'],
                status_summary['error_count'],
                generated_time.date()
            ))
            
            print("✅ 报告信息导入成功")
            return True
            
        except Exception as e:
            print(f"❌ 报告信息导入失败: {e}")
            return False
    
    def _import_etf_status(self, data: Dict[str, Any]) -> bool:
        """导入ETF状况数据（私有方法）"""
        try:
            etf_details = data['etf_details']
            report_date = datetime.strptime(
                data['report_info']['generated_time'], '%Y-%m-%d %H:%M:%S'
            ).date()
            
            insert_sql = """
            INSERT INTO market_status.etf_status 
            (etf_code, status, status_code, latest_date, days_behind, 
             analysis, last_check, check_time_info, report_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (etf_code, report_date) DO UPDATE SET
                status = EXCLUDED.status,
                status_code = EXCLUDED.status_code,
                latest_date = EXCLUDED.latest_date,
                days_behind = EXCLUDED.days_behind,
                analysis = EXCLUDED.analysis,
                last_check = EXCLUDED.last_check,
                check_time_info = EXCLUDED.check_time_info
            """
            
            success_count = 0
            total_count = len(etf_details)
            
            for etf_code, etf_info in etf_details.items():
                try:
                    latest_date = datetime.strptime(
                        etf_info['latest_date'], '%Y-%m-%d'
                    ).date()
                    
                    last_check = datetime.strptime(
                        etf_info['last_check'], '%Y-%m-%d %H:%M:%S'
                    )
                    
                    self.cursor.execute(insert_sql, (
                        etf_info['code'],
                        etf_info['status'],
                        etf_info['status_code'],
                        latest_date,
                        etf_info['days_behind'],
                        etf_info['analysis'],
                        last_check,
                        etf_info['check_time_info'],
                        report_date
                    ))
                    
                    success_count += 1
                    
                except Exception as e:
                    print(f"⚠️ ETF {etf_code} 导入失败: {e}")
            
            print(f"✅ ETF状况导入完成: {success_count}/{total_count}")
            return success_count > 0
            
        except Exception as e:
            print(f"❌ ETF状况导入失败: {e}")
            return False
    
    def _show_import_summary(self):
        """显示导入汇总信息（私有方法）"""
        try:
            # 报告统计
            self.cursor.execute("SELECT COUNT(*) FROM market_status.report_info")
            report_count = self.cursor.fetchone()[0]
            
            # ETF状况统计
            self.cursor.execute("SELECT COUNT(*) FROM market_status.etf_status")
            status_count = self.cursor.fetchone()[0]
            
            # 最新报告
            self.cursor.execute("""
                SELECT report_date, total_etf_count, active_count, delisted_count 
                FROM market_status.report_info 
                ORDER BY report_date DESC LIMIT 1
            """)
            latest_report = self.cursor.fetchone()
            
            print(f"\n📊 导入汇总:")
            print(f"  📈 报告数量: {report_count}")
            print(f"  📊 ETF状况记录: {status_count}")
            
            if latest_report:
                print(f"  📅 最新报告: {latest_report[0]}")
                print(f"  🎯 ETF总数: {latest_report[1]}")
                print(f"  ✅ 活跃ETF: {latest_report[2]}")
                print(f"  ⚠️ 可能退市: {latest_report[3]}")
                
        except Exception as e:
            print(f"❌ 汇总信息获取失败: {e}")


def main():
    """独立运行测试"""
    print("🚀 ETF市场状况导入器测试")
    
    # 默认JSON文件路径
    json_file = "../../ETF市场状况/etf_market_status.json"
    
    importer = MarketStatusImporter()
    success = importer.import_json_file(json_file)
    
    if success:
        print("✅ 测试成功!")
    else:
        print("❌ 测试失败!")
    
    return success


if __name__ == "__main__":
    main() 