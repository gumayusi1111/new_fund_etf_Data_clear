#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ETF数据验证工具
随机抽样验证数据库中的数据与原始CSV文件是否一致
"""

import random
import pandas as pd
import psycopg2
import os
from typing import List, Dict, Any, Tuple
from decimal import Decimal
import logging

# 设置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ETFDataValidator:
    def __init__(self):
        self.connection = None
        self.cursor = None
        
        # 数据库连接参数
        self.db_config = {
            'host': 'localhost',
            'port': 5432,
            'database': 'etf',
            'user': 'postgres',
            'password': os.getenv('DB_PASSWORD', '')
        }
        
        # CSV目录路径
        self.csv_dirs = {
            'forward_adjusted': '../ETF日更/0_ETF日K(前复权)',
            'backward_adjusted': '../ETF日更/0_ETF日K(后复权)', 
            'ex_rights': '../ETF日更/0_ETF日K(除权)'
        }
        
        # 数据库表名
        self.db_tables = {
            'forward_adjusted': 'basic_info_daily.forward_adjusted',
            'backward_adjusted': 'basic_info_daily.backward_adjusted',
            'ex_rights': 'basic_info_daily.ex_rights'
        }
        
        # CSV字段到数据库字段的映射
        self.field_mapping = {
            '代码': 'etf_code',
            '日期': 'trade_date',
            '开盘价': 'open_price',
            '最高价': 'high_price', 
            '最低价': 'low_price',
            '收盘价': 'close_price',
            '上日收盘': 'prev_close',
            '涨跌': 'change_amount',
            '涨幅%': 'change_percent',
            '成交量(手数)': 'volume',
            '成交额(千元)': 'turnover'
        }
    
    def connect(self) -> bool:
        """连接数据库"""
        try:
            self.connection = psycopg2.connect(**self.db_config)
            self.cursor = self.connection.cursor()
            logger.info("✅ 已连接到数据库")
            return True
        except Exception as e:
            logger.error(f"❌ 数据库连接失败: {e}")
            return False
    
    def disconnect(self):
        """断开数据库连接"""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        logger.info("🔌 数据库连接已断开")
    
    def get_random_etfs(self, count: int = 5) -> List[str]:
        """获取随机ETF代码"""
        try:
            self.cursor.execute("""
                SELECT DISTINCT etf_code 
                FROM basic_info_daily.forward_adjusted 
                ORDER BY RANDOM() 
                LIMIT %s
            """, (count,))
            
            etfs = [row[0] for row in self.cursor.fetchall()]
            logger.info(f"📋 随机选择了 {len(etfs)} 个ETF: {', '.join(etfs)}")
            return etfs
            
        except Exception as e:
            logger.error(f"❌ 获取随机ETF失败: {e}")
            return []
    
    def get_db_sample_data(self, etf_code: str, table_type: str, sample_size: int = 50) -> List[Dict]:
        """从数据库获取样本数据"""
        try:
            table_name = self.db_tables[table_type]
            
            query = f"""
                SELECT etf_code, trade_date, open_price, high_price, low_price, 
                       close_price, prev_close, change_amount, change_percent, 
                       volume, turnover
                FROM {table_name}
                WHERE etf_code = %s
                ORDER BY RANDOM()
                LIMIT %s
            """
            
            self.cursor.execute(query, (etf_code, sample_size))
            rows = self.cursor.fetchall()
            
            columns = ['etf_code', 'trade_date', 'open_price', 'high_price', 'low_price',
                      'close_price', 'prev_close', 'change_amount', 'change_percent', 
                      'volume', 'turnover']
            
            data = []
            for row in rows:
                record = {}
                for i, col in enumerate(columns):
                    value = row[i]
                    # 转换日期格式
                    if col == 'trade_date':
                        value = value.strftime('%Y-%m-%d')
                    # 转换Decimal为float
                    elif isinstance(value, Decimal):
                        value = float(value)
                    record[col] = value
                data.append(record)
            
            logger.info(f"📊 从数据库获取 {etf_code} ({table_type}) 数据: {len(data)} 条记录")
            return data
            
        except Exception as e:
            logger.error(f"❌ 获取数据库数据失败: {e}")
            return []
    
    def get_csv_data(self, etf_code: str, table_type: str, target_dates: List[str]) -> List[Dict]:
        """从CSV文件获取对应日期的数据"""
        try:
            csv_dir = self.csv_dirs[table_type]
            csv_file = os.path.join(csv_dir, f"{etf_code}.csv")
            
            if not os.path.exists(csv_file):
                logger.warning(f"⚠️ CSV文件不存在: {csv_file}")
                return []
            
            # 读取CSV文件
            df = pd.read_csv(csv_file, encoding='utf-8-sig')
            
            # 过滤目标日期
            df['日期'] = pd.to_datetime(df['日期']).dt.strftime('%Y-%m-%d')
            filtered_df = df[df['日期'].isin(target_dates)]
            
            # 转换为字典列表
            data = []
            for _, row in filtered_df.iterrows():
                record = {}
                for csv_field, db_field in self.field_mapping.items():
                    value = row[csv_field]
                    
                    # 数据类型转换
                    if db_field == 'etf_code':
                        # 确保ETF代码格式一致
                        if '.' not in str(value):
                            # 根据代码前缀添加后缀
                            code_str = str(value)
                            if code_str.startswith('1'):
                                value = f"{code_str}.SZ"
                            elif code_str.startswith(('5', '6')):
                                value = f"{code_str}.SH"
                    elif db_field == 'trade_date':
                        value = str(value)
                    elif db_field in ['volume', 'turnover']:
                        value = int(value) if pd.notna(value) else 0
                    else:
                        value = float(value) if pd.notna(value) else 0.0
                    
                    record[db_field] = value
                data.append(record)
            
            logger.info(f"📁 从CSV获取 {etf_code} ({table_type}) 数据: {len(data)} 条记录")
            return data
            
        except Exception as e:
            logger.error(f"❌ 读取CSV数据失败: {e}")
            return []
    
    def compare_records(self, db_record: Dict, csv_record: Dict) -> Tuple[bool, List[str]]:
        """比较单条记录的一致性"""
        differences = []
        
        for field in self.field_mapping.values():
            db_value = db_record.get(field)
            csv_value = csv_record.get(field)
            
            # 数值比较（允许小的浮点数误差）
            if field in ['open_price', 'high_price', 'low_price', 'close_price', 
                        'prev_close', 'change_amount', 'change_percent']:
                if abs(float(db_value or 0) - float(csv_value or 0)) > 0.001:
                    differences.append(f"{field}: DB={db_value}, CSV={csv_value}")
            
            # 整数比较
            elif field in ['volume', 'turnover']:
                if int(db_value or 0) != int(csv_value or 0):
                    differences.append(f"{field}: DB={db_value}, CSV={csv_value}")
            
            # 字符串比较
            else:
                if str(db_value) != str(csv_value):
                    differences.append(f"{field}: DB={db_value}, CSV={csv_value}")
        
        return len(differences) == 0, differences
    
    def validate_etf_data(self, etf_code: str, table_type: str, sample_size: int = 50):
        """验证单个ETF的数据一致性"""
        logger.info(f"\n🔍 验证 {etf_code} ({table_type}) 数据一致性...")
        
        # 获取数据库样本数据
        db_data = self.get_db_sample_data(etf_code, table_type, sample_size)
        if not db_data:
            logger.warning(f"⚠️ 数据库中没有 {etf_code} 的数据")
            return False, 0, 0
        
        # 提取日期列表
        target_dates = [record['trade_date'] for record in db_data]
        
        # 获取CSV对应数据
        csv_data = self.get_csv_data(etf_code, table_type, target_dates)
        if not csv_data:
            logger.warning(f"⚠️ CSV文件中没有找到对应数据")
            return False, 0, 0
        
        # 创建CSV数据索引（按日期）
        csv_index = {record['trade_date']: record for record in csv_data}
        
        # 逐条对比
        total_compared = 0
        total_matched = 0
        
        for db_record in db_data:
            trade_date = db_record['trade_date']
            csv_record = csv_index.get(trade_date)
            
            if csv_record:
                total_compared += 1
                is_match, differences = self.compare_records(db_record, csv_record)
                
                if is_match:
                    total_matched += 1
                    logger.info(f"✅ {trade_date}: 数据一致")
                else:
                    logger.warning(f"❌ {trade_date}: 数据不一致")
                    for diff in differences[:3]:  # 只显示前3个差异
                        logger.warning(f"   - {diff}")
            else:
                logger.warning(f"⚠️ {trade_date}: CSV中未找到对应数据")
        
        match_rate = (total_matched / total_compared * 100) if total_compared > 0 else 0
        logger.info(f"📊 {etf_code} ({table_type}) 匹配率: {match_rate:.1f}% ({total_matched}/{total_compared})")
        
        return True, total_matched, total_compared
    
    def run_validation(self, etf_count: int = 5, sample_size: int = 50):
        """运行完整的数据验证"""
        logger.info("🚀 开始ETF数据验证...")
        
        if not self.connect():
            return
        
        try:
            # 获取随机ETF样本
            random_etfs = self.get_random_etfs(etf_count)
            if not random_etfs:
                logger.error("❌ 无法获取随机ETF样本")
                return
            
            # 验证每种复权类型
            table_types = ['forward_adjusted', 'backward_adjusted', 'ex_rights']
            type_names = ['前复权', '后复权', '除权']
            
            overall_stats = {
                'total_compared': 0,
                'total_matched': 0,
                'etf_results': {}
            }
            
            for table_type, type_name in zip(table_types, type_names):
                logger.info(f"\n{'='*60}")
                logger.info(f"🔍 验证 {type_name} 数据")
                logger.info(f"{'='*60}")
                
                type_matched = 0
                type_compared = 0
                
                for etf_code in random_etfs:
                    success, matched, compared = self.validate_etf_data(etf_code, table_type, sample_size)
                    
                    if success:
                        type_matched += matched
                        type_compared += compared
                        overall_stats['total_matched'] += matched
                        overall_stats['total_compared'] += compared
                        
                        if etf_code not in overall_stats['etf_results']:
                            overall_stats['etf_results'][etf_code] = {}
                        overall_stats['etf_results'][etf_code][table_type] = {
                            'matched': matched,
                            'compared': compared,
                            'rate': (matched/compared*100) if compared > 0 else 0
                        }
                
                type_rate = (type_matched / type_compared * 100) if type_compared > 0 else 0
                logger.info(f"\n📊 {type_name} 整体匹配率: {type_rate:.1f}% ({type_matched}/{type_compared})")
            
            # 总结报告
            self.print_validation_summary(overall_stats, random_etfs)
            
        finally:
            self.disconnect()
    
    def print_validation_summary(self, stats: Dict, etfs: List[str]):
        """打印验证总结报告"""
        logger.info(f"\n{'='*80}")
        logger.info("📋 数据验证总结报告")
        logger.info(f"{'='*80}")
        
        overall_rate = (stats['total_matched'] / stats['total_compared'] * 100) if stats['total_compared'] > 0 else 0
        
        logger.info(f"🎯 总体验证结果:")
        logger.info(f"   📊 总匹配率: {overall_rate:.1f}%")
        logger.info(f"   ✅ 匹配记录: {stats['total_matched']:,}")
        logger.info(f"   📈 总验证数: {stats['total_compared']:,}")
        
        logger.info(f"\n📋 各ETF详细结果:")
        for etf_code in etfs:
            if etf_code in stats['etf_results']:
                logger.info(f"\n  🏢 {etf_code}:")
                etf_data = stats['etf_results'][etf_code]
                
                for table_type in ['forward_adjusted', 'backward_adjusted', 'ex_rights']:
                    type_names = {'forward_adjusted': '前复权', 'backward_adjusted': '后复权', 'ex_rights': '除权'}
                    if table_type in etf_data:
                        result = etf_data[table_type]
                        logger.info(f"    📊 {type_names[table_type]}: {result['rate']:.1f}% ({result['matched']}/{result['compared']})")
        
        if overall_rate >= 99.0:
            logger.info(f"\n🎉 数据验证通过！数据库与CSV文件数据高度一致！")
        elif overall_rate >= 95.0:
            logger.info(f"\n✅ 数据验证基本通过，存在少量差异，建议进一步检查")
        else:
            logger.info(f"\n⚠️ 数据验证发现较多差异，建议详细检查导入过程")


def main():
    """主函数"""
    print("🔍 ETF数据验证工具")
    print("随机抽样验证数据库与CSV文件数据一致性")
    print("="*80)
    
    # 创建验证器
    validator = ETFDataValidator()
    
    # 运行验证 (5个随机ETF，每个50条记录样本)
    validator.run_validation(etf_count=5, sample_size=50)


if __name__ == "__main__":
    main() 