#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ETF市场状况监控器
基于日更数据科学判断ETF的在市情况和退市情况
"""

import sys
import os
import json
import csv
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

# 添加项目根目录到Python路径
script_dir = Path(__file__).parent
project_root = script_dir.parent
sys.path.insert(0, str(project_root))

from config.logger_config import setup_system_logger

class ETFMarketMonitor:
    """ETF市场状况监控器"""
    
    def __init__(self):
        self.logger = setup_system_logger()
        self.project_root = project_root
        self.daily_dir = self.project_root / "ETF日更"
        self.status_file = Path(__file__).parent / "etf_market_status.json"
        
        # 交易日判断：简单排除周末，节假日可扩展
        self.today = datetime.now()
        
        self.logger.info("ETF市场状况监控器初始化完成")
    
    def is_trading_day(self, date: datetime) -> bool:
        """判断是否为交易日（排除周末）"""
        return date.weekday() < 5  # 0-4是周一到周五
    
    def get_latest_trading_day(self) -> datetime:
        """获取最近的交易日"""
        check_date = self.today
        while not self.is_trading_day(check_date):
            check_date -= timedelta(days=1)
            # 防止无限循环
            if (self.today - check_date).days > 10:
                break
        return check_date
    
    def get_etf_latest_date(self, csv_file: Path) -> Optional[str]:
        """获取ETF文件中的最新数据日期"""
        try:
            # 读取CSV文件
            df = pd.read_csv(csv_file, encoding='utf-8')
            if df.empty:
                return None
            
            # 找到日期列
            date_column = None
            for col in ['日期', 'date', 'Date', '交易日期']:
                if col in df.columns:
                    date_column = col
                    break
            
            if date_column is None:
                # 假设第二列是日期（第一列通常是代码）
                if len(df.columns) >= 2:
                    date_column = df.columns[1]
                else:
                    return None
            
            # 获取所有日期并找到最新的
            dates = []
            for date_str in df[date_column]:
                try:
                    # 尝试解析不同格式的日期
                    if isinstance(date_str, str):
                        date_str = date_str.strip()
                        # 尝试YYYYMMDD格式
                        if len(date_str) == 8 and date_str.isdigit():
                            parsed_date = datetime.strptime(date_str, '%Y%m%d')
                            dates.append(parsed_date)
                        else:
                            # 尝试其他格式
                            for fmt in ['%Y-%m-%d', '%Y/%m/%d', '%Y.%m.%d']:
                                try:
                                    parsed_date = datetime.strptime(date_str, fmt)
                                    dates.append(parsed_date)
                                    break
                                except ValueError:
                                    continue
                    else:
                        # 处理数值类型的日期
                        date_int = int(float(date_str))
                        if len(str(date_int)) == 8:
                            parsed_date = datetime.strptime(str(date_int), '%Y%m%d')
                            dates.append(parsed_date)
                except (ValueError, TypeError):
                    continue
            
            if dates:
                latest_date = max(dates)
                return latest_date.strftime('%Y-%m-%d')
            
            return None
            
        except Exception as e:
            self.logger.warning(f"读取ETF文件失败 {csv_file}: {e}")
            return None
    
    def determine_etf_status(self, etf_code: str, latest_date: str) -> Dict:
        """判断ETF状态（考虑18:00的数据更新时间）"""
        if not latest_date:
            return {
                'code': etf_code,
                'status': '数据异常',
                'status_code': 'data_error',
                'latest_date': None,
                'days_behind': None,
                'analysis': '无法读取数据日期'
            }
        
        try:
            latest_dt = datetime.strptime(latest_date, '%Y-%m-%d')
            
            # 判断当前时间是否已过18:00
            current_hour = self.today.hour
            is_after_1800 = current_hour >= 18
            
            # 确定期望的最新数据日期
            if is_after_1800:
                # 18:00后，应该有今天的数据
                expected_latest_date = self.today.date()
                reference_description = "18:00后应有今天数据"
            else:
                # 18:00前，最多有昨天的数据
                expected_latest_date = (self.today - timedelta(days=1)).date()
                reference_description = "18:00前最多有昨天数据"
            
            # 计算从期望日期开始落后的交易日数
            trading_days_behind = 0
            check_date = datetime.combine(expected_latest_date, datetime.min.time())
            
            # 从期望日期开始往前计算落后天数
            while check_date.date() > latest_dt.date():
                if self.is_trading_day(check_date):
                    trading_days_behind += 1
                check_date -= timedelta(days=1)
                # 防止计算过久
                if (datetime.combine(expected_latest_date, datetime.min.time()) - check_date).days > 30:
                    break
            
            # 精确的判断逻辑
            if latest_dt.date() >= expected_latest_date:
                # 有期望日期或更新的数据
                status = '活跃'
                status_code = 'active'
                analysis = f'数据正常（{reference_description}）'
            elif trading_days_behind == 1:
                # 落后1个交易日 - 可能正常（周末/节假日）
                status = '正常'
                status_code = 'normal'
                analysis = f'落后1个交易日（可能周末/节假日，{reference_description}）'
            elif trading_days_behind <= 3:
                # 落后2-3个交易日 - 可能暂停
                status = '可能暂停'
                status_code = 'suspended'
                analysis = f'连续{trading_days_behind}个交易日无数据'
            else:
                # 落后超过3个交易日 - 可能退市
                status = '可能退市'
                status_code = 'delisted'
                analysis = f'连续{trading_days_behind}个交易日无数据'
            
            return {
                'code': etf_code,
                'status': status,
                'status_code': status_code,
                'latest_date': latest_date,
                'days_behind': trading_days_behind,
                'analysis': analysis,
                'last_check': self.today.strftime('%Y-%m-%d %H:%M:%S'),
                'check_time_info': f'当前{self.today.hour:02d}:{self.today.minute:02d}，{reference_description}'
            }
            
        except Exception as e:
            self.logger.error(f"判断ETF状态失败 {etf_code}: {e}")
            return {
                'code': etf_code,
                'status': '判断失败',
                'status_code': 'error',
                'latest_date': latest_date,
                'days_behind': None,
                'analysis': f'状态判断异常: {e}'
            }
    
    def scan_all_etfs(self) -> Dict:
        """扫描所有ETF并判断状态"""
        self.logger.info("🔍 开始扫描所有ETF的市场状况...")
        
        # 只扫描前复权数据作为标准
        target_dir = self.daily_dir / "0_ETF日K(前复权)"
        if not target_dir.exists():
            self.logger.error(f"目录不存在: {target_dir}")
            return {}
        
        csv_files = list(target_dir.glob("*.csv"))
        self.logger.info(f"找到 {len(csv_files)} 个ETF文件")
        
        etf_statuses = {}
        progress_count = 0
        
        for csv_file in csv_files:
            try:
                # 提取ETF代码
                filename = csv_file.name
                etf_code = filename.replace('.csv', '')
                # 移除交易所后缀
                if '.' in etf_code:
                    etf_code = etf_code.split('.')[0]
                
                # 获取最新数据日期
                latest_date = self.get_etf_latest_date(csv_file)
                
                # 判断状态
                status_info = self.determine_etf_status(etf_code, latest_date)
                etf_statuses[etf_code] = status_info
                
                progress_count += 1
                if progress_count % 100 == 0:
                    self.logger.info(f"已处理 {progress_count}/{len(csv_files)} 个ETF...")
                
            except Exception as e:
                self.logger.error(f"处理ETF文件失败 {csv_file}: {e}")
                continue
        
        self.logger.info(f"✅ 完成扫描，共处理 {len(etf_statuses)} 个ETF")
        return etf_statuses
    
    def generate_market_status_report(self) -> bool:
        """生成市场状况报告"""
        try:
            etf_statuses = self.scan_all_etfs()
            
            if not etf_statuses:
                self.logger.error("❌ 没有获取到ETF状态数据")
                return False
            
            # 统计各状态数量
            status_stats = {}
            for etf_info in etf_statuses.values():
                status_code = etf_info['status_code']
                status_stats[status_code] = status_stats.get(status_code, 0) + 1
            
            # 生成报告
            report = {
                'report_info': {
                    'generated_time': self.today.strftime('%Y-%m-%d %H:%M:%S'),
                    'total_etf_count': len(etf_statuses),
                    'data_source': '日更数据',
                    'latest_trading_day': self.get_latest_trading_day().strftime('%Y-%m-%d')
                },
                'status_summary': {
                    'active_count': status_stats.get('active', 0),
                    'normal_count': status_stats.get('normal', 0),
                    'suspended_count': status_stats.get('suspended', 0),
                    'delisted_count': status_stats.get('delisted', 0),
                    'error_count': status_stats.get('data_error', 0) + status_stats.get('error', 0)
                },
                'etf_details': etf_statuses
            }
            
            # 保存到固定文件
            with open(self.status_file, 'w', encoding='utf-8') as f:
                json.dump(report, f, indent=2, ensure_ascii=False)
            
            # 打印摘要
            self.print_status_summary(report)
            
            self.logger.info(f"📄 ETF市场状况报告已更新: {self.status_file}")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ 生成市场状况报告失败: {e}")
            return False
    
    def print_status_summary(self, report: Dict):
        """打印状态摘要"""
        info = report['report_info']
        summary = report['status_summary']
        
        print("\n" + "=" * 60)
        print("📊 ETF市场状况报告")
        print("=" * 60)
        print(f"📅 报告时间: {info['generated_time']}")
        print(f"📊 ETF总数: {info['total_etf_count']} 个")
        print(f"📈 最新交易日: {info['latest_trading_day']}")
        
        # 显示当前时间和判断基准
        current_time = datetime.now()
        is_after_1800 = current_time.hour >= 18
        time_info = f"18:00后应有今天数据" if is_after_1800 else f"18:00前最多有昨天数据"
        print(f"⏰ 当前时间: {current_time.strftime('%H:%M')} ({time_info})")
        print()
        
        total = info['total_etf_count']
        print("📈 市场状况分布:")
        print(f"  🟢 活跃ETF:     {summary['active_count']:4d} 个 ({summary['active_count']/total*100:.1f}%)")
        print(f"  🔵 正常ETF:     {summary['normal_count']:4d} 个 ({summary['normal_count']/total*100:.1f}%)")
        print(f"  🟡 可能暂停:    {summary['suspended_count']:4d} 个 ({summary['suspended_count']/total*100:.1f}%)")
        print(f"  🔴 可能退市:    {summary['delisted_count']:4d} 个 ({summary['delisted_count']/total*100:.1f}%)")
        print(f"  ⚪ 数据异常:    {summary['error_count']:4d} 个 ({summary['error_count']/total*100:.1f}%)")
        
        # 显示可能退市的ETF
        delisted_etfs = []
        for code, info in report['etf_details'].items():
            if info['status_code'] == 'delisted':
                delisted_etfs.append((code, info['latest_date'], info['days_behind']))
        
        if delisted_etfs:
            print(f"\n🔴 可能已退市的ETF (前10个):")
            delisted_etfs.sort(key=lambda x: x[2] if x[2] else 0, reverse=True)
            for i, (code, last_date, days) in enumerate(delisted_etfs[:10]):
                print(f"  {i+1:2d}. {code:8s} - 最后数据: {last_date or '未知':10s} (落后{days or 0}个交易日)")
            
            if len(delisted_etfs) > 10:
                print(f"  ... 还有 {len(delisted_etfs) - 10} 个（详见报告文件）")
        
        print("=" * 60)

def main():
    """主函数"""
    monitor = ETFMarketMonitor()
    
    print("🚀 开始ETF市场状况监控...")
    
    success = monitor.generate_market_status_report()
    
    if success:
        print("✅ ETF市场状况监控完成！")
    else:
        print("❌ ETF市场状况监控失败！")
    
    return success

if __name__ == "__main__":
    import sys
    success = main()
    sys.exit(0 if success else 1) 