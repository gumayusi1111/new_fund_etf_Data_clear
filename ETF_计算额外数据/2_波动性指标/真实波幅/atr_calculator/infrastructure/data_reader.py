#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ATR数据读取模块
==============

负责从各种数据源读取ETF数据，支持：
- CSV文件读取
- 数据清洗和预处理
- 门槛筛选逻辑
- 数据格式标准化
- 异常数据处理

支持的数据格式：
- 标准ETF CSV格式
- 前复权/后复权数据
- 多种编码格式
"""

import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
import logging
import warnings

# 抑制pandas警告
warnings.filterwarnings('ignore', category=pd.errors.DtypeWarning)


class ATRDataReader:
    """ATR数据读取器"""
    
    def __init__(self, config=None):
        """初始化数据读取器"""
        if config:
            self.etf_data_path = config.etf_data_path
            self.thresholds = config.thresholds
        else:
            self.etf_data_path = ""
            self.thresholds = ["3000万门槛", "5000万门槛"]
        
        # 设置日志
        self.logger = logging.getLogger(__name__)
        
        # 门槛筛选条件
        self.threshold_conditions = {
            "3000万门槛": {
                "min_market_value": 30,      # 3000万 = 30 * 100万
                "min_trading_days": 100,     # 最少交易天数
                "min_avg_volume": 100000,    # 最小平均成交量
            },
            "5000万门槛": {
                "min_market_value": 50,      # 5000万 = 50 * 100万
                "min_trading_days": 120,     # 最少交易天数
                "min_avg_volume": 200000,    # 最小平均成交量
            }
        }
        
        # 必需列名
        self.required_columns = ['日期', '最高价', '最低价', '收盘价']
        
        # 可选列名（用于计算优化）
        self.optional_columns = ['开盘价', '涨幅%', '换手率%', '成交量', '成交量(手数)', '成交额(万)', '成交额(千元)', '上日收盘']
    
    def _standardize_column_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """标准化列名"""
        # 常见列名映射
        column_mapping = {
            'date': '日期', 'Date': '日期', 'DATE': '日期',
            'high': '最高价', 'High': '最高价', 'HIGH': '最高价', '最高': '最高价',
            'low': '最低价', 'Low': '最低价', 'LOW': '最低价', '最低': '最低价',
            'close': '收盘价', 'Close': '收盘价', 'CLOSE': '收盘价', '收盘': '收盘价',
            'volume': '成交量', 'Volume': '成交量', 'VOLUME': '成交量',
            'open': '开盘价', 'Open': '开盘价', 'OPEN': '开盘价', '开盘': '开盘价',
            'change_pct': '涨幅%', 'Change_Pct': '涨幅%', '涨跌幅': '涨幅%',
            'turnover': '换手率%', 'Turnover': '换手率%',
            'amount': '成交额(万)', 'Amount': '成交额(万)', '成交额': '成交额(万)',
            '成交量(手数)': '成交量',  # 标准化成交量列名
            '成交额(千元)': '成交额(万)',  # 标准化成交额列名，同时转换单位
        }
        
        # 重命名列
        df = df.rename(columns=column_mapping)
        
        # 处理单位转换：千元转万元
        if '成交额(万)' in df.columns and '成交额(千元)' in df.columns:
            # 如果同时存在，优先使用万元单位
            df = df.drop(columns=['成交额(千元)'])
        elif '成交额(千元)' in column_mapping and '成交额(万)' in df.columns:
            # 转换千元到万元 (除以10)
            df['成交额(万)'] = df['成交额(万)'] / 10
        
        return df
    
    def _clean_numeric_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """清理数值型列"""
        numeric_columns = ['最高价', '最低价', '收盘价', '开盘价', '成交量', '成交额(万)', '涨幅%', '换手率%']
        
        for col in numeric_columns:
            if col in df.columns:
                # 移除非数值字符
                if df[col].dtype == 'object':
                    df[col] = df[col].astype(str).str.replace(',', '').str.replace('%', '')
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                
                # 处理异常值
                if col in ['最高价', '最低价', '收盘价', '开盘价']:
                    # 价格不能为负或零
                    df[col] = df[col].where(df[col] > 0, np.nan)
                elif col == '成交量':
                    # 成交量不能为负
                    df[col] = df[col].where(df[col] >= 0, np.nan)
        
        return df
    
    def _validate_price_logic(self, df: pd.DataFrame) -> pd.DataFrame:
        """验证价格逻辑关系"""
        # 检查最高价 >= 最低价
        invalid_hl = df['最高价'] < df['最低价']
        if invalid_hl.any():
            self.logger.warning(f"发现{invalid_hl.sum()}条最高价小于最低价的记录，已标记为无效")
            df.loc[invalid_hl, ['最高价', '最低价', '收盘价']] = np.nan
        
        # 检查收盘价在最高最低价范围内
        if '收盘价' in df.columns:
            out_of_range = (df['收盘价'] > df['最高价']) | (df['收盘价'] < df['最低价'])
            if out_of_range.any():
                self.logger.warning(f"发现{out_of_range.sum()}条收盘价超出最高最低价范围的记录")
                # 可以选择修正或标记为无效
                # 这里选择将收盘价修正到合理范围
                df.loc[df['收盘价'] > df['最高价'], '收盘价'] = df['最高价']
                df.loc[df['收盘价'] < df['最低价'], '收盘价'] = df['最低价']
        
        return df
    
    def _process_date_column(self, df: pd.DataFrame) -> pd.DataFrame:
        """处理日期列"""
        if '日期' in df.columns:
            try:
                # 检查数据类型和格式
                if df['日期'].dtype in ['int64', 'float64']:
                    # 数字格式，假设为YYYYMMDD
                    df['日期'] = pd.to_datetime(df['日期'], format='%Y%m%d')
                elif df['日期'].dtype == 'object':
                    # 字符串格式，检查是否为8位数字
                    sample_date = str(df['日期'].iloc[0]).strip()
                    if len(sample_date) == 8 and sample_date.isdigit():
                        df['日期'] = pd.to_datetime(df['日期'], format='%Y%m%d')
                    else:
                        df['日期'] = pd.to_datetime(df['日期'])
                else:
                    df['日期'] = pd.to_datetime(df['日期'])
                    
            except Exception as e:
                self.logger.warning(f"日期解析失败: {e}")
                # 尝试不同的日期格式
                for fmt in ['%Y%m%d', '%Y-%m-%d', '%Y/%m/%d', '%m/%d/%Y', '%d/%m/%Y']:
                    try:
                        df['日期'] = pd.to_datetime(df['日期'], format=fmt)
                        break
                    except:
                        continue
        
        return df
    
    def read_etf_file(self, file_path: str, etf_code: str = None) -> Optional[pd.DataFrame]:
        """读取单个ETF文件"""
        try:
            if not Path(file_path).exists():
                self.logger.error(f"文件不存在: {file_path}")
                return None
            
            # 尝试多种编码读取
            encodings = ['utf-8', 'gbk', 'gb2312', 'utf-8-sig']
            df = None
            
            for encoding in encodings:
                try:
                    df = pd.read_csv(file_path, encoding=encoding)
                    break
                except UnicodeDecodeError:
                    continue
                except Exception as e:
                    self.logger.warning(f"读取文件失败 (编码: {encoding}): {e}")
                    continue
            
            if df is None:
                self.logger.error(f"无法读取文件: {file_path}")
                return None
            
            # 数据预处理
            df = self._standardize_column_names(df)
            df = self._process_date_column(df)
            df = self._clean_numeric_columns(df)
            df = self._validate_price_logic(df)
            
            # 检查必需列
            missing_columns = [col for col in self.required_columns if col not in df.columns]
            if missing_columns:
                self.logger.error(f"缺少必需列 {file_path}: {missing_columns}")
                return None
            
            # 排序并重置索引（按日期降序，最新日期在前）
            if '日期' in df.columns:
                df = df.sort_values('日期', ascending=False).reset_index(drop=True)
            
            # 移除完全重复的行
            df = df.drop_duplicates()
            
            # 添加ETF代码列（如果提供）
            if etf_code:
                df['ETF代码'] = etf_code
            
            self.logger.debug(f"成功读取文件: {file_path}, 形状: {df.shape}")
            return df
            
        except Exception as e:
            self.logger.error(f"读取ETF文件失败 {file_path}: {e}")
            return None
    
    def check_threshold_conditions(self, df: pd.DataFrame, threshold: str) -> Tuple[bool, Dict[str, Any]]:
        """检查ETF是否满足门槛条件"""
        if threshold not in self.threshold_conditions:
            return False, {"error": f"未知门槛类型: {threshold}"}
        
        conditions = self.threshold_conditions[threshold]
        results = {
            "threshold": threshold,
            "meets_criteria": False,
            "checks": {}
        }
        
        try:
            # 检查数据量
            total_days = len(df)
            valid_days = len(df.dropna(subset=['收盘价', '成交量']))
            
            results["checks"]["total_days"] = total_days
            results["checks"]["valid_days"] = valid_days
            results["checks"]["min_trading_days_required"] = conditions["min_trading_days"]
            results["checks"]["trading_days_ok"] = valid_days >= conditions["min_trading_days"]
            
            # 检查平均成交量
            if '成交量' in df.columns:
                avg_volume = df['成交量'].mean()
                results["checks"]["avg_volume"] = avg_volume
                results["checks"]["min_volume_required"] = conditions["min_avg_volume"]
                results["checks"]["volume_ok"] = avg_volume >= conditions["min_avg_volume"]
            else:
                results["checks"]["volume_ok"] = False
            
            # 检查市值（通过成交额估算）
            market_value_ok = True  # 默认通过，因为市值计算复杂
            if '成交额(万)' in df.columns:
                avg_amount = df['成交额(万)'].mean()
                results["checks"]["avg_amount"] = avg_amount
                # 简单估算：平均成交额应该大于门槛的一定比例
                min_amount = conditions["min_market_value"] * 0.1  # 假设成交额为市值的10%
                market_value_ok = avg_amount >= min_amount
                results["checks"]["market_value_ok"] = market_value_ok
                results["checks"]["min_amount_required"] = min_amount
            else:
                results["checks"]["market_value_ok"] = True  # 无数据时默认通过
            
            # 综合判断
            all_checks = [
                results["checks"]["trading_days_ok"],
                results["checks"]["volume_ok"],
                results["checks"]["market_value_ok"]
            ]
            
            results["meets_criteria"] = all(all_checks)
            
            return results["meets_criteria"], results
            
        except Exception as e:
            results["error"] = str(e)
            return False, results
    
    def get_available_etf_files(self) -> List[str]:
        """获取所有可用的ETF文件"""
        try:
            data_path = Path(self.etf_data_path)
            if not data_path.exists():
                self.logger.error(f"数据路径不存在: {self.etf_data_path}")
                return []
            
            # 查找所有CSV文件
            csv_files = list(data_path.glob("*.csv"))
            
            # 提取ETF代码并排序
            etf_files = []
            for file in csv_files:
                if file.stem.replace('.', '').isdigit() and len(file.stem.split('.')[0]) == 6:
                    etf_files.append(str(file))
            
            etf_files.sort()
            self.logger.info(f"发现{len(etf_files)}个ETF文件")
            return etf_files
            
        except Exception as e:
            self.logger.error(f"获取ETF文件列表失败: {e}")
            return []
    
    def extract_etf_code_from_filename(self, file_path: str) -> Optional[str]:
        """从文件名提取ETF代码"""
        try:
            filename = Path(file_path).stem
            
            # 移除扩展名，提取数字部分
            if '.' in filename:
                code_part = filename.split('.')[0]
            else:
                code_part = filename
            
            # 验证是否为6位数字
            if code_part.isdigit() and len(code_part) == 6:
                # 根据代码规则添加交易所后缀
                if code_part.startswith(('50', '51', '52', '56', '58')):
                    return f"{code_part}.SH"
                elif code_part.startswith(('15', '16', '18')):
                    return f"{code_part}.SZ"
                else:
                    return f"{code_part}.SH"  # 默认上海
            
            return None
            
        except Exception as e:
            self.logger.warning(f"提取ETF代码失败 {file_path}: {e}")
            return None