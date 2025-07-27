"""
OBV指标数据读取器 - 数据源接口
=============================

统一的数据读取接口，支持多种数据源
提供数据预处理、验证和格式标准化

核心功能:
- ETF数据文件读取
- 数据格式验证和转换
- 成交额门槛筛选
- 数据质量检查
- 增量数据获取

技术特点:
- 内存优化的大文件处理
- 错误恢复和容错机制
- 标准化数据输出格式
- 支持多种编码格式
"""

import os
import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any, Union
import logging
from datetime import datetime, timedelta
import warnings
import psutil
from functools import wraps

# 忽略pandas警告
warnings.filterwarnings('ignore', category=pd.errors.ParserWarning)
warnings.filterwarnings('ignore', category=pd.errors.DtypeWarning)

class OBVDataReader:
    """OBV指标数据读取器"""
    
    def __init__(self, source_dir: Path, encoding: str = 'utf-8'):
        """
        初始化数据读取器
        
        Args:
            source_dir: 数据源目录
            encoding: 文件编码格式
        """
        self.source_dir = Path(source_dir)
        self.encoding = encoding
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # 数据字段映射
        self.field_mapping = {
            '代码': 'code',
            '日期': 'date',
            '开盘价': 'open',
            '最高价': 'high', 
            '最低价': 'low',
            '收盘价': 'close',
            '上日收盘': 'prev_close',
            '涨跌': 'change',
            '涨幅%': 'change_pct',
            '成交量(手数)': 'volume',
            '成交额(千元)': 'amount'
        }
        
        # 必需字段
        self.required_fields = ['代码', '日期', '收盘价', '成交量(手数)']
        
        # 门槛配置
        self.threshold_config = {
            '3000万门槛': {'min_amount': 3000, 'unit': '万元'},
            '5000万门槛': {'min_amount': 5000, 'unit': '万元'}
        }
        
        self.logger.info(f"数据读取器初始化完成 - 源目录: {source_dir}")
    
    def read_etf_data(self, etf_code: str, 
                     start_date: Optional[str] = None,
                     end_date: Optional[str] = None) -> Optional[pd.DataFrame]:
        """
        读取单个ETF数据
        
        Args:
            etf_code: ETF代码
            start_date: 开始日期(YYYY-MM-DD)
            end_date: 结束日期(YYYY-MM-DD)
            
        Returns:
            ETF数据DataFrame或None
        """
        try:
            # 查找数据文件
            data_file = self._find_etf_file(etf_code)
            if not data_file:
                self.logger.warning(f"未找到ETF数据文件: {etf_code}")
                return None
            
            # 读取数据
            df = self._read_csv_with_fallback(data_file)
            if df is None:
                return None
            
            # 数据预处理
            df = self._preprocess_dataframe(df, etf_code)
            if df is None:
                return None
            
            # 日期过滤
            if start_date or end_date:
                df = self._filter_by_date_range(df, start_date, end_date)
            
            # 数据验证
            if not self._validate_data_quality(df, etf_code):
                return None
            
            self.logger.debug(f"成功读取ETF数据: {etf_code}, 记录数: {len(df)}")
            return df
            
        except FileNotFoundError as e:
            self.logger.error(f"ETF数据文件不存在 {etf_code}: {str(e)}")
            return None
        except pd.errors.EmptyDataError as e:
            self.logger.error(f"ETF数据文件为空 {etf_code}: {str(e)}")
            return None
        except pd.errors.ParserError as e:
            self.logger.error(f"ETF数据文件格式错误 {etf_code}: {str(e)}")
            return None
        except (KeyError, ValueError) as e:
            self.logger.error(f"ETF数据字段错误 {etf_code}: {str(e)}")
            return None
        except MemoryError as e:
            self.logger.error(f"ETF数据文件过大，内存不足 {etf_code}: {str(e)}")
            return None
        except Exception as e:
            self.logger.error(f"读取ETF数据失败(未知错误) {etf_code}: {type(e).__name__}: {str(e)}")
            return None
    
    def read_batch_etf_data(self, etf_codes: Optional[List[str]] = None,
                           threshold: Optional[str] = None,
                           start_date: Optional[str] = None,
                           end_date: Optional[str] = None,
                           max_files: Optional[int] = None) -> pd.DataFrame:
        """
        批量读取ETF数据
        
        Args:
            etf_codes: ETF代码列表，None则读取所有
            threshold: 门槛筛选，None则不筛选
            start_date: 开始日期
            end_date: 结束日期
            max_files: 最大文件数限制
            
        Returns:
            合并的DataFrame
        """
        try:
            all_data = []
            processed_count = 0
            error_count = 0
            
            # 确定要处理的ETF列表
            if etf_codes:
                target_codes = etf_codes
            else:
                target_codes = self._discover_available_etfs()
            
            if max_files:
                target_codes = target_codes[:max_files]
            
            self.logger.info(f"开始批量读取数据 - ETF数量: {len(target_codes)}")
            
            # 逐个处理ETF
            for etf_code in target_codes:
                try:
                    df = self.read_etf_data(etf_code, start_date, end_date)
                    
                    if df is not None and len(df) > 0:
                        # 门槛筛选
                        if threshold:
                            df = self._apply_threshold_filter(df, threshold)
                        
                        if len(df) > 0:
                            all_data.append(df)
                            processed_count += 1
                    
                except Exception as e:
                    self.logger.error(f"处理ETF数据失败 {etf_code}: {str(e)}")
                    error_count += 1
                    continue
            
            # 合并所有数据
            if all_data:
                combined_df = pd.concat(all_data, ignore_index=True)
                
                # 最终排序 - 按时间正序，便于OBV计算
                combined_df = combined_df.sort_values(['代码', '日期'], ascending=[True, True]).reset_index(drop=True)
                
                self.logger.info(f"批量读取完成 - 成功: {processed_count}, 失败: {error_count}, "
                              f"总记录: {len(combined_df)}")
                
                return combined_df
            else:
                self.logger.warning("批量读取未获得任何有效数据")
                return pd.DataFrame()
                
        except Exception as e:
            self.logger.error(f"批量读取数据异常: {str(e)}")
            return pd.DataFrame()
    
    def get_etf_latest_date(self, etf_code: str) -> Optional[str]:
        """
        获取ETF最新数据日期
        
        Args:
            etf_code: ETF代码
            
        Returns:
            最新日期字符串或None
        """
        try:
            df = self.read_etf_data(etf_code)
            if df is not None and len(df) > 0:
                return df['日期'].max()
            return None
            
        except Exception as e:
            self.logger.error(f"获取最新日期失败 {etf_code}: {str(e)}")
            return None
    
    def get_incremental_data(self, etf_code: str, 
                           last_date: str) -> Optional[pd.DataFrame]:
        """
        获取增量数据(指定日期之后的数据)
        
        Args:
            etf_code: ETF代码
            last_date: 最后更新日期
            
        Returns:
            增量数据DataFrame或None
        """
        try:
            # 读取全量数据
            df = self.read_etf_data(etf_code)
            if df is None:
                return None
            
            # 筛选增量数据
            df['日期'] = pd.to_datetime(df['日期'])
            last_datetime = pd.to_datetime(last_date)
            
            incremental_df = df[df['日期'] > last_datetime].copy()
            
            if len(incremental_df) > 0:
                # 转换回字符串格式
                incremental_df['日期'] = incremental_df['日期'].dt.strftime('%Y-%m-%d')
                
                self.logger.debug(f"获取增量数据: {etf_code}, 新增记录: {len(incremental_df)}")
                return incremental_df
            else:
                self.logger.debug(f"无增量数据: {etf_code}")
                return pd.DataFrame()
                
        except Exception as e:
            self.logger.error(f"获取增量数据失败 {etf_code}: {str(e)}")
            return None
    
    def check_data_availability(self, etf_codes: List[str]) -> Dict[str, Any]:
        """
        检查数据可用性
        
        Args:
            etf_codes: 要检查的ETF代码列表
            
        Returns:
            可用性检查结果
        """
        try:
            result = {
                'total_codes': len(etf_codes),
                'available_codes': [],
                'missing_codes': [],
                'available_count': 0,
                'data_info': {}
            }
            
            for etf_code in etf_codes:
                data_file = self._find_etf_file(etf_code)
                
                if data_file and data_file.exists():
                    result['available_codes'].append(etf_code)
                    
                    # 获取数据基本信息
                    try:
                        df = self._read_csv_with_fallback(data_file)
                        if df is not None:
                            result['data_info'][etf_code] = {
                                'file_size_mb': data_file.stat().st_size / (1024 * 1024),
                                'record_count': len(df),
                                'date_range': {
                                    'start': str(df['日期'].min()) if '日期' in df.columns else None,
                                    'end': str(df['日期'].max()) if '日期' in df.columns else None
                                }
                            }
                    except:
                        result['data_info'][etf_code] = {'error': '数据读取失败'}
                else:
                    result['missing_codes'].append(etf_code)
            
            result['available_count'] = len(result['available_codes'])
            result['availability_rate'] = (result['available_count'] / result['total_codes'] * 100) if result['total_codes'] > 0 else 0
            
            return result
            
        except Exception as e:
            self.logger.error(f"检查数据可用性异常: {str(e)}")
            return {'error': str(e)}
    
    def _find_etf_file(self, etf_code: str) -> Optional[Path]:
        """
        查找ETF数据文件
        
        Args:
            etf_code: ETF代码
            
        Returns:
            文件路径或None
        """
        try:
            # 尝试不同的文件名格式
            possible_names = [
                f"{etf_code}.csv",
                f"{etf_code}.CSV",
                f"{etf_code}.txt",
            ]
            
            for name in possible_names:
                file_path = self.source_dir / name
                if file_path.exists():
                    return file_path
            
            return None
            
        except Exception as e:
            self.logger.error(f"查找数据文件失败 {etf_code}: {str(e)}")
            return None
    
    def _read_csv_with_fallback(self, file_path: Path) -> Optional[pd.DataFrame]:
        """
        使用多种编码尝试读取CSV文件
        
        Args:
            file_path: 文件路径
            
        Returns:
            DataFrame或None
        """
        # 尝试的编码列表
        encodings = [self.encoding, 'gbk', 'gb2312', 'utf-8-sig', 'latin1']
        
        for encoding in encodings:
            try:
                df = pd.read_csv(file_path, encoding=encoding)
                
                # 基本验证
                if df.empty:
                    continue
                
                self.logger.debug(f"成功读取文件 {file_path.name} (编码: {encoding})")
                return df
                
            except Exception as e:
                if encoding == encodings[-1]:
                    self.logger.error(f"所有编码尝试失败 {file_path}: {str(e)}")
                continue
        
        return None
    
    def _preprocess_dataframe(self, df: pd.DataFrame, etf_code: str) -> Optional[pd.DataFrame]:
        """
        预处理DataFrame
        
        Args:
            df: 原始DataFrame
            etf_code: ETF代码
            
        Returns:
            处理后的DataFrame或None
        """
        try:
            # 检查必需字段
            missing_fields = [f for f in self.required_fields if f not in df.columns]
            if missing_fields:
                self.logger.error(f"ETF {etf_code} 缺少必需字段: {missing_fields}")
                return None
            
            # 确保代码字段正确
            if '代码' not in df.columns:
                df['代码'] = etf_code
            
            # 日期格式转换 (YYYYMMDD -> YYYY-MM-DD)
            df['日期'] = pd.to_datetime(df['日期'], format='%Y%m%d', errors='coerce')
            df = df.dropna(subset=['日期'])
            df['日期'] = df['日期'].dt.strftime('%Y-%m-%d')
            
            # 数值字段转换
            numeric_fields = ['收盘价', '成交量(手数)', '成交额(千元)']
            for field in numeric_fields:
                if field in df.columns:
                    df[field] = pd.to_numeric(df[field], errors='coerce')
            
            # 排序 - 按时间正序，便于OBV计算
            df = df.sort_values('日期', ascending=True).reset_index(drop=True)
            
            # 基本清理
            df = df.dropna(subset=self.required_fields)
            
            # 去重
            df = df.drop_duplicates(subset=['代码', '日期']).reset_index(drop=True)
            
            return df
            
        except Exception as e:
            self.logger.error(f"数据预处理失败 {etf_code}: {str(e)}")
            return None
    
    def _filter_by_date_range(self, df: pd.DataFrame, 
                             start_date: Optional[str] = None,
                             end_date: Optional[str] = None) -> pd.DataFrame:
        """
        按日期范围过滤数据
        
        Args:
            df: 数据DataFrame
            start_date: 开始日期
            end_date: 结束日期
            
        Returns:
            过滤后的DataFrame
        """
        try:
            if start_date:
                df = df[df['日期'] >= start_date]
            
            if end_date:
                df = df[df['日期'] <= end_date]
            
            return df.reset_index(drop=True)
            
        except Exception as e:
            self.logger.error(f"日期范围过滤失败: {str(e)}")
            return df
    
    def _apply_threshold_filter(self, df: pd.DataFrame, threshold: str) -> pd.DataFrame:
        """
        应用成交额门槛筛选
        
        Args:
            df: 数据DataFrame
            threshold: 门槛类型
            
        Returns:
            筛选后的DataFrame
        """
        try:
            if threshold not in self.threshold_config:
                self.logger.warning(f"不支持的门槛类型: {threshold}")
                return df
            
            min_amount = self.threshold_config[threshold]['min_amount']
            
            # 成交额筛选(千元转万元)
            if '成交额(千元)' in df.columns:
                amount_wan = df['成交额(千元)'] / 10  # 千元转万元
                filtered_df = df[amount_wan >= min_amount].copy()
                
                original_count = len(df)
                filtered_count = len(filtered_df)
                
                if original_count > 0:
                    filter_rate = (original_count - filtered_count) / original_count * 100
                    self.logger.debug(f"门槛筛选 {threshold}: {original_count} -> {filtered_count} "
                                   f"(过滤率: {filter_rate:.1f}%)")
                
                return filtered_df.reset_index(drop=True)
            else:
                self.logger.warning("缺少成交额字段，无法应用门槛筛选")
                return df
                
        except Exception as e:
            self.logger.error(f"门槛筛选失败: {str(e)}")
            return df
    
    def _validate_data_quality(self, df: pd.DataFrame, etf_code: str) -> bool:
        """
        验证数据质量
        
        Args:
            df: 数据DataFrame
            etf_code: ETF代码
            
        Returns:
            数据质量是否合格
        """
        try:
            if df.empty:
                self.logger.warning(f"ETF {etf_code} 数据为空")
                return False
            
            # 检查最少数据量
            min_records = 21  # OBV计算需要的最少记录数
            if len(df) < min_records:
                self.logger.warning(f"ETF {etf_code} 数据量不足: {len(df)} < {min_records}")
                return False
            
            # 检查关键字段的缺失率
            for field in self.required_fields:
                if field in df.columns:
                    missing_rate = df[field].isna().sum() / len(df)
                    if missing_rate > 0.05:  # 缺失率超过5%
                        self.logger.warning(f"ETF {etf_code} 字段 {field} 缺失率过高: {missing_rate:.1%}")
                        return False
            
            # 检查数值字段的合理性
            if '收盘价' in df.columns:
                if (df['收盘价'] <= 0).any():
                    self.logger.warning(f"ETF {etf_code} 存在非正数收盘价")
                    return False
            
            if '成交量(手数)' in df.columns:
                if (df['成交量(手数)'] < 0).any():
                    self.logger.warning(f"ETF {etf_code} 存在负成交量")
                    return False
            
            return True
            
        except Exception as e:
            self.logger.error(f"数据质量验证异常 {etf_code}: {str(e)}")
            return False
    
    def _discover_available_etfs(self) -> List[str]:
        """
        发现可用的ETF列表
        
        Returns:
            ETF代码列表
        """
        try:
            etf_codes = []
            
            # 扫描数据目录
            for file_path in self.source_dir.glob("*.csv"):
                etf_code = file_path.stem
                
                # 验证ETF代码格式(6位数字)
                if etf_code.isdigit() and len(etf_code) == 6:
                    etf_codes.append(etf_code)
            
            etf_codes.sort()
            self.logger.info(f"发现可用ETF: {len(etf_codes)}个")
            
            return etf_codes
            
        except Exception as e:
            self.logger.error(f"发现ETF列表失败: {str(e)}")
            return []
    
    def get_reader_statistics(self) -> Dict[str, Any]:
        """
        获取读取器统计信息
        
        Returns:
            统计信息字典
        """
        try:
            # 扫描数据目录
            all_files = list(self.source_dir.glob("*.csv"))
            valid_etfs = self._discover_available_etfs()
            
            # 计算总大小
            total_size = sum(f.stat().st_size for f in all_files)
            total_size_mb = total_size / (1024 * 1024)
            
            stats = {
                'source_directory': str(self.source_dir),
                'total_files': len(all_files),
                'valid_etf_files': len(valid_etfs),
                'total_size_mb': round(total_size_mb, 2),
                'encoding': self.encoding,
                'required_fields': self.required_fields,
                'supported_thresholds': list(self.threshold_config.keys()),
                'directory_exists': self.source_dir.exists(),
                'directory_readable': os.access(self.source_dir, os.R_OK) if self.source_dir.exists() else False
            }
            
            return stats
            
        except Exception as e:
            self.logger.error(f"获取读取器统计失败: {str(e)}")
            return {'error': str(e)}