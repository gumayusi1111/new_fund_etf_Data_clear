"""
OBV指标CSV输出处理器 - 数据输出管理
===================================

管理OBV指标计算结果的CSV文件输出
支持标准格式输出、增量更新和文件管理

核心功能:
- 标准CSV格式输出
- 增量数据追加
- 文件完整性验证
- 精度控制和格式化
- 批量文件操作

技术特点:
- 8位小数精度输出
- 原子性文件操作
- 编码格式统一(UTF-8)
- 文件大小优化
"""

import os
import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
import logging
from datetime import datetime
import csv
import tempfile
import shutil

class OBVCSVHandler:
    """OBV指标CSV输出处理器"""
    
    def __init__(self, output_dir: Path, precision: int = 8):
        """
        初始化CSV处理器
        
        Args:
            output_dir: 输出目录
            precision: 数值精度(小数位数)
        """
        self.output_dir = Path(output_dir)
        self.precision = precision
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # 确保输出目录存在
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # CSV输出配置
        self.csv_config = {
            'encoding': 'utf-8',
            'index': False,
            'float_format': f'%.{precision}f',
            'date_format': '%Y-%m-%d'
        }
        
        # 标准字段顺序
        self.standard_fields = [
            'code',           # ETF代码
            'date',           # 计算日期
            'obv',            # OBV指标值
            'obv_ma10',       # OBV 10日移动平均
            'obv_change_5',   # OBV 5日变化率(%)
            'obv_change_20',  # OBV 20日变化率(%)
            'calc_time'       # 计算时间戳
        ]
        
        self.logger.info(f"CSV处理器初始化完成 - 输出目录: {output_dir}, 精度: {precision}位")
    
    def save_etf_data(self, etf_code: str, data: pd.DataFrame) -> Optional[Path]:
        """
        保存单个ETF的OBV数据
        
        Args:
            etf_code: ETF代码
            data: OBV计算结果数据
            
        Returns:
            保存的文件路径或None
        """
        try:
            if data.empty:
                self.logger.warning(f"尝试保存空数据: {etf_code}")
                return None
            
            # 验证数据格式
            if not self._validate_data_format(data, etf_code):
                return None
            
            # 准备输出数据
            output_data = self._prepare_output_data(data)
            
            # 确定输出文件路径
            output_path = self.output_dir / f"{etf_code}.csv"
            
            # 原子性保存
            if self._save_csv_atomic(output_path, output_data):
                self.logger.debug(f"保存ETF数据成功: {etf_code}, 记录数: {len(data)}")
                return output_path
            else:
                return None
                
        except Exception as e:
            self.logger.error(f"保存ETF数据失败 {etf_code}: {str(e)}")
            return None
    
    def update_etf_data_incremental(self, etf_code: str, 
                                   new_data: pd.DataFrame) -> Optional[Path]:
        """
        增量更新ETF数据
        
        Args:
            etf_code: ETF代码
            new_data: 新增数据
            
        Returns:
            更新后的文件路径或None
        """
        try:
            if new_data.empty:
                self.logger.debug(f"没有新数据需要更新: {etf_code}")
                return self.output_dir / f"{etf_code}.csv"
            
            output_path = self.output_dir / f"{etf_code}.csv"
            
            # 如果文件不存在，直接保存新数据
            if not output_path.exists():
                return self.save_etf_data(etf_code, new_data)
            
            # 读取现有数据
            try:
                existing_data = pd.read_csv(output_path, encoding='utf-8')
            except Exception as e:
                self.logger.warning(f"读取现有文件失败 {etf_code}: {str(e)}, 将覆盖保存")
                return self.save_etf_data(etf_code, new_data)
            
            # 合并数据
            if 'date' in existing_data.columns and 'date' in new_data.columns:
                # 过滤重复日期
                max_existing_date = existing_data['date'].max()
                filtered_new_data = new_data[new_data['date'] > max_existing_date].copy()
                
                if filtered_new_data.empty:
                    self.logger.debug(f"新数据无有效增量: {etf_code}")
                    return output_path
                
                # 合并数据
                combined_data = pd.concat([existing_data, filtered_new_data], 
                                        ignore_index=True)
            else:
                # 无法判断重复，直接追加
                combined_data = pd.concat([existing_data, new_data], 
                                        ignore_index=True)
            
            # 按日期排序
            if 'date' in combined_data.columns:
                combined_data = combined_data.sort_values('date').reset_index(drop=True)
            
            # 保存合并后的数据
            output_data = self._prepare_output_data(combined_data)
            
            if self._save_csv_atomic(output_path, output_data):
                self.logger.debug(f"增量更新成功: {etf_code}, 新增记录: {len(new_data)}")
                return output_path
            else:
                return None
                
        except Exception as e:
            self.logger.error(f"增量更新失败 {etf_code}: {str(e)}")
            return None
    
    def save_batch_data(self, batch_data: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
        """
        批量保存多个ETF数据
        
        Args:
            batch_data: ETF代码到数据的映射字典
            
        Returns:
            批量保存结果
        """
        try:
            start_time = datetime.now()
            results = {
                'success': True,
                'saved_files': [],
                'failed_files': [],
                'total_count': len(batch_data),
                'success_count': 0,
                'error_count': 0
            }
            
            for etf_code, data in batch_data.items():
                try:
                    output_path = self.save_etf_data(etf_code, data)
                    
                    if output_path:
                        results['saved_files'].append({
                            'etf_code': etf_code,
                            'file_path': str(output_path),
                            'record_count': len(data)
                        })
                        results['success_count'] += 1
                    else:
                        results['failed_files'].append({
                            'etf_code': etf_code,
                            'error': '保存失败'
                        })
                        results['error_count'] += 1
                        
                except Exception as e:
                    results['failed_files'].append({
                        'etf_code': etf_code,
                        'error': str(e)
                    })
                    results['error_count'] += 1
            
            # 计算统计信息
            processing_time = (datetime.now() - start_time).total_seconds()
            results['processing_time'] = processing_time
            results['success_rate'] = (results['success_count'] / results['total_count'] * 100) if results['total_count'] > 0 else 0
            
            if results['error_count'] > 0:
                results['success'] = False
            
            self.logger.info(f"批量保存完成 - 成功: {results['success_count']}, "
                          f"失败: {results['error_count']}, 耗时: {processing_time:.2f}秒")
            
            return results
            
        except Exception as e:
            self.logger.error(f"批量保存异常: {str(e)}")
            return {'success': False, 'error': str(e)}
    
    def read_etf_data(self, etf_code: str) -> Optional[pd.DataFrame]:
        """
        读取ETF的OBV数据
        
        Args:
            etf_code: ETF代码
            
        Returns:
            OBV数据DataFrame或None
        """
        try:
            file_path = self.output_dir / f"{etf_code}.csv"
            
            if not file_path.exists():
                self.logger.debug(f"文件不存在: {etf_code}")
                return None
            
            # 读取CSV文件
            df = pd.read_csv(file_path, encoding='utf-8')
            
            # 验证数据格式
            if not self._validate_data_format(df, etf_code):
                return None
            
            return df
            
        except Exception as e:
            self.logger.error(f"读取ETF数据失败 {etf_code}: {str(e)}")
            return None
    
    def get_file_info(self, etf_code: str) -> Dict[str, Any]:
        """
        获取文件信息
        
        Args:
            etf_code: ETF代码
            
        Returns:
            文件信息字典
        """
        try:
            file_path = self.output_dir / f"{etf_code}.csv"
            
            if not file_path.exists():
                return {'exists': False}
            
            # 获取文件统计信息
            stat = file_path.stat()
            
            # 读取数据获取记录信息
            try:
                df = pd.read_csv(file_path, encoding='utf-8')
                record_count = len(df)
                date_range = {
                    'start': df['date'].min() if 'date' in df.columns and not df.empty else None,
                    'end': df['date'].max() if 'date' in df.columns and not df.empty else None
                }
            except:
                record_count = 0
                date_range = {'start': None, 'end': None}
            
            return {
                'exists': True,
                'file_path': str(file_path),
                'file_size_bytes': stat.st_size,
                'file_size_kb': round(stat.st_size / 1024, 2),
                'modified_time': datetime.fromtimestamp(stat.st_mtime).isoformat(),
                'record_count': record_count,
                'date_range': date_range
            }
            
        except Exception as e:
            self.logger.error(f"获取文件信息失败 {etf_code}: {str(e)}")
            return {'exists': False, 'error': str(e)}
    
    def list_output_files(self) -> List[Dict[str, Any]]:
        """
        列出所有输出文件
        
        Returns:
            文件信息列表
        """
        try:
            files_info = []
            
            # 扫描输出目录
            for csv_file in self.output_dir.glob("*.csv"):
                etf_code = csv_file.stem
                
                # 获取文件信息
                file_info = self.get_file_info(etf_code)
                if file_info.get('exists', False):
                    file_info['etf_code'] = etf_code
                    files_info.append(file_info)
            
            # 按ETF代码排序
            files_info.sort(key=lambda x: x.get('etf_code', ''))
            
            return files_info
            
        except Exception as e:
            self.logger.error(f"列出输出文件失败: {str(e)}")
            return []
    
    def cleanup_files(self, etf_codes: Optional[List[str]] = None, 
                     older_than_days: Optional[int] = None) -> Dict[str, Any]:
        """
        清理输出文件
        
        Args:
            etf_codes: 要清理的ETF代码列表，None则不按代码过滤
            older_than_days: 清理N天前的文件，None则不按时间过滤
            
        Returns:
            清理结果
        """
        try:
            cleanup_stats = {
                'files_removed': 0,
                'space_freed_bytes': 0,
                'errors': []
            }
            
            cutoff_time = None
            if older_than_days:
                cutoff_time = datetime.now() - timedelta(days=older_than_days)
            
            # 扫描文件
            for csv_file in self.output_dir.glob("*.csv"):
                etf_code = csv_file.stem
                
                # 按ETF代码过滤
                if etf_codes and etf_code not in etf_codes:
                    continue
                
                # 按时间过滤
                if cutoff_time:
                    file_mtime = datetime.fromtimestamp(csv_file.stat().st_mtime)
                    if file_mtime >= cutoff_time:
                        continue
                
                # 删除文件
                try:
                    file_size = csv_file.stat().st_size
                    csv_file.unlink()
                    
                    cleanup_stats['files_removed'] += 1
                    cleanup_stats['space_freed_bytes'] += file_size
                    
                    self.logger.debug(f"删除文件: {csv_file.name}")
                    
                except Exception as e:
                    cleanup_stats['errors'].append({
                        'file': str(csv_file),
                        'error': str(e)
                    })
            
            cleanup_stats['space_freed_mb'] = cleanup_stats['space_freed_bytes'] / (1024 * 1024)
            
            self.logger.info(f"文件清理完成 - 删除: {cleanup_stats['files_removed']}个, "
                          f"释放: {cleanup_stats['space_freed_mb']:.1f}MB")
            
            return cleanup_stats
            
        except Exception as e:
            self.logger.error(f"文件清理异常: {str(e)}")
            return {'error': str(e)}
    
    def _validate_data_format(self, data: pd.DataFrame, etf_code: str) -> bool:
        """
        验证数据格式
        
        Args:
            data: 要验证的数据
            etf_code: ETF代码
            
        Returns:
            格式是否正确
        """
        try:
            if data.empty:
                self.logger.warning(f"数据为空: {etf_code}")
                return False
            
            # 检查必需字段
            required_fields = ['code', 'date', 'obv']
            missing_fields = [f for f in required_fields if f not in data.columns]
            
            if missing_fields:
                self.logger.error(f"ETF {etf_code} 缺少必需字段: {missing_fields}")
                return False
            
            # 检查数据类型
            if not pd.api.types.is_numeric_dtype(data['obv']):
                self.logger.error(f"ETF {etf_code} OBV字段非数值类型")
                return False
            
            return True
            
        except Exception as e:
            self.logger.error(f"数据格式验证异常 {etf_code}: {str(e)}")
            return False
    
    def _prepare_output_data(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        准备输出数据
        
        Args:
            data: 原始数据
            
        Returns:
            格式化后的输出数据
        """
        try:
            # 复制数据避免修改原始数据
            output_data = data.copy()
            
            # 确保字段顺序
            available_fields = [f for f in self.standard_fields if f in output_data.columns]
            output_data = output_data[available_fields]
            
            # 数值精度控制
            numeric_fields = ['obv', 'obv_ma10', 'obv_change_5', 'obv_change_20']
            for field in numeric_fields:
                if field in output_data.columns:
                    output_data[field] = output_data[field].round(self.precision)
            
            # 确保日期格式统一
            if 'date' in output_data.columns:
                output_data['date'] = pd.to_datetime(output_data['date']).dt.strftime('%Y-%m-%d')
            
            # 排序(按ETF代码和日期)
            sort_columns = []
            if 'code' in output_data.columns:
                sort_columns.append('code')
            if 'date' in output_data.columns:
                sort_columns.append('date')
            
            if sort_columns:
                output_data = output_data.sort_values(sort_columns).reset_index(drop=True)
            
            return output_data
            
        except Exception as e:
            self.logger.error(f"准备输出数据失败: {str(e)}")
            return data
    
    def _save_csv_atomic(self, file_path: Path, data: pd.DataFrame) -> bool:
        """
        原子性保存CSV文件
        
        Args:
            file_path: 目标文件路径
            data: 要保存的数据
            
        Returns:
            保存是否成功
        """
        try:
            # 创建临时文件
            temp_file = file_path.with_suffix('.tmp')
            
            # 写入临时文件
            data.to_csv(
                temp_file,
                encoding=self.csv_config['encoding'],
                index=self.csv_config['index'],
                float_format=self.csv_config['float_format']
            )
            
            # 原子性重命名
            temp_file.replace(file_path)
            
            return True
            
        except Exception as e:
            self.logger.error(f"原子性保存失败 {file_path}: {str(e)}")
            
            # 清理临时文件
            try:
                temp_file.unlink(missing_ok=True)
            except:
                pass
            
            return False
    
    def get_handler_statistics(self) -> Dict[str, Any]:
        """
        获取处理器统计信息
        
        Returns:
            统计信息字典
        """
        try:
            # 扫描输出目录
            csv_files = list(self.output_dir.glob("*.csv"))
            
            # 计算总大小
            total_size = sum(f.stat().st_size for f in csv_files)
            
            # 统计记录数(采样前10个文件)
            total_records = 0
            sampled_files = 0
            
            for csv_file in csv_files[:10]:
                try:
                    df = pd.read_csv(csv_file, encoding='utf-8')
                    total_records += len(df)
                    sampled_files += 1
                except:
                    continue
            
            # 估算总记录数
            if sampled_files > 0:
                avg_records_per_file = total_records / sampled_files
                estimated_total_records = int(avg_records_per_file * len(csv_files))
            else:
                estimated_total_records = 0
            
            stats = {
                'output_directory': str(self.output_dir),
                'total_files': len(csv_files),
                'total_size_bytes': total_size,
                'total_size_mb': round(total_size / (1024 * 1024), 2),
                'estimated_total_records': estimated_total_records,
                'precision': self.precision,
                'encoding': self.csv_config['encoding'],
                'directory_exists': self.output_dir.exists(),
                'directory_writable': os.access(self.output_dir, os.W_OK) if self.output_dir.exists() else False,
                'standard_fields': self.standard_fields
            }
            
            return stats
            
        except Exception as e:
            self.logger.error(f"获取处理器统计失败: {str(e)}")
            return {'error': str(e)}