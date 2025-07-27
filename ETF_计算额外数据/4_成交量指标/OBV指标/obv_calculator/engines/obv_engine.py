"""
OBV指标计算引擎 - 核心计算逻辑
==============================

实现OBV (On-Balance Volume) 能量潮指标的向量化计算
基于约瑟夫·格兰维尔理论，针对中国ETF市场优化

核心算法:
- OBV累积计算 (向量化实现)
- OBV移动平均线计算
- 短期和中期变化率计算
- 异常值检测和处理

技术特点:
- NumPy向量化计算，高性能
- 8位小数精度
- 滑动窗口优化
- 内存友好设计
"""

import numpy as np
import pandas as pd
from typing import Dict, List, Tuple, Optional, Any
import logging
from datetime import datetime
import warnings

# 忽略pandas性能警告
warnings.filterwarnings('ignore', category=pd.errors.PerformanceWarning)

class OBVEngine:
    """OBV指标计算引擎"""
    
    def __init__(self, precision: int = 8):
        """
        初始化OBV计算引擎
        
        Args:
            precision: 计算精度(小数位数)
        """
        self.precision = precision
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # 计算参数
        self.ma_period = 10
        self.change_periods = [5, 20]
        self.min_data_points = 21
        self.epsilon = 1e-8
        
        # 异常值处理参数
        self.volume_zero_threshold = 1e-6
        self.volume_max_multiplier = 50
        self.price_change_threshold = 0.15
        
        self.logger.info(f"OBVEngine初始化完成 - 精度:{precision}位小数")
    
    def calculate_obv_batch(self, data: pd.DataFrame) -> Dict[str, Any]:
        """
        批量计算OBV指标 (向量化实现)
        
        Args:
            data: 包含['代码','日期','收盘价','成交量(手数)']的DataFrame
            
        Returns:
            包含计算结果和统计信息的字典
        """
        try:
            start_time = datetime.now()
            
            # 数据预处理和验证
            processed_data = self._preprocess_data(data)
            if processed_data is None:
                return {'success': False, 'error': '数据预处理失败'}
            
            # 检查数据量
            if len(processed_data) < self.min_data_points:
                return {
                    'success': False, 
                    'error': f'数据量不足，需要至少{self.min_data_points}个交易日'
                }
            
            # 核心OBV计算 (向量化)
            obv_results = self._calculate_obv_vectorized(processed_data)
            
            # 计算移动平均线
            obv_ma = self._calculate_moving_average(obv_results['obv'], self.ma_period)
            
            # 计算变化率
            change_rates = self._calculate_change_rates(
                obv_results['obv'], self.change_periods
            )
            
            # 组装最终结果
            final_results = self._assemble_results(
                processed_data, obv_results['obv'], obv_ma, change_rates
            )
            
            # 计算统计信息
            processing_time = (datetime.now() - start_time).total_seconds()
            stats = self._calculate_statistics(final_results, processing_time)
            
            return {
                'success': True,
                'data': final_results,
                'statistics': stats,
                'processing_time': processing_time,
                'data_points': len(final_results)
            }
            
        except Exception as e:
            self.logger.error(f"OBV批量计算异常: {str(e)}")
            return {'success': False, 'error': str(e)}
    
    def calculate_obv_incremental(self, existing_data: pd.DataFrame, 
                                 new_data: pd.DataFrame) -> Dict[str, Any]:
        """
        增量计算OBV指标
        
        Args:
            existing_data: 已有的OBV计算结果
            new_data: 新增的原始数据
            
        Returns:
            增量计算结果
        """
        try:
            start_time = datetime.now()
            
            # 获取最后的OBV值作为初始值
            if len(existing_data) > 0:
                last_obv = existing_data.iloc[-1]['obv']
                last_date = existing_data.iloc[-1]['date']
            else:
                last_obv = 0.0
                last_date = None
            
            # 预处理新数据
            processed_new = self._preprocess_data(new_data)
            if processed_new is None:
                return {'success': False, 'error': '新数据预处理失败'}
            
            # 过滤出需要计算的新数据
            if last_date:
                processed_new = processed_new[
                    processed_new['日期'] > last_date
                ].copy()
            
            if len(processed_new) == 0:
                return {
                    'success': True,
                    'data': pd.DataFrame(),
                    'message': '没有新数据需要计算'
                }
            
            # 增量计算OBV
            incremental_obv = self._calculate_obv_incremental_core(
                processed_new, last_obv
            )
            
            # 合并历史数据用于计算移动平均和变化率
            if len(existing_data) > 0:
                # 获取足够的历史数据用于计算
                history_needed = max(self.ma_period, max(self.change_periods))
                recent_history = existing_data.tail(history_needed).copy()
                
                # 组合历史OBV和新计算的OBV
                combined_obv = np.concatenate([
                    recent_history['obv'].values,
                    incremental_obv['obv'].values
                ])
                
                # 重新计算移动平均和变化率(仅针对新数据部分)
                obv_ma = self._calculate_moving_average(combined_obv, self.ma_period)
                obv_ma_new = obv_ma[-len(incremental_obv):]
                
                change_rates = self._calculate_change_rates(
                    combined_obv, self.change_periods
                )
                change_rates_new = {
                    period: rates[-len(incremental_obv):]
                    for period, rates in change_rates.items()
                }
            else:
                # 没有历史数据，直接计算
                obv_ma_new = self._calculate_moving_average(
                    incremental_obv['obv'].values, self.ma_period
                )
                change_rates_new = self._calculate_change_rates(
                    incremental_obv['obv'].values, self.change_periods
                )
            
            # 组装增量结果
            incremental_results = self._assemble_results(
                processed_new, incremental_obv['obv'].values, 
                obv_ma_new, change_rates_new
            )
            
            processing_time = (datetime.now() - start_time).total_seconds()
            
            return {
                'success': True,
                'data': incremental_results,
                'processing_time': processing_time,
                'data_points': len(incremental_results),
                'incremental': True
            }
            
        except Exception as e:
            self.logger.error(f"OBV增量计算异常: {str(e)}")
            return {'success': False, 'error': str(e)}
    
    def _preprocess_data(self, data: pd.DataFrame) -> Optional[pd.DataFrame]:
        """
        数据预处理和清洗
        
        Args:
            data: 原始数据
            
        Returns:
            处理后的数据或None
        """
        try:
            # 复制数据避免修改原始数据
            df = data.copy()
            
            # 检查必需字段
            required_fields = ['代码', '日期', '收盘价', '成交量(手数)']
            missing_fields = [f for f in required_fields if f not in df.columns]
            if missing_fields:
                self.logger.error(f"缺少必需字段: {missing_fields}")
                return None
            
            # 日期转换和排序 - 按时间正序，便于OBV计算
            df['日期'] = pd.to_datetime(df['日期'])
            df = df.sort_values(['代码', '日期'], ascending=[True, True]).reset_index(drop=True)
            
            # 数值类型转换
            numeric_fields = ['收盘价', '成交量(手数)']
            for field in numeric_fields:
                df[field] = pd.to_numeric(df[field], errors='coerce')
            
            # 异常值处理
            df = self._handle_anomalies(df)
            
            # 移除无效记录
            df = df.dropna(subset=required_fields).reset_index(drop=True)
            
            if len(df) == 0:
                self.logger.warning("预处理后数据为空")
                return None
            
            self.logger.debug(f"数据预处理完成，记录数: {len(df)}")
            return df
            
        except Exception as e:
            self.logger.error(f"数据预处理异常: {str(e)}")
            return None
    
    def _handle_anomalies(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        处理异常值
        
        Args:
            df: 待处理的数据
            
        Returns:
            处理后的数据
        """
        try:
            # 处理成交量异常
            # 零成交量或极小成交量
            df.loc[df['成交量(手数)'] < self.volume_zero_threshold, '成交量(手数)'] = 0
            
            # 异常巨量处理
            volume_median = df['成交量(手数)'].median()
            if volume_median > 0:
                volume_threshold = volume_median * self.volume_max_multiplier
                abnormal_volume = df['成交量(手数)'] > volume_threshold
                if abnormal_volume.any():
                    self.logger.warning(f"发现{abnormal_volume.sum()}个异常成交量记录")
                    # 用中位数替换异常值
                    df.loc[abnormal_volume, '成交量(手数)'] = volume_median
            
            # 处理价格异常变动
            df['price_change'] = df.groupby('代码')['收盘价'].pct_change()
            abnormal_price = abs(df['price_change']) > self.price_change_threshold
            if abnormal_price.any():
                self.logger.debug(f"发现{abnormal_price.sum()}个异常价格变动记录(>15%变动)")
                # 对于异常价格变动，可以选择保留或平滑处理
                # 这里选择保留，因为可能是真实的市场事件
            
            # 清理临时列
            if 'price_change' in df.columns:
                df = df.drop('price_change', axis=1)
            
            return df
            
        except Exception as e:
            self.logger.error(f"异常值处理失败: {str(e)}")
            return df
    
    def _calculate_obv_vectorized(self, data: pd.DataFrame) -> Dict[str, np.ndarray]:
        """
        向量化计算OBV指标
        
        Args:
            data: 预处理后的数据
            
        Returns:
            包含OBV数组和相关信息的字典
        """
        try:
            # 按ETF代码分组计算
            results = []
            
            for code in data['代码'].unique():
                etf_data = data[data['代码'] == code].copy()
                # OBV计算必须按时间正序（累积指标）
                etf_data = etf_data.sort_values('日期', ascending=True).reset_index(drop=True)
                
                if len(etf_data) < 2:
                    continue
                
                # 提取价格和成交量
                close_prices = etf_data['收盘价'].values
                volumes = etf_data['成交量(手数)'].values
                
                # 计算价格变化方向 (向量化)
                price_changes = np.diff(close_prices)
                
                # 初始化OBV数组
                obv = np.zeros(len(close_prices))
                obv[0] = volumes[0]  # 初始值设为首日成交量
                
                # 真正的向量化计算OBV
                # 创建价格方向向量：上涨=1, 下跌=-1, 平盘=0
                price_direction = np.zeros(len(price_changes))
                price_direction[price_changes > self.epsilon] = 1
                price_direction[price_changes < -self.epsilon] = -1
                
                # 计算每日OBV变化量 (向量化)
                obv_changes = price_direction * volumes[1:]  # 从第二天开始
                
                # 使用cumsum累积计算OBV (向量化)
                obv[1:] = obv[0] + np.cumsum(obv_changes)
                
                # 精度控制
                obv = np.round(obv, self.precision)
                
                results.append({
                    'code': code,
                    'obv': obv,
                    'data_points': len(obv)
                })
            
            # 合并所有ETF的结果
            if results:
                all_obv = np.concatenate([r['obv'] for r in results])
                return {
                    'obv': all_obv,
                    'etf_count': len(results),
                    'total_points': len(all_obv)
                }
            else:
                return {'obv': np.array([]), 'etf_count': 0, 'total_points': 0}
                
        except Exception as e:
            self.logger.error(f"OBV向量化计算异常: {str(e)}")
            raise
    
    def _calculate_obv_incremental_core(self, new_data: pd.DataFrame, 
                                      last_obv: float) -> pd.DataFrame:
        """
        增量计算OBV核心逻辑
        
        Args:
            new_data: 新增数据
            last_obv: 上一个OBV值
            
        Returns:
            包含新计算OBV的DataFrame
        """
        try:
            results = []
            current_obv = last_obv
            
            # 按ETF代码分组处理
            for code in new_data['代码'].unique():
                etf_data = new_data[new_data['代码'] == code].copy()
                # OBV增量计算也必须按时间正序
                etf_data = etf_data.sort_values('日期', ascending=True).reset_index(drop=True)
                
                # 获取该ETF的历史OBV值作为起点
                # (这里简化处理，实际应该从缓存中获取该ETF的最后OBV值)
                etf_obv = current_obv
                
                for idx, row in etf_data.iterrows():
                    if idx == 0:
                        # 第一条记录需要获取前一日收盘价来判断方向
                        # 这里简化处理，假设为平盘
                        results.append({
                            '代码': code,
                            '日期': row['日期'],
                            'obv': etf_obv
                        })
                        continue
                    
                    prev_close = etf_data.iloc[idx-1]['收盘价']
                    curr_close = row['收盘价']
                    volume = row['成交量(手数)']
                    
                    price_change = curr_close - prev_close
                    
                    if price_change > self.epsilon:
                        etf_obv += volume
                    elif price_change < -self.epsilon:
                        etf_obv -= volume
                    # 平盘不变
                    
                    etf_obv = round(etf_obv, self.precision)
                    
                    results.append({
                        '代码': code,
                        '日期': row['日期'],
                        'obv': etf_obv
                    })
            
            return pd.DataFrame(results)
            
        except Exception as e:
            self.logger.error(f"增量OBV计算核心逻辑异常: {str(e)}")
            raise
    
    def _calculate_moving_average(self, obv_values: np.ndarray, 
                                 period: int) -> np.ndarray:
        """
        计算OBV移动平均线 (向量化实现)
        
        Args:
            obv_values: OBV值数组
            period: 移动平均周期
            
        Returns:
            移动平均数组
        """
        try:
            if len(obv_values) < period:
                return np.full(len(obv_values), np.nan)
            
            # 使用pandas rolling计算移动平均(更高效)
            obv_series = pd.Series(obv_values)
            ma_values = obv_series.rolling(window=period, min_periods=1).mean().values
            
            # 精度控制
            ma_values = np.round(ma_values, self.precision)
            
            return ma_values
            
        except Exception as e:
            self.logger.error(f"移动平均计算异常: {str(e)}")
            return np.full(len(obv_values), np.nan)
    
    def _calculate_change_rates(self, obv_values: np.ndarray, 
                               periods: List[int]) -> Dict[int, np.ndarray]:
        """
        计算OBV变化率 (向量化实现)
        
        Args:
            obv_values: OBV值数组
            periods: 变化率周期列表
            
        Returns:
            各周期变化率字典
        """
        try:
            change_rates = {}
            
            for period in periods:
                if len(obv_values) < period + 1:
                    change_rates[period] = np.full(len(obv_values), np.nan)
                    continue
                
                # 向量化计算变化率
                rates = np.full(len(obv_values), np.nan)
                
                for i in range(period, len(obv_values)):
                    prev_value = obv_values[i - period]
                    curr_value = obv_values[i]
                    
                    if abs(prev_value) > self.epsilon:
                        rate = (curr_value - prev_value) / abs(prev_value) * 100
                        rates[i] = round(rate, self.precision)
                
                change_rates[period] = rates
            
            return change_rates
            
        except Exception as e:
            self.logger.error(f"变化率计算异常: {str(e)}")
            return {period: np.full(len(obv_values), np.nan) for period in periods}
    
    def _assemble_results(self, original_data: pd.DataFrame, 
                         obv_values: np.ndarray,
                         ma_values: np.ndarray,
                         change_rates: Dict[int, np.ndarray]) -> pd.DataFrame:
        """
        组装最终计算结果
        
        Args:
            original_data: 原始数据
            obv_values: OBV值数组
            ma_values: 移动平均数组
            change_rates: 变化率字典
            
        Returns:
            最终结果DataFrame
        """
        try:
            # 创建结果DataFrame
            results = original_data[['代码', '日期']].copy()
            results['obv'] = obv_values
            results['obv_ma10'] = ma_values
            
            # 添加变化率字段
            for period in sorted(change_rates.keys()):
                field_name = f'obv_change_{period}'
                results[field_name] = change_rates[period]
            
            # 添加计算时间戳
            results['calc_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            # 重命名列名以匹配输出格式
            results = results.rename(columns={'代码': 'code', '日期': 'date'})
            
            # 最终输出按日期倒序排列（最新日期在前）
            # 先转换为datetime进行正确的日期排序
            results['date_temp'] = pd.to_datetime(results['date'])
            results = results.sort_values(['code', 'date_temp'], ascending=[True, False]).reset_index(drop=True)
            results = results.drop('date_temp', axis=1)
            
            # 确保日期格式正确
            results['date'] = pd.to_datetime(results['date']).dt.strftime('%Y-%m-%d')
            
            return results
            
        except Exception as e:
            self.logger.error(f"结果组装异常: {str(e)}")
            raise
    
    def _calculate_statistics(self, results: pd.DataFrame, 
                            processing_time: float) -> Dict[str, Any]:
        """
        计算统计信息
        
        Args:
            results: 计算结果
            processing_time: 处理时间
            
        Returns:
            统计信息字典
        """
        try:
            if results.empty:
                return {'error': '结果为空，无法计算统计信息'}
            
            stats = {
                'total_records': len(results),
                'etf_count': results['code'].nunique(),
                'date_range': {
                    'start': results['date'].min(),
                    'end': results['date'].max()
                },
                'processing_time': processing_time,
                'avg_time_per_record': processing_time / len(results) if len(results) > 0 else 0,
                'obv_statistics': {
                    'min': float(results['obv'].min()),
                    'max': float(results['obv'].max()),
                    'mean': float(results['obv'].mean()),
                    'std': float(results['obv'].std())
                },
                'data_quality': {
                    'obv_valid_ratio': (results['obv'].notna().sum() / len(results)),
                    'ma_valid_ratio': (results['obv_ma10'].notna().sum() / len(results)),
                    'change_5_valid_ratio': (results['obv_change_5'].notna().sum() / len(results)) if 'obv_change_5' in results.columns else 0,
                    'change_20_valid_ratio': (results['obv_change_20'].notna().sum() / len(results)) if 'obv_change_20' in results.columns else 0
                }
            }
            
            return stats
            
        except Exception as e:
            self.logger.error(f"统计信息计算异常: {str(e)}")
            return {'error': str(e)}
    
    def validate_input_data(self, data: pd.DataFrame) -> Dict[str, Any]:
        """
        验证输入数据
        
        Args:
            data: 待验证的数据
            
        Returns:
            验证结果
        """
        try:
            validation_result = {
                'valid': True,
                'errors': [],
                'warnings': [],
                'summary': {}
            }
            
            # 检查基本结构
            if data.empty:
                validation_result['valid'] = False
                validation_result['errors'].append('数据为空')
                return validation_result
            
            # 检查必需字段
            required_fields = ['代码', '日期', '收盘价', '成交量(手数)']
            missing_fields = [f for f in required_fields if f not in data.columns]
            if missing_fields:
                validation_result['valid'] = False
                validation_result['errors'].append(f'缺少必需字段: {missing_fields}')
            
            # 检查数据类型和范围
            if '收盘价' in data.columns:
                if not pd.api.types.is_numeric_dtype(data['收盘价']):
                    validation_result['warnings'].append('收盘价字段非数值类型')
                elif (data['收盘价'] <= 0).any():
                    validation_result['warnings'].append('存在非正数收盘价')
            
            if '成交量(手数)' in data.columns:
                if not pd.api.types.is_numeric_dtype(data['成交量(手数)']):
                    validation_result['warnings'].append('成交量字段非数值类型')
                elif (data['成交量(手数)'] < 0).any():
                    validation_result['warnings'].append('存在负成交量')
            
            # 检查数据量
            etf_counts = data.groupby('代码').size()
            insufficient_data = etf_counts[etf_counts < self.min_data_points]
            if len(insufficient_data) > 0:
                validation_result['warnings'].append(
                    f'{len(insufficient_data)}个ETF数据量不足{self.min_data_points}个交易日'
                )
            
            # 汇总信息
            validation_result['summary'] = {
                'total_records': len(data),
                'etf_count': data['代码'].nunique() if '代码' in data.columns else 0,
                'date_range': {
                    'start': str(data['日期'].min()) if '日期' in data.columns else None,
                    'end': str(data['日期'].max()) if '日期' in data.columns else None
                },
                'avg_records_per_etf': len(data) / data['代码'].nunique() if '代码' in data.columns and data['代码'].nunique() > 0 else 0
            }
            
            return validation_result
            
        except Exception as e:
            return {
                'valid': False,
                'errors': [f'验证过程异常: {str(e)}'],
                'warnings': [],
                'summary': {}
            }